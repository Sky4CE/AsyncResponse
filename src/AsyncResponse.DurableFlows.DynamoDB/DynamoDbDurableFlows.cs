using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using AsyncResponse;
using AsyncResponse.DurableFlows.DynamoDB;
using AsyncResponse.DurableFlows.Internal;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using System.Globalization;

namespace Microsoft.Extensions.DependencyInjection
{
    /// <summary>DI registration for the DynamoDB durable-flow state store.</summary>
    public static class DynamoDbDurableFlowServiceCollectionExtensions
    {
        /// <summary>
        /// Stores durable-flow state in DynamoDB. Hosts may register an <see cref="IAmazonDynamoDB"/>
        /// client; otherwise the default AWS credential/region chain is used.
        /// </summary>
        public static AsyncResponseRegistrationBuilder WithDynamoDbDurableFlows(
            this AsyncResponseRegistrationBuilder builder,
            Action<DynamoDbDurableFlowOptions>? configure = null)
        {
            builder.Services.AddOptions();
            if (configure is not null)
                builder.Services.Configure(configure);

            builder.Services.TryAddSingleton<IAmazonDynamoDB>(_ => new AmazonDynamoDBClient());
            builder.Services.TryAddScoped<DynamoDbFlowStateStore>();
            return builder.WithCustomDurableFlows<DynamoDbFlowStateStore>();
        }
    }
}

namespace AsyncResponse.DurableFlows.DynamoDB
{
/// <summary>Options for the DynamoDB durable-flow state store.</summary>
public sealed class DynamoDbDurableFlowOptions
{
    /// <summary>Table storing one durable-flow ledger item per flow id.</summary>
    public string TableName { get; set; } = "AsyncResponseFlowState";

    /// <summary>Creates the table on first use when it does not exist.</summary>
    public bool AutoCreateTable { get; set; } = true;

    /// <summary>Enables DynamoDB TTL on the expiry attribute when auto-creating the table.</summary>
    public bool EnableTimeToLive { get; set; } = true;

    /// <summary>Attribute used for DynamoDB TTL. Default: <c>expires_at</c>.</summary>
    public string TimeToLiveAttributeName { get; set; } = "expires_at";

    /// <summary>Validates option values and throws on misconfiguration.</summary>
    public void Validate()
    {
        if (string.IsNullOrWhiteSpace(TableName))
            throw new InvalidOperationException($"{nameof(DynamoDbDurableFlowOptions)}.{nameof(TableName)} must be configured.");
        if (string.IsNullOrWhiteSpace(TimeToLiveAttributeName))
            throw new InvalidOperationException($"{nameof(DynamoDbDurableFlowOptions)}.{nameof(TimeToLiveAttributeName)} must be configured.");
    }
}

/// <summary>DynamoDB implementation of <see cref="IFlowStateStore"/>.</summary>
public sealed class DynamoDbFlowStateStore : IFlowStateStore
{
    private const string FlowIdAttribute = "flow_id";
    private const string StateJsonAttribute = "state_json";
    private const string UpdatedAtAttribute = "updated_at";

    private readonly IAmazonDynamoDB _client;
    private readonly DynamoDbDurableFlowOptions _options;
    private readonly SemaphoreSlim _ensureGate = new(1, 1);
    private bool _created;

    public DynamoDbFlowStateStore(IAmazonDynamoDB client, IOptions<DynamoDbDurableFlowOptions> options)
    {
        _client = client;
        _options = options.Value;
        _options.Validate();
    }

    public async Task SaveAsync(string flowId, FlowState state, TimeSpan ttl, CancellationToken cancellationToken = default)
    {
        DurableFlowStoreShared.ValidateSave(flowId, state, ttl);
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);

        var now = DateTimeOffset.UtcNow;
        await _client.PutItemAsync(new PutItemRequest
        {
            TableName = _options.TableName,
            Item = new Dictionary<string, AttributeValue>
            {
                [FlowIdAttribute] = new() { S = flowId },
                [StateJsonAttribute] = new() { S = DurableFlowStoreShared.Serialize(state) },
                [_options.TimeToLiveAttributeName] = new() { N = UnixSeconds(now.Add(ttl)) },
                [UpdatedAtAttribute] = new() { N = UnixSeconds(now) }
            }
        }, cancellationToken).ConfigureAwait(false);
    }

    public async Task<FlowState?> LoadAsync(string flowId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(flowId);
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);

        var response = await _client.GetItemAsync(new GetItemRequest
        {
            TableName = _options.TableName,
            Key = Key(flowId),
            ConsistentRead = true
        }, cancellationToken).ConfigureAwait(false);

        if (response.Item is null || response.Item.Count == 0)
            return null;
        if (!response.Item.TryGetValue(_options.TimeToLiveAttributeName, out var expires) ||
            !long.TryParse(expires.N, NumberStyles.Integer, CultureInfo.InvariantCulture, out var expiresAt) ||
            expiresAt <= DateTimeOffset.UtcNow.ToUnixTimeSeconds())
            return null;
        if (!response.Item.TryGetValue(StateJsonAttribute, out var json) || string.IsNullOrEmpty(json.S))
            return null;

        return DurableFlowStoreShared.Deserialize(json.S);
    }

    public async Task<bool> TryDeleteAsync(string flowId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(flowId);
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);

        var response = await _client.DeleteItemAsync(new DeleteItemRequest
        {
            TableName = _options.TableName,
            Key = Key(flowId),
            ReturnValues = ReturnValue.ALL_OLD
        }, cancellationToken).ConfigureAwait(false);
        return response.Attributes is { Count: > 0 };
    }

    private async Task EnsureCreatedAsync(CancellationToken cancellationToken)
    {
        if (_created || !_options.AutoCreateTable)
            return;

        await _ensureGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_created)
                return;

            try
            {
                await _client.DescribeTableAsync(_options.TableName, cancellationToken).ConfigureAwait(false);
            }
            catch (ResourceNotFoundException)
            {
                await _client.CreateTableAsync(new CreateTableRequest
                {
                    TableName = _options.TableName,
                    BillingMode = BillingMode.PAY_PER_REQUEST,
                    AttributeDefinitions =
                    [
                        new AttributeDefinition(FlowIdAttribute, ScalarAttributeType.S)
                    ],
                    KeySchema =
                    [
                        new KeySchemaElement(FlowIdAttribute, KeyType.HASH)
                    ]
                }, cancellationToken).ConfigureAwait(false);
            }

            await WaitForTableActiveAsync(cancellationToken).ConfigureAwait(false);

            if (_options.EnableTimeToLive)
            {
                try
                {
                    await _client.UpdateTimeToLiveAsync(new UpdateTimeToLiveRequest
                    {
                        TableName = _options.TableName,
                        TimeToLiveSpecification = new TimeToLiveSpecification
                        {
                            AttributeName = _options.TimeToLiveAttributeName,
                            Enabled = true
                        }
                    }, cancellationToken).ConfigureAwait(false);
                }
                catch (AmazonDynamoDBException ex) when (string.Equals(ex.ErrorCode, "ValidationException", StringComparison.Ordinal))
                {
                }
            }

            _created = true;
        }
        finally
        {
            _ensureGate.Release();
        }
    }

    private async Task WaitForTableActiveAsync(CancellationToken cancellationToken)
    {
        var deadline = DateTime.UtcNow.AddSeconds(30);
        while (true)
        {
            var response = await _client.DescribeTableAsync(_options.TableName, cancellationToken).ConfigureAwait(false);
            if (response.Table.TableStatus == TableStatus.ACTIVE)
                return;
            if (DateTime.UtcNow >= deadline)
                throw new TimeoutException($"DynamoDB table '{_options.TableName}' did not become ACTIVE within 30 seconds.");

            await Task.Delay(TimeSpan.FromMilliseconds(250), cancellationToken).ConfigureAwait(false);
        }
    }

    private static Dictionary<string, AttributeValue> Key(string flowId)
        => new(StringComparer.Ordinal) { [FlowIdAttribute] = new AttributeValue { S = flowId } };

    private static string UnixSeconds(DateTimeOffset value)
        => value.ToUnixTimeSeconds().ToString(CultureInfo.InvariantCulture);
}
}
