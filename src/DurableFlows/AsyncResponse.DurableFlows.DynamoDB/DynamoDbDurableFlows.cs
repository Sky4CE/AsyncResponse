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

            // Singleton on purpose: table/TTL provisioning is cached per store instance and DynamoDB
            // control-plane calls are throttled account-wide — a scoped store would re-issue them on
            // every flow execution. A host-registered IAmazonDynamoDB is reused when present;
            // otherwise the store creates and owns a client from the default AWS credential/region
            // chain. Nothing is registered as a bare IAmazonDynamoDB service, so unrelated
            // resolutions of that type are never answered — or broken — by this package.
            builder.Services.TryAddSingleton(provider =>
            {
                var options = provider.GetRequiredService<IOptions<DynamoDbDurableFlowOptions>>();
                var shared = provider.GetService<IAmazonDynamoDB>();
                return shared is not null
                    ? new DynamoDbFlowStateStore(shared, options)
                    : new DynamoDbFlowStateStore(new AmazonDynamoDBClient(), options, ownsClient: true);
            });
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
public sealed class DynamoDbFlowStateStore : IFlowStateStore, IDisposable
{
    private const string FlowIdAttribute = "flow_id";
    private const string StateJsonAttribute = "state_json";
    private const string UpdatedAtAttribute = "updated_at";

    private readonly IAmazonDynamoDB _client;
    private readonly DynamoDbDurableFlowOptions _options;
    private readonly SemaphoreSlim _ensureGate = new(1, 1);
    private readonly bool _ownsClient;
    private bool _created;

    public DynamoDbFlowStateStore(IAmazonDynamoDB client, IOptions<DynamoDbDurableFlowOptions> options, bool ownsClient = false)
    {
        _client = client;
        _options = options.Value;
        _options.Validate();
        _ownsClient = ownsClient;
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
                // Ceiling, not floor: DynamoDB TTL has whole-second granularity, and rounding down
                // would make the effective TTL up to a second SHORTER than requested.
                [_options.TimeToLiveAttributeName] = new() { N = UnixSecondsCeiling(now.Add(ttl)) },
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

            TableDescription? table = null;
            try
            {
                var described = await _client.DescribeTableAsync(_options.TableName, cancellationToken).ConfigureAwait(false);
                table = described.Table;
            }
            catch (ResourceNotFoundException)
            {
            }

            if (table is null)
            {
                try
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
                catch (ResourceInUseException)
                {
                    // Another process won the create race; fall through and wait for ACTIVE.
                }

                await WaitForTableActiveAsync(cancellationToken).ConfigureAwait(false);
            }
            else if (table.TableStatus != TableStatus.ACTIVE)
            {
                await WaitForTableActiveAsync(cancellationToken).ConfigureAwait(false);
            }

            if (_options.EnableTimeToLive)
            {
                // Check the TTL status instead of blind-enabling: UpdateTimeToLive throws when TTL
                // is already enabled, and relying on a swallowed exception per provisioning is noise.
                var ttlStatus = await _client.DescribeTimeToLiveAsync(new DescribeTimeToLiveRequest
                {
                    TableName = _options.TableName
                }, cancellationToken).ConfigureAwait(false);

                var status = ttlStatus.TimeToLiveDescription?.TimeToLiveStatus;
                if (status != TimeToLiveStatus.ENABLED && status != TimeToLiveStatus.ENABLING)
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
                        // A concurrent process enabled TTL between the describe and the update.
                    }
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

    private static string UnixSecondsCeiling(DateTimeOffset value)
        => ((long)Math.Ceiling(value.ToUnixTimeMilliseconds() / 1000.0)).ToString(CultureInfo.InvariantCulture);

    /// <summary>Disposes the DynamoDB client when the store created (and therefore owns) it.</summary>
    public void Dispose()
    {
        _ensureGate.Dispose();
        if (_ownsClient)
            _client.Dispose();
    }
}
}
