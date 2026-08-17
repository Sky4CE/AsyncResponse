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
            return builder.WithDurableFlows<DynamoDbFlowStateStore, DynamoDbDurableFlowOptions>(configure);
        }
    }
}

namespace AsyncResponse.DurableFlows.DynamoDB
{
/// <summary>Options for the DynamoDB durable-flow state store.</summary>
public sealed class DynamoDbDurableFlowOptions : DurableFlowOptions
{
    /// <summary>Table storing one durable-flow ledger item per flow id.</summary>
    public string TableName { get; set; } = "AsyncResponseFlowState";

    /// <summary>Creates the table on first use when it does not exist.</summary>
    public bool AutoCreateTable { get; set; } = true;

    /// <summary>Enables DynamoDB TTL on the expiry attribute when auto-creating the table.</summary>
    public bool EnableTimeToLive { get; set; } = true;

    /// <summary>Attribute used for DynamoDB TTL. Default: <c>expires_at</c>.</summary>
    public string TimeToLiveAttributeName { get; set; } = "expires_at";

    /// <summary>
    /// Maximum serialized flow-state size in bytes accepted by writes; oversized ledgers fail fast
    /// with an actionable error instead of the raw 400 KB item-cap ValidationException the
    /// executor would retry into the dead-letter queue. Default: 350 KB (headroom under DynamoDB's
    /// 400 KB item cap for the sibling attributes); <c>null</c> disables the guard.
    /// </summary>
    public long? MaxStateBytes { get; set; } = 350_000;

    /// <summary>Validates option values and throws on misconfiguration.</summary>
    public void Validate()
    {
        if (string.IsNullOrWhiteSpace(TableName))
            throw new InvalidOperationException($"{nameof(DynamoDbDurableFlowOptions)}.{nameof(TableName)} must be configured.");
        if (string.IsNullOrWhiteSpace(TimeToLiveAttributeName))
            throw new InvalidOperationException($"{nameof(DynamoDbDurableFlowOptions)}.{nameof(TimeToLiveAttributeName)} must be configured.");
        // A TTL attribute colliding with one of the store's own attributes would let the later
        // item-initializer assignment silently overwrite the TTL value (e.g. with the revision
        // number), making every new flow read back as already-expired with no error anywhere.
        if (TimeToLiveAttributeName is DynamoDbFlowStateStore.FlowIdAttribute
            or DynamoDbFlowStateStore.StateJsonAttribute
            or DynamoDbFlowStateStore.UpdatedAtAttribute
            or DynamoDbFlowStateStore.RevisionAttribute
            or DynamoDbFlowStateStore.LeaseIdAttribute
            or DynamoDbFlowStateStore.LeaseExpiresAtAttribute)
        {
            throw new InvalidOperationException(
                $"{nameof(DynamoDbDurableFlowOptions)}.{nameof(TimeToLiveAttributeName)} must not collide with one of the " +
                $"store's own attributes ('{DynamoDbFlowStateStore.FlowIdAttribute}', '{DynamoDbFlowStateStore.StateJsonAttribute}', " +
                $"'{DynamoDbFlowStateStore.UpdatedAtAttribute}', '{DynamoDbFlowStateStore.RevisionAttribute}', " +
                $"'{DynamoDbFlowStateStore.LeaseIdAttribute}', '{DynamoDbFlowStateStore.LeaseExpiresAtAttribute}').");
        }
        DurableFlowStoreShared.ValidateMaxStateBytes(MaxStateBytes, nameof(DynamoDbDurableFlowOptions));
    }
}

/// <summary>DynamoDB implementation of <see cref="IFlowStateStore"/>.</summary>
public sealed class DynamoDbFlowStateStore : IFlowStateStore, IDisposable
{
    internal const string FlowIdAttribute = "flow_id";
    internal const string StateJsonAttribute = "state_json";
    internal const string UpdatedAtAttribute = "updated_at";
    internal const string RevisionAttribute = "revision";
    internal const string LeaseIdAttribute = "lease_id";
    internal const string LeaseExpiresAtAttribute = "lease_expires_at_ms";

    // Time authority: this store keeps the app clock (DateTimeOffset.UtcNow) for expiry and lease
    // comparisons. DynamoDB condition expressions evaluate client-supplied values only — there is
    // no server-clock function available in a conditional write — so multi-node deployments
    // should keep worker clocks synchronized well inside the lease window. (DynamoDB's own TTL
    // reaper, by contrast, runs on the service clock against the epoch-seconds expiry attribute.)

    private readonly IAmazonDynamoDB _client;
    private readonly DynamoDbDurableFlowOptions _options;
    private readonly SemaphoreSlim _ensureGate = new(1, 1);
    private readonly bool _ownsClient;
    private volatile bool _created;

    public DynamoDbFlowStateStore(IAmazonDynamoDB client, IOptions<DynamoDbDurableFlowOptions> options, bool ownsClient = false)
    {
        _client = client;
        _options = options.Value;
        _options.Validate();
        _ownsClient = ownsClient;
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

        // No item: the run is genuinely gone. This is the ONLY shape that may read as absence,
        // because callers acknowledge the wake-up on null.
        if (response.Item is null || response.Item.Count == 0)
            return null;

        // From here the item EXISTS. A required attribute that is missing or unparseable means this
        // build cannot interpret a ledger that is still physically there — categorically different
        // from absence, and reporting it as absence acknowledged a live run's only wake-up. Only a
        // well-formed, elapsed TTL is real expiry.
        if (!response.Item.TryGetValue(_options.TimeToLiveAttributeName, out var expires)
            || !long.TryParse(expires.N, NumberStyles.Integer, CultureInfo.InvariantCulture, out var expiresAt))
            throw new FlowStateUnreadableException(flowId, $"its '{_options.TimeToLiveAttributeName}' attribute is missing or not an epoch-seconds number");

        if (expiresAt <= DateTimeOffset.UtcNow.ToUnixTimeSeconds())
            return null;

        if (!response.Item.TryGetValue(StateJsonAttribute, out var json) || string.IsNullOrEmpty(json.S))
            throw new FlowStateUnreadableException(flowId, $"its '{StateJsonAttribute}' attribute is missing or empty");

        if (!response.Item.TryGetValue(RevisionAttribute, out var revision)
            || !long.TryParse(revision.N, NumberStyles.Integer, CultureInfo.InvariantCulture, out var value))
            throw new FlowStateUnreadableException(flowId, $"its '{RevisionAttribute}' attribute is missing or not a number");

        return DurableFlowStoreShared.ReadState(flowId, json.S, value);
    }

    public async Task<bool> TryCreateAsync(string flowId, FlowState state, TimeSpan ttl, CancellationToken cancellationToken = default)
    {
        DurableFlowStoreShared.ValidateCreate(flowId, state, ttl);
        var stateJson = DurableFlowStoreShared.SerializeBounded(flowId, state, _options.MaxStateBytes, "DynamoDB");
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);

        var now = DateTimeOffset.UtcNow;
        try
        {
            await _client.PutItemAsync(new PutItemRequest
            {
                TableName = _options.TableName,
                Item = CreateItem(flowId, stateJson, state.Revision, ttl, now),
                ConditionExpression = "attribute_not_exists(#flow_id) OR #expires <= :now",
                ExpressionAttributeNames = new Dictionary<string, string>
                {
                    ["#flow_id"] = FlowIdAttribute,
                    ["#expires"] = _options.TimeToLiveAttributeName
                },
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                {
                    [":now"] = new() { N = UnixSeconds(now) }
                }
            }, cancellationToken).ConfigureAwait(false);
            return true;
        }
        catch (ConditionalCheckFailedException)
        {
            return false;
        }
    }

    public async Task<bool> TryUpdateAsync(
        string flowId,
        FlowState state,
        long expectedRevision,
        TimeSpan ttl,
        string? leaseId = null,
        CancellationToken cancellationToken = default)
    {
        DurableFlowStoreShared.ValidateUpdate(flowId, state, expectedRevision, ttl);
        var stateJson = DurableFlowStoreShared.SerializeBounded(flowId, state, _options.MaxStateBytes, "DynamoDB");
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);

        var now = DateTimeOffset.UtcNow;
        var names = new Dictionary<string, string>
        {
            ["#state"] = StateJsonAttribute,
            ["#expires"] = _options.TimeToLiveAttributeName,
            ["#updated"] = UpdatedAtAttribute,
            ["#revision"] = RevisionAttribute
        };
        var values = new Dictionary<string, AttributeValue>
        {
            [":state"] = new() { S = stateJson },
            [":expires"] = new() { N = UnixSecondsCeiling(DurableFlowStoreShared.AddSaturating(now, ttl)) },
            [":updated"] = new() { N = UnixSeconds(now) },
            [":expected_revision"] = new() { N = expectedRevision.ToString(CultureInfo.InvariantCulture) },
            [":new_revision"] = new() { N = state.Revision.ToString(CultureInfo.InvariantCulture) },
            [":now"] = new() { N = UnixSeconds(now) }
        };
        var condition = "#revision = :expected_revision AND #expires > :now";
        if (leaseId is not null)
        {
            condition += " AND #lease_id = :lease_id AND #lease_expires > :now_ms";
            names["#lease_id"] = LeaseIdAttribute;
            names["#lease_expires"] = LeaseExpiresAtAttribute;
            values[":lease_id"] = new AttributeValue { S = leaseId };
            values[":now_ms"] = new AttributeValue { N = UnixMilliseconds(now) };
        }

        try
        {
            await _client.UpdateItemAsync(new UpdateItemRequest
            {
                TableName = _options.TableName,
                Key = Key(flowId),
                UpdateExpression = "SET #state = :state, #expires = :expires, #updated = :updated, #revision = :new_revision",
                ConditionExpression = condition,
                ExpressionAttributeNames = names,
                ExpressionAttributeValues = values
            }, cancellationToken).ConfigureAwait(false);
            return true;
        }
        catch (ConditionalCheckFailedException)
        {
            return false;
        }
    }

    public Task<bool> TryAcquireLeaseAsync(string flowId, string leaseId, TimeSpan leaseDuration, CancellationToken cancellationToken = default)
        => UpdateLeaseAsync(flowId, leaseId, leaseDuration, acquire: true, cancellationToken);

    public Task<bool> TryRenewLeaseAsync(string flowId, string leaseId, TimeSpan leaseDuration, CancellationToken cancellationToken = default)
        => UpdateLeaseAsync(flowId, leaseId, leaseDuration, acquire: false, cancellationToken);

    public async Task ReleaseLeaseAsync(string flowId, string leaseId, CancellationToken cancellationToken = default)
    {
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await _client.UpdateItemAsync(new UpdateItemRequest
            {
                TableName = _options.TableName,
                Key = Key(flowId),
                UpdateExpression = "REMOVE #lease_id, #lease_expires",
                ConditionExpression = "#lease_id = :lease_id",
                ExpressionAttributeNames = new Dictionary<string, string>
                {
                    ["#lease_id"] = LeaseIdAttribute,
                    ["#lease_expires"] = LeaseExpiresAtAttribute
                },
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                {
                    [":lease_id"] = new() { S = leaseId }
                }
            }, cancellationToken).ConfigureAwait(false);
        }
        catch (ConditionalCheckFailedException)
        {
        }
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
        if (_created)
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
                if (!_options.AutoCreateTable)
                    throw new InvalidOperationException(
                        $"DynamoDB table '{_options.TableName}' does not exist and {nameof(DynamoDbDurableFlowOptions.AutoCreateTable)} is disabled.");

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

                table = await WaitForTableActiveAsync(cancellationToken).ConfigureAwait(false);
            }
            else if (table.TableStatus != TableStatus.ACTIVE)
            {
                table = await WaitForTableActiveAsync(cancellationToken).ConfigureAwait(false);
            }

            ValidateTableSchema(table);

            if (_options.EnableTimeToLive)
            {
                // Check the TTL status instead of blind-enabling: UpdateTimeToLive throws when TTL
                // is already enabled, and relying on a swallowed exception per provisioning is noise.
                var ttlStatus = await _client.DescribeTimeToLiveAsync(new DescribeTimeToLiveRequest
                {
                    TableName = _options.TableName
                }, cancellationToken).ConfigureAwait(false);

                var description = ttlStatus.TimeToLiveDescription;
                var status = description?.TimeToLiveStatus;
                if ((status == TimeToLiveStatus.ENABLED || status == TimeToLiveStatus.ENABLING)
                    && !string.Equals(description?.AttributeName, _options.TimeToLiveAttributeName, StringComparison.Ordinal))
                {
                    throw new InvalidOperationException(
                        $"DynamoDB table '{_options.TableName}' has TTL configured on attribute '{description?.AttributeName}', " +
                        $"but '{_options.TimeToLiveAttributeName}' is required.");
                }

                if (status != TimeToLiveStatus.ENABLED && status != TimeToLiveStatus.ENABLING)
                {
                    if (!_options.AutoCreateTable)
                    {
                        throw new InvalidOperationException(
                            $"DynamoDB table '{_options.TableName}' does not have TTL enabled on " +
                            $"'{_options.TimeToLiveAttributeName}'. Enable it in infrastructure before using the table.");
                    }

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
                        // Accept only the one safe race: another process enabled the expected TTL
                        // attribute between our describe and update. Do not swallow unrelated
                        // validation failures.
                        var raced = await _client.DescribeTimeToLiveAsync(new DescribeTimeToLiveRequest
                        {
                            TableName = _options.TableName
                        }, cancellationToken).ConfigureAwait(false);
                        var racedDescription = raced.TimeToLiveDescription;
                        if ((racedDescription?.TimeToLiveStatus != TimeToLiveStatus.ENABLED
                                && racedDescription?.TimeToLiveStatus != TimeToLiveStatus.ENABLING)
                            || !string.Equals(racedDescription.AttributeName, _options.TimeToLiveAttributeName, StringComparison.Ordinal))
                        {
                            throw;
                        }
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

    private async Task<TableDescription> WaitForTableActiveAsync(CancellationToken cancellationToken)
    {
        var deadline = DateTime.UtcNow.AddSeconds(30);
        while (true)
        {
            var response = await _client.DescribeTableAsync(_options.TableName, cancellationToken).ConfigureAwait(false);
            if (response.Table.TableStatus == TableStatus.ACTIVE)
                return response.Table;
            if (DateTime.UtcNow >= deadline)
                throw new TimeoutException($"DynamoDB table '{_options.TableName}' did not become ACTIVE within 30 seconds.");

            await Task.Delay(TimeSpan.FromMilliseconds(250), cancellationToken).ConfigureAwait(false);
        }
    }

    private void ValidateTableSchema(TableDescription table)
    {
        var hashKey = table.KeySchema?.SingleOrDefault(key => key.KeyType == KeyType.HASH);
        var keyDefinition = table.AttributeDefinitions?
            .SingleOrDefault(attribute => string.Equals(attribute.AttributeName, FlowIdAttribute, StringComparison.Ordinal));
        if (table.KeySchema?.Count != 1
            || !string.Equals(hashKey?.AttributeName, FlowIdAttribute, StringComparison.Ordinal)
            || keyDefinition?.AttributeType != ScalarAttributeType.S)
        {
            throw new InvalidOperationException(
                $"DynamoDB table '{_options.TableName}' must use one string partition key named '{FlowIdAttribute}' and no sort key.");
        }
    }

    private async Task<bool> UpdateLeaseAsync(
        string flowId,
        string leaseId,
        TimeSpan leaseDuration,
        bool acquire,
        CancellationToken cancellationToken)
    {
        DurableFlowStoreShared.ValidateLeaseArgs(flowId, leaseId, leaseDuration);

        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        var now = DateTimeOffset.UtcNow;
        try
        {
            await _client.UpdateItemAsync(new UpdateItemRequest
            {
                TableName = _options.TableName,
                Key = Key(flowId),
                UpdateExpression = "SET #lease_id = :lease_id, #lease_expires = :lease_expires",
                ConditionExpression = acquire
                    ? "#expires > :now AND attribute_exists(#revision) AND (attribute_not_exists(#lease_id) OR #lease_expires <= :now_ms OR #lease_id = :lease_id)"
                    : "#expires > :now AND attribute_exists(#revision) AND #lease_id = :lease_id AND #lease_expires > :now_ms",
                ExpressionAttributeNames = new Dictionary<string, string>
                {
                    ["#expires"] = _options.TimeToLiveAttributeName,
                    ["#revision"] = RevisionAttribute,
                    ["#lease_id"] = LeaseIdAttribute,
                    ["#lease_expires"] = LeaseExpiresAtAttribute
                },
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                {
                    [":now"] = new() { N = UnixSeconds(now) },
                    [":now_ms"] = new() { N = UnixMilliseconds(now) },
                    [":lease_id"] = new() { S = leaseId },
                    [":lease_expires"] = new() { N = UnixMilliseconds(DurableFlowStoreShared.AddSaturating(now, leaseDuration)) }
                }
            }, cancellationToken).ConfigureAwait(false);
            return true;
        }
        catch (ConditionalCheckFailedException)
        {
            return false;
        }
    }

    private Dictionary<string, AttributeValue> CreateItem(string flowId, string stateJson, long revision, TimeSpan ttl, DateTimeOffset now)
        => new(StringComparer.Ordinal)
        {
            [FlowIdAttribute] = new() { S = flowId },
            [StateJsonAttribute] = new() { S = stateJson },
            [_options.TimeToLiveAttributeName] = new() { N = UnixSecondsCeiling(DurableFlowStoreShared.AddSaturating(now, ttl)) },
            [UpdatedAtAttribute] = new() { N = UnixSeconds(now) },
            [RevisionAttribute] = new() { N = revision.ToString(CultureInfo.InvariantCulture) }
        };

    private static Dictionary<string, AttributeValue> Key(string flowId)
        => new(StringComparer.Ordinal) { [FlowIdAttribute] = new AttributeValue { S = flowId } };

    private static string UnixSeconds(DateTimeOffset value)
        => value.ToUnixTimeSeconds().ToString(CultureInfo.InvariantCulture);

    private static string UnixSecondsCeiling(DateTimeOffset value)
        => ((long)Math.Ceiling(value.ToUnixTimeMilliseconds() / 1000.0)).ToString(CultureInfo.InvariantCulture);

    private static string UnixMilliseconds(DateTimeOffset value)
        => value.ToUnixTimeMilliseconds().ToString(CultureInfo.InvariantCulture);

    /// <summary>Disposes the DynamoDB client when the store created (and therefore owns) it.</summary>
    public void Dispose()
    {
        _ensureGate.Dispose();
        if (_ownsClient)
            _client.Dispose();
    }
}
}
