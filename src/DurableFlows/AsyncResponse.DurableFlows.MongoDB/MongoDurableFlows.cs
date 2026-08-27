using AsyncResponse;
using AsyncResponse.DurableFlows.Internal;
using AsyncResponse.DurableFlows.MongoDB;
using AsyncResponse.Internal;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;

namespace Microsoft.Extensions.DependencyInjection
{
    /// <summary>DI registration for the MongoDB durable-flow state store.</summary>
    public static class MongoDurableFlowServiceCollectionExtensions
    {
        /// <summary>
        /// Stores durable-flow state in MongoDB. Hosts may either register an
        /// <see cref="IMongoDatabase"/> singleton or set connection options here.
        /// </summary>
        public static AsyncResponseRegistrationBuilder WithMongoDbDurableFlows(
            this AsyncResponseRegistrationBuilder builder,
            Action<MongoDbDurableFlowOptions>? configure = null)
        {
            // Singleton on purpose: index provisioning is cached per store instance, and the
            // executor resolves the store from a fresh scope per flow execution. Host-registered
            // IMongoDatabase / IMongoClient services are reused when present; otherwise the store
            // creates and owns a client from the options. Nothing is registered as a bare
            // IMongoClient/IMongoDatabase service, so unrelated resolutions of those types are
            // never answered — or broken — by this package.
            builder.Services.TryAddSingleton<IMongoNamespaceRegistry, MongoNamespaceRegistry>();
            builder.Services.TryAddSingleton(provider =>
            {
                var options = provider.GetRequiredService<IOptions<MongoDbDurableFlowOptions>>();

                var database = provider.GetService<IMongoDatabase>();
                if (database is not null)
                    return new MongoDbFlowStateStore(database, options, ownedClient: null, provider.GetRequiredService<IMongoNamespaceRegistry>());

                if (string.IsNullOrWhiteSpace(options.Value.DatabaseName))
                    throw new InvalidOperationException($"{nameof(MongoDbDurableFlowOptions)}.{nameof(MongoDbDurableFlowOptions.DatabaseName)} must be configured when no IMongoDatabase is registered.");

                var sharedClient = provider.GetService<IMongoClient>();
                if (sharedClient is not null)
                    return new MongoDbFlowStateStore(sharedClient.GetDatabase(options.Value.DatabaseName), options, ownedClient: null, provider.GetRequiredService<IMongoNamespaceRegistry>());

                if (string.IsNullOrWhiteSpace(options.Value.ConnectionString))
                    throw new InvalidOperationException($"{nameof(MongoDbDurableFlowOptions)}.{nameof(MongoDbDurableFlowOptions.ConnectionString)} must be configured when no IMongoDatabase or IMongoClient is registered.");

                var ownedClient = new MongoClient(options.Value.ConnectionString);
                return new MongoDbFlowStateStore(ownedClient.GetDatabase(options.Value.DatabaseName), options, ownedClient, provider.GetRequiredService<IMongoNamespaceRegistry>());
            });
            return builder.WithDurableFlows<MongoDbFlowStateStore, MongoDbDurableFlowOptions>(configure);
        }
    }
}

namespace AsyncResponse.DurableFlows.MongoDB
{
/// <summary>Options for the MongoDB durable-flow state store.</summary>
public sealed class MongoDbDurableFlowOptions : DurableFlowOptions
{
    /// <summary>Optional MongoDB connection string used when no <see cref="IMongoDatabase"/> is registered.</summary>
    public string? ConnectionString { get; set; }

    /// <summary>Optional database name used when no <see cref="IMongoDatabase"/> is registered.</summary>
    public string? DatabaseName { get; set; }

    /// <summary>Collection storing one durable-flow ledger document per flow id.</summary>
    public string CollectionName { get; set; } = "asyncresponse_flow_state";

    /// <summary>Creates the expiry index on first use.</summary>
    public bool AutoCreateIndexes { get; set; } = true;

    /// <summary>
    /// Claims the ledger collection in the persisted cross-component ownership ledger
    /// (<c>asyncresponse_ownership</c>) at first use, so another AsyncResponse component — in
    /// this or any other process — misconfigured onto the same collection fails startup instead
    /// of silently corrupting data (a flow store on the channel's derived counters collection
    /// would let this store's TTL index delete the ack counter). Independent of
    /// <see cref="AutoCreateIndexes"/>. Disable only for least-privilege deployments that cannot
    /// write the ledger collection. Default: <c>true</c>.
    /// </summary>
    public bool UseOwnershipLedger { get; set; } = true;

    /// <summary>
    /// Maximum serialized flow-state size in bytes accepted by writes; oversized ledgers fail fast
    /// with an actionable error instead of the raw 16 MB BSON-document error the executor would
    /// retry into the dead-letter queue. Default: 15 MB (headroom under MongoDB's 16 MB document
    /// cap for the sibling fields); <c>null</c> disables the guard.
    /// </summary>
    public long? MaxStateBytes { get; set; } = 15_000_000;

    /// <summary>Validates option values and throws on misconfiguration.</summary>
    public void Validate()
    {
        if (string.IsNullOrWhiteSpace(CollectionName))
            throw new InvalidOperationException($"{nameof(MongoDbDurableFlowOptions)}.{nameof(CollectionName)} must be configured.");
        if (CollectionName.Contains('$') || CollectionName.Contains('\0')
            || CollectionName.StartsWith("system.", StringComparison.Ordinal) || CollectionName.Contains(".system.", StringComparison.Ordinal))
            throw new InvalidOperationException(
                $"{nameof(MongoDbDurableFlowOptions)}.{nameof(CollectionName)} '{CollectionName}' must be a valid MongoDB collection name (no '$' or NUL characters, not in or containing the reserved system namespace).");
        if (string.Equals(CollectionName, MongoOwnershipLedger.CollectionName, StringComparison.Ordinal))
            throw new InvalidOperationException(
                $"{nameof(MongoDbDurableFlowOptions)}.{nameof(CollectionName)} '{CollectionName}' is reserved for the cross-component ownership ledger.");
        DurableFlowStoreShared.ValidateMaxStateBytes(MaxStateBytes, nameof(MongoDbDurableFlowOptions));
    }
}

/// <summary>MongoDB implementation of <see cref="IFlowStateStore"/>.</summary>
public sealed class MongoDbFlowStateStore : IFlowStateStore, IDisposable
{
    private readonly IMongoDatabase _database;
    private readonly IMongoCollection<MongoFlowStateDocument> _collection;
    private readonly MongoDbDurableFlowOptions _options;
    private readonly SemaphoreSlim _ensureGate = new(1, 1);
    private readonly IMongoClient? _ownedClient;
    private volatile bool _created;

    /// <summary>
    /// DI construction path: also claims the collection in the container's cross-component
    /// ownership ledger — a flow store configured onto the channel's derived counters collection
    /// would let the flow TTL index silently delete the ack-sequence counter.
    /// </summary>
    internal MongoDbFlowStateStore(
        IMongoDatabase database,
        IOptions<MongoDbDurableFlowOptions> options,
        IMongoClient? ownedClient,
        IMongoNamespaceRegistry? namespaceRegistry)
        : this(database, options, ownedClient)
    {
        namespaceRegistry?.Claim(
            MongoNamespaceRegistry.ClusterKey(database),
            database.DatabaseNamespace.DatabaseName,
            "MongoDB durable-flow store",
            [(_options.CollectionName, nameof(_options.CollectionName))]);
    }

    public MongoDbFlowStateStore(
        IMongoDatabase database,
        IOptions<MongoDbDurableFlowOptions> options,
        IMongoClient? ownedClient = null)
    {
        _options = options.Value;
        _options.Validate();
        _database = database;

        // The namespace BYTE limit can only be checked here, where the actual database name is
        // first known; a near-limit configuration otherwise passes every static check and fails
        // at the first server operation.
        MongoNamespaceRegistry.ValidateEffectiveNamespace(database, _options.CollectionName, nameof(_options.CollectionName));

        _collection = database.GetCollection<MongoFlowStateDocument>(_options.CollectionName);
        _ownedClient = ownedClient;
    }

    public async Task<FlowState?> LoadAsync(string flowId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(flowId);
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);

        // Expiry is evaluated against the server clock ($$NOW) — the same authority the TTL
        // monitor reaps with — so app clock skew can never resurrect an expired ledger or hide a
        // live one. All lease fencing below uses the same authority.
        var document = await _collection.Find(BuildLiveFilter(flowId)).FirstOrDefaultAsync(cancellationToken).ConfigureAwait(false);
        if (document is null)
            return null;

        // BuildLiveFilter already excluded expired documents server-side, so reaching here with a
        // document means the ledger is present and live. A missing required field is therefore an
        // unreadable ledger, not an absent one — returning null for it acknowledged the only
        // wake-up of a run still sitting in the collection.
        if (document.Revision is not { } revision)
            throw new FlowStateUnreadableException(flowId, "its stored document has no revision");

        if (string.IsNullOrEmpty(document.StateJson))
            throw new FlowStateUnreadableException(flowId, "its stored document has no state JSON");

        return DurableFlowStoreShared.ReadState(flowId, document.StateJson, revision);
    }

    public async Task<bool> TryCreateAsync(string flowId, FlowState state, TimeSpan ttl, CancellationToken cancellationToken = default)
    {
        DurableFlowStoreShared.ValidateCreate(flowId, state, ttl);
        var stateJson = DurableFlowStoreShared.SerializeBounded(flowId, state, _options.MaxStateBytes, "MongoDB");
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);

        // Two server-side steps instead of one upsert because MongoDB rejects upserts whose query
        // uses $expr, and $expr is what lets the expired-check run on the server clock.
        //
        // Step 1: atomically replace an expired ledger in place. Filter and assignments both
        // evaluate on $$NOW, so exactly one competing creator wins and every loser then sees the
        // fresh future expiry.
        var replaced = await _collection.UpdateOneAsync(
            BuildExpiredReplaceFilter(flowId),
            BuildStateUpdate(stateJson, state.Revision, ttl, resetLease: true),
            options: null,
            cancellationToken).ConfigureAwait(false);
        if (replaced.ModifiedCount > 0)
            return true;

        // Step 2: the id was absent (or the expired document was TTL-purged after step 1 looked):
        // insert a fresh ledger. A plain insert has no aggregation context, so $$NOW is
        // unavailable — instead the server clock is read with one cheap `hello` round-trip and
        // stamped client-side. Creation then uses the same authority as every $$NOW comparison
        // and refresh below, so app clock skew can never mint a ledger that is born expired or
        // outlives its TTL window. A duplicate key means a live ledger owns the id.
        var serverNow = await ReadServerNowAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await _collection.InsertOneAsync(
                new MongoFlowStateDocument
                {
                    FlowId = flowId,
                    StateJson = stateJson,
                    ExpiresAtUtc = DurableFlowStoreShared.AddSaturating(serverNow, ttl),
                    UpdatedAtUtc = serverNow,
                    Revision = state.Revision
                },
                options: null,
                cancellationToken).ConfigureAwait(false);
            return true;
        }
        catch (MongoWriteException ex) when (ex.WriteError?.Category == ServerErrorCategory.DuplicateKey)
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
        var stateJson = DurableFlowStoreShared.SerializeBounded(flowId, state, _options.MaxStateBytes, "MongoDB");
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);

        var result = await _collection.UpdateOneAsync(
            BuildCheckpointFilter(flowId, expectedRevision, leaseId),
            BuildStateUpdate(stateJson, state.Revision, ttl, resetLease: false),
            options: null,
            cancellationToken).ConfigureAwait(false);
        // ModifiedCount is safe here (unlike lease renewal): a checkpoint always bumps the
        // revision, so a matched document is always modified.
        return result.ModifiedCount > 0;
    }

    public Task<bool> TryAcquireLeaseAsync(string flowId, string leaseId, TimeSpan leaseDuration, CancellationToken cancellationToken = default)
        => UpdateLeaseAsync(flowId, leaseId, leaseDuration, acquire: true, cancellationToken);

    public Task<bool> TryRenewLeaseAsync(string flowId, string leaseId, TimeSpan leaseDuration, CancellationToken cancellationToken = default)
        => UpdateLeaseAsync(flowId, leaseId, leaseDuration, acquire: false, cancellationToken);

    public async Task ReleaseLeaseAsync(string flowId, string leaseId, CancellationToken cancellationToken = default)
    {
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        var filter = Builders<MongoFlowStateDocument>.Filter.Eq(item => item.FlowId, flowId)
                     & Builders<MongoFlowStateDocument>.Filter.Eq(item => item.LeaseId, leaseId);
        var update = Builders<MongoFlowStateDocument>.Update
            .Unset(item => item.LeaseId)
            .Unset(item => item.LeaseExpiresAtUtc);
        await _collection.UpdateOneAsync(filter, update, cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    public async Task<bool> TryDeleteAsync(string flowId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(flowId);
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);

        var result = await _collection.DeleteOneAsync(
            Builders<MongoFlowStateDocument>.Filter.Eq(item => item.FlowId, flowId),
            cancellationToken).ConfigureAwait(false);
        return result.DeletedCount > 0;
    }

    private async Task EnsureCreatedAsync(CancellationToken cancellationToken)
    {
        if (_created || (!_options.AutoCreateIndexes && !_options.UseOwnershipLedger))
            return;

        await _ensureGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_created)
                return;

            // Persisted cross-host ownership, independent of AutoCreateIndexes: a flow store
            // configured onto the channel's derived counters collection would let this TTL
            // index silently delete the ack-sequence counter — see MongoOwnershipLedger.
            if (_options.UseOwnershipLedger)
            {
                await MongoOwnershipLedger.ClaimAsync(
                    _database,
                    "MongoDB durable-flow store",
                    [(_options.CollectionName, nameof(_options.CollectionName))],
                    cancellationToken).ConfigureAwait(false);
            }

            if (!_options.AutoCreateIndexes)
            {
                _created = true;
                return;
            }

            // A TTL index (expireAfterSeconds = 0 on the expiry timestamp) makes MongoDB itself
            // reap expired ledgers — no application-side pruning needed. Loads still filter on
            // ExpiresAtUtc because the TTL monitor only runs periodically (~60s).
            var indexName = $"{_options.CollectionName}_expires_idx";
            var model = new CreateIndexModel<MongoFlowStateDocument>(
                Builders<MongoFlowStateDocument>.IndexKeys.Ascending(item => item.ExpiresAtUtc),
                new CreateIndexOptions { Name = indexName, ExpireAfter = TimeSpan.Zero });
            // Do not drop or rewrite a conflicting application-owned index. MongoDB reports the
            // mismatch and startup fails, leaving the operator to correct schema intentionally.
            await _collection.Indexes.CreateOneAsync(model, cancellationToken: cancellationToken).ConfigureAwait(false);
            _created = true;
        }
        finally
        {
            _ensureGate.Release();
        }
    }

    /// <summary>
    /// Server clock for the one write that cannot compute it in place: plain inserts evaluate no
    /// pipeline, so <c>$$NOW</c> is out of reach. <c>hello</c> is answered by every supported
    /// server (the 4.2+ floor the <c>$$NOW</c> filters already require) and carries the node's
    /// <c>localTime</c>; reading it from the primary keeps the authority the same node whose
    /// <c>$$NOW</c> the filters evaluate against.
    /// </summary>
    private async Task<DateTime> ReadServerNowAsync(CancellationToken cancellationToken)
    {
        var reply = await _database.RunCommandAsync<BsonDocument>(
            new BsonDocument("hello", 1),
            ReadPreference.Primary,
            cancellationToken).ConfigureAwait(false);
        // Defensive: a mongo-compatible endpoint omitting localTime — or answering with a
        // non-date value (only BsonDateTime implements ToUniversalTime; every other BsonValue
        // throws) — falls back to the app clock, the pre-server-clock behavior, instead of
        // failing every create.
        return reply.TryGetValue("localTime", out var localTime) && localTime is BsonDateTime serverTime
            ? serverTime.ToUniversalTime()
            : DateTime.UtcNow;
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
        var result = await _collection.UpdateOneAsync(
            BuildLeaseFilter(flowId, leaseId, acquire),
            BuildLeaseUpdate(leaseId, leaseDuration),
            options: null,
            cancellationToken).ConfigureAwait(false);
        // MatchedCount, not ModifiedCount: matching the filter proves this owner held (or could
        // take) the lease — the atomic update then applied. A renewal that lands in the same
        // millisecond as the previous one writes an identical lease_expires_at_utc, which MongoDB
        // reports as matched-but-not-modified; treating that no-op as failure would abort a
        // healthy execution mid-flight.
        return result.MatchedCount > 0;
    }

    /// <summary>Live-ledger filter: id match plus a server-clock ($$NOW) expiry check.</summary>
    internal static FilterDefinition<MongoFlowStateDocument> BuildLiveFilter(string flowId)
        => Builders<MongoFlowStateDocument>.Filter.Eq(item => item.FlowId, flowId)
           & ServerClockExpr(new BsonDocument("$gt", new BsonArray { "$expires_at_utc", "$$NOW" }));

    /// <summary>Expired-ledger filter used by create to replace a dead ledger in place.</summary>
    internal static FilterDefinition<MongoFlowStateDocument> BuildExpiredReplaceFilter(string flowId)
        => Builders<MongoFlowStateDocument>.Filter.Eq(item => item.FlowId, flowId)
           & ServerClockExpr(new BsonDocument("$lte", new BsonArray { "$expires_at_utc", "$$NOW" }));

    /// <summary>
    /// Checkpoint filter: revision fence plus server-clock expiry (and, when fenced by a lease,
    /// server-clock lease validity).
    /// </summary>
    internal static FilterDefinition<MongoFlowStateDocument> BuildCheckpointFilter(string flowId, long expectedRevision, string? leaseId)
    {
        var filter = Builders<MongoFlowStateDocument>.Filter.Eq(item => item.FlowId, flowId)
                     & Builders<MongoFlowStateDocument>.Filter.Eq(item => item.Revision, expectedRevision)
                     & ServerClockExpr(new BsonDocument("$gt", new BsonArray { "$expires_at_utc", "$$NOW" }));
        if (leaseId is not null)
        {
            filter &= Builders<MongoFlowStateDocument>.Filter.Eq(item => item.LeaseId, leaseId)
                      & ServerClockExpr(new BsonDocument("$gt", new BsonArray { "$lease_expires_at_utc", "$$NOW" }));
        }

        return filter;
    }

    /// <summary>
    /// Lease filter: acquire takes a free lease (absent, expired on the server clock, or already
    /// ours); renew requires ours and still live on the server clock. A missing
    /// <c>lease_expires_at_utc</c> compares below any date, so it counts as expired for acquire
    /// and as unrenewable for renew.
    /// </summary>
    internal static FilterDefinition<MongoFlowStateDocument> BuildLeaseFilter(string flowId, string leaseId, bool acquire)
    {
        var filter = Builders<MongoFlowStateDocument>.Filter.Eq(item => item.FlowId, flowId)
                     & Builders<MongoFlowStateDocument>.Filter.Ne(item => item.Revision, null)
                     & ServerClockExpr(new BsonDocument("$gt", new BsonArray { "$expires_at_utc", "$$NOW" }));
        filter &= acquire
            ? Builders<MongoFlowStateDocument>.Filter.Or(
                Builders<MongoFlowStateDocument>.Filter.Eq(item => item.LeaseId, null),
                ServerClockExpr(new BsonDocument("$lte", new BsonArray { "$lease_expires_at_utc", "$$NOW" })),
                Builders<MongoFlowStateDocument>.Filter.Eq(item => item.LeaseId, leaseId))
            : Builders<MongoFlowStateDocument>.Filter.Eq(item => item.LeaseId, leaseId)
              & ServerClockExpr(new BsonDocument("$gt", new BsonArray { "$lease_expires_at_utc", "$$NOW" }));
        return filter;
    }

    /// <summary>
    /// Full-state write as an aggregation-pipeline update so the expiry lands on the server clock
    /// ($$NOW + ttl). <paramref name="resetLease"/> clears the lease columns (create-over-expired
    /// replaces ownership); checkpoints leave the running lease in place.
    /// </summary>
    internal static UpdateDefinition<MongoFlowStateDocument> BuildStateUpdate(string stateJson, long revision, TimeSpan ttl, bool resetLease)
    {
        var stages = new List<BsonDocument>
        {
            new("$set", new BsonDocument
            {
                // $literal keeps the JSON payload a value: a pipeline $set treats "$"-prefixed
                // strings as field paths.
                ["state_json"] = new BsonDocument("$literal", stateJson),
                ["expires_at_utc"] = new BsonDocument("$add", new BsonArray
                {
                    "$$NOW",
                    DurableFlowStoreShared.ServerClockTtlMilliseconds(ttl)
                }),
                ["updated_at_utc"] = "$$NOW",
                ["revision"] = revision
            })
        };
        if (resetLease)
            stages.Add(new BsonDocument("$unset", new BsonArray { "lease_id", "lease_expires_at_utc" }));
        return Builders<MongoFlowStateDocument>.Update.Pipeline(stages.ToArray());
    }

    /// <summary>Lease grant/renewal on the server clock: <c>lease_expires_at_utc = $$NOW + duration</c>.</summary>
    internal static UpdateDefinition<MongoFlowStateDocument> BuildLeaseUpdate(string leaseId, TimeSpan leaseDuration)
        => Builders<MongoFlowStateDocument>.Update.Pipeline(new[]
        {
            new BsonDocument("$set", new BsonDocument
            {
                ["lease_id"] = new BsonDocument("$literal", leaseId),
                ["lease_expires_at_utc"] = new BsonDocument("$add", new BsonArray
                {
                    "$$NOW",
                    DurableFlowStoreShared.ServerClockTtlMilliseconds(leaseDuration)
                })
            })
        });

    private static FilterDefinition<MongoFlowStateDocument> ServerClockExpr(BsonDocument comparison)
        => new BsonDocumentFilterDefinition<MongoFlowStateDocument>(new BsonDocument("$expr", comparison));

    /// <summary>Disposes the Mongo client when the store created (and therefore owns) it.</summary>
    public void Dispose()
    {
        _ensureGate.Dispose();
        (_ownedClient as IDisposable)?.Dispose();
    }
}

/// <remarks>
/// [BsonIgnoreExtraElements] for the same reason the transport's queue document carries it: the
/// driver's default is to THROW FormatException for any element outside this class map, so one
/// column a newer build added would make every older replica in a rolling deploy fail to read a
/// live flow's ledger — and an unreadable ledger is the one outcome the store contract refuses to
/// report as "absent".
/// </remarks>
[BsonIgnoreExtraElements]
internal sealed class MongoFlowStateDocument
{
    [BsonId]
    [BsonElement("_id")]
    public string FlowId { get; set; } = "";

    [BsonElement("state_json")]
    public string StateJson { get; set; } = "";

    [BsonElement("expires_at_utc")]
    public DateTime ExpiresAtUtc { get; set; }

    [BsonElement("updated_at_utc")]
    public DateTime UpdatedAtUtc { get; set; }

    [BsonElement("revision")]
    public long? Revision { get; set; }

    [BsonElement("lease_id")]
    [BsonIgnoreIfNull]
    public string? LeaseId { get; set; }

    [BsonElement("lease_expires_at_utc")]
    [BsonIgnoreIfNull]
    public DateTime? LeaseExpiresAtUtc { get; set; }
}
}
