using AsyncResponse;
using AsyncResponse.DurableFlows.Internal;
using AsyncResponse.DurableFlows.MongoDB;
using AsyncResponse.Internal;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Bson.Serialization.Serializers;
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
    private readonly IMongoCollection<MongoFlowStateDocument> _currentCollection;
    private readonly MongoDbDurableFlowOptions _options;
    private readonly SemaphoreSlim _ensureGate = new(1, 1);
    private readonly IMongoClient? _ownedClient;
    private volatile bool _created;
    private volatile bool _linearizableUnsupported;

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

        // Primary reads, whatever read preference the host-supplied database carries: a
        // secondaryPreferred connection string would route every ledger load to a lagging
        // secondary, where a stale revision replays an already-checkpointed step and a
        // not-yet-replicated ledger reads as null — the one answer callers ACK a wake-up on
        // (LoadAsync's contract, and the reason the DynamoDB sibling pins ConsistentRead).
        // It also keeps reads on the same authority whose $$NOW the filters evaluate against
        // (see ReadServerNowAsync).
        //
        // Majority writes, whatever write concern the host-supplied database carries: under an
        // inherited w=1 (a connection-string default, or any pre-5.0 deployment) the primary
        // acknowledges a checkpoint, lease, or create before a single secondary has it, and a
        // failover rolls it back — the lease a worker is executing under, or the step result it
        // just recorded, silently disappears and the step's side effect runs again. The read
        // concern stays inherited on purpose: primary reads already see every write this store
        // had acknowledged (read-your-writes needs nothing more), and a majority snapshot could
        // only hide a competitor's newer write, which the revision/lease filters reject anyway —
        // except on the no-write paths, which read linearizably (see LoadCurrentAsync).
        // Majority with a BOUND (see MongoWriteConcerns): a bare WMajority carried no wtimeout, so
        // on a primary-secondary-arbiter set with its secondary down every checkpoint blocked
        // indefinitely — and it discarded the operator's own wtimeoutMS/journal besides.
        _collection = database.GetCollection<MongoFlowStateDocument>(_options.CollectionName)
            .WithReadPreference(ReadPreference.Primary)
            .WithWriteConcern(MongoWriteConcerns.BoundedMajority(database));
        _currentCollection = _collection.WithReadConcern(ReadConcern.Linearizable);
        _ownedClient = ownedClient;
    }

    public Task<FlowState?> LoadAsync(string flowId, CancellationToken cancellationToken = default)
        => LoadCoreAsync(flowId, current: false, cancellationToken);

    /// <inheritdoc />
    /// <remarks>
    /// A primary read is current except while a partitioned primary has not yet noticed it was
    /// deposed (up to about <c>electionTimeoutMillis</c>): it still serves reads, and a process
    /// that can reach only it misses a majority-acknowledged write the new primary took — a
    /// breadcrumb a recovered response matches, a status an operator set back to Running. That
    /// answer is dropped with no fence behind it, so this reads with <c>linearizable</c> read
    /// concern, which a deposed primary cannot satisfy ("majority" would not help: its majority
    /// snapshot is just as stale). A linearizable read waits for a majority to confirm the
    /// primary, so it carries the same bound as the store's writes
    /// (<see cref="MongoWriteConcerns.DefaultMajorityTimeout"/>, as <c>maxTimeMS</c>) and fails
    /// rather than blocking while the set is degraded. A standalone server, which has no second
    /// primary to be stale against, rejects the read concern (<c>NotAReplicaSet</c>); the store
    /// then uses the plain read from that point on.
    /// </remarks>
    public Task<FlowState?> LoadCurrentAsync(string flowId, CancellationToken cancellationToken = default)
        => LoadCoreAsync(flowId, current: true, cancellationToken);

    private async Task<FlowState?> LoadCoreAsync(string flowId, bool current, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(flowId);
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);

        // Expiry is evaluated against the server clock ($$NOW) — the same authority the TTL
        // monitor reaps with — so app clock skew can never resurrect an expired ledger or hide a
        // live one. All lease fencing below uses the same authority.
        var document = current
            ? await FindLinearizableAsync(flowId, cancellationToken).ConfigureAwait(false)
            : await _collection.Find(BuildLiveFilter(flowId)).FirstOrDefaultAsync(cancellationToken).ConfigureAwait(false);
        if (document is null)
        {
            // A document whose expiry is missing or not a BSON date also misses the live filter —
            // by BSON type order a string, a number or a missing field compares below any date —
            // and the TTL monitor never reaps it, so "absent" would acknowledge the only wake-up
            // of a ledger that sits in the collection forever. Only a well-formed, elapsed expiry
            // reads as absent (DynamoDB refuses the same shape). One id-only probe, on this path only.
            // Neither the reaper nor a create (BuildExpiredReplaceFilter) ever removes such a
            // document, so the id stays blocked until an operator does: the reason carries the cleanup.
            if (await _collection.CountDocumentsAsync(BuildMalformedExpiryFilter(flowId), new CountOptions { Limit = 1 }, cancellationToken).ConfigureAwait(false) > 0)
                throw new FlowStateUnreadableException(flowId, MalformedExpiryReason(_options.CollectionName));

            return null;
        }

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

    /// <summary>The live-filter read of <see cref="LoadCurrentAsync"/> (see its remarks).</summary>
    private async Task<MongoFlowStateDocument?> FindLinearizableAsync(string flowId, CancellationToken cancellationToken)
    {
        if (!_linearizableUnsupported)
        {
            try
            {
                return await _currentCollection
                    .Find(BuildLiveFilter(flowId), new FindOptions { MaxTime = MongoWriteConcerns.DefaultMajorityTimeout })
                    .FirstOrDefaultAsync(cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (MongoCommandException ex) when (ex.Code == NotAReplicaSet)
            {
                _linearizableUnsupported = true;
            }
        }

        return await _collection.Find(BuildLiveFilter(flowId)).FirstOrDefaultAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>The server's rejection of a replica-set-only read concern on a standalone node.</summary>
    private const int NotAReplicaSet = 123;

    /// <inheritdoc />
    public void ValidateCreate(string flowId, FlowState state, TimeSpan ttl)
    {
        DurableFlowStoreShared.ValidateCreate(flowId, state, ttl);
        if (_options.MaxStateBytes is not null)
            _ = DurableFlowStoreShared.SerializeBounded(flowId, state, _options.MaxStateBytes, "MongoDB");
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
            // The document that holds the id may have expired during the `hello` round trip
            // between step 1 and this insert. Answering "exists" for it sends the executor to load
            // a ledger that reads as absent, and the start job is acknowledged with no run
            // created — so replace it once more before conceding the id to a live owner.
            var retried = await _collection.UpdateOneAsync(
                BuildExpiredReplaceFilter(flowId),
                BuildStateUpdate(stateJson, state.Revision, ttl, resetLease: true),
                options: null,
                cancellationToken).ConfigureAwait(false);
            return retried.ModifiedCount > 0;
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

    /// <inheritdoc />
    public async Task<FlowLeaseObservation?> ObserveLeaseAsync(string flowId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(flowId);
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);

        // The two lease fields exactly as stored — deliberately an id-only filter with no $$NOW
        // comparison, unlike every other read and write in this store: an expired lease nobody has
        // taken over must keep reading as the same lease, because the engine's proof of a live
        // holder is that two observations DIFFER. Whether it has lapsed stays BuildLeaseFilter's
        // call, on the server clock. Read from the primary like every ledger read (the collection
        // handle is pinned at construction), so a lagging secondary can never replay a stale
        // lease as "unchanged"; the projection keeps state_json off the wire.
        var document = await _collection
            .Find(Builders<MongoFlowStateDocument>.Filter.Eq(item => item.FlowId, flowId))
            .Project<MongoFlowStateDocument>(BuildLeaseProjection())
            .FirstOrDefaultAsync(cancellationToken)
            .ConfigureAwait(false);

        // BSON dates are UTC milliseconds and the driver materializes them as DateTimeKind.Utc.
        return DurableFlowStoreShared.LeaseObservation(document?.LeaseId, document?.LeaseExpiresAtUtc);
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
        // No AutoCreateIndexes/UseOwnershipLedger fast-path here: with both disabled there is no
        // DDL and no ledger claim, but the TTL-reaper VERIFICATION below must still run once — an
        // early-out on the flag pair skipped it for exactly the locked-down deployment
        // (operator-provisioned indexes, no ledger writes) it exists to protect, and the
        // collection grew without bound with no error and no log line.
        if (_created)
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

            // One index listing serves both startup checks: the collation refusal, and (with
            // AutoCreateIndexes off) the TTL verification. With AutoCreateIndexes on, a deployment
            // whose credentials may create but not list indexes keeps working — unverified, as
            // before the refusal existed.
            var indexes = await ListIndexesAsync(tolerateUnauthorized: _options.AutoCreateIndexes, cancellationToken).ConfigureAwait(false);
            if (indexes is not null)
                ThrowIfFoldingCollation(indexes);

            if (!_options.AutoCreateIndexes)
            {
                // The TTL index is this store's ONLY cleanup mechanism (no application-side
                // pruning exists), so an operator-provisioned collection must be verified to
                // carry one — Cosmos and DynamoDB hard-fail the same way when their server-side
                // reaper is missing. Without this, a collection provisioned without
                // expireAfterSeconds grew without bound, with no error and no log line.
                VerifyTtlIndex(indexes!);
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
            // mismatch and startup fails, leaving the operator to correct schema intentionally —
            // unless what conflicts is the reaper this store needs under another name: the index
            // the docs and the AutoCreateIndexes = false error prescribe,
            // createIndex({ expires_at_utc: 1 }, { expireAfterSeconds: 0 }), is named
            // "expires_at_utc_1", and MongoDB refuses the same key and options under a second name
            // (85 IndexOptionsConflict; 86 IndexKeySpecsConflict for a same-named different key).
            try
            {
                await _collection.Indexes.CreateOneAsync(model, cancellationToken: cancellationToken).ConfigureAwait(false);
            }
            catch (MongoCommandException ex) when (ex.Code is 85 or 86)
            {
                indexes ??= await ListIndexesAsync(tolerateUnauthorized: false, cancellationToken).ConfigureAwait(false);
                if (!HasTtlIndex(indexes!))
                    throw;
            }

            _created = true;
        }
        finally
        {
            _ensureGate.Release();
        }
    }

    /// <summary>
    /// The collection's indexes; empty when the collection does not exist yet (the first write
    /// creates it, with the simple collation: MongoDB has no database-level default). <c>null</c>
    /// when the credentials may not list indexes and <paramref name="tolerateUnauthorized"/> is set.
    /// </summary>
    private async Task<List<BsonDocument>?> ListIndexesAsync(bool tolerateUnauthorized, CancellationToken cancellationToken)
    {
        try
        {
            using var cursor = await _collection.Indexes.ListAsync(cancellationToken).ConfigureAwait(false);
            return await cursor.ToListAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (MongoCommandException ex) when (ex.Code == NamespaceNotFound)
        {
            return [];
        }
        catch (MongoCommandException ex) when (tolerateUnauthorized && ex.Code == Unauthorized)
        {
            return null;
        }
    }

    private const int NamespaceNotFound = 26;
    private const int Unauthorized = 13;

    /// <summary>
    /// Refuses a collection whose default collation folds: the flow id is the <c>_id</c>, and a
    /// collection created with a default collation builds its <c>_id_</c> index — which cannot be
    /// rebuilt — and evaluates every id equality under it. Two case- or accent-variant flow ids
    /// then collide: the second run's create reports the first run's ledger as existing, its load
    /// reads that ledger (refused as unreadable, so its start job dead-letters while the caller
    /// was told it started), and a delete removes the other run. Pinning the simple collation on
    /// the queries cannot help — the unique <c>_id_</c> index still folds — so startup refuses,
    /// as the relational stores refuse a folding <c>flow_id</c> column.
    /// </summary>
    private void ThrowIfFoldingCollation(List<BsonDocument> indexes)
    {
        var idIndex = indexes.Find(index => index.TryGetValue("name", out var name) && name == "_id_");
        if (idIndex is null
            || !idIndex.TryGetValue("collation", out var collation)
            || collation is not BsonDocument collationDocument
            || (collationDocument.TryGetValue("locale", out var locale) && locale == "simple"))
        {
            return;
        }

        throw new InvalidOperationException(
            $"The MongoDB durable-flow collection '{_options.CollectionName}' was created with the default collation " +
            $"{collationDocument.ToJson()}, which its _id index — the flow id — carries too. The store compares flow ids " +
            "ORDINALLY, and a collation folds whatever its rules call equal — case, accents, or width, depending on the " +
            "locale and strength — into one key. Distinct flow ids would then collide: the second run's create finds the " +
            "first run's ledger, its start job dead-letters, and a delete removes the other run. The _id index cannot be " +
            "rebuilt: recreate the collection without a collation (or with { locale: 'simple' }) and copy the documents " +
            "over, or configure a new CollectionName.");
    }

    /// <summary>
    /// Verifies an operator-provisioned collection carries the TTL reaper this store depends on:
    /// a single-field index on the expiry timestamp with <c>expireAfterSeconds</c> set (any
    /// value — a delayed reap is bounded; a missing one is unbounded growth).
    /// </summary>
    private void VerifyTtlIndex(List<BsonDocument> indexes)
    {
        if (HasTtlIndex(indexes))
            return;

        throw new InvalidOperationException(
            $"The MongoDB durable-flow collection '{_options.CollectionName}' has no TTL index on 'expires_at_utc' and " +
            $"{nameof(MongoDbDurableFlowOptions)}.{nameof(MongoDbDurableFlowOptions.AutoCreateIndexes)} is disabled. The TTL index is " +
            "the store's only cleanup mechanism; without it expired flow ledgers accumulate forever. Create it " +
            "(createIndex({ expires_at_utc: 1 }, { expireAfterSeconds: 0 })) or enable AutoCreateIndexes.");
    }

    /// <summary>
    /// Whether the collection carries a TTL reaper on the expiry timestamp, whatever it is named:
    /// a single-field index on <c>expires_at_utc</c> with <c>expireAfterSeconds</c> set.
    /// </summary>
    private static bool HasTtlIndex(List<BsonDocument> indexes)
        => indexes.Exists(index =>
            index.Contains("expireAfterSeconds")
            && index.TryGetValue("key", out var key)
            && key is BsonDocument keyDocument
            && keyDocument.ElementCount == 1
            && keyDocument.Contains("expires_at_utc"));

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

    /// <summary>
    /// Expired-ledger filter used by create to replace a dead ledger in place. Only a BSON date
    /// can be elapsed: a missing or non-date expiry also compares <c>$lte</c> <c>$$NOW</c> by BSON
    /// type order, and replacing that corrupt, still-present ledger would silently discard it
    /// (DynamoDB's condition refuses the same create).
    /// </summary>
    internal static FilterDefinition<MongoFlowStateDocument> BuildExpiredReplaceFilter(string flowId)
        => Builders<MongoFlowStateDocument>.Filter.Eq(item => item.FlowId, flowId)
           & Builders<MongoFlowStateDocument>.Filter.Type(item => item.ExpiresAtUtc, BsonType.DateTime)
           & ServerClockExpr(new BsonDocument("$lte", new BsonArray { "$expires_at_utc", "$$NOW" }));

    /// <summary>
    /// Operator-facing reason for a malformed expiry, with the cleanup that frees the id: the TTL
    /// monitor never reaps a non-date and no create replaces one, so nothing else ever will.
    /// </summary>
    internal static string MalformedExpiryReason(string collectionName)
        => "its stored document's expires_at_utc is missing or not a date, so the TTL index never reaps it and no create " +
           "replaces it. Earlier AsyncResponse releases wrote such expiries when the host had registered a non-date " +
           "DateTime serializer globally; once no run needs them, remove them with " +
           $"db.getCollection(\"{collectionName}\").deleteMany({{ expires_at_utc: {{ $not: {{ $type: \"date\" }} }} }}) " +
           "(add an _id condition to clear one flow)";

    /// <summary>The id with an expiry that is missing or not a BSON date — a corrupt ledger, not an absent one.</summary>
    internal static FilterDefinition<MongoFlowStateDocument> BuildMalformedExpiryFilter(string flowId)
        => Builders<MongoFlowStateDocument>.Filter.Eq(item => item.FlowId, flowId)
           & Builders<MongoFlowStateDocument>.Filter.Not(
               Builders<MongoFlowStateDocument>.Filter.Type(item => item.ExpiresAtUtc, BsonType.DateTime));

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

    /// <summary>
    /// Projection for <see cref="ObserveLeaseAsync"/>: only <c>lease_id</c> and
    /// <c>lease_expires_at_utc</c> (plus the implicit <c>_id</c>) leave the server, so observing a
    /// lease costs the same whatever the ledger's size.
    /// </summary>
    internal static ProjectionDefinition<MongoFlowStateDocument> BuildLeaseProjection()
        => Builders<MongoFlowStateDocument>.Projection
            .Include(item => item.LeaseId)
            .Include(item => item.LeaseExpiresAtUtc);

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
/// <para>
/// The instants are pinned to BSON dates (<see cref="UtcBsonDateSerializer"/>), whatever
/// <see cref="DateTime"/> serializer the host registered globally: every expiry and lease filter
/// compares them with <c>$$NOW</c>, and the TTL monitor reaps only dates. Under a host-wide
/// String or Document representation the insert path wrote a string, which by BSON type order
/// never compares above <c>$$NOW</c> — every new ledger read as absent (its start or child job was
/// acknowledged "nothing to execute") and was never reaped.
/// </para>
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
    [BsonSerializer(typeof(UtcBsonDateSerializer))]
    public DateTime ExpiresAtUtc { get; set; }

    [BsonElement("updated_at_utc")]
    [BsonSerializer(typeof(UtcBsonDateSerializer))]
    public DateTime UpdatedAtUtc { get; set; }

    [BsonElement("revision")]
    public long? Revision { get; set; }

    [BsonElement("lease_id")]
    [BsonIgnoreIfNull]
    public string? LeaseId { get; set; }

    [BsonElement("lease_expires_at_utc")]
    [BsonIgnoreIfNull]
    [BsonSerializer(typeof(NullableUtcBsonDateSerializer))]
    public DateTime? LeaseExpiresAtUtc { get; set; }
}

/// <summary>
/// A ledger instant as a UTC BSON date, set on the member itself so the global serializer registry
/// is never consulted. <c>[BsonDateTimeOptions]</c> would not do: it RECONFIGURES whatever
/// serializer the registry returns for <see cref="DateTime"/>, and for a host that registered its
/// own <c>IBsonSerializer&lt;DateTime&gt;</c> (anything but the driver's
/// <see cref="DateTimeSerializer"/>) freezing this class map throws
/// <see cref="NotSupportedException"/>, failing every flow-store operation. The driver's
/// serializers are sealed, so this delegates to a privately held one instead of deriving from it.
/// </summary>
internal sealed class UtcBsonDateSerializer : SerializerBase<DateTime>
{
    internal static readonly DateTimeSerializer Pinned = new(DateTimeKind.Utc, BsonType.DateTime);

    public override DateTime Deserialize(BsonDeserializationContext context, BsonDeserializationArgs args)
        => Pinned.Deserialize(context, args);

    public override void Serialize(BsonSerializationContext context, BsonSerializationArgs args, DateTime value)
        => Pinned.Serialize(context, args, value);
}

/// <summary>The nullable twin of <see cref="UtcBsonDateSerializer"/>, for the lease expiry.</summary>
internal sealed class NullableUtcBsonDateSerializer : SerializerBase<DateTime?>
{
    private static readonly NullableSerializer<DateTime> Pinned = new(UtcBsonDateSerializer.Pinned);

    public override DateTime? Deserialize(BsonDeserializationContext context, BsonDeserializationArgs args)
        => Pinned.Deserialize(context, args);

    public override void Serialize(BsonSerializationContext context, BsonSerializationArgs args, DateTime? value)
        => Pinned.Serialize(context, args, value);
}
}
