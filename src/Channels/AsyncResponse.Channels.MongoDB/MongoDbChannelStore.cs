using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;
using System.Runtime.CompilerServices;
using AsyncResponse.Internal;

namespace AsyncResponse.Channels.MongoDB;

/// <summary>One stored response envelope row/document as the channel store returns it.</summary>
/// <remarks>
/// <c>EnvelopeJson</c> is the stored envelope, or <c>null</c> for a document the dispatch sweep loaded header-only (an
/// already-acknowledged one — see <see cref="MongoDbChannelStore.LoadMessagesAsync"/>); the
/// sweep hydrates the few such documents it still has to deliver through
/// <see cref="MongoDbChannelStore.LoadMessagesByIdAsync"/> before handing them to a waiter.
/// </remarks>
internal readonly record struct MongoDbChannelMessage(
    Guid Id,
    string CorrelationId,
    string? EnvelopeJson,
    DateTimeOffset CreatedAtUtc,
    DateTimeOffset? AckedAtUtc = null,
    long? AckedSeq = null);

/// <summary>Document adapter for the MongoDB channel collections and change-stream wake.</summary>
internal sealed class MongoDbChannelStore : IDisposable
{
    private readonly IMongoCollection<MongoRecoveryStateDocument> _recovery;
    private readonly IMongoCollection<MongoChannelMessageDocument> _messages;
    private readonly IMongoCollection<MongoChannelSubscriberDocument> _subscribers;
    private readonly IMongoCollection<BsonDocument> _counters;
    private readonly IMongoCollection<MongoChannelMessageDocument> _messagesReadBack;
    private readonly IMongoCollection<BsonDocument> _countersReadBack;
    private readonly IMongoDatabase _database;
    private readonly MongoDbAsyncResponseChannelOptions _options;
    private readonly SemaphoreSlim _ensureGate = new(1, 1);
    private readonly IMongoClient? _ownedClient;
    private readonly ILogger _logger;
    private bool _created;

    public MongoDbChannelStore(
        IMongoDatabase database,
        IOptions<MongoDbAsyncResponseChannelOptions> options,
        IMongoClient? ownedClient = null,
        IMongoNamespaceRegistry? namespaceRegistry = null,
        ILogger? logger = null)
    {
        _options = options.Value;
        _options.Validate();
        _database = database;
        _logger = logger ?? NullLogger.Instance;

        // Cross-component collection ownership (DI-hosted stores only): another AsyncResponse
        // component configured onto one of these collections — the derived counters collection
        // included — must fail startup in either construction order.
        namespaceRegistry?.Claim(
            MongoNamespaceRegistry.ClusterKey(database),
            database.DatabaseNamespace.DatabaseName,
            "MongoDB channel",
            [
                (_options.RecoveryStateCollection, nameof(_options.RecoveryStateCollection)),
                (_options.MessageCollection, nameof(_options.MessageCollection)),
                (_options.SubscriberCollection, nameof(_options.SubscriberCollection)),
                (CountersCollectionName(_options.MessageCollection), "derived ack-counter collection"),
            ]);

        // Namespace BYTE limits can only be checked here, where the actual database name is
        // first known — and the derived counters namespace is 9 bytes longer than the configured
        // message collection, so a near-limit configuration passed every static check and failed
        // at the first ack-sequence draw.
        MongoNamespaceRegistry.ValidateEffectiveNamespace(database, _options.RecoveryStateCollection, nameof(_options.RecoveryStateCollection));
        MongoNamespaceRegistry.ValidateEffectiveNamespace(database, _options.MessageCollection, nameof(_options.MessageCollection));
        MongoNamespaceRegistry.ValidateEffectiveNamespace(database, _options.SubscriberCollection, nameof(_options.SubscriberCollection));
        MongoNamespaceRegistry.ValidateEffectiveNamespace(database, CountersCollectionName(_options.MessageCollection), "the derived ack-counter collection");

        // Primary reads override whatever the host-registered client/connection string configured:
        // a secondaryPreferred connection routed the liveness probe, recovery-state read and message
        // load to a lagging secondary, so a publisher racing a fresh registration saw 0 subscribers
        // AND 0 recovery states and dropped the response while reporting success. It also keeps
        // reads on the same authority whose $$NOW the expiry filters evaluate against (same
        // reasoning as the MongoDB durable-flow store's pin). Writes are bounded-majority on every
        // handle for the same parity (see MongoWriteConcerns): under an inherited w=1 a failover
        // rolled back an acknowledged response, recovery registration, or ack-sequence draw.
        var writeConcern = MongoWriteConcerns.BoundedMajority(database);
        _recovery = database.GetCollection<MongoRecoveryStateDocument>(_options.RecoveryStateCollection)
            .WithReadPreference(ReadPreference.Primary)
            .WithWriteConcern(writeConcern);
        _messages = database.GetCollection<MongoChannelMessageDocument>(_options.MessageCollection)
            .WithReadPreference(ReadPreference.Primary)
            .WithWriteConcern(writeConcern);
        _subscribers = database.GetCollection<MongoChannelSubscriberDocument>(_options.SubscriberCollection)
            .WithReadPreference(ReadPreference.Primary)
            .WithWriteConcern(writeConcern);
        // The monotonic ack sequence: delivery claims and subscription registrations draw from
        // this ONE counter, giving acked_seq and a subscription's start position a total order no
        // pair of same-tick timestamps has. Created on first upsert; no index needed (_id only).
        _counters = database.GetCollection<BsonDocument>(CountersCollectionName(_options.MessageCollection))
            .WithReadPreference(ReadPreference.Primary)
            .WithWriteConcern(writeConcern);
        // The replication-timeout read-backs (see ReadOnPrimaryAsync) read at LOCAL concern: a
        // write whose majority acknowledgement lapsed is by definition not majority-committed, so
        // under an inherited readConcernLevel=majority the read-back missed the write it exists to
        // find — a recovery claim that won read back as lost, and the response was dropped.
        _messagesReadBack = _messages.WithReadConcern(ReadConcern.Local);
        _countersReadBack = _counters.WithReadConcern(ReadConcern.Local);
        _ownedClient = ownedClient;
    }

    public async Task EnsureCreatedAsync(CancellationToken cancellationToken = default)
    {
        if (_created)
            return;

        await _ensureGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_created)
                return;

            // Persisted cross-host ownership: the in-container registry cannot see other hosts
            // or directly constructed stores, so claim the effective collections here — before
            // any index DDL, and INDEPENDENTLY of AutoCreateIndexes (disabling index DDL must
            // not disable collision protection) — and fail startup when another component
            // already owns one.
            if (_options.UseOwnershipLedger)
            {
                await MongoOwnershipLedger.ClaimAsync(
                    _database,
                    "MongoDB channel",
                    [
                        (_options.RecoveryStateCollection, nameof(_options.RecoveryStateCollection)),
                        (_options.MessageCollection, nameof(_options.MessageCollection)),
                        (_options.SubscriberCollection, nameof(_options.SubscriberCollection)),
                        (CountersCollectionName(_options.MessageCollection), "derived ack-counter collection"),
                    ],
                    cancellationToken).ConfigureAwait(false);
            }

            // Both branches: nothing else notices a folding default collation (see the method).
            await WarnIfFoldingCollationAsync(cancellationToken).ConfigureAwait(false);

            if (!_options.AutoCreateIndexes)
            {
                // Manually managed indexes get a one-time read-only check instead of DDL. There
                // is no collection shape to verify (documents are schemaless), so the silent
                // failure modes are all indexes — above all a missing TTL index, which means
                // nothing ever reaps expired documents. Absence is a warning, never a startup
                // failure: indexes degrade retention and performance, not correctness, and a
                // least-privilege operator may provision them out of band.
                await WarnIfManagedIndexesMissingAsync(cancellationToken).ConfigureAwait(false);
                _created = true;
                return;
            }

            // TTL indexes (expireAfterSeconds = 0 on the expiry timestamp) make MongoDB itself reap
            // expired documents — no application-side pruning needed. Reads still filter on the
            // expiry because the TTL monitor only runs periodically (~60s). A TTL index an earlier
            // deployment created under the same name with different options is replaced in place;
            // an equivalent index under another name — the one the AutoCreateIndexes = false
            // warning prescribes — is accepted (see MongoIndexes).
            await CreateTtlIndexAsync(
                _recovery,
                Builders<MongoRecoveryStateDocument>.IndexKeys.Ascending(item => item.ExpiresAtUtc),
                $"{_options.RecoveryStateCollection}_expires_idx",
                cancellationToken).ConfigureAwait(false);
            await MongoIndexes.CreateOrAcceptEquivalentAsync(
                _recovery,
                new CreateIndexModel<MongoRecoveryStateDocument>(
                    Builders<MongoRecoveryStateDocument>.IndexKeys
                        .Ascending(item => item.CorrelationId)
                        .Ascending(item => item.RegisteredAtUtc),
                    new CreateIndexOptions { Name = $"{_options.RecoveryStateCollection}_correlation_idx" }),
                replaceConflicting: false,
                cancellationToken).ConfigureAwait(false);

            await CreateTtlIndexAsync(
                _messages,
                Builders<MongoChannelMessageDocument>.IndexKeys.Ascending(item => item.ExpiresAtUtc),
                $"{_options.MessageCollection}_expires_idx",
                cancellationToken).ConfigureAwait(false);
            await MongoIndexes.CreateOrAcceptEquivalentAsync(
                _messages,
                new CreateIndexModel<MongoChannelMessageDocument>(
                    Builders<MongoChannelMessageDocument>.IndexKeys
                        .Ascending(item => item.CorrelationId)
                        .Ascending(item => item.CreatedAtUtc),
                    new CreateIndexOptions { Name = $"{_options.MessageCollection}_correlation_created_idx" }),
                replaceConflicting: false,
                cancellationToken).ConfigureAwait(false);

            await CreateTtlIndexAsync(
                _subscribers,
                Builders<MongoChannelSubscriberDocument>.IndexKeys.Ascending(item => item.ExpiresAtUtc),
                $"{_options.SubscriberCollection}_expires_idx",
                cancellationToken).ConfigureAwait(false);
            await MongoIndexes.CreateOrAcceptEquivalentAsync(
                _subscribers,
                new CreateIndexModel<MongoChannelSubscriberDocument>(
                    Builders<MongoChannelSubscriberDocument>.IndexKeys.Ascending(item => item.CorrelationId),
                    new CreateIndexOptions { Name = $"{_options.SubscriberCollection}_correlation_idx" }),
                replaceConflicting: false,
                cancellationToken).ConfigureAwait(false);

            _created = true;
        }
        finally
        {
            _ensureGate.Release();
        }
    }

    private static Task CreateTtlIndexAsync<TDocument>(
        IMongoCollection<TDocument> collection,
        IndexKeysDefinition<TDocument> keys,
        string indexName,
        CancellationToken cancellationToken)
        => MongoIndexes.CreateOrAcceptEquivalentAsync(
            collection,
            new CreateIndexModel<TDocument>(keys, new CreateIndexOptions { Name = indexName, ExpireAfter = TimeSpan.Zero }),
            replaceConflicting: true,
            cancellationToken);

    /// <summary>
    /// Warns once when a channel collection was created with a non-simple default collation. The
    /// per-id reads use it (they stay unpinned on purpose: an explicit simple collation cannot use
    /// an index built under a folding one, so every targeted pass would scan the collection), and
    /// under a case- or accent-folding one a read for "ABC" also returns "abc"'s documents: the
    /// shared ordinal re-check drops them, but logs an Error per document per pass, which the
    /// lookback window repeats. Nothing is misdelivered, so this is a warning, not a startup
    /// failure. Fail-soft: without the listCollections privilege the check is skipped.
    /// </summary>
    private async Task WarnIfFoldingCollationAsync(CancellationToken cancellationToken)
    {
        try
        {
            var names = new BsonArray { _options.RecoveryStateCollection, _options.MessageCollection, _options.SubscriberCollection };
            using var cursor = await _database.ListCollectionsAsync(
                new ListCollectionsOptions { Filter = new BsonDocument("name", new BsonDocument("$in", names)) },
                cancellationToken).ConfigureAwait(false);
            foreach (var collection in await cursor.ToListAsync(cancellationToken).ConfigureAwait(false))
            {
                if (FoldingCollation(collection) is not { } collation)
                    continue;

                _logger.LogWarning(
                    "MongoDB collection {Database}.{Collection} was created with the default collation {Collation}. The channel " +
                    "compares correlation ids ordinally, and under a case- or accent-folding collation a lookup for one id also " +
                    "returns the documents of ids that differ only by case or accents: they are discarded, with an Error logged " +
                    "for each on every pass. Recreate the collection without a collation (or with {{ locale: 'simple' }}).",
                    _database.DatabaseNamespace.DatabaseName,
                    collection.GetValue("name", BsonNull.Value).ToString(),
                    collation.ToJson());
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // Same containment as the managed-index check: a deployment that cannot list
            // collections must not lose the actual operation to a diagnostic (nor to a logging
            // provider that throws).
            SafeLog.Try(() => _logger.LogDebug(ex, "Skipping the collation check for the MongoDB channel collections; listCollections was not available."));
        }
    }

    /// <summary>A listCollections entry's default collation when it is not the simple (binary) one.</summary>
    internal static BsonDocument? FoldingCollation(BsonDocument collection)
        => collection.TryGetValue("options", out var options)
           && options is BsonDocument optionsDocument
           && optionsDocument.TryGetValue("collation", out var collation)
           && collation is BsonDocument collationDocument
           && !(collationDocument.TryGetValue("locale", out var locale) && locale == "simple")
            ? collationDocument
            : null;

    private async Task WarnIfManagedIndexesMissingAsync(CancellationToken cancellationToken)
    {
        try
        {
            await WarnIfCollectionIndexesMissingAsync(_recovery, _options.RecoveryStateCollection, cancellationToken).ConfigureAwait(false);
            await WarnIfCollectionIndexesMissingAsync(_messages, _options.MessageCollection, cancellationToken).ConfigureAwait(false);
            await WarnIfCollectionIndexesMissingAsync(_subscribers, _options.SubscriberCollection, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // A deployment that cannot even list indexes (no listIndexes privilege, server
            // unreachable at first use) must not lose the actual operation to the check: the
            // caller's own store call surfaces any real connectivity failure.
            _logger.LogDebug(ex, "Skipping index verification for the manually managed MongoDB channel collections; listIndexes was not available.");
        }
    }

    private async Task WarnIfCollectionIndexesMissingAsync<TDocument>(
        IMongoCollection<TDocument> collection,
        string collectionName,
        CancellationToken cancellationToken)
    {
        List<BsonDocument> indexes;
        try
        {
            using var cursor = await collection.Indexes.ListAsync(cancellationToken).ConfigureAwait(false);
            indexes = await cursor.ToListAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (MongoCommandException ex) when (ex.Code == 26)
        {
            // NamespaceNotFound: the collection does not exist yet. MongoDB creates it bare on
            // the first write — which, with index DDL disabled, is exactly a collection with no
            // TTL index.
            indexes = [];
        }

        // Matched by KEY, not by name: operators own the naming of manually provisioned indexes.
        if (!indexes.Any(index => IndexLeadsOn(index, "expires_at") && index.Contains("expireAfterSeconds")))
        {
            _logger.LogWarning(
                "MongoDB collection {Database}.{Collection} has no TTL index on 'expires_at' and AutoCreateIndexes is disabled. " +
                "Expired documents are never reaped, so the collection grows without bound. " +
                "Create a TTL index (expireAfterSeconds: 0 on 'expires_at') or enable AutoCreateIndexes.",
                _database.DatabaseNamespace.DatabaseName, collectionName);
        }

        if (!indexes.Any(index => IndexLeadsOn(index, "correlation_id")))
        {
            _logger.LogWarning(
                "MongoDB collection {Database}.{Collection} has no index leading on 'correlation_id' and AutoCreateIndexes is disabled. " +
                "Correlation-id lookups scan the whole collection — performance only; create the index to restore indexed dispatch.",
                _database.DatabaseNamespace.DatabaseName, collectionName);
        }
    }

    /// <summary>
    /// Whether the listed index's FIRST key field is <paramref name="field"/> — what a prefix
    /// lookup uses, and (TTL indexes being single-field) what identifies the TTL index.
    /// </summary>
    private static bool IndexLeadsOn(BsonDocument index, string field)
        => index.TryGetValue("key", out var key)
           && key is BsonDocument keyDocument
           && keyDocument.ElementCount > 0
           && keyDocument.GetElement(0).Name == field;

    public async Task SaveRecoveryStateAsync(string correlationId, RecoveryState state, TimeSpan ttl, CancellationToken cancellationToken)
    {
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        // Upsert pipeline stamped with the server clock ($$NOW), matching the message-side
        // discipline (and the PG/SqlServer DB-clock discipline): app-clock expiry math would shift
        // the recovery window by whatever the client clock is skewed.
        await _recovery.UpdateOneAsync(
            Builders<MongoRecoveryStateDocument>.Filter.Eq(item => item.Id, RegistrationKey(correlationId, state.RegistrationId)),
            BuildRecoveryStateUpsertPipeline(correlationId, state, ttl),
            new UpdateOptions { IsUpsert = true },
            cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Upsert pipeline for a recovery registration: every field is overwritten (a re-save refreshes
    /// the registration), with <c>expires_at</c>/<c>registered_at</c> computed on the server clock.
    /// </summary>
    internal static UpdateDefinition<MongoRecoveryStateDocument> BuildRecoveryStateUpsertPipeline(
        string correlationId,
        RecoveryState state,
        TimeSpan ttl)
        => Builders<MongoRecoveryStateDocument>.Update.Pipeline(new[]
        {
            new BsonDocument("$set", new BsonDocument
            {
                ["correlation_id"] = Literal(correlationId),
                ["registration_id"] = new BsonBinaryData(state.RegistrationId, GuidRepresentation.Standard),
                ["state_json"] = Literal(AsyncResponseJson.Serialize(state)),
                ["expires_at"] = new BsonDocument("$add", new BsonArray { "$$NOW", ttl.TotalMilliseconds }),
                ["registered_at"] = "$$NOW"
            })
        });

    public async Task<IReadOnlyList<string>> LoadRecoveryStatesAsync(string correlationId, CancellationToken cancellationToken)
    {
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        var filter = Builders<MongoRecoveryStateDocument>.Filter.Eq(item => item.CorrelationId, correlationId)
                     & NotExpiredOnServerClock<MongoRecoveryStateDocument>();
        var documents = await _recovery.Find(filter)
            .SortBy(item => item.RegisteredAtUtc)
            .Project(item => item.StateJson)
            .ToListAsync(cancellationToken).ConfigureAwait(false);
        return documents;
    }

    public async Task<bool> DeleteRecoveryStateAsync(string correlationId, Guid registrationId, CancellationToken cancellationToken)
    {
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        var single = await _recovery.DeleteOneAsync(
            Builders<MongoRecoveryStateDocument>.Filter.Eq(item => item.Id, RegistrationKey(correlationId, registrationId)),
            cancellationToken).ConfigureAwait(false);
        return single.DeletedCount > 0;
    }

    public async IAsyncEnumerable<string> ScanRecoveryStateJsonAsync([EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        var filter = NotExpiredOnServerClock<MongoRecoveryStateDocument>();
        using var cursor = await _recovery.Find(filter)
            .SortBy(item => item.RegisteredAtUtc)
            .Project(item => item.StateJson)
            .ToCursorAsync(cancellationToken).ConfigureAwait(false);
        while (await cursor.MoveNextAsync(cancellationToken).ConfigureAwait(false))
        {
            foreach (var json in cursor.Current)
                yield return json;
        }
    }

    /// <summary>
    /// Inserts a response envelope document. The caller supplies the message id and the write is an
    /// upsert that preserves an existing document, so a retried publish is idempotent rather than
    /// duplicating the response. Timestamps are stamped with the server clock (<c>$$NOW</c>) so
    /// dispatch watermarks never mix client and server clocks. The insert itself is the wake signal:
    /// every process's change stream observes it.
    /// </summary>
    public Task<MongoDbChannelMessage> InsertMessageAsync(Guid id, string correlationId, string envelopeJson, TimeSpan retention, CancellationToken cancellationToken)
        => AsyncResponseRetry.ExecuteAsync(
            token => InsertMessageOnceAsync(id, correlationId, envelopeJson, retention, token),
            IsTransient,
            _options.PublishMaxAttempts,
            _options.PublishRetryBaseDelay,
            _options.PublishRetryMaxDelay,
            cancellationToken);

    private async Task<MongoDbChannelMessage> InsertMessageOnceAsync(Guid id, string correlationId, string envelopeJson, TimeSpan retention, CancellationToken cancellationToken)
    {
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);

        // findOneAndUpdate instead of updateOne so the returned document carries the
        // server-stamped ($$NOW) created_at — the original document's on a publish retry, per the
        // pipeline's $ifNull, together with its settlement columns — for the same-process fast
        // path's watermark comparison (a fabricated null acked_at replayed an already-consumed
        // response to a waiter registered after the ack).
        MongoChannelMessageDocument? document;
        try
        {
            document = await _messages.FindOneAndUpdateAsync(
                Builders<MongoChannelMessageDocument>.Filter.Eq(item => item.Id, id),
                BuildInsertMessagePipeline(correlationId, envelopeJson, retention),
                new FindOneAndUpdateOptions<MongoChannelMessageDocument>
                {
                    IsUpsert = true,
                    ReturnDocument = ReturnDocument.After,
                    // The caller already holds the envelope: only the stamps travel back.
                    Projection = WithoutEnvelopeProjection
                },
                cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (MongoWriteConcerns.IsReplicationTimeout(ex))
        {
            // The upsert applied on the primary — waiters there can already claim it — and only
            // its majority acknowledgement timed out. Failing the publish made the ingress
            // re-publish under a NEW message id (an Until waiter received the response once per
            // attempt) and then store a failure envelope on top; retrying the same id re-waits on
            // the same replication. So the stored document is read back by id and reported
            // stored, like any other insert.
            document = await ReadOnPrimaryAsync(
                Builders<MongoChannelMessageDocument>.Filter.Eq(item => item.Id, id),
                WithoutEnvelopeProjection,
                cancellationToken).ConfigureAwait(false);
            if (document is null)
                throw;
            LogReplicationTimeout(ex, "response insert", id);
        }

        // Upsert + ReturnDocument.After cannot return null from a healthy server. If a driver
        // anomaly ever surfaces one, persistence is UNKNOWN — reporting success with a fabricated
        // app-clock timestamp would both lie about it and feed a client clock into the
        // server-clock watermark. Fail instead, so the retry/error path runs.
        return document is null
            ? throw new InvalidOperationException(
                $"MongoDB response upsert for message {id} returned no document despite IsUpsert + ReturnDocument.After; persistence is unknown.")
            : new MongoDbChannelMessage(
                id,
                correlationId,
                envelopeJson,
                new DateTimeOffset(document.CreatedAtUtc, TimeSpan.Zero),
                document.AckedAtUtc is { } acked ? new DateTimeOffset(acked, TimeSpan.Zero) : null,
                document.AckedSeq);
    }

    /// <summary>
    /// One message document as the primary holds it now (the handle is pinned to the primary and
    /// reads at local concern, so it sees a write that is not yet majority-committed): how a
    /// write whose majority acknowledgement timed out (see
    /// <see cref="MongoWriteConcerns.IsReplicationTimeout"/>) learns what it applied — a read
    /// waits on no replication, so it answers while the set is still degraded.
    /// </summary>
    private async Task<MongoChannelMessageDocument?> ReadOnPrimaryAsync(
        FilterDefinition<MongoChannelMessageDocument> filter,
        ProjectionDefinition<MongoChannelMessageDocument, MongoChannelMessageDocument> projection,
        CancellationToken cancellationToken)
        => await _messagesReadBack.Find(filter).Project(projection).FirstOrDefaultAsync(cancellationToken).ConfigureAwait(false);

    private void LogReplicationTimeout(Exception exception, string operation, Guid messageId)
        => SafeLog.Try(() => _logger.LogWarning(
            exception,
            "MongoDB channel {Operation} for message {MessageId} was applied on the primary, but its majority acknowledgement timed out; " +
            "acting on what the primary holds. A failover before it replicates can still roll it back — restore the replica set's " +
            "secondaries (or remove the arbiter) rather than lowering the write concern.",
            operation,
            messageId));

    /// <summary>Every field but the envelope — what the insert needs back.</summary>
    internal static readonly ProjectionDefinition<MongoChannelMessageDocument, MongoChannelMessageDocument> WithoutEnvelopeProjection =
        new BsonDocumentProjectionDefinition<MongoChannelMessageDocument, MongoChannelMessageDocument>(new BsonDocument("envelope_json", 0));

    /// <summary>The id alone — all a claim's caller inspects is whether a document came back.</summary>
    internal static readonly ProjectionDefinition<MongoChannelMessageDocument, MongoChannelMessageDocument> IdOnlyProjection =
        new BsonDocumentProjectionDefinition<MongoChannelMessageDocument, MongoChannelMessageDocument>(new BsonDocument("_id", 1));

    /// <summary>
    /// Upsert pipeline for a response envelope: <c>$ifNull</c> keeps the original server-stamped
    /// timestamps and claim flags when a publish retry finds the document already present.
    /// </summary>
    internal static UpdateDefinition<MongoChannelMessageDocument> BuildInsertMessagePipeline(
        string correlationId,
        string envelopeJson,
        TimeSpan retention)
        => Builders<MongoChannelMessageDocument>.Update.Pipeline(new[]
        {
            new BsonDocument("$set", new BsonDocument
            {
                ["correlation_id"] = Literal(correlationId),
                ["envelope_json"] = Literal(envelopeJson),
                ["created_at"] = new BsonDocument("$ifNull", new BsonArray { "$created_at", "$$NOW" }),
                ["expires_at"] = new BsonDocument("$ifNull", new BsonArray
                {
                    "$expires_at",
                    new BsonDocument("$add", new BsonArray { "$$NOW", retention.TotalMilliseconds })
                }),
                ["acked_at"] = new BsonDocument("$ifNull", new BsonArray { "$acked_at", BsonNull.Value }),
                ["recovery_claimed"] = new BsonDocument("$ifNull", new BsonArray { "$recovery_claimed", false })
            })
        });

    public async Task<IReadOnlyList<MongoDbChannelMessage>> LoadMessagesAsync(
        string correlationId,
        DateTimeOffset sinceUtc,
        int batchSize,
        DateTimeOffset? afterCreatedAtUtc,
        Guid? afterId,
        CancellationToken cancellationToken)
    {
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        var filter = Builders<MongoChannelMessageDocument>.Filter.Eq(item => item.CorrelationId, correlationId)
                     & Builders<MongoChannelMessageDocument>.Filter.Gte(item => item.CreatedAtUtc, sinceUtc.UtcDateTime)
                     & NotExpiredOnServerClock<MongoChannelMessageDocument>();
        if (afterCreatedAtUtc is not null)
        {
            var afterCreated = afterCreatedAtUtc.Value.UtcDateTime;
            var cursorId = afterId ?? throw new ArgumentNullException(nameof(afterId));
            filter &= Builders<MongoChannelMessageDocument>.Filter.Or(
                Builders<MongoChannelMessageDocument>.Filter.Gt(item => item.CreatedAtUtc, afterCreated),
                Builders<MongoChannelMessageDocument>.Filter.And(
                    Builders<MongoChannelMessageDocument>.Filter.Eq(item => item.CreatedAtUtc, afterCreated),
                    Builders<MongoChannelMessageDocument>.Filter.Gt(item => item.Id, cursorId)));
        }
        var documents = await _messages.Find(filter)
            .Project(SweepProjection)
            .Sort(Builders<MongoChannelMessageDocument>.Sort
                .Ascending(item => item.CreatedAtUtc)
                .Ascending(item => item.Id))
            .Limit(batchSize)
            .ToListAsync(cancellationToken).ConfigureAwait(false);
        return ToMessages(documents);
    }

    /// <summary>
    /// The sweep's projection: every field but the envelope, and the envelope only for a document
    /// nobody has acknowledged yet. Acknowledged documents are the consumed history the sweep
    /// re-reads on every tick (they stay in the result so a fan-out waiter in ANOTHER process
    /// still receives them): shipping their bodies with each sweep made a long-lived progress
    /// subscription's cost grow with its whole retained history. The shared sweep fetches the
    /// envelope by id for the rare acknowledged document a live subscription has not seen.
    /// <c>$ifNull</c> folds a missing <c>acked_at</c> (a pre-settlement document) into null.
    /// An aggregation expression inside a <c>find</c> projection needs MongoDB 4.4+, which makes
    /// 4.4 the channel's documented server floor (4.2 rejects the projection on every sweep).
    /// </summary>
    internal static readonly ProjectionDefinition<MongoChannelMessageDocument, MongoChannelMessageDocument> SweepProjection =
        new BsonDocumentProjectionDefinition<MongoChannelMessageDocument, MongoChannelMessageDocument>(new BsonDocument
        {
            ["_id"] = 1,
            ["correlation_id"] = 1,
            ["created_at"] = 1,
            ["expires_at"] = 1,
            ["acked_at"] = 1,
            ["acked_seq"] = 1,
            ["recovery_claimed"] = 1,
            ["envelope_json"] = new BsonDocument("$cond", new BsonArray
            {
                new BsonDocument("$eq", new BsonArray
                {
                    new BsonDocument("$ifNull", new BsonArray { "$acked_at", BsonNull.Value }),
                    BsonNull.Value
                }),
                "$envelope_json",
                BsonNull.Value
            })
        });

    /// <summary>
    /// The full documents (envelope included) for <paramref name="ids"/> under
    /// <paramref name="correlationId"/>, in sweep order — how the dispatch sweep hydrates the
    /// header-only acknowledged documents it still has to deliver. A document reaped between the
    /// sweep's page and this read is simply absent.
    /// </summary>
    public async Task<IReadOnlyList<MongoDbChannelMessage>> LoadMessagesByIdAsync(
        string correlationId,
        IReadOnlyList<Guid> ids,
        CancellationToken cancellationToken)
    {
        if (ids.Count == 0)
            return [];

        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        var filter = Builders<MongoChannelMessageDocument>.Filter.Eq(item => item.CorrelationId, correlationId)
                     & Builders<MongoChannelMessageDocument>.Filter.In(item => item.Id, ids)
                     & NotExpiredOnServerClock<MongoChannelMessageDocument>();
        var documents = await _messages.Find(filter)
            .Sort(Builders<MongoChannelMessageDocument>.Sort
                .Ascending(item => item.CreatedAtUtc)
                .Ascending(item => item.Id))
            .ToListAsync(cancellationToken).ConfigureAwait(false);
        return ToMessages(documents);
    }

    private static List<MongoDbChannelMessage> ToMessages(List<MongoChannelMessageDocument> documents)
    {
        var messages = new List<MongoDbChannelMessage>(documents.Count);
        foreach (var document in documents)
            messages.Add(new MongoDbChannelMessage(
                document.Id,
                document.CorrelationId,
                document.EnvelopeJson,
                new DateTimeOffset(document.CreatedAtUtc, TimeSpan.Zero),
                document.AckedAtUtc is { } acked ? new DateTimeOffset(acked, TimeSpan.Zero) : null,
                document.AckedSeq));
        return messages;
    }

    /// <summary>
    /// Atomically claims a message for live delivery via <c>findOneAndUpdate</c>: sets
    /// <c>acked_at</c> unless the publisher has already routed it to the lost-subscriber path
    /// (<c>recovery_claimed</c>). Returns <c>false</c> when recovery owns the message, so a
    /// slow-but-live waiter does not deliver a response the recovery callback already handled.
    /// Multiple processes may each win this claim, preserving cross-process fan-out, because it
    /// gates only on <c>recovery_claimed</c>, not on <c>acked_at</c>.
    /// </summary>
    public async Task<bool> TryClaimForDeliveryAsync(Guid messageId, CancellationToken cancellationToken)
    {
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        // The sequence value is drawn BEFORE the claim lands, so its position reflects when this
        // delivery happened relative to subscription registrations (which draw from the same
        // counter). An unused draw on an already-acked row leaves a harmless gap; $ifNull keeps
        // the FIRST claim's stamp, mirroring acked_at.
        var ackSeq = await DrawAckSequenceAsync(cancellationToken).ConfigureAwait(false);
        MongoChannelMessageDocument? claimed;
        try
        {
            claimed = await _messages.FindOneAndUpdateAsync(
                BuildDeliveryClaimFilter(messageId),
                BuildDeliveryClaimUpdate(ackSeq),
                new FindOneAndUpdateOptions<MongoChannelMessageDocument> { ReturnDocument = ReturnDocument.After, Projection = IdOnlyProjection },
                cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (MongoWriteConcerns.IsReplicationTimeout(ex))
        {
            // The claim ran on the primary and only its majority acknowledgement timed out, so
            // acked_at may already be stamped — and the publisher's acknowledgement poll and its
            // recovery claim both read that stamp as "delivered". Throwing here left the response
            // acknowledged but never dispatched. What the claim left behind is its outcome: a
            // document carrying acked_at and still not recovery-claimed (recovery_claimed only
            // ever moves one way, and never once acked_at is set) is one a delivery claim won.
            // The claim's own filter is NOT re-evaluated: its expiry check, run a wtimeout after
            // the claim, reported a response that expired in between as unclaimed although the
            // claim had stamped it — acknowledged and lost. The cost of reading the stamp instead
            // is at worst a re-dispatch of a fan-out response that expired before this claim.
            claimed = await ReadOnPrimaryAsync(BuildDeliveryClaimReadBackFilter(messageId), IdOnlyProjection, cancellationToken).ConfigureAwait(false);
            LogReplicationTimeout(ex, "delivery claim", messageId);
        }

        return claimed is not null;
    }

    internal static FilterDefinition<MongoChannelMessageDocument> BuildDeliveryClaimFilter(Guid messageId)
        => Builders<MongoChannelMessageDocument>.Filter.Eq(item => item.Id, messageId)
           & Builders<MongoChannelMessageDocument>.Filter.Eq(item => item.RecoveryClaimed, false)
           & NotExpiredOnServerClock<MongoChannelMessageDocument>();

    /// <summary>What a delivery claim whose majority acknowledgement lapsed reads back: the stamp it leaves.</summary>
    internal static FilterDefinition<MongoChannelMessageDocument> BuildDeliveryClaimReadBackFilter(Guid messageId)
        => Builders<MongoChannelMessageDocument>.Filter.Eq(item => item.Id, messageId)
           & Builders<MongoChannelMessageDocument>.Filter.Eq(item => item.RecoveryClaimed, false)
           & Builders<MongoChannelMessageDocument>.Filter.Ne(item => item.AckedAtUtc, null);

    internal static UpdateDefinition<MongoChannelMessageDocument> BuildDeliveryClaimUpdate(long ackSeq)
        => Builders<MongoChannelMessageDocument>.Update.Pipeline(new[]
        {
            // Both fields are computed from the PRE-update document (one $set stage), and the
            // sequence is stamped ONLY when this same update transitions acked_at from null: a
            // row acked by a pre-sequence build must stay permanently unsequenced — back-filling
            // it on a later fan-out re-claim would pair an OLD acked_at with a FRESH sequence
            // value, and a waiter that registered in the original ack's tick would then read the
            // tie as post-registration fan-out, replaying a response its predecessor consumed.
            new BsonDocument("$set", new BsonDocument
            {
                ["acked_at"] = new BsonDocument("$ifNull", new BsonArray { "$acked_at", "$$NOW" }),
                ["acked_seq"] = new BsonDocument("$cond", new BsonArray
                {
                    new BsonDocument("$eq", new BsonArray { new BsonDocument("$ifNull", new BsonArray { "$acked_at", BsonNull.Value }), BsonNull.Value }),
                    ackSeq,
                    "$acked_seq"
                })
            })
        });

    private async Task<long> DrawAckSequenceAsync(CancellationToken cancellationToken)
    {
        BsonDocument? counter;
        try
        {
            counter = await _counters.FindOneAndUpdateAsync<BsonDocument>(
                new BsonDocument("_id", "ack_seq"),
                new BsonDocument("$inc", new BsonDocument("seq", 1L)),
                new FindOneAndUpdateOptions<BsonDocument, BsonDocument> { IsUpsert = true, ReturnDocument = ReturnDocument.After },
                cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (MongoWriteConcerns.IsReplicationTimeout(ex))
        {
            // The $inc applied on the primary; only its reply (the drawn value) is lost. The
            // counter as read now is at or past that draw and is read before the claim lands, so
            // it orders the delivery against registrations exactly as a draw made at this read
            // would — a registration that drew in between ties it, and the watermark resolves a
            // tie conservatively, as history. Throwing here stalled every delivery claim for as
            // long as the set stayed degraded.
            counter = await _countersReadBack.Find(new BsonDocument("_id", "ack_seq")).FirstOrDefaultAsync(cancellationToken).ConfigureAwait(false);
            if (counter is null)
                throw;
            SafeLog.Try(() => _logger.LogDebug(ex, "MongoDB channel ack-sequence draw was applied on the primary, but its majority acknowledgement timed out; using the counter's current value."));
        }

        return counter["seq"].ToInt64();
    }

    /// <summary>
    /// Atomically claims a message for the lost-subscriber path: sets <c>recovery_claimed</c> only
    /// while no waiter has delivered (<c>acked_at</c> is still null). Returns <c>true</c> when
    /// recovery wins; <c>false</c> means a live waiter already took the message, so the publisher
    /// must not also fire the recovery callback. Document-level atomicity of
    /// <c>findOneAndUpdate</c> serializes this against <see cref="TryClaimForDeliveryAsync"/>.
    /// </summary>
    public async Task<bool> TryClaimForRecoveryAsync(Guid messageId, CancellationToken cancellationToken)
    {
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        MongoChannelMessageDocument? claimed;
        try
        {
            claimed = await _messages.FindOneAndUpdateAsync(
                BuildRecoveryClaimFilter(messageId),
                Builders<MongoChannelMessageDocument>.Update.Set(item => item.RecoveryClaimed, true),
                new FindOneAndUpdateOptions<MongoChannelMessageDocument> { ReturnDocument = ReturnDocument.After, Projection = IdOnlyProjection },
                cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (MongoWriteConcerns.IsReplicationTimeout(ex))
        {
            // As for the delivery claim: the claim ran on the primary. recovery_claimed is set only
            // by the publisher's recovery claim, and only while acked_at is null — after which no
            // delivery claim can match — so a document carrying it is one recovery won. Throwing
            // failed a publish whose response was already stored, and the ingress re-published it
            // under a new id.
            claimed = await ReadOnPrimaryAsync(
                Builders<MongoChannelMessageDocument>.Filter.Eq(item => item.Id, messageId)
                & Builders<MongoChannelMessageDocument>.Filter.Eq(item => item.RecoveryClaimed, true),
                IdOnlyProjection,
                cancellationToken).ConfigureAwait(false);
            LogReplicationTimeout(ex, "recovery claim", messageId);
        }

        return claimed is not null;
    }

    internal static FilterDefinition<MongoChannelMessageDocument> BuildRecoveryClaimFilter(Guid messageId)
        => Builders<MongoChannelMessageDocument>.Filter.Eq(item => item.Id, messageId)
           & Builders<MongoChannelMessageDocument>.Filter.Eq(item => item.AckedAtUtc, null);

    /// <summary>Returns the server's current UTC time, used as a clock-safe delivery watermark.</summary>
    public async Task<DateTimeOffset> GetServerTimeUtcAsync(CancellationToken cancellationToken)
    {
        // Primary, explicitly: the watermark must come from the clock whose $$NOW stamps the rows
        // it bounds — a secondary's localTime can lag or skew from the primary's.
        var reply = await _database.RunCommandAsync<BsonDocument>(
            new BsonDocument("hello", 1),
            ReadPreference.Primary,
            cancellationToken).ConfigureAwait(false);
        return reply.TryGetValue("localTime", out var localTime) && localTime is BsonDateTime serverTime
            ? new DateTimeOffset(serverTime.ToUniversalTime(), TimeSpan.Zero)
            : DateTimeOffset.UtcNow;
    }

    /// <summary>
    /// A subscription's registration watermark: the server's UTC clock (for the created-at bound)
    /// and a fresh position in the monotonic ack sequence (for the exact acked-history bound —
    /// see the watermark in the shared channel base). Drawn by ONE atomic counter update whose
    /// pipeline advances the sequence and stamps <c>$$NOW</c> in the same document write
    /// (PG/SqlServer single-statement parity): with separate clock and sequence round trips, a
    /// delivery claim landing between them pairs a same-millisecond <c>acked_at</c> with a lower
    /// sequence, and the same-tick tie-breaker then files a legitimate fan-out delivery as
    /// history.
    /// </summary>
    public async Task<(DateTimeOffset ServerTimeUtc, long StartSeq)> GetSubscriptionStartAsync(CancellationToken cancellationToken)
    {
        var counter = await _counters.FindOneAndUpdateAsync<BsonDocument>(
            new BsonDocument("_id", "ack_seq"),
            BuildSubscriptionStartPipeline(),
            new FindOneAndUpdateOptions<BsonDocument, BsonDocument> { IsUpsert = true, ReturnDocument = ReturnDocument.After },
            cancellationToken).ConfigureAwait(false);

        // The pipeline stamps drawn_at from $$NOW unconditionally, so anything else is a driver
        // anomaly. Failing beats an app-clock fallback, which would silently feed a client clock
        // into the server-clock watermark this draw exists to protect.
        return counter is not null
               && counter.TryGetValue("drawn_at", out var drawnAt)
               && drawnAt is BsonDateTime serverTime
            ? (new DateTimeOffset(serverTime.ToUniversalTime(), TimeSpan.Zero), counter["seq"].ToInt64())
            : throw new InvalidOperationException(
                "MongoDB subscription-start draw returned no server-stamped counter document despite IsUpsert + ReturnDocument.After; the registration watermark is unknown.");
    }

    /// <summary>
    /// Counter-update pipeline for a subscription registration: advances the monotonic ack
    /// sequence AND stamps the draw with the server clock in one atomic document update, so no
    /// delivery claim can interleave between the sequence draw and the clock read.
    /// <c>$ifNull</c> seeds a fresh counter document at 1, matching the delivery claim's
    /// <c>$inc</c> upsert.
    /// </summary>
    internal static UpdateDefinition<BsonDocument> BuildSubscriptionStartPipeline()
        => Builders<BsonDocument>.Update.Pipeline(new[]
        {
            new BsonDocument("$set", new BsonDocument
            {
                ["seq"] = new BsonDocument("$add", new BsonArray { new BsonDocument("$ifNull", new BsonArray { "$seq", 0L }), 1L }),
                ["drawn_at"] = "$$NOW"
            })
        });

    public async Task<bool> IsMessageAcknowledgedAsync(Guid messageId, CancellationToken cancellationToken)
    {
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        var filter = Builders<MongoChannelMessageDocument>.Filter.Eq(item => item.Id, messageId)
                     & NotExpiredOnServerClock<MongoChannelMessageDocument>();
        // Direct member projection, not an anonymous type: anonymous projections lower to the
        // RequiresUnreferencedCode Expression.New(ctor, args, members) overload, which the ILC
        // trim analysis rejects (Roslyn's analyzer skips compiler-lowered expression trees, so
        // only Native AOT publishes catch it). Missing document and unacked document both come
        // back as null, which is exactly the contract here.
        var ackedAtUtc = await _messages.Find(filter)
            .Project(item => item.AckedAtUtc)
            .FirstOrDefaultAsync(cancellationToken).ConfigureAwait(false);
        return ackedAtUtc is not null;
    }

    public async Task UpsertSubscriberAsync(string correlationId, Guid registrationId, string instanceId, TimeSpan ttl, CancellationToken cancellationToken)
    {
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        await _subscribers.UpdateOneAsync(
            Builders<MongoChannelSubscriberDocument>.Filter.Eq(item => item.Id, RegistrationKey(correlationId, registrationId)),
            BuildSubscriberUpsertPipeline(correlationId, registrationId, instanceId, ttl),
            new UpdateOptions { IsUpsert = true },
            cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Upsert pipeline for a subscriber liveness document, with <c>expires_at</c> computed on the
    /// server clock ($$NOW) — app-clock liveness math would let a skewed client look dead (or
    /// immortal) to publishers comparing against server-side expiry.
    /// </summary>
    internal static UpdateDefinition<MongoChannelSubscriberDocument> BuildSubscriberUpsertPipeline(
        string correlationId,
        Guid registrationId,
        string instanceId,
        TimeSpan ttl)
        => Builders<MongoChannelSubscriberDocument>.Update.Pipeline(new[]
        {
            new BsonDocument("$set", new BsonDocument
            {
                ["correlation_id"] = Literal(correlationId),
                ["registration_id"] = new BsonBinaryData(registrationId, GuidRepresentation.Standard),
                ["instance_id"] = Literal(instanceId),
                ["expires_at"] = new BsonDocument("$add", new BsonArray { "$$NOW", ttl.TotalMilliseconds })
            })
        });

    public async Task HeartbeatSubscribersAsync(
        string instanceId,
        IReadOnlyCollection<(string CorrelationId, Guid RegistrationId)> registrations,
        TimeSpan ttl,
        CancellationToken cancellationToken)
    {
        if (registrations.Count == 0)
            return;

        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);

        // Per-registration upserts rather than one bare UpdateMany: the caller only heartbeats
        // registrations that are live in this process, so a missing document means the TTL reaper
        // deleted it (e.g. after a >timeout stall) — re-creating it here is what brings the waiter
        // back from "permanently invisible". Same document shape as UpsertSubscriberAsync.
        var writes = new List<WriteModel<MongoChannelSubscriberDocument>>(registrations.Count);
        foreach (var (correlationId, registrationId) in registrations)
        {
            writes.Add(new UpdateOneModel<MongoChannelSubscriberDocument>(
                Builders<MongoChannelSubscriberDocument>.Filter.Eq(item => item.Id, RegistrationKey(correlationId, registrationId)),
                BuildSubscriberUpsertPipeline(correlationId, registrationId, instanceId, ttl))
            {
                IsUpsert = true
            });
        }

        await _subscribers.BulkWriteAsync(
            writes,
            new BulkWriteOptions { IsOrdered = false },
            cancellationToken).ConfigureAwait(false);
    }

    public async Task DeleteSubscriberAsync(string correlationId, Guid registrationId, CancellationToken cancellationToken)
    {
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        await _subscribers.DeleteOneAsync(
            Builders<MongoChannelSubscriberDocument>.Filter.Eq(item => item.Id, RegistrationKey(correlationId, registrationId)),
            cancellationToken).ConfigureAwait(false);
    }

    public async Task<long> CountActiveSubscribersAsync(string correlationId, CancellationToken cancellationToken)
    {
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        var filter = Builders<MongoChannelSubscriberDocument>.Filter.Eq(item => item.CorrelationId, correlationId)
                     & NotExpiredOnServerClock<MongoChannelSubscriberDocument>();
        // Simple (binary) collation pinned, as the Mongo transport pins it: under an operator-
        // provisioned case- or accent-folding default collation, a count for "ABC" counted "abc"'s
        // live waiter, so a publish to "ABC" took the live route and sat out the whole
        // confirmation budget before recovery, and the watchdog never saw "ABC" as stale. A count
        // has no rows for the dispatch loop's ordinal re-check to screen. (A folding collection
        // loses index use for this query — the transport's documented trade-off.)
        return await _subscribers.CountDocumentsAsync(filter, SimpleCollationCount, cancellationToken).ConfigureAwait(false);
    }

    private static readonly CountOptions SimpleCollationCount = new() { Collation = Collation.Simple };

    /// <summary>
    /// Server-clock expiry filter (<c>$expr: expires_at &gt; $$NOW</c>): message, liveness, and
    /// recovery expiry are all stamped with $$NOW, so comparing them against the app clock would
    /// reintroduce the clock-skew hole the server-side stamps exist to close.
    /// </summary>
    internal static FilterDefinition<TDocument> NotExpiredOnServerClock<TDocument>()
        => new BsonDocument("$expr", new BsonDocument("$gt", new BsonArray { "$expires_at", "$$NOW" }));

    /// <summary>
    /// Watches the message collection with a change stream and invokes
    /// <paramref name="onNotification"/> with the correlation id of every inserted response. One
    /// stream serves every local waiter — the caller routes the id to the right subscription — so
    /// waiter count never multiplies server-side cursors. Runs until cancellation or a stream error;
    /// <paramref name="onOpened"/> runs once the stream is open.
    /// </summary>
    public async Task WatchMessagesAsync(Func<string?, Task> onNotification, CancellationToken cancellationToken, Action? onOpened = null)
    {
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        using var cursor = await _messages.WatchAsync(
            BuildMessageWatchPipeline(),
            new ChangeStreamOptions { FullDocument = ChangeStreamFullDocumentOption.UpdateLookup },
            cancellationToken).ConfigureAwait(false);
        onOpened?.Invoke();
        while (await cursor.MoveNextAsync(cancellationToken).ConfigureAwait(false))
        {
            foreach (var change in cursor.Current)
                await onNotification(change.FullDocument?.CorrelationId).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Change-stream pipeline for response wakes: a <c>$match</c> on insert events. The correlation
    /// id travels in the event's full document, letting the dispatcher scan only the signaled
    /// correlation id — the targeted-wake contract. The <c>$project</c> keeps only that id (plus
    /// the event's <c>_id</c> resume token, which an inclusion projection keeps implicitly, and its
    /// operation type): every process watches every insert, and shipping each full envelope to
    /// every watcher just to read one field cost payload × watchers on every publish.
    /// </summary>
    internal static PipelineDefinition<ChangeStreamDocument<MongoChannelMessageDocument>, ChangeStreamDocument<MongoChannelMessageDocument>> BuildMessageWatchPipeline()
        => new EmptyPipelineDefinition<ChangeStreamDocument<MongoChannelMessageDocument>>()
            .Match(change => change.OperationType == ChangeStreamOperationType.Insert)
            .Project(new BsonDocumentProjectionDefinition<ChangeStreamDocument<MongoChannelMessageDocument>, ChangeStreamDocument<MongoChannelMessageDocument>>(
                new BsonDocument
                {
                    ["operationType"] = 1,
                    ["fullDocument.correlation_id"] = 1
                }));

    /// <summary>The fault classification the shared channel base retries its post-insert store calls on.</summary>
    internal static bool IsTransient(Exception exception) => MongoTransientFaults.IsTransient(exception);

    /// <summary>Returns <c>true</c> when the server rejected the change stream itself (not a transient cursor error).</summary>
    internal static bool IsChangeStreamUnsupported(Exception exception)
        => exception is MongoCommandException commandException
           && (commandException.Code == 40573
               || commandException.Message.Contains("only supported on replica sets", StringComparison.OrdinalIgnoreCase));

    internal static string RegistrationKey(string correlationId, Guid registrationId)
        => $"{correlationId}:{registrationId:N}";

    /// <summary>
    /// A caller-supplied string as a pipeline-update VALUE. The upserts are aggregation pipelines,
    /// where a plain string beginning with <c>$</c> is read as a field path (<c>$$</c>: a variable)
    /// — so a correlation id like <c>"$order-17"</c> resolved to a missing field and was never
    /// stored: the subscriber and recovery documents lost their <c>correlation_id</c>, every
    /// lookup for the id matched nothing, and each response for it was silently dropped. The
    /// transport and flow stores wrap their user strings the same way.
    /// </summary>
    private static BsonDocument Literal(string value) => new("$literal", value);

    /// <summary>
    /// Name of the derived ack-counter collection. Part of the effective collection-name plan:
    /// options validation must keep the configured collections distinct from this derived name,
    /// or counter documents land in (for example) the TTL-indexed recovery collection, where the
    /// reaper would silently delete the ack sequence and reset the same-tick tie-breaker.
    /// </summary>
    internal static string CountersCollectionName(string messageCollection) => $"{messageCollection}_counters";

    /// <summary>Validates a MongoDB collection name coming from options.</summary>
    public static void ValidateCollectionName(string? value, string name)
    {
        if (string.IsNullOrWhiteSpace(value))
            throw new InvalidOperationException($"{nameof(MongoDbAsyncResponseChannelOptions)}.{name} must be configured.");
        if (value.Contains('$') || value.Contains('\0')
            || value.StartsWith("system.", StringComparison.Ordinal) || value.Contains(".system.", StringComparison.Ordinal))
            throw new InvalidOperationException(
                $"{nameof(MongoDbAsyncResponseChannelOptions)}.{name} '{value}' must be a valid MongoDB collection name (no '$' or NUL characters, not in or containing the reserved system namespace).");
        if (string.Equals(value, MongoOwnershipLedger.CollectionName, StringComparison.Ordinal))
            throw new InvalidOperationException(
                $"{nameof(MongoDbAsyncResponseChannelOptions)}.{name} '{value}' is reserved for the cross-component ownership ledger.");
    }

    /// <summary>Disposes the Mongo client when the store created (and therefore owns) it.</summary>
    public void Dispose()
    {
        _ensureGate.Dispose();
        (_ownedClient as IDisposable)?.Dispose();
    }
}

/// <remarks>
/// <c>[BsonIgnoreExtraElements]</c> is load-bearing, as on the transport's and flow store's
/// documents: the driver's default is to THROW for any element outside this class map, so the
/// next element a newer build adds would break older hosts mid rolling deploy — a claim that
/// committed <c>acked_at</c> and then failed to deserialize its reply, a hydration read, a
/// change-stream event. Unknown elements are ignored instead.
/// </remarks>
[BsonIgnoreExtraElements]
internal sealed class MongoRecoveryStateDocument
{
    [BsonId]
    [BsonElement("_id")]
    public string Id { get; set; } = "";

    [BsonElement("correlation_id")]
    public string CorrelationId { get; set; } = "";

    [BsonElement("registration_id")]
    [BsonGuidRepresentation(GuidRepresentation.Standard)]
    public Guid RegistrationId { get; set; }

    [BsonElement("state_json")]
    public string StateJson { get; set; } = "";

    [BsonElement("expires_at")]
    public DateTime ExpiresAtUtc { get; set; }

    [BsonElement("registered_at")]
    public DateTime RegisteredAtUtc { get; set; }
}

/// <remarks>
/// <c>[BsonIgnoreExtraElements]</c> is load-bearing, as on the transport's and flow store's
/// documents: the driver's default is to THROW for any element outside this class map, so the
/// next element a newer build adds would break older hosts mid rolling deploy — a claim that
/// committed <c>acked_at</c> and then failed to deserialize its reply, a hydration read, a
/// change-stream event. Unknown elements are ignored instead.
/// </remarks>
[BsonIgnoreExtraElements]
internal sealed class MongoChannelMessageDocument
{
    [BsonId]
    [BsonElement("_id")]
    [BsonGuidRepresentation(GuidRepresentation.Standard)]
    public Guid Id { get; set; }

    [BsonElement("correlation_id")]
    public string CorrelationId { get; set; } = "";

    /// <summary>Null only on a sweep projection of an acknowledged document (<see cref="MongoDbChannelStore.SweepProjection"/>).</summary>
    [BsonElement("envelope_json")]
    public string? EnvelopeJson { get; set; } = "";

    [BsonElement("created_at")]
    public DateTime CreatedAtUtc { get; set; }

    [BsonElement("expires_at")]
    public DateTime ExpiresAtUtc { get; set; }

    [BsonElement("acked_at")]
    public DateTime? AckedAtUtc { get; set; }

    [BsonElement("acked_seq")]
    public long? AckedSeq { get; set; }

    [BsonElement("recovery_claimed")]
    public bool RecoveryClaimed { get; set; }
}

/// <remarks>
/// <c>[BsonIgnoreExtraElements]</c> is load-bearing, as on the transport's and flow store's
/// documents: the driver's default is to THROW for any element outside this class map, so the
/// next element a newer build adds would break older hosts mid rolling deploy — a claim that
/// committed <c>acked_at</c> and then failed to deserialize its reply, a hydration read, a
/// change-stream event. Unknown elements are ignored instead.
/// </remarks>
[BsonIgnoreExtraElements]
internal sealed class MongoChannelSubscriberDocument
{
    [BsonId]
    [BsonElement("_id")]
    public string Id { get; set; } = "";

    [BsonElement("correlation_id")]
    public string CorrelationId { get; set; } = "";

    [BsonElement("registration_id")]
    [BsonGuidRepresentation(GuidRepresentation.Standard)]
    public Guid RegistrationId { get; set; }

    [BsonElement("instance_id")]
    public string InstanceId { get; set; } = "";

    [BsonElement("expires_at")]
    public DateTime ExpiresAtUtc { get; set; }
}
