using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Bson.Serialization.Serializers;
using MongoDB.Driver;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using AsyncResponse.Internal;

namespace AsyncResponse.Transports.MongoDB;

internal enum MongoDbSubscriberRole
{
    Worker,
    ResponseIngress
}

/// <summary>A claimed MongoDB transport document, decoupled from driver types for dispatch tests.</summary>
/// <remarks>
/// <c>RenewAsync</c> extends the claim's lease (<c>locked_until</c>) by the original lock timeout,
/// fenced on the claim's <c>lock_id</c>, abandoning the attempt when its token fires (the heartbeat
/// bounds every attempt so a hung one is retried inside the lease); it returns <c>false</c> when the fence no longer matches
/// (the lease lapsed and another subscriber re-claimed the document).
/// </remarks>
internal sealed record MongoDbTransportDelivery(
    Guid Id,
    string Queue,
    string Payload,
    IReadOnlyDictionary<string, string> Headers,
    int Attempt,
    Func<ValueTask> AckAsync,
    Func<TimeSpan, ValueTask> NakAsync,
    Func<Exception, bool, CancellationToken, ValueTask<bool>> DeadLetterAsync,
    Func<CancellationToken, ValueTask<bool>> RenewAsync);

/// <summary>Small document adapter for the MongoDB transport queue collection.</summary>
internal sealed class MongoDbTransportStore : IDisposable
{
    private readonly IMongoCollection<MongoTransportMessageDocument> _messages;
    private readonly IMongoCollection<MongoTransportMessageDocument> _leaseMessages;
    private readonly IMongoCollection<BsonDocument> _rawLeaseMessages;
    private readonly MongoDbAsyncResponseTransportOptions _options;
    private readonly ILogger<MongoDbTransportStore>? _logger;
    private readonly SemaphoreSlim _ensureGate = new(1, 1);
    private readonly IMongoClient? _ownedClient;
    private bool _created;
    private long _lastDeadLetterPruneStamp;
    private readonly IMongoDatabase _database;

    public MongoDbTransportStore(
        IMongoDatabase database,
        IOptions<MongoDbAsyncResponseTransportOptions> options,
        IMongoClient? ownedClient = null,
        ILogger<MongoDbTransportStore>? logger = null,
        IMongoNamespaceRegistry? namespaceRegistry = null)
    {
        _options = options.Value;
        _logger = logger;
        MongoDbTransportOptionsValidator.ValidateCommon(_options);

        // Cross-component collection ownership (DI-hosted stores only): see MongoNamespaceRegistry.
        namespaceRegistry?.Claim(
            MongoNamespaceRegistry.ClusterKey(database),
            database.DatabaseNamespace.DatabaseName,
            "MongoDB transport",
            [(_options.MessageCollection, nameof(_options.MessageCollection))]);

        // The namespace BYTE limit can only be checked here, where the actual database name is
        // first known; a near-limit configuration otherwise passes every static check and fails
        // at the first server operation.
        MongoNamespaceRegistry.ValidateEffectiveNamespace(database, _options.MessageCollection, nameof(_options.MessageCollection));

        _database = database;
        // Pinned to the primary (channel / flow-store parity): a secondaryPreferred client would
        // route the change-stream wake to a lagging secondary, so worker jobs woke at replication
        // lag and delivery quietly degraded to EmptyPollDelay polling.
        //
        // Two write concerns on the one namespace. The INSERTS — the publish upsert and the
        // dead-letter insert — pin a bounded majority (flow-store / channel parity, see
        // MongoWriteConcerns): under an inherited w=1 the primary acknowledged a job — or a
        // durable flow's wake-up — that a failover then rolled back, so a publish the caller saw
        // succeed was simply gone. Everything else — the LEASE writes (claim, renew, NAK) and the
        // DELETES (ack, burial, prune) — pins w=1, keeping the inherited journal: rolling one of
        // them back only returns a document to claimable, exactly as a lapsed lease does (a
        // rolled-back ack reruns the job under the at-least-once contract, a rolled-back burial
        // re-buries onto the same deterministic dead-letter id, whose insert — majority, and
        // written first — cannot be rolled back without the delete after it), so majority buys
        // them nothing, and it cost three things. A claim whose wtimeout lapsed had still
        // stamped attempts+1 on the primary but threw before any delivery existed, so a
        // replication stall of a few LockTimeouts dead-lettered (or, with the DLQ off, deleted)
        // a job that never ran; a claim acknowledged only after the replication wait could return
        // after its own server-stamped lease had expired, with a peer already running the same
        // document; and every settlement held the claim loop for the whole wtimeout, then
        // reported a delete the primary had applied as failed. Pinned, not inherited: the
        // inherited concern is itself majority on most deployments (an Atlas-style w=majority
        // string, or no w at all against a 5.0+ primary-secondary-secondary set), which kept
        // those hazards there.
        var primaryMessages = database.GetCollection<MongoTransportMessageDocument>(_options.MessageCollection)
            .WithReadPreference(ReadPreference.Primary);
        _leaseMessages = primaryMessages.WithWriteConcern(MongoWriteConcerns.PrimaryAcknowledged(database));
        _messages = primaryMessages.WithWriteConcern(MongoWriteConcerns.BoundedMajority(database));
        // The same namespace, untyped, for the claim and the unreadable-document delete only: the
        // claim reads the stamped document as raw BSON and maps it inside a try (see
        // TryClaimAsync), so a document no class map can read is buried instead of throwing from
        // inside findOneAndUpdate.
        _rawLeaseMessages = database.GetCollection<BsonDocument>(_options.MessageCollection)
            .WithReadPreference(ReadPreference.Primary)
            .WithWriteConcern(MongoWriteConcerns.PrimaryAcknowledged(database));
        _ownedClient = ownedClient;
    }

    public async Task EnsureCreatedAsync(CancellationToken cancellationToken = default)
    {
        // Not short-circuited on AutoCreateIndexes/UseOwnershipLedger both being off: the
        // read-only index check below still runs once, and _created then keeps this cheap.
        if (_created)
            return;

        await _ensureGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_created)
                return;

            // Persisted cross-host ownership, independent of AutoCreateIndexes: see
            // MongoOwnershipLedger.
            if (_options.UseOwnershipLedger)
            {
                await MongoOwnershipLedger.ClaimAsync(
                    _database,
                    "MongoDB transport",
                    [(_options.MessageCollection, nameof(_options.MessageCollection))],
                    cancellationToken).ConfigureAwait(false);
            }

            if (!_options.AutoCreateIndexes)
            {
                // Channel / flow-store parity: verify (read-only, warn-only) instead of skipping
                // silently. A missing claim index turned every claim into a full collection scan
                // per poll tick, per subscriber, with no error and no log line.
                await WarnIfClaimIndexMissingAsync(cancellationToken).ConfigureAwait(false);
                _created = true;
                return;
            }

            // Through the lease handle's w=1, like the claim itself: createIndexes carries the
            // handle's write concern, so under the bounded majority a host that started
            // during a replication stall could fail EnsureCreated — which every claim runs first —
            // and could not claim through the very stall the lease handle exists to ride out. Index
            // DDL is idempotent, and a build a failover rolled back simply reruns on the next start.
            // An equivalent index under another name — the default-named one the
            // AutoCreateIndexes = false warning prescribes — is accepted (see MongoIndexes).
            await MongoIndexes.CreateOrAcceptEquivalentAsync(
                _leaseMessages,
                new CreateIndexModel<MongoTransportMessageDocument>(
                    Builders<MongoTransportMessageDocument>.IndexKeys
                        .Ascending(item => item.Queue)
                        .Ascending(item => item.AvailableAtUtc)
                        .Ascending(item => item.CreatedAtUtc),
                    new CreateIndexOptions { Name = $"{_options.MessageCollection}_claim_idx" }),
                replaceConflicting: false,
                cancellationToken).ConfigureAwait(false);
            await MongoIndexes.CreateOrAcceptEquivalentAsync(
                _leaseMessages,
                new CreateIndexModel<MongoTransportMessageDocument>(
                    Builders<MongoTransportMessageDocument>.IndexKeys.Ascending(item => item.CreatedAtUtc),
                    new CreateIndexOptions { Name = $"{_options.MessageCollection}_created_idx" }),
                replaceConflicting: false,
                cancellationToken).ConfigureAwait(false);
            _created = true;
        }
        finally
        {
            _ensureGate.Release();
        }
    }

    private async Task WarnIfClaimIndexMissingAsync(CancellationToken cancellationToken)
    {
        try
        {
            List<BsonDocument> indexes;
            try
            {
                using var cursor = await _messages.Indexes.ListAsync(cancellationToken).ConfigureAwait(false);
                indexes = await cursor.ToListAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (MongoCommandException ex) when (ex.Code == 26)
            {
                // NamespaceNotFound: MongoDB creates the collection bare on the first write —
                // which, with index DDL disabled, is exactly a collection with no claim index.
                indexes = [];
            }

            // Matched by KEY, not by name: operators own the naming of manually provisioned indexes.
            var claimIndexed = indexes.Any(index =>
                index.TryGetValue("key", out var key)
                && key is BsonDocument keys
                && keys.ElementCount > 0
                && string.Equals(keys.GetElement(0).Name, "queue", StringComparison.Ordinal));
            if (!claimIndexed)
            {
                _logger?.LogWarning(
                    "MongoDB collection {Database}.{Collection} has no index leading on 'queue' and AutoCreateIndexes is disabled. " +
                    "Every claim scans the whole collection on every poll tick — performance only; create the claim index " +
                    "(queue, available_at, created_at) or enable AutoCreateIndexes.",
                    _database.DatabaseNamespace.DatabaseName,
                    _options.MessageCollection);
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // A deployment that cannot even list indexes (no listIndexes privilege, server
            // unreachable at first use) must not lose the actual operation to the check: the
            // caller's own store call surfaces any real connectivity failure.
            _logger?.LogDebug(ex, "Skipping index verification for the manually managed MongoDB transport collection; listIndexes was not available.");
        }
    }

    /// <summary>
    /// Publishes a queue document. The caller supplies the id so a retried publish is idempotent —
    /// a duplicate-key insert is treated as success rather than enqueuing the same job twice.
    /// </summary>
    public async Task PublishAsync(
        Guid id,
        string queue,
        string payload,
        IReadOnlyDictionary<string, string>? headers,
        CancellationToken cancellationToken,
        TimeSpan? delay = null)
    {
        await InsertAsync(id, queue, payload, headers, deadLetterReason: null, cancellationToken, delay).ConfigureAwait(false);

        // The document is committed: nothing after this line may fail the publish. A prune that
        // threw (a connection error, the caller's token firing mid-delete) reported a FAILED
        // publish for a job that is already claimable, and the caller's retry ran it twice. The
        // prune swallows its own failures (see DbDeadLetterPrune).
        await DbDeadLetterPrune.RunIfDueAsync(
            ref _lastDeadLetterPruneStamp,
            _options.DeadLetterRetention,
            PruneDeadLetterBatchAsync,
            _logger,
            "MongoDB",
            cancellationToken).ConfigureAwait(false);
    }

    public async Task<MongoDbTransportDelivery?> TryClaimAsync(string queue, TimeSpan lockTimeout, CancellationToken cancellationToken)
    {
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);

        MongoTransportMessageDocument claimed;
        Guid lockId;
        while (true)
        {
            lockId = Guid.NewGuid();

            // findOneAndUpdate is atomic per document: of all competing consumers, exactly one
            // observes the document unlocked and stamps its lock_id/locked_until in the same
            // server-side step. Binary (simple) collation pinned on the claim (SQL Server BIN2 /
            // PostgreSQL deterministic-collation parity): the three logical queues share this
            // collection and are told apart by nothing but the queue field, and an operator-created
            // collection with a case- or accent-folding default collation made the worker
            // subscriber claim response documents — which the ingress then dropped and ACKed with
            // no dead-letter record. An explicit simple collation cannot use an index built with a
            // folding one, so correctness costs a scan there; the default (bare) collection is
            // unaffected.
            var raw = await _rawLeaseMessages.FindOneAndUpdateAsync(
                BuildClaimFilter(queue),
                BuildClaimUpdate(lockId, lockTimeout),
                new FindOneAndUpdateOptions<BsonDocument>
                {
                    Sort = ClaimSort,
                    ReturnDocument = ReturnDocument.After,
                    Collation = Collation.Simple
                },
                cancellationToken).ConfigureAwait(false);
            if (raw is null)
                return null;

            // Mapped HERE, inside a try, not by the driver inside findOneAndUpdate. Queue documents
            // can be written by foreign producers (the documented reply-target contract), and a
            // field the class map cannot read — a driver-generated ObjectId _id (every Mongo
            // driver's default), a payload written as an embedded document, any future mistyped
            // field — threw FormatException AFTER the server had stamped attempts+1/lock_id and
            // before any delivery existed: the document could never reach the failure handler or
            // the dead-letter queue, so it was re-claimed every LockTimeout forever, faulting the
            // subscriber each time. It cannot succeed on any attempt, so it is buried on sight —
            // fenced by the lock_id this claim just stamped, which needs no typed _id — and the
            // claim moves on to the next document.
            try
            {
                claimed = BsonSerializer.Deserialize<MongoTransportMessageDocument>(raw);
                break;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                await BuryUnreadableAsync(raw, lockId, queue, ex).ConfigureAwait(false);
            }
        }

        // Indexer, not the copying constructor: documents can be written by foreign producers, and
        // BSON legally carries field names differing only in case — the constructor's internal Add
        // would throw AFTER the claim already stamped attempts+1/lock_id, before any delivery
        // exists, so the document could never reach HandleFailureAsync or dead-letter: an
        // unkillable poison document that tears down the subscriber on every re-claim. Last-wins,
        // matching the ASB/SQS receive adapters.
        IReadOnlyDictionary<string, string> headers;
        if (claimed.Headers is null)
        {
            headers = EmptyHeaders;
        }
        else
        {
            var copied = new Dictionary<string, string>(claimed.Headers.Count, StringComparer.OrdinalIgnoreCase);
            foreach (var pair in claimed.Headers)
                copied[pair.Key] = pair.Value;
            headers = copied;
        }

        return new MongoDbTransportDelivery(
            claimed.Id,
            claimed.Queue,
            claimed.Payload,
            headers,
            claimed.Attempts,
            () => AckAsync(claimed.Id, lockId),
            delay => NakAsync(claimed.Id, lockId, delay),
            (exception, deleteOriginal, token) => DeadLetterAsync(claimed.Id, lockId, queue, claimed.Payload, headers, exception, deleteOriginal, token),
            token => RenewLeaseAsync(claimed.Id, lockId, lockTimeout, token));
    }

    /// <summary>
    /// Claim order: due time first, then arrival — the claim index's own key order
    /// (<c>queue, available_at, created_at</c>), so the index walks the queue's due documents in
    /// order and the first unlocked one ends the scan (PostgreSQL / SQL Server parity). The
    /// former <c>created_at</c> sort sat behind a range on <c>available_at</c>, which breaks the
    /// index order: every claim sorted all of the queue's due documents in memory to take one —
    /// draining a backlog was quadratic — and a NAKed or delayed document jumped ahead of
    /// everything published while it waited. Documents written by earlier builds carry an epoch
    /// <c>available_at</c> and simply sort first.
    /// </summary>
    internal static readonly SortDefinition<BsonDocument> ClaimSort =
        Builders<BsonDocument>.Sort.Ascending("available_at").Ascending("created_at");

    /// <summary>
    /// Claim filter: available and not (still) locked, evaluated against the server clock
    /// (<c>$$NOW</c>) so publisher/consumer clock skew never fences messages in or out.
    /// A missing <c>locked_until</c> compares as null and therefore as expired.
    /// </summary>
    internal static FilterDefinition<BsonDocument> BuildClaimFilter(string queue)
        => Builders<BsonDocument>.Filter.Eq("queue", queue)
           & new BsonDocumentFilterDefinition<BsonDocument>(new BsonDocument(
               "$expr",
               new BsonDocument("$and", new BsonArray
               {
                   new BsonDocument("$lte", new BsonArray { "$available_at", "$$NOW" }),
                   new BsonDocument("$or", new BsonArray
                   {
                       new BsonDocument("$eq", new BsonArray { "$locked_until", BsonNull.Value }),
                       new BsonDocument("$lte", new BsonArray { "$locked_until", "$$NOW" })
                   })
               })));

    /// <remarks>
    /// <c>attempts</c> goes through <c>$convert</c> (onError/onNull 0), not a bare <c>$add</c>: a
    /// foreign producer's non-numeric <c>attempts</c> made the server-side update itself fail, so
    /// the document was never locked, stayed at the head of the claim order, and every claim of
    /// the whole queue failed behind it.
    /// </remarks>
    internal static UpdateDefinition<BsonDocument> BuildClaimUpdate(Guid lockId, TimeSpan lockTimeout)
        => Builders<BsonDocument>.Update.Pipeline(new[]
        {
            new BsonDocument("$set", new BsonDocument
            {
                ["attempts"] = new BsonDocument("$add", new BsonArray
                {
                    new BsonDocument("$convert", new BsonDocument
                    {
                        ["input"] = "$attempts",
                        ["to"] = "int",
                        ["onError"] = 0,
                        ["onNull"] = 0
                    }),
                    1
                }),
                ["locked_until"] = new BsonDocument("$add", new BsonArray { "$$NOW", lockTimeout.TotalMilliseconds }),
                ["lock_id"] = new BsonBinaryData(lockId, GuidRepresentation.Standard)
            })
        });

    /// <summary>
    /// Buries a claimed document the class map cannot read (see <see cref="TryClaimAsync"/>) and
    /// removes it by the <c>lock_id</c> this claim stamped. The dead-letter copy keeps what can be
    /// kept — the payload (as-is when it is a string, as JSON otherwise), the headers (read
    /// leniently), the reason, and the raw <c>_id</c> — under an id derived from the raw
    /// <c>_id</c>, so a crash between the insert and the delete re-buries onto the same document
    /// (the same insert-first idempotency as <see cref="DeadLetterAsync"/>). Settlement runs on
    /// <see cref="CancellationToken.None"/>: burying a poison document must not be abandoned
    /// half-done by a shutdown. A failure propagates like any claim failure; the lease lapses and
    /// the next claim buries it again. The Error is logged after the burial, through a guard: a
    /// throwing logging provider must not abort a burial half-done. A burial that fails logs the
    /// document and why it could not be read before the failure propagates, so a burial that
    /// fails on every claim (a dead-letter copy over the document size limit) still names it.
    /// </summary>
    private async Task BuryUnreadableAsync(BsonDocument raw, Guid lockId, string queue, Exception error)
    {
        var rawId = raw.GetValue("_id", BsonNull.Value);
        try
        {
            await BuryUnreadableCoreAsync(raw, rawId, lockId, queue, error).ConfigureAwait(false);
        }
        catch (Exception buryFailure)
        {
            SafeLog.Try(
                (Logger: _logger, ReadError: error.Message, Failure: buryFailure, DocumentId: rawId.ToString(), Queue: queue),
                static state => state.Logger?.LogError(
                    state.Failure,
                    "MongoDB transport document {DocumentId} on queue {Queue} could not be read as a transport message ({ReadError}), and burying it failed; it stays claimed until its lease lapses and the next claim buries it again.",
                    state.DocumentId,
                    state.Queue,
                    state.ReadError));
            throw;
        }
    }

    private async Task BuryUnreadableCoreAsync(BsonDocument raw, BsonValue rawId, Guid lockId, string queue, Exception error)
    {
        var reason = $"The document could not be read as a transport message: {error.Message}";

        if (_options.DeadLetterEnabled)
        {
            var payload = raw.GetValue("payload", BsonNull.Value) switch
            {
                { IsString: true } text => text.AsString,
                { IsBsonNull: true } => "",
                var other => other.ToJson()
            };

            // Indexer copy: case-variant duplicate keys are legal BSON (see TryClaimAsync).
            var deadHeaders = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            if (LenientTransportHeaderSerializer.Materialize(raw.GetValue("headers", BsonNull.Value)) is { } headers)
            {
                foreach (var pair in headers)
                    deadHeaders[pair.Key] = pair.Value;
            }

            deadHeaders["AR-DeadLetter-Reason"] = Sanitize(reason);
            deadHeaders["AR-DeadLetter-Source-Queue"] = queue;
            deadHeaders["AR-DeadLetter-Source-Id"] = rawId.ToString() ?? "";
            await InsertAsync(UnreadableDeadLetterId(rawId), _options.DeadLetterQueue, payload, deadHeaders, reason, CancellationToken.None).ConfigureAwait(false);
        }

        await _rawLeaseMessages.DeleteOneAsync(
            new BsonDocument("lock_id", new BsonBinaryData(lockId, GuidRepresentation.Standard)),
            CancellationToken.None).ConfigureAwait(false);

        SafeLog.Try(() =>
        {
            if (_options.DeadLetterEnabled)
            {
                _logger?.LogError(
                    error,
                    "MongoDB transport document {DocumentId} on queue {Queue} could not be read as a transport message; dead-lettered it without executing it.",
                    rawId.ToString(),
                    queue);
            }
            else
            {
                _logger?.LogError(
                    error,
                    "MongoDB transport document {DocumentId} on queue {Queue} could not be read as a transport message; removed it without executing it (DeadLetterEnabled is false, so no dead-letter copy was written).",
                    rawId.ToString(),
                    queue);
            }
        });
    }

    /// <summary>
    /// Deterministic dead-letter id for an UNREADABLE source document, derived from its raw
    /// <c>_id</c> bytes (whatever their BSON type) — the counterpart of <see cref="DeadLetterId"/>.
    /// </summary>
    internal static Guid UnreadableDeadLetterId(BsonValue rawId)
    {
        var prefix = Encoding.UTF8.GetBytes("asyncresponse:deadletter:raw:");
        var idBytes = new BsonDocument("_id", rawId).ToBson();
        var input = new byte[prefix.Length + idBytes.Length];
        prefix.CopyTo(input, 0);
        idBytes.CopyTo(input, prefix.Length);
        return new Guid(SHA256.HashData(input).AsSpan(0, 16));
    }

    public async IAsyncEnumerable<MongoDbTransportDelivery> ClaimBatchAsync(
        string queue,
        int batchSize,
        TimeSpan lockTimeout,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        for (var i = 0; i < batchSize; i++)
        {
            var delivery = await TryClaimAsync(queue, lockTimeout, cancellationToken).ConfigureAwait(false);
            if (delivery is null)
                yield break;
            yield return delivery;
        }
    }

    private async Task InsertAsync(
        Guid id,
        string queue,
        string payload,
        IReadOnlyDictionary<string, string>? headers,
        string? deadLetterReason,
        CancellationToken cancellationToken,
        TimeSpan? delay = null)
    {
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await _messages.UpdateOneAsync(
                Builders<MongoTransportMessageDocument>.Filter.Eq(item => item.Id, id),
                BuildInsertPipeline(queue, payload, headers, deadLetterReason, delay),
                new UpdateOptions { IsUpsert = true },
                cancellationToken).ConfigureAwait(false);
        }
        catch (MongoWriteException ex) when (ex.WriteError?.Category == ServerErrorCategory.DuplicateKey)
        {
            // Two concurrent upserts of one id can both attempt the insert; the loser's
            // duplicate-key error is the outcome the caller asked for (a retried publish found the
            // document already present): idempotent success.
        }
        catch (Exception ex) when (MongoWriteConcerns.IsReplicationTimeout(ex))
        {
            // The upsert applied on the primary — subscribers claim from there, so the job will
            // run — and only its majority acknowledgement timed out. Reporting it failed made the
            // caller re-publish under a new id (the flow engine re-parks the same way): the job ran
            // twice, and a burial logged "no dead-letter copy" for a copy that exists. A same-id
            // retry is no safer: a subscriber can claim, run and ack (delete) the document inside
            // the retry window, and the upsert's $ifNull defaults then re-create it. So it counts
            // as written; what it gives up is the majority guarantee, which the warning says. (Static
            // state form: a capturing lambda would allocate its closure on every publish — the
            // parameters it captures are in scope from method entry.)
            SafeLog.Try((Logger: _logger, Error: ex, Id: id, Queue: queue), static state => state.Logger?.LogWarning(
                state.Error,
                "MongoDB transport document {MessageId} on queue {Queue} was written on the primary, but its majority acknowledgement " +
                "timed out; treating it as written rather than risking a duplicate. A failover before it replicates can still roll it " +
                "back — restore the replica set's secondaries (or remove the arbiter) rather than lowering the write concern.",
                state.Id,
                state.Queue));
        }
    }

    /// <summary>
    /// Upsert pipeline for a queue document. <c>available_at</c> and <c>created_at</c> — the claim
    /// <see cref="ClaimSort"/> keys, and <c>created_at</c> the dead-letter prune cutoff — are
    /// stamped from the SERVER clock (<c>$$NOW</c>), matching the claim/renew/NAK updates: a
    /// behind-clock publisher's client stamp would otherwise sort its rows permanently to the
    /// queue head and shift them across the prune boundary. <c>$ifNull</c> keeps the first
    /// (server-stamped) values when a publish retry finds the document already present, and a
    /// retry must not reset a claimed document's attempts or lease either — those fields are left
    /// alone entirely.
    /// </summary>
    /// <remarks>
    /// "Available immediately on arrival" stamps <c>$$NOW</c>, the SQL stores'
    /// <c>available_at DEFAULT now()</c>: the claim filter's <c>$lte</c> against its own
    /// <c>$$NOW</c> admits it at once, and it takes its arrival place in the claim order. (It
    /// stamped epoch before, which under an <c>available_at</c>-first order would starve every
    /// due NAKed or delayed document behind a steady stream of immediate ones.) A DELAYED
    /// publish computes its due time server-relative (<c>$$NOW + delay</c>), mirroring the NAK
    /// update, so client clock skew cannot shift it. User-supplied strings ride inside
    /// <c>$literal</c>: in a pipeline expression a plain string beginning with <c>$</c> would
    /// otherwise be read as a field path or variable.
    /// </remarks>
    internal static UpdateDefinition<MongoTransportMessageDocument> BuildInsertPipeline(
        string queue,
        string payload,
        IReadOnlyDictionary<string, string>? headers,
        string? deadLetterReason,
        TimeSpan? delay)
    {
        var headerArray = new BsonArray();
        if (headers is not null)
        {
            foreach (var pair in headers)
                headerArray.Add(new BsonDocument { ["k"] = pair.Key, ["v"] = pair.Value });
        }

        return Builders<MongoTransportMessageDocument>.Update.Pipeline(new[]
        {
            new BsonDocument("$set", new BsonDocument
            {
                ["queue"] = new BsonDocument("$literal", queue),
                ["payload"] = new BsonDocument("$literal", payload),
                ["headers"] = new BsonDocument("$ifNull", new BsonArray { "$headers", new BsonDocument("$literal", headerArray) }),
                ["created_at"] = new BsonDocument("$ifNull", new BsonArray { "$created_at", "$$NOW" }),
                ["available_at"] = new BsonDocument("$ifNull", new BsonArray
                {
                    "$available_at",
                    delay is { } pending
                        ? new BsonDocument("$add", new BsonArray { "$$NOW", pending.TotalMilliseconds })
                        : (BsonValue)"$$NOW"
                }),
                ["attempts"] = new BsonDocument("$ifNull", new BsonArray { "$attempts", 0 }),
                ["dead_letter_reason"] = new BsonDocument("$ifNull", new BsonArray
                {
                    "$dead_letter_reason",
                    deadLetterReason is null ? BsonNull.Value : new BsonDocument("$literal", deadLetterReason)
                })
            })
        });
    }

    private async ValueTask AckAsync(Guid id, Guid lockId)
    {
        await _leaseMessages.DeleteOneAsync(
            Builders<MongoTransportMessageDocument>.Filter.Eq(item => item.Id, id)
            & Builders<MongoTransportMessageDocument>.Filter.Eq(item => item.LockId, lockId),
            CancellationToken.None).ConfigureAwait(false);
    }

    private async ValueTask NakAsync(Guid id, Guid lockId, TimeSpan delay)
    {
        await _leaseMessages.UpdateOneAsync(
            Builders<MongoTransportMessageDocument>.Filter.Eq(item => item.Id, id)
            & Builders<MongoTransportMessageDocument>.Filter.Eq(item => item.LockId, lockId),
            BuildNakUpdate(delay),
            options: null,
            CancellationToken.None).ConfigureAwait(false);
    }

    // The token is the heartbeat's per-attempt bound: the driver's socket timeout is infinite by
    // default, so a renew hung on a dead connection or a failover never came back and the lease
    // lapsed before the short-backoff retry ever ran.
    private async ValueTask<bool> RenewLeaseAsync(Guid id, Guid lockId, TimeSpan lockTimeout, CancellationToken cancellationToken)
    {
        var result = await _leaseMessages.UpdateOneAsync(
            Builders<MongoTransportMessageDocument>.Filter.Eq(item => item.Id, id)
            & Builders<MongoTransportMessageDocument>.Filter.Eq(item => item.LockId, lockId),
            BuildRenewUpdate(lockTimeout),
            options: null,
            cancellationToken).ConfigureAwait(false);
        return result.MatchedCount > 0;
    }

    /// <summary>
    /// Fenced lease renewal: extends <c>locked_until</c> from the server clock (<c>$$NOW</c>) only
    /// while the claim's <c>lock_id</c> fence still matches, mirroring the claim update.
    /// </summary>
    internal static UpdateDefinition<MongoTransportMessageDocument> BuildRenewUpdate(TimeSpan lockTimeout)
        => Builders<MongoTransportMessageDocument>.Update.Pipeline(new[]
        {
            new BsonDocument("$set", new BsonDocument
            {
                ["locked_until"] = new BsonDocument("$add", new BsonArray { "$$NOW", lockTimeout.TotalMilliseconds })
            })
        });

    internal static UpdateDefinition<MongoTransportMessageDocument> BuildNakUpdate(TimeSpan delay)
        => Builders<MongoTransportMessageDocument>.Update.Pipeline(new[]
        {
            new BsonDocument("$set", new BsonDocument
            {
                ["available_at"] = new BsonDocument("$add", new BsonArray { "$$NOW", delay.TotalMilliseconds }),
                ["locked_until"] = BsonNull.Value,
                ["lock_id"] = BsonNull.Value
            })
        });

    private async ValueTask<bool> DeadLetterAsync(
        Guid id,
        Guid lockId,
        string sourceQueue,
        string payload,
        IReadOnlyDictionary<string, string> headers,
        Exception exception,
        bool deleteOriginal,
        CancellationToken cancellationToken)
    {
        if (!_options.DeadLetterEnabled)
        {
            if (deleteOriginal)
                await AckAsync(id, lockId).ConfigureAwait(false);
            return true;
        }

        var deadHeaders = new Dictionary<string, string>(headers, StringComparer.OrdinalIgnoreCase)
        {
            ["AR-DeadLetter-Reason"] = Sanitize(exception.Message),
            ["AR-DeadLetter-Source-Queue"] = sourceQueue
        };

        try
        {
            if (!deleteOriginal)
            {
                await InsertAsync(Guid.NewGuid(), _options.DeadLetterQueue, payload, deadHeaders, exception.Message, cancellationToken).ConfigureAwait(false);
                return true;
            }

            // MongoDB has no cross-document transaction we can rely on here (the transport must also
            // work on standalone servers), so the DLQ insert uses an id derived deterministically
            // from the source document: if a crash lands between the insert and the delete, the
            // redelivered message dead-letters onto the same id and the duplicate-key insert is
            // swallowed — the DLQ never accumulates copies of one poison message. Insert stays
            // FIRST for that reason: delete-first would lose the message outright in the same window.
            var deadLetterId = DeadLetterId(id);
            await InsertAsync(deadLetterId, _options.DeadLetterQueue, payload, deadHeaders, exception.Message, cancellationToken).ConfigureAwait(false);

            // ...but the burial only counts if the fenced delete matched. A stale claim (the lease
            // lapsed and a peer re-claimed the document) must no-op here exactly as the fenced ack
            // and NAK do. The DLQ copy written a moment ago is deliberately NOT compensated away:
            // the deterministic id means a peer that also reached the cap buried into the SAME
            // document, so deleting it here erased the peer's just-logged burial and the message
            // vanished from both the live queue and the DLQ. The worst a kept copy can be is a
            // spurious DLQ entry for a message whose new owner later succeeds — visible, prunable
            // by dead-letter retention, and strictly better than losing the only record. (The SQL
            // siblings avoid the dilemma by making delete+insert one atomic statement; standalone
            // MongoDB has no equivalent.)
            var removed = await _leaseMessages.DeleteOneAsync(
                Builders<MongoTransportMessageDocument>.Filter.Eq(item => item.Id, id)
                & Builders<MongoTransportMessageDocument>.Filter.Eq(item => item.LockId, lockId),
                CancellationToken.None).ConfigureAwait(false);

            if (removed.DeletedCount == 0)
            {
                SafeLog.Try(() => _logger?.LogWarning(
                    "MongoDB dead-letter for message {MessageId} from queue {SourceQueue} no-opped: the claim's lease had lapsed and the document was re-claimed. The dead-letter copy is kept in case the burial raced a peer's.",
                    id,
                    sourceQueue));
                return false;
            }

            return true;
        }
        catch (Exception ex)
        {
            // Callers decide the redelivery consequence from the false return; log the cause here so
            // a failing dead-letter write is never silent (guarded: a throwing logging provider
            // must not turn that false into an exception).
            SafeLog.Try(() => _logger?.LogError(
                ex,
                "Failed to write MongoDB dead-letter document for message {MessageId} from queue {SourceQueue}.",
                id,
                sourceQueue));
            return false;
        }
    }

    /// <summary>
    /// Watches the queue collection with a change stream and invokes
    /// <paramref name="onNotification"/> whenever a document is inserted into
    /// <paramref name="queue"/>. Runs until cancellation or a stream error; callers treat the wake
    /// as an optimization over <see cref="MongoDbSubscriberOptions.EmptyPollDelay"/> polling.
    /// </summary>
    public async Task WatchQueueAsync(string queue, Func<Task> onNotification, CancellationToken cancellationToken)
    {
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        using var cursor = await _messages.WatchAsync(
            BuildQueueWatchPipeline(queue),
            cancellationToken: cancellationToken).ConfigureAwait(false);
        while (await cursor.MoveNextAsync(cancellationToken).ConfigureAwait(false))
        {
            foreach (var change in cursor.Current)
            {
                if (IsClaimableOnArrival(change?.BackingDocument))
                    await onNotification().ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// Whether an insert event's document was claimable when it was written — whether waking the
    /// queue's subscribers for it can find anything. A DELAYED publish (a durable-flow timer, a
    /// redelay hop, a user's delayed enqueue) woke every subscriber of the queue on every process
    /// for a document none of them could claim yet; the claim poll picks it up once it is due
    /// (as it picks up a NAKed document, whose update is no insert and never woke anyone). The event's
    /// <c>clusterTime</c> is the server's write time at second resolution, hence the slack: an
    /// immediate publish stamps <c>available_at</c> with its own <c>$$NOW</c>, within that second.
    /// Read from the raw event, never through the typed full document: a foreign producer's
    /// document the class map cannot read must not tear the watch down. Anything missing or
    /// mistyped reads as claimable — an extra wake costs one empty claim, a skipped one a poll.
    /// </summary>
    internal static bool IsClaimableOnArrival(BsonDocument? change)
        => !(change is not null
             && change.TryGetValue("clusterTime", out var clusterTime)
             && clusterTime is BsonTimestamp writtenAt
             && change.TryGetValue("fullDocument", out var fullDocument)
             && fullDocument is BsonDocument document
             && document.TryGetValue("available_at", out var availableAt)
             && availableAt is BsonDateTime due
             && due.MillisecondsSinceEpoch > (writtenAt.Timestamp * 1000L) + (long)WakeSlack.TotalMilliseconds);

    /// <summary>One second, the resolution of an event's <c>clusterTime</c> (see <see cref="IsClaimableOnArrival"/>).</summary>
    private static readonly TimeSpan WakeSlack = TimeSpan.FromSeconds(1);

    /// <summary>Change-stream pipeline for queue wakes: a <c>$match</c> on inserts into one logical queue.</summary>
    internal static PipelineDefinition<ChangeStreamDocument<MongoTransportMessageDocument>, ChangeStreamDocument<MongoTransportMessageDocument>> BuildQueueWatchPipeline(string queue)
        => new EmptyPipelineDefinition<ChangeStreamDocument<MongoTransportMessageDocument>>()
            .Match(new BsonDocument("$and", new BsonArray
            {
                new BsonDocument("operationType", "insert"),
                new BsonDocument("fullDocument.queue", queue)
            }));

    /// <summary>Returns <c>true</c> when the server rejected the change stream itself (not a transient cursor error).</summary>
    internal static bool IsChangeStreamUnsupported(Exception exception)
        => exception is MongoCommandException commandException
           && (commandException.Code == 40573
               || commandException.Message.Contains("only supported on replica sets", StringComparison.OrdinalIgnoreCase));

    /// <summary>
    /// One batch of the dead-letter retention prune (<see cref="DbDeadLetterPrune"/> throttles,
    /// budgets and guards the drain): the ids of at most <see cref="OpportunisticPrune.BatchSize"/>
    /// eligible documents, then one <c>deleteMany</c> of those ids — the PostgreSQL
    /// (<c>ctid … LIMIT</c>) and SQL Server (<c>DELETE TOP</c>) shape. A single unbounded
    /// <c>deleteMany</c> was the whole drain in one statement: the first publish after
    /// <c>DeadLetterRetention</c> was enabled over a large retained backlog waited for all of it
    /// (with no socket timeout by default), and the drain's budget, checked between batches,
    /// never applied.
    /// </summary>
    private async Task<int> PruneDeadLetterBatchAsync(TimeSpan retention, CancellationToken cancellationToken)
    {
        // Same binary collation as the claim, on the lookup and the delete alike: under a folding
        // collection collation this prune matched live-queue documents whose name differed only
        // by case. The delete re-applies the eligibility filter to the ids it was handed.
        var eligible = BuildDeadLetterPruneFilter(_options.DeadLetterQueue, retention);
        var ids = await _leaseMessages.Find(eligible, new FindOptions { Collation = Collation.Simple })
            .Project(item => item.Id)
            .Limit(OpportunisticPrune.BatchSize)
            .ToListAsync(cancellationToken).ConfigureAwait(false);
        if (ids.Count == 0)
            return 0;

        var result = await _leaseMessages.DeleteManyAsync(
            Builders<MongoTransportMessageDocument>.Filter.In(item => item.Id, ids) & eligible,
            new DeleteOptions { Collation = Collation.Simple },
            cancellationToken).ConfigureAwait(false);
        return result.IsAcknowledged ? (int)result.DeletedCount : 0;
    }

    /// <summary>
    /// Prune filter: age is evaluated ENTIRELY on the server clock — <c>$$NOW</c> against the
    /// server-stamped <c>created_at</c> — mirroring the claim filter. Comparing an app-clock
    /// cutoff against another instance's stamp mixes two clocks: a pruner running behind the
    /// publisher deletes fresh dead letters on arrival, destroying the forensic record of a
    /// poison message, and one running ahead keeps them past retention.
    /// </summary>
    internal static FilterDefinition<MongoTransportMessageDocument> BuildDeadLetterPruneFilter(string deadLetterQueue, TimeSpan retention)
        => Builders<MongoTransportMessageDocument>.Filter.Eq(item => item.Queue, deadLetterQueue)
           & new BsonDocumentFilterDefinition<MongoTransportMessageDocument>(new BsonDocument(
               "$expr",
               new BsonDocument("$lt", new BsonArray
               {
                   "$created_at",
                   new BsonDocument("$subtract", new BsonArray { "$$NOW", retention.TotalMilliseconds })
               })));

    /// <summary>
    /// Deterministic dead-letter document id for a source message: the same poison message always
    /// maps to the same DLQ id, making the insert-then-delete pair idempotent under crash-redelivery.
    /// </summary>
    internal static Guid DeadLetterId(Guid sourceId)
    {
        var hash = SHA256.HashData(Encoding.UTF8.GetBytes($"asyncresponse:deadletter:{sourceId:N}"));
        return new Guid(hash.AsSpan(0, 16));
    }

    private static string Sanitize(string value) => value.Replace('\r', ' ').Replace('\n', ' ');

    private static readonly IReadOnlyDictionary<string, string> EmptyHeaders =
        new Dictionary<string, string>(0, StringComparer.OrdinalIgnoreCase);

    /// <summary>Disposes the Mongo client when the store created (and therefore owns) it.</summary>
    public void Dispose()
    {
        _ensureGate.Dispose();
        (_ownedClient as IDisposable)?.Dispose();
    }
}

/// <remarks>
/// [BsonIgnoreExtraElements] is load-bearing, not tidiness. Queue documents can be written by
/// foreign producers (the documented reply-target consumer) and by newer builds mid-rolling-deploy,
/// and the driver's default is to THROW FormatException for any element outside this class map.
/// That throw lands after findOneAndUpdate has already stamped attempts+1/lock_id and before any
/// delivery object exists, so the document could never reach HandleFailureAsync or the dead-letter
/// queue: it tore the subscriber down on every re-claim, forever, with attempts climbing unbounded.
/// </remarks>
[BsonIgnoreExtraElements]
internal sealed class MongoTransportMessageDocument
{
    [BsonId]
    [BsonElement("_id")]
    [BsonGuidRepresentation(GuidRepresentation.Standard)]
    public Guid Id { get; set; }

    [BsonElement("queue")]
    public string Queue { get; set; } = "";

    [BsonElement("payload")]
    public string Payload { get; set; } = "";

    // Array-of-documents representation keeps arbitrary header names (dots, dollars) legal as
    // values rather than as BSON field names; the lenient serializer keeps that wire shape while
    // never rejecting a foreign producer's value types.
    [BsonElement("headers")]
    [BsonSerializer(typeof(LenientTransportHeaderSerializer))]
    public Dictionary<string, string>? Headers { get; set; }

    [BsonElement("created_at")]
    public DateTime CreatedAtUtc { get; set; }

    [BsonElement("available_at")]
    public DateTime AvailableAtUtc { get; set; }

    [BsonElement("locked_until")]
    public DateTime? LockedUntilUtc { get; set; }

    [BsonElement("lock_id")]
    [BsonGuidRepresentation(GuidRepresentation.Standard)]
    public Guid? LockId { get; set; }

    [BsonElement("attempts")]
    public int Attempts { get; set; }

    [BsonElement("dead_letter_reason")]
    public string? DeadLetterReason { get; set; }
}

/// <summary>
/// Serializes headers in the driver's array-of-documents shape (<c>[{ "k": …, "v": … }, …]</c>)
/// and deserializes them without rejecting ANY BSON a foreign producer can legally store there.
/// The default dictionary serializer throws on a wrong-typed value ("Cannot deserialize a
/// 'String' from BsonType 'Int32'") — and header materialization runs inside the claim's
/// <c>findOneAndUpdate</c>, AFTER the server already stamped <c>attempts+1</c>/<c>lock_id</c> and
/// before any delivery object exists, so that throw could never reach the failure handler or
/// dead-letter: an unkillable poison document that tears down the subscriber on every re-claim.
/// Instead, string values are taken as-is, other scalars keep their canonical (culture-free)
/// string form, document/array values keep their JSON text so correlation extraction still sees a
/// usable string, nulls are skipped, and unusable shapes degrade to no headers — a genuinely
/// poison message then fails in the handler and flows through the NORMAL dead-letter path.
/// </summary>
internal sealed class LenientTransportHeaderSerializer : SerializerBase<Dictionary<string, string>?>
{
    public override Dictionary<string, string>? Deserialize(BsonDeserializationContext context, BsonDeserializationArgs args)
        => Materialize(BsonValueSerializer.Instance.Deserialize(context));

    internal static Dictionary<string, string>? Materialize(BsonValue value)
    {
        if (value is not BsonArray entries)
            return null;

        // Default (ordinal) comparer, matching the driver's own dictionary: the claim-side copy is
        // the case-folding point. Indexer writes, so duplicate keys — case-variant or exact, both
        // legal BSON — are last-wins instead of a throw.
        var headers = new Dictionary<string, string>(entries.Count);
        foreach (var entry in entries)
        {
            if (entry is not BsonDocument pair
                || !pair.TryGetValue("k", out var key)
                || !pair.TryGetValue("v", out var rawValue))
            {
                continue;
            }

            var name = Coerce(key);
            var text = Coerce(rawValue);
            if (name is not null && text is not null)
                headers[name] = text;
        }

        return headers;
    }

    private static string? Coerce(BsonValue value) => value.BsonType switch
    {
        BsonType.String => value.AsString,
        BsonType.Null or BsonType.Undefined => null,
        BsonType.Document or BsonType.Array => value.ToJson(),
        // The scalar ToString overrides are JSON-flavored and culture-free ("1.5", "123", "true",
        // ISO-8601 dates — raw epoch millis when out of DateTime range), and none of them throws.
        _ => value.ToString() ?? ""
    };

    public override void Serialize(BsonSerializationContext context, BsonSerializationArgs args, Dictionary<string, string>? value)
    {
        var writer = context.Writer;
        if (value is null)
        {
            writer.WriteNull();
            return;
        }

        writer.WriteStartArray();
        foreach (var pair in value)
        {
            writer.WriteStartDocument();
            writer.WriteName("k");
            writer.WriteString(pair.Key);
            writer.WriteName("v");
            writer.WriteString(pair.Value);
            writer.WriteEndDocument();
        }

        writer.WriteEndArray();
    }
}
