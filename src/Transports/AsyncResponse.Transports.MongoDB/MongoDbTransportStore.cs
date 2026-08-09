using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Bson.Serialization.Options;
using MongoDB.Driver;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;

namespace AsyncResponse.Transports.MongoDB;

internal enum MongoDbSubscriberRole
{
    Worker,
    ResponseIngress
}

/// <summary>A claimed MongoDB transport document, decoupled from driver types for dispatch tests.</summary>
/// <remarks>
/// <c>RenewAsync</c> extends the claim's lease (<c>locked_until</c>) by the original lock timeout,
/// fenced on the claim's <c>lock_id</c>; it returns <c>false</c> when the fence no longer matches
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
    Func<ValueTask<bool>> RenewAsync);

/// <summary>Small document adapter for the MongoDB transport queue collection.</summary>
internal sealed class MongoDbTransportStore : IDisposable
{
    private readonly IMongoCollection<MongoTransportMessageDocument> _messages;
    private readonly MongoDbAsyncResponseTransportOptions _options;
    private readonly ILogger<MongoDbTransportStore>? _logger;
    private readonly SemaphoreSlim _ensureGate = new(1, 1);
    private readonly IMongoClient? _ownedClient;
    private bool _created;
    private long _lastDeadLetterPruneTicks;

    public MongoDbTransportStore(
        IMongoDatabase database,
        IOptions<MongoDbAsyncResponseTransportOptions> options,
        IMongoClient? ownedClient = null,
        ILogger<MongoDbTransportStore>? logger = null)
    {
        _options = options.Value;
        _logger = logger;
        MongoDbTransportOptionsValidator.ValidateCommon(_options);

        // The namespace BYTE limit can only be checked here, where the actual database name is
        // first known; a near-limit configuration otherwise passes every static check and fails
        // at the first server operation.
        var ns = $"{database.DatabaseNamespace.DatabaseName}.{_options.MessageCollection}";
        var byteLength = Encoding.UTF8.GetByteCount(ns);
        if (byteLength > 255)
            throw new InvalidOperationException(
                $"The MongoDB namespace '{ns}' ({nameof(_options.MessageCollection)}) is {byteLength} UTF-8 bytes; MongoDB limits namespaces " +
                "(database + '.' + collection) to 255 bytes, and sharded collections to 235. Shorten the database or collection name.");

        _messages = database.GetCollection<MongoTransportMessageDocument>(_options.MessageCollection);
        _ownedClient = ownedClient;
    }

    public async Task EnsureCreatedAsync(CancellationToken cancellationToken = default)
    {
        if (_created || !_options.AutoCreateIndexes)
            return;

        await _ensureGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_created)
                return;

            await _messages.Indexes.CreateOneAsync(
                new CreateIndexModel<MongoTransportMessageDocument>(
                    Builders<MongoTransportMessageDocument>.IndexKeys
                        .Ascending(item => item.Queue)
                        .Ascending(item => item.AvailableAtUtc)
                        .Ascending(item => item.CreatedAtUtc),
                    new CreateIndexOptions { Name = $"{_options.MessageCollection}_claim_idx" }),
                cancellationToken: cancellationToken).ConfigureAwait(false);
            await _messages.Indexes.CreateOneAsync(
                new CreateIndexModel<MongoTransportMessageDocument>(
                    Builders<MongoTransportMessageDocument>.IndexKeys.Ascending(item => item.CreatedAtUtc),
                    new CreateIndexOptions { Name = $"{_options.MessageCollection}_created_idx" }),
                cancellationToken: cancellationToken).ConfigureAwait(false);
            _created = true;
        }
        finally
        {
            _ensureGate.Release();
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
        CancellationToken cancellationToken)
    {
        await InsertAsync(id, queue, payload, headers, deadLetterReason: null, cancellationToken).ConfigureAwait(false);
        await PruneDeadLettersIfDueAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task<MongoDbTransportDelivery?> TryClaimAsync(string queue, TimeSpan lockTimeout, CancellationToken cancellationToken)
    {
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        var lockId = Guid.NewGuid();

        // findOneAndUpdate is atomic per document: of all competing consumers, exactly one observes
        // the document unlocked and stamps its lock_id/locked_until in the same server-side step.
        var claimed = await _messages.FindOneAndUpdateAsync(
            BuildClaimFilter(queue),
            BuildClaimUpdate(lockId, lockTimeout),
            new FindOneAndUpdateOptions<MongoTransportMessageDocument>
            {
                Sort = Builders<MongoTransportMessageDocument>.Sort.Ascending(item => item.CreatedAtUtc),
                ReturnDocument = ReturnDocument.After
            },
            cancellationToken).ConfigureAwait(false);
        if (claimed is null)
            return null;

        var headers = claimed.Headers is null
            ? EmptyHeaders
            : new Dictionary<string, string>(claimed.Headers, StringComparer.OrdinalIgnoreCase);

        return new MongoDbTransportDelivery(
            claimed.Id,
            claimed.Queue,
            claimed.Payload,
            headers,
            claimed.Attempts,
            () => AckAsync(claimed.Id, lockId),
            delay => NakAsync(claimed.Id, lockId, delay),
            (exception, deleteOriginal, token) => DeadLetterAsync(claimed.Id, lockId, queue, claimed.Payload, headers, exception, deleteOriginal, token),
            () => RenewLeaseAsync(claimed.Id, lockId, lockTimeout));
    }

    /// <summary>
    /// Claim filter: available and not (still) locked, evaluated against the server clock
    /// (<c>$$NOW</c>) so publisher/consumer clock skew never fences messages in or out.
    /// A missing <c>locked_until</c> compares as null and therefore as expired.
    /// </summary>
    internal static FilterDefinition<MongoTransportMessageDocument> BuildClaimFilter(string queue)
        => Builders<MongoTransportMessageDocument>.Filter.Eq(item => item.Queue, queue)
           & new BsonDocumentFilterDefinition<MongoTransportMessageDocument>(new BsonDocument(
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

    internal static UpdateDefinition<MongoTransportMessageDocument> BuildClaimUpdate(Guid lockId, TimeSpan lockTimeout)
        => Builders<MongoTransportMessageDocument>.Update.Pipeline(new[]
        {
            new BsonDocument("$set", new BsonDocument
            {
                ["attempts"] = new BsonDocument("$add", new BsonArray
                {
                    new BsonDocument("$ifNull", new BsonArray { "$attempts", 0 }),
                    1
                }),
                ["locked_until"] = new BsonDocument("$add", new BsonArray { "$$NOW", lockTimeout.TotalMilliseconds }),
                ["lock_id"] = new BsonBinaryData(lockId, GuidRepresentation.Standard)
            })
        });

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
        CancellationToken cancellationToken)
    {
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        var now = DateTime.UtcNow;
        var document = new MongoTransportMessageDocument
        {
            Id = id,
            Queue = queue,
            Payload = payload,
            Headers = headers is null
                ? new Dictionary<string, string>(0, StringComparer.OrdinalIgnoreCase)
                : new Dictionary<string, string>(headers, StringComparer.OrdinalIgnoreCase),
            CreatedAtUtc = now,
            // "Available immediately on arrival": InsertOne cannot evaluate $$NOW, and stamping the
            // client clock here would let client-ahead-of-server skew hide a fresh message from the
            // server-clock claim filter until the skew elapsed. Epoch expresses what the SQL stores'
            // "available_at DEFAULT now()" expresses; a NAK re-stamps a real server-relative time.
            AvailableAtUtc = DateTime.UnixEpoch,
            Attempts = 0,
            DeadLetterReason = deadLetterReason
        };
        try
        {
            await _messages.InsertOneAsync(document, cancellationToken: cancellationToken).ConfigureAwait(false);
        }
        catch (MongoWriteException ex) when (ex.WriteError?.Category == ServerErrorCategory.DuplicateKey)
        {
            // A retried publish found the document already inserted: idempotent success.
        }
    }

    private async ValueTask AckAsync(Guid id, Guid lockId)
    {
        await _messages.DeleteOneAsync(
            Builders<MongoTransportMessageDocument>.Filter.Eq(item => item.Id, id)
            & Builders<MongoTransportMessageDocument>.Filter.Eq(item => item.LockId, lockId),
            CancellationToken.None).ConfigureAwait(false);
    }

    private async ValueTask NakAsync(Guid id, Guid lockId, TimeSpan delay)
    {
        await _messages.UpdateOneAsync(
            Builders<MongoTransportMessageDocument>.Filter.Eq(item => item.Id, id)
            & Builders<MongoTransportMessageDocument>.Filter.Eq(item => item.LockId, lockId),
            BuildNakUpdate(delay),
            options: null,
            CancellationToken.None).ConfigureAwait(false);
    }

    private async ValueTask<bool> RenewLeaseAsync(Guid id, Guid lockId, TimeSpan lockTimeout)
    {
        var result = await _messages.UpdateOneAsync(
            Builders<MongoTransportMessageDocument>.Filter.Eq(item => item.Id, id)
            & Builders<MongoTransportMessageDocument>.Filter.Eq(item => item.LockId, lockId),
            BuildRenewUpdate(lockTimeout),
            options: null,
            CancellationToken.None).ConfigureAwait(false);
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
            // swallowed — the DLQ never accumulates copies of one poison message.
            await InsertAsync(DeadLetterId(id), _options.DeadLetterQueue, payload, deadHeaders, exception.Message, cancellationToken).ConfigureAwait(false);
            await AckAsync(id, lockId).ConfigureAwait(false);
            return true;
        }
        catch (Exception ex)
        {
            // Callers decide the redelivery consequence from the false return; log the cause here so
            // a failing dead-letter write is never silent.
            _logger?.LogError(
                ex,
                "Failed to write MongoDB dead-letter document for message {MessageId} from queue {SourceQueue}.",
                id,
                sourceQueue);
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
            foreach (var _ in cursor.Current)
                await onNotification().ConfigureAwait(false);
        }
    }

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
    /// Opportunistically deletes dead-letter documents older than the configured retention. No-op
    /// unless <see cref="MongoDbAsyncResponseTransportOptions.DeadLetterRetention"/> is set, and
    /// throttled so the delete runs at most once per minute regardless of publish rate.
    /// </summary>
    private async Task PruneDeadLettersIfDueAsync(CancellationToken cancellationToken)
    {
        if (_options.DeadLetterRetention is not { } retention || !ShouldPruneDeadLetters())
            return;

        await _messages.DeleteManyAsync(
            Builders<MongoTransportMessageDocument>.Filter.Eq(item => item.Queue, _options.DeadLetterQueue)
            & Builders<MongoTransportMessageDocument>.Filter.Lt(item => item.CreatedAtUtc, DateTime.UtcNow.Subtract(retention)),
            cancellationToken).ConfigureAwait(false);
    }

    private bool ShouldPruneDeadLetters()
    {
        var now = DateTime.UtcNow.Ticks;
        var last = Interlocked.Read(ref _lastDeadLetterPruneTicks);
        return now - last >= DeadLetterPruneThrottle.Ticks
            && Interlocked.CompareExchange(ref _lastDeadLetterPruneTicks, now, last) == last;
    }

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

    private static readonly TimeSpan DeadLetterPruneThrottle = TimeSpan.FromMinutes(1);

    private static readonly IReadOnlyDictionary<string, string> EmptyHeaders =
        new Dictionary<string, string>(0, StringComparer.OrdinalIgnoreCase);

    /// <summary>Disposes the Mongo client when the store created (and therefore owns) it.</summary>
    public void Dispose()
    {
        _ensureGate.Dispose();
        (_ownedClient as IDisposable)?.Dispose();
    }
}

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

    // Array-of-documents representation keeps arbitrary header names (dots, dollars) legal as values
    // rather than as BSON field names.
    [BsonElement("headers")]
    [BsonDictionaryOptions(DictionaryRepresentation.ArrayOfDocuments)]
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
