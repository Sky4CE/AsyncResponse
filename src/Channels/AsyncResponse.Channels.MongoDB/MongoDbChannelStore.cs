using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;
using System.Runtime.CompilerServices;

namespace AsyncResponse.Channels.MongoDB;

internal readonly record struct MongoDbChannelMessage(
    Guid Id,
    string CorrelationId,
    string EnvelopeJson,
    DateTimeOffset CreatedAtUtc);

/// <summary>Document adapter for the MongoDB channel collections and change-stream wake.</summary>
internal sealed class MongoDbChannelStore : IDisposable
{
    private readonly IMongoCollection<MongoRecoveryStateDocument> _recovery;
    private readonly IMongoCollection<MongoChannelMessageDocument> _messages;
    private readonly IMongoCollection<MongoChannelSubscriberDocument> _subscribers;
    private readonly IMongoDatabase _database;
    private readonly MongoDbAsyncResponseChannelOptions _options;
    private readonly SemaphoreSlim _ensureGate = new(1, 1);
    private readonly IMongoClient? _ownedClient;
    private bool _created;

    public MongoDbChannelStore(
        IMongoDatabase database,
        IOptions<MongoDbAsyncResponseChannelOptions> options,
        IMongoClient? ownedClient = null)
    {
        _options = options.Value;
        _options.Validate();
        _database = database;
        _recovery = database.GetCollection<MongoRecoveryStateDocument>(_options.RecoveryStateCollection);
        _messages = database.GetCollection<MongoChannelMessageDocument>(_options.MessageCollection);
        _subscribers = database.GetCollection<MongoChannelSubscriberDocument>(_options.SubscriberCollection);
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

            // TTL indexes (expireAfterSeconds = 0 on the expiry timestamp) make MongoDB itself reap
            // expired documents — no application-side pruning needed. Reads still filter on the
            // expiry because the TTL monitor only runs periodically (~60s).
            await CreateTtlIndexAsync(
                _recovery,
                Builders<MongoRecoveryStateDocument>.IndexKeys.Ascending(item => item.ExpiresAtUtc),
                $"{_options.RecoveryStateCollection}_expires_idx",
                cancellationToken).ConfigureAwait(false);
            await _recovery.Indexes.CreateOneAsync(
                new CreateIndexModel<MongoRecoveryStateDocument>(
                    Builders<MongoRecoveryStateDocument>.IndexKeys
                        .Ascending(item => item.CorrelationId)
                        .Ascending(item => item.RegisteredAtUtc),
                    new CreateIndexOptions { Name = $"{_options.RecoveryStateCollection}_correlation_idx" }),
                cancellationToken: cancellationToken).ConfigureAwait(false);

            await CreateTtlIndexAsync(
                _messages,
                Builders<MongoChannelMessageDocument>.IndexKeys.Ascending(item => item.ExpiresAtUtc),
                $"{_options.MessageCollection}_expires_idx",
                cancellationToken).ConfigureAwait(false);
            await _messages.Indexes.CreateOneAsync(
                new CreateIndexModel<MongoChannelMessageDocument>(
                    Builders<MongoChannelMessageDocument>.IndexKeys
                        .Ascending(item => item.CorrelationId)
                        .Ascending(item => item.CreatedAtUtc),
                    new CreateIndexOptions { Name = $"{_options.MessageCollection}_correlation_created_idx" }),
                cancellationToken: cancellationToken).ConfigureAwait(false);

            await CreateTtlIndexAsync(
                _subscribers,
                Builders<MongoChannelSubscriberDocument>.IndexKeys.Ascending(item => item.ExpiresAtUtc),
                $"{_options.SubscriberCollection}_expires_idx",
                cancellationToken).ConfigureAwait(false);
            await _subscribers.Indexes.CreateOneAsync(
                new CreateIndexModel<MongoChannelSubscriberDocument>(
                    Builders<MongoChannelSubscriberDocument>.IndexKeys.Ascending(item => item.CorrelationId),
                    new CreateIndexOptions { Name = $"{_options.SubscriberCollection}_correlation_idx" }),
                cancellationToken: cancellationToken).ConfigureAwait(false);

            _created = true;
        }
        finally
        {
            _ensureGate.Release();
        }
    }

    private static async Task CreateTtlIndexAsync<TDocument>(
        IMongoCollection<TDocument> collection,
        IndexKeysDefinition<TDocument> keys,
        string indexName,
        CancellationToken cancellationToken)
    {
        var model = new CreateIndexModel<TDocument>(
            keys,
            new CreateIndexOptions { Name = indexName, ExpireAfter = TimeSpan.Zero });
        try
        {
            await collection.Indexes.CreateOneAsync(model, cancellationToken: cancellationToken).ConfigureAwait(false);
        }
        catch (MongoCommandException ex) when (ex.Code is 85 or 86)
        {
            // IndexOptionsConflict/IndexKeySpecsConflict: an earlier deployment created the
            // same-named index with different options. Replace it in place.
            await collection.Indexes.DropOneAsync(indexName, cancellationToken).ConfigureAwait(false);
            await collection.Indexes.CreateOneAsync(model, cancellationToken: cancellationToken).ConfigureAwait(false);
        }
    }

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
                ["correlation_id"] = correlationId,
                ["registration_id"] = new BsonBinaryData(state.RegistrationId, GuidRepresentation.Standard),
                ["state_json"] = AsyncResponseJson.Serialize(state),
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
    public Task InsertMessageAsync(Guid id, string correlationId, string envelopeJson, TimeSpan retention, CancellationToken cancellationToken)
        => AsyncResponseRetry.ExecuteAsync(
            token => InsertMessageOnceAsync(id, correlationId, envelopeJson, retention, token),
            IsTransient,
            _options.PublishMaxAttempts,
            _options.PublishRetryBaseDelay,
            _options.PublishRetryMaxDelay,
            cancellationToken);

    private async Task<bool> InsertMessageOnceAsync(Guid id, string correlationId, string envelopeJson, TimeSpan retention, CancellationToken cancellationToken)
    {
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        await _messages.UpdateOneAsync(
            Builders<MongoChannelMessageDocument>.Filter.Eq(item => item.Id, id),
            BuildInsertMessagePipeline(correlationId, envelopeJson, retention),
            new UpdateOptions { IsUpsert = true },
            cancellationToken).ConfigureAwait(false);
        return true;
    }

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
                ["correlation_id"] = correlationId,
                ["envelope_json"] = envelopeJson,
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
                     & Builders<MongoChannelMessageDocument>.Filter.Gt(item => item.ExpiresAtUtc, DateTime.UtcNow);
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
            .Sort(Builders<MongoChannelMessageDocument>.Sort
                .Ascending(item => item.CreatedAtUtc)
                .Ascending(item => item.Id))
            .Limit(batchSize)
            .ToListAsync(cancellationToken).ConfigureAwait(false);
        var messages = new List<MongoDbChannelMessage>(documents.Count);
        foreach (var document in documents)
            messages.Add(new MongoDbChannelMessage(
                document.Id,
                document.CorrelationId,
                document.EnvelopeJson,
                new DateTimeOffset(document.CreatedAtUtc, TimeSpan.Zero)));
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
        var claimed = await _messages.FindOneAndUpdateAsync(
            BuildDeliveryClaimFilter(messageId),
            BuildDeliveryClaimUpdate(),
            new FindOneAndUpdateOptions<MongoChannelMessageDocument> { ReturnDocument = ReturnDocument.After },
            cancellationToken).ConfigureAwait(false);
        return claimed is not null;
    }

    internal static FilterDefinition<MongoChannelMessageDocument> BuildDeliveryClaimFilter(Guid messageId)
        => Builders<MongoChannelMessageDocument>.Filter.Eq(item => item.Id, messageId)
           & Builders<MongoChannelMessageDocument>.Filter.Eq(item => item.RecoveryClaimed, false)
           & Builders<MongoChannelMessageDocument>.Filter.Gt(item => item.ExpiresAtUtc, DateTime.UtcNow);

    internal static UpdateDefinition<MongoChannelMessageDocument> BuildDeliveryClaimUpdate()
        => Builders<MongoChannelMessageDocument>.Update.Pipeline(new[]
        {
            new BsonDocument("$set", new BsonDocument(
                "acked_at",
                new BsonDocument("$ifNull", new BsonArray { "$acked_at", "$$NOW" })))
        });

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
        var claimed = await _messages.FindOneAndUpdateAsync(
            BuildRecoveryClaimFilter(messageId),
            Builders<MongoChannelMessageDocument>.Update.Set(item => item.RecoveryClaimed, true),
            new FindOneAndUpdateOptions<MongoChannelMessageDocument> { ReturnDocument = ReturnDocument.After },
            cancellationToken).ConfigureAwait(false);
        return claimed is not null;
    }

    internal static FilterDefinition<MongoChannelMessageDocument> BuildRecoveryClaimFilter(Guid messageId)
        => Builders<MongoChannelMessageDocument>.Filter.Eq(item => item.Id, messageId)
           & Builders<MongoChannelMessageDocument>.Filter.Eq(item => item.AckedAtUtc, null);

    /// <summary>Returns the server's current UTC time, used as a clock-safe delivery watermark.</summary>
    public async Task<DateTimeOffset> GetServerTimeUtcAsync(CancellationToken cancellationToken)
    {
        var reply = await _database.RunCommandAsync<BsonDocument>(
            new BsonDocument("hello", 1),
            cancellationToken: cancellationToken).ConfigureAwait(false);
        return reply.TryGetValue("localTime", out var localTime) && localTime is BsonDateTime serverTime
            ? new DateTimeOffset(serverTime.ToUniversalTime(), TimeSpan.Zero)
            : DateTimeOffset.UtcNow;
    }

    public async Task<bool> IsMessageAcknowledgedAsync(Guid messageId, CancellationToken cancellationToken)
    {
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        var filter = Builders<MongoChannelMessageDocument>.Filter.Eq(item => item.Id, messageId)
                     & Builders<MongoChannelMessageDocument>.Filter.Gt(item => item.ExpiresAtUtc, DateTime.UtcNow);
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
                ["correlation_id"] = correlationId,
                ["registration_id"] = new BsonBinaryData(registrationId, GuidRepresentation.Standard),
                ["instance_id"] = instanceId,
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
        return await _subscribers.CountDocumentsAsync(filter, cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Server-clock expiry filter (<c>$expr: expires_at &gt; $$NOW</c>): liveness and recovery
    /// expiry are stamped with $$NOW, so comparing them against the app clock would reintroduce
    /// the clock-skew hole the server-side stamps exist to close.
    /// </summary>
    internal static FilterDefinition<TDocument> NotExpiredOnServerClock<TDocument>()
        => new BsonDocument("$expr", new BsonDocument("$gt", new BsonArray { "$expires_at", "$$NOW" }));

    /// <summary>
    /// Watches the message collection with a change stream and invokes
    /// <paramref name="onNotification"/> with the correlation id of every inserted response. One
    /// stream serves every local waiter — the caller routes the id to the right subscription — so
    /// waiter count never multiplies server-side cursors. Runs until cancellation or a stream error.
    /// </summary>
    public async Task WatchMessagesAsync(Func<string?, Task> onNotification, CancellationToken cancellationToken)
    {
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        using var cursor = await _messages.WatchAsync(
            BuildMessageWatchPipeline(),
            new ChangeStreamOptions { FullDocument = ChangeStreamFullDocumentOption.UpdateLookup },
            cancellationToken).ConfigureAwait(false);
        while (await cursor.MoveNextAsync(cancellationToken).ConfigureAwait(false))
        {
            foreach (var change in cursor.Current)
                await onNotification(change.FullDocument?.CorrelationId).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Change-stream pipeline for response wakes: a <c>$match</c> on insert events. The correlation
    /// id travels in the event's full document, letting the dispatcher scan only the signaled
    /// correlation id — the targeted-wake contract.
    /// </summary>
    internal static PipelineDefinition<ChangeStreamDocument<MongoChannelMessageDocument>, ChangeStreamDocument<MongoChannelMessageDocument>> BuildMessageWatchPipeline()
        => new EmptyPipelineDefinition<ChangeStreamDocument<MongoChannelMessageDocument>>()
            .Match(change => change.OperationType == ChangeStreamOperationType.Insert);

    /// <summary>Returns <c>true</c> when the server rejected the change stream itself (not a transient cursor error).</summary>
    internal static bool IsChangeStreamUnsupported(Exception exception)
        => exception is MongoCommandException commandException
           && (commandException.Code == 40573
               || commandException.Message.Contains("only supported on replica sets", StringComparison.OrdinalIgnoreCase));

    internal static string RegistrationKey(string correlationId, Guid registrationId)
        => $"{correlationId}:{registrationId:N}";

    internal static bool IsTransient(Exception exception)
        => exception is not OperationCanceledException
           && (exception is MongoConnectionException
               or MongoNotPrimaryException
               or MongoNodeIsRecoveringException
               or MongoExecutionTimeoutException
               or TimeoutException
               || (exception is MongoException mongoException && mongoException.HasErrorLabel("RetryableWriteError")));

    /// <summary>Validates a MongoDB collection name coming from options.</summary>
    public static void ValidateCollectionName(string? value, string name)
    {
        if (string.IsNullOrWhiteSpace(value))
            throw new InvalidOperationException($"{nameof(MongoDbAsyncResponseChannelOptions)}.{name} must be configured.");
        if (value.Contains('$') || value.Contains('\0') || value.StartsWith("system.", StringComparison.Ordinal))
            throw new InvalidOperationException(
                $"{nameof(MongoDbAsyncResponseChannelOptions)}.{name} '{value}' must be a valid MongoDB collection name (no '$' or NUL characters, not in the system namespace).");
    }

    /// <summary>Disposes the Mongo client when the store created (and therefore owns) it.</summary>
    public void Dispose()
    {
        _ensureGate.Dispose();
        (_ownedClient as IDisposable)?.Dispose();
    }
}

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

internal sealed class MongoChannelMessageDocument
{
    [BsonId]
    [BsonElement("_id")]
    [BsonGuidRepresentation(GuidRepresentation.Standard)]
    public Guid Id { get; set; }

    [BsonElement("correlation_id")]
    public string CorrelationId { get; set; } = "";

    [BsonElement("envelope_json")]
    public string EnvelopeJson { get; set; } = "";

    [BsonElement("created_at")]
    public DateTime CreatedAtUtc { get; set; }

    [BsonElement("expires_at")]
    public DateTime ExpiresAtUtc { get; set; }

    [BsonElement("acked_at")]
    public DateTime? AckedAtUtc { get; set; }

    [BsonElement("recovery_claimed")]
    public bool RecoveryClaimed { get; set; }
}

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
