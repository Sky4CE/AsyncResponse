using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;
using NpgsqlTypes;
using System.Runtime.CompilerServices;
using System.Text;

using AsyncResponse.Internal;

namespace AsyncResponse.Transports.PostgreSQL;

internal enum PostgreSqlSubscriberRole
{
    Worker,
    ResponseIngress
}

/// <summary>A claimed PostgreSQL transport row, decoupled from Npgsql types for dispatch tests.</summary>
/// <remarks>
/// <c>RenewAsync</c> extends the claim's lease (<c>locked_until</c>) by the original lock timeout,
/// fenced on the claim's <c>lock_id</c>; it returns <c>false</c> when the fence no longer matches
/// (the lease lapsed and another subscriber re-claimed the row).
/// </remarks>
internal sealed record PostgreSqlTransportDelivery(
    Guid Id,
    string Queue,
    string Payload,
    IReadOnlyDictionary<string, string> Headers,
    int Attempt,
    Func<ValueTask> AckAsync,
    Func<TimeSpan, ValueTask> NakAsync,
    Func<Exception, bool, CancellationToken, ValueTask<bool>> DeadLetterAsync,
    Func<ValueTask<bool>> RenewAsync);

/// <summary>Small SQL adapter for the PostgreSQL transport queue table.</summary>
internal sealed class PostgreSqlTransportStore
{
    private readonly NpgsqlDataSource _dataSource;
    private readonly PostgreSqlAsyncResponseTransportOptions _options;
    private readonly ILogger<PostgreSqlTransportStore>? _logger;
    private readonly SemaphoreSlim _ensureGate = new(1, 1);
    private bool _created;
    private readonly long _schemaLockKey;
    private long _lastDeadLetterPruneTicks;

    public PostgreSqlTransportStore(
        NpgsqlDataSource dataSource,
        IOptions<PostgreSqlAsyncResponseTransportOptions> options,
        ILogger<PostgreSqlTransportStore>? logger = null)
    {
        _dataSource = dataSource;
        _options = options.Value;
        _logger = logger;
        PostgreSqlTransportOptionsValidator.ValidateCommon(_options);
        Schema = Quote(_options.SchemaName);
        MessageTable = $"{Schema}.{Quote(_options.MessageTable)}";
        _schemaLockKey = SchemaAdvisoryLockKey(_options.SchemaName);
    }

    public string Schema { get; }
    public string MessageTable { get; }

    public async Task EnsureCreatedAsync(CancellationToken cancellationToken = default)
    {
        if (_created)
            return;

        await _ensureGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_created)
                return;

            await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
            await using var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);

            if (_options.AutoCreateSchema)
            {
                // Serialize schema creation across processes. CREATE ... IF NOT EXISTS is not atomic against a
                // concurrent create of the same object: two instances starting together both pass the existence
                // check and collide on the system catalog ("duplicate key ... pg_type_typname_nsp_index"). A
                // transaction-scoped advisory lock (keyed by schema, shared with the channel store) lets one
                // instance build the schema while the rest wait and then find it already present.
                await using (var lockCommand = connection.CreateCommand())
                {
                    lockCommand.Transaction = transaction;
                    lockCommand.CommandText = "SELECT pg_advisory_xact_lock(@lock_key);";
                    lockCommand.Parameters.AddWithValue("lock_key", _schemaLockKey);
                    await lockCommand.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                }

                // The dequeue index is (queue, available_at, created_at) and the claim orders by
                // exactly that tail, so a claim is one ordered index descent that stops at the
                // first unleased row. The previous pair — an index over (queue, available_at,
                // locked_until, created_at) behind ORDER BY created_at — could not serve its own
                // ordering past the available_at range: the planner either walked the created_at
                // index through every older row of the OTHER logical queues (dead letters kept for
                // retention, delayed jobs) or sorted the whole ready set, on every claim, so
                // draining a burst cost its square. A table created by an older build keeps its
                // "<table>_claim_idx"; nothing reads it any more and nothing here drops it (DROP
                // INDEX needs an ACCESS EXCLUSIVE lock on a live queue) — see docs/postgresql.md.
                await using var command = connection.CreateCommand();
                command.Transaction = transaction;
                command.CommandText =
                    $"""
                    CREATE SCHEMA IF NOT EXISTS {Schema};

                    CREATE TABLE IF NOT EXISTS {MessageTable} (
                        id uuid PRIMARY KEY,
                        queue text NOT NULL,
                        payload_json jsonb NOT NULL,
                        headers_json jsonb NOT NULL DEFAULT jsonb_build_object(),
                        created_at timestamptz NOT NULL DEFAULT now(),
                        available_at timestamptz NOT NULL DEFAULT now(),
                        locked_until timestamptz NULL,
                        lock_id uuid NULL,
                        attempts integer NOT NULL DEFAULT 0,
                        dead_letter_reason text NULL
                    );
                    CREATE INDEX IF NOT EXISTS {Quote(IndexName(_options.MessageTable, "ready"))}
                        ON {MessageTable} (queue, available_at, created_at);
                    CREATE INDEX IF NOT EXISTS {Quote(IndexName(_options.MessageTable, "created"))}
                        ON {MessageTable} (created_at);
                    """;
                try
                {
                    await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                }
                catch (PostgresException ex) when (ex.SqlState is PostgresErrorCodes.WrongObjectType or PostgresErrorCodes.UndefinedColumn)
                {
                    // E.g. CREATE INDEX ... ON a name that is really another component's index:
                    // IF NOT EXISTS skipped the table create, and the dependent statement then hits
                    // the wrong relation kind mid-batch — surface the namespace collision instead of
                    // the raw "cannot open relation".
                    throw new InvalidOperationException(PostgreSqlRelationVerifier.DdlCollisionMessage("transport", _options.SchemaName), ex);
                }
            }
            else if (!await RelationExistsAsync(connection, transaction, _options.MessageTable, cancellationToken).ConfigureAwait(false))
            {
                // Operator-managed schema and the migration has not run yet: the first query
                // surfaces a clear PostgreSQL error (the documented "create it yourself, later"
                // workflow), and _created stays unlatched so a later operation re-verifies once
                // the migration lands. When the relation DOES exist it flows into the same catalog
                // verification the DDL path uses — operator-provisioned schemas are exactly what
                // that check exists for.
                return;
            }

            // The transport can share a schema with the channel and durable-flow stores (and
            // unrelated objects), whose derived names its own validation cannot see — and
            // IF NOT EXISTS also accepts a same-name index with the WRONG definition, exactly as
            // an operator-provisioned table can carry the wrong shape. Verify against the catalog
            // that every relation actually IS what this store reads and writes, definitions
            // included (in-transaction, under the shared DDL lock when this build just ran the DDL).
            //
            // The dequeue index is REQUIRED only where this build's DDL just guaranteed it. On an
            // operator-managed schema it is verified when present and only warned about when
            // absent: it is claim performance, not correctness, and a migration written for an
            // older build (which carried "<table>_claim_idx" instead) must not fail startup over it.
            var readyIndex = IndexName(_options.MessageTable, "ready");
            var verifyReadyIndex = _options.AutoCreateSchema
                || await RelationExistsAsync(connection, transaction, readyIndex, cancellationToken).ConfigureAwait(false);
            if (!verifyReadyIndex)
            {
                _logger?.LogWarning(
                    "PostgreSQL transport table {Schema}.{Table} has no dequeue index {Index} and AutoCreateSchema is disabled. " +
                    "Claims still work, but their cost grows with the backlog — performance only; create the index over " +
                    "(queue, available_at, created_at) as described in docs/postgresql.md.",
                    _options.SchemaName,
                    _options.MessageTable,
                    readyIndex);
            }

            await PostgreSqlRelationVerifier.VerifyAsync(
                connection,
                transaction,
                _options.SchemaName,
                "transport",
                [
                    new(_options.MessageTable, 'r', Columns:
                        [
                            new("id", "uuid", Nullable: false),
                            new("queue", "text", Nullable: false, RequiresDeterministicCollation: true),
                            new("payload_json", "jsonb", Nullable: false),
                            new("headers_json", "jsonb", Nullable: false, DefaultExpression: "jsonb_build_object()"),
                            new("created_at", "timestamp with time zone", Nullable: false, DefaultExpression: "now()"),
                            new("available_at", "timestamp with time zone", Nullable: false, DefaultExpression: "now()"),
                            new("locked_until", "timestamp with time zone", Nullable: true),
                            new("lock_id", "uuid", Nullable: true),
                            new("attempts", "integer", Nullable: false, DefaultExpression: "0"),
                            new("dead_letter_reason", "text", Nullable: true),
                        ], PrimaryKey: ["id"]),
                    .. verifyReadyIndex
                        ? (PostgreSqlRelationVerifier.ExpectedRelation[])
                            [new(readyIndex, 'i', _options.MessageTable, ["queue", "available_at", "created_at"])]
                        : [],
                    new(IndexName(_options.MessageTable, "created"), 'i', _options.MessageTable, ["created_at"]),
                ],
                cancellationToken).ConfigureAwait(false);

            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            _created = true;
        }
        finally
        {
            _ensureGate.Release();
        }
    }

    /// <summary>
    /// Reports whether ANY relation occupies the given name in the configured schema (any relkind:
    /// a view or foreign component's object must reach verification, which names the precise
    /// wrong-kind reason instead of skipping the checks).
    /// </summary>
    private async Task<bool> RelationExistsAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, string relation, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText =
            """
            SELECT EXISTS (
                SELECT 1
                FROM pg_catalog.pg_class c
                JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = @schema AND c.relname = @table);
            """;
        command.Parameters.AddWithValue("schema", _options.SchemaName);
        command.Parameters.AddWithValue("table", relation);
        return (bool)(await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false))!;
    }

    /// <summary>
    /// Publishes a queue row. The caller supplies the id so a retried publish is idempotent
    /// (<c>ON CONFLICT DO NOTHING</c>) rather than inserting a duplicate job.
    /// </summary>
    public async Task PublishAsync(
        Guid id,
        string queue,
        string payload,
        IReadOnlyDictionary<string, string>? headers,
        CancellationToken cancellationToken,
        TimeSpan? delay = null)
    {
        await InsertAsync(id, queue, payload, headers, deadLetterReason: null, notify: true, cancellationToken, delay).ConfigureAwait(false);

        // The row is committed: nothing after this line may fail the publish. A prune that threw
        // (a lock timeout, a dropped connection, the caller's token firing mid-DELETE) reported a
        // FAILED publish for a job that is already claimable, and the caller's retry inserts it
        // again once a subscriber has consumed and deleted the first copy — one job, run twice.
        // The prune is opportunistic housekeeping; the next throttle window retries it.
        try
        {
            await PruneDeadLettersIfDueAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger?.LogWarning(ex, "PostgreSQL dead-letter prune failed; the publish it followed is committed and unaffected, and the prune is retried in the next throttle window.");
        }
    }

    public async Task<PostgreSqlTransportDelivery?> TryClaimAsync(string queue, TimeSpan lockTimeout, CancellationToken cancellationToken)
    {
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        var lockId = Guid.NewGuid();

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        // Ready order is AVAILABILITY order — (available_at, created_at), the dequeue index's own
        // key order behind the queue equality — so the scan returns its first unleased row without
        // sorting. It equals publish order for every row that was neither delayed nor NAKed (both
        // columns default to the same now()); a delayed or redelivered row queues by when it became
        // due instead of jumping ahead of everything published while it waited.
        command.CommandText =
            $"""
            WITH next AS (
                SELECT id
                FROM {MessageTable}
                WHERE queue = @queue
                  AND available_at <= now()
                  AND (locked_until IS NULL OR locked_until <= now())
                ORDER BY available_at, created_at
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            UPDATE {MessageTable} AS message
            SET attempts = message.attempts + 1,
                locked_until = now() + @lock_timeout,
                lock_id = @lock_id
            FROM next
            WHERE message.id = next.id
            RETURNING message.id, message.queue, message.payload_json::text, message.headers_json::text, message.attempts;
            """;
        command.Parameters.AddWithValue("queue", queue);
        command.Parameters.AddWithValue("lock_timeout", lockTimeout);
        command.Parameters.AddWithValue("lock_id", lockId);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            return null;

        var id = reader.GetGuid(0);
        var payload = reader.GetString(2);
        var headerJson = reader.GetString(3);
        var attempt = reader.GetInt32(4);
        var headers = DeserializeHeaders(headerJson);

        return new PostgreSqlTransportDelivery(
            id,
            reader.GetString(1),
            payload,
            headers,
            attempt,
            () => AckAsync(id, lockId),
            delay => NakAsync(id, lockId, delay),
            (exception, deleteOriginal, token) => DeadLetterAsync(id, lockId, queue, payload, headers, exception, deleteOriginal, token),
            () => RenewLeaseAsync(id, lockId, lockTimeout));
    }

    public async IAsyncEnumerable<PostgreSqlTransportDelivery> ClaimBatchAsync(
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
        bool notify,
        CancellationToken cancellationToken,
        TimeSpan? delay = null)
    {
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        // Native delayed delivery: available_at gates the claim query, and the delay arithmetic
        // runs on the DATABASE clock (now() + interval), matching the claim-side now() so client
        // clock skew cannot shift the due time. A due row is picked up by the subscriber's next
        // poll tick (EmptyPollDelay bounds the extra latency).
        command.CommandText =
            delay is null
                ? $"""
                  INSERT INTO {MessageTable} (id, queue, payload_json, headers_json, dead_letter_reason)
                  VALUES (@id, @queue, @payload_json, @headers_json, @dead_letter_reason)
                  ON CONFLICT (id) DO NOTHING;
                  """
                : $"""
                  INSERT INTO {MessageTable} (id, queue, payload_json, headers_json, dead_letter_reason, available_at)
                  VALUES (@id, @queue, @payload_json, @headers_json, @dead_letter_reason, now() + make_interval(secs => @delay_seconds))
                  ON CONFLICT (id) DO NOTHING;
                  """;
        if (notify)
            command.CommandText += "SELECT pg_notify(@channel, @payload);";

        command.Parameters.AddWithValue("id", id);
        command.Parameters.AddWithValue("queue", queue);
        command.Parameters.Add("payload_json", NpgsqlDbType.Jsonb).Value = payload;
        command.Parameters.Add("headers_json", NpgsqlDbType.Jsonb).Value = AsyncResponseJson.Serialize(headers ?? EmptyHeaders);
        command.Parameters.AddWithValue("dead_letter_reason", (object?)deadLetterReason ?? DBNull.Value);
        if (delay is { } pending)
            command.Parameters.AddWithValue("delay_seconds", pending.TotalSeconds);
        if (notify)
        {
            command.Parameters.AddWithValue("channel", _options.NotificationChannel);
            command.Parameters.AddWithValue("payload", queue);
        }

        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask AckAsync(Guid id, Guid lockId)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(CancellationToken.None).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = $"DELETE FROM {MessageTable} WHERE id = @id AND lock_id = @lock_id;";
        command.Parameters.AddWithValue("id", id);
        command.Parameters.AddWithValue("lock_id", lockId);
        await command.ExecuteNonQueryAsync(CancellationToken.None).ConfigureAwait(false);
    }

    private async ValueTask<bool> RenewLeaseAsync(Guid id, Guid lockId, TimeSpan lockTimeout)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(CancellationToken.None).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText =
            $"""
            UPDATE {MessageTable}
            SET locked_until = now() + @lock_timeout
            WHERE id = @id AND lock_id = @lock_id;
            """;
        command.Parameters.AddWithValue("id", id);
        command.Parameters.AddWithValue("lock_id", lockId);
        command.Parameters.AddWithValue("lock_timeout", lockTimeout);
        return await command.ExecuteNonQueryAsync(CancellationToken.None).ConfigureAwait(false) > 0;
    }

    private async ValueTask NakAsync(Guid id, Guid lockId, TimeSpan delay)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(CancellationToken.None).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        // No NOTIFY: the released row only becomes claimable once its delay has passed
        // (RedeliveryDelay is validated positive), so a wake sent now made every idle subscriber
        // of every queue, in every process, poll for a row none of them could claim yet — on each
        // handler failure. The row is picked up by the first poll tick after it falls due.
        command.CommandText =
            $"""
            UPDATE {MessageTable}
            SET available_at = now() + @delay,
                locked_until = NULL,
                lock_id = NULL
            WHERE id = @id AND lock_id = @lock_id;
            """;
        command.Parameters.AddWithValue("id", id);
        command.Parameters.AddWithValue("lock_id", lockId);
        command.Parameters.AddWithValue("delay", delay);
        await command.ExecuteNonQueryAsync(CancellationToken.None).ConfigureAwait(false);
    }

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
                await InsertAsync(Guid.NewGuid(), _options.DeadLetterQueue, payload, deadHeaders, exception.Message, notify: false, cancellationToken).ConfigureAwait(false);
                return true;
            }

            // The DLQ insert and the original-row delete must commit atomically: split across two
            // connections, a crash between them leaves the original row to be redelivered and
            // dead-lettered again, duplicating the DLQ entry.
            await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
            await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
            await using var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
            await using var command = connection.CreateCommand();
            command.Transaction = transaction;
            // The DLQ row is written ONLY if the fenced delete matched. A stale claim (the lease
            // lapsed and a peer re-claimed the row) must no-op here exactly as the fenced ack and
            // NAK do; writing the row unconditionally buried a full copy of a message that is still
            // live and may yet succeed under its new owner, so the DLQ showed a poison entry for
            // work that completed — and an operator replaying it duplicated its side effects.
            command.CommandText =
                $"""
                WITH removed AS (
                    DELETE FROM {MessageTable} WHERE id = @source_id AND lock_id = @lock_id RETURNING id
                )
                INSERT INTO {MessageTable} (id, queue, payload_json, headers_json, dead_letter_reason)
                SELECT @id, @queue, @payload_json, @headers_json, @dead_letter_reason FROM removed
                ON CONFLICT (id) DO NOTHING;
                """;
            command.Parameters.AddWithValue("id", Guid.NewGuid());
            command.Parameters.AddWithValue("queue", _options.DeadLetterQueue);
            command.Parameters.Add("payload_json", NpgsqlDbType.Jsonb).Value = payload;
            command.Parameters.Add("headers_json", NpgsqlDbType.Jsonb).Value = AsyncResponseJson.Serialize(deadHeaders);
            command.Parameters.AddWithValue("dead_letter_reason", exception.Message);
            command.Parameters.AddWithValue("source_id", id);
            command.Parameters.AddWithValue("lock_id", lockId);
            var inserted = await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);

            // Zero rows means the fence was lost, not that the write failed. Report it as a
            // non-dead-letter so the caller does not log a burial that did not happen; its NAK
            // fallback is fenced too, so the new owner keeps the row untouched.
            if (inserted == 0)
            {
                _logger?.LogWarning(
                    "PostgreSQL dead-letter for message {MessageId} from queue {SourceQueue} no-opped: the claim's lease had lapsed and the row was re-claimed.",
                    id,
                    sourceQueue);
                return false;
            }

            return true;
        }
        catch (Exception ex)
        {
            // Callers decide the redelivery consequence from the false return; log the cause here so
            // a failing dead-letter write is never silent.
            _logger?.LogError(
                ex,
                "Failed to write PostgreSQL dead-letter row for message {MessageId} from queue {SourceQueue}.",
                id,
                sourceQueue);
            return false;
        }
    }

    /// <summary>
    /// LISTENs on the transport's notification channel and invokes <paramref name="onNotification"/>
    /// for each wake. Every logical queue of every process shares the one channel and a publish
    /// NOTIFYs the queue it inserted into, so a listener that names its <paramref name="queue"/>
    /// is woken only for that queue; without it (<c>null</c>) every publish to any queue wakes it
    /// into a claim that finds nothing.
    /// </summary>
    public async Task ExecuteListenAsync(Func<Task> onNotification, CancellationToken cancellationToken, string? queue = null)
    {
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        connection.Notification += (_, e) =>
        {
            if (IsWakeFor(queue, e.Payload))
                _ = onNotification();
        };
        await using (var command = connection.CreateCommand())
        {
            command.CommandText = $"LISTEN {Quote(_options.NotificationChannel)};";
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        while (!cancellationToken.IsCancellationRequested)
            await connection.WaitAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Whether a NOTIFY payload is a wake for <paramref name="queue"/>. The payload is the queue
    /// name a publish inserted into, compared ordinally like the queue column itself; a listener
    /// that names no queue keeps every wake.
    /// </summary>
    internal static bool IsWakeFor(string? queue, string payload)
        => queue is null || string.Equals(payload, queue, StringComparison.Ordinal);

    /// <summary>
    /// Opportunistically deletes dead-letter rows older than the configured retention. No-op unless
    /// <see cref="PostgreSqlAsyncResponseTransportOptions.DeadLetterRetention"/> is set, and throttled
    /// so the DELETE runs at most once per minute regardless of publish rate.
    /// </summary>
    private async Task PruneDeadLettersIfDueAsync(CancellationToken cancellationToken)
    {
        if (_options.DeadLetterRetention is not { } retention || !ShouldPruneDeadLetters())
            return;

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = $"DELETE FROM {MessageTable} WHERE queue = @queue AND created_at < now() - @retention;";
        command.Parameters.AddWithValue("queue", _options.DeadLetterQueue);
        command.Parameters.AddWithValue("retention", retention);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private bool ShouldPruneDeadLetters()
    {
        var now = DateTime.UtcNow.Ticks;
        var last = Interlocked.Read(ref _lastDeadLetterPruneTicks);
        return now - last >= DeadLetterPruneThrottle.Ticks
            && Interlocked.CompareExchange(ref _lastDeadLetterPruneTicks, now, last) == last;
    }

    // Lenient by contract (see DbTransportHeaders): this runs after the claim already committed
    // attempts+1/lock_id, so rejecting any JSON the column legally holds would create an
    // unkillable poison row.
    private static IReadOnlyDictionary<string, string> DeserializeHeaders(string json)
        => DbTransportHeaders.Materialize(json);

    private static string Sanitize(string value) => value.Replace('\r', ' ').Replace('\n', ' ');

    /// <summary>
    /// Stable 64-bit advisory-lock key for serializing schema creation. Must be byte-for-byte identical
    /// to the channel store's algorithm/discriminator so that, for a shared schema, the channel and
    /// transport take the same lock and never race each other on CREATE SCHEMA.
    /// </summary>
    internal static long SchemaAdvisoryLockKey(string schemaName)
    {
        const ulong offset = 14695981039346656037UL;
        const ulong prime = 1099511628211UL;
        var hash = offset;
        foreach (var b in Encoding.UTF8.GetBytes($"asyncresponse:ddl:{schemaName}"))
        {
            hash ^= b;
            hash *= prime;
        }

        return unchecked((long)hash);
    }

    private static string Quote(string identifier) => "\"" + identifier + "\"";

    // Suffix space is RESERVED before capping; see RelationalNamePlan.DerivedName for why and for
    // the single implementation this and the SQL Server / channel stores all share.
    internal static string IndexName(string table, string suffix)
        => RelationalNamePlan.DerivedName(table, $"_{suffix}_idx", identifierCap: 63);

    private static readonly TimeSpan DeadLetterPruneThrottle = TimeSpan.FromMinutes(1);

    private static readonly IReadOnlyDictionary<string, string> EmptyHeaders =
        new Dictionary<string, string>(0, StringComparer.OrdinalIgnoreCase);
}
