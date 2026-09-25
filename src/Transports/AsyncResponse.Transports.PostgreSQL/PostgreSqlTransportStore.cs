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
/// fenced on the claim's <c>lock_id</c>, abandoning the attempt when its token fires (the heartbeat
/// bounds every attempt so a hung one is retried inside the lease); it returns <c>false</c> when the fence no longer matches
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
    Func<CancellationToken, ValueTask<bool>> RenewAsync);

/// <summary>Small SQL adapter for the PostgreSQL transport queue table.</summary>
internal sealed class PostgreSqlTransportStore
{
    private readonly NpgsqlDataSource _dataSource;
    private readonly PostgreSqlAsyncResponseTransportOptions _options;
    private readonly ILogger<PostgreSqlTransportStore>? _logger;
    private readonly SemaphoreSlim _ensureGate = new(1, 1);
    private bool _created;
    private DdlBackoff? _ddlBackoff;
    private readonly long _schemaLockKey;
    private long _lastDeadLetterPruneStamp;

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

    /// <summary>The clock of the lock-timeout retry-after window (test seam; see <see cref="DdlLockTimeoutBackoff"/>).</summary>
    internal TimeProvider Clock { get; set; } = TimeProvider.System;

    public async Task EnsureCreatedAsync(CancellationToken cancellationToken = default)
    {
        if (_created)
            return;

        ThrowIfDdlBackingOff();
        await _ensureGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_created)
                return;

            // Again under the gate: a caller that queued behind the attempt that just lost its lock
            // wait fails here instead of starting the next one.
            ThrowIfDdlBackingOff();

            await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
            await using var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);

            var readyIndex = IndexName(_options.MessageTable, "ready");
            var createdIndex = IndexName(_options.MessageTable, "created");
            var readyIndexState = ReadyIndexState.Usable;
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
                //
                // The DDL transaction runs under a lock_timeout (see DdlLockTimeout): the startup DDL
                // is otherwise bounded only by the command timeout, and while its ACCESS EXCLUSIVE
                // (the jsonb rewrite) or SHARE (an index build) request waits, every later statement
                // on the queue table — old-build hosts' renewals included — queues behind it.
                try
                {
                    await using (var command = connection.CreateCommand())
                    {
                        command.Transaction = transaction;
                        command.CommandText =
                            $"""
                            SET LOCAL lock_timeout = '{DdlLockTimeout}';
                            CREATE SCHEMA IF NOT EXISTS {Schema};

                            CREATE TABLE IF NOT EXISTS {MessageTable} (
                                id uuid PRIMARY KEY,
                                queue text NOT NULL,
                                payload_json text NOT NULL,
                                headers_json text NOT NULL DEFAULT {EmptyJsonObjectLiteral},
                                created_at timestamptz NOT NULL DEFAULT now(),
                                available_at timestamptz NOT NULL DEFAULT now(),
                                locked_until timestamptz NULL,
                                lock_id uuid NULL,
                                attempts integer NOT NULL DEFAULT 0,
                                dead_letter_reason text NULL
                            );
                            """;
                        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                    }

                    // jsonb REJECTS the \u0000 escape that System.Text.Json emits for U+0000 (SQLSTATE
                    // 22P05), so any job or response whose payload, context value, or callback argument
                    // carried a NUL was unpublishable on PostgreSQL alone (SQL Server and MongoDB store
                    // it), and jsonb's key re-sorting moved a "$type" discriminator behind other keys.
                    // Nothing here ever queries INSIDE the documents — both are read back with ::text —
                    // so text costs nothing (round-29 channel / flow-store parity). A table an older
                    // build created is converted once, in ONE statement: it rewrites the table under
                    // ACCESS EXCLUSIVE (see docs/postgresql.md).
                    var (payloadJsonb, headersJsonb) = await GetJsonbColumnsAsync(connection, transaction, cancellationToken).ConfigureAwait(false);
                    if (payloadJsonb || headersJsonb)
                    {
                        await using var migration = LongRunningDdlCommand(JsonbToTextMigrationSql(MessageTable, payloadJsonb, headersJsonb), connection, transaction);
                        await migration.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                    }

                    // Only an ABSENT index is created. CREATE INDEX IF NOT EXISTS takes its SHARE lock
                    // on the table BEFORE it finds the name taken, so on every start it queued behind
                    // an operator's CREATE INDEX CONCURRENTLY for the whole build (docs/postgresql.md
                    // recommends one on large tables), with every writer queued behind it in turn.
                    readyIndexState = await GetIndexStateAsync(connection, transaction, readyIndex, cancellationToken).ConfigureAwait(false);
                    var createdIndexAbsent = await GetIndexStateAsync(connection, transaction, createdIndex, cancellationToken).ConfigureAwait(false) is ReadyIndexState.Absent;
                    if (readyIndexState is ReadyIndexState.Absent || createdIndexAbsent)
                    {
                        var builds = new StringBuilder();
                        if (readyIndexState is ReadyIndexState.Absent)
                            builds.Append($"CREATE INDEX IF NOT EXISTS {Quote(readyIndex)} ON {MessageTable} (queue, available_at, created_at);\n");
                        if (createdIndexAbsent)
                            builds.Append($"CREATE INDEX IF NOT EXISTS {Quote(createdIndex)} ON {MessageTable} (created_at);\n");
                        await using var build = LongRunningDdlCommand(builds.ToString(), connection, transaction);
                        await build.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                        if (readyIndexState is ReadyIndexState.Absent)
                            readyIndexState = ReadyIndexState.Usable;
                    }
                }
                catch (PostgresException ex) when (ex.SqlState is PostgresErrorCodes.WrongObjectType or PostgresErrorCodes.UndefinedColumn)
                {
                    // E.g. CREATE INDEX ... ON a name that is really another component's index:
                    // IF NOT EXISTS skipped the table create, and the dependent statement then hits
                    // the wrong relation kind mid-batch — surface the namespace collision instead of
                    // the raw "cannot open relation".
                    throw new InvalidOperationException(PostgreSqlRelationVerifier.DdlCollisionMessage("transport", _options.SchemaName), ex);
                }
                catch (PostgresException ex) when (ex.SqlState is PostgresErrorCodes.LockNotAvailable)
                {
                    var retryAfter = BackOffDdlAfterLockTimeout(ex);
                    _logger?.LogWarning(
                        ex,
                        "PostgreSQL transport startup DDL on {Schema}.{Table} could not take its table lock within {LockTimeout} " +
                        "(another session holds a conflicting lock); nothing was changed. This host retries it in {RetryAfter}, " +
                        "and its transport operations fail at once until then.",
                        _options.SchemaName,
                        _options.MessageTable,
                        DdlLockTimeout,
                        retryAfter);
                    throw;
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
            //
            // "Present" means valid and ready, too, on BOTH paths. docs/postgresql.md has operators
            // build the index with CREATE INDEX CONCURRENTLY — on a large auto-created table too,
            // ahead of the rollout — and its catalog row exists (indisvalid / indisready false) for
            // the whole build, and stays, invalid, after a build that failed, which the DDL above
            // then leaves alone on every rerun. Verified as present, that index failed EnsureCreated
            // (and so every publish) on every host that started during the build, and forever after
            // a failed one. An unusable index is claim performance lost, exactly like an absent one:
            // warn and carry on.
            if (!_options.AutoCreateSchema)
            {
                await ThrowIfJsonbColumnsAsync(connection, transaction, cancellationToken).ConfigureAwait(false);
                readyIndexState = await GetIndexStateAsync(connection, transaction, readyIndex, cancellationToken).ConfigureAwait(false);
            }

            var verifyReadyIndex = readyIndexState is ReadyIndexState.Usable;
            if (readyIndexState is ReadyIndexState.Absent)
            {
                _logger?.LogWarning(
                    "PostgreSQL transport table {Schema}.{Table} has no dequeue index {Index} and AutoCreateSchema is disabled. " +
                    "Claims still work, but their cost grows with the backlog — performance only; create the index over " +
                    "(queue, available_at, created_at) as described in docs/postgresql.md.",
                    _options.SchemaName,
                    _options.MessageTable,
                    readyIndex);
            }
            else if (readyIndexState is ReadyIndexState.NotReady)
            {
                _logger?.LogWarning(
                    "PostgreSQL transport dequeue index {Schema}.{Index} exists but is not valid and ready (a CREATE INDEX " +
                    "CONCURRENTLY still running, or one that failed). Claims still work, but their cost grows with the backlog " +
                    "until it is usable — performance only; if its build failed, drop it and run the CREATE INDEX CONCURRENTLY " +
                    "from docs/postgresql.md again.",
                    _options.SchemaName,
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
                            new("payload_json", "text", Nullable: false),
                            new("headers_json", "text", Nullable: false, DefaultExpression: "'{}'::text"),
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
                    new(createdIndex, 'i', _options.MessageTable, ["created_at"]),
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

    private enum ReadyIndexState
    {
        Absent,
        Usable,
        NotReady
    }

    /// <summary>
    /// Whether an index is absent, usable, or present but not valid and ready (a concurrent build
    /// in progress, or one that failed). A relation of another kind under the name counts as
    /// present, so verification names the precise wrong-kind reason.
    /// </summary>
    private async Task<ReadyIndexState> GetIndexStateAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, string index, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText =
            """
            SELECT COALESCE(i.indisvalid AND i.indisready, true)
            FROM pg_catalog.pg_class c
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            LEFT JOIN pg_catalog.pg_index i ON i.indexrelid = c.oid
            WHERE n.nspname = @schema AND c.relname = @index;
            """;
        command.Parameters.AddWithValue("schema", _options.SchemaName);
        command.Parameters.AddWithValue("index", index);
        return await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) switch
        {
            null or DBNull => ReadyIndexState.Absent,
            true => ReadyIndexState.Usable,
            _ => ReadyIndexState.NotReady
        };
    }

    /// <summary>
    /// Operator-managed schema still carrying the pre-text column types: fail with the migration
    /// itself rather than the generic shape mismatch (whose guidance is about name collisions).
    /// </summary>
    private async Task ThrowIfJsonbColumnsAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, CancellationToken cancellationToken)
    {
        var (payloadJsonb, headersJsonb) = await GetJsonbColumnsAsync(connection, transaction, cancellationToken).ConfigureAwait(false);
        if (!payloadJsonb && !headersJsonb)
            return;

        throw new InvalidOperationException(
            $"The PostgreSQL transport table '{_options.SchemaName}.{_options.MessageTable}' stores payload_json/headers_json as jsonb, " +
            "which rejects the \\u0000 escape System.Text.Json emits for U+0000 — any job or response carrying a NUL would be " +
            "unpublishable. With AutoCreateSchema disabled the store does not migrate it; run (the statement rewrites the table " +
            $"under an ACCESS EXCLUSIVE lock): {JsonbToTextMigrationSql(MessageTable, payloadJsonb, headersJsonb)} — see docs/postgresql.md.");
    }

    /// <summary>Which of the two document columns still carry the pre-text <c>jsonb</c> type.</summary>
    private async Task<(bool PayloadJson, bool HeadersJson)> GetJsonbColumnsAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText =
            """
            SELECT coalesce(bool_or(column_name = 'payload_json'), false), coalesce(bool_or(column_name = 'headers_json'), false)
            FROM information_schema.columns
            WHERE table_schema = @schema AND table_name = @table
              AND column_name IN ('payload_json', 'headers_json') AND data_type = 'jsonb';
            """;
        command.Parameters.AddWithValue("schema", _options.SchemaName);
        command.Parameters.AddWithValue("table", _options.MessageTable);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        await reader.ReadAsync(cancellationToken).ConfigureAwait(false);
        return (reader.GetBoolean(0), reader.GetBoolean(1));
    }

    /// <summary>
    /// The jsonb → text conversion of whichever columns still need it, as ONE <c>ALTER TABLE</c>:
    /// every <c>ALTER COLUMN … TYPE</c> statement rewrites the whole table and rebuilds each of its
    /// indexes under ACCESS EXCLUSIVE, so a statement per column did all of it twice. The
    /// <c>headers_json</c> default is dropped and re-set around its conversion, or the jsonb default
    /// would be carried over as <c>('{}'::jsonb)::text</c>.
    /// </summary>
    internal static string JsonbToTextMigrationSql(string messageTable, bool payloadJson, bool headersJson)
    {
        var subcommands = new List<string>(4);
        if (payloadJson)
            subcommands.Add("ALTER COLUMN payload_json TYPE text USING payload_json::text");
        if (headersJson)
        {
            subcommands.Add("ALTER COLUMN headers_json DROP DEFAULT");
            subcommands.Add("ALTER COLUMN headers_json TYPE text USING headers_json::text");
            subcommands.Add($"ALTER COLUMN headers_json SET DEFAULT {EmptyJsonObjectLiteral}");
        }

        return $"ALTER TABLE {messageTable} {string.Join(", ", subcommands)};";
    }

    /// <summary>
    /// The lock-wait bound of the auto-create DDL transaction (<c>SET LOCAL lock_timeout</c>). While
    /// the jsonb rewrite's ACCESS EXCLUSIVE request — or an index build's SHARE request — waits, every
    /// later statement on the queue table queues behind it, on every host, so the wait IS a queue
    /// outage. 5 s outwaits the transport's own statements (single-statement, sub-second) and an
    /// ORDINARY autovacuum, which cancels itself for a conflicting lock request after
    /// <c>deadlock_timeout</c> (1 s by default), and stays under the lease-renewal beat
    /// (<c>LockTimeout</c>/3, 10 s by default), so no running handler's lease lapses behind it. What
    /// does not yield — an anti-wraparound autovacuum, a manual VACUUM or ANALYZE, pg_dump, an
    /// idle-in-transaction session — fails the DDL fast instead, changing nothing, and
    /// <see cref="DdlLockTimeoutBackoff"/> paces the retries.
    /// </summary>
    internal const string DdlLockTimeout = "5s";

    /// <summary>
    /// The shortest retry-after window a lock-timeout failure of the startup DDL latches; the actual
    /// window is jittered up to twice this (30–60 s). Without it every operation on the host started
    /// the next attempt at once, and a lock held for minutes or hours (the holders named on
    /// <see cref="DdlLockTimeout"/>) stalled the whole queue — every host's statements, parked behind
    /// each attempt's lock request — in back-to-back 5 s cycles, the k-th caller queued on the gate
    /// waiting k × 5 s to fail. 30 s keeps one host's attempts to a small fraction of the table's time;
    /// the jitter keeps a fleet started together from retrying in lockstep (the advisory lock would
    /// chain their waits back to back), and the 60 s ceiling bounds how long this host stays unable
    /// to publish or claim after the conflicting lock is gone.
    /// </summary>
    internal static readonly TimeSpan DdlLockTimeoutBackoff = TimeSpan.FromSeconds(30);

    /// <summary>
    /// The command timeout of DDL whose run time grows with the table — the jsonb rewrite, the first
    /// build of an index on an existing table — in seconds. Not the data source's (30 s by default):
    /// each is one-time, and one that outran it was rolled back and retried by EVERY later operation
    /// (EnsureCreated runs until it succeeds), each attempt holding its ACCESS EXCLUSIVE or SHARE lock
    /// for another full timeout and never finishing. Not unbounded either: the command runs holding
    /// the store's gate, and Npgsql sends no keepalive by default, so a socket black-holed mid-rewrite
    /// (a failover to a new address, a NAT or load-balancer drop) left it waiting forever and every
    /// later operation on the host queued on the gate behind it. An hour is an order of magnitude past
    /// what a queue table's rewrite takes; a larger one is converted ahead of the rollout
    /// (docs/postgresql.md).
    /// </summary>
    internal const int LongRunningDdlCommandTimeoutSeconds = 3600;

    /// <summary>
    /// A command for DDL whose run time grows with the table (see
    /// <see cref="LongRunningDdlCommandTimeoutSeconds"/>). Lock WAITS stay bounded by
    /// <see cref="DdlLockTimeout"/>; a <c>statement_timeout</c> the operator configured still applies.
    /// </summary>
    internal static NpgsqlCommand LongRunningDdlCommand(string sql, NpgsqlConnection? connection = null, NpgsqlTransaction? transaction = null)
        => new(sql, connection, transaction) { CommandTimeout = LongRunningDdlCommandTimeoutSeconds };

    /// <summary>
    /// Latches this store's retry-after window after the startup DDL lost its lock wait (see
    /// <see cref="DdlLockTimeoutBackoff"/>) and returns its length. Until it ends,
    /// <see cref="EnsureCreatedAsync"/> fails at once — no connection, no transaction, no lock
    /// request queued ahead of the other hosts' statements.
    /// </summary>
    internal TimeSpan BackOffDdlAfterLockTimeout(Exception cause)
    {
        var window = DdlLockTimeoutBackoff + TimeSpan.FromTicks(Random.Shared.NextInt64(DdlLockTimeoutBackoff.Ticks));
        Volatile.Write(ref _ddlBackoff, new DdlBackoff(Clock.GetTimestamp(), window, cause));
        return window;
    }

    private void ThrowIfDdlBackingOff()
    {
        if (Volatile.Read(ref _ddlBackoff) is not { } backoff)
            return;

        var remaining = backoff.Window - Clock.GetElapsedTime(backoff.StartedAt);
        if (remaining <= TimeSpan.Zero)
            return;

        throw new InvalidOperationException(
            $"The PostgreSQL transport's startup DDL on '{_options.SchemaName}.{_options.MessageTable}' could not take its table " +
            $"lock within {DdlLockTimeout}: another session holds a conflicting lock on the table (an anti-wraparound or manual " +
            "VACUUM, ANALYZE, pg_dump, or a long-running or idle-in-transaction session — find it in pg_locks joined to " +
            $"pg_stat_activity). Nothing was changed. This host retries it in {Math.Ceiling(remaining.TotalSeconds):0} s — every " +
            "attempt parks every statement on the queue table, on every host, behind its lock request — and its transport " +
            "operations fail until the DDL succeeds; see docs/postgresql.md.",
            backoff.Cause);
    }

    private sealed record DdlBackoff(long StartedAt, TimeSpan Window, Exception Cause);

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
        // The prune is opportunistic housekeeping and swallows its own failures (see
        // DbDeadLetterPrune); the next throttle window retries it.
        await DbDeadLetterPrune.RunIfDueAsync(
            ref _lastDeadLetterPruneStamp,
            _options.DeadLetterRetention,
            PruneDeadLetterBatchAsync,
            _logger,
            "PostgreSQL",
            cancellationToken).ConfigureAwait(false);
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
            token => RenewLeaseAsync(id, lockId, lockTimeout, token));
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
        command.Parameters.Add("payload_json", NpgsqlDbType.Text).Value = payload;
        command.Parameters.Add("headers_json", NpgsqlDbType.Text).Value = AsyncResponseJson.Serialize(headers ?? EmptyHeaders);
        command.Parameters.AddWithValue("dead_letter_reason", deadLetterReason is null ? DBNull.Value : DeadLetterReason(deadLetterReason));
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

    // The token is the heartbeat's per-attempt bound: a renew hung on a black-holed pooled
    // connection or a failover inherited Npgsql's 30 s command timeout, which outlasted the 20 s of
    // lease left after the beat — the lease lapsed before the short-backoff retry ever ran.
    private async ValueTask<bool> RenewLeaseAsync(Guid id, Guid lockId, TimeSpan lockTimeout, CancellationToken cancellationToken)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
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
        return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) > 0;
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
            command.Parameters.Add("payload_json", NpgsqlDbType.Text).Value = payload;
            command.Parameters.Add("headers_json", NpgsqlDbType.Text).Value = AsyncResponseJson.Serialize(deadHeaders);
            command.Parameters.AddWithValue("dead_letter_reason", DeadLetterReason(exception.Message));
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
        // Not `await using`: the release below owns disposal, and may finish it after this returns.
        var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        connection.Notification += (_, e) =>
        {
            if (IsWakeFor(queue, e.Payload))
                _ = onNotification();
        };
        var listening = false;
        try
        {
            await using (var command = connection.CreateCommand())
            {
                command.CommandText = $"LISTEN {Quote(_options.NotificationChannel)};";
                await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }

            listening = true;

            while (!cancellationToken.IsCancellationRequested)
                await connection.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            // Every supervised-attempt restart cancels this wait, and Npgsql keeps the connector
            // usable after a cancelled WaitAsync: with No Reset On Close=true (which
            // docs/postgresql.md recommends) nothing ever DISCARDs the LISTEN, so each restart
            // returned one more still-listening connection to the pool, every one of them
            // receiving every transport NOTIFY. Bounded, so a half-open socket cannot hold the
            // restart (see PostgreSqlListenConnection); never replaces an unwinding fault.
            await PostgreSqlListenConnection.ReleaseAsync(_dataSource, connection, listening).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Whether a NOTIFY payload is a wake for <paramref name="queue"/>. The payload is the queue
    /// name a publish inserted into, compared ordinally like the queue column itself; a listener
    /// that names no queue keeps every wake. An EMPTY payload wakes every listener: it is what a
    /// foreign producer's bare <c>NOTIFY channel</c> sends, and the reply-target contract publishes
    /// the channel without promising a payload — such a producer must not be demoted to polling.
    /// </summary>
    internal static bool IsWakeFor(string? queue, string payload)
        => queue is null || payload.Length == 0 || string.Equals(payload, queue, StringComparison.Ordinal);

    /// <summary>
    /// One bounded batch of the dead-letter retention prune (<see cref="DbDeadLetterPrune"/> throttles,
    /// drains, and guards it). Bounded like every other relational prune: one unbounded DELETE over
    /// the backlog an operator faces the first time they enable <c>DeadLetterRetention</c> (dead
    /// letters are kept forever by default) outran the command timeout, rolled back, and was retried
    /// every minute without ever shrinking it — holding one publish for the whole timeout each time.
    /// </summary>
    private async Task<int> PruneDeadLetterBatchAsync(TimeSpan retention, CancellationToken cancellationToken)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = DeadLetterPruneSql(MessageTable);
        command.Parameters.AddWithValue("queue", _options.DeadLetterQueue);
        command.Parameters.AddWithValue("retention", retention);
        return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>The bounded dead-letter prune statement (durable-flow-store <c>ctid … LIMIT</c> shape).</summary>
    internal static string DeadLetterPruneSql(string messageTable)
        => $"""
           DELETE FROM {messageTable}
           WHERE ctid IN (
               SELECT ctid FROM {messageTable}
               WHERE queue = @queue AND created_at < now() - @retention
               LIMIT {OpportunisticPrune.BatchSize});
           """;

    // Lenient by contract (see DbTransportHeaders): this runs after the claim already committed
    // attempts+1/lock_id, so rejecting any JSON the column legally holds would create an
    // unkillable poison row.
    private static IReadOnlyDictionary<string, string> DeserializeHeaders(string json)
        => DbTransportHeaders.Materialize(json);

    private static string Sanitize(string value) => value.Replace('\r', ' ').Replace('\n', ' ');

    /// <summary>
    /// The raw exception message bound into the <c>text</c> column <c>dead_letter_reason</c>, with
    /// any U+0000 replaced by U+FFFD: PostgreSQL <c>text</c> rejects a NUL character outright
    /// (SQLSTATE 22021) whatever the JSON columns' type, so a failure whose message carried one could
    /// never be dead-lettered — and after an early ACK the job was lost with only an Error log.
    /// </summary>
    internal static string DeadLetterReason(string message) => message.Replace('\0', '\uFFFD');

    // Interpolated into DDL: literal braces cannot appear directly inside the interpolated raw string.
    private const string EmptyJsonObjectLiteral = "'{}'";

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

    private static readonly IReadOnlyDictionary<string, string> EmptyHeaders =
        new Dictionary<string, string>(0, StringComparer.OrdinalIgnoreCase);
}
