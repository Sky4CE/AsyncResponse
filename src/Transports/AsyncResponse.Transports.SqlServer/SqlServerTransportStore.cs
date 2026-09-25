using AsyncResponse.Internal;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Runtime.CompilerServices;

namespace AsyncResponse.Transports.SqlServer;

internal enum SqlServerSubscriberRole
{
    Worker,
    ResponseIngress
}

/// <summary>A claimed SQL Server transport row, decoupled from SqlClient types for dispatch tests.</summary>
/// <remarks>
/// <c>RenewAsync</c> extends the claim's lease (<c>locked_until</c>) by the original lock timeout,
/// fenced on the claim's <c>lock_id</c>, abandoning the attempt when its token fires (the heartbeat
/// bounds every attempt so a hung one is retried inside the lease); it returns <c>false</c> when the fence no longer matches
/// (the lease lapsed and another subscriber re-claimed the row).
/// </remarks>
internal sealed record SqlServerTransportDelivery(
    Guid Id,
    string Queue,
    string Payload,
    IReadOnlyDictionary<string, string> Headers,
    int Attempt,
    Func<ValueTask> AckAsync,
    Func<TimeSpan, ValueTask> NakAsync,
    Func<Exception, bool, CancellationToken, ValueTask<bool>> DeadLetterAsync,
    Func<CancellationToken, ValueTask<bool>> RenewAsync);

/// <summary>Small SQL adapter for the SQL Server transport queue table.</summary>
internal sealed class SqlServerTransportStore
{
    // SQL Server duplicate-key error numbers: 2627 = PRIMARY KEY/UNIQUE constraint violation,
    // 2601 = unique index violation. A concurrent retry of the same idempotent publish can lose the
    // WHERE NOT EXISTS race; the duplicate is the outcome the caller asked for, not a failure.
    private const int PrimaryKeyViolation = 2627;
    private const int UniqueIndexViolation = 2601;

    // "Lock request time out period exceeded": the dequeue-index build's LOCK_TIMEOUT lapsed.
    private const int LockRequestTimeout = 1222;

    // Interpolated into DDL: literal braces cannot appear directly inside the interpolated raw string.
    private const string EmptyJsonObject = "{}";

    /// <summary>
    /// An EXACT queue-name predicate — the only kind this table can be filtered by safely, because
    /// its three logical queues share one table and are told apart by nothing but this column.
    /// <c>queue = @queue</c> alone is not exact in two independent ways: SQL Server pads the shorter
    /// operand of an equality comparison with spaces (under EVERY collation, binary ones included),
    /// so <c>'worker '</c> answers a query for <c>'worker'</c>; and on a table an older build or a
    /// hand-written migration left with the server's default collation, the comparison also folds
    /// case, accent, and width.
    /// <para>
    /// The second comparison closes both. Appending a non-blank sentinel to each side makes the
    /// last character non-blank, so the padding SQL Server may add can no longer bridge two
    /// different strings — <c>'worker .'</c> versus <c>'worker. '</c> differ at the seventh
    /// character — and the explicit collation makes the comparison ordinal whatever the column's
    /// own collation is. The first comparison is kept as the seekable driver, so the claim index is
    /// still used and this only filters the rows it returns.
    /// </para>
    /// <para>
    /// Verified on SQL Server 2022, which is also why the shape is this one and not the more
    /// obvious <c>DATALENGTH(queue) = DATALENGTH(@queue)</c>: byte counts are meaningless across
    /// types, so that form silently matches NOTHING on a <c>varchar</c> column, and pushing the
    /// explicit collation onto the driver comparison costs the index seek on a case-folding column.
    /// This form keeps an Index Seek on both, and was measured exact against <c>nvarchar</c>
    /// binary, <c>nvarchar</c> case-insensitive, and <c>varchar</c> columns alike.
    /// </para>
    /// <para>
    /// Exactness belongs HERE rather than in a post-claim re-check: a row the query returns has
    /// already been claimed, and releasing it leaves it first in line for the very next poll, which
    /// starves every valid row behind it.
    /// </para>
    /// </summary>
    private const string ExactQueueMatch =
        "queue = @queue AND queue + N'.' = @queue + N'.' COLLATE Latin1_General_100_BIN2";

    private readonly string _connectionString;
    private readonly SqlServerAsyncResponseTransportOptions _options;
    private readonly ILogger<SqlServerTransportStore>? _logger;
    private readonly SemaphoreSlim _ensureGate = new(1, 1);
    private bool _created;
    private DdlBackoff? _ddlBackoff;
    private long _lastDeadLetterPruneStamp;

    public SqlServerTransportStore(
        IOptions<SqlServerAsyncResponseTransportOptions> options,
        ILogger<SqlServerTransportStore>? logger = null)
    {
        _options = options.Value;
        _logger = logger;
        SqlServerTransportOptionsValidator.ValidateCommon(_options);
        _connectionString = _options.ConnectionString!;
        Schema = Quote(_options.SchemaName);
        MessageTable = $"{Schema}.{Quote(_options.MessageTable)}";
    }

    public string Schema { get; }
    public string MessageTable { get; }

    /// <summary>
    /// Raised after a row is inserted, with the logical queue name. Same-process subscribers use it
    /// to wake immediately instead of waiting out their empty-poll delay; SQL Server has no
    /// LISTEN/NOTIFY, so cross-process wakes rely on polling. A NAK raises nothing (PostgreSQL
    /// parity): the released row is not claimable until its redelivery delay has passed, so a wake
    /// sent then only sent every same-process subscriber into a claim that found nothing.
    /// </summary>
    public event Action<string?>? MessagePublished;

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

            await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);

            if (!_options.AutoCreateSchema)
            {
                // Operator-managed schema: no DDL and no DDL lock, but catalog verification all the
                // same — an operator-provisioned queue table whose payload_json, headers_json, or
                // timestamp columns have the wrong shape breaks every insert or silently reorders
                // the timestamps this store compares, which is exactly what verification exists to
                // catch. An absent object is fine: the migration has not run yet, the first query
                // surfaces a clear SQL Server error (the documented "create it yourself, later"
                // workflow), and _created stays unlatched so a later operation re-verifies once the
                // migration lands.
                if (!await ObjectExistsAsync(connection, cancellationToken).ConfigureAwait(false))
                    return;

                await VerifyRelationsAsync(connection, transaction: null, selfCreated: false, cancellationToken).ConfigureAwait(false);
                await WarnIfClaimIndexMissingAsync(connection, cancellationToken).ConfigureAwait(false);
                _created = true;
                return;
            }

            await using var transaction = (SqlTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);

            // Serialize schema creation across processes. The IF-NOT-EXISTS guards are not atomic
            // against a concurrent create of the same object: two instances starting together both
            // pass the existence check and collide on the catalog (error 2714/2627). A
            // transaction-scoped application lock (keyed by schema, shared with the channel store)
            // lets one instance build the schema while the rest wait and then find it already present.
            await using (var lockCommand = connection.CreateCommand())
            {
                lockCommand.Transaction = transaction;
                lockCommand.CommandText =
                    """
                    DECLARE @lock_result int;
                    EXEC @lock_result = sp_getapplock
                        @Resource = @lock_resource,
                        @LockMode = 'Exclusive',
                        @LockOwner = 'Transaction',
                        @LockTimeout = 60000;
                    IF @lock_result < 0
                        THROW 51000, N'Failed to acquire the AsyncResponse DDL application lock.', 1;
                    """;
                lockCommand.Parameters.AddWithValue("@lock_resource", SchemaLockResource(_options.SchemaName));
                await lockCommand.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }

            // The dequeue index is (queue, available_at, created_at) and the claim orders by exactly
            // that tail (PostgreSQL round-42 parity), so a claim is one ordered index descent that
            // stops at the first unleased row. The previous pair — "<table>_claim_idx" over (queue,
            // available_at, locked_until, created_at) behind ORDER BY created_at — could not serve its
            // own ordering past the available_at range: the plan either sorted the whole ready set
            // (U-locking every row it scanned under UPDLOCK/READPAST, so competing claimers skipped
            // them all and slept EmptyPollDelay) or walked created_idx with lookups through every
            // older row of the OTHER logical queues (retained dead letters, delayed jobs), so
            // draining a burst cost its square. A table created by an older build keeps its
            // "<table>_claim_idx"; nothing reads it any more and nothing here drops it — see
            // docs/sqlserver.md.
            var readyIndex = IndexName(_options.MessageTable, "ready");
            try
            {
                await using (var command = connection.CreateCommand())
                {
                    command.Transaction = transaction;
                    command.CommandText =
                        $"""
                        IF SCHEMA_ID(N'{_options.SchemaName}') IS NULL
                            EXEC(N'CREATE SCHEMA {Schema}');

                        IF OBJECT_ID(N'{MessageTable}', N'U') IS NULL
                        CREATE TABLE {MessageTable} (
                            id uniqueidentifier NOT NULL PRIMARY KEY NONCLUSTERED,
                            queue nvarchar(200) COLLATE Latin1_General_100_BIN2 NOT NULL,
                            payload_json nvarchar(max) NOT NULL,
                            headers_json nvarchar(max) NOT NULL DEFAULT N'{EmptyJsonObject}',
                            created_at datetime2 NOT NULL DEFAULT SYSUTCDATETIME(),
                            available_at datetime2 NOT NULL DEFAULT SYSUTCDATETIME(),
                            locked_until datetime2 NULL,
                            lock_id uniqueidentifier NULL,
                            attempts int NOT NULL DEFAULT 0,
                            dead_letter_reason nvarchar(max) NULL
                        );

                        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'{IndexName(_options.MessageTable, "created")}' AND object_id = OBJECT_ID(N'{MessageTable}'))
                            CREATE INDEX {Quote(IndexName(_options.MessageTable, "created"))}
                                ON {MessageTable} (created_at);
                        """;
                    await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                }

                // The dequeue index gets its own command, run only when the index is absent: on a
                // table an older build created, its first build is an offline CREATE INDEX over
                // every retained row, holding a table S lock that blocks every write. Under the
                // default 30 s command timeout a large table's build timed out, rolled back, and was
                // retried by EVERY later operation (EnsureCreated runs per insert, claim, and
                // dead-letter until it succeeds), each attempt blocking the queue's writes for
                // another 30 s and never finishing — see ReadyIndexBuildSql.
                if (!await IndexExistsAsync(connection, transaction, readyIndex, cancellationToken).ConfigureAwait(false))
                {
                    await using var build = LongRunningDdlCommand(ReadyIndexBuildSql(MessageTable, readyIndex), connection, transaction);
                    await build.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                }
            }
            catch (SqlException ex)
            {
                if (ex.Number == LockRequestTimeout)
                {
                    var retryAfter = BackOffDdlAfterLockTimeout(ex);
                    _logger?.LogWarning(
                        ex,
                        "SQL Server transport startup DDL on {Schema}.{Table} could not take its table lock within {LockTimeoutMs} ms " +
                        "(another session holds a conflicting lock); nothing was changed. This host retries it in {RetryAfter}, " +
                        "and its transport operations fail at once until then.",
                        _options.SchemaName,
                        _options.MessageTable,
                        DdlLockTimeoutMilliseconds,
                        retryAfter);
                }

                // The batch can break BEFORE the verification below runs: a name held by another
                // component's table suppresses the guarded CREATE and the index that follows hits
                // the wrong table, and a name held by a view fails outright with error 2714. Run
                // the same catalog checks now, on a fresh connection (the objects in question are
                // somebody else's and already committed) after rolling this transaction back, so
                // the operator gets the precise reason.
                await SqlServerRelationVerifier.ThrowDiagnosedCollisionAsync(
                    OpenConnectionAsync,
                    ex,
                    transaction,
                    _options.SchemaName,
                    "transport",
                    ExpectedObjects(selfCreated: true),
                    cancellationToken).ConfigureAwait(false);
                throw;
            }

            // Post-DDL catalog verification (below, after the commit): the existence guard above
            // only asks "is there a user table with this name", so another component's table
            // silently suppresses creation and a view or synonym makes the CREATE fail with raw
            // error 2714.
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);

            // Verified AFTER the commit, on the same connection but outside the transaction. The
            // checks read the catalog, and a transaction that has just run DDL still holds
            // schema-modification locks — catalog reads under those deadlock (error 1205) against
            // this store's own live traffic, which is already polling by the time a later
            // EnsureCreated re-runs. Correctness does not need the transaction: the application
            // lock serialized the DDL, and what these checks look for is somebody ELSE'S committed
            // object occupying a name, never our own uncommitted work.
            await VerifyRelationsAsync(connection, transaction: null, selfCreated: true, cancellationToken).ConfigureAwait(false);
            _created = true;
        }
        finally
        {
            _ensureGate.Release();
        }
    }

    private async Task<bool> IndexExistsAsync(SqlConnection connection, SqlTransaction transaction, string index, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "SELECT CASE WHEN EXISTS (SELECT 1 FROM sys.indexes WHERE name = @index AND object_id = OBJECT_ID(@table)) THEN 1 ELSE 0 END;";
        command.Parameters.AddWithValue("@index", index);
        command.Parameters.AddWithValue("@table", MessageTable);
        return (int)(await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false))! == 1;
    }

    /// <summary>
    /// The dequeue-index build (run by <see cref="EnsureCreatedAsync"/> through
    /// <see cref="LongRunningDdlCommand"/>, only when the index is absent). Its lock WAIT is bounded
    /// (<see cref="DdlLockTimeoutMilliseconds"/>): while the build's S request waits behind in-flight
    /// writers, every later write to the table queues behind it, so the wait is a queue outage. The
    /// session setting is restored in the same batch (a lock-timeout error ends only its statement),
    /// before the post-commit verification reuses the connection. The guard stays: an operator's
    /// concurrent <c>ONLINE</c> build may land between the existence check and this batch.
    /// </summary>
    internal static string ReadyIndexBuildSql(string messageTable, string readyIndex)
        => $"""
           SET LOCK_TIMEOUT {DdlLockTimeoutMilliseconds};
           IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'{readyIndex}' AND object_id = OBJECT_ID(N'{messageTable}'))
               CREATE INDEX {Quote(readyIndex)}
                   ON {messageTable} (queue, available_at, created_at);
           SET LOCK_TIMEOUT -1;
           """;

    /// <summary>
    /// The dequeue-index build's lock-wait bound, in milliseconds (PostgreSQL <c>DdlLockTimeout</c>
    /// parity): long enough to outwait the transport's own single-statement writes, and under the
    /// lease-renewal beat (<c>LockTimeout</c>/3, 10 s by default) so no running handler's renewal
    /// lapses behind a waiting build. On a genuinely busy table — a long transaction, an index
    /// rebuild, a bulk load holding a conflicting lock — the build fails fast instead, changing
    /// nothing, and <see cref="DdlLockTimeoutBackoff"/> paces the retries.
    /// </summary>
    internal const int DdlLockTimeoutMilliseconds = 5000;

    /// <summary>
    /// The shortest retry-after window a lock-timeout failure of the startup DDL latches; the actual
    /// window is jittered up to twice this (30–60 s) — PostgreSQL <c>DdlLockTimeoutBackoff</c> parity.
    /// Without it every operation on the host started the next attempt at once, and a lock held for
    /// minutes stalled the whole queue — every host's writes, queued behind each attempt's lock
    /// request — in back-to-back 5 s cycles, the k-th caller queued on the gate waiting k × 5 s to
    /// fail. 30 s keeps one host's attempts to a small fraction of the table's time; the jitter keeps
    /// a fleet started together from retrying in lockstep (the application lock would chain their
    /// waits back to back), and the 60 s ceiling bounds how long this host stays unable to publish
    /// or claim after the conflicting lock is gone.
    /// </summary>
    internal static readonly TimeSpan DdlLockTimeoutBackoff = TimeSpan.FromSeconds(30);

    /// <summary>
    /// The command timeout of DDL whose run time grows with the table — the first build of the
    /// dequeue index on an existing one — in seconds (PostgreSQL parity). Not SqlClient's 30 s
    /// default: the build is one-time, and one that outran it rolled back and was retried by every
    /// later operation, blocking writes for another full timeout each time and never finishing. Not
    /// unbounded either: the command runs holding the store's gate, so a connection that never
    /// answers would leave every later operation on the host queued behind it. An hour is an order
    /// of magnitude past what a queue table's index build takes; a larger table gets the index ahead
    /// of the rollout (docs/sqlserver.md).
    /// </summary>
    internal const int LongRunningDdlCommandTimeoutSeconds = 3600;

    /// <summary>
    /// A command for DDL whose run time grows with the table (see
    /// <see cref="LongRunningDdlCommandTimeoutSeconds"/>). Its lock wait stays bounded by the batch's
    /// own <c>SET LOCK_TIMEOUT</c> (<see cref="ReadyIndexBuildSql"/>).
    /// </summary>
    internal static SqlCommand LongRunningDdlCommand(string sql, SqlConnection? connection = null, SqlTransaction? transaction = null)
        => new(sql, connection, transaction) { CommandTimeout = LongRunningDdlCommandTimeoutSeconds };

    /// <summary>
    /// Latches this store's retry-after window after the startup DDL lost its lock wait (see
    /// <see cref="DdlLockTimeoutBackoff"/>) and returns its length. Until it ends,
    /// <see cref="EnsureCreatedAsync"/> fails at once — no connection, no transaction, no lock
    /// request queued ahead of the other hosts' writes.
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
            $"The SQL Server transport's startup DDL on '{_options.SchemaName}.{_options.MessageTable}' could not take its table " +
            $"lock within {DdlLockTimeoutMilliseconds} ms: another session holds a conflicting lock on the table (a long-running " +
            "transaction, an index rebuild, a bulk load — find it in sys.dm_tran_locks joined to sys.dm_exec_sessions). Nothing " +
            $"was changed. This host retries it in {Math.Ceiling(remaining.TotalSeconds):0} s — every attempt queues every write " +
            "to the queue table, on every host, behind its lock request — and its transport operations fail until the DDL " +
            "succeeds; see docs/sqlserver.md.",
            backoff.Cause);
    }

    private sealed record DdlBackoff(long StartedAt, TimeSpan Window, Exception Cause);

    private Task VerifyRelationsAsync(SqlConnection connection, SqlTransaction? transaction, bool selfCreated, CancellationToken cancellationToken)
        => SqlServerRelationVerifier.VerifyAsync(
            connection,
            transaction,
            _options.SchemaName,
            "transport",
            ExpectedObjects(selfCreated),
            cancellationToken);

    /// <summary>
    /// Reports whether ANY object occupies the configured queue-table name (any kind: a view or
    /// foreign component's object must reach verification, which names the precise wrong-kind
    /// reason instead of skipping the checks). The catalog's own collation decides case matching,
    /// exactly as the server resolves the runtime identifier.
    /// </summary>
    private async Task<bool> ObjectExistsAsync(SqlConnection connection, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.CommandText =
            """
            SELECT CASE WHEN EXISTS (
                SELECT 1
                FROM sys.objects o
                JOIN sys.schemas s ON s.schema_id = o.schema_id
                WHERE s.name = @schema AND o.name = @table) THEN 1 ELSE 0 END;
            """;
        command.Parameters.AddWithValue("@schema", _options.SchemaName);
        command.Parameters.AddWithValue("@table", _options.MessageTable);
        return (int)(await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false))! == 1;
    }

    /// <summary>
    /// Operator-managed schema: warns (read-only, never fails) when no index on the queue table
    /// leads on <c>queue</c> (PostgreSQL / MongoDB parity). Without one every claim scans the whole
    /// table under UPDLOCK, per poll tick, per subscriber — with no error and no log line. Indexes
    /// stay the operator's (performance, not correctness), so any index leading on <c>queue</c>
    /// satisfies this; a catalog that cannot be read (no VIEW DEFINITION) skips the check.
    /// </summary>
    private async Task WarnIfClaimIndexMissingAsync(SqlConnection connection, CancellationToken cancellationToken)
    {
        try
        {
            await using var command = connection.CreateCommand();
            command.CommandText =
                """
                SELECT CASE WHEN EXISTS (
                    SELECT 1
                    FROM sys.indexes i
                    JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
                    JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
                    WHERE i.object_id = OBJECT_ID(@table) AND ic.key_ordinal = 1 AND c.name = N'queue') THEN 1 ELSE 0 END;
                """;
            command.Parameters.AddWithValue("@table", MessageTable);
            if ((int)(await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false))! == 1)
                return;

            _logger?.LogWarning(
                "SQL Server transport table {Schema}.{Table} has no index leading on 'queue' and AutoCreateSchema is disabled. " +
                "Every claim scans the whole table under UPDLOCK on every poll tick — performance only; create the dequeue index " +
                "over (queue, available_at, created_at) as described in docs/sqlserver.md.",
                _options.SchemaName,
                _options.MessageTable);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger?.LogDebug(ex, "Skipping the dequeue-index check for the operator-managed SQL Server transport table; the catalog could not be read.");
        }
    }

    /// <summary>The catalog shape this store expects — the single source for the post-DDL
    /// verification, the failed-batch diagnosis, and the operator-provisioned check.</summary>
    /// <remarks>
    /// A bare <c>datetime2</c> declaration is <c>datetime2(7)</c>; the expected types state the
    /// scale, because a reduced-scale column rounds <c>available_at</c>/<c>locked_until</c> on
    /// store — a claim lease that rounds backwards is already expired when it is written.
    /// <para>
    /// <paramref name="selfCreated"/> distinguishes a table this build's DDL created — where any
    /// drift means somebody ALTERed it, so the queue column is held to the exact declared shape —
    /// from an operator-provisioned one, where the queue column's type and collation are
    /// deliberately unconstrained: <see cref="ExactQueueMatch"/> supplies the binary collation in
    /// the query itself and its sentinel concat defeats trailing-space padding, so every logical
    /// queue is told apart exactly whatever string type the migration chose and whatever collation
    /// the column carries. Every other column keeps its expectation on both paths: a wrong
    /// <c>payload_json</c>, <c>headers_json</c>, or timestamp shape breaks inserts or reorders the
    /// timestamps this store compares no matter who created the table.
    /// </para>
    /// </remarks>
    private SqlServerRelationVerifier.ExpectedObject[] ExpectedObjects(bool selfCreated) =>
            [
                new(_options.MessageTable, SqlServerObjectKind.Table,
                [
                    new("id", "uniqueidentifier", Nullable: false),
                    selfCreated
                        ? new("queue", "nvarchar(200)", Nullable: false, RequiresBinaryCollation: true)
                        : new("queue", Type: null, Nullable: false),
                    new("payload_json", "nvarchar(max)", Nullable: false),
                    new("headers_json", "nvarchar(max)", Nullable: false, DefaultExpression: "(N'{}')"),
                    new("created_at", "datetime2(7)", Nullable: false, DefaultExpression: "(sysutcdatetime())"),
                    new("available_at", "datetime2(7)", Nullable: false, DefaultExpression: "(sysutcdatetime())"),
                    new("locked_until", "datetime2(7)", Nullable: true),
                    new("lock_id", "uniqueidentifier", Nullable: true),
                    new("attempts", "int", Nullable: false, DefaultExpression: "((0))"),
                    new("dead_letter_reason", "nvarchar(max)", Nullable: true)
                ],
                PrimaryKey: ["id"]),
                // Only on the table this build created (PostgreSQL-sibling parity): the DDL's
                // index guard is name-only, so a pre-existing same-name index with the WRONG
                // definition silently suppressed the CREATE and cost the claim its seek. An
                // operator-owned table keeps its own indexing strategy — the same philosophy as
                // the unconstrained queue column — because indexes are claim performance, not
                // correctness.
                .. selfCreated
                    ? (SqlServerRelationVerifier.ExpectedObject[])
                    [
                        new(IndexName(_options.MessageTable, "ready"), SqlServerObjectKind.Index,
                            OwningTable: _options.MessageTable, KeyColumns: ["queue", "available_at", "created_at"]),
                        new(IndexName(_options.MessageTable, "created"), SqlServerObjectKind.Index,
                            OwningTable: _options.MessageTable, KeyColumns: ["created_at"])
                    ]
                    : []
            ];

    /// <summary>
    /// Publishes a queue row. The caller supplies the id so a retried publish is idempotent
    /// (insert-if-absent) rather than inserting a duplicate job.
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
        // (a 1205 deadlock victim, a 1222 lock timeout, the caller's token firing mid-DELETE)
        // reported a FAILED publish for a job that is already claimable — and the transient-fault
        // retry then re-inserted it once a subscriber had consumed and deleted the first copy: one
        // job, run twice. The prune swallows its own failures (see DbDeadLetterPrune).
        await DbDeadLetterPrune.RunIfDueAsync(
            ref _lastDeadLetterPruneStamp,
            _options.DeadLetterRetention,
            PruneDeadLetterBatchAsync,
            _logger,
            "SQL Server",
            cancellationToken).ConfigureAwait(false);
    }

    public async Task<SqlServerTransportDelivery?> TryClaimAsync(string queue, TimeSpan lockTimeout, CancellationToken cancellationToken)
    {
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        var lockId = Guid.NewGuid();

        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        // READPAST skips rows other subscribers hold UPDLOCK on — SQL Server's equivalent of
        // PostgreSQL's FOR UPDATE SKIP LOCKED — so competing consumers never block on each other.
        // Ready order is AVAILABILITY order — (available_at, created_at), the dequeue index's own
        // key order behind the queue equality — so the scan returns its first unleased row without
        // sorting. It equals publish order for every row that was neither delayed nor NAKed (both
        // columns default to the same SYSUTCDATETIME()); a delayed or redelivered row queues by
        // when it became due instead of jumping ahead of everything published while it waited.
        command.CommandText =
            $"""
            WITH next AS (
                SELECT TOP (1) id, payload_json, headers_json, attempts, locked_until, lock_id
                FROM {MessageTable} WITH (UPDLOCK, ROWLOCK, READPAST)
                WHERE {ExactQueueMatch}
                  AND available_at <= SYSUTCDATETIME()
                  AND (locked_until IS NULL OR locked_until <= SYSUTCDATETIME())
                ORDER BY available_at, created_at
            )
            UPDATE next
            SET attempts = attempts + 1,
                locked_until = {AddMilliseconds("@lock_timeout_ms")},
                lock_id = @lock_id
            OUTPUT inserted.id, inserted.payload_json, inserted.headers_json, inserted.attempts;
            """;
        command.Parameters.AddWithValue("@queue", queue);
        command.Parameters.AddWithValue("@lock_timeout_ms", (long)lockTimeout.TotalMilliseconds);
        command.Parameters.AddWithValue("@lock_id", lockId);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            return null;

        var id = reader.GetGuid(0);
        var payload = reader.GetString(1);
        var headerJson = reader.GetString(2);
        var attempt = reader.GetInt32(3);
        var headers = DeserializeHeaders(headerJson);

        // The claim predicate matches the queue exactly (see ExactQueueMatch), so the claimed row's
        // queue IS the requested one — no post-claim re-check, and therefore no row that gets
        // claimed, rejected, and released back to the head of the same ordering on every poll.
        return new SqlServerTransportDelivery(
            id,
            queue,
            payload,
            headers,
            attempt,
            () => AckAsync(id, lockId),
            delay => NakAsync(id, lockId, delay),
            (exception, deleteOriginal, token) => DeadLetterAsync(id, lockId, queue, payload, headers, exception, deleteOriginal, token),
            token => RenewLeaseAsync(id, lockId, lockTimeout, token));
    }

    public async IAsyncEnumerable<SqlServerTransportDelivery> ClaimBatchAsync(
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
        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        // Insert-if-absent keeps a retried publish idempotent. The UPDLOCK/HOLDLOCK hints make the
        // existence check and the insert atomic; a concurrent same-id insert that still slips through
        // surfaces as a duplicate-key error, which is treated as success below.
        // Native delayed delivery: available_at gates the claim query, computed on the DATABASE
        // clock (SYSUTCDATETIME + delay) so client clock skew cannot shift the due time.
        command.CommandText =
            delay is null
                ? $"""
                  INSERT INTO {MessageTable} (id, queue, payload_json, headers_json, dead_letter_reason)
                  SELECT @id, @queue, @payload_json, @headers_json, @dead_letter_reason
                  WHERE NOT EXISTS (SELECT 1 FROM {MessageTable} WITH (UPDLOCK, HOLDLOCK) WHERE id = @id);
                  """
                : $"""
                  INSERT INTO {MessageTable} (id, queue, payload_json, headers_json, dead_letter_reason, available_at)
                  SELECT @id, @queue, @payload_json, @headers_json, @dead_letter_reason, {AddMilliseconds("@available_delay_ms")}
                  WHERE NOT EXISTS (SELECT 1 FROM {MessageTable} WITH (UPDLOCK, HOLDLOCK) WHERE id = @id);
                  """;
        command.Parameters.AddWithValue("@id", id);
        command.Parameters.AddWithValue("@queue", queue);
        command.Parameters.AddWithValue("@payload_json", payload);
        command.Parameters.AddWithValue("@headers_json", AsyncResponseJson.Serialize(headers ?? EmptyHeaders));
        command.Parameters.AddWithValue("@dead_letter_reason", (object?)deadLetterReason ?? DBNull.Value);
        if (delay is { } pending)
            command.Parameters.AddWithValue("@available_delay_ms", (long)pending.TotalMilliseconds);

        try
        {
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (SqlException ex) when (ex.Number is PrimaryKeyViolation or UniqueIndexViolation)
        {
        }

        if (notify)
            MessagePublished?.Invoke(queue);
    }

    private async ValueTask AckAsync(Guid id, Guid lockId)
    {
        await using var connection = await OpenConnectionAsync(CancellationToken.None).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = $"DELETE FROM {MessageTable} WHERE id = @id AND lock_id = @lock_id;";
        command.Parameters.AddWithValue("@id", id);
        command.Parameters.AddWithValue("@lock_id", lockId);
        await command.ExecuteNonQueryAsync(CancellationToken.None).ConfigureAwait(false);
    }

    // The token is the heartbeat's per-attempt bound: a renew hung on a black-holed pooled
    // connection, a failover, or a lock wait inherited SqlClient's 30 s command timeout, which
    // outlasted the 20 s of lease left after the beat — the lease lapsed before the short-backoff
    // retry ever ran. CommandTimeout is the backstop for a cancellation whose attention packet
    // itself hangs on a dead socket.
    private async ValueTask<bool> RenewLeaseAsync(Guid id, Guid lockId, TimeSpan lockTimeout, CancellationToken cancellationToken)
    {
        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandTimeout = RenewCommandTimeoutSeconds(lockTimeout);
        // The row count comes from the SERVER (@@ROWCOUNT, which SET NOCOUNT does not affect), not
        // from ExecuteNonQuery: under a server-wide `user options` NOCOUNT (sp_configure 512) that
        // returns -1, every successful renew read as "lease lost", the heartbeat stopped after its
        // first beat, and a handler longer than the lease ran twice concurrently. (The dead-letter
        // batch below reads its outcome the same way.)
        command.CommandText =
            $"""
            UPDATE {MessageTable}
            SET locked_until = {AddMilliseconds("@lock_timeout_ms")}
            WHERE id = @id AND lock_id = @lock_id;
            SELECT @@ROWCOUNT;
            """;
        command.Parameters.AddWithValue("@id", id);
        command.Parameters.AddWithValue("@lock_id", lockId);
        command.Parameters.AddWithValue("@lock_timeout_ms", (long)lockTimeout.TotalMilliseconds);
        return await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) is int renewed && renewed > 0;
    }

    private async ValueTask NakAsync(Guid id, Guid lockId, TimeSpan delay)
    {
        await using var connection = await OpenConnectionAsync(CancellationToken.None).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText =
            $"""
            UPDATE {MessageTable}
            SET available_at = {AddMilliseconds("@delay_ms")},
                locked_until = NULL,
                lock_id = NULL
            WHERE id = @id AND lock_id = @lock_id;
            """;
        command.Parameters.AddWithValue("@id", id);
        command.Parameters.AddWithValue("@lock_id", lockId);
        command.Parameters.AddWithValue("@delay_ms", (long)delay.TotalMilliseconds);
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
            await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
            await using var transaction = (SqlTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
            await using var command = connection.CreateCommand();
            command.Transaction = transaction;
            // Delete FIRST and write the DLQ row only if the fence matched. A stale claim (the lease
            // lapsed and a peer re-claimed the row) must no-op here exactly as the fenced ack and
            // NAK do; writing the row unconditionally buried a full copy of a message that is still
            // live and may yet succeed under its new owner, so the DLQ showed a poison entry for
            // work that completed — and an operator replaying it duplicated its side effects.
            command.CommandText =
                $"""
                SET NOCOUNT ON;
                DELETE FROM {MessageTable} WHERE id = @source_id AND lock_id = @lock_id;
                IF @@ROWCOUNT = 1
                BEGIN
                    INSERT INTO {MessageTable} (id, queue, payload_json, headers_json, dead_letter_reason)
                    VALUES (@id, @queue, @payload_json, @headers_json, @dead_letter_reason);
                    SELECT 1;
                END
                ELSE
                    SELECT 0;
                """;
            command.Parameters.AddWithValue("@id", Guid.NewGuid());
            command.Parameters.AddWithValue("@queue", _options.DeadLetterQueue);
            command.Parameters.AddWithValue("@payload_json", payload);
            command.Parameters.AddWithValue("@headers_json", AsyncResponseJson.Serialize(deadHeaders));
            command.Parameters.AddWithValue("@dead_letter_reason", exception.Message);
            command.Parameters.AddWithValue("@source_id", id);
            command.Parameters.AddWithValue("@lock_id", lockId);
            var buried = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) is int and 1;
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);

            // Zero means the fence was lost, not that the write failed. Report it as a
            // non-dead-letter so the caller does not log a burial that did not happen; its NAK
            // fallback is fenced too, so the new owner keeps the row untouched.
            if (!buried)
            {
                _logger?.LogWarning(
                    "SQL Server dead-letter for message {MessageId} from queue {SourceQueue} no-opped: the claim's lease had lapsed and the row was re-claimed.",
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
                "Failed to write SQL Server dead-letter row for message {MessageId} from queue {SourceQueue}.",
                id,
                sourceQueue);
            return false;
        }
    }

    /// <summary>
    /// One bounded batch of the dead-letter retention prune (<see cref="DbDeadLetterPrune"/> throttles,
    /// drains, and guards it). Bounded (SQL Server channel parity): the dead-letter rows share the
    /// queue table with live claims, and an unbounded DELETE over a backlog past SQL Server's
    /// ~5,000-lock escalation threshold takes a table X lock that READPAST cannot skip — every
    /// claim, ACK and lease renewal blocked behind it for up to the command timeout, which is the
    /// whole LockTimeout, so a live handler's lease lapsed and a peer re-ran its job concurrently.
    /// Each batch is its own autocommit statement, so looping batches stays under escalation; the
    /// loop is what lifts the old one-batch-per-window ceiling (1,000 rows a minute, which a poison
    /// storm outgrew while retention was set). The count it returns is what ends the drain, so the
    /// batch turns the row count back on for itself (<c>SqlServerDurableFlows.RowCountOn</c>
    /// parity): under a server-wide NOCOUNT (<c>sp_configure 'user options', 512</c>)
    /// <c>ExecuteNonQuery</c> returned -1 and every drain stopped after its first batch.
    /// </summary>
    private async Task<int> PruneDeadLetterBatchAsync(TimeSpan retention, CancellationToken cancellationToken)
    {
        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = $"SET NOCOUNT OFF; DELETE TOP ({OpportunisticPrune.BatchSize}) FROM {MessageTable} WHERE {ExactQueueMatch} AND created_at < {AddMilliseconds("@negative_retention_ms")};";
        command.Parameters.AddWithValue("@queue", _options.DeadLetterQueue);
        command.Parameters.AddWithValue("@negative_retention_ms", -(long)retention.TotalMilliseconds);
        return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private async Task<SqlConnection> OpenConnectionAsync(CancellationToken cancellationToken)
    {
        var connection = new SqlConnection(_connectionString);
        try
        {
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            return connection;
        }
        catch
        {
            await connection.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    // Lenient by contract (see DbTransportHeaders): this runs after the claim already committed
    // attempts+1/lock_id, so rejecting any content the nvarchar column legally holds would create
    // an unkillable poison row.
    private static IReadOnlyDictionary<string, string> DeserializeHeaders(string json)
        => DbTransportHeaders.Materialize(json);

    private static string Sanitize(string value) => value.Replace('\r', ' ').Replace('\n', ' ');

    /// <summary>
    /// The renew command's <c>CommandTimeout</c>: the heartbeat's per-attempt bound
    /// (<c>LockTimeout</c>/3) rounded UP to whole seconds (CommandTimeout's unit, where 0 would mean
    /// "no limit"), so the backstop never fires before the token does.
    /// </summary>
    internal static int RenewCommandTimeoutSeconds(TimeSpan lockTimeout)
        => (int)Math.Clamp(Math.Ceiling(lockTimeout.TotalSeconds / 3), 1, int.MaxValue);

    /// <summary>
    /// SQL expression adding a millisecond bigint parameter to the database clock. DATEADD only takes
    /// int arguments, so the value is split into whole seconds and a sub-second remainder — intervals
    /// (lock timeouts, redelivery delays, retentions) stay on the database clock, immune to app-side
    /// clock skew, without overflowing on long spans.
    /// </summary>
    internal static string AddMilliseconds(string parameterName)
        => $"DATEADD(SECOND, CAST({parameterName} / 1000 AS int), DATEADD(MILLISECOND, CAST({parameterName} % 1000 AS int), SYSUTCDATETIME()))";

    /// <summary>
    /// Stable application-lock resource for serializing schema creation. Must be byte-for-byte
    /// identical to the channel store's resource so that, for a shared schema, the channel and
    /// transport take the same lock and never race each other on CREATE SCHEMA.
    /// </summary>
    internal static string SchemaLockResource(string schemaName)
        => $"asyncresponse:ddl:{schemaName}";

    private static string Quote(string identifier) => "[" + identifier + "]";

    // Suffix space is RESERVED before capping; see RelationalNamePlan.DerivedName for why and for
    // the single implementation this and the PostgreSQL / channel stores all share.
    internal static string IndexName(string table, string suffix)
        => RelationalNamePlan.DerivedName(table, $"_{suffix}_idx", identifierCap: 128);

    private static readonly IReadOnlyDictionary<string, string> EmptyHeaders =
        new Dictionary<string, string>(0, StringComparer.OrdinalIgnoreCase);
}
