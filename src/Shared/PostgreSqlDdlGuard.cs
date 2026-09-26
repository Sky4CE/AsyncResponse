using Microsoft.Extensions.Logging;
using Npgsql;
using System.Text;

namespace AsyncResponse.Internal;

/// <summary>
/// The bounds every AsyncResponse PostgreSQL store (channel, transport, durable-flow) runs its
/// auto-create DDL under, and the retry-after latch that paces a failed attempt. Source-linked
/// into the three packages (separate packages cannot share compiled code); each store owns one
/// instance, so its latch is per store.
/// <para>
/// Startup DDL is what every operation runs first until it succeeds once, so an attempt that
/// waits or fails is paid by every operation on the host — and while its lock request waits,
/// every later statement on the table, from every host, queues behind it. Hence:
/// every DDL transaction bounds its lock waits (the schema's advisory DDL key included) with
/// <see cref="DdlLockTimeout"/> (<see cref="BeginLockedTransactionAsync"/>); DDL whose run time
/// grows with the table runs under <see cref="LongRunningDdlCommandTimeoutSeconds"/> instead of the
/// data source's timeout (<see cref="ExecuteLongRunningAsync"/>), in its own transaction under a
/// key scoped to its table (<see cref="TableLockKey"/>) rather than the schema-wide key the other
/// stores take; only objects the catalog shows absent are created
/// (<see cref="GetIndexStateAsync"/>), since <c>ALTER TABLE … IF NOT EXISTS</c> and
/// <c>CREATE INDEX IF NOT EXISTS</c> take their table lock before they find the object present;
/// and a lock-wait timeout or any failure of the long-running DDL latches a jittered retry-after
/// window (<see cref="BackOff"/>) inside which the store's operations fail at once
/// (<see cref="ThrowIfBackingOff"/>).
/// </para>
/// </summary>
internal sealed class PostgreSqlDdlGuard
{
    /// <summary>
    /// The lock-wait bound of every auto-create DDL transaction (<c>SET LOCAL lock_timeout</c>). While
    /// a jsonb rewrite's ACCESS EXCLUSIVE request — or an index build's SHARE request, or an
    /// <c>ADD COLUMN</c>'s ACCESS EXCLUSIVE one — waits, every later statement on the table queues
    /// behind it, on every host, so the wait IS an outage of that table. 5 s outwaits the stores' own
    /// statements (single-statement, sub-second) and an ORDINARY autovacuum, which cancels itself for
    /// a conflicting lock request after <c>deadlock_timeout</c> (1 s by default), and stays under the
    /// transport's lease-renewal beat (<c>LockTimeout</c>/3, 10 s by default), so no running
    /// handler's lease lapses behind it. What does not yield — an anti-wraparound autovacuum, a
    /// manual VACUUM or ANALYZE, pg_dump, an idle-in-transaction session — fails the DDL fast
    /// instead, changing nothing, and <see cref="RetryAfter"/> paces the retries. It bounds the
    /// wait for the advisory DDL key too, which another host holds while it runs its own DDL.
    /// </summary>
    internal const string DdlLockTimeout = "5s";

    /// <summary>
    /// The shortest retry-after window a failed attempt latches; the actual window is jittered up to
    /// twice this (30–60 s). Without it every operation on the host started the next attempt at
    /// once, and a lock held for minutes or hours (the holders named on <see cref="DdlLockTimeout"/>)
    /// stalled the table — every host's statements, parked behind each attempt's lock request — in
    /// back-to-back 5 s cycles, the k-th caller queued on the store's gate waiting k × 5 s to fail;
    /// and a long-running step that failed for another reason (a <c>statement_timeout</c> it
    /// outran, a dependent view, a full disk) re-ran, lock and all, on every operation. 30 s keeps
    /// one host's attempts to a small fraction of the table's time; the jitter keeps a fleet started
    /// together from retrying in lockstep (the advisory lock would chain their waits back to back),
    /// and the 60 s ceiling bounds how long this host stays unable to operate once the cause is gone.
    /// </summary>
    internal static readonly TimeSpan RetryAfter = TimeSpan.FromSeconds(30);

    /// <summary>
    /// The command timeout of DDL whose run time grows with the table — a jsonb → text rewrite, the
    /// first build of an index on an existing table — in seconds. Not the data source's (30 s by
    /// default): each is one-time, and one that outran it was rolled back and retried by EVERY later
    /// operation (EnsureCreated runs until it succeeds), each attempt holding its ACCESS EXCLUSIVE
    /// or SHARE lock for another full timeout and never finishing. Not unbounded either: Npgsql
    /// sends no keepalive by default, so a socket black-holed mid-rewrite (a failover to a new
    /// address, a NAT or load-balancer drop) would leave it waiting forever, with every later
    /// operation on the host waiting behind it. An hour is an order of magnitude past what these
    /// tables' rewrites take; a larger one is converted ahead of the rollout (docs/postgresql.md).
    /// </summary>
    internal const int LongRunningDdlCommandTimeoutSeconds = 3600;

    private readonly string _componentName;
    private readonly string _subject;
    private readonly string _docs;
    private readonly ILogger? _logger;
    private Backoff? _backoff;

    /// <param name="componentName">"channel", "transport" or "durable-flow", as the store names itself.</param>
    /// <param name="subject">What the DDL is about, for messages: a quoted table, or the component's tables in a schema.</param>
    /// <param name="docs">The docs page describing the store's DDL.</param>
    /// <param name="logger">The store's logger.</param>
    public PostgreSqlDdlGuard(string componentName, string subject, string docs, ILogger? logger)
    {
        _componentName = componentName;
        _subject = subject;
        _docs = docs;
        _logger = logger;
    }

    /// <summary>The clock of the retry-after window (test seam).</summary>
    internal TimeProvider Clock { get; set; } = TimeProvider.System;

    /// <summary>
    /// Opens a DDL transaction holding the advisory key <paramref name="lockKey"/>, with every lock
    /// wait in it bounded by <see cref="DdlLockTimeout"/>. The bound is set FIRST: <c>lock_timeout</c>
    /// applies to advisory locks too, and the key's holder may be another host running its own DDL —
    /// taken first, that wait was bounded only by the command timeout (30 s by default), after which
    /// the next operation queued for it again.
    /// </summary>
    public static async Task<NpgsqlTransaction> BeginLockedTransactionAsync(NpgsqlConnection connection, long lockKey, CancellationToken cancellationToken)
    {
        var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await using var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = $"SET LOCAL lock_timeout = '{DdlLockTimeout}';";
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

            command.CommandText = "SELECT pg_advisory_xact_lock(@lock_key);";
            command.Parameters.AddWithValue("lock_key", lockKey);
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            return transaction;
        }
        catch
        {
            await transaction.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>
    /// The advisory key serializing schema-object creation across processes and across the three
    /// stores: <c>CREATE … IF NOT EXISTS</c> is not atomic against a concurrent create of the same
    /// object, so two instances starting together both pass the existence check and collide on the
    /// system catalog ("duplicate key … pg_type_typname_nsp_index"). Held only for DDL whose run time
    /// does not grow with a table (see <see cref="TableLockKey"/>).
    /// </summary>
    public static long SchemaLockKey(string schemaName) => LockKey($"asyncresponse:ddl:{schemaName}");

    /// <summary>
    /// The advisory key under which one table's long-running DDL — the jsonb rewrite, an index build
    /// on an existing table — runs, in a transaction of its own after the schema-shared DDL has
    /// committed. Held for up to <see cref="LongRunningDdlCommandTimeoutSeconds"/>, so it must not be
    /// the schema-wide key: under that one every host starting meanwhile could not initialize ANY
    /// AsyncResponse store on the schema (the channel's and flow store's included, and other
    /// applications' with different table names) until the rewrite ended.
    /// </summary>
    public static long TableLockKey(string schemaName, string table) => LockKey($"asyncresponse:ddl:{schemaName}.{table}");

    // FNV-1a over the UTF-8 resource name: deterministic across processes (string.GetHashCode is
    // randomized per process) and identical in every package that compiles this file.
    private static long LockKey(string resource)
    {
        const ulong offset = 14695981039346656037UL;
        const ulong prime = 1099511628211UL;
        var hash = offset;
        foreach (var b in Encoding.UTF8.GetBytes(resource))
        {
            hash ^= b;
            hash *= prime;
        }

        return unchecked((long)hash);
    }

    /// <summary>A command for DDL whose run time grows with the table (see <see cref="LongRunningDdlCommandTimeoutSeconds"/>).</summary>
    public static NpgsqlCommand LongRunningDdlCommand(string sql, NpgsqlConnection? connection = null, NpgsqlTransaction? transaction = null)
        => new(sql, connection, transaction) { CommandTimeout = LongRunningDdlCommandTimeoutSeconds };

    /// <summary>
    /// Runs DDL whose run time grows with the table through <see cref="LongRunningDdlCommand"/>. Lock
    /// WAITS stay bounded by the transaction's <see cref="DdlLockTimeout"/>; a <c>statement_timeout</c>
    /// the operator configured still applies (overriding it is not the library's call). ANY server
    /// error it raises latches the retry-after window, not just a lock timeout: a rewrite that outran
    /// a role's <c>statement_timeout</c> (57014), one blocked by a view, rule or index on the column
    /// (0A000), a full disk (53100) — each held its ACCESS EXCLUSIVE or SHARE lock for as long as it
    /// ran, and without the latch every later operation of every host re-ran it at once. A caller's
    /// cancellation surfaces as <see cref="OperationCanceledException"/>, not a server error, and
    /// latches nothing; nor does a name collision (42809 wrong object type, 42703 undefined column —
    /// the statement hit another component's relation), which fails at once without holding the
    /// table, and which the store reports as the configuration error it is on every attempt; nor a
    /// transient fault that ends the statement at once — a deadlock, a serialization failure, a
    /// server shutdown, a broken connection (see <see cref="LatchesOnFailure"/>).
    /// </summary>
    public async Task ExecuteLongRunningAsync(string sql, NpgsqlConnection connection, NpgsqlTransaction? transaction, CancellationToken cancellationToken)
    {
        await using var command = LongRunningDdlCommand(sql, connection, transaction);
        try
        {
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (PostgresException ex) when (LatchesOnFailure(ex))
        {
            BackOff(ex);
            throw;
        }
    }

    /// <summary>
    /// Whether a long-running step that failed with <paramref name="failure"/> latches the
    /// retry-after window: anything but a name collision or a transient fault that ends the
    /// statement as soon as the server detects it — a deadlock or serialization failure (class 40),
    /// an administrator or crash shutdown or a server not accepting connections yet (57P0x), a
    /// broken connection (class 08). Those (transient to the driver, <c>NpgsqlException.IsTransient</c>,
    /// the classification <c>PostgreSqlTransientFaults</c> wraps) are worth retrying at once;
    /// latching one cost the host 30–60 s of failed operations. The driver's other transient codes
    /// keep latching: a lock wait lost (55P03, the latch's reason) and what a step reaches only by
    /// running, holding its lock — a full disk or exhausted memory (class 53), an I/O error.
    /// </summary>
    internal static bool LatchesOnFailure(PostgresException failure)
        => failure.SqlState is not (PostgresErrorCodes.WrongObjectType or PostgresErrorCodes.UndefinedColumn)
           && !(failure.IsTransient
                && (failure.SqlState.StartsWith("40", StringComparison.Ordinal)
                    || failure.SqlState.StartsWith("57P", StringComparison.Ordinal)
                    || failure.SqlState.StartsWith("08", StringComparison.Ordinal)));

    /// <summary>Whether an index is absent, usable, or present but not valid and ready.</summary>
    public enum IndexState
    {
        Absent,
        Usable,
        NotReady
    }

    /// <summary>
    /// Whether an index is absent, usable, or present but not valid and ready (a
    /// <c>CREATE INDEX CONCURRENTLY</c> still running, or one that failed). A relation of another kind
    /// under the name counts as present, so verification names the precise wrong-kind reason instead
    /// of the DDL tripping over it.
    /// </summary>
    public static async Task<IndexState> GetIndexStateAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        string schemaName,
        string index,
        CancellationToken cancellationToken)
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
        command.Parameters.AddWithValue("schema", schemaName);
        command.Parameters.AddWithValue("index", index);
        return await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) switch
        {
            null or DBNull => IndexState.Absent,
            true => IndexState.Usable,
            _ => IndexState.NotReady
        };
    }

    /// <summary>
    /// Latches this store's retry-after window after <paramref name="cause"/> failed its startup DDL
    /// (see <see cref="RetryAfter"/>), logs it, and returns the window's length. Until it ends,
    /// <see cref="ThrowIfBackingOff"/> fails the store's operations at once — no connection, no
    /// transaction, no lock request queued ahead of the other hosts' statements. Latching the same
    /// failure twice (the long-running step, then the store's lock-timeout arm) keeps the first window.
    /// </summary>
    public TimeSpan BackOff(Exception cause)
    {
        if (Volatile.Read(ref _backoff) is { } current && ReferenceEquals(current.Cause, cause))
            return current.Window;

        var window = RetryAfter + TimeSpan.FromTicks(Random.Shared.NextInt64(RetryAfter.Ticks));
        Volatile.Write(ref _backoff, new Backoff(Clock.GetTimestamp(), window, cause));

        // After the latch, and never able to replace the failure being rethrown.
        if (_logger is { } logger)
        {
            if (IsLockTimeout(cause))
            {
                SafeLog.Try(() => logger.LogWarning(
                    cause,
                    "PostgreSQL {Component} startup DDL on {Subject} could not take a lock within {LockTimeout} " +
                    "(another session holds a conflicting lock); nothing was changed. This host retries it in {RetryAfter}, " +
                    "and the store's operations fail at once until then.",
                    _componentName,
                    _subject,
                    DdlLockTimeout,
                    window));
            }
            else
            {
                SafeLog.Try(() => logger.LogWarning(
                    cause,
                    "PostgreSQL {Component} startup DDL on {Subject} failed and was rolled back; the usual causes are a " +
                    "statement_timeout the one-time conversion or index build outran, a view, rule or index depending on a " +
                    "column being converted, or a full disk. This host retries it in {RetryAfter}, and the store's " +
                    "operations fail at once until then.",
                    _componentName,
                    _subject,
                    window));
            }
        }

        return window;
    }

    /// <summary>Throws while a retry-after window latched by <see cref="BackOff"/> is open.</summary>
    public void ThrowIfBackingOff()
    {
        if (Volatile.Read(ref _backoff) is not { } backoff)
            return;

        var remaining = backoff.Window - Clock.GetElapsedTime(backoff.StartedAt);
        if (remaining <= TimeSpan.Zero)
            return;

        var retry = Math.Ceiling(remaining.TotalSeconds).ToString("0", System.Globalization.CultureInfo.InvariantCulture);
        if (IsLockTimeout(backoff.Cause))
        {
            throw new InvalidOperationException(
                $"The PostgreSQL {_componentName} store's startup DDL on {_subject} could not take its lock within " +
                $"{DdlLockTimeout}: another session holds a conflicting lock on the table (an anti-wraparound or manual VACUUM, " +
                "ANALYZE, pg_dump, or a long-running or idle-in-transaction session — find it in pg_locks joined to " +
                "pg_stat_activity), or another host is running AsyncResponse startup DDL on the schema or the table. Nothing " +
                $"was changed. This host retries it in {retry} s — every attempt parks every statement on the table, on every " +
                $"host, behind its lock request — and its {_componentName} operations fail until the DDL succeeds; see {_docs}.",
                backoff.Cause);
        }

        throw new InvalidOperationException(
            $"The PostgreSQL {_componentName} store's startup DDL on {_subject} failed and was rolled back (the inner " +
            "exception is the server's error): a statement_timeout set for the role or database that the one-time jsonb " +
            "conversion or index build outran, a view, rule or index that depends on a column being converted, or a full " +
            $"disk. This host retries it in {retry} s — each attempt holds its ACCESS EXCLUSIVE or SHARE lock on the table, " +
            $"blocking every statement on it, for as long as it runs — and its {_componentName} operations fail until the " +
            $"DDL succeeds; see {_docs}.",
            backoff.Cause);
    }

    private static bool IsLockTimeout(Exception cause)
        => cause is PostgresException { SqlState: PostgresErrorCodes.LockNotAvailable };

    private sealed record Backoff(long StartedAt, TimeSpan Window, Exception Cause);
}
