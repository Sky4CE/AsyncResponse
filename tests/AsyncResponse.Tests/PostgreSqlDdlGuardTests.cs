using System.Reflection;
using AsyncResponse.Channels.PostgreSQL;
using AsyncResponse.DurableFlows.PostgreSQL;
using AsyncResponse.Testing;
using AsyncResponse.Transports.PostgreSQL;
using Microsoft.Extensions.Options;
using Npgsql;
using Xunit;
using static AsyncResponse.Tests.SqlServerTransportStorePruneTests;

namespace AsyncResponse.Tests;

/// <summary>
/// <c>src/Shared/PostgreSqlDdlGuard.cs</c> (fixpoint r2): the bounds all three PostgreSQL stores run
/// their auto-create DDL under — the transport's round-43 hardening, which the channel (S5#2, S5#11)
/// and the durable-flow store (S10#2) lacked — and the retry-after latch, which now also catches a
/// long-running step's non-lock failure (S9#2). The file is source-linked into the channel,
/// transport and durable-flow packages, so every fact runs against all three compiled copies,
/// reached by reflection (the internal type shares one full name across the three assemblies).
/// Behaviour against a real server is pinned by PostgreSqlDirectIntegrationTests.
/// </summary>
public sealed class PostgreSqlDdlGuardTests
{
    public enum Package
    {
        Channel,
        Transport,
        DurableFlow
    }

    private static readonly TimeSpan HangGuard = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Regression (S5#11 and S10#2's "lock_timeout BEFORE the advisory lock"): the channel and flow
    /// store took the schema's advisory DDL key with no lock bound at all, and the transport set its
    /// lock_timeout only after taking it — so a host whose key was held by another host's DDL waited
    /// the whole 30 s command timeout, failed unlatched, and the next operation queued for it again.
    /// Every store now opens its DDL transactions through the guard, whose first statement bounds
    /// every lock wait in the transaction, the advisory key's included.
    /// </summary>
    [Theory]
    [InlineData(Package.Channel)]
    [InlineData(Package.Transport)]
    [InlineData(Package.DurableFlow)]
    public void EveryStoresDdl_BoundsTheAdvisoryKeyWaitBeforeTakingIt(Package package)
    {
        var guard = GuardType(package);
        var opening = Literals(AsyncBody(guard, "BeginLockedTransactionAsync")).ToList();
        var bound = opening.FindIndex(literal => literal == "SET LOCAL lock_timeout = '5s';");
        var key = opening.FindIndex(literal => literal.Contains("pg_advisory_xact_lock(@lock_key)", StringComparison.Ordinal));
        Assert.True(bound >= 0 && key > bound, string.Join(" | ", opening));

        var (store, ddlMethod) = DdlMethod(package);
        var calls = Decode(AsyncBody(store, ddlMethod)).Select(instruction => instruction.Operand).OfType<MethodBase>().ToArray();
        Assert.Contains(calls, method => method.Name == "BeginLockedTransactionAsync" && method.DeclaringType == guard);
        Assert.DoesNotContain(
            Literals(AsyncBody(store, ddlMethod)),
            literal => literal.Contains("pg_advisory_xact_lock", StringComparison.Ordinal));
    }

    /// <summary>
    /// Regression (S5#2): the channel ran <c>ALTER TABLE … ADD COLUMN IF NOT EXISTS</c> twice and
    /// <c>CREATE INDEX IF NOT EXISTS</c> four times on every process start. PostgreSQL takes the
    /// ACCESS EXCLUSIVE (ALTER) and SHARE (CREATE INDEX) lock before it checks IF NOT EXISTS, so a host
    /// starting while anything held a conflicting lock on the table (pg_dump, an idle-in-transaction
    /// reader, an anti-wraparound vacuum) queued behind it, with every channel statement of every host
    /// queued behind that request. The creates carry no ALTER or index statement any more; the table
    /// work is assembled only from what the catalog shows missing, and runs as long-running DDL.
    /// </summary>
    [Fact]
    public void ChannelDdl_AltersAndIndexesOnlyWhatTheCatalogShowsMissing()
    {
        var ddl = Literals(AsyncBody(typeof(PostgreSqlChannelSql), nameof(PostgreSqlChannelSql.EnsureCreatedAsync)));
        Assert.Contains(ddl, literal => literal.Contains("CREATE TABLE IF NOT EXISTS", StringComparison.Ordinal));
        Assert.DoesNotContain(ddl, literal => literal.Contains("ALTER TABLE", StringComparison.Ordinal));
        Assert.DoesNotContain(ddl, literal => literal.Contains("CREATE INDEX", StringComparison.Ordinal));
        Assert.DoesNotContain(ddl, literal => literal.Contains("DO $$", StringComparison.Ordinal));

        var calls = Decode(AsyncBody(typeof(PostgreSqlChannelSql), nameof(PostgreSqlChannelSql.EnsureCreatedAsync)))
            .Select(instruction => instruction.Operand).OfType<MethodBase>().Select(method => method.Name).ToArray();
        Assert.Contains("ExecuteLongRunningAsync", calls);

        // The table work reads the catalog first: the two columns, the two jsonb columns, each index.
        var work = Decode(AsyncBody(typeof(PostgreSqlChannelSql), "ReadTableWorkAsync")).ToArray();
        var workLiterals = work.Where(instruction => instruction.Operand is string).Select(instruction => (string)instruction.Operand!).ToArray();
        Assert.Contains(workLiterals, literal => literal.Contains("column_name = 'recovery_claimed'", StringComparison.Ordinal)
            && literal.Contains("column_name = 'acked_seq'", StringComparison.Ordinal)
            && literal.Contains("column_name = 'envelope_json' AND data_type = 'jsonb'", StringComparison.Ordinal)
            && literal.Contains("column_name = 'state_json' AND data_type = 'jsonb'", StringComparison.Ordinal));
        Assert.Contains(work.Select(instruction => instruction.Operand).OfType<MethodBase>(), method => method.Name == "GetIndexStateAsync");
    }

    /// <summary>
    /// Regression (S10#2): the flow store ran <c>CREATE INDEX IF NOT EXISTS {table}_expires_idx</c> on
    /// every process start, taking its SHARE lock before finding the name taken — behind an
    /// operator's <c>CREATE INDEX CONCURRENTLY</c>, an anti-wraparound vacuum of this update-heavy
    /// table, or an idle-in-transaction checkpoint writer, with every checkpoint, lease acquire and
    /// renewal of every host queued behind it. Its jsonb rewrite ran under the 30 s default too. The
    /// index is built only when the catalog shows it absent, both as long-running DDL.
    /// </summary>
    [Fact]
    public void FlowStoreDdl_BuildsTheIndexOnlyWhenAbsent_AsLongRunningDdl()
    {
        var ddl = Literals(AsyncBody(typeof(PostgreSqlFlowStateStore), "EnsureCreatedAsync"));
        var creates = ddl.Where(literal => literal.Contains("lease_expires_at_utc timestamptz NULL", StringComparison.Ordinal)).ToArray();
        Assert.NotEmpty(creates);
        Assert.DoesNotContain(creates, literal => literal.Contains("CREATE INDEX", StringComparison.Ordinal));
        Assert.DoesNotContain(ddl, literal => literal.Contains("DO $$", StringComparison.Ordinal));

        var calls = Decode(AsyncBody(typeof(PostgreSqlFlowStateStore), "EnsureCreatedAsync"))
            .Select(instruction => instruction.Operand).OfType<MethodBase>().Select(method => method.Name).ToArray();
        Assert.Contains("ExecuteLongRunningAsync", calls);
        Assert.Contains(
            Decode(AsyncBody(typeof(PostgreSqlFlowStateStore), "ReadTableWorkAsync")).Select(instruction => instruction.Operand).OfType<MethodBase>(),
            method => method.Name == "GetIndexStateAsync");
    }

    /// <summary>
    /// Regression (S9#2, S11#2): only a lock timeout (55P03) latched the retry-after window, so a
    /// long-running step that failed any other way — a rewrite that outran a role's
    /// <c>statement_timeout</c> (57014), one a dependent view blocks (0A000), a full disk — was re-run
    /// by the next operation at once, each attempt holding its ACCESS EXCLUSIVE or SHARE lock on the
    /// table for as long as it ran. Any server error of a long-running step now latches the window.
    /// The fake server answers the statement with an error (55000); a name collision (42809/42703)
    /// is the one kind that does not latch, since the store reports it as such on every attempt.
    /// </summary>
    [Theory]
    [InlineData(Package.Channel)]
    [InlineData(Package.Transport)]
    [InlineData(Package.DurableFlow)]
    public async Task LongRunningDdl_AnyServerError_LatchesTheRetryAfter(Package package)
    {
        await using var server = new FakePostgresWireServer
        {
            Respond = (_, sql) => Task.FromResult(sql.StartsWith("ALTER TABLE", StringComparison.Ordinal)
                ? FakePostgresWireServer.Reply.Error("canceling statement due to statement timeout")
                : FakePostgresWireServer.Reply.Complete(sql))
        };
        await using var dataSource = NpgsqlDataSource.Create(server.ConnectionString());
        await using var connection = await dataSource.OpenConnectionAsync();
        var guard = new Guard(package);

        var failure = await Assert.ThrowsAsync<PostgresException>(
            () => guard.ExecuteLongRunningAsync("ALTER TABLE \"s\".\"t\" ALTER COLUMN c TYPE text USING c::text;", connection));

        var refused = Assert.Throws<InvalidOperationException>(guard.ThrowIfBackingOff);
        Assert.Same(failure, refused.InnerException);
        Assert.Contains("failed and was rolled back", refused.Message, StringComparison.Ordinal);
        Assert.Contains("statement_timeout", refused.Message, StringComparison.Ordinal);

        // A collision latches nothing: the store turns it into its own actionable error.
        var collisions = new Guard(package);
        foreach (var sqlState in new[] { PostgresErrorCodes.WrongObjectType, PostgresErrorCodes.UndefinedColumn })
        {
            Assert.False(collisions.LatchesOnLongRunningFailure(new PostgresException("collision", "ERROR", "ERROR", sqlState)));
        }

        Assert.True(collisions.LatchesOnLongRunningFailure(new PostgresException("dependent view", "ERROR", "ERROR", PostgresErrorCodes.FeatureNotSupported)));
    }

    /// <summary>
    /// Fixpoint r2 precommit (D residual): the long-running step latched every server error but a
    /// collision — a deadlock (40P01), a serialization failure (40001), an administrator shutdown
    /// (57P01) included, each ending the statement as soon as the server detected it — so a deadlock
    /// during the channel's one-time <c>ADD COLUMN</c> cost the host 30–60 s of failed operations
    /// instead of an immediate retry. Transient quick failures now latch nothing; the driver's other
    /// transient codes (a lost lock wait, a full disk, exhausted memory) and a
    /// <c>statement_timeout</c> still latch. Red on the old code: every transient row latched.
    /// </summary>
    [Theory]
    [InlineData(Package.Channel)]
    [InlineData(Package.Transport)]
    [InlineData(Package.DurableFlow)]
    public void LongRunningDdl_ATransientQuickFailure_LatchesNothing(Package package)
    {
        var guard = new Guard(package);
        foreach (var sqlState in new[]
                 {
                     PostgresErrorCodes.DeadlockDetected,
                     PostgresErrorCodes.SerializationFailure,
                     PostgresErrorCodes.AdminShutdown,
                     PostgresErrorCodes.CrashShutdown,
                     PostgresErrorCodes.CannotConnectNow,
                     PostgresErrorCodes.ConnectionFailure
                 })
        {
            Assert.False(guard.LatchesOnLongRunningFailure(new PostgresException("transient", "ERROR", "ERROR", sqlState)), sqlState);
        }

        foreach (var sqlState in new[]
                 {
                     PostgresErrorCodes.LockNotAvailable,
                     PostgresErrorCodes.DiskFull,
                     PostgresErrorCodes.OutOfMemory,
                     PostgresErrorCodes.QueryCanceled,
                     PostgresErrorCodes.ObjectNotInPrerequisiteState
                 })
        {
            Assert.True(guard.LatchesOnLongRunningFailure(new PostgresException("held its lock", "ERROR", "ERROR", sqlState)), sqlState);
        }
    }

    /// <summary>
    /// The latch itself: a jittered 30–60 s window on the store's clock; a lock-wait timeout and any
    /// other failure name themselves differently; the same failure latched twice (the long-running
    /// step, then the store's lock-timeout arm) keeps its first window; past the window nothing throws.
    /// </summary>
    [Theory]
    [InlineData(Package.Channel)]
    [InlineData(Package.Transport)]
    [InlineData(Package.DurableFlow)]
    public void RetryAfter_IsAJitteredWindow_ThatNamesItsCause(Package package)
    {
        var clock = new VirtualTimeProvider();
        var guard = new Guard(package) { Clock = clock };
        guard.ThrowIfBackingOff();

        var lockTimeout = new PostgresException("canceling statement due to lock timeout", "ERROR", "ERROR", PostgresErrorCodes.LockNotAvailable);
        var window = guard.BackOff(lockTimeout);
        Assert.InRange(window, TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(60));
        Assert.Equal(window, guard.BackOff(lockTimeout));
        var locked = Assert.Throws<InvalidOperationException>(guard.ThrowIfBackingOff);
        Assert.Same(lockTimeout, locked.InnerException);
        Assert.Contains("could not take its lock within 5s", locked.Message, StringComparison.Ordinal);

        clock.Advance(window);
        guard.ThrowIfBackingOff();

        var outran = new PostgresException("canceling statement due to statement timeout", "ERROR", "ERROR", PostgresErrorCodes.QueryCanceled);
        guard.BackOff(outran);
        var failed = Assert.Throws<InvalidOperationException>(guard.ThrowIfBackingOff);
        Assert.Same(outran, failed.InnerException);
        Assert.Contains("failed and was rolled back", failed.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// Regression (S5#2, S10#2: "reuse the transport's retry-after latch"): a failed channel or flow
    /// store DDL attempt left nothing behind, so the very next operation started another — a lock held
    /// for minutes stalled the table in back-to-back cycles, the k-th caller queued on the gate
    /// waiting its turn to fail. Inside the window EnsureCreated now fails at once, a caller already
    /// queued on the gate included, without opening a connection; past it the next operation tries
    /// again (nothing listens on port 1, so that attempt fails with the driver's connection error).
    /// </summary>
    [Theory]
    [InlineData(Package.Channel)]
    [InlineData(Package.DurableFlow)]
    public async Task ChannelAndFlowStore_FailAtOnceInsideTheWindow_AndTryAgainAfterIt(Package package)
    {
        await using var dataSource = NpgsqlDataSource.Create(
            "Host=127.0.0.1;Port=1;Username=unused;Password=unused;Database=none;Timeout=1;Pooling=false");
        var clock = new VirtualTimeProvider();
        object store;
        Func<Task> operation;
        if (package == Package.Channel)
        {
            var channel = new PostgreSqlChannelSql(dataSource, Options.Create(new PostgreSqlAsyncResponseChannelOptions())) { Clock = clock };
            store = channel;
            operation = () => channel.EnsureCreatedAsync();
        }
        else
        {
            var flows = new PostgreSqlFlowStateStore(dataSource, Options.Create(new PostgreSqlDurableFlowOptions())) { Clock = clock };
            store = flows;
            operation = () => flows.LoadAsync("flow");
        }

        var guard = new Guard(store.GetType().GetField("_ddl", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(store)!);
        var lockTimeout = new PostgresException("canceling statement due to lock timeout", "ERROR", "ERROR", PostgresErrorCodes.LockNotAvailable);

        // A caller already queued on the gate behind the failing attempt.
        var gate = (SemaphoreSlim)store.GetType().GetField("_ensureGate", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(store)!;
        await gate.WaitAsync();
        var queued = operation();
        Assert.False(queued.IsCompleted);
        var window = guard.BackOff(lockTimeout);
        gate.Release();
        Assert.Same(lockTimeout, (await Assert.ThrowsAsync<InvalidOperationException>(() => queued.WaitAsync(HangGuard))).InnerException);

        var refused = await Assert.ThrowsAsync<InvalidOperationException>(operation);
        Assert.Same(lockTimeout, refused.InnerException);
        clock.Advance(window - TimeSpan.FromSeconds(1));
        await Assert.ThrowsAsync<InvalidOperationException>(operation);

        clock.Advance(TimeSpan.FromSeconds(1));
        await Assert.ThrowsAnyAsync<NpgsqlException>(operation);
    }

    private static Type GuardType(Package package)
        => AnchorType(package).Assembly.GetType("AsyncResponse.Internal.PostgreSqlDdlGuard", throwOnError: true)!;

    private static Type AnchorType(Package package) => package switch
    {
        Package.Channel => typeof(PostgreSqlChannelSql),
        Package.Transport => typeof(PostgreSqlTransportStore),
        _ => typeof(PostgreSqlFlowStateStore)
    };

    private static (Type Store, string Method) DdlMethod(Package package) => package switch
    {
        Package.Channel => (typeof(PostgreSqlChannelSql), nameof(PostgreSqlChannelSql.EnsureCreatedAsync)),
        Package.Transport => (typeof(PostgreSqlTransportStore), "EnsureCreatedCoreAsync"),
        _ => (typeof(PostgreSqlFlowStateStore), "EnsureCreatedAsync")
    };

    /// <summary>Reflection facade over one package's compiled copy of the guard.</summary>
    private sealed class Guard
    {
        private readonly object _instance;
        private readonly Type _type;

        public Guard(Package package)
            : this(Activator.CreateInstance(GuardType(package), "test", "'s.t'", "docs/postgresql.md", null)!)
        {
        }

        public Guard(object instance)
        {
            _instance = instance;
            _type = instance.GetType();
        }

        public TimeProvider Clock
        {
            get => (TimeProvider)Property("Clock").GetValue(_instance)!;
            init => Property("Clock").SetValue(_instance, value);
        }

        public TimeSpan BackOff(Exception cause) => (TimeSpan)Invoke("BackOff", cause)!;

        public void ThrowIfBackingOff() => Invoke("ThrowIfBackingOff");

        public Task ExecuteLongRunningAsync(string sql, NpgsqlConnection connection)
            => (Task)Invoke("ExecuteLongRunningAsync", sql, connection, null, CancellationToken.None)!;

        /// <summary>Whether a long-running step failing with <paramref name="failure"/> latches the window.</summary>
        public bool LatchesOnLongRunningFailure(PostgresException failure)
        {
            // The step's catch filter (the fake server answers every error with one SQLSTATE).
            var filter = _type.GetMethod("LatchesOnFailure", BindingFlags.Static | BindingFlags.NonPublic)!;
            return (bool)filter.Invoke(null, [failure])!;
        }

        private PropertyInfo Property(string name) => _type.GetProperty(name, BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public)!;

        private object? Invoke(string name, params object?[] args)
        {
            try
            {
                return _type.GetMethod(name, BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public)!.Invoke(_instance, args);
            }
            catch (TargetInvocationException wrapped) when (wrapped.InnerException is not null)
            {
                System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(wrapped.InnerException).Throw();
                throw;
            }
        }
    }
}
