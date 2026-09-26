using AsyncResponse.Testing;
using AsyncResponse.Transports.SqlServer;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Options;
using System.Reflection;
using System.Reflection.Emit;
using System.Runtime.CompilerServices;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Regression (round 33): the SQL Server transport's dead-letter prune was an unbounded
/// <c>DELETE FROM</c> over the queue table that live claims share. Past SQL Server's ~5,000-lock
/// escalation threshold the statement takes a table X lock that READPAST cannot skip, so every
/// claim, ACK and lease renewal blocked behind it for up to the command timeout — a live handler's
/// lease lapsed and a peer re-ran its job concurrently. The prune is now <c>DELETE TOP (1000)</c>
/// (SQL Server channel parity). The statement is only assembled on the far side of an opened
/// connection, so this fact reads the literals the compiled method carries — the same reflection
/// stance as the channel's bounded-prune fact: an older build has no bounded statement, so this
/// fails there instead of failing to compile.
/// </summary>
public sealed class SqlServerTransportStorePruneTests
{
    [Fact]
    public void DeadLetterPrune_IsBounded_SoItCannotEscalateToATableLock()
    {
        // The one-batch delete the shared DbDeadLetterPrune drain loops (it moved out of the old
        // PruneDeadLettersIfDueAsync when the throttle/drain/guard became shared).
        var instructions = Decode(AsyncBody(typeof(SqlServerTransportStore), "PruneDeadLetterBatchAsync")).ToArray();
        var literals = instructions
            .Where(instruction => instruction.Op == OpCodes.Ldstr)
            .Select(instruction => (string)instruction.Operand!)
            .ToArray();

        Assert.Contains(literals, literal => literal.Contains("DELETE TOP (", StringComparison.Ordinal));
        Assert.DoesNotContain(literals, literal => literal.Contains("DELETE FROM", StringComparison.Ordinal));

        // The batch size. An int constant hole is appended as `ldc.i4 1000`; a compiler that folds
        // it into the literal instead satisfies the first arm.
        Assert.True(
            literals.Any(literal => literal.Contains("DELETE TOP (1000)", StringComparison.Ordinal))
            || instructions.Any(instruction => instruction.Op == OpCodes.Ldc_I4 && instruction.Operand is 1000),
            "the dead-letter prune batch is not 1000 rows");

        // Still the retention prune, on the server clock.
        Assert.Contains(literals, literal => literal.Contains("created_at <", StringComparison.Ordinal));
    }

    /// <summary>
    /// Regression: the prune batch's row count — what ends the drain loop — came from
    /// <c>ExecuteNonQuery</c>, which is -1 under a server-wide NOCOUNT (<c>sp_configure 'user options',
    /// 512</c>), so every drain stopped after its first batch and a large dead-letter backlog shrank by
    /// one batch a minute (the flow store's <c>RowCountOn</c> fixed the same). The batch turns the count
    /// back on for itself.
    /// </summary>
    [Fact]
    public void DeadLetterPrune_TurnsTheRowCountBackOn_SoNocountCannotEndTheDrain()
    {
        var batch = Literals(AsyncBody(typeof(SqlServerTransportStore), "PruneDeadLetterBatchAsync"))
            .Single(literal => literal.Contains("DELETE TOP (", StringComparison.Ordinal));

        Assert.StartsWith("SET NOCOUNT OFF;", batch, StringComparison.Ordinal);
    }

    /// <summary>
    /// Regression: the new dequeue index was built inside the DDL batch under SqlClient's 30 s command
    /// timeout. On a large table an older build created, that offline build (a table S lock blocking
    /// every write) timed out and rolled back — and EnsureCreated runs per insert, claim, and
    /// dead-letter until it succeeds, so every operation re-imposed another 30 s write block and the
    /// index never got built. It now runs, only when absent, under an hour-long command timeout (finite:
    /// the build runs holding the store's gate, so a connection that never answered would wedge every
    /// later operation on the host), and its lock wait is bounded instead.
    /// </summary>
    [Fact]
    public void ReadyIndexBuild_RunsUnderAnHourLongCommandTimeout_AndBoundsItsLockWait()
    {
        using var command = SqlServerTransportStore.LongRunningDdlCommand("CREATE INDEX i ON t (c);");
        Assert.Equal(3600, command.CommandTimeout);

        var sql = SqlServerTransportStore.IndexBuildSql("[dbo].[jobs]", "jobs_ready_idx", createdIndex: null);
        Assert.StartsWith($"SET LOCK_TIMEOUT {SqlServerTransportStore.DdlLockTimeoutMilliseconds};", sql, StringComparison.Ordinal);
        Assert.Equal(5000, SqlServerTransportStore.DdlLockTimeoutMilliseconds);
        Assert.Contains("CREATE INDEX [jobs_ready_idx]", sql, StringComparison.Ordinal);
        Assert.Contains("ON [dbo].[jobs] (queue, available_at, created_at);", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("(created_at);", sql, StringComparison.Ordinal);
        Assert.EndsWith("SET LOCK_TIMEOUT -1;", sql.TrimEnd(), StringComparison.Ordinal);

        // EnsureCreated builds it through that command, and nowhere else (the DDL lives in the
        // shared attempt, EnsureCreatedCoreAsync, since round 2's S9#9).
        var ddl = Decode(AsyncBody(typeof(SqlServerTransportStore), "EnsureCreatedCoreAsync")).ToArray();
        var calls = ddl.Select(instruction => instruction.Operand).OfType<MethodBase>().ToArray();
        Assert.Contains(calls, method => method.Name == nameof(SqlServerTransportStore.IndexBuildSql));
        Assert.Contains(calls, method => method.Name == nameof(SqlServerTransportStore.LongRunningDdlCommand));
        Assert.DoesNotContain(
            ddl.Where(instruction => instruction.Op == OpCodes.Ldstr).Select(instruction => (string)instruction.Operand!),
            literal => literal.Contains("(queue, available_at, created_at)", StringComparison.Ordinal));
    }

    /// <summary>
    /// Regression (fixpoint r2 S9#11): the <c>created_at</c> index was still built in the first DDL
    /// batch, under SqlClient's 30 s default command timeout and with no <c>LOCK_TIMEOUT</c>. On an
    /// existing large table without it (a formerly operator-managed table switched to auto-create, an
    /// index someone dropped) that offline build queued every write behind its S lock for up to 30 s,
    /// timed out with -2 — not 1222, so nothing latched — and was retried by every operation: the loop
    /// round 43 fixed for the dequeue index. Both builds are now the one lock-bounded, long-running
    /// batch, and the first batch builds no index at all.
    /// </summary>
    [Fact]
    public void CreatedIndexBuild_IsInTheLockBoundedLongRunningBatch_NotTheFirstBatch()
    {
        var firstBatch = Literals(AsyncBody(typeof(SqlServerTransportStore), "EnsureCreatedCoreAsync"))
            .Where(literal => literal.Contains("CREATE TABLE", StringComparison.Ordinal) || literal.Contains("dead_letter_reason nvarchar(max) NULL", StringComparison.Ordinal))
            .ToArray();
        Assert.NotEmpty(firstBatch);
        Assert.DoesNotContain(firstBatch, literal => literal.Contains("CREATE INDEX", StringComparison.Ordinal));
        Assert.DoesNotContain(
            Literals(AsyncBody(typeof(SqlServerTransportStore), "EnsureCreatedCoreAsync")),
            literal => literal.Contains("(created_at)", StringComparison.Ordinal));

        var created = SqlServerTransportStore.IndexBuildSql("[dbo].[jobs]", readyIndex: null, "jobs_created_idx");
        Assert.StartsWith($"SET LOCK_TIMEOUT {SqlServerTransportStore.DdlLockTimeoutMilliseconds};", created, StringComparison.Ordinal);
        Assert.Contains("CREATE INDEX [jobs_created_idx]", created, StringComparison.Ordinal);
        Assert.Contains("ON [dbo].[jobs] (created_at);", created, StringComparison.Ordinal);
        Assert.DoesNotContain("jobs_ready_idx", created, StringComparison.Ordinal);
        Assert.EndsWith("SET LOCK_TIMEOUT -1;", created.TrimEnd(), StringComparison.Ordinal);

        // Both absent: one batch, one lock bound, both guarded builds.
        var both = SqlServerTransportStore.IndexBuildSql("[dbo].[jobs]", "jobs_ready_idx", "jobs_created_idx");
        Assert.Equal(2, both.Split("CREATE INDEX", StringSplitOptions.None).Length - 1);
        Assert.Equal(2, both.Split("IF NOT EXISTS", StringSplitOptions.None).Length - 1);
        Assert.Equal(1, both.Split($"SET LOCK_TIMEOUT {SqlServerTransportStore.DdlLockTimeoutMilliseconds};", StringSplitOptions.None).Length - 1);
    }

    /// <summary>
    /// Regression (fixpoint r2 GS3#2, the SQL Server twin of S9#6): the hour-bounded dequeue-index
    /// build ran inside the transaction holding the schema-wide application lock
    /// <c>asyncresponse:ddl:{schema}</c>, which the SQL Server channel and flow store take for their
    /// own DDL — so for the whole build every host starting meanwhile could not initialize either of
    /// them (their lock commands hit SqlClient's 30 s timeout, unlatched, on every operation). The
    /// schema-shared DDL now commits first, and the builds run in a transaction of their own under a
    /// lock scoped to the table.
    /// </summary>
    [Fact]
    public void IndexBuilds_RunAfterTheSchemaLockIsReleased_UnderATableScopedLock()
    {
        Assert.NotEqual(SqlServerTransportStore.SchemaLockResource("dbo"), SqlServerTransportStore.TableLockResource("dbo", "jobs"));
        Assert.NotEqual(SqlServerTransportStore.TableLockResource("dbo", "jobs"), SqlServerTransportStore.TableLockResource("dbo", "other"));

        var calls = Decode(AsyncBody(typeof(SqlServerTransportStore), "EnsureCreatedCoreAsync"))
            .Select(instruction => instruction.Operand)
            .OfType<MethodBase>()
            .Select(method => method.Name)
            .ToList();
        var schemaLock = calls.IndexOf(nameof(SqlServerTransportStore.SchemaLockResource));
        var tableLock = calls.IndexOf(nameof(SqlServerTransportStore.TableLockResource));
        var build = calls.IndexOf(nameof(SqlServerTransportStore.IndexBuildSql));
        Assert.True(schemaLock >= 0 && tableLock > schemaLock && build > tableLock, string.Join(", ", calls));

        // The schema transaction commits between the two locks.
        Assert.Contains(calls.Skip(schemaLock).Take(tableLock - schemaLock), name => name == "CommitAsync");
    }

    /// <summary>
    /// Regression (fixpoint r2 S9#2, SQL Server half): only a lock timeout (1222) latched the
    /// startup-DDL retry-after window, so an index build that failed any other way — a full log
    /// (9002) or filegroup (1105), a build that outran its command timeout — was re-run by the next
    /// operation at once, each attempt blocking the queue table's writes for as long as it ran. Any
    /// failure of the table work now latches the window; a lock wait (1222, or the table-scoped
    /// application lock another host's build holds — the batch's THROW 51000) says so, anything
    /// else names the failed build.
    /// </summary>
    [Theory]
    [InlineData(9002, false)]
    [InlineData(1105, false)]
    [InlineData(-2, false)]
    [InlineData(1222, true)]
    [InlineData(51000, true)]
    public async Task ATableWorkFailureOfAnyKind_LatchesTheRetryAfter(int number, bool lockWait)
    {
        var clock = new VirtualTimeProvider();
        var store = new SqlServerTransportStore(Options.Create(new SqlServerAsyncResponseTransportOptions
        {
            ConnectionString = "Server=tcp:127.0.0.1,1;Database=none;User ID=sa;Password=unused;Encrypt=False;Connect Timeout=1"
        })) { Clock = clock };
        var failure = RelationalSharedHelperTests.SqlExceptionWith(number);

        var window = store.BackOffDdlAfterTableWorkFailure(failure);

        Assert.InRange(window, SqlServerTransportStore.DdlLockTimeoutBackoff, 2 * SqlServerTransportStore.DdlLockTimeoutBackoff);
        var refused = await Assert.ThrowsAsync<InvalidOperationException>(() => store.EnsureCreatedAsync());
        Assert.Same(failure, refused.InnerException);
        Assert.Equal(lockWait, refused.Message.Contains("could not take its table lock within 5000 ms", StringComparison.Ordinal));
        Assert.Equal(!lockWait, refused.Message.Contains("index build", StringComparison.Ordinal) && refused.Message.Contains("failed and was rolled back", StringComparison.Ordinal));

        // The attempt's failure path latches through it — whatever the error number.
        var calls = Decode(AsyncBody(typeof(SqlServerTransportStore), "EnsureCreatedCoreAsync"))
            .Select(instruction => instruction.Operand)
            .OfType<MethodBase>()
            .Count(method => method.Name == nameof(SqlServerTransportStore.BackOffDdlAfterTableWorkFailure));
        Assert.Equal(1, calls);
    }

    /// <summary>
    /// Fixpoint r2 precommit D1: the table-scoped application-lock step runs on EVERY auto-create
    /// start, and its failure latched the 30–60 s retry-after window whatever the error — so a
    /// failover blip (a class-20 fault, a 10054 transport reset, an Azure 40613) landing on that one
    /// round trip failed every transport operation of the host for 30–60 s with the latch's
    /// InvalidOperationException, which no retry policy retries (the build keeps "any failure
    /// latches"). The lock step now latches only a lock wait lost. Red on the old code (the
    /// predicate made permissive, i.e. "any failure latches"): every transient row latched.
    /// </summary>
    [Theory]
    [InlineData(51000, (byte)16, true)]
    [InlineData(1222, (byte)16, true)]
    [InlineData(10054, (byte)20, false)]
    [InlineData(10054, (byte)0, false)]
    [InlineData(40613, (byte)0, false)]
    [InlineData(1205, (byte)13, false)]
    [InlineData(51000, (byte)20, false)]
    public void TableLockStep_LatchesOnlyALostLockWait_NeverATransientConnectionFault(int number, byte errorClass, bool latches)
    {
        Assert.Equal(latches, SqlServerTransportStore.LatchesOnTableLockFailure(RelationalSharedHelperTests.SqlExceptionWith(number, errorClass)));

        // And the attempt's failure path consults it.
        var calls = Decode(AsyncBody(typeof(SqlServerTransportStore), "EnsureCreatedCoreAsync"))
            .Select(instruction => instruction.Operand)
            .OfType<MethodBase>()
            .Select(method => method.Name);
        Assert.Contains(nameof(SqlServerTransportStore.LatchesOnTableLockFailure), calls);
    }

    /// <summary>
    /// Regression (fixpoint r2 S9#9, SQL Server half): the startup DDL ran under the token of
    /// whichever caller took the gate first, so a publish whose request token fired partway cancelled
    /// the hour-bounded first index build and rolled it back — after the table's writes had been
    /// blocked all that time — for the next caller to start from scratch. One attempt now runs under
    /// the store's own lifetime, shared by every caller that arrives meanwhile; a caller's token
    /// bounds only its own wait. The server here never answers, so the attempt stays in flight: the
    /// cancelled caller leaves, the caller that joined keeps waiting on the same attempt (the one
    /// connection it opened), and only the store's disposal ends it. Red on the old code: the
    /// cancellation aborted the attempt, and the queued caller opened a second connection for an
    /// attempt of its own, which disposal (had there been any) could not reach.
    /// </summary>
    [Fact]
    public async Task EnsureCreated_ACallerTokenBoundsOnlyItsOwnWait_AndTheAttemptRunsOnForTheOthers()
    {
        await using var server = new SilentTcpServer();
        var store = new SqlServerTransportStore(Options.Create(new SqlServerAsyncResponseTransportOptions
        {
            ConnectionString = $"Server=tcp:127.0.0.1,{server.Port};Database=none;User ID=sa;Password=unused;Encrypt=False;Connect Timeout=120;Pooling=false"
        }));
        using var caller = new CancellationTokenSource();

        var cancelled = store.EnsureCreatedAsync(caller.Token);
        await server.FirstAccepted.WaitAsync(HangGuard);
        var joined = store.EnsureCreatedAsync();
        caller.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => cancelled.WaitAsync(HangGuard));
        Assert.False(joined.IsCompleted);

        store.Dispose();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => joined.WaitAsync(HangGuard));
        Assert.Equal(1, server.Accepted);
    }

    private static readonly TimeSpan HangGuard = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Regression (fixpoint r2 GS3#4): a delayed publish raised the in-process
    /// <c>MessagePublished</c> wake for a row nobody can claim until its delay has passed, sending
    /// every same-process subscriber of the queue into a claim that found nothing — on every
    /// durable-flow timer park and redelay hop (the NAK stopped doing the same in round 43).
    /// </summary>
    [Fact]
    public void Publish_WakesSubscribersOnlyForARowClaimableAtOnce()
    {
        Assert.True(SqlServerTransportStore.WakesSubscribers(delay: null));
        Assert.False(SqlServerTransportStore.WakesSubscribers(TimeSpan.FromSeconds(5)));
        Assert.False(SqlServerTransportStore.WakesSubscribers(TimeSpan.FromMilliseconds(1)));

        var calls = Decode(AsyncBody(typeof(SqlServerTransportStore), nameof(SqlServerTransportStore.PublishAsync)))
            .Select(instruction => instruction.Operand)
            .OfType<MethodBase>()
            .ToArray();
        Assert.Contains(calls, method => method.Name == nameof(SqlServerTransportStore.WakesSubscribers));
    }

    /// <summary>
    /// Regression (fixpoint r2 S9#10): the operator-managed missing-index warning was satisfied by
    /// ANY index leading on <c>queue</c> — including the previous build's <c>_claim_idx</c> over
    /// (queue, available_at, locked_until, created_at), which cannot serve the claim's
    /// <c>ORDER BY available_at, created_at</c> (a Top-N sort that U-locks the whole ready set per
    /// claim), and a disabled index. That table is exactly what a migration written for the previous
    /// build leaves behind, and it got no warning. The check now matches the claim's key, not a name.
    /// </summary>
    [Fact]
    public void OperatorSchemaIndexCheck_MatchesTheClaimKey_NotAnyIndexLeadingOnQueue()
    {
        var query = SqlServerTransportStore.DequeueIndexQuery;
        Assert.Contains("ic.key_ordinal = 1 AND c.name = N'queue'", query, StringComparison.Ordinal);
        Assert.Contains("ic.key_ordinal = 2 AND c.name = N'available_at'", query, StringComparison.Ordinal);
        Assert.Contains("ic.key_ordinal = 3 AND c.name = N'created_at'", query, StringComparison.Ordinal);
        Assert.Contains(") = 3", query, StringComparison.Ordinal);
        Assert.Contains("i.is_disabled = 0", query, StringComparison.Ordinal);
        Assert.Contains("i.is_hypothetical = 0", query, StringComparison.Ordinal);

        // And it is the query the warning runs.
        Assert.Contains(query, Literals(AsyncBody(typeof(SqlServerTransportStore), "WarnIfClaimIndexMissingAsync")));
    }

    /// <summary>
    /// Regression: a lock-timeout failure (1222) of the dequeue-index build left nothing behind, so the
    /// very next operation started another attempt, and a conflicting lock held for minutes stalled the
    /// whole queue in back-to-back 5 s cycles — every host's writes queued behind each attempt's lock
    /// request, the k-th caller queued on the gate waiting k × 5 s to fail (PostgreSQL parity). The
    /// failure now latches a jittered 30–60 s retry-after; inside it EnsureCreated fails at once, a
    /// caller already queued on the gate included, without opening a connection. Nothing listens on
    /// port 1, so an attempt that does reach the server fails with SqlClient's connection error.
    /// </summary>
    [Fact]
    public async Task DdlLockTimeout_LatchesARetryAfter_InsideWhichEnsureCreatedFailsWithoutConnecting()
    {
        var clock = new VirtualTimeProvider();
        var store = new SqlServerTransportStore(Options.Create(new SqlServerAsyncResponseTransportOptions
        {
            ConnectionString = "Server=tcp:127.0.0.1,1;Database=none;User ID=sa;Password=unused;Encrypt=False;Connect Timeout=1"
        })) { Clock = clock };
        // SqlException has no public constructor; the latch only carries its cause through.
        var lockTimeout = new TimeoutException("Lock request time out period exceeded.");

        // A caller already queued on the gate behind the failing attempt.
        var gate = (SemaphoreSlim)typeof(SqlServerTransportStore)
            .GetField("_ensureGate", BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(store)!;
        await gate.WaitAsync();
        var queued = store.EnsureCreatedAsync();
        Assert.False(queued.IsCompleted);
        var window = store.BackOffDdlAfterLockTimeout(lockTimeout);
        gate.Release();
        Assert.Same(lockTimeout, (await Assert.ThrowsAsync<InvalidOperationException>(() => queued)).InnerException);
        Assert.Equal(TimeSpan.FromSeconds(30), SqlServerTransportStore.DdlLockTimeoutBackoff);
        Assert.InRange(window, SqlServerTransportStore.DdlLockTimeoutBackoff, 2 * SqlServerTransportStore.DdlLockTimeoutBackoff);

        // A new operation inside the window: at once, naming the lock wait, the lock timeout as its cause.
        var refused = await Assert.ThrowsAsync<InvalidOperationException>(() => store.EnsureCreatedAsync());
        Assert.Same(lockTimeout, refused.InnerException);
        Assert.Contains("could not take its table lock within 5000 ms", refused.Message, StringComparison.Ordinal);
        clock.Advance(window - TimeSpan.FromSeconds(1));
        await Assert.ThrowsAsync<InvalidOperationException>(() => store.EnsureCreatedAsync());

        // Past it, the next operation attempts the DDL again.
        clock.Advance(TimeSpan.FromSeconds(1));
        await Assert.ThrowsAnyAsync<SqlException>(() => store.EnsureCreatedAsync());

        // The lock-wait arm of the table work latches it (through BackOffDdlAfterTableWorkFailure,
        // which also latches every other failure of that step — see the S9#2 fact below).
        var arms = Decode(typeof(SqlServerTransportStore).GetMethod(nameof(SqlServerTransportStore.BackOffDdlAfterTableWorkFailure), BindingFlags.Instance | BindingFlags.NonPublic)!)
            .Select(instruction => instruction.Operand)
            .OfType<MethodBase>()
            .Count(method => method.Name == nameof(SqlServerTransportStore.BackOffDdlAfterLockTimeout));
        Assert.Equal(1, arms);
    }

    /// <summary>
    /// Regression: the lease renewal trusted <c>ExecuteNonQuery</c>'s row count, which is -1 under a
    /// server-wide <c>SET NOCOUNT ON</c> (<c>sp_configure 'user options', 512</c>) — every successful
    /// renew then read as "lease lost", the heartbeat stopped after its first beat, and a handler
    /// longer than the lease ran twice concurrently. The count now comes from <c>@@ROWCOUNT</c>, which
    /// NOCOUNT does not affect. Read from the compiled literals (the statement only exists past an
    /// opened connection).
    /// </summary>
    [Fact]
    public void LeaseRenewal_ReadsItsRowCountFromTheServer_WhichNocountCannotHide()
    {
        var literals = Decode(AsyncBody(typeof(SqlServerTransportStore), "RenewLeaseAsync"))
            .Where(instruction => instruction.Op == OpCodes.Ldstr)
            .Select(instruction => (string)instruction.Operand!)
            .ToArray();

        Assert.Contains(literals, literal => literal.Contains("SELECT @@ROWCOUNT", StringComparison.Ordinal));
        Assert.Contains(literals, literal => literal.Contains("lock_id = @lock_id", StringComparison.Ordinal));
    }

    /// <summary>
    /// Regression: the claim ordered by <c>created_at</c> behind an index keyed (queue, available_at,
    /// locked_until, created_at) that cannot serve that order past the available_at range — the plan
    /// sorted the whole ready set (U-locking every row it scanned, so competing READPAST claimers
    /// found nothing) or walked <c>created_idx</c> through every older row of the other logical
    /// queues, so draining K rows cost O(K²) (PostgreSQL fixed the same in round 42). The claim now
    /// orders by the dequeue index's own key tail. Read from the compiled literals.
    /// </summary>
    [Fact]
    public void Claim_OrdersByTheDequeueIndexKey_WhichTheIndexItselfServes()
    {
        var claim = Literals(AsyncBody(typeof(SqlServerTransportStore), "TryClaimAsync"));
        Assert.Contains(claim, literal => literal.Contains("ORDER BY available_at, created_at", StringComparison.Ordinal));

        var build = SqlServerTransportStore.IndexBuildSql("[dbo].[jobs]", "jobs_ready_idx", "jobs_created_idx");
        Assert.Contains("(queue, available_at, created_at)", build, StringComparison.Ordinal);
        var ddl = Literals(AsyncBody(typeof(SqlServerTransportStore), "EnsureCreatedCoreAsync"));
        Assert.DoesNotContain(ddl, literal => literal.Contains("locked_until, created_at)", StringComparison.Ordinal));
        Assert.DoesNotContain("locked_until, created_at)", build, StringComparison.Ordinal);
    }

    /// <summary>
    /// Regression: a NAK raised the in-process <c>MessagePublished(null)</c> wake, which sent every
    /// same-process subscriber into a claim for a row not claimable until its redelivery delay has
    /// passed (PostgreSQL removed its NAK wake in round 42). The NAK now raises nothing.
    /// </summary>
    [Fact]
    public void Nak_RaisesNoInProcessWake()
    {
        var calls = Decode(AsyncBody(typeof(SqlServerTransportStore), "NakAsync"))
            .Select(instruction => instruction.Operand)
            .OfType<MethodBase>()
            .ToArray();

        Assert.DoesNotContain(calls, method => method.Name == nameof(Action.Invoke) && method.DeclaringType == typeof(Action<string?>));
        Assert.Contains(calls, method => method.Name == "ExecuteNonQueryAsync");
    }

    internal static string[] Literals(MethodBase method)
        => Decode(method)
            .Where(instruction => instruction.Op == OpCodes.Ldstr)
            .Select(instruction => (string)instruction.Operand!)
            .ToArray();

    /// <summary>The compiled body of an async method: its state machine's <c>MoveNext</c>.</summary>
    internal static MethodBase AsyncBody(Type type, string methodName)
    {
        var method = type.GetMethod(methodName, BindingFlags.Instance | BindingFlags.Static | BindingFlags.NonPublic | BindingFlags.Public);
        Assert.NotNull(method);
        var stateMachine = method!.GetCustomAttribute<AsyncStateMachineAttribute>()!.StateMachineType;
        var map = stateMachine.GetInterfaceMap(typeof(IAsyncStateMachine));
        var index = Array.FindIndex(map.InterfaceMethods, candidate => candidate.Name == nameof(IAsyncStateMachine.MoveNext));
        return map.TargetMethods[index];
    }

    private static readonly Dictionary<ushort, OpCode> OpCodeTable = typeof(OpCodes)
        .GetFields(BindingFlags.Public | BindingFlags.Static)
        .Select(field => (OpCode)field.GetValue(null)!)
        .Where(op => op.OpCodeType != OpCodeType.Nternal)
        .ToDictionary(op => (ushort)op.Value);

    /// <summary>A minimal IL walk: every instruction, with string, int32, and method operands resolved.</summary>
    internal static IEnumerable<(OpCode Op, object? Operand)> Decode(MethodBase method)
    {
        var il = method.GetMethodBody()!.GetILAsByteArray()!;
        var module = method.Module;
        for (var i = 0; i < il.Length;)
        {
            ushort code = il[i++];
            if (code == 0xFE)
                code = (ushort)(0xFE00 | il[i++]);
            var op = OpCodeTable[code];
            object? operand = null;
            switch (op.OperandType)
            {
                case OperandType.InlineNone:
                    break;
                case OperandType.ShortInlineI:
                    operand = (int)(sbyte)il[i];
                    i += 1;
                    break;
                case OperandType.ShortInlineBrTarget:
                case OperandType.ShortInlineVar:
                    i += 1;
                    break;
                case OperandType.InlineVar:
                    i += 2;
                    break;
                case OperandType.InlineI8:
                case OperandType.InlineR:
                    i += 8;
                    break;
                case OperandType.InlineSwitch:
                    i += 4 + 4 * BitConverter.ToInt32(il, i);
                    break;
                case OperandType.InlineString:
                    operand = module.ResolveString(BitConverter.ToInt32(il, i));
                    i += 4;
                    break;
                case OperandType.InlineI:
                    operand = BitConverter.ToInt32(il, i);
                    i += 4;
                    break;
                case OperandType.InlineMethod:
                    try
                    {
                        operand = module.ResolveMethod(BitConverter.ToInt32(il, i));
                    }
                    catch (ArgumentException)
                    {
                        // A generic-context member reference this minimal walk does not resolve.
                    }

                    i += 4;
                    break;
                default:
                    // InlineBrTarget, InlineField, InlineSig, InlineTok, InlineType, ShortInlineR.
                    i += 4;
                    break;
            }

            yield return (op, operand);
        }
    }
}
