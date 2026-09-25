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

        var sql = SqlServerTransportStore.ReadyIndexBuildSql("[dbo].[jobs]", "jobs_ready_idx");
        Assert.StartsWith($"SET LOCK_TIMEOUT {SqlServerTransportStore.DdlLockTimeoutMilliseconds};", sql, StringComparison.Ordinal);
        Assert.Equal(5000, SqlServerTransportStore.DdlLockTimeoutMilliseconds);
        Assert.Contains("CREATE INDEX [jobs_ready_idx]", sql, StringComparison.Ordinal);
        Assert.Contains("ON [dbo].[jobs] (queue, available_at, created_at);", sql, StringComparison.Ordinal);
        Assert.EndsWith("SET LOCK_TIMEOUT -1;", sql.TrimEnd(), StringComparison.Ordinal);

        // EnsureCreated builds it through that command, and nowhere else.
        var ddl = Decode(AsyncBody(typeof(SqlServerTransportStore), nameof(SqlServerTransportStore.EnsureCreatedAsync))).ToArray();
        var calls = ddl.Select(instruction => instruction.Operand).OfType<MethodBase>().ToArray();
        Assert.Contains(calls, method => method.Name == nameof(SqlServerTransportStore.ReadyIndexBuildSql));
        Assert.Contains(calls, method => method.Name == nameof(SqlServerTransportStore.LongRunningDdlCommand));
        Assert.DoesNotContain(
            ddl.Where(instruction => instruction.Op == OpCodes.Ldstr).Select(instruction => (string)instruction.Operand!),
            literal => literal.Contains("(queue, available_at, created_at)", StringComparison.Ordinal));
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

        // Only the lock-timeout arm of EnsureCreated latches it.
        var arms = Decode(AsyncBody(typeof(SqlServerTransportStore), nameof(SqlServerTransportStore.EnsureCreatedAsync)))
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

        var build = SqlServerTransportStore.ReadyIndexBuildSql("[dbo].[jobs]", "jobs_ready_idx");
        Assert.Contains("(queue, available_at, created_at)", build, StringComparison.Ordinal);
        var ddl = Literals(AsyncBody(typeof(SqlServerTransportStore), "EnsureCreatedAsync"));
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
        var method = type.GetMethod(methodName, BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public);
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
