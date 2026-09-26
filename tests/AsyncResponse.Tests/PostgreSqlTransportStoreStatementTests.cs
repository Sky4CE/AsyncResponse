using System.Reflection;
using System.Reflection.Emit;
using AsyncResponse.Channels.PostgreSQL;
using AsyncResponse.Testing;
using AsyncResponse.Transports.PostgreSQL;
using Microsoft.Extensions.Options;
using Npgsql;
using NpgsqlTypes;
using Xunit;
using static AsyncResponse.Tests.SqlServerTransportStorePruneTests;

namespace AsyncResponse.Tests;

/// <summary>
/// The PostgreSQL transport store's statements, read from the compiled method bodies (every one is
/// only assembled on the far side of an opened connection — the same stance as
/// <see cref="SqlServerTransportStorePruneTests"/>, whose IL walk this reuses). Behaviour against a
/// real server is pinned by the PostgreSqlDirectIntegrationTests counterparts.
/// </summary>
public sealed class PostgreSqlTransportStoreStatementTests
{
    /// <summary>
    /// Regression (round-29 parity): the queue table stored <c>payload_json</c>/<c>headers_json</c>
    /// as jsonb, which REJECTS the <c>\u0000</c> escape System.Text.Json emits for U+0000 (SQLSTATE
    /// 22P05) — any job or response carrying a NUL in a string argument or context value was
    /// unpublishable on PostgreSQL alone, and jsonb's key re-sorting moved a <c>$type</c>
    /// discriminator behind other keys. The columns are text, bound as text, and an existing jsonb
    /// table is converted in place on the auto-create path.
    /// </summary>
    [Fact]
    public void QueueTable_StoresAndBindsPayloadAndHeadersAsText()
    {
        var ddl = Literals(AsyncBody(typeof(PostgreSqlTransportStore), "EnsureCreatedCoreAsync"));
        Assert.Contains(ddl, literal => literal.Contains("payload_json text NOT NULL", StringComparison.Ordinal));
        Assert.Contains(ddl, literal => literal.Contains("headers_json text NOT NULL DEFAULT", StringComparison.Ordinal));
        Assert.DoesNotContain(ddl, literal => literal.Contains("jsonb NOT NULL", StringComparison.Ordinal));
        Assert.Contains(
            "TYPE text USING payload_json::text",
            PostgreSqlTransportStore.JsonbToTextMigrationSql("\"s\".\"jobs\"", payloadJson: true, headersJson: true),
            StringComparison.Ordinal);

        // No jsonb binds left on either write path.
        var jsonb = (int)NpgsqlDbType.Jsonb;
        foreach (var writer in new[] { "InsertAsync", "DeadLetterAsync" })
        {
            var loadsJsonb = Decode(AsyncBody(typeof(PostgreSqlTransportStore), writer))
                .Any(instruction => (instruction.Op == OpCodes.Ldc_I4 || instruction.Op == OpCodes.Ldc_I4_S) && instruction.Operand is int value && value == jsonb);
            Assert.False(loadsJsonb, $"{writer} still binds NpgsqlDbType.Jsonb");
        }
    }

    /// <summary>
    /// Regression: the in-place jsonb → text conversion issued one <c>ALTER TABLE … TYPE</c> per column,
    /// and each rewrites the whole table and rebuilds every index under ACCESS EXCLUSIVE — twice the
    /// outage for the table an upgrade converts. Whatever needs converting is one statement.
    /// </summary>
    [Theory]
    [InlineData(true, true, 4)]
    [InlineData(true, false, 1)]
    [InlineData(false, true, 3)]
    public void JsonbConversion_IsOneAlterTable_WithEveryColumnThatNeedsIt(bool payloadJson, bool headersJson, int subcommands)
    {
        var sql = PostgreSqlTransportStore.JsonbToTextMigrationSql("\"s\".\"jobs\"", payloadJson, headersJson);

        Assert.StartsWith("ALTER TABLE \"s\".\"jobs\" ", sql, StringComparison.Ordinal);
        Assert.Equal(1, Occurrences(sql, "ALTER TABLE"));
        Assert.Equal(1, Occurrences(sql, ";"));
        Assert.Equal(subcommands, Occurrences(sql, "ALTER COLUMN"));
        Assert.Equal(payloadJson, sql.Contains("ALTER COLUMN payload_json TYPE text USING payload_json::text", StringComparison.Ordinal));
        Assert.Equal(headersJson, sql.Contains(
            "ALTER COLUMN headers_json DROP DEFAULT, ALTER COLUMN headers_json TYPE text USING headers_json::text, ALTER COLUMN headers_json SET DEFAULT '{}'",
            StringComparison.Ordinal));

        static int Occurrences(string text, string value)
            => (text.Length - text.Replace(value, "", StringComparison.Ordinal).Length) / value.Length;
    }

    /// <summary>
    /// Regression: the auto-create DDL — the jsonb rewrite, and the first build of the dequeue index on
    /// an existing table — ran under the data source's 30 s command timeout with no lock bound. A
    /// rewrite that outran it rolled back and was retried by every later operation, each holding
    /// ACCESS EXCLUSIVE for another 30 s and never finishing; while its lock request waited behind a
    /// busy table, every statement on the queue queued behind IT. The work itself now runs under an
    /// hour-long command timeout, and the transaction's lock waits are bounded by a lock_timeout.
    /// The timeout is finite on purpose: the command runs holding the store's gate, and Npgsql sends
    /// no keepalive by default, so with none a socket black-holed mid-rewrite wedged the host forever.
    /// </summary>
    [Fact]
    public void AutoCreateDdl_BoundsItsLockWaits_AndGivesTheRewriteAnHourLongCommandTimeout()
    {
        // Every DDL transaction is opened by the shared guard (src/Shared/PostgreSqlDdlGuard.cs, since
        // fixpoint r2), whose first statement is the lock_timeout — see PostgreSqlDdlGuardTests.
        Assert.Equal("5s", PostgreSqlTransportStore.DdlLockTimeout);
        using var command = PostgreSqlTransportStore.LongRunningDdlCommand("ALTER TABLE t ALTER COLUMN c TYPE text;");
        Assert.Equal(3600, command.CommandTimeout);

        // The DDL (in the shared attempt, EnsureCreatedCoreAsync) opens its transactions through the
        // guard, and the rewrite goes through the guard's long-running command.
        var calls = Decode(AsyncBody(typeof(PostgreSqlTransportStore), "EnsureCreatedCoreAsync"))
            .Select(instruction => instruction.Operand)
            .OfType<MethodBase>()
            .ToArray();
        Assert.Contains(calls, method => method.Name == "BeginLockedTransactionAsync");
        Assert.Contains(calls, method => method.Name == nameof(PostgreSqlTransportStore.JsonbToTextMigrationSql));
        Assert.Contains(calls, method => method.Name == "ExecuteLongRunningAsync");
        Assert.DoesNotContain(
            Literals(AsyncBody(typeof(PostgreSqlTransportStore), "EnsureCreatedCoreAsync")),
            literal => literal.Contains("pg_advisory_xact_lock", StringComparison.Ordinal));
    }

    /// <summary>
    /// Regression (fixpoint r2 S9#6): the hour-bounded jsonb rewrite and first index build ran inside
    /// the transaction holding the schema-wide advisory key <c>asyncresponse:ddl:{schema}</c> — the key
    /// the PostgreSQL channel and flow store take for their own DDL — so for the whole rewrite every
    /// host starting meanwhile could not initialize ANY AsyncResponse store on the schema (other
    /// applications' with different table names included). The schema-shared DDL now commits first,
    /// and the table work runs in a transaction of its own under a key scoped to the table.
    /// </summary>
    [Fact]
    public void LongRunningTableWork_RunsAfterTheSchemaKeyIsReleased_UnderATableScopedKey()
    {
        Assert.NotEqual(
            PostgreSqlTransportStore.SchemaAdvisoryLockKey("public"),
            PostgreSqlTransportStore.TableAdvisoryLockKey("public", "jobs"));
        Assert.NotEqual(
            PostgreSqlTransportStore.TableAdvisoryLockKey("public", "jobs"),
            PostgreSqlTransportStore.TableAdvisoryLockKey("public", "other"));

        var calls = Decode(AsyncBody(typeof(PostgreSqlTransportStore), "EnsureCreatedCoreAsync"))
            .Select(instruction => instruction.Operand)
            .OfType<MethodBase>()
            .Select(method => method.Name)
            .ToList();
        var schemaTransaction = calls.IndexOf("BeginLockedTransactionAsync");
        var tableTransaction = calls.LastIndexOf("BeginLockedTransactionAsync");
        var rewrite = calls.IndexOf("ExecuteLongRunningAsync");
        Assert.True(schemaTransaction >= 0 && tableTransaction > schemaTransaction && rewrite > tableTransaction, string.Join(", ", calls));
        Assert.Contains(calls.Skip(schemaTransaction).Take(tableTransaction - schemaTransaction), name => name == "CommitAsync");

        // The second transaction's key is the table's (the store computes both up front).
        using var dataSource = NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Username=u;Password=p;Database=d");
        using var store = new PostgreSqlTransportStore(
            dataSource,
            Options.Create(new PostgreSqlAsyncResponseTransportOptions { SchemaName = "s", MessageTable = "jobs" }));
        Assert.Equal(PostgreSqlTransportStore.TableAdvisoryLockKey("s", "jobs"), Field<long>(store, "_tableLockKey"));
        Assert.Equal(PostgreSqlTransportStore.SchemaAdvisoryLockKey("s"), Field<long>(store, "_schemaLockKey"));
    }

    /// <summary>
    /// Regression (fixpoint r2 S9#9): the startup DDL ran under the token of whichever caller took the
    /// gate first, so a publish whose request token fired partway cancelled the hour-bounded rewrite
    /// or index build and rolled it back — after the table had been locked all that time — for the
    /// next caller to start from scratch. One attempt now runs under the store's own lifetime, shared
    /// by every caller that arrives meanwhile; a caller's token bounds only its own wait. The server
    /// here never answers, so the attempt stays in flight until the test drops its connection: the
    /// cancelled caller leaves, the attempt carries on, and the caller that joined it sees its real
    /// outcome — on the one connection it opened. Red on the old shape (the gate held around the DDL,
    /// run under the first caller's token): the cancelled caller could not leave until its own
    /// attempt ended; and with the attempt run under the caller's token, the joined caller saw that
    /// caller's cancellation instead of the attempt's outcome.
    /// </summary>
    [Fact]
    public async Task EnsureCreated_ACallerTokenBoundsOnlyItsOwnWait_AndTheAttemptRunsOnForTheOthers()
    {
        await using var server = new SilentTcpServer();
        await using var dataSource = NpgsqlDataSource.Create(
            $"Host=127.0.0.1;Port={server.Port};Username=u;Password=p;Database=d;SSL Mode=Disable;Gss Encryption Mode=Disable;Timeout=120;Pooling=false");
        var store = new PostgreSqlTransportStore(dataSource, Options.Create(new PostgreSqlAsyncResponseTransportOptions()));
        using var caller = new CancellationTokenSource();

        var cancelled = store.EnsureCreatedAsync(caller.Token);
        await server.FirstAccepted.WaitAsync(HangGuard);
        var joined = store.EnsureCreatedAsync();
        caller.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => cancelled.WaitAsync(HangGuard));
        Assert.False(joined.IsCompleted);

        // The attempt's own failure — the dropped connection (Npgsql surfaces it as an
        // NpgsqlException, or on some runtimes as the raw SocketException of its next socket
        // option) — not a cancellation, and not the hang guard.
        server.CloseAll();
        var failure = await Record.ExceptionAsync(() => joined.WaitAsync(HangGuard));
        Assert.NotNull(failure);
        Assert.IsNotAssignableFrom<OperationCanceledException>(failure);
        Assert.IsNotType<TimeoutException>(failure);
        Assert.Equal(1, server.Accepted);
    }

    /// <summary>
    /// Regression (fixpoint r2 GS3#4): a delayed publish NOTIFYed the queue for a row nobody can claim
    /// until its delay has passed, sending every idle subscriber of the queue, in every process, into
    /// a claim that found nothing — on every durable-flow timer park and redelay hop (the NAK stopped
    /// doing the same in round 42). Only a row claimable at once wakes the subscribers now. The fake
    /// server records the statements each publish sent.
    /// </summary>
    [Fact]
    public async Task Publish_NotifiesOnlyForARowClaimableAtOnce()
    {
        await using var server = new FakePostgresWireServer();
        await using var dataSource = NpgsqlDataSource.Create(server.ConnectionString());
        var store = new PostgreSqlTransportStore(dataSource, Options.Create(new PostgreSqlAsyncResponseTransportOptions()));
        typeof(PostgreSqlTransportStore).GetField("_created", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(store, true);

        await store.PublishAsync(Guid.NewGuid(), "worker", "{}", null, CancellationToken.None, delay: TimeSpan.FromSeconds(5));
        var delayed = server.Statements.Select(statement => statement.Sql).ToArray();
        Assert.DoesNotContain(delayed, sql => sql.Contains("pg_notify", StringComparison.Ordinal));
        Assert.Contains(delayed, sql => sql.Contains("INSERT INTO", StringComparison.Ordinal));

        await store.PublishAsync(Guid.NewGuid(), "worker", "{}", null, CancellationToken.None);
        Assert.Contains(server.Statements.Skip(delayed.Length), statement => statement.Sql.Contains("pg_notify", StringComparison.Ordinal));
        Assert.False(PostgreSqlTransportStore.WakesSubscribers(TimeSpan.FromMilliseconds(1)));
        Assert.True(PostgreSqlTransportStore.WakesSubscribers(null));
    }

    /// <summary>
    /// Regression: a lock-timeout failure (55P03) of the startup DDL left nothing behind, so the very
    /// next operation started another attempt. A lock held for minutes or hours — an anti-wraparound
    /// or manual VACUUM, pg_dump, an idle-in-transaction reader — then stalled the whole queue in
    /// back-to-back 5 s cycles, every host's statements parked behind each attempt's lock request,
    /// and the k-th caller queued on the gate waited k × 5 s to fail. The failure now latches a
    /// jittered 30–60 s retry-after; inside it EnsureCreated fails at once, a caller already queued on
    /// the gate included, without opening a connection. Nothing listens on port 1, so an attempt
    /// that does reach the data source fails with the driver's connection error instead.
    /// </summary>
    [Fact]
    public async Task DdlLockTimeout_LatchesARetryAfter_InsideWhichEnsureCreatedFailsWithoutConnecting()
    {
        await using var dataSource = NpgsqlDataSource.Create(
            "Host=127.0.0.1;Port=1;Username=unused;Password=unused;Database=none;Timeout=1;Pooling=false");
        var clock = new VirtualTimeProvider();
        var store = new PostgreSqlTransportStore(dataSource, Options.Create(new PostgreSqlAsyncResponseTransportOptions())) { Clock = clock };
        var lockTimeout = new PostgresException("canceling statement due to lock timeout", "ERROR", "ERROR", PostgresErrorCodes.LockNotAvailable);

        // A caller already queued on the gate behind the failing attempt.
        var gate = (SemaphoreSlim)typeof(PostgreSqlTransportStore)
            .GetField("_ensureGate", BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(store)!;
        await gate.WaitAsync();
        var queued = store.EnsureCreatedAsync();
        Assert.False(queued.IsCompleted);
        var window = store.BackOffDdlAfterLockTimeout(lockTimeout);
        gate.Release();
        Assert.Same(lockTimeout, (await Assert.ThrowsAsync<InvalidOperationException>(() => queued)).InnerException);
        Assert.Equal(TimeSpan.FromSeconds(30), PostgreSqlTransportStore.DdlLockTimeoutBackoff);
        Assert.InRange(window, PostgreSqlTransportStore.DdlLockTimeoutBackoff, 2 * PostgreSqlTransportStore.DdlLockTimeoutBackoff);

        // A new operation inside the window: at once, naming the lock wait, the lock timeout as its cause.
        var refused = await Assert.ThrowsAsync<InvalidOperationException>(() => store.EnsureCreatedAsync());
        Assert.Same(lockTimeout, refused.InnerException);
        Assert.Contains("could not take its lock within 5s", refused.Message, StringComparison.Ordinal);
        clock.Advance(window - TimeSpan.FromSeconds(1));
        await Assert.ThrowsAsync<InvalidOperationException>(() => store.EnsureCreatedAsync());

        // Past it, the next operation attempts the DDL again.
        clock.Advance(TimeSpan.FromSeconds(1));
        await Assert.ThrowsAnyAsync<NpgsqlException>(() => store.EnsureCreatedAsync());

        // The lock-timeout arm of the DDL latches it (a failed long-running step latches it inside
        // the guard — see PostgreSqlDdlGuardTests).
        var arms = Decode(AsyncBody(typeof(PostgreSqlTransportStore), "EnsureCreatedCoreAsync"))
            .Select(instruction => instruction.Operand)
            .OfType<MethodBase>()
            .Count(method => method.Name == "BackOff");
        Assert.Equal(1, arms);
    }

    /// <summary>
    /// <c>dead_letter_reason</c> is <c>text</c>, and PostgreSQL <c>text</c> rejects a NUL character
    /// outright (SQLSTATE 22021) whatever the JSON columns' type, so a failure whose exception message
    /// carried one could never be dead-lettered. The raw message is bound with U+0000 replaced.
    /// </summary>
    [Fact]
    public void DeadLetterReason_ReplacesTheNulTextRejects()
    {
        var reason = PostgreSqlTransportStore.DeadLetterReason("bad \0 byte");

        Assert.DoesNotContain('\0', reason);
        Assert.Equal("bad \uFFFD byte", reason);
    }

    /// <summary>
    /// Round 42 (S9#15): the NAK sends no NOTIFY — the released row is not claimable until its
    /// redelivery delay has passed, so a wake made every idle subscriber of every queue, in every
    /// process, poll for nothing on each handler failure.
    /// </summary>
    [Fact]
    public void Nak_SendsNoNotify()
    {
        var nak = Literals(AsyncBody(typeof(PostgreSqlTransportStore), "NakAsync"));
        Assert.DoesNotContain(nak, literal => literal.Contains("pg_notify", StringComparison.Ordinal));
        Assert.Contains(nak, literal => literal.Contains("lock_id = @lock_id", StringComparison.Ordinal));
    }

    /// <summary>Round 42 (S9#15): the claim orders by the dequeue index's own key tail.</summary>
    [Fact]
    public void Claim_OrdersByAvailabilityThenPublish()
    {
        var claim = Literals(AsyncBody(typeof(PostgreSqlTransportStore), nameof(PostgreSqlTransportStore.TryClaimAsync)));
        Assert.Contains(claim, literal => literal.Contains("ORDER BY available_at, created_at", StringComparison.Ordinal));
    }

    /// <summary>
    /// The queue-scoped wake filter (S9#10): a listener naming its queue keeps only that queue's
    /// wakes; an unscoped one keeps all. An EMPTY payload — a foreign producer's bare
    /// <c>NOTIFY channel</c>, which the reply-target contract never promised to fill — still wakes
    /// every listener instead of demoting that producer to polling.
    /// </summary>
    [Theory]
    [InlineData(null, "worker", true)]
    [InlineData("worker", "worker", true)]
    [InlineData("worker", "response", false)]
    [InlineData("worker", "WORKER", false)]
    [InlineData("worker", "", true)]
    public void IsWakeFor_FiltersByQueue_AndKeepsABarePayload(string? queue, string payload, bool expected)
        => Assert.Equal(expected, PostgreSqlTransportStore.IsWakeFor(queue, payload));

    /// <summary>
    /// S9#11: the dead-letter prune is one bounded batch (the durable-flow <c>ctid … LIMIT</c>
    /// shape) that the shared drain loops, not one unbounded DELETE that outran the command timeout
    /// on the backlog an operator faces when first enabling retention, rolled back, and never shrank.
    /// </summary>
    [Fact]
    public void DeadLetterPrune_IsOneBoundedBatch()
    {
        var sql = PostgreSqlTransportStore.DeadLetterPruneSql("\"s\".\"jobs\"");

        Assert.Contains("WHERE ctid IN (", sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT 1000", sql, StringComparison.Ordinal);
        Assert.Contains("queue = @queue AND created_at < now() - @retention", sql, StringComparison.Ordinal);
    }

    private static readonly TimeSpan HangGuard = TimeSpan.FromSeconds(30);

    private static T Field<T>(object target, string name)
        => (T)target.GetType().GetField(name, BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(target)!;

    /// <summary>
    /// S5#18: the channel's table-wide prunes are bounded batches too; the one on the publish path
    /// was a single unbounded DELETE of everything expired since the last window.
    /// </summary>
    [Fact]
    public void ChannelExpiryPrune_IsOneBoundedBatch()
    {
        var sql = (string)typeof(PostgreSqlChannelSql)
            .GetMethod("ExpiredPruneSql", BindingFlags.NonPublic | BindingFlags.Static)!
            .Invoke(null, ["\"s\".\"messages\""])!;

        Assert.StartsWith("DELETE FROM \"s\".\"messages\" WHERE ctid IN (", sql, StringComparison.Ordinal);
        Assert.Contains("expires_at <= now() LIMIT 1000", sql, StringComparison.Ordinal);
    }
}
