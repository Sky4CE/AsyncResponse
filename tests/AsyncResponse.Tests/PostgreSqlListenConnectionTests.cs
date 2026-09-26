using AsyncResponse.Channels.PostgreSQL;
using AsyncResponse.Transports.PostgreSQL;
using Npgsql;
using System.Reflection;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// <c>src/Shared/PostgreSqlListenConnection.cs</c>: how a PostgreSQL <c>LISTEN</c> connection is
/// handed back when its listener stops. The file is source-linked into the channel and the
/// transport package, so every fact runs against both compiled copies (reached by reflection — the
/// two internal types share one full name). A <see cref="FakePostgresWireServer"/> stands in for
/// the server, since the states that matter — an <c>UNLISTEN</c> nobody answers, one answered with
/// an error — are exactly what a real container never does on cue.
/// </summary>
public sealed class PostgreSqlListenConnectionTests
{
    public enum Package
    {
        Channel,
        Transport
    }

    /// <summary>
    /// Pre-commit fix (fixpoint r1 D5): the teardown awaited <c>UNLISTEN *</c> in full. On a
    /// half-open socket — still <c>Open</c> to the client — that held a channel's
    /// <c>DisposeAsync</c> or a transport subscriber's restart for the command timeout, Npgsql's
    /// cancel request and its cancellation timeout (about 7 s, up to about 22 s). The wait is now
    /// bounded, and the release finishes in the background. The server here never answers and the
    /// command timeout is infinite, so only the bound can end the wait; the 30 s is a hang guard,
    /// not an assertion window. Red with the bound removed: the release never returned.
    /// </summary>
    [Theory]
    [InlineData(Package.Channel)]
    [InlineData(Package.Transport)]
    public async Task Release_AnUnlistenTheServerNeverAnswers_DoesNotHoldTheTeardown(Package package)
    {
        await using var server = new FakePostgresWireServer();
        var unlistenReceived = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var answer = new TaskCompletionSource<FakePostgresWireServer.Reply>(TaskCreationOptions.RunContinuationsAsynchronously);
        server.Respond = (_, sql) =>
        {
            if (!sql.StartsWith("UNLISTEN", StringComparison.Ordinal))
                return Task.FromResult(FakePostgresWireServer.Reply.Complete(sql));
            unlistenReceived.TrySetResult();
            return answer.Task;
        };
        await using var dataSource = NpgsqlDataSource.Create(server.ConnectionString());
        var connection = await dataSource.OpenConnectionAsync();

        await Release(package, dataSource, connection, listening: true, budget: TimeSpan.FromMilliseconds(50), commandTimeoutSeconds: 0).WaitAsync(TimeSpan.FromSeconds(30));

        await unlistenReceived.Task.WaitAsync(TimeSpan.FromSeconds(30));
        Assert.False(answer.Task.IsCompleted);
        // Let the background release finish before the server goes away.
        answer.TrySetResult(FakePostgresWireServer.Reply.Complete("UNLISTEN"));
    }

    /// <summary>
    /// The other half of D5: a connection whose <c>UNLISTEN</c> did not succeed (nor its one retry)
    /// but which is still open (the server answered with an error, or honoured the cancel of a
    /// timed-out one) is still listening, and must not go back to the pool — it is closed instead (the client sends
    /// Terminate). Red on the round's code: it was disposed back into the pool, still listening;
    /// and with <c>NpgsqlConnection.ClearPool</c> in place of the data source's own clear, which
    /// never reaches a data source's pool.
    /// </summary>
    [Theory]
    [InlineData(Package.Channel)]
    [InlineData(Package.Transport)]
    public async Task Release_AnUnlistenThatFails_ClosesTheStillListeningConnectionInsteadOfPoolingIt(Package package)
    {
        await using var server = new FakePostgresWireServer();
        server.Respond = (_, sql) => Task.FromResult(sql.StartsWith("UNLISTEN", StringComparison.Ordinal)
            ? FakePostgresWireServer.Reply.Error("unlisten refused")
            : FakePostgresWireServer.Reply.Complete(sql));
        await using var dataSource = NpgsqlDataSource.Create(server.ConnectionString());
        var connection = await dataSource.OpenConnectionAsync();

        await Release(package, dataSource, connection, listening: true, budget: TimeSpan.FromSeconds(30), commandTimeoutSeconds: 0);

        await server.Terminated(1).WaitAsync(TimeSpan.FromSeconds(30));
    }

    /// <summary>
    /// Pre-commit fix (fixpoint r1 pass 2): the pool the release clears is the application's own
    /// (the host registers one shared data source), and it was cleared whenever the UNLISTEN failed
    /// on a still-open connection — including one whose LISTEN had itself failed, which cannot be
    /// listening, so a server that rejected both wiped every connection EF Core and the rest of the
    /// application held on every listen retry. Without an established LISTEN the connection goes
    /// back to the pool. Red on the round's code: the pool was cleared, so the next open started a
    /// second session.
    /// </summary>
    [Theory]
    [InlineData(Package.Channel)]
    [InlineData(Package.Transport)]
    public async Task Release_AFailedUnlistenAfterAListenThatNeverTookHold_LeavesThePoolAlone(Package package)
    {
        await using var server = new FakePostgresWireServer();
        server.Respond = (_, sql) => Task.FromResult(sql.StartsWith("UNLISTEN", StringComparison.Ordinal)
            ? FakePostgresWireServer.Reply.Error("unlisten refused")
            : FakePostgresWireServer.Reply.Complete(sql));
        await using var dataSource = NpgsqlDataSource.Create(server.ConnectionString());
        var connection = await dataSource.OpenConnectionAsync();

        await Release(package, dataSource, connection, listening: false, budget: TimeSpan.FromSeconds(30), commandTimeoutSeconds: 0);

        await using var reused = await dataSource.OpenConnectionAsync();
        Assert.Equal(1, server.Sessions);
        Assert.False(server.Terminated(1).IsCompleted);
    }

    /// <summary>
    /// The healthy path is unchanged: an answered <c>UNLISTEN</c> hands the connection back to the
    /// pool, and the next open reuses it instead of starting a second session.
    /// </summary>
    [Theory]
    [InlineData(Package.Channel)]
    [InlineData(Package.Transport)]
    public async Task Release_AnAnsweredUnlisten_ReturnsTheConnectionToThePool(Package package)
    {
        await using var server = new FakePostgresWireServer();
        await using var dataSource = NpgsqlDataSource.Create(server.ConnectionString());
        var connection = await dataSource.OpenConnectionAsync();

        await Release(package, dataSource, connection, listening: true, budget: TimeSpan.FromSeconds(30), commandTimeoutSeconds: 0);

        await using var reused = await dataSource.OpenConnectionAsync();
        Assert.Equal(1, server.Sessions);
        Assert.Contains(server.Statements, statement => statement.Sql.StartsWith("UNLISTEN", StringComparison.Ordinal));
        Assert.False(server.Terminated(1).IsCompleted);
    }

    /// <summary>
    /// Fixpoint r2 S5#8 (i): an overloaded server that timed out the listener's liveness probe
    /// (honouring its cancel, so the connection stayed open) usually timed out the UNLISTEN as
    /// well, and every such listen cycle — about every 15 s while the overload lasted — cleared the
    /// application's whole pool. A failed UNLISTEN on a still-open connection after an established
    /// LISTEN is now retried once, with a longer timeout, before the pool is cleared; here the
    /// retry succeeds and the connection goes back to the pool. Red on the old code: the first
    /// failure cleared the pool, so the connection was closed and the next open started a second
    /// session.
    /// </summary>
    [Theory]
    [InlineData(Package.Channel)]
    [InlineData(Package.Transport)]
    public async Task Release_AFailedUnlistenThatSucceedsOnItsRetry_ReturnsTheConnectionToThePool(Package package)
    {
        await using var server = new FakePostgresWireServer();
        var unlistens = 0;
        server.Respond = (_, sql) => Task.FromResult(!sql.StartsWith("UNLISTEN", StringComparison.Ordinal)
            ? FakePostgresWireServer.Reply.Complete(sql)
            : Interlocked.Increment(ref unlistens) == 1
                ? FakePostgresWireServer.Reply.Error("canceling statement due to statement timeout")
                : FakePostgresWireServer.Reply.Complete(sql));
        await using var dataSource = NpgsqlDataSource.Create(server.ConnectionString());
        var connection = await dataSource.OpenConnectionAsync();

        await Release(package, dataSource, connection, listening: true, budget: TimeSpan.FromSeconds(30), commandTimeoutSeconds: 0);

        await using var reused = await dataSource.OpenConnectionAsync();
        Assert.Equal(1, server.Sessions);
        Assert.False(server.Terminated(1).IsCompleted);
        Assert.Equal(2, Volatile.Read(ref unlistens));
    }

    /// <summary>
    /// Fixpoint r2 (the round-1 pre-commit B-p2 #2 redo): the pool clear is now reported — but
    /// only once the connection is disposed, and a logging provider that throws cannot skip that
    /// disposal. Round 1's version logged inside the dispose <c>try</c>, so a throwing provider
    /// (Microsoft.Extensions.Logging rethrows a provider's failure) leaked the still-listening
    /// connection: its pool slot was never returned and its backend kept receiving every NOTIFY.
    /// Both UNLISTEN attempts fail here. Red with the log moved back inside the dispose try: the
    /// connection was never closed.
    /// </summary>
    [Theory]
    [InlineData(Package.Channel)]
    [InlineData(Package.Transport)]
    public async Task Release_ClearingThePool_IsLogged_AndALoggerThatThrowsStillClosesTheConnection(Package package)
    {
        await using var server = new FakePostgresWireServer();
        server.Respond = (_, sql) => Task.FromResult(sql.StartsWith("UNLISTEN", StringComparison.Ordinal)
            ? FakePostgresWireServer.Reply.Error("unlisten refused")
            : FakePostgresWireServer.Reply.Complete(sql));
        await using var dataSource = NpgsqlDataSource.Create(server.ConnectionString());
        var connection = await dataSource.OpenConnectionAsync();
        const string clearFragment = "cleared the NpgsqlDataSource's connection pool";
        var logger = new CollectingLogger { ThrowOnMessageContaining = clearFragment };

        await Release(package, dataSource, connection, listening: true, budget: TimeSpan.FromSeconds(30), commandTimeoutSeconds: 0, logger: logger);

        await server.Terminated(1).WaitAsync(TimeSpan.FromSeconds(30));
        var cleared = Assert.Single(logger.Entries, entry => entry.Message.Contains(clearFragment, StringComparison.Ordinal));
        Assert.Contains("unlisten refused", cleared.Exception?.Message, StringComparison.Ordinal);
        Assert.Equal(2, server.Statements.Count(statement => statement.Sql.StartsWith("UNLISTEN", StringComparison.Ordinal)));
    }

    private static Task Release(
        Package package,
        NpgsqlDataSource dataSource,
        NpgsqlConnection connection,
        bool listening,
        TimeSpan budget,
        int commandTimeoutSeconds,
        Microsoft.Extensions.Logging.ILogger? logger = null)
    {
        var release = ListenConnectionType(package)
            .GetMethod(
                "ReleaseAsync",
                BindingFlags.Static | BindingFlags.NonPublic | BindingFlags.Public,
                [typeof(NpgsqlDataSource), typeof(NpgsqlConnection), typeof(bool), typeof(Microsoft.Extensions.Logging.ILogger), typeof(TimeSpan), typeof(int), typeof(int)])!;
        return (Task)release.Invoke(null, [dataSource, connection, listening, logger, budget, commandTimeoutSeconds, commandTimeoutSeconds])!;
    }

    private static Type ListenConnectionType(Package package)
        => (package == Package.Channel ? typeof(PostgreSqlChannelSql).Assembly : typeof(PostgreSqlTransportStore).Assembly)
            .GetType("AsyncResponse.Internal.PostgreSqlListenConnection", throwOnError: true)!;
}
