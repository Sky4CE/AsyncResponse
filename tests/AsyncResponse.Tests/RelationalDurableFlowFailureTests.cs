using AsyncResponse.DurableFlows.MySql;
using AsyncResponse.DurableFlows.Oracle;
using AsyncResponse.DurableFlows.Sqlite;
using AsyncResponse.DurableFlows.SqlServer;
using Microsoft.Extensions.Options;
using Xunit;

namespace AsyncResponse.Tests;

public sealed class RelationalDurableFlowFailureTests
{
    [Fact]
    public async Task Stores_DisposeConnectionsWhenOpenFails()
    {
        var sqlite = new SqliteFlowStateStore(Options.Create(new SqliteDurableFlowOptions
        {
            ConnectionString = $"Data Source={Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N"), "flows.db")}",
            AutoCreateSchema = false
        }));
        await Assert.ThrowsAnyAsync<Exception>(() => sqlite.LoadAsync("flow"));

        var sqlServer = new SqlServerFlowStateStore(Options.Create(new SqlServerDurableFlowOptions
        {
            ConnectionString = "Server=tcp:127.0.0.1,1;Database=flows;User ID=sa;Password=unused;Encrypt=False;Connect Timeout=1",
            AutoCreateSchema = false
        }));
        await Assert.ThrowsAnyAsync<Exception>(() => sqlServer.LoadAsync("flow"));

        var mySql = new MySqlFlowStateStore(Options.Create(new MySqlDurableFlowOptions
        {
            ConnectionString = "Server=127.0.0.1;Port=1;Database=flows;User ID=root;Password=unused;Connection Timeout=1",
            AutoCreateSchema = false
        }));
        await Assert.ThrowsAnyAsync<Exception>(() => mySql.LoadAsync("flow"));

        var oracle = new OracleFlowStateStore(Options.Create(new OracleDurableFlowOptions
        {
            ConnectionString = "User Id=flows;Password=unused;Data Source=127.0.0.1:1/XEPDB1",
            AutoCreateSchema = false
        }));
        await Assert.ThrowsAnyAsync<Exception>(() => oracle.LoadAsync("flow"));
    }

    [Fact]
    public async Task Stores_RejectNonPositiveLeaseDurationBeforeOpeningConnections()
    {
        var sqlite = new SqliteFlowStateStore(Options.Create(new SqliteDurableFlowOptions
        {
            ConnectionString = "Data Source=:memory:",
            AutoCreateSchema = false
        }));
        var sqlServer = new SqlServerFlowStateStore(Options.Create(new SqlServerDurableFlowOptions
        {
            ConnectionString = "Server=unused;Database=flows;Integrated Security=true",
            AutoCreateSchema = false
        }));
        var mySql = new MySqlFlowStateStore(Options.Create(new MySqlDurableFlowOptions
        {
            ConnectionString = "Server=unused;Database=flows;User ID=root",
            AutoCreateSchema = false
        }));
        var oracle = new OracleFlowStateStore(Options.Create(new OracleDurableFlowOptions
        {
            ConnectionString = "User Id=flows;Password=unused;Data Source=unused",
            AutoCreateSchema = false
        }));

        foreach (var store in new IFlowStateStore[] { sqlite, sqlServer, mySql, oracle })
        {
            await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
                store.TryAcquireLeaseAsync("flow", "owner", TimeSpan.Zero));
        }
    }

    [Fact]
    public async Task MySqlStore_RejectsUseAffectedRowsConnectionString_BeforeOpeningAConnection()
    {
        // Regression (review fix): UseAffectedRows=true switches ExecuteNonQuery from rows-MATCHED
        // to rows-CHANGED semantics, silently breaking lease renewal and update fencing. The store
        // must reject the connection STRING before opening any connection — the old code surfaced
        // an unrelated connection failure from the unreachable endpoint instead.
        var store = new MySqlFlowStateStore(Options.Create(new MySqlDurableFlowOptions
        {
            ConnectionString = "Server=127.0.0.1;Port=9;Database=x;Uid=u;Pwd=p;UseAffectedRows=true;Connection Timeout=1",
            AutoCreateSchema = false
        }));

        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => store.LoadAsync("flow"));
        stopwatch.Stop();

        Assert.Contains("UseAffectedRows", ex.Message, StringComparison.Ordinal);
        Assert.True(
            stopwatch.Elapsed < TimeSpan.FromSeconds(2),
            $"the guard took {stopwatch.Elapsed}; it must fail from the connection string alone, without a connection attempt");
    }

    [Fact]
    public async Task OracleStore_RechecksCreatedStateAfterWaitingForSchemaGate()
    {
        var store = new OracleFlowStateStore(Options.Create(new OracleDurableFlowOptions
        {
            ConnectionString = "User Id=flows;Password=unused;Data Source=127.0.0.1:1/XEPDB1"
        }));
        var storeType = typeof(OracleFlowStateStore);
        var gate = Assert.IsType<SemaphoreSlim>(storeType
            .GetField("_ensureGate", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!
            .GetValue(store));
        var createdField = storeType.GetField(
            "_created",
            System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!;

        await gate.WaitAsync();
        var released = false;
        try
        {
            var loadTask = store.LoadAsync("flow");
            createdField.SetValue(store, true);
            gate.Release();
            released = true;

            await Assert.ThrowsAnyAsync<Exception>(() => loadTask);
        }
        finally
        {
            if (!released)
                gate.Release();
        }
    }
}
