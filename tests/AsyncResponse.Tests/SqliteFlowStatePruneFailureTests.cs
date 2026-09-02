using AsyncResponse.DurableFlows.Sqlite;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Options;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Regression (round 33), proven against the real in-process SQLite store: TryCreateAsync awaited
/// its opportunistic prune bare, so a prune DELETE that failed — a deadlock victim or lock-wait
/// timeout on the server stores; here a trigger that aborts every DELETE — failed StartAsync for a
/// flow whose row would have been created without incident. The prune now runs quietly and the
/// ledger is created regardless.
/// </summary>
public sealed class SqliteFlowStatePruneFailureTests
{
    [Fact]
    public async Task TryCreate_SurvivesAFailedPrune_AndStillCreatesTheLedger()
    {
        await using var database = new TempSqlite();
        var store = new SqliteFlowStateStore(Options.Create(new SqliteDurableFlowOptions
        {
            ConnectionString = database.ConnectionString,
            // Zero prunes on every save, so the create under test is guaranteed to prune first.
            PruneInterval = TimeSpan.Zero
        }));

        // An expired ledger for the prune to find: a BEFORE DELETE trigger fires per matched row,
        // so a sweep over an empty backlog would never reach it.
        Assert.True(await store.TryCreateAsync("expired", NewState("expired"), TimeSpan.FromMilliseconds(1)));
        await Task.Delay(50);
        await database.ExecuteAsync(
            """
            CREATE TRIGGER no_prune BEFORE DELETE ON "asyncresponse_flow_state"
            BEGIN
                SELECT RAISE(ABORT, 'no prune');
            END;
            """);

        // Pre-fix: the aborted prune escaped as SqliteException and the ledger was never written.
        Assert.True(await store.TryCreateAsync("flow", NewState("flow"), TimeSpan.FromMinutes(5)));
        Assert.Equal("flow", (await store.LoadAsync("flow"))?.FlowId);
        // The prune was attempted and blocked: the expired row is still there.
        Assert.Equal(1, await database.CountAsync("expired"));

        // Control: with the trigger gone the very same path sweeps the expired row.
        await database.ExecuteAsync("DROP TRIGGER no_prune;");
        Assert.True(await store.TryCreateAsync("another", NewState("another"), TimeSpan.FromMinutes(5)));
        Assert.Equal(0, await database.CountAsync("expired"));
    }

    private static FlowState NewState(string flowId) => new()
    {
        FlowId = flowId,
        FlowTypeName = "PruneFailureFlow",
        Status = FlowRunStatus.Running,
        Steps = []
    };

    private sealed class TempSqlite : IAsyncDisposable
    {
        private readonly string _path = Path.Combine(Path.GetTempPath(), $"ar-prune-failure-{Guid.NewGuid():N}.db");

        public string ConnectionString => $"Data Source={_path}";

        public async Task ExecuteAsync(string sql)
        {
            await using var connection = new SqliteConnection(ConnectionString);
            await connection.OpenAsync();
            await using var command = connection.CreateCommand();
            command.CommandText = sql;
            await command.ExecuteNonQueryAsync();
        }

        public async Task<long> CountAsync(string flowId)
        {
            await using var connection = new SqliteConnection(ConnectionString);
            await connection.OpenAsync();
            await using var command = connection.CreateCommand();
            command.CommandText = """SELECT COUNT(*) FROM "asyncresponse_flow_state" WHERE flow_id = $flow_id;""";
            command.Parameters.AddWithValue("$flow_id", flowId);
            return (long)(await command.ExecuteScalarAsync())!;
        }

        public ValueTask DisposeAsync()
        {
            SqliteConnection.ClearPool(new SqliteConnection(ConnectionString));
            foreach (var suffix in new[] { "", "-wal", "-shm" })
            {
                try
                {
                    File.Delete(_path + suffix);
                }
                catch (IOException)
                {
                }
            }

            return ValueTask.CompletedTask;
        }
    }
}
