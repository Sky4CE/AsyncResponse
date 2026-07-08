using Microsoft.Data.Sqlite;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using System.Data.Common;
using System.Text.Json;
using Xunit;

namespace AsyncResponse.Tests;

public sealed class DurableFlowStateStoreExampleTests
{
    [Fact]
    public async Task RelationalStore_RoundTrips_Expires_Deletes()
    {
        await using var database = await SqliteFlowStateDatabase.CreateAsync();
        var store = new RelationalFlowStateStore(database.OpenConnectionAsync);

        await AssertStoreContractAsync(store);
    }

    [Fact]
    public async Task DocumentStore_RoundTrips_Expires_Deletes()
        => await AssertStoreContractAsync(new DocumentFlowStateStore(new InMemoryFlowStateDocuments()));

    [Fact]
    public async Task KeyValueStore_RoundTrips_Expires_Deletes()
        => await AssertStoreContractAsync(new KeyValueFlowStateStore(new InMemoryFlowStateKeyValueTable()));

    [Fact]
    public async Task RelationalStore_RunsDurableFlowEndToEnd()
    {
        await using var database = await SqliteFlowStateDatabase.CreateAsync();
        await RunFlowWithStoreAsync(builder =>
        {
            builder.Services.AddSingleton<Func<CancellationToken, ValueTask<DbConnection>>>(database.OpenConnectionAsync);
            builder.WithDurableFlows<RelationalFlowStateStore>();
        });
    }

    [Fact]
    public async Task DocumentStore_RunsDurableFlowEndToEnd()
        => await RunFlowWithStoreAsync(builder =>
        {
            builder.Services.AddSingleton<IFlowStateDocuments, InMemoryFlowStateDocuments>();
            builder.WithDurableFlows<DocumentFlowStateStore>();
        });

    [Fact]
    public async Task KeyValueStore_RunsDurableFlowEndToEnd()
        => await RunFlowWithStoreAsync(builder =>
        {
            builder.Services.AddSingleton<IFlowStateKeyValueTable, InMemoryFlowStateKeyValueTable>();
            builder.WithDurableFlows<KeyValueFlowStateStore>();
        });

    private static async Task AssertStoreContractAsync(IFlowStateStore store)
    {
        var state = CreateState("flow-example");

        await store.SaveAsync(state.FlowId!, state, TimeSpan.FromMinutes(5));

        var loaded = await store.LoadAsync(state.FlowId!);
        Assert.NotNull(loaded);
        Assert.Equal(FlowRunStatus.Running, loaded!.Status);
        Assert.True(loaded.Steps!["step-a"].Completed);
        Assert.Equal("7", loaded.Values!["tenant"]);

        state.Status = FlowRunStatus.Succeeded;
        state.LastMessage = "done";
        await store.SaveAsync(state.FlowId!, state, TimeSpan.FromMinutes(5));
        Assert.Equal(FlowRunStatus.Succeeded, (await store.LoadAsync(state.FlowId!))!.Status);

        await store.SaveAsync("expired-flow", CreateState("expired-flow"), TimeSpan.FromMilliseconds(1));
        await Task.Delay(20);
        Assert.Null(await store.LoadAsync("expired-flow"));

        Assert.True(await store.TryDeleteAsync(state.FlowId!));
        Assert.Null(await store.LoadAsync(state.FlowId!));
        Assert.False(await store.TryDeleteAsync(state.FlowId!));
    }

    private static async Task RunFlowWithStoreAsync(Action<AsyncResponseRegistrationBuilder> configureStore)
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddSingleton<FlowProbe>();
        services.AddScoped<TestOnboardingFlow>();

        var builder = services.AddAsyncResponse()
            .WithInMemoryChannel(options =>
            {
                options.DefaultTimeout = TimeSpan.FromSeconds(10);
                options.RecoveryStateExpiry = TimeSpan.FromMinutes(5);
            })
            .WithInMemoryTransport();
        configureStore(builder);

        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        var flows = provider.GetRequiredService<IDurableFlows>();
        var executor = provider.GetRequiredService<IDurableFlowExecutor>();
        var publisher = provider.GetRequiredService<IAsyncResponsePublisher>();
        var probe = provider.GetRequiredService<FlowProbe>();

        var flowId = await flows.StartAsync<TestOnboardingFlow, TestFlowInput>(new TestFlowInput(7));
        var run = executor.ExecuteAsync(flowId);
        var correlationId = await probe.TriggerFired.Task.WaitAsync(TimeSpan.FromSeconds(5));

        await publisher.SetResponse(new OperationResult { Status = OperationStatus.Running, Message = "halfway" }, correlationId);
        await publisher.SetResponse(new OperationResult { Status = OperationStatus.Completed }, correlationId);
        await run.WaitAsync(TimeSpan.FromSeconds(5));

        var state = await flows.GetStateAsync(flowId);
        Assert.Equal(FlowRunStatus.Succeeded, state!.Status);
        Assert.True(state.Steps!["compute-stamp"].Completed);
        Assert.True(state.Steps["remote-op"].Completed);
        Assert.True(state.Steps["notify"].Completed);
    }

    private static FlowState CreateState(string flowId)
        => new()
        {
            FlowId = flowId,
            FlowTypeName = typeof(TestOnboardingFlow).FullName,
            InputTypeName = typeof(TestFlowInput).FullName,
            InputJson = JsonSerializer.Serialize(new TestFlowInput(7)),
            Status = FlowRunStatus.Running,
            LastMessage = "started",
            CreatedAtUtc = DateTime.UtcNow,
            UpdatedAtUtc = DateTime.UtcNow,
            Steps = new Dictionary<string, FlowStepState>(StringComparer.Ordinal)
            {
                ["step-a"] = new() { Completed = true, ResultJson = "123", CompletedAtUtc = DateTime.UtcNow }
            },
            Values = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["tenant"] = "7"
            }
        };

    private sealed class SqliteFlowStateDatabase : IAsyncDisposable
    {
        private readonly string _path;

        private SqliteFlowStateDatabase(string path) => _path = path;

        public static async Task<SqliteFlowStateDatabase> CreateAsync()
        {
            var database = new SqliteFlowStateDatabase(Path.Combine(Path.GetTempPath(), $"ar-flow-state-{Guid.NewGuid():N}.db"));
            await using var connection = await database.OpenConnectionAsync();
            await using var command = connection.CreateCommand();
            command.CommandText = """
                CREATE TABLE async_response_flow_state (
                    flow_id TEXT NOT NULL PRIMARY KEY,
                    state_json TEXT NOT NULL,
                    expires_at_utc TEXT NOT NULL,
                    updated_at_utc TEXT NOT NULL
                );

                CREATE INDEX ix_async_response_flow_state_expires_at_utc
                    ON async_response_flow_state (expires_at_utc);
                """;
            await command.ExecuteNonQueryAsync();
            return database;
        }

        public async ValueTask<DbConnection> OpenConnectionAsync(CancellationToken cancellationToken = default)
        {
            var connection = new SqliteConnection($"Data Source={_path}");
            await connection.OpenAsync(cancellationToken);
            return connection;
        }

        public ValueTask DisposeAsync()
        {
            File.Delete(_path);
            return ValueTask.CompletedTask;
        }
    }

    private sealed class RelationalFlowStateStore(Func<CancellationToken, ValueTask<DbConnection>> openConnection) : IFlowStateStore
    {
        public async Task SaveAsync(string flowId, FlowState state, TimeSpan ttl, CancellationToken cancellationToken = default)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(flowId);
            ArgumentNullException.ThrowIfNull(state);

            var now = DateTime.UtcNow;
            var json = JsonSerializer.Serialize(state);
            var expires = now.Add(ttl);

            await using var connection = await openConnection(cancellationToken);
            var updated = await ExecuteAsync(
                connection,
                """
                UPDATE async_response_flow_state
                   SET state_json = @state_json,
                       expires_at_utc = @expires_at_utc,
                       updated_at_utc = @updated_at_utc
                 WHERE flow_id = @flow_id
                """,
                cancellationToken,
                ("flow_id", flowId),
                ("state_json", json),
                ("expires_at_utc", expires),
                ("updated_at_utc", now));

            if (updated != 0)
                return;

            await ExecuteAsync(
                connection,
                """
                INSERT INTO async_response_flow_state
                    (flow_id, state_json, expires_at_utc, updated_at_utc)
                VALUES
                    (@flow_id, @state_json, @expires_at_utc, @updated_at_utc)
                """,
                cancellationToken,
                ("flow_id", flowId),
                ("state_json", json),
                ("expires_at_utc", expires),
                ("updated_at_utc", now));
        }

        public async Task<FlowState?> LoadAsync(string flowId, CancellationToken cancellationToken = default)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(flowId);

            await using var connection = await openConnection(cancellationToken);
            await using var command = CreateCommand(
                connection,
                """
                SELECT state_json
                  FROM async_response_flow_state
                 WHERE flow_id = @flow_id
                   AND expires_at_utc > @now_utc
                """,
                ("flow_id", flowId),
                ("now_utc", DateTime.UtcNow));

            var json = await command.ExecuteScalarAsync(cancellationToken) as string;
            if (json is null)
                return null;

            var state = JsonSerializer.Deserialize<FlowState>(json);
            return state is not null && FlowStateSchema.IsReadable(state.SchemaVersion) ? state : null;
        }

        public async Task<bool> TryDeleteAsync(string flowId, CancellationToken cancellationToken = default)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(flowId);

            await using var connection = await openConnection(cancellationToken);
            return await ExecuteAsync(
                connection,
                "DELETE FROM async_response_flow_state WHERE flow_id = @flow_id",
                cancellationToken,
                ("flow_id", flowId)) > 0;
        }

        private static async Task<int> ExecuteAsync(
            DbConnection connection,
            string sql,
            CancellationToken cancellationToken,
            params (string Name, object Value)[] parameters)
        {
            await using var command = CreateCommand(connection, sql, parameters);
            return await command.ExecuteNonQueryAsync(cancellationToken);
        }

        private static DbCommand CreateCommand(
            DbConnection connection,
            string sql,
            params (string Name, object Value)[] parameters)
        {
            var command = connection.CreateCommand();
            command.CommandText = sql;
            foreach (var (name, value) in parameters)
            {
                var parameter = command.CreateParameter();
                parameter.ParameterName = "@" + name;
                parameter.Value = value;
                command.Parameters.Add(parameter);
            }

            return command;
        }
    }

    private sealed record FlowStateDocument(
        string FlowId,
        string StateJson,
        DateTime ExpiresAtUtc,
        DateTime UpdatedAtUtc);

    private interface IFlowStateDocuments
    {
        Task UpsertAsync(FlowStateDocument document, CancellationToken cancellationToken);
        Task<FlowStateDocument?> FindAsync(string flowId, DateTime nowUtc, CancellationToken cancellationToken);
        Task<bool> DeleteAsync(string flowId, CancellationToken cancellationToken);
    }

    private sealed class DocumentFlowStateStore(IFlowStateDocuments documents) : IFlowStateStore
    {
        public Task SaveAsync(string flowId, FlowState state, TimeSpan ttl, CancellationToken cancellationToken = default)
        {
            var now = DateTime.UtcNow;
            return documents.UpsertAsync(
                new FlowStateDocument(flowId, JsonSerializer.Serialize(state), now.Add(ttl), now),
                cancellationToken);
        }

        public async Task<FlowState?> LoadAsync(string flowId, CancellationToken cancellationToken = default)
        {
            var document = await documents.FindAsync(flowId, DateTime.UtcNow, cancellationToken);
            if (document is null)
                return null;

            var state = JsonSerializer.Deserialize<FlowState>(document.StateJson);
            return state is not null && FlowStateSchema.IsReadable(state.SchemaVersion) ? state : null;
        }

        public Task<bool> TryDeleteAsync(string flowId, CancellationToken cancellationToken = default)
            => documents.DeleteAsync(flowId, cancellationToken);
    }

    private sealed class InMemoryFlowStateDocuments : IFlowStateDocuments
    {
        private readonly Dictionary<string, FlowStateDocument> _documents = new(StringComparer.Ordinal);

        public Task UpsertAsync(FlowStateDocument document, CancellationToken cancellationToken)
        {
            _documents[document.FlowId] = document;
            return Task.CompletedTask;
        }

        public Task<FlowStateDocument?> FindAsync(string flowId, DateTime nowUtc, CancellationToken cancellationToken)
            => Task.FromResult(_documents.TryGetValue(flowId, out var document) && document.ExpiresAtUtc > nowUtc
                ? document
                : null);

        public Task<bool> DeleteAsync(string flowId, CancellationToken cancellationToken)
            => Task.FromResult(_documents.Remove(flowId));
    }

    private interface IFlowStateKeyValueTable
    {
        Task PutAsync(string key, string json, DateTime expiresAtUtc, CancellationToken cancellationToken);
        Task<(string Json, DateTime ExpiresAtUtc)?> GetAsync(string key, DateTime nowUtc, CancellationToken cancellationToken);
        Task<bool> DeleteAsync(string key, CancellationToken cancellationToken);
    }

    private sealed class KeyValueFlowStateStore(IFlowStateKeyValueTable table) : IFlowStateStore
    {
        public Task SaveAsync(string flowId, FlowState state, TimeSpan ttl, CancellationToken cancellationToken = default)
            => table.PutAsync(flowId, JsonSerializer.Serialize(state), DateTime.UtcNow.Add(ttl), cancellationToken);

        public async Task<FlowState?> LoadAsync(string flowId, CancellationToken cancellationToken = default)
        {
            var item = await table.GetAsync(flowId, DateTime.UtcNow, cancellationToken);
            if (item is null)
                return null;

            var state = JsonSerializer.Deserialize<FlowState>(item.Value.Json);
            return state is not null && FlowStateSchema.IsReadable(state.SchemaVersion) ? state : null;
        }

        public Task<bool> TryDeleteAsync(string flowId, CancellationToken cancellationToken = default)
            => table.DeleteAsync(flowId, cancellationToken);
    }

    private sealed class InMemoryFlowStateKeyValueTable : IFlowStateKeyValueTable
    {
        private readonly Dictionary<string, (string Json, DateTime ExpiresAtUtc)> _items = new(StringComparer.Ordinal);

        public Task PutAsync(string key, string json, DateTime expiresAtUtc, CancellationToken cancellationToken)
        {
            _items[key] = (json, expiresAtUtc);
            return Task.CompletedTask;
        }

        public Task<(string Json, DateTime ExpiresAtUtc)?> GetAsync(string key, DateTime nowUtc, CancellationToken cancellationToken)
            => Task.FromResult(_items.TryGetValue(key, out var item) && item.ExpiresAtUtc > nowUtc
                ? item
                : ((string Json, DateTime ExpiresAtUtc)?)null);

        public Task<bool> DeleteAsync(string key, CancellationToken cancellationToken)
            => Task.FromResult(_items.Remove(key));
    }
}
