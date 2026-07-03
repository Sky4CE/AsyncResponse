using AsyncResponse.Channels.NATS;
using AsyncResponse.Channels.Redis;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using StackExchange.Redis;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Runs a real multi-step flow (local step → awaited step → value bag → local step) with the
/// flow state persisted through each channel's recovery store reachable without Docker, proving
/// <see cref="FlowState"/> survives every store's serialization envelope — not just the in-memory
/// default. (PostgreSQL and SQL Server stores are SQL-side and covered by integration tests.)
/// </summary>
public class DurableFlowStoreMatrixTests
{
    [Fact]
    public Task InMemoryRecoveryStore_RunsFlowEndToEnd()
        => RunFlowAgainstStoreAsync(flowStateStore: null); // the default registration

    [Fact]
    public async Task NatsKvBackedStore_RunsFlowEndToEnd_AndStatePersistsInBucket()
    {
        var kv = new FakeNatsKvStore();
        var recoveryStore = new NatsRecoveryStateStore(
            kv,
            Options.Create(new NatsAsyncResponseChannelOptions()),
            NullLogger<NatsRecoveryStateStore>.Instance,
            new TestTimeProvider());

        var flowId = await RunFlowAgainstStoreAsync(new RecoveryBackedFlowStateStore(recoveryStore));

        // The ledger physically lives in the KV bucket, under the encoded recovery key.
        Assert.Contains(kv.Entries.Keys, key => key == NatsSubjectSchema.RecoveryKey(flowId));
    }

    [Fact]
    public async Task RedisBackedStore_RunsFlowEndToEnd_AndStatePersistsInKey()
    {
        var (database, backing) = CreateWriteThroughRedisDatabase();
        var multiplexer = new Mock<IConnectionMultiplexer>();
        multiplexer.Setup(m => m.GetDatabase(It.IsAny<int>(), It.IsAny<object?>())).Returns(database);

        var recoveryStore = new RedisRecoveryStateStore(
            multiplexer.Object,
            Options.Create(new RedisAsyncResponseOptions { KeyPrefix = "ar" }),
            NullLogger<RedisRecoveryStateStore>.Instance);

        var flowId = await RunFlowAgainstStoreAsync(new RecoveryBackedFlowStateStore(recoveryStore));

        Assert.Contains(backing.Keys, key => key.ToString().Contains(flowId));
    }

    private static async Task<string> RunFlowAgainstStoreAsync(IFlowStateStore? flowStateStore)
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddSingleton<FlowProbe>();
        services.AddScoped<TestOnboardingFlow>();
        if (flowStateStore is not null)
            services.AddSingleton(flowStateStore); // wins over the TryAdd default registration
        services.AddAsyncResponse()
            .WithInMemoryChannel(options =>
            {
                options.DefaultTimeout = TimeSpan.FromSeconds(10);
                options.RecoveryStateExpiry = TimeSpan.FromMinutes(5);
            })
            .WithInMemoryTransport();
        await using var provider = services.BuildServiceProvider();

        var flows = provider.GetRequiredService<IDurableFlows>();
        var executor = provider.GetRequiredService<IDurableFlowExecutor>();
        var publisher = provider.GetRequiredService<IAsyncResponsePublisher>();
        var probe = provider.GetRequiredService<FlowProbe>();

        var flowId = await flows.StartAsync<TestOnboardingFlow, TestFlowInput>(new TestFlowInput(7));

        var run = executor.ExecuteAsync(flowId);
        var correlationId = await probe.TriggerFired.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await publisher.SetResponse(new OperationResult { Status = OperationStatus.Running, Message = "halfway" }, correlationId);
        await publisher.SetResponse(new OperationResult { Status = OperationStatus.Completed }, correlationId);
        await run.WaitAsync(TimeSpan.FromSeconds(10));

        // Reload through the store (GetStateAsync is a raw store read — no in-process cache), so
        // these assertions prove the full serialize→envelope→deserialize round trip.
        var state = await flows.GetStateAsync(flowId);
        Assert.NotNull(state);
        Assert.Equal(FlowRunStatus.Succeeded, state!.Status);
        Assert.True(state.Steps!["compute-stamp"].Completed);
        Assert.Contains("107", state.Steps["compute-stamp"].ResultJson);
        Assert.True(state.Steps["remote-op"].Completed);
        Assert.Null(state.Steps["remote-op"].PendingCorrelationId);
        Assert.True(state.Steps["notify"].Completed);
        Assert.Contains("final-status", state.Values!.Keys);
        Assert.Equal(1, state.Attempts);

        return flowId;
    }

    private static (IDatabase Database, Dictionary<RedisKey, RedisValue> Backing) CreateWriteThroughRedisDatabase()
    {
        var backing = new Dictionary<RedisKey, RedisValue>();
        var gate = new object();

        var database = new Mock<IDatabase>();
        database
            .Setup(d => d.StringGetAsync(It.IsAny<RedisKey>(), It.IsAny<CommandFlags>()))
            .ReturnsAsync((RedisKey key, CommandFlags _) =>
            {
                lock (gate) return backing.TryGetValue(key, out var value) ? value : RedisValue.Null;
            });
        database
            .Setup(d => d.KeyDeleteAsync(It.IsAny<RedisKey>(), It.IsAny<CommandFlags>()))
            .ReturnsAsync((RedisKey key, CommandFlags _) =>
            {
                lock (gate) return backing.Remove(key);
            });

        var transaction = new Mock<ITransaction>();
        transaction
            .Setup(t => t.StringSetAsync(It.IsAny<RedisKey>(), It.IsAny<RedisValue>(), It.IsAny<TimeSpan?>(), It.IsAny<When>(), It.IsAny<CommandFlags>()))
            .ReturnsAsync((RedisKey key, RedisValue value, TimeSpan? _, When _, CommandFlags _) =>
            {
                lock (gate) backing[key] = value;
                return true;
            });
        transaction
            .Setup(t => t.StringSetAsync(It.IsAny<RedisKey>(), It.IsAny<RedisValue>(), It.IsAny<Expiration>(), It.IsAny<ValueCondition>(), It.IsAny<CommandFlags>()))
            .ReturnsAsync((RedisKey key, RedisValue value, Expiration _, ValueCondition _, CommandFlags _) =>
            {
                lock (gate) backing[key] = value;
                return true;
            });
        transaction
            .Setup(t => t.KeyDeleteAsync(It.IsAny<RedisKey>(), It.IsAny<CommandFlags>()))
            .ReturnsAsync((RedisKey key, CommandFlags _) =>
            {
                lock (gate) return backing.Remove(key);
            });
        transaction
            .Setup(t => t.ExecuteAsync(It.IsAny<CommandFlags>()))
            .ReturnsAsync(true);
        database
            .Setup(d => d.CreateTransaction(It.IsAny<object?>()))
            .Returns(transaction.Object);

        return (database.Object, backing);
    }
}
