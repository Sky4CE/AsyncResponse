using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using System.Linq.Expressions;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Release-audit hardening of the durable-flow recovery seam: the recovered checkpoint must
/// serialize as the step's DECLARED response type (a polymorphic derived payload's runtime-type
/// serialization drops the discriminator and breaks every replay), a Suspended run must preserve
/// the recovered terminal payload without being woken, and a re-attaching execution must
/// short-circuit on a checkpoint that recovery completed between its state load and its waiter
/// registration (recovery's wake-up would find the execution's own lease alive and ack as a
/// duplicate, leaving nothing to wake the parked wait).
/// </summary>
public class DurableFlowRecoveryHardeningTests
{
    [Fact]
    public async Task RecoverAsync_PolymorphicRecoveredPayload_CheckpointsAsTheDeclaredStepType()
    {
        var store = new InMemoryFlowStateStore();
        await SeedAsync(store, NewState("poly-recover", FlowRunStatus.Running, pendingType: typeof(PolyStepBase).FullName));
        await using var harness = Harness.Create(store);

        await harness.Executor.RecoverAsync("poly-recover", new PolyStepCompleted { Message = "rec" }, PendingCorrelationId);

        var state = await store.LoadAsync("poly-recover");
        var step = Assert.Contains(StepName, (IDictionary<string, FlowStepState>)state!.Steps!);
        Assert.True(step.Completed);
        Assert.Null(step.PendingCorrelationId);
        Assert.Null(step.PendingPayloadTypeFullName);
        // The checkpoint carries the discriminator, so replay's declared-type deserialization
        // round-trips the DERIVED payload instead of throwing on the abstract base.
        Assert.Contains("$kind", step.ResultJson, StringComparison.Ordinal);
        var replayed = AsyncResponseJson.Deserialize<PolyStepBase>(step.ResultJson!);
        var typed = Assert.IsType<PolyStepCompleted>(replayed);
        Assert.Equal("rec", typed.Message);
        harness.Builder.Verify(
            b => b.EnqueueWorkerAsync(It.IsAny<Expression<Func<IDurableFlowExecutor, Task>>>(), It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task RecoverAsync_WithoutRecordedDeclaredType_FallsBackToTheRuntimeType()
    {
        // Ledgers written before PendingPayloadTypeFullName existed: recovery keeps working by
        // serializing the payload's runtime type, exactly as before.
        var store = new InMemoryFlowStateStore();
        await SeedAsync(store, NewState("legacy-recover", FlowRunStatus.Running, pendingType: null));
        await using var harness = Harness.Create(store);

        await harness.Executor.RecoverAsync(
            "legacy-recover", new OperationResult { Status = OperationStatus.Completed, Message = "kept" }, PendingCorrelationId);

        var state = await store.LoadAsync("legacy-recover");
        var step = state!.Steps![StepName];
        Assert.True(step.Completed);
        var replayed = AsyncResponseJson.Deserialize<OperationResult>(step.ResultJson!);
        Assert.Equal("kept", replayed!.Message);
    }

    [Fact]
    public async Task RecoverAsync_SuspendedRun_CheckpointsTheTerminalPayloadWithoutWaking()
    {
        // Suspension is operator control: the run must not be woken — but the recovered response
        // exists nowhere else once the callback returns, so discarding it (the old behavior)
        // meant an un-suspended run re-attached to a correlation id nothing could answer.
        var store = new InMemoryFlowStateStore();
        await SeedAsync(store, NewState("suspended-recover", FlowRunStatus.Suspended, pendingType: typeof(OperationResult).FullName));
        await using var harness = Harness.Create(store);

        await harness.Executor.RecoverAsync(
            "suspended-recover", new OperationResult { Status = OperationStatus.Completed, Message = "held" }, PendingCorrelationId);

        var state = await store.LoadAsync("suspended-recover");
        Assert.Equal(FlowRunStatus.Suspended, state!.Status);
        var step = state.Steps![StepName];
        Assert.True(step.Completed);
        Assert.Contains("held", step.ResultJson, StringComparison.Ordinal);
        harness.Builder.Verify(
            b => b.EnqueueWorkerAsync(It.IsAny<Expression<Func<IDurableFlowExecutor, Task>>>(), It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task ExecuteAsync_RecoveryCheckpointLandingBeforeTheReattachPark_ShortCircuitsWithoutWaiting()
    {
        // The stall this closes: a redelivered execution loads state, sees the re-attach
        // breadcrumb, and registers its waiter — while recovery, racing it, consumes the response,
        // checkpoints the step, and enqueues a wake-up. The wake-up finds THIS execution's lease
        // alive and acks as a duplicate, so the parked wait would only end at the full step
        // timeout. The execution must re-read the persisted checkpoint once its registration
        // exists and take the recovered result immediately.
        ReattachProbeFlow.Reset();
        var store = new InMemoryFlowStateStore();
        var state = NewState("reattach-race", FlowRunStatus.Running, pendingType: typeof(OperationResult).FullName);
        state.FlowTypeName = typeof(ReattachProbeFlow).FullName;
        state.InputTypeName = typeof(HardeningFlowInput).FullName;
        state.InputJson = """{"Id":1}""";
        await SeedAsync(store, state);

        await using var harness = Harness.Create(
            store,
            services =>
            {
                services.AddSingleton<ReattachProbeFlow>();
                services.AddAsyncResponse().WithInMemoryChannel();
            },
            (provider, inner) => new CheckpointDuringRegistrationSubscriber(
                provider.GetRequiredService<IAsyncResponseSubscriber>(),
                store,
                flowId: "reattach-race",
                resultJson: AsyncResponseJson.Serialize(new OperationResult { Status = OperationStatus.Completed, Message = "recovered" })));

        // Bounded WELL below the 60s step timeout: without the post-registration re-read the
        // first execution parks until the step timeout because nothing can wake it. With it, the
        // execution takes the checkpointed result immediately; its FINAL save then loses the
        // revision CAS to recovery's bump — the designed "abandon and let the delivery retry"
        // path — so model the transport retry, which completes from the memoized checkpoint.
        async Task RunLikeATransportDeliveryAsync()
        {
            try
            {
                await harness.Executor.ExecuteAsync("reattach-race");
            }
            catch (InvalidOperationException)
            {
                await harness.Executor.ExecuteAsync("reattach-race");
            }
        }

        await RunLikeATransportDeliveryAsync().WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Equal(0, ReattachProbeFlow.TriggerCount);
        Assert.Equal("recovered", ReattachProbeFlow.Result);
        var final = await store.LoadAsync("reattach-race");
        Assert.Equal(FlowRunStatus.Succeeded, final!.Status);
    }

    // ---------------------------------------------------------------------------------------
    // Harness
    // ---------------------------------------------------------------------------------------

    private const string StepName = "remote";
    private const string PendingCorrelationId = "cid-pending";

    private static FlowState NewState(string flowId, FlowRunStatus status, string? pendingType)
        => new()
        {
            FlowId = flowId,
            Status = status,
            Steps = new Dictionary<string, FlowStepState>
            {
                [StepName] = new()
                {
                    PendingCorrelationId = PendingCorrelationId,
                    PendingPayloadTypeFullName = pendingType
                }
            }
        };

    private static async Task SeedAsync(InMemoryFlowStateStore store, FlowState state)
        => Assert.True(await store.TryCreateAsync(state.FlowId!, state, TimeSpan.FromMinutes(5)));

    private sealed class Harness : IAsyncDisposable
    {
        private readonly ServiceProvider _provider;

        private Harness(ServiceProvider provider, DurableFlowExecutor executor, Mock<IAsyncResponseBuilder> builder)
        {
            _provider = provider;
            Executor = executor;
            Builder = builder;
        }

        public DurableFlowExecutor Executor { get; }
        public Mock<IAsyncResponseBuilder> Builder { get; }

        public static Harness Create(
            IFlowStateStore store,
            Action<IServiceCollection>? configure = null,
            Func<ServiceProvider, IAsyncResponseSubscriber?, IAsyncResponseSubscriber>? subscriberFactory = null)
        {
            var services = new ServiceCollection();
            services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
            services.AddSingleton(store);
            configure?.Invoke(services);
            var provider = services.BuildServiceProvider();

            var builder = new Mock<IAsyncResponseBuilder>();
            builder.Setup(instance => instance.EnqueueWorkerAsync(
                    It.IsAny<Expression<Func<IDurableFlowExecutor, Task>>>(),
                    It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);

            var subscriber = subscriberFactory is null
                ? Mock.Of<IAsyncResponseSubscriber>()
                : subscriberFactory(provider, null);

            var executor = new DurableFlowExecutor(
                provider.GetRequiredService<IServiceScopeFactory>(),
                builder.Object,
                subscriber,
                recoverableSubscriber: null,
                new AsyncResponseContextPropagation([]),
                new DurableFlowOptions { DefaultStepTimeout = TimeSpan.FromSeconds(60) },
                NullLogger<DurableFlowExecutor>.Instance);

            return new Harness(provider, executor, builder);
        }

        public ValueTask DisposeAsync() => _provider.DisposeAsync();
    }

    /// <summary>
    /// Wraps the real channel subscriber and, INSIDE the step's waiter registration, completes the
    /// step checkpoint in the store — the deterministic encoding of recovery landing between the
    /// execution's state load and its park.
    /// </summary>
    private sealed class CheckpointDuringRegistrationSubscriber(
        IAsyncResponseSubscriber _inner,
        InMemoryFlowStateStore _store,
        string flowId,
        string resultJson) : IAsyncResponseSubscriber
    {
        public async Task<IAsyncResponseWaiter<T>> CreateResponseWaiter<T>(
            string correlationId,
            Func<T, ValueTask<bool>>? completionPredicate = null,
            TimeSpan? timeout = null) where T : IAsyncResponsePayload
        {
            var waiter = await _inner.CreateResponseWaiter(correlationId, completionPredicate, timeout);

            await FlowStateConcurrency.MutateAsync(
                _store,
                flowId,
                TimeSpan.FromMinutes(5),
            timeProvider: null,
                state =>
                {
                    var step = state.Steps![StepName];
                    step.Completed = true;
                    step.ResultJson = resultJson;
                    step.PendingCorrelationId = null;
                    step.PendingPayloadTypeFullName = null;
                    return true;
                });

            return waiter;
        }
    }
}

public sealed record HardeningFlowInput(int Id);

public sealed class ReattachProbeFlow : IDurableFlow<HardeningFlowInput>
{
    private static int _triggerCount;

    public static int TriggerCount => Volatile.Read(ref _triggerCount);
    public static string? Result { get; private set; }

    public static void Reset()
    {
        Volatile.Write(ref _triggerCount, 0);
        Result = null;
    }

    public async Task ExecuteAsync(IDurableFlowContext flow, HardeningFlowInput input)
    {
        var response = await flow.AwaitStepAsync<OperationResult>(
            "remote",
            _ =>
            {
                Interlocked.Increment(ref _triggerCount);
                return Task.CompletedTask;
            },
            timeout: TimeSpan.FromSeconds(60));
        Result = response.Message;
    }
}
