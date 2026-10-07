using System.Diagnostics;
using System.Linq.Expressions;
using System.Reflection;
using System.Text.Json;
using AsyncResponse.Testing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Coverage-gap pins for the flow engine's failure plumbing: the execution lease's guarded
/// checkpoint-cause logging, pause/end over a faulted renewal and an abandoned release that fails
/// late; the lost-subscriber dispatcher's diagnostics-only fallbacks (an unserializable payload,
/// an out-of-limits type name, a failed batch delete, the payload-type tag); and the flow
/// executor's reflection fallback edges.
/// </summary>
public sealed class FlowEngineCoverageGapTests
{
    // ---------------------------------------------------------------------------------------
    // FlowExecutionLease
    // ---------------------------------------------------------------------------------------

    private static DurableFlowOptions LongLease => new()
    {
        ExecutionLeaseDuration = TimeSpan.FromDays(30),
        ExecutionLeaseRenewInterval = TimeSpan.FromDays(10)
    };

    private static Mock<IFlowStateStore> LeaseStore()
    {
        var store = new Mock<IFlowStateStore>();
        store.Setup(s => s.ReleaseLeaseAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        return store;
    }

    private static void SetupUpdate(Mock<IFlowStateStore> store, Func<CancellationToken, Task<bool>> update)
        => store.Setup(s => s.TryUpdateAsync(
                It.IsAny<string>(),
                It.IsAny<FlowState>(),
                It.IsAny<long>(),
                It.IsAny<TimeSpan>(),
                It.IsAny<string?>(),
                It.IsAny<CancellationToken>()))
            .Returns((string _, FlowState _, long _, TimeSpan _, string? _, CancellationToken token) => update(token));

    [Fact]
    public async Task ACheckpointWhoseSettleReadFails_MarksTheLeaseLost_AndLogsTheFailureItWasRecording()
    {
        var store = LeaseStore();
        using var cancel = new CancellationTokenSource();
        SetupUpdate(store, token =>
        {
            cancel.Cancel();
            return Task.FromException<bool>(new OperationCanceledException(token));
        });
        store.Setup(s => s.LoadCurrentAsync("gap-uncertain", It.IsAny<CancellationToken>())).ThrowsAsync(new TimeoutException("settle read failed"));
        var logger = new CollectingLogger();
        await using var lease = new FlowExecutionLease(store.Object, "gap-uncertain", "lease", LongLease, logger, new VirtualTimeProvider());
        var state = new FlowState { FlowId = "gap-uncertain", Revision = 3 };

        // The caller cancels its own write: the outcome is unknown, not failed.
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => lease.SaveAsync(state, TimeSpan.FromMinutes(1), cancel.Token));
        Assert.False(lease.IsLost);

        var cause = new InvalidOperationException("the attempt's own failure");
        await Assert.ThrowsAsync<TimeoutException>(() => lease.SaveAsync(state, TimeSpan.FromMinutes(1), cause: cause));

        Assert.True(lease.IsLost);
        var logged = Assert.Single(logger.Entries, entry => entry.Message.Contains("failed to checkpoint", StringComparison.Ordinal));
        Assert.Same(cause, logged.Exception);
    }

    [Fact]
    public async Task ACheckpointTheStoreFails_LogsTheFailureItWasRecording()
    {
        var store = LeaseStore();
        SetupUpdate(store, _ => Task.FromException<bool>(new TimeoutException("store down")));
        var logger = new CollectingLogger();
        await using var lease = new FlowExecutionLease(store.Object, "gap-store-down", "lease", LongLease, logger, new VirtualTimeProvider());
        var state = new FlowState { FlowId = "gap-store-down", Revision = 5 };
        var cause = new InvalidOperationException("the attempt's own failure");

        await Assert.ThrowsAsync<TimeoutException>(() => lease.SaveAsync(state, TimeSpan.FromMinutes(1), cause: cause));

        Assert.Equal(5, state.Revision);
        Assert.True(lease.IsLost);
        Assert.Same(cause, Assert.Single(logger.Entries).Exception);
    }

    [Fact]
    public async Task ARejectedCheckpoint_WhoseDiagnosisReadFails_StillReportsTheRejection()
    {
        var store = LeaseStore();
        SetupUpdate(store, _ => Task.FromResult(false));
        store.Setup(s => s.LoadAsync("gap-rejected", It.IsAny<CancellationToken>())).ThrowsAsync(new TimeoutException("diagnosis read failed"));
        await using var lease = new FlowExecutionLease(store.Object, "gap-rejected", "lease", LongLease, NullLogger.Instance, new VirtualTimeProvider());
        var cause = new InvalidOperationException("recorded failure");

        var ex = await Assert.ThrowsAsync<DurableFlowLeaseLostException>(() => lease.SaveAsync(new FlowState { FlowId = "gap-rejected", Revision = 1 }, TimeSpan.FromMinutes(1), cause: cause));

        Assert.Contains("no longer held", ex.Message, StringComparison.Ordinal);
        Assert.Same(cause, ex.InnerException);
    }

    [Fact]
    public async Task ResumeRenewal_WhileTheRenewalStillRuns_IsANoOp()
    {
        var store = LeaseStore();
        await using var lease = new FlowExecutionLease(store.Object, "gap-resume", "lease", LongLease, NullLogger.Instance, new VirtualTimeProvider());
        var renewal = typeof(FlowExecutionLease).GetField("_renewal", BindingFlags.Instance | BindingFlags.NonPublic)!;
        var before = renewal.GetValue(lease);

        lease.ResumeRenewal();

        Assert.Same(before, renewal.GetValue(lease));
    }

    [Fact]
    public async Task AFaultedRenewalLoop_StillPauses_AndEndingTheLeaseStillReleasesIt()
    {
        var store = LeaseStore();
        var lease = new FlowExecutionLease(store.Object, "gap-faulted-loop", "lease", LongLease, NullLogger.Instance, new VirtualTimeProvider());
        var renewal = typeof(FlowExecutionLease).GetField("_renewal", BindingFlags.Instance | BindingFlags.NonPublic)!;
        var running = (Task)renewal.GetValue(lease)!;

        // The loop swallows its own failures; one that faulted anyway has still ended.
        renewal.SetValue(lease, Task.FromException(new InvalidOperationException("renewal loop faulted")));
        Assert.True(await lease.PauseRenewalAsync());
        await running.WaitAsync(TimeSpan.FromSeconds(30));

        await lease.DisposeAsync();

        store.Verify(s => s.ReleaseLeaseAsync("gap-faulted-loop", "lease", It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task AnAbandonedRelease_ThatFailsLater_IsObservedAndLogged()
    {
        var clock = new VirtualTimeProvider();
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseCalled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var store = new Mock<IFlowStateStore>();
        store.Setup(s => s.ReleaseLeaseAsync("gap-late-release", "lease", It.IsAny<CancellationToken>()))
            .Returns(() =>
            {
                releaseCalled.TrySetResult();
                return release.Task;
            });
        var logger = new CollectingLogger();
        var lease = new FlowExecutionLease(store.Object, "gap-late-release", "lease", LongLease, logger, clock);

        var dispose = lease.DisposeAsync().AsTask();
        await releaseCalled.Task.WaitAsync(TimeSpan.FromSeconds(30));
        while (!dispose.IsCompleted)
        {
            clock.Advance(TimeSpan.FromSeconds(11));
            await Task.WhenAny(dispose, Task.Delay(10));
        }

        await dispose;
        Assert.Contains(logger.Messages, message => message.Contains("release did not complete within", StringComparison.Ordinal));

        release.SetException(new IOException("the store finally answered with an error"));

        await logger.WaitForAsync("eventually failed");
        Assert.IsType<IOException>(Assert.Single(logger.Entries, entry => entry.Message.Contains("eventually failed", StringComparison.Ordinal)).Exception);
    }

    // ---------------------------------------------------------------------------------------
    // LostSubscriberCallbackDispatcher
    // ---------------------------------------------------------------------------------------

    public interface IGapRecoverySpy
    {
        Task Resume(OperationResult payload);

        Task Fail(Exception exception);
    }

    public sealed class GapRecoverySpy : IGapRecoverySpy
    {
        public int Resumed;
        public readonly List<Exception> Failures = [];

        public Task Resume(OperationResult payload)
        {
            Interlocked.Increment(ref Resumed);
            return Task.CompletedTask;
        }

        public Task Fail(Exception exception)
        {
            lock (Failures)
                Failures.Add(exception);
            return Task.CompletedTask;
        }
    }

    /// <summary>A payload with no wire form: serializing it detects a cycle.</summary>
    public sealed class GapCyclicPayload : IAsyncResponsePayload
    {
        public GapCyclicPayload? Self { get; set; }

        public RecoveryAction OnRecovery() => RecoveryAction.Resume;
    }

    private static (ServiceProvider Provider, LostSubscriberCallbackDispatcher Dispatcher) Dispatcher(GapRecoverySpy spy, ILogger? logger = null)
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddSingleton<IGapRecoverySpy>(spy);
        services.AddAsyncResponse().WithInMemoryChannel();
        var provider = services.BuildServiceProvider();
        return (provider, new LostSubscriberCallbackDispatcher(
            provider.GetRequiredService<IServiceScopeFactory>(),
            provider.GetRequiredService<AsyncResponseContextPropagation>(),
            logger ?? NullLogger.Instance));
    }

    private static RecoveryState Registration(string correlationId, string? payloadType, bool resume = false, bool failure = true) => new()
    {
        RegistrationId = Guid.NewGuid(),
        CorrelationId = correlationId,
        PayloadTypeFullName = payloadType,
        RegisteredAtUtc = DateTime.UtcNow,
        ResumeCallback = resume
            ? new ReflectionCallDto
            {
                ServiceInterfaceFullName = typeof(IGapRecoverySpy).FullName!,
                MethodName = nameof(IGapRecoverySpy.Resume),
                Params = [CallbackParam.ForPlaceholder(PlaceholderType.Payload)]
            }
            : null,
        FailureCallback = failure
            ? new ReflectionCallDto
            {
                ServiceInterfaceFullName = typeof(IGapRecoverySpy).FullName!,
                MethodName = nameof(IGapRecoverySpy.Fail),
                Params = [CallbackParam.ForPlaceholder(PlaceholderType.Exception)]
            }
            : null
    };

    private sealed class ListRecoveryStore(params RecoveryState[] states) : IRecoveryStateStore, IRecoveryStateBatchDeletion
    {
        public Exception? BatchDeleteFailure { get; init; }

        public int BatchDeletes;

        public Task SaveAsync(string correlationId, RecoveryState state, TimeSpan ttl, CancellationToken cancellationToken = default)
            => Task.CompletedTask;

        public Task<IReadOnlyList<RecoveryState>> GetAllAsync(string correlationId, CancellationToken cancellationToken = default)
            => Task.FromResult<IReadOnlyList<RecoveryState>>(states);

        public Task<bool> TryDeleteAsync(string correlationId, Guid registrationId, CancellationToken cancellationToken = default)
            => Task.FromResult(true);

        public Task<int> TryDeleteManyAsync(string correlationId, IReadOnlyCollection<Guid> registrationIds, CancellationToken cancellationToken = default)
        {
            Interlocked.Increment(ref BatchDeletes);
            return BatchDeleteFailure is { } failure ? Task.FromException<int>(failure) : Task.FromResult(registrationIds.Count);
        }
    }

    [Fact]
    public async Task APayloadWithNoWireForm_TakesTheFailureRoute_WithoutDiagnosticJson()
    {
        var spy = new GapRecoverySpy();
        var (provider, dispatcher) = Dispatcher(spy);
        await using var _ = provider;
        var store = new ListRecoveryStore(Registration("gap-cyclic", typeof(GapCyclicPayload).FullName));
        var payload = new GapCyclicPayload();
        payload.Self = payload;

        var result = await dispatcher.DispatchLostResponses(store, "gap-cyclic", payload, "gap", CancellationToken.None);

        // Unclassifiable (it could never have crossed the wire): never resumed, failed instead.
        Assert.True(result.CallbackInvoked);
        var failure = Assert.IsType<AsyncResponseDomainFailureException>(Assert.Single(spy.Failures));
        Assert.Null(failure.PayloadJson);
    }

    [Fact]
    public async Task ARegistrationNamingAnOutOfLimitsType_IsReportedAndTreatedAsUnclassifiable()
    {
        var spy = new GapRecoverySpy();
        var logger = new CollectingLogger();
        var (provider, dispatcher) = Dispatcher(spy, logger);
        await using var _ = provider;
        var store = new ListRecoveryStore(Registration("gap-pointer-type", "Gap.Payload*"));

        await dispatcher.DispatchLostResponses(store, "gap-pointer-type", new OperationResult { Status = OperationStatus.Completed }, "gap", CancellationToken.None);

        Assert.Contains(logger.Messages, message => message.Contains("names a payload type that is not resolved: Gap.Payload*", StringComparison.Ordinal));
        Assert.Equal(0, spy.Resumed);
        Assert.IsType<AsyncResponseDomainFailureException>(Assert.Single(spy.Failures));
    }

    [Fact]
    public async Task AFailedBatchDelete_AfterSuccessfulCallbacks_IsLoggedNotThrown()
    {
        var spy = new GapRecoverySpy();
        var logger = new CollectingLogger();
        var (provider, dispatcher) = Dispatcher(spy, logger);
        await using var _ = provider;
        var store = new ListRecoveryStore(
            Registration("gap-batch", typeof(OperationResult).FullName, resume: true),
            Registration("gap-batch", typeof(OperationResult).FullName, resume: true))
        {
            BatchDeleteFailure = new TimeoutException("batch delete failed")
        };

        var result = await dispatcher.DispatchLostResponses(store, "gap-batch", new OperationResult { Status = OperationStatus.Completed }, "gap", CancellationToken.None);

        Assert.True(result.CallbackInvoked);
        Assert.Equal(2, spy.Resumed);
        Assert.Equal(1, store.BatchDeletes);
        var warning = Assert.Single(logger.Entries, entry => entry.Message.Contains("deleting their 2 registration(s) failed", StringComparison.Ordinal));
        Assert.IsType<TimeoutException>(warning.Exception);
    }

    [Fact]
    public async Task AnUnresolvableRegisteredType_IsTheDispatchSpansPayloadTypeTag()
    {
        var thisFlow = new AsyncLocal<bool> { Value = true };
        var tags = new List<string?>();
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == AsyncResponseDiagnostics.ActivitySourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) =>
                thisFlow.Value ? ActivitySamplingResult.AllData : ActivitySamplingResult.None,
            ActivityStopped = activity =>
            {
                if (thisFlow.Value && activity.OperationName == "asyncresponse.lost_subscriber.dispatch")
                    lock (tags) tags.Add(activity.GetTagItem("asyncresponse.payload_type") as string);
            }
        };
        ActivitySource.AddActivityListener(listener);
        var spy = new GapRecoverySpy();
        var (provider, dispatcher) = Dispatcher(spy);
        await using var _ = provider;
        var store = new ListRecoveryStore(Registration("gap-tag", "Gap.Renamed.PayloadType", failure: false));

        await dispatcher.DispatchLostResponses(store, "gap-tag", JsonDocument.Parse("""{"Status":2}""").RootElement, "gap", CancellationToken.None);

        // Neither the JSON container's type nor nothing: the registration's own (escaped) name.
        lock (tags) Assert.Equal(["Gap.Renamed.PayloadType"], tags);
    }

    // ---------------------------------------------------------------------------------------
    // DurableFlowExecutor
    // ---------------------------------------------------------------------------------------

    public sealed record GapInput(int A, string? B = null);

    public sealed class GapReflectedFlow : IDurableFlow<GapInput>
    {
        public Task ExecuteAsync(IDurableFlowContext context, GapInput input) => Task.CompletedTask;
    }

    private static bool ReflectedInputEquivalent(IServiceProvider provider, FlowState existing, string? persisted, string requested)
        => (bool)typeof(DurableFlowExecutor)
            .GetMethod("ReflectedInputEquivalent", BindingFlags.Static | BindingFlags.NonPublic)!
            .Invoke(null, [provider, existing, persisted, requested])!;

    [Fact]
    public async Task ReflectedInputEquivalence_ComparesValues_AndAnUnreadableSideIsAMismatch()
    {
        await using var provider = new ServiceCollection().AddSingleton<GapReflectedFlow>().BuildServiceProvider();
        var existing = new FlowState
        {
            FlowId = "gap-equivalence",
            FlowTypeName = typeof(GapReflectedFlow).FullName,
            InputTypeName = typeof(GapInput).FullName
        };

        Assert.False(ReflectedInputEquivalent(provider, existing, null, """{"A":1}"""));
        Assert.True(ReflectedInputEquivalent(provider, existing, """{"A":1}""", """{"A":1,"B":null}"""));
        Assert.False(ReflectedInputEquivalent(provider, existing, """{"A":1""", """{"A":1,"B":null}"""));
    }

    private static DurableFlowExecutor Executor(ServiceProvider provider, ILogger<DurableFlowExecutor>? logger = null, IEnumerable<DurableFlowRegistration>? registrations = null)
    {
        var builder = new Mock<IAsyncResponseBuilder>();
        builder.Setup(b => b.EnqueueWorkerAsync(It.IsAny<Expression<Func<IDurableFlowExecutor, Task>>>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        return new DurableFlowExecutor(
            provider.GetRequiredService<IServiceScopeFactory>(),
            builder.Object,
            Mock.Of<IAsyncResponseSubscriber>(),
            recoverableSubscriber: null,
            new AsyncResponseContextPropagation([]),
            new DurableFlowOptions(),
            logger ?? NullLogger<DurableFlowExecutor>.Instance,
            registrations: registrations);
    }

    [Fact]
    public async Task AFlowRegistrationWhoseFactoryReturnsAnotherType_FailsTheContractCheck()
    {
        var store = new InMemoryFlowStateStore();
        var services = new ServiceCollection();
        services.AddSingleton<IFlowStateStore>(store);
        services.AddSingleton(typeof(GapReflectedFlow), _ => new object());
        await using var provider = services.BuildServiceProvider();
        var state = new FlowState
        {
            FlowId = "gap-contract",
            Status = FlowRunStatus.Running,
            FlowTypeName = typeof(GapReflectedFlow).FullName,
            InputTypeName = typeof(GapInput).FullName,
            InputJson = """{"A":1}"""
        };
        Assert.True(await store.TryCreateAsync(state.FlowId!, state, TimeSpan.FromMinutes(5)));

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => Executor(provider).ExecuteAsync(state.FlowId!));

        Assert.Contains(nameof(GapReflectedFlow), ex.Message, StringComparison.Ordinal);
        Assert.Contains("IDurableFlow", ex.Message, StringComparison.Ordinal);
    }

    public sealed class GapConvertingParkFlow : IDurableFlow<TestFlowInput>
    {
        public async Task ExecuteAsync(IDurableFlowContext context, TestFlowInput input)
        {
            try
            {
                await context.AwaitChildFlowAsync<TestOnboardingFlow, TestFlowInput>("child", input);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException("flow code converted the park", ex);
            }
        }
    }

    [Fact]
    public async Task ARegisteredFlowThatConvertsItsPark_StaysParked_WithAWarning()
    {
        var store = new InMemoryFlowStateStore();
        await using var provider = new ServiceCollection()
            .AddSingleton<IFlowStateStore>(store)
            .AddSingleton<GapConvertingParkFlow>()
            .BuildServiceProvider();
        var registration = new DurableFlowRegistration
        {
            FlowTypeFullName = typeof(GapConvertingParkFlow).FullName!,
            InputTypeFullName = typeof(TestFlowInput).FullName!,
            FlowType = typeof(GapConvertingParkFlow),
            DeserializeInput = json => JsonSerializer.Deserialize<TestFlowInput>(json),
            ExecuteAsync = (flow, context, input) => ((GapConvertingParkFlow)flow).ExecuteAsync(context, (TestFlowInput)input!)
        };
        var state = new FlowState
        {
            FlowId = "gap-converted-park",
            Status = FlowRunStatus.Running,
            FlowTypeName = typeof(GapConvertingParkFlow).FullName,
            InputTypeName = typeof(TestFlowInput).FullName,
            InputJson = JsonSerializer.Serialize(new TestFlowInput(7))
        };
        Assert.True(await store.TryCreateAsync(state.FlowId!, state, TimeSpan.FromMinutes(5)));
        var logger = new CollectingLogger();

        await Executor(provider, logger.For<DurableFlowExecutor>(), [registration]).ExecuteAsync(state.FlowId!);

        var parent = await store.LoadAsync(state.FlowId!);
        Assert.Equal(FlowRunStatus.Running, parent!.Status);
        Assert.NotNull(await store.LoadAsync($"{state.FlowId}:child"));
        Assert.Contains(logger.Messages, message => message.Contains("converted the park's cancellation into System.InvalidOperationException", StringComparison.Ordinal));
    }
}
