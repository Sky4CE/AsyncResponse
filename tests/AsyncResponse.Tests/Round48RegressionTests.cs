using AsyncResponse.Testing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Moq;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Linq.Expressions;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Review of 2026-09-27 (whole repository at ba8e8e70) — the regressions that can be written
/// against the pre-existing API, so this file compiles, and fails, on that commit. Pinned here:
/// F4 — telemetry that throws changed business outcomes — and the reviewer's end-to-end
/// reproduction of F2. The pins that live next to their harnesses: F2's executor paths in
/// <see cref="DurableFlowExecutorCoverageTests"/>, F1 (the Cosmos DB store refusing an account it
/// cannot honour its contract on) in <see cref="CosmosDurableFlowStateStoreTests"/>, and F3's
/// checkpoint-size instrument in <see cref="DurableFlowStoreSharedTests"/>. The API this round
/// added is pinned in <see cref="Round48NewApiTests"/>.
/// </summary>
public sealed class Round48RegressionTests
{
    // ---------------------------------------------------------------------------------------
    // F4 (HIGH): a MeterListener's measurement callback runs inside Counter.Add, on the recording
    // thread, and whatever it throws came out of the Add — into code that had just decided
    // something. Emission is now nonthrowing in the shared diagnostic helpers.
    // ---------------------------------------------------------------------------------------

    [Fact]
    public async Task ANonTerminalCheckpoint_WithAThrowingMetricsListener_KeepsItsRegistration_AndInvokesNoFailureCallback()
    {
        // A KeepWaiting response with nobody listening invokes nothing and keeps the registration
        // armed for the terminal response. The dispatch was then counted — and the listener's
        // exception made the publish "fail": the ingress retried it, escalated through
        // SetException, and the FAILURE callback ran and consumed the registration.
        var sink = new Round48FailureSink();
        await using var provider = Provider(services => services.AddSingleton(sink));
        var recovery = provider.GetRequiredService<IRecoveryStateStore>();
        await recovery.SaveAsync(
            "r48-progress",
            new RecoveryState
            {
                CorrelationId = "r48-progress",
                RegistrationId = Guid.NewGuid(),
                PayloadTypeFullName = typeof(Round48ProgressPayload).FullName,
                FailureCallback = new ReflectionCallDto
                {
                    ServiceInterfaceFullName = typeof(Round48FailureSink).FullName!,
                    MethodName = nameof(Round48FailureSink.Fail),
                    Params = [CallbackParam.ForPlaceholder(PlaceholderType.Exception)]
                }
            },
            TimeSpan.FromHours(1));
        using var metrics = new Round48ThrowingMetricsListener("asyncresponse.lost_subscriber.dispatches");

        await provider.GetRequiredService<IAsyncResponseIngress>().HandleResponseMessageAsync("{}", "r48-progress");

        Assert.True(metrics.Thrown > 0, "The listener was never reached: the test proved nothing.");
        Assert.Equal(0, sink.Calls);
        Assert.Single(await recovery.GetAllAsync("r48-progress"));
    }

    [Fact]
    public async Task AWorkerJob_WithAThrowingMetricsListener_IsNotReportedFailedAfterItRan()
    {
        // The "executed" outcome is recorded after the handler returned. The listener's exception
        // landed in the executor's catch: a job that had run was reported failed, and every
        // redelivery ran its side effects again.
        var sink = new Round48WorkSink();
        await using var provider = Provider(services => services.AddSingleton(sink));
        using var metrics = new Round48ThrowingMetricsListener("asyncresponse.worker.jobs");
        var job = AsyncResponseJson.Serialize(new WorkerJobEnvelope
        {
            Call = new ReflectionCallDto
            {
                ServiceInterfaceFullName = typeof(Round48WorkSink).FullName!,
                MethodName = nameof(Round48WorkSink.Run),
                Params = []
            }
        });

        await provider.GetRequiredService<IAsyncResponseIngress>().HandleWorkerMessageAsync(job);

        Assert.True(metrics.Thrown > 0, "The listener was never reached: the test proved nothing.");
        Assert.Equal(1, sink.Calls);
    }

    [Fact]
    public async Task ASpan_WhoseListenerThrowsWhenItStarts_CostsTheSpan_NotTheJob()
    {
        var sink = new Round48WorkSink();
        await using var provider = Provider(services => services.AddSingleton(sink));
        var thisFlow = new AsyncLocal<bool> { Value = true };
        var thrown = 0;
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == AsyncResponseDiagnostics.ActivitySourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) =>
            {
                // The source is process-wide and other tests run alongside: this flow's spans only.
                if (!thisFlow.Value)
                    return ActivitySamplingResult.None;

                Interlocked.Increment(ref thrown);
                throw new InvalidOperationException("trace sampler failed");
            }
        };
        ActivitySource.AddActivityListener(listener);
        var job = AsyncResponseJson.Serialize(new WorkerJobEnvelope
        {
            Call = new ReflectionCallDto
            {
                ServiceInterfaceFullName = typeof(Round48WorkSink).FullName!,
                MethodName = nameof(Round48WorkSink.Run),
                Params = []
            }
        });

        await provider.GetRequiredService<IAsyncResponseIngress>().HandleWorkerMessageAsync(job);

        Assert.True(thrown > 0, "The sampler was never reached: the test proved nothing.");
        Assert.Equal(1, sink.Calls);
    }

    [Fact]
    public void ASpan_WhoseListenerThrowsOnceItStarted_IsEnded_AndLeavesTheAmbientSpanAsItWas()
    {
        // A started callback runs after the span became the ambient one. Swallowing its exception
        // alone would leave that span current: the parent of everything the operation starts next,
        // and never exported itself.
        var thisFlow = new AsyncLocal<bool> { Value = true };
        var stopped = new List<string>();
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == AsyncResponseDiagnostics.ActivitySourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) =>
                thisFlow.Value ? ActivitySamplingResult.AllData : ActivitySamplingResult.None,
            ActivityStarted = activity =>
            {
                if (thisFlow.Value && activity.OperationName == "asyncresponse.test.round48_started")
                    throw new InvalidOperationException("trace exporter failed");
            },
            ActivityStopped = activity =>
            {
                if (thisFlow.Value)
                    lock (stopped) stopped.Add(activity.OperationName);
            }
        };
        ActivitySource.AddActivityListener(listener);
        using var ambient = new Activity("round48-ambient").Start();

        var span = AsyncResponseDiagnostics.StartActivity("asyncresponse.test.round48_started");

        Assert.Null(span);
        Assert.Same(ambient, Activity.Current);
        lock (stopped) Assert.Equal(["asyncresponse.test.round48_started"], stopped);

        // The next span starts under the ambient one, not under the span that failed to start.
        using var next = AsyncResponseDiagnostics.StartActivity("asyncresponse.test.round48_next");
        Assert.NotNull(next);
        Assert.Same(ambient, next.Parent);
    }

    // ---------------------------------------------------------------------------------------
    // F4, the recovery dispatcher: its log lines BEFORE a decision were unguarded (the ones after
    // a callback already went through SafeLog). A logging provider that throws failed the
    // dispatch itself — and the ingress then escalated a routed response through SetException.
    // ---------------------------------------------------------------------------------------

    [Fact]
    public async Task ANonTerminalCheckpoint_WithAThrowingLoggingProvider_KeepsItsRegistration_AndInvokesNoFailureCallback()
    {
        var sink = new Round48FailureSink();
        var logging = new ThrowingLoggerProvider();
        await using var provider = Provider(services => services.AddSingleton(sink), logging);
        var recovery = provider.GetRequiredService<IRecoveryStateStore>();
        await recovery.SaveAsync("r48-progress-log", Registration<Round48ProgressPayload>("r48-progress-log", failure: true), TimeSpan.FromHours(1));

        await provider.GetRequiredService<IAsyncResponseIngress>().HandleResponseMessageAsync("{}", "r48-progress-log");

        Assert.True(logging.Calls > 0, "The logging provider was never reached: the test proved nothing.");
        Assert.Equal(0, sink.Calls);
        Assert.Single(await recovery.GetAllAsync("r48-progress-log"));
    }

    [Fact]
    public async Task AResumableResponse_WithAThrowingLoggingProvider_InvokesItsResumeCallbackOnce_AndNoFailureCallback()
    {
        var resumed = new Round48WorkSink();
        var failed = new Round48FailureSink();
        var logging = new ThrowingLoggerProvider();
        await using var provider = Provider(services => services.AddSingleton(resumed).AddSingleton(failed), logging);
        var recovery = provider.GetRequiredService<IRecoveryStateStore>();
        await recovery.SaveAsync("r48-resume-log", Registration<Round48ResumePayload>("r48-resume-log", failure: true, resume: true), TimeSpan.FromHours(1));

        await provider.GetRequiredService<IAsyncResponseIngress>().HandleResponseMessageAsync("{}", "r48-resume-log");

        Assert.True(logging.Calls > 0, "The logging provider was never reached: the test proved nothing.");
        Assert.Equal(1, resumed.Calls);
        Assert.Equal(0, failed.Calls);
        Assert.Empty(await recovery.GetAllAsync("r48-resume-log"));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task AFailure_WithAThrowingLoggingProvider_InvokesItsFailureCallbackOnce(bool asException)
    {
        var failed = new Round48FailureSink();
        var logging = new ThrowingLoggerProvider();
        await using var provider = Provider(services => services.AddSingleton(failed), logging);
        var recovery = provider.GetRequiredService<IRecoveryStateStore>();
        var correlationId = $"r48-fail-log-{asException}";
        await recovery.SaveAsync(correlationId, Registration<Round48FailedPayload>(correlationId, failure: true), TimeSpan.FromHours(1));

        if (asException)
            await provider.GetRequiredService<IAsyncResponsePublisher>().SetException(new InvalidOperationException("remote failure"), correlationId);
        else
            await provider.GetRequiredService<IAsyncResponseIngress>().HandleResponseMessageAsync("{}", correlationId);

        Assert.True(logging.Calls > 0, "The logging provider was never reached: the test proved nothing.");
        Assert.Equal(1, failed.Calls);
        Assert.Empty(await recovery.GetAllAsync(correlationId));
    }

    // ---------------------------------------------------------------------------------------
    // F4, the starter: past its publish a start is committed — the run WILL execute — and the
    // flow id is the caller's only handle on it. A logging provider that throws (MEL's aggregate
    // logger rethrows a provider's failure) made StartAsync throw the provider's exception
    // instead of returning the id, so a caller retrying with a generated id started a second run.
    // ---------------------------------------------------------------------------------------

    public enum StartOutcome
    {
        Created,
        LedgerWriteFailed,
        ExistingLedgerUnreadable,
        ExistingLedgerExpired,
        ExistingLedgerIsTheSameStart
    }

    [Theory]
    [InlineData(StartOutcome.Created)]
    [InlineData(StartOutcome.LedgerWriteFailed)]
    [InlineData(StartOutcome.ExistingLedgerUnreadable)]
    [InlineData(StartOutcome.ExistingLedgerExpired)]
    [InlineData(StartOutcome.ExistingLedgerIsTheSameStart)]
    public async Task StartAsync_WithAThrowingLogger_StillReturnsTheIdOfTheRunItPublished(StartOutcome outcome)
    {
        var store = new Mock<IFlowStateStore>();
        FlowState? created = null;
        var create = store.Setup(instance => instance.TryCreateAsync(It.IsAny<string>(), It.IsAny<FlowState>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()));
        switch (outcome)
        {
            case StartOutcome.Created:
                create.Callback((string _, FlowState state, TimeSpan _, CancellationToken _) => created = state).ReturnsAsync(true);
                break;
            case StartOutcome.LedgerWriteFailed:
                create.ThrowsAsync(new TimeoutException("store unreachable"));
                break;
            default:
                create.Callback((string _, FlowState state, TimeSpan _, CancellationToken _) => created = state).ReturnsAsync(false);
                break;
        }

        var load = store.Setup(instance => instance.LoadAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()));
        switch (outcome)
        {
            case StartOutcome.ExistingLedgerUnreadable:
                load.ThrowsAsync(new TimeoutException("store unreachable"));
                break;
            case StartOutcome.ExistingLedgerIsTheSameStart:
                load.ReturnsAsync(() => created);
                break;
            default:
                load.ReturnsAsync((FlowState?)null);
                break;
        }

        var (flows, builder, logger) = Starter(store.Object);

        var flowId = await flows.StartAsync<Round40FlowLeaseTestSupport.CountingFlow, TestFlowInput>(new TestFlowInput(1));

        Assert.StartsWith("flow-", flowId, StringComparison.Ordinal);
        Assert.True(logger.Calls > 0, "The logger was never reached: the test proved nothing.");
        builder.Verify(instance => instance.EnqueueWorkerAsync(
            It.IsAny<Expression<Func<IDurableFlowExecutor, Task>>>(),
            It.IsAny<CancellationToken>()), Times.Once);
        if (created is not null)
            Assert.Equal(flowId, created.FlowId);
    }

    [Fact]
    public async Task StartAsync_WhoseExplicitIdCannotBeReadBeforeThePublish_WithAThrowingLogger_StillPublishes()
    {
        var store = new Mock<IFlowStateStore>();
        store.SetupSequence(instance => instance.LoadAsync("r48-explicit", It.IsAny<CancellationToken>()))
            .ThrowsAsync(new TimeoutException("store unreachable"));
        store.Setup(instance => instance.TryCreateAsync(It.IsAny<string>(), It.IsAny<FlowState>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);
        var (flows, builder, _) = Starter(store.Object);

        var flowId = await flows.StartAsync<Round40FlowLeaseTestSupport.CountingFlow, TestFlowInput>(new TestFlowInput(1), "r48-explicit");

        Assert.Equal("r48-explicit", flowId);
        builder.Verify(instance => instance.EnqueueWorkerAsync(
            It.IsAny<Expression<Func<IDurableFlowExecutor, Task>>>(),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task StartAsync_WhosePublishFails_WithAThrowingLogger_StillSurfacesTheExceptionThatCarriesTheId()
    {
        // A failed attempt may have landed all the same; DurableFlowNotDispatchedException.FlowId
        // is what lets the caller retry the SAME start. The logger's exception replaced it.
        var time = new VirtualTimeProvider();
        var (flows, builder, logger) = Starter(new InMemoryFlowStateStore(), time);
        builder.Setup(instance => instance.EnqueueWorkerAsync(
                It.IsAny<Expression<Func<IDurableFlowExecutor, Task>>>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(new TimeoutException("broker unreachable"));

        var start = flows.StartAsync<Round40FlowLeaseTestSupport.CountingFlow, TestFlowInput>(new TestFlowInput(1), "r48-not-dispatched");
        var guard = Stopwatch.StartNew();
        while (!start.IsCompleted)
        {
            if (guard.Elapsed > TimeSpan.FromSeconds(30))
                throw new TimeoutException("The start's publish ladder never finished on the virtual clock.");
            if (time.NextTimerDueAt is not null)
                time.Advance(TimeSpan.FromSeconds(2));
            await Task.Delay(1);
        }

        var ex = await Assert.ThrowsAsync<DurableFlowNotDispatchedException>(() => start);

        Assert.Equal("r48-not-dispatched", ex.FlowId);
        Assert.True(logger.Calls > 0, "The logger was never reached: the test proved nothing.");
    }

    [Fact]
    public async Task StartAsync_WhoseStartJobIsTooLarge_WithAThrowingLogger_StillSurfacesThatException()
    {
        var (flows, builder, logger) = Starter(new InMemoryFlowStateStore());
        builder.Setup(instance => instance.EnqueueWorkerAsync(
                It.IsAny<Expression<Func<IDurableFlowExecutor, Task>>>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(new WorkerJobTooLargeException(2048, 1024));

        var ex = await Assert.ThrowsAsync<WorkerJobTooLargeException>(
            () => flows.StartAsync<Round40FlowLeaseTestSupport.CountingFlow, TestFlowInput>(new TestFlowInput(1)));

        Assert.Equal(2048, ex.SerializedLength);
        Assert.True(logger.Calls > 0, "The logger was never reached: the test proved nothing.");
    }

    [Fact]
    public async Task TheOperatorsResume_OfAFinishedRun_WithAThrowingLogger_IsStillIgnoredQuietly()
    {
        var store = new InMemoryFlowStateStore();
        Assert.True(await store.TryCreateAsync(
            "r48-finished",
            new FlowState { FlowId = "r48-finished", Status = FlowRunStatus.Succeeded },
            TimeSpan.FromMinutes(5)));
        var (flows, builder, logger) = Starter(store);

        await flows.ResumeAsync("r48-finished");

        Assert.True(logger.Calls > 0, "The logger was never reached: the test proved nothing.");
        builder.Verify(instance => instance.EnqueueWorkerAsync(
            It.IsAny<Expression<Func<IDurableFlowExecutor, Task>>>(),
            It.IsAny<CancellationToken>()), Times.Never);
    }

    // ---------------------------------------------------------------------------------------
    // F2 (HIGH), end to end — the reviewer's reproduction. A terminal ledger is deleted and its
    // id reused; the process that receives the new run's response still reads the PREVIOUS run's
    // ledger. RecoverAsync trusted that copy (it looked again only behind Running or Suspended),
    // returned, and the dispatcher deleted the registration: zero current reads, zero checkpoint
    // writes, the response gone and the new run still pending.
    // ---------------------------------------------------------------------------------------

    [Theory]
    [InlineData(FlowRunStatus.Succeeded)]
    [InlineData(FlowRunStatus.Failed)]
    public async Task AResponseForAReusedFlowId_IsCheckpointedOnTheNewRun_BeforeItsRegistrationIsConsumed(FlowRunStatus previousRun)
    {
        var store = new ReusedIdStore(
            previous: new FlowState { FlowId = "r48-reused", Status = previousRun, Revision = 10 },
            current: new FlowState
            {
                FlowId = "r48-reused",
                Status = FlowRunStatus.Running,
                Steps = new Dictionary<string, FlowStepState> { ["await-result"] = new() { PendingCorrelationId = "r48-new-correlation" } }
            });
        await using var provider = Provider(services => services.AddSingleton<IFlowStateStore>(store));
        var recovery = provider.GetRequiredService<IRecoveryStateStore>();
        await recovery.SaveAsync(
            "r48-new-correlation",
            new RecoveryState
            {
                CorrelationId = "r48-new-correlation",
                RegistrationId = Guid.NewGuid(),
                PayloadTypeFullName = typeof(Round48ResumePayload).FullName,
                ResumeCallback = new ReflectionCallDto
                {
                    ServiceInterfaceFullName = typeof(IDurableFlowExecutor).FullName!,
                    MethodName = nameof(IDurableFlowExecutor.RecoverAsync),
                    Params =
                    [
                        CallbackParam.ForValue("r48-reused"),
                        CallbackParam.ForPlaceholder(PlaceholderType.Payload),
                        CallbackParam.ForPlaceholder(PlaceholderType.CorrelationId)
                    ]
                }
            },
            TimeSpan.FromHours(1));

        await provider.GetRequiredService<IAsyncResponsePublisher>().SetResponse(new Round48ResumePayload(), "r48-new-correlation");

        Assert.Equal(1, store.CurrentLoads);
        Assert.Equal(1, store.Updates);
        var step = store.Current.Steps!["await-result"];
        Assert.True(step.Completed);
        Assert.Null(step.PendingCorrelationId);
        Assert.NotNull(step.ResultJson);

        // Consumed — by a callback that checkpointed the response, not by one that dropped it.
        Assert.Empty(await recovery.GetAllAsync("r48-new-correlation"));
    }

    private static ServiceProvider Provider(Action<IServiceCollection> configure, ILoggerProvider? logging = null)
    {
        var services = new ServiceCollection();
        services.AddLogging(builder =>
        {
            if (logging is not null)
                builder.SetMinimumLevel(LogLevel.Trace).AddProvider(logging);
        });
        services.AddAsyncResponse().WithInMemoryChannel().WithInMemoryTransport().WithInMemoryDurableFlows();
        configure(services);
        return services.BuildServiceProvider();
    }

    private static RecoveryState Registration<TPayload>(string correlationId, bool failure, bool resume = false)
        => new()
        {
            CorrelationId = correlationId,
            RegistrationId = Guid.NewGuid(),
            PayloadTypeFullName = typeof(TPayload).FullName,
            ResumeCallback = resume
                ? new ReflectionCallDto
                {
                    ServiceInterfaceFullName = typeof(Round48WorkSink).FullName!,
                    MethodName = nameof(Round48WorkSink.Run),
                    Params = []
                }
                : null,
            FailureCallback = failure
                ? new ReflectionCallDto
                {
                    ServiceInterfaceFullName = typeof(Round48FailureSink).FullName!,
                    MethodName = nameof(Round48FailureSink.Fail),
                    Params = [CallbackParam.ForPlaceholder(PlaceholderType.Exception)]
                }
                : null
        };

    /// <summary>
    /// A logging provider that is down, behind the real Microsoft.Extensions.Logging pipeline:
    /// every category is enabled at every level, every line throws, and the aggregate logger
    /// rethrows it to the caller.
    /// </summary>
    private sealed class ThrowingLoggerProvider : ILoggerProvider
    {
        private int _calls;

        public int Calls => Volatile.Read(ref _calls);

        public ILogger CreateLogger(string categoryName) => new Sink(this);

        public void Dispose()
        {
        }

        private sealed class Sink(ThrowingLoggerProvider owner) : ILogger
        {
            public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

            public bool IsEnabled(LogLevel logLevel) => true;

            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
            {
                Interlocked.Increment(ref owner._calls);
                throw new InvalidOperationException("log sink failed");
            }
        }
    }

    private static (DurableFlowService Flows, Mock<IAsyncResponseBuilder> Builder, AlwaysThrowingLogger Logger) Starter(
        IFlowStateStore store,
        TimeProvider? timeProvider = null)
    {
        var services = new ServiceCollection();
        services.AddSingleton(store);
        var provider = services.BuildServiceProvider();
        var builder = new Mock<IAsyncResponseBuilder>();
        builder.Setup(instance => instance.EnqueueWorkerAsync(
                It.IsAny<Expression<Func<IDurableFlowExecutor, Task>>>(),
                It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        var logger = new AlwaysThrowingLogger();
        var flows = new DurableFlowService(
            provider.GetRequiredService<IServiceScopeFactory>(),
            builder.Object,
            new AsyncResponseContextPropagation([]),
            new DurableFlowOptions(),
            logger,
            timeProvider);
        return (flows, builder, logger);
    }

    /// <summary>A logging provider that is down: enabled at every level, and every line throws.</summary>
    private sealed class AlwaysThrowingLogger : ILogger<DurableFlowService>
    {
        private int _calls;

        public int Calls => Volatile.Read(ref _calls);

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            Interlocked.Increment(ref _calls);
            throw new InvalidOperationException("log sink failed");
        }
    }

    /// <summary>
    /// A store read from a process whose plain loads still see the id's previous run, while the
    /// authoritative read — and every write — sees the run that reused the id.
    /// </summary>
    private sealed class ReusedIdStore(FlowState previous, FlowState current) : IFlowStateStore
    {
        public FlowState Current { get; private set; } = current;

        public int CurrentLoads;

        public int Updates;

        public Task<FlowState?> LoadAsync(string flowId, CancellationToken cancellationToken = default)
            => Task.FromResult<FlowState?>(Copy(previous));

        public Task<FlowState?> LoadCurrentAsync(string flowId, CancellationToken cancellationToken = default)
        {
            Interlocked.Increment(ref CurrentLoads);
            return Task.FromResult<FlowState?>(Copy(Current));
        }

        public Task<bool> TryUpdateAsync(string flowId, FlowState state, long expectedRevision, TimeSpan ttl, string? leaseId = null, CancellationToken cancellationToken = default)
        {
            if (expectedRevision != Current.Revision)
                return Task.FromResult(false);

            Interlocked.Increment(ref Updates);
            Current = Copy(state);
            return Task.FromResult(true);
        }

        public Task<bool> TryCreateAsync(string flowId, FlowState state, TimeSpan ttl, CancellationToken cancellationToken = default)
            => Task.FromResult(false);

        public Task<bool> TryAcquireLeaseAsync(string flowId, string leaseId, TimeSpan leaseDuration, CancellationToken cancellationToken = default)
            => Task.FromResult(false);

        public Task<bool> TryRenewLeaseAsync(string flowId, string leaseId, TimeSpan leaseDuration, CancellationToken cancellationToken = default)
            => Task.FromResult(false);

        public Task ReleaseLeaseAsync(string flowId, string leaseId, CancellationToken cancellationToken = default)
            => Task.CompletedTask;

        public Task<bool> TryDeleteAsync(string flowId, CancellationToken cancellationToken = default)
            => Task.FromResult(false);

        private static FlowState Copy(FlowState state) => FlowStateJson.Deserialize(FlowStateJson.Serialize(state), state.FlowId!);
    }
}

/// <summary>
/// A metrics pipeline that is down, for the measurements recorded in THIS test's flow: the
/// meter is process-wide and other tests run alongside, and an exception out of one listener
/// also keeps the measurement from the listeners behind it.
/// </summary>
internal sealed class Round48ThrowingMetricsListener : IDisposable
{
    private readonly AsyncLocal<bool> _thisFlow = new();
    private readonly MeterListener _listener;
    private int _thrown;

    /// <param name="instrument">The instrument to fail, or <c>null</c> for every instrument of the library's meter.</param>
    public Round48ThrowingMetricsListener(string? instrument)
    {
        _thisFlow.Value = true;
        _listener = new MeterListener
        {
            InstrumentPublished = (published, listener) =>
            {
                if (published.Meter.Name == AsyncResponseDiagnostics.MeterName
                    && published is not ObservableInstrument<long>
                    && (instrument is null || published.Name == instrument))
                {
                    listener.EnableMeasurementEvents(published);
                }
            }
        };
        _listener.SetMeasurementEventCallback<long>((_, _, _, _) => Fail());
        _listener.SetMeasurementEventCallback<double>((_, _, _, _) => Fail());
        _listener.Start();
    }

    public int Thrown => Volatile.Read(ref _thrown);

    private void Fail()
    {
        if (!_thisFlow.Value)
            return;

        Interlocked.Increment(ref _thrown);
        throw new InvalidOperationException("metric sink failed");
    }

    public void Dispose() => _listener.Dispose();
}

public sealed class Round48WorkSink
{
    private int _calls;

    public int Calls => Volatile.Read(ref _calls);

    public Task Run()
    {
        Interlocked.Increment(ref _calls);
        return Task.CompletedTask;
    }
}

public sealed class Round48FailureSink
{
    private int _calls;

    public int Calls => Volatile.Read(ref _calls);

    public Task Fail(Exception exception)
    {
        Interlocked.Increment(ref _calls);
        return Task.CompletedTask;
    }
}

public sealed class Round48ProgressPayload : IAsyncResponsePayload
{
    public RecoveryAction OnRecovery() => RecoveryAction.KeepWaiting;
}

public sealed class Round48ResumePayload : IAsyncResponsePayload
{
    public RecoveryAction OnRecovery() => RecoveryAction.Resume;
}

public sealed class Round48FailedPayload : IAsyncResponsePayload
{
    public RecoveryAction OnRecovery() => RecoveryAction.Fail;
}
