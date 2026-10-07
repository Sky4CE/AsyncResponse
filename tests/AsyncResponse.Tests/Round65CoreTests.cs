using System.Diagnostics.Metrics;
using AsyncResponse.Testing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Round 65 (2026-10-07) core regressions: a permanent resume fault settled on its own
/// registration (no whole-id escalation failing a KeepWaiting sibling), For&lt;T&gt;() no longer
/// overwriting the caller's ambient correlation id, the in-memory channel's exception snapshot
/// and log lines, the disposal exclusion from callback candidates, the payload-definition gate
/// before a generic instantiation is built, and the health check's bounded, escaped stale entries.
/// Each pin was proven red on 6d7e1aeb.
/// </summary>
public sealed class Round65CoreTests
{
    // ---------------------------------------------------------------------------------------
    // F-07: one registration's permanently broken resume target failed EVERY registration of
    // the correlation id — the dispatcher rethrew the fault raw and the ingress escalated it
    // through SetException, whose dispatch reloaded the whole id and ran each failure callback,
    // including a sibling whose payload had just classified as KeepWaiting.
    // ---------------------------------------------------------------------------------------

    public enum ProgressStatus
    {
        Running = 0,
        Done = 1,
        Failed = 2
    }

    /// <summary>Flow A's payload: a progress report is a checkpoint to wait past.</summary>
    public sealed class CheckpointPayload : IAsyncResponsePayload
    {
        public ProgressStatus Status { get; set; }

        public RecoveryAction OnRecovery() => Status == ProgressStatus.Done ? RecoveryAction.Resume : RecoveryAction.KeepWaiting;
    }

    /// <summary>Flow B's payload: any message is terminal.</summary>
    public sealed class TerminalPayload : IAsyncResponsePayload
    {
        public ProgressStatus Status { get; set; }

        public RecoveryAction OnRecovery() => Status == ProgressStatus.Failed ? RecoveryAction.Fail : RecoveryAction.Resume;
    }

    public interface IFlowSpy
    {
        Task Resume(string who, string correlationId);

        Task Fail(string who, object? payload, Exception exception, string correlationId);
    }

    /// <summary>Never registered in DI: wiring a callback up to it fails deterministically.</summary>
    public interface INotRegisteredFlow
    {
        Task Resume(string correlationId);
    }

    private sealed class FlowSpy : IFlowSpy
    {
        private readonly List<string> _calls = [];

        public List<(string Who, object? Payload, Exception Exception)> Failures { get; } = [];

        public IReadOnlyList<string> Calls
        {
            get
            {
                lock (_calls)
                    return [.. _calls];
            }
        }

        public Task Resume(string who, string correlationId)
        {
            lock (_calls)
                _calls.Add($"RESUME {who}");
            return Task.CompletedTask;
        }

        public Task Fail(string who, object? payload, Exception exception, string correlationId)
        {
            lock (_calls)
            {
                _calls.Add($"FAIL {who}");
                Failures.Add((who, payload, exception));
            }

            return Task.CompletedTask;
        }
    }

    private static ReflectionCallDto Call(Type service, string method, params CallbackParam[] parameters)
        => new() { ServiceInterfaceFullName = service.FullName!, MethodName = method, Params = parameters };

    private static ReflectionCallDto SpyResume(string who)
        => Call(typeof(IFlowSpy), nameof(IFlowSpy.Resume), CallbackParam.ForValue(who), CallbackParam.ForPlaceholder(PlaceholderType.CorrelationId));

    private static ReflectionCallDto SpyFail(string who)
        => Call(
            typeof(IFlowSpy),
            nameof(IFlowSpy.Fail),
            CallbackParam.ForValue(who),
            CallbackParam.ForPlaceholder(PlaceholderType.Payload),
            CallbackParam.ForPlaceholder(PlaceholderType.Exception),
            CallbackParam.ForPlaceholder(PlaceholderType.CorrelationId));

    private static ReflectionCallDto BrokenResume()
        => Call(typeof(INotRegisteredFlow), nameof(INotRegisteredFlow.Resume), CallbackParam.ForPlaceholder(PlaceholderType.CorrelationId));

    private static ServiceProvider BuildRecoveryProvider(FlowSpy spy)
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddSingleton<ILoggerFactory, NullLoggerFactory>();
        services.AddSingleton<IFlowSpy>(spy);
        services.AddAsyncResponse().WithInMemoryChannel();
        return services.BuildServiceProvider();
    }

    private static Task SaveAsync(IRecoveryStateStore store, string correlationId, Guid registrationId, Type payloadType, ReflectionCallDto? resume, ReflectionCallDto? failure)
        => store.SaveAsync(correlationId, new RecoveryState
        {
            RegistrationId = registrationId,
            CorrelationId = correlationId,
            PayloadTypeFullName = payloadType.FullName,
            RegisteredAtUtc = DateTime.UtcNow,
            ResumeCallback = resume,
            FailureCallback = failure
        }, TimeSpan.FromHours(1));

    [Fact]
    public async Task Ingress_APermanentResumeFault_FailsOnlyItsOwnRegistration_AndKeepsTheKeepWaitingSibling()
    {
        const string correlationId = "round65-f07-mixed";
        var spy = new FlowSpy();
        await using var provider = BuildRecoveryProvider(spy);
        var store = provider.GetRequiredService<IRecoveryStateStore>();
        var a = Guid.NewGuid();
        var b = Guid.NewGuid();
        await SaveAsync(store, correlationId, a, typeof(CheckpointPayload), SpyResume("A"), SpyFail("A"));
        await SaveAsync(store, correlationId, b, typeof(TerminalPayload), BrokenResume(), SpyFail("B"));

        // A progress message: A classifies KeepWaiting (invoke nothing, keep armed), B Resume.
        await provider.GetRequiredService<IAsyncResponseIngress>().HandleResponseMessageAsync("""{"Status":0}""", correlationId);

        Assert.Equal(["FAIL B"], spy.Calls);
        var failure = Assert.Single(spy.Failures);
        Assert.IsType<CallbackTargetUnresolvableException>(failure.Exception);
        var remaining = Assert.Single(await store.GetAllAsync(correlationId));
        Assert.Equal(a, remaining.RegistrationId);
    }

    [Fact]
    public async Task Ingress_APermanentResumeFault_OnASingleRegistration_RunsItsFailureCallbackExactlyOnce()
    {
        const string correlationId = "round65-f07-single-ingress";
        var spy = new FlowSpy();
        await using var provider = BuildRecoveryProvider(spy);
        var store = provider.GetRequiredService<IRecoveryStateStore>();
        await SaveAsync(store, correlationId, Guid.NewGuid(), typeof(TerminalPayload), BrokenResume(), SpyFail("B"));

        await provider.GetRequiredService<IAsyncResponseIngress>().HandleResponseMessageAsync("""{"Status":1}""", correlationId);

        Assert.Equal(["FAIL B"], spy.Calls);
        Assert.IsType<CallbackTargetUnresolvableException>(Assert.Single(spy.Failures).Exception);
        Assert.Empty(await store.GetAllAsync(correlationId));
    }

    [Fact]
    public async Task SetResponse_APermanentResumeFault_RoutesToTheFailureCallback_WithTheMaterializedPayload()
    {
        // A direct publisher (an HTTP callback endpoint) was handed the internal fault type and
        // the flow was never failed: only the broker ingress escalated.
        const string correlationId = "round65-f07-single-direct";
        var spy = new FlowSpy();
        await using var provider = BuildRecoveryProvider(spy);
        var store = provider.GetRequiredService<IRecoveryStateStore>();
        await SaveAsync(store, correlationId, Guid.NewGuid(), typeof(TerminalPayload), BrokenResume(), SpyFail("B"));

        await provider.GetRequiredService<IAsyncResponsePublisher>()
            .SetResponse(new TerminalPayload { Status = ProgressStatus.Done }, correlationId);

        var failure = Assert.Single(spy.Failures);
        Assert.Equal("B", failure.Who);
        Assert.Equal(ProgressStatus.Done, Assert.IsType<TerminalPayload>(failure.Payload).Status);
        Assert.IsType<CallbackTargetUnresolvableException>(failure.Exception);
        Assert.Empty(await store.GetAllAsync(correlationId));
    }

    [Fact]
    public async Task SetResponse_APermanentResumeFault_WithNoFailureCallback_IsAcknowledgedAndKeepsTheRegistration()
    {
        const string correlationId = "round65-f07-no-failure-callback";
        var spy = new FlowSpy();
        await using var provider = BuildRecoveryProvider(spy);
        var store = provider.GetRequiredService<IRecoveryStateStore>();
        var registration = Guid.NewGuid();
        await SaveAsync(store, correlationId, registration, typeof(TerminalPayload), BrokenResume(), failure: null);

        await provider.GetRequiredService<IAsyncResponsePublisher>()
            .SetResponse(new TerminalPayload { Status = ProgressStatus.Done }, correlationId);

        Assert.Empty(spy.Calls);
        Assert.Equal(registration, Assert.Single(await store.GetAllAsync(correlationId)).RegistrationId);
    }

    // ---------------------------------------------------------------------------------------
    // F-08: For<T>() wrote the generated id into the ambient context synchronously, in the
    // caller's frame — after a nested wait, a worker handler enqueued its follow-up jobs under
    // the finished nested wait's id instead of its own request's.
    // ---------------------------------------------------------------------------------------

    public interface IFollowUpWorker
    {
        Task Run(int value);
    }

    [Fact]
    public void For_WithAGeneratedId_LeavesTheCallersAmbientCorrelationIdUntouched()
    {
        AsyncResponseContext.SetCorrelationId("round65-outer");
        try
        {
            new AsyncResponseBuilder(Mock.Of<IAsyncResponseSubscriber>()).For<OperationResult>();
            Assert.Equal("round65-outer", AsyncResponseContext.CorrelationId);

            new RecoverableAsyncResponseBuilder(Mock.Of<IRecoverableAsyncResponseSubscriber>()).For<OperationResult>();
            Assert.Equal("round65-outer", AsyncResponseContext.CorrelationId);
        }
        finally
        {
            AsyncResponseContext.ClearCorrelationId();
        }
    }

    [Fact]
    public async Task NestedWait_InsideAJob_FollowUpWorkIsStillStampedWithTheJobsCorrelationId()
    {
        var waiter = new Mock<IAsyncResponseWaiter<OperationResult>>();
        waiter.SetupGet(w => w.ResponseTask).Returns(Task.FromResult(new OperationResult { Status = OperationStatus.Completed }));
        string? subscribedWith = null;
        var subscriber = new Mock<IAsyncResponseSubscriber>();
        subscriber
            .Setup(s => s.CreateResponseWaiter<OperationResult>(
                It.IsAny<string>(), It.IsAny<Func<OperationResult, ValueTask<bool>>?>(), It.IsAny<TimeSpan?>()))
            .Callback<string, Func<OperationResult, ValueTask<bool>>?, TimeSpan?>((correlationId, _, _) => subscribedWith = correlationId)
            .ReturnsAsync(waiter.Object);
        var published = new List<WorkerJobEnvelope>();
        var transport = new Mock<IWorkerTransport>();
        transport
            .Setup(t => t.PublishAsync(It.IsAny<WorkerJobEnvelope>(), It.IsAny<CancellationToken>()))
            .Callback<WorkerJobEnvelope, CancellationToken>((job, _) => published.Add(job))
            .Returns(Task.CompletedTask);
        var builder = new AsyncResponseBuilder(subscriber.Object, transport.Object);

        // The ambient id a worker handler runs under: its own job's (WorkerJobExecutor pushes it).
        AsyncResponseContext.SetCorrelationId("round65-job-id");
        try
        {
            string? ambientInsideTrigger = null;
            string? triggerId = null;
            await builder.For<OperationResult>().WaitAsync(context =>
            {
                ambientInsideTrigger = AsyncResponseContext.CorrelationId;
                triggerId = context.CorrelationId;
                return Task.CompletedTask;
            });

            // Inside the trigger the nested request's id is ambient, for the outgoing request.
            Assert.Equal(subscribedWith, triggerId);
            Assert.Equal(triggerId, ambientInsideTrigger);
            Assert.NotEqual("round65-job-id", triggerId);

            // After it, the handler is back on its own job's id.
            Assert.Equal("round65-job-id", AsyncResponseContext.CorrelationId);
            await builder.EnqueueWorkerAsync<IFollowUpWorker>(worker => worker.Run(1));
            Assert.Equal("round65-job-id", Assert.Single(published).CorrelationId);
        }
        finally
        {
            AsyncResponseContext.ClearCorrelationId();
        }
    }

    // ---------------------------------------------------------------------------------------
    // L-02: the in-memory channel latched the waiter terminal BEFORE reading the published
    // exception's (virtual) Message; a throwing getter escaped after the latch, so the waiter
    // was never settled and its timeout no-opped behind the latch — a wait that never ended.
    // ---------------------------------------------------------------------------------------

    private sealed class BrokenMessageException : Exception
    {
        public override string Message => throw new InvalidOperationException("Message getter throws");

        public override string? StackTrace => throw new InvalidOperationException("StackTrace getter throws");
    }

    [Fact]
    public async Task InMemory_SetException_WithAThrowingMessageGetter_StillSettlesTheWaiter()
    {
        var services = new ServiceCollection().AddLogging();
        services.AddAsyncResponse().WithInMemoryChannel();
        await using var provider = services.BuildServiceProvider();
        var subscriber = provider.GetRequiredService<IAsyncResponseSubscriber>();
        await using var waiter = await subscriber.CreateResponseWaiter<OperationResult>("round65-l02", timeout: TimeSpan.FromMinutes(5));

        await provider.GetRequiredService<IAsyncResponsePublisher>().SetException(new BrokenMessageException(), "round65-l02");

        var fault = await Assert.ThrowsAsync<Exception>(() => waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(10)));
        Assert.Equal(nameof(BrokenMessageException), fault.Message);
    }

    // ---------------------------------------------------------------------------------------
    // L-01: in-memory channel log lines after a state transition, unguarded — a throwing
    // logging provider reported a delivered response as a failed publish (a worker publishing
    // its reply was then retried), failed a fully registered create, and skipped the timeout
    // metric.
    // ---------------------------------------------------------------------------------------

    /// <summary>
    /// A logging provider behind the real Microsoft.Extensions.Logging pipeline that, once
    /// <see cref="Armed"/>, throws on every line at every level (the aggregate logger rethrows).
    /// </summary>
    private sealed class SwitchableThrowingLoggerProvider : ILoggerProvider
    {
        private volatile bool _armed;

        public bool Armed
        {
            get => _armed;
            set => _armed = value;
        }

        public ILogger CreateLogger(string categoryName) => new Sink(this);

        public void Dispose()
        {
        }

        private sealed class Sink(SwitchableThrowingLoggerProvider owner) : ILogger
        {
            public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

            public bool IsEnabled(LogLevel logLevel) => true;

            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
            {
                if (owner.Armed)
                    throw new InvalidOperationException("log sink failed");
            }
        }
    }

    private static ServiceProvider BuildInMemoryProvider(SwitchableThrowingLoggerProvider logs, TimeProvider? time = null)
    {
        var services = new ServiceCollection()
            .AddLogging(logging => logging.ClearProviders().AddProvider(logs).SetMinimumLevel(LogLevel.Debug));
        if (time is not null)
            services.AddSingleton(time);
        services.AddAsyncResponse().WithInMemoryChannel();
        return services.BuildServiceProvider();
    }

    [Fact]
    public async Task InMemory_SetResponse_DeliveredUnderAThrowingDebugProvider_ReturnsNormally()
    {
        var logs = new SwitchableThrowingLoggerProvider();
        await using var provider = BuildInMemoryProvider(logs);
        await using var waiter = await provider.GetRequiredService<IAsyncResponseSubscriber>()
            .CreateResponseWaiter<OperationResult>("round65-l01-typed", timeout: TimeSpan.FromMinutes(5));
        logs.Armed = true;

        await provider.GetRequiredService<IAsyncResponsePublisher>()
            .SetResponse(new OperationResult { Status = OperationStatus.Completed, Message = "x" }, "round65-l01-typed");

        Assert.Equal("x", (await waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(10))).Message);
    }

    [Fact]
    public async Task InMemory_RawResponse_DeliveredUnderAThrowingDebugProvider_ReturnsNormally()
    {
        var logs = new SwitchableThrowingLoggerProvider();
        await using var provider = BuildInMemoryProvider(logs);
        await using var waiter = await provider.GetRequiredService<IAsyncResponseSubscriber>()
            .CreateResponseWaiter<OperationResult>("round65-l01-raw", timeout: TimeSpan.FromMinutes(5));
        logs.Armed = true;

        await provider.GetRequiredService<IRawAsyncResponsePublisher>()
            .SetRawResponseJson("""{"Status":2,"Message":"raw"}""", "round65-l01-raw");

        Assert.Equal("raw", (await waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(10))).Message);
    }

    [Fact]
    public async Task InMemory_SetException_DeliveredUnderAThrowingDebugProvider_ReturnsNormally()
    {
        var logs = new SwitchableThrowingLoggerProvider();
        await using var provider = BuildInMemoryProvider(logs);
        await using var waiter = await provider.GetRequiredService<IAsyncResponseSubscriber>()
            .CreateResponseWaiter<OperationResult>("round65-l01-exception", timeout: TimeSpan.FromMinutes(5));
        logs.Armed = true;

        await provider.GetRequiredService<IAsyncResponsePublisher>()
            .SetException(new InvalidOperationException("remote boom"), "round65-l01-exception");

        var fault = await Assert.ThrowsAsync<Exception>(() => waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(10)));
        Assert.Equal("remote boom", fault.Message);
    }

    [Fact]
    public async Task InMemory_CreateResponseWaiter_UnderAThrowingDebugProvider_StillRegistersTheWait()
    {
        var logs = new SwitchableThrowingLoggerProvider { Armed = true };
        await using var provider = BuildInMemoryProvider(logs);

        await using var waiter = await provider.GetRequiredService<IAsyncResponseSubscriber>()
            .CreateResponseWaiter<OperationResult>("round65-l01-create", timeout: TimeSpan.FromMinutes(5));
        await provider.GetRequiredService<IAsyncResponsePublisher>()
            .SetResponse(new OperationResult { Status = OperationStatus.Completed, Message = "created" }, "round65-l01-create");

        Assert.Equal("created", (await waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(10))).Message);
    }

    private static readonly AsyncLocal<StrongBox<int>?> CountingTimeouts = new();

    [Fact]
    public async Task InMemory_Timeout_UnderAThrowingWarningProvider_StillRecordsTheTimeoutMetric()
    {
        var logs = new SwitchableThrowingLoggerProvider();
        var time = new VirtualTimeProvider();
        await using var provider = BuildInMemoryProvider(logs, time);

        // Counted on THIS test's async flow only: the meter is process-wide and tests run in
        // parallel; the virtual clock fires the waiter's timer on the flow that advances it.
        var counter = new StrongBox<int>();
        CountingTimeouts.Value = counter;
        using var listener = new MeterListener
        {
            InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Meter.Name == AsyncResponseDiagnostics.MeterName && instrument.Name == "asyncresponse.waiter.timeouts")
                    l.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<long>((_, _, _, _) =>
        {
            if (ReferenceEquals(CountingTimeouts.Value, counter))
                Interlocked.Increment(ref counter.Value);
        });
        listener.Start();

        var waiter = await provider.GetRequiredService<IAsyncResponseSubscriber>()
            .CreateResponseWaiter<OperationResult>("round65-l01-timeout", timeout: TimeSpan.FromSeconds(1));
        logs.Armed = true;
        time.Advance(TimeSpan.FromSeconds(2));
        await Assert.ThrowsAsync<TimeoutException>(() => waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(10)));

        // Disposal drains behind the per-waiter dispatch gate the timeout runs under, so once it
        // returns the timeout path has finished.
        await waiter.DisposeAsync();

        Assert.Equal(1, Volatile.Read(ref counter.Value));
    }

    private sealed class StrongBox<T>
    {
        public T Value = default!;
    }

    // Sweep (same class as L-01): the watchdog's own log lines decided its outcome. A throwing
    // provider escaped ExecuteAsync at "started" (the host stops under the default
    // BackgroundServiceExceptionBehavior), and inside a completed scan it turned the report into
    // "scan failed".

    private sealed class ThrowingLogger<T> : ILogger<T>
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
            => throw new InvalidOperationException("log sink failed");
    }

    private sealed class SingleEntryScanner(RecoveryState state) : IRecoveryStateScanner
    {
        public async IAsyncEnumerable<RecoveryState> ScanAsync(
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            yield return state;
            await Task.CompletedTask;
        }
    }

    private sealed class NoSubscriberProbe : IActiveSubscriberProbe
    {
        public ValueTask<long> CountActiveSubscribersAsync(string correlationId, CancellationToken cancellationToken = default)
            => ValueTask.FromResult(0L);
    }

    [Fact]
    public async Task Watchdog_UnderAThrowingLoggingProvider_StillPublishesTheReportItComputed()
    {
        var state = new AsyncResponseWatchdogState();
        var watchdog = new AsyncResponseWatchdog(
            [new SingleEntryScanner(new RecoveryState
            {
                CorrelationId = "round65-stale",
                PayloadTypeFullName = typeof(OperationResult).FullName,
                RegisteredAtUtc = DateTime.UtcNow.AddHours(-1)
            })],
            [new NoSubscriberProbe()],
            state,
            Microsoft.Extensions.Options.Options.Create(new AsyncResponseOptions
            {
                Watchdog = new AsyncResponseWatchdogOptions
                {
                    Enabled = true,
                    StartupDelay = TimeSpan.Zero,
                    Interval = TimeSpan.FromMinutes(5),
                    StaleAfter = TimeSpan.FromMinutes(1)
                }
            }),
            new ThrowingLogger<AsyncResponseWatchdog>());

        await watchdog.StartAsync(CancellationToken.None);
        try
        {
            var deadline = DateTime.UtcNow.AddSeconds(30);
            while (state.Latest is null && DateTime.UtcNow < deadline)
                await Task.Delay(20);
        }
        finally
        {
            await watchdog.StopAsync(CancellationToken.None);
        }

        var snapshot = Assert.IsType<AsyncResponseWatchdogSnapshot>(state.Latest);
        Assert.Null(snapshot.Error);
        Assert.Equal("round65-stale", Assert.Single(snapshot.Report!.StaleEntries).CorrelationId);
    }

    // ---------------------------------------------------------------------------------------
    // L-05: a type-level allowance of an interface deriving from IDisposable authorized Dispose
    // through the base-interface search: a worker job naming it disposed the DI singleton.
    // ---------------------------------------------------------------------------------------

    public interface IDisposableFlow : IDisposable, IAsyncDisposable
    {
        Task Run(int value);
    }

    private sealed class DisposableFlow : IDisposableFlow
    {
        public int Runs;
        public bool Disposed;

        public Task Run(int value)
        {
            Interlocked.Increment(ref Runs);
            return Task.CompletedTask;
        }

        public void Dispose() => Disposed = true;

        public ValueTask DisposeAsync()
        {
            Disposed = true;
            return ValueTask.CompletedTask;
        }
    }

    [Theory]
    [InlineData(nameof(IDisposable.Dispose))]
    [InlineData(nameof(IAsyncDisposable.DisposeAsync))]
    public async Task WorkerJob_NamingDisposalOnAnAllowedInterface_IsRefused_AndTheSingletonSurvives(string method)
    {
        var flow = new DisposableFlow();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<IDisposableFlow>(flow);
        services.AddAsyncResponse().AuthorizeCallbacks(a => a.Allow<IDisposableFlow>())
            .WithInMemoryChannel().WithInMemoryTransport();
        await using var provider = services.BuildServiceProvider();
        var ingress = provider.GetRequiredService<IAsyncResponseIngress>();

        var job = new WorkerJobEnvelope
        {
            Call = new ReflectionCallDto { ServiceInterfaceFullName = typeof(IDisposableFlow).FullName!, MethodName = method, Params = [] }
        };
        await Assert.ThrowsAsync<CallbackTargetUnresolvableException>(() => ingress.HandleWorkerMessageAsync(AsyncResponseJson.Serialize(job)));
        Assert.False(flow.Disposed);

        // The allowance itself still admits the interface's ordinary methods.
        var run = new WorkerJobEnvelope
        {
            Call = new ReflectionCallDto
            {
                ServiceInterfaceFullName = typeof(IDisposableFlow).FullName!,
                MethodName = nameof(IDisposableFlow.Run),
                Params = [CallbackParam.ForValue(1)]
            }
        };
        await ingress.HandleWorkerMessageAsync(AsyncResponseJson.Serialize(run));
        Assert.Equal(1, flow.Runs);
    }

    // ---------------------------------------------------------------------------------------
    // L-06: the payload resolver built any closed generic (or array) over loaded types BEFORE
    // the marker gate looked at it; the runtime keeps every constructed type for good.
    // ---------------------------------------------------------------------------------------

    public sealed class GenericEnvelope<T> : IAsyncResponsePayload
    {
        public T? Value { get; set; }

        public RecoveryAction OnRecovery() => RecoveryAction.Resume;
    }

    public sealed class Round65Key;

    public sealed class Round65Value;

    [Fact]
    public void ResolvePayloadType_ANonPayloadGenericDefinition_IsRefusedWithoutBeingInstantiated()
    {
        // A closed type is only ever handed back once it exists, so a null here (where the old
        // resolver returned the constructed Dictionary) means the scan never built it.
        Assert.Null(PayloadRecoveryClassifier.ResolvePayloadType(typeof(Dictionary<Round65Key, Round65Value>).FullName!));
        Assert.Null(PayloadRecoveryClassifier.ResolvePayloadType(typeof(Dictionary<Round65Key, Round65Value>).AssemblyQualifiedName!));
    }

    [Fact]
    public void ResolvePayloadType_AnArray_IsRefused()
    {
        Assert.Null(PayloadRecoveryClassifier.ResolvePayloadType(typeof(Round65Value[]).FullName!));
        Assert.Null(PayloadRecoveryClassifier.ResolvePayloadType(typeof(GenericEnvelope<Round65Key>[]).FullName!));
    }

    [Fact]
    public void ResolvePayloadType_AGenericPayload_StillResolves()
    {
        Assert.Equal(
            typeof(GenericEnvelope<Round65Key>),
            PayloadRecoveryClassifier.ResolvePayloadType(typeof(GenericEnvelope<Round65Key>).FullName!));
        Assert.Equal(
            typeof(GenericEnvelope<List<Round65Value>>),
            PayloadRecoveryClassifier.ResolvePayloadType(typeof(GenericEnvelope<List<Round65Value>>).AssemblyQualifiedName!));
        Assert.Equal(typeof(OperationResult), PayloadRecoveryClassifier.ResolvePayloadType(typeof(OperationResult).FullName!));
    }

    [Fact]
    public async Task LostResponse_ForAGenericPayloadRegistration_StillResumes()
    {
        const string correlationId = "round65-l06-generic";
        var spy = new FlowSpy();
        await using var provider = BuildRecoveryProvider(spy);
        var store = provider.GetRequiredService<IRecoveryStateStore>();
        await SaveAsync(store, correlationId, Guid.NewGuid(), typeof(GenericEnvelope<OperationResult>), SpyResume("G"), SpyFail("G"));

        await provider.GetRequiredService<IAsyncResponsePublisher>()
            .SetResponse(new GenericEnvelope<OperationResult> { Value = new OperationResult { Status = OperationStatus.Completed } }, correlationId);

        Assert.Equal(["RESUME G"], spy.Calls);
    }

    // ---------------------------------------------------------------------------------------
    // L-07: the health check copied store-written correlation ids and payload type names into
    // its Data verbatim — unbounded, CR/LF and all — while the watchdog's log line escaped them.
    // ---------------------------------------------------------------------------------------

    [Fact]
    public async Task HealthCheck_StaleEntries_QuoteStoreWrittenTextBoundedAndEscaped()
    {
        var hostileType = "Evil\r\n[Error] forged" + new string('t', 1_000_000);
        var hostileId = "corr\r\nforged" + new string('c', 100_000);
        var ordinaryType = typeof(OperationResult).FullName!;
        var report = new AsyncResponseWatchdogReport(
            2,
            0,
            [
                new RecoveryStateObservation(hostileId, DateTime.UtcNow.AddDays(-1), 0, hostileType),
                new RecoveryStateObservation("corr-ordinary", DateTime.UtcNow.AddDays(-1), 0, ordinaryType)
            ],
            0);
        var state = new AsyncResponseWatchdogState();
        state.Publish(new AsyncResponseWatchdogSnapshot(DateTime.UtcNow, TimeSpan.FromHours(6), report, Error: null));

        var result = await new AsyncResponseRecoveryHealthCheck(state).CheckHealthAsync(new HealthCheckContext());

        var entries = Assert.IsType<List<AsyncResponseStaleRecoveryEntry>>(result.Data["staleEntries"]);
        var hostile = entries[0];
        Assert.DoesNotContain('\r', hostile.PayloadType!);
        Assert.DoesNotContain('\n', hostile.PayloadType!);
        Assert.True(hostile.PayloadType!.Length < 1_000);
        Assert.DoesNotContain('\r', hostile.CorrelationId!);
        Assert.DoesNotContain('\n', hostile.CorrelationId!);
        // The portable id cap, plus the escapes of the two control characters and the ellipsis.
        Assert.True(hostile.CorrelationId!.Length < AsyncResponseChannelOptions.MaxCorrelationIdLength + 32);

        // An ordinary value reads exactly as before.
        Assert.Equal("corr-ordinary", entries[1].CorrelationId);
        Assert.Equal(ordinaryType, entries[1].PayloadType);
    }
}
