using AsyncResponse.Testing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Round 39 (2026-09-14) regressions: lost-subscriber settlement over the WHOLE failure set,
/// body-free typed in-memory delivery, and the test harness refusing a "restart" that user code
/// survived. Each pin was proven red on f92f1e7.
/// </summary>
public sealed class Round39RegressionTests
{
    // ---------------------------------------------------------------------------------------
    // F1 (HIGH): shared-correlation dispatch settled on the FIRST failure only. A deterministic
    // failure ahead of a transient one hid the transient sibling: the message was acknowledged
    // and the transient registration — a valid waiter whose dependency was briefly down — lost
    // the only copy of its payload. The verdict depended on the order the store returned the
    // registrations in.
    // ---------------------------------------------------------------------------------------

    public interface IOrderedResumeSpy
    {
        Task ResumeOk(OperationResult payload);
        Task ResumeBoom(OperationResult payload);
    }

    /// <summary>No implementation is registered: wiring its callback up fails deterministically.</summary>
    public interface IUnregisteredResumeSpy
    {
        Task Resume(OperationResult payload);
    }

    public interface IOrderedFailSpy
    {
        Task FailOk(Exception exception);
        Task FailBoom(Exception exception);
    }

    public interface IUnregisteredFailSpy
    {
        Task Fail(Exception exception);
    }

    private sealed class OrderedSpy : IOrderedResumeSpy, IOrderedFailSpy
    {
        private int _ok;
        private int _boom;

        public int Ok => Volatile.Read(ref _ok);
        public int Boom => Volatile.Read(ref _boom);

        public Task ResumeOk(OperationResult payload)
        {
            Interlocked.Increment(ref _ok);
            return Task.CompletedTask;
        }

        public Task ResumeBoom(OperationResult payload)
        {
            Interlocked.Increment(ref _boom);
            throw new InvalidOperationException("re-enqueue failed on a publish-blocked broker");
        }

        public Task FailOk(Exception exception)
        {
            Interlocked.Increment(ref _ok);
            return Task.CompletedTask;
        }

        public Task FailBoom(Exception exception)
        {
            Interlocked.Increment(ref _boom);
            throw new InvalidOperationException("failure callback hit a dependency that is down");
        }
    }

    private static ServiceProvider BuildProvider(OrderedSpy spy, TimeProvider? timeProvider = null)
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        if (timeProvider is not null)
            services.AddSingleton(timeProvider);
        services.AddSingleton<IOrderedResumeSpy>(spy);
        services.AddSingleton<IOrderedFailSpy>(spy);
        services.AddAsyncResponse().WithInMemoryChannel();
        return services.BuildServiceProvider();
    }

    /// <summary>
    /// Every ordering of a permanent failure (P), a transient failure (T) and a success (S)
    /// across three registrations of one correlation id must end the same way: the response is
    /// left for redelivery (<see cref="RecoveryCallbackFailedException"/>), the success is
    /// consumed, and the two failed registrations stay armed. Pre-fix, P ahead of T (with S
    /// anywhere) returned normally: the message was acknowledged and T's payload was gone.
    /// </summary>
    [Theory]
    [InlineData("PTS")]
    [InlineData("PST")]
    [InlineData("SPT")]
    [InlineData("TPS")]
    [InlineData("TSP")]
    [InlineData("STP")]
    public async Task DispatchLostResponses_ATransientSiblingFailure_PreservesRedelivery_WhateverTheOrder(string order)
    {
        var correlationId = $"round39-response-{order}";
        var spy = new OrderedSpy();
        await using var provider = BuildProvider(spy);
        var store = provider.GetRequiredService<IRecoveryStateStore>();

        var successId = Guid.NewGuid();
        foreach (var kind in order)
        {
            var (id, callback) = kind switch
            {
                'S' => (successId, Resume(typeof(IOrderedResumeSpy), nameof(IOrderedResumeSpy.ResumeOk))),
                'T' => (Guid.NewGuid(), Resume(typeof(IOrderedResumeSpy), nameof(IOrderedResumeSpy.ResumeBoom))),
                _ => (Guid.NewGuid(), Resume(typeof(IUnregisteredResumeSpy), nameof(IUnregisteredResumeSpy.Resume)))
            };
            await store.SaveAsync(correlationId, new RecoveryState
            {
                RegistrationId = id,
                CorrelationId = correlationId,
                PayloadTypeFullName = typeof(OperationResult).FullName,
                RegisteredAtUtc = DateTime.UtcNow,
                ResumeCallback = callback
            }, TimeSpan.FromMinutes(5));
        }

        var publisher = provider.GetRequiredService<IAsyncResponsePublisher>();
        var ex = await Assert.ThrowsAsync<RecoveryCallbackFailedException>(() => publisher.SetResponse(
            new OperationResult { Status = OperationStatus.Completed, Message = "late response" },
            correlationId));

        Assert.Equal(correlationId, ex.CorrelationId);
        Assert.IsType<InvalidOperationException>(ex.InnerException, exactMatch: true);
        Assert.Equal(1, spy.Ok);
        Assert.Equal(1, spy.Boom);

        var remaining = await store.GetAllAsync(correlationId);
        Assert.Equal(2, remaining.Count);
        Assert.DoesNotContain(remaining, registration => registration.RegistrationId == successId);
    }

    /// <summary>The exception-envelope twin: every registration's FAILURE callback, same verdict.</summary>
    [Theory]
    [InlineData("PTS")]
    [InlineData("PST")]
    [InlineData("SPT")]
    [InlineData("TPS")]
    [InlineData("TSP")]
    [InlineData("STP")]
    public async Task DispatchLostExceptions_ATransientSiblingFailure_PreservesRedelivery_WhateverTheOrder(string order)
    {
        var correlationId = $"round39-exception-{order}";
        var spy = new OrderedSpy();
        await using var provider = BuildProvider(spy);
        var store = provider.GetRequiredService<IRecoveryStateStore>();

        var successId = Guid.NewGuid();
        foreach (var kind in order)
        {
            var (id, callback) = kind switch
            {
                'S' => (successId, Failure(typeof(IOrderedFailSpy), nameof(IOrderedFailSpy.FailOk))),
                'T' => (Guid.NewGuid(), Failure(typeof(IOrderedFailSpy), nameof(IOrderedFailSpy.FailBoom))),
                _ => (Guid.NewGuid(), Failure(typeof(IUnregisteredFailSpy), nameof(IUnregisteredFailSpy.Fail)))
            };
            await store.SaveAsync(correlationId, new RecoveryState
            {
                RegistrationId = id,
                CorrelationId = correlationId,
                PayloadTypeFullName = typeof(OperationResult).FullName,
                RegisteredAtUtc = DateTime.UtcNow,
                FailureCallback = callback
            }, TimeSpan.FromMinutes(5));
        }

        var publisher = provider.GetRequiredService<IAsyncResponsePublisher>();
        var ex = await Assert.ThrowsAsync<RecoveryCallbackFailedException>(
            () => publisher.SetException(new InvalidOperationException("remote boom"), correlationId));

        Assert.Equal(correlationId, ex.CorrelationId);
        Assert.Equal(1, spy.Ok);
        Assert.Equal(1, spy.Boom);

        var remaining = await store.GetAllAsync(correlationId);
        Assert.Equal(2, remaining.Count);
        Assert.DoesNotContain(remaining, registration => registration.RegistrationId == successId);
    }

    /// <summary>
    /// No success at all, with a deterministic failure AHEAD of a sibling whose failure-callback
    /// ladder was exhausted: the exhausted sibling's <see cref="RecoveryCallbackFailedException"/>
    /// must be what propagates, whatever its position. Pre-fix the first failure propagated, so
    /// the ingress would have burned its own retry ladder on the deterministic fault and then
    /// escalated through SetException — re-invoking the failure callback that had just given up.
    /// </summary>
    [Fact]
    public async Task DispatchLostResponses_WithNoSuccess_AnExhaustedSiblingPropagatesOverAnEarlierDeterministicFault()
    {
        const string correlationId = "round39-exhausted-precedence";
        var time = new VirtualTimeProvider();
        var spy = new OrderedSpy();
        await using var provider = BuildProvider(spy, time);
        var store = provider.GetRequiredService<IRecoveryStateStore>();

        // First: a resume callback nothing can wire up. Second: no resume callback, so the
        // resumable payload is routed to the failure callback, whose ladder exhausts.
        await store.SaveAsync(correlationId, new RecoveryState
        {
            RegistrationId = Guid.NewGuid(),
            CorrelationId = correlationId,
            PayloadTypeFullName = typeof(OperationResult).FullName,
            RegisteredAtUtc = DateTime.UtcNow,
            ResumeCallback = Resume(typeof(IUnregisteredResumeSpy), nameof(IUnregisteredResumeSpy.Resume))
        }, TimeSpan.FromMinutes(5));
        await store.SaveAsync(correlationId, new RecoveryState
        {
            RegistrationId = Guid.NewGuid(),
            CorrelationId = correlationId,
            PayloadTypeFullName = typeof(OperationResult).FullName,
            RegisteredAtUtc = DateTime.UtcNow,
            FailureCallback = Failure(typeof(IOrderedFailSpy), nameof(IOrderedFailSpy.FailBoom))
        }, TimeSpan.FromMinutes(5));

        var publisher = provider.GetRequiredService<IAsyncResponsePublisher>();
        var dispatching = publisher.SetResponse(
            new OperationResult { Status = OperationStatus.Completed, Message = "late response" },
            correlationId);

        // The failure-callback ladder backs off on the virtual clock; walk it.
        var deadline = DateTime.UtcNow.AddSeconds(10);
        while (!dispatching.IsCompleted && DateTime.UtcNow < deadline)
        {
            time.Advance(TimeSpan.FromSeconds(3));
            await Task.Delay(10);
        }

        var ex = await Assert.ThrowsAsync<RecoveryCallbackFailedException>(() => dispatching);
        Assert.Equal(correlationId, ex.CorrelationId);
        Assert.Equal(4, spy.Boom);
        Assert.Equal(2, (await store.GetAllAsync(correlationId)).Count);
    }

    private static ReflectionCallDto Resume(Type service, string method) => new()
    {
        ServiceInterfaceFullName = service.FullName!,
        MethodName = method,
        Params = [CallbackParam.ForPlaceholder(PlaceholderType.Payload)]
    };

    private static ReflectionCallDto Failure(Type service, string method) => new()
    {
        ServiceInterfaceFullName = service.FullName!,
        MethodName = method,
        Params = [CallbackParam.ForPlaceholder(PlaceholderType.Exception)]
    };

    // ---------------------------------------------------------------------------------------
    // F3: typed in-memory delivery materialized each waiter's payload through the raw reader.
    // A publisher's payload that does not fit the waiter's type fails INSIDE the payload, and
    // the reader's own message named the offending dictionary key — into the waiter's task and,
    // through SetError, into the wait activity's status.
    // ---------------------------------------------------------------------------------------

    public sealed class KeyedStringsPayload : IAsyncResponsePayload
    {
        public Dictionary<string, string> Values { get; set; } = [];
        public RecoveryAction OnRecovery() => RecoveryAction.Resume;
    }

    public sealed class KeyedIntsPayload : IAsyncResponsePayload
    {
        public Dictionary<string, int> Values { get; set; } = [];
        public RecoveryAction OnRecovery() => RecoveryAction.Resume;
    }

    [Fact]
    public async Task InMemoryTypedDelivery_APayloadThatDoesNotFitTheWaiter_FaultsWithoutTheBody()
    {
        const string customerKey = "private_customer_42@example.invalid";
        using var activities = new AsyncResponseActivityCollector();
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddAsyncResponse().WithInMemoryChannel();
        await using var provider = services.BuildServiceProvider();
        var channel = provider.GetRequiredService<InMemoryAsyncResponseChannel>();
        var publisher = provider.GetRequiredService<IAsyncResponsePublisher>();
        var correlationId = $"round39-typed-{Guid.NewGuid():N}";

        await using var waiter = await channel.CreateResponseWaiter<KeyedIntsPayload>(correlationId, timeout: TimeSpan.FromSeconds(5));
        await publisher.SetResponse(new KeyedStringsPayload { Values = { [customerKey] = "not an int" } }, correlationId);

        // Same failure shape as every broker channel: InvalidDataException, size and position
        // only, nothing of the body anywhere in the chain.
        var ex = await Assert.ThrowsAsync<InvalidDataException>(() => waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(5)));
        for (Exception? current = ex; current is not null; current = current.InnerException)
        {
            Assert.DoesNotContain(customerKey, current.Message, StringComparison.Ordinal);
            Assert.DoesNotContain("Values", current.Message, StringComparison.Ordinal);
        }

        foreach (var activity in activities.All())
        {
            Assert.DoesNotContain(customerKey, activity.StatusDescription ?? "", StringComparison.Ordinal);
            foreach (var tag in activity.TagObjects)
                Assert.DoesNotContain(customerKey, tag.Value?.ToString() ?? "", StringComparison.Ordinal);
        }
    }

    // ---------------------------------------------------------------------------------------
    // F7: SimulateRestartAsync proceeded past a step body that outlived the graceful stop — the
    // "dead" execution kept running beside the new incarnation and performed its side effect
    // after the restart had returned. The restart is cooperative and cannot kill it; it now
    // refuses to report a restart such an execution contradicts unless the test opts in.
    // ---------------------------------------------------------------------------------------

    public sealed record LingerInput(string Name);

    /// <summary>The step's dependency: blocks until the test releases it, ignoring cancellation.</summary>
    public sealed class LingerGate
    {
        private int _sideEffects;

        public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public int SideEffects => Volatile.Read(ref _sideEffects);
        public void RecordSideEffect() => Interlocked.Increment(ref _sideEffects);
    }

    public sealed class LingeringStepFlow(LingerGate _gate) : IDurableFlow<LingerInput>
    {
        public async Task ExecuteAsync(IDurableFlowContext flow, LingerInput input)
        {
            await flow.StepAsync("blocked", async () =>
            {
                _gate.Entered.TrySetResult();
                await _gate.Release.Task;
                _gate.RecordSideEffect();
            });
        }
    }

    [Fact]
    public async Task SimulateRestart_UserCodeStillRunningAfterTheStopLapsed_IsRefused()
    {
        var gate = new LingerGate();
        await using var harness = await AsyncResponseTestHarness.StartAsync(options =>
        {
            options.RealTimeGuard = TimeSpan.FromMilliseconds(300);
            options.ConfigureServices = services => services.AddSingleton(gate);
            options.ConfigureAsyncResponse = builder => builder.WithDurableFlow<LingeringStepFlow, LingerInput>();
        });

        await harness.Flows.StartAsync<LingeringStepFlow, LingerInput>(new LingerInput("linger"));
        await gate.Entered.Task.WaitAsync(TimeSpan.FromSeconds(5));

        // Released in finally: a failed assertion must not leave the step blocked forever under
        // the harness's disposal.
        InvalidOperationException ex;
        try
        {
            ex = await Assert.ThrowsAsync<InvalidOperationException>(() => harness.SimulateRestartAsync());
        }
        finally
        {
            gate.Release.TrySetResult();
        }

        Assert.Contains("could not establish quiescence", ex.Message, StringComparison.Ordinal);
        Assert.Contains(nameof(AsyncResponseTestHarnessOptions.AbandonLingeringExecutionsOnRestart), ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task SimulateRestart_WithTheOptIn_AbandonsTheExecution_WhichStillRunsAfterTheRestart()
    {
        var gate = new LingerGate();
        await using var harness = await AsyncResponseTestHarness.StartAsync(options =>
        {
            options.RealTimeGuard = TimeSpan.FromMilliseconds(300);
            options.AbandonLingeringExecutionsOnRestart = true;
            options.ConfigureServices = services => services.AddSingleton(gate);
            options.ConfigureAsyncResponse = builder => builder.WithDurableFlow<LingeringStepFlow, LingerInput>();
        });

        await harness.Flows.StartAsync<LingeringStepFlow, LingerInput>(new LingerInput("linger"));
        await gate.Entered.Task.WaitAsync(TimeSpan.FromSeconds(5));

        await harness.SimulateRestartAsync();
        Assert.Equal(0, gate.SideEffects);

        // The documented overlap the opt-in accepts: the abandoned execution is not dead, and its
        // side effect lands after the "restart".
        gate.Release.TrySetResult();
        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (gate.SideEffects == 0 && DateTime.UtcNow < deadline)
            await Task.Delay(10);
        Assert.Equal(1, gate.SideEffects);
    }
}
