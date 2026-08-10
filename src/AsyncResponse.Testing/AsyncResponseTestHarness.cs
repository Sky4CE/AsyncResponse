using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace AsyncResponse.Testing;

/// <summary>Options for <see cref="AsyncResponseTestHarness.StartAsync"/>.</summary>
public sealed class AsyncResponseTestHarnessOptions
{
    /// <summary>The virtual clock's start instant. Default: <see cref="VirtualTimeProvider.DefaultStartTime"/>.</summary>
    public DateTimeOffset? StartTime { get; set; }

    /// <summary>Extra service registrations (fakes for the services your flows and triggers inject).</summary>
    public Action<IServiceCollection>? ConfigureServices { get; set; }

    /// <summary>
    /// Continues the <c>AddAsyncResponse()</c> fluent chain: register flows
    /// (<c>WithDurableFlow</c>), schedules (<c>WithScheduledFlow</c>), context propagators, …
    /// The in-memory channel, transport, and flow store are already registered.
    /// </summary>
    public Action<AsyncResponseRegistrationBuilder>? ConfigureAsyncResponse { get; set; }

    /// <summary>In-memory channel options (timeouts, recovery expiry).</summary>
    public Action<InMemoryAsyncResponseOptions>? Channel { get; set; }

    /// <summary>In-memory worker transport options (concurrency, retry budget).</summary>
    public Action<InMemoryWorkerTransportOptions>? Transport { get; set; }

    /// <summary>Durable-flow engine options (lease durations, step timeout, timer threshold).</summary>
    public Action<DurableFlowOptions>? DurableFlows { get; set; }

    /// <summary>
    /// Bound on how long harness idle-waits spin in <em>real</em> time before failing the test —
    /// the safety net that turns a hang into a diagnosable failure. Default: 10 seconds.
    /// </summary>
    public TimeSpan RealTimeGuard { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Flow-execution observers installed into every incarnation (the current one and each
    /// simulated restart). <see cref="FlowTestHarness"/> installs its probe here.
    /// </summary>
    public IList<IDurableFlowExecutionObserver> FlowObservers { get; } = [];
}

/// <summary>
/// Hosts the complete AsyncResponse engine in process for tests: the in-memory channel (with full
/// lost-subscriber recovery), the in-memory worker transport (with native delayed delivery), the
/// in-memory durable-flow store, and every background service — all running on a
/// <see cref="VirtualTimeProvider"/>. Production-sized timeouts, leases, timers, and cron
/// schedules elapse only when the test calls <see cref="AdvanceAsync"/>, so nothing in a test ever
/// sleeps for real.
/// <para>
/// <see cref="SimulateRestartAsync"/> models a redeploy: the service provider (and with it every
/// live waiter and in-flight execution context) is discarded and rebuilt, while the recovery
/// store, the flow ledgers, and scheduled (delayed) worker jobs survive — the durable state a
/// broker- and store-backed deployment would retain. Responses published after the restart route
/// through the real lost-subscriber recovery machinery. Waiter tasks obtained before the restart
/// never complete (their process is gone — do not await them across a restart).
/// </para>
/// <para>For flow-focused tests, <see cref="FlowTestHarness"/> wraps this with step-level tooling.</para>
/// </summary>
public sealed class AsyncResponseTestHarness : IAsyncDisposable
{
    private readonly AsyncResponseTestHarnessOptions _options;
    private readonly InMemoryRecoveryStateStore _recoveryStore;
    private readonly InMemoryFlowStateStore _flowStore;
    private readonly IDurableFlowExecutionObserver[] _observers;
    private readonly QuiesceProbe _quiesce = new();
    private ServiceProvider _provider = null!;
    private IHostedService[] _started = [];
    private bool _disposed;

    private AsyncResponseTestHarness(AsyncResponseTestHarnessOptions options)
    {
        _options = options;
        _observers = [.. options.FlowObservers];
        Clock = new VirtualTimeProvider(options.StartTime ?? VirtualTimeProvider.DefaultStartTime);
        _recoveryStore = new InMemoryRecoveryStateStore(Clock);
        _flowStore = new InMemoryFlowStateStore(Clock);
    }

    /// <summary>Builds the engine and starts its background services.</summary>
    public static async Task<AsyncResponseTestHarness> StartAsync(Action<AsyncResponseTestHarnessOptions>? configure = null)
    {
        var options = new AsyncResponseTestHarnessOptions();
        configure?.Invoke(options);

        var harness = new AsyncResponseTestHarness(options);
        harness.BuildProvider();
        try
        {
            await harness.StartHostedServicesAsync().ConfigureAwait(false);
        }
        catch
        {
            await harness.DisposeAsync().ConfigureAwait(false);
            throw;
        }

        return harness;
    }

    /// <summary>The virtual clock every engine component runs on.</summary>
    public VirtualTimeProvider Clock { get; }

    /// <summary>The current incarnation's service provider (rebuilt by <see cref="SimulateRestartAsync"/>).</summary>
    public IServiceProvider Services => _provider;

    /// <summary>The fluent waiter builder (recoverable: the in-memory channel supports lost-subscriber callbacks).</summary>
    public IRecoverableAsyncResponseBuilder Builder => _provider.GetRequiredService<IRecoverableAsyncResponseBuilder>();

    /// <summary>The response publisher — the test's stand-in for the remote systems that answer requests.</summary>
    public IAsyncResponsePublisher Publisher => _provider.GetRequiredService<IAsyncResponsePublisher>();

    /// <summary>Durable-flow starter/operations surface.</summary>
    public IDurableFlows Flows => _provider.GetRequiredService<IDurableFlows>();

    /// <summary>The flow executor, for driving runs directly instead of through the worker queue.</summary>
    public IDurableFlowExecutor FlowExecutor => _provider.GetRequiredService<IDurableFlowExecutor>();

    private InMemoryWorkerTransport Transport => _provider.GetRequiredService<InMemoryWorkerTransport>();

    /// <summary>Publishes a response payload to a correlation id (what the remote system would do).</summary>
    public Task PublishAsync<T>(T response, string correlationId) where T : IAsyncResponsePayload
        => Publisher.SetResponse(response, correlationId);

    /// <summary>Publishes an exception to a correlation id (a remote failure).</summary>
    public Task PublishExceptionAsync(Exception exception, string correlationId)
        => Publisher.SetException(exception, correlationId);

    /// <summary>
    /// Advances virtual time by <paramref name="delta"/>, stepping timer-by-timer and letting the
    /// worker pipeline settle between steps, so work a fired timer enqueues (a durable-timer
    /// wake-up that re-arms the next chunk, a retry backoff that re-executes) is honored within
    /// this same advance.
    /// </summary>
    public async Task AdvanceAsync(TimeSpan delta)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(delta, TimeSpan.Zero);
        var target = Clock.GetUtcNow() + delta;

        while (true)
        {
            await SettleAsync().ConfigureAwait(false);

            var next = Clock.NextTimerDueAt;
            if (next is null || next > target)
            {
                Clock.AdvanceTo(target);
                break;
            }

            Clock.AdvanceTo(next.Value);
        }

        await SettleAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Waits (bounded by <see cref="AsyncResponseTestHarnessOptions.RealTimeGuard"/> of real time)
    /// until the in-memory worker transport has no queued or executing jobs. Do not call while a
    /// flow is parked on an in-process virtual-time wait — that job stays outstanding until the
    /// clock advances; await the flow's outcome instead.
    /// </summary>
    public async Task WaitForWorkerIdleAsync()
    {
        var guard = TimeProvider.System.GetUtcNow() + _options.RealTimeGuard;
        while (Transport.OutstandingJobs != 0)
        {
            if (TimeProvider.System.GetUtcNow() > guard)
            {
                throw new TimeoutException(
                    $"The in-memory worker transport still has {Transport.OutstandingJobs} outstanding job(s) after {_options.RealTimeGuard} of real time. " +
                    "A job may be parked on virtual time (advance the clock) or genuinely stuck.");
            }

            await Task.Delay(TimeSpan.FromMilliseconds(1)).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Simulates a redeploy/restart. Durable state survives: recovery registrations, flow ledgers,
    /// and scheduled (delayed) worker jobs — re-published into the new incarnation with their
    /// remaining virtual delay, as a broker would retain them. Everything process-bound dies: live
    /// waiters, subscriptions, in-flight executions (their execution leases are broken, as a real
    /// crash's silence would let them expire, so the new incarnation takes their flows over
    /// immediately). Queued immediate jobs are drained gracefully before the old incarnation
    /// stops, bounded by the real-time guard.
    /// </summary>
    /// <param name="whileDown">
    /// Runs between the old incarnation stopping and the new one starting — with no engine
    /// running. Advance the clock here to simulate an outage: <c>Clock.Advance(TimeSpan.FromHours(4))</c>
    /// makes cron schedules skip the occurrences that fell into the downtime, exactly as a real
    /// outage would.
    /// </param>
    public async Task SimulateRestartAsync(Action? whileDown = null)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var pendingDelayed = Transport.SnapshotDelayedJobs();
        await StopHostedServicesAsync().ConfigureAwait(false);
        await _provider.DisposeAsync().ConfigureAwait(false);

        // Hard-crash semantics for whatever survived the graceful stop: a parked execution's
        // lease-renew loop runs on the SHARED virtual clock against the SHARED flow store, so it
        // would keep the lease alive forever and the new incarnation could never take the flow
        // over — the redelivered wake-up would see "executing on another live worker", ack, and
        // the flow would never resume. Breaking the leases is what a real crash's silence does
        // (the lease expires); the zombie's next renewal fails, marks itself lost, and stops. Any
        // zombie retry after that dies against its own disposed provider before touching the
        // store. The quiesce probe's parked entries died with the incarnation too.
        _flowStore.ExpireAllLeases();
        _quiesce.Reset();

        whileDown?.Invoke();

        BuildProvider();
        await StartHostedServicesAsync().ConfigureAwait(false);

        if (pendingDelayed.Count > 0)
        {
            var transport = (IDelayedWorkerTransport)Transport;
            var now = Clock.GetUtcNow().UtcDateTime;
            foreach (var job in pendingDelayed)
            {
                var remaining = job.NotBeforeUtc is { } notBefore && notBefore > now
                    ? notBefore - now
                    : TimeSpan.Zero;
                if (remaining > TimeSpan.Zero)
                    await transport.PublishAsync(job, remaining).ConfigureAwait(false);
                else
                    await ((IWorkerTransport)transport).PublishAsync(job).ConfigureAwait(false);
            }
        }
    }

    internal TimeSpan RealTimeGuard => _options.RealTimeGuard;

    private void BuildProvider()
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));

        // The engine clock, the shared durable state, and the harness observers go in FIRST so the
        // TryAdd registrations inside AddAsyncResponse()/With*() adopt them.
        services.AddSingleton<TimeProvider>(Clock);
        services.AddSingleton(_recoveryStore);
        services.AddSingleton(_flowStore);
        services.AddSingleton<IDurableFlowExecutionObserver>(_quiesce);
        foreach (var observer in _observers)
            services.AddSingleton<IDurableFlowExecutionObserver>(observer);

        _options.ConfigureServices?.Invoke(services);

        var builder = services.AddAsyncResponse()
            .WithInMemoryChannel(channel =>
            {
                _options.Channel?.Invoke(channel);
            })
            .WithInMemoryTransport(transport =>
            {
                _options.Transport?.Invoke(transport);
            })
            .WithInMemoryDurableFlows(flows =>
            {
                _options.DurableFlows?.Invoke(flows);
            });

        _options.ConfigureAsyncResponse?.Invoke(builder);

        _provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });

        // The probe needs the engine clock and this incarnation's timer threshold to tell an
        // in-process timer park (holds a worker slot) from a suspension (the job ends) — see
        // QuiesceProbe.OnStepWaitingAsync.
        _quiesce.Arm(Clock, _provider.GetRequiredService<DurableFlowOptions>().TimerInProcessThreshold);
    }

    private async Task StartHostedServicesAsync()
    {
        var hosted = _provider.GetServices<IHostedService>().ToArray();
        foreach (var service in hosted)
            await service.StartAsync(CancellationToken.None).ConfigureAwait(false);
        _started = hosted;
    }

    private async Task StopHostedServicesAsync()
    {
        // Bounded by the real-time guard: a graceful stop can legitimately hang when a drained job
        // is parked on virtual time; the restart then proceeds like a hard crash and the lease
        // machinery reconciles the leftover execution.
        using var cutoff = new CancellationTokenSource(_options.RealTimeGuard);
        foreach (var service in Enumerable.Reverse(_started))
        {
            try
            {
                await service.StopAsync(cutoff.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Hard-crash semantics for whatever did not stop in time.
            }
        }

        _started = [];
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        _disposed = true;
        await StopHostedServicesAsync().ConfigureAwait(false);
        await _provider.DisposeAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Give worker jobs that are still RUNNING code a real chance to reach their next stable
    /// state before the clock moves. Advancing while a job is mid-code would let a later
    /// <c>DelayAsync</c> or deadline be computed from the already-advanced clock and park beyond
    /// the advance target — the load-dependent flake this settle exists to prevent. Three
    /// signals, cheapest first:
    /// <list type="bullet">
    /// <item>Every outstanding job is parked on an event-visible engine wait (an awaited reply,
    /// an in-process flow timer) — the quiesce probe's count covers the outstanding count, and
    /// settling costs nothing.</item>
    /// <item>A virtual timer was armed while settling — the busy job just began a virtual-time
    /// wait (a retry backoff, a lease-acquisition poll, a timer chunk), so advancing the clock is
    /// exactly how it progresses.</item>
    /// <item>A bounded real-time grace for what cannot be attributed (a job blocked on a virtual
    /// timer it armed before this settle began looks identical to one stuck in user code). The
    /// budget is generous relative to scheduling noise; a fake that sleeps on the SYSTEM clock
    /// longer than this is the documented harness anti-pattern (use the injected TimeProvider).</item>
    /// </list>
    /// </summary>
    private async Task SettleAsync()
    {
        // Let just-released continuations (a fired timer's write, worker dispatch) reach the
        // transport counters before reading them.
        for (var round = 0; round < 3; round++)
        {
            await Task.Yield();
            await Task.Delay(TimeSpan.FromMilliseconds(1)).ConfigureAwait(false);
        }

        var budget = TimeProvider.System.GetUtcNow() + TimeSpan.FromMilliseconds(500);
        while (Transport.OutstandingJobs > _quiesce.ParkedCount
               && TimeProvider.System.GetUtcNow() < budget)
        {
            var next = Clock.NextTimerDueAt;
            await Task.Delay(TimeSpan.FromMilliseconds(1)).ConfigureAwait(false);
            if (next != Clock.NextTimerDueAt)
                return; // A virtual wait just began — the advance loop re-evaluates immediately.
        }
    }

    /// <summary>
    /// Tracks flow executions parked on an engine-owned wait: between a step's Waiting event and
    /// its Completed event the execution holds a worker slot but is idle by design, waiting on
    /// virtual time or a test-published reply. Only waits that actually park in process count —
    /// child-flow steps always suspend (the job ends), and a timer whose remainder exceeds the
    /// in-process threshold suspends too on this delayed-capable transport, so its Waiting is not
    /// counted (a stale entry would outlive the worker slot until the wake-up replay and mask a
    /// genuinely busy job in SettleAsync's guard). Cleared on restart: the old incarnation's
    /// parked executions die with it.
    /// </summary>
    private sealed class QuiesceProbe : IDurableFlowExecutionObserver
    {
        private readonly HashSet<(string FlowId, string Step)> _parked = [];
        private TimeProvider _clock = TimeProvider.System;
        private TimeSpan _timerInProcessThreshold;

        public int ParkedCount
        {
            get { lock (_parked) return _parked.Count; }
        }

        /// <summary>Binds the engine clock and timer threshold of the current incarnation.</summary>
        public void Arm(TimeProvider clock, TimeSpan timerInProcessThreshold)
        {
            _clock = clock;
            _timerInProcessThreshold = timerInProcessThreshold;
        }

        public void Reset()
        {
            lock (_parked) _parked.Clear();
        }

        public ValueTask OnStepWaitingAsync(DurableFlowStepEvent step)
        {
            // Awaited steps always park in process holding their worker slot. Timer steps only
            // park when the remainder is at or below the in-process threshold — a longer wait
            // suspends (the engine mirrors this decision in DelayCoreAsync against the same
            // clock), ending the worker job this entry would otherwise be offsetting.
            var parksInProcess = step.Kind switch
            {
                DurableFlowStepKind.Awaited => true,
                DurableFlowStepKind.Timer => step.WakeAtUtc is { } wakeAtUtc
                    && wakeAtUtc - _clock.GetUtcNow().UtcDateTime <= _timerInProcessThreshold,
                _ => false
            };

            if (parksInProcess)
                lock (_parked) _parked.Add((step.FlowId, step.StepName));
            return default;
        }

        public ValueTask OnStepCompletedAsync(DurableFlowStepEvent step)
        {
            lock (_parked) _parked.Remove((step.FlowId, step.StepName));
            return default;
        }

        public ValueTask OnRunFinishedAsync(DurableFlowRunEvent run)
        {
            lock (_parked) _parked.RemoveWhere(entry => string.Equals(entry.FlowId, run.FlowId, StringComparison.Ordinal));
            return default;
        }
    }
}
