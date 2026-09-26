using System.Runtime.ExceptionServices;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
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
    /// What <see cref="AsyncResponseTestHarness.SimulateRestartAsync"/> does when user code is
    /// still executing after the old incarnation's graceful stop lapsed
    /// (<see cref="RealTimeGuard"/>): a step body that ignored its cancellation and is blocked on
    /// something the test controls, for example. A simulated restart is <b>cooperative</b> — it
    /// discards the process-bound state a crash would lose, but it cannot terminate a running
    /// delegate the way a process kill does — so such an execution would keep running beside the
    /// new incarnation and perform its side effects after the "restart" returned, proving less
    /// than the test claims. Default (<c>false</c>): the restart fails with
    /// <see cref="InvalidOperationException"/> naming the count. <c>true</c>: the executions are
    /// abandoned (their leases broken, their provider disposed) and the restart proceeds; the
    /// test then owns the overlap. Engine-owned waits — an awaited step or an in-process timer
    /// holding its worker slot, a crashed attempt asleep in the redelivery backoff (one taken
    /// before the stop is dropped by it, one taken during the drain waits on the clock), a
    /// delivery polling for another execution's lease, all on the virtual clock — and jobs still
    /// queued behind them are not user code and never trip this.
    /// </summary>
    public bool AbandonLingeringExecutionsOnRestart { get; set; }

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
            // The start failure is what the test needs to see: a teardown that fails as well (a
            // started service whose StopAsync throws) must not replace it.
            try
            {
                await harness.DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception)
            {
                // Deliberately swallowed in favour of the start failure rethrown below.
            }

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

    /// <summary>
    /// The incarnation's flow executor, for the deliveries it has asleep in its lease-contention
    /// poll (engine-owned waits); <c>null</c> when a test replaced the executor, or when it cannot
    /// be resolved (a provider already disposed, options a failed start rejected) — a diagnostic
    /// read must never replace the failure the caller is reporting or cleaning up after.
    /// </summary>
    private DurableFlowExecutor? FlowExecutorInternal
    {
        get
        {
            try
            {
                return _provider.GetService<IDurableFlowExecutor>() as DurableFlowExecutor;
            }
            catch (Exception ex) when (ex is ObjectDisposedException or InvalidOperationException or ArgumentException)
            {
                return null;
            }
        }
    }

    /// <summary>
    /// Publishes a response payload to a correlation id (what the remote system would do). A
    /// publish that waits on a virtual timer it armed itself — a lost-subscriber callback's retry
    /// backoff — drives the clock to that timer while it is pending; any other pending publish is
    /// bounded by <see cref="AsyncResponseTestHarnessOptions.RealTimeGuard"/>.
    /// </summary>
    public Task PublishAsync<T>(T response, string correlationId) where T : IAsyncResponsePayload
        => DrivePublishAsync(() => Publisher.SetResponse(response, correlationId), correlationId);

    /// <summary>Publishes an exception to a correlation id (a remote failure); driven like <see cref="PublishAsync{T}"/>.</summary>
    public Task PublishExceptionAsync(Exception exception, string correlationId)
        => DrivePublishAsync(() => Publisher.SetException(exception, correlationId), correlationId);

    /// <summary>
    /// Runs a publish and, while it is pending, advances the clock to the timers it armed itself.
    /// The in-memory channel runs a lost subscriber's callbacks INSIDE the publish, and a callback
    /// that throws is retried with backoff on the engine clock: nothing moved that clock while the
    /// test awaited the publish, so a throwing failure callback hung the test with no bound and no
    /// diagnosis (production finishes the same ladder in under two seconds). Only the publish's own
    /// timers are driven — timers armed on its async flow, never those of work it enqueues — and
    /// only ones due within <see cref="PublishDriveHorizon"/>, so a publish that is merely slow (a
    /// callback doing real work, a flow run it started) never fires unrelated waiter timeouts,
    /// leases, or flow timers; it is bounded by the real-time guard instead, like every other
    /// harness wait.
    /// </summary>
    private async Task DrivePublishAsync(Func<Task> publish, string correlationId)
    {
        var owner = new object();
        var operation = VirtualTimeProvider.StartAttributed(owner, publish);
        if (operation.IsCompleted)
        {
            await operation.ConfigureAwait(false);
            return;
        }

        var guard = TimeProvider.System.GetUtcNow() + _options.RealTimeGuard;
        while (!operation.IsCompleted)
        {
            if (TimeProvider.System.GetUtcNow() > guard)
            {
                throw new TimeoutException(
                    $"The publish to correlation id '{correlationId}' was still running after {_options.RealTimeGuard} of real time. " +
                    "The in-memory channel runs a lost subscriber's recovery callbacks inside the publish: check for a callback blocked on " +
                    "something the test controls, one waiting on a virtual timer further away than a retry backoff (advance the clock " +
                    "yourself), or one waiting on the SYSTEM clock instead of the injected TimeProvider.");
            }

            if (Clock.NextTimerDueAtFor(owner) is { } due && due - Clock.GetUtcNow() <= PublishDriveHorizon)
            {
                // A backoff the publish armed on the virtual clock: move time to it, timer by
                // timer, exactly as AdvanceAsync would — and not a step further once the publish
                // has finished. A later wait it armed is no backoff (a waiter timeout a callback
                // armed, say): jumping to it fired every unrelated timer in between.
                await AdvanceToAsync(due, operation).ConfigureAwait(false);
                continue;
            }

            await Task.WhenAny(operation, Task.Delay(TimeSpan.FromMilliseconds(1))).ConfigureAwait(false);
        }

        await operation.ConfigureAwait(false);
    }

    /// <summary>
    /// How far ahead a pending publish drives the clock to a timer it armed: more than twice the
    /// longest step of every in-process retry ladder a publish runs (2 s — the failure-callback,
    /// ingress and flow-start ladders).
    /// </summary>
    private static readonly TimeSpan PublishDriveHorizon = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Advances virtual time by <paramref name="delta"/>, stepping timer-by-timer and letting the
    /// worker pipeline settle between steps, so work a fired timer enqueues (a durable-timer
    /// wake-up that re-arms the next chunk, a retry backoff that re-executes) is honored within
    /// this same advance.
    /// </summary>
    public Task AdvanceAsync(TimeSpan delta)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(delta, TimeSpan.Zero);
        return AdvanceToAsync(Clock.GetUtcNow() + delta);
    }

    /// <summary>
    /// <see cref="AdvanceAsync"/> to an absolute instant (never backwards: a target the clock
    /// already passed is complete — also when a second driver, a pending publish, passed it
    /// meanwhile). Stops early, before moving the clock again, once <paramref name="until"/> has
    /// completed.
    /// </summary>
    private async Task AdvanceToAsync(DateTimeOffset target, Task? until = null)
    {
        while (true)
        {
            await SettleAsync().ConfigureAwait(false);
            if (until is { IsCompleted: true })
                return;

            var next = Clock.NextTimerDueAt;
            if (next is null || next > target)
            {
                Clock.AdvanceToAtLeast(target);
                break;
            }

            Clock.AdvanceToAtLeast(next.Value);
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
    /// crash's silence would let them expire, so nothing blocks the new incarnation from taking
    /// their flows over). Queued immediate jobs are drained gracefully before the old incarnation
    /// stops, bounded by the real-time guard.
    /// <para>
    /// Jobs still queued behind a park the stop could not wait for never started, so they are
    /// carried over too and run in the new incarnation. The wake-up of an execution the stop
    /// abandoned — one parked on an awaited step or an in-process timer, or one whose crashed
    /// attempt was asleep in the transport's redelivery backoff (the restart's stop ends that sleep
    /// without the retry a production host stop would still run) — dies with the old incarnation,
    /// and the new one does not redeliver it: resume those flows explicitly
    /// (<see cref="IDurableFlows.ResumeAsync"/>, <c>FlowRunHandle.ResumeAsync</c>) after the
    /// restart, or, for an awaited step, publish its response, which lost-subscriber recovery
    /// routes into the run.
    /// </para>
    /// </summary>
    /// <param name="whileDown">
    /// Runs between the old incarnation stopping and the new one starting — with no engine
    /// running. Advance the clock here to simulate an outage: <c>Clock.Advance(TimeSpan.FromHours(4))</c>
    /// makes cron schedules skip the occurrences that fell into the downtime, exactly as a real
    /// outage would.
    /// </param>
    /// <exception cref="InvalidOperationException">
    /// User code of the old incarnation was still executing after the graceful stop lapsed and
    /// <see cref="AsyncResponseTestHarnessOptions.AbandonLingeringExecutionsOnRestart"/> is off:
    /// the restart is cooperative and cannot kill that code, so it refuses to report a restart
    /// the surviving execution would contradict.
    /// </exception>
    public async Task SimulateRestartAsync(Action? whileDown = null)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        // Retention, not a snapshot: the drain moves every pending delayed job into the transport's
        // retention list and also captures delayed publishes made BY draining jobs (a flow
        // suspending mid-drain), so nothing falls between a pre-stop snapshot and the drain — a
        // broker would keep all of it.
        var dyingTransport = Transport;
        dyingTransport.BeginRetainingDelayedJobs();
        // Crash semantics for a crashed attempt asleep in its retry backoff: the stop ends that
        // sleep and the job dies with this incarnation (resume the flow after the restart),
        // instead of retrying during the drain — as a production host stop lets a job with
        // attempts left do — and running the flow's next steps here, inside the restart.
        dyingTransport.DropInterruptedBackoffs();
        // Resolved BEFORE the provider goes away; abandoned after, once nothing can add to it.
        var dyingChannel = _provider.GetService<InMemoryAsyncResponseChannel>();
        var dyingExecutor = FlowExecutorInternal;
        await StopHostedServicesAsync().ConfigureAwait(false);

        // No worker of the dying incarnation starts another job from here on (a job a freed worker
        // reads is held back for the take below): closed BEFORE the quiescence check, so every
        // execution that could still run user code here is one the check counts.
        dyingTransport.StopStartingJobs();

        // Quiescence check BEFORE the provider is discarded. Executions still running after the
        // bounded stop are ones the stop could not end. Engine-owned waits (an awaited step or an
        // in-process timer holding its worker slot, a crashed attempt asleep in the redelivery
        // backoff, a delivery polling for another execution's lease — all on the virtual clock)
        // are expected: their leases are broken below, as after a real crash. Anything beyond them
        // is USER code still running: this restart cannot terminate it (there is no process to
        // kill), so reporting a restart while it keeps executing — and performs side effects after
        // the restart "completed" — would prove less than the test claims. Refuse unless the test
        // opted into owning that overlap. A job still QUEUED behind a park never started, so it
        // runs no user code at all.
        var lingering = UserCodeRunning(dyingTransport, dyingExecutor);
        if (lingering > 0 && !_options.AbandonLingeringExecutionsOnRestart)
        {
            throw new InvalidOperationException(
                $"{nameof(SimulateRestartAsync)} could not establish quiescence: {lingering} execution(s) of the old incarnation " +
                $"were still running user code after the graceful stop lapsed ({_options.RealTimeGuard} of real time). A simulated " +
                "restart is cooperative — it cannot terminate a running delegate the way a process kill does — so that code would " +
                "keep running beside the new incarnation and perform its side effects after the restart. Let the step observe its " +
                "cancellation token or finish before restarting, inject a crash at the checkpoint boundary with " +
                "FlowTestHarness.CrashBeforeStep/CrashAfterStep, or set " +
                $"{nameof(AsyncResponseTestHarnessOptions)}.{nameof(AsyncResponseTestHarnessOptions.AbandonLingeringExecutionsOnRestart)} " +
                "to accept the overlap.");
        }

        // Jobs still queued behind a park the stop was cut short on never started, so they are
        // carried over like messages a broker still holds — a queued flow start has no ledger yet,
        // and dropped here nothing could ever recover it. Starts are closed, so no worker of this
        // incarnation can take one of them first — not even one freed below, when disposing the
        // provider or abandoning the waiters ends a park: each job runs exactly once, in the new
        // incarnation.
        var unstarted = dyingTransport.TakeUnstartedJobs();

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

        // Same hard-crash semantics for the dead incarnation's response waiters. Their timeout
        // timers were armed on the SHARED virtual clock, so without this they stayed live past the
        // "restart": advancing time fired them, completed a pre-restart ResponseTask that a crash
        // would leave hanging forever, and ran the cleanup that DELETES the registration from the
        // SHARED recovery store — so the late response this scenario exists to test found no
        // waiter AND no registration, and was silently dropped. Abandoning leaves the registration
        // exactly as a crash does: alive until its TTL, recoverable by the new incarnation.
        if (dyingChannel is not null)
            await dyingChannel.AbandonAllAsync().ConfigureAwait(false);

        // Taken under the transport's retention gate, now that the stop is over: an execution the
        // restart abandoned can still append to the live list.
        var pendingDelayed = dyingTransport.TakeRetainedDelayedJobs();

        // Whatever the dying incarnation queued since the first take — a follow-up published by
        // an execution the dispose or the abandonment above just ended — never started either.
        // This final take reopens starts: a zombie that publishes after it runs its job here and
        // fails loudly against the disposed provider, rather than leaving it unseen in a dead queue.
        var heldBack = dyingTransport.TakeUnstartedJobs(reopen: true);

        // The step barriers see only what the new incarnation does from here on.
        foreach (var observer in _observers)
            (observer as FlowProbe)?.BeginIncarnation();

        whileDown?.Invoke();

        BuildProvider();
        await StartHostedServicesAsync().ConfigureAwait(false);

        // Immediate jobs are readmitted without waiting for queue room: the new workers may all
        // park on waits only the test can end, and a publish waiting for room would then hang the
        // restart with no guard.
        var transport = Transport;
        foreach (var queued in unstarted)
            transport.Readmit(queued);
        foreach (var queued in heldBack)
            transport.Readmit(queued);

        // Each retained job keeps the ExecutionContext its publisher captured, like the unstarted
        // jobs above: re-scheduled from a bare envelope, a carried-over scheduled job ran under
        // the ambient state of the test that called the restart instead of its publisher's.
        var now = Clock.GetUtcNow().UtcDateTime;
        foreach (var retained in pendingDelayed)
        {
            var remaining = retained.Job.NotBeforeUtc is { } notBefore && WorkerJobExecutor.AsUtc(notBefore) > now
                ? WorkerJobExecutor.AsUtc(notBefore) - now
                : TimeSpan.Zero;
            if (remaining > TimeSpan.Zero)
            {
                // Per-hop clamp, as every production publisher applies: NotBeforeUtc rides the
                // envelope, so the executor re-delays the remainder on delivery. Unclamped, a
                // legal 60-day sleep would throw here and silently lose the rest of the list.
                // Scheduled without waiting for a delayed slot: the drain frees every slot
                // before it retains, so the retained set can exceed DelayedJobCapacity, and a
                // slot only frees when a timer fires — on a clock that cannot move while the
                // test is in here.
                var hop = remaining <= transport.MaxPublishDelay ? remaining : transport.MaxPublishDelay;
                transport.ScheduleRetained(retained, hop);
            }
            else
                transport.Readmit(retained);
        }
    }

    internal TimeSpan RealTimeGuard => _options.RealTimeGuard;

    /// <summary>
    /// Settles that spent their whole real-time grace with a job still counted busy (test
    /// inspection): a wait the settle should recognise as one on the virtual clock shows up here
    /// deterministically, where a wall-clock bound on the advance would be a flake.
    /// </summary>
    internal int SettleBudgetLapses => Volatile.Read(ref _settleBudgetLapses);

    private int _settleBudgetLapses;

    private void BuildProvider()
    {
        var services = new ServiceCollection();

        // The engine clock, the shared durable state, and the harness observers go in FIRST so the
        // TryAdd registrations inside AddAsyncResponse()/With*() adopt them.
        services.AddSingleton<TimeProvider>(Clock);
        services.AddSingleton(_recoveryStore);
        services.AddSingleton(_flowStore);

        // Per incarnation, the harness's own probes are registered through a view stamped with
        // this incarnation: an execution the restart abandoned keeps its executor (and so the view
        // it resolved), and its late events — a zombie's failed attempt — must not rewrite what
        // the new incarnation recorded for the same flow.
        services.AddSingleton(_quiesce.ForCurrentGeneration());
        foreach (var observer in _observers)
            services.AddSingleton(observer is FlowProbe probe ? probe.ForCurrentIncarnation() : observer);

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

        // Both checks run after EVERY user hook — ConfigureAsyncResponse included, whose builder
        // exposes the same service collection: run before it, a clock or a logging registration
        // made there slipped past them.
        //
        // The engine resolves the LAST TimeProvider registration, so a clock registered by the
        // test would silently displace the virtual one: no engine timer ever arms, AdvanceAsync
        // advances a clock nothing reads, and every wait dies as an unexplained RealTimeGuard
        // timeout. Fail construction instead, naming the fix.
        var lastClock = services.LastOrDefault(d => !d.IsKeyedService && d.ServiceType == typeof(TimeProvider));
        if (lastClock is null || !ReferenceEquals(lastClock.ImplementationInstance, Clock))
        {
            throw new InvalidOperationException(
                $"{nameof(AsyncResponseTestHarness)} drives the whole engine on its own virtual clock; a TimeProvider " +
                "registered via ConfigureServices or ConfigureAsyncResponse would displace it and no timer, timeout, lease, " +
                "or backoff would ever elapse. Use harness.Clock / AdvanceAsync instead of registering your own TimeProvider.");
        }

        // Fallback only, and only AFTER the user's registrations: AddLogging registers ILogger<>
        // with TryAdd semantics, so a non-Try registration made before them would silently pin
        // NullLogger and swallow the very diagnostics the harness's failure messages tell users
        // to check.
        services.TryAddSingleton(typeof(ILogger<>), typeof(NullLogger<>));

        _provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });

        // The probe needs the engine clock and this incarnation's timer settings to tell an
        // in-process timer park (holds a worker slot) from a suspension (the job ends), and to
        // know when a park's hop ends — see QuiesceProbe.OnStepWaitingAsync.
        var flowOptions = _provider.GetRequiredService<DurableFlowOptions>();
        _quiesce.Arm(Clock, flowOptions.TimerInProcessThreshold, flowOptions.MaxInProcessParkDuration);

        // The flow probe makes the same in-process/suspended distinction: only an in-process
        // park dies with its incarnation, so only its Waiting event is hidden from a barrier
        // after a restart.
        foreach (var observer in _observers)
            (observer as FlowProbe)?.Arm(Clock, flowOptions.TimerInProcessThreshold);
    }

    private async Task StartHostedServicesAsync()
    {
        var hosted = _provider.GetServices<IHostedService>().ToArray();
        var started = new List<IHostedService>(hosted.Length);
        try
        {
            foreach (var service in hosted)
            {
                await service.StartAsync(CancellationToken.None).ConfigureAwait(false);
                started.Add(service);
            }
        }
        finally
        {
            // Every service that did start, also when a later one throws: recorded only after the
            // loop, a failed start left the earlier services running with nothing to stop them.
            _started = [.. started];
        }
    }

    private async Task StopHostedServicesAsync()
    {
        // Nothing started, nothing to stop or watch — also after a failed rebuild, whose provider
        // is the disposed one of the incarnation before it.
        if (_started.Length == 0)
            return;

        // Bounded by the real-time guard: a graceful stop can legitimately hang when a drained job
        // is parked on virtual time; the restart then proceeds like a hard crash and the lease
        // machinery reconciles the leftover execution.
        using var cutoff = new CancellationTokenSource(_options.RealTimeGuard);

        // An engine-owned park — an awaited step, an in-process timer on the virtual clock — ends
        // only when the test replies or moves the clock, and neither can happen while the test is
        // awaiting this stop. Waiting it out therefore always burned the WHOLE guard (10 s by
        // default), once per restart and again per disposal, in a kit whose point is that nothing
        // sleeps for real. The moment a park is all that is left, end the wait: that is where the
        // lapsed guard was heading anyway — the leases are broken right after and the new
        // incarnation takes the execution over, exactly as after a real crash.
        using var stopWatching = new CancellationTokenSource();
        var watcher = AbandonOnceOnlyParkedAsync(cutoff, stopWatching.Token);
        List<Exception>? failures = null;
        try
        {
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
                catch (Exception ex)
                {
                    // Every service still gets its stop: one whose StopAsync throws must not leave
                    // the services after it running. Reported once all of them have stopped.
                    (failures ??= []).Add(ex);
                }
            }
        }
        finally
        {
            await stopWatching.CancelAsync().ConfigureAwait(false);
            await watcher.ConfigureAwait(false);
        }

        _started = [];
        if (failures is [var single])
            ExceptionDispatchInfo.Throw(single);
        if (failures is not null)
            throw new AggregateException("More than one hosted service failed to stop.", failures);
    }

    /// <summary>
    /// Cancels <paramref name="cutoff"/> as soon as every execution still running is waiting on
    /// the virtual clock or a reply — an engine-owned park, a crashed attempt asleep in a
    /// redelivery backoff the drain took, or a delivery polling for another execution's lease —
    /// and no queued job can still get a worker. Deliberately does nothing while NOTHING is
    /// waiting that way: a stop that can still make progress is left to finish cleanly, and the
    /// real-time guard stays the backstop for user code that is genuinely stuck. A backoff taken
    /// BEFORE the stop is no such wait: it is bound to the worker host's stopping token, so the
    /// host's own stop ends it — a restart's stop then drops its job (crash semantics), a
    /// disposal's retries a bounded one during the drain (and a failure there IS such a wait) —
    /// this check runs before any hosted service has stopped, and counting it cut that stop short
    /// at once.
    /// </summary>
    private async Task AbandonOnceOnlyParkedAsync(CancellationTokenSource cutoff, CancellationToken stopWatching)
    {
        var transport = Transport;
        var executor = FlowExecutorInternal;
        try
        {
            while (!stopWatching.IsCancellationRequested)
            {
                // Queued jobs never started and run no user code, but while a worker is free they
                // still make progress on their own; once every worker is held they wait on the
                // same clock (or reply) as whatever holds it.
                var executing = transport.ExecutingJobs;
                var queuedCanRun = transport.OutstandingJobs > executing && executing < transport.Options.WorkerCount;
                var waiting = _quiesce.ParkedCount + transport.DrainBackingOffJobs + (executor?.LeaseContentionWaits ?? 0);
                if (waiting > 0 && !queuedCanRun && UserCodeRunning(transport, executor) <= 0)
                {
                    await cutoff.CancelAsync().ConfigureAwait(false);
                    return;
                }

                // The SYSTEM clock: the harness's virtual one is not moving while a test awaits
                // this stop, which is the whole reason the park cannot end by itself.
                await Task.Delay(ParkedStopPollInterval, TimeProvider.System, stopWatching).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
            // The stop finished on its own.
        }
    }

    /// <summary>How often the stop checks whether a park is all that is left.</summary>
    private static readonly TimeSpan ParkedStopPollInterval = TimeSpan.FromMilliseconds(5);

    /// <summary>
    /// Executions of <paramref name="transport"/>'s incarnation still running user code: jobs a
    /// worker has taken (queued ones never started) plus inline direct runs, minus the ones
    /// waiting on the virtual clock or a reply — engine-owned parks, crashed attempts asleep in a
    /// redelivery backoff the drain took, and deliveries asleep in <paramref name="executor"/>'s
    /// lease-contention poll (which only a moving clock or the holder's release ends: counted as
    /// user code, one duplicate wake-up racing a restart burned the whole guard and then refused
    /// it). A backoff taken before the stop counts as running: the worker host's stop ends it and
    /// the job then retries or leaves (see <see cref="AbandonOnceOnlyParkedAsync"/>).
    /// </summary>
    private int UserCodeRunning(InMemoryWorkerTransport transport, DurableFlowExecutor? executor)
        => transport.ExecutingJobs - transport.DrainBackingOffJobs - (executor?.LeaseContentionWaits ?? 0)
            + _quiesce.DirectRunsInFlight - _quiesce.ParkedCount;

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        _disposed = true;
        try
        {
            await StopHostedServicesAsync().ConfigureAwait(false);
        }
        finally
        {
            // Also when a service failed to stop: the rest of the incarnation still goes away.
            await _provider.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Reports an inline executor attempt starting (see QuiesceProbe.DirectRunsInFlight); pass the
    /// returned token to <see cref="OnDirectRunFinished"/>.
    /// </summary>
    internal int OnDirectRunStarted() => _quiesce.OnDirectRunStarted();

    /// <summary>Reports an inline executor attempt finished.</summary>
    internal void OnDirectRunFinished(int token) => _quiesce.OnDirectRunFinished(token);

    /// <summary>
    /// Give worker jobs that are still RUNNING code a real chance to reach their next stable
    /// state before the clock moves. Advancing while a job is mid-code would let a later
    /// <c>DelayAsync</c> or deadline be computed from the already-advanced clock and park beyond
    /// the advance target — the load-dependent flake this settle exists to prevent. Three
    /// signals, cheapest first:
    /// <list type="bullet">
    /// <item>Every outstanding job is parked on an event-visible engine wait (an awaited reply,
    /// an in-process flow timer), asleep in a retry backoff, or polling for another execution's
    /// lease — all counted only once their virtual timer exists, so advancing the clock is exactly
    /// how they progress — and settling costs nothing.</item>
    /// <item>A virtual timer was armed while settling — the busy job just began a virtual-time
    /// wait (a retry backoff, a lease-acquisition poll, a timer chunk), so advancing the clock is
    /// exactly how it progresses.</item>
    /// <item>A bounded real-time grace for what cannot be attributed (a job blocked on some other
    /// virtual timer it armed before this settle began looks identical to one stuck in user code).
    /// The budget is generous relative to scheduling noise; a fake that sleeps on the SYSTEM clock
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

        // A job asleep in a retry backoff, or a delivery polling for another execution's lease,
        // is waiting on the virtual clock as surely as a park: counted as busy, every settle over
        // one spun its whole real-time budget (a crash-injected advance cost half a second each).
        var transport = Transport;
        var executor = FlowExecutorInternal;
        var budget = TimeProvider.System.GetUtcNow() + TimeSpan.FromMilliseconds(500);
        while (transport.OutstandingJobs - transport.BackingOffJobs - (executor?.LeaseContentionWaits ?? 0) + _quiesce.DirectRunsInFlight > _quiesce.ParkedCount)
        {
            if (TimeProvider.System.GetUtcNow() >= budget)
            {
                // The grace ran out with a job still counted busy (see SettleBudgetLapses).
                Interlocked.Increment(ref _settleBudgetLapses);
                return;
            }

            // A new earliest due time counts only when a timer was actually ARMED: disposing the
            // earliest timer (a finished wait, a cancelled delay) moves NextTimerDueAt too, and
            // ended the settle while the job that disposed it was still running code — the clock
            // then advanced under it. (Any arm alone is not enough either: a job that just took a
            // worker arms its lease-renew timer long before it reaches a wait of its own.)
            var armed = Clock.ArmSequence;
            var next = Clock.NextTimerDueAt;
            await Task.Delay(TimeSpan.FromMilliseconds(1)).ConfigureAwait(false);
            if (armed != Clock.ArmSequence && next != Clock.NextTimerDueAt)
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
    /// <para>
    /// One probe spans every incarnation, but each incarnation observes through its own view
    /// (<see cref="ForCurrentGeneration"/>) stamped with the generation it was built in, and a
    /// restart bumps the generation: an execution the restart abandoned keeps the view its
    /// executor resolved, so its late events — a zombie's failed attempt erasing every entry of
    /// its flow — are ignored instead of wiping the new incarnation's park of the same flow.
    /// </para>
    /// </summary>
    private sealed class QuiesceProbe
    {
        /// <summary>
        /// Parked steps, each with the instant its in-process wait ENDS: <c>null</c> for an awaited
        /// step (it ends on a reply), the hop's end for a timer.
        /// </summary>
        private readonly Dictionary<(string FlowId, string Step), DateTime?> _parked = [];

        /// <summary>The clock at each timer step's latest Starting event — the instant the engine decides at.</summary>
        private readonly Dictionary<(string FlowId, string Step), DateTime> _timerStarted = [];

        private readonly object _gate = new();
        private TimeProvider _clock = TimeProvider.System;
        private TimeSpan _timerInProcessThreshold;
        private TimeSpan? _maxInProcessParkDuration;
        private int _directRunsInFlight;
        private int _generation;

        /// <summary>
        /// Steps parked right now. A timer entry stops counting once the clock reaches its hop's
        /// end: the wait has fired and the job is running code again — completing the step, or
        /// handing the timer over to a fresh delivery, which ends the job without any observer
        /// event (a suspension notifies nothing), so the entry cannot be removed there.
        /// </summary>
        public int ParkedCount
        {
            get
            {
                var now = _clock.GetUtcNow().UtcDateTime;
                lock (_gate)
                    return _parked.Values.Count(waitEnd => waitEnd is not { } end || end > now);
            }
        }

        /// <summary>
        /// Executor attempts running inline (<c>FlowRunHandle.ExecuteDirectAsync</c>), which hold
        /// no worker slot: SettleAsync counts them beside <c>Transport.OutstandingJobs</c>, since
        /// a step they park would otherwise make ParkedCount exceed the outstanding jobs and let
        /// the clock advance under a direct run still executing user code.
        /// </summary>
        public int DirectRunsInFlight
        {
            get { lock (_gate) return _directRunsInFlight; }
        }

        /// <summary>Counts a direct run in; returns the generation its finish must match.</summary>
        public int OnDirectRunStarted()
        {
            lock (_gate)
            {
                _directRunsInFlight++;
                return _generation;
            }
        }

        /// <summary>
        /// Counts a direct run out — unless a restart reset the count since it started: a direct
        /// run the restart abandoned (a parked one, which does not stop a restart) ends only after
        /// the reset, and decrementing then drove the count to -1, so every later check
        /// under-counted one busy direct run until the next restart.
        /// </summary>
        public void OnDirectRunFinished(int generation)
        {
            lock (_gate)
            {
                if (generation == _generation)
                    _directRunsInFlight--;
            }
        }

        /// <summary>Binds the engine clock and the in-process timer settings of the current incarnation.</summary>
        public void Arm(TimeProvider clock, TimeSpan timerInProcessThreshold, TimeSpan? maxInProcessParkDuration)
        {
            _clock = clock;
            _timerInProcessThreshold = timerInProcessThreshold;
            _maxInProcessParkDuration = maxInProcessParkDuration;
        }

        public void Reset()
        {
            lock (_gate)
            {
                _parked.Clear();
                _timerStarted.Clear();
                _generation++;
                _directRunsInFlight = 0;
            }
        }

        /// <summary>The observer the incarnation being built registers: this probe, stamped with the current generation.</summary>
        public IDurableFlowExecutionObserver ForCurrentGeneration()
        {
            lock (_gate)
                return new GenerationView(this, _generation);
        }

        private void OnStepStarting(DurableFlowStepEvent step, int generation)
        {
            if (step.Kind != DurableFlowStepKind.Timer)
                return;

            var now = _clock.GetUtcNow().UtcDateTime;
            lock (_gate)
            {
                if (generation == _generation)
                    _timerStarted[(step.FlowId, step.StepName)] = now;
            }
        }

        private void OnStepWaiting(DurableFlowStepEvent step, int generation)
        {
            // Awaited steps always park in process holding their worker slot. Timer steps only
            // park when the remainder is at or below the in-process threshold — a longer wait
            // suspends (the engine mirrors this decision in DelayCoreAsync against the same
            // clock), ending the worker job this entry would otherwise be offsetting — and then
            // only for one hop: at most MaxInProcessParkDuration (the harness transport advertises
            // no in-flight ceiling, so that option is the whole in-process budget), after which
            // the engine hands the rest over to a fresh delivery.
            var now = _clock.GetUtcNow().UtcDateTime;
            lock (_gate)
            {
                if (generation != _generation)
                    return;

                var key = (step.FlowId, step.StepName);
                switch (step.Kind)
                {
                    case DurableFlowStepKind.Awaited:
                        _parked[key] = null;
                        break;

                    case DurableFlowStepKind.Timer when step.WakeAtUtc is { } wakeAtUtc:
                        // The remainder the engine decided on: measured right after the step's
                        // Starting event, before a first pass saves its breadcrumb and notifies
                        // Waiting. Measured here instead, a clock advanced during that save made a
                        // suspended timer look in process — a phantom park until its due time,
                        // which let the settle advance the clock under another busy job (the
                        // flow probe's round-43 fix, FlowProbe.ParksInProcess).
                        var decidedAt = _timerStarted.TryGetValue(key, out var started) ? started : now;
                        var remaining = wakeAtUtc - decidedAt;
                        if (remaining > _timerInProcessThreshold)
                            break;

                        // The engine's wait, from the remainder it decided on, starts no earlier
                        // than this event.
                        var wait = _maxInProcessParkDuration is { } budget && budget < remaining ? budget : remaining;
                        _parked[key] = now + wait;
                        break;
                }
            }
        }

        private void OnStepCompleted(DurableFlowStepEvent step, int generation)
        {
            lock (_gate)
            {
                if (generation != _generation)
                    return;

                _parked.Remove((step.FlowId, step.StepName));
                _timerStarted.Remove((step.FlowId, step.StepName));
            }
        }

        /// <summary>
        /// A run finished, or an attempt failed: the failed attempt released its worker slot with
        /// its waits unresolved (a waiter timeout, a published failure the flow does not treat as
        /// terminal), and the redelivery re-parks whatever still applies. Left in place, the stale
        /// entry offset a genuinely busy job in SettleAsync's guard for the rest of the incarnation.
        /// </summary>
        private void RemoveRun(string flowId, int generation)
        {
            lock (_gate)
            {
                if (generation != _generation)
                    return;

                foreach (var entry in _parked.Keys.Where(entry => string.Equals(entry.FlowId, flowId, StringComparison.Ordinal)).ToArray())
                    _parked.Remove(entry);
                foreach (var entry in _timerStarted.Keys.Where(entry => string.Equals(entry.FlowId, flowId, StringComparison.Ordinal)).ToArray())
                    _timerStarted.Remove(entry);
            }
        }

        /// <summary>One incarnation's view of the probe (see <see cref="ForCurrentGeneration"/>).</summary>
        private sealed class GenerationView(QuiesceProbe probe, int generation) : IDurableFlowExecutionObserver
        {
            public ValueTask OnStepStartingAsync(DurableFlowStepEvent step)
            {
                probe.OnStepStarting(step, generation);
                return default;
            }

            public ValueTask OnStepWaitingAsync(DurableFlowStepEvent step)
            {
                probe.OnStepWaiting(step, generation);
                return default;
            }

            public ValueTask OnStepCompletedAsync(DurableFlowStepEvent step)
            {
                probe.OnStepCompleted(step, generation);
                return default;
            }

            public ValueTask OnRunFinishedAsync(DurableFlowRunEvent run)
            {
                probe.RemoveRun(run.FlowId, generation);
                return default;
            }

            public ValueTask OnRunAttemptFailedAsync(DurableFlowRunEvent run)
            {
                probe.RemoveRun(run.FlowId, generation);
                return default;
            }
        }
    }
}
