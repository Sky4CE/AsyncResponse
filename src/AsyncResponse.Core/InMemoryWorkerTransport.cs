using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Diagnostics;
using System.Threading.Channels;

namespace AsyncResponse;

/// <summary>
/// An in-memory <see cref="IWorkerTransport"/> backed by a bounded
/// <see cref="Channel{T}"/>, registered by <c>AddAsyncResponse().WithInMemoryTransport()</c>.
/// Jobs run in the current process and survive only as long as it does — use a broker-backed
/// transport for durability. Intended for development, tests, and single-node deployments.
/// <para>
/// Envelopes have broker wire parity: each publish serializes the job to its wire JSON and the
/// worker receives an instance materialized from it — <c>[JsonIgnore]</c> argument state is
/// excluded, post-publish mutations are invisible, and a non-serializable argument throws at
/// <see cref="PublishAsync(WorkerJobEnvelope, CancellationToken)"/> — so behavior observed here
/// carries over unchanged to every broker-backed transport.
/// </para>
/// <para>
/// Because the job stays in-process, the enqueuer's <see cref="ExecutionContext"/> is captured and
/// the job runs under it (see <see cref="InMemoryWorkerHost"/>), so ambient <see cref="AsyncLocal{T}"/>
/// state — trace id, principal, logging scope — flows automatically without any serializable
/// context propagator.
/// </para>
/// </summary>
public sealed class InMemoryWorkerTransport : IWorkerTransport, IDelayedWorkerTransport
{
    private readonly Channel<QueuedJob> _queue;
    private readonly TimeProvider _timeProvider;
    private readonly object _delayedGate = new();
    private readonly Dictionary<DelayedJob, ITimer> _delayedJobs = [];
    private int _outstanding;
    private volatile bool _draining;

    /// <summary>Creates a transport with default bounded-queue options.</summary>
    public InMemoryWorkerTransport()
        : this(Microsoft.Extensions.Options.Options.Create(new InMemoryWorkerTransportOptions()))
    {
    }

    /// <summary>Creates a transport with configured capacity and worker concurrency.</summary>
    public InMemoryWorkerTransport(IOptions<InMemoryWorkerTransportOptions> options, TimeProvider? timeProvider = null)
    {
        Options = options.Value;
        Options.Validate();
        _timeProvider = timeProvider ?? TimeProvider.System;
        _queue = Channel.CreateBounded<QueuedJob>(new BoundedChannelOptions(Options.QueueCapacity)
        {
            SingleReader = Options.WorkerCount == 1,
            SingleWriter = false,
            FullMode = BoundedChannelFullMode.Wait,
            AllowSynchronousContinuations = false
        });
    }

    internal ChannelReader<QueuedJob> Reader => _queue.Reader;
    internal InMemoryWorkerTransportOptions Options { get; }
    internal ILogger? DrainLogger { get; set; }

    /// <summary>Jobs accepted but not yet finished (queued + executing). Test-harness idle probe.</summary>
    internal int OutstandingJobs => Volatile.Read(ref _outstanding);

    /// <summary>The delayed jobs currently waiting on their due-time timers (test inspection).</summary>
    internal IReadOnlyList<WorkerJobEnvelope> SnapshotDelayedJobs()
    {
        lock (_delayedGate)
        {
            if (_delayedJobs.Count == 0)
                return [];

            var envelopes = new WorkerJobEnvelope[_delayedJobs.Count];
            var index = 0;
            foreach (var delayed in _delayedJobs.Keys)
                envelopes[index++] = delayed.Envelope;
            return envelopes;
        }
    }

    /// <summary>
    /// AsyncResponse.Testing only. From this call on, the shutdown drain RETAINS delayed jobs in
    /// the returned list instead of dropping them, and a delayed publish that arrives while the
    /// transport is draining (a flow suspending mid-drain) is retained instead of rejected —
    /// modeling the broker that keeps scheduled messages across a redeploy. A snapshot taken
    /// before the stop cannot do this: it misses both the drain-time publishes and any job armed
    /// between the snapshot and the drain. Read the list only after the stop has completed.
    /// </summary>
    internal List<WorkerJobEnvelope> BeginRetainingDelayedJobs()
    {
        lock (_delayedGate)
            return _drainRetention ??= [];
    }

    private List<WorkerJobEnvelope>? _drainRetention;

    /// <summary>
    /// Begins the shutdown drain. Called by <see cref="InMemoryWorkerHost"/> when the host starts
    /// stopping. The writer is deliberately NOT completed while anything is queued or running:
    /// accepted jobs were promised in-process execution, and a draining job may legitimately
    /// enqueue follow-up work (a durable-flow parent wake-up, a recovery re-enqueue) that must not
    /// hit a closed channel — losing it would strand the dependent flow with no redelivery to
    /// recover it. The last finishing job completes the writer instead, once the transport is idle.
    /// <para>
    /// Pending DELAYED jobs are different: their due time may be days away, and holding shutdown
    /// for them would hang the host. They are dropped with a warning — the in-memory transport is
    /// process-local by contract, so delayed jobs share the process's lifetime. A durable flow
    /// sleeping on such a wake-up must be resumed explicitly after restart (or use a broker
    /// transport, whose delayed messages survive). The test harness opts out of the drop via
    /// <see cref="BeginRetainingDelayedJobs"/> and re-publishes the retained jobs into the next
    /// incarnation.
    /// </para>
    /// </summary>
    internal void BeginShutdownDrain()
    {
        _draining = true;

        KeyValuePair<DelayedJob, ITimer>[] pending;
        List<WorkerJobEnvelope>? retention;
        lock (_delayedGate)
        {
            pending = [.. _delayedJobs];
            _delayedJobs.Clear();
            retention = _drainRetention;

            // Retention Adds run under the same lock as the delayed-publish Add: a flow
            // suspending mid-drain appends to this same List concurrently, and two
            // unsynchronized List<T>.Add calls can silently lose a wake-up or throw mid-grow.
            if (retention is not null)
            {
                foreach (var (job, _) in pending)
                    retention.Add(job.Envelope);
            }
        }

        foreach (var (job, timer) in pending)
        {
            timer.Dispose();
            if (retention is not null)
                continue;

            DrainLogger?.LogWarning(
                "Dropping delayed in-memory worker job {Target}.{Method} due at {NotBeforeUtc} at shutdown; in-memory delayed jobs do not survive the process. A durable flow waiting on this wake-up must be resumed explicitly after restart.",
                job.Envelope.Call.ServiceInterfaceFullName, job.Envelope.Call.MethodName, job.Envelope.NotBeforeUtc);
        }

        // Interlocked read pairs with the increment in PublishAsync: either this sees the
        // publisher's count (the finishing job completes the writer) or the publisher's write
        // lands before completion. Only a publish initiated after the transport is already idle
        // and draining can observe a completed channel.
        if (Interlocked.CompareExchange(ref _outstanding, 0, 0) == 0)
            _queue.Writer.TryComplete();
    }

    /// <summary>
    /// Called by the worker host after a dequeued job finished (successfully or not). A job counts
    /// as outstanding from publish until here, so follow-up publishes made while it runs always
    /// find the writer open during the drain.
    /// </summary>
    internal void OnJobFinished()
    {
        if (Interlocked.Decrement(ref _outstanding) == 0 && _draining)
            _queue.Writer.TryComplete();
    }

    /// <inheritdoc/>
    public async Task PublishAsync(WorkerJobEnvelope job, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(job);
        job = MaterializeFromWire(job);

        using var activity = AsyncResponseDiagnostics.StartActivity(
            "asyncresponse.worker.publish",
            ActivityKind.Producer,
            job.CorrelationId);
        activity?.SetTag("asyncresponse.transport", "inmemory");
        AsyncResponseDiagnostics.SetReplyTarget(activity, job.ReplyTarget);
        AsyncResponseDiagnostics.SetWorker(activity, job.Call);

        Interlocked.Increment(ref _outstanding);
        try
        {
            await _queue.Writer.WriteAsync(new QueuedJob(job, ExecutionContext.Capture()), cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            // OnJobFinished, not a bare decrement: if this failed publish is the last thing the
            // drain was waiting on (the drain observed the incremented count and declined to
            // complete the writer), a bare decrement leaves an empty, never-completed channel —
            // the workers park in ReadAllAsync forever and shutdown stalls to the host's budget.
            OnJobFinished();
            AsyncResponseDiagnostics.SetError(activity, ex);
            throw;
        }
    }

    // -----------------------------------------------------------------------------------------
    // IDelayedWorkerTransport

    /// <inheritdoc/>
    /// <remarks>The in-process timer wheel has no per-hop cap; delays are bounded only by the BCL timer ceiling.</remarks>
    public TimeSpan MaxPublishDelay => TimeSpan.FromMilliseconds(uint.MaxValue - 1);

    /// <inheritdoc/>
    public Task PublishAsync(WorkerJobEnvelope job, TimeSpan delay, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(job);
        if (delay <= TimeSpan.Zero)
            return PublishAsync(job, cancellationToken);
        if (delay > MaxPublishDelay)
            throw new ArgumentOutOfRangeException(nameof(delay), delay, $"Delay must be at most {MaxPublishDelay.TotalDays:0.#} days (the .NET timer ceiling).");

        cancellationToken.ThrowIfCancellationRequested();
        job = MaterializeFromWire(job);

        using var activity = AsyncResponseDiagnostics.StartActivity(
            "asyncresponse.worker.publish",
            ActivityKind.Producer,
            job.CorrelationId);
        activity?.SetTag("asyncresponse.transport", "inmemory");
        activity?.SetTag("asyncresponse.worker.delay_seconds", delay.TotalSeconds);
        AsyncResponseDiagnostics.SetReplyTarget(activity, job.ReplyTarget);
        AsyncResponseDiagnostics.SetWorker(activity, job.Call);

        var delayed = new DelayedJob(this, new QueuedJob(job, ExecutionContext.Capture()));
        lock (_delayedGate)
        {
            if (_draining)
            {
                // Harness restart: a flow suspending mid-drain parks its wake-up with "the
                // broker" instead of faulting the draining job (and stalling the stop on the
                // redelivery backoff).
                if (_drainRetention is { } retained)
                {
                    retained.Add(job);
                    return Task.CompletedTask;
                }

                // Same contract as the shutdown drain below: delayed in-memory jobs share the
                // process lifetime, and a publish racing shutdown is dropped loudly, not queued
                // onto a channel that will complete underneath it.
                DrainLogger?.LogWarning(
                    "Rejecting delayed in-memory worker job {Target}.{Method} published during shutdown; in-memory delayed jobs do not survive the process.",
                    job.Call.ServiceInterfaceFullName, job.Call.MethodName);
                throw new InvalidOperationException("The in-memory worker transport is shutting down and no longer accepts delayed jobs.");
            }

            // The timer is created inside the gate so a concurrent drain either sees it in the map
            // (and disposes it) or the publish observed _draining above. One-shot; Fire removes it.
            var timer = _timeProvider.CreateTimer(static state => ((DelayedJob)state!).Fire(), delayed, delay, Timeout.InfiniteTimeSpan);
            _delayedJobs.Add(delayed, timer);
        }

        return Task.CompletedTask;
    }

    private void FireDelayed(DelayedJob delayed)
    {
        ITimer? timer;
        lock (_delayedGate)
        {
            if (!_delayedJobs.Remove(delayed, out timer))
                return; // The shutdown drain already claimed (and dropped) it.

            // Count as outstanding INSIDE the gate, atomically with the removal: incremented
            // after the lock released, a drain snapshotting in that window saw neither the
            // timer-map entry nor the count — it neither retained nor waited for this job and
            // completed the writer underneath the write below.
            Interlocked.Increment(ref _outstanding);
        }

        timer.Dispose();
        _ = WriteFiredAsync(delayed.Queued);
    }

    private async Task WriteFiredAsync(QueuedJob queued)
    {
        try
        {
            await _queue.Writer.WriteAsync(queued).ConfigureAwait(false);
        }
        catch (ChannelClosedException)
        {
            // OnJobFinished, not a bare decrement, on both failure paths: if this count is the
            // last one a drain is waiting on, only the drain-aware decrement completes the writer
            // (TryComplete on an already-completed channel is a no-op here).
            OnJobFinished();
            DrainLogger?.LogWarning(
                "Dropping delayed in-memory worker job {Target}.{Method}: its due time fired after the transport completed its shutdown drain.",
                queued.Job.Call.ServiceInterfaceFullName, queued.Job.Call.MethodName);
        }
        catch (Exception ex)
        {
            OnJobFinished();
            DrainLogger?.LogError(ex,
                "Failed to enqueue fired delayed in-memory worker job {Target}.{Method}.",
                queued.Job.Call.ServiceInterfaceFullName, queued.Job.Call.MethodName);
        }
    }

    // Wire parity for EVERY job, in-process included: the envelope the worker receives is
    // re-materialized from the publisher's wire JSON — the same representation a broker delivery
    // carries, [JsonIgnore] argument state excluded, post-publish mutations invisible, and a
    // non-serializable argument failing HERE, at the publish, exactly where every broker transport
    // fails it. Handing the caller's live envelope through (the old path) let tests and single-node
    // deployments run on state no broker-backed transport can deliver. The publish serializes with
    // the transports' wire options and re-binds with the broker ingress's case-insensitive options.
    // The enqueuer's captured ExecutionContext still flows: ambient AsyncLocal state is this
    // transport's documented in-process feature, not envelope state.
    private static WorkerJobEnvelope MaterializeFromWire(WorkerJobEnvelope job)
        => AsyncResponseJson.DeserializeCaseInsensitive<WorkerJobEnvelope>(AsyncResponseJson.SerializeToUtf8Bytes(job))!;

    /// <summary>Identity handle for one scheduled delayed job (reference equality keys the timer map).</summary>
    private sealed class DelayedJob(InMemoryWorkerTransport owner, QueuedJob queued)
    {
        public QueuedJob Queued { get; } = queued;
        public WorkerJobEnvelope Envelope => Queued.Job;

        public void Fire() => owner.FireDelayed(this);
    }

    /// <summary>A queued job paired with the ambient execution context captured when it was enqueued.</summary>
    internal readonly record struct QueuedJob(WorkerJobEnvelope Job, ExecutionContext? Context);
}

/// <summary>Capacity and concurrency options for the process-local worker transport.</summary>
public sealed class InMemoryWorkerTransportOptions
{
    /// <summary>Maximum queued jobs before publishers asynchronously wait. Default: 1024.</summary>
    public int QueueCapacity { get; set; } = 1024;

    /// <summary>Number of jobs that may execute concurrently. Default: 1.</summary>
    public int WorkerCount { get; set; } = 1;

    /// <summary>
    /// Maximum number of delivery attempts before a failing job is dropped, with an error log and
    /// a <c>dropped</c> outcome on the worker-jobs counter. The process-local queue has no broker
    /// to redeliver, so retries run in-process with backoff and occupy the worker slot while they
    /// run — the same head-of-line trade the Kafka transport documents. This is what honors the
    /// durable-flow redelivery contract on this transport: a transiently failing wake-up (a lease
    /// held a beat too long, a revision conflict's designed "abandon and let the delivery retry")
    /// gets its retry instead of silently stranding the flow. <c>0</c> means unlimited retries —
    /// the job retries until it succeeds or the process exits, which also means a permanently
    /// failing job holds its worker slot (and the shutdown drain) indefinitely. Default: <c>5</c>.
    /// </summary>
    public int MaxDeliveryAttempts { get; set; } = 5;

    /// <summary>Initial delay between in-process retry attempts (doubles per attempt). Default: <c>100ms</c>.</summary>
    public TimeSpan RetryBaseDelay { get; set; } = TimeSpan.FromMilliseconds(100);

    /// <summary>Maximum delay between in-process retry attempts. Default: <c>5s</c>.</summary>
    public TimeSpan RetryMaxDelay { get; set; } = TimeSpan.FromSeconds(5);

    internal void Validate()
    {
        if (QueueCapacity <= 0)
            throw new InvalidOperationException($"{nameof(QueueCapacity)} must be positive.");
        if (WorkerCount <= 0)
            throw new InvalidOperationException($"{nameof(WorkerCount)} must be positive.");
        if (MaxDeliveryAttempts < 0)
            throw new InvalidOperationException($"{nameof(MaxDeliveryAttempts)} must be zero (unlimited) or positive.");
        if (RetryBaseDelay <= TimeSpan.Zero)
            throw new InvalidOperationException($"{nameof(RetryBaseDelay)} must be positive.");
        if (RetryMaxDelay < RetryBaseDelay)
            throw new InvalidOperationException($"{nameof(RetryMaxDelay)} must be at least {nameof(RetryBaseDelay)}.");
        // Bounded by the BCL timer ceiling: a value Task.Delay rejects would fail at the FIRST
        // retry, get swallowed by the worker loop's backstop, and drop the job without its
        // configured attempts or the terminal `dropped` outcome — the exact silent loss the
        // redelivery loop exists to prevent.
        if (RetryMaxDelay > AsyncResponseChannelOptions.MaxTimerBackedTimeout)
            throw new InvalidOperationException(
                $"{nameof(RetryMaxDelay)} must be at most {AsyncResponseChannelOptions.MaxTimerBackedTimeout.TotalDays:0.#} days (the .NET timer ceiling).");
    }
}

/// <summary>
/// Background consumer for <see cref="InMemoryWorkerTransport"/>: drains the queue and executes
/// each job via <see cref="WorkerJobExecutor"/>, under the enqueuer's captured
/// <see cref="ExecutionContext"/> so ambient context flows in-process. Failures are logged and
/// never break the loop.
/// <para>
/// Deliberately a plain <see cref="IHostedService"/>, not a <see cref="BackgroundService"/>:
/// since Microsoft.Extensions.Hosting.Abstractions 10.0.10, <c>BackgroundService.StartAsync</c>
/// queues <c>ExecuteAsync</c> to the thread pool and DISCARDS the queued work when the stopping
/// token fires before the pool runs it. The shutdown-drain hook is installed inside the
/// execution loop, so a fast start→stop under thread-pool pressure never installed it — pending
/// delayed jobs were neither dropped loudly nor retained (the test harness's simulated restart
/// lost retained wake-ups exactly this way), and accepted queued jobs sat unread forever. The
/// worker loops and the drain hook are part of this host's STARTED contract, so they come up
/// synchronously inside <see cref="StartAsync"/>, before it returns.
/// </para>
/// </summary>
internal sealed class InMemoryWorkerHost(
    InMemoryWorkerTransport _transport,
    WorkerJobExecutor _executor,
    ILogger<InMemoryWorkerHost> _logger,
    TimeProvider? _timeProvider = null) : IHostedService, IDisposable
{
    private CancellationTokenSource? _stopping;
    private Task? _execution;

    /// <summary>Starts the worker loops and installs the shutdown-drain hook, synchronously.</summary>
    public Task StartAsync(CancellationToken cancellationToken)
    {
        _stopping = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _execution = RunAsync(_stopping.Token);
        return _execution.IsCompleted ? _execution : Task.CompletedTask;
    }

    /// <summary>
    /// Signals the drain (synchronously, via the stop registration) and waits for the workers to
    /// finish what was accepted, bounded by <paramref name="cancellationToken"/> — the same
    /// contract <c>BackgroundService.StopAsync</c> has.
    /// </summary>
    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_execution is null)
            return;

        try
        {
            _stopping!.Cancel();
        }
        finally
        {
            var cutoff = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            await using var registration = cancellationToken.Register(
                static state => ((TaskCompletionSource)state!).TrySetResult(), cutoff);
            await Task.WhenAny(_execution, cutoff.Task).ConfigureAwait(false);
        }
    }

    /// <summary>Parity with <c>BackgroundService.Dispose</c>: cancel, never dispose the source — a
    /// still-draining worker may hold its token.</summary>
    public void Dispose() => _stopping?.Cancel();

    private async Task RunAsync(CancellationToken stoppingToken)
    {
        _transport.DrainLogger = _logger;

        // Shutdown quiesces instead of cancelling the readers: accepted jobs were promised
        // in-process execution, so the workers drain the queue — including follow-up work those
        // jobs enqueue while draining — and the writer completes only once the transport is idle.
        // The drain is bounded because the queue is bounded and each job's follow-ups are finite.
        using var stopRegistration = stoppingToken.Register(static state =>
            ((InMemoryWorkerTransport)state!).BeginShutdownDrain(), _transport);

        try
        {
            var workers = new Task[_transport.Options.WorkerCount];
            for (var index = 0; index < workers.Length; index++)
                workers[index] = RunWorkerAsync(stoppingToken);
            await Task.WhenAll(workers).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Host shutdown.
        }
    }

    private async Task RunWorkerAsync(CancellationToken stoppingToken)
    {
        // Deliberately no cancellation token on the read: the loop ends when the completed queue
        // is empty, never by abandoning accepted jobs mid-queue.
        await foreach (var queued in _transport.Reader.ReadAllAsync().ConfigureAwait(false))
        {
            if (stoppingToken.IsCancellationRequested && _logger.IsEnabled(LogLevel.Debug))
                _logger.LogDebug("Draining in-memory worker job {Target}.{Method} during shutdown.", queued.Job.Call.ServiceInterfaceFullName, queued.Job.Call.MethodName);

            try
            {
                await ExecuteWithRedeliveryAsync(queued).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                // Backstop only — the redelivery loop already contains job failures. Nothing may
                // break this loop: it is the transport's delivery guarantee for everything still
                // queued behind the current job.
                _logger.LogError(ex, "In-memory worker job {Target}.{Method} failed.", queued.Job.Call.ServiceInterfaceFullName, queued.Job.Call.MethodName);
            }
            finally
            {
                _transport.OnJobFinished();
            }
        }
    }

    /// <summary>
    /// The transport's stand-in for broker redelivery: a failing job is retried in place with
    /// exponential backoff up to <see cref="InMemoryWorkerTransportOptions.MaxDeliveryAttempts"/>
    /// (0 = unlimited) — durable-flow wake-ups ride this queue and rely on redelivery for crash
    /// and contention recovery, so dropping a job on its first failure (the old behavior) could
    /// strand a flow that a broker-backed transport would have recovered. Retries deliberately
    /// run during the shutdown drain too: accepted jobs were promised in-process execution, and
    /// the retry budget is bounded when attempts are.
    /// </summary>
    private async Task ExecuteWithRedeliveryAsync(InMemoryWorkerTransport.QueuedJob queued)
    {
        var options = _transport.Options;
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                await RunAsync(queued).ConfigureAwait(false);
                return;
            }
            catch (Exception ex)
            {
                if (options.MaxDeliveryAttempts > 0 && attempt >= options.MaxDeliveryAttempts)
                {
                    // No broker, no dead-letter queue: dropping is the terminal outcome, so it is
                    // loud — an error log plus a distinct `dropped` outcome on the worker-jobs
                    // counter (broker transports dead-letter here instead).
                    _logger.LogError(ex,
                        "In-memory worker job {Target}.{Method} failed after {Attempts} attempts; dropping it. A durable flow waiting on this job must be recovered or resumed explicitly.",
                        queued.Job.Call.ServiceInterfaceFullName, queued.Job.Call.MethodName, attempt);
                    AsyncResponseDiagnostics.RecordWorkerOutcome("dropped");
                    return;
                }

                var delay = RetryDelay(options, attempt);
                _logger.LogWarning(ex,
                    "In-memory worker job {Target}.{Method} failed on attempt {Attempt}; retrying in {Delay}.",
                    queued.Job.Call.ServiceInterfaceFullName, queued.Job.Call.MethodName, attempt, delay);
                await Task.Delay(delay, _timeProvider ?? TimeProvider.System).ConfigureAwait(false);
            }
        }
    }

    private static TimeSpan RetryDelay(InMemoryWorkerTransportOptions options, int attempt)
    {
        // Exponential backoff from the base delay, saturating at the max. Computed in ticks with
        // a pre-shift comparison so neither pathological attempt counts (unlimited retries) nor a
        // pathological base delay can overflow before the saturation check.
        var exponent = Math.Min(attempt - 1, 20);
        var baseTicks = options.RetryBaseDelay.Ticks;
        var maxTicks = options.RetryMaxDelay.Ticks;
        return baseTicks > maxTicks >> exponent
            ? options.RetryMaxDelay
            : TimeSpan.FromTicks(baseTicks << exponent);
    }

    private Task RunAsync(InMemoryWorkerTransport.QueuedJob queued)
    {
        // No captured context (flow suppressed): execute directly.
        if (queued.Context is null)
            return _executor.ExecuteAsync(queued.Job);

        // Run under the enqueue-time ExecutionContext so the job inherits its ambient AsyncLocals.
        Task? task = null;
        ExecutionContext.Run(queued.Context, _ => task = _executor.ExecuteAsync(queued.Job), null);
        return task!;
    }

}
