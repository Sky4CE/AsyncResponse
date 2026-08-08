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
/// Because the job stays in-process, the enqueuer's <see cref="ExecutionContext"/> is captured and
/// the job runs under it (see <see cref="InMemoryWorkerHost"/>), so ambient <see cref="AsyncLocal{T}"/>
/// state — trace id, principal, logging scope — flows automatically without any serializable
/// context propagator.
/// </para>
/// </summary>
public sealed class InMemoryWorkerTransport : IWorkerTransport
{
    private readonly Channel<QueuedJob> _queue;
    private int _outstanding;
    private volatile bool _draining;

    /// <summary>Creates a transport with default bounded-queue options.</summary>
    public InMemoryWorkerTransport()
        : this(Microsoft.Extensions.Options.Options.Create(new InMemoryWorkerTransportOptions()))
    {
    }

    /// <summary>Creates a transport with configured capacity and worker concurrency.</summary>
    public InMemoryWorkerTransport(IOptions<InMemoryWorkerTransportOptions> options)
    {
        Options = options.Value;
        Options.Validate();
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

    /// <summary>
    /// Begins the shutdown drain. Called by <see cref="InMemoryWorkerHost"/> when the host starts
    /// stopping. The writer is deliberately NOT completed while anything is queued or running:
    /// accepted jobs were promised in-process execution, and a draining job may legitimately
    /// enqueue follow-up work (a durable-flow parent wake-up, a recovery re-enqueue) that must not
    /// hit a closed channel — losing it would strand the dependent flow with no redelivery to
    /// recover it. The last finishing job completes the writer instead, once the transport is idle.
    /// </summary>
    internal void BeginShutdownDrain()
    {
        _draining = true;
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
            Interlocked.Decrement(ref _outstanding);
            AsyncResponseDiagnostics.SetError(activity, ex);
            throw;
        }
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
    }
}

/// <summary>
/// Background consumer for <see cref="InMemoryWorkerTransport"/>: drains the queue and executes
/// each job via <see cref="WorkerJobExecutor"/>, under the enqueuer's captured
/// <see cref="ExecutionContext"/> so ambient context flows in-process. Failures are logged and
/// never break the loop.
/// </summary>
internal sealed class InMemoryWorkerHost(
    InMemoryWorkerTransport _transport,
    WorkerJobExecutor _executor,
    ILogger<InMemoryWorkerHost> _logger) : BackgroundService
{
    /// <summary>Runs this background operation until cancellation is requested.</summary>
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
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
                await Task.Delay(delay).ConfigureAwait(false);
            }
        }
    }

    private static TimeSpan RetryDelay(InMemoryWorkerTransportOptions options, int attempt)
    {
        // Exponential backoff from the base delay, saturating at the max. The shift is capped so
        // pathological attempt counts (unlimited retries) cannot overflow.
        var exponent = Math.Min(attempt - 1, 20);
        var scaled = options.RetryBaseDelay * (1L << exponent);
        return scaled < options.RetryMaxDelay ? scaled : options.RetryMaxDelay;
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
