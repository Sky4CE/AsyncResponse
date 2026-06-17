using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Threading.Channels;

namespace AsyncResponse;

/// <summary>
/// An in-memory <see cref="IWorkerTransport"/> backed by an unbounded
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
    private readonly Channel<QueuedJob> _queue = Channel.CreateUnbounded<QueuedJob>(
        new UnboundedChannelOptions { SingleReader = true });

    internal ChannelReader<QueuedJob> Reader => _queue.Reader;

    /// <inheritdoc/>
    public async Task PublishAsync(WorkerJobEnvelope job, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(job);
        await _queue.Writer.WriteAsync(new QueuedJob(job, ExecutionContext.Capture()), cancellationToken).ConfigureAwait(false);
    }

    /// <summary>A queued job paired with the ambient execution context captured when it was enqueued.</summary>
    internal readonly record struct QueuedJob(WorkerJobEnvelope Job, ExecutionContext? Context);
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
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            await foreach (var queued in _transport.Reader.ReadAllAsync(stoppingToken).ConfigureAwait(false))
            {
                try
                {
                    await RunAsync(queued).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "In-memory worker job {Target}.{Method} failed.", queued.Job.Call.ServiceInterfaceFullName, queued.Job.Call.MethodName);
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Host shutdown.
        }
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
