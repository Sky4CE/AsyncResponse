using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Threading.Channels;

namespace AsyncResponse;

/// <summary>
/// An in-memory <see cref="IWorkerTransport"/> backed by an unbounded
/// <see cref="Channel{T}"/>, registered by <c>AddAsyncResponse().WithInMemoryTransport()</c>.
/// Jobs run in the current process and survive only as long as it does — use a broker-backed
/// transport for durability. Intended for development, tests, and single-node deployments.
/// </summary>
public sealed class InMemoryWorkerTransport : IWorkerTransport
{
    private readonly Channel<WorkerJobEnvelope> _queue = Channel.CreateUnbounded<WorkerJobEnvelope>(
        new UnboundedChannelOptions { SingleReader = true });

    internal ChannelReader<WorkerJobEnvelope> Reader => _queue.Reader;

    /// <inheritdoc/>
    public async Task PublishAsync(WorkerJobEnvelope job, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(job);
        await _queue.Writer.WriteAsync(job, cancellationToken).ConfigureAwait(false);
    }
}

/// <summary>
/// Background consumer for <see cref="InMemoryWorkerTransport"/>: drains the queue and executes
/// each job via <see cref="WorkerJobExecutor"/>. Failures are logged and never break the loop.
/// </summary>
internal sealed class InMemoryWorkerHost(
    InMemoryWorkerTransport _transport,
    WorkerJobExecutor _executor,
    ILogger<InMemoryWorkerHost> _logger) : BackgroundService
{
    private const string SERVICE_NAME = nameof(InMemoryWorkerHost);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            await foreach (var job in _transport.Reader.ReadAllAsync(stoppingToken).ConfigureAwait(false))
            {
                try
                {
                    await _executor.ExecuteAsync(job).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "{ServiceName}: worker job {Target}.{Method} failed.",
                        SERVICE_NAME, job.Call.ServiceInterfaceFullName, job.Call.MethodName);
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Host shutdown.
        }
    }
}
