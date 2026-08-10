using Microsoft.Extensions.Options;
using System.Diagnostics;
using System.Text;
using System.Text.Json;

namespace AsyncResponse.Transports.RabbitMQ;

/// <summary>
/// Publishes <see cref="WorkerJobEnvelope"/> messages to a RabbitMQ exchange.
/// </summary>
/// <remarks>
/// The publish channel is created lazily and re-created on demand: a transient broker outage when the
/// first job is published no longer permanently breaks the transport (a faulted connect attempt is not
/// cached). The channel is opened with publisher confirmations so <see cref="PublishAsync"/> only completes
/// once the broker has accepted the message. A single channel is shared across concurrent publishers;
/// RabbitMQ.Client v7 tracks each in-flight confirmation independently, so concurrent publishing is safe.
/// </remarks>
public sealed class RabbitMqWorkerTransport : IWorkerTransport, IAsyncDisposable
{
    private readonly RabbitMqAsyncResponseOptions _options;
    private readonly IRabbitMqConnectionFactory _connectionFactory;
    private readonly SemaphoreSlim _connectionGate = new(1, 1);
    private IRabbitMqConnection? _connection;
    private IRabbitMqChannel? _channel;
    private int _disposeGate;
    private bool _disposed;

    /// <summary>Runs the RabbitMqWorkerTransport operation.</summary>
    public RabbitMqWorkerTransport(IOptions<RabbitMqAsyncResponseOptions> options)
        : this(options, new RabbitMqConnectionFactoryAdapter(options.Value))
    {
    }

    internal RabbitMqWorkerTransport(
        IOptions<RabbitMqAsyncResponseOptions> options,
        IRabbitMqConnectionFactory connectionFactory)
    {
        _options = options.Value;
        ValidatePublishOptions(_options);
        _connectionFactory = connectionFactory;
    }

    private static void ValidatePublishOptions(RabbitMqAsyncResponseOptions options)
    {
        _ = RabbitMqOptionsValidator.Required(options.WorkerExchange, nameof(options.WorkerExchange));
        _ = RabbitMqOptionsValidator.Required(options.WorkerQueue, nameof(options.WorkerQueue));
        _ = RabbitMqOptionsValidator.Required(options.WorkerRoutingKey, nameof(options.WorkerRoutingKey));
        RabbitMqOptionsValidator.ValidateConnection(options);
        AsyncResponseChannelOptions.EnsureTimerBacked(options.ShutdownTimeout, nameof(RabbitMqAsyncResponseOptions), nameof(options.ShutdownTimeout));
    }

    private async Task<IRabbitMqChannel> GetChannelAsync(CancellationToken cancellationToken)
    {
        var channel = Volatile.Read(ref _channel);
        if (channel is not null)
            return channel;

        await _connectionGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (_channel is not null)
                return _channel;

            // ??= only assigns when the await succeeds, so a failed connect leaves _connection null and
            // the next publish retries. A successful connection is reused even if channel/topology setup fails.
            _connection ??= await _connectionFactory.CreateConnectionAsync(cancellationToken).ConfigureAwait(false);
            var created = await _connection.CreateChannelAsync(publisherConfirmations: true, cancellationToken).ConfigureAwait(false);
            await RabbitMqTopology.EnsureWorkerAsync(created, _options, cancellationToken).ConfigureAwait(false);

            // Publish the channel only once it is fully initialized; if anything above threw, _channel stays
            // null so a later publish recreates it instead of awaiting a permanently faulted task.
            _channel = created;
            return created;
        }
        finally
        {
            _connectionGate.Release();
        }
    }

    /// <summary>Publishes the supplied message.</summary>
    public async Task PublishAsync(WorkerJobEnvelope job, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(job);

        using var activity = AsyncResponseDiagnostics.StartActivity(
            "asyncresponse.worker.publish",
            ActivityKind.Producer,
            job.CorrelationId);
        activity?.SetTag("asyncresponse.transport", "rabbitmq");
        activity?.SetTag("messaging.system", "rabbitmq");
        activity?.SetTag("messaging.destination.name", _options.WorkerExchange);
        activity?.SetTag("messaging.rabbitmq.routing_key", _options.WorkerRoutingKey);
        AsyncResponseDiagnostics.SetReplyTarget(activity, job.ReplyTarget);
        AsyncResponseDiagnostics.SetWorker(activity, job.Call);

        try
        {
            var payload = Encoding.UTF8.GetBytes(AsyncResponseJson.Serialize(job));
            var properties = RabbitMqTopology.CreatePersistentJsonProperties(job.CorrelationId, _options.CorrelationIdHeader);
            var channel = await GetChannelAsync(cancellationToken).ConfigureAwait(false);
            await channel.BasicPublishAsync(
                _options.WorkerExchange,
                _options.WorkerRoutingKey,
                properties,
                payload,
                cancellationToken).ConfigureAwait(false);
            activity?.SetTag("messaging.message.id", properties.MessageId);
        }
        catch (Exception ex)
        {
            AsyncResponseDiagnostics.SetError(activity, ex);
            throw;
        }
    }

    /// <summary>Releases resources held by this instance.</summary>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposeGate, 1) != 0)
            return;

        await _connectionGate.WaitAsync().ConfigureAwait(false);
        try
        {
            _disposed = true;

            if (_channel is not null)
            {
                using var cts = new CancellationTokenSource(_options.ShutdownTimeout);
                try
                {
                    await _channel.CloseAsync(cts.Token).ConfigureAwait(false);
                }
                catch
                {
                    // Best effort: the channel may already be closed by broker-side shutdown.
                }

                await _channel.DisposeAsync().ConfigureAwait(false);
            }

            if (_connection is not null)
            {
                using var cts = new CancellationTokenSource(_options.ShutdownTimeout);
                try
                {
                    await _connection.CloseAsync(_options.ShutdownTimeout, cts.Token).ConfigureAwait(false);
                }
                catch
                {
                    // Best effort.
                }

                await _connection.DisposeAsync().ConfigureAwait(false);
            }
        }
        finally
        {
            _connectionGate.Release();
            _connectionGate.Dispose();
        }
    }
}
