using Microsoft.Extensions.Options;
using System.Diagnostics;
using System.Text;
using System.Text.Json;

namespace AsyncResponse.Transports.RabbitMQ;

/// <summary>
/// Publishes <see cref="WorkerJobEnvelope"/> messages to a RabbitMQ exchange.
/// </summary>
public sealed class RabbitMqWorkerTransport : IWorkerTransport, IAsyncDisposable
{
    private readonly RabbitMqAsyncResponseOptions _options;
    private readonly Lazy<Task<IRabbitMqConnection>> _connection;
    private readonly Lazy<Task<IRabbitMqChannel>> _channel;

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
        _connection = new Lazy<Task<IRabbitMqConnection>>(() => connectionFactory.CreateConnectionAsync());
        _channel = new Lazy<Task<IRabbitMqChannel>>(CreateChannelAsync);
    }

    private static void ValidatePublishOptions(RabbitMqAsyncResponseOptions options)
    {
        _ = RabbitMqOptionsValidator.Required(options.WorkerExchange, nameof(options.WorkerExchange));
        _ = RabbitMqOptionsValidator.Required(options.WorkerQueue, nameof(options.WorkerQueue));
        _ = RabbitMqOptionsValidator.Required(options.WorkerRoutingKey, nameof(options.WorkerRoutingKey));
        RabbitMqOptionsValidator.Positive(options.ShutdownTimeout, nameof(options.ShutdownTimeout));
    }

    private async Task<IRabbitMqChannel> CreateChannelAsync()
    {
        var connection = await _connection.Value.ConfigureAwait(false);
        var channel = await connection.CreateChannelAsync().ConfigureAwait(false);
        await RabbitMqTopology.EnsureWorkerAsync(channel, _options).ConfigureAwait(false);
        return channel;
    }

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
            var payload = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(job));
            var properties = RabbitMqTopology.CreatePersistentJsonProperties(job.CorrelationId, _options.CorrelationIdHeader);
            var channel = await _channel.Value.WaitAsync(cancellationToken).ConfigureAwait(false);
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

    public async ValueTask DisposeAsync()
    {
        if (_channel.IsValueCreated)
        {
            var channel = await _channel.Value.ConfigureAwait(false);
            using var cts = new CancellationTokenSource(_options.ShutdownTimeout);
            await channel.CloseAsync(cts.Token).ConfigureAwait(false);
            await channel.DisposeAsync().ConfigureAwait(false);
        }

        if (_connection.IsValueCreated)
        {
            var connection = await _connection.Value.ConfigureAwait(false);
            using var cts = new CancellationTokenSource(_options.ShutdownTimeout);
            await connection.CloseAsync(_options.ShutdownTimeout, cts.Token).ConfigureAwait(false);
            await connection.DisposeAsync().ConfigureAwait(false);
        }
    }
}
