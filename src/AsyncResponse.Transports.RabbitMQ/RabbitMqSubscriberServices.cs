using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Text;

namespace AsyncResponse.Transports.RabbitMQ;

internal abstract class RabbitMqSubscriberService : BackgroundService
{
    private readonly IRabbitMqConnectionFactory _connectionFactory;

    protected RabbitMqSubscriberService(
        IOptions<RabbitMqAsyncResponseOptions> options,
        ILogger logger)
        : this(options, logger, new RabbitMqConnectionFactoryAdapter(options.Value))
    {
    }

    protected RabbitMqSubscriberService(
        IOptions<RabbitMqAsyncResponseOptions> options,
        ILogger logger,
        IRabbitMqConnectionFactory connectionFactory)
    {
        Options = options.Value;
        Logger = logger;
        _connectionFactory = connectionFactory;
    }

    protected RabbitMqAsyncResponseOptions Options { get; }
    protected ILogger Logger { get; }

    protected abstract string QueueName { get; }
    protected abstract RabbitMqSubscriberOptions SubscriberOptions { get; }
    protected abstract RabbitMqSubscriberRole SubscriberRole { get; }
    protected abstract Task EnsureTopologyAsync(IRabbitMqChannel channel, CancellationToken cancellationToken);
    protected abstract Task HandleMessageAsync(RabbitMqDelivery delivery, CancellationToken cancellationToken);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var queue = QueueName;
        RabbitMqMessageDispatcher.ValidateOptions(Options, SubscriberOptions, SubscriberRole);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await RunSubscriberAsync(queue, stoppingToken).ConfigureAwait(false);
                return;
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex) when (!stoppingToken.IsCancellationRequested)
            {
                var retryDelay = Options.NetworkRecoveryInterval > TimeSpan.Zero
                    ? Options.NetworkRecoveryInterval
                    : TimeSpan.FromSeconds(5);
                Logger.LogWarning(
                    ex,
                    "RabbitMQ subscriber could not start for queue {Queue} ({Role}); retrying in {RetryDelay}.",
                    queue,
                    SubscriberRole,
                    retryDelay);
                await Task.Delay(retryDelay, stoppingToken).ConfigureAwait(false);
            }
        }
    }

    private async Task RunSubscriberAsync(string queue, CancellationToken stoppingToken)
    {
        await using var connection = await _connectionFactory.CreateConnectionAsync(stoppingToken).ConfigureAwait(false);
        await using var channel = await connection.CreateChannelAsync(cancellationToken: stoppingToken).ConfigureAwait(false);
        await EnsureTopologyAsync(channel, stoppingToken).ConfigureAwait(false);
        await channel.BasicQosAsync(SubscriberOptions.PrefetchCount, stoppingToken).ConfigureAwait(false);

        await using var dispatcher = RabbitMqMessageDispatcher.Create(
            HandleMessageAsync,
            Options,
            SubscriberOptions,
            Logger,
            queue,
            SubscriberRole);

        Logger.LogInformation(
            "RabbitMQ subscriber started. Queue: {Queue}. Role: {Role}. AckMode: {AckMode}.",
            queue,
            SubscriberRole,
            SubscriberOptions.AckMode);

        var consumerTag = await channel.BasicConsumeAsync(
            queue,
            delivery => dispatcher.HandleAsync(delivery, channel, stoppingToken),
            stoppingToken).ConfigureAwait(false);

        try
        {
            await Task.Delay(Timeout.InfiniteTimeSpan, stoppingToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            using var shutdown = new CancellationTokenSource(Options.ShutdownTimeout);
            await channel.BasicCancelAsync(consumerTag, shutdown.Token).ConfigureAwait(false);
            await channel.CloseAsync(shutdown.Token).ConfigureAwait(false);
            await connection.CloseAsync(Options.ShutdownTimeout, shutdown.Token).ConfigureAwait(false);
        }
    }
}

internal sealed class RabbitMqWorkerSubscriber : RabbitMqSubscriberService
{
    private readonly IAsyncResponseIngress _ingress;

    public RabbitMqWorkerSubscriber(
        IOptions<RabbitMqAsyncResponseOptions> options,
        IAsyncResponseIngress ingress,
        ILogger<RabbitMqWorkerSubscriber> logger)
        : base(options, logger)
    {
        _ingress = ingress;
    }

    internal RabbitMqWorkerSubscriber(
        IOptions<RabbitMqAsyncResponseOptions> options,
        IAsyncResponseIngress ingress,
        ILogger<RabbitMqWorkerSubscriber> logger,
        IRabbitMqConnectionFactory connectionFactory)
        : base(options, logger, connectionFactory)
    {
        _ingress = ingress;
    }

    protected override string QueueName
        => RabbitMqOptionsValidator.Required(Options.WorkerQueue, nameof(Options.WorkerQueue));

    protected override RabbitMqSubscriberOptions SubscriberOptions => Options.WorkerSubscriber;
    protected override RabbitMqSubscriberRole SubscriberRole => RabbitMqSubscriberRole.Worker;

    protected override Task EnsureTopologyAsync(IRabbitMqChannel channel, CancellationToken cancellationToken)
        => RabbitMqTopology.EnsureWorkerAsync(channel, Options, cancellationToken);

    protected override Task HandleMessageAsync(RabbitMqDelivery delivery, CancellationToken cancellationToken)
        => _ingress.HandleWorkerMessageAsync(Encoding.UTF8.GetString(delivery.Body.Span));
}

internal sealed class RabbitMqResponseIngressSubscriber : RabbitMqSubscriberService
{
    private readonly IAsyncResponseIngress _ingress;

    public RabbitMqResponseIngressSubscriber(
        IOptions<RabbitMqAsyncResponseOptions> options,
        IAsyncResponseIngress ingress,
        ILogger<RabbitMqResponseIngressSubscriber> logger)
        : base(options, logger)
    {
        _ingress = ingress;
    }

    internal RabbitMqResponseIngressSubscriber(
        IOptions<RabbitMqAsyncResponseOptions> options,
        IAsyncResponseIngress ingress,
        ILogger<RabbitMqResponseIngressSubscriber> logger,
        IRabbitMqConnectionFactory connectionFactory)
        : base(options, logger, connectionFactory)
    {
        _ingress = ingress;
    }

    protected override string QueueName
        => RabbitMqOptionsValidator.Required(Options.ResponseQueue, nameof(Options.ResponseQueue));

    protected override RabbitMqSubscriberOptions SubscriberOptions => Options.ResponseSubscriber;
    protected override RabbitMqSubscriberRole SubscriberRole => RabbitMqSubscriberRole.ResponseIngress;

    protected override Task EnsureTopologyAsync(IRabbitMqChannel channel, CancellationToken cancellationToken)
        => RabbitMqTopology.EnsureResponseAsync(channel, Options, cancellationToken);

    protected override Task HandleMessageAsync(RabbitMqDelivery delivery, CancellationToken cancellationToken)
    {
        var messageJson = Encoding.UTF8.GetString(delivery.Body.Span);
        var correlationId = RabbitMqCorrelationIdExtractor.Extract(delivery, messageJson, Options);
        return _ingress.HandleResponseMessageAsync(messageJson, correlationId);
    }
}
