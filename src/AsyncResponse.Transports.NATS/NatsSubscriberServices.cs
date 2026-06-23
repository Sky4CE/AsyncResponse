using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NATS.Client.Core;
using NATS.Net;

namespace AsyncResponse.Transports.NATS;

/// <summary>
/// Base hosted service that consumes a JetStream subject through a durable consumer and routes each
/// message to the AsyncResponse ingress with the configured acknowledgement/redelivery/dead-letter
/// policy. A failed consume loop is retried with bounded backoff so a transient NATS outage does not
/// kill the subscriber.
/// </summary>
internal abstract class NatsSubscriberService : BackgroundService
{
    private readonly INatsJetStreamTransport _jetStream;

    protected NatsSubscriberService(
        IOptions<NatsAsyncResponseTransportOptions> options,
        INatsConnection connection,
        ILogger logger)
        : this(options, new NatsJetStreamTransportAdapter(connection.CreateJetStreamContext()), logger)
    {
    }

    protected NatsSubscriberService(
        IOptions<NatsAsyncResponseTransportOptions> options,
        INatsJetStreamTransport jetStream,
        ILogger logger)
    {
        Options = options.Value;
        NatsTransportOptionsValidator.ValidateCommon(Options);
        _jetStream = jetStream;
        Logger = logger;
        Schema = new NatsTransportSubjectSchema(Options);
    }

    protected NatsAsyncResponseTransportOptions Options { get; }
    protected ILogger Logger { get; }
    protected NatsTransportSubjectSchema Schema { get; }

    protected abstract string Subject { get; }
    protected abstract string Stream { get; }
    protected abstract string Consumer { get; }
    protected abstract NatsSubscriberOptions SubscriberOptions { get; }
    protected abstract NatsSubscriberRole Role { get; }
    protected abstract Task HandleMessageAsync(NatsJobDelivery delivery, CancellationToken cancellationToken);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        NatsTransportOptionsValidator.ValidateSubscriber(SubscriberOptions, Role.ToString());

        var failures = 0;
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await RunSubscriberAsync(stoppingToken).ConfigureAwait(false);
                return;
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex) when (!stoppingToken.IsCancellationRequested)
            {
                failures++;
                var retryDelay = NatsTransportRetry.Backoff(failures, Options.SubscriberRetryBaseDelay, Options.SubscriberRetryMaxDelay);
                Logger.LogWarning(ex, "NATS subscriber failed for subject {Subject} ({Role}); retrying in {RetryDelay}.", Subject, Role, retryDelay);
                await Task.Delay(retryDelay, stoppingToken).ConfigureAwait(false);
            }
        }
    }

    private async Task RunSubscriberAsync(CancellationToken stoppingToken)
    {
        if (Options.CreateStreams)
        {
            await _jetStream.EnsureStreamAsync(Stream, Subject, Options.StreamMaxMessages, stoppingToken).ConfigureAwait(false);
            if (Options.DeadLetterEnabled)
                await _jetStream.EnsureStreamAsync(Schema.DeadLetterStream, Schema.DeadLetterSubject, Options.DeadLetterStreamMaxMessages, stoppingToken).ConfigureAwait(false);
        }

        await _jetStream.EnsureConsumerAsync(Stream, Consumer, Options.AckWait, stoppingToken).ConfigureAwait(false);

        await using var dispatcher = new NatsMessageDispatcher(
            HandleMessageAsync,
            _jetStream,
            Options,
            SubscriberOptions,
            Schema,
            Logger,
            Role,
            Consumer);

        Logger.LogInformation(
            "NATS subscriber started. Subject: {Subject}. Stream: {Stream}. Consumer: {Consumer}. Role: {Role}. AckMode: {AckMode}.",
            Subject, Stream, Consumer, Role, SubscriberOptions.AckMode);

        await foreach (var delivery in _jetStream.ConsumeAsync(Stream, Consumer, SubscriberOptions.BatchSize, stoppingToken).ConfigureAwait(false))
        {
            await dispatcher.HandleAsync(delivery, stoppingToken).ConfigureAwait(false);
        }
    }
}

/// <summary>Consumes worker-job messages and executes them through the AsyncResponse ingress.</summary>
internal sealed class NatsWorkerSubscriber : NatsSubscriberService
{
    private readonly IAsyncResponseIngress _ingress;

    public NatsWorkerSubscriber(
        IOptions<NatsAsyncResponseTransportOptions> options,
        INatsConnection connection,
        IAsyncResponseIngress ingress,
        ILogger<NatsWorkerSubscriber> logger)
        : base(options, connection, logger)
        => _ingress = ingress;

    internal NatsWorkerSubscriber(
        IOptions<NatsAsyncResponseTransportOptions> options,
        INatsJetStreamTransport jetStream,
        IAsyncResponseIngress ingress,
        ILogger<NatsWorkerSubscriber> logger)
        : base(options, jetStream, logger)
        => _ingress = ingress;

    protected override string Subject => Schema.WorkerSubject;
    protected override string Stream => Schema.WorkerStream;
    protected override string Consumer => Options.WorkerConsumer;
    protected override NatsSubscriberOptions SubscriberOptions => Options.WorkerSubscriber;
    protected override NatsSubscriberRole Role => NatsSubscriberRole.Worker;

    protected override Task HandleMessageAsync(NatsJobDelivery delivery, CancellationToken cancellationToken)
        => _ingress.HandleWorkerMessageAsync(delivery.Payload);
}

/// <summary>Consumes response messages and feeds them into the AsyncResponse ingress, correlated by header or JSON body.</summary>
internal sealed class NatsResponseIngressSubscriber : NatsSubscriberService
{
    private readonly IAsyncResponseIngress _ingress;

    public NatsResponseIngressSubscriber(
        IOptions<NatsAsyncResponseTransportOptions> options,
        INatsConnection connection,
        IAsyncResponseIngress ingress,
        ILogger<NatsResponseIngressSubscriber> logger)
        : base(options, connection, logger)
        => _ingress = ingress;

    internal NatsResponseIngressSubscriber(
        IOptions<NatsAsyncResponseTransportOptions> options,
        INatsJetStreamTransport jetStream,
        IAsyncResponseIngress ingress,
        ILogger<NatsResponseIngressSubscriber> logger)
        : base(options, jetStream, logger)
        => _ingress = ingress;

    protected override string Subject => Schema.ResponseSubject;
    protected override string Stream => Schema.ResponseStream;
    protected override string Consumer => Options.ResponseConsumer;
    protected override NatsSubscriberOptions SubscriberOptions => Options.ResponseSubscriber;
    protected override NatsSubscriberRole Role => NatsSubscriberRole.ResponseIngress;

    protected override Task HandleMessageAsync(NatsJobDelivery delivery, CancellationToken cancellationToken)
    {
        var correlationId = NatsCorrelationIdExtractor.Extract(delivery.Headers, delivery.Payload, Options);
        return _ingress.HandleResponseMessageAsync(delivery.Payload, correlationId);
    }
}
