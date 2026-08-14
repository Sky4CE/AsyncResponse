using Microsoft.Extensions.Logging;
using System.Diagnostics;
using System.Threading.Channels;

namespace AsyncResponse.Transports.NATS;

internal enum NatsSubscriberRole
{
    Worker,
    ResponseIngress
}

/// <summary>
/// Applies the acknowledgement, redelivery, and dead-letter policy to JetStream deliveries.
/// <list type="bullet">
/// <item><description><see cref="NatsAckMode.AckAfterHandlerCompletes"/>: run the handler, then ACK;
/// on failure NAK for redelivery until <see cref="NatsSubscriberOptions.MaxDeliveryAttempts"/>, then
/// dead-letter and terminate.</description></item>
/// <item><description><see cref="NatsAckMode.AckAfterEnqueue"/>: enqueue to a bounded background
/// queue and ACK immediately; background handler failures are dead-lettered and reported.</description></item>
/// </list>
/// </summary>
internal sealed class NatsMessageDispatcher : IAsyncDisposable
{
    private readonly Func<NatsJobDelivery, CancellationToken, Task> _handler;
    private readonly INatsJetStreamTransport _jetStream;
    private readonly NatsAsyncResponseTransportOptions _options;
    private readonly NatsSubscriberOptions _subscriberOptions;
    private readonly NatsTransportSubjectSchema _schema;
    private readonly ILogger _logger;
    private readonly NatsSubscriberRole _role;
    private readonly string _consumer;

    private readonly Channel<NatsJobDelivery>? _backgroundQueue;
    private readonly Task[]? _backgroundWorkers;
    private readonly CancellationTokenSource? _backgroundCts;

    /// <summary>Runs the NatsMessageDispatcher operation.</summary>
    public NatsMessageDispatcher(
        Func<NatsJobDelivery, CancellationToken, Task> handler,
        INatsJetStreamTransport jetStream,
        NatsAsyncResponseTransportOptions options,
        NatsSubscriberOptions subscriberOptions,
        NatsTransportSubjectSchema schema,
        ILogger logger,
        NatsSubscriberRole role,
        string consumer)
    {
        NatsTransportOptionsValidator.ValidateSubscriber(options, subscriberOptions, role.ToString());

        _handler = handler;
        _jetStream = jetStream;
        _options = options;
        _subscriberOptions = subscriberOptions;
        _schema = schema;
        _logger = logger;
        _role = role;
        _consumer = consumer;

        if (subscriberOptions.AckMode is NatsAckMode.AckAfterEnqueue)
        {
            _backgroundQueue = Channel.CreateBounded<NatsJobDelivery>(new BoundedChannelOptions(subscriberOptions.BackgroundQueueCapacity)
            {
                SingleReader = false,
                SingleWriter = true,
                FullMode = BoundedChannelFullMode.Wait
            });
            _backgroundCts = new CancellationTokenSource();
            _backgroundWorkers = new Task[subscriberOptions.BackgroundWorkerCount];
            for (var i = 0; i < _backgroundWorkers.Length; i++)
                _backgroundWorkers[i] = Task.Run(() => BackgroundWorkerLoopAsync(_backgroundCts.Token));
        }
    }

    /// <summary>Handles the delivered message.</summary>
    public async Task HandleAsync(NatsJobDelivery delivery, CancellationToken cancellationToken)
    {
        if (_subscriberOptions.AckMode is NatsAckMode.AckAfterEnqueue)
        {
            await HandleEarlyAckAsync(delivery, cancellationToken).ConfigureAwait(false);
            return;
        }

        try
        {
            await ExecuteHandlerAsync(delivery, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Host shutdown, not a handler failure: NAK would burn a delivery attempt on work
            // that never ran, and at the attempt cap the failure path would dead-letter — or,
            // with dead-lettering disabled, TERMINATE — healthy work. Leave the delivery
            // unsettled; AckWait lapses on its own and at-least-once redelivery applies after
            // restart (parity with the RabbitMQ/Redis/Kafka/DB dispatchers).
            throw;
        }
        catch (Exception ex)
        {
            await HandleFailureAsync(delivery, ex, cancellationToken).ConfigureAwait(false);
            return;
        }

        // The ACK sits outside the handler's try/catch: a transient ack failure after a successful
        // handler must not be misread as a handler failure — NAK/dead-letter here would redeliver
        // (or bury) work whose side effects already completed. Swallow and log instead; the ack
        // window lapses on its own and at-least-once redelivery applies.
        try
        {
            await delivery.AckAsync().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "Failed to ACK NATS message on subject {Subject} ({Role}) after a successful handler; it may be redelivered.",
                delivery.Subject,
                _role);
        }
    }

    // Single choke point for handler execution so both ACK modes emit the consumer receive span.
    private async Task ExecuteHandlerAsync(NatsJobDelivery delivery, CancellationToken cancellationToken)
    {
        using var activity = AsyncResponseDiagnostics.StartActivity(
            "asyncresponse.nats.receive",
            ActivityKind.Consumer);
        activity?.SetTag("asyncresponse.transport", "nats");
        activity?.SetTag("asyncresponse.nats.role", _role.ToString());
        activity?.SetTag("asyncresponse.nats.ack_mode", _subscriberOptions.AckMode.ToString());
        activity?.SetTag("messaging.system", "nats");
        activity?.SetTag("messaging.destination.name", delivery.Subject);
        activity?.SetTag("messaging.nats.num_delivered", delivery.NumDelivered);

        if (delivery.Headers.TryGetValue(_options.CorrelationIdHeader, out var correlationId))
            AsyncResponseDiagnostics.SetCorrelationId(activity, correlationId);

        try
        {
            await _handler(delivery, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            AsyncResponseDiagnostics.SetError(activity, ex);
            throw;
        }
    }

    private async Task HandleEarlyAckAsync(NatsJobDelivery delivery, CancellationToken cancellationToken)
    {
        // Accept into the background queue and ACK. If the queue is saturated, wait for a worker to
        // free a slot instead of NAKing: the wait blocks the consume loop, so the subscriber stops
        // pulling new messages until capacity frees rather than churning NAK/redeliver cycles.
        if (!_backgroundQueue!.Writer.TryWrite(delivery))
        {
            try
            {
                _logger.LogDebug("Background queue full for {Role}; pausing the consume loop until capacity frees.", _role);
                await _backgroundQueue.Writer.WriteAsync(delivery, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is OperationCanceledException or ChannelClosedException)
            {
                // Subscriber stopping or dispatcher disposing while parked: the delivery was never
                // enqueued, so NAK so JetStream redelivers elsewhere; if the NAK itself fails the
                // AckWait lapses to the same effect.
                _logger.LogDebug("Background queue unavailable for {Role} during shutdown; NAKing message for redelivery.", _role);
                try
                {
                    await delivery.NakAsync(_subscriberOptions.RedeliveryDelay).ConfigureAwait(false);
                }
                catch (Exception nakException)
                {
                    _logger.LogWarning(
                        nakException,
                        "Failed to NAK NATS message on subject {Subject} ({Role}) while stopping; AckWait will lapse and it will be redelivered.",
                        delivery.Subject,
                        _role);
                }

                return;
            }
        }

        // The ACK sits outside the enqueue try/catch, and never NAKs or escapes: the delivery is
        // already owned by a background worker, so a NAK would redeliver a job that is being
        // executed, and a thrown ack failure would unwind the consume loop and rebuild the whole
        // subscriber — draining the workers mid-handler while the un-ACKed message redelivers
        // after AckWait and runs again. Swallow and log; at-least-once redelivery applies.
        try
        {
            await delivery.AckAsync().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "Failed to ACK NATS message on subject {Subject} ({Role}) after enqueueing it for background execution; it may be redelivered.",
                delivery.Subject,
                _role);
        }
    }

    private async Task BackgroundWorkerLoopAsync(CancellationToken cancellationToken)
    {
        // Token-less ReadAllAsync: on shutdown the queue is completed and fully drained, so every
        // already-ACKed delivery is attempted (with the drain token once the drain budget lapses)
        // instead of being silently dropped; each failure is dead-lettered and surfaced below.
        await foreach (var delivery in _backgroundQueue!.Reader.ReadAllAsync().ConfigureAwait(false))
        {
            try
            {
                await ExecuteHandlerAsync(delivery, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Background handler failed for {Role} on subject {Subject} after early ACK.", _role, delivery.Subject);
                await DeadLetterAsync(delivery, ex, CancellationToken.None).ConfigureAwait(false);
                await InvokeBackgroundFailureAsync(delivery, ex).ConfigureAwait(false);
            }
        }
    }

    private async Task HandleFailureAsync(NatsJobDelivery delivery, Exception exception, CancellationToken cancellationToken)
    {
        var maxAttempts = _subscriberOptions.MaxDeliveryAttempts;
        if (maxAttempts > 0 && delivery.NumDelivered >= maxAttempts)
        {
            _logger.LogError(
                exception,
                "Message on subject {Subject} ({Role}) failed after {Attempts} attempts; dead-lettering.",
                delivery.Subject,
                _role,
                delivery.NumDelivered);

            var shouldTerminate = await DeadLetterAsync(delivery, exception, cancellationToken).ConfigureAwait(false);
            if (shouldTerminate)
            {
                await delivery.TermAsync().ConfigureAwait(false);
            }
            else
            {
                _logger.LogWarning(
                    exception,
                    "Dead-letter publish failed for subject {Subject} ({Role}); NAKing so the message can be retried.",
                    delivery.Subject,
                    _role);
                await delivery.NakAsync(_subscriberOptions.RedeliveryDelay).ConfigureAwait(false);
            }
        }
        else
        {
            _logger.LogWarning(
                exception,
                "Message on subject {Subject} ({Role}) failed on attempt {Attempt}; NAKing for redelivery.",
                delivery.Subject,
                _role,
                delivery.NumDelivered);
            await delivery.NakAsync(_subscriberOptions.RedeliveryDelay).ConfigureAwait(false);
        }
    }

    private async Task<bool> DeadLetterAsync(NatsJobDelivery delivery, Exception exception, CancellationToken cancellationToken)
    {
        if (!_options.DeadLetterEnabled)
        {
            _logger.LogError(
                exception,
                "Message on subject {Subject} ({Role}) is unprocessable and dead-lettering is disabled; it will be dropped.",
                delivery.Subject,
                _role);
            return true;
        }

        var headers = new Dictionary<string, string>(delivery.Headers, StringComparer.OrdinalIgnoreCase)
        {
            ["AR-DeadLetter-Reason"] = SanitizeHeaderValue(exception.Message),
            ["AR-DeadLetter-Source-Subject"] = delivery.Subject,
            ["AR-DeadLetter-Role"] = _role.ToString()
        };

        try
        {
            await _jetStream.PublishAsync(_schema.DeadLetterSubject, delivery.Payload, headers, cancellationToken).ConfigureAwait(false);
            _logger.LogInformation("Dead-lettered message from subject {Subject} ({Role}) to {DeadLetterSubject}.", delivery.Subject, _role, _schema.DeadLetterSubject);
            return true;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to dead-letter message from subject {Subject} ({Role}).", delivery.Subject, _role);
            return false;
        }
    }

    private async Task InvokeBackgroundFailureAsync(NatsJobDelivery delivery, Exception exception)
    {
        if (_subscriberOptions.OnBackgroundFailure is null)
            return;

        try
        {
            delivery.Headers.TryGetValue(_options.CorrelationIdHeader, out var correlationId);
            var context = new NatsBackgroundFailureContext(delivery.Subject, _consumer, _role.ToString(), delivery.NumDelivered, correlationId, exception);
            await _subscriberOptions.OnBackgroundFailure(context).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "OnBackgroundFailure callback threw for {Role}.", _role);
        }
    }

    private static string SanitizeHeaderValue(string value)
        => value.Replace('\r', ' ').Replace('\n', ' ');

    /// <summary>Releases resources held by this instance.</summary>
    public async ValueTask DisposeAsync()
    {
        if (_backgroundQueue is null)
            return;

        _backgroundQueue.Writer.TryComplete();
        try
        {
            await Task.WhenAll(_backgroundWorkers!).WaitAsync(_subscriberOptions.BackgroundDrainTimeout).ConfigureAwait(false);
            _backgroundCts!.Dispose();
        }
        catch (TimeoutException)
        {
            _logger.LogWarning("Background handlers for {Role} did not drain within {Timeout}.", _role, _subscriberOptions.BackgroundDrainTimeout);
            await _backgroundCts!.CancelAsync().ConfigureAwait(false);

            // The workers are still running and observe _backgroundCts.Token inside ReadAllAsync, so disposing
            // it now would throw ObjectDisposedException inside them. Dispose once they actually finish, off
            // the shutdown path, so the source is not leaked either.
            _ = Task.WhenAll(_backgroundWorkers!).ContinueWith(
                _ => _backgroundCts.Dispose(),
                CancellationToken.None,
                TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default);
        }
        catch (Exception ex)
        {
            // WhenAll only completes once every worker has finished, so the source is safe to dispose here.
            _logger.LogDebug(ex, "Background worker drain for {Role} ended with an error.", _role);
            _backgroundCts!.Dispose();
        }
    }
}
