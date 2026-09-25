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
    private int _handBackSignalled;

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

    /// <summary>
    /// Whether the flow engine has handed a delivery back (<see cref="DurableFlowInterruptedException"/>)
    /// — inline, or in an early-ACK worker. That only happens because the host is stopping, and
    /// it arrives before the subscriber's token (the engine reacts to ApplicationStopping, which
    /// fires first), so the subscriber stops fetching from here on. Latched: the host does not
    /// come back from a stop.
    /// </summary>
    public bool HandBackSignalled => Volatile.Read(ref _handBackSignalled) != 0;

    /// <summary>Handles the delivered message.</summary>
    public async Task HandleAsync(NatsJobDelivery delivery, CancellationToken cancellationToken)
    {
        // Pre-execution cap, BEFORE either ack mode (DB/Redis dispatcher parity).
        // HandleFailureAsync below is the only other place the cap is consulted, and it runs only
        // when the handler THREW — so a delivery that ends any other way (the process dies
        // mid-handler, the host is killed, a NAK fails) never reaches it. The consumer is created
        // with MaxDeliver = -1 on the premise that THIS dispatcher bounds attempts, so without
        // this check such a message redelivered forever after each AckWait, killing each replica
        // in turn, and was never dead-lettered. Settlement uses CancellationToken.None for the
        // usual reason: burying a poison message must not be abandoned half-done by a shutdown.
        var cap = _subscriberOptions.MaxDeliveryAttempts;
        if (cap > 0 && delivery.NumDelivered > cap)
        {
            _logger.LogError(
                "Message on subject {Subject} ({Role}) arrived on delivery {NumDelivered} with a cap of {MaxDeliveryAttempts}; dead-lettering without executing it.",
                delivery.Subject,
                _role,
                delivery.NumDelivered,
                cap);

            var shouldTerminate = await DeadLetterAsync(
                delivery,
                new InvalidOperationException(
                    $"Message exceeded {cap} delivery attempts without settling (delivery {delivery.NumDelivered})."),
                CancellationToken.None).ConfigureAwait(false);
            if (shouldTerminate)
            {
                // Guarded like the failure path's Term: a thrown settlement would unwind the
                // consume loop while the un-termed message redelivers and is dead-lettered again.
                try
                {
                    await delivery.TermAsync().ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(
                        ex,
                        "Failed to TERM NATS message on subject {Subject} ({Role}) after dead-lettering; it may redeliver and be dead-lettered again.",
                        delivery.Subject,
                        _role);
                }
            }
            else
            {
                await NakQuietlyAsync(delivery).ConfigureAwait(false);
            }

            return;
        }

        if (_subscriberOptions.AckMode is NatsAckMode.AckAfterEnqueue)
        {
            await HandleEarlyAckAsync(delivery, cancellationToken).ConfigureAwait(false);
            return;
        }

        try
        {
            await ExecuteHandlerAsync(delivery, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException ex) when (cancellationToken.IsCancellationRequested || ex is DurableFlowInterruptedException)
        {
            // Host shutdown, not a handler failure: the failure path would NAK it with the
            // redelivery delay and, at the attempt cap, dead-letter — or, with dead-lettering
            // disabled, TERMINATE — healthy work. Leave the delivery unsettled; AckWait lapses on
            // its own and at-least-once redelivery applies after restart (parity with the
            // RabbitMQ/Redis/Kafka/DB dispatchers). This does NOT spare the delivery attempt:
            // JetStream counted it when it handed the message over, settled or not. It is not
            // NAKed for immediate redelivery either — while this host is still fetching, that
            // would hand the job straight back to the host that is stopping.
            //
            // The flow engine's own hand-back (DurableFlowInterruptedException) arrives on
            // ApplicationStopping, which fires before any hosted service stops — so this
            // subscriber's token can still be live. It means the same thing: return without
            // settling instead of rethrowing, because a throw with a live token reaches the
            // supervisor as a subscriber failure (a Warning and a rebuild) for a routine deploy.
            if (cancellationToken.IsCancellationRequested)
                throw;

            Volatile.Write(ref _handBackSignalled, 1);
            _logger.LogInformation(
                "Handler for message on subject {Subject} ({Role}) was interrupted because the host is stopping; leaving the delivery unsettled for redelivery.",
                delivery.Subject,
                _role);
            return;
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
        catch (Exception ex) when (ex is not DurableFlowInterruptedException)
        {
            // The flow engine's host-stop hand-back is a shutdown, not a failed receive (parity
            // with the other transports): no error span on every rolling deploy.
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

                // Wait for the slot, then re-check the hand-back latch before taking it: a
                // delivery parked here when a worker set HandBackSignalled was still enqueued and
                // ACKed once a slot freed — the next flow wake-up for this stopping host to hand
                // back, a dead-letter copy where a live peer could have run it. Hand it back
                // unstarted instead, with no delay, as the batch loop does with the rest of its batch.
                do
                {
                    if (!await _backgroundQueue.Writer.WaitToWriteAsync(cancellationToken).ConfigureAwait(false))
                        throw new ChannelClosedException();

                    if (HandBackSignalled)
                    {
                        await NakQuietlyAsync(delivery, TimeSpan.Zero).ConfigureAwait(false);
                        return;
                    }
                }
                while (!_backgroundQueue.Writer.TryWrite(delivery));
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
        // already-ACKed delivery is either attempted or explicitly dead-lettered below — never
        // silently dropped.
        await foreach (var delivery in _backgroundQueue!.Reader.ReadAllAsync().ConfigureAwait(false))
        {
            // Once the drain budget has lapsed, STOP executing (DB/Redis/Pub-Sub parity). The
            // token below cannot stop the real handler — it is `_ingress.HandleWorkerMessageAsync
            // (payload)`, whose target takes no CancellationToken — so past the budget the loop
            // kept starting fresh work beyond the host's shutdown budget, and every entry still
            // queued at process exit vanished with no record (ACKed at enqueue, so JetStream never
            // redelivers it). Route the rest through the dead-letter/OnBackgroundFailure path.
            if (_backgroundCts!.IsCancellationRequested)
            {
                await RouteUndrainedAsync(delivery, CancellationToken.None).ConfigureAwait(false);
                continue;
            }

            try
            {
                await ExecuteHandlerAsync(delivery, cancellationToken).ConfigureAwait(false);
            }
            catch (DurableFlowInterruptedException ex)
            {
                // The flow engine's host-stop hand-back, raised on ApplicationStopping — usually
                // before this dispatcher's drain has begun. Not a handler failure, so no Error log;
                // but the message was ACKed at enqueue and JetStream will never redeliver it, so
                // beyond the OnBackgroundFailure report a dead-letter copy under its own reason is
                // its only durable record (Redis dispatcher parity): a replay is safe, the run
                // resuming from its last checkpoint.
                Volatile.Write(ref _handBackSignalled, 1);
                if (_options.DeadLetterEnabled)
                {
                    _logger.LogWarning(
                        "NATS background handler for already-ACKed message on subject {Subject} ({Role}) was handed back by the flow engine because the host is stopping; JetStream will not redeliver it. Dead-lettering a copy ({Reason}) and surfacing via OnBackgroundFailure.",
                        delivery.Subject,
                        _role,
                        HandedBackAfterCommitReason);
                    await DeadLetterAsync(delivery, ex, CancellationToken.None, HandedBackAfterCommitReason).ConfigureAwait(false);
                }
                else
                {
                    // No copy can be written (the burial would only log it as dropped): the
                    // wake-up is lost unless the report records it, so say so at Error.
                    _logger.LogError(
                        "NATS background handler for already-ACKed message on subject {Subject} ({Role}) was handed back by the flow engine because the host is stopping; JetStream will not redeliver it, and no dead-letter destination is configured, so the wake-up is lost unless OnBackgroundFailure records it (resume the flow explicitly).",
                        delivery.Subject,
                        _role);
                }

                await InvokeBackgroundFailureAsync(delivery, ex).ConfigureAwait(false);
            }
            catch (OperationCanceledException ex) when (_backgroundCts!.IsCancellationRequested)
            {
                // The drain budget lapsed with this already-ACKed message still unprocessed:
                // JetStream will not redeliver it, so surface the drop through OnBackgroundFailure
                // instead of dead-lettering a never-run job as a handler failure.
                _logger.LogWarning(
                    "NATS background handler for already-ACKed message on subject {Subject} ({Role}) was canceled during dispatcher shutdown; surfacing via OnBackgroundFailure.",
                    delivery.Subject,
                    _role);
                await InvokeBackgroundFailureAsync(delivery, ex).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Background handler failed for {Role} on subject {Subject} after early ACK.", _role, delivery.Subject);
                await DeadLetterAsync(delivery, ex, CancellationToken.None).ConfigureAwait(false);
                await InvokeBackgroundFailureAsync(delivery, ex).ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// Buries an already-ACKed entry the drain budget left unstarted: JetStream will never
    /// redeliver it, so the dead-letter copy and the OnBackgroundFailure report are its only record.
    /// </summary>
    private async Task RouteUndrainedAsync(NatsJobDelivery delivery, CancellationToken cancellationToken)
    {
        var lapsed = new OperationCanceledException(
            "The ACK-after-enqueue drain budget lapsed before this already-ACKed message was handled.");
        _logger.LogWarning(
            "NATS background handler for already-ACKed message on subject {Subject} ({Role}) was not started: the drain budget had lapsed. Dead-lettering and surfacing via OnBackgroundFailure.",
            delivery.Subject,
            _role);
        await DeadLetterAsync(delivery, lapsed, cancellationToken).ConfigureAwait(false);
        await InvokeBackgroundFailureAsync(delivery, lapsed).ConfigureAwait(false);
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

            // CancellationToken.None like every other settlement in this package (the
            // pre-execution cap and the early-ACK failure path already pin it): burying a poison
            // message must not be abandoned by a shutdown — with the stopping token, a handler
            // failing on its LAST attempt during a stop had the DLQ publish throw on the cancelled
            // token, and the message was NAKed back instead of buried.
            var shouldTerminate = await DeadLetterAsync(delivery, exception, CancellationToken.None).ConfigureAwait(false);
            if (shouldTerminate)
            {
                // Guarded like both ack sites: TermAsync is the same JetStream request/reply as
                // Ack/Nak and can throw, and a thrown settlement would unwind the consume loop and
                // rebuild the whole subscriber — while the un-termed message redelivers after
                // AckWait and dead-letters AGAIN, forever. Swallow and log; the duplicate
                // dead-letter on redelivery is the bounded at-least-once outcome.
                try
                {
                    await delivery.TermAsync().ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(
                        ex,
                        "Failed to TERM NATS message on subject {Subject} ({Role}) after dead-lettering; it may redeliver and be dead-lettered again.",
                        delivery.Subject,
                        _role);
                }
            }
            else
            {
                _logger.LogWarning(
                    exception,
                    "Dead-letter publish failed for subject {Subject} ({Role}); NAKing so the message can be retried.",
                    delivery.Subject,
                    _role);
                await NakQuietlyAsync(delivery).ConfigureAwait(false);
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
            await NakQuietlyAsync(delivery).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// NAKs with the configured redelivery delay (or <paramref name="delay"/>), swallowing
    /// settlement failures like the ack sites: a thrown NAK would unwind the consume loop and
    /// rebuild the subscriber, and the only consequence of a lost NAK is that redelivery waits for
    /// AckWait instead of the delay.
    /// </summary>
    private async Task NakQuietlyAsync(NatsJobDelivery delivery, TimeSpan? delay = null)
    {
        try
        {
            await delivery.NakAsync(delay ?? _subscriberOptions.RedeliveryDelay).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "Failed to NAK NATS message on subject {Subject} ({Role}); redelivery falls back to AckWait.",
                delivery.Subject,
                _role);
        }
    }

    /// <summary>
    /// Leads the <c>AR-DeadLetter-Reason</c> of an already-ACKed early-ACK message the flow engine
    /// handed back at host stop, so the copy can be told from a handler failure.
    /// </summary>
    internal const string HandedBackAfterCommitReason = "handed_back_after_commit";

    private async Task<bool> DeadLetterAsync(NatsJobDelivery delivery, Exception exception, CancellationToken cancellationToken, string? reasonCode = null)
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

        // Every Nats-* header is dropped: those are JetStream publish directives and server-set
        // metadata that belonged to the LIVE publish, not message data, and the server applies them
        // to this republish verbatim. Nats-Msg-Id made a second dead-letter of the same message
        // inside the DLQ stream's duplicate window — reachable whenever the Term below fails and
        // the message redelivers after AckWait — a deduplicated publish; a producer's
        // Nats-Expected-Stream / -Last-Sequence / -Last-Subject-Sequence, Nats-Rollup or Nats-TTL
        // made the DLQ stream reject the burial outright. Either way the caller read a DLQ failure
        // and NAKed, and with MaxDeliver = -1 the message looped every RedeliveryDelay forever.
        var headers = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var (name, value) in delivery.Headers)
        {
            if (!name.StartsWith("Nats-", StringComparison.OrdinalIgnoreCase))
                headers[name] = value;
        }

        // Capped (RabbitMQ/Kafka parity) so an arbitrarily long exception message cannot push the
        // burial past the server's max_payload; surrogate-aware (see PortableText.TruncateWellFormed).
        var reason = reasonCode is null ? exception.Message : $"{reasonCode}: {exception.Message}";
        headers["AR-DeadLetter-Reason"] = SanitizeHeaderValue(PortableText.TruncateWellFormed(reason, MaxDeadLetterReasonLength));
        headers["AR-DeadLetter-Source-Subject"] = delivery.Subject;
        headers["AR-DeadLetter-Role"] = _role.ToString();

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

    /// <summary>Longest <c>AR-DeadLetter-Reason</c> header value, in UTF-16 code units.</summary>
    internal const int MaxDeadLetterReasonLength = 512;

    private static string SanitizeHeaderValue(string value)
        => value.Replace('\r', ' ').Replace('\n', ' ');

    /// <summary>Releases resources held by this instance.</summary>
    public async ValueTask DisposeAsync()
    {
        if (_backgroundQueue is null)
            return;

        _backgroundQueue.Writer.TryComplete();

        // BackgroundDrainTimeout is the whole spend the shutdown-budget validator sums for this
        // dispatcher, so it is split rather than exceeded (DbTransportShared parity): most of it
        // lets queued and running handlers finish, and the rest is RESERVED for burying whatever
        // is still queued once it lapses.
        var routingReserve = TimeSpan.FromTicks(_subscriberOptions.BackgroundDrainTimeout.Ticks / 4);
        var drainBudget = _subscriberOptions.BackgroundDrainTimeout - routingReserve;
        var workers = Task.WhenAll(_backgroundWorkers!);
        try
        {
            await workers.WaitAsync(drainBudget).ConfigureAwait(false);
            _backgroundCts!.Dispose();
            return;
        }
        catch (TimeoutException)
        {
            _logger.LogWarning("Background handlers for {Role} did not drain within {Timeout}; dead-lettering the entries still queued.", _role, drainBudget);
            await _backgroundCts!.CancelAsync().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            // WhenAll only completes once every worker has finished, so the source is safe to dispose here.
            _logger.LogDebug(ex, "Background worker drain for {Role} ended with an error.", _role);
            _backgroundCts!.Dispose();
            return;
        }

        // Bury the queued entries HERE, not in the worker loop: an entry still queued when the
        // drain lapses means every worker is inside a handler (an idle one would have dequeued
        // it), and the handler takes no token — so the workers' own drain-lapsed routing ran only
        // once some handler happened to finish, after this method had returned and the host had
        // torn the connection down or exited. Those already-ACKed jobs vanished with only a
        // warning. A worker that does come free routes entries too; each is read exactly once.
        using var reserve = new CancellationTokenSource(routingReserve);
        while (!reserve.IsCancellationRequested && _backgroundQueue.Reader.TryRead(out var undrained))
            await RouteUndrainedAsync(undrained, reserve.Token).ConfigureAwait(false);

        try
        {
            // What remains of the reserve lets a handler that honors the cancellation report it.
            await workers.WaitAsync(reserve.Token).ConfigureAwait(false);
            _backgroundCts.Dispose();
            return;
        }
        catch (OperationCanceledException) when (reserve.IsCancellationRequested)
        {
            if (_backgroundQueue.Reader.Count > 0)
            {
                _logger.LogError(
                    "{Count} already-ACKed NATS message(s) for {Role} were still queued when the reserved {Reserve} ran out; they are lost at process exit (JetStream will not redeliver them).",
                    _backgroundQueue.Reader.Count,
                    _role,
                    routingReserve);
            }
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Background worker drain for {Role} ended with an error.", _role);
            _backgroundCts.Dispose();
            return;
        }

        // The workers are still running and observe _backgroundCts.Token inside ReadAllAsync, so disposing
        // it now would throw ObjectDisposedException inside them. Dispose once they actually finish, off
        // the shutdown path, so the source is not leaked either.
        _ = workers.ContinueWith(
            _ => _backgroundCts.Dispose(),
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);
    }
}
