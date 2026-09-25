using Microsoft.Extensions.Logging;
using System.Text.Json;
using System.Threading.Channels;

namespace AsyncResponse.Transports;

// Shared source for the database-backed worker transports (PostgreSQL, SQL Server, MongoDB),
// mirroring src/Channels/Shared/DbChannelShared.cs: each transport csproj pulls this file in via
// <Compile Include="..\Shared\DbTransportShared.cs" />, so the base class compiles INTO each
// provider assembly against that provider's concrete seam types. The seam is bound per project
// with global using aliases (declared at the top of the provider's MessageDispatcher file):
//
//   DbTransportOptions          -> the provider's transport options (e.g. PostgreSqlAsyncResponseTransportOptions)
//   DbSubscriberOptions         -> the provider's subscriber options (e.g. PostgreSqlSubscriberOptions)
//   DbTransportDelivery         -> the provider's claimed-delivery type (e.g. PostgreSqlTransportDelivery)
//   DbSubscriberRole            -> the provider's subscriber-role enum
//   DbAckMode                   -> the provider's ack-mode enum
//   DbTransportOptionsValidator -> the provider's static options validator
//   DbBackgroundFailureContext  -> the provider's OnBackgroundFailure context type
//
// Because the aliases resolve to concrete sealed types at compile time, delivery calls stay
// direct — no interface dispatch on the per-message path. The only provider-specific inputs are
// three display strings supplied by the derived constructor: the provider name rendered into log
// messages, the queue-item noun ("row"/"document"), and the lowercase telemetry tag. Rendered log
// output and activity tags are byte-identical to the pre-extraction per-provider sources.

/// <summary>
/// Applies acknowledgement, redelivery, and dead-letter policy to database transport deliveries:
/// ack-after-handler with fenced lease renewal, opt-in early ACK behind a bounded in-process
/// queue with drain-on-dispose, attempt-capped dead-lettering, and the consumer receive span.
/// Derived dispatchers supply only the provider display name, queue-item noun, and telemetry tag.
/// </summary>
internal abstract class DbMessageDispatcherBase : IAsyncDisposable
{
    private readonly Func<DbTransportDelivery, CancellationToken, Task> _handler;
    private readonly DbTransportOptions _options;
    private readonly DbSubscriberOptions _subscriberOptions;
    private readonly ILogger _logger;
    private readonly DbSubscriberRole _role;
    private readonly string _providerName;
    private readonly string _unitNoun;
    private readonly string _receiveActivityName;
    private readonly string _transportTag;
    private readonly string _roleTagName;
    private readonly string _ackModeTagName;
    private readonly TimeProvider _timeProvider;

    private readonly Channel<DbTransportDelivery>? _backgroundQueue;
    private readonly Task[]? _backgroundWorkers;
    private readonly CancellationTokenSource? _backgroundCts;

    protected DbMessageDispatcherBase(
        Func<DbTransportDelivery, CancellationToken, Task> handler,
        DbTransportOptions options,
        DbSubscriberOptions subscriberOptions,
        ILogger logger,
        DbSubscriberRole role,
        string providerName,
        string unitNoun,
        string telemetryName,
        TimeProvider? timeProvider = null)
    {
        DbTransportOptionsValidator.ValidateSubscriber(options, subscriberOptions, role.ToString());

        _handler = handler;
        _options = options;
        _subscriberOptions = subscriberOptions;
        _logger = logger;
        _role = role;
        _providerName = providerName;
        _unitNoun = unitNoun;
        _receiveActivityName = $"asyncresponse.{telemetryName}.receive";
        _transportTag = telemetryName;
        _roleTagName = $"asyncresponse.{telemetryName}.role";
        _ackModeTagName = $"asyncresponse.{telemetryName}.ack_mode";

        // Clocks the lease-renewal beat only (a test seam; the system clock when omitted).
        _timeProvider = timeProvider ?? TimeProvider.System;

        if (subscriberOptions.AckMode is DbAckMode.AckAfterEnqueue)
        {
            _backgroundQueue = Channel.CreateBounded<DbTransportDelivery>(new BoundedChannelOptions(subscriberOptions.BackgroundQueueCapacity)
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

    /// <summary>Handles one claimed queue item.</summary>
    public async Task HandleAsync(DbTransportDelivery delivery, CancellationToken cancellationToken)
    {
        // Pre-execution cap, BEFORE either ack mode. HandleFailureAsync below is the only other
        // place the cap is consulted, and it runs only when the handler THREW — so a delivery that
        // ends any other way (the process dies mid-handler, the host is killed, the lease lapses
        // while the DB is unreachable at settlement) never reaches it. The claim already stamped
        // attempts+1, so the row comes back at attempts cap+1, cap+2, ... and would be executed
        // again every time: redelivered forever, killing each replica in turn, and never
        // dead-lettered — the opposite of what MaxDeliveryAttempts documents. Settlement uses
        // CancellationToken.None for the usual reason: burying a poison row must not be abandoned
        // half-done by a shutdown. Mirrors the Redis dispatcher's AlreadyExceededDeliveryAttempts.
        var cap = _subscriberOptions.MaxDeliveryAttempts;
        if (cap > 0 && delivery.Attempt > cap)
        {
            _logger.LogError(
                "{Provider} message on queue {Queue} ({Role}) arrived on attempt {Attempt} with a cap of {MaxDeliveryAttempts}; dead-lettering without executing it.",
                _providerName,
                delivery.Queue,
                _role,
                delivery.Attempt,
                cap);

            var buried = await DeadLetterSwallowingFailureAsync(
                    delivery,
                    new InvalidOperationException(
                        $"Message exceeded {cap} delivery attempts without settling (attempt {delivery.Attempt})."),
                    deleteOriginal: true)
                .ConfigureAwait(false);

            if (!buried)
            {
                _logger.LogWarning(
                    "{Provider} dead-letter publish failed for over-cap message on queue {Queue} ({Role}); releasing for retry.",
                    _providerName,
                    delivery.Queue,
                    _role);
                await NakSwallowingFailureAsync(delivery).ConfigureAwait(false);
            }

            return;
        }

        if (_subscriberOptions.AckMode is DbAckMode.AckAfterEnqueue)
        {
            await HandleEarlyAckAsync(delivery, cancellationToken).ConfigureAwait(false);
            return;
        }

        try
        {
            // While the handler runs, a fenced heartbeat keeps extending the claim's lease at
            // LockTimeout/3 cadence so a slow handler does not let the lock lapse and a competing
            // subscriber re-claim (and duplicate-process) the queue item. The heartbeat MUST be
            // armed before any user code runs: a handler can burn its lease entirely
            // synchronously (CPU work or blocking I/O before its first await), and only an
            // already-armed beat — firing on a timer thread — renews under a blocked handler
            // thread. Teardown is exception-free (SuppressThrowing beat), so the always-armed
            // loop costs allocations per delivery, not a thrown TaskCanceledException.
            //
            // The source is NOT linked to the subscriber's stopping token (SQS/NATS parity): the
            // handler takes no token and keeps running through the host's stop budget — longer if
            // the operator raised HostOptions.ShutdownTimeout to let long jobs finish — so a beat
            // that stopped with the subscriber let locked_until pass under a live handler, a peer
            // (a new replica in a rolling deploy) claimed the row and ran it concurrently, and the
            // original's fenced ack silently no-opped. The beat ends only when the handler does.
            using var renewalCancellation = new CancellationTokenSource();
            var renewalTask = RenewLeaseLoopAsync(delivery, renewalCancellation.Token);
            try
            {
                await ExecuteHandlerAsync(delivery, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                renewalCancellation.Cancel();
                ObserveRenewal(renewalTask);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Host shutdown, not a handler failure: NAK would burn an attempt and dead-letter
            // would bury healthy work once the cap is reached. Leave the claim unsettled — the
            // lease lapses on its own and at-least-once redelivery applies after restart.
            throw;
        }
        catch (DurableFlowInterruptedException ex)
        {
            // The flow engine's "host is stopping, hand this delivery back" signal — the same
            // shutdown as above, but it arrives BEFORE this subscriber's own token is cancelled:
            // ApplicationStopping fires ahead of every hosted service's StopAsync, and the worker
            // subscriber, registered first, is stopped last. Treated as a handler failure it logged
            // "failed on attempt N", marked the receive span as an error, NAKed — and at the cap
            // buried the flow's wake-up with a "Host is stopping" reason. Leave the claim unsettled
            // exactly like the stop path (the heartbeat ends with the handler, so the lease lapses
            // and the row is redelivered after the restart), and RETURN rather than rethrow: with
            // a live token, a rethrow reads to the supervisor as a subscriber fault.
            _logger.LogDebug(
                ex,
                "{Provider} message {MessageId} on queue {Queue} ({Role}) was handed back by a stopping host; leaving the claim unsettled for redelivery.",
                _providerName,
                delivery.Id,
                delivery.Queue,
                _role);
            return;
        }
        catch (Exception ex)
        {
            await HandleFailureAsync(delivery, ex).ConfigureAwait(false);
            return;
        }

        // The ack runs outside the handler's try/catch: a transient ack failure after a
        // successful handler must not be misread as a handler failure — NAK/dead-letter here
        // would redeliver (or bury) work whose side effects already completed. Swallow and log
        // instead; the claim's lease lapses on its own and at-least-once redelivery applies.
        try
        {
            await delivery.AckAsync().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "Failed to ACK {Provider} message {MessageId} on queue {Queue} ({Role}) after a successful handler; the lease will lapse and the {Unit} may be redelivered.",
                _providerName,
                delivery.Id,
                delivery.Queue,
                _role,
                _unitNoun);
        }
    }

    /// <param name="delivery">The claimed delivery whose lease is renewed.</param>
    /// <param name="cancellationToken">Ends the loop (the handler finished, or the park ended).</param>
    /// <param name="leaseLost">Cancelled when a renew reports the <c>lock_id</c> fence gone — positive
    /// knowledge that a peer re-claimed (or finished) the row, since the renew is fenced on
    /// <c>lock_id</c> alone.</param>
    private async Task RenewLeaseLoopAsync(DbTransportDelivery delivery, CancellationToken cancellationToken, CancellationTokenSource? leaseLost = null)
    {
        // A third of the lease, and a FAILED beat retries on a short backoff instead of waiting out
        // another full beat. At LockTimeout/2 with the retry one more beat away, the retry landed
        // at claim + LockTimeout — after locked_until, every time: ONE transient renew failure (a
        // command timeout, a broken pooled connection, a SQL Server 1205 deadlock victim)
        // guaranteed the lease lapsed, a peer claimed the row within its EmptyPollDelay, and a
        // healthy long handler ran twice concurrently. Now a failed beat leaves two thirds of the
        // lease for retries a second (or LockTimeout/10) apart. Both waits are floored at a
        // millisecond: Task.Delay truncates to whole milliseconds, and a zero wait would spin.
        var interval = TimeSpan.FromTicks(Math.Max(TimeSpan.TicksPerMillisecond, _options.LockTimeout.Ticks / 3));
        var retryInterval = TimeSpan.FromTicks(Math.Max(TimeSpan.TicksPerMillisecond, Math.Min(TimeSpan.TicksPerSecond, _options.LockTimeout.Ticks / 10)));
        var wait = interval;
        var failing = false;
        try
        {
            while (true)
            {
                // Exception-free beat: the loop is cancelled once per delivery when the handler
                // finishes, and a thrown-and-caught TaskCanceledException per message dominated
                // the dispatch cost. SuppressThrowing observes the cancelled delay without
                // throwing; cancellation still disarms the underlying timer immediately.
                await Task.Delay(wait, _timeProvider, cancellationToken).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
                if (cancellationToken.IsCancellationRequested)
                    return; // The handler finished or the subscriber is stopping.

                // Every attempt is BOUNDED by the beat interval (the stores honor the token: connect,
                // command, and SqlClient's CommandTimeout backstop). Unbounded, a renew hung on a
                // black-holed pooled connection, a failover, or a lock wait inherited the provider's
                // command timeout — 30 s on Npgsql/SqlClient, none on MongoDB — which outlasted the
                // two thirds of the lease left after the beat, so the lease lapsed before the
                // short-backoff retry below ever ran and a peer re-ran a healthy handler. A timed-out
                // attempt is a failed beat: retried, on a fresh connection, inside the lease.
                bool renewed;
                try
                {
                    using var attemptTimeout = new CancellationTokenSource(interval, _timeProvider);
                    using var attempt = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, attemptTimeout.Token);
                    renewed = await delivery.RenewAsync(attempt.Token).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    if (cancellationToken.IsCancellationRequested)
                        return;

                    // Keep retrying past locked_until too: the renew is fenced on lock_id alone,
                    // so until a peer actually re-claims the row a late renew still re-establishes
                    // the lease. Only the first failure of a streak is a warning — at this
                    // cadence a database outage would otherwise log one per second per in-flight
                    // delivery.
                    _logger.Log(
                        failing ? LogLevel.Debug : LogLevel.Warning,
                        ex,
                        "Failed to renew the lease of {Provider} message {MessageId} on queue {Queue} ({Role}); retrying every {RetryInterval} until it succeeds or the lease is lost.",
                        _providerName,
                        delivery.Id,
                        delivery.Queue,
                        _role,
                        retryInterval);
                    failing = true;
                    wait = retryInterval;
                    continue;
                }

                // The beat is not joined before settlement (see ObserveRenewal), so a renew that
                // was in flight when the handler finished can land AFTER the fenced ack/NAK cleared
                // the row's lock_id. That "no match" is the settlement's own doing, not a lost lease.
                if (cancellationToken.IsCancellationRequested)
                    return;

                if (!renewed)
                {
                    // The lock_id fence no longer matches: the lease expired and another subscriber
                    // claimed the queue item. Stop renewing; the fenced ack/NAK will no-op for this
                    // claim.
                    _logger.LogWarning(
                        "Lease of {Provider} message {MessageId} on queue {Queue} ({Role}) was lost; another subscriber may process it (at-least-once preserved).",
                        _providerName,
                        delivery.Id,
                        delivery.Queue,
                        _role);
                    try
                    {
                        leaseLost?.Cancel();
                    }
                    catch (ObjectDisposedException)
                    {
                        // The park already ended and disposed its source; nobody is listening.
                    }

                    return;
                }

                failing = false;
                wait = interval;
            }
        }
        catch (OperationCanceledException)
        {
            // A cancellation surfacing through RenewAsync while the token fires; the beat wait
            // itself never throws.
        }
    }

    // Single choke point for handler execution so both ACK modes emit the consumer receive span.
    private async Task ExecuteHandlerAsync(DbTransportDelivery delivery, CancellationToken cancellationToken)
    {
        using var activity = AsyncResponseDiagnostics.StartActivity(
            _receiveActivityName,
            System.Diagnostics.ActivityKind.Consumer);
        activity?.SetTag("asyncresponse.transport", _transportTag);
        activity?.SetTag(_roleTagName, _role.ToString());
        activity?.SetTag(_ackModeTagName, _subscriberOptions.AckMode.ToString());
        activity?.SetTag("messaging.system", _transportTag);
        activity?.SetTag("messaging.destination.name", delivery.Queue);
        activity?.SetTag("messaging.message.id", delivery.Id.ToString());
        activity?.SetTag("messaging.message.delivery_attempt", delivery.Attempt);

        if (delivery.Headers.TryGetValue(_options.CorrelationIdHeader, out var correlationId))
            AsyncResponseDiagnostics.SetCorrelationId(activity, correlationId);

        try
        {
            await _handler(delivery, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not DurableFlowInterruptedException)
        {
            // A stopping host's hand-back (DurableFlowInterruptedException) is not a handler error:
            // HandleAsync leaves the claim unsettled for redelivery, so an error-status span only
            // put a failure on every trace of a routine shutdown.
            AsyncResponseDiagnostics.SetError(activity, ex);
            throw;
        }
    }

    /// <summary>
    /// The cancelled lease-renewal heartbeat is NOT joined before settlement. Every settlement (ack,
    /// NAK, dead-letter) is fenced by <c>lock_id</c> in all three stores, so a beat still in flight
    /// is a no-op against it — and a join held the ack behind a slow renew (which then pinned
    /// <see cref="CancellationToken.None"/> for its connect and command; each attempt is now bounded
    /// by the beat interval): a handler that had already SUCCEEDED waited on a degraded database until the lease it was trying to extend had
    /// lapsed, and the row was claimed and run again before its ack went out. (While the subscriber
    /// is stopping the wait was also up to <c>LockTimeout</c> of the host's stop budget, a term no
    /// shutdown validator sums.) The loop swallows its own faults; observe defensively and let the
    /// beat finish on its own.
    /// </summary>
    private static void ObserveRenewal(Task renewalTask)
        => _ = renewalTask.ContinueWith(
            static task => _ = task.Exception,
            CancellationToken.None,
            TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);

    private async Task HandleEarlyAckAsync(DbTransportDelivery delivery, CancellationToken cancellationToken)
    {
        if (!_backgroundQueue!.Writer.TryWrite(delivery))
        {
            // Saturated: wait for a worker to free a slot instead of NAKing. The subscriber loop
            // treats every claimed row as progress and re-claims immediately, so NAK-on-full spins
            // at full database rate — one claim plus one NAK round trip per queued row, each NAK
            // burning an attempt (and on PostgreSQL notifying the whole fleet to come do the same).
            // Parking here pauses the claim loop, which is the actual backpressure (mirrors the
            // RabbitMQ/Kafka/NATS pause); the queue is built with FullMode.Wait for exactly this.
            _logger.LogDebug("Background queue full for {Provider} {Role}; pausing the claim loop until capacity frees.", _providerName, _role);

            // The park is unbounded by design, but the claim's lease is not — and in early-ACK
            // mode the inline path's heartbeat never runs, so nothing renews it. A park longer
            // than LockTimeout would let the lock lapse, a competing subscriber re-claim and run
            // the row, and this subscriber enqueue its own copy once the park completes: one job,
            // two concurrent executions, with the second ack's lock_id fence failing silently.
            // Arm the same fenced heartbeat as the inline path for exactly the park's duration.
            //
            // The park also listens for the heartbeat's "lease lost": a renew that no longer matches
            // the lock_id fence means a peer re-claimed the row (the database was unreachable from
            // here for most of a LockTimeout, a long GC or VM pause). Enqueueing it anyway once
            // capacity freed ran a job a peer already owned — or had finished — a second time,
            // with this ack's fence then failing silently. Drop it instead: not ours any more.
            using var renewalCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            using var leaseLost = new CancellationTokenSource();
            using var parkCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, leaseLost.Token);
            var renewalTask = RenewLeaseLoopAsync(delivery, renewalCancellation.Token, leaseLost);
            try
            {
                await _backgroundQueue.Writer.WriteAsync(delivery, parkCancellation.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (leaseLost.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
            {
                // No NAK either: the fence is gone, so it would no-op against the new owner's claim.
                _logger.LogDebug(
                    "{Provider} message {MessageId} on queue {Queue} ({Role}) lost its lease while parked on a full background queue; dropped without enqueueing it.",
                    _providerName,
                    delivery.Id,
                    delivery.Queue,
                    _role);
                return;
            }
            catch (Exception ex) when (ex is OperationCanceledException or ChannelClosedException)
            {
                // Subscriber stopping or dispatcher draining while parked: the delivery was never
                // enqueued, so release it promptly; if the NAK itself fails the lease lapses to
                // the same effect.
                try
                {
                    await delivery.NakAsync(_subscriberOptions.RedeliveryDelay).ConfigureAwait(false);
                }
                catch (Exception nakException)
                {
                    _logger.LogWarning(
                        nakException,
                        "Failed to NAK {Provider} message {MessageId} on queue {Queue} ({Role}) while stopping; the lease will lapse and the {Unit} will be redelivered.",
                        _providerName,
                        delivery.Id,
                        delivery.Queue,
                        _role,
                        _unitNoun);
                }

                return;
            }
            finally
            {
                renewalCancellation.Cancel();
                ObserveRenewal(renewalTask);
            }
        }

        // Same rule as the post-handler ACK above: the delivery is already owned by a background
        // worker, so an ACK failure must not escape and tear down the subscriber — that would
        // drain the workers (running the handler) while the un-ACKed row is re-claimed and run
        // again. Swallow and log; the lease lapses and at-least-once redelivery applies.
        try
        {
            await delivery.AckAsync().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "Failed to ACK {Provider} message {MessageId} on queue {Queue} ({Role}) after enqueueing it for background execution; the lease will lapse and the {Unit} may be redelivered.",
                _providerName,
                delivery.Id,
                delivery.Queue,
                _role,
                _unitNoun);
        }
    }

    private async Task BackgroundWorkerLoopAsync(CancellationToken cancellationToken)
    {
        // Token-less ReadAllAsync: on shutdown the queue is completed and fully drained, so every
        // already-ACKed queue item is accounted for instead of being silently dropped; each
        // failure is dead-lettered and surfaced below.
        await foreach (var delivery in _backgroundQueue!.Reader.ReadAllAsync().ConfigureAwait(false))
        {
            // Once the drain budget has lapsed, STOP executing (Redis/Pub-Sub parity). The token
            // below cannot stop the real handler — it is `_ingress.HandleWorkerMessageAsync(payload)`,
            // whose target takes no CancellationToken — so past the budget the loop kept starting
            // fresh work beyond the HostShutdownTimeout the options size, and every entry still
            // queued at process exit vanished with no record (its queue row was deleted by the
            // early ACK, so nothing redelivers it). Route the rest through the same
            // dead-letter/OnBackgroundFailure path instead of losing them silently.
            if (cancellationToken.IsCancellationRequested)
            {
                var lapsed = new OperationCanceledException(
                    "The ACK-after-enqueue drain budget lapsed before this already-ACKed message was handled.");

                _logger.LogWarning(
                    "{Provider} background handler for already-ACKed message {MessageId} on queue {Queue} ({Role}) was not started: the drain budget had lapsed. Dead-lettering and surfacing via OnBackgroundFailure.",
                    _providerName,
                    delivery.Id,
                    delivery.Queue,
                    _role);

                if (!await DeadLetterSwallowingFailureAsync(delivery, lapsed, deleteOriginal: false).ConfigureAwait(false))
                {
                    _logger.LogError(
                        "Failed to dead-letter undrained {Provider} message {MessageId} on queue {Queue} ({Role}); the loss is only observable via logs and OnBackgroundFailure.",
                        _providerName,
                        delivery.Id,
                        delivery.Queue,
                        _role);
                }

                await InvokeBackgroundFailureAsync(delivery, lapsed).ConfigureAwait(false);
                continue;
            }

            try
            {
                await ExecuteHandlerAsync(delivery, cancellationToken).ConfigureAwait(false);
            }
            catch (DurableFlowInterruptedException ex)
            {
                // The flow engine handed the job back because the host is stopping — not a handler
                // failure — but the early ACK already deleted its queue row and nothing redelivers
                // it. Dead-letter a copy under its own reason so the wake-up keeps a record
                // (replaying it is safe: the run resumes from its last checkpoint) and surface it
                // (Kafka/RabbitMQ/Redis/NATS parity). A Warning when the copy was written; an Error
                // when it was not (dead-lettering disabled or the burial failed), since the wake-up
                // is then lost unless OnBackgroundFailure records it.
                var handedBack = new DurableFlowInterruptedException($"{HandedBackAfterCommitReason}: {ex.Message}", ex);
                if (await DeadLetterSwallowingFailureAsync(delivery, handedBack, deleteOriginal: false).ConfigureAwait(false))
                {
                    _logger.LogWarning(
                        ex,
                        "{Provider} background handler for already-ACKed message {MessageId} on queue {Queue} ({Role}) was handed back by the flow engine because the host is stopping; the early ACK already removed its row. Dead-lettered a copy ({Reason}) and surfacing via OnBackgroundFailure.",
                        _providerName,
                        delivery.Id,
                        delivery.Queue,
                        _role,
                        HandedBackAfterCommitReason);
                }
                else
                {
                    _logger.LogError(
                        ex,
                        "{Provider} background handler for already-ACKed message {MessageId} on queue {Queue} ({Role}) was handed back by the flow engine because the host is stopping, and no dead-letter copy could be written (dead-lettering disabled or the burial failed): the wake-up is lost unless OnBackgroundFailure records it — resume the flow explicitly.",
                        _providerName,
                        delivery.Id,
                        delivery.Queue,
                        _role);
                }

                await InvokeBackgroundFailureAsync(delivery, ex).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "{Provider} background handler failed for {Role} on queue {Queue} after early ACK.", _providerName, _role, delivery.Queue);
                if (!await DeadLetterSwallowingFailureAsync(delivery, ex, deleteOriginal: false).ConfigureAwait(false))
                {
                    _logger.LogError(
                        "Failed to dead-letter already-ACKed {Provider} message {MessageId} on queue {Queue} ({Role}); the failure is only observable via logs and OnBackgroundFailure.",
                        _providerName,
                        delivery.Id,
                        delivery.Queue,
                        _role);
                }

                await InvokeBackgroundFailureAsync(delivery, ex).ConfigureAwait(false);
            }
        }
    }

    private async Task HandleFailureAsync(DbTransportDelivery delivery, Exception exception)
    {
        var maxAttempts = _subscriberOptions.MaxDeliveryAttempts;
        if (maxAttempts > 0 && delivery.Attempt >= maxAttempts)
        {
            _logger.LogError(
                exception,
                "{Provider} message on queue {Queue} ({Role}) failed after {Attempts} attempts; dead-lettering.",
                _providerName,
                delivery.Queue,
                _role,
                delivery.Attempt);

            // CancellationToken.None like every other settlement in this file: burying a poison
            // row must not be abandoned half-done by a shutdown — with the stopping token, a
            // handler failing on its LAST attempt during a stop had the burial aborted (the
            // store's connection/transaction calls throw on the cancelled token) and the row was
            // NAKed back instead of dead-lettered.
            var deadLettered = await DeadLetterSwallowingFailureAsync(delivery, exception, deleteOriginal: true).ConfigureAwait(false);
            if (!deadLettered)
            {
                _logger.LogWarning(exception, "{Provider} dead-letter publish failed for queue {Queue} ({Role}); releasing for retry.", _providerName, delivery.Queue, _role);
                await NakSwallowingFailureAsync(delivery).ConfigureAwait(false);
            }
        }
        else
        {
            _logger.LogWarning(
                exception,
                "{Provider} message on queue {Queue} ({Role}) failed on attempt {Attempt}; releasing for redelivery.",
                _providerName,
                delivery.Queue,
                _role,
                delivery.Attempt);
            await NakSwallowingFailureAsync(delivery).ConfigureAwait(false);
        }
    }

    /// <summary>The dead-letter reason prefix of an early-ACK job the flow engine handed back at host stop.</summary>
    internal const string HandedBackAfterCommitReason = "handed_back_after_commit";

    // Burial with the same containment rule as NakSwallowingFailureAsync below: the delivery
    // contract says DeadLetterAsync returns false rather than throwing, but the stores'
    // DeadLetterEnabled = false branch runs its ack OUTSIDE their guarded region, so a transient
    // DB failure there escaped as a throw — out of HandleAsync, tearing the subscriber down (and,
    // from the drain loop, killing the background worker). A burial that throws is a burial that
    // failed: report false and let the caller's NAK / lease-lapse path apply.
    private async Task<bool> DeadLetterSwallowingFailureAsync(DbTransportDelivery delivery, Exception exception, bool deleteOriginal)
    {
        try
        {
            return await delivery.DeadLetterAsync(exception, deleteOriginal, CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "Failed to dead-letter {Provider} message {MessageId} on queue {Queue} ({Role}); treating the burial as failed.",
                _providerName,
                delivery.Id,
                delivery.Queue,
                _role);
            return false;
        }
    }

    // Same rule as the post-handler ACK above: the handler's outcome is already decided, so a
    // transient NAK failure must not escape HandleAsync and tear down the subscriber — that would
    // dispose the dispatcher mid-flight and dead-letter unrelated already-ACKed background work on
    // the way down. Swallow and log; the claim's lease lapses on its own and at-least-once
    // redelivery applies either way.
    private async Task NakSwallowingFailureAsync(DbTransportDelivery delivery)
    {
        try
        {
            await delivery.NakAsync(_subscriberOptions.RedeliveryDelay).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "Failed to NAK {Provider} message {MessageId} on queue {Queue} ({Role}) after a failed handler; the lease will lapse and the {Unit} will be redelivered.",
                _providerName,
                delivery.Id,
                delivery.Queue,
                _role,
                _unitNoun);
        }
    }

    private async Task InvokeBackgroundFailureAsync(DbTransportDelivery delivery, Exception exception)
    {
        if (_subscriberOptions.OnBackgroundFailure is null)
            return;

        try
        {
            delivery.Headers.TryGetValue(_options.CorrelationIdHeader, out var correlationId);
            var context = new DbBackgroundFailureContext(delivery.Queue, _role.ToString(), delivery.Attempt, correlationId, exception);
            await _subscriberOptions.OnBackgroundFailure(context).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "{Provider} OnBackgroundFailure callback threw for {Role}.", _providerName, _role);
        }
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_backgroundQueue is null)
            return;

        _backgroundQueue.Writer.TryComplete();

        // BackgroundDrainTimeout is the whole spend the shutdown-budget validator sums for this
        // dispatcher, so it is split rather than exceeded: most of it lets queued and running
        // handlers finish, and the rest is RESERVED for the post-lapse routing the worker loop
        // performs once cancelled (dead-letter + OnBackgroundFailure for every entry still
        // queued). That routing used to be fire-and-forget with no budget at all: DisposeAsync
        // returned, the subscriber and then the host finished stopping, and the already-ACKed
        // entries the workers were only starting to bury vanished with no record — the very
        // loss docs/transport-semantics.md promises this path prevents.
        var routingReserve = TimeSpan.FromTicks(_subscriberOptions.BackgroundDrainTimeout.Ticks / 4);
        var drainBudget = _subscriberOptions.BackgroundDrainTimeout - routingReserve;
        try
        {
            await Task.WhenAll(_backgroundWorkers!).WaitAsync(drainBudget).ConfigureAwait(false);
            _backgroundCts!.Dispose();
        }
        catch (TimeoutException)
        {
            _logger.LogWarning("{Provider} background handlers for {Role} did not drain within {Timeout}; dead-lettering the entries still queued.", _providerName, _role, drainBudget);
            await _backgroundCts!.CancelAsync().ConfigureAwait(false);

            try
            {
                await Task.WhenAll(_backgroundWorkers!).WaitAsync(routingReserve).ConfigureAwait(false);
            }
            catch (TimeoutException)
            {
                _logger.LogError(
                    "{Provider} background workers for {Role} did not finish dead-lettering the undrained entries within the reserved {Reserve}; entries still queued at process exit are lost (their rows were deleted by the early ACK).",
                    _providerName,
                    _role,
                    routingReserve);

                // The workers are still running and observe _backgroundCts.Token inside ReadAllAsync, so disposing
                // it now would throw ObjectDisposedException inside them. Dispose once they actually finish, off
                // the shutdown path, so the source is not leaked either.
                _ = Task.WhenAll(_backgroundWorkers!).ContinueWith(
                    _ => _backgroundCts.Dispose(),
                    CancellationToken.None,
                    TaskContinuationOptions.ExecuteSynchronously,
                    TaskScheduler.Default);
                return;
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "{Provider} background worker drain for {Role} ended with an error.", _providerName, _role);
            }

            // WhenAll completed one way or the other, so every worker has finished.
            _backgroundCts.Dispose();
        }
        catch (Exception ex)
        {
            // WhenAll only completes once every worker has finished, so the source is safe to dispose here.
            _logger.LogDebug(ex, "{Provider} background worker drain for {Role} ended with an error.", _providerName, _role);
            _backgroundCts!.Dispose();
        }
    }
}

/// <summary>
/// Extracts the AsyncResponse correlation id from the queue item's metadata first, then from the
/// JSON response body via configured paths (walked by the shared <see cref="CorrelationIdJsonPaths"/>,
/// same as the broker transports). Shared verbatim by the three database transports — the header
/// name and JSON paths both come from the aliased options type.
/// </summary>
internal static class DbCorrelationIdExtractor
{
    public static string? Extract(
        IReadOnlyDictionary<string, string>? headers,
        string messageJson,
        DbTransportOptions options)
    {
        var headerName = DbTransportOptionsValidator.Required(options.CorrelationIdHeader, nameof(options.CorrelationIdHeader));
        if (headers is not null && headers.TryGetValue(headerName, out var headerValue) && !string.IsNullOrWhiteSpace(headerValue))
            return headerValue;

        return CorrelationIdJsonPaths.Extract(messageJson, options.CorrelationIdJsonPaths);
    }
}

/// <summary>
/// The dead-letter retention prune every database transport runs after a publish: throttled to
/// once per <see cref="Throttle"/> per store on the monotonic clock, drained in bounded batches
/// under a budget, and guarded so it can never fail the publish it follows. The three stores had
/// each carried their own copy, and the copies drifted twice: only SQL Server was bounded (one
/// batch per window, a hard ceiling a poison storm outgrew), only PostgreSQL was guarded (the other
/// two reported a COMMITTED publish as failed when the prune threw, and the caller's retry ran the
/// job twice). Each store now supplies only its one-batch delete.
/// </summary>
internal static class DbDeadLetterPrune
{
    /// <summary>Minimum spacing between two prunes of one store, whatever the publish rate.</summary>
    public static readonly TimeSpan Throttle = TimeSpan.FromMinutes(1);

    /// <summary>
    /// Runs the prune when <paramref name="retention"/> is set and the throttle window has elapsed;
    /// never throws (see <see cref="AsyncResponse.Internal.OpportunisticPrune.DrainQuietlyAsync"/>).
    /// </summary>
    /// <param name="lastStamp">The store's throttle stamp (monotonic; 0 = never pruned).</param>
    /// <param name="retention">The configured <c>DeadLetterRetention</c>; <c>null</c> keeps dead letters forever.</param>
    /// <param name="pruneBatch">Deletes one bounded batch of over-retention dead letters and returns the count.</param>
    /// <param name="logger">The store's logger, when DI supplied one.</param>
    /// <param name="providerName">Provider display name for the log subject.</param>
    /// <param name="cancellationToken">The publisher's token.</param>
    public static Task RunIfDueAsync(
        ref long lastStamp,
        TimeSpan? retention,
        Func<TimeSpan, CancellationToken, Task<int>> pruneBatch,
        ILogger? logger,
        string providerName,
        CancellationToken cancellationToken)
    {
        if (retention is not { } keep || !AsyncResponse.Internal.OpportunisticPrune.ShouldRun(ref lastStamp, Throttle))
            return Task.CompletedTask;

        return AsyncResponse.Internal.OpportunisticPrune.DrainQuietlyAsync(
            token => pruneBatch(keep, token),
            logger,
            $"{providerName} transport dead-letter prune",
            cancellationToken);
    }
}

/// <summary>
/// Materializes a claimed queue item's <c>headers_json</c> without rejecting ANY content the
/// column can legally hold. This runs after the claim already committed <c>attempts+1</c>/<c>lock_id</c>
/// and before any delivery object exists, so a throw here (a wrong-typed value, a non-object root,
/// malformed text in an unchecked column) could never reach the failure handler or dead-letter:
/// an unkillable poison row that tears down the subscriber on every re-claim. Instead, string
/// values are taken as-is, scalars keep their raw JSON text (culture-free by construction),
/// object/array values keep their raw JSON so correlation extraction still sees a usable string,
/// nulls are skipped — as is a header whose name or string value cannot be transcoded (an escaped
/// lone surrogate) — and anything unusable degrades to no headers — a genuinely poison message
/// then fails in the handler and flows through the NORMAL dead-letter path. Keys differing only
/// in case (legal JSON from foreign producers) are last-wins, matching the ASB/SQS receive
/// adapters.
/// </summary>
internal static class DbTransportHeaders
{
    public static IReadOnlyDictionary<string, string> Materialize(string json)
    {
        JsonDocument document;
        try
        {
            document = JsonDocument.Parse(json);
        }
        catch (Exception ex) when (ex is JsonException or ArgumentException)
        {
            // ArgumentException: a RAW lone surrogate (legal in SQL Server's nvarchar(max), which
            // stores UTF-16 code units unvalidated) cannot even be transcoded to the UTF-8 that
            // Parse(string) reads, and that throws before any JSON is seen.
            return Empty;
        }

        using (document)
        {
            if (document.RootElement.ValueKind is not JsonValueKind.Object)
                return Empty;

            var headers = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            foreach (var property in document.RootElement.EnumerateObject())
            {
                try
                {
                    var value = property.Value.ValueKind switch
                    {
                        JsonValueKind.String => property.Value.GetString(),
                        JsonValueKind.Null or JsonValueKind.Undefined => null,
                        _ => property.Value.GetRawText()
                    };
                    if (value is not null)
                        headers[property.Name] = value;
                }
                catch (InvalidOperationException)
                {
                    // An ESCAPED lone surrogate ("\ud800") parses — it is well-formed JSON — but has
                    // no UTF-16 string form, so GetString/Name throw InvalidOperationException, not
                    // the JsonException guarded above: the same after-the-claim, before-any-delivery
                    // throw this type exists to prevent. The header is unusable; skip it and keep
                    // the rest.
                }
            }

            return headers;
        }
    }

    private static readonly IReadOnlyDictionary<string, string> Empty =
        new Dictionary<string, string>(0, StringComparer.OrdinalIgnoreCase);
}
