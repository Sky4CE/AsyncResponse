using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using System.Diagnostics;
using System.Threading.Channels;

namespace AsyncResponse.Transports.Redis;

internal enum RedisSubscriberRole
{
    Worker,
    ResponseIngress
}

internal enum RedisDispatchOutcome
{
    /// <summary>The entry was handled, ACKed, or dead-lettered. Counts as progress for the poll loop.</summary>
    Processed,

    /// <summary>The entry could not be accepted right now (background queue full) and was left pending for retry.</summary>
    Deferred,

    /// <summary>
    /// The flow engine handed the delivery back because the host is stopping
    /// (<see cref="DurableFlowInterruptedException"/>): it was left pending, unsettled, for
    /// redelivery — not progress, and nothing further in the batch should start.
    /// </summary>
    HandedBack
}

internal sealed record RedisStreamDelivery(
    RedisKey Stream,
    RedisValue ConsumerGroup,
    RedisValue MessageId,
    string Payload,
    string? CorrelationId,
    int Attempt,
    StreamEntry Entry);

internal abstract class RedisMessageDispatcher : IAsyncDisposable
{
    private readonly Func<RedisStreamDelivery, CancellationToken, Task> _handler;
    private readonly RedisSubscriberOptions _subscriberOptions;
    private readonly IRedisStreamDatabase _database;
    private readonly RedisTransportKeySchema _keys;
    private readonly string _stream;
    private readonly string _consumerGroup;
    private readonly RedisSubscriberRole _role;

    /// <summary>Runs the RedisMessageDispatcher operation.</summary>
    protected RedisMessageDispatcher(
        Func<RedisStreamDelivery, CancellationToken, Task> handler,
        IRedisStreamDatabase database,
        RedisAsyncResponseTransportOptions transportOptions,
        RedisSubscriberOptions subscriberOptions,
        ILogger logger,
        RedisKey stream,
        RedisValue consumerGroup,
        RedisSubscriberRole role)
    {
        _handler = handler;
        _database = database;
        TransportOptions = transportOptions;
        _subscriberOptions = subscriberOptions;
        _keys = new RedisTransportKeySchema(transportOptions);
        Logger = logger;
        _stream = stream.ToString();
        _consumerGroup = consumerGroup.ToString();
        _role = role;
    }

    protected RedisAsyncResponseTransportOptions TransportOptions { get; }
    protected ILogger Logger { get; }

    protected int MaxDeliveryAttempts => _subscriberOptions.MaxDeliveryAttempts;

    /// <summary>Creates the configured dispatcher.</summary>
    public static RedisMessageDispatcher Create(
        Func<RedisStreamDelivery, CancellationToken, Task> handler,
        IRedisStreamDatabase database,
        RedisAsyncResponseTransportOptions transportOptions,
        RedisSubscriberOptions subscriberOptions,
        ILogger logger,
        RedisKey stream,
        RedisValue consumerGroup,
        RedisSubscriberRole role)
    {
        ValidateOptions(transportOptions, subscriberOptions, role);

        if (subscriberOptions.AckMode is RedisAckMode.AckAfterEnqueue)
        {
            return new QueuedRedisMessageDispatcher(
                handler,
                database,
                transportOptions,
                subscriberOptions,
                logger,
                stream,
                consumerGroup,
                role);
        }

        return new AwaitingRedisMessageDispatcher(
            handler,
            database,
            transportOptions,
            subscriberOptions,
            logger,
            stream,
            consumerGroup,
            role);
    }

    /// <summary>Validates the supplied options.</summary>
    public static void ValidateOptions(
        RedisAsyncResponseTransportOptions transportOptions,
        RedisSubscriberOptions subscriberOptions,
        RedisSubscriberRole role)
    {
        RedisTransportOptionsValidator.ValidateCommon(transportOptions);

        var optionPath = role is RedisSubscriberRole.Worker
            ? $"{nameof(RedisAsyncResponseTransportOptions)}.{nameof(RedisAsyncResponseTransportOptions.WorkerSubscriber)}"
            : $"{nameof(RedisAsyncResponseTransportOptions)}.{nameof(RedisAsyncResponseTransportOptions.ResponseSubscriber)}";

        if (subscriberOptions.BatchSize <= 0)
            throw new InvalidOperationException($"{optionPath}.{nameof(RedisSubscriberOptions.BatchSize)} must be positive.");
        // EmptyPollDelay arms the idle Task.Delay (timer ceiling). PendingMessageMinIdleTime is
        // the server-side XAUTOCLAIM min-idle in milliseconds, but it ALSO arms the in-process
        // idle-reset heartbeat's Task.Delay at one third of its value, so its real sink is the
        // timer ceiling too — under the persistence bound a legal 200-day value passed validation
        // and then killed every batch with ArgumentOutOfRangeException from the heartbeat's delay.
        // PendingClaimInterval is only compared with elapsed monotonic time and keeps the
        // persistence bound.
        AsyncResponseChannelOptions.EnsureTimerBacked(subscriberOptions.EmptyPollDelay, optionPath, nameof(RedisSubscriberOptions.EmptyPollDelay));
        AsyncResponseChannelOptions.EnsureTimerBacked(subscriberOptions.PendingMessageMinIdleTime, optionPath, nameof(RedisSubscriberOptions.PendingMessageMinIdleTime));
        AsyncResponseChannelOptions.EnsurePersistedTtl(subscriberOptions.PendingClaimInterval, optionPath, nameof(RedisSubscriberOptions.PendingClaimInterval));
        if (subscriberOptions.PendingClaimBatchSize <= 0)
            throw new InvalidOperationException($"{optionPath}.{nameof(RedisSubscriberOptions.PendingClaimBatchSize)} must be positive.");
        if (subscriberOptions.MaxDeliveryAttempts < 0)
            throw new InvalidOperationException($"{optionPath}.{nameof(RedisSubscriberOptions.MaxDeliveryAttempts)} cannot be negative.");

        switch (subscriberOptions.AckMode)
        {
            case RedisAckMode.AckAfterHandlerCompletes:
                return;

            case RedisAckMode.AckAfterEnqueue:
                if (subscriberOptions.BackgroundWorkerCount <= 0)
                {
                    throw new InvalidOperationException(
                        $"{optionPath}.{nameof(RedisSubscriberOptions.BackgroundWorkerCount)} must be explicitly configured " +
                        $"when {nameof(RedisSubscriberOptions.AckMode)} is {nameof(RedisAckMode.AckAfterEnqueue)}.");
                }

                if (subscriberOptions.BackgroundQueueCapacity <= 0)
                {
                    throw new InvalidOperationException(
                        $"{optionPath}.{nameof(RedisSubscriberOptions.BackgroundQueueCapacity)} must be explicitly configured " +
                        $"when {nameof(RedisSubscriberOptions.AckMode)} is {nameof(RedisAckMode.AckAfterEnqueue)}.");
                }

                AsyncResponseChannelOptions.EnsureTimerBacked(subscriberOptions.BackgroundDrainTimeout, optionPath, nameof(RedisSubscriberOptions.BackgroundDrainTimeout));

                // Redis subscribers spend only the background drain at shutdown; the read loop
                // stops with the host token and the multiplexer teardown is not separately bounded.
                ShutdownBudgetValidator.Validate(
                    "Redis",
                    $"{nameof(RedisAsyncResponseTransportOptions)}.{nameof(RedisAsyncResponseTransportOptions.HostShutdownTimeout)}",
                    transportOptions.HostShutdownTimeout,
                    ($"{optionPath}.{nameof(RedisSubscriberOptions.BackgroundDrainTimeout)}", subscriberOptions.BackgroundDrainTimeout));

                return;

            default:
                throw new InvalidOperationException(
                    $"{optionPath}.{nameof(RedisSubscriberOptions.AckMode)} has unsupported value '{subscriberOptions.AckMode}'.");
        }
    }

    /// <summary>Handles the delivered message.</summary>
    public abstract Task<RedisDispatchOutcome> HandleAsync(
        RedisStreamDelivery delivery,
        CancellationToken subscriberCancellationToken);

    /// <summary>
    /// Whether the dispatcher can accept more deliveries right now. Awaiting dispatchers always can
    /// (handlers run inline); the queued dispatcher returns <c>false</c> while its bounded queue is
    /// saturated so the subscriber stops pulling new entries into the pending-entry list instead of
    /// busy-reading and rejecting them.
    /// </summary>
    public virtual bool CanAcceptMore => true;

    /// <summary>
    /// How many entries the dispatcher can take right now without deferring any (ASB/SQS parity);
    /// unbounded for the awaiting dispatcher. The subscriber clamps every read and claim to it.
    /// </summary>
    public virtual int FreeCapacity => int.MaxValue;

    /// <summary>Releases resources held by this instance.</summary>
    public virtual ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private int _handBackSignalled;

    /// <summary>
    /// Whether the flow engine has handed a delivery back (<see cref="DurableFlowInterruptedException"/>)
    /// — inline, or in an early-ACK worker. That only happens because the host is stopping, and
    /// it arrives before this subscriber's token (the engine reacts to ApplicationStopping, which
    /// fires first), so the subscriber stops reading and claiming from here on. Latched: the host
    /// does not come back from a stop.
    /// </summary>
    public bool HandBackSignalled => Volatile.Read(ref _handBackSignalled) != 0;

    /// <summary>Latches <see cref="HandBackSignalled"/>.</summary>
    protected void SignalHandBack() => Volatile.Write(ref _handBackSignalled, 1);

    /// <summary>Runs the ExecuteHandlerAsync operation.</summary>
    protected async Task ExecuteHandlerAsync(
        RedisStreamDelivery delivery,
        CancellationToken cancellationToken,
        bool logFailures = true)
    {
        using var activity = AsyncResponseDiagnostics.StartActivity(
            "asyncresponse.redis.receive",
            ActivityKind.Consumer,
            delivery.CorrelationId);
        activity?.SetTag("asyncresponse.transport", "redis");
        activity?.SetTag("asyncresponse.redis.role", _role.ToString());
        activity?.SetTag("asyncresponse.redis.ack_mode", _subscriberOptions.AckMode.ToString());
        activity?.SetTag("asyncresponse.redis.delivery_attempt", delivery.Attempt);
        activity?.SetTag("messaging.system", "redis");
        activity?.SetTag("messaging.destination.name", _stream);
        activity?.SetTag("messaging.message.id", delivery.MessageId.ToString());

        try
        {
            await _handler(delivery, cancellationToken).ConfigureAwait(false);
        }
        catch (DurableFlowInterruptedException)
        {
            // Not a failure: the flow engine hands the delivery back because the host is stopping.
            // The caller settles it as a hand-back, with no failure log and no error span.
            throw;
        }
        catch (Exception ex)
        {
            if (logFailures)
            {
                // Guarded: a throwing provider here replaced the handler's failure with its own.
                SafeLog.Try(
                    (Logger, ex, _stream, Id: delivery.MessageId.ToString()),
                    static s => s.Logger.LogError(
                        s.ex,
                        "Redis stream message handling failed for {Stream}/{MessageId}.",
                        s._stream,
                        s.Id));
            }

            AsyncResponseDiagnostics.SetError(activity, ex);
            throw;
        }
    }

    /// <summary>Acknowledges the delivered message.</summary>
    protected Task AckAsync(RedisStreamDelivery delivery, CancellationToken cancellationToken)
        => _database.StreamAcknowledgeAsync(
            delivery.Stream,
            delivery.ConsumerGroup,
            delivery.MessageId,
            cancellationToken);

    /// <summary>Runs the AlreadyExceededDeliveryAttempts operation.</summary>
    protected bool AlreadyExceededDeliveryAttempts(RedisStreamDelivery delivery)
        => MaxDeliveryAttempts > 0 && delivery.Attempt > MaxDeliveryAttempts;

    /// <summary>Runs the ReachedDeliveryAttempts operation.</summary>
    protected bool ReachedDeliveryAttempts(RedisStreamDelivery delivery)
        => MaxDeliveryAttempts > 0 && delivery.Attempt >= MaxDeliveryAttempts;

    /// <summary>
    /// Writes the dead-letter copy of <paramref name="delivery"/>. Returns <c>false</c> without
    /// writing anything when dead-lettering is disabled; a failed write throws.
    /// </summary>
    private async Task<bool> WriteDeadLetterAsync(
        RedisStreamDelivery delivery,
        Exception exception,
        string reason,
        CancellationToken cancellationToken)
    {
        if (!TransportOptions.DeadLetterEnabled)
            return false;

        var fields = new[]
        {
            new NameValueEntry("sourceStream", delivery.Stream.ToString()),
            new NameValueEntry("consumerGroup", delivery.ConsumerGroup.ToString()),
            new NameValueEntry("subscriberRole", _role.ToString()),
            new NameValueEntry("messageId", delivery.MessageId.ToString()),
            new NameValueEntry("correlationId", delivery.CorrelationId ?? string.Empty),
            new NameValueEntry("attempt", delivery.Attempt),
            new NameValueEntry("reason", reason),
            new NameValueEntry("exceptionType", exception.GetType().FullName!),
            new NameValueEntry("exceptionMessage", exception.Message),
            new NameValueEntry("payload", delivery.Payload),
            new NameValueEntry("occurredAtUtc", DateTimeOffset.UtcNow.ToString("O"))
        };

        await _database.StreamAddAsync(
            _keys.DeadLetterStream,
            fields,
            TransportOptions.DeadLetterStreamMaxLength,
            TransportOptions.UseApproximateStreamTrimming,
            cancellationToken).ConfigureAwait(false);
        return true;
    }

    /// <summary>
    /// Moves a still-pending delivery to dead-letter storage and acknowledges it. With
    /// dead-lettering disabled the XACK discards the entry for good with no copy anywhere: that
    /// drop is logged at Error (NATS parity), once the XACK has settled it — silent, a poison
    /// entry past the attempt cap vanished without a trace.
    /// </summary>
    protected async Task DeadLetterAndAckAsync(
        RedisStreamDelivery delivery,
        Exception exception,
        string reason,
        CancellationToken cancellationToken)
    {
        var copied = await WriteDeadLetterAsync(delivery, exception, reason, cancellationToken).ConfigureAwait(false);
        await AckAsync(delivery, cancellationToken).ConfigureAwait(false);
        if (!copied)
        {
            SafeLog.Try(
                (Logger, exception, Id: delivery.MessageId.ToString(), Stream: delivery.Stream.ToString(), reason),
                static s => s.Logger.LogError(
                    s.exception,
                    "Redis message {MessageId} on {Stream} was dropped ({Reason}): dead-lettering is disabled, so it was ACKed with no dead-letter copy.",
                    s.Id,
                    s.Stream,
                    s.reason));
        }
    }

    /// <summary>
    /// Dead-letters an entry the early ACK already settled — Redis will never redeliver it, so the
    /// copy is its only durable record — then XACKs it again, which only matters when the
    /// enqueue-time XACK failed. Never throws. Returns whether a copy was written: <c>false</c>
    /// when dead-lettering is disabled or the write failed (a failed write skips the XACK, so an
    /// entry whose enqueue-time XACK also failed stays pending and is redelivered).
    /// </summary>
    protected async Task<bool> TryDeadLetterAfterAckAsync(
        RedisStreamDelivery delivery,
        Exception exception,
        string reason,
        CancellationToken cancellationToken)
    {
        bool copied;
        try
        {
            copied = await WriteDeadLetterAsync(delivery, exception, reason, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception deadLetterException)
        {
            SafeLog.Try(
                (Logger, deadLetterException, Id: delivery.MessageId.ToString(), Stream: delivery.Stream.ToString(), reason),
                static s => s.Logger.LogError(
                    s.deadLetterException,
                    "Failed to dead-letter already-ACKed Redis message {MessageId} on {Stream} ({Reason}).",
                    s.Id,
                    s.Stream,
                    s.reason));
            return false;
        }

        try
        {
            await AckAsync(delivery, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ackException)
        {
            SafeLog.Try(
                (Logger, ackException, Id: delivery.MessageId.ToString(), Stream: delivery.Stream.ToString()),
                static s => s.Logger.LogWarning(
                    s.ackException,
                    "Failed to re-ACK already-ACKed Redis message {MessageId} on {Stream} after dead-lettering it; if its enqueue-time ACK failed too, it is redelivered.",
                    s.Id,
                    s.Stream));
        }

        return copied;
    }

    /// <summary>
    /// Dead-letters (when enabled) and ACKs a stream entry that could not be turned into a delivery —
    /// for example a foreign or malformed entry with no payload field, or a tombstone left behind when
    /// trimming evicts a still-pending entry. Without this, such an entry throws before
    /// <see cref="HandleAsync"/> runs, so it is never ACKed: the pending-claim loop re-claims it every
    /// cycle and the subscriber faults and restarts indefinitely while the entry never drains.
    /// </summary>
    public async Task DiscardUnprocessableAsync(
        RedisKey stream,
        RedisValue consumerGroup,
        StreamEntry entry,
        Exception failure,
        CancellationToken cancellationToken)
    {
        if (entry.Id.IsNull)
        {
            // A trimmed-while-pending tombstone (Redis 6.2 answers XCLAIM with a nil entry) carries
            // no id to ACK and no payload to record: sending its null id to XACK is rejected by the
            // client from inside the caller's catch, which replaced the original error, faulted the
            // subscriber, and re-dead-lettered the tombstone every claim cycle. The claim loop
            // drains it by its pending id instead; nothing to settle here.
            SafeLog.Try(
                (Logger, failure, _stream),
                static s => s.Logger.LogDebug(s.failure, "Redis claim on {Stream} returned a trimmed tombstone; skipping it.", s._stream));
            return;
        }

        // With dead-lettering disabled the burial itself logs the drop, with this failure attached.
        if (TransportOptions.DeadLetterEnabled)
        {
            SafeLog.Try(
                (Logger, failure, Id: entry.Id.ToString(), _stream),
                static s => s.Logger.LogError(
                    s.failure,
                    "Redis entry {MessageId} on {Stream} could not be parsed into a delivery; dead-lettering and ACKing it to avoid a poison-message loop.",
                    s.Id,
                    s._stream));
        }

        var delivery = new RedisStreamDelivery(
            stream,
            consumerGroup,
            entry.Id,
            DescribeRawEntry(entry),
            RedisCorrelationIdExtractor.TryReadField(entry, TransportOptions.CorrelationIdField),
            Attempt: 0,
            entry);

        await TryDeadLetterAndAckAsync(delivery, failure, "unparsable_entry").ConfigureAwait(false);
    }

    /// <summary>
    /// Burial that never throws ("a burial that throws is a burial that failed" — DB-transport
    /// parity). Unguarded, a dead-letter XADD that failed — MISCONF/OOM, the adapter's timeout, a
    /// WRONGTYPE on the dead-letter key — escaped past the XACK to the supervisor, which restarted
    /// the subscriber; the pending-claim loop then re-claimed the same entry every cycle, and
    /// every restart abandoned the rest of the claimed batch with bumped counts. Every burial of
    /// an entry that is still pending goes through here (the early-ACK workers guard their own
    /// post-ACK burials). Settlement deliberately ignores cancellation: a shutdown
    /// landing between the XADD and the XACK would leave the entry in the PEL to be reclaimed and
    /// dead-lettered a SECOND time.
    /// </summary>
    protected async Task TryDeadLetterAndAckAsync(RedisStreamDelivery delivery, Exception exception, string reason)
    {
        try
        {
            await DeadLetterAndAckAsync(delivery, exception, reason, CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception deadLetterException)
        {
            SafeLog.Try(
                (Logger, deadLetterException, Id: delivery.MessageId.ToString(), Stream: delivery.Stream.ToString(), reason),
                static s => s.Logger.LogError(
                    s.deadLetterException,
                    "Failed to dead-letter Redis message {MessageId} on {Stream} ({Reason}); the entry stays pending and is reclaimed on the next pending-claim cycle.",
                    s.Id,
                    s.Stream,
                    s.reason));
        }
    }

    private static string DescribeRawEntry(StreamEntry entry)
        => entry.Values is { Length: > 0 }
            ? string.Join("; ", entry.Values.Select(value => $"{value.Name}={value.Value}"))
            : string.Empty;

    /// <summary>Reports an already-ACKed entry to <see cref="RedisSubscriberOptions.OnBackgroundFailure"/>. Never throws.</summary>
    protected ValueTask NotifyBackgroundFailureAsync(RedisStreamDelivery delivery, Exception exception)
        => NotifyBackgroundFailureWithinAsync(delivery, exception, CancellationToken.None);

    /// <summary>
    /// <see cref="NotifyBackgroundFailureAsync"/>, waiting for the callback only until
    /// <paramref name="cancellationToken"/> — the stop-time reserve passes its own, so a slow
    /// callback cannot hold the stop past it; the callback itself runs on, and a late fault is
    /// still logged. Never throws.
    /// </summary>
    protected async ValueTask NotifyBackgroundFailureWithinAsync(
        RedisStreamDelivery delivery,
        Exception exception,
        CancellationToken cancellationToken)
    {
        var callback = _subscriberOptions.OnBackgroundFailure;
        if (callback is null)
            return;

        Task? pending = null;
        try
        {
            var notification = callback(new RedisBackgroundFailureContext(
                _stream,
                _consumerGroup,
                _role.ToString(),
                delivery.MessageId.ToString(),
                delivery.CorrelationId,
                exception));
            if (!cancellationToken.CanBeCanceled || notification.IsCompleted)
            {
                await notification.ConfigureAwait(false);
                return;
            }

            pending = notification.AsTask();
            await pending.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (pending is not null && cancellationToken.IsCancellationRequested)
        {
            SafeLog.Try(
                (Logger, Id: delivery.MessageId.ToString(), _stream),
                static s => s.Logger.LogWarning(
                    "Redis OnBackgroundFailure callback for already-ACKed message {MessageId} on {Stream} did not finish within the stop-time reserve; no longer waiting for it.",
                    s.Id,
                    s._stream));
            _ = pending.ContinueWith(
                static (abandoned, state) =>
                {
                    var (logger, id, stream) = ((ILogger, string, string))state!;
                    SafeLog.Try(
                        (logger, abandoned.Exception, id, stream),
                        static s => s.logger.LogError(
                            s.Exception,
                            "Redis background failure callback failed for already-ACKed message {MessageId} on {Stream}.",
                            s.id,
                            s.stream));
                },
                (Logger, delivery.MessageId.ToString(), _stream),
                CancellationToken.None,
                TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default);
        }
        catch (Exception callbackException)
        {
            SafeLog.Try(
                (Logger, callbackException, Id: delivery.MessageId.ToString(), _stream),
                static s => s.Logger.LogError(
                    s.callbackException,
                    "Redis background failure callback failed for already-ACKed message {MessageId} on {Stream}.",
                    s.Id,
                    s._stream));
        }
    }
}

internal sealed class AwaitingRedisMessageDispatcher(
    Func<RedisStreamDelivery, CancellationToken, Task> handler,
    IRedisStreamDatabase database,
    RedisAsyncResponseTransportOptions transportOptions,
    RedisSubscriberOptions subscriberOptions,
    ILogger logger,
    RedisKey stream,
    RedisValue consumerGroup,
    RedisSubscriberRole role)
    : RedisMessageDispatcher(handler, database, transportOptions, subscriberOptions, logger, stream, consumerGroup, role)
{
    /// <summary>Handles the delivered message.</summary>
    public override async Task<RedisDispatchOutcome> HandleAsync(
        RedisStreamDelivery delivery,
        CancellationToken subscriberCancellationToken)
    {
        if (AlreadyExceededDeliveryAttempts(delivery))
        {
            await TryDeadLetterAndAckAsync(
                delivery,
                new InvalidOperationException($"Redis message exceeded {MaxDeliveryAttempts} delivery attempts."),
                "max_delivery_attempts_exceeded").ConfigureAwait(false);
            return RedisDispatchOutcome.Processed;
        }

        try
        {
            await ExecuteHandlerAsync(delivery, subscriberCancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (subscriberCancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (DurableFlowInterruptedException)
        {
            // Host stop, recognised by type rather than by token: ApplicationStopping fires before
            // hosted services stop, so the flow engine interrupts the run while this subscriber's
            // token is still live. Read as a handler failure it was logged as one and — at
            // MaxDeliveryAttempts — dead-lettered and ACKed, burying a flow's only wake-up because
            // of a deploy. Leave it pending, unsettled, for redelivery after the restart.
            SignalHandBack();
            SafeLog.Try(
                (Logger, Id: delivery.MessageId.ToString(), Stream: delivery.Stream.ToString()),
                static s => s.Logger.LogInformation(
                    "Redis message {MessageId} on {Stream} was handed back by the flow engine because the host is stopping; it stays pending for redelivery.",
                    s.Id,
                    s.Stream));
            return RedisDispatchOutcome.HandedBack;
        }
        catch (Exception ex) when (ReachedDeliveryAttempts(delivery))
        {
            // With dead-lettering disabled the burial itself logs the drop, with this failure attached.
            if (TransportOptions.DeadLetterEnabled)
            {
                SafeLog.Try(
                    (Logger, ex, Id: delivery.MessageId.ToString(), MaxDeliveryAttempts),
                    static s => s.Logger.LogWarning(
                        s.ex,
                        "Redis message {MessageId} reached max delivery attempts ({MaxDeliveryAttempts}); writing to dead-letter stream.",
                        s.Id,
                        s.MaxDeliveryAttempts));
            }

            await TryDeadLetterAndAckAsync(delivery, ex, "handler_failed_max_attempts").ConfigureAwait(false);
            return RedisDispatchOutcome.Processed;
        }
        catch
        {
            // Leave the entry pending. The subscriber's pending-claim loop reclaims it after
            // PendingMessageMinIdleTime, giving Redis-backed retry without a hot loop.
            return RedisDispatchOutcome.Processed;
        }

        // The ACK sits outside the handler's try/catch: a transient XACK failure after a
        // successful handler must not be misread as a handler failure — dead-lettering or leaving
        // it for reclaim here would redeliver (or bury) work whose side effects already completed.
        // Swallow and log instead; the entry stays pending and at-least-once redelivery applies.
        // Settlement deliberately ignores cancellation (as every sibling transport does): the
        // handler already completed, and abandoning the XACK on shutdown leaves the entry in the
        // PEL to be reclaimed and re-run after restart.
        try
        {
            await AckAsync(delivery, CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            SafeLog.Try(
                (Logger, ex, Id: delivery.MessageId.ToString(), Stream: delivery.Stream.ToString()),
                static s => s.Logger.LogWarning(
                    s.ex,
                    "Failed to ACK Redis message {MessageId} on {Stream} after a successful handler; the entry stays pending and may be redelivered.",
                    s.Id,
                    s.Stream));
        }

        return RedisDispatchOutcome.Processed;
    }
}

internal sealed class QueuedRedisMessageDispatcher : RedisMessageDispatcher
{
    private readonly Channel<RedisStreamDelivery> _queue;
    private readonly Task[] _workers;
    private readonly CancellationTokenSource _drainCancellation = new();
    private readonly TimeSpan _drainTimeout;
    private readonly int _capacity;
    private readonly string _stream;
    private int _pendingCount;
    private int _runningCount;
    private int _disposeStarted;

    /// <summary>Runs the QueuedRedisMessageDispatcher operation.</summary>
    public QueuedRedisMessageDispatcher(
        Func<RedisStreamDelivery, CancellationToken, Task> handler,
        IRedisStreamDatabase database,
        RedisAsyncResponseTransportOptions transportOptions,
        RedisSubscriberOptions subscriberOptions,
        ILogger logger,
        RedisKey stream,
        RedisValue consumerGroup,
        RedisSubscriberRole role)
        : base(handler, database, transportOptions, subscriberOptions, logger, stream, consumerGroup, role)
    {
        _stream = stream.ToString();
        _drainTimeout = subscriberOptions.BackgroundDrainTimeout;
        _capacity = subscriberOptions.BackgroundQueueCapacity;
        _queue = Channel.CreateBounded<RedisStreamDelivery>(new BoundedChannelOptions(subscriberOptions.BackgroundQueueCapacity)
        {
            AllowSynchronousContinuations = false,
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = subscriberOptions.BackgroundWorkerCount == 1,
            SingleWriter = false
        });

        _workers = Enumerable.Range(0, subscriberOptions.BackgroundWorkerCount)
            .Select(workerIndex => Task.Run(() => RunWorkerAsync(workerIndex)))
            .ToArray();

        Logger.LogInformation(
            "Created Redis ACK-after-enqueue dispatcher for {Stream} with {WorkerCount} worker(s), queue capacity {QueueCapacity}, drain timeout {DrainTimeout}.",
            _stream,
            subscriberOptions.BackgroundWorkerCount,
            subscriberOptions.BackgroundQueueCapacity,
            _drainTimeout);
    }

    internal int PendingCount => Volatile.Read(ref _pendingCount);
    internal int RunningCount => Volatile.Read(ref _runningCount);

    public override bool CanAcceptMore => Volatile.Read(ref _pendingCount) < _capacity;

    public override int FreeCapacity => Math.Max(0, _capacity - Volatile.Read(ref _pendingCount));

    /// <summary>Handles the delivered message.</summary>
    public override async Task<RedisDispatchOutcome> HandleAsync(
        RedisStreamDelivery delivery,
        CancellationToken subscriberCancellationToken)
    {
        // Pre-execution cap, BEFORE the enqueue-and-ACK (awaiting-dispatcher parity): the
        // pending-claim loop feeds this dispatcher real XPENDING delivery counts too, and without
        // the check an over-cap entry — deferred under backpressure and reclaimed each cycle, or
        // re-claimed after a swallowed post-enqueue ACK failure — was re-enqueued and re-executed
        // forever, with nothing ever consulting MaxDeliveryAttempts to bury it.
        if (AlreadyExceededDeliveryAttempts(delivery))
        {
            await TryDeadLetterAndAckAsync(
                delivery,
                new InvalidOperationException($"Redis message exceeded {MaxDeliveryAttempts} delivery attempts."),
                "max_delivery_attempts_exceeded").ConfigureAwait(false);
            return RedisDispatchOutcome.Processed;
        }

        Interlocked.Increment(ref _pendingCount);
        if (!_queue.Writer.TryWrite(delivery))
        {
            Interlocked.Decrement(ref _pendingCount);
            SafeLog.Try(
                (Logger, Id: delivery.MessageId.ToString(), _stream, PendingCount, RunningCount),
                static s => s.Logger.LogWarning(
                    "Redis background queue rejected message {MessageId} for {Stream}; leaving it pending for retry. Pending={PendingCount}, Running={RunningCount}.",
                    s.Id,
                    s._stream,
                    s.PendingCount,
                    s.RunningCount));
            return RedisDispatchOutcome.Deferred;
        }

        // The entry now belongs to a background worker, which decrements _pendingCount when it dequeues.
        // Do not touch the counter again here, even if the ACK below fails — otherwise it double-counts.
        // Settlement deliberately ignores cancellation (as every sibling transport does): a graceful
        // shutdown drains the background queue and runs this entry, so abandoning the XACK on the
        // stopping token would leave completed work in the PEL to be reclaimed and re-run.
        try
        {
            await AckAsync(delivery, CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            SafeLog.Try(
                (Logger, ex, Id: delivery.MessageId.ToString(), _stream),
                static s => s.Logger.LogError(
                    s.ex,
                    "Failed to ACK Redis message {MessageId} for {Stream} after enqueue; it is being processed but Redis will redeliver it after the pending idle window.",
                    s.Id,
                    s._stream));
        }

        return RedisDispatchOutcome.Processed;
    }

    /// <summary>The dead-letter reason of an early-ACK entry the flow engine handed back at host stop.</summary>
    internal const string HandedBackAfterCommitReason = "handed_back_after_commit";

    /// <summary>The dead-letter reason of an early-ACK entry the drain budget left unstarted.</summary>
    internal const string DrainBudgetLapsedReason = "drain_budget_lapsed_after_ack";

    /// <summary>
    /// Drains the workers, then buries whatever is still queued.
    /// <see cref="RedisSubscriberOptions.BackgroundDrainTimeout"/> is the whole spend the
    /// shutdown-budget validator sums for this dispatcher, so it is split rather than exceeded
    /// (NATS/DB parity): three quarters let queued and running handlers finish, and the last
    /// quarter is RESERVED for dead-lettering and reporting what is still queued once that lapses.
    /// </summary>
    public override async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposeStarted, 1) != 0)
            return;

        SafeLog.Try(
            (Logger, _stream, PendingCount, RunningCount),
            static s => s.Logger.LogInformation(
                "Draining Redis ACK-after-enqueue dispatcher for {Stream}. Pending={PendingCount}, Running={RunningCount}.",
                s._stream,
                s.PendingCount,
                s.RunningCount));
        _queue.Writer.TryComplete();

        var routingReserve = TimeSpan.FromTicks(_drainTimeout.Ticks / 4);
        var drainBudget = _drainTimeout - routingReserve;
        var workers = Task.WhenAll(_workers);
        try
        {
            await workers.WaitAsync(drainBudget).ConfigureAwait(false);
            _drainCancellation.Dispose();
            return;
        }
        catch (TimeoutException ex)
        {
            _drainCancellation.Cancel();
            SafeLog.Try(
                (Logger, ex, _stream, drainBudget, PendingCount, RunningCount),
                static s => s.Logger.LogWarning(
                    s.ex,
                    "Timed out after {DrainBudget} while draining Redis ACK-after-enqueue dispatcher for {Stream}. Pending={PendingCount}, Running={RunningCount}. Dead-lettering the entries still queued; already-ACKed work still running may be interrupted by host shutdown.",
                    s.drainBudget,
                    s._stream,
                    s.PendingCount,
                    s.RunningCount));
        }
        catch (Exception ex)
        {
            // A worker faulted outside its own handler guard (DB/NATS dispatcher parity). WhenAll
            // only completes once every worker has finished, so the source is safe to dispose here
            // — and the fault must not escape DisposeAsync and mask the real shutdown path.
            SafeLog.Try(
                (Logger, ex, _stream),
                static s => s.Logger.LogDebug(s.ex, "Redis ACK-after-enqueue dispatcher drain for {Stream} ended with an error.", s._stream));
            _drainCancellation.Dispose();
            return;
        }

        // Bury the queued entries HERE, not in the worker loop: an entry still queued when the
        // drain lapses means every worker is inside a handler (an idle one would have dequeued
        // it), and the handler takes no token — so the workers' own drain-lapsed routing ran only
        // once some handler happened to finish, after this method had returned and the host had
        // torn the multiplexer down or exited. Those entries were ACKed at enqueue, so Redis never
        // redelivers them: they vanished with no dead-letter copy and no callback. A worker that
        // does come free routes entries too; each is read exactly once.
        //
        // EVERY entry is buried before ANY is reported (NATS/Kafka/RabbitMQ parity): the report
        // awaits the user's OnBackgroundFailure, and awaited between burials, one slow callback (an
        // alert stuck on HTTP) spent the whole reserve on the first entry — the entries behind it
        // got no dead-letter copy, only the loss count below.
        using var reserve = new CancellationTokenSource(routingReserve);
        var routed = new List<(RedisStreamDelivery Delivery, OperationCanceledException Lapsed)>();
        while (!reserve.IsCancellationRequested && _queue.Reader.TryRead(out var undrained))
        {
            Interlocked.Decrement(ref _pendingCount);
            var lapsed = DrainLapsed();
            routed.Add((undrained, lapsed));
            await BuryUndrainedAsync(undrained, lapsed, reserve.Token).ConfigureAwait(false);
        }

        // Each callback is awaited only within what is left of the reserve; one that outlives it
        // runs on unawaited (NotifyBackgroundFailureWithinAsync logs that, and a late fault).
        foreach (var (delivery, lapsed) in routed)
            await NotifyBackgroundFailureWithinAsync(delivery, lapsed, reserve.Token).ConfigureAwait(false);

        try
        {
            // What remains of the reserve lets a handler that honors the cancellation report it.
            await workers.WaitAsync(reserve.Token).ConfigureAwait(false);
            _drainCancellation.Dispose();
            return;
        }
        catch (OperationCanceledException) when (reserve.IsCancellationRequested)
        {
            var leftover = _queue.Reader.Count;
            if (leftover > 0)
            {
                SafeLog.Try(
                    (Logger, leftover, _stream, routingReserve),
                    static s => s.Logger.LogError(
                        "{Count} already-ACKed Redis message(s) on {Stream} were still queued when the reserved {Reserve} ran out; they are lost at process exit (Redis will not redeliver them).",
                        s.leftover,
                        s._stream,
                        s.routingReserve));
            }
        }
        catch (Exception ex)
        {
            SafeLog.Try(
                (Logger, ex, _stream),
                static s => s.Logger.LogDebug(s.ex, "Redis ACK-after-enqueue dispatcher drain for {Stream} ended with an error.", s._stream));
            _drainCancellation.Dispose();
            return;
        }

        // The workers are still running and observe _drainCancellation.Token, so disposing it now
        // would throw ObjectDisposedException inside them. Dispose once they actually finish, off
        // the shutdown path, so the source is not leaked either.
        _ = workers.ContinueWith(
            _ => _drainCancellation.Dispose(),
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);
    }

    /// <summary>
    /// Buries an already-ACKed entry the drain budget left unstarted: Redis will never redeliver
    /// it, so the dead-letter copy and the OnBackgroundFailure report are its only record. The
    /// copy is written first and the report made second, so a slow callback cannot cost the copy;
    /// the log says which of them exist. <paramref name="cancellationToken"/> bounds both (none
    /// from a worker); the stop-time reserve runs the two halves as separate passes instead, every
    /// burial before any report.
    /// </summary>
    private async Task RouteUndrainedAsync(RedisStreamDelivery delivery, CancellationToken cancellationToken)
    {
        var lapsed = DrainLapsed();
        await BuryUndrainedAsync(delivery, lapsed, cancellationToken).ConfigureAwait(false);
        await NotifyBackgroundFailureWithinAsync(delivery, lapsed, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>The failure an entry the drain budget left unstarted is dead-lettered and reported with.</summary>
    private static OperationCanceledException DrainLapsed() => new(
        "The ACK-after-enqueue drain budget lapsed before this already-ACKed message was handled.");

    /// <summary>
    /// The burial half of <see cref="RouteUndrainedAsync"/>: writes the dead-letter copy, bounded by
    /// <paramref name="cancellationToken"/>, and logs which record exists. Never throws.
    /// </summary>
    private async Task BuryUndrainedAsync(RedisStreamDelivery delivery, OperationCanceledException lapsed, CancellationToken cancellationToken)
    {
        if (await TryDeadLetterAfterAckAsync(delivery, lapsed, DrainBudgetLapsedReason, cancellationToken).ConfigureAwait(false))
        {
            SafeLog.Try(
                (Logger, Id: delivery.MessageId.ToString(), _stream),
                static s => s.Logger.LogWarning(
                    "Redis background handler for already-ACKed message {MessageId} on {Stream} was not started: the dispatcher's drain budget had lapsed. Dead-lettered a copy (drain_budget_lapsed_after_ack) and surfacing via OnBackgroundFailure.",
                    s.Id,
                    s._stream));
        }
        else
        {
            SafeLog.Try(
                (Logger, Id: delivery.MessageId.ToString(), _stream, Why: NoCopyReason),
                static s => s.Logger.LogError(
                    "Redis background handler for already-ACKed message {MessageId} on {Stream} was not started: the dispatcher's drain budget had lapsed, Redis will not redeliver it, and {Why}, so the job is lost unless OnBackgroundFailure records it.",
                    s.Id,
                    s._stream,
                    s.Why));
        }
    }

    /// <summary>Why an early-ACK burial wrote no copy, for its Error log.</summary>
    private string NoCopyReason => TransportOptions.DeadLetterEnabled
        ? "the dead-letter write failed"
        : "no dead-letter destination is configured";

    private async Task RunWorkerAsync(int workerIndex)
    {
        await foreach (var delivery in _queue.Reader.ReadAllAsync().ConfigureAwait(false))
        {
            Interlocked.Decrement(ref _pendingCount);
            Interlocked.Increment(ref _runningCount);

            // Once the drain budget has lapsed, STOP executing. The token below cannot stop the
            // real handler — it is `_ingress.HandleWorkerMessageAsync(payload)`, whose target takes
            // no CancellationToken — so nothing ever raised the OperationCanceledException the arm
            // below was written for, the loop kept starting fresh work past the budget, and every
            // entry still queued at process exit vanished with no record (they were ACKed at
            // enqueue, so Redis will not redeliver them). Route them through the same
            // dead-letter/OnBackgroundFailure path the stop-time reserve uses.
            if (_drainCancellation.IsCancellationRequested)
            {
                try
                {
                    await RouteUndrainedAsync(delivery, CancellationToken.None).ConfigureAwait(false);
                }
                finally
                {
                    Interlocked.Decrement(ref _runningCount);
                }

                continue;
            }

            try
            {
                await ExecuteHandlerAsync(
                    delivery,
                    _drainCancellation.Token,
                    logFailures: false).ConfigureAwait(false);
            }
            catch (DurableFlowInterruptedException ex)
            {
                // Host stop — even when the drain has not started (ApplicationStopping fires
                // first): not a handler failure, so no Error log. But the entry was ACKed at
                // enqueue and Redis will never redeliver it, so beyond the OnBackgroundFailure
                // report a dead-letter copy under its own reason is its only durable record: a
                // replay is safe, the run resuming from its last checkpoint. Checked before the
                // drain's own cancellation below, so a hand-back is recorded as one whenever it
                // lands. The copy is written BEFORE the user's callback is awaited: behind a slow
                // callback (an alert over HTTP) the stop could tear the multiplexer down before the
                // only durable record was ever written, after a Warning had already claimed it.
                SignalHandBack();
                if (await TryDeadLetterAfterAckAsync(delivery, ex, HandedBackAfterCommitReason, CancellationToken.None).ConfigureAwait(false))
                {
                    SafeLog.Try(
                        (Logger, Id: delivery.MessageId.ToString(), _stream),
                        static s => s.Logger.LogWarning(
                            "Redis background handler for already-ACKed message {MessageId} on {Stream} was handed back by the flow engine because the host is stopping; Redis will not redeliver it. Dead-lettered a copy (handed_back_after_commit) and surfacing via OnBackgroundFailure.",
                            s.Id,
                            s._stream));
                }
                else
                {
                    // No copy exists: the wake-up is lost unless the report records it.
                    SafeLog.Try(
                        (Logger, Id: delivery.MessageId.ToString(), _stream, Why: NoCopyReason),
                        static s => s.Logger.LogError(
                            "Redis background handler for already-ACKed message {MessageId} on {Stream} was handed back by the flow engine because the host is stopping; Redis will not redeliver it, and {Why}, so the wake-up is lost unless OnBackgroundFailure records it (resume the flow explicitly).",
                            s.Id,
                            s._stream,
                            s.Why));
                }

                await NotifyBackgroundFailureAsync(delivery, ex).ConfigureAwait(false);
            }
            catch (OperationCanceledException ex) when (_drainCancellation.IsCancellationRequested)
            {
                // The drain budget lapsed with this already-ACKed entry still unprocessed: Redis
                // will not redeliver it, so surface the drop through OnBackgroundFailure instead of
                // losing it silently.
                SafeLog.Try(
                    (Logger, Id: delivery.MessageId.ToString(), _stream),
                    static s => s.Logger.LogWarning(
                        "Redis background handler for already-ACKed message {MessageId} on {Stream} was canceled during dispatcher shutdown; surfacing via OnBackgroundFailure.",
                        s.Id,
                        s._stream));
                await NotifyBackgroundFailureAsync(delivery, ex).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                SafeLog.Try(
                    (Logger, ex, Id: delivery.MessageId.ToString(), _stream),
                    static s => s.Logger.LogError(
                        s.ex,
                        "Redis background handler failed for already-ACKed message {MessageId} on {Stream}.",
                        s.Id,
                        s._stream));

                // Copy first, report second (see the hand-back arm above).
                if (!await TryDeadLetterAfterAckAsync(delivery, ex, "background_handler_failed_after_ack", CancellationToken.None).ConfigureAwait(false))
                {
                    SafeLog.Try(
                        (Logger, Id: delivery.MessageId.ToString(), _stream, Why: NoCopyReason),
                        static s => s.Logger.LogError(
                            "No dead-letter copy of already-ACKed Redis message {MessageId} on {Stream} was written ({Why}); the failure is only observable via logs and OnBackgroundFailure.",
                            s.Id,
                            s._stream,
                            s.Why));
                }

                await NotifyBackgroundFailureAsync(delivery, ex).ConfigureAwait(false);
            }
            finally
            {
                Interlocked.Decrement(ref _runningCount);
            }
        }
    }
}
