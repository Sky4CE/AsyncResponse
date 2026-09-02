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
    Deferred
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
        // PendingClaimInterval is a "now + interval" schedule stamp and keeps the persistence bound.
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
        catch (Exception ex)
        {
            if (logFailures)
            {
                Logger.LogError(
                    ex,
                    "Redis stream message handling failed for {Stream}/{MessageId}.",
                    _stream,
                    delivery.MessageId.ToString());
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

    /// <summary>Moves the delivered message to dead-letter storage and acknowledges it.</summary>
    protected async Task DeadLetterAndAckAsync(
        RedisStreamDelivery delivery,
        Exception exception,
        string reason,
        CancellationToken cancellationToken)
    {
        if (TransportOptions.DeadLetterEnabled)
        {
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
        }

        await AckAsync(delivery, cancellationToken).ConfigureAwait(false);
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
            // A trimmed-while-pending tombstone (Redis 5/6 answer XCLAIM with a nil entry) carries
            // no id to ACK and no payload to record: sending its null id to XACK is rejected by the
            // client from inside the caller's catch, which replaced the original error, faulted the
            // subscriber, and re-dead-lettered the tombstone every claim cycle. The claim loop
            // drains it by its pending id instead; nothing to settle here.
            Logger.LogDebug(failure, "Redis claim on {Stream} returned a trimmed tombstone; skipping it.", _stream);
            return;
        }

        Logger.LogError(
            failure,
            "Redis entry {MessageId} on {Stream} could not be parsed into a delivery; dead-lettering and ACKing it to avoid a poison-message loop.",
            entry.Id.ToString(),
            _stream);

        var delivery = new RedisStreamDelivery(
            stream,
            consumerGroup,
            entry.Id,
            DescribeRawEntry(entry),
            RedisCorrelationIdExtractor.TryReadField(entry, TransportOptions.CorrelationIdField),
            Attempt: 0,
            entry);

        // Settlement deliberately ignores cancellation (as every other settlement in this file
        // does): a shutdown landing between the dead-letter XADD and the XACK left the entry in
        // the PEL to be reclaimed and dead-lettered a SECOND time after restart.
        await DeadLetterAndAckAsync(delivery, failure, "unparsable_entry", CancellationToken.None).ConfigureAwait(false);
    }

    private static string DescribeRawEntry(StreamEntry entry)
        => entry.Values is { Length: > 0 }
            ? string.Join("; ", entry.Values.Select(value => $"{value.Name}={value.Value}"))
            : string.Empty;

    /// <summary>Runs the NotifyBackgroundFailureAsync operation.</summary>
    protected async ValueTask NotifyBackgroundFailureAsync(
        RedisStreamDelivery delivery,
        Exception exception)
    {
        var callback = _subscriberOptions.OnBackgroundFailure;
        if (callback is null)
            return;

        try
        {
            await callback(new RedisBackgroundFailureContext(
                _stream,
                _consumerGroup,
                _role.ToString(),
                delivery.MessageId.ToString(),
                delivery.CorrelationId,
                exception)).ConfigureAwait(false);
        }
        catch (Exception callbackException)
        {
            Logger.LogError(
                callbackException,
                "Redis background failure callback failed for already-ACKed message {MessageId} on {Stream}.",
                delivery.MessageId.ToString(),
                _stream);
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
        catch (Exception ex) when (ReachedDeliveryAttempts(delivery))
        {
            Logger.LogWarning(
                ex,
                "Redis message {MessageId} reached max delivery attempts ({MaxDeliveryAttempts}); writing to dead-letter stream.",
                delivery.MessageId.ToString(),
                MaxDeliveryAttempts);
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
            Logger.LogWarning(
                ex,
                "Failed to ACK Redis message {MessageId} on {Stream} after a successful handler; the entry stays pending and may be redelivered.",
                delivery.MessageId.ToString(),
                delivery.Stream.ToString());
        }

        return RedisDispatchOutcome.Processed;
    }

    /// <summary>
    /// Burial that never throws (queued-dispatcher and DB-transport parity: "a burial that throws
    /// is a burial that failed"). Unguarded, a dead-letter XADD that failed — MISCONF/OOM, the
    /// adapter's timeout, a WRONGTYPE on the dead-letter key — escaped past the XACK to the
    /// supervisor, which restarted the subscriber; the pending-claim loop then re-claimed the
    /// same entry every cycle and the whole stream stopped draining. Settlement deliberately
    /// ignores cancellation: a shutdown landing between the XADD and the XACK would leave the
    /// entry in the PEL to be reclaimed and dead-lettered a SECOND time.
    /// </summary>
    private async Task TryDeadLetterAndAckAsync(RedisStreamDelivery delivery, Exception exception, string reason)
    {
        try
        {
            await DeadLetterAndAckAsync(delivery, exception, reason, CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception deadLetterException)
        {
            Logger.LogError(
                deadLetterException,
                "Failed to dead-letter Redis message {MessageId} on {Stream} ({Reason}); the entry stays pending and is reclaimed on the next pending-claim cycle.",
                delivery.MessageId.ToString(),
                delivery.Stream.ToString(),
                reason);
        }
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
            await DeadLetterAndAckAsync(
                delivery,
                new InvalidOperationException($"Redis message exceeded {MaxDeliveryAttempts} delivery attempts."),
                "max_delivery_attempts_exceeded",
                CancellationToken.None).ConfigureAwait(false);
            return RedisDispatchOutcome.Processed;
        }

        Interlocked.Increment(ref _pendingCount);
        if (!_queue.Writer.TryWrite(delivery))
        {
            Interlocked.Decrement(ref _pendingCount);
            Logger.LogWarning(
                "Redis background queue rejected message {MessageId} for {Stream}; leaving it pending for retry. Pending={PendingCount}, Running={RunningCount}.",
                delivery.MessageId.ToString(),
                _stream,
                PendingCount,
                RunningCount);
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
            Logger.LogError(
                ex,
                "Failed to ACK Redis message {MessageId} for {Stream} after enqueue; it is being processed but Redis will redeliver it after the pending idle window.",
                delivery.MessageId.ToString(),
                _stream);
        }

        return RedisDispatchOutcome.Processed;
    }

    /// <summary>Releases resources held by this instance.</summary>
    public override async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposeStarted, 1) != 0)
            return;

        Logger.LogInformation(
            "Draining Redis ACK-after-enqueue dispatcher for {Stream}. Pending={PendingCount}, Running={RunningCount}.",
            _stream,
            PendingCount,
            RunningCount);
        _queue.Writer.TryComplete();

        try
        {
            await Task.WhenAll(_workers).WaitAsync(_drainTimeout).ConfigureAwait(false);
            _drainCancellation.Dispose();
        }
        catch (TimeoutException ex)
        {
            _drainCancellation.Cancel();
            Logger.LogWarning(
                ex,
                "Timed out while draining Redis ACK-after-enqueue dispatcher for {Stream}. Pending={PendingCount}, Running={RunningCount}. Already ACKed work may be interrupted by host shutdown.",
                _stream,
                PendingCount,
                RunningCount);

            _ = Task.WhenAll(_workers).ContinueWith(
                _ => _drainCancellation.Dispose(),
                CancellationToken.None,
                TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default);
        }
        catch (Exception ex)
        {
            // A worker faulted outside its own handler guard (DB/NATS dispatcher parity). WhenAll
            // only completes once every worker has finished, so the source is safe to dispose here
            // — and the fault must not escape DisposeAsync and mask the real shutdown path.
            Logger.LogDebug(ex, "Redis ACK-after-enqueue dispatcher drain for {Stream} ended with an error.", _stream);
            _drainCancellation.Dispose();
        }
    }

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
            // OnBackgroundFailure/dead-letter path instead of losing them silently.
            if (_drainCancellation.IsCancellationRequested)
            {
                var lapsed = new OperationCanceledException(
                    "The ACK-after-enqueue drain budget lapsed before this already-ACKed message was handled.");

                Logger.LogWarning(
                    "Redis background handler for already-ACKed message {MessageId} on {Stream} was not started: the dispatcher's drain budget had lapsed. Surfacing via OnBackgroundFailure.",
                    delivery.MessageId.ToString(),
                    _stream);

                await NotifyBackgroundFailureAsync(delivery, lapsed).ConfigureAwait(false);

                try
                {
                    await DeadLetterAndAckAsync(
                        delivery,
                        lapsed,
                        "drain_budget_lapsed_after_ack",
                        CancellationToken.None).ConfigureAwait(false);
                }
                catch (Exception deadLetterException)
                {
                    Logger.LogError(
                        deadLetterException,
                        "Failed to dead-letter undrained Redis message {MessageId} on {Stream}.",
                        delivery.MessageId.ToString(),
                        _stream);
                }

                Interlocked.Decrement(ref _runningCount);
                continue;
            }

            try
            {
                await ExecuteHandlerAsync(
                    delivery,
                    _drainCancellation.Token,
                    logFailures: false).ConfigureAwait(false);
            }
            catch (OperationCanceledException ex) when (_drainCancellation.IsCancellationRequested)
            {
                // The drain budget lapsed with this already-ACKed entry still unprocessed: Redis
                // will not redeliver it, so surface the drop through OnBackgroundFailure instead of
                // losing it silently.
                Logger.LogWarning(
                    "Redis background handler for already-ACKed message {MessageId} on {Stream} was canceled during dispatcher shutdown; surfacing via OnBackgroundFailure.",
                    delivery.MessageId.ToString(),
                    _stream);
                await NotifyBackgroundFailureAsync(delivery, ex).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Logger.LogError(
                    ex,
                    "Redis background handler failed for already-ACKed message {MessageId} on {Stream}.",
                    delivery.MessageId.ToString(),
                    _stream);
                await NotifyBackgroundFailureAsync(delivery, ex).ConfigureAwait(false);

                try
                {
                    await DeadLetterAndAckAsync(
                        delivery,
                        ex,
                        "background_handler_failed_after_ack",
                        CancellationToken.None).ConfigureAwait(false);
                }
                catch (Exception deadLetterException)
                {
                    Logger.LogError(
                        deadLetterException,
                        "Failed to dead-letter already-ACKed Redis message {MessageId} on {Stream}.",
                        delivery.MessageId.ToString(),
                        _stream);
                }
            }
            finally
            {
                Interlocked.Decrement(ref _runningCount);
            }
        }
    }
}
