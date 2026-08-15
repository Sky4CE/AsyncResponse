using Microsoft.Extensions.Logging;
using System.Diagnostics;
using System.Threading.Channels;

namespace AsyncResponse.Transports.AzureServiceBus;

internal enum AzureServiceBusSubscriberRole
{
    Worker,
    ResponseIngress
}

internal abstract class AzureServiceBusMessageDispatcher : IAsyncDisposable
{
    private readonly Func<AzureServiceBusTransportDelivery, CancellationToken, Task> _handler;
    private readonly AzureServiceBusAsyncResponseOptions _transportOptions;
    private readonly AzureServiceBusSubscriberOptions _subscriberOptions;
    private readonly string _queue;
    private readonly AzureServiceBusSubscriberRole _role;

    protected AzureServiceBusMessageDispatcher(
        Func<AzureServiceBusTransportDelivery, CancellationToken, Task> handler,
        AzureServiceBusAsyncResponseOptions transportOptions,
        AzureServiceBusSubscriberOptions subscriberOptions,
        ILogger logger,
        string queue,
        AzureServiceBusSubscriberRole role)
    {
        _handler = handler;
        _transportOptions = transportOptions;
        _subscriberOptions = subscriberOptions;
        Logger = logger;
        _queue = queue;
        _role = role;
    }

    protected ILogger Logger { get; }
    protected int MaxDeliveryAttempts => _subscriberOptions.MaxDeliveryAttempts;

    /// <summary>Creates the dispatcher configured by the subscriber options.</summary>
    public static AzureServiceBusMessageDispatcher Create(
        Func<AzureServiceBusTransportDelivery, CancellationToken, Task> handler,
        AzureServiceBusAsyncResponseOptions transportOptions,
        AzureServiceBusSubscriberOptions subscriberOptions,
        ILogger logger,
        string queue,
        AzureServiceBusSubscriberRole role)
    {
        AzureServiceBusOptionsValidator.ValidateSubscriber(transportOptions, subscriberOptions, role);

        return subscriberOptions.AckMode == AzureServiceBusAckMode.AckAfterHandlerCompletes
            ? new AwaitingAzureServiceBusMessageDispatcher(
                handler,
                transportOptions,
                subscriberOptions,
                logger,
                queue,
                role)
            : new QueuedAzureServiceBusMessageDispatcher(
                handler,
                transportOptions,
                subscriberOptions,
                logger,
                queue,
                role);
    }

    /// <summary>Validates the supplied subscriber options.</summary>
    public static void ValidateOptions(
        AzureServiceBusAsyncResponseOptions transportOptions,
        AzureServiceBusSubscriberOptions subscriberOptions,
        AzureServiceBusSubscriberRole role)
        => AzureServiceBusOptionsValidator.ValidateSubscriber(transportOptions, subscriberOptions, role);

    /// <summary>Handles the delivered message.</summary>
    public abstract Task HandleAsync(
        AzureServiceBusTransportDelivery delivery,
        CancellationToken subscriberCancellationToken);

    /// <summary>
    /// Whether the dispatcher can accept more deliveries right now. Awaiting dispatchers always can
    /// (handlers run inline); the queued dispatcher returns <c>false</c> while its bounded queue is
    /// saturated so the receive loop stops pulling messages instead of receiving and abandoning them —
    /// every abandon burns <c>DeliveryCount</c> toward the entity's MaxDeliveryCount.
    /// </summary>
    public virtual bool CanAcceptMore => true;

    /// <summary>
    /// Number of deliveries the dispatcher can accept right now. The receive loop requests at most
    /// this many messages per receive in early-ACK mode so a burst never overflows the background queue.
    /// </summary>
    public virtual int FreeCapacity => int.MaxValue;

    /// <summary>
    /// Waits until the dispatcher can accept at least one more delivery. Completes immediately for
    /// awaiting dispatchers; the queued dispatcher waits for a background worker to free a slot.
    /// </summary>
    public virtual ValueTask WaitForCapacityAsync(CancellationToken cancellationToken) => ValueTask.CompletedTask;

    /// <summary>Releases resources held by this instance.</summary>
    public virtual ValueTask DisposeAsync() => ValueTask.CompletedTask;

    protected async Task ExecuteHandlerAsync(
        AzureServiceBusTransportDelivery delivery,
        CancellationToken cancellationToken,
        bool logFailures = true)
    {
        using var activity = AsyncResponseDiagnostics.StartActivity(
            "asyncresponse.azure_service_bus.receive",
            ActivityKind.Consumer);
        activity?.SetTag("asyncresponse.transport", "azure_service_bus");
        activity?.SetTag("asyncresponse.azure_service_bus.role", _role.ToString());
        activity?.SetTag("asyncresponse.azure_service_bus.ack_mode", _subscriberOptions.AckMode.ToString());
        activity?.SetTag("messaging.system", "azure_service_bus");
        activity?.SetTag("messaging.destination.name", _queue);
        activity?.SetTag("messaging.message.id", delivery.MessageId);
        activity?.SetTag("messaging.azure_service_bus.sequence_number", delivery.SequenceNumber);

        if (!string.IsNullOrWhiteSpace(delivery.CorrelationId))
            AsyncResponseDiagnostics.SetCorrelationId(activity, delivery.CorrelationId);

        try
        {
            await _handler(delivery, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            if (logFailures)
                Logger.LogError(ex, "Azure Service Bus message handling failed for message {MessageId}.", delivery.MessageId);
            AsyncResponseDiagnostics.SetError(activity, ex);
            throw;
        }
    }

    protected async ValueTask NotifyBackgroundFailureAsync(
        AzureServiceBusTransportDelivery delivery,
        Exception exception,
        string queue,
        AzureServiceBusSubscriberRole role)
    {
        var callback = _subscriberOptions.OnBackgroundFailure;
        if (callback is null)
            return;

        try
        {
            var context = new AzureServiceBusBackgroundFailureContext(
                queue,
                role.ToString(),
                delivery.SequenceNumber,
                delivery.MessageId,
                delivery.CorrelationId ?? TryReadApplicationCorrelationId(delivery),
                exception);
            await callback(context).ConfigureAwait(false);
        }
        catch (Exception callbackException)
        {
            Logger.LogError(
                callbackException,
                "Azure Service Bus background failure callback failed for already-completed message {MessageId} on {Queue}.",
                delivery.MessageId,
                queue);
        }
    }

    private string? TryReadApplicationCorrelationId(AzureServiceBusTransportDelivery delivery)
    {
        if (!string.IsNullOrWhiteSpace(_transportOptions.CorrelationIdProperty)
            && delivery.ApplicationProperties.TryGetValue(_transportOptions.CorrelationIdProperty, out var value))
        {
            return AzureServiceBusCorrelationIdExtractor.TryConvertProperty(value);
        }

        return null;
    }
}

internal sealed class AwaitingAzureServiceBusMessageDispatcher(
    Func<AzureServiceBusTransportDelivery, CancellationToken, Task> handler,
    AzureServiceBusAsyncResponseOptions transportOptions,
    AzureServiceBusSubscriberOptions subscriberOptions,
    ILogger logger,
    string queue,
    AzureServiceBusSubscriberRole role)
    : AzureServiceBusMessageDispatcher(handler, transportOptions, subscriberOptions, logger, queue, role)
{
    /// <summary>Handles the delivered message.</summary>
    public override async Task HandleAsync(
        AzureServiceBusTransportDelivery delivery,
        CancellationToken subscriberCancellationToken)
    {
        try
        {
            await ExecuteHandlerAsync(delivery, subscriberCancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (subscriberCancellationToken.IsCancellationRequested)
        {
            // Host shutdown, not a handler failure: abandoning would burn a delivery count on
            // work that never ran, and at the cap the branch below would dead-letter healthy
            // work. Leave the delivery unsettled — the peek lock lapses on its own and
            // at-least-once redelivery applies after restart (parity with the
            // RabbitMQ/Redis/Kafka/DB dispatchers).
            throw;
        }
        catch (Exception ex)
        {
            // Failure-path settlement is guarded like the Complete below: a slow handler that
            // outlived its peek lock makes DeadLetter/Abandon throw MessageLockLost, and an
            // escaping settlement would tear down the whole receiver — dropping the rest of the
            // already-received batch un-settled. On a lost settle the lock lapses on its own and
            // at-least-once redelivery applies (DeliveryCount still advances broker-side).
            if (MaxDeliveryAttempts > 0 && delivery.DeliveryCount >= MaxDeliveryAttempts)
            {
                try
                {
                    await delivery.DeadLetterAsync(
                        "AsyncResponseHandlerFailed",
                        ex.Message).ConfigureAwait(false);
                }
                catch (Exception settleEx)
                {
                    Logger.LogWarning(
                        settleEx,
                        "Failed to dead-letter Azure Service Bus message {MessageId} after a failed handler; the lock will lapse and the message will be redelivered.",
                        delivery.MessageId);
                }

                return;
            }

            try
            {
                await delivery.AbandonAsync().ConfigureAwait(false);
            }
            catch (Exception settleEx)
            {
                Logger.LogWarning(
                    settleEx,
                    "Failed to abandon Azure Service Bus message {MessageId} after a failed handler; the lock will lapse and the message will be redelivered.",
                    delivery.MessageId);
            }

            return;
        }

        // The Complete sits outside the handler's try/catch: a transient settlement failure after
        // a successful handler must not be misread as a handler failure — dead-lettering or
        // abandoning here would redeliver (or bury) work whose side effects already completed.
        // Swallow and log instead; the peek-lock lapses on its own and at-least-once redelivery
        // applies.
        try
        {
            await delivery.CompleteAsync().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            Logger.LogWarning(
                ex,
                "Failed to complete Azure Service Bus message {MessageId} after a successful handler; the lock will lapse and the message may be redelivered.",
                delivery.MessageId);
        }
    }
}

internal sealed class QueuedAzureServiceBusMessageDispatcher : AzureServiceBusMessageDispatcher
{
    private readonly Channel<AzureServiceBusTransportDelivery> _queue;
    private readonly Task[] _workers;
    private readonly CancellationTokenSource _drainCancellation = new();
    private readonly TimeSpan _drainTimeout;
    private readonly int _capacity;
    private readonly string _queueName;
    private readonly AzureServiceBusSubscriberRole _role;
    private int _pendingCount;
    private int _runningCount;
    private int _disposeStarted;

    /// <summary>Creates an ACK-after-enqueue dispatcher with a bounded background queue.</summary>
    public QueuedAzureServiceBusMessageDispatcher(
        Func<AzureServiceBusTransportDelivery, CancellationToken, Task> handler,
        AzureServiceBusAsyncResponseOptions transportOptions,
        AzureServiceBusSubscriberOptions subscriberOptions,
        ILogger logger,
        string queue,
        AzureServiceBusSubscriberRole role)
        : base(handler, transportOptions, subscriberOptions, logger, queue, role)
    {
        _drainTimeout = subscriberOptions.BackgroundDrainTimeout;
        _capacity = subscriberOptions.BackgroundQueueCapacity;
        _queueName = queue;
        _role = role;
        _queue = Channel.CreateBounded<AzureServiceBusTransportDelivery>(new BoundedChannelOptions(subscriberOptions.BackgroundQueueCapacity)
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
            "Created Azure Service Bus ACK-after-enqueue dispatcher for {Queue} with {WorkerCount} worker(s), queue capacity {QueueCapacity}, drain timeout {DrainTimeout}.",
            _queueName,
            subscriberOptions.BackgroundWorkerCount,
            subscriberOptions.BackgroundQueueCapacity,
            _drainTimeout);
    }

    internal int PendingCount => Volatile.Read(ref _pendingCount);
    internal int RunningCount => Volatile.Read(ref _runningCount);

    public override bool CanAcceptMore => Volatile.Read(ref _pendingCount) < _capacity;

    public override int FreeCapacity => Math.Max(0, _capacity - Volatile.Read(ref _pendingCount));

    public override async ValueTask WaitForCapacityAsync(CancellationToken cancellationToken)
    {
        // WaitToWriteAsync completes when the bounded channel has room (or the channel is completed
        // during dispose, in which case there is nothing left to gate).
        while (!CanAcceptMore)
        {
            if (!await _queue.Writer.WaitToWriteAsync(cancellationToken).ConfigureAwait(false))
                return;
        }
    }

    /// <summary>Handles the delivered message.</summary>
    public override async Task HandleAsync(
        AzureServiceBusTransportDelivery delivery,
        CancellationToken subscriberCancellationToken)
    {
        Interlocked.Increment(ref _pendingCount);
        if (!_queue.Writer.TryWrite(delivery))
        {
            // The receive loop gates on free capacity, so this only covers the residual race between
            // its capacity check and this write. The abandon burns one DeliveryCount, but the loop
            // never receives while saturated, so a healthy message cannot repeat this path toward
            // the entity's MaxDeliveryCount.
            Interlocked.Decrement(ref _pendingCount);
            Logger.LogWarning(
                "Azure Service Bus background queue rejected message {MessageId} for {Queue}; abandoning for redelivery. Pending={PendingCount}, Running={RunningCount}.",
                delivery.MessageId,
                _queueName,
                PendingCount,
                RunningCount);
            try
            {
                await delivery.AbandonAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                // Guarded like every other settlement: an escaping MessageLockLost would tear down
                // the receiver, and the lock lapsing redelivers the message on its own anyway.
                Logger.LogWarning(
                    ex,
                    "Failed to abandon Azure Service Bus message {MessageId} for {Queue}; the lock will lapse and the message will be redelivered.",
                    delivery.MessageId,
                    _queueName);
            }

            return;
        }

        // The delivery now belongs to a background worker, which decrements _pendingCount when it dequeues.
        // Do not touch the counter or abandon here, even if the Complete below fails — the message is already
        // executing in-process and abandoning it would trigger a duplicate execution via redelivery.
        try
        {
            await delivery.CompleteAsync().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            Logger.LogError(
                ex,
                "Failed to complete Azure Service Bus message {MessageId} for {Queue} after enqueue; it is being processed but Service Bus will redeliver it after the lock expires.",
                delivery.MessageId,
                _queueName);
        }
    }

    /// <summary>Releases resources held by this instance.</summary>
    public override async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposeStarted, 1) != 0)
            return;

        Logger.LogInformation(
            "Draining Azure Service Bus ACK-after-enqueue dispatcher for {Queue}. Pending={PendingCount}, Running={RunningCount}.",
            _queueName,
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
                "Timed out while draining Azure Service Bus ACK-after-enqueue dispatcher for {Queue}. Pending={PendingCount}, Running={RunningCount}. Already-completed work may be interrupted by host shutdown.",
                _queueName,
                PendingCount,
                RunningCount);

            _ = Task.WhenAll(_workers).ContinueWith(
                _ => _drainCancellation.Dispose(),
                CancellationToken.None,
                TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default);
        }
    }

    private async Task RunWorkerAsync(int workerIndex)
    {
        await foreach (var delivery in _queue.Reader.ReadAllAsync().ConfigureAwait(false))
        {
            Interlocked.Decrement(ref _pendingCount);
            Interlocked.Increment(ref _runningCount);

            try
            {
                Logger.LogDebug(
                    "Azure Service Bus background worker {WorkerIndex} handling message {MessageId} for {Queue}. Pending={PendingCount}, Running={RunningCount}.",
                    workerIndex,
                    delivery.MessageId,
                    _queueName,
                    PendingCount,
                    RunningCount);
                await ExecuteHandlerAsync(
                    delivery,
                    _drainCancellation.Token,
                    logFailures: false).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Logger.LogError(
                    ex,
                    "Azure Service Bus background handler failed for already-completed message {MessageId} on {Queue}.",
                    delivery.MessageId,
                    _queueName);
                await NotifyBackgroundFailureAsync(
                    delivery,
                    ex,
                    _queueName,
                    _role).ConfigureAwait(false);
            }
            finally
            {
                Interlocked.Decrement(ref _runningCount);
            }
        }
    }
}
