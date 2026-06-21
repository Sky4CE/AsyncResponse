using Microsoft.Extensions.Logging;
using System.Diagnostics;
using System.Threading.Channels;

namespace AsyncResponse.Transports.RabbitMQ;

internal enum RabbitMqSubscriberRole
{
    Worker,
    ResponseIngress
}

internal abstract class RabbitMqMessageDispatcher : IAsyncDisposable
{
    private readonly Func<RabbitMqDelivery, CancellationToken, Task> _handler;
    private readonly RabbitMqSubscriberOptions _subscriberOptions;
    private readonly string _queue;
    private readonly RabbitMqSubscriberRole _role;

    protected RabbitMqMessageDispatcher(
        Func<RabbitMqDelivery, CancellationToken, Task> handler,
        RabbitMqAsyncResponseOptions transportOptions,
        RabbitMqSubscriberOptions subscriberOptions,
        ILogger logger,
        string queue,
        RabbitMqSubscriberRole role)
    {
        _handler = handler;
        TransportOptions = transportOptions;
        _subscriberOptions = subscriberOptions;
        Logger = logger;
        _queue = queue;
        _role = role;
    }

    protected RabbitMqAsyncResponseOptions TransportOptions { get; }
    protected ILogger Logger { get; }

    public static RabbitMqMessageDispatcher Create(
        Func<RabbitMqDelivery, CancellationToken, Task> handler,
        RabbitMqAsyncResponseOptions transportOptions,
        RabbitMqSubscriberOptions subscriberOptions,
        ILogger logger,
        string queue,
        RabbitMqSubscriberRole role)
    {
        ValidateOptions(transportOptions, subscriberOptions, role);

        return subscriberOptions.AckMode switch
        {
            RabbitMqAckMode.AckAfterHandlerCompletes => new AwaitingRabbitMqMessageDispatcher(
                handler,
                transportOptions,
                subscriberOptions,
                logger,
                queue,
                role),
            RabbitMqAckMode.AckAfterEnqueue => new QueuedRabbitMqMessageDispatcher(
                handler,
                transportOptions,
                subscriberOptions,
                logger,
                queue,
                role),
            _ => throw new InvalidOperationException(
                $"Unsupported RabbitMQ ACK mode '{subscriberOptions.AckMode}'.")
        };
    }

    public static void ValidateOptions(
        RabbitMqAsyncResponseOptions transportOptions,
        RabbitMqSubscriberOptions subscriberOptions,
        RabbitMqSubscriberRole role)
    {
        var optionPath = role is RabbitMqSubscriberRole.Worker
            ? $"{nameof(RabbitMqAsyncResponseOptions)}.{nameof(RabbitMqAsyncResponseOptions.WorkerSubscriber)}"
            : $"{nameof(RabbitMqAsyncResponseOptions)}.{nameof(RabbitMqAsyncResponseOptions.ResponseSubscriber)}";

        if (subscriberOptions.PrefetchCount == 0)
            throw new InvalidOperationException($"{optionPath}.{nameof(RabbitMqSubscriberOptions.PrefetchCount)} must be positive.");

        switch (subscriberOptions.AckMode)
        {
            case RabbitMqAckMode.AckAfterHandlerCompletes:
                return;

            case RabbitMqAckMode.AckAfterEnqueue:
                if (subscriberOptions.BackgroundWorkerCount <= 0)
                {
                    throw new InvalidOperationException(
                        $"{optionPath}.{nameof(RabbitMqSubscriberOptions.BackgroundWorkerCount)} must be explicitly configured " +
                        $"when {nameof(RabbitMqSubscriberOptions.AckMode)} is {nameof(RabbitMqAckMode.AckAfterEnqueue)}.");
                }

                if (subscriberOptions.BackgroundQueueCapacity <= 0)
                {
                    throw new InvalidOperationException(
                        $"{optionPath}.{nameof(RabbitMqSubscriberOptions.BackgroundQueueCapacity)} must be explicitly configured " +
                        $"when {nameof(RabbitMqSubscriberOptions.AckMode)} is {nameof(RabbitMqAckMode.AckAfterEnqueue)}.");
                }

                if (subscriberOptions.BackgroundDrainTimeout <= TimeSpan.Zero)
                    throw new InvalidOperationException($"{optionPath}.{nameof(RabbitMqSubscriberOptions.BackgroundDrainTimeout)} must be positive.");

                if (transportOptions.ShutdownTimeout <= TimeSpan.Zero)
                    throw new InvalidOperationException($"{nameof(RabbitMqAsyncResponseOptions)}.{nameof(RabbitMqAsyncResponseOptions.ShutdownTimeout)} must be positive.");

                if (transportOptions.HostShutdownTimeout is { } hostShutdownTimeout)
                {
                    if (hostShutdownTimeout <= TimeSpan.Zero)
                        throw new InvalidOperationException($"{nameof(RabbitMqAsyncResponseOptions)}.{nameof(RabbitMqAsyncResponseOptions.HostShutdownTimeout)} must be positive when set.");

                    var requiredShutdownBudget = transportOptions.ShutdownTimeout + subscriberOptions.BackgroundDrainTimeout;
                    if (requiredShutdownBudget > hostShutdownTimeout)
                    {
                        throw new InvalidOperationException(
                            $"{optionPath}.{nameof(RabbitMqSubscriberOptions.BackgroundDrainTimeout)} plus " +
                            $"{nameof(RabbitMqAsyncResponseOptions)}.{nameof(RabbitMqAsyncResponseOptions.ShutdownTimeout)} " +
                            $"requires {requiredShutdownBudget}, which exceeds " +
                            $"{nameof(RabbitMqAsyncResponseOptions)}.{nameof(RabbitMqAsyncResponseOptions.HostShutdownTimeout)} " +
                            $"({hostShutdownTimeout}). Increase Microsoft.Extensions.Hosting.HostOptions.ShutdownTimeout " +
                            $"and mirror that value in {nameof(RabbitMqAsyncResponseOptions)}.{nameof(RabbitMqAsyncResponseOptions.HostShutdownTimeout)}, " +
                            "or reduce the RabbitMQ shutdown/drain timeouts.");
                    }
                }

                return;

            default:
                throw new InvalidOperationException(
                    $"{optionPath}.{nameof(RabbitMqSubscriberOptions.AckMode)} has unsupported value '{subscriberOptions.AckMode}'.");
        }
    }

    public abstract Task HandleAsync(
        RabbitMqDelivery delivery,
        IRabbitMqChannel channel,
        CancellationToken subscriberCancellationToken);

    public virtual ValueTask DisposeAsync() => ValueTask.CompletedTask;

    protected async Task ExecuteHandlerAsync(
        RabbitMqDelivery delivery,
        CancellationToken cancellationToken,
        bool logFailures = true)
    {
        using var activity = AsyncResponseDiagnostics.StartActivity(
            "asyncresponse.rabbitmq.receive",
            ActivityKind.Consumer);
        activity?.SetTag("asyncresponse.transport", "rabbitmq");
        activity?.SetTag("asyncresponse.rabbitmq.role", _role.ToString());
        activity?.SetTag("asyncresponse.rabbitmq.ack_mode", _subscriberOptions.AckMode.ToString());
        activity?.SetTag("messaging.system", "rabbitmq");
        activity?.SetTag("messaging.destination.name", _queue);
        activity?.SetTag("messaging.rabbitmq.exchange", delivery.Exchange);
        activity?.SetTag("messaging.rabbitmq.routing_key", delivery.RoutingKey);
        activity?.SetTag("messaging.rabbitmq.delivery_tag", delivery.DeliveryTag);
        activity?.SetTag("messaging.message.id", delivery.BasicProperties.MessageId);

        if (!string.IsNullOrWhiteSpace(delivery.BasicProperties.CorrelationId))
            AsyncResponseDiagnostics.SetCorrelationId(activity, delivery.BasicProperties.CorrelationId);

        try
        {
            await _handler(delivery, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            if (logFailures)
                Logger.LogError(ex, "RabbitMQ message handling failed for delivery {DeliveryTag}.", delivery.DeliveryTag);
            AsyncResponseDiagnostics.SetError(activity, ex);
            throw;
        }
    }

    protected async ValueTask NotifyBackgroundFailureAsync(
        RabbitMqDelivery delivery,
        Exception exception,
        string queue,
        RabbitMqSubscriberRole role)
    {
        var callback = _subscriberOptions.OnBackgroundFailure;
        if (callback is null)
            return;

        try
        {
            await callback(new RabbitMqBackgroundFailureContext(
                queue,
                role.ToString(),
                delivery.Exchange,
                delivery.RoutingKey,
                delivery.DeliveryTag,
                exception)).ConfigureAwait(false);
        }
        catch (Exception callbackException)
        {
            Logger.LogError(
                callbackException,
                "RabbitMQ background failure callback failed for already-ACKed delivery {DeliveryTag} on {Queue}.",
                delivery.DeliveryTag,
                queue);
        }
    }
}

internal sealed class AwaitingRabbitMqMessageDispatcher(
    Func<RabbitMqDelivery, CancellationToken, Task> handler,
    RabbitMqAsyncResponseOptions transportOptions,
    RabbitMqSubscriberOptions subscriberOptions,
    ILogger logger,
    string queue,
    RabbitMqSubscriberRole role)
    : RabbitMqMessageDispatcher(handler, transportOptions, subscriberOptions, logger, queue, role)
{
    public override async Task HandleAsync(
        RabbitMqDelivery delivery,
        IRabbitMqChannel channel,
        CancellationToken subscriberCancellationToken)
    {
        try
        {
            await ExecuteHandlerAsync(delivery, subscriberCancellationToken).ConfigureAwait(false);
            await channel.BasicAckAsync(delivery.DeliveryTag, subscriberCancellationToken).ConfigureAwait(false);
        }
        catch
        {
            await channel.BasicNackAsync(delivery.DeliveryTag, requeue: true, CancellationToken.None).ConfigureAwait(false);
        }
    }
}

internal sealed class QueuedRabbitMqMessageDispatcher : RabbitMqMessageDispatcher
{
    private readonly Channel<RabbitMqDelivery> _queue;
    private readonly Task[] _workers;
    private readonly CancellationTokenSource _drainCancellation = new();
    private readonly TimeSpan _drainTimeout;
    private readonly string _queueName;
    private readonly RabbitMqSubscriberRole _role;
    private int _pendingCount;
    private int _runningCount;
    private int _disposeStarted;

    public QueuedRabbitMqMessageDispatcher(
        Func<RabbitMqDelivery, CancellationToken, Task> handler,
        RabbitMqAsyncResponseOptions transportOptions,
        RabbitMqSubscriberOptions subscriberOptions,
        ILogger logger,
        string queue,
        RabbitMqSubscriberRole role)
        : base(handler, transportOptions, subscriberOptions, logger, queue, role)
    {
        _drainTimeout = subscriberOptions.BackgroundDrainTimeout;
        _queueName = queue;
        _role = role;
        _queue = Channel.CreateBounded<RabbitMqDelivery>(new BoundedChannelOptions(subscriberOptions.BackgroundQueueCapacity)
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
            "Created RabbitMQ ACK-after-enqueue dispatcher for {Queue} with {WorkerCount} worker(s), queue capacity {QueueCapacity}, drain timeout {DrainTimeout}.",
            _queueName,
            subscriberOptions.BackgroundWorkerCount,
            subscriberOptions.BackgroundQueueCapacity,
            _drainTimeout);
    }

    internal int PendingCount => Volatile.Read(ref _pendingCount);
    internal int RunningCount => Volatile.Read(ref _runningCount);

    public override async Task HandleAsync(
        RabbitMqDelivery delivery,
        IRabbitMqChannel channel,
        CancellationToken subscriberCancellationToken)
    {
        try
        {
            Interlocked.Increment(ref _pendingCount);
            if (_queue.Writer.TryWrite(delivery))
            {
                await channel.BasicAckAsync(delivery.DeliveryTag, subscriberCancellationToken).ConfigureAwait(false);
                return;
            }

            Interlocked.Decrement(ref _pendingCount);
            Logger.LogWarning(
                "RabbitMQ background queue rejected delivery {DeliveryTag} for {Queue}; returning NACK. Pending={PendingCount}, Running={RunningCount}.",
                delivery.DeliveryTag,
                _queueName,
                PendingCount,
                RunningCount);
            await channel.BasicNackAsync(delivery.DeliveryTag, requeue: true, subscriberCancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            Interlocked.Decrement(ref _pendingCount);
            Logger.LogError(ex, "Failed to enqueue RabbitMQ delivery {DeliveryTag} for {Queue}; returning NACK.", delivery.DeliveryTag, _queueName);
            await channel.BasicNackAsync(delivery.DeliveryTag, requeue: true, CancellationToken.None).ConfigureAwait(false);
        }
    }

    public override async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposeStarted, 1) != 0)
            return;

        Logger.LogInformation(
            "Draining RabbitMQ ACK-after-enqueue dispatcher for {Queue}. Pending={PendingCount}, Running={RunningCount}.",
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
                "Timed out while draining RabbitMQ ACK-after-enqueue dispatcher for {Queue}. Pending={PendingCount}, Running={RunningCount}. Already ACKed work may be interrupted by host shutdown.",
                _queueName,
                PendingCount,
                RunningCount);
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
                await ExecuteHandlerAsync(
                    delivery,
                    _drainCancellation.Token,
                    logFailures: false).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Logger.LogError(
                    ex,
                    "RabbitMQ background handler failed for already-ACKed delivery {DeliveryTag} on {Queue}.",
                    delivery.DeliveryTag,
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
