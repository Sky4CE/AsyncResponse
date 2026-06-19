using Google.Cloud.PubSub.V1;
using Microsoft.Extensions.Logging;
using System.Diagnostics;
using System.Threading.Channels;

namespace AsyncResponse.Transports.GooglePubSub;

internal enum GooglePubSubSubscriberRole
{
    Worker,
    ResponseIngress
}

internal abstract class GooglePubSubMessageDispatcher : IAsyncDisposable
{
    private readonly Func<PubsubMessage, CancellationToken, Task> _handler;
    private readonly GooglePubSubAsyncResponseOptions _transportOptions;
    private readonly GooglePubSubSubscriberOptions _subscriberOptions;
    private readonly string _subscriptionId;
    private readonly GooglePubSubSubscriberRole _role;

    protected GooglePubSubMessageDispatcher(
        Func<PubsubMessage, CancellationToken, Task> handler,
        GooglePubSubAsyncResponseOptions transportOptions,
        GooglePubSubSubscriberOptions subscriberOptions,
        ILogger logger,
        string subscriptionId,
        GooglePubSubSubscriberRole role)
    {
        _handler = handler;
        _transportOptions = transportOptions;
        _subscriberOptions = subscriberOptions;
        Logger = logger;
        _subscriptionId = subscriptionId;
        _role = role;
    }

    protected ILogger Logger { get; }

    public static GooglePubSubMessageDispatcher Create(
        Func<PubsubMessage, CancellationToken, Task> handler,
        GooglePubSubAsyncResponseOptions transportOptions,
        GooglePubSubSubscriberOptions subscriberOptions,
        ILogger logger,
        string subscriptionId,
        GooglePubSubSubscriberRole role)
    {
        ValidateOptions(subscriberOptions, role);

        return subscriberOptions.AckMode switch
        {
            GooglePubSubAckMode.AckAfterHandlerCompletes => new AwaitingGooglePubSubMessageDispatcher(
                handler,
                transportOptions,
                subscriberOptions,
                logger,
                subscriptionId,
                role),
            GooglePubSubAckMode.AckAfterEnqueue => new QueuedGooglePubSubMessageDispatcher(
                handler,
                transportOptions,
                subscriberOptions,
                logger,
                subscriptionId,
                role),
            _ => throw new InvalidOperationException(
                $"Unsupported Google Pub/Sub ACK mode '{subscriberOptions.AckMode}'.")
        };
    }

    public static void ValidateOptions(GooglePubSubSubscriberOptions subscriberOptions, GooglePubSubSubscriberRole role)
    {
        var optionPath = role is GooglePubSubSubscriberRole.Worker
            ? $"{nameof(GooglePubSubAsyncResponseOptions)}.{nameof(GooglePubSubAsyncResponseOptions.WorkerSubscriber)}"
            : $"{nameof(GooglePubSubAsyncResponseOptions)}.{nameof(GooglePubSubAsyncResponseOptions.ResponseSubscriber)}";

        switch (subscriberOptions.AckMode)
        {
            case GooglePubSubAckMode.AckAfterHandlerCompletes:
                return;

            case GooglePubSubAckMode.AckAfterEnqueue:
                if (subscriberOptions.BackgroundWorkerCount <= 0)
                {
                    throw new InvalidOperationException(
                        $"{optionPath}.{nameof(GooglePubSubSubscriberOptions.BackgroundWorkerCount)} must be explicitly configured " +
                        $"when {nameof(GooglePubSubSubscriberOptions.AckMode)} is {nameof(GooglePubSubAckMode.AckAfterEnqueue)}.");
                }

                if (subscriberOptions.BackgroundQueueCapacity <= 0)
                {
                    throw new InvalidOperationException(
                        $"{optionPath}.{nameof(GooglePubSubSubscriberOptions.BackgroundQueueCapacity)} must be explicitly configured " +
                        $"when {nameof(GooglePubSubSubscriberOptions.AckMode)} is {nameof(GooglePubSubAckMode.AckAfterEnqueue)}.");
                }

                if (subscriberOptions.BackgroundDrainTimeout <= TimeSpan.Zero)
                {
                    throw new InvalidOperationException(
                        $"{optionPath}.{nameof(GooglePubSubSubscriberOptions.BackgroundDrainTimeout)} must be positive.");
                }

                return;

            default:
                throw new InvalidOperationException(
                    $"{optionPath}.{nameof(GooglePubSubSubscriberOptions.AckMode)} has unsupported value '{subscriberOptions.AckMode}'.");
        }
    }

    public abstract Task<SubscriberClient.Reply> HandleAsync(
        PubsubMessage message,
        CancellationToken subscriberCancellationToken);

    public virtual ValueTask DisposeAsync() => ValueTask.CompletedTask;

    protected async Task ExecuteHandlerAsync(PubsubMessage message, CancellationToken cancellationToken)
    {
        using var activity = AsyncResponseDiagnostics.StartActivity(
            "asyncresponse.pubsub.receive",
            ActivityKind.Consumer);
        activity?.SetTag("asyncresponse.transport", "google_pubsub");
        activity?.SetTag("asyncresponse.pubsub.role", _role.ToString());
        activity?.SetTag("asyncresponse.pubsub.ack_mode", _subscriberOptions.AckMode.ToString());
        activity?.SetTag("messaging.system", "gcp_pubsub");
        activity?.SetTag("messaging.destination.name", _subscriptionId);
        activity?.SetTag("messaging.message.id", message.MessageId);

        if (message.Attributes.TryGetValue(_transportOptions.CorrelationIdAttribute, out var correlationId))
            AsyncResponseDiagnostics.SetCorrelationId(activity, correlationId);

        try
        {
            await _handler(message, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Pub/Sub message handling failed for message {MessageId}.", message.MessageId);
            AsyncResponseDiagnostics.SetError(activity, ex);
            throw;
        }
    }
}

internal sealed class AwaitingGooglePubSubMessageDispatcher(
    Func<PubsubMessage, CancellationToken, Task> handler,
    GooglePubSubAsyncResponseOptions transportOptions,
    GooglePubSubSubscriberOptions subscriberOptions,
    ILogger logger,
    string subscriptionId,
    GooglePubSubSubscriberRole role)
    : GooglePubSubMessageDispatcher(handler, transportOptions, subscriberOptions, logger, subscriptionId, role)
{
    public override async Task<SubscriberClient.Reply> HandleAsync(
        PubsubMessage message,
        CancellationToken subscriberCancellationToken)
    {
        try
        {
            await ExecuteHandlerAsync(message, subscriberCancellationToken).ConfigureAwait(false);
            return SubscriberClient.Reply.Ack;
        }
        catch
        {
            return SubscriberClient.Reply.Nack;
        }
    }
}

internal sealed class QueuedGooglePubSubMessageDispatcher : GooglePubSubMessageDispatcher
{
    private readonly Channel<PubsubMessage> _queue;
    private readonly Task[] _workers;
    private readonly TimeSpan _drainTimeout;
    private readonly string _subscriptionId;
    private int _pendingCount;
    private int _runningCount;
    private int _disposeStarted;

    public QueuedGooglePubSubMessageDispatcher(
        Func<PubsubMessage, CancellationToken, Task> handler,
        GooglePubSubAsyncResponseOptions transportOptions,
        GooglePubSubSubscriberOptions subscriberOptions,
        ILogger logger,
        string subscriptionId,
        GooglePubSubSubscriberRole role)
        : base(handler, transportOptions, subscriberOptions, logger, subscriptionId, role)
    {
        _drainTimeout = subscriberOptions.BackgroundDrainTimeout;
        _subscriptionId = subscriptionId;
        _queue = Channel.CreateBounded<PubsubMessage>(new BoundedChannelOptions(subscriberOptions.BackgroundQueueCapacity)
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
            "Created Pub/Sub ACK-after-enqueue dispatcher for {SubscriptionId} with {WorkerCount} worker(s), queue capacity {QueueCapacity}, drain timeout {DrainTimeout}.",
            _subscriptionId,
            subscriberOptions.BackgroundWorkerCount,
            subscriberOptions.BackgroundQueueCapacity,
            _drainTimeout);
    }

    internal int PendingCount => Volatile.Read(ref _pendingCount);
    internal int RunningCount => Volatile.Read(ref _runningCount);

    public override Task<SubscriberClient.Reply> HandleAsync(
        PubsubMessage message,
        CancellationToken subscriberCancellationToken)
    {
        try
        {
            Interlocked.Increment(ref _pendingCount);
            if (_queue.Writer.TryWrite(message))
            {
                Logger.LogDebug(
                    "Enqueued Pub/Sub message {MessageId} for background handling on {SubscriptionId}. Pending={PendingCount}, Running={RunningCount}.",
                    message.MessageId,
                    _subscriptionId,
                    PendingCount,
                    RunningCount);
                return Task.FromResult(SubscriberClient.Reply.Ack);
            }

            Interlocked.Decrement(ref _pendingCount);
            Logger.LogWarning(
                "Pub/Sub background queue rejected message {MessageId} for {SubscriptionId}; returning NACK. Pending={PendingCount}, Running={RunningCount}.",
                message.MessageId,
                _subscriptionId,
                PendingCount,
                RunningCount);
            return Task.FromResult(SubscriberClient.Reply.Nack);
        }
        catch (Exception ex)
        {
            Interlocked.Decrement(ref _pendingCount);
            Logger.LogError(ex, "Failed to enqueue Pub/Sub message {MessageId} for {SubscriptionId}; returning NACK.", message.MessageId, _subscriptionId);
            return Task.FromResult(SubscriberClient.Reply.Nack);
        }
    }

    public override async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposeStarted, 1) != 0)
            return;

        Logger.LogInformation(
            "Draining Pub/Sub ACK-after-enqueue dispatcher for {SubscriptionId}. Pending={PendingCount}, Running={RunningCount}.",
            _subscriptionId,
            PendingCount,
            RunningCount);
        _queue.Writer.TryComplete();

        try
        {
            await Task.WhenAll(_workers).WaitAsync(_drainTimeout).ConfigureAwait(false);
            Logger.LogInformation(
                "Drained Pub/Sub ACK-after-enqueue dispatcher for {SubscriptionId}. Pending={PendingCount}, Running={RunningCount}.",
                _subscriptionId,
                PendingCount,
                RunningCount);
        }
        catch (TimeoutException ex)
        {
            Logger.LogWarning(
                ex,
                "Timed out while draining Pub/Sub ACK-after-enqueue dispatcher for {SubscriptionId}. Pending={PendingCount}, Running={RunningCount}. Already ACKed work may be interrupted by host shutdown.",
                _subscriptionId,
                PendingCount,
                RunningCount);
        }
    }

    private async Task RunWorkerAsync(int workerIndex)
    {
        await foreach (var message in _queue.Reader.ReadAllAsync().ConfigureAwait(false))
        {
            Interlocked.Decrement(ref _pendingCount);
            Interlocked.Increment(ref _runningCount);

            try
            {
                Logger.LogDebug(
                    "Pub/Sub background worker {WorkerIndex} handling message {MessageId} for {SubscriptionId}. Pending={PendingCount}, Running={RunningCount}.",
                    workerIndex,
                    message.MessageId,
                    _subscriptionId,
                    PendingCount,
                    RunningCount);
                await ExecuteHandlerAsync(message, CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Logger.LogError(
                    ex,
                    "Pub/Sub background handler failed for already-ACKed message {MessageId} on {SubscriptionId}.",
                    message.MessageId,
                    _subscriptionId);
            }
            finally
            {
                Interlocked.Decrement(ref _runningCount);
            }
        }
    }
}
