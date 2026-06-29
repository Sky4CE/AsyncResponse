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

    /// <summary>Runs the GooglePubSubMessageDispatcher operation.</summary>
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

    /// <summary>Creates the configured dispatcher.</summary>
    public static GooglePubSubMessageDispatcher Create(
        Func<PubsubMessage, CancellationToken, Task> handler,
        GooglePubSubAsyncResponseOptions transportOptions,
        GooglePubSubSubscriberOptions subscriberOptions,
        ILogger logger,
        string subscriptionId,
        GooglePubSubSubscriberRole role)
    {
        ValidateOptions(transportOptions, subscriberOptions, role);

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

    /// <summary>Validates the supplied options.</summary>
    public static void ValidateOptions(
        GooglePubSubAsyncResponseOptions transportOptions,
        GooglePubSubSubscriberOptions subscriberOptions,
        GooglePubSubSubscriberRole role)
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

                if (transportOptions.ShutdownTimeout <= TimeSpan.Zero)
                {
                    throw new InvalidOperationException(
                        $"{nameof(GooglePubSubAsyncResponseOptions)}.{nameof(GooglePubSubAsyncResponseOptions.ShutdownTimeout)} must be positive.");
                }

                if (transportOptions.HostShutdownTimeout is { } hostShutdownTimeout)
                {
                    if (hostShutdownTimeout <= TimeSpan.Zero)
                    {
                        throw new InvalidOperationException(
                            $"{nameof(GooglePubSubAsyncResponseOptions)}.{nameof(GooglePubSubAsyncResponseOptions.HostShutdownTimeout)} must be positive when set.");
                    }

                    var requiredShutdownBudget = transportOptions.ShutdownTimeout + subscriberOptions.BackgroundDrainTimeout;
                    if (requiredShutdownBudget > hostShutdownTimeout)
                    {
                        throw new InvalidOperationException(
                            $"{optionPath}.{nameof(GooglePubSubSubscriberOptions.BackgroundDrainTimeout)} plus " +
                            $"{nameof(GooglePubSubAsyncResponseOptions)}.{nameof(GooglePubSubAsyncResponseOptions.ShutdownTimeout)} " +
                            $"requires {requiredShutdownBudget}, which exceeds " +
                            $"{nameof(GooglePubSubAsyncResponseOptions)}.{nameof(GooglePubSubAsyncResponseOptions.HostShutdownTimeout)} " +
                            $"({hostShutdownTimeout}). Increase Microsoft.Extensions.Hosting.HostOptions.ShutdownTimeout " +
                            $"and mirror that value in {nameof(GooglePubSubAsyncResponseOptions)}.{nameof(GooglePubSubAsyncResponseOptions.HostShutdownTimeout)}, " +
                            "or reduce the Pub/Sub shutdown/drain timeouts.");
                    }
                }

                return;

            default:
                throw new InvalidOperationException(
                    $"{optionPath}.{nameof(GooglePubSubSubscriberOptions.AckMode)} has unsupported value '{subscriberOptions.AckMode}'.");
        }
    }

    /// <summary>Handles the delivered message.</summary>
    public abstract Task<SubscriberClient.Reply> HandleAsync(
        PubsubMessage message,
        CancellationToken subscriberCancellationToken);

    /// <summary>Releases resources held by this instance.</summary>
    public virtual ValueTask DisposeAsync() => ValueTask.CompletedTask;

    /// <summary>Runs the ExecuteHandlerAsync operation.</summary>
    protected async Task ExecuteHandlerAsync(
        PubsubMessage message,
        CancellationToken cancellationToken,
        bool logFailures = true)
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
            if (logFailures)
                Logger.LogError(ex, "Pub/Sub message handling failed for message {MessageId}.", message.MessageId);
            AsyncResponseDiagnostics.SetError(activity, ex);
            throw;
        }
    }

    /// <summary>Runs the NotifyBackgroundFailureAsync operation.</summary>
    protected async ValueTask NotifyBackgroundFailureAsync(
        PubsubMessage message,
        Exception exception,
        string subscriptionId,
        GooglePubSubSubscriberRole role)
    {
        var callback = _subscriberOptions.OnBackgroundFailure;
        if (callback is null)
            return;

        try
        {
            await callback(new GooglePubSubBackgroundFailureContext(
                subscriptionId,
                role.ToString(),
                message,
                exception)).ConfigureAwait(false);
        }
        catch (Exception callbackException)
        {
            Logger.LogError(
                callbackException,
                "Pub/Sub background failure callback failed for already-ACKed message {MessageId} on {SubscriptionId}.",
                message.MessageId,
                subscriptionId);
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
    /// <summary>Handles the delivered message.</summary>
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
    private readonly CancellationTokenSource _drainCancellation = new();
    private readonly TimeSpan _drainTimeout;
    private readonly string _subscriptionId;
    private readonly GooglePubSubSubscriberRole _role;
    private int _pendingCount;
    private int _runningCount;
    private int _disposeStarted;

    /// <summary>Runs the QueuedGooglePubSubMessageDispatcher operation.</summary>
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
        _role = role;
        _queue = Channel.CreateBounded<PubsubMessage>(new BoundedChannelOptions(subscriberOptions.BackgroundQueueCapacity)
        {
            AllowSynchronousContinuations = false,
            // TryWrite never waits; NACK-on-full comes from TryWrite returning false. Keep Wait so
            // any future WriteAsync usage would apply backpressure instead of dropping messages.
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

    /// <summary>Handles the delivered message.</summary>
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

    /// <summary>Releases resources held by this instance.</summary>
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
            _drainCancellation.Dispose();
            Logger.LogInformation(
                "Drained Pub/Sub ACK-after-enqueue dispatcher for {SubscriptionId}. Pending={PendingCount}, Running={RunningCount}.",
                _subscriptionId,
                PendingCount,
                RunningCount);
        }
        catch (TimeoutException ex)
        {
            _drainCancellation.Cancel();
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
                await ExecuteHandlerAsync(
                    message,
                    _drainCancellation.Token,
                    logFailures: false).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Logger.LogError(
                    ex,
                    "Pub/Sub background handler failed for already-ACKed message {MessageId} on {SubscriptionId}.",
                    message.MessageId,
                    _subscriptionId);
                await NotifyBackgroundFailureAsync(
                    message,
                    ex,
                    _subscriptionId,
                    _role).ConfigureAwait(false);
            }
            finally
            {
                Interlocked.Decrement(ref _runningCount);
            }
        }
    }
}
