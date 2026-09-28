using AsyncResponse.Transports.GooglePubSub;
using Google.Cloud.PubSub.V1;
using Google.Protobuf;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using System.Collections.Concurrent;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Pins the Google Pub/Sub dispatcher and subscriber paths the broader suites leave unexercised:
/// the inline dispatcher's host-stop Nack and hard-stop hold release, the base dispatcher's no-op
/// dispose, the early-ACK drain's surfacing cut-off, lapsed-worker surfacing and drain fault, a
/// cancelled streaming pull at stop, and a failed client whose best-effort stop also fails.
/// </summary>
public sealed class GooglePubSubCoverageGapTests
{
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(10);

    [Fact]
    public async Task AwaitingDispatcher_HandlerCancelledByTheSubscriberStop_NacksWithoutAFailureLog()
    {
        var logger = new CollectingLogger();
        var options = TransportOptions();
        var dispatcher = new AwaitingGooglePubSubMessageDispatcher(
            (_, cancellationToken) =>
            {
                cancellationToken.ThrowIfCancellationRequested();
                return Task.CompletedTask;
            },
            options,
            options.WorkerSubscriber,
            logger,
            "workers",
            GooglePubSubSubscriberRole.Worker);
        using var stopped = new CancellationTokenSource();
        await stopped.CancelAsync();

        var reply = await dispatcher.HandleAsync(Message("m1"), stopped.Token);

        Assert.Equal(SubscriberClient.Reply.Nack, reply);
        Assert.DoesNotContain(logger.Messages, message => message.Contains("handling failed", StringComparison.Ordinal));
    }

    [Fact]
    public async Task AwaitingDispatcher_DeliveryHeldDuringTheDrain_IsNackedByTheSdkHardStop()
    {
        // A delivery arriving once the stop's drain began is held unstarted for the client stop;
        // when the SDK's own hard stop cancels the callback first, the hold hands it back at once.
        var handled = 0;
        var options = TransportOptions();
        await using var dispatcher = new AwaitingGooglePubSubMessageDispatcher(
            (_, _) =>
            {
                Interlocked.Increment(ref handled);
                return Task.CompletedTask;
            },
            options,
            options.WorkerSubscriber,
            NullLogger.Instance,
            "workers",
            GooglePubSubSubscriberRole.Worker);
        await dispatcher.DrainInFlightAsync(TimeSpan.Zero, TimeProvider.System);
        using var hardStop = new CancellationTokenSource();

        var reply = dispatcher.HandleAsync(Message("m1"), hardStop.Token);
        Assert.False(reply.IsCompleted);

        await hardStop.CancelAsync();

        Assert.Equal(SubscriberClient.Reply.Nack, await reply.WaitAsync(Wait));
        Assert.Equal(0, Volatile.Read(ref handled));
    }

    [Fact]
    public async Task BaseDispatcher_DisposeIsANoOp_AndLeavesHeldDeliveriesForTheClientStop()
    {
        // Only the concrete dispatchers release held deliveries on dispose; the base contract
        // disposes nothing, so a held delivery waits for ReleaseHeldDeliveries.
        var options = TransportOptions();
        var dispatcher = new HoldingDispatcher(options);

        var reply = dispatcher.HandleAsync(Message("m1"), CancellationToken.None);
        Assert.True(dispatcher.DisposeAsync().IsCompletedSuccessfully);
        Assert.False(reply.IsCompleted);

        dispatcher.ReleaseHeldDeliveries();

        Assert.Equal(SubscriberClient.Reply.Nack, await reply.WaitAsync(Wait));
    }

    [Fact]
    public async Task QueuedDispatcher_DrainLapse_SlowCallbackCutsSurfacingShort_ThenAFreedWorkerSurfacesTheRest()
    {
        var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseCallback = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var thirdSurfaced = new TaskCompletionSource<GooglePubSubBackgroundFailureContext>(TaskCreationOptions.RunContinuationsAsynchronously);
        var surfaced = new ConcurrentQueue<string>();
        var handled = new ConcurrentQueue<string>();
        var logger = new CollectingLogger();
        var subscriberOptions = EarlyAck(workers: 1, capacity: 3, drain: TimeSpan.FromMilliseconds(400));
        subscriberOptions.OnBackgroundFailure = async context =>
        {
            surfaced.Enqueue(context.MessageId);
            if (context.MessageId == "m2")
                await releaseCallback.Task;
            else
                thirdSurfaced.TrySetResult(context);
        };
        var dispatcher = (QueuedGooglePubSubMessageDispatcher)GooglePubSubMessageDispatcher.Create(
            async (message, _) =>
            {
                handled.Enqueue(message.MessageId);
                if (message.MessageId == "m1")
                    await releaseFirst.Task;
            },
            TransportOptions(),
            subscriberOptions,
            logger,
            "workers",
            GooglePubSubSubscriberRole.Worker);

        try
        {
            Assert.Equal(SubscriberClient.Reply.Ack, await dispatcher.HandleAsync(Message("m1"), CancellationToken.None));
            await WaitUntilAsync(() => dispatcher.RunningCount == 1);
            Assert.Equal(SubscriberClient.Reply.Ack, await dispatcher.HandleAsync(Message("m2"), CancellationToken.None));
            Assert.Equal(SubscriberClient.Reply.Ack, await dispatcher.HandleAsync(Message("m3"), CancellationToken.None));

            await dispatcher.DisposeAsync().AsTask().WaitAsync(Wait);

            Assert.Equal(["m2"], surfaced.ToArray());
            Assert.Contains(logger.Messages, message => message.Contains("Surfaced 1 via OnBackgroundFailure", StringComparison.Ordinal)
                && message.Contains("1 could not be surfaced", StringComparison.Ordinal));

            releaseFirst.TrySetResult();
            var third = await thirdSurfaced.Task.WaitAsync(Wait);

            Assert.Equal("workers", third.SubscriptionId);
            Assert.IsType<OperationCanceledException>(third.Exception);
            Assert.Equal(["m1"], handled.ToArray());
            await WaitUntilAsync(() => dispatcher.RunningCount == 0);
        }
        finally
        {
            releaseFirst.TrySetResult();
            releaseCallback.TrySetResult();
        }
    }

    [Fact]
    public async Task QueuedDispatcher_ADrainFault_StaysInsideDisposeAsync()
    {
        // A drain budget past the timer ceiling (reachable only by direct construction — the
        // validator refuses it) faults the drain wait itself: logged at Debug, never escaping.
        var logger = new CollectingLogger();
        var dispatcher = new QueuedGooglePubSubMessageDispatcher(
            (_, _) => Task.CompletedTask,
            TransportOptions(),
            EarlyAck(workers: 1, capacity: 1, drain: TimeSpan.MaxValue),
            logger,
            "workers",
            GooglePubSubSubscriberRole.Worker);

        await dispatcher.DisposeAsync();

        Assert.Contains(
            logger.Entries,
            entry => entry.Message.Contains("drain for workers ended with an error", StringComparison.Ordinal)
                && entry.Exception is ArgumentOutOfRangeException);
    }

    [Fact]
    public async Task SubscriberService_StreamingPullCancelledByTheClientStop_EndsGracefullyWithOneStop()
    {
        // The SDK completes the streaming pull as cancelled once the client stops: that
        // OperationCanceledException surfaces after the stop and must end the attempt as a graceful
        // shutdown — no best-effort second stop, no supervisor retry.
        var client = new CancelOnStopClient();
        var logger = new CollectingLogger();
        var subscriber = new GooglePubSubWorkerSubscriber(
            Options.Create(TransportOptions()),
            Mock.Of<IAsyncResponseIngress>(),
            logger.For<GooglePubSubWorkerSubscriber>(),
            (_, _, _) => Task.FromResult<IGooglePubSubSubscriberClient>(client));

        await subscriber.StartAsync(CancellationToken.None);
        await client.Started.Task.WaitAsync(Wait);
        await subscriber.StopAsync(CancellationToken.None).WaitAsync(Wait);

        Assert.Equal(1, client.StopCalls);
        Assert.Equal(SubscriberClient.ShutdownMode.NackImmediately, client.LastShutdownOptions!.Mode);
        Assert.DoesNotContain(logger.Messages, message => message.Contains("retrying in", StringComparison.Ordinal));
        Assert.DoesNotContain(logger.Messages, message => message.Contains("Best-effort stop", StringComparison.Ordinal));
    }

    [Fact]
    public async Task SubscriberService_FailedClientWhoseStopAlsoFails_IsReleasedBestEffort_AndRebuilt()
    {
        var stopFailure = new InvalidOperationException("stop failed");
        var healthy = new CancelOnStopClient();
        var clients = 0;
        var logger = new CollectingLogger();
        var options = TransportOptions();
        options.SubscriberRetryBaseDelay = TimeSpan.FromMilliseconds(1);
        options.SubscriberRetryMaxDelay = TimeSpan.FromMilliseconds(1);
        var subscriber = new GooglePubSubWorkerSubscriber(
            Options.Create(options),
            Mock.Of<IAsyncResponseIngress>(),
            logger.For<GooglePubSubWorkerSubscriber>(),
            (_, _, _) => Task.FromResult<IGooglePubSubSubscriberClient>(
                Interlocked.Increment(ref clients) == 1 ? new FailingClient(stopFailure) : healthy));

        await subscriber.StartAsync(CancellationToken.None);
        await healthy.Started.Task.WaitAsync(Wait);
        await subscriber.StopAsync(CancellationToken.None).WaitAsync(Wait);

        Assert.Equal(2, Volatile.Read(ref clients));
        Assert.Contains(
            logger.Entries,
            entry => entry.Message.Contains("Best-effort stop of a failed Pub/Sub subscriber client did not complete cleanly", StringComparison.Ordinal)
                && ReferenceEquals(entry.Exception, stopFailure));
        Assert.Contains(logger.Messages, message => message.Contains("retrying in", StringComparison.Ordinal));
    }

    private static GooglePubSubAsyncResponseOptions TransportOptions()
        => new()
        {
            ProjectId = "project-a",
            WorkerSubscriptionId = "workers",
            ResponseSubscriptionId = "responses"
        };

    private static GooglePubSubSubscriberOptions EarlyAck(int workers, int capacity, TimeSpan drain)
        => new()
        {
            AckMode = GooglePubSubAckMode.AckAfterEnqueue,
            BackgroundWorkerCount = workers,
            BackgroundQueueCapacity = capacity,
            BackgroundDrainTimeout = drain
        };

    private static PubsubMessage Message(string messageId)
        => new() { MessageId = messageId, Data = ByteString.CopyFromUtf8("{}") };

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        using var cts = new CancellationTokenSource(Wait);
        while (!condition())
            await Task.Delay(10, cts.Token);
    }

    /// <summary>A dispatcher that only holds deliveries, so the base class's own members run.</summary>
    private sealed class HoldingDispatcher(GooglePubSubAsyncResponseOptions options)
        : GooglePubSubMessageDispatcher(
            (_, _) => Task.CompletedTask,
            options,
            options.WorkerSubscriber,
            NullLogger.Instance,
            "workers",
            GooglePubSubSubscriberRole.Worker)
    {
        public override Task<SubscriberClient.Reply> HandleAsync(PubsubMessage message, CancellationToken subscriberCancellationToken)
            => HoldUntilClientStopAsync(subscriberCancellationToken);
    }

    /// <summary>Streams until stopped, then completes the pull as cancelled — the SDK's shape after a stop.</summary>
    private sealed class CancelOnStopClient : IGooglePubSubSubscriberClient
    {
        private readonly TaskCompletionSource _run = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _stopCalls;

        public TaskCompletionSource Started { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public int StopCalls => Volatile.Read(ref _stopCalls);
        public SubscriberClient.ShutdownOptions? LastShutdownOptions { get; private set; }

        public Task StartAsync(Func<PubsubMessage, CancellationToken, Task<SubscriberClient.Reply>> handler)
        {
            Started.TrySetResult();
            return _run.Task;
        }

        public Task StopAsync(SubscriberClient.ShutdownOptions options, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _stopCalls);
            LastShutdownOptions = options;
            _run.TrySetCanceled();
            return Task.CompletedTask;
        }
    }

    /// <summary>A streaming pull that faults at once, and a stop that fails as well.</summary>
    private sealed class FailingClient(Exception stopFailure) : IGooglePubSubSubscriberClient
    {
        public Task StartAsync(Func<PubsubMessage, CancellationToken, Task<SubscriberClient.Reply>> handler)
            => Task.FromException(new InvalidOperationException("streaming pull faulted"));

        public Task StopAsync(SubscriberClient.ShutdownOptions options, CancellationToken cancellationToken)
            => Task.FromException(stopFailure);
    }
}
