using AsyncResponse.Transports.RabbitMQ;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using RabbitMQ.Client;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Settlement edges of the RabbitMQ dispatchers, publish-channel reuse in the worker transport and
/// header rendering in the correlation-id extractor that the main RabbitMQ suites do not reach:
/// failed ACKs after a handler or a park, a stop landing inside the park backoff, dead-letter copies
/// that wait for (or give up on) the next attempt's channel, the shutdown reserve lapsing on the
/// publish gate, and the drain's handling of a worker that faulted outside its handler guard.
/// </summary>
public class RabbitMqCoverageGapTests
{
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(10);

    // ---------- AwaitingRabbitMqMessageDispatcher ----------

    [Fact]
    public async Task Awaiting_AckFailureAfterASuccessfulHandler_IsLoggedAndNeverNacked()
    {
        // A NACK here would requeue work whose side effects already ran; the un-ACKed delivery is
        // redelivered by the channel close instead, and the failure is only logged.
        var channel = new GapChannel { ThrowOnAck = new InvalidOperationException("ack boom") };
        var logger = new GapLogger();
        var runs = 0;
        await using var dispatcher = RabbitMqMessageDispatcher.Create(
            (_, _) =>
            {
                Interlocked.Increment(ref runs);
                return Task.CompletedTask;
            },
            new RabbitMqAsyncResponseOptions(),
            new RabbitMqSubscriberOptions { MaxDeliveryAttempts = 3 },
            logger,
            "worker.q",
            RabbitMqSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery("payload", deliveryTag: 11), channel, CancellationToken.None);

        Assert.Equal(1, runs);
        Assert.Empty(channel.Nacks);
        Assert.Empty(channel.Acks);
        var entry = Assert.Single(logger.Entries, e => e.Message.Contains("Failed to ACK RabbitMQ delivery 11", StringComparison.Ordinal));
        Assert.Equal(LogLevel.Error, entry.Level);
        Assert.Equal("ack boom", entry.Exception?.Message);
    }

    [Fact]
    public async Task Awaiting_NegativeDrainTimeout_ClampsToNoInFlightWait()
    {
        // The ack-after-handler mode does not validate BackgroundDrainTimeout, so a negative value
        // must clamp to "do not wait" rather than reach WaitAsync (which rejects it).
        var channel = new GapChannel();
        var logger = new GapLogger();
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var dispatcher = RabbitMqMessageDispatcher.Create(
            async (_, _) =>
            {
                started.TrySetResult();
                await release.Task;
            },
            new RabbitMqAsyncResponseOptions { HostShutdownTimeout = null },
            new RabbitMqSubscriberOptions { BackgroundDrainTimeout = TimeSpan.FromSeconds(-1) },
            logger,
            "worker.q",
            RabbitMqSubscriberRole.Worker);

        var handling = dispatcher.HandleAsync(Delivery("payload", deliveryTag: 3), channel, CancellationToken.None);
        await started.Task.WaitAsync(Wait);

        var dispose = dispatcher.DisposeAsync();
        Assert.True(dispose.IsCompletedSuccessfully, "a clamped (zero) in-flight wait must not wait for the running handler");
        await dispose;

        Assert.Contains(logger.Entries, e => e.Level == LogLevel.Debug && e.Message.Contains("Not waiting for the RabbitMQ handler", StringComparison.Ordinal));
        Assert.DoesNotContain(logger.Entries, e => e.Message.Contains("Waiting up to", StringComparison.Ordinal));
        // Not a configuration the startup notice is about (BackgroundDrainTimeout is not positive).
        Assert.DoesNotContain(logger.Entries, e => e.Message.Contains("will not wait for a running handler", StringComparison.Ordinal));

        release.SetResult();
        await handling.WaitAsync(Wait);
        Assert.Equal([3UL], channel.Acks);
    }

    [Fact]
    public async Task Awaiting_PreExecutionCap_OnAClosedChannel_ParksNothingAndSettlesNothing()
    {
        // Past the cap with x-death present the delivery is parked — but a closed channel already
        // requeued it, so nothing is published, ACKed or NACKed, and the handler never runs.
        var channel = new GapChannel { Open = false };
        var runs = 0;
        await using var dispatcher = RabbitMqMessageDispatcher.Create(
            (_, _) =>
            {
                Interlocked.Increment(ref runs);
                return Task.CompletedTask;
            },
            new RabbitMqAsyncResponseOptions { ParkQueue = "park.q" },
            new RabbitMqSubscriberOptions { MaxDeliveryAttempts = 2 },
            new GapLogger(),
            "worker.q",
            RabbitMqSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery("payload", DeathCount(2), deliveryTag: 4, redelivered: true), channel, CancellationToken.None);

        Assert.Equal(0, runs);
        Assert.Empty(channel.Publishes);
        Assert.Empty(channel.Acks);
        Assert.Empty(channel.Nacks);
    }

    [Fact]
    public async Task Awaiting_FailedPark_WhenTheStopCutsTheBackoffShort_StillRequeues()
    {
        // The park backoff is skipped on stop, never the requeue: without the NACK the delivery would
        // pin a prefetch credit on an open channel until it closed.
        using var stop = new CancellationTokenSource();
        var channel = new GapChannel
        {
            OnPublish = (_, _) =>
            {
                stop.Cancel();
                throw new InvalidOperationException("park publish boom");
            }
        };
        var logger = new GapLogger();
        await using var dispatcher = RabbitMqMessageDispatcher.Create(
            (_, _) => Task.CompletedTask,
            new RabbitMqAsyncResponseOptions
            {
                ParkQueue = "park.q",
                // A backoff the test would notice: it only ends early because the stop cancels it.
                SubscriberRetryBaseDelay = TimeSpan.FromMinutes(1),
                SubscriberRetryMaxDelay = TimeSpan.FromMinutes(1)
            },
            new RabbitMqSubscriberOptions { MaxDeliveryAttempts = 2 },
            logger,
            "worker.q",
            RabbitMqSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery("payload", DeathCount(2), deliveryTag: 8, redelivered: true), channel, stop.Token)
            .WaitAsync(Wait);

        Assert.Equal([(8UL, true)], channel.Nacks);
        Assert.Empty(channel.Acks);
        Assert.Contains(logger.Entries, e => e.Level == LogLevel.Error && e.Message.Contains("Failed to park capped RabbitMQ delivery 8", StringComparison.Ordinal));
    }

    [Fact]
    public async Task Awaiting_ParkThatLandsButCannotBeAcked_IsAWarningNotARequeue()
    {
        var channel = new GapChannel { ThrowOnAck = new InvalidOperationException("ack boom") };
        var logger = new GapLogger();
        await using var dispatcher = RabbitMqMessageDispatcher.Create(
            (_, _) => Task.CompletedTask,
            new RabbitMqAsyncResponseOptions { ParkQueue = "park.q" },
            new RabbitMqSubscriberOptions { MaxDeliveryAttempts = 2 },
            logger,
            "worker.q",
            RabbitMqSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery("payload", DeathCount(2), deliveryTag: 9, redelivered: true), channel, CancellationToken.None);

        var parked = Assert.Single(channel.Publishes);
        Assert.Equal(string.Empty, parked.Exchange);
        Assert.Equal("park.q", parked.RoutingKey);
        Assert.Empty(channel.Nacks);
        var entry = Assert.Single(logger.Entries, e => e.Message.Contains("Failed to ACK parked RabbitMQ delivery 9", StringComparison.Ordinal));
        Assert.Equal(LogLevel.Warning, entry.Level);
    }

    // ---------- QueuedRabbitMqMessageDispatcher: dead-letter routing ----------

    [Fact]
    public async Task Queued_HandlerFailure_OnACopyWhoseSourceHeaderIsReadOnlyMemory_IsParkedNotCycled()
    {
        // The source-queue marker may reach the dispatcher as ReadOnlyMemory<byte>; it must be
        // recognized exactly like the string and byte[] shapes, or the copy rides the cycle again.
        var headers = new Dictionary<string, object?>
        {
            [RabbitMqMessageDispatcher.DeadLetterSourceQueueHeader] = new ReadOnlyMemory<byte>(Encoding.UTF8.GetBytes("worker.q"))
        };

        var channel = await RunFailingEarlyAckDeliveryAsync(new BasicProperties { Headers = headers });

        var copy = Assert.Single(channel.Publishes);
        Assert.Equal(string.Empty, copy.Exchange);
        Assert.Equal("park.q", copy.RoutingKey);
    }

    [Fact]
    public async Task Queued_HandlerFailure_OnASourceHeaderOfAnUnknownShape_IsDeadLetteredAsAFirstFailure()
    {
        // A marker of a shape the dispatcher does not decode is not proof of a cycle: the copy goes
        // to the dead-letter exchange, not the park queue.
        var headers = new Dictionary<string, object?>
        {
            [RabbitMqMessageDispatcher.DeadLetterSourceQueueHeader] = 42
        };

        var channel = await RunFailingEarlyAckDeliveryAsync(new BasicProperties { Headers = headers });

        var copy = Assert.Single(channel.Publishes);
        Assert.Equal("dlx", copy.Exchange);
        Assert.Equal("route", copy.RoutingKey);
    }

    private static async Task<GapChannel> RunFailingEarlyAckDeliveryAsync(BasicProperties properties)
    {
        var channel = new GapChannel();
        var notified = new TaskCompletionSource<RabbitMqBackgroundFailureContext>(TaskCreationOptions.RunContinuationsAsynchronously);
        var subscriber = EnqueueSubscriber();
        subscriber.OnBackgroundFailure = context =>
        {
            notified.TrySetResult(context);
            return ValueTask.CompletedTask;
        };

        await using var dispatcher = RabbitMqMessageDispatcher.Create(
            (_, _) => throw new InvalidOperationException("handler boom"),
            new RabbitMqAsyncResponseOptions { DeadLetterExchange = "dlx", ParkQueue = "park.q" },
            subscriber,
            new GapLogger(),
            "worker.q",
            RabbitMqSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery("payload", properties, deliveryTag: 21), channel, CancellationToken.None);
        var context = await notified.Task.WaitAsync(Wait);
        Assert.Equal(21UL, context.DeliveryTag);
        return channel;
    }

    [Fact]
    public async Task Queued_DeadLetter_WithNoOpenChannelAttachedInTime_GivesUpAndStillNotifies()
    {
        // The channel the delivery arrived on closed and no subscriber attempt attached a new one
        // within the (short) channel wait: no copy, one error, and OnBackgroundFailure still runs.
        var channel = new GapChannel();
        var logger = new GapLogger();
        var notified = new TaskCompletionSource<RabbitMqBackgroundFailureContext>(TaskCreationOptions.RunContinuationsAsynchronously);
        var subscriber = EnqueueSubscriber();
        subscriber.OnBackgroundFailure = context =>
        {
            notified.TrySetResult(context);
            return ValueTask.CompletedTask;
        };

        await using var dispatcher = RabbitMqMessageDispatcher.Create(
            (_, _) =>
            {
                channel.Open = false;
                throw new InvalidOperationException("handler boom");
            },
            new RabbitMqAsyncResponseOptions
            {
                DeadLetterExchange = "dlx",
                SubscriberRetryBaseDelay = TimeSpan.FromMilliseconds(10),
                SubscriberRetryMaxDelay = TimeSpan.FromMilliseconds(20)
            },
            subscriber,
            logger,
            "worker.q",
            RabbitMqSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery("payload", deliveryTag: 5), channel, CancellationToken.None);
        var context = await notified.Task.WaitAsync(Wait);

        Assert.Equal(5UL, context.DeliveryTag);
        Assert.Empty(channel.Publishes);
        Assert.Contains(logger.Entries, e => e.Level == LogLevel.Error
            && e.Message.Contains("Cannot dead-letter already-ACKed RabbitMQ delivery 5", StringComparison.Ordinal)
            && e.Message.Contains("no new one was attached", StringComparison.Ordinal));
    }

    [Fact]
    public async Task Queued_ChannelDyingUnderTheDeadLetterPublish_RetriesOnTheNextAttemptsChannel()
    {
        // The attached channel dies mid-publish: the copy waits for the NEXT attach (not the one
        // that already happened) and lands on the rebuilt attempt's channel.
        var first = new GapChannel();
        first.OnPublish = (_, _) =>
        {
            first.Open = false;
            throw new InvalidOperationException("channel closed under the publish");
        };
        var second = new GapChannel();
        var logger = new GapLogger();
        var notified = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var subscriber = EnqueueSubscriber();
        subscriber.OnBackgroundFailure = _ =>
        {
            notified.TrySetResult();
            return ValueTask.CompletedTask;
        };

        await using var dispatcher = RabbitMqMessageDispatcher.Create(
            (_, _) => throw new InvalidOperationException("handler boom"),
            new RabbitMqAsyncResponseOptions { DeadLetterExchange = "dlx" },
            subscriber,
            logger,
            "worker.q",
            RabbitMqSubscriberRole.Worker);

        using var firstAttempt = dispatcher.AttachChannel(first);
        await dispatcher.HandleAsync(Delivery("payload", deliveryTag: 6), first, CancellationToken.None);

        // Read 1: the publish's exception filter; read 2: the channel wait, which then parks on a
        // fresh attach signal while holding the attach gate. Attaching now wakes exactly that wait.
        await first.SecondClosedRead.Task.WaitAsync(Wait);
        using var secondAttempt = dispatcher.AttachChannel(second);
        await notified.Task.WaitAsync(Wait);

        var copy = Assert.Single(second.Publishes);
        Assert.Equal("dlx", copy.Exchange);
        Assert.Empty(first.Publishes);
        Assert.Contains(logger.Entries, e => e.Level == LogLevel.Debug
            && e.Message.Contains("closed while dead-lettering already-ACKed RabbitMQ delivery 6", StringComparison.Ordinal));
    }

    // ---------- QueuedRabbitMqMessageDispatcher: drain lapse ----------

    [Fact]
    public async Task Queued_DrainLapse_ReserveSpentOnThePublishGate_CountsTheRestAndAWorkerBuriesItLater()
    {
        // Worker holds the dead-letter publish gate (its copy of d1 is stuck on the broker) through
        // the drain. The reserve then lapses while d2's burial waits for that gate — no copy, an
        // error — and d3, never reached, is counted. Once the worker frees up it buries d3 itself.
        var publishEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releasePublish = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var publishes = 0;
        var channel = new GapChannel
        {
            OnPublish = async (_, _) =>
            {
                if (Interlocked.Increment(ref publishes) == 1)
                {
                    publishEntered.TrySetResult();
                    await releasePublish.Task;
                }
            }
        };
        var logger = new GapLogger();
        var notified = new ConcurrentQueue<ulong>();
        var d3Notified = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var subscriber = EnqueueSubscriber(drain: TimeSpan.FromMilliseconds(400));
        subscriber.OnBackgroundFailure = context =>
        {
            notified.Enqueue(context.DeliveryTag);
            if (context.DeliveryTag == 3)
                d3Notified.TrySetResult();
            return ValueTask.CompletedTask;
        };
        var runs = 0;

        var dispatcher = RabbitMqMessageDispatcher.Create(
            (_, _) =>
            {
                Interlocked.Increment(ref runs);
                throw new InvalidOperationException("handler boom");
            },
            new RabbitMqAsyncResponseOptions { DeadLetterExchange = "dlx" },
            subscriber,
            logger,
            "worker.q",
            RabbitMqSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery("m1", deliveryTag: 1), channel, CancellationToken.None);
        await publishEntered.Task.WaitAsync(Wait);
        await dispatcher.HandleAsync(Delivery("m2", deliveryTag: 2), channel, CancellationToken.None);
        await dispatcher.HandleAsync(Delivery("m3", deliveryTag: 3), channel, CancellationToken.None);

        await dispatcher.DisposeAsync().AsTask().WaitAsync(Wait);

        Assert.Contains(logger.Entries, e => e.Level == LogLevel.Error
            && e.Message.Contains("Cannot dead-letter already-ACKed RabbitMQ delivery 2", StringComparison.Ordinal)
            && e.Message.Contains("shutdown budget ran out", StringComparison.Ordinal));
        Assert.Contains(logger.Entries, e => e.Level == LogLevel.Error
            && e.Message.StartsWith("1 already-ACKed RabbitMQ deliveries on worker.q were neither handled nor dead-lettered", StringComparison.Ordinal));

        releasePublish.SetResult();
        await d3Notified.Task.WaitAsync(Wait);

        Assert.Equal(1, Volatile.Read(ref runs));
        Assert.Contains(1UL, notified);
        Assert.DoesNotContain(2UL, notified); // the reserve ran out before its callback
        List<GapChannel.Publish> copies;
        lock (channel.Publishes)
            copies = [.. channel.Publishes];
        Assert.Equal(["m1", "m3"], copies.Select(p => Encoding.UTF8.GetString(p.Body.Span)).Order(StringComparer.Ordinal));
        var lapsedCopy = copies.Single(p => Encoding.UTF8.GetString(p.Body.Span) == "m3");
        Assert.StartsWith(
            RabbitMqMessageDispatcher.DrainLapsedAfterCommitReason,
            Assert.IsType<string>(lapsedCopy.Properties.Headers!["AR-DeadLetter-Reason"]),
            StringComparison.Ordinal);
    }

    [Fact]
    public async Task Queued_DrainLapse_CallbackOutlivingTheReserve_IsAbandonedAndItsFaultObserved()
    {
        var releaseHandler = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        // Synchronous continuations: the observing continuation runs inside SetException below.
        var hangingCallback = new TaskCompletionSource();
        var logger = new GapLogger();
        var subscriber = EnqueueSubscriber(drain: TimeSpan.FromMilliseconds(400));
        subscriber.OnBackgroundFailure = context => context.DeliveryTag == 2
            ? new ValueTask(hangingCallback.Task)
            : ValueTask.CompletedTask;

        var dispatcher = RabbitMqMessageDispatcher.Create(
            async (delivery, _) =>
            {
                if (delivery.DeliveryTag == 1)
                {
                    started.TrySetResult();
                    await releaseHandler.Task; // ignores the drain token
                }
            },
            new RabbitMqAsyncResponseOptions(),
            subscriber,
            logger,
            "worker.q",
            RabbitMqSubscriberRole.Worker);

        var channel = new GapChannel();
        await dispatcher.HandleAsync(Delivery("m1", deliveryTag: 1), channel, CancellationToken.None);
        await started.Task.WaitAsync(Wait);
        await dispatcher.HandleAsync(Delivery("m2", deliveryTag: 2), channel, CancellationToken.None);

        await dispatcher.DisposeAsync().AsTask().WaitAsync(Wait);

        Assert.Contains(logger.Entries, e => e.Level == LogLevel.Warning
            && e.Message.Contains("OnBackgroundFailure callback for already-ACKed delivery 2", StringComparison.Ordinal)
            && e.Message.Contains("did not complete within the shutdown reserve", StringComparison.Ordinal));

        // The abandoned callback faults later: observed by the dispatcher's continuation.
        hangingCallback.SetException(new InvalidOperationException("late callback failure"));
        Assert.True(hangingCallback.Task.IsFaulted);

        releaseHandler.SetResult();
    }

    [Fact]
    public async Task Queued_HandlerCutShortByTheLapse_WhoseCopyCannotBeWritten_ReportsTheLoss()
    {
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var notified = new TaskCompletionSource<RabbitMqBackgroundFailureContext>(TaskCreationOptions.RunContinuationsAsynchronously);
        var channel = new GapChannel { OnPublish = (_, _) => throw new InvalidOperationException("broker nack") };
        var logger = new GapLogger();
        var subscriber = EnqueueSubscriber(drain: TimeSpan.FromMilliseconds(400));
        subscriber.OnBackgroundFailure = context =>
        {
            notified.TrySetResult(context);
            return ValueTask.CompletedTask;
        };

        var dispatcher = RabbitMqMessageDispatcher.Create(
            async (_, cancellationToken) =>
            {
                started.TrySetResult();
                await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            },
            new RabbitMqAsyncResponseOptions { DeadLetterExchange = "dlx" },
            subscriber,
            logger,
            "worker.q",
            RabbitMqSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery("m1", deliveryTag: 1), channel, CancellationToken.None);
        await started.Task.WaitAsync(Wait);
        await dispatcher.DisposeAsync().AsTask().WaitAsync(Wait);
        var context = await notified.Task.WaitAsync(Wait);

        Assert.IsAssignableFrom<OperationCanceledException>(context.Exception);
        Assert.Contains(logger.Entries, e => e.Level == LogLevel.Error
            && e.Message.Contains("canceled because the drain budget lapsed while it was still running", StringComparison.Ordinal)
            && e.Message.Contains("its dead-letter copy could not be written", StringComparison.Ordinal));
        Assert.Empty(channel.Publishes);
    }

    // ---------- QueuedRabbitMqMessageDispatcher: backpressure and attachments ----------

    [Fact]
    public async Task Queued_ParkedWrite_EndedByItsAttempt_HandsTheDeliveryBack_EvenWhenTheNackFails()
    {
        // A delivery parked on a full queue belongs to the attempt that received it: when that
        // attempt ends it is handed back (a NACK that may itself fail — logged, never thrown into
        // the client's delivery callback) and never enqueued or ACKed.
        var channel = new GapChannel { ThrowOnNack = new InvalidOperationException("nack boom") };
        var logger = new GapLogger();
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handled = new ConcurrentQueue<ulong>();
        var dispatcher = RabbitMqMessageDispatcher.Create(
            async (delivery, _) =>
            {
                handled.Enqueue(delivery.DeliveryTag);
                if (delivery.DeliveryTag == 1)
                {
                    started.TrySetResult();
                    await release.Task;
                }
            },
            new RabbitMqAsyncResponseOptions(),
            EnqueueSubscriber(capacity: 1),
            logger,
            "worker.q",
            RabbitMqSubscriberRole.Worker);

        var attachment = dispatcher.AttachChannel(channel);
        await dispatcher.HandleAsync(Delivery("m1", deliveryTag: 1), channel, CancellationToken.None);
        await started.Task.WaitAsync(Wait);
        await dispatcher.HandleAsync(Delivery("m2", deliveryTag: 2), channel, CancellationToken.None);

        var parked = dispatcher.HandleAsync(Delivery("m3", deliveryTag: 3), channel, CancellationToken.None);
        Assert.False(parked.IsCompleted, "the third delivery must wait for queue capacity");

        attachment.Dispose();
        await parked.WaitAsync(Wait);
        attachment.Dispose(); // idempotent: the attempt ends once

        Assert.Equal([1UL, 2UL], channel.Acks);
        Assert.Contains(logger.Entries, e => e.Level == LogLevel.Debug
            && e.Message.Contains("Failed to NACK delivery 3", StringComparison.Ordinal));

        release.SetResult();
        await dispatcher.DisposeAsync().AsTask().WaitAsync(Wait);
        Assert.Equal([1UL, 2UL], handled.Order());
    }

    [Fact]
    public async Task Queued_DeadLetterBuildFailure_IsReported_AndTheWorkerKeepsDrainingTheQueue()
    {
        // A handler exception whose Message getter throws is user code the burial reads while it
        // builds the dead-letter headers. That used to escape the worker loop: the worker died, and
        // every already-ACKed delivery queued behind it was never run, copied or reported. The burial
        // is best-effort (Kafka parity) — the failure is still reported, and the next delivery runs.
        var logger = new GapLogger();
        var reported = new TaskCompletionSource<RabbitMqBackgroundFailureContext>(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondRan = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var subscriber = EnqueueSubscriber();
        subscriber.OnBackgroundFailure = context =>
        {
            reported.TrySetResult(context);
            return ValueTask.CompletedTask;
        };
        var dispatcher = RabbitMqMessageDispatcher.Create(
            (delivery, _) =>
            {
                if (delivery.DeliveryTag == 1)
                    throw new MessageThrowsWhileDeadLetteringException();
                secondRan.TrySetResult();
                return Task.CompletedTask;
            },
            new RabbitMqAsyncResponseOptions { DeadLetterExchange = "dlx" },
            subscriber,
            logger,
            "worker.q",
            RabbitMqSubscriberRole.Worker);
        var channel = new GapChannel();

        await dispatcher.HandleAsync(Delivery("m1", deliveryTag: 1), channel, CancellationToken.None);
        await dispatcher.HandleAsync(Delivery("m2", deliveryTag: 2), channel, CancellationToken.None);

        var failure = await reported.Task.WaitAsync(Wait);
        Assert.Equal(1UL, failure.DeliveryTag);
        Assert.IsType<MessageThrowsWhileDeadLetteringException>(failure.Exception);
        await secondRan.Task.WaitAsync(Wait);
        await dispatcher.DisposeAsync().AsTask().WaitAsync(Wait);

        Assert.Empty(channel.Publishes); // no copy could be built, so none is claimed
        Assert.Contains(logger.Entries, e => e.Level == LogLevel.Error
            && e.Message.Contains("Failed to dead-letter already-ACKed RabbitMQ delivery 1", StringComparison.Ordinal)
            && e.Exception is InvalidOperationException);
        Assert.DoesNotContain(logger.Entries, e => e.Message.Contains("ended with an error", StringComparison.Ordinal));
    }

    // ---------- RabbitMqMessageDispatcher base contract ----------

    [Fact]
    public async Task BaseDispatcher_DefaultsAttachNothingAndDisposeSynchronously()
    {
        var runs = 0;
        var dispatcher = new MinimalDispatcher(
            (_, _) =>
            {
                Interlocked.Increment(ref runs);
                return Task.CompletedTask;
            });

        var first = dispatcher.AttachChannel(new GapChannel());
        var second = dispatcher.AttachChannel(new GapChannel());
        Assert.Same(first, second); // one shared no-op handle: nothing is bound per attempt
        first.Dispose();

        await dispatcher.HandleAsync(Delivery("payload"), new GapChannel(), CancellationToken.None);
        Assert.Equal(1, runs);

        var dispose = dispatcher.DisposeAsync();
        Assert.True(dispose.IsCompletedSuccessfully);
        await dispose;
    }

    private sealed class MinimalDispatcher(Func<RabbitMqDelivery, CancellationToken, Task> handler)
        : RabbitMqMessageDispatcher(
            handler,
            new RabbitMqAsyncResponseOptions(),
            new RabbitMqSubscriberOptions(),
            new GapLogger(),
            "worker.q",
            RabbitMqSubscriberRole.Worker,
            hostLifetime: null)
    {
        public override Task HandleAsync(RabbitMqDelivery delivery, IRabbitMqChannel channel, CancellationToken subscriberCancellationToken)
            => ExecuteHandlerAsync(delivery, subscriberCancellationToken);
    }

    // ---------- RabbitMqWorkerTransport ----------

    [Fact]
    public async Task WorkerTransport_PublisherQueuedOnTheGate_ReusesTheChannelTheFirstOneCreated()
    {
        // Both publishers miss the lock-free fast path; the second waits on the gate while the first
        // connects, and must then reuse the channel instead of opening a second one.
        var connecting = new TaskCompletionSource<IRabbitMqConnection>(TaskCreationOptions.RunContinuationsAsynchronously);
        var channel = new GapChannel();
        var connection = new Mock<IRabbitMqConnection>();
        connection.SetupGet(c => c.IsOpen).Returns(true);
        connection.Setup(c => c.CreateChannelAsync(It.IsAny<bool>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(channel);
        var factory = new Mock<IRabbitMqConnectionFactory>();
        factory.Setup(f => f.CreateConnectionAsync(It.IsAny<CancellationToken>())).Returns(connecting.Task);

        await using var transport = new RabbitMqWorkerTransport(
            Options.Create(new RabbitMqAsyncResponseOptions { DeclareTopology = false }),
            factory.Object);

        var first = transport.PublishAsync(Job("c1"));
        var second = transport.PublishAsync(Job("c2"));
        Assert.False(first.IsCompleted);
        Assert.False(second.IsCompleted);

        connecting.SetResult(connection.Object);
        await Task.WhenAll(first, second).WaitAsync(Wait);

        factory.Verify(f => f.CreateConnectionAsync(It.IsAny<CancellationToken>()), Times.Once);
        connection.Verify(c => c.CreateChannelAsync(It.IsAny<bool>(), It.IsAny<CancellationToken>()), Times.Once);
        Assert.Equal(2, channel.Publishes.Count);
    }

    [Fact]
    public async Task WorkerTransport_TopologyFailure_WhoseChannelDisposeAlsoThrows_SurfacesTheTopologyError()
    {
        // The half-built channel is released best-effort: its own dispose failure is swallowed so the
        // caller sees why the publish failed, and the next publish starts over.
        var channel = new Mock<IRabbitMqChannel>();
        channel.SetupGet(c => c.IsOpen).Returns(true);
        channel.Setup(c => c.ExchangeDeclareAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("406 PRECONDITION_FAILED"));
        channel.Setup(c => c.DisposeAsync()).Throws(new ObjectDisposedException("channel"));
        var connection = new Mock<IRabbitMqConnection>();
        connection.SetupGet(c => c.IsOpen).Returns(true);
        connection.Setup(c => c.CreateChannelAsync(It.IsAny<bool>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(channel.Object);
        var factory = new Mock<IRabbitMqConnectionFactory>();
        factory.Setup(f => f.CreateConnectionAsync(It.IsAny<CancellationToken>())).ReturnsAsync(connection.Object);

        await using var transport = new RabbitMqWorkerTransport(
            Options.Create(new RabbitMqAsyncResponseOptions()),
            factory.Object);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => transport.PublishAsync(Job("c1")));
        Assert.Equal("406 PRECONDITION_FAILED", ex.Message);
        channel.Verify(c => c.DisposeAsync(), Times.Once);

        await Assert.ThrowsAsync<InvalidOperationException>(() => transport.PublishAsync(Job("c2")));
        connection.Verify(c => c.CreateChannelAsync(It.IsAny<bool>(), It.IsAny<CancellationToken>()), Times.Exactly(2));
    }

    private static WorkerJobEnvelope Job(string correlationId)
        => new()
        {
            CorrelationId = correlationId,
            Call = new ReflectionCallDto { ServiceInterfaceFullName = "Tests.IService", MethodName = "Run", Params = [] }
        };

    // ---------- RabbitMqCorrelationIdExtractor ----------

    [Fact]
    public void CorrelationIdExtractor_NonFormattableHeaderValue_RendersThroughToString()
    {
        var delivery = Delivery(
            "{}",
            new BasicProperties { Headers = new Dictionary<string, object?> { ["x-corr"] = true } });

        var correlationId = RabbitMqCorrelationIdExtractor.Extract(
            delivery,
            "{}",
            new RabbitMqAsyncResponseOptions { CorrelationIdHeader = "x-corr" });

        Assert.Equal(bool.TrueString, correlationId);
    }

    // ---------- helpers ----------

    private static RabbitMqSubscriberOptions EnqueueSubscriber(int workers = 1, int capacity = 8, TimeSpan? drain = null)
        => new RabbitMqSubscriberOptions().UseAckAfterEnqueue(workers, capacity, drain ?? TimeSpan.FromSeconds(5));

    private static BasicProperties DeathCount(long count)
        => new()
        {
            Headers = new Dictionary<string, object?>
            {
                ["x-death"] = new List<object?> { new Dictionary<string, object?> { ["count"] = count } }
            }
        };

    private static RabbitMqDelivery Delivery(
        string body,
        BasicProperties? properties = null,
        ulong deliveryTag = 1,
        bool redelivered = false)
        => new(
            "consumer",
            deliveryTag,
            redelivered,
            "exchange",
            "route",
            properties ?? new BasicProperties(),
            Encoding.UTF8.GetBytes(body),
            CancellationToken.None);

    /// <summary>
    /// A handler failure whose <see cref="Exception.Message"/> throws only while the early-ACK
    /// dispatcher builds its dead-letter copy (read anywhere else — a trace span's status, say — it
    /// is an ordinary message), so the test does not depend on whether an activity listener is on.
    /// </summary>
    private sealed class MessageThrowsWhileDeadLetteringException : Exception
    {
        public override string Message
            => new StackTrace().ToString().Contains("TryDeadLetterAlreadyAcked", StringComparison.Ordinal)
                ? throw new InvalidOperationException("Message getter boom")
                : "handler boom";
    }

    private sealed class GapChannel : IRabbitMqChannel
    {
        private int _closedReads;
        private volatile bool _open = true;

        public bool Open
        {
            get => _open;
            set => _open = value;
        }

        /// <summary>Completed on the second read of <see cref="IsOpen"/> that returns false.</summary>
        public TaskCompletionSource SecondClosedRead { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public bool IsOpen
        {
            get
            {
                if (_open)
                    return true;

                if (Interlocked.Increment(ref _closedReads) == 2)
                    SecondClosedRead.TrySetResult();
                return false;
            }
        }

        public List<ulong> Acks { get; } = [];
        public List<(ulong DeliveryTag, bool Requeue)> Nacks { get; } = [];
        public List<Publish> Publishes { get; } = [];
        public Exception? ThrowOnAck { get; init; }
        public Exception? ThrowOnNack { get; init; }
        public Func<string, CancellationToken, ValueTask>? OnPublish { get; set; }

        public ValueTask BasicAckAsync(ulong deliveryTag, CancellationToken cancellationToken = default)
        {
            if (ThrowOnAck is not null)
                throw ThrowOnAck;

            lock (Acks)
                Acks.Add(deliveryTag);
            return ValueTask.CompletedTask;
        }

        public ValueTask BasicNackAsync(ulong deliveryTag, bool requeue, CancellationToken cancellationToken = default)
        {
            if (ThrowOnNack is not null)
                throw ThrowOnNack;

            lock (Nacks)
                Nacks.Add((deliveryTag, requeue));
            return ValueTask.CompletedTask;
        }

        public async ValueTask BasicPublishAsync(string exchange, string routingKey, BasicProperties properties, ReadOnlyMemory<byte> body, CancellationToken cancellationToken = default)
        {
            if (OnPublish is { } hook)
                await hook(routingKey, cancellationToken);

            lock (Publishes)
                Publishes.Add(new Publish(exchange, routingKey, properties, body));
        }

        public Task ExchangeDeclareAsync(string exchange, string type, bool durable, bool autoDelete, CancellationToken cancellationToken = default)
            => Task.CompletedTask;

        public Task QueueDeclareAsync(string queue, bool durable, bool exclusive, bool autoDelete, IDictionary<string, object?>? arguments = null, CancellationToken cancellationToken = default)
            => Task.CompletedTask;

        public Task QueueBindAsync(string queue, string exchange, string routingKey, CancellationToken cancellationToken = default)
            => Task.CompletedTask;

        public Task BasicQosAsync(ushort prefetchCount, CancellationToken cancellationToken = default)
            => Task.CompletedTask;

        public Task<RabbitMqConsumer> BasicConsumeAsync(string queue, Func<RabbitMqDelivery, Task> handler, CancellationToken cancellationToken = default)
            => Task.FromResult(new RabbitMqConsumer("consumer-tag", new TaskCompletionSource<string>().Task));

        public Task BasicCancelAsync(string consumerTag, CancellationToken cancellationToken = default)
            => Task.CompletedTask;

        public Task CloseAsync(CancellationToken cancellationToken = default)
            => Task.CompletedTask;

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;

        public sealed record Publish(string Exchange, string RoutingKey, BasicProperties Properties, ReadOnlyMemory<byte> Body);
    }

    private sealed class GapLogger : ILogger
    {
        public ConcurrentQueue<GapLogEntry> Entries { get; } = new();

        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull
            => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
            => Entries.Enqueue(new GapLogEntry(logLevel, formatter(state, exception), exception));
    }

    private sealed record GapLogEntry(LogLevel Level, string Message, Exception? Exception);
}
