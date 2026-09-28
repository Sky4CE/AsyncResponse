using AsyncResponse.Transports.SQS;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using System.Collections.Concurrent;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Pins the SQS subscriber, dispatcher, transport and validator paths the broader suites leave
/// unexercised: the host-default hand-back visibility, a retry delay whose renewal never settles,
/// a hand-back that hangs or throws, the early-ACK queue's capacity wait across dispose, the
/// drain's surfacing cut-off and lapsed-worker surfacing, a drain fault kept inside
/// <c>DisposeAsync</c>, FIFO delayed publishes and the derived-DLQ collision warning.
/// </summary>
public sealed class SqsCoverageGapTests
{
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(10);

    [Fact]
    public async Task AwaitingDispatcher_FlowHandBackWithoutAHostBudget_ShortensVisibilityToTheHostDefault()
    {
        // HostShutdownTimeout validated externally (null): the hand-back bound falls back to the
        // Generic Host's 30-second default, so a 10-minute visibility is cut to 30 seconds.
        var changes = new ConcurrentQueue<TimeSpan>();
        var options = new SqsAsyncResponseOptions { WorkerQueue = "workers", ResponseQueue = "responses", HostShutdownTimeout = null };
        await using var dispatcher = SqsMessageDispatcher.Create(
            (_, _) => throw new DurableFlowInterruptedException("Host is stopping; the delivery is handed back."),
            options,
            new SqsSubscriberOptions { VisibilityTimeout = TimeSpan.FromMinutes(10) },
            NullLogger.Instance,
            "workers",
            SqsSubscriberRole.Worker);

        var outcome = await dispatcher.HandleAsync(
            Delivery("m1", changeVisibility: (delay, _) =>
            {
                changes.Enqueue(delay);
                return ValueTask.CompletedTask;
            }),
            CancellationToken.None);

        Assert.Equal(SqsDispatchOutcome.HandedBack, outcome);
        Assert.Equal([TimeSpan.FromSeconds(30)], changes.ToArray());
    }

    [Fact]
    public async Task RetryDelay_WhenAnInFlightRenewalNeverSettles_GivesUpAfterTheShutdownTimeout()
    {
        // The failure path's retry delay waits for the renewal already in flight on the same
        // message; a renewal stuck past its token must not hold the batch forever — the wait is
        // bounded by ShutdownTimeout, and the shorter delay is then skipped (logged).
        var renewing = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseRenewal = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var applied = new ConcurrentQueue<TimeSpan>();
        var client = new FakeSqsClient();
        var logger = new CollectingLogger();
        var ingress = new Mock<IAsyncResponseIngress>();
        ingress.Setup(i => i.HandleWorkerMessageAsync("body")).Returns(async () =>
        {
            await renewing.Task;
            throw new InvalidOperationException("handler failed");
        });
        var options = new SqsAsyncResponseOptions
        {
            WorkerQueue = "workers",
            ResponseQueue = "responses",
            ReceiveWaitTime = TimeSpan.FromMilliseconds(10),
            ShutdownTimeout = TimeSpan.FromMilliseconds(200)
        };
        options.WorkerSubscriber.VisibilityTimeout = TimeSpan.FromSeconds(45);
        options.WorkerSubscriber.VisibilityRenewalInterval = TimeSpan.FromMilliseconds(20);
        options.WorkerSubscriber.RedeliveryDelay = TimeSpan.FromSeconds(3);
        client.Enqueue(Delivery("m1", body: "body", changeVisibility: async (delay, _) =>
        {
            if (delay == TimeSpan.FromSeconds(45))
            {
                renewing.TrySetResult();
                await releaseRenewal.Task;
            }

            applied.Enqueue(delay);
        }));
        using var subscriber = new SqsWorkerSubscriber(Options.Create(options), client, ingress.Object, logger.For<SqsWorkerSubscriber>());

        await subscriber.StartAsync(CancellationToken.None);
        try
        {
            await logger.WaitForAsync("Failed to change visibility of SQS message m1");

            Assert.Contains(
                logger.Entries,
                entry => entry.Message.Contains("Failed to change visibility of SQS message m1", StringComparison.Ordinal)
                    && entry.Exception is TimeoutException);
            Assert.DoesNotContain(TimeSpan.FromSeconds(3), applied);
        }
        finally
        {
            releaseRenewal.TrySetResult();
            await subscriber.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task HandBack_AReleaseThatHangsOrThrows_IsBoundedByTheShutdownTimeoutAndLogged()
    {
        // The stop cuts a batch short after m1: m2's release hangs past its budget token and m3's
        // throws. Both are logged, and the stop still finishes within ShutdownTimeout.
        var firstStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseHang = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondAsked = new ConcurrentQueue<TimeSpan>();
        var releaseFailure = new InvalidOperationException("change visibility failed");
        var client = new FakeSqsClient { ReturnAllAvailable = true };
        var logger = new CollectingLogger();
        var ingress = new Mock<IAsyncResponseIngress>();
        ingress.Setup(i => i.HandleWorkerMessageAsync(It.IsAny<string>())).Returns(Task.CompletedTask);
        ingress.Setup(i => i.HandleWorkerMessageAsync("first")).Returns(async () =>
        {
            firstStarted.TrySetResult();
            await releaseFirst.Task;
        });
        var options = new SqsAsyncResponseOptions
        {
            WorkerQueue = "workers",
            ResponseQueue = "responses",
            ReceiveWaitTime = TimeSpan.FromMilliseconds(10),
            ShutdownTimeout = TimeSpan.FromMilliseconds(200)
        };
        client.Enqueue(Delivery("m1", body: "first"));
        client.Enqueue(Delivery("m2", body: "second", changeVisibility: async (delay, _) =>
        {
            secondAsked.Enqueue(delay);
            await releaseHang.Task;
        }));
        client.Enqueue(Delivery("m3", body: "third", changeVisibility: (_, _) => throw releaseFailure));
        using var subscriber = new SqsWorkerSubscriber(Options.Create(options), client, ingress.Object, logger.For<SqsWorkerSubscriber>());

        try
        {
            await subscriber.StartAsync(CancellationToken.None);
            await firstStarted.Task.WaitAsync(Wait);

            var stopping = subscriber.StopAsync(CancellationToken.None);
            releaseFirst.TrySetResult();
            await stopping.WaitAsync(Wait);

            ingress.Verify(i => i.HandleWorkerMessageAsync("second"), Times.Never);
            ingress.Verify(i => i.HandleWorkerMessageAsync("third"), Times.Never);
            Assert.Equal([TimeSpan.Zero], secondAsked.ToArray());
            Assert.Contains(logger.Messages, message => message.Contains("Handing unstarted SQS messages back to workers", StringComparison.Ordinal)
                && message.Contains("did not finish within the shutdown budget", StringComparison.Ordinal));
            Assert.Contains(
                logger.Entries,
                entry => entry.Message.Contains("Failed to hand unstarted SQS message m3", StringComparison.Ordinal)
                    && ReferenceEquals(entry.Exception, releaseFailure));
        }
        finally
        {
            releaseFirst.TrySetResult();
            releaseHang.TrySetResult();
        }
    }

    [Fact]
    public async Task QueuedDispatcher_CapacityWait_EndsWhenTheDispatcherIsDisposedWhileSaturated()
    {
        var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handled = new ConcurrentQueue<string>();
        var dispatcher = (QueuedSqsMessageDispatcher)SqsMessageDispatcher.Create(
            async (delivery, _) =>
            {
                handled.Enqueue(delivery.MessageId);
                if (delivery.MessageId == "m1")
                    await releaseFirst.Task;
            },
            TransportOptions(),
            EarlyAck(workers: 1, capacity: 1, drain: TimeSpan.FromSeconds(10)),
            NullLogger.Instance,
            "workers",
            SqsSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery("m1"), CancellationToken.None);
        await WaitUntilAsync(() => dispatcher.RunningCount == 1);
        await dispatcher.HandleAsync(Delivery("m2"), CancellationToken.None);
        Assert.False(dispatcher.CanAcceptMore);

        var capacity = dispatcher.WaitForCapacityAsync(CancellationToken.None).AsTask();
        Assert.False(capacity.IsCompleted);

        var disposing = dispatcher.DisposeAsync().AsTask();
        await capacity.WaitAsync(Wait);
        Assert.False(disposing.IsCompleted);

        releaseFirst.TrySetResult();
        await disposing.WaitAsync(Wait);
        Assert.Equal(["m1", "m2"], handled.ToArray());
    }

    [Fact]
    public async Task QueuedDispatcher_DrainLapse_SlowCallbackCutsSurfacingShort_ThenAFreedWorkerSurfacesTheRest()
    {
        var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseCallback = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var thirdSurfaced = new TaskCompletionSource<SqsBackgroundFailureContext>(TaskCreationOptions.RunContinuationsAsynchronously);
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
        var dispatcher = (QueuedSqsMessageDispatcher)SqsMessageDispatcher.Create(
            async (delivery, _) =>
            {
                handled.Enqueue(delivery.MessageId);
                if (delivery.MessageId == "m1")
                    await releaseFirst.Task;
            },
            TransportOptions(),
            subscriberOptions,
            logger,
            "workers",
            SqsSubscriberRole.Worker);

        try
        {
            await dispatcher.HandleAsync(Delivery("m1"), CancellationToken.None);
            await WaitUntilAsync(() => dispatcher.RunningCount == 1);
            await dispatcher.HandleAsync(Delivery("m2"), CancellationToken.None);
            await dispatcher.HandleAsync(Delivery("m3", receiveCount: 4), CancellationToken.None);

            await dispatcher.DisposeAsync().AsTask().WaitAsync(Wait);

            Assert.Equal(["m2"], surfaced.ToArray());
            Assert.Contains(logger.Messages, message => message.Contains("Surfaced 1 via OnBackgroundFailure", StringComparison.Ordinal)
                && message.Contains("1 could not be surfaced", StringComparison.Ordinal));

            releaseFirst.TrySetResult();
            var third = await thirdSurfaced.Task.WaitAsync(Wait);

            Assert.Equal(4, third.ReceiveCount);
            Assert.IsType<OperationCanceledException>(third.Exception);
            Assert.Equal(["m1"], handled.ToArray());
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
        var dispatcher = new QueuedSqsMessageDispatcher(
            (_, _) => Task.CompletedTask,
            TransportOptions(),
            EarlyAck(workers: 1, capacity: 1, drain: TimeSpan.MaxValue),
            logger,
            "workers",
            SqsSubscriberRole.Worker);

        await dispatcher.DisposeAsync();

        Assert.Contains(
            logger.Entries,
            entry => entry.Message.Contains("drain for workers ended with an error", StringComparison.Ordinal)
                && entry.Exception is ArgumentOutOfRangeException);
    }

    [Fact]
    public async Task WorkerTransport_FifoQueue_RefusesADelayedPublishWithTheWayOut()
    {
        var client = new FakeSqsClient();
        var transport = new SqsWorkerTransport(
            Options.Create(new SqsAsyncResponseOptions { WorkerQueue = "jobs.fifo", ResponseQueue = "responses" }),
            client);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => transport.PublishAsync(
                new WorkerJobEnvelope
                {
                    CorrelationId = "c-delayed",
                    Call = new ReflectionCallDto { ServiceInterfaceFullName = "Svc", MethodName = "Run", Params = [] }
                },
                TimeSpan.FromSeconds(5)));

        Assert.Contains("'jobs.fifo'", ex.Message, StringComparison.Ordinal);
        Assert.Contains("standard worker queue", ex.Message, StringComparison.Ordinal);
        Assert.Empty(client.SentMessages);
        Assert.Equal(TimeSpan.Zero, transport.MaxPublishDelay);
    }

    [Fact]
    public void PossibleQueueCollisions_FlagsADerivedDeadLetterQueueThatMayBeALiveQueueUrl()
    {
        // CreateQueues derives "responses-dlq" for the response queue — and the worker queue is a
        // URL whose queue name is exactly that: the same queue if the URL is in this account.
        var options = new SqsAsyncResponseOptions
        {
            CreateQueues = true,
            WorkerQueue = "https://sqs.us-east-1.amazonaws.com/000000000000/responses-dlq",
            ResponseQueue = "responses"
        };
        // A reply target on an unrelated queue is checked too, and flags nothing.
        options.ReplyTargets["billing"] = new SqsReplyTargetOptions { Queue = "billing" };

        var collisions = SqsOptionsValidator.PossibleQueueCollisions(options).ToList();

        var collision = Assert.Single(collisions);
        Assert.Contains("the derived dead-letter queue 'responses-dlq'", collision, StringComparison.Ordinal);
        Assert.Contains(options.WorkerQueue, collision, StringComparison.Ordinal);
    }

    private static SqsAsyncResponseOptions TransportOptions()
        => new() { WorkerQueue = "workers", ResponseQueue = "responses" };

    private static SqsSubscriberOptions EarlyAck(int workers, int capacity, TimeSpan drain)
        => new()
        {
            AckMode = SqsAckMode.AckAfterEnqueue,
            BackgroundWorkerCount = workers,
            BackgroundQueueCapacity = capacity,
            BackgroundDrainTimeout = drain
        };

    private static SqsTransportDelivery Delivery(
        string messageId,
        string body = "{}",
        int receiveCount = 1,
        Func<TimeSpan, CancellationToken, ValueTask>? changeVisibility = null)
        => new(
            FakeSqsClient.UrlFor("workers"),
            body,
            messageId,
            $"receipt-{messageId}",
            receiveCount,
            new Dictionary<string, string>(StringComparer.Ordinal),
            () => ValueTask.CompletedTask,
            changeVisibility ?? ((_, _) => ValueTask.CompletedTask));

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        using var cts = new CancellationTokenSource(Wait);
        while (!condition())
            await Task.Delay(10, cts.Token);
    }
}
