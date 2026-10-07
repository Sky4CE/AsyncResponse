using AsyncResponse.Transports.Kafka;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Moq;
using System.Collections.Concurrent;
using System.Globalization;
using System.Text;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Kafka dispatcher and adapter edges the main Kafka suites do not reach: dead-letter headers cut
/// to what the producer's <c>message.max.bytes</c> leaves, the drain reserve's bounded
/// OnBackgroundFailure wait (completed, faulted, abandoned), already-committed messages the reserve
/// cannot cover, detached handlers handed back or failing at stop, the eventual outcome of
/// abandoned handlers, and the consumer adapter's re-assignment resume.
/// </summary>
public class KafkaCoverageGapTests
{
    private const string Topic = "worker-topic";
    private const string Group = "worker-group";
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(10);

    // ---------- Dead-letter header budget ----------

    [Fact]
    public async Task DeadLetter_WithRoomForLessThanTheExceptionType_DropsTheMessageAndCutsTheType()
    {
        var failure = new InvalidOperationException("handler boom");
        var typeName = failure.GetType().FullName!;
        var (published, maxBytes) = await DeadLetterUnderBudgetAsync(failure, extraRoom: 10);

        Assert.Equal(string.Empty, FakeKafkaProducerClient.Header(published.Headers, "exceptionMessage"));
        var cutType = FakeKafkaProducerClient.Header(published.Headers, "exceptionType")!;
        Assert.Equal(typeName[..10], cutType);
        AssertFits(published, maxBytes);
    }

    [Fact]
    public async Task DeadLetter_MultiByteExceptionMessage_IsCutOnAWholeCharacterWithinTheBudget()
    {
        // Two UTF-8 bytes per character: the byte budget (21) ends inside a character, so the
        // binary search must step below it to 10 whole characters.
        var failure = new InvalidOperationException(new string('é', 100));
        var typeBytes = Encoding.UTF8.GetByteCount(failure.GetType().FullName!);
        var (published, maxBytes) = await DeadLetterUnderBudgetAsync(failure, extraRoom: typeBytes + 21);

        Assert.Equal(new string('é', 10), FakeKafkaProducerClient.Header(published.Headers, "exceptionMessage"));
        Assert.Equal(failure.GetType().FullName, FakeKafkaProducerClient.Header(published.Headers, "exceptionType"));
        AssertFits(published, maxBytes);
    }

    /// <summary>
    /// Buries one delivery at its cap with the producer's <c>message.max.bytes</c> set so the two
    /// exception headers get exactly <paramref name="extraRoom"/> bytes after every fixed header.
    /// </summary>
    private static async Task<(FakeKafkaProducerClient.PublishCall Published, int MaxBytes)> DeadLetterUnderBudgetAsync(
        Exception failure,
        int extraRoom)
    {
        const long offset = 7;
        var delivery = KafkaTestData.Delivery(Topic, offset);
        var fixedHeaders = new List<KafkaTransportHeader>(delivery.Headers)
        {
            KafkaTransportHeader.Utf8("sourceTopic", Topic),
            KafkaTransportHeader.Utf8("sourcePartition", "0"),
            KafkaTransportHeader.Utf8("sourceOffset", offset.ToString(CultureInfo.InvariantCulture)),
            KafkaTransportHeader.Utf8("consumerGroup", Group),
            KafkaTransportHeader.Utf8("subscriberRole", nameof(KafkaSubscriberRole.Worker)),
            KafkaTransportHeader.Utf8("attempts", "1"),
            KafkaTransportHeader.Utf8("reason", "handler_failed_max_attempts"),
            KafkaTransportHeader.Utf8("occurredAtUtc", DateTimeOffset.UtcNow.ToString("O"))
        };

        // Each exception header costs its framing (10) and key on top of its value.
        var fixedSize = KafkaMessageDispatcher.EstimateRecordSize(delivery.CorrelationId, Encoding.UTF8.GetBytes(delivery.Payload), fixedHeaders)
            + (2 * 10) + "exceptionType".Length + "exceptionMessage".Length;
        var maxBytes = checked((int)fixedSize + extraRoom);

        var options = KafkaTestData.NewOptions();
        options.ConfigureProducer = config => config.MessageMaxBytes = maxBytes;
        options.PublishMaxAttempts = 1;
        var producer = new FakeKafkaProducerClient();
        var consumer = new FakeKafkaConsumerClient();
        await using var dispatcher = KafkaMessageDispatcher.Create(
            (_, _) => Task.FromException(failure),
            consumer,
            producer,
            options,
            new KafkaSubscriberOptions { MaxDeliveryAttempts = 1 },
            new GapLogger(),
            Topic,
            Group,
            KafkaSubscriberRole.Worker);

        await dispatcher.HandleAsync(delivery, CancellationToken.None);

        Assert.Equal(new FakeKafkaConsumerClient.StoredOffset(Topic, 0, offset), Assert.Single(consumer.StoredOffsets));
        return (Assert.Single(producer.Publishes), maxBytes);
    }

    private static void AssertFits(FakeKafkaProducerClient.PublishCall published, int maxBytes)
        => Assert.True(
            KafkaMessageDispatcher.EstimateRecordSize(published.Key, Encoding.UTF8.GetBytes(published.Payload), published.Headers) <= maxBytes,
            "the dead-letter copy must fit the producer's message.max.bytes");

    [Fact]
    public void ResolveMessageMaxBytes_AThrowingProducerHook_FallsBackToTheLibrdkafkaDefault()
    {
        var options = KafkaTestData.NewOptions();
        options.ConfigureProducer = _ => throw new InvalidOperationException("hook boom");

        Assert.Equal(KafkaProducerClientAdapter.DefaultMessageMaxBytes, KafkaProducerClientAdapter.ResolveMessageMaxBytes(options));
    }

    // ---------- Queued dispatcher: the drain reserve ----------

    [Fact]
    public async Task Queued_DrainLapse_BoundsEveryCallbackByTheReserve_CompletedFaultedAndAbandoned()
    {
        // Three unstarted messages are routed by the stop: one callback finishes inside the
        // reserve, one fails inside it (logged, not rethrown), and one outlives it — no longer
        // waited for, and its eventual fault observed.
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseHandler = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var hangingCallback = new TaskCompletionSource(); // synchronous continuations: observed inside SetException
        var contexts = new ConcurrentQueue<KafkaBackgroundFailureContext>();
        var logger = new GapLogger();
        var options = KafkaTestData.NewOptions();
        options.DeadLetterEnabled = false;
        var subscriber = new KafkaSubscriberOptions().UseAckAfterEnqueue(1, 8, TimeSpan.FromMilliseconds(800));
        subscriber.OnBackgroundFailure = async context =>
        {
            contexts.Enqueue(context);
            switch (context.Offset)
            {
                case 2:
                    await Task.Delay(10);
                    return;
                case 3:
                    await Task.Delay(10);
                    throw new InvalidOperationException("callback boom");
                default:
                    await hangingCallback.Task;
                    return;
            }
        };

        var dispatcher = KafkaMessageDispatcher.Create(
            async (delivery, _) =>
            {
                if (delivery.Offset == 1)
                {
                    started.TrySetResult();
                    await releaseHandler.Task; // ignores the drain token
                }
            },
            new FakeKafkaConsumerClient(),
            new FakeKafkaProducerClient(),
            options,
            subscriber,
            logger,
            Topic,
            Group,
            KafkaSubscriberRole.Worker);

        await dispatcher.HandleAsync(KafkaTestData.Delivery(Topic, 1, partition: 1), CancellationToken.None);
        await started.Task.WaitAsync(Wait);
        for (var offset = 2; offset <= 4; offset++)
            await dispatcher.HandleAsync(KafkaTestData.Delivery(Topic, offset, partition: offset), CancellationToken.None);

        await dispatcher.DisposeAsync().AsTask().WaitAsync(Wait);

        Assert.Equal([2L, 3L, 4L], contexts.Select(c => c.Offset));
        Assert.All(contexts, c => Assert.Equal((int)c.Offset, c.Partition));
        Assert.All(contexts, c => Assert.IsAssignableFrom<OperationCanceledException>(c.Exception));
        Assert.Contains(logger.Entries, e => e.Level == LogLevel.Error
            && e.Message.Contains("background failure callback failed for already-committed message worker-topic[3]@3", StringComparison.Ordinal)
            && e.Exception is InvalidOperationException);
        Assert.Contains(logger.Entries, e => e.Level == LogLevel.Warning
            && e.Message.Contains("worker-topic[4]@4 did not finish within the stop's drain reserve", StringComparison.Ordinal));
        Assert.DoesNotContain(logger.Entries, e => e.Message.Contains("worker-topic[2]@2 did not finish", StringComparison.Ordinal));

        hangingCallback.SetException(new InvalidOperationException("late callback failure"));
        Assert.True(hangingCallback.Task.IsFaulted);
        releaseHandler.SetResult();
    }

    [Fact]
    public async Task Queued_DrainLapse_ReserveSpentOnOneBurial_CountsTheRest_AndTheFreedWorkerBuriesThem()
    {
        // The first dead-letter produce hangs until the reserve lapses: that message has no copy,
        // the one behind it is counted as neither handled nor dead-lettered — and once the worker's
        // own handler returns, its lapse branch buries that message after all.
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseHandler = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var thirdNotified = new TaskCompletionSource<KafkaBackgroundFailureContext>(TaskCreationOptions.RunContinuationsAsynchronously);
        var publishCalls = 0;
        var producer = new FakeKafkaProducerClient
        {
            PublishDelay = async cancellationToken =>
            {
                if (Interlocked.Increment(ref publishCalls) == 1)
                    await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            }
        };
        var logger = new GapLogger();
        var options = KafkaTestData.NewOptions();
        options.PublishMaxAttempts = 1;
        var subscriber = new KafkaSubscriberOptions().UseAckAfterEnqueue(1, 8, TimeSpan.FromMilliseconds(400));
        subscriber.OnBackgroundFailure = context =>
        {
            if (context.Offset == 3)
                thirdNotified.TrySetResult(context);
            return ValueTask.CompletedTask;
        };
        var runs = new ConcurrentQueue<long>();

        var dispatcher = KafkaMessageDispatcher.Create(
            async (delivery, _) =>
            {
                runs.Enqueue(delivery.Offset);
                if (delivery.Offset == 1)
                {
                    started.TrySetResult();
                    await releaseHandler.Task; // ignores the drain token
                }
            },
            new FakeKafkaConsumerClient(),
            producer,
            options,
            subscriber,
            logger,
            Topic,
            Group,
            KafkaSubscriberRole.Worker);

        await dispatcher.HandleAsync(KafkaTestData.Delivery(Topic, 1), CancellationToken.None);
        await started.Task.WaitAsync(Wait);
        await dispatcher.HandleAsync(KafkaTestData.Delivery(Topic, 2), CancellationToken.None);
        await dispatcher.HandleAsync(KafkaTestData.Delivery(Topic, 3), CancellationToken.None);

        await dispatcher.DisposeAsync().AsTask().WaitAsync(Wait);

        Assert.Contains(logger.Entries, e => e.Level == LogLevel.Error
            && e.Message.StartsWith("1 already-committed Kafka messages on worker-topic were neither handled nor dead-lettered", StringComparison.Ordinal));
        // The hung produce was abandoned at the reserve, not failed: librdkafka may still deliver
        // it, so the log says unconfirmed (Warning) rather than lost (round 65).
        Assert.Contains(logger.Entries, e => e.Level == LogLevel.Warning
            && e.Message.Contains("worker-topic[0]@2 was not started", StringComparison.Ordinal)
            && e.Message.Contains("its dead-letter copy is unconfirmed", StringComparison.Ordinal));
        Assert.Empty(producer.Publishes);

        releaseHandler.SetResult();
        var context = await thirdNotified.Task.WaitAsync(Wait);

        Assert.IsAssignableFrom<OperationCanceledException>(context.Exception);
        Assert.Equal([1L], runs);
        var copy = Assert.Single(producer.Publishes);
        Assert.Equal("3", FakeKafkaProducerClient.Header(copy.Headers, "sourceOffset"));
        Assert.Equal("drain_budget_lapsed_after_commit", FakeKafkaProducerClient.Header(copy.Headers, "reason"));
    }

    [Fact]
    public async Task Queued_HandlerCanceledByTheLapse_WithoutDeadLettering_ReportsTheLoss()
    {
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var notified = new TaskCompletionSource<KafkaBackgroundFailureContext>(TaskCreationOptions.RunContinuationsAsynchronously);
        var logger = new GapLogger();
        var options = KafkaTestData.NewOptions();
        options.DeadLetterEnabled = false;
        var subscriber = new KafkaSubscriberOptions().UseAckAfterEnqueue(1, 8, TimeSpan.FromMilliseconds(400));
        subscriber.OnBackgroundFailure = context =>
        {
            notified.TrySetResult(context);
            return ValueTask.CompletedTask;
        };

        var dispatcher = KafkaMessageDispatcher.Create(
            async (_, cancellationToken) =>
            {
                started.TrySetResult();
                await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            },
            new FakeKafkaConsumerClient(),
            new FakeKafkaProducerClient(),
            options,
            subscriber,
            logger,
            Topic,
            Group,
            KafkaSubscriberRole.Worker);

        await dispatcher.HandleAsync(KafkaTestData.Delivery(Topic, 5), CancellationToken.None);
        await started.Task.WaitAsync(Wait);
        await dispatcher.DisposeAsync().AsTask().WaitAsync(Wait);
        var context = await notified.Task.WaitAsync(Wait);

        Assert.Equal(5, context.Offset);
        Assert.Contains(logger.Entries, e => e.Level == LogLevel.Error
            && e.Message.Contains("worker-topic[0]@5 was canceled during dispatcher shutdown", StringComparison.Ordinal)
            && e.Message.Contains("no dead-letter destination is configured", StringComparison.Ordinal));
    }

    [Fact]
    public async Task Queued_FullQueue_WaitsForCapacity_ThenStoresTheOffsetInOrder()
    {
        // The race the subscriber's capacity check leaves: a message arriving at a full queue is
        // enqueued once a worker frees a slot, and only then is its offset stored.
        var consumer = new FakeKafkaConsumerClient();
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handled = new ConcurrentQueue<long>();
        var allHandled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var dispatcher = KafkaMessageDispatcher.Create(
            async (delivery, _) =>
            {
                handled.Enqueue(delivery.Offset);
                if (delivery.Offset == 1)
                {
                    started.TrySetResult();
                    await release.Task;
                }

                if (handled.Count == 3)
                    allHandled.TrySetResult();
            },
            consumer,
            new FakeKafkaProducerClient(),
            KafkaTestData.NewOptions(),
            new KafkaSubscriberOptions().UseAckAfterEnqueue(1, 1, TimeSpan.FromSeconds(5)),
            new GapLogger(),
            Topic,
            Group,
            KafkaSubscriberRole.Worker);

        await dispatcher.HandleAsync(KafkaTestData.Delivery(Topic, 1), CancellationToken.None);
        await started.Task.WaitAsync(Wait);
        await dispatcher.HandleAsync(KafkaTestData.Delivery(Topic, 2), CancellationToken.None);
        Assert.False(dispatcher.CanAcceptMore);

        var third = dispatcher.HandleAsync(KafkaTestData.Delivery(Topic, 3), CancellationToken.None);
        Assert.False(third.IsCompleted, "the third message must wait for a free slot");
        Assert.Equal([1L, 2L], consumer.StoredOffsets.Select(o => o.Offset));

        release.SetResult();
        await third.WaitAsync(Wait);
        await allHandled.Task.WaitAsync(Wait);

        Assert.Equal([1L, 2L, 3L], consumer.StoredOffsets.Select(o => o.Offset));
        Assert.Equal([1L, 2L, 3L], handled);
    }

    // ---------- Awaiting dispatcher: hand-backs and stop-time settlement ----------

    [Fact]
    public async Task Awaiting_UnprocessableMessage_OnAHandedBackPartition_IsLeftUnsettled()
    {
        var consumer = new FakeKafkaConsumerClient();
        var producer = new FakeKafkaProducerClient();
        await using var dispatcher = KafkaMessageDispatcher.Create(
            (_, _) => Task.FromException(new DurableFlowInterruptedException("host stopping; handed back")),
            consumer,
            producer,
            KafkaTestData.NewOptions(),
            new KafkaSubscriberOptions(),
            new GapLogger(),
            Topic,
            Group,
            KafkaSubscriberRole.Worker);
        var awaiting = Assert.IsType<AwaitingKafkaMessageDispatcher>(dispatcher);

        dispatcher.Accept(KafkaTestData.Delivery(Topic, 1), CancellationToken.None);
        Assert.Equal(0, awaiting.DetachedCount);

        // Consumed behind the handed-back message: neither buried nor stored, so nothing is ever
        // committed past the message the flow engine handed back.
        dispatcher.AcceptUnprocessable(
            KafkaTestData.Message(Topic, 2, string.Empty),
            new InvalidDataException("no payload"),
            CancellationToken.None);

        Assert.Empty(consumer.StoredOffsets);
        Assert.Empty(producer.Publishes);
        Assert.True(consumer.IsPartitionPaused(0));
        Assert.Equal(0, awaiting.DetachedCount);
    }

    [Fact]
    public async Task Awaiting_DetachedHandlerHandedBack_IsSettledAsAHandBackOnTheTick()
    {
        var consumer = new FakeKafkaConsumerClient();
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var logger = new GapLogger();
        var runs = new ConcurrentQueue<long>();
        await using var dispatcher = KafkaMessageDispatcher.Create(
            async (delivery, _) =>
            {
                runs.Enqueue(delivery.Offset);
                await release.Task;
                throw new DurableFlowInterruptedException("host stopping; handed back");
            },
            consumer,
            new FakeKafkaProducerClient(),
            KafkaTestData.NewOptions(),
            new KafkaSubscriberOptions { DetachHandlerAfter = TimeSpan.Zero },
            logger,
            Topic,
            Group,
            KafkaSubscriberRole.Worker);

        dispatcher.Accept(KafkaTestData.Delivery(Topic, 4), CancellationToken.None);
        Assert.True(dispatcher.HasDetachedWork);

        release.SetResult();
        await KafkaTestData.WaitUntilAsync(() =>
        {
            dispatcher.SettleCompleted();
            return !dispatcher.HasDetachedWork;
        }, Wait);

        Assert.Empty(consumer.StoredOffsets);
        Assert.True(consumer.IsPartitionPaused(0));
        Assert.Contains(logger.Entries, e => e.Level == LogLevel.Information
            && e.Message.Contains("worker-topic[0]@4 was handed back because the host is stopping", StringComparison.Ordinal));

        // The partition is parked: a later message of it is neither started nor settled.
        dispatcher.Accept(KafkaTestData.Delivery(Topic, 5), CancellationToken.None);
        Assert.Equal([4L], runs);
        Assert.Empty(consumer.StoredOffsets);
    }

    [Fact]
    public async Task Awaiting_Dispose_DetachedBurialThatFailedForGood_LeavesTheOffsetUnstored()
    {
        var consumer = new FakeKafkaConsumerClient();
        var producer = new FakeKafkaProducerClient { PublishException = new InvalidOperationException("dead-letter topic gone") };
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var logger = new GapLogger();
        var options = KafkaTestData.NewOptions();
        options.PublishMaxAttempts = 1;
        var dispatcher = KafkaMessageDispatcher.Create(
            async (_, _) =>
            {
                await release.Task;
                throw new InvalidOperationException("handler boom");
            },
            consumer,
            producer,
            options,
            new KafkaSubscriberOptions { DetachHandlerAfter = TimeSpan.Zero, MaxDeliveryAttempts = 1 },
            logger,
            Topic,
            Group,
            KafkaSubscriberRole.Worker);

        dispatcher.Accept(KafkaTestData.Delivery(Topic, 9), CancellationToken.None);
        Assert.True(dispatcher.HasDetachedWork);

        release.SetResult();
        await dispatcher.DisposeAsync().AsTask().WaitAsync(Wait);

        Assert.Empty(consumer.StoredOffsets);
        Assert.Contains(logger.Entries, e => e.Level == LogLevel.Error
            && e.Message.Contains("worker-topic[0]@9 failed while the subscriber was stopping", StringComparison.Ordinal)
            && e.Exception is KafkaDeadLetterPublishFailedException);
    }

    [Fact]
    public async Task Awaiting_FaultTeardown_LogsEachAbandonedHandlersEventualOutcome_EvenWhenTheLoggerThrows()
    {
        // Abandoned at once (no fault drain budget), each handler's outcome is still reported when
        // it settles: a hand-back as a cancellation, a failed burial as a failure — and a logger that
        // throws from that continuation must not fault it.
        var consumer = new FakeKafkaConsumerClient();
        var producer = new FakeKafkaProducerClient { PublishException = new InvalidOperationException("dead-letter topic gone") };
        var releases = Enumerable.Range(0, 3).Select(_ => new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)).ToArray();
        var logger = new GapLogger { ThrowWhen = message => message.StartsWith("Abandoned Kafka handler for worker-topic[2]", StringComparison.Ordinal) };
        var options = KafkaTestData.NewOptions();
        options.PublishMaxAttempts = 1;
        await using var dispatcher = KafkaMessageDispatcher.Create(
            async (delivery, _) =>
            {
                await releases[delivery.Partition].Task;
                switch (delivery.Partition)
                {
                    case 0:
                        throw new DurableFlowInterruptedException("host stopping; handed back");
                    case 1:
                        throw new InvalidOperationException("handler boom");
                }
            },
            consumer,
            producer,
            options,
            new KafkaSubscriberOptions
            {
                DetachHandlerAfter = TimeSpan.Zero,
                FaultDrainTimeout = TimeSpan.Zero,
                MaxDeliveryAttempts = 1
            },
            logger,
            Topic,
            Group,
            KafkaSubscriberRole.Worker);

        for (var partition = 0; partition < 3; partition++)
            dispatcher.Accept(KafkaTestData.Delivery(Topic, 10 + partition, partition: partition), CancellationToken.None);
        Assert.Equal(3, Assert.IsType<AwaitingKafkaMessageDispatcher>(dispatcher).DetachedCount);

        await dispatcher.TeardownAfterFaultAsync();
        Assert.False(dispatcher.HasDetachedWork);

        foreach (var release in releases)
            release.SetResult();

        await KafkaTestData.WaitUntilAsync(() => logger.Entries.Count(e => e.Message.StartsWith("Abandoned Kafka handler", StringComparison.Ordinal)) == 3, Wait);

        Assert.Contains(logger.Entries, e => e.Level == LogLevel.Information
            && e.Message.StartsWith("Abandoned Kafka handler for worker-topic[0]@10 stopped on cancellation after the consumer it was consumed on was rebuilt", StringComparison.Ordinal));
        Assert.Contains(logger.Entries, e => e.Level == LogLevel.Warning
            && e.Message.StartsWith("Abandoned Kafka handler for worker-topic[1]@11 failed after the consumer it was consumed on was rebuilt", StringComparison.Ordinal)
            && e.Exception is KafkaDeadLetterPublishFailedException);
        Assert.Contains(logger.Entries, e => e.Message.StartsWith("Abandoned Kafka handler for worker-topic[2]@12 completed", StringComparison.Ordinal));
        Assert.Empty(consumer.StoredOffsets);
    }

    // ---------- KafkaMessageDispatcher base contract ----------

    [Fact]
    public async Task BaseDispatcher_TearsDownAfterAFaultAsAGracefulDispose()
    {
        var consumer = new FakeKafkaConsumerClient();
        var runs = 0;
        var dispatcher = new MinimalDispatcher(
            (_, _) =>
            {
                Interlocked.Increment(ref runs);
                return Task.CompletedTask;
            },
            consumer);

        Assert.False(dispatcher.HasDetachedWork);
        Assert.True(dispatcher.CanAcceptMore); // no intake gate
        dispatcher.SettleCompleted();

        dispatcher.Accept(KafkaTestData.Delivery(Topic, 3), CancellationToken.None);
        Assert.Equal(1, runs);
        Assert.Equal(new FakeKafkaConsumerClient.StoredOffset(Topic, 0, 3), Assert.Single(consumer.StoredOffsets));

        var teardown = dispatcher.TeardownAfterFaultAsync();
        Assert.True(teardown.IsCompletedSuccessfully);
        await teardown;
        var dispose = dispatcher.DisposeAsync();
        Assert.True(dispose.IsCompletedSuccessfully);
        await dispose;
    }

    private sealed class MinimalDispatcher(Func<KafkaDelivery, CancellationToken, Task> handler, IKafkaConsumerClient consumer)
        : KafkaMessageDispatcher(
            handler,
            consumer,
            new FakeKafkaProducerClient(),
            KafkaTestData.NewOptions(),
            new KafkaSubscriberOptions(),
            new GapLogger(),
            Topic,
            Group,
            KafkaSubscriberRole.Worker,
            intakeGate: null)
    {
        public override async Task HandleAsync(KafkaDelivery delivery, CancellationToken subscriberCancellationToken)
        {
            await ExecuteHandlerAsync(delivery, attempt: 1, subscriberCancellationToken);
            StoreOffsetAfterSettlement(delivery);
        }
    }

    // ---------- KafkaSubscriberService base contract ----------

    [Fact]
    public async Task BaseSubscriber_ResponseRoleWithoutAnInboundBudget_ReadsTheCorrelationIdFromTheBody()
    {
        // The default budget admits every payload: a response-role subscriber that does not
        // override it still falls back to the JSON body for the correlation id.
        var consumer = new FakeKafkaConsumerClient();
        consumer.Enqueue(KafkaTestData.Message("responses", offset: 1, payload: """{"CorrelationId":"corr-body"}"""));
        var options = KafkaTestData.NewOptions();
        options.ResponseTopic = "responses";
        options.SubscriberRetryBaseDelay = TimeSpan.FromMilliseconds(1);
        options.SubscriberRetryMaxDelay = TimeSpan.FromMilliseconds(5);
        options.ResponseSubscriber.PollTimeout = TimeSpan.FromMilliseconds(10);
        options.ResponseSubscriber.BackpressurePollDelay = TimeSpan.FromMilliseconds(5);
        var subscriber = new BudgetlessResponseSubscriber(options, consumer);

        await subscriber.StartAsync(CancellationToken.None);
        var delivery = await subscriber.Handled.Task.WaitAsync(Wait);
        await subscriber.StopAsync(CancellationToken.None);

        Assert.Equal("corr-body", delivery.CorrelationId);
        Assert.Equal(new FakeKafkaConsumerClient.StoredOffset("responses", 0, 1), Assert.Single(consumer.StoredOffsets));
    }

    private sealed class BudgetlessResponseSubscriber(KafkaAsyncResponseTransportOptions options, FakeKafkaConsumerClient consumer)
        : KafkaSubscriberService(
            Microsoft.Extensions.Options.Options.Create(options),
            new FakeKafkaConsumerClientFactory(consumer),
            new FakeKafkaProducerClient(),
            new FakeKafkaAdminClient(),
            new GapLogger())
    {
        public TaskCompletionSource<KafkaDelivery> Handled { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        protected override string Topic => "responses";
        protected override string ConsumerGroup => Options.ResponseConsumerGroup;
        protected override KafkaSubscriberOptions SubscriberOptions => Options.ResponseSubscriber;
        protected override KafkaSubscriberRole SubscriberRole => KafkaSubscriberRole.ResponseIngress;

        protected override Task HandleMessageAsync(KafkaDelivery delivery, CancellationToken cancellationToken)
        {
            Handled.TrySetResult(delivery);
            return Task.CompletedTask;
        }
    }

    // ---------- Consumer adapter: re-assignment resume ----------

    [Fact]
    public void ConsumerAdapter_ReassignedPartition_WhoseResumeThrows_DoesNotFaultThePoll()
    {
        var (adapter, consumer, resumed) = ReassignedAfterLiftedBackpressure();
        consumer.Setup(c => c.Resume(It.IsAny<IEnumerable<TopicPartition>>()))
            .Callback<IEnumerable<TopicPartition>>(partitions => resumed.Add([.. partitions]))
            .Throws(new KafkaException(new Error(ErrorCode.Local_State)));

        Assert.Null(adapter.Consume(TimeSpan.FromMilliseconds(1)));

        Assert.Equal([new TopicPartition(Topic, new Partition(0))], resumed[^1]);
    }

    [Fact]
    public void ConsumerAdapter_ReassignedPartition_UnderRenewedBackpressure_StaysPaused()
    {
        // The backpressure pause was taken again between the re-assignment and the next poll: it
        // now covers the partition, so the queued resume is dropped rather than fetching into a full
        // queue.
        var (adapter, consumer, resumed) = ReassignedAfterLiftedBackpressure();
        adapter.PauseAssignment();
        var resumesBefore = resumed.Count;

        Assert.Null(adapter.Consume(TimeSpan.FromMilliseconds(1)));
        Assert.Null(adapter.Consume(TimeSpan.FromMilliseconds(1)));

        Assert.Equal(resumesBefore, resumed.Count);
        consumer.Verify(c => c.Consume(It.IsAny<TimeSpan>()), Times.Exactly(2));
    }

    /// <summary>
    /// A partition paused by backpressure, revoked, the pause lifted while it was away, then
    /// assigned again: the adapter has queued its resume for the next poll.
    /// </summary>
    private static (KafkaConsumerClientAdapter Adapter, Mock<IConsumer<string?, byte[]>> Consumer, List<List<TopicPartition>> Resumed) ReassignedAfterLiftedBackpressure()
    {
        var partition = new TopicPartition(Topic, new Partition(0));
        var assignment = new List<TopicPartition> { partition };
        var resumed = new List<List<TopicPartition>>();
        var consumer = new Mock<IConsumer<string?, byte[]>>();
        consumer.SetupGet(c => c.Assignment).Returns(() => [.. assignment]);
        consumer.Setup(c => c.Resume(It.IsAny<IEnumerable<TopicPartition>>()))
            .Callback<IEnumerable<TopicPartition>>(partitions => resumed.Add([.. partitions]));
        var adapter = new KafkaConsumerClientAdapter(consumer.Object);

        adapter.PauseAssignment();
        adapter.OnPartitionsRemoved([new TopicPartitionOffset(partition, Offset.Unset)]);
        assignment.Clear();
        adapter.ResumeAssignment(); // lifted while the partition is away: nothing assigned to resume
        adapter.OnPartitionsAssigned([partition]);
        assignment.Add(partition);

        Assert.All(resumed, batch => Assert.Empty(batch));
        return (adapter, consumer, resumed);
    }

    // ---------- Options validation ----------

    [Fact]
    public void EnsureConsumerConfigAccepted_Kip848GroupProtocol_SkipsTheSessionTimeoutRule()
    {
        // Under the classic protocol a poll interval below the session timeout is refused; under
        // KIP-848 the session timeout is broker-side and librdkafka does not check it.
        var classic = KafkaTestData.NewOptions();
        classic.ConfigureConsumer = config =>
        {
            config.MaxPollIntervalMs = 10_000;
            config.SessionTimeoutMs = 45_000;
        };
        Assert.Throws<InvalidOperationException>(() => KafkaTransportOptionsValidator.EnsureConsumerConfigAccepted(classic, KafkaSubscriberRole.Worker));

        var kip848 = KafkaTestData.NewOptions();
        kip848.ConfigureConsumer = config =>
        {
            config.GroupProtocol = GroupProtocol.Consumer;
            config.MaxPollIntervalMs = 10_000;
            config.SessionTimeoutMs = 45_000;
        };
        KafkaTransportOptionsValidator.EnsureConsumerConfigAccepted(kip848, KafkaSubscriberRole.Worker);
    }

    // ---------- helpers ----------

    private sealed class GapLogger : ILogger
    {
        public ConcurrentQueue<GapLogEntry> Entries { get; } = new();

        /// <summary>When it matches a formatted entry, the entry is recorded and then the call throws.</summary>
        public Func<string, bool>? ThrowWhen { get; init; }

        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull
            => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            var message = formatter(state, exception);
            Entries.Enqueue(new GapLogEntry(logLevel, message, exception));
            if (ThrowWhen?.Invoke(message) == true)
                throw new InvalidOperationException("log sink boom");
        }
    }

    private sealed record GapLogEntry(LogLevel Level, string Message, Exception? Exception);
}
