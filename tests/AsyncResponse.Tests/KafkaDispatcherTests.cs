using AsyncResponse.Transports.Kafka;
using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace AsyncResponse.Tests;

public class KafkaDispatcherTests
{
    private const string Topic = "worker-topic";
    private const string Group = "worker-group";

    // ---------- ValidateOptions ----------

    [Fact]
    public void ValidateOptions_Defaults_DoNotThrow()
        => KafkaMessageDispatcher.ValidateOptions(
            KafkaTestData.NewOptions(),
            new KafkaSubscriberOptions(),
            KafkaSubscriberRole.Worker);

    [Fact]
    public void ValidateOptions_MissingBootstrapServers_Throws()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            KafkaMessageDispatcher.ValidateOptions(
                new KafkaAsyncResponseTransportOptions(),
                new KafkaSubscriberOptions(),
                KafkaSubscriberRole.Worker));

        Assert.Contains(nameof(KafkaAsyncResponseTransportOptions.BootstrapServers), ex.Message, StringComparison.Ordinal);
    }

    [Theory]
    [MemberData(nameof(InvalidSubscriberOptions))]
    public void ValidateOptions_RejectsInvalidSubscriberOptions(
        KafkaSubscriberOptions subscriberOptions,
        string expectedMessageFragment)
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            KafkaMessageDispatcher.ValidateOptions(
                KafkaTestData.NewOptions(),
                subscriberOptions,
                KafkaSubscriberRole.ResponseIngress));

        Assert.Contains(expectedMessageFragment, ex.Message, StringComparison.Ordinal);
        Assert.Contains(nameof(KafkaAsyncResponseTransportOptions.ResponseSubscriber), ex.Message, StringComparison.Ordinal);
    }

    public static TheoryData<KafkaSubscriberOptions, string> InvalidSubscriberOptions()
        => new()
        {
            { new KafkaSubscriberOptions { PollTimeout = TimeSpan.Zero }, nameof(KafkaSubscriberOptions.PollTimeout) },
            { new KafkaSubscriberOptions { BackpressurePollDelay = TimeSpan.Zero }, nameof(KafkaSubscriberOptions.BackpressurePollDelay) },
            { new KafkaSubscriberOptions { MaxDeliveryAttempts = -1 }, nameof(KafkaSubscriberOptions.MaxDeliveryAttempts) },
            { new KafkaSubscriberOptions { HandlerRetryBaseDelay = TimeSpan.Zero }, nameof(KafkaSubscriberOptions.HandlerRetryBaseDelay) },
            { new KafkaSubscriberOptions { HandlerRetryMaxDelay = TimeSpan.Zero }, nameof(KafkaSubscriberOptions.HandlerRetryMaxDelay) },
            {
                new KafkaSubscriberOptions
                {
                    HandlerRetryBaseDelay = TimeSpan.FromSeconds(10),
                    HandlerRetryMaxDelay = TimeSpan.FromSeconds(1)
                },
                nameof(KafkaSubscriberOptions.HandlerRetryBaseDelay)
            }
        };

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void ValidateOptions_RejectsNonPositiveMaxPollInterval(int seconds)
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            KafkaMessageDispatcher.ValidateOptions(
                KafkaTestData.NewOptions(),
                new KafkaSubscriberOptions { MaxPollInterval = TimeSpan.FromSeconds(seconds) },
                KafkaSubscriberRole.Worker));

        Assert.Contains(nameof(KafkaSubscriberOptions.MaxPollInterval), ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateOptions_RejectsMaxPollIntervalAboveTheLibrdkafkaRange()
    {
        // > 86,400,000 ms fails consumer CONSTRUCTION inside the subscriber loop, not validation.
        var ex = Assert.Throws<InvalidOperationException>(() =>
            KafkaMessageDispatcher.ValidateOptions(
                KafkaTestData.NewOptions(),
                new KafkaSubscriberOptions { MaxPollInterval = TimeSpan.FromDays(2) },
                KafkaSubscriberRole.Worker));

        Assert.Contains("max.poll.interval.ms", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateOptions_RejectsDetachHandlerAfterThatCannotFitTheMaxPollInterval()
    {
        // Round 37: the poll thread's longest gap is one inline handler wait plus one poll. A
        // 3-minute inline budget plus a 200 ms poll cannot fit within half of a 5-minute
        // max.poll.interval.ms — the broker would evict the consumer while it waited inline.
        var ex = Assert.Throws<InvalidOperationException>(() =>
            KafkaMessageDispatcher.ValidateOptions(
                KafkaTestData.NewOptions(),
                new KafkaSubscriberOptions { DetachHandlerAfter = TimeSpan.FromMinutes(3) },
                KafkaSubscriberRole.Worker));

        Assert.Contains(nameof(KafkaSubscriberOptions.DetachHandlerAfter), ex.Message, StringComparison.Ordinal);
        Assert.Contains(nameof(KafkaSubscriberOptions.MaxPollInterval), ex.Message, StringComparison.Ordinal);
        Assert.Contains("evicted", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateOptions_AcceptsDetachHandlerAfterOnceMaxPollIntervalIsRaised()
    {
        // The same inline budget passes when the operator raises the poll interval to hold it.
        KafkaMessageDispatcher.ValidateOptions(
            KafkaTestData.NewOptions(),
            new KafkaSubscriberOptions
            {
                DetachHandlerAfter = TimeSpan.FromMinutes(3),
                MaxPollInterval = TimeSpan.FromMinutes(15)
            },
            KafkaSubscriberRole.Worker);
    }

    [Fact]
    public void ValidateOptions_RetryDelaysNoLongerCountAgainstTheMaxPollInterval()
    {
        // The retry ladder runs inside the (detached) handler task, so a delay budget of 300 s
        // against a 5-minute interval — rejected before round 37 — is accepted: it stalls only the
        // message's partition, never the poll thread.
        KafkaMessageDispatcher.ValidateOptions(
            KafkaTestData.NewOptions(),
            new KafkaSubscriberOptions
            {
                MaxDeliveryAttempts = 5,
                HandlerRetryBaseDelay = TimeSpan.FromSeconds(20),
                HandlerRetryMaxDelay = TimeSpan.FromSeconds(160)
            },
            KafkaSubscriberRole.Worker);
    }

    [Fact]
    public void ValidateOptions_DetachHandlerAfter_AllowsZero_RejectsNegative()
    {
        // Zero detaches every handler immediately; negative is meaningless.
        KafkaMessageDispatcher.ValidateOptions(
            KafkaTestData.NewOptions(),
            new KafkaSubscriberOptions { DetachHandlerAfter = TimeSpan.Zero },
            KafkaSubscriberRole.Worker);

        var ex = Assert.Throws<InvalidOperationException>(() =>
            KafkaMessageDispatcher.ValidateOptions(
                KafkaTestData.NewOptions(),
                new KafkaSubscriberOptions { DetachHandlerAfter = TimeSpan.FromMilliseconds(-1) },
                KafkaSubscriberRole.Worker));
        Assert.Contains(nameof(KafkaSubscriberOptions.DetachHandlerAfter), ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateOptions_EarlyAck_DoesNotApplyTheInlineGapRule()
    {
        // Under AckAfterEnqueue the handler never runs on the poll thread, so DetachHandlerAfter
        // has no gap to bound; only its own range is validated.
        KafkaMessageDispatcher.ValidateOptions(
            KafkaTestData.NewOptions(),
            new KafkaSubscriberOptions
            {
                DetachHandlerAfter = TimeSpan.FromMinutes(3),
                BackgroundWorkerCount = 1,
                BackgroundQueueCapacity = 1,
                AckMode = KafkaAckMode.AckAfterEnqueue
            },
            KafkaSubscriberRole.Worker);
    }

    [Fact]
    public void ValidateOptions_AckAfterEnqueue_RequiresBackgroundWorkerCount()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            KafkaMessageDispatcher.ValidateOptions(
                KafkaTestData.NewOptions(),
                new KafkaSubscriberOptions { AckMode = KafkaAckMode.AckAfterEnqueue },
                KafkaSubscriberRole.Worker));

        Assert.Contains(nameof(KafkaSubscriberOptions.BackgroundWorkerCount), ex.Message, StringComparison.Ordinal);
        Assert.Contains(nameof(KafkaAsyncResponseTransportOptions.WorkerSubscriber), ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateOptions_AckAfterEnqueue_RequiresBackgroundQueueCapacity()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            KafkaMessageDispatcher.ValidateOptions(
                KafkaTestData.NewOptions(),
                new KafkaSubscriberOptions
                {
                    AckMode = KafkaAckMode.AckAfterEnqueue,
                    BackgroundWorkerCount = 2
                },
                KafkaSubscriberRole.Worker));

        Assert.Contains(nameof(KafkaSubscriberOptions.BackgroundQueueCapacity), ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateOptions_AckAfterEnqueue_RejectsNonPositiveDrainTimeout()
    {
        var subscriberOptions = new KafkaSubscriberOptions
        {
            AckMode = KafkaAckMode.AckAfterEnqueue,
            BackgroundWorkerCount = 2,
            BackgroundQueueCapacity = 8,
            BackgroundDrainTimeout = TimeSpan.Zero
        };

        var ex = Assert.Throws<InvalidOperationException>(() =>
            KafkaMessageDispatcher.ValidateOptions(
                KafkaTestData.NewOptions(),
                subscriberOptions,
                KafkaSubscriberRole.Worker));

        Assert.Contains(nameof(KafkaSubscriberOptions.BackgroundDrainTimeout), ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateOptions_AckAfterEnqueue_RejectsDrainBudgetExceedingHostShutdown()
    {
        var options = KafkaTestData.NewOptions();
        options.HostShutdownTimeout = TimeSpan.FromSeconds(30);

        var ex = Assert.Throws<InvalidOperationException>(() =>
            KafkaMessageDispatcher.ValidateOptions(
                options,
                new KafkaSubscriberOptions().UseAckAfterEnqueue(2, 8, TimeSpan.FromSeconds(31)),
                KafkaSubscriberRole.Worker));

        Assert.Contains(nameof(KafkaAsyncResponseTransportOptions.HostShutdownTimeout), ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateOptions_AckAfterEnqueue_AllowsDrainBudgetWithinHostShutdown()
    {
        var options = KafkaTestData.NewOptions();
        options.HostShutdownTimeout = TimeSpan.FromSeconds(60);

        KafkaMessageDispatcher.ValidateOptions(
            options,
            new KafkaSubscriberOptions().UseAckAfterEnqueue(2, 8, TimeSpan.FromSeconds(15)),
            KafkaSubscriberRole.Worker);
    }

    [Fact]
    public void ValidateOptions_DocumentedEarlyAckDefaults_Pass()
    {
        // Regression: the documented two-arg early-ACK opt-in with stock defaults
        // (BackgroundDrainTimeout 20s vs HostShutdownTimeout 30s) must not fail startup.
        KafkaMessageDispatcher.ValidateOptions(
            KafkaTestData.NewOptions(),
            new KafkaSubscriberOptions().UseAckAfterEnqueue(4, 256),
            KafkaSubscriberRole.Worker);
    }

    [Fact]
    public void ValidateOptions_ShutdownBudget_BoundaryIsInclusive()
    {
        // A budget exactly equal to HostShutdownTimeout completes within the host's grant, so it
        // passes; one tick over is a guaranteed truncation and throws the itemized message.
        var options = KafkaTestData.NewOptions();
        options.HostShutdownTimeout = TimeSpan.FromSeconds(30);

        KafkaMessageDispatcher.ValidateOptions(
            options,
            new KafkaSubscriberOptions().UseAckAfterEnqueue(2, 8, TimeSpan.FromSeconds(30)),
            KafkaSubscriberRole.Worker);

        var oneTickOver = TimeSpan.FromSeconds(30) + TimeSpan.FromTicks(1);
        var ex = Assert.Throws<InvalidOperationException>(() =>
            KafkaMessageDispatcher.ValidateOptions(
                options,
                new KafkaSubscriberOptions().UseAckAfterEnqueue(2, 8, oneTickOver),
                KafkaSubscriberRole.Worker));

        Assert.Contains(
            $"{nameof(KafkaAsyncResponseTransportOptions)}.{nameof(KafkaAsyncResponseTransportOptions.WorkerSubscriber)}.{nameof(KafkaSubscriberOptions.BackgroundDrainTimeout)} ({oneTickOver})",
            ex.Message,
            StringComparison.Ordinal);
        Assert.Contains($"requires a shutdown budget of {oneTickOver}", ex.Message, StringComparison.Ordinal);
        Assert.Contains(
            $"{nameof(KafkaAsyncResponseTransportOptions)}.{nameof(KafkaAsyncResponseTransportOptions.HostShutdownTimeout)} ({TimeSpan.FromSeconds(30)})",
            ex.Message,
            StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateOptions_UnsupportedAckMode_Throws()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            KafkaMessageDispatcher.ValidateOptions(
                KafkaTestData.NewOptions(),
                new KafkaSubscriberOptions { AckMode = (KafkaAckMode)999 },
                KafkaSubscriberRole.Worker));

        Assert.Contains(nameof(KafkaSubscriberOptions.AckMode), ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void UseAckAfterEnqueue_RejectsNonPositiveArguments()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new KafkaSubscriberOptions().UseAckAfterEnqueue(0, 8));
        Assert.Throws<ArgumentOutOfRangeException>(() => new KafkaSubscriberOptions().UseAckAfterEnqueue(1, 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => new KafkaSubscriberOptions().UseAckAfterEnqueue(1, 8, TimeSpan.Zero));
    }

    // ---------- Dispatcher creation ----------

    [Fact]
    public async Task Create_AckAfterHandlerCompletes_ReturnsAwaitingDispatcher()
    {
        await using var dispatcher = CreateDispatcher(
            (_, _) => Task.CompletedTask,
            new KafkaSubscriberOptions());

        Assert.IsType<AwaitingKafkaMessageDispatcher>(dispatcher);
        Assert.True(dispatcher.CanAcceptMore);
    }

    [Fact]
    public async Task Create_AckAfterEnqueue_ReturnsQueuedDispatcher()
    {
        await using var dispatcher = CreateDispatcher(
            (_, _) => Task.CompletedTask,
            new KafkaSubscriberOptions().UseAckAfterEnqueue(1, 8));

        Assert.IsType<QueuedKafkaMessageDispatcher>(dispatcher);
    }

    // ---------- Awaiting mode ----------

    [Fact]
    public async Task Awaiting_HandlerSucceeds_StoresOffset()
    {
        var consumer = new FakeKafkaConsumerClient();
        var producer = new FakeKafkaProducerClient();
        KafkaDelivery? handled = null;
        await using var dispatcher = CreateDispatcher(
            (delivery, _) =>
            {
                handled = delivery;
                return Task.CompletedTask;
            },
            new KafkaSubscriberOptions(),
            consumer: consumer,
            producer: producer);

        await dispatcher.HandleAsync(KafkaTestData.Delivery(Topic, offset: 7), CancellationToken.None);

        Assert.NotNull(handled);
        Assert.Equal(7, handled!.Offset);
        var stored = Assert.Single(consumer.StoredOffsets);
        Assert.Equal(new FakeKafkaConsumerClient.StoredOffset(Topic, 0, 7), stored);
        Assert.Empty(producer.Publishes);
    }

    [Fact]
    public async Task Awaiting_HandlerSucceeds_EmitsKafkaReceiveActivityTags()
    {
        using var collector = new AsyncResponseActivityCollector();
        await using var dispatcher = CreateDispatcher(
            (_, _) => Task.CompletedTask,
            new KafkaSubscriberOptions());

        await dispatcher.HandleAsync(KafkaTestData.Delivery(Topic, offset: 7), CancellationToken.None);

        var activity = collector.Single("asyncresponse.kafka.receive", "asyncresponse.transport", "kafka");
        Assert.Equal("Worker", AsyncResponseActivityCollector.Tag(activity, "asyncresponse.kafka.role"));
        Assert.Equal(nameof(KafkaAckMode.AckAfterHandlerCompletes), AsyncResponseActivityCollector.Tag(activity, "asyncresponse.kafka.ack_mode"));
        Assert.Equal(1, AsyncResponseActivityCollector.Tag(activity, "asyncresponse.kafka.delivery_attempt"));
        Assert.Equal("kafka", AsyncResponseActivityCollector.Tag(activity, "messaging.system"));
        Assert.Equal(Topic, AsyncResponseActivityCollector.Tag(activity, "messaging.destination.name"));
        Assert.Equal(Group, AsyncResponseActivityCollector.Tag(activity, "messaging.kafka.consumer.group"));
        Assert.Equal(0, AsyncResponseActivityCollector.Tag(activity, "messaging.kafka.destination.partition"));
        Assert.Equal(7L, AsyncResponseActivityCollector.Tag(activity, "messaging.kafka.message.offset"));
    }

    [Fact]
    public async Task Awaiting_AHostStopHandBack_DoesNotMarkTheReceiveSpanAsAnError()
    {
        // Pre-commit r1 pass 2 (critic G 1.1): the hand-back was no longer logged as a failure, but
        // the receive span still got an Error status — every deploy painted a red span per parked
        // flow (RabbitMQ parity).
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == AsyncResponseDiagnostics.ActivitySourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded
        };
        ActivitySource.AddActivityListener(listener);

        static async Task<Activity> ReceiveSpanOfAsync(Exception thrown)
        {
            Activity? observed = null;
            await using var dispatcher = CreateDispatcher(
                (_, _) =>
                {
                    observed = Activity.Current;
                    return Task.FromException(thrown);
                },
                new KafkaSubscriberOptions { MaxDeliveryAttempts = 1 });

            // The hand-back propagates (offset unstored); the real failure is buried at the cap.
            _ = await Record.ExceptionAsync(() => dispatcher.HandleAsync(KafkaTestData.Delivery(Topic, offset: 7), CancellationToken.None));
            Assert.NotNull(observed);
            Assert.Equal("asyncresponse.kafka.receive", observed!.OperationName);
            return observed;
        }

        var handedBack = await ReceiveSpanOfAsync(new DurableFlowInterruptedException("Host is stopping."));
        Assert.NotEqual(ActivityStatusCode.Error, handedBack.Status);
        Assert.Null(handedBack.GetTagItem("error.type"));

        // A real failure still is one.
        Assert.Equal(ActivityStatusCode.Error, (await ReceiveSpanOfAsync(new InvalidOperationException("handler boom"))).Status);
    }

    [Fact]
    public async Task Awaiting_HandlerFailsThenSucceeds_RetriesInProcessWithoutDeadLettering()
    {
        var consumer = new FakeKafkaConsumerClient();
        var producer = new FakeKafkaProducerClient();
        var attempts = 0;
        await using var dispatcher = CreateDispatcher(
            (_, _) => ++attempts < 3
                ? throw new InvalidOperationException("handler boom")
                : Task.CompletedTask,
            FastRetries(new KafkaSubscriberOptions { MaxDeliveryAttempts = 5 }),
            consumer: consumer,
            producer: producer);

        await dispatcher.HandleAsync(KafkaTestData.Delivery(Topic, offset: 1), CancellationToken.None);

        Assert.Equal(3, attempts);
        Assert.Single(consumer.StoredOffsets);
        Assert.Empty(producer.Publishes);
    }

    [Fact]
    public async Task Awaiting_HandlerFailsAtMax_DeadLettersAndStoresOffset()
    {
        var consumer = new FakeKafkaConsumerClient();
        var producer = new FakeKafkaProducerClient();
        var attempts = 0;
        await using var dispatcher = CreateDispatcher(
            (_, _) =>
            {
                attempts++;
                throw new InvalidOperationException("handler boom");
            },
            FastRetries(new KafkaSubscriberOptions { MaxDeliveryAttempts = 2 }),
            consumer: consumer,
            producer: producer);

        await dispatcher.HandleAsync(KafkaTestData.Delivery(Topic, offset: 3), CancellationToken.None);

        Assert.Equal(2, attempts);
        Assert.Single(consumer.StoredOffsets);

        var dead = Assert.Single(producer.Publishes);
        Assert.Equal($"{Topic}.deadletter", dead.Topic);
        Assert.Equal("corr", dead.Key);
        Assert.Equal("payload-json", dead.Payload);
        Assert.Equal("handler_failed_max_attempts", FakeKafkaProducerClient.Header(dead.Headers, "reason"));
        Assert.Equal("2", FakeKafkaProducerClient.Header(dead.Headers, "attempts"));
        Assert.Equal(Topic, FakeKafkaProducerClient.Header(dead.Headers, "sourceTopic"));
        Assert.Equal("3", FakeKafkaProducerClient.Header(dead.Headers, "sourceOffset"));
        Assert.Equal(Group, FakeKafkaProducerClient.Header(dead.Headers, "consumerGroup"));
        Assert.Equal("Worker", FakeKafkaProducerClient.Header(dead.Headers, "subscriberRole"));
        Assert.Equal(typeof(InvalidOperationException).FullName, FakeKafkaProducerClient.Header(dead.Headers, "exceptionType"));
        Assert.Equal("handler boom", FakeKafkaProducerClient.Header(dead.Headers, "exceptionMessage"));
        // The original message headers travel with the dead-lettered copy.
        Assert.Equal("corr", FakeKafkaProducerClient.Header(dead.Headers, "correlationId"));
    }

    [Fact]
    public async Task Awaiting_UnlimitedAttempts_RetriesUntilSuccess()
    {
        var consumer = new FakeKafkaConsumerClient();
        var producer = new FakeKafkaProducerClient();
        var attempts = 0;
        await using var dispatcher = CreateDispatcher(
            (_, _) => ++attempts < 4
                ? throw new InvalidOperationException("handler boom")
                : Task.CompletedTask,
            FastRetries(new KafkaSubscriberOptions { MaxDeliveryAttempts = 0 }),
            consumer: consumer,
            producer: producer);

        await dispatcher.HandleAsync(KafkaTestData.Delivery(Topic, offset: 1), CancellationToken.None);

        Assert.Equal(4, attempts);
        Assert.Single(consumer.StoredOffsets);
        Assert.Empty(producer.Publishes);
    }

    [Fact]
    public async Task Awaiting_DeadLetterDisabled_StoresOffsetWithoutPublishing()
    {
        var consumer = new FakeKafkaConsumerClient();
        var producer = new FakeKafkaProducerClient();
        var options = KafkaTestData.NewOptions();
        options.DeadLetterEnabled = false;
        await using var dispatcher = CreateDispatcher(
            (_, _) => throw new InvalidOperationException("handler boom"),
            FastRetries(new KafkaSubscriberOptions { MaxDeliveryAttempts = 1 }),
            options,
            consumer,
            producer);

        await dispatcher.HandleAsync(KafkaTestData.Delivery(Topic, offset: 1), CancellationToken.None);

        Assert.Single(consumer.StoredOffsets);
        Assert.Empty(producer.Publishes);
    }

    [Fact]
    public async Task Awaiting_ExplicitDeadLetterTopic_OverridesSuffixDerivation()
    {
        var consumer = new FakeKafkaConsumerClient();
        var producer = new FakeKafkaProducerClient();
        var options = KafkaTestData.NewOptions();
        options.DeadLetterTopic = "poison";
        await using var dispatcher = CreateDispatcher(
            (_, _) => throw new InvalidOperationException("handler boom"),
            FastRetries(new KafkaSubscriberOptions { MaxDeliveryAttempts = 1 }),
            options,
            consumer,
            producer);

        await dispatcher.HandleAsync(KafkaTestData.Delivery(Topic, offset: 1), CancellationToken.None);

        Assert.Equal("poison", Assert.Single(producer.Publishes).Topic);
    }

    [Fact]
    public async Task Awaiting_DeadLetterPublish_RetriesTransientFailures()
    {
        var consumer = new FakeKafkaConsumerClient();
        var producer = new FakeKafkaProducerClient { TransientPublishFailuresBeforeSuccess = 2 };
        await using var dispatcher = CreateDispatcher(
            (_, _) => throw new InvalidOperationException("handler boom"),
            FastRetries(new KafkaSubscriberOptions { MaxDeliveryAttempts = 1 }),
            consumer: consumer,
            producer: producer);

        await dispatcher.HandleAsync(KafkaTestData.Delivery(Topic, offset: 1), CancellationToken.None);

        Assert.Equal(3, producer.PublishAttempts);
        Assert.Single(producer.Publishes);
        Assert.Single(consumer.StoredOffsets);
    }

    [Fact]
    public async Task Awaiting_DeadLetterPublishFailsPermanently_FaultsTheDispatch_SoNoLaterSettlementCommitsPastIt()
    {
        var consumer = new FakeKafkaConsumerClient();
        var producer = new FakeKafkaProducerClient { PublishException = new InvalidOperationException("broker gone") };
        await using var dispatcher = CreateDispatcher(
            (_, _) => throw new InvalidOperationException("handler boom"),
            FastRetries(new KafkaSubscriberOptions { MaxDeliveryAttempts = 1 }),
            consumer: consumer,
            producer: producer);

        // Round 35: round 31 swallowed this burial failure and merely left the offset unstored —
        // which protects nothing on Kafka, because a later successful settlement on the same
        // partition stores a HIGHER offset and the auto-committer commits past the failed message
        // (see the subscriber-level pin in KafkaSubscriberTests). The failure now faults the
        // dispatch so the poll loop stops at this message and the supervisor restarts the
        // consumer after its backoff. Pre-fix failure: HandleAsync returned normally.
        var ex = await Assert.ThrowsAsync<KafkaDeadLetterPublishFailedException>(
            () => dispatcher.HandleAsync(KafkaTestData.Delivery(Topic, offset: 1), CancellationToken.None));

        Assert.Equal(Topic, ex.Topic);
        Assert.Equal(1, ex.Offset);
        Assert.IsType<InvalidOperationException>(ex.InnerException);
        // The offset stays unstored so the restarted consumer re-consumes the message and retries
        // the burial.
        Assert.Empty(consumer.StoredOffsets);
    }

    [Fact]
    public async Task Awaiting_CancellationDuringHandler_PropagatesWithoutStoringOffset()
    {
        var consumer = new FakeKafkaConsumerClient();
        using var cancellation = new CancellationTokenSource();
        await using var dispatcher = CreateDispatcher(
            (_, token) =>
            {
                cancellation.Cancel();
                token.ThrowIfCancellationRequested();
                return Task.CompletedTask;
            },
            new KafkaSubscriberOptions(),
            consumer: consumer);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            dispatcher.HandleAsync(KafkaTestData.Delivery(Topic, offset: 1), cancellation.Token));

        Assert.Empty(consumer.StoredOffsets);
    }

    // ---------- Queued (ACK-after-enqueue) mode ----------

    [Fact]
    public async Task Queued_StoresOffsetBeforeBackgroundHandlerCompletes()
    {
        var consumer = new FakeKafkaConsumerClient();
        var handlerStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseHandler = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var dispatcher = CreateDispatcher(
            async (_, _) =>
            {
                handlerStarted.TrySetResult();
                await releaseHandler.Task.ConfigureAwait(false);
            },
            new KafkaSubscriberOptions().UseAckAfterEnqueue(1, 8, TimeSpan.FromSeconds(5)),
            consumer: consumer);

        await dispatcher.HandleAsync(KafkaTestData.Delivery(Topic, offset: 1), CancellationToken.None);

        Assert.Single(consumer.StoredOffsets); // offset stored before the handler finished
        await handlerStarted.Task.WaitAsync(TimeSpan.FromSeconds(2));
        releaseHandler.TrySetResult();
    }

    [Fact]
    public async Task Queued_BackgroundFailureAtMax_NotifiesCallbackAndDeadLetters()
    {
        var consumer = new FakeKafkaConsumerClient();
        var producer = new FakeKafkaProducerClient();
        var failureReported = new TaskCompletionSource<KafkaBackgroundFailureContext>(TaskCreationOptions.RunContinuationsAsynchronously);
        var subscriberOptions = FastRetries(new KafkaSubscriberOptions { MaxDeliveryAttempts = 2 })
            .UseAckAfterEnqueue(1, 8, TimeSpan.FromSeconds(5));
        subscriberOptions.OnBackgroundFailure = context =>
        {
            failureReported.TrySetResult(context);
            return ValueTask.CompletedTask;
        };

        var attempts = 0;
        await using var dispatcher = CreateDispatcher(
            (_, _) =>
            {
                Interlocked.Increment(ref attempts);
                throw new InvalidOperationException("background boom");
            },
            subscriberOptions,
            consumer: consumer,
            producer: producer);

        await dispatcher.HandleAsync(KafkaTestData.Delivery(Topic, offset: 9), CancellationToken.None);

        var failure = await failureReported.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(Topic, failure.Topic);
        Assert.Equal(Group, failure.ConsumerGroup);
        Assert.Equal("Worker", failure.SubscriberRole);
        Assert.Equal(9, failure.Offset);
        Assert.Equal("corr", failure.CorrelationId);
        Assert.IsType<InvalidOperationException>(failure.Exception);

        await KafkaTestData.WaitUntilAsync(() => producer.Publishes.Count == 1);
        Assert.Equal(2, attempts);
        var dead = Assert.Single(producer.Publishes);
        Assert.Equal($"{Topic}.deadletter", dead.Topic);
        Assert.Equal("background_handler_failed_after_commit", FakeKafkaProducerClient.Header(dead.Headers, "reason"));
        Assert.Single(consumer.StoredOffsets); // only the enqueue-time store; dead-lettering does not store again
    }

    /// <summary>
    /// Round 33 (B4): the queued (early-ACK) retry loop's only exit was <c>ReachedDeliveryAttempts</c>,
    /// which is never true for the documented "unlimited" value <c>MaxDeliveryAttempts = 0</c> — so an
    /// already-committed message retried on a background worker forever: no <c>OnBackgroundFailure</c>,
    /// no dead-letter record (both live inside the exit block), the bounded queue filled and the
    /// subscriber wedged. Pre-fix: the failure callback never fires and nothing is produced. After an
    /// early ACK 0 now means a single attempt, like the sibling transports that never retry there.
    /// </summary>
    [Fact]
    public async Task Queued_UnlimitedAttempts_MeansOneAttemptAfterTheEarlyAck_ThenNotifiesAndDeadLetters()
    {
        var consumer = new FakeKafkaConsumerClient();
        var producer = new FakeKafkaProducerClient();
        var failureReported = new TaskCompletionSource<KafkaBackgroundFailureContext>(TaskCreationOptions.RunContinuationsAsynchronously);
        var subscriberOptions = new KafkaSubscriberOptions
        {
            MaxDeliveryAttempts = 0,
            // Slow enough that the pre-fix loop is no hot spin, fast enough to stay well inside the wait below.
            HandlerRetryBaseDelay = TimeSpan.FromMilliseconds(50),
            HandlerRetryMaxDelay = TimeSpan.FromMilliseconds(100)
        }.UseAckAfterEnqueue(1, 8, TimeSpan.FromMilliseconds(250));
        subscriberOptions.OnBackgroundFailure = context =>
        {
            failureReported.TrySetResult(context);
            return ValueTask.CompletedTask;
        };

        var attempts = 0;
        await using var dispatcher = CreateDispatcher(
            (_, _) =>
            {
                Interlocked.Increment(ref attempts);
                throw new InvalidOperationException("background boom");
            },
            subscriberOptions,
            consumer: consumer,
            producer: producer);

        await dispatcher.HandleAsync(KafkaTestData.Delivery(Topic, offset: 5), CancellationToken.None);

        var failure = await failureReported.Task.WaitAsync(TimeSpan.FromSeconds(2));
        Assert.Equal(5, failure.Offset);
        Assert.IsType<InvalidOperationException>(failure.Exception);

        await KafkaTestData.WaitUntilAsync(() => producer.Publishes.Count == 1);
        var dead = Assert.Single(producer.Publishes);
        Assert.Equal($"{Topic}.deadletter", dead.Topic);
        Assert.Equal("background_handler_failed_after_commit", FakeKafkaProducerClient.Header(dead.Headers, "reason"));
        Assert.Equal("1", FakeKafkaProducerClient.Header(dead.Headers, "attempts"));
        Assert.Single(consumer.StoredOffsets); // the enqueue-time store only

        // The worker moved on: nothing retries the already-committed message once it is buried.
        await Task.Delay(TimeSpan.FromMilliseconds(300));
        Assert.Equal(1, Volatile.Read(ref attempts));
    }

    [Fact]
    public async Task Queued_BackgroundFailureCallbackThrow_IsSwallowed()
    {
        var producer = new FakeKafkaProducerClient();
        var subscriberOptions = FastRetries(new KafkaSubscriberOptions { MaxDeliveryAttempts = 1 })
            .UseAckAfterEnqueue(1, 8, TimeSpan.FromSeconds(5));
        subscriberOptions.OnBackgroundFailure = _ => throw new InvalidOperationException("callback boom");

        await using var dispatcher = CreateDispatcher(
            (_, _) => throw new InvalidOperationException("background boom"),
            subscriberOptions,
            producer: producer);

        await dispatcher.HandleAsync(KafkaTestData.Delivery(Topic, offset: 1), CancellationToken.None);

        // The callback exception must not kill the worker: dead-lettering still happens.
        await KafkaTestData.WaitUntilAsync(() => producer.Publishes.Count == 1);
    }

    [Fact]
    public async Task Queued_BackgroundDeadLetterFailure_IsSwallowed()
    {
        var producer = new FakeKafkaProducerClient { PublishException = new InvalidOperationException("broker gone") };
        var attempts = 0;
        await using var dispatcher = CreateDispatcher(
            (_, _) =>
            {
                Interlocked.Increment(ref attempts);
                throw new InvalidOperationException("background boom");
            },
            FastRetries(new KafkaSubscriberOptions { MaxDeliveryAttempts = 1 })
                .UseAckAfterEnqueue(1, 8, TimeSpan.FromSeconds(5)),
            producer: producer);

        await dispatcher.HandleAsync(KafkaTestData.Delivery(Topic, offset: 1), CancellationToken.None);

        await KafkaTestData.WaitUntilAsync(() => Volatile.Read(ref attempts) == 1 && producer.PublishAttempts == 1);
        Assert.Equal(1, producer.PublishAttempts);
    }

    [Fact]
    public async Task Queued_BackgroundFailureRecoversOnRetry_DoesNotDeadLetter()
    {
        var producer = new FakeKafkaProducerClient();
        var processed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var attempts = 0;
        await using var dispatcher = CreateDispatcher(
            (_, _) =>
            {
                if (Interlocked.Increment(ref attempts) < 2)
                    throw new InvalidOperationException("background boom");

                processed.TrySetResult();
                return Task.CompletedTask;
            },
            FastRetries(new KafkaSubscriberOptions { MaxDeliveryAttempts = 3 })
                .UseAckAfterEnqueue(1, 8, TimeSpan.FromSeconds(5)),
            producer: producer);

        await dispatcher.HandleAsync(KafkaTestData.Delivery(Topic, offset: 1), CancellationToken.None);

        await processed.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(2, attempts);
        Assert.Empty(producer.Publishes);
    }

    [Fact]
    public async Task Queued_StoreOffsetFailureAfterEnqueue_IsLoggedAndProcessingContinues()
    {
        var consumer = new FakeKafkaConsumerClient { StoreOffsetException = new InvalidOperationException("rebalanced") };
        var processed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var dispatcher = CreateDispatcher(
            (_, _) =>
            {
                processed.TrySetResult();
                return Task.CompletedTask;
            },
            new KafkaSubscriberOptions().UseAckAfterEnqueue(1, 8, TimeSpan.FromSeconds(5)),
            consumer: consumer);

        await dispatcher.HandleAsync(KafkaTestData.Delivery(Topic, offset: 1), CancellationToken.None);

        await processed.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Empty(consumer.StoredOffsets);
    }

    [Fact]
    public async Task Awaiting_StoreOffsetFailureAfterSuccessfulHandler_DoesNotRerunOrDeadLetter()
    {
        // Regression (r24): the awaiting dispatcher stored its offset INSIDE the handler try, so a
        // StoreOffset throw after a successful handler (routine when a rebalance revoked the
        // partition mid-handler) was misread as a handler failure — the already-succeeded handler
        // re-ran up to MaxDeliveryAttempts and the message was then produced to the dead-letter
        // topic. Settlement now sits outside the try, parity with the queued dispatcher.
        var consumer = new FakeKafkaConsumerClient { StoreOffsetException = new InvalidOperationException("rebalanced") };
        var producer = new FakeKafkaProducerClient();
        var executions = 0;
        await using var dispatcher = CreateDispatcher(
            (_, _) =>
            {
                Interlocked.Increment(ref executions);
                return Task.CompletedTask;
            },
            FastRetries(new KafkaSubscriberOptions { MaxDeliveryAttempts = 3 }),
            consumer: consumer,
            producer: producer);

        await dispatcher.HandleAsync(KafkaTestData.Delivery(Topic, offset: 1), CancellationToken.None);

        Assert.Equal(1, executions);          // the handler ran exactly once
        Assert.Empty(producer.Publishes);     // nothing was dead-lettered
        Assert.Empty(consumer.StoredOffsets); // the store failed; redelivery owns the retry
    }

    [Fact]
    public async Task Queued_SaturatedQueue_ReportsCannotAcceptMore_UntilDrained()
    {
        var releaseHandler = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handlerStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var dispatcher = CreateDispatcher(
            async (_, _) =>
            {
                handlerStarted.TrySetResult();
                await releaseHandler.Task.ConfigureAwait(false);
            },
            new KafkaSubscriberOptions().UseAckAfterEnqueue(1, backgroundQueueCapacity: 1, TimeSpan.FromSeconds(5)));

        // First message: dequeued by the single worker, which blocks in the handler.
        await dispatcher.HandleAsync(KafkaTestData.Delivery(Topic, offset: 1), CancellationToken.None);
        await handlerStarted.Task.WaitAsync(TimeSpan.FromSeconds(2));

        // Second message: sits in the queue, filling the single-slot capacity.
        await dispatcher.HandleAsync(KafkaTestData.Delivery(Topic, offset: 2), CancellationToken.None);

        Assert.False(dispatcher.CanAcceptMore);

        releaseHandler.TrySetResult();
        await KafkaTestData.WaitUntilAsync(() => dispatcher.CanAcceptMore);
    }

    [Fact]
    public async Task Queued_WriteWaitCancellation_RollsBackPendingCount()
    {
        var releaseHandler = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handlerStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var dispatcher = (QueuedKafkaMessageDispatcher)CreateDispatcher(
            async (_, _) =>
            {
                handlerStarted.TrySetResult();
                await releaseHandler.Task.ConfigureAwait(false);
            },
            new KafkaSubscriberOptions().UseAckAfterEnqueue(1, backgroundQueueCapacity: 1, TimeSpan.FromSeconds(5)));

        await dispatcher.HandleAsync(KafkaTestData.Delivery(Topic, offset: 1), CancellationToken.None);
        await handlerStarted.Task.WaitAsync(TimeSpan.FromSeconds(2));
        await dispatcher.HandleAsync(KafkaTestData.Delivery(Topic, offset: 2), CancellationToken.None);

        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            dispatcher.HandleAsync(KafkaTestData.Delivery(Topic, offset: 3), cancellation.Token));

        Assert.Equal(1, dispatcher.PendingCount);
        releaseHandler.TrySetResult();
    }

    [Fact]
    public async Task Queued_Dispose_DrainsQueuedWork()
    {
        var processed = 0;
        var dispatcher = CreateDispatcher(
            async (_, _) =>
            {
                await Task.Delay(10).ConfigureAwait(false);
                Interlocked.Increment(ref processed);
            },
            new KafkaSubscriberOptions().UseAckAfterEnqueue(2, 64, TimeSpan.FromSeconds(10)));

        for (var i = 0; i < 16; i++)
            await dispatcher.HandleAsync(KafkaTestData.Delivery(Topic, offset: i), CancellationToken.None);

        await dispatcher.DisposeAsync();

        Assert.Equal(16, Volatile.Read(ref processed));
    }

    [Fact]
    public async Task Queued_Dispose_IsIdempotent()
    {
        var dispatcher = CreateDispatcher(
            (_, _) => Task.CompletedTask,
            new KafkaSubscriberOptions().UseAckAfterEnqueue(1, 8, TimeSpan.FromSeconds(5)));

        await dispatcher.DisposeAsync();
        await dispatcher.DisposeAsync();
    }

    [Fact]
    public async Task Queued_Dispose_TimesOutOnWedgedHandler_AndCancelsIt()
    {
        var handlerStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handlerCanceled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var dispatcher = CreateDispatcher(
            async (_, token) =>
            {
                handlerStarted.TrySetResult();
                try
                {
                    await Task.Delay(Timeout.InfiniteTimeSpan, token).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    handlerCanceled.TrySetResult();
                    throw;
                }
            },
            new KafkaSubscriberOptions().UseAckAfterEnqueue(1, 8, TimeSpan.FromMilliseconds(100)));

        await dispatcher.HandleAsync(KafkaTestData.Delivery(Topic, offset: 1), CancellationToken.None);
        await handlerStarted.Task.WaitAsync(TimeSpan.FromSeconds(2));

        await dispatcher.DisposeAsync(); // drain budget elapses, the wedged handler is canceled
        await handlerCanceled.Task.WaitAsync(TimeSpan.FromSeconds(2));
    }

    [Fact]
    public async Task Queued_Dispose_CancelledDrain_SurfacesDroppedMessagesViaOnBackgroundFailure()
    {
        var handlerStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var failures = new List<KafkaBackgroundFailureContext>();
        var subscriberOptions = new KafkaSubscriberOptions
        {
            OnBackgroundFailure = context =>
            {
                lock (failures)
                {
                    failures.Add(context);
                }

                return ValueTask.CompletedTask;
            }
        }.UseAckAfterEnqueue(1, 8, TimeSpan.FromMilliseconds(100));
        var dispatcher = CreateDispatcher(
            async (_, token) =>
            {
                handlerStarted.TrySetResult();
                await Task.Delay(Timeout.InfiniteTimeSpan, token).ConfigureAwait(false);
            },
            subscriberOptions);

        await dispatcher.HandleAsync(KafkaTestData.Delivery(Topic, offset: 1), CancellationToken.None);
        await handlerStarted.Task.WaitAsync(TimeSpan.FromSeconds(2));
        await dispatcher.HandleAsync(KafkaTestData.Delivery(Topic, offset: 2), CancellationToken.None); // committed, waiting in queue

        await dispatcher.DisposeAsync(); // drain budget elapses; the interrupted work must not vanish silently

        // Both the in-handler message and the still-queued one are already committed: the shutdown
        // interruption is surfaced through OnBackgroundFailure for each instead of only debug-logged.
        await WaitUntilAsync(() =>
        {
            lock (failures)
            {
                return failures.Count == 2;
            }
        });
        lock (failures)
        {
            Assert.All(failures, context => Assert.IsAssignableFrom<OperationCanceledException>(context.Exception));
            Assert.Equal([1L, 2L], failures.Select(context => context.Offset).OrderBy(offset => offset));
        }
    }

    [Fact]
    public async Task Queued_AfterTheDrainBudgetLapses_DoesNotStartFreshWork_DeadLettersAndSurfacesIt()
    {
        // Regression (round 31): the drain token cannot stop the REAL handler — it is
        // _ingress.HandleWorkerMessageAsync(payload), whose target takes no CancellationToken — so
        // the sibling fact above only passes because its handler honors the token. With a handler
        // that ignores it (as the ingress does), the loop kept dequeuing and EXECUTING past the
        // budget, and whatever was still queued at process exit vanished with no record: those
        // offsets were stored at enqueue, so Kafka never redelivers them (DB/Redis/Pub-Sub parity).
        var producer = new FakeKafkaProducerClient();
        var firstStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handlerRuns = 0;
        var failures = new List<KafkaBackgroundFailureContext>();
        var subscriberOptions = new KafkaSubscriberOptions
        {
            OnBackgroundFailure = context =>
            {
                lock (failures)
                {
                    failures.Add(context);
                }

                return ValueTask.CompletedTask;
            }
        }.UseAckAfterEnqueue(1, 8, TimeSpan.FromMilliseconds(100));
        var dispatcher = CreateDispatcher(
            async (_, _) =>
            {
                // Deliberately ignores the token, exactly like the ingress handler in production.
                if (Interlocked.Increment(ref handlerRuns) == 1)
                {
                    firstStarted.TrySetResult();
                    await releaseFirst.Task.ConfigureAwait(false);
                }
            },
            subscriberOptions,
            producer: producer);

        await dispatcher.HandleAsync(KafkaTestData.Delivery(Topic, offset: 1), CancellationToken.None);
        await firstStarted.Task.WaitAsync(TimeSpan.FromSeconds(2));
        await dispatcher.HandleAsync(KafkaTestData.Delivery(Topic, offset: 2), CancellationToken.None); // committed, waiting in queue

        await dispatcher.DisposeAsync(); // the 100ms drain budget lapses while the first handler blocks
        releaseFirst.TrySetResult();      // ...and only now can the loop reach the queued entry

        await WaitUntilAsync(() =>
        {
            lock (failures)
            {
                return failures.Count == 1;
            }
        });
        lock (failures)
        {
            var dropped = Assert.Single(failures);
            Assert.Equal(2L, dropped.Offset);
            Assert.IsAssignableFrom<OperationCanceledException>(dropped.Exception);
        }

        // The queued entry was NOT executed after the budget lapsed — and, unlike the in-handler
        // interruption, it was written to the dead-letter topic: it was committed at enqueue, so
        // nothing else can ever record it.
        Assert.Equal(1, Volatile.Read(ref handlerRuns));
        var buried = Assert.Single(producer.Publishes);
        Assert.Equal($"{Topic}.deadletter", buried.Topic);
    }

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        while (!condition())
        {
            cts.Token.ThrowIfCancellationRequested();
            await Task.Delay(20, cts.Token);
        }
    }

    // ---------- Unprocessable messages ----------

    [Fact]
    public async Task DiscardUnprocessable_DeadLettersRawPayloadAndStoresOffset()
    {
        var consumer = new FakeKafkaConsumerClient();
        var producer = new FakeKafkaProducerClient();
        await using var dispatcher = CreateDispatcher(
            (_, _) => Task.CompletedTask,
            new KafkaSubscriberOptions(),
            consumer: consumer,
            producer: producer);

        var message = KafkaTestData.Message(Topic, offset: 4, payload: "", ("correlationId", "corr-x"));
        await dispatcher.DiscardUnprocessableAsync(
            message,
            new InvalidDataException("no payload"),
            CancellationToken.None);

        var dead = Assert.Single(producer.Publishes);
        Assert.Equal($"{Topic}.deadletter", dead.Topic);
        Assert.Equal("unprocessable_message", FakeKafkaProducerClient.Header(dead.Headers, "reason"));
        Assert.Equal("corr-x", dead.Key);
        var stored = Assert.Single(consumer.StoredOffsets);
        Assert.Equal(4, stored.Offset);
    }

    [Fact]
    public async Task DiscardUnprocessable_WhenTheDeadLetterPublishFailsPermanently_FaultsTheDispatch_SoNoLaterSettlementCommitsPastIt()
    {
        // Round 31 guarded this burial and swallowed its failure with the offset unstored; round
        // 35 reverses the swallow (the malformed-message path reproduced the same commit-past
        // loss as the handler-failure path). The typed fault is what the poll loop lets escape so
        // the supervisor restarts the consumer with backoff and the partition stays parked at
        // this message. Pre-fix failure: DiscardUnprocessableAsync returned normally.
        var consumer = new FakeKafkaConsumerClient();
        var producer = new FakeKafkaProducerClient { PublishException = new InvalidOperationException("broker gone") };
        await using var dispatcher = CreateDispatcher(
            (_, _) => Task.CompletedTask,
            new KafkaSubscriberOptions(),
            consumer: consumer,
            producer: producer);

        var message = KafkaTestData.Message(Topic, offset: 4, payload: "", ("correlationId", "corr-x"));

        var ex = await Assert.ThrowsAsync<KafkaDeadLetterPublishFailedException>(() => dispatcher.DiscardUnprocessableAsync(
            message,
            new InvalidDataException("no payload"),
            CancellationToken.None));

        Assert.Equal(4, ex.Offset);
        // No burial and no commit: the offset stays unstored so the restarted consumer retries
        // the burial instead of dropping the message with no record.
        Assert.Empty(consumer.StoredOffsets);
    }

    [Fact]
    public async Task DiscardUnprocessable_WhenStoringTheOffsetThrows_DoesNotFaultThePollLoop()
    {
        // Regression (round 29): the offset store here was unguarded, and this call originates
        // INSIDE the poll loop's own catch arm — so nothing could catch it. A rebalance revoking
        // the partition made StoreOffset throw AFTER the message was already produced to the
        // dead-letter topic, faulting the loop; the restart then dead-lettered it a second time.
        var consumer = new FakeKafkaConsumerClient { StoreOffsetException = new InvalidOperationException("rebalanced") };
        var producer = new FakeKafkaProducerClient();
        await using var dispatcher = CreateDispatcher(
            (_, _) => Task.CompletedTask,
            new KafkaSubscriberOptions(),
            consumer: consumer,
            producer: producer);

        var message = KafkaTestData.Message(Topic, offset: 4, payload: "", ("correlationId", "corr-x"));

        await dispatcher.DiscardUnprocessableAsync(
            message,
            new InvalidDataException("no payload"),
            CancellationToken.None);

        // The burial still happened; only the commit was lost, and it is logged rather than thrown.
        Assert.Single(producer.Publishes);
    }

    [Fact]
    public async Task DiscardUnprocessable_SettlesEvenWhenTheSubscriberIsAlreadyStopping()
    {
        // Settlement ignores the stopping token, as every sibling settlement path does: a shutdown
        // landing between the dead-letter publish and the offset store would abort the publish
        // mid-flight and leave the poison message neither buried nor committed.
        var consumer = new FakeKafkaConsumerClient();
        var producer = new FakeKafkaProducerClient();
        await using var dispatcher = CreateDispatcher(
            (_, _) => Task.CompletedTask,
            new KafkaSubscriberOptions(),
            consumer: consumer,
            producer: producer);

        using var stopping = new CancellationTokenSource();
        await stopping.CancelAsync();

        await dispatcher.DiscardUnprocessableAsync(
            KafkaTestData.Message(Topic, offset: 9, payload: "", ("correlationId", "corr-y")),
            new InvalidDataException("no payload"),
            stopping.Token);

        Assert.Single(producer.Publishes);
        Assert.Equal(9, Assert.Single(consumer.StoredOffsets).Offset);
    }

    // ---------- Helpers ----------

    private static KafkaSubscriberOptions FastRetries(KafkaSubscriberOptions options)
    {
        options.HandlerRetryBaseDelay = TimeSpan.FromMilliseconds(1);
        options.HandlerRetryMaxDelay = TimeSpan.FromMilliseconds(2);
        return options;
    }

    [Fact]
    public async Task DiscardUnprocessable_BoundsTheDeadLetterProduceToAFractionOfThePollInterval()
    {
        // Regression: both burial paths block the poll thread on the dead-letter produce, and a
        // produce to an undeliverable topic waits out librdkafka's message.timeout.ms (5 min by
        // default) PER attempt — past max.poll.interval.ms, evicting the consumer mid-burial and
        // rebalancing the partition to a peer that hit the same message: a rebalance storm. The
        // ladder is now bounded to a quarter of the poll interval; once it runs out the caller
        // faults the dispatch (round 35) with the offset left unstored, so the supervisor restarts
        // the consumer instead of a later settlement committing past the message.
        var producer = new HangingKafkaProducerClient();
        await using var dispatcher = KafkaMessageDispatcher.Create(
            (_, _) => Task.CompletedTask,
            new FakeKafkaConsumerClient(),
            producer,
            KafkaTestData.NewOptions(),
            new KafkaSubscriberOptions
            {
                MaxDeliveryAttempts = 0,
                PollTimeout = TimeSpan.FromMilliseconds(10),
                DetachHandlerAfter = TimeSpan.FromMilliseconds(50),
                MaxPollInterval = TimeSpan.FromMilliseconds(400)
            },
            NullLogger.Instance,
            Topic,
            Group,
            KafkaSubscriberRole.Worker);

        await Assert.ThrowsAsync<KafkaDeadLetterPublishFailedException>(() => dispatcher.DiscardUnprocessableAsync(
            KafkaTestData.Message(Topic, offset: 4, payload: ""),
            new InvalidDataException("no payload"),
            CancellationToken.None).WaitAsync(TimeSpan.FromSeconds(5)));

        Assert.True(producer.SawCancellation);
    }

    [Fact]
    public async Task QueuedDispose_SurvivesAWorkerFaultingOutsideItsHandlerGuard()
    {
        // Regression: the drain join caught only TimeoutException (the shared DB base and NATS
        // also carry a general arm). A worker faulting outside its handler guard — here the log
        // sink throwing from the "handler failed" entry inside the catch arm — rethrew from
        // Task.WhenAll, escaped DisposeAsync into the subscriber's `await using` (masking the real
        // shutdown path) and leaked the drain token source. Since round 2 that log line is guarded
        // (a throwing provider must never change an outcome), so the sink failure no longer faults
        // the worker at all; the dispose still has to complete cleanly after it.
        var logger = new ErrorThrowingLogger();
        var dispatcher = KafkaMessageDispatcher.Create(
            (_, _) => throw new InvalidOperationException("handler boom"),
            new FakeKafkaConsumerClient(),
            new FakeKafkaProducerClient(),
            KafkaTestData.NewOptions(),
            new KafkaSubscriberOptions().UseAckAfterEnqueue(1, 8, TimeSpan.FromSeconds(5)),
            logger,
            Topic,
            Group,
            KafkaSubscriberRole.Worker);

        await dispatcher.HandleAsync(KafkaTestData.Delivery(Topic, offset: 7), CancellationToken.None);
        await logger.ErrorThrown.Task.WaitAsync(TimeSpan.FromSeconds(5));

        await dispatcher.DisposeAsync();
    }

    /// <summary>A producer whose publish never completes until its token is cancelled.</summary>
    private sealed class HangingKafkaProducerClient : IKafkaProducerClient
    {
        public bool SawCancellation { get; private set; }

        public async Task<KafkaPublishResult> PublishAsync(
            string topic,
            string? key,
            byte[] payload,
            IReadOnlyList<KafkaTransportHeader> headers,
            CancellationToken cancellationToken)
        {
            try
            {
                await Task.Delay(Timeout.Infinite, cancellationToken);
            }
            catch (OperationCanceledException)
            {
                SawCancellation = true;
                throw;
            }

            throw new InvalidOperationException("unreachable");
        }

        public void Dispose()
        {
        }
    }

    // ---------- Round 37: ack-after-handler detachment (the poll-thread API) ----------

    [Fact]
    public async Task Awaiting_Accept_SettlesInlineWithinTheBudget_WithoutPausing()
    {
        var consumer = new FakeKafkaConsumerClient();
        await using var dispatcher = CreateDispatcher(
            (_, _) => Task.CompletedTask,
            new KafkaSubscriberOptions { DetachHandlerAfter = TimeSpan.FromSeconds(5) },
            consumer: consumer);

        dispatcher.Accept(KafkaTestData.Delivery(Topic, offset: 3), CancellationToken.None);

        Assert.Equal(new FakeKafkaConsumerClient.StoredOffset(Topic, 0, 3), Assert.Single(consumer.StoredOffsets));
        Assert.False(dispatcher.HasDetachedWork);
        Assert.Empty(consumer.PartitionPauses);
    }

    [Fact]
    public async Task Awaiting_Accept_DetachesAHandlerPastTheBudget_PausesItsPartition_AndSettlesOnATick()
    {
        var consumer = new FakeKafkaConsumerClient();
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var dispatcher = CreateDispatcher(
            async (_, _) => await release.Task,
            new KafkaSubscriberOptions { DetachHandlerAfter = TimeSpan.FromMilliseconds(20) },
            consumer: consumer);

        var accepted = Stopwatch.StartNew();
        dispatcher.Accept(KafkaTestData.Delivery(Topic, offset: 9, partition: 4), CancellationToken.None);
        accepted.Stop();

        // Back on the poll thread within the budget (generous bound for a slow runner), the
        // partition paused, nothing stored: the handler is still running.
        Assert.True(accepted.Elapsed < TimeSpan.FromSeconds(2), $"Accept blocked for {accepted.Elapsed}.");
        Assert.True(dispatcher.HasDetachedWork);
        Assert.Equal(4, Assert.Single(consumer.PartitionPauses));
        Assert.True(consumer.IsPartitionPaused(4));
        Assert.Empty(consumer.StoredOffsets);

        // A tick with the handler still running settles nothing.
        dispatcher.SettleCompleted();
        Assert.Empty(consumer.StoredOffsets);
        Assert.True(dispatcher.HasDetachedWork);

        release.SetResult();
        await KafkaTestData.WaitUntilAsync(() =>
        {
            dispatcher.SettleCompleted();
            return !dispatcher.HasDetachedWork;
        });

        Assert.Equal(new FakeKafkaConsumerClient.StoredOffset(Topic, 4, 9), Assert.Single(consumer.StoredOffsets));
        Assert.Equal(4, Assert.Single(consumer.PartitionResumes));
        Assert.False(consumer.IsPartitionPaused(4));
    }

    [Fact]
    public async Task Awaiting_Accept_ZeroBudget_DetachesImmediately()
    {
        var consumer = new FakeKafkaConsumerClient();
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var dispatcher = CreateDispatcher(
            async (_, _) =>
            {
                started.TrySetResult();
                await release.Task;
            },
            new KafkaSubscriberOptions { DetachHandlerAfter = TimeSpan.Zero },
            consumer: consumer);

        dispatcher.Accept(KafkaTestData.Delivery(Topic, offset: 1), CancellationToken.None);

        Assert.True(dispatcher.HasDetachedWork);
        await started.Task.WaitAsync(TimeSpan.FromSeconds(5));
        release.SetResult();
        await KafkaTestData.WaitUntilAsync(() =>
        {
            dispatcher.SettleCompleted();
            return !dispatcher.HasDetachedWork;
        });
        Assert.Single(consumer.StoredOffsets);
    }

    [Fact]
    public async Task Awaiting_SettleCompleted_RethrowsADetachedBurialFailure_WithoutStoringTheOffset()
    {
        // The detached path keeps the round-35 contract: a message that exhausted its attempts and
        // could not be dead-lettered faults the poll loop (through the tick) with its offset
        // unstored, so no later settlement on the partition commits past it.
        var consumer = new FakeKafkaConsumerClient();
        var producer = new FakeKafkaProducerClient { PublishException = new InvalidOperationException("dead-letter topic gone") };
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var dispatcher = CreateDispatcher(
            async (_, _) =>
            {
                await release.Task;
                throw new InvalidOperationException("handler boom");
            },
            new KafkaSubscriberOptions
            {
                DetachHandlerAfter = TimeSpan.FromMilliseconds(20),
                MaxDeliveryAttempts = 1
            },
            consumer: consumer,
            producer: producer);

        dispatcher.Accept(KafkaTestData.Delivery(Topic, offset: 7), CancellationToken.None);
        Assert.True(dispatcher.HasDetachedWork);

        release.SetResult();
        Exception? faulted = null;
        await KafkaTestData.WaitUntilAsync(() =>
        {
            try
            {
                dispatcher.SettleCompleted();
                return false;
            }
            catch (Exception ex)
            {
                faulted = ex;
                return true;
            }
        });

        Assert.IsType<KafkaDeadLetterPublishFailedException>(faulted);
        Assert.Empty(consumer.StoredOffsets);
        Assert.False(dispatcher.HasDetachedWork);
    }

    [Fact]
    public async Task Awaiting_Dispose_WaitsForDetachedHandlers_AndStoresTheirOffsets()
    {
        // A stop lets a detached handler finish and stores its offset before the consumer's close
        // commits, so a routine deploy does not redeliver work that completed.
        var consumer = new FakeKafkaConsumerClient();
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var dispatcher = CreateDispatcher(
            async (_, _) => await release.Task,
            new KafkaSubscriberOptions { DetachHandlerAfter = TimeSpan.FromMilliseconds(20) },
            consumer: consumer);

        dispatcher.Accept(KafkaTestData.Delivery(Topic, offset: 11), CancellationToken.None);
        Assert.True(dispatcher.HasDetachedWork);

        var disposal = dispatcher.DisposeAsync().AsTask();
        await Task.Delay(100);
        Assert.False(disposal.IsCompleted);
        Assert.Empty(consumer.StoredOffsets);

        release.SetResult();
        await disposal.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(new FakeKafkaConsumerClient.StoredOffset(Topic, 0, 11), Assert.Single(consumer.StoredOffsets));
    }

    [Fact]
    public async Task Awaiting_Dispose_CanceledDetachedHandler_LeavesTheOffsetUnstored()
    {
        var consumer = new FakeKafkaConsumerClient();
        using var stopping = new CancellationTokenSource();
        var dispatcher = CreateDispatcher(
            async (_, token) => await Task.Delay(Timeout.InfiniteTimeSpan, token),
            new KafkaSubscriberOptions { DetachHandlerAfter = TimeSpan.FromMilliseconds(20) },
            consumer: consumer);

        dispatcher.Accept(KafkaTestData.Delivery(Topic, offset: 2), stopping.Token);
        Assert.True(dispatcher.HasDetachedWork);

        stopping.Cancel();
        await dispatcher.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Empty(consumer.StoredOffsets);
    }

    [Fact]
    public async Task Awaiting_AMessageArrivingForADetachedPartition_IsHeldAndRunsAfterIt_InOrder()
    {
        // A rebalance can hand a paused partition back with its pause reset; a message that
        // arrives for a partition whose handler is detached is held behind it — order preserved —
        // and the pause re-asserted so the hold never grows.
        var consumer = new FakeKafkaConsumerClient();
        var order = new List<long>();
        var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var dispatcher = CreateDispatcher(
            async (delivery, _) =>
            {
                lock (order)
                {
                    order.Add(delivery.Offset);
                }

                if (delivery.Offset == 1)
                    await releaseFirst.Task;
            },
            new KafkaSubscriberOptions { DetachHandlerAfter = TimeSpan.FromMilliseconds(20) },
            consumer: consumer);

        dispatcher.Accept(KafkaTestData.Delivery(Topic, offset: 1), CancellationToken.None);
        dispatcher.Accept(KafkaTestData.Delivery(Topic, offset: 2), CancellationToken.None);

        // Held, not started: the second handler must not run while the first is in flight.
        await Task.Delay(100);
        lock (order)
        {
            Assert.Equal([1], order);
        }

        Assert.Equal(2, consumer.PartitionPauses.Count); // re-asserted on the held message
        Assert.Empty(consumer.StoredOffsets);

        releaseFirst.SetResult();
        await KafkaTestData.WaitUntilAsync(() =>
        {
            dispatcher.SettleCompleted();
            return consumer.StoredOffsets.Count == 2;
        });

        lock (order)
        {
            Assert.Equal([1, 2], order);
        }

        Assert.Equal([1L, 2L], consumer.StoredOffsets.Select(stored => stored.Offset));
        Assert.Single(consumer.PartitionResumes); // resumed only once the hold was drained
    }

    [Fact]
    public async Task Awaiting_PauseFailureOfARevokedPartition_DoesNotFaultTheDetach()
    {
        var consumer = new FakeKafkaConsumerClient
        {
            PartitionPauseException = new Confluent.Kafka.KafkaException(new Confluent.Kafka.Error(Confluent.Kafka.ErrorCode.Local_UnknownPartition))
        };
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var dispatcher = CreateDispatcher(
            async (_, _) => await release.Task,
            new KafkaSubscriberOptions { DetachHandlerAfter = TimeSpan.FromMilliseconds(20) },
            consumer: consumer);

        dispatcher.Accept(KafkaTestData.Delivery(Topic, offset: 5), CancellationToken.None);
        Assert.True(dispatcher.HasDetachedWork);

        release.SetResult();
        await KafkaTestData.WaitUntilAsync(() =>
        {
            dispatcher.SettleCompleted();
            return !dispatcher.HasDetachedWork;
        });
        Assert.Single(consumer.StoredOffsets);
    }

    // ---------- Round-1 fixpoint (G10) ----------

    [Fact]
    public async Task Awaiting_HostStopHandBack_WhileTheSubscriberTokenIsLive_IsNeitherRetriedNorBuriedNorStored()
    {
        // Regression (r1 S7#1): the host fires ApplicationStopping before it stops any hosted
        // service, so the flow engine hands a parked flow's delivery back
        // (DurableFlowInterruptedException) while this subscriber's own token is still live. The
        // shutdown filter keyed only on that token: the hand-back ran the in-process retry ladder
        // against the stopping host and, past it, produced the flow's only wake-up to the
        // dead-letter topic and stored its offset.
        var consumer = new FakeKafkaConsumerClient();
        var producer = new FakeKafkaProducerClient();
        var calls = 0;
        await using var dispatcher = CreateDispatcher(
            (_, _) =>
            {
                Interlocked.Increment(ref calls);
                return Task.FromException(new DurableFlowInterruptedException("Host is stopping; the wake-up is handed back."));
            },
            FastRetries(new KafkaSubscriberOptions { MaxDeliveryAttempts = 3 }),
            consumer: consumer,
            producer: producer);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            dispatcher.HandleAsync(KafkaTestData.Delivery(Topic, offset: 1), CancellationToken.None));

        Assert.Equal(1, Volatile.Read(ref calls));
        Assert.Empty(producer.Publishes);
        Assert.Empty(consumer.StoredOffsets);
    }

    [Fact]
    public async Task Awaiting_Accept_AHostStopHandBack_ParksThePartition_AndNothingLaterOfItIsStored()
    {
        // r1 S7#1, the Kafka caveat: Kafka commits a POSITION, so ending the hand-back with its
        // offset unstored protects nothing if the next message of the partition is then settled —
        // storing @2 lets the auto-committer commit past the handed-back @1. The partition is parked
        // for the rest of the stop instead (never skip-and-continue); other partitions keep flowing.
        var consumer = new FakeKafkaConsumerClient();
        var producer = new FakeKafkaProducerClient();
        var handled = new System.Collections.Concurrent.ConcurrentQueue<(int Partition, long Offset)>();
        await using var dispatcher = CreateDispatcher(
            (delivery, _) =>
            {
                handled.Enqueue((delivery.Partition, delivery.Offset));
                return delivery.Offset == 1
                    ? Task.FromException(new DurableFlowInterruptedException("Host is stopping; the wake-up is handed back."))
                    : Task.CompletedTask;
            },
            FastRetries(new KafkaSubscriberOptions { MaxDeliveryAttempts = 3, DetachHandlerAfter = TimeSpan.FromSeconds(5) }),
            consumer: consumer,
            producer: producer);

        dispatcher.Accept(KafkaTestData.Delivery(Topic, offset: 1, partition: 0), CancellationToken.None);
        dispatcher.Accept(KafkaTestData.Delivery(Topic, offset: 2, partition: 0), CancellationToken.None);
        dispatcher.Accept(KafkaTestData.Delivery(Topic, offset: 7, partition: 1), CancellationToken.None);

        Assert.Equal([(0, 1L), (1, 7L)], handled.ToArray()); // @2 was never started
        Assert.Empty(producer.Publishes);
        Assert.Equal(new FakeKafkaConsumerClient.StoredOffset(Topic, 1, 7), Assert.Single(consumer.StoredOffsets));
        Assert.True(consumer.IsPartitionPaused(0));
    }

    [Fact]
    public async Task Queued_HostStopHandBack_EndsTheRetryLadderAtOnce_AndLeavesTheRecord()
    {
        // r1 S7#1, early ACK: the offset was committed at enqueue, so there is no delivery to hand
        // back; retrying against the stopping host only repeated the interruption MaxDeliveryAttempts
        // times before the same dead-letter record was written. Regression (r1 critic H6): the
        // record was written as a handler FAILURE — an Error log (an alert on every deploy that
        // stopped a host mid-flow) and reason background_handler_failed_after_commit. A hand-back
        // is a Warning with its own dead-letter reason, still surfaced via OnBackgroundFailure.
        var logger = new ListLogger();
        var producer = new FakeKafkaProducerClient();
        var notified = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var calls = 0;
        var subscriber = FastRetries(new KafkaSubscriberOptions { MaxDeliveryAttempts = 5 }.UseAckAfterEnqueue(1, 8));
        subscriber.OnBackgroundFailure = _ =>
        {
            notified.TrySetResult();
            return ValueTask.CompletedTask;
        };
        var dispatcher = CreateDispatcher(
            (_, _) =>
            {
                Interlocked.Increment(ref calls);
                return Task.FromException(new DurableFlowInterruptedException("Host is stopping."));
            },
            subscriber,
            producer: producer,
            logger: logger);

        await dispatcher.HandleAsync(KafkaTestData.Delivery(Topic, offset: 1), CancellationToken.None);
        await notified.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await dispatcher.DisposeAsync();

        Assert.Equal(1, Volatile.Read(ref calls));
        Assert.Equal(
            "handed_back_after_commit",
            FakeKafkaProducerClient.Header(Assert.Single(producer.Publishes).Headers, "reason"));
        Assert.DoesNotContain(logger.Entries, entry => entry.Level >= LogLevel.Error);
        var warning = Assert.Single(logger.Entries, entry => entry.Level == LogLevel.Warning && entry.Message.Contains("handed back", StringComparison.Ordinal));
        // Logged once the copy is written (r2 S6a#5: dead-letter first, then log and notify), so
        // it reports the copy as made rather than as about to be.
        Assert.Contains("Dead-lettered a copy (handed_back_after_commit)", warning.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Queued_HostStopHandBack_WithDeadLetteringDisabled_IsAnError_ThatSaysTheWakeUpIsLost()
    {
        // Pre-commit r1 pass 2 (critic E 1): with DeadLetterEnabled = false the hand-back logged a
        // Warning claiming the message "is dead-lettered for replay", but no copy is written and the
        // offset is already committed: a lost wake-up, reported as a routine stop. It is an Error
        // that says so; OnBackgroundFailure is still its one record.
        var logger = new ListLogger();
        var producer = new FakeKafkaProducerClient();
        var notified = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var subscriber = FastRetries(new KafkaSubscriberOptions { MaxDeliveryAttempts = 5 }.UseAckAfterEnqueue(1, 8));
        subscriber.OnBackgroundFailure = _ =>
        {
            notified.TrySetResult();
            return ValueTask.CompletedTask;
        };
        var options = KafkaTestData.NewOptions();
        options.DeadLetterEnabled = false;
        var dispatcher = CreateDispatcher(
            (_, _) => Task.FromException(new DurableFlowInterruptedException("Host is stopping.")),
            subscriber,
            options,
            producer: producer,
            logger: logger);

        await dispatcher.HandleAsync(KafkaTestData.Delivery(Topic, offset: 1), CancellationToken.None);
        await notified.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await dispatcher.DisposeAsync();

        Assert.Empty(producer.Publishes);
        Assert.DoesNotContain(logger.Entries, entry => entry.Level == LogLevel.Warning && entry.Message.Contains("handed back", StringComparison.Ordinal));
        var error = Assert.Single(logger.Entries, entry => entry.Level >= LogLevel.Error);
        Assert.Contains("no dead-letter destination is configured, so the wake-up is lost", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Queued_DrainLapse_DisposeItselfDeadLettersAndSurfacesTheQueuedMessages_BeforeReturning()
    {
        // Regression (r1 GS5#4): once the drain budget lapsed, the messages still queued were
        // buried only by the worker loop's lapse branch — which runs only when a busy worker frees
        // up. With the one worker still inside a long handler, DisposeAsync returned with nothing
        // buried, and the committed messages raced the host disposing the producer and the process
        // exiting. A reserved quarter of the budget now buries them from DisposeAsync itself.
        var producer = new FakeKafkaProducerClient();
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var failures = new System.Collections.Concurrent.ConcurrentQueue<KafkaBackgroundFailureContext>();
        var subscriber = new KafkaSubscriberOptions().UseAckAfterEnqueue(1, 8, TimeSpan.FromSeconds(2)); // a 500 ms reserve: generous on a loaded runner
        subscriber.OnBackgroundFailure = context =>
        {
            failures.Enqueue(context);
            return ValueTask.CompletedTask;
        };
        var dispatcher = CreateDispatcher(
            async (delivery, _) =>
            {
                if (delivery.Offset == 1)
                {
                    started.TrySetResult();
                    await release.Task.ConfigureAwait(false); // ignores the drain token, like the ingress
                }
            },
            subscriber,
            producer: producer);

        try
        {
            await dispatcher.HandleAsync(KafkaTestData.Delivery(Topic, offset: 1), CancellationToken.None);
            await started.Task.WaitAsync(TimeSpan.FromSeconds(5));
            await dispatcher.HandleAsync(KafkaTestData.Delivery(Topic, offset: 2), CancellationToken.None);
            await dispatcher.HandleAsync(KafkaTestData.Delivery(Topic, offset: 3), CancellationToken.None);

            await dispatcher.DisposeAsync();

            // Asserted the moment DisposeAsync returned — while the producer is still alive.
            Assert.Equal([2L, 3L], failures.Select(failure => failure.Offset).Order().ToArray());
            Assert.Equal(
                ["2", "3"],
                producer.Publishes.Select(publish => FakeKafkaProducerClient.Header(publish.Headers, "sourceOffset")).Order().ToArray());
            Assert.All(
                producer.Publishes,
                publish => Assert.Equal("drain_budget_lapsed_after_commit", FakeKafkaProducerClient.Header(publish.Headers, "reason")));
        }
        finally
        {
            release.TrySetResult();
        }
    }

    [Fact]
    public async Task Queued_DrainLapseDuringARetryBackoff_DeadLettersTheMessage_NotOnlyNotifies()
    {
        // Regression (r1 GS5#4): a committed message whose handler failed and whose retry backoff
        // the lapse cut short was only notified — never buried — while an unstarted one on the
        // very same lapse was dead-lettered.
        var producer = new FakeKafkaProducerClient();
        var failedOnce = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var subscriber = new KafkaSubscriberOptions
        {
            MaxDeliveryAttempts = 5,
            HandlerRetryBaseDelay = TimeSpan.FromSeconds(30),
            HandlerRetryMaxDelay = TimeSpan.FromSeconds(30)
        }.UseAckAfterEnqueue(1, 8, TimeSpan.FromMilliseconds(200));
        var dispatcher = CreateDispatcher(
            (_, _) =>
            {
                failedOnce.TrySetResult();
                return Task.FromException(new InvalidOperationException("boom"));
            },
            subscriber,
            producer: producer);

        await dispatcher.HandleAsync(KafkaTestData.Delivery(Topic, offset: 4), CancellationToken.None);
        await failedOnce.Task.WaitAsync(TimeSpan.FromSeconds(5)); // now inside its 15-30 s retry backoff
        await dispatcher.DisposeAsync();

        await KafkaTestData.WaitUntilAsync(() => producer.Publishes.Count == 1);
        var buried = Assert.Single(producer.Publishes);
        Assert.Equal("4", FakeKafkaProducerClient.Header(buried.Headers, "sourceOffset"));
        Assert.Equal("drain_budget_lapsed_after_commit", FakeKafkaProducerClient.Header(buried.Headers, "reason"));
    }

    [Fact]
    public async Task Awaiting_ADetachedHandlerWhosePartitionWasRevokedMeanwhile_NeverStoresItsOffset()
    {
        // Regression (r1 S7#6): a detached handler settled after its partition had been revoked
        // (moved to a peer, which committed further — and possibly handed back since) still stored
        // its offset. Once the partition is assigned again librdkafka accepts that store, and
        // Kafka's offset commit is not monotonic: the group's committed position rewound to it and
        // the peer's work since was re-consumed on the next restart or rebalance.
        var consumer = new FakeKafkaConsumerClient();
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var dispatcher = CreateDispatcher(
            async (_, _) => await release.Task,
            new KafkaSubscriberOptions { DetachHandlerAfter = TimeSpan.Zero },
            consumer: consumer);

        dispatcher.Accept(KafkaTestData.Delivery(Topic, offset: 500, partition: 3), CancellationToken.None);
        Assert.True(dispatcher.HasDetachedWork);

        consumer.Revoke(3); // a rebalance took P3 away (and may have handed it back)
        release.SetResult();
        await KafkaTestData.WaitUntilAsync(() =>
        {
            dispatcher.SettleCompleted();
            return !dispatcher.HasDetachedWork;
        });

        Assert.Empty(consumer.StoredOffsets);
    }

    [Fact]
    public async Task Awaiting_AMessageOfTheReassignedPartition_IsNotHeldBehindAnOrphanedHandler()
    {
        // Regression (r1 S7#6): after P3 moved away and came back, P3@621 was held behind the stale
        // @500 handler for as long as that one still ran — a 45-minute flow park stalled the whole
        // partition. The orphan no longer holds anything, and never stores its offset.
        // r2 S11#16(e): "never stores" is asserted only once the orphan's settlement has really
        // completed — observed through its outcome log, written by a continuation of that task —
        // and a settlement tick has run after it; a signal from inside the handler fired before
        // the settlement finished, so the old assertion ran ahead of the path it was pinning.
        var consumer = new FakeKafkaConsumerClient();
        var logger = new ListLogger();
        var releaseStale = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var freshHandled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var dispatcher = CreateDispatcher(
            async (delivery, _) =>
            {
                if (delivery.Offset == 500)
                {
                    await releaseStale.Task;
                    return;
                }

                freshHandled.TrySetResult();
            },
            new KafkaSubscriberOptions { DetachHandlerAfter = TimeSpan.FromMilliseconds(20) },
            consumer: consumer,
            logger: logger);

        try
        {
            dispatcher.Accept(KafkaTestData.Delivery(Topic, offset: 500, partition: 3), CancellationToken.None);
            Assert.True(dispatcher.HasDetachedWork);

            consumer.Revoke(3); // P3 moved to a peer, which committed up to 621, then came back
            dispatcher.Accept(KafkaTestData.Delivery(Topic, offset: 621, partition: 3), CancellationToken.None);

            await freshHandled.Task.WaitAsync(TimeSpan.FromSeconds(5));
            await KafkaTestData.WaitUntilAsync(() =>
            {
                dispatcher.SettleCompleted();
                return consumer.StoredOffsets.Count == 1;
            });

            releaseStale.SetResult();
            await KafkaTestData.WaitUntilAsync(() => logger.Entries.Any(entry => IsOrphanCompletion(entry.Message, 500)));
            dispatcher.SettleCompleted();

            Assert.Equal(new FakeKafkaConsumerClient.StoredOffset(Topic, 3, 621), Assert.Single(consumer.StoredOffsets));
        }
        finally
        {
            // Released on every path: disposal waits for detached handlers, so a failed assertion
            // above must not leave the stale one gated for ever.
            releaseStale.TrySetResult();
            await dispatcher.DisposeAsync();
        }
    }

    [Fact]
    public async Task Awaiting_Dispose_NeitherWaitsForNorStoresAHandlerThatOutlivedItsPartition()
    {
        // r1 S7#6 on the stop path: the unbounded stop-time wait exists to store detached offsets
        // before the close commits — for a revoked partition there is nothing of ours to store, so
        // the stop neither waits for that handler nor stores its offset.
        // r2 S11#16(e): the disposal is awaited and must have completed SUCCESSFULLY while the
        // handler was still gated (IsCompleted is true for a faulted dispose too), and "never
        // stores" is asserted once the orphan's completion path has actually run.
        var consumer = new FakeKafkaConsumerClient();
        var logger = new ListLogger();
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var dispatcher = CreateDispatcher(
            async (_, _) => await release.Task,
            new KafkaSubscriberOptions { DetachHandlerAfter = TimeSpan.Zero },
            consumer: consumer,
            logger: logger);

        dispatcher.Accept(KafkaTestData.Delivery(Topic, offset: 500, partition: 3), CancellationToken.None);
        consumer.Revoke(3);

        var disposal = dispatcher.DisposeAsync();
        try
        {
            Assert.True(disposal.IsCompletedSuccessfully);
        }
        finally
        {
            release.SetResult();
        }

        await disposal;
        await KafkaTestData.WaitUntilAsync(() => logger.Entries.Any(entry => IsOrphanCompletion(entry.Message, 500)));
        Assert.Empty(consumer.StoredOffsets);
    }

    /// <summary>The outcome log a revoked handler's settlement writes once it has completed.</summary>
    private static bool IsOrphanCompletion(string message, long offset)
        => message.StartsWith("Orphaned Kafka handler", StringComparison.Ordinal)
            && message.Contains($"@{offset} completed", StringComparison.Ordinal);

    [Fact]
    public async Task Awaiting_APartitionHandedStraightBack_RedeliveringTheRunningMessage_HoldsTheCopyBehindIt()
    {
        // Regression (r1 critic H1): the default (eager) assignor revokes EVERY partition on every
        // rebalance — any member joining or leaving, so every rolling deploy — and hands most of
        // them straight back. The new assignment re-fetches from the group's committed offset,
        // which is the still-running message itself (its offset is stored only once it settles).
        // That copy was taken for a message of a NEW assignment: the running handler was orphaned
        // and the copy started alongside it — two concurrent runs of one message (a durable flow's
        // second copy contends its own lease and is dead-lettered). A first message at or before
        // the running one shows nobody moved past it: the handler keeps the partition, the copy
        // waits behind it and is dropped once the handler has settled the same offset.
        var consumer = new FakeKafkaConsumerClient();
        var firstStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var running = 0;
        var maxConcurrent = 0;
        var runsOf500 = 0;
        var dispatcher = CreateDispatcher(
            async (delivery, _) =>
            {
                var now = Interlocked.Increment(ref running);
                int seen;
                while (now > (seen = Volatile.Read(ref maxConcurrent)) && Interlocked.CompareExchange(ref maxConcurrent, now, seen) != seen)
                {
                }

                try
                {
                    if (delivery.Offset == 500)
                    {
                        Interlocked.Increment(ref runsOf500);
                        firstStarted.TrySetResult();
                        await release.Task;
                    }
                }
                finally
                {
                    Interlocked.Decrement(ref running);
                }
            },
            new KafkaSubscriberOptions { DetachHandlerAfter = TimeSpan.FromMilliseconds(20) },
            consumer: consumer);

        try
        {
            dispatcher.Accept(KafkaTestData.Delivery(Topic, offset: 500, partition: 3), CancellationToken.None);
            await firstStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.True(dispatcher.HasDetachedWork);

            consumer.Revoke(3); // an eager rebalance revoked P3 and handed it straight back
            dispatcher.Accept(KafkaTestData.Delivery(Topic, offset: 500, partition: 3), CancellationToken.None); // re-fetched from the committed offset
            dispatcher.Accept(KafkaTestData.Delivery(Topic, offset: 501, partition: 3), CancellationToken.None); // fetched before the re-asserted pause

            Assert.Equal(1, Volatile.Read(ref runsOf500));
            Assert.True(consumer.IsPartitionPaused(3));

            release.SetResult();
            await KafkaTestData.WaitUntilAsync(() =>
            {
                dispatcher.SettleCompleted();
                return consumer.StoredOffsets.Count >= 2 && !dispatcher.HasDetachedWork;
            });

            Assert.Equal(
                [new FakeKafkaConsumerClient.StoredOffset(Topic, 3, 500), new FakeKafkaConsumerClient.StoredOffset(Topic, 3, 501)],
                consumer.StoredOffsets);
            Assert.Equal(1, Volatile.Read(ref runsOf500));
            Assert.Equal(1, Volatile.Read(ref maxConcurrent));
            Assert.False(consumer.IsPartitionPaused(3));
        }
        finally
        {
            release.TrySetResult();
            await dispatcher.DisposeAsync();
        }
    }

    [Fact]
    public async Task Queued_StoreOffsetForARevokedPartition_IsLoggedAsRoutine_NotAsAnError()
    {
        // Regression (r1 critic H7): the ack-after-handler settlement already logged librdkafka's
        // refusal to store for a revoked partition at Information; the early-ACK store after
        // enqueue still logged the same routine rebalance at Error.
        var logger = new ListLogger();
        var consumer = new FakeKafkaConsumerClient
        {
            StoreOffsetException = new KafkaPartitionNotAssignedException(Topic, 0, new InvalidOperationException("Local_State"))
        };
        var handled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var dispatcher = CreateDispatcher(
            (_, _) =>
            {
                handled.TrySetResult();
                return Task.CompletedTask;
            },
            new KafkaSubscriberOptions().UseAckAfterEnqueue(1, 8),
            consumer: consumer,
            logger: logger);

        await dispatcher.HandleAsync(KafkaTestData.Delivery(Topic, offset: 3), CancellationToken.None);
        await handled.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await dispatcher.DisposeAsync();

        Assert.DoesNotContain(logger.Entries, entry => entry.Level >= LogLevel.Error);
        Assert.Contains(logger.Entries, entry => entry.Level == LogLevel.Information && entry.Message.Contains("revoked", StringComparison.Ordinal));
    }

    [Fact]
    public async Task StoreOffsetForARevokedPartition_IsLoggedAsRoutine_NotAsAnError()
    {
        // Regression (r1 S7#18): librdkafka refusing a store because a rebalance revoked the
        // partition is routine in a consumer group — its new owner re-consumes the message — yet
        // every such settlement logged at Error.
        var logger = new ListLogger();
        var consumer = new FakeKafkaConsumerClient
        {
            StoreOffsetException = new KafkaPartitionNotAssignedException(Topic, 0, new InvalidOperationException("Local_State"))
        };
        await using var dispatcher = CreateDispatcher(
            (_, _) => Task.CompletedTask,
            new KafkaSubscriberOptions(),
            consumer: consumer,
            logger: logger);

        await dispatcher.HandleAsync(KafkaTestData.Delivery(Topic, offset: 3), CancellationToken.None);

        Assert.DoesNotContain(logger.Entries, entry => entry.Level >= LogLevel.Error);
        Assert.Contains(logger.Entries, entry => entry.Level == LogLevel.Information && entry.Message.Contains("revoked", StringComparison.Ordinal));
    }

    [Fact]
    public async Task DeadLetter_AReplayedRecordsEarlierBurialHeaders_AreReplaced_NotStacked()
    {
        // Regression (r1 GS5#8): a record replayed from a dead-letter topic already carries a
        // burial set; the copy APPENDED a second one, so every first-match reader saw the stale
        // sourceTopic/reason/exceptionMessage, and each replay cycle grew the record.
        var producer = new FakeKafkaProducerClient();
        await using var dispatcher = CreateDispatcher(
            (_, _) => throw new InvalidOperationException("boom"),
            FastRetries(new KafkaSubscriberOptions { MaxDeliveryAttempts = 1 }),
            producer: producer);

        var replayed = new KafkaDelivery(
            Topic,
            0,
            8,
            "payload",
            "corr",
            [
                KafkaTransportHeader.Utf8("correlationId", "corr"),
                KafkaTransportHeader.Utf8("sourceTopic", "older.topic"),
                KafkaTransportHeader.Utf8("sourceOffset", "3"),
                KafkaTransportHeader.Utf8("reason", "handler_failed_max_attempts"),
                KafkaTransportHeader.Utf8("exceptionMessage", "the old failure")
            ]);
        await dispatcher.HandleAsync(replayed, CancellationToken.None);

        var headers = Assert.Single(producer.Publishes).Headers;
        Assert.Equal(Topic, Assert.Single(headers, header => header.Key == "sourceTopic").ValueUtf8);
        Assert.Single(headers, header => header.Key == "reason");
        Assert.Equal("boom", Assert.Single(headers, header => header.Key == "exceptionMessage").ValueUtf8);
        Assert.Equal("corr", Assert.Single(headers, header => header.Key == "correlationId").ValueUtf8);
    }

    [Fact]
    public async Task DeadLetter_AUsersOwnGenericHeadersOnARecordNeverBuried_AreKept()
    {
        // Regression (r1 critic H5): the burial-set replacement stripped every header named like a
        // burial one — "reason", "attempts", "occurredAtUtc", "consumerGroup" — from ANY record,
        // so a producer's own header of that name vanished from the dead-letter copy. Only a record
        // that carries a previous burial set (sourceTopic AND sourceOffset) is stripped now.
        var producer = new FakeKafkaProducerClient();
        await using var dispatcher = CreateDispatcher(
            (_, _) => throw new InvalidOperationException("boom"),
            FastRetries(new KafkaSubscriberOptions { MaxDeliveryAttempts = 1 }),
            producer: producer);

        var delivery = new KafkaDelivery(
            Topic,
            0,
            8,
            "payload",
            "corr",
            [
                KafkaTransportHeader.Utf8("correlationId", "corr"),
                KafkaTransportHeader.Utf8("reason", "customer-requested"),
                KafkaTransportHeader.Utf8("attempts", "user-3")
            ]);
        await dispatcher.HandleAsync(delivery, CancellationToken.None);

        var headers = Assert.Single(producer.Publishes).Headers;
        Assert.Contains(headers, header => header.Key == "reason" && header.ValueUtf8 == "customer-requested");
        Assert.Contains(headers, header => header.Key == "attempts" && header.ValueUtf8 == "user-3");
        Assert.Contains(headers, header => header.Key == "reason" && header.ValueUtf8 == "handler_failed_max_attempts");
    }

    [Fact]
    public async Task DeadLetter_ShortensTheExceptionHeaders_SoTheCopyFitsTheProducersMessageMaxBytes()
    {
        // Regression (r1 GS5#8): the per-header cap still let the copy carry ~8 KB of exception
        // text on top of the record, so a message within that margin of message.max.bytes could
        // never be buried — and a burial that fails for good restarts the subscriber for ever.
        var options = KafkaTestData.NewOptions();
        options.ConfigureProducer = config => config.MessageMaxBytes = 10_000;
        var producer = new FakeKafkaProducerClient();
        await using var dispatcher = CreateDispatcher(
            (_, _) => throw new InvalidOperationException(new string('x', 5_000)),
            FastRetries(new KafkaSubscriberOptions { MaxDeliveryAttempts = 1 }),
            options: options,
            producer: producer);

        var payload = new string('p', 8_000);
        await dispatcher.HandleAsync(KafkaTestData.Delivery(Topic, offset: 1, payload: payload), CancellationToken.None);

        var buried = Assert.Single(producer.Publishes);
        var recordBytes = System.Text.Encoding.UTF8.GetByteCount(buried.Key ?? string.Empty)
            + System.Text.Encoding.UTF8.GetByteCount(buried.Payload)
            + buried.Headers.Sum(header => System.Text.Encoding.UTF8.GetByteCount(header.Key) + (header.Value?.Length ?? 0));
        Assert.True(recordBytes < 10_000, $"the dead-letter copy is {recordBytes} bytes, past message.max.bytes");
        Assert.InRange(FakeKafkaProducerClient.Header(buried.Headers, "exceptionMessage")!.Length, 1, KafkaMessageDispatcher.MaxDeadLetterHeaderLength - 1);
    }

    [Fact]
    public async Task DeadLetter_CapsTheExceptionHeadersAtMaxDeadLetterHeaderLength()
    {
        // r1 S7#11: round 42's per-header cap had no test.
        var producer = new FakeKafkaProducerClient();
        await using var dispatcher = CreateDispatcher(
            (_, _) => throw new InvalidOperationException(new string('x', 10_000)),
            FastRetries(new KafkaSubscriberOptions { MaxDeliveryAttempts = 1 }),
            producer: producer);

        await dispatcher.HandleAsync(KafkaTestData.Delivery(Topic, offset: 1), CancellationToken.None);

        Assert.Equal(
            KafkaMessageDispatcher.MaxDeadLetterHeaderLength,
            FakeKafkaProducerClient.Header(Assert.Single(producer.Publishes).Headers, "exceptionMessage")!.Length);
    }

    // ---------- Fixpoint round 2 (G9) ----------

    [Fact]
    public async Task DeadLetter_ARecordThatLeavesNoRoomForTheBurialHeaders_FailsNamingTheSize_WithoutAProduce()
    {
        // Regression (r2 S7#4): only the two exception headers were cut to fit message.max.bytes;
        // the fixed burial headers were not. A record within ~270 bytes of the limit (a foreign
        // one, or one published with a larger limit) produced a copy librdkafka rejects locally on
        // every attempt, so the subscriber restarted on it for ever — and the error told the
        // operator to "fix the dead-letter topic", which cannot help. It now fails before the
        // produce with the size cause and the ConfigureProducer remedy (the stall itself is the
        // standing position-commit design).
        var options = KafkaTestData.NewOptions();
        options.ConfigureProducer = config => config.MessageMaxBytes = 10_000;
        var producer = new FakeKafkaProducerClient();
        await using var dispatcher = CreateDispatcher(
            (_, _) => throw new InvalidOperationException("boom"),
            FastRetries(new KafkaSubscriberOptions { MaxDeliveryAttempts = 1 }),
            options: options,
            producer: producer);

        var ex = await Assert.ThrowsAsync<KafkaDeadLetterPublishFailedException>(() =>
            dispatcher.HandleAsync(KafkaTestData.Delivery(Topic, offset: 1, payload: new string('p', 9_900)), CancellationToken.None));

        var tooLarge = Assert.IsType<KafkaDeadLetterTooLargeException>(ex.InnerException);
        Assert.Equal(10_000, tooLarge.MessageMaxBytes);
        Assert.True(tooLarge.RequiredBytes > 10_000);
        Assert.Contains("ConfigureProducer", ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("fix the dead-letter topic", ex.Message, StringComparison.Ordinal);
        Assert.Empty(producer.Publishes);
    }

    [Fact]
    public void ValidateOptions_JudgesThePollGapByTheMaxPollIntervalConfigureConsumerSets()
    {
        // Regression (r2 S7#5): ConfigureConsumer runs last and may set max.poll.interval.ms, but
        // the poll-gap rule read the MaxPollInterval option: a 60 s override passed validation for
        // a 35 s gap that only fits half of the 5-minute option.
        var options = KafkaTestData.NewOptions();
        options.ConfigureConsumer = config => config.MaxPollIntervalMs = 60_000;
        var subscriber = new KafkaSubscriberOptions
        {
            DetachHandlerAfter = TimeSpan.FromSeconds(25),
            PollTimeout = TimeSpan.FromSeconds(10)
        };

        var ex = Assert.Throws<InvalidOperationException>(() =>
            KafkaMessageDispatcher.ValidateOptions(options, subscriber, KafkaSubscriberRole.Worker));

        Assert.Contains(nameof(KafkaAsyncResponseTransportOptions.ConfigureConsumer), ex.Message, StringComparison.Ordinal);
        Assert.Contains("00:01:00", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task DiscardUnprocessable_BoundsTheDeadLetterProduceByTheMaxPollIntervalTheConsumerRunsWith()
    {
        // Regression (r2 S7#5): the burial budget was a quarter of the MaxPollInterval OPTION —
        // 75 s by default — even with ConfigureConsumer setting a shorter max.poll.interval.ms, so
        // an unprocessable-message burial blocking the poll thread outlived the interval the
        // consumer actually ran with and got it evicted: the storm the budget exists to prevent.
        var producer = new HangingKafkaProducerClient();
        var options = KafkaTestData.NewOptions();
        options.ConfigureConsumer = config => config.MaxPollIntervalMs = 400;
        await using var dispatcher = KafkaMessageDispatcher.Create(
            (_, _) => Task.CompletedTask,
            new FakeKafkaConsumerClient(),
            producer,
            options,
            new KafkaSubscriberOptions
            {
                MaxDeliveryAttempts = 0,
                PollTimeout = TimeSpan.FromMilliseconds(10),
                DetachHandlerAfter = TimeSpan.FromMilliseconds(50)
            },
            NullLogger.Instance,
            Topic,
            Group,
            KafkaSubscriberRole.Worker);

        await Assert.ThrowsAsync<KafkaDeadLetterPublishFailedException>(() => dispatcher.DiscardUnprocessableAsync(
            KafkaTestData.Message(Topic, offset: 4, payload: ""),
            new InvalidDataException("no payload"),
            CancellationToken.None).WaitAsync(TimeSpan.FromSeconds(5)));

        Assert.True(producer.SawCancellation);
    }

    [Fact]
    public async Task Awaiting_HeldUnprocessableMessages_AreBuriedOffThePollThread()
    {
        // Regression (r2 S7#6): once a detached handler settled, every unprocessable message held
        // behind it was buried on the poll thread, back to back within one tick — each produce
        // bounded only by a quarter of max.poll.interval.ms, with no Consume in between, so a few
        // slow produces on a degraded broker overran the interval and evicted the consumer. The
        // burial now runs detached like a handler: the tick returns while it is still producing,
        // and its offset is stored — in order — on a later tick.
        var consumer = new FakeKafkaConsumerClient();
        var publishStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releasePublish = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var producer = new FakeKafkaProducerClient
        {
            PublishDelay = async _ =>
            {
                publishStarted.TrySetResult();
                await releasePublish.Task;
            }
        };
        var releaseHandler = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var dispatcher = CreateDispatcher(
            async (_, _) => await releaseHandler.Task,
            new KafkaSubscriberOptions { DetachHandlerAfter = TimeSpan.Zero },
            consumer: consumer,
            producer: producer);

        try
        {
            dispatcher.Accept(KafkaTestData.Delivery(Topic, offset: 10, partition: 3), CancellationToken.None);
            dispatcher.AcceptUnprocessable(KafkaTestData.MessageOn(Topic, 3, 11, ""), new InvalidDataException("no payload"), CancellationToken.None);
            dispatcher.AcceptUnprocessable(KafkaTestData.MessageOn(Topic, 3, 12, ""), new InvalidDataException("no payload"), CancellationToken.None);
            releaseHandler.SetResult();

            // Poll-thread ticks until the handler's offset is stored — the tick that then reaches the
            // held burials. On a separate thread, bounded: the old tick never came back from them.
            var ticks = Task.Run(() =>
            {
                while (consumer.StoredOffsets.Count == 0)
                {
                    dispatcher.SettleCompleted();
                    Thread.Sleep(2);
                }
            });
            await publishStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));
            await ticks.WaitAsync(TimeSpan.FromSeconds(10)); // returned with the burial still producing

            Assert.Equal([10L], consumer.StoredOffsets.Select(stored => stored.Offset));
            Assert.Empty(producer.Publishes);

            releasePublish.SetResult();
            await KafkaTestData.WaitUntilAsync(() =>
            {
                dispatcher.SettleCompleted();
                return consumer.StoredOffsets.Count == 3;
            });

            Assert.Equal([10L, 11L, 12L], consumer.StoredOffsets.Select(stored => stored.Offset));
            Assert.Equal(
                ["11", "12"],
                producer.Publishes.Select(publish => FakeKafkaProducerClient.Header(publish.Headers, "sourceOffset")));
        }
        finally
        {
            releaseHandler.TrySetResult();
            releasePublish.TrySetResult();
            await dispatcher.DisposeAsync();
        }
    }

    [Fact]
    public async Task Queued_AHandBackAfterTheDrainLapsed_IsBuriedAsAHandBack_NotAsALapse()
    {
        // Regression (r2 S7#8): DurableFlowInterruptedException IS an OperationCanceledException,
        // and once the drain had lapsed the lapse filter caught it before the hand-back branch — a
        // flow step still running at the lapse that then reached a timer on the stopping host was
        // buried as drain_budget_lapsed_after_commit, which replay tooling reads as "never started".
        var producer = new FakeKafkaProducerClient();
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var subscriber = new KafkaSubscriberOptions { MaxDeliveryAttempts = 5 }.UseAckAfterEnqueue(1, 8, TimeSpan.FromMilliseconds(400));
        var dispatcher = CreateDispatcher(
            async (_, drainToken) =>
            {
                started.TrySetResult();
                try
                {
                    await Task.Delay(Timeout.Infinite, drainToken); // a step still running at the lapse...
                }
                catch (OperationCanceledException)
                {
                    // ...that then reaches a timer on the stopping host.
                    throw new DurableFlowInterruptedException("Host is stopping; the wake-up is handed back.");
                }
            },
            subscriber,
            producer: producer);

        await dispatcher.HandleAsync(KafkaTestData.Delivery(Topic, offset: 3), CancellationToken.None);
        await started.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await dispatcher.DisposeAsync();

        await KafkaTestData.WaitUntilAsync(() => producer.Publishes.Count == 1);
        Assert.Equal("handed_back_after_commit", FakeKafkaProducerClient.Header(Assert.Single(producer.Publishes).Headers, "reason"));
    }

    [Fact]
    public async Task Awaiting_DeadLetteringDisabled_TheCapLogClaimsNoCopy()
    {
        // Regression (r2 S7#9): with DeadLetterEnabled = false the cap path logged "producing to
        // dead-letter topic" and then stored the offset: the message was silently discarded.
        var logger = new ListLogger();
        var options = KafkaTestData.NewOptions();
        options.DeadLetterEnabled = false;
        await using var dispatcher = CreateDispatcher(
            (_, _) => throw new InvalidOperationException("boom"),
            FastRetries(new KafkaSubscriberOptions { MaxDeliveryAttempts = 1 }),
            options: options,
            logger: logger);

        await dispatcher.HandleAsync(KafkaTestData.Delivery(Topic, offset: 1), CancellationToken.None);

        Assert.DoesNotContain(logger.Entries, entry => entry.Message.Contains("producing to dead-letter", StringComparison.Ordinal));
        var error = Assert.Single(logger.Entries, entry => entry.Level == LogLevel.Error && entry.Message.Contains("max delivery attempts", StringComparison.Ordinal));
        Assert.Contains("dead-lettering is disabled", error.Message, StringComparison.Ordinal);
        Assert.Contains("no copy is kept", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task DiscardUnprocessable_DeadLetteringDisabled_TheLogClaimsNoCopy()
    {
        // Regression (r2 S7#9): "dead-lettering and committing it" was logged with no copy written.
        var logger = new ListLogger();
        var options = KafkaTestData.NewOptions();
        options.DeadLetterEnabled = false;
        var consumer = new FakeKafkaConsumerClient();
        await using var dispatcher = CreateDispatcher(
            (_, _) => Task.CompletedTask,
            new KafkaSubscriberOptions(),
            options: options,
            consumer: consumer,
            logger: logger);

        await dispatcher.DiscardUnprocessableAsync(
            KafkaTestData.Message(Topic, offset: 4, payload: ""),
            new InvalidDataException("no payload"),
            CancellationToken.None);

        Assert.DoesNotContain(logger.Entries, entry => entry.Message.Contains("dead-lettering and committing", StringComparison.Ordinal));
        var error = Assert.Single(logger.Entries, entry => entry.Level == LogLevel.Error);
        Assert.Contains("dead-lettering is disabled", error.Message, StringComparison.Ordinal);
        Assert.Equal(4, Assert.Single(consumer.StoredOffsets).Offset);
    }

    [Fact]
    public async Task Queued_DrainLapse_WithDeadLetteringDisabled_SaysEachUnstartedMessageIsLost_NotDeadLettered()
    {
        // Regression (r2 S7#9): the lapse paths logged "Dead-lettering ..." (the stop's warning and
        // each unstarted entry's) with no copy written — for an already-committed message, a loss.
        var logger = new ListLogger();
        var options = KafkaTestData.NewOptions();
        options.DeadLetterEnabled = false;
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var dispatcher = CreateDispatcher(
            async (delivery, _) =>
            {
                if (delivery.Offset == 1)
                {
                    started.TrySetResult();
                    await release.Task.ConfigureAwait(false); // ignores the drain token, like the ingress
                }
            },
            new KafkaSubscriberOptions().UseAckAfterEnqueue(1, 8, TimeSpan.FromSeconds(2)),
            options,
            logger: logger);

        try
        {
            await dispatcher.HandleAsync(KafkaTestData.Delivery(Topic, offset: 1), CancellationToken.None);
            await started.Task.WaitAsync(TimeSpan.FromSeconds(5));
            await dispatcher.HandleAsync(KafkaTestData.Delivery(Topic, offset: 2), CancellationToken.None);

            await dispatcher.DisposeAsync();
        }
        finally
        {
            release.TrySetResult();
        }

        Assert.DoesNotContain(logger.Entries, entry => entry.Message.Contains("Dead-lettering", StringComparison.OrdinalIgnoreCase)
            || entry.Message.Contains("dead-lettered a copy", StringComparison.OrdinalIgnoreCase));
        var lost = Assert.Single(logger.Entries, entry => entry.Level == LogLevel.Error && entry.Message.Contains("@2", StringComparison.Ordinal));
        Assert.Contains("no dead-letter destination is configured, so the message is lost", lost.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Queued_AHandBack_IsDeadLetteredBeforeOnBackgroundFailureIsAwaited()
    {
        // Regression (r2 S6a#5): the early-ACK arms awaited OnBackgroundFailure BEFORE the dead-letter
        // produce. A slow callback during the stop (an alert on a timeout-less HTTP client) held the
        // only durable record back until the drain lapsed and the producer was disposed. The copy
        // is written first now: it exists by the time the callback runs.
        var producer = new FakeKafkaProducerClient();
        var callbackEntered = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseCallback = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var subscriber = FastRetries(new KafkaSubscriberOptions { MaxDeliveryAttempts = 5 }.UseAckAfterEnqueue(1, 8));
        subscriber.OnBackgroundFailure = async _ =>
        {
            callbackEntered.TrySetResult(producer.Publishes.Count);
            await releaseCallback.Task;
        };
        var dispatcher = CreateDispatcher(
            (_, _) => Task.FromException(new DurableFlowInterruptedException("Host is stopping.")),
            subscriber,
            producer: producer);

        try
        {
            await dispatcher.HandleAsync(KafkaTestData.Delivery(Topic, offset: 1), CancellationToken.None);

            Assert.Equal(1, await callbackEntered.Task.WaitAsync(TimeSpan.FromSeconds(5)));
        }
        finally
        {
            releaseCallback.TrySetResult();
            await dispatcher.DisposeAsync();
        }
    }

    [Fact]
    public async Task Queued_AFailureAtTheCap_IsDeadLetteredBeforeOnBackgroundFailureIsAwaited()
    {
        // r2 S6a#5, the background_handler_failed_after_commit arm.
        var producer = new FakeKafkaProducerClient();
        var callbackEntered = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseCallback = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var subscriber = FastRetries(new KafkaSubscriberOptions { MaxDeliveryAttempts = 1 }.UseAckAfterEnqueue(1, 8));
        subscriber.OnBackgroundFailure = async _ =>
        {
            callbackEntered.TrySetResult(producer.Publishes.Count);
            await releaseCallback.Task;
        };
        var dispatcher = CreateDispatcher(
            (_, _) => Task.FromException(new InvalidOperationException("boom")),
            subscriber,
            producer: producer);

        try
        {
            await dispatcher.HandleAsync(KafkaTestData.Delivery(Topic, offset: 1), CancellationToken.None);

            Assert.Equal(1, await callbackEntered.Task.WaitAsync(TimeSpan.FromSeconds(5)));
        }
        finally
        {
            releaseCallback.TrySetResult();
            await dispatcher.DisposeAsync();
        }
    }

    [Fact]
    public async Task Queued_DrainLapse_BuriesEveryQueuedEntryFirst_AndBoundsTheCallbacksByTheReserve()
    {
        // Regression (r2 S6b#5): the dispose reserve surfaced each still-queued entry through
        // OnBackgroundFailure before burying it, and awaited that callback with no bound — one
        // callback writing to a database that is down held DisposeAsync past its budget, and the
        // entries behind it were neither buried nor counted. Every entry is buried first now, and
        // each callback is waited for only as long as the reserve lasts.
        var producer = new FakeKafkaProducerClient();
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var callbacksHang = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var notified = new System.Collections.Concurrent.ConcurrentQueue<long>();
        var subscriber = new KafkaSubscriberOptions().UseAckAfterEnqueue(1, 8, TimeSpan.FromSeconds(2)); // a 500 ms reserve
        subscriber.OnBackgroundFailure = async context =>
        {
            notified.Enqueue(context.Offset);
            await callbacksHang.Task;
        };
        var dispatcher = CreateDispatcher(
            async (delivery, _) =>
            {
                if (delivery.Offset == 1)
                {
                    started.TrySetResult();
                    await release.Task.ConfigureAwait(false); // ignores the drain token, like the ingress
                }
            },
            subscriber,
            producer: producer);

        try
        {
            await dispatcher.HandleAsync(KafkaTestData.Delivery(Topic, offset: 1), CancellationToken.None);
            await started.Task.WaitAsync(TimeSpan.FromSeconds(5));
            await dispatcher.HandleAsync(KafkaTestData.Delivery(Topic, offset: 2), CancellationToken.None);
            await dispatcher.HandleAsync(KafkaTestData.Delivery(Topic, offset: 3), CancellationToken.None);

            // Bounded by the drain budget, not by the hanging callback.
            await dispatcher.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10));

            Assert.Equal(
                ["2", "3"],
                producer.Publishes.Select(publish => FakeKafkaProducerClient.Header(publish.Headers, "sourceOffset")).Order().ToArray());
            Assert.Equal([2L, 3L], notified.Order().ToArray()); // every entry still surfaced
        }
        finally
        {
            release.TrySetResult();
            callbacksHang.TrySetResult();
        }
    }

    [Fact]
    public async Task Queued_AThrowingLoggingProvider_NeitherStopsTheWorkerNorCostsTheDeadLetterCopy()
    {
        // Round-2 rule (a throwing logging provider must never change an outcome): the Error logged
        // at the cap ran before the burial, so a provider failure there escaped the worker loop —
        // no dead-letter copy for an already-committed message, and no worker left to run the jobs
        // queued behind it.
        var producer = new FakeKafkaProducerClient();
        var handled = new System.Collections.Concurrent.ConcurrentQueue<long>();
        var dispatcher = CreateDispatcher(
            (delivery, _) =>
            {
                handled.Enqueue(delivery.Offset);
                return delivery.Offset == 1
                    ? Task.FromException(new InvalidOperationException("boom"))
                    : Task.CompletedTask;
            },
            FastRetries(new KafkaSubscriberOptions { MaxDeliveryAttempts = 1 }.UseAckAfterEnqueue(1, 8)),
            producer: producer,
            logger: new ErrorThrowingLogger());

        await dispatcher.HandleAsync(KafkaTestData.Delivery(Topic, offset: 1), CancellationToken.None);
        await dispatcher.HandleAsync(KafkaTestData.Delivery(Topic, offset: 2), CancellationToken.None);

        await KafkaTestData.WaitUntilAsync(() => handled.Count == 2 && producer.Publishes.Count == 1);
        await dispatcher.DisposeAsync();

        Assert.Equal("1", FakeKafkaProducerClient.Header(Assert.Single(producer.Publishes).Headers, "sourceOffset"));
    }

    private static KafkaMessageDispatcher CreateDispatcher(
        Func<KafkaDelivery, CancellationToken, Task> handler,
        KafkaSubscriberOptions subscriberOptions,
        KafkaAsyncResponseTransportOptions? options = null,
        FakeKafkaConsumerClient? consumer = null,
        FakeKafkaProducerClient? producer = null,
        ILogger? logger = null)
    {
        options ??= KafkaTestData.NewOptions();
        options.PublishRetryBaseDelay = TimeSpan.FromMilliseconds(1);
        options.PublishRetryMaxDelay = TimeSpan.FromMilliseconds(2);
        options.HostShutdownTimeout = TimeSpan.FromSeconds(60);

        return KafkaMessageDispatcher.Create(
            handler,
            consumer ?? new FakeKafkaConsumerClient(),
            producer ?? new FakeKafkaProducerClient(),
            options,
            subscriberOptions,
            logger ?? NullLogger.Instance,
            Topic,
            Group,
            KafkaSubscriberRole.Worker);
    }

    private sealed class ListLogger : ILogger
    {
        public System.Collections.Concurrent.ConcurrentQueue<(LogLevel Level, string Message)> Entries { get; } = new();

        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull
            => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
            => Entries.Enqueue((logLevel, formatter(state, exception)));
    }
}
