using AsyncResponse.Transports.Kafka;
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
    public void ValidateOptions_RejectsRetryDelayBudgetThatCannotFitTheMaxPollInterval()
    {
        // The in-process retry delays run on the poll thread: 4 completed attempts back off
        // 20+40+80+160 = 300s of pure delay, which cannot fit within half of a 5-minute
        // max.poll.interval.ms — the broker would evict the consumer mid-retry.
        var subscriberOptions = new KafkaSubscriberOptions
        {
            MaxDeliveryAttempts = 5,
            HandlerRetryBaseDelay = TimeSpan.FromSeconds(20),
            HandlerRetryMaxDelay = TimeSpan.FromSeconds(160)
        };

        var ex = Assert.Throws<InvalidOperationException>(() =>
            KafkaMessageDispatcher.ValidateOptions(
                KafkaTestData.NewOptions(),
                subscriberOptions,
                KafkaSubscriberRole.Worker));

        Assert.Contains(nameof(KafkaSubscriberOptions.MaxPollInterval), ex.Message, StringComparison.Ordinal);
        Assert.Contains("evicted", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateOptions_AcceptsRetryBudgetOnceMaxPollIntervalIsRaised()
    {
        // The same budget passes when the operator raises the poll interval to hold it.
        KafkaMessageDispatcher.ValidateOptions(
            KafkaTestData.NewOptions(),
            new KafkaSubscriberOptions
            {
                MaxDeliveryAttempts = 5,
                HandlerRetryBaseDelay = TimeSpan.FromSeconds(20),
                HandlerRetryMaxDelay = TimeSpan.FromSeconds(160),
                MaxPollInterval = TimeSpan.FromMinutes(15)
            },
            KafkaSubscriberRole.Worker);
    }

    [Fact]
    public void ValidateOptions_UnlimitedRetries_SkipTheRetryBudgetCheck()
    {
        // MaxDeliveryAttempts = 0 has no finite delay budget; the option's doc pins staying
        // under the ceiling as the operator's responsibility.
        KafkaMessageDispatcher.ValidateOptions(
            KafkaTestData.NewOptions(),
            new KafkaSubscriberOptions
            {
                MaxDeliveryAttempts = 0,
                HandlerRetryBaseDelay = TimeSpan.FromMinutes(2),
                HandlerRetryMaxDelay = TimeSpan.FromMinutes(10)
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
    public async Task Awaiting_DeadLetterPublishFailsPermanently_SwallowsWithoutStoringOffset()
    {
        var consumer = new FakeKafkaConsumerClient();
        var producer = new FakeKafkaProducerClient { PublishException = new InvalidOperationException("broker gone") };
        await using var dispatcher = CreateDispatcher(
            (_, _) => throw new InvalidOperationException("handler boom"),
            FastRetries(new KafkaSubscriberOptions { MaxDeliveryAttempts = 1 }),
            consumer: consumer,
            producer: producer);

        // A failed burial must NOT escape: this call runs inside the poll loop, and a propagated
        // throw faulted the whole subscriber with the offset unstored — the supervisor rebuilt
        // the consumer, re-consumed the message, and re-executed the failing handler
        // MaxDeliveryAttempts more times per restart, forever.
        await dispatcher.HandleAsync(KafkaTestData.Delivery(Topic, offset: 1), CancellationToken.None);

        // The offset stays unstored so a restart or rebalance redelivers the message and the
        // burial is retried.
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
    public async Task DiscardUnprocessable_WhenTheDeadLetterPublishFailsPermanently_DoesNotFaultThePollLoop()
    {
        // Regression (round 31): the burial itself was unguarded on this path (only the offset
        // store was wrapped), and this call originates inside the poll loop's own catch arm — a
        // permanently failing dead-letter topic burned the publish retries, rethrew, faulted the
        // subscriber with the offset unstored, and the restart re-ran the same discard forever.
        var consumer = new FakeKafkaConsumerClient();
        var producer = new FakeKafkaProducerClient { PublishException = new InvalidOperationException("broker gone") };
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

        // No burial and no commit: the offset stays unstored so a restart or rebalance retries
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
        // ladder is now bounded to a quarter of the poll interval and the caller keeps treating a
        // failed burial as "offset left unstored, retried later".
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
                MaxPollInterval = TimeSpan.FromMilliseconds(400)
            },
            NullLogger.Instance,
            Topic,
            Group,
            KafkaSubscriberRole.Worker);

        await dispatcher.DiscardUnprocessableAsync(
            KafkaTestData.Message(Topic, offset: 4, payload: ""),
            new InvalidDataException("no payload"),
            CancellationToken.None).WaitAsync(TimeSpan.FromSeconds(5));

        Assert.True(producer.SawCancellation);
    }

    [Fact]
    public async Task QueuedDispose_SurvivesAWorkerFaultingOutsideItsHandlerGuard()
    {
        // Regression: the drain join caught only TimeoutException (the shared DB base and NATS
        // also carry a general arm). A worker faulting outside its handler guard — here the log
        // sink throwing from the "handler failed" entry inside the catch arm — rethrew from
        // Task.WhenAll, escaped DisposeAsync into the subscriber's `await using` (masking the real
        // shutdown path) and leaked the drain token source.
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

    private static KafkaMessageDispatcher CreateDispatcher(
        Func<KafkaDelivery, CancellationToken, Task> handler,
        KafkaSubscriberOptions subscriberOptions,
        KafkaAsyncResponseTransportOptions? options = null,
        FakeKafkaConsumerClient? consumer = null,
        FakeKafkaProducerClient? producer = null)
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
            NullLogger.Instance,
            Topic,
            Group,
            KafkaSubscriberRole.Worker);
    }
}
