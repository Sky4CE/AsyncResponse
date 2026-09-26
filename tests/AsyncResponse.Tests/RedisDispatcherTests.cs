using AsyncResponse.Transports.Redis;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using StackExchange.Redis;
using System.Diagnostics;
using Xunit;

namespace AsyncResponse.Tests;

public class RedisDispatcherTests
{
    [Fact]
    public void ValidateOptions_AckAfterHandlerCompletes_DoesNotThrow()
    {
        RedisMessageDispatcher.ValidateOptions(
            new RedisAsyncResponseTransportOptions(),
            new RedisSubscriberOptions(),
            RedisSubscriberRole.Worker);
    }

    [Fact]
    public void ValidateOptions_AckAfterEnqueue_RequiresBackgroundQueue()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            RedisMessageDispatcher.ValidateOptions(
                new RedisAsyncResponseTransportOptions(),
                new RedisSubscriberOptions { AckMode = RedisAckMode.AckAfterEnqueue },
                RedisSubscriberRole.Worker));

        Assert.Contains(nameof(RedisSubscriberOptions.BackgroundWorkerCount), ex.Message, StringComparison.Ordinal);
    }

    [Theory]
    [MemberData(nameof(InvalidSubscriberOptions))]
    public void ValidateOptions_RejectsInvalidSubscriberOptions(
        RedisSubscriberOptions subscriberOptions,
        string expectedMessageFragment)
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            RedisMessageDispatcher.ValidateOptions(
                new RedisAsyncResponseTransportOptions(),
                subscriberOptions,
                RedisSubscriberRole.ResponseIngress));

        Assert.Contains(expectedMessageFragment, ex.Message, StringComparison.Ordinal);
        Assert.Contains(nameof(RedisAsyncResponseTransportOptions.ResponseSubscriber), ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateOptions_AckAfterEnqueue_RequiresQueueCapacity()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            RedisMessageDispatcher.ValidateOptions(
                new RedisAsyncResponseTransportOptions(),
                new RedisSubscriberOptions
                {
                    AckMode = RedisAckMode.AckAfterEnqueue,
                    BackgroundWorkerCount = 1
                },
                RedisSubscriberRole.Worker));

        Assert.Contains(nameof(RedisSubscriberOptions.BackgroundQueueCapacity), ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateOptions_AckAfterEnqueue_RequiresPositiveDrainTimeout()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            RedisMessageDispatcher.ValidateOptions(
                new RedisAsyncResponseTransportOptions(),
                new RedisSubscriberOptions
                {
                    AckMode = RedisAckMode.AckAfterEnqueue,
                    BackgroundWorkerCount = 1,
                    BackgroundQueueCapacity = 8,
                    BackgroundDrainTimeout = TimeSpan.Zero
                },
                RedisSubscriberRole.Worker));

        Assert.Contains(nameof(RedisSubscriberOptions.BackgroundDrainTimeout), ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateOptions_AckAfterEnqueue_RejectsDrainExceedingHostBudget()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            RedisMessageDispatcher.ValidateOptions(
                new RedisAsyncResponseTransportOptions
                {
                    HostShutdownTimeout = TimeSpan.FromSeconds(25)
                },
                EnqueueSubscriber(drain: TimeSpan.FromSeconds(26)),
                RedisSubscriberRole.Worker));

        Assert.Contains(nameof(RedisAsyncResponseTransportOptions.HostShutdownTimeout), ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateOptions_AckAfterEnqueue_NullHostShutdownTimeout_Passes()
    {
        RedisMessageDispatcher.ValidateOptions(
            new RedisAsyncResponseTransportOptions
            {
                HostShutdownTimeout = null
            },
            EnqueueSubscriber(drain: TimeSpan.FromSeconds(31)),
            RedisSubscriberRole.Worker);
    }

    [Fact]
    public void ValidateOptions_DocumentedEarlyAckDefaults_Pass()
    {
        // Regression: the documented two-arg early-ACK opt-in with stock defaults
        // (BackgroundDrainTimeout 20s vs HostShutdownTimeout 30s) must not fail startup.
        RedisMessageDispatcher.ValidateOptions(
            new RedisAsyncResponseTransportOptions(),
            new RedisSubscriberOptions().UseAckAfterEnqueue(4, 256),
            RedisSubscriberRole.Worker);
    }

    [Fact]
    public void ValidateOptions_UnsupportedAckMode_Throws()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            RedisMessageDispatcher.ValidateOptions(
                new RedisAsyncResponseTransportOptions(),
                new RedisSubscriberOptions { AckMode = (RedisAckMode)99 },
                RedisSubscriberRole.Worker));

        Assert.Contains("unsupported", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Create_AckAfterHandlerCompletes_BuildsAwaitingDispatcher()
    {
        await using var dispatcher = RedisMessageDispatcher.Create(
            (_, _) => Task.CompletedTask,
            new RedisTransportTests.FakeRedisStreamDatabase(),
            new RedisAsyncResponseTransportOptions(),
            new RedisSubscriberOptions { AckMode = RedisAckMode.AckAfterHandlerCompletes },
            NullLogger.Instance,
            "worker-stream",
            "worker-group",
            RedisSubscriberRole.Worker);

        Assert.IsType<AwaitingRedisMessageDispatcher>(dispatcher);
    }

    [Fact]
    public async Task Create_AckAfterEnqueue_BuildsQueuedDispatcher()
    {
        await using var dispatcher = RedisMessageDispatcher.Create(
            (_, _) => Task.CompletedTask,
            new RedisTransportTests.FakeRedisStreamDatabase(),
            new RedisAsyncResponseTransportOptions(),
            EnqueueSubscriber(),
            NullLogger.Instance,
            "worker-stream",
            "worker-group",
            RedisSubscriberRole.Worker);

        Assert.IsType<QueuedRedisMessageDispatcher>(dispatcher);
    }

    [Fact]
    public async Task Awaiting_HandlerSucceeds_AcksMessage()
    {
        var database = new RedisTransportTests.FakeRedisStreamDatabase();
        var handled = new TaskCompletionSource<RedisStreamDelivery>(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var dispatcher = RedisMessageDispatcher.Create(
            (delivery, _) =>
            {
                handled.TrySetResult(delivery);
                return Task.CompletedTask;
            },
            database,
            new RedisAsyncResponseTransportOptions(),
            new RedisSubscriberOptions(),
            NullLogger.Instance,
            "worker-stream",
            "worker-group",
            RedisSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery("1-0", attempt: 1), CancellationToken.None);

        Assert.Equal("1-0", (await handled.Task.WaitAsync(TimeSpan.FromSeconds(2))).MessageId.ToString());
        var ack = Assert.Single(database.Acks);
        Assert.Equal("worker-stream", ack.Stream);
        Assert.Equal("worker-group", ack.Group);
        Assert.Equal("1-0", ack.MessageId);
    }

    [Fact]
    public async Task Awaiting_SettlementIgnoresTheStoppingToken()
    {
        // A graceful shutdown cancels the subscriber token while a handler is mid-flight; the
        // XACK for that completed work must still go out. Forwarding the stopping token into the
        // ack abandons it (WithCancellation drops the command), leaving the entry in the PEL to
        // be reclaimed and re-run after restart — every sibling transport settles on
        // CancellationToken.None.
        var database = new RedisTransportTests.FakeRedisStreamDatabase();
        await using var dispatcher = RedisMessageDispatcher.Create(
            (_, _) => Task.CompletedTask,
            database,
            new RedisAsyncResponseTransportOptions(),
            new RedisSubscriberOptions(),
            NullLogger.Instance,
            "worker-stream",
            "worker-group",
            RedisSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery("1-0", attempt: 1), new CancellationToken(canceled: true));

        Assert.Single(database.Acks);
        Assert.False(Assert.Single(database.AckTokens).CanBeCanceled); // CancellationToken.None, not the stopping token
    }

    [Fact]
    public async Task Queued_PostEnqueueSettlementIgnoresTheStoppingToken()
    {
        // Same rule on the early-ack path: once the entry is handed to a background worker the
        // XACK must not be abandoned because shutdown cancelled the poll loop's token — the
        // worker runs the entry during the drain, and an un-ACKed entry is re-claimed and
        // re-executed after PendingMessageMinIdleTime.
        var database = new RedisTransportTests.FakeRedisStreamDatabase();
        await using var dispatcher = RedisMessageDispatcher.Create(
            (_, _) => Task.CompletedTask,
            database,
            new RedisAsyncResponseTransportOptions(),
            new RedisSubscriberOptions().UseAckAfterEnqueue(1, 8, TimeSpan.FromSeconds(5)),
            NullLogger.Instance,
            "worker-stream",
            "worker-group",
            RedisSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery("1-0", attempt: 1), new CancellationToken(canceled: true));

        Assert.Single(database.Acks);
        Assert.False(Assert.Single(database.AckTokens).CanBeCanceled); // CancellationToken.None, not the stopping token
    }

    [Fact]
    public async Task Awaiting_HandlerFailsBelowMax_LeavesMessagePending()
    {
        var database = new RedisTransportTests.FakeRedisStreamDatabase();
        await using var dispatcher = RedisMessageDispatcher.Create(
            (_, _) => throw new InvalidOperationException("handler boom"),
            database,
            new RedisAsyncResponseTransportOptions(),
            new RedisSubscriberOptions { MaxDeliveryAttempts = 2 },
            NullLogger.Instance,
            "worker-stream",
            "worker-group",
            RedisSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery("1-0", attempt: 1), CancellationToken.None);

        Assert.Empty(database.Acks);
        Assert.Empty(database.Adds);
    }

    [Fact]
    public async Task Awaiting_HandlerFailsWithUnlimitedAttempts_LeavesMessagePending()
    {
        var database = new RedisTransportTests.FakeRedisStreamDatabase();
        await using var dispatcher = RedisMessageDispatcher.Create(
            (_, _) => throw new InvalidOperationException("handler boom"),
            database,
            new RedisAsyncResponseTransportOptions(),
            new RedisSubscriberOptions { MaxDeliveryAttempts = 0 },
            NullLogger.Instance,
            "worker-stream",
            "worker-group",
            RedisSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery("1-0", attempt: 99), CancellationToken.None);

        Assert.Empty(database.Acks);
        Assert.Empty(database.Adds);
    }

    [Fact]
    public async Task Awaiting_HandlerFailsAtMax_DeadLettersAndAcks()
    {
        var database = new RedisTransportTests.FakeRedisStreamDatabase();
        await using var dispatcher = RedisMessageDispatcher.Create(
            (_, _) => throw new InvalidOperationException("handler boom"),
            database,
            new RedisAsyncResponseTransportOptions { DeadLetterStream = "dead" },
            new RedisSubscriberOptions { MaxDeliveryAttempts = 1 },
            NullLogger.Instance,
            "worker-stream",
            "worker-group",
            RedisSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery("1-0", attempt: 1), CancellationToken.None);

        Assert.Single(database.Acks);
        var dead = Assert.Single(database.Adds);
        Assert.Equal("dead", dead.Stream);
        Assert.Equal("handler_failed_max_attempts", RedisTransportTests.Field(dead.Values, "reason"));
        Assert.Equal("payload-json", RedisTransportTests.Field(dead.Values, "payload"));
    }

    /// <summary>
    /// Fixpoint r1 (host-stop hand-back). The flow engine signals "the host is stopping, hand this
    /// delivery back" with <see cref="DurableFlowInterruptedException"/>, and it arrives while the
    /// subscriber's own token is still live (ApplicationStopping fires before hosted services
    /// stop). The dispatcher recognised its own token only, so the hand-back was logged as a
    /// handler failure and — at MaxDeliveryAttempts — dead-lettered and ACKed: a flow's only
    /// wake-up buried because of a deploy. It is now left pending, unsettled, with no failure log.
    /// </summary>
    [Fact]
    public async Task Awaiting_HostStopHandBack_WithALiveToken_LeavesTheEntryPendingEvenAtTheCap()
    {
        var database = new RedisTransportTests.FakeRedisStreamDatabase();
        var logger = new RecordingThrowingLogger<RedisDispatcherTests>();
        await using var dispatcher = RedisMessageDispatcher.Create(
            (_, _) => throw new DurableFlowInterruptedException("Host is stopping; durable flow 'flow-1' left its in-process wait and the delivery is abandoned for redelivery."),
            database,
            new RedisAsyncResponseTransportOptions { DeadLetterStream = "dead" },
            new RedisSubscriberOptions { MaxDeliveryAttempts = 1 },
            logger,
            "worker-stream",
            "worker-group",
            RedisSubscriberRole.Worker);

        var outcome = await dispatcher.HandleAsync(Delivery("1-0", attempt: 1), CancellationToken.None);

        Assert.Equal(RedisDispatchOutcome.HandedBack, outcome);
        Assert.Empty(database.Adds);
        Assert.Empty(database.Acks);
        Assert.False(logger.HasEntry(LogLevel.Error, string.Empty));
        Assert.False(logger.HasEntry(LogLevel.Warning, string.Empty));
        Assert.True(dispatcher.HandBackSignalled); // the subscriber stops reading and claiming on it
    }

    /// <summary>
    /// Fixpoint r1 (host-stop hand-back), the early-ACK half: the entry was ACKed at enqueue, so
    /// it cannot be handed back to Redis. It is not a handler failure (no Error log), but — per
    /// the early-ACK hand-back rule of the pre-commit review of fixpoint round 1 — Redis will never
    /// redeliver it, so besides the OnBackgroundFailure report it gets a dead-letter copy under its
    /// own reason, its only durable record (a replay resumes the run from its last checkpoint).
    /// </summary>
    [Fact]
    public async Task Queued_HostStopHandBack_IsSurfacedAndDeadLetteredAsHandedBackAfterCommit_WithoutAnErrorLog()
    {
        var database = new RedisTransportTests.FakeRedisStreamDatabase();
        var logger = new RecordingThrowingLogger<RedisDispatcherTests>();
        var failureReported = new TaskCompletionSource<RedisBackgroundFailureContext>(TaskCreationOptions.RunContinuationsAsynchronously);
        var options = new RedisSubscriberOptions().UseAckAfterEnqueue(1, 8, TimeSpan.FromSeconds(5));
        options.OnBackgroundFailure = context =>
        {
            failureReported.TrySetResult(context);
            return ValueTask.CompletedTask;
        };
        var dispatcher = RedisMessageDispatcher.Create(
            (_, _) => throw new DurableFlowInterruptedException("Host is stopping; durable flow 'flow-1' wake-up is abandoned for redelivery."),
            database,
            new RedisAsyncResponseTransportOptions { DeadLetterStream = "dead" },
            options,
            logger,
            "worker-stream",
            "worker-group",
            RedisSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery("1-0", attempt: 1), CancellationToken.None);
        var failure = await failureReported.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await dispatcher.DisposeAsync(); // joins the worker, so nothing more can be written

        Assert.IsType<DurableFlowInterruptedException>(failure.Exception);
        var dead = Assert.Single(database.Adds);
        Assert.Equal("dead", dead.Stream);
        Assert.Equal("handed_back_after_commit", RedisTransportTests.Field(dead.Values, "reason"));
        Assert.Equal("payload-json", RedisTransportTests.Field(dead.Values, "payload"));
        Assert.True(logger.HasEntry(LogLevel.Warning, "handed back by the flow engine"));
        Assert.False(logger.HasEntry(LogLevel.Error, string.Empty));
        Assert.True(dispatcher.HandBackSignalled);
    }

    /// <summary>
    /// Pass 2 of the pre-commit review of fixpoint round 1: with dead-lettering disabled no copy
    /// is written, yet the hand-back still logged a Warning claiming one — a wake-up Redis will
    /// never redeliver was lost at Warning under a false "Dead-lettering a copy". Without a
    /// destination the loss is now logged at Error and says so.
    /// </summary>
    [Fact]
    public async Task Queued_HostStopHandBack_WithDeadLetteringDisabled_LogsTheLossAtError()
    {
        var database = new RedisTransportTests.FakeRedisStreamDatabase();
        var logger = new RecordingThrowingLogger<RedisDispatcherTests>();
        var failureReported = new TaskCompletionSource<RedisBackgroundFailureContext>(TaskCreationOptions.RunContinuationsAsynchronously);
        var options = new RedisSubscriberOptions().UseAckAfterEnqueue(1, 8, TimeSpan.FromSeconds(5));
        options.OnBackgroundFailure = context =>
        {
            failureReported.TrySetResult(context);
            return ValueTask.CompletedTask;
        };
        var dispatcher = RedisMessageDispatcher.Create(
            (_, _) => throw new DurableFlowInterruptedException("Host is stopping; durable flow 'flow-1' wake-up is abandoned for redelivery."),
            database,
            new RedisAsyncResponseTransportOptions { DeadLetterStream = "dead", DeadLetterEnabled = false },
            options,
            logger,
            "worker-stream",
            "worker-group",
            RedisSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery("1-0", attempt: 1), CancellationToken.None);
        var failure = await failureReported.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await dispatcher.DisposeAsync(); // joins the worker, so nothing more can be logged

        Assert.IsType<DurableFlowInterruptedException>(failure.Exception);
        Assert.Empty(database.Adds);
        Assert.True(logger.HasEntry(LogLevel.Error, "no dead-letter destination is configured"));
        Assert.False(logger.HasEntry(LogLevel.Warning, "Dead-lettering a copy"));
        Assert.True(dispatcher.HandBackSignalled);
    }

    [Fact]
    public async Task Awaiting_AckFailureAfterSuccessfulHandler_DoesNotDeadLetter()
    {
        // Regression (review fix): the XACK used to sit inside the handler try, so an ack failure
        // after a successful handler was misread as a handler failure — dead-lettering
        // already-processed work as "handler_failed_max_attempts" at max attempts. The ack
        // failure is now swallowed and logged; the entry stays pending and the pending-claim
        // loop owns redelivery.
        var database = new RedisTransportTests.FakeRedisStreamDatabase
        {
            AckException = new InvalidOperationException("xack failed")
        };
        var handled = 0;
        await using var dispatcher = RedisMessageDispatcher.Create(
            (_, _) =>
            {
                handled++;
                return Task.CompletedTask;
            },
            database,
            new RedisAsyncResponseTransportOptions { DeadLetterStream = "dead" },
            new RedisSubscriberOptions { MaxDeliveryAttempts = 1 },
            NullLogger.Instance,
            "worker-stream",
            "worker-group",
            RedisSubscriberRole.Worker);

        var outcome = await dispatcher.HandleAsync(Delivery("1-0", attempt: 1), CancellationToken.None);

        Assert.Equal(1, handled);
        Assert.Equal(RedisDispatchOutcome.Processed, outcome);
        Assert.Empty(database.Adds);
    }

    [Fact]
    public async Task Awaiting_AlreadyExceededMax_DeadLettersWithoutHandling()
    {
        var database = new RedisTransportTests.FakeRedisStreamDatabase();
        var handled = false;
        await using var dispatcher = RedisMessageDispatcher.Create(
            (_, _) =>
            {
                handled = true;
                return Task.CompletedTask;
            },
            database,
            new RedisAsyncResponseTransportOptions { DeadLetterStream = "dead" },
            new RedisSubscriberOptions { MaxDeliveryAttempts = 3 },
            NullLogger.Instance,
            "worker-stream",
            "worker-group",
            RedisSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery("1-0", attempt: 4), CancellationToken.None);

        Assert.False(handled);
        Assert.Single(database.Acks);
        Assert.Equal("max_delivery_attempts_exceeded", RedisTransportTests.Field(Assert.Single(database.Adds).Values, "reason"));
    }

    [Fact]
    public async Task Awaiting_AlreadyExceededMax_SettlementIgnoresTheSubscriberToken()
    {
        // Regression (r24): the PRE-handler over-cap path passed subscriberCancellationToken into
        // DeadLetterAndAckAsync (the post-handler path already used None), and the token gates
        // both the XADD and the XACK — a shutdown landing between the two left the entry in the
        // PEL to be reclaimed and dead-lettered a SECOND time after restart. Settlement now
        // deliberately ignores cancellation on both over-cap paths.
        var database = new RedisTransportTests.FakeRedisStreamDatabase();
        using var cts = new CancellationTokenSource();
        await using var dispatcher = RedisMessageDispatcher.Create(
            (_, _) => Task.CompletedTask,
            database,
            new RedisAsyncResponseTransportOptions { DeadLetterStream = "dead" },
            new RedisSubscriberOptions { MaxDeliveryAttempts = 3 },
            NullLogger.Instance,
            "worker-stream",
            "worker-group",
            RedisSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery("1-0", attempt: 4), cts.Token);

        Assert.False(Assert.Single(database.AddTokens).CanBeCanceled); // CancellationToken.None, not the subscriber token
        Assert.False(Assert.Single(database.AckTokens).CanBeCanceled);
    }

    [Fact]
    public async Task Awaiting_HandlerFailsAtMax_WhenDeadLetterDisabled_AcksWithoutAdding()
    {
        var database = new RedisTransportTests.FakeRedisStreamDatabase();
        await using var dispatcher = RedisMessageDispatcher.Create(
            (_, _) => throw new InvalidOperationException("handler boom"),
            database,
            new RedisAsyncResponseTransportOptions { DeadLetterEnabled = false },
            new RedisSubscriberOptions { MaxDeliveryAttempts = 1 },
            NullLogger.Instance,
            "worker-stream",
            "worker-group",
            RedisSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery("1-0", attempt: 1), CancellationToken.None);

        Assert.Single(database.Acks);
        Assert.Empty(database.Adds);
    }

    [Fact]
    public async Task Awaiting_HandlerFailsAtMax_WithNullCorrelationId_DeadLettersEmptyCorrelationField()
    {
        var database = new RedisTransportTests.FakeRedisStreamDatabase();
        await using var dispatcher = RedisMessageDispatcher.Create(
            (_, _) => throw new InvalidOperationException("handler boom"),
            database,
            new RedisAsyncResponseTransportOptions { DeadLetterStream = "dead" },
            new RedisSubscriberOptions { MaxDeliveryAttempts = 1 },
            NullLogger.Instance,
            "worker-stream",
            "worker-group",
            RedisSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery("1-0", attempt: 1, correlationId: null), CancellationToken.None);

        Assert.Equal("", RedisTransportTests.Field(Assert.Single(database.Adds).Values, "correlationId"));
    }

    [Fact]
    public async Task Awaiting_WhenSubscriberCancellationIsObserved_RethrowsWithoutAck()
    {
        var database = new RedisTransportTests.FakeRedisStreamDatabase();
        await using var dispatcher = RedisMessageDispatcher.Create(
            (_, token) => throw new OperationCanceledException(token),
            database,
            new RedisAsyncResponseTransportOptions(),
            new RedisSubscriberOptions(),
            NullLogger.Instance,
            "worker-stream",
            "worker-group",
            RedisSubscriberRole.Worker);
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            dispatcher.HandleAsync(Delivery("1-0", attempt: 1), cts.Token));
        Assert.Empty(database.Acks);
    }

    [Fact]
    public async Task Awaiting_WithActivityListener_TagsRedisReceiveActivity()
    {
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == AsyncResponseDiagnostics.ActivitySourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded
        };
        ActivitySource.AddActivityListener(listener);
        var database = new RedisTransportTests.FakeRedisStreamDatabase();
        Activity? observed = null;
        await using var dispatcher = RedisMessageDispatcher.Create(
            (_, _) =>
            {
                observed = Activity.Current;
                return Task.CompletedTask;
            },
            database,
            new RedisAsyncResponseTransportOptions(),
            new RedisSubscriberOptions(),
            NullLogger.Instance,
            "worker-stream",
            "worker-group",
            RedisSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery("1-0", attempt: 1), CancellationToken.None);

        Assert.NotNull(observed);
        Assert.Contains(observed!.Tags, tag => tag.Key == "asyncresponse.transport" && tag.Value == "redis");
        Assert.Contains(observed.Tags, tag => tag.Key == "messaging.message.id" && tag.Value == "1-0");
    }

    [Fact]
    public async Task Queued_AlreadyExceededMax_DeadLettersWithoutEnqueueing()
    {
        // Regression (round 31): the pre-execution delivery cap lived only in the awaiting
        // dispatcher, and the pending-claim loop feeds THIS dispatcher real XPENDING delivery
        // counts too — so in AckAfterEnqueue mode an over-cap entry (deferred under backpressure
        // and reclaimed each cycle, or re-claimed after a swallowed post-enqueue ACK failure) was
        // re-enqueued and re-executed with duplicate side effects forever, with nothing ever
        // consulting MaxDeliveryAttempts to bury it.
        var database = new RedisTransportTests.FakeRedisStreamDatabase();
        var handled = false;
        var subscriber = new RedisSubscriberOptions { MaxDeliveryAttempts = 3 }
            .UseAckAfterEnqueue(1, 8, TimeSpan.FromSeconds(5));
        await using var dispatcher = RedisMessageDispatcher.Create(
            (_, _) =>
            {
                handled = true;
                return Task.CompletedTask;
            },
            database,
            new RedisAsyncResponseTransportOptions { DeadLetterStream = "dead" },
            subscriber,
            NullLogger.Instance,
            "worker-stream",
            "worker-group",
            RedisSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery("1-0", attempt: 4), CancellationToken.None);
        await dispatcher.DisposeAsync();

        Assert.False(handled);
        Assert.Single(database.Acks);
        Assert.Equal("max_delivery_attempts_exceeded", RedisTransportTests.Field(Assert.Single(database.Adds).Values, "reason"));
    }

    [Fact]
    public async Task Queued_AcksBeforeBackgroundHandlerCompletes()
    {
        var database = new RedisTransportTests.FakeRedisStreamDatabase();
        var handlerStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseHandler = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var dispatcher = RedisMessageDispatcher.Create(
            async (_, _) =>
            {
                handlerStarted.TrySetResult();
                await releaseHandler.Task.ConfigureAwait(false);
            },
            database,
            new RedisAsyncResponseTransportOptions(),
            new RedisSubscriberOptions().UseAckAfterEnqueue(1, 8, TimeSpan.FromSeconds(5)),
            NullLogger.Instance,
            "worker-stream",
            "worker-group",
            RedisSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery("1-0", attempt: 1), CancellationToken.None);

        Assert.Single(database.Acks);
        await handlerStarted.Task.WaitAsync(TimeSpan.FromSeconds(2));
        releaseHandler.TrySetResult();
    }

    [Fact]
    public async Task Queued_AfterTheDrainBudgetLapses_DoesNotStartFreshWork_AndSurfacesIt()
    {
        // Regression (round 29): the drain token cannot stop the REAL handler — it is
        // _ingress.HandleWorkerMessageAsync(payload), whose target takes no CancellationToken — so
        // the sibling fact's token-honoring handler hid the defect. With a handler that ignores the
        // token (as the ingress does), the loop kept dequeuing and EXECUTING past the budget, and
        // whatever was still queued at process exit vanished with no record: those entries were
        // ACKed at enqueue, so Redis never redelivers them.
        var database = new RedisTransportTests.FakeRedisStreamDatabase();
        var firstStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handlerRuns = 0;
        var failures = new List<RedisBackgroundFailureContext>();

        var options = new RedisSubscriberOptions().UseAckAfterEnqueue(1, 8, TimeSpan.FromMilliseconds(100));
        options.OnBackgroundFailure = context =>
        {
            lock (failures)
            {
                failures.Add(context);
            }

            return ValueTask.CompletedTask;
        };

        var dispatcher = RedisMessageDispatcher.Create(
            async (_, _) =>
            {
                // Deliberately ignores the token, exactly like the ingress handler in production.
                if (Interlocked.Increment(ref handlerRuns) == 1)
                {
                    firstStarted.TrySetResult();
                    await releaseFirst.Task.ConfigureAwait(false);
                }
            },
            database,
            new RedisAsyncResponseTransportOptions(),
            options,
            NullLogger.Instance,
            "worker-stream",
            "worker-group",
            RedisSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery("1-0", attempt: 1), CancellationToken.None);
        await firstStarted.Task.WaitAsync(TimeSpan.FromSeconds(2));
        await dispatcher.HandleAsync(Delivery("2-0", attempt: 1), CancellationToken.None); // ACKed, waiting in queue

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
            Assert.Equal("2-0", dropped.MessageId);
            Assert.IsAssignableFrom<OperationCanceledException>(dropped.Exception);
        }

        // The queued entry was NOT executed after the budget lapsed.
        Assert.Equal(1, Volatile.Read(ref handlerRuns));
    }

    [Fact]
    public async Task Queued_Dispose_CancelledDrain_SurfacesDroppedEntriesViaOnBackgroundFailure()
    {
        var database = new RedisTransportTests.FakeRedisStreamDatabase();
        var handlerStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var failures = new List<RedisBackgroundFailureContext>();
        var options = new RedisSubscriberOptions().UseAckAfterEnqueue(1, 8, TimeSpan.FromMilliseconds(100));
        options.OnBackgroundFailure = context =>
        {
            lock (failures)
            {
                failures.Add(context);
            }

            return ValueTask.CompletedTask;
        };
        var dispatcher = RedisMessageDispatcher.Create(
            async (_, token) =>
            {
                handlerStarted.TrySetResult();
                await Task.Delay(Timeout.InfiniteTimeSpan, token).ConfigureAwait(false);
            },
            database,
            new RedisAsyncResponseTransportOptions(),
            options,
            NullLogger.Instance,
            "worker-stream",
            "worker-group",
            RedisSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery("1-0", attempt: 1), CancellationToken.None);
        await handlerStarted.Task.WaitAsync(TimeSpan.FromSeconds(2));
        await dispatcher.HandleAsync(Delivery("2-0", attempt: 1), CancellationToken.None); // ACKed, waiting in queue

        await dispatcher.DisposeAsync(); // drain budget elapses; the interrupted work must not vanish silently

        // Both the in-handler entry and the still-queued one were already ACKed: the shutdown
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
            Assert.Equal(["1-0", "2-0"], failures.Select(context => context.MessageId).OrderBy(id => id, StringComparer.Ordinal));
        }
    }

    [Fact]
    public async Task Queued_BackgroundFailure_ReportsAndDeadLetters()
    {
        var database = new RedisTransportTests.FakeRedisStreamDatabase();
        var failureReported = new TaskCompletionSource<RedisBackgroundFailureContext>(TaskCreationOptions.RunContinuationsAsynchronously);
        var options = new RedisSubscriberOptions().UseAckAfterEnqueue(1, 8, TimeSpan.FromSeconds(5));
        options.OnBackgroundFailure = context =>
        {
            failureReported.TrySetResult(context);
            return ValueTask.CompletedTask;
        };
        await using var dispatcher = RedisMessageDispatcher.Create(
            (_, _) => throw new InvalidOperationException("background boom"),
            database,
            new RedisAsyncResponseTransportOptions { DeadLetterStream = "dead" },
            options,
            NullLogger.Instance,
            "worker-stream",
            "worker-group",
            RedisSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery("1-0", attempt: 1), CancellationToken.None);

        var failure = await failureReported.Task.WaitAsync(TimeSpan.FromSeconds(2));
        Assert.Equal("worker-stream", failure.Stream);
        Assert.Equal("1-0", failure.MessageId);
        await WaitUntilAsync(() => database.Adds.Count == 1);
        Assert.Single(database.Adds);
    }

    [Fact]
    public async Task Queued_WhenQueueIsFull_LeavesOverflowMessagePending()
    {
        var database = new RedisTransportTests.FakeRedisStreamDatabase();
        var firstHandlerStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseHandlers = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var dispatcher = RedisMessageDispatcher.Create(
            async (_, _) =>
            {
                firstHandlerStarted.TrySetResult();
                await releaseHandlers.Task.ConfigureAwait(false);
            },
            database,
            new RedisAsyncResponseTransportOptions(),
            EnqueueSubscriber(workers: 1, capacity: 1),
            NullLogger.Instance,
            "worker-stream",
            "worker-group",
            RedisSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery("1-0", attempt: 1), CancellationToken.None);
        await firstHandlerStarted.Task.WaitAsync(TimeSpan.FromSeconds(2));
        await dispatcher.HandleAsync(Delivery("2-0", attempt: 1), CancellationToken.None);
        await dispatcher.HandleAsync(Delivery("3-0", attempt: 1), CancellationToken.None);

        Assert.Equal(["1-0", "2-0"], database.Acks.Select(ack => ack.MessageId));
        releaseHandlers.TrySetResult();
    }

    [Fact]
    public async Task Queued_CanAcceptMore_IsFalseWhileQueueIsSaturated()
    {
        var database = new RedisTransportTests.FakeRedisStreamDatabase();
        var firstHandlerStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseHandlers = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var dispatcher = RedisMessageDispatcher.Create(
            async (_, _) =>
            {
                firstHandlerStarted.TrySetResult();
                await releaseHandlers.Task.ConfigureAwait(false);
            },
            database,
            new RedisAsyncResponseTransportOptions(),
            EnqueueSubscriber(workers: 1, capacity: 1),
            NullLogger.Instance,
            "worker-stream",
            "worker-group",
            RedisSubscriberRole.Worker);

        Assert.True(dispatcher.CanAcceptMore);

        // Worker picks up 1-0 and blocks; 2-0 then fills the single queue slot.
        await dispatcher.HandleAsync(Delivery("1-0", attempt: 1), CancellationToken.None);
        await firstHandlerStarted.Task.WaitAsync(TimeSpan.FromSeconds(2));
        await dispatcher.HandleAsync(Delivery("2-0", attempt: 1), CancellationToken.None);

        Assert.False(dispatcher.CanAcceptMore);

        var outcome = await dispatcher.HandleAsync(Delivery("3-0", attempt: 1), CancellationToken.None);
        Assert.Equal(RedisDispatchOutcome.Deferred, outcome);

        releaseHandlers.TrySetResult();
    }

    [Fact]
    public async Task Queued_WhenAckAfterEnqueueFails_DoesNotThrowToSubscriberLoop()
    {
        var database = new RedisTransportTests.FakeRedisStreamDatabase
        {
            AckException = new RedisConnectionException(
                ConnectionFailureType.UnableToConnect, CommandFlags.None, "ack failed", null, CommandStatus.Unknown)
        };
        var handlerStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseHandler = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var dispatcher = RedisMessageDispatcher.Create(
            async (_, _) =>
            {
                handlerStarted.TrySetResult();
                await releaseHandler.Task.ConfigureAwait(false);
            },
            database,
            new RedisAsyncResponseTransportOptions(),
            EnqueueSubscriber(),
            NullLogger.Instance,
            "worker-stream",
            "worker-group",
            RedisSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery("1-0", attempt: 1), CancellationToken.None);

        await handlerStarted.Task.WaitAsync(TimeSpan.FromSeconds(2));
        Assert.Empty(database.Acks);
        releaseHandler.TrySetResult();
    }

    [Fact]
    public async Task Queued_DisposeTimeout_CancelsDrainAndReturns()
    {
        var database = new RedisTransportTests.FakeRedisStreamDatabase();
        var handlerStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var dispatcher = RedisMessageDispatcher.Create(
            async (_, token) =>
            {
                handlerStarted.TrySetResult();
                await Task.Delay(TimeSpan.FromSeconds(30), token).ConfigureAwait(false);
            },
            database,
            new RedisAsyncResponseTransportOptions { DeadLetterEnabled = false },
            EnqueueSubscriber(drain: TimeSpan.FromMilliseconds(10)),
            NullLogger.Instance,
            "worker-stream",
            "worker-group",
            RedisSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery("1-0", attempt: 1), CancellationToken.None);
        await handlerStarted.Task.WaitAsync(TimeSpan.FromSeconds(2));

        await dispatcher.DisposeAsync();
        await dispatcher.DisposeAsync();

        Assert.Single(database.Acks);
    }

    [Fact]
    public async Task Queued_BackgroundFailure_WhenCallbackThrows_StillDeadLetters()
    {
        var database = new RedisTransportTests.FakeRedisStreamDatabase();
        var options = EnqueueSubscriber();
        options.OnBackgroundFailure = _ => throw new InvalidOperationException("callback boom");
        await using var dispatcher = RedisMessageDispatcher.Create(
            (_, _) => throw new InvalidOperationException("background boom"),
            database,
            new RedisAsyncResponseTransportOptions { DeadLetterStream = "dead" },
            options,
            NullLogger.Instance,
            "worker-stream",
            "worker-group",
            RedisSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery("1-0", attempt: 1), CancellationToken.None);

        await WaitUntilAsync(() => database.Adds.Count == 1);
        Assert.Equal("background_handler_failed_after_ack", RedisTransportTests.Field(Assert.Single(database.Adds).Values, "reason"));
    }

    [Fact]
    public async Task Queued_BackgroundFailure_WhenDeadLetterWriteFails_CompletesWorker()
    {
        var database = new RedisTransportTests.FakeRedisStreamDatabase
        {
            AddException = new RedisConnectionException(
                ConnectionFailureType.UnableToConnect, CommandFlags.None, "deadletter failed", null, CommandStatus.Unknown)
        };
        var failureReported = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var options = EnqueueSubscriber();
        options.OnBackgroundFailure = _ =>
        {
            failureReported.TrySetResult();
            return ValueTask.CompletedTask;
        };
        await using var dispatcher = RedisMessageDispatcher.Create(
            (_, _) => throw new InvalidOperationException("background boom"),
            database,
            new RedisAsyncResponseTransportOptions { DeadLetterStream = "dead" },
            options,
            NullLogger.Instance,
            "worker-stream",
            "worker-group",
            RedisSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery("1-0", attempt: 1), CancellationToken.None);

        await failureReported.Task.WaitAsync(TimeSpan.FromSeconds(2));
        await WaitUntilAsync(() => database.AddAttempts == 1);
    }

    [Fact]
    public void UseAckAfterEnqueue_RequiresPositiveSettings()
    {
        var options = new RedisSubscriberOptions();

        Assert.Throws<ArgumentOutOfRangeException>(() => options.UseAckAfterEnqueue(0, 10));
        Assert.Throws<ArgumentOutOfRangeException>(() => options.UseAckAfterEnqueue(1, 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => options.UseAckAfterEnqueue(1, 10, TimeSpan.Zero));
    }

    [Fact]
    public void UseAckAfterEnqueue_AppliesSettings()
    {
        var options = new RedisSubscriberOptions()
            .UseAckAfterEnqueue(3, 64, TimeSpan.FromSeconds(7));

        Assert.Equal(RedisAckMode.AckAfterEnqueue, options.AckMode);
        Assert.Equal(3, options.BackgroundWorkerCount);
        Assert.Equal(64, options.BackgroundQueueCapacity);
        Assert.Equal(TimeSpan.FromSeconds(7), options.BackgroundDrainTimeout);
    }

    [Fact]
    public void UseAckAfterEnqueue_NullDrainTimeout_KeepsDefault()
    {
        var options = new RedisSubscriberOptions();
        var defaultDrain = options.BackgroundDrainTimeout;

        options.UseAckAfterEnqueue(1, 8);

        Assert.Equal(defaultDrain, options.BackgroundDrainTimeout);
    }

    /// <summary>
    /// Fixpoint round 2 (GS4#1). The early-ACK dispatcher spent its whole drain budget joining the
    /// workers and then returned: an entry still queued behind a slow handler — ACKed at enqueue,
    /// so Redis never redelivers it — was dead-lettered and reported only once some worker came
    /// free, after the stop had already moved on to tearing the multiplexer down or exiting. The
    /// budget is now split (NATS/DB parity): three quarters drain, and the reserved quarter buries
    /// and reports what is still queued before DisposeAsync returns. Pre-fix: nothing for 2-0
    /// until the first handler is released.
    /// </summary>
    [Fact]
    public async Task Queued_DrainLapse_DeadLettersAndReportsTheStillQueuedEntry_BeforeDisposeReturns()
    {
        var database = new RedisTransportTests.FakeRedisStreamDatabase();
        var firstStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var failures = new List<RedisBackgroundFailureContext>();
        var logger = new RecordingThrowingLogger<RedisDispatcherTests>();
        // A 4 s drain leaves a 1 s reserve for the burial and its report (the fakes complete
        // synchronously; a 250 ms reserve failed only on a GC pause or preemption).
        var options = new RedisSubscriberOptions().UseAckAfterEnqueue(1, 8, TimeSpan.FromSeconds(4));
        options.OnBackgroundFailure = context =>
        {
            lock (failures)
            {
                failures.Add(context);
            }

            return ValueTask.CompletedTask;
        };
        var dispatcher = RedisMessageDispatcher.Create(
            async (_, _) =>
            {
                // Ignores the token, like the ingress handler in production.
                firstStarted.TrySetResult();
                await releaseFirst.Task.ConfigureAwait(false);
            },
            database,
            new RedisAsyncResponseTransportOptions { DeadLetterStream = "dead" },
            options,
            logger,
            "worker-stream",
            "worker-group",
            RedisSubscriberRole.Worker);

        try
        {
            await dispatcher.HandleAsync(Delivery("1-0", attempt: 1), CancellationToken.None);
            await firstStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));
            await dispatcher.HandleAsync(Delivery("2-0", attempt: 1), CancellationToken.None); // ACKed, queued

            await dispatcher.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10));

            // Before the blocked handler lets go: the reserve did it.
            var dead = Assert.Single(database.Adds);
            Assert.Equal("drain_budget_lapsed_after_ack", RedisTransportTests.Field(dead.Values, "reason"));
            Assert.Equal("2-0", RedisTransportTests.Field(dead.Values, "messageId"));
            lock (failures)
            {
                var reported = Assert.Single(failures);
                Assert.Equal("2-0", reported.MessageId);
                Assert.IsAssignableFrom<OperationCanceledException>(reported.Exception);
            }

            Assert.True(logger.HasEntry(LogLevel.Warning, "Dead-lettered a copy (drain_budget_lapsed_after_ack)"));
        }
        finally
        {
            releaseFirst.TrySetResult();
        }
    }

    /// <summary>
    /// Fixpoint round 2 (GS4#1): with dead-lettering disabled the reserve still reports the entry,
    /// and says at Error that no copy exists — never a Warning claiming one.
    /// </summary>
    [Fact]
    public async Task Queued_DrainLapse_WithDeadLetteringDisabled_ReportsTheLossAtError()
    {
        var database = new RedisTransportTests.FakeRedisStreamDatabase();
        var firstStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var reported = new TaskCompletionSource<RedisBackgroundFailureContext>(TaskCreationOptions.RunContinuationsAsynchronously);
        var logger = new RecordingThrowingLogger<RedisDispatcherTests>();
        var options = new RedisSubscriberOptions().UseAckAfterEnqueue(1, 8, TimeSpan.FromSeconds(1));
        options.OnBackgroundFailure = context =>
        {
            reported.TrySetResult(context);
            return ValueTask.CompletedTask;
        };
        var dispatcher = RedisMessageDispatcher.Create(
            async (_, _) =>
            {
                firstStarted.TrySetResult();
                await releaseFirst.Task.ConfigureAwait(false);
            },
            database,
            new RedisAsyncResponseTransportOptions { DeadLetterEnabled = false },
            options,
            logger,
            "worker-stream",
            "worker-group",
            RedisSubscriberRole.Worker);

        try
        {
            await dispatcher.HandleAsync(Delivery("1-0", attempt: 1), CancellationToken.None);
            await firstStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));
            await dispatcher.HandleAsync(Delivery("2-0", attempt: 1), CancellationToken.None);

            await dispatcher.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10));

            Assert.True(reported.Task.IsCompletedSuccessfully);
            Assert.Equal("2-0", (await reported.Task).MessageId);
            Assert.Empty(database.Adds);
            Assert.True(logger.HasEntry(LogLevel.Error, "no dead-letter destination is configured"));
            Assert.False(logger.HasEntry(LogLevel.Warning, "Dead-lettered a copy"));
        }
        finally
        {
            releaseFirst.TrySetResult();
        }
    }

    /// <summary>
    /// Fixpoint round 2 (GS4#1): the reserve waits for the user's OnBackgroundFailure callback only
    /// within itself — a callback that never finishes (an alert stuck on an HTTP timeout) must not
    /// hold the host's stop past the drain budget. The dead-letter copy is written first either way.
    /// Red-on-old (fixpoint r2 pre-commit, H1): the reserve buried and reported one entry at a
    /// time, so the first entry's stuck callback spent the whole reserve and the second entry got
    /// no dead-letter copy (one XADD, not two). Every entry is now buried before any is reported.
    /// </summary>
    [Fact]
    public async Task Queued_DrainLapse_AStuckCallbackDoesNotHoldDisposePastTheReserve()
    {
        var database = new RedisTransportTests.FakeRedisStreamDatabase();
        var firstStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseCallback = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var callbacks = 0;
        // A 4 s drain leaves a 1 s reserve: ample for two synchronous burials, spent by the stuck callback.
        var options = new RedisSubscriberOptions().UseAckAfterEnqueue(1, 8, TimeSpan.FromSeconds(4));
        options.OnBackgroundFailure = _ =>
        {
            Interlocked.Increment(ref callbacks);
            return new ValueTask(releaseCallback.Task);
        };
        var dispatcher = RedisMessageDispatcher.Create(
            async (_, _) =>
            {
                firstStarted.TrySetResult();
                await releaseFirst.Task.ConfigureAwait(false);
            },
            database,
            new RedisAsyncResponseTransportOptions { DeadLetterStream = "dead" },
            options,
            NullLogger.Instance,
            "worker-stream",
            "worker-group",
            RedisSubscriberRole.Worker);

        try
        {
            await dispatcher.HandleAsync(Delivery("1-0", attempt: 1), CancellationToken.None);
            await firstStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));
            await dispatcher.HandleAsync(Delivery("2-0", attempt: 1), CancellationToken.None);
            await dispatcher.HandleAsync(Delivery("3-0", attempt: 1), CancellationToken.None);

            // Hang guard only: the bound under test is the 1 s reserve.
            await dispatcher.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10));

            Assert.Equal(2, database.Adds.Count);
            Assert.All(database.Adds, add => Assert.Equal("drain_budget_lapsed_after_ack", RedisTransportTests.Field(add.Values, "reason")));
            Assert.Equal(["2-0", "3-0"], database.Adds.Select(add => RedisTransportTests.Field(add.Values, "messageId")));
            // Both reports were made; only the reserve bounded how long they were awaited.
            Assert.Equal(2, Volatile.Read(ref callbacks));
        }
        finally
        {
            releaseCallback.TrySetResult();
            releaseFirst.TrySetResult();
        }
    }

    public static TheoryData<string, string> EarlyAckFailureArms() => new()
    {
        { "hand-back", "handed_back_after_commit" },
        { "handler failure", "background_handler_failed_after_ack" }
    };

    /// <summary>
    /// Fixpoint round 2 (S6a#5). The early-ACK arms awaited the user's OnBackgroundFailure callback
    /// BEFORE writing the dead-letter copy — the only durable record of an entry Redis will never
    /// redeliver. A slow callback outlived the drain, the stop tore the multiplexer down, and the
    /// copy was never written (after a Warning had already claimed it). The copy now comes first
    /// (NATS/DB parity). Pre-fix: no XADD while the callback is still running.
    /// </summary>
    [Theory]
    [MemberData(nameof(EarlyAckFailureArms))]
    public async Task Queued_EarlyAckFailureArms_WriteTheDeadLetterCopyBeforeAwaitingTheCallback(string arm, string reason)
    {
        var database = new RedisTransportTests.FakeRedisStreamDatabase();
        var callbackEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseCallback = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var addsSeenByTheCallback = -1;
        var options = new RedisSubscriberOptions().UseAckAfterEnqueue(1, 8, TimeSpan.FromSeconds(5));
        options.OnBackgroundFailure = async _ =>
        {
            addsSeenByTheCallback = database.Adds.Count;
            callbackEntered.TrySetResult();
            await releaseCallback.Task.ConfigureAwait(false);
        };
        Exception failure = arm == "hand-back"
            ? new DurableFlowInterruptedException("Host is stopping; durable flow 'flow-1' wake-up is abandoned for redelivery.")
            : new InvalidOperationException("background boom");
        var dispatcher = RedisMessageDispatcher.Create(
            (_, _) => throw failure,
            database,
            new RedisAsyncResponseTransportOptions { DeadLetterStream = "dead" },
            options,
            NullLogger.Instance,
            "worker-stream",
            "worker-group",
            RedisSubscriberRole.Worker);

        try
        {
            await dispatcher.HandleAsync(Delivery("1-0", attempt: 1), CancellationToken.None);
            await callbackEntered.Task.WaitAsync(TimeSpan.FromSeconds(5));

            Assert.Equal(1, addsSeenByTheCallback);
            Assert.Equal(reason, RedisTransportTests.Field(Assert.Single(database.Adds).Values, "reason"));
        }
        finally
        {
            releaseCallback.TrySetResult();
            await dispatcher.DisposeAsync();
        }
    }

    /// <summary>
    /// Fixpoint round 2 (S6a#5): a hand-back whose dead-letter write fails is logged by outcome —
    /// an Error that the wake-up is lost unless OnBackgroundFailure records it, never the Warning
    /// that claims a copy.
    /// </summary>
    [Fact]
    public async Task Queued_HostStopHandBack_WhoseDeadLetterWriteFails_LogsTheLossAtError()
    {
        var database = new RedisTransportTests.FakeRedisStreamDatabase
        {
            AddException = new RedisConnectionException(
                ConnectionFailureType.UnableToConnect, CommandFlags.None, "deadletter failed", null, CommandStatus.Unknown)
        };
        var logger = new RecordingThrowingLogger<RedisDispatcherTests>();
        var reported = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var options = new RedisSubscriberOptions().UseAckAfterEnqueue(1, 8, TimeSpan.FromSeconds(5));
        options.OnBackgroundFailure = _ =>
        {
            reported.TrySetResult();
            return ValueTask.CompletedTask;
        };
        var dispatcher = RedisMessageDispatcher.Create(
            (_, _) => throw new DurableFlowInterruptedException("Host is stopping; durable flow 'flow-1' wake-up is abandoned for redelivery."),
            database,
            new RedisAsyncResponseTransportOptions { DeadLetterStream = "dead" },
            options,
            logger,
            "worker-stream",
            "worker-group",
            RedisSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery("1-0", attempt: 1), CancellationToken.None);
        await reported.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await dispatcher.DisposeAsync();

        Assert.True(logger.HasEntry(LogLevel.Error, "the dead-letter write failed, so the wake-up is lost"));
        Assert.False(logger.HasEntry(LogLevel.Warning, "Dead-lettered a copy"));
        Assert.Single(database.Acks); // the enqueue-time ACK only: no copy, so no second XACK
    }

    /// <summary>
    /// Fixpoint round 2 (GS4#8). With <c>DeadLetterEnabled = false</c> a poison entry past the
    /// attempt cap was XACKed with nothing logged at all — gone without a trace — and the at-cap
    /// and unparsable paths claimed a dead-letter write that never happened. The drop is now an
    /// Error (NATS parity) and nothing claims a copy. Pre-fix: no drop Error on any path, and the
    /// at-cap Warning / unparsable Error claim a dead-letter write.
    /// </summary>
    [Fact]
    public async Task DeadLetteringDisabled_EveryBurialPath_LogsTheDrop_AndNeverClaimsACopy()
    {
        var database = new RedisTransportTests.FakeRedisStreamDatabase();
        var logger = new RecordingThrowingLogger<RedisDispatcherTests>();
        var transportOptions = new RedisAsyncResponseTransportOptions { DeadLetterEnabled = false };
        await using var awaiting = RedisMessageDispatcher.Create(
            (_, _) => throw new InvalidOperationException("handler boom"),
            database,
            transportOptions,
            new RedisSubscriberOptions { MaxDeliveryAttempts = 2 },
            logger,
            "worker-stream",
            "worker-group",
            RedisSubscriberRole.Worker);
        await using var queued = RedisMessageDispatcher.Create(
            (_, _) => Task.CompletedTask,
            database,
            transportOptions,
            new RedisSubscriberOptions { MaxDeliveryAttempts = 2 }.UseAckAfterEnqueue(1, 8, TimeSpan.FromSeconds(5)),
            logger,
            "worker-stream",
            "worker-group",
            RedisSubscriberRole.Worker);

        await awaiting.HandleAsync(Delivery("1-0", attempt: 3), CancellationToken.None); // pre-execution cap
        await awaiting.HandleAsync(Delivery("2-0", attempt: 2), CancellationToken.None); // fails at the cap
        await queued.HandleAsync(Delivery("3-0", attempt: 3), CancellationToken.None);   // pre-execution cap
        await awaiting.DiscardUnprocessableAsync(
            "worker-stream",
            "worker-group",
            RedisTransportTests.Entry("4-0", ("correlationId", "c4")), // no payload field
            new InvalidDataException("no payload field"),
            CancellationToken.None);

        Assert.Empty(database.Adds);
        Assert.Equal(["1-0", "2-0", "3-0", "4-0"], database.Acks.Select(ack => ack.MessageId));
        foreach (var (id, reason) in new[]
                 {
                     ("1-0", "max_delivery_attempts_exceeded"),
                     ("2-0", "handler_failed_max_attempts"),
                     ("3-0", "max_delivery_attempts_exceeded"),
                     ("4-0", "unparsable_entry")
                 })
        {
            Assert.True(
                logger.HasEntry(LogLevel.Error, $"Redis message {id} on worker-stream was dropped ({reason}): dead-lettering is disabled"),
                $"no drop Error for {id}");
        }

        Assert.False(logger.HasEntry(LogLevel.Warning, "writing to dead-letter stream"));
        Assert.False(logger.HasEntry(LogLevel.Error, "dead-lettering and ACKing it"));
    }

    private static RedisStreamDelivery Delivery(string id, int attempt, string? correlationId = "corr")
        => new(
            "worker-stream",
            "worker-group",
            id,
            "payload-json",
            correlationId,
            attempt,
            RedisTransportTests.Entry(id, ("payload", "payload-json"), ("correlationId", "corr")));

    public static TheoryData<RedisSubscriberOptions, string> InvalidSubscriberOptions()
        => new()
        {
            { new RedisSubscriberOptions { BatchSize = 0 }, nameof(RedisSubscriberOptions.BatchSize) },
            { new RedisSubscriberOptions { EmptyPollDelay = TimeSpan.Zero }, nameof(RedisSubscriberOptions.EmptyPollDelay) },
            { new RedisSubscriberOptions { PendingMessageMinIdleTime = TimeSpan.Zero }, nameof(RedisSubscriberOptions.PendingMessageMinIdleTime) },
            { new RedisSubscriberOptions { PendingClaimInterval = TimeSpan.Zero }, nameof(RedisSubscriberOptions.PendingClaimInterval) },
            { new RedisSubscriberOptions { PendingClaimBatchSize = 0 }, nameof(RedisSubscriberOptions.PendingClaimBatchSize) },
            { new RedisSubscriberOptions { MaxDeliveryAttempts = -1 }, nameof(RedisSubscriberOptions.MaxDeliveryAttempts) }
        };

    [Fact]
    public async Task QueuedDispose_SurvivesAWorkerFaultingOutsideItsHandlerGuard()
    {
        // Regression: the drain join caught only TimeoutException (the shared DB base and NATS
        // also carry a general arm). A worker faulting outside its handler guard — here the log
        // sink throwing from the "handler failed" entry inside the catch arm — rethrew from
        // Task.WhenAll, escaped DisposeAsync into the subscriber's `await using` and leaked the
        // drain token source.
        var logger = new ErrorThrowingLogger();
        var dispatcher = RedisMessageDispatcher.Create(
            (_, _) => throw new InvalidOperationException("handler boom"),
            new RedisTransportTests.FakeRedisStreamDatabase(),
            new RedisAsyncResponseTransportOptions(),
            EnqueueSubscriber(),
            logger,
            "worker-stream",
            "worker-group",
            RedisSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery("1-0", attempt: 1), CancellationToken.None);
        await logger.ErrorThrown.Task.WaitAsync(TimeSpan.FromSeconds(5));

        await dispatcher.DisposeAsync();
    }

    private static RedisSubscriberOptions EnqueueSubscriber(
        int workers = 1,
        int capacity = 8,
        TimeSpan? drain = null)
        => new RedisSubscriberOptions()
            .UseAckAfterEnqueue(workers, capacity, drain ?? TimeSpan.FromSeconds(5));

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(2);
        while (!condition())
        {
            if (DateTime.UtcNow >= deadline)
                throw new TimeoutException("Condition was not satisfied before the timeout.");

            await Task.Delay(10);
        }
    }
}
