using AsyncResponse.Transports.NATS;
using Microsoft.Extensions.Logging.Abstractions;
using System.Diagnostics;
using Xunit;

namespace AsyncResponse.Tests;

public class NatsMessageDispatcherTests
{
    private const string DeadLetterSubject = "asyncresponse.transport.deadletter";
    private readonly FakeNatsJetStreamTransport _jetStream = new();

    private NatsMessageDispatcher CreateDispatcher(
        Func<NatsJobDelivery, CancellationToken, Task> handler,
        NatsSubscriberOptions subscriber,
        NatsAsyncResponseTransportOptions? options = null)
    {
        options ??= new NatsAsyncResponseTransportOptions();
        return new NatsMessageDispatcher(
            handler,
            _jetStream,
            options,
            subscriber,
            new NatsTransportSubjectSchema(options),
            new TestLogger(),
            NatsSubscriberRole.Worker,
            "test-consumer");
    }

    [Fact]
    public async Task HandlerCompletes_AcksMessage()
    {
        var rec = new RecordingDelivery();
        await using var dispatcher = CreateDispatcher((_, _) => Task.CompletedTask, new NatsSubscriberOptions());

        await dispatcher.HandleAsync(rec.Create("payload", numDelivered: 1), CancellationToken.None);

        Assert.Equal(1, rec.Acks);
        Assert.Empty(rec.Naks);
        Assert.Equal(0, rec.Terms);
        Assert.Empty(_jetStream.Published);
    }

    [Fact]
    public async Task HandlerExecution_EmitsNatsReceiveSpanWithMessagingTags()
    {
        using var collector = new AsyncResponseActivityCollector();
        var rec = new RecordingDelivery();
        var options = new NatsAsyncResponseTransportOptions();
        var headers = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase) { [options.CorrelationIdHeader] = "corr-nats" };
        await using var dispatcher = CreateDispatcher((_, _) => Task.CompletedTask, new NatsSubscriberOptions(), options);

        await dispatcher.HandleAsync(rec.Create("payload", numDelivered: 1, headers: headers), CancellationToken.None);

        var activity = collector.Single("asyncresponse.nats.receive", "asyncresponse.transport", "nats");
        Assert.Equal(ActivityKind.Consumer, activity.Kind);
        Assert.Equal("Worker", AsyncResponseActivityCollector.Tag(activity, "asyncresponse.nats.role"));
        Assert.Equal(nameof(NatsAckMode.AckAfterHandlerCompletes), AsyncResponseActivityCollector.Tag(activity, "asyncresponse.nats.ack_mode"));
        Assert.Equal("nats", AsyncResponseActivityCollector.Tag(activity, "messaging.system"));
        Assert.Equal("asyncresponse.transport.worker", AsyncResponseActivityCollector.Tag(activity, "messaging.destination.name"));
        Assert.Equal(1L, AsyncResponseActivityCollector.Tag(activity, "messaging.nats.num_delivered"));
        Assert.Equal("corr-nats", AsyncResponseActivityCollector.Tag(activity, "asyncresponse.correlation_id"));
    }

    [Fact]
    public async Task EarlyAck_BackgroundHandlerStillEmitsReceiveSpan()
    {
        using var collector = new AsyncResponseActivityCollector();
        var processed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var subscriber = new NatsSubscriberOptions().UseAckAfterEnqueue(backgroundWorkerCount: 1, backgroundQueueCapacity: 4, backgroundDrainTimeout: TimeSpan.FromSeconds(5));
        await using var dispatcher = CreateDispatcher((_, _) => { processed.TrySetResult(); return Task.CompletedTask; }, subscriber);

        var rec = new RecordingDelivery();
        await dispatcher.HandleAsync(rec.Create("payload", numDelivered: 1), CancellationToken.None);
        await processed.Task.WaitAsync(TimeSpan.FromSeconds(2));

        // Regression guard for the "both ACK modes emit the span" claim: the early-ACK path runs the
        // handler on a background worker, and a refactor that splits it from the shared handler
        // execution path would lose the span silently.
        await WaitUntilAsync(() => collector.Count("asyncresponse.nats.receive") == 1);
        var activity = collector.Single("asyncresponse.nats.receive", "asyncresponse.transport", "nats");
        Assert.Equal(ActivityKind.Consumer, activity.Kind);
        Assert.Equal(nameof(NatsAckMode.AckAfterEnqueue), AsyncResponseActivityCollector.Tag(activity, "asyncresponse.nats.ack_mode"));
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

    [Fact]
    public async Task HandlerFailure_MarksNatsReceiveSpanError()
    {
        using var collector = new AsyncResponseActivityCollector();
        var rec = new RecordingDelivery();
        var subscriber = new NatsSubscriberOptions { MaxDeliveryAttempts = 5 };
        await using var dispatcher = CreateDispatcher((_, _) => throw new InvalidOperationException("boom"), subscriber);

        await dispatcher.HandleAsync(rec.Create("payload", numDelivered: 1), CancellationToken.None);

        var activity = collector.Single("asyncresponse.nats.receive", "asyncresponse.transport", "nats");
        Assert.Equal(typeof(InvalidOperationException).FullName, AsyncResponseActivityCollector.Tag(activity, "error.type"));
        Assert.Equal(ActivityStatusCode.Error, activity.Status);
    }

    [Fact]
    public async Task ShutdownCancellation_AtMaxAttempts_LeavesDeliveryUnsettled()
    {
        // Regression (r23): a graceful drain cancelling the stoppingToken while user code was in
        // the handler used to route the OperationCanceledException through HandleFailureAsync —
        // dead-lettering (and, with dead-lettering disabled, TERMinating) healthy work that never
        // ran, or NAKing away a delivery attempt below the cap. Shutdown now rethrows and leaves
        // the delivery unsettled, like the RabbitMQ/Redis/Kafka/DB dispatchers.
        var rec = new RecordingDelivery();
        using var stopping = new CancellationTokenSource();
        stopping.Cancel();
        await using var dispatcher = CreateDispatcher(
            (_, _) => throw new OperationCanceledException(stopping.Token),
            new NatsSubscriberOptions { MaxDeliveryAttempts = 5 });

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => dispatcher.HandleAsync(rec.Create("payload", numDelivered: 5), stopping.Token));

        Assert.Equal(0, rec.Acks);
        Assert.Empty(rec.Naks);
        Assert.Equal(0, rec.Terms);
        Assert.Empty(_jetStream.Published);
    }

    [Fact]
    public async Task EarlyAck_FastPathAckFailure_IsSwallowedAndDoesNotNak()
    {
        // Regression (r23): the fast-path ACK after a successful TryWrite was unguarded, so a
        // transient ack failure escaped HandleAsync and tore down the whole subscriber while a
        // background worker was already running the delivery. It is now swallowed and logged;
        // AckWait redelivery owns the retry.
        var rec = new RecordingDelivery { AckException = new InvalidOperationException("ack failed") };
        var handled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var dispatcher = CreateDispatcher(
            (_, _) =>
            {
                handled.TrySetResult();
                return Task.CompletedTask;
            },
            new NatsSubscriberOptions().UseAckAfterEnqueue(backgroundWorkerCount: 1, backgroundQueueCapacity: 4, backgroundDrainTimeout: TimeSpan.FromSeconds(5)));

        await dispatcher.HandleAsync(rec.Create("payload", numDelivered: 1), CancellationToken.None);

        await handled.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(1, rec.Acks);
        Assert.Empty(rec.Naks);
        Assert.Equal(0, rec.Terms);
    }

    [Fact]
    public async Task EarlyAck_AckFailureAfterParkedEnqueue_IsSwallowedAndDoesNotNak()
    {
        // Regression (r23): the post-park ACK sat inside the try whose catch NAKs, so an ack
        // failure after the delivery was already handed to a background worker NAK'd (or, for
        // non-matching exception types, escaped and tore down the subscriber) a job that was
        // being executed — JetStream redelivered it into a concurrent duplicate run.
        var handlerStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseHandler = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var dispatcher = CreateDispatcher(
            (_, _) =>
            {
                handlerStarted.TrySetResult();
                return releaseHandler.Task;
            },
            new NatsSubscriberOptions().UseAckAfterEnqueue(backgroundWorkerCount: 1, backgroundQueueCapacity: 1, backgroundDrainTimeout: TimeSpan.FromSeconds(5)));

        var blocking = new RecordingDelivery();
        var queued = new RecordingDelivery();
        var parked = new RecordingDelivery { AckException = new InvalidOperationException("ack failed") };

        // First delivery occupies the single worker; second fills the queue slot; third parks.
        await dispatcher.HandleAsync(blocking.Create("payload", numDelivered: 1), CancellationToken.None);
        await handlerStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await dispatcher.HandleAsync(queued.Create("payload", numDelivered: 1), CancellationToken.None);
        var parkTask = dispatcher.HandleAsync(parked.Create("payload", numDelivered: 1), CancellationToken.None);
        Assert.False(parkTask.IsCompleted);

        releaseHandler.TrySetResult();
        await parkTask.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Equal(1, parked.Acks);
        Assert.Empty(parked.Naks);
        Assert.Equal(0, parked.Terms);
    }

    [Fact]
    public async Task EarlyAck_NakFailureWhileStopping_IsSwallowed()
    {
        // Regression (r23): the shutdown NAK for a delivery that was never enqueued was itself
        // unguarded — a NAK failure on a closing connection escaped HandleAsync and turned a
        // graceful stop into a subscriber-teardown error. It is now swallowed; AckWait lapses to
        // the same redelivery.
        var handlerStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseHandler = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var dispatcher = CreateDispatcher(
            (_, _) =>
            {
                handlerStarted.TrySetResult();
                return releaseHandler.Task;
            },
            new NatsSubscriberOptions().UseAckAfterEnqueue(backgroundWorkerCount: 1, backgroundQueueCapacity: 1, backgroundDrainTimeout: TimeSpan.FromSeconds(5)));

        var blocking = new RecordingDelivery();
        var queued = new RecordingDelivery();
        var parked = new RecordingDelivery { NakException = new InvalidOperationException("nak failed") };

        await dispatcher.HandleAsync(blocking.Create("payload", numDelivered: 1), CancellationToken.None);
        await handlerStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await dispatcher.HandleAsync(queued.Create("payload", numDelivered: 1), CancellationToken.None);

        using var stopping = new CancellationTokenSource();
        var parkTask = dispatcher.HandleAsync(parked.Create("payload", numDelivered: 1), stopping.Token);
        Assert.False(parkTask.IsCompleted);

        stopping.Cancel();
        await parkTask.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Single(parked.Naks);
        Assert.Equal(0, parked.Acks);
        releaseHandler.TrySetResult();
    }

    [Fact]
    public async Task AckFailureAfterSuccessfulHandler_DoesNotNakOrDeadLetter()
    {
        // Regression (review fix): the ACK used to sit inside the handler try, so an ack failure
        // after a successful handler took HandleFailureAsync — TERMing already-run work into the
        // DLQ at max attempts, or NAKing a guaranteed duplicate below it. The failure is now
        // swallowed and logged; JetStream's ack-wait redelivery owns the retry.
        var rec = new RecordingDelivery { AckException = new InvalidOperationException("ack failed") };
        var handled = 0;
        await using var dispatcher = CreateDispatcher(
            (_, _) => { handled++; return Task.CompletedTask; },
            new NatsSubscriberOptions { MaxDeliveryAttempts = 5 });

        // numDelivered at max attempts: the old in-try ack routed this to dead-letter + TERM.
        await dispatcher.HandleAsync(rec.Create("payload", numDelivered: 5), CancellationToken.None);

        Assert.Equal(1, handled);
        Assert.Equal(1, rec.Acks);
        Assert.Empty(rec.Naks);
        Assert.Equal(0, rec.Terms);
        Assert.Empty(_jetStream.Published);
    }

    [Fact]
    public async Task HandlerFailureBelowMaxAttempts_NaksForRedelivery()
    {
        var rec = new RecordingDelivery();
        var subscriber = new NatsSubscriberOptions { MaxDeliveryAttempts = 5, RedeliveryDelay = TimeSpan.FromSeconds(7) };
        await using var dispatcher = CreateDispatcher((_, _) => throw new InvalidOperationException("boom"), subscriber);

        await dispatcher.HandleAsync(rec.Create("payload", numDelivered: 2), CancellationToken.None);

        Assert.Equal(0, rec.Acks);
        Assert.Equal(0, rec.Terms);
        Assert.Equal([TimeSpan.FromSeconds(7)], rec.Naks);
        Assert.Empty(_jetStream.Published);
    }

    [Fact]
    public async Task HandlerFailureAtMaxAttempts_DeadLettersAndTerminates()
    {
        var rec = new RecordingDelivery();
        var subscriber = new NatsSubscriberOptions { MaxDeliveryAttempts = 5 };
        await using var dispatcher = CreateDispatcher((_, _) => throw new InvalidOperationException("boom"), subscriber);

        await dispatcher.HandleAsync(rec.Create("payload", numDelivered: 5), CancellationToken.None);

        Assert.Equal(1, rec.Terms);
        Assert.Empty(rec.Naks);
        var deadLettered = Assert.Single(_jetStream.Published);
        Assert.Equal(DeadLetterSubject, deadLettered.Subject);
        Assert.Equal("payload", deadLettered.Payload);
        Assert.Equal("boom", deadLettered.Headers!["AR-DeadLetter-Reason"]);
    }

    [Fact]
    public async Task DeadLetterPublish_DropsTheInboundNatsMsgId()
    {
        // Regression (round 29): the inbound Nats-Msg-Id belongs to the LIVE publish, not to this
        // one. Carrying it over made a second dead-letter of the same message inside the DLQ
        // stream's duplicate window — reachable whenever the Term fails and the message redelivers
        // after AckWait — a deduplicated publish, which the caller reads as a DLQ failure and
        // answers with a NAK, looping until the window passes.
        var rec = new RecordingDelivery();
        var subscriber = new NatsSubscriberOptions { MaxDeliveryAttempts = 5 };
        await using var dispatcher = CreateDispatcher((_, _) => throw new InvalidOperationException("boom"), subscriber);

        var headers = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["Nats-Msg-Id"] = "worker-job-42",
            ["AR-Correlation-Id"] = "corr-1"
        };

        await dispatcher.HandleAsync(rec.Create("payload", numDelivered: 5, headers: headers), CancellationToken.None);

        var deadLettered = Assert.Single(_jetStream.Published);
        Assert.Equal(DeadLetterSubject, deadLettered.Subject);
        Assert.DoesNotContain("Nats-Msg-Id", deadLettered.Headers!.Keys);
        // The rest of the inbound headers still travel with the buried copy.
        Assert.Equal("corr-1", deadLettered.Headers["AR-Correlation-Id"]);
    }

    [Fact]
    public async Task TermFailureAfterDeadLetterPublish_DoesNotUnwindTheConsumeLoop()
    {
        // Regression (r24): TermAsync at the attempt cap ran bare while both ack sites were
        // guarded — it is the same JetStream request/reply and can throw, and the escaping
        // settlement unwound the consume loop and rebuilt the whole subscriber. The un-termed
        // message then redelivered after AckWait still over the cap, so it was dead-lettered
        // AGAIN, forever. Term is now swallow-and-log like the acks.
        var rec = new RecordingDelivery { TermException = new InvalidOperationException("ack subject no responders") };
        var subscriber = new NatsSubscriberOptions { MaxDeliveryAttempts = 5 };
        await using var dispatcher = CreateDispatcher((_, _) => throw new InvalidOperationException("boom"), subscriber);

        // Must NOT throw: a thrown Term would tear down the subscriber mid-batch.
        await dispatcher.HandleAsync(rec.Create("payload", numDelivered: 5), CancellationToken.None);

        Assert.Equal(1, rec.Terms);
        Assert.Single(_jetStream.Published); // the dead-letter publish itself succeeded, once
    }

    [Fact]
    public async Task NakFailure_DoesNotUnwindTheConsumeLoop()
    {
        // Regression (r24): both NakAsync sites ran bare; a thrown NAK unwound the consume loop
        // and rebuilt the subscriber. A lost NAK merely means redelivery waits for AckWait
        // instead of the configured delay, so it is swallow-and-log like the acks.
        var rec = new RecordingDelivery { NakException = new InvalidOperationException("nak failed") };
        var subscriber = new NatsSubscriberOptions { MaxDeliveryAttempts = 5, RedeliveryDelay = TimeSpan.FromSeconds(7) };
        await using var dispatcher = CreateDispatcher((_, _) => throw new InvalidOperationException("boom"), subscriber);

        // Below the cap: the failure path NAKs, and the NAK failure must NOT escape.
        await dispatcher.HandleAsync(rec.Create("payload", numDelivered: 2), CancellationToken.None);

        Assert.Equal([TimeSpan.FromSeconds(7)], rec.Naks);
        Assert.Equal(0, rec.Terms);
        Assert.Empty(_jetStream.Published);
    }

    [Fact]
    public async Task HandlerFailureAtMaxAttempts_WithDeadLetterDisabled_TerminatesWithoutPublishing()
    {
        var rec = new RecordingDelivery();
        var options = new NatsAsyncResponseTransportOptions { DeadLetterEnabled = false };
        var subscriber = new NatsSubscriberOptions { MaxDeliveryAttempts = 3 };
        await using var dispatcher = CreateDispatcher((_, _) => throw new InvalidOperationException("boom"), subscriber, options);

        await dispatcher.HandleAsync(rec.Create("payload", numDelivered: 3), CancellationToken.None);

        Assert.Equal(1, rec.Terms);
        Assert.Empty(_jetStream.Published);
    }

    [Fact]
    public async Task EarlyAck_AcksImmediately_AndProcessesInBackground()
    {
        var processed = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        var subscriber = new NatsSubscriberOptions().UseAckAfterEnqueue(backgroundWorkerCount: 1, backgroundQueueCapacity: 4, backgroundDrainTimeout: TimeSpan.FromSeconds(5));
        await using var dispatcher = CreateDispatcher((delivery, _) => { processed.TrySetResult(delivery.Payload); return Task.CompletedTask; }, subscriber);

        var rec = new RecordingDelivery();
        await dispatcher.HandleAsync(rec.Create("payload", numDelivered: 1), CancellationToken.None);

        Assert.Equal(1, rec.Acks);
        Assert.Equal("payload", await processed.Task.WaitAsync(TimeSpan.FromSeconds(2)));
    }

    [Fact]
    public async Task EarlyAck_BackgroundFailure_DeadLettersAndReportsContext()
    {
        var failure = new TaskCompletionSource<NatsBackgroundFailureContext>(TaskCreationOptions.RunContinuationsAsynchronously);
        var subscriber = new NatsSubscriberOptions { OnBackgroundFailure = ctx => { failure.TrySetResult(ctx); return ValueTask.CompletedTask; } }
            .UseAckAfterEnqueue(backgroundWorkerCount: 1, backgroundQueueCapacity: 4, backgroundDrainTimeout: TimeSpan.FromSeconds(5));
        await using var dispatcher = CreateDispatcher((_, _) => throw new InvalidOperationException("bg-boom"), subscriber);

        var rec = new RecordingDelivery();
        var headers = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase) { ["AR-Correlation-Id"] = "c1" };
        await dispatcher.HandleAsync(rec.Create("payload", numDelivered: 1, headers: headers), CancellationToken.None);

        Assert.Equal(1, rec.Acks);
        var context = await failure.Task.WaitAsync(TimeSpan.FromSeconds(2));
        Assert.Equal("c1", context.CorrelationId);
        Assert.Equal("bg-boom", context.Exception.Message);
        Assert.Equal("test-consumer", context.Consumer);
        // Dead-lettering happens before the failure callback, so the DLQ entry is present.
        Assert.Contains(_jetStream.Published, p => p.Subject == DeadLetterSubject);
    }

    [Fact]
    public async Task EarlyAck_WhenQueueFull_PausesConsumeLoopUntilCapacityFrees()
    {
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var subscriber = new NatsSubscriberOptions().UseAckAfterEnqueue(backgroundWorkerCount: 1, backgroundQueueCapacity: 1, backgroundDrainTimeout: TimeSpan.FromSeconds(5));
        await using var dispatcher = CreateDispatcher(async (_, _) => { started.TrySetResult(); await gate.Task; }, subscriber);

        var first = new RecordingDelivery();
        await dispatcher.HandleAsync(first.Create("p1", 1), CancellationToken.None);
        await started.Task.WaitAsync(TimeSpan.FromSeconds(2)); // worker is now busy on p1; queue is empty

        var second = new RecordingDelivery();
        await dispatcher.HandleAsync(second.Create("p2", 1), CancellationToken.None); // fills the single queue slot

        // Queue full: HandleAsync must wait for capacity (pausing the consume loop that awaits it)
        // instead of NAKing — NAK churn burns redeliveries without making progress.
        var third = new RecordingDelivery();
        var thirdHandle = dispatcher.HandleAsync(third.Create("p3", 1), CancellationToken.None);
        await Task.Delay(100);
        Assert.False(thirdHandle.IsCompleted);
        Assert.Equal(0, third.Acks);
        Assert.Empty(third.Naks);

        gate.TrySetResult(); // p1 finishes, the worker frees a slot, p3 is accepted and ACKed
        await thirdHandle.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Equal(1, first.Acks);
        Assert.Equal(1, second.Acks);
        Assert.Equal(1, third.Acks);
        Assert.Empty(third.Naks);
    }

    [Fact]
    public async Task EarlyAck_WhenQueueFullAndSubscriberStops_NaksWaitingMessageForRedelivery()
    {
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var subscriber = new NatsSubscriberOptions
        {
            RedeliveryDelay = TimeSpan.FromSeconds(3)
        }.UseAckAfterEnqueue(backgroundWorkerCount: 1, backgroundQueueCapacity: 1, backgroundDrainTimeout: TimeSpan.FromSeconds(5));
        await using var dispatcher = CreateDispatcher(async (_, _) => { started.TrySetResult(); await gate.Task; }, subscriber);

        var first = new RecordingDelivery();
        await dispatcher.HandleAsync(first.Create("p1", 1), CancellationToken.None);
        await started.Task.WaitAsync(TimeSpan.FromSeconds(2));

        var second = new RecordingDelivery();
        await dispatcher.HandleAsync(second.Create("p2", 1), CancellationToken.None);

        using var stopping = new CancellationTokenSource();
        var third = new RecordingDelivery();
        var thirdHandle = dispatcher.HandleAsync(third.Create("p3", 1), stopping.Token);
        await Task.Delay(50);

        stopping.Cancel();
        await thirdHandle.WaitAsync(TimeSpan.FromSeconds(5));

        // A message caught waiting when the subscriber stops is NAKed so JetStream redelivers it.
        Assert.Equal(0, third.Acks);
        Assert.Equal([TimeSpan.FromSeconds(3)], third.Naks);

        gate.TrySetResult();
    }

    [Fact]
    public async Task HandlerFailureAtMaxAttempts_WhenDeadLetterPublishFails_NaksForRedelivery()
    {
        _jetStream.PublishFailureForAttempt = _ => new InvalidOperationException("dead-letter stream down");
        var rec = new RecordingDelivery();
        var subscriber = new NatsSubscriberOptions { MaxDeliveryAttempts = 1, RedeliveryDelay = TimeSpan.FromSeconds(11) };
        await using var dispatcher = CreateDispatcher((_, _) => throw new InvalidOperationException("boom"), subscriber);

        await dispatcher.HandleAsync(rec.Create("payload", numDelivered: 1), CancellationToken.None);

        Assert.Equal(0, rec.Terms);
        Assert.Equal([TimeSpan.FromSeconds(11)], rec.Naks);
        Assert.Empty(_jetStream.Published);
    }

    [Fact]
    public async Task EarlyAck_ToleratesThrowingBackgroundFailureCallback()
    {
        var callbackInvoked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var subscriber = new NatsSubscriberOptions
        {
            OnBackgroundFailure = _ =>
            {
                callbackInvoked.TrySetResult();
                throw new InvalidOperationException("callback boom");
            }
        }.UseAckAfterEnqueue(backgroundWorkerCount: 1, backgroundQueueCapacity: 4, backgroundDrainTimeout: TimeSpan.FromSeconds(5));
        await using var dispatcher = CreateDispatcher((_, _) => throw new InvalidOperationException("bg"), subscriber);

        var rec = new RecordingDelivery();
        await dispatcher.HandleAsync(rec.Create("payload", numDelivered: 1), CancellationToken.None);

        // The throwing OnBackgroundFailure callback is invoked and its exception is swallowed (no crash).
        await callbackInvoked.Task.WaitAsync(TimeSpan.FromSeconds(2));
    }

    [Fact]
    public async Task DisposeAsync_LogsAndCompletes_WhenBackgroundDrainTimesOut()
    {
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var subscriber = new NatsSubscriberOptions().UseAckAfterEnqueue(
            backgroundWorkerCount: 1,
            backgroundQueueCapacity: 4,
            backgroundDrainTimeout: TimeSpan.FromMilliseconds(50));
        var dispatcher = CreateDispatcher(async (_, _) => { started.TrySetResult(); await gate.Task; }, subscriber);

        var rec = new RecordingDelivery();
        await dispatcher.HandleAsync(rec.Create("payload", numDelivered: 1), CancellationToken.None);
        await started.Task.WaitAsync(TimeSpan.FromSeconds(2)); // worker is blocked in the handler

        // Drain cannot complete within the timeout; DisposeAsync must still return (not hang).
        await dispatcher.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(2));

        gate.TrySetResult();
    }

    [Fact]
    public async Task DisposeAsync_DrainTimeout_RunningWorkerStillCompletesFailurePathAfterDispose()
    {
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var failure = new TaskCompletionSource<NatsBackgroundFailureContext>(TaskCreationOptions.RunContinuationsAsynchronously);
        var subscriber = new NatsSubscriberOptions
        {
            OnBackgroundFailure = ctx =>
            {
                failure.TrySetResult(ctx);
                return ValueTask.CompletedTask;
            }
        }.UseAckAfterEnqueue(
            backgroundWorkerCount: 1,
            backgroundQueueCapacity: 4,
            backgroundDrainTimeout: TimeSpan.FromMilliseconds(50));
        var dispatcher = CreateDispatcher(
            async (_, cancellationToken) =>
            {
                started.TrySetResult();
                await Task.Delay(TimeSpan.FromSeconds(30), cancellationToken);
            },
            subscriber);

        var rec = new RecordingDelivery();
        await dispatcher.HandleAsync(rec.Create("payload", numDelivered: 1), CancellationToken.None);
        await started.Task.WaitAsync(TimeSpan.FromSeconds(2)); // worker is blocked in the handler

        await dispatcher.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(2));

        // The drain CTS must stay alive until the worker actually finishes: after the timed-out
        // dispose the cancelled handler still reports through the failure callback (no
        // ObjectDisposedException) — but the drain cancellation is a shutdown, not a handler
        // failure, so nothing is dead-lettered (Kafka/Redis dispatcher parity).
        var context = await failure.Task.WaitAsync(TimeSpan.FromSeconds(2));
        Assert.IsAssignableFrom<OperationCanceledException>(context.Exception);
        Assert.DoesNotContain(_jetStream.Published, p => p.Subject == DeadLetterSubject);
    }

    [Fact]
    public async Task DisposeAsync_QueuedButUnstartedMessages_AreSurfacedViaOnBackgroundFailure_NotDeadLettered()
    {
        // Regression (r25): the drain-cancellation OperationCanceledException fell into the
        // generic background catch, which wrote never-completed jobs to the dead-letter subject
        // with reason "A task was canceled" — misfiling a shutdown as a handler failure. Kafka
        // and Redis both carve the drain cancellation out; NATS now does too: the drop is
        // surfaced via OnBackgroundFailure only.
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var failures = new List<NatsBackgroundFailureContext>();
        var subscriber = new NatsSubscriberOptions
        {
            OnBackgroundFailure = context =>
            {
                lock (failures)
                {
                    failures.Add(context);
                }

                return ValueTask.CompletedTask;
            }
        }.UseAckAfterEnqueue(
            backgroundWorkerCount: 1,
            backgroundQueueCapacity: 4,
            backgroundDrainTimeout: TimeSpan.FromMilliseconds(50));
        var dispatcher = CreateDispatcher(
            async (_, cancellationToken) =>
            {
                started.TrySetResult();
                await Task.Delay(TimeSpan.FromSeconds(30), cancellationToken);
            },
            subscriber);

        var running = new RecordingDelivery();
        var queued = new RecordingDelivery();
        await dispatcher.HandleAsync(running.Create("p1", 1), CancellationToken.None);
        await started.Task.WaitAsync(TimeSpan.FromSeconds(2)); // worker is blocked in the handler
        await dispatcher.HandleAsync(queued.Create("p2", 1), CancellationToken.None); // already ACKed, waiting in queue

        await dispatcher.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(2));

        // Both the mid-handler message and the still-queued one were already ACKed: the shutdown
        // interruption is surfaced through OnBackgroundFailure for each, and neither is written
        // to the dead-letter subject as a phantom handler failure.
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
        }

        Assert.Empty(_jetStream.Published);
    }

    [Fact]
    public async Task DisposeAsync_DisposesCancellationSourceWhenWorkerAggregationFaults()
    {
        var subscriber = new NatsSubscriberOptions().UseAckAfterEnqueue(1, 4, TimeSpan.FromSeconds(1));
        var dispatcher = CreateDispatcher((_, _) => Task.CompletedTask, subscriber);
        var workersField = typeof(NatsMessageDispatcher)
            .GetField("_backgroundWorkers", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!;
        var workers = (Task[])workersField.GetValue(dispatcher)!;
        workersField.SetValue(dispatcher, workers.Append(Task.FromException(new InvalidOperationException("worker"))).ToArray());

        await dispatcher.DisposeAsync();
    }
}
