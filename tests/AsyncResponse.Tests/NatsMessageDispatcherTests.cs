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
        NatsAsyncResponseTransportOptions? options = null,
        Microsoft.Extensions.Logging.ILogger? logger = null)
    {
        options ??= new NatsAsyncResponseTransportOptions();
        return new NatsMessageDispatcher(
            handler,
            _jetStream,
            options,
            subscriber,
            new NatsTransportSubjectSchema(options),
            logger ?? new TestLogger(),
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
    public async Task OverCapDelivery_DeadLettersAndTermsWithoutExecutingTheHandler()
    {
        // Regression: the consumer is created with MaxDeliver = -1 on the premise that the
        // dispatcher bounds attempts, but the only cap check ran inside HandleFailureAsync —
        // reached only when the handler THREW. A delivery whose earlier attempts never settled
        // (process killed mid-handler, a failed NAK) therefore redelivered forever and was never
        // dead-lettered (DB/Redis dispatcher parity).
        var rec = new RecordingDelivery();
        var handled = false;
        await using var dispatcher = CreateDispatcher(
            (_, _) =>
            {
                handled = true;
                return Task.CompletedTask;
            },
            new NatsSubscriberOptions { MaxDeliveryAttempts = 3 });

        await dispatcher.HandleAsync(rec.Create("payload", numDelivered: 4), CancellationToken.None);

        Assert.False(handled);
        Assert.Equal(1, rec.Terms);
        Assert.Equal(0, rec.Acks);
        Assert.Empty(rec.Naks);
        var published = Assert.Single(_jetStream.Published);
        Assert.Equal(DeadLetterSubject, published.Subject);
    }

    [Fact]
    public async Task LastAttemptFailure_DuringShutdown_StillDeadLetters()
    {
        // Regression (round 31): HandleFailureAsync forwarded the subscriber's stopping token into
        // the dead-letter publish — the single unpinned settlement in the package (the
        // pre-execution cap and the early-ACK failure path already used CancellationToken.None) —
        // so a handler failing on its LAST attempt while the host stopped had the burial aborted
        // on the cancelled token and the message was NAKed back instead of buried.
        var rec = new RecordingDelivery();
        using var stopping = new CancellationTokenSource();
        stopping.Cancel();
        await using var dispatcher = CreateDispatcher(
            (_, _) => throw new InvalidOperationException("handler boom"),
            new NatsSubscriberOptions { MaxDeliveryAttempts = 3 });

        await dispatcher.HandleAsync(rec.Create("payload", numDelivered: 3), stopping.Token);

        var published = Assert.Single(_jetStream.Published);
        Assert.Equal(DeadLetterSubject, published.Subject);
        Assert.Equal(1, rec.Terms);
        Assert.Empty(rec.Naks);
    }

    [Fact]
    public async Task AtCapDelivery_StillExecutesTheHandler()
    {
        // The cap's LAST allowed attempt must still run: the pre-execution check refuses only
        // deliveries beyond it (NumDelivered > cap), matching HandleFailureAsync's >= cap burial
        // for an attempt that throws.
        var rec = new RecordingDelivery();
        var handled = false;
        await using var dispatcher = CreateDispatcher(
            (_, _) =>
            {
                handled = true;
                return Task.CompletedTask;
            },
            new NatsSubscriberOptions { MaxDeliveryAttempts = 3 });

        await dispatcher.HandleAsync(rec.Create("payload", numDelivered: 3), CancellationToken.None);

        Assert.True(handled);
        Assert.Equal(1, rec.Acks);
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
    public async Task FlowHostStopInterruption_WithLiveSubscriberToken_LeavesDeliveryUnsettled()
    {
        // The flow engine hands a delivery back on ApplicationStopping, which fires BEFORE any
        // hosted service stops — so the worker subscriber's own token is usually still live when
        // DurableFlowInterruptedException arrives. The old filter keyed on that token only, so the
        // interruption went through HandleFailureAsync: at the cap it dead-lettered and TERMed a
        // healthy flow wake-up (below it, a NAK logged as a handler failure).
        var rec = new RecordingDelivery();
        await using var dispatcher = CreateDispatcher(
            (_, _) => throw new DurableFlowInterruptedException("Host is stopping."),
            new NatsSubscriberOptions { MaxDeliveryAttempts = 5 });

        await dispatcher.HandleAsync(rec.Create("payload", numDelivered: 5), CancellationToken.None);

        Assert.Equal(0, rec.Acks);
        Assert.Empty(rec.Naks);
        Assert.Equal(0, rec.Terms);
        Assert.Empty(_jetStream.Published);
        Assert.True(dispatcher.HandBackSignalled); // the subscriber stops fetching on it
    }

    [Fact]
    public async Task EarlyAck_FlowHostStopInterruption_IsSurfacedAndDeadLetteredAsHandedBackAfterCommit()
    {
        // A host-stop hand-back of an already-ACKed job is a shutdown, not a handler failure (no
        // Error log). The early-ACK hand-back rule of the pre-commit review of fixpoint round 1:
        // JetStream will never redeliver it, so besides the OnBackgroundFailure report it gets a
        // dead-letter copy whose reason names it (only the report was made before, contradicting
        // the drain path's own "the dead-letter copy is its only record"). Replay is safe: the run
        // resumes from its last checkpoint.
        var failure = new TaskCompletionSource<NatsBackgroundFailureContext>(TaskCreationOptions.RunContinuationsAsynchronously);
        var subscriber = new NatsSubscriberOptions { OnBackgroundFailure = ctx => { failure.TrySetResult(ctx); return ValueTask.CompletedTask; } }
            .UseAckAfterEnqueue(backgroundWorkerCount: 1, backgroundQueueCapacity: 4, backgroundDrainTimeout: TimeSpan.FromSeconds(5));
        var logger = new RecordingThrowingLogger<NatsMessageDispatcherTests>();
        await using var dispatcher = CreateDispatcher((_, _) => throw new DurableFlowInterruptedException("Host is stopping."), subscriber, logger: logger);

        var rec = new RecordingDelivery();
        await dispatcher.HandleAsync(rec.Create("payload", numDelivered: 1), CancellationToken.None);

        var context = await failure.Task.WaitAsync(TimeSpan.FromSeconds(2));
        Assert.IsType<DurableFlowInterruptedException>(context.Exception);
        var published = Assert.Single(_jetStream.Published); // the burial precedes the report
        Assert.Equal(DeadLetterSubject, published.Subject);
        Assert.Equal("handed_back_after_commit: Host is stopping.", published.Headers!["AR-DeadLetter-Reason"]);
        Assert.Equal(1, rec.Acks); // ACKed at enqueue; never NAKed or TERMed afterwards
        Assert.True(dispatcher.HandBackSignalled);
        Assert.True(logger.HasEntry(Microsoft.Extensions.Logging.LogLevel.Warning, "Dead-lettering a copy"));
        Assert.False(logger.HasEntry(Microsoft.Extensions.Logging.LogLevel.Error, string.Empty));
    }

    [Fact]
    public async Task EarlyAck_FlowHostStopInterruption_WithDeadLetteringDisabled_LogsTheLossAtError()
    {
        // Pass 2 of the pre-commit review of fixpoint round 1: with dead-lettering disabled the
        // burial is skipped, yet the hand-back still logged a Warning claiming "Dead-lettering a
        // copy" — a wake-up JetStream will never redeliver was lost at Warning under a false
        // claim. Without a destination the loss is now logged at Error and says so.
        var failure = new TaskCompletionSource<NatsBackgroundFailureContext>(TaskCreationOptions.RunContinuationsAsynchronously);
        var subscriber = new NatsSubscriberOptions { OnBackgroundFailure = ctx => { failure.TrySetResult(ctx); return ValueTask.CompletedTask; } }
            .UseAckAfterEnqueue(backgroundWorkerCount: 1, backgroundQueueCapacity: 4, backgroundDrainTimeout: TimeSpan.FromSeconds(5));
        var logger = new RecordingThrowingLogger<NatsMessageDispatcherTests>();
        await using var dispatcher = CreateDispatcher(
            (_, _) => throw new DurableFlowInterruptedException("Host is stopping."),
            subscriber,
            new NatsAsyncResponseTransportOptions { DeadLetterEnabled = false },
            logger);

        var rec = new RecordingDelivery();
        await dispatcher.HandleAsync(rec.Create("payload", numDelivered: 1), CancellationToken.None);

        var context = await failure.Task.WaitAsync(TimeSpan.FromSeconds(2));
        Assert.IsType<DurableFlowInterruptedException>(context.Exception);
        Assert.Empty(_jetStream.Published);
        Assert.True(logger.HasEntry(Microsoft.Extensions.Logging.LogLevel.Error, "no dead-letter destination is configured"));
        Assert.False(logger.HasEntry(Microsoft.Extensions.Logging.LogLevel.Warning, "Dead-lettering a copy"));
        Assert.True(dispatcher.HandBackSignalled);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task FlowHostStopInterruption_DoesNotMarkTheReceiveSpanError(bool earlyAck)
    {
        // Pass 2 of the pre-commit review of fixpoint round 1: the shared handler execution marked
        // the receive span as an error for every exception, the flow engine's host-stop hand-back
        // included, so every rolling deploy produced error spans (HandlerFailure_MarksNatsReceiveSpanError
        // pins that a real failure still does).
        using var collector = new AsyncResponseActivityCollector();
        var subscriber = earlyAck
            ? new NatsSubscriberOptions().UseAckAfterEnqueue(backgroundWorkerCount: 1, backgroundQueueCapacity: 4, backgroundDrainTimeout: TimeSpan.FromSeconds(5))
            : new NatsSubscriberOptions();
        await using var dispatcher = CreateDispatcher((_, _) => throw new DurableFlowInterruptedException("Host is stopping."), subscriber);

        await dispatcher.HandleAsync(new RecordingDelivery().Create("payload", numDelivered: 1), CancellationToken.None);

        await WaitUntilAsync(() => collector.Count("asyncresponse.nats.receive") == 1);
        var activity = collector.Single("asyncresponse.nats.receive", "asyncresponse.transport", "nats");
        Assert.NotEqual(ActivityStatusCode.Error, activity.Status);
        Assert.Null(AsyncResponseActivityCollector.Tag(activity, "error.type"));
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
    public async Task DeadLetterPublish_DropsEveryJetStreamDirective_AndCapsTheReason()
    {
        // A producer's Nats-Expected-Stream (or -Last-Sequence, Nats-Rollup, Nats-TTL) was copied
        // into the burial, where the server applied it to the DLQ publish and refused it; the
        // dispatcher then NAKed, and under MaxDeliver = -1 the message looped forever. The reason
        // header was also unbounded, so a long exception message could push the burial past the
        // server's max_payload the same way.
        var rec = new RecordingDelivery();
        var headers = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["Nats-Msg-Id"] = "live-publish-id",
            ["Nats-Expected-Stream"] = "asyncresponse_transport_worker",
            ["Nats-Expected-Last-Subject-Sequence"] = "41",
            ["Nats-Rollup"] = "sub",
            ["Nats-TTL"] = "1h",
            ["AR-Correlation-Id"] = "corr-dlq",
            ["X-App"] = "kept"
        };
        var longMessage = "line one\r\nline two " + new string('x', 10_000);
        await using var dispatcher = CreateDispatcher(
            (_, _) => throw new InvalidOperationException(longMessage),
            new NatsSubscriberOptions { MaxDeliveryAttempts = 1 });

        await dispatcher.HandleAsync(rec.Create("payload", numDelivered: 1, headers: headers), CancellationToken.None);

        var buried = Assert.Single(_jetStream.Published);
        Assert.DoesNotContain(buried.Headers!.Keys, name => name.StartsWith("Nats-", StringComparison.OrdinalIgnoreCase));
        Assert.Equal("corr-dlq", buried.Headers!["AR-Correlation-Id"]);
        Assert.Equal("kept", buried.Headers!["X-App"]);
        var reason = buried.Headers!["AR-DeadLetter-Reason"];
        Assert.Equal(NatsMessageDispatcher.MaxDeadLetterReasonLength, reason.Length);
        Assert.DoesNotContain('\r', reason);
        Assert.DoesNotContain('\n', reason);
        Assert.Equal(1, rec.Terms);
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
    public async Task DisposeAsync_QueuedButUnstartedMessages_AreDeadLetteredAndSurfacedViaOnBackgroundFailure()
    {
        // Regression (r25): the drain-cancellation OperationCanceledException fell into the
        // generic background catch, which wrote never-completed jobs to the dead-letter subject
        // with reason "A task was canceled" — misfiling a shutdown as a handler failure. The
        // mid-handler message is still surfaced via OnBackgroundFailure only. The QUEUED,
        // never-started message is now (DB/Redis parity) explicitly dead-lettered by the
        // drain-budget pre-check AND surfaced: it was ACKed at enqueue, so without the DLQ copy
        // it would vanish at process exit with no record.
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
        // interruption is surfaced through OnBackgroundFailure for each. Only the never-started
        // one is written to the dead-letter subject — as a drain-budget lapse, not a phantom
        // handler failure — because nothing else can ever record it.
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

        var buried = Assert.Single(_jetStream.Published);
        Assert.Equal("p2", buried.Payload);
        Assert.Contains("drain budget lapsed", buried.Headers!["AR-DeadLetter-Reason"], StringComparison.Ordinal);
    }

    [Fact]
    public async Task DisposeAsync_WithEveryWorkerStuckInAHandler_BuriesTheQueuedEntriesBeforeReturning()
    {
        // Entries still queued when the drain lapses mean every worker is inside a handler — and
        // the real handler takes no token. The drain-lapsed routing lived only in the worker loop,
        // so it ran once some handler happened to finish: after DisposeAsync had returned and the
        // host had torn the connection down (or exited). Those already-ACKed jobs vanished. The
        // dispatcher now buries them itself inside a reserved slice of the drain budget.
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var failures = 0;
        var subscriber = new NatsSubscriberOptions
        {
            OnBackgroundFailure = _ =>
            {
                Interlocked.Increment(ref failures);
                return ValueTask.CompletedTask;
            }
        }.UseAckAfterEnqueue(
            backgroundWorkerCount: 1,
            backgroundQueueCapacity: 4,
            backgroundDrainTimeout: TimeSpan.FromSeconds(1));
        var dispatcher = CreateDispatcher(
            async (_, _) =>
            {
                started.TrySetResult();
                await release.Task; // ignores the cancellation, like the real ingress handler
            },
            subscriber);

        await dispatcher.HandleAsync(new RecordingDelivery().Create("running", 1), CancellationToken.None);
        await started.Task.WaitAsync(TimeSpan.FromSeconds(2));
        await dispatcher.HandleAsync(new RecordingDelivery().Create("queued-1", 1), CancellationToken.None);
        await dispatcher.HandleAsync(new RecordingDelivery().Create("queued-2", 1), CancellationToken.None);

        await dispatcher.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Equal(["queued-1", "queued-2"], _jetStream.Published.Select(p => p.Payload).Order().ToArray());
        Assert.All(_jetStream.Published, p => Assert.Equal(DeadLetterSubject, p.Subject));
        Assert.Equal(2, Volatile.Read(ref failures));

        release.TrySetResult();
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
