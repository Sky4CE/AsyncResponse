using AsyncResponse.Transports.MongoDB;
using Microsoft.Extensions.Logging.Abstractions;
using System.Diagnostics;
using Xunit;

namespace AsyncResponse.Tests;

public sealed class MongoDbDispatcherTests
{
    [Fact]
    public async Task AckAfterHandlerCompletes_AcksOnlyAfterSuccessfulHandler()
    {
        var calls = new Calls();
        var dispatcher = new MongoDbMessageDispatcher(
            (_, _) =>
            {
                calls.Handler++;
                Assert.Equal(0, calls.Ack);
                return Task.CompletedTask;
            },
            new MongoDbAsyncResponseTransportOptions(),
            new MongoDbSubscriberOptions(),
            NullLogger.Instance,
            MongoDbSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery(calls), CancellationToken.None);

        Assert.Equal(1, calls.Handler);
        Assert.Equal(1, calls.Ack);
        Assert.Equal(0, calls.Nak);
        Assert.Equal(0, calls.DeadLetter);
    }

    [Fact]
    public async Task HandlerExecution_EmitsMongoDbReceiveSpanWithMessagingTags()
    {
        using var collector = new AsyncResponseActivityCollector();
        var options = new MongoDbAsyncResponseTransportOptions();
        var id = Guid.NewGuid();
        var headers = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase) { [options.CorrelationIdHeader] = "corr-mongo" };
        var delivery = new MongoDbTransportDelivery(
            id,
            "worker",
            "{}",
            headers,
            1,
            () => ValueTask.CompletedTask,
            _ => ValueTask.CompletedTask,
            (_, _, _) => ValueTask.FromResult(true),
            () => ValueTask.FromResult(true));
        var dispatcher = new MongoDbMessageDispatcher(
            (_, _) => Task.CompletedTask,
            options,
            new MongoDbSubscriberOptions(),
            NullLogger.Instance,
            MongoDbSubscriberRole.Worker);

        await dispatcher.HandleAsync(delivery, CancellationToken.None);

        var activity = collector.Single("asyncresponse.mongodb.receive", "asyncresponse.transport", "mongodb");
        Assert.Equal(ActivityKind.Consumer, activity.Kind);
        Assert.Equal("Worker", AsyncResponseActivityCollector.Tag(activity, "asyncresponse.mongodb.role"));
        Assert.Equal(nameof(MongoDbAckMode.AckAfterHandlerCompletes), AsyncResponseActivityCollector.Tag(activity, "asyncresponse.mongodb.ack_mode"));
        Assert.Equal("mongodb", AsyncResponseActivityCollector.Tag(activity, "messaging.system"));
        Assert.Equal("worker", AsyncResponseActivityCollector.Tag(activity, "messaging.destination.name"));
        Assert.Equal(id.ToString(), AsyncResponseActivityCollector.Tag(activity, "messaging.message.id"));
        Assert.Equal(1, AsyncResponseActivityCollector.Tag(activity, "messaging.message.delivery_attempt"));
        Assert.Equal("corr-mongo", AsyncResponseActivityCollector.Tag(activity, "asyncresponse.correlation_id"));
    }

    [Fact]
    public async Task AckAfterEnqueue_BackgroundHandlerStillEmitsReceiveSpan()
    {
        using var collector = new AsyncResponseActivityCollector();
        var calls = new Calls();
        var handled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var dispatcher = new MongoDbMessageDispatcher(
            (_, _) =>
            {
                handled.SetResult();
                return Task.CompletedTask;
            },
            new MongoDbAsyncResponseTransportOptions(),
            new MongoDbSubscriberOptions().UseAckAfterEnqueue(1, 8, TimeSpan.FromSeconds(5)),
            NullLogger.Instance,
            MongoDbSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery(calls), CancellationToken.None);
        await handled.Task.WaitAsync(TimeSpan.FromSeconds(5));

        // Regression guard for the "both ACK modes emit the span" claim: the early-ACK path runs the
        // handler on a background worker, and a refactor that splits it from ExecuteHandlerAsync
        // would lose the span silently.
        await WaitUntilAsync(() => collector.Count("asyncresponse.mongodb.receive") == 1);
        var activity = collector.Single("asyncresponse.mongodb.receive", "asyncresponse.transport", "mongodb");
        Assert.Equal(ActivityKind.Consumer, activity.Kind);
        Assert.Equal(nameof(MongoDbAckMode.AckAfterEnqueue), AsyncResponseActivityCollector.Tag(activity, "asyncresponse.mongodb.ack_mode"));
    }

    [Fact]
    public async Task HandlerFailure_MarksMongoDbReceiveSpanError()
    {
        using var collector = new AsyncResponseActivityCollector();
        var calls = new Calls();
        var dispatcher = new MongoDbMessageDispatcher(
            (_, _) => throw new InvalidOperationException("boom"),
            new MongoDbAsyncResponseTransportOptions(),
            new MongoDbSubscriberOptions { MaxDeliveryAttempts = 2 },
            NullLogger.Instance,
            MongoDbSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery(calls, attempt: 1), CancellationToken.None);

        var activity = collector.Single("asyncresponse.mongodb.receive", "asyncresponse.transport", "mongodb");
        Assert.Equal(typeof(InvalidOperationException).FullName, AsyncResponseActivityCollector.Tag(activity, "error.type"));
        Assert.Equal(ActivityStatusCode.Error, activity.Status);
    }

    [Fact]
    public async Task AckAfterHandlerCompletes_NaksBeforeMaxAttempts()
    {
        var calls = new Calls();
        var dispatcher = new MongoDbMessageDispatcher(
            (_, _) => throw new InvalidOperationException("boom"),
            new MongoDbAsyncResponseTransportOptions(),
            new MongoDbSubscriberOptions { MaxDeliveryAttempts = 2 },
            NullLogger.Instance,
            MongoDbSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery(calls, attempt: 1), CancellationToken.None);

        Assert.Equal(0, calls.Ack);
        Assert.Equal(1, calls.Nak);
        Assert.Equal(0, calls.DeadLetter);
    }

    [Fact]
    public async Task AckAfterHandlerCompletes_DeadLettersAtMaxAttempts()
    {
        var calls = new Calls();
        var dispatcher = new MongoDbMessageDispatcher(
            (_, _) => throw new InvalidOperationException("boom"),
            new MongoDbAsyncResponseTransportOptions(),
            new MongoDbSubscriberOptions { MaxDeliveryAttempts = 2 },
            NullLogger.Instance,
            MongoDbSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery(calls, attempt: 2), CancellationToken.None);

        Assert.Equal(0, calls.Nak);
        Assert.Equal(1, calls.DeadLetter);
        Assert.True(calls.DeleteOriginalOnDeadLetter);
    }

    [Fact]
    public async Task AckAfterHandlerCompletes_NaksWhenDeadLetterPublishFails()
    {
        var calls = new Calls { DeadLetterResult = false };
        var dispatcher = new MongoDbMessageDispatcher(
            (_, _) => throw new InvalidOperationException("boom"),
            new MongoDbAsyncResponseTransportOptions(),
            new MongoDbSubscriberOptions { MaxDeliveryAttempts = 1 },
            NullLogger.Instance,
            MongoDbSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery(calls), CancellationToken.None);

        Assert.Equal(1, calls.DeadLetter);
        Assert.True(calls.DeleteOriginalOnDeadLetter);
        Assert.Equal(1, calls.Nak);
        Assert.Equal(TimeSpan.FromSeconds(5), calls.LastNakDelay);
    }

    [Fact]
    public async Task AckAfterHandlerCompletes_UnlimitedAttemptsAlwaysNaks()
    {
        var calls = new Calls();
        var dispatcher = new MongoDbMessageDispatcher(
            (_, _) => throw new InvalidOperationException("boom"),
            new MongoDbAsyncResponseTransportOptions(),
            new MongoDbSubscriberOptions { MaxDeliveryAttempts = 0 },
            NullLogger.Instance,
            MongoDbSubscriberRole.ResponseIngress);

        await dispatcher.HandleAsync(Delivery(calls, attempt: 99), CancellationToken.None);

        Assert.Equal(0, calls.Ack);
        Assert.Equal(1, calls.Nak);
        Assert.Equal(0, calls.DeadLetter);
    }

    [Fact]
    public async Task AckAfterEnqueue_AcksOnReceive_WithoutWaitingForHandler()
    {
        var calls = new Calls();
        var handled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var dispatcher = new MongoDbMessageDispatcher(
            (_, _) =>
            {
                calls.Handler++;
                handled.SetResult();
                return Task.CompletedTask;
            },
            new MongoDbAsyncResponseTransportOptions(),
            new MongoDbSubscriberOptions().UseAckAfterEnqueue(1, 8, TimeSpan.FromSeconds(5)),
            NullLogger.Instance,
            MongoDbSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery(calls), CancellationToken.None);

        // AckAfterEnqueue acks the document as part of accepting it into the background queue, so it
        // is already acknowledged the moment HandleAsync returns — without waiting for the handler.
        // The handler runs on the background worker and may execute concurrently with the ack, so the
        // test must not assert their relative ordering.
        Assert.Equal(1, calls.Ack);

        await handled.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(1, calls.Handler);
    }

    [Fact]
    public async Task AckAfterEnqueue_BackgroundFailure_DeadLettersWithoutDeletingOriginal_AndInvokesCallback()
    {
        var calls = new Calls();
        MongoDbBackgroundFailureContext? callback = null;
        var handled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var dispatcher = new MongoDbMessageDispatcher(
            (_, _) =>
            {
                handled.SetResult();
                throw new InvalidOperationException("background boom");
            },
            new MongoDbAsyncResponseTransportOptions(),
            new MongoDbSubscriberOptions
            {
                OnBackgroundFailure = context =>
                {
                    callback = context;
                    return ValueTask.CompletedTask;
                }
            }.UseAckAfterEnqueue(1, 8, TimeSpan.FromSeconds(5)),
            NullLogger.Instance,
            MongoDbSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery(calls, headers: new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["AR-Correlation-Id"] = "corr-background"
        }), CancellationToken.None);

        await handled.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await WaitUntilAsync(() => calls.DeadLetter == 1 && callback is not null);

        Assert.Equal(1, calls.Ack);
        Assert.Equal(1, calls.DeadLetter);
        Assert.False(calls.DeleteOriginalOnDeadLetter);
        Assert.NotNull(callback);
        Assert.Equal("worker", callback!.Queue);
        Assert.Equal("Worker", callback.SubscriberRole);
        Assert.Equal("corr-background", callback.CorrelationId);
        Assert.IsType<InvalidOperationException>(callback.Exception);
    }

    [Fact]
    public async Task AckAfterEnqueue_BackgroundFailureCallbackExceptionsAreSwallowed()
    {
        var calls = new Calls();
        var handled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var dispatcher = new MongoDbMessageDispatcher(
            (_, _) =>
            {
                handled.SetResult();
                throw new InvalidOperationException("background boom");
            },
            new MongoDbAsyncResponseTransportOptions(),
            new MongoDbSubscriberOptions
            {
                OnBackgroundFailure = _ => throw new InvalidOperationException("callback boom")
            }.UseAckAfterEnqueue(1, 8, TimeSpan.FromSeconds(5)),
            NullLogger.Instance,
            MongoDbSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery(calls), CancellationToken.None);

        await handled.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await WaitUntilAsync(() => calls.DeadLetter == 1);
        Assert.Equal(1, calls.DeadLetter);
    }

    [Fact]
    public async Task AckAfterEnqueue_Overflow_PausesTheClaimLoopInsteadOfNaking()
    {
        var calls = new Calls();
        using var entered = new ManualResetEventSlim();
        using var release = new ManualResetEventSlim();
        await using var dispatcher = new MongoDbMessageDispatcher(
            (_, _) =>
            {
                entered.Set();
                // Held while the test overflows the queue; must not lapse early on a
                // starved runner or the overflow scenario silently collapses.
                release.Wait(TimeSpan.FromSeconds(30));
                return Task.CompletedTask;
            },
            new MongoDbAsyncResponseTransportOptions(),
            new MongoDbSubscriberOptions().UseAckAfterEnqueue(1, 1, TimeSpan.FromSeconds(5)),
            NullLogger.Instance,
            MongoDbSubscriberRole.Worker);

        // The gated worker holds the first document; the second fills the capacity-1 queue; the third overflows.
        await dispatcher.HandleAsync(Delivery(calls), CancellationToken.None);
        Assert.True(entered.Wait(TimeSpan.FromSeconds(30)));
        await dispatcher.HandleAsync(Delivery(calls), CancellationToken.None);
        var overflow = dispatcher.HandleAsync(Delivery(calls), CancellationToken.None);

        // The overflow claim must park — pausing the claim loop, which is the actual
        // backpressure — not NAK: the subscriber loop treats every claimed document as progress,
        // so a NAK-on-full is re-claimed immediately and spins at full database rate, burning an
        // attempt per lap. No ACK either until the document is actually enqueued.
        Assert.False(overflow.IsCompleted);
        Assert.Equal(0, calls.Nak);
        Assert.Equal(2, calls.Ack);

        release.Set();
        await overflow.WaitAsync(TimeSpan.FromSeconds(30));

        Assert.Equal(0, calls.Nak);
        Assert.Equal(3, calls.Ack); // every claimed document ACKed exactly once
    }

    [Fact]
    public async Task Handler_ShutdownCancellation_PropagatesWithoutSettling()
    {
        // A handler cancelled by host shutdown is not a handler failure: routing it through
        // HandleFailureAsync NAKs (burning an attempt) or, at the cap, dead-letters healthy work.
        // The cancellation must propagate with the document unsettled — the claim's lease lapses
        // and at-least-once redelivery applies after restart.
        var calls = new Calls();
        var stopping = new CancellationToken(canceled: true);
        await using var dispatcher = new MongoDbMessageDispatcher(
            (_, token) => Task.FromException(new OperationCanceledException(token)),
            new MongoDbAsyncResponseTransportOptions(),
            new MongoDbSubscriberOptions(),
            NullLogger.Instance,
            MongoDbSubscriberRole.Worker);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => dispatcher.HandleAsync(Delivery(calls), stopping));

        Assert.Equal(0, calls.Ack);
        Assert.Equal(0, calls.Nak);
        Assert.Equal(0, calls.DeadLetter);
    }

    [Fact]
    public async Task AckAfterEnqueue_AckFailure_DoesNotTearDownTheSubscriber()
    {
        // Once TryWrite succeeds the document belongs to a background worker: an ACK failure
        // after that must not escape HandleAsync — unwinding tears down the subscriber (draining
        // the worker, which RUNS the handler) while the un-ACKed document's lease lapses, so a
        // rebuilt subscriber claims and runs the same job a second time. Same rule as the
        // post-handler ACK: swallow, log, let at-least-once redelivery apply.
        var calls = new Calls { AckFailure = new InvalidOperationException("ack boom") };
        var handled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var dispatcher = new MongoDbMessageDispatcher(
            (_, _) =>
            {
                handled.TrySetResult();
                return Task.CompletedTask;
            },
            new MongoDbAsyncResponseTransportOptions(),
            new MongoDbSubscriberOptions().UseAckAfterEnqueue(1, 8, TimeSpan.FromSeconds(5)),
            NullLogger.Instance,
            MongoDbSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery(calls), CancellationToken.None); // must not throw

        await handled.Task.WaitAsync(TimeSpan.FromSeconds(5)); // the enqueued document still executes
        Assert.Equal(0, calls.Ack);
        Assert.Equal(0, calls.Nak);
    }

    [Fact]
    public async Task AckAfterEnqueue_BackgroundFailureWithoutCallback_DeadLetters()
    {
        var calls = new Calls();
        var handled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var dispatcher = new MongoDbMessageDispatcher(
            (_, _) =>
            {
                handled.SetResult();
                throw new InvalidOperationException("background boom");
            },
            new MongoDbAsyncResponseTransportOptions(),
            new MongoDbSubscriberOptions().UseAckAfterEnqueue(1, 8, TimeSpan.FromSeconds(5)),
            NullLogger.Instance,
            MongoDbSubscriberRole.ResponseIngress);

        await dispatcher.HandleAsync(Delivery(calls), CancellationToken.None);

        await handled.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await WaitUntilAsync(() => calls.DeadLetter == 1);
        Assert.Equal(1, calls.DeadLetter);
    }

    [Fact]
    public async Task AckAfterEnqueue_BackgroundOperationCanceled_DeadLettersLikeHandlerFailure()
    {
        var calls = new Calls();
        var handled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var dispatcher = new MongoDbMessageDispatcher(
            (_, _) =>
            {
                handled.SetResult();
                throw new OperationCanceledException();
            },
            new MongoDbAsyncResponseTransportOptions(),
            new MongoDbSubscriberOptions().UseAckAfterEnqueue(1, 8, TimeSpan.FromSeconds(5)),
            NullLogger.Instance,
            MongoDbSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery(calls), CancellationToken.None);

        await handled.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await WaitUntilAsync(() => calls.DeadLetter == 1);
        Assert.Equal(1, calls.DeadLetter);
    }

    [Fact]
    public async Task DisposeAsync_TimesOutWhenBackgroundWorkerDoesNotDrain()
    {
        var calls = new Calls();
        var handlerEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var dispatcher = new MongoDbMessageDispatcher(
            async (_, _) =>
            {
                handlerEntered.SetResult();
                await release.Task;
            },
            new MongoDbAsyncResponseTransportOptions(),
            new MongoDbSubscriberOptions().UseAckAfterEnqueue(1, 8, TimeSpan.FromMilliseconds(1)),
            NullLogger.Instance,
            MongoDbSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery(calls), CancellationToken.None);
        await handlerEntered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await dispatcher.DisposeAsync();
        release.SetResult();

        Assert.Equal(1, calls.Ack);
    }

    [Fact]
    public async Task DisposeAsync_DrainTimeout_RunningWorkerStillDeadLettersAfterDispose()
    {
        var calls = new Calls();
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var dispatcher = new MongoDbMessageDispatcher(
            async (_, cancellationToken) =>
            {
                started.TrySetResult();
                await Task.Delay(TimeSpan.FromSeconds(30), cancellationToken);
            },
            new MongoDbAsyncResponseTransportOptions(),
            new MongoDbSubscriberOptions().UseAckAfterEnqueue(1, 8, TimeSpan.FromMilliseconds(50)),
            NullLogger.Instance,
            MongoDbSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery(calls), CancellationToken.None);
        await started.Task.WaitAsync(TimeSpan.FromSeconds(5)); // worker is blocked in the handler

        await dispatcher.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5));

        // The drain CTS must stay alive until the worker actually finishes: after the timed-out dispose the
        // cancelled handler still dead-letters through the background failure path (no ObjectDisposedException).
        await calls.DeadLettered.Task.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.False(calls.DeleteOriginalOnDeadLetter);
    }

    [Fact]
    public void Constructor_RejectsInvalidSubscriberOptions()
    {
        var ex = Assert.Throws<InvalidOperationException>(() => new MongoDbMessageDispatcher(
            (_, _) => Task.CompletedTask,
            new MongoDbAsyncResponseTransportOptions(),
            new MongoDbSubscriberOptions { AckMode = MongoDbAckMode.AckAfterEnqueue },
            NullLogger.Instance,
            MongoDbSubscriberRole.Worker));

        Assert.Contains(nameof(MongoDbSubscriberOptions.BackgroundWorkerCount), ex.Message);
    }

    [Fact]
    public void ValidateSubscriber_DrainBudgetExceedingHostShutdownBudget_Throws()
    {
        var options = new MongoDbAsyncResponseTransportOptions
        {
            ShutdownTimeout = TimeSpan.FromSeconds(20),
            HostShutdownTimeout = TimeSpan.FromSeconds(25)
        };
        var subscriber = new MongoDbSubscriberOptions().UseAckAfterEnqueue(1, 8, TimeSpan.FromSeconds(10));

        var ex = Assert.Throws<InvalidOperationException>(() =>
            MongoDbTransportOptionsValidator.ValidateSubscriber(options, subscriber, "Worker"));

        Assert.Contains(nameof(MongoDbAsyncResponseTransportOptions.HostShutdownTimeout), ex.Message);

        // A null host budget or an awaiting-mode subscriber skips the check.
        options.HostShutdownTimeout = null;
        MongoDbTransportOptionsValidator.ValidateSubscriber(options, subscriber, "Worker");
        MongoDbTransportOptionsValidator.ValidateSubscriber(
            new MongoDbAsyncResponseTransportOptions { HostShutdownTimeout = TimeSpan.FromSeconds(1) },
            new MongoDbSubscriberOptions(),
            "Worker");
    }

    [Fact]
    public void ValidateSubscriber_NonPositiveHostShutdownTimeout_Throws()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            MongoDbTransportOptionsValidator.ValidateSubscriber(
                new MongoDbAsyncResponseTransportOptions { HostShutdownTimeout = TimeSpan.Zero },
                new MongoDbSubscriberOptions().UseAckAfterEnqueue(1, 8),
                "Worker"));

        Assert.Contains(nameof(MongoDbAsyncResponseTransportOptions.HostShutdownTimeout), ex.Message);
    }

    [Fact]
    public void ValidateSubscriber_DocumentedEarlyAckDefaults_Pass()
    {
        // Regression: the documented two-arg early-ACK opt-in with stock defaults
        // (5s ShutdownTimeout + 20s BackgroundDrainTimeout vs HostShutdownTimeout 30s)
        // must not fail startup.
        MongoDbTransportOptionsValidator.ValidateSubscriber(
            new MongoDbAsyncResponseTransportOptions(),
            new MongoDbSubscriberOptions().UseAckAfterEnqueue(4, 256),
            "Worker");
    }

    [Fact]
    public async Task AckAfterHandlerCompletes_SlowHandler_RenewsLeaseUntilHandlerFinishes()
    {
        var calls = new Calls();
        var dispatcher = new MongoDbMessageDispatcher(
            async (_, _) =>
            {
                while (Volatile.Read(ref calls.Renew) < 2)
                    await Task.Delay(10);
            },
            new MongoDbAsyncResponseTransportOptions { LockTimeout = TimeSpan.FromMilliseconds(100) },
            new MongoDbSubscriberOptions(),
            NullLogger.Instance,
            MongoDbSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery(calls), CancellationToken.None);

        Assert.True(calls.Renew >= 2);
        Assert.Equal(1, calls.Ack);

        // The renewal loop stops with the handler; no further renewals happen afterwards.
        var renewalsAfterAck = calls.Renew;
        await Task.Delay(200);
        Assert.Equal(renewalsAfterAck, calls.Renew);
    }

    [Fact]
    public async Task AckAfterHandlerCompletes_LeaseFenceLost_StopsRenewingAndKeepsProcessing()
    {
        var calls = new Calls { RenewResult = false };
        var dispatcher = new MongoDbMessageDispatcher(
            async (_, _) =>
            {
                while (Volatile.Read(ref calls.Renew) < 1)
                    await Task.Delay(10);
                await Task.Delay(250); // long enough for several more beats if the loop kept going
            },
            new MongoDbAsyncResponseTransportOptions { LockTimeout = TimeSpan.FromMilliseconds(100) },
            new MongoDbSubscriberOptions(),
            NullLogger.Instance,
            MongoDbSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery(calls), CancellationToken.None);

        // The fence was lost on the first beat: the loop stops instead of hammering the store, and
        // the handler still completes with the fenced ack no-oping server-side.
        Assert.Equal(1, calls.Renew);
        Assert.Equal(1, calls.Ack);
    }

    [Fact]
    public async Task DisposeAsync_QueuedButUnstartedDocuments_AreAttemptedAndSurfacedInsteadOfDropped()
    {
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var failures = 0;
        var runningCalls = new Calls();
        var queuedCalls = new Calls();
        var subscriberOptions = new MongoDbSubscriberOptions
        {
            OnBackgroundFailure = _ =>
            {
                Interlocked.Increment(ref failures);
                return ValueTask.CompletedTask;
            }
        }.UseAckAfterEnqueue(1, 8, TimeSpan.FromMilliseconds(50));
        var dispatcher = new MongoDbMessageDispatcher(
            async (_, cancellationToken) =>
            {
                started.TrySetResult();
                await Task.Delay(TimeSpan.FromSeconds(30), cancellationToken);
            },
            new MongoDbAsyncResponseTransportOptions(),
            subscriberOptions,
            NullLogger.Instance,
            MongoDbSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery(runningCalls), CancellationToken.None);
        await started.Task.WaitAsync(TimeSpan.FromSeconds(2)); // worker is blocked in the handler
        await dispatcher.HandleAsync(Delivery(queuedCalls), CancellationToken.None); // already ACKed, waiting in queue

        await dispatcher.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(2));

        // The hard stop must not silently drop the already-ACKed queued document: it is attempted
        // with the cancelled drain token, dead-lettered, and surfaced via OnBackgroundFailure — same
        // as the one that was mid-handler.
        await WaitUntilAsync(() => Volatile.Read(ref failures) == 2);
        Assert.Equal(1, runningCalls.DeadLetter);
        Assert.Equal(1, queuedCalls.DeadLetter);
    }

    private static MongoDbTransportDelivery Delivery(
        Calls calls,
        int attempt = 1,
        IReadOnlyDictionary<string, string>? headers = null)
        => new(
            Guid.NewGuid(),
            "worker",
            "{}",
            headers ?? new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase),
            attempt,
            () =>
            {
                if (calls.AckFailure is { } ackFailure)
                    throw ackFailure;

                calls.Ack++;
                return ValueTask.CompletedTask;
            },
            _ =>
            {
                calls.Nak++;
                calls.LastNakDelay = _;
                return ValueTask.CompletedTask;
            },
            (_, deleteOriginal, _) =>
            {
                calls.DeadLetter++;
                calls.DeleteOriginalOnDeadLetter = deleteOriginal;
                calls.DeadLettered.TrySetResult();
                return ValueTask.FromResult(calls.DeadLetterResult);
            },
            () =>
            {
                calls.Renew++;
                return ValueTask.FromResult(calls.RenewResult);
            });

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        while (!condition())
        {
            cts.Token.ThrowIfCancellationRequested();
            await Task.Delay(20, cts.Token);
        }
    }

    private sealed class Calls
    {
        public Calls()
        {
            DeadLetterResult = true;
            RenewResult = true;
        }

        public int Handler;
        public int Ack;
        public int Nak;
        public int DeadLetter;
        public int Renew;
        public Exception? AckFailure;
        public TaskCompletionSource DeadLettered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public bool DeleteOriginalOnDeadLetter;
        public bool DeadLetterResult;
        public bool RenewResult;
        public TimeSpan LastNakDelay;
    }
}
