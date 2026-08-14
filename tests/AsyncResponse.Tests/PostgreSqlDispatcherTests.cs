using AsyncResponse.Transports.PostgreSQL;
using Microsoft.Extensions.Logging.Abstractions;
using System.Diagnostics;
using Xunit;

namespace AsyncResponse.Tests;

public sealed class PostgreSqlDispatcherTests
{
    [Fact]
    public async Task AckAfterHandlerCompletes_AcksOnlyAfterSuccessfulHandler()
    {
        var calls = new Calls();
        var dispatcher = new PostgreSqlMessageDispatcher(
            (_, _) =>
            {
                calls.Handler++;
                Assert.Equal(0, calls.Ack);
                return Task.CompletedTask;
            },
            new PostgreSqlAsyncResponseTransportOptions(),
            new PostgreSqlSubscriberOptions(),
            NullLogger.Instance,
            PostgreSqlSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery(calls), CancellationToken.None);

        Assert.Equal(1, calls.Handler);
        Assert.Equal(1, calls.Ack);
        Assert.Equal(0, calls.Nak);
        Assert.Equal(0, calls.DeadLetter);
    }

    [Fact]
    public async Task HandlerExecution_EmitsPostgreSqlReceiveSpanWithMessagingTags()
    {
        using var collector = new AsyncResponseActivityCollector();
        var options = new PostgreSqlAsyncResponseTransportOptions();
        var id = Guid.NewGuid();
        var headers = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase) { [options.CorrelationIdHeader] = "corr-pg" };
        var delivery = new PostgreSqlTransportDelivery(
            id,
            "worker",
            "{}",
            headers,
            1,
            () => ValueTask.CompletedTask,
            _ => ValueTask.CompletedTask,
            (_, _, _) => ValueTask.FromResult(true),
            () => ValueTask.FromResult(true));
        var dispatcher = new PostgreSqlMessageDispatcher(
            (_, _) => Task.CompletedTask,
            options,
            new PostgreSqlSubscriberOptions(),
            NullLogger.Instance,
            PostgreSqlSubscriberRole.Worker);

        await dispatcher.HandleAsync(delivery, CancellationToken.None);

        var activity = collector.Single("asyncresponse.postgresql.receive", "asyncresponse.transport", "postgresql");
        Assert.Equal(ActivityKind.Consumer, activity.Kind);
        Assert.Equal("Worker", AsyncResponseActivityCollector.Tag(activity, "asyncresponse.postgresql.role"));
        Assert.Equal(nameof(PostgreSqlAckMode.AckAfterHandlerCompletes), AsyncResponseActivityCollector.Tag(activity, "asyncresponse.postgresql.ack_mode"));
        Assert.Equal("postgresql", AsyncResponseActivityCollector.Tag(activity, "messaging.system"));
        Assert.Equal("worker", AsyncResponseActivityCollector.Tag(activity, "messaging.destination.name"));
        Assert.Equal(id.ToString(), AsyncResponseActivityCollector.Tag(activity, "messaging.message.id"));
        Assert.Equal(1, AsyncResponseActivityCollector.Tag(activity, "messaging.message.delivery_attempt"));
        Assert.Equal("corr-pg", AsyncResponseActivityCollector.Tag(activity, "asyncresponse.correlation_id"));
    }

    [Fact]
    public async Task AckAfterEnqueue_BackgroundHandlerStillEmitsReceiveSpan()
    {
        using var collector = new AsyncResponseActivityCollector();
        var calls = new Calls();
        var handled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var dispatcher = new PostgreSqlMessageDispatcher(
            (_, _) =>
            {
                handled.SetResult();
                return Task.CompletedTask;
            },
            new PostgreSqlAsyncResponseTransportOptions(),
            new PostgreSqlSubscriberOptions().UseAckAfterEnqueue(1, 8, TimeSpan.FromSeconds(5)),
            NullLogger.Instance,
            PostgreSqlSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery(calls), CancellationToken.None);
        await handled.Task.WaitAsync(TimeSpan.FromSeconds(5));

        // Regression guard for the "both ACK modes emit the span" claim: the early-ACK path runs the
        // handler on a background worker, and a refactor that splits it from ExecuteHandlerAsync
        // would lose the span silently.
        await WaitUntilAsync(() => collector.Count("asyncresponse.postgresql.receive") == 1);
        var activity = collector.Single("asyncresponse.postgresql.receive", "asyncresponse.transport", "postgresql");
        Assert.Equal(ActivityKind.Consumer, activity.Kind);
        Assert.Equal(nameof(PostgreSqlAckMode.AckAfterEnqueue), AsyncResponseActivityCollector.Tag(activity, "asyncresponse.postgresql.ack_mode"));
    }

    [Fact]
    public async Task HandlerFailure_MarksPostgreSqlReceiveSpanError()
    {
        using var collector = new AsyncResponseActivityCollector();
        var calls = new Calls();
        var dispatcher = new PostgreSqlMessageDispatcher(
            (_, _) => throw new InvalidOperationException("boom"),
            new PostgreSqlAsyncResponseTransportOptions(),
            new PostgreSqlSubscriberOptions { MaxDeliveryAttempts = 2 },
            NullLogger.Instance,
            PostgreSqlSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery(calls, attempt: 1), CancellationToken.None);

        var activity = collector.Single("asyncresponse.postgresql.receive", "asyncresponse.transport", "postgresql");
        Assert.Equal(typeof(InvalidOperationException).FullName, AsyncResponseActivityCollector.Tag(activity, "error.type"));
        Assert.Equal(ActivityStatusCode.Error, activity.Status);
    }

    [Fact]
    public async Task AckAfterHandlerCompletes_NaksBeforeMaxAttempts()
    {
        var calls = new Calls();
        var dispatcher = new PostgreSqlMessageDispatcher(
            (_, _) => throw new InvalidOperationException("boom"),
            new PostgreSqlAsyncResponseTransportOptions(),
            new PostgreSqlSubscriberOptions { MaxDeliveryAttempts = 2 },
            NullLogger.Instance,
            PostgreSqlSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery(calls, attempt: 1), CancellationToken.None);

        Assert.Equal(0, calls.Ack);
        Assert.Equal(1, calls.Nak);
        Assert.Equal(0, calls.DeadLetter);
    }

    [Fact]
    public async Task AckAfterHandlerCompletes_DeadLettersAtMaxAttempts()
    {
        var calls = new Calls();
        var dispatcher = new PostgreSqlMessageDispatcher(
            (_, _) => throw new InvalidOperationException("boom"),
            new PostgreSqlAsyncResponseTransportOptions(),
            new PostgreSqlSubscriberOptions { MaxDeliveryAttempts = 2 },
            NullLogger.Instance,
            PostgreSqlSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery(calls, attempt: 2), CancellationToken.None);

        Assert.Equal(0, calls.Nak);
        Assert.Equal(1, calls.DeadLetter);
        Assert.True(calls.DeleteOriginalOnDeadLetter);
    }

    [Fact]
    public async Task AckAfterHandlerCompletes_NaksWhenDeadLetterPublishFails()
    {
        var calls = new Calls { DeadLetterResult = false };
        var dispatcher = new PostgreSqlMessageDispatcher(
            (_, _) => throw new InvalidOperationException("boom"),
            new PostgreSqlAsyncResponseTransportOptions(),
            new PostgreSqlSubscriberOptions { MaxDeliveryAttempts = 1 },
            NullLogger.Instance,
            PostgreSqlSubscriberRole.Worker);

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
        var dispatcher = new PostgreSqlMessageDispatcher(
            (_, _) => throw new InvalidOperationException("boom"),
            new PostgreSqlAsyncResponseTransportOptions(),
            new PostgreSqlSubscriberOptions { MaxDeliveryAttempts = 0 },
            NullLogger.Instance,
            PostgreSqlSubscriberRole.ResponseIngress);

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
        await using var dispatcher = new PostgreSqlMessageDispatcher(
            (_, _) =>
            {
                calls.Handler++;
                handled.SetResult();
                return Task.CompletedTask;
            },
            new PostgreSqlAsyncResponseTransportOptions(),
            new PostgreSqlSubscriberOptions().UseAckAfterEnqueue(1, 8, TimeSpan.FromSeconds(5)),
            NullLogger.Instance,
            PostgreSqlSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery(calls), CancellationToken.None);

        // AckAfterEnqueue acks the row as part of accepting it into the background queue, so it is
        // already acknowledged the moment HandleAsync returns — without waiting for the handler. The
        // handler runs on the background worker and may execute concurrently with the ack, so the test
        // must not assert their relative ordering (asserting calls.Ack inside the handler was the
        // source of an intermittent timeout when the handler won that race).
        Assert.Equal(1, calls.Ack);

        await handled.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(1, calls.Handler);
    }

    [Fact]
    public async Task AckAfterEnqueue_BackgroundFailure_DeadLettersWithoutDeletingOriginal_AndInvokesCallback()
    {
        var calls = new Calls();
        PostgreSqlBackgroundFailureContext? callback = null;
        var handled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var dispatcher = new PostgreSqlMessageDispatcher(
            (_, _) =>
            {
                handled.SetResult();
                throw new InvalidOperationException("background boom");
            },
            new PostgreSqlAsyncResponseTransportOptions(),
            new PostgreSqlSubscriberOptions
            {
                OnBackgroundFailure = context =>
                {
                    callback = context;
                    return ValueTask.CompletedTask;
                }
            }.UseAckAfterEnqueue(1, 8, TimeSpan.FromSeconds(5)),
            NullLogger.Instance,
            PostgreSqlSubscriberRole.Worker);

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
        await using var dispatcher = new PostgreSqlMessageDispatcher(
            (_, _) =>
            {
                handled.SetResult();
                throw new InvalidOperationException("background boom");
            },
            new PostgreSqlAsyncResponseTransportOptions(),
            new PostgreSqlSubscriberOptions
            {
                OnBackgroundFailure = _ => throw new InvalidOperationException("callback boom")
            }.UseAckAfterEnqueue(1, 8, TimeSpan.FromSeconds(5)),
            NullLogger.Instance,
            PostgreSqlSubscriberRole.Worker);

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
        await using var dispatcher = new PostgreSqlMessageDispatcher(
            (_, _) =>
            {
                entered.Set();
                release.Wait(TimeSpan.FromSeconds(30));
                return Task.CompletedTask;
            },
            new PostgreSqlAsyncResponseTransportOptions(),
            new PostgreSqlSubscriberOptions().UseAckAfterEnqueue(1, 1, TimeSpan.FromSeconds(5)),
            NullLogger.Instance,
            PostgreSqlSubscriberRole.Worker);

        // The gated worker holds the first row; the second fills the capacity-1 queue; the third overflows.
        await dispatcher.HandleAsync(Delivery(calls), CancellationToken.None);
        Assert.True(entered.Wait(TimeSpan.FromSeconds(30)));
        await dispatcher.HandleAsync(Delivery(calls), CancellationToken.None);
        var overflow = dispatcher.HandleAsync(Delivery(calls), CancellationToken.None);

        // The overflow claim must park — pausing the claim loop, which is the actual
        // backpressure — not NAK: the subscriber loop treats every claimed row as progress, so a
        // NAK-on-full is re-claimed immediately and spins at full database rate, burning an
        // attempt per lap. No ACK either until the row is actually enqueued.
        Assert.False(overflow.IsCompleted);
        Assert.Equal(0, calls.Nak);
        Assert.Equal(2, calls.Ack);

        release.Set();
        await overflow.WaitAsync(TimeSpan.FromSeconds(30));

        Assert.Equal(0, calls.Nak);
        Assert.Equal(3, calls.Ack); // every claimed row ACKed exactly once
    }

    [Fact]
    public async Task AckAfterEnqueue_DisposeWhileParkedOnFullQueue_NaksTheParkedRowOnce()
    {
        var calls = new Calls();
        using var entered = new ManualResetEventSlim();
        using var release = new ManualResetEventSlim();
        var dispatcher = new PostgreSqlMessageDispatcher(
            (_, _) =>
            {
                entered.Set();
                release.Wait(TimeSpan.FromSeconds(30));
                return Task.CompletedTask;
            },
            new PostgreSqlAsyncResponseTransportOptions(),
            new PostgreSqlSubscriberOptions().UseAckAfterEnqueue(1, 1, TimeSpan.FromSeconds(5)),
            NullLogger.Instance,
            PostgreSqlSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery(calls), CancellationToken.None);
        Assert.True(entered.Wait(TimeSpan.FromSeconds(30)));
        await dispatcher.HandleAsync(Delivery(calls), CancellationToken.None);
        var overflow = dispatcher.HandleAsync(Delivery(calls), CancellationToken.None);
        Assert.False(overflow.IsCompleted);

        // Draining completes the queue writer; the parked write must fall back to one NAK — the
        // row was never enqueued or ACKed — instead of throwing out of the claim loop.
        var disposing = dispatcher.DisposeAsync().AsTask();
        await overflow.WaitAsync(TimeSpan.FromSeconds(5));
        release.Set();
        await disposing.WaitAsync(TimeSpan.FromSeconds(30));

        Assert.Equal(1, calls.Nak);
        Assert.Equal(2, calls.Ack); // the two enqueued rows; never the parked one
    }

    [Fact]
    public async Task Handler_ShutdownCancellation_PropagatesWithoutSettling()
    {
        // A handler cancelled by host shutdown is not a handler failure: routing it through
        // HandleFailureAsync NAKs (burning an attempt) or, at the cap, dead-letters healthy work.
        // The cancellation must propagate with the row unsettled — the claim's lease lapses and
        // at-least-once redelivery applies after restart.
        var calls = new Calls();
        var stopping = new CancellationToken(canceled: true);
        await using var dispatcher = new PostgreSqlMessageDispatcher(
            (_, token) => Task.FromException(new OperationCanceledException(token)),
            new PostgreSqlAsyncResponseTransportOptions(),
            new PostgreSqlSubscriberOptions(),
            NullLogger.Instance,
            PostgreSqlSubscriberRole.Worker);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => dispatcher.HandleAsync(Delivery(calls), stopping));

        Assert.Equal(0, calls.Ack);
        Assert.Equal(0, calls.Nak);
        Assert.Equal(0, calls.DeadLetter);
    }

    [Fact]
    public async Task AckAfterEnqueue_AckFailure_DoesNotTearDownTheSubscriber()
    {
        // Once TryWrite succeeds the row belongs to a background worker: an ACK failure after
        // that must not escape HandleAsync — unwinding tears down the subscriber (draining the
        // worker, which RUNS the handler) while the un-ACKed row's lease lapses, so a rebuilt
        // subscriber claims and runs the same job a second time. Same rule as the post-handler
        // ACK: swallow, log, let at-least-once redelivery apply.
        var calls = new Calls { AckFailure = new InvalidOperationException("ack boom") };
        var handled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var dispatcher = new PostgreSqlMessageDispatcher(
            (_, _) =>
            {
                handled.TrySetResult();
                return Task.CompletedTask;
            },
            new PostgreSqlAsyncResponseTransportOptions(),
            new PostgreSqlSubscriberOptions().UseAckAfterEnqueue(1, 8, TimeSpan.FromSeconds(5)),
            NullLogger.Instance,
            PostgreSqlSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery(calls), CancellationToken.None); // must not throw

        await handled.Task.WaitAsync(TimeSpan.FromSeconds(5)); // the enqueued row still executes
        Assert.Equal(0, calls.Ack);
        Assert.Equal(0, calls.Nak);
    }

    [Fact]
    public async Task AckAfterEnqueue_BackgroundFailureWithoutCallback_DeadLetters()
    {
        var calls = new Calls();
        var handled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var dispatcher = new PostgreSqlMessageDispatcher(
            (_, _) =>
            {
                handled.SetResult();
                throw new InvalidOperationException("background boom");
            },
            new PostgreSqlAsyncResponseTransportOptions(),
            new PostgreSqlSubscriberOptions().UseAckAfterEnqueue(1, 8, TimeSpan.FromSeconds(5)),
            NullLogger.Instance,
            PostgreSqlSubscriberRole.ResponseIngress);

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
        await using var dispatcher = new PostgreSqlMessageDispatcher(
            (_, _) =>
            {
                handled.SetResult();
                throw new OperationCanceledException();
            },
            new PostgreSqlAsyncResponseTransportOptions(),
            new PostgreSqlSubscriberOptions().UseAckAfterEnqueue(1, 8, TimeSpan.FromSeconds(5)),
            NullLogger.Instance,
            PostgreSqlSubscriberRole.Worker);

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
        var dispatcher = new PostgreSqlMessageDispatcher(
            async (_, _) =>
            {
                handlerEntered.SetResult();
                await release.Task;
            },
            new PostgreSqlAsyncResponseTransportOptions(),
            new PostgreSqlSubscriberOptions().UseAckAfterEnqueue(1, 8, TimeSpan.FromMilliseconds(1)),
            NullLogger.Instance,
            PostgreSqlSubscriberRole.Worker);

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
        var dispatcher = new PostgreSqlMessageDispatcher(
            async (_, cancellationToken) =>
            {
                started.TrySetResult();
                await Task.Delay(TimeSpan.FromSeconds(30), cancellationToken);
            },
            new PostgreSqlAsyncResponseTransportOptions(),
            new PostgreSqlSubscriberOptions().UseAckAfterEnqueue(1, 8, TimeSpan.FromMilliseconds(50)),
            NullLogger.Instance,
            PostgreSqlSubscriberRole.Worker);

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
        var ex = Assert.Throws<InvalidOperationException>(() => new PostgreSqlMessageDispatcher(
            (_, _) => Task.CompletedTask,
            new PostgreSqlAsyncResponseTransportOptions(),
            new PostgreSqlSubscriberOptions { AckMode = PostgreSqlAckMode.AckAfterEnqueue },
            NullLogger.Instance,
            PostgreSqlSubscriberRole.Worker));

        Assert.Contains(nameof(PostgreSqlSubscriberOptions.BackgroundWorkerCount), ex.Message);
    }

    [Fact]
    public void ValidateSubscriber_DrainBudgetExceedingHostShutdownBudget_Throws()
    {
        var options = new PostgreSqlAsyncResponseTransportOptions
        {
            ShutdownTimeout = TimeSpan.FromSeconds(20),
            HostShutdownTimeout = TimeSpan.FromSeconds(25)
        };
        var subscriber = new PostgreSqlSubscriberOptions().UseAckAfterEnqueue(1, 8, TimeSpan.FromSeconds(10));

        var ex = Assert.Throws<InvalidOperationException>(() =>
            PostgreSqlTransportOptionsValidator.ValidateSubscriber(options, subscriber, "Worker"));

        Assert.Contains(nameof(PostgreSqlAsyncResponseTransportOptions.HostShutdownTimeout), ex.Message);

        // A null host budget or an awaiting-mode subscriber skips the check.
        options.HostShutdownTimeout = null;
        PostgreSqlTransportOptionsValidator.ValidateSubscriber(options, subscriber, "Worker");
        PostgreSqlTransportOptionsValidator.ValidateSubscriber(
            new PostgreSqlAsyncResponseTransportOptions { HostShutdownTimeout = TimeSpan.FromSeconds(1) },
            new PostgreSqlSubscriberOptions(),
            "Worker");
    }

    [Fact]
    public void ValidateSubscriber_NonPositiveHostShutdownTimeout_Throws()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            PostgreSqlTransportOptionsValidator.ValidateSubscriber(
                new PostgreSqlAsyncResponseTransportOptions { HostShutdownTimeout = TimeSpan.Zero },
                new PostgreSqlSubscriberOptions().UseAckAfterEnqueue(1, 8),
                "Worker"));

        Assert.Contains(nameof(PostgreSqlAsyncResponseTransportOptions.HostShutdownTimeout), ex.Message);
    }

    [Fact]
    public void ValidateSubscriber_DocumentedEarlyAckDefaults_Pass()
    {
        // Regression: the documented two-arg early-ACK opt-in with stock defaults
        // (5s ShutdownTimeout + 20s BackgroundDrainTimeout vs HostShutdownTimeout 30s)
        // must not fail startup.
        PostgreSqlTransportOptionsValidator.ValidateSubscriber(
            new PostgreSqlAsyncResponseTransportOptions(),
            new PostgreSqlSubscriberOptions().UseAckAfterEnqueue(4, 256),
            "Worker");
    }

    [Fact]
    public async Task AckAfterHandlerCompletes_SlowHandler_RenewsLeaseUntilHandlerFinishes()
    {
        var calls = new Calls();
        var dispatcher = new PostgreSqlMessageDispatcher(
            async (_, _) =>
            {
                while (Volatile.Read(ref calls.Renew) < 2)
                    await Task.Delay(10);
            },
            new PostgreSqlAsyncResponseTransportOptions { LockTimeout = TimeSpan.FromMilliseconds(100) },
            new PostgreSqlSubscriberOptions(),
            NullLogger.Instance,
            PostgreSqlSubscriberRole.Worker);

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
        var dispatcher = new PostgreSqlMessageDispatcher(
            async (_, _) =>
            {
                while (Volatile.Read(ref calls.Renew) < 1)
                    await Task.Delay(10);
                await Task.Delay(250); // long enough for several more beats if the loop kept going
            },
            new PostgreSqlAsyncResponseTransportOptions { LockTimeout = TimeSpan.FromMilliseconds(100) },
            new PostgreSqlSubscriberOptions(),
            NullLogger.Instance,
            PostgreSqlSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery(calls), CancellationToken.None);

        // The fence was lost on the first beat: the loop stops instead of hammering the store, and
        // the handler still completes with the fenced ack no-oping server-side.
        Assert.Equal(1, calls.Renew);
        Assert.Equal(1, calls.Ack);
    }

    [Fact]
    public async Task AckAfterHandlerCompletes_FastHandler_NeverRenews()
    {
        var calls = new Calls();
        var dispatcher = new PostgreSqlMessageDispatcher(
            (_, _) => Task.CompletedTask,
            new PostgreSqlAsyncResponseTransportOptions(),
            new PostgreSqlSubscriberOptions(),
            NullLogger.Instance,
            PostgreSqlSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery(calls), CancellationToken.None);

        Assert.Equal(0, calls.Renew);
        Assert.Equal(1, calls.Ack);
    }

    [Fact]
    public async Task AckAfterEnqueue_BackgroundFailureWithFailedDeadLetterWrite_StillInvokesCallback()
    {
        var calls = new Calls { DeadLetterResult = false };
        var failureReported = new TaskCompletionSource<PostgreSqlBackgroundFailureContext>(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var dispatcher = new PostgreSqlMessageDispatcher(
            (_, _) => throw new InvalidOperationException("background boom"),
            new PostgreSqlAsyncResponseTransportOptions(),
            new PostgreSqlSubscriberOptions
            {
                OnBackgroundFailure = context =>
                {
                    failureReported.TrySetResult(context);
                    return ValueTask.CompletedTask;
                }
            }.UseAckAfterEnqueue(1, 8, TimeSpan.FromSeconds(5)),
            NullLogger.Instance,
            PostgreSqlSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery(calls), CancellationToken.None);

        // A failing dead-letter write must not break the failure surfacing: the callback still runs
        // (and the dispatcher logs an explicit error for the lost DLQ write).
        await failureReported.Task.WaitAsync(TimeSpan.FromSeconds(2));
        Assert.Equal(1, calls.DeadLetter);
    }

    [Fact]
    public async Task DisposeAsync_QueuedButUnstartedRows_AreAttemptedAndSurfacedInsteadOfDropped()
    {
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var failures = 0;
        var runningCalls = new Calls();
        var queuedCalls = new Calls();
        var subscriberOptions = new PostgreSqlSubscriberOptions
        {
            OnBackgroundFailure = _ =>
            {
                Interlocked.Increment(ref failures);
                return ValueTask.CompletedTask;
            }
        }.UseAckAfterEnqueue(1, 8, TimeSpan.FromMilliseconds(50));
        var dispatcher = new PostgreSqlMessageDispatcher(
            async (_, cancellationToken) =>
            {
                started.TrySetResult();
                await Task.Delay(TimeSpan.FromSeconds(30), cancellationToken);
            },
            new PostgreSqlAsyncResponseTransportOptions(),
            subscriberOptions,
            NullLogger.Instance,
            PostgreSqlSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery(runningCalls), CancellationToken.None);
        await started.Task.WaitAsync(TimeSpan.FromSeconds(2)); // worker is blocked in the handler
        await dispatcher.HandleAsync(Delivery(queuedCalls), CancellationToken.None); // already ACKed, waiting in queue

        await dispatcher.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(2));

        // The hard stop must not silently drop the already-ACKed queued row: it is attempted with
        // the cancelled drain token, dead-lettered, and surfaced via OnBackgroundFailure — same as
        // the one that was mid-handler.
        await WaitUntilAsync(() => Volatile.Read(ref failures) == 2);
        Assert.Equal(1, runningCalls.DeadLetter);
        Assert.Equal(1, queuedCalls.DeadLetter);
    }

    [Fact]
    public async Task AckAfterEnqueue_QueueFullPark_RenewsTheClaimLeaseWhileParked()
    {
        // Regression (r23): the r22 park-on-full replaced NAK-on-full, but in early-ACK mode the
        // inline path's lease heartbeat never runs — the parked delivery's claim silently lapsed
        // at LockTimeout and a competing subscriber re-claimed and ran the same row concurrently.
        // The park must renew the lease for its whole duration, exactly like the inline path.
        var handlerStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseHandler = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var dispatcher = new PostgreSqlMessageDispatcher(
            (_, _) =>
            {
                handlerStarted.TrySetResult();
                return releaseHandler.Task;
            },
            new PostgreSqlAsyncResponseTransportOptions { LockTimeout = TimeSpan.FromMilliseconds(100) },
            new PostgreSqlSubscriberOptions().UseAckAfterEnqueue(1, 1, TimeSpan.FromSeconds(5)),
            NullLogger.Instance,
            PostgreSqlSubscriberRole.Worker);

        var blocking = new Calls();
        var queued = new Calls();
        var parked = new Calls();

        // First delivery occupies the single worker; second fills the queue slot; third parks.
        await dispatcher.HandleAsync(Delivery(blocking), CancellationToken.None);
        await handlerStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await dispatcher.HandleAsync(Delivery(queued), CancellationToken.None);
        var parkTask = dispatcher.HandleAsync(Delivery(parked), CancellationToken.None);
        Assert.False(parkTask.IsCompleted);

        // The heartbeat beats at LockTimeout/2 (50 ms here): the parked claim must be renewed
        // while the park lasts. On the old code no renewal ever fires and this wait times out.
        await WaitUntilAsync(() => Volatile.Read(ref parked.Renew) >= 2);
        Assert.False(parkTask.IsCompleted);
        Assert.Equal(0, parked.Ack);
        Assert.Equal(0, parked.Nak);

        releaseHandler.TrySetResult();
        await parkTask.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Equal(1, parked.Ack);
        Assert.Equal(0, parked.Nak);

        // The heartbeat stops with the park: no further renewals accrue afterwards.
        var renewalsAfterAck = Volatile.Read(ref parked.Renew);
        await Task.Delay(200);
        Assert.Equal(renewalsAfterAck, Volatile.Read(ref parked.Renew));
    }

    private static PostgreSqlTransportDelivery Delivery(
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
