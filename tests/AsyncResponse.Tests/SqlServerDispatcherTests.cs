using AsyncResponse.Transports.SqlServer;
using Microsoft.Extensions.Logging.Abstractions;
using System.Diagnostics;
using Xunit;

namespace AsyncResponse.Tests;

public sealed class SqlServerDispatcherTests
{
    [Fact]
    public async Task HandlerExecution_EmitsSqlServerReceiveSpanWithMessagingTags()
    {
        using var collector = new AsyncResponseActivityCollector();
        var options = Options();
        var calls = new Calls();
        var headers = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase) { [options.CorrelationIdHeader] = "corr-sql" };
        var dispatcher = new SqlServerMessageDispatcher(
            (_, _) => Task.CompletedTask,
            options,
            new SqlServerSubscriberOptions(),
            NullLogger.Instance,
            SqlServerSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery(calls, headers: headers), CancellationToken.None);

        // The span follows the per-broker convention (asyncresponse.<broker>.receive + role/ack_mode
        // tags) instead of the old asyncresponse.worker.receive/asyncresponse.response.receive names.
        var activity = collector.Single("asyncresponse.sqlserver.receive", "asyncresponse.transport", "sqlserver");
        Assert.Equal(ActivityKind.Consumer, activity.Kind);
        Assert.Equal("Worker", AsyncResponseActivityCollector.Tag(activity, "asyncresponse.sqlserver.role"));
        Assert.Equal(nameof(SqlServerAckMode.AckAfterHandlerCompletes), AsyncResponseActivityCollector.Tag(activity, "asyncresponse.sqlserver.ack_mode"));
        Assert.Equal("sqlserver", AsyncResponseActivityCollector.Tag(activity, "messaging.system"));
        Assert.Equal("worker", AsyncResponseActivityCollector.Tag(activity, "messaging.destination.name"));
        Assert.Equal(1, AsyncResponseActivityCollector.Tag(activity, "messaging.message.delivery_attempt"));
        Assert.Equal("corr-sql", AsyncResponseActivityCollector.Tag(activity, "asyncresponse.correlation_id"));
    }

    [Fact]
    public async Task AckAfterEnqueue_BackgroundHandlerStillEmitsReceiveSpan()
    {
        using var collector = new AsyncResponseActivityCollector();
        var calls = new Calls();
        var handled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var dispatcher = new SqlServerMessageDispatcher(
            (_, _) =>
            {
                handled.TrySetResult();
                return Task.CompletedTask;
            },
            Options(),
            new SqlServerSubscriberOptions().UseAckAfterEnqueue(1, 8, TimeSpan.FromSeconds(5)),
            NullLogger.Instance,
            SqlServerSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery(calls), CancellationToken.None);
        await handled.Task.WaitAsync(TimeSpan.FromSeconds(5));

        await WaitUntilAsync(() => collector.Count("asyncresponse.sqlserver.receive") == 1);
        var activity = collector.Single("asyncresponse.sqlserver.receive", "asyncresponse.transport", "sqlserver");
        Assert.Equal(ActivityKind.Consumer, activity.Kind);
        Assert.Equal(nameof(SqlServerAckMode.AckAfterEnqueue), AsyncResponseActivityCollector.Tag(activity, "asyncresponse.sqlserver.ack_mode"));
    }

    [Fact]
    public async Task AckAfterHandlerCompletes_AcksOnlyAfterSuccessfulHandler()
    {
        var calls = new Calls();
        var dispatcher = new SqlServerMessageDispatcher(
            (_, _) =>
            {
                calls.Handler++;
                Assert.Equal(0, calls.Ack);
                return Task.CompletedTask;
            },
            Options(),
            new SqlServerSubscriberOptions(),
            NullLogger.Instance,
            SqlServerSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery(calls), CancellationToken.None);

        Assert.Equal(1, calls.Handler);
        Assert.Equal(1, calls.Ack);
        Assert.Equal(0, calls.Nak);
        Assert.Equal(0, calls.DeadLetter);
    }

    [Fact]
    public async Task AckAfterHandlerCompletes_NaksBeforeMaxAttempts()
    {
        var calls = new Calls();
        var dispatcher = new SqlServerMessageDispatcher(
            (_, _) => throw new InvalidOperationException("boom"),
            Options(),
            new SqlServerSubscriberOptions { MaxDeliveryAttempts = 2 },
            NullLogger.Instance,
            SqlServerSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery(calls, attempt: 1), CancellationToken.None);

        Assert.Equal(0, calls.Ack);
        Assert.Equal(1, calls.Nak);
        Assert.Equal(0, calls.DeadLetter);
    }

    [Fact]
    public async Task AckAfterHandlerCompletes_DeadLettersAtMaxAttempts()
    {
        var calls = new Calls();
        var dispatcher = new SqlServerMessageDispatcher(
            (_, _) => throw new InvalidOperationException("boom"),
            Options(),
            new SqlServerSubscriberOptions { MaxDeliveryAttempts = 2 },
            NullLogger.Instance,
            SqlServerSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery(calls, attempt: 2), CancellationToken.None);

        Assert.Equal(0, calls.Nak);
        Assert.Equal(1, calls.DeadLetter);
        Assert.True(calls.DeleteOriginalOnDeadLetter);
    }

    [Fact]
    public async Task AckAfterHandlerCompletes_NaksWhenDeadLetterPublishFails()
    {
        var calls = new Calls { DeadLetterResult = false };
        var dispatcher = new SqlServerMessageDispatcher(
            (_, _) => throw new InvalidOperationException("boom"),
            Options(),
            new SqlServerSubscriberOptions { MaxDeliveryAttempts = 1 },
            NullLogger.Instance,
            SqlServerSubscriberRole.Worker);

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
        var dispatcher = new SqlServerMessageDispatcher(
            (_, _) => throw new InvalidOperationException("boom"),
            Options(),
            new SqlServerSubscriberOptions { MaxDeliveryAttempts = 0 },
            NullLogger.Instance,
            SqlServerSubscriberRole.ResponseIngress);

        await dispatcher.HandleAsync(Delivery(calls, attempt: 99), CancellationToken.None);

        Assert.Equal(0, calls.Ack);
        Assert.Equal(1, calls.Nak);
        Assert.Equal(0, calls.DeadLetter);
    }

    [Fact]
    public async Task AckAfterEnqueue_AcksOnEnqueue_WithoutWaitingForHandler()
    {
        var calls = new Calls();
        var handled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var dispatcher = new SqlServerMessageDispatcher(
            (_, _) =>
            {
                calls.Handler++;
                handled.SetResult();
                return Task.CompletedTask;
            },
            Options(),
            new SqlServerSubscriberOptions().UseAckAfterEnqueue(1, 8, TimeSpan.FromSeconds(5)),
            NullLogger.Instance,
            SqlServerSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery(calls), CancellationToken.None);

        // AckAfterEnqueue acks the row as part of accepting it into the background queue, so it is
        // already acknowledged the moment HandleAsync returns — without waiting for the handler. The
        // handler runs on the background worker and may execute concurrently with the ack, so the test
        // must not assert their relative ordering.
        Assert.Equal(1, calls.Ack);

        await handled.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(1, calls.Handler);
    }

    [Fact]
    public async Task AckAfterEnqueue_BackgroundFailure_DeadLettersWithoutDeletingOriginal_AndInvokesCallback()
    {
        var calls = new Calls();
        SqlServerBackgroundFailureContext? callback = null;
        var handled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var dispatcher = new SqlServerMessageDispatcher(
            (_, _) =>
            {
                handled.SetResult();
                throw new InvalidOperationException("background boom");
            },
            Options(),
            new SqlServerSubscriberOptions
            {
                OnBackgroundFailure = context =>
                {
                    callback = context;
                    return ValueTask.CompletedTask;
                }
            }.UseAckAfterEnqueue(1, 8, TimeSpan.FromSeconds(5)),
            NullLogger.Instance,
            SqlServerSubscriberRole.Worker);

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
        await using var dispatcher = new SqlServerMessageDispatcher(
            (_, _) =>
            {
                handled.SetResult();
                throw new InvalidOperationException("background boom");
            },
            Options(),
            new SqlServerSubscriberOptions
            {
                OnBackgroundFailure = _ => throw new InvalidOperationException("callback boom")
            }.UseAckAfterEnqueue(1, 8, TimeSpan.FromSeconds(5)),
            NullLogger.Instance,
            SqlServerSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery(calls), CancellationToken.None);

        await handled.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await WaitUntilAsync(() => calls.DeadLetter == 1);
        Assert.Equal(1, calls.DeadLetter);
    }

    [Fact]
    public async Task AckAfterEnqueue_Overflow_ReleasesRowForRetry()
    {
        var calls = new Calls();
        using var entered = new ManualResetEventSlim();
        using var release = new ManualResetEventSlim();
        await using var dispatcher = new SqlServerMessageDispatcher(
            (_, _) =>
            {
                entered.Set();
                release.Wait(TimeSpan.FromSeconds(5));
                return Task.CompletedTask;
            },
            Options(),
            new SqlServerSubscriberOptions().UseAckAfterEnqueue(1, 1, TimeSpan.FromSeconds(5)),
            NullLogger.Instance,
            SqlServerSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery(calls), CancellationToken.None);
        Assert.True(entered.Wait(TimeSpan.FromSeconds(5)));
        await dispatcher.HandleAsync(Delivery(calls), CancellationToken.None);
        await dispatcher.HandleAsync(Delivery(calls), CancellationToken.None);

        await WaitUntilAsync(() => calls.Nak == 1);
        Assert.Equal(TimeSpan.FromSeconds(5), calls.LastNakDelay);

        release.Set();
    }

    [Fact]
    public async Task AckAfterEnqueue_BackgroundFailureWithoutCallback_DeadLetters()
    {
        var calls = new Calls();
        var handled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var dispatcher = new SqlServerMessageDispatcher(
            (_, _) =>
            {
                handled.SetResult();
                throw new InvalidOperationException("background boom");
            },
            Options(),
            new SqlServerSubscriberOptions().UseAckAfterEnqueue(1, 8, TimeSpan.FromSeconds(5)),
            NullLogger.Instance,
            SqlServerSubscriberRole.ResponseIngress);

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
        await using var dispatcher = new SqlServerMessageDispatcher(
            (_, _) =>
            {
                handled.SetResult();
                throw new OperationCanceledException();
            },
            Options(),
            new SqlServerSubscriberOptions().UseAckAfterEnqueue(1, 8, TimeSpan.FromSeconds(5)),
            NullLogger.Instance,
            SqlServerSubscriberRole.Worker);

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
        var dispatcher = new SqlServerMessageDispatcher(
            async (_, _) =>
            {
                handlerEntered.SetResult();
                await release.Task;
            },
            Options(),
            new SqlServerSubscriberOptions().UseAckAfterEnqueue(1, 8, TimeSpan.FromMilliseconds(1)),
            NullLogger.Instance,
            SqlServerSubscriberRole.Worker);

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
        var dispatcher = new SqlServerMessageDispatcher(
            async (_, cancellationToken) =>
            {
                started.TrySetResult();
                await Task.Delay(TimeSpan.FromSeconds(30), cancellationToken);
            },
            Options(),
            new SqlServerSubscriberOptions().UseAckAfterEnqueue(1, 8, TimeSpan.FromMilliseconds(50)),
            NullLogger.Instance,
            SqlServerSubscriberRole.Worker);

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
        var ex = Assert.Throws<InvalidOperationException>(() => new SqlServerMessageDispatcher(
            (_, _) => Task.CompletedTask,
            Options(),
            new SqlServerSubscriberOptions { AckMode = SqlServerAckMode.AckAfterEnqueue },
            NullLogger.Instance,
            SqlServerSubscriberRole.Worker));

        Assert.Contains(nameof(SqlServerSubscriberOptions.BackgroundWorkerCount), ex.Message);
    }

    [Fact]
    public void ValidateSubscriber_DrainBudgetExceedingHostShutdownBudget_Throws()
    {
        var options = Options();
        options.HostShutdownTimeout = TimeSpan.FromSeconds(25);
        var subscriber = new SqlServerSubscriberOptions().UseAckAfterEnqueue(1, 8, TimeSpan.FromSeconds(26));

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlServerTransportOptionsValidator.ValidateSubscriber(options, subscriber, "Worker"));

        Assert.Contains(nameof(SqlServerAsyncResponseTransportOptions.HostShutdownTimeout), ex.Message);

        // A null host budget or an awaiting-mode subscriber skips the check.
        options.HostShutdownTimeout = null;
        SqlServerTransportOptionsValidator.ValidateSubscriber(options, subscriber, "Worker");
        var awaiting = Options();
        awaiting.HostShutdownTimeout = TimeSpan.FromSeconds(1);
        SqlServerTransportOptionsValidator.ValidateSubscriber(awaiting, new SqlServerSubscriberOptions(), "Worker");
    }

    [Fact]
    public void ValidateSubscriber_DocumentedEarlyAckDefaults_Pass()
    {
        // Regression: the documented two-arg early-ACK opt-in with stock defaults
        // (BackgroundDrainTimeout 20s vs HostShutdownTimeout 30s) must not fail startup.
        SqlServerTransportOptionsValidator.ValidateSubscriber(
            Options(),
            new SqlServerSubscriberOptions().UseAckAfterEnqueue(4, 256),
            "Worker");
    }

    [Fact]
    public void ValidateSubscriber_NonPositiveHostShutdownTimeout_Throws()
    {
        var options = Options();
        options.HostShutdownTimeout = TimeSpan.Zero;

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlServerTransportOptionsValidator.ValidateSubscriber(
                options,
                new SqlServerSubscriberOptions().UseAckAfterEnqueue(1, 8),
                "Worker"));

        Assert.Contains(nameof(SqlServerAsyncResponseTransportOptions.HostShutdownTimeout), ex.Message);
    }

    [Fact]
    public async Task AckAfterHandlerCompletes_SlowHandler_RenewsLeaseUntilHandlerFinishes()
    {
        var calls = new Calls();
        var options = Options();
        options.LockTimeout = TimeSpan.FromMilliseconds(100);
        var dispatcher = new SqlServerMessageDispatcher(
            async (_, _) =>
            {
                while (Volatile.Read(ref calls.Renew) < 2)
                    await Task.Delay(10);
            },
            options,
            new SqlServerSubscriberOptions(),
            NullLogger.Instance,
            SqlServerSubscriberRole.Worker);

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
        var options = Options();
        options.LockTimeout = TimeSpan.FromMilliseconds(100);
        var dispatcher = new SqlServerMessageDispatcher(
            async (_, _) =>
            {
                while (Volatile.Read(ref calls.Renew) < 1)
                    await Task.Delay(10);
                await Task.Delay(250); // long enough for several more beats if the loop kept going
            },
            options,
            new SqlServerSubscriberOptions(),
            NullLogger.Instance,
            SqlServerSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery(calls), CancellationToken.None);

        // The fence was lost on the first beat: the loop stops instead of hammering the store, and
        // the handler still completes with the fenced ack no-oping server-side.
        Assert.Equal(1, calls.Renew);
        Assert.Equal(1, calls.Ack);
    }

    [Fact]
    public async Task DisposeAsync_QueuedButUnstartedRows_AreAttemptedAndSurfacedInsteadOfDropped()
    {
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var failures = 0;
        var runningCalls = new Calls();
        var queuedCalls = new Calls();
        var subscriberOptions = new SqlServerSubscriberOptions
        {
            OnBackgroundFailure = _ =>
            {
                Interlocked.Increment(ref failures);
                return ValueTask.CompletedTask;
            }
        }.UseAckAfterEnqueue(1, 8, TimeSpan.FromMilliseconds(50));
        var dispatcher = new SqlServerMessageDispatcher(
            async (_, cancellationToken) =>
            {
                started.TrySetResult();
                await Task.Delay(TimeSpan.FromSeconds(30), cancellationToken);
            },
            Options(),
            subscriberOptions,
            NullLogger.Instance,
            SqlServerSubscriberRole.Worker);

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

    internal static SqlServerAsyncResponseTransportOptions Options()
        => new() { ConnectionString = "Server=localhost;Database=asyncresponse_tests;User ID=sa;Password=unused;TrustServerCertificate=True" };

    private static SqlServerTransportDelivery Delivery(
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
        public TaskCompletionSource DeadLettered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public bool DeleteOriginalOnDeadLetter;
        public bool DeadLetterResult;
        public bool RenewResult;
        public TimeSpan LastNakDelay;
    }
}
