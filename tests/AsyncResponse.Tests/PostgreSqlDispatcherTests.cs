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
            (_, _, _) => ValueTask.FromResult(true));
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
    public async Task AckAfterReceive_BackgroundHandlerStillEmitsReceiveSpan()
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
            new PostgreSqlSubscriberOptions().UseAckAfterReceive(1, 8, TimeSpan.FromSeconds(5)),
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
        Assert.Equal(nameof(PostgreSqlAckMode.AckAfterReceive), AsyncResponseActivityCollector.Tag(activity, "asyncresponse.postgresql.ack_mode"));
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
    public async Task AckAfterReceive_AcksOnReceive_WithoutWaitingForHandler()
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
            new PostgreSqlSubscriberOptions().UseAckAfterReceive(1, 8, TimeSpan.FromSeconds(5)),
            NullLogger.Instance,
            PostgreSqlSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery(calls), CancellationToken.None);

        // AckAfterReceive acks the row as part of accepting it into the background queue, so it is
        // already acknowledged the moment HandleAsync returns — without waiting for the handler. The
        // handler runs on the background worker and may execute concurrently with the ack, so the test
        // must not assert their relative ordering (asserting calls.Ack inside the handler was the
        // source of an intermittent timeout when the handler won that race).
        Assert.Equal(1, calls.Ack);

        await handled.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(1, calls.Handler);
    }

    [Fact]
    public async Task AckAfterReceive_BackgroundFailure_DeadLettersWithoutDeletingOriginal_AndInvokesCallback()
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
            }.UseAckAfterReceive(1, 8, TimeSpan.FromSeconds(5)),
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
    public async Task AckAfterReceive_BackgroundFailureCallbackExceptionsAreSwallowed()
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
            }.UseAckAfterReceive(1, 8, TimeSpan.FromSeconds(5)),
            NullLogger.Instance,
            PostgreSqlSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery(calls), CancellationToken.None);

        await handled.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await WaitUntilAsync(() => calls.DeadLetter == 1);
        Assert.Equal(1, calls.DeadLetter);
    }

    [Fact]
    public async Task AckAfterReceive_Overflow_ReleasesRowForRetry()
    {
        var calls = new Calls();
        using var entered = new ManualResetEventSlim();
        using var release = new ManualResetEventSlim();
        await using var dispatcher = new PostgreSqlMessageDispatcher(
            (_, _) =>
            {
                entered.Set();
                release.Wait(TimeSpan.FromSeconds(5));
                return Task.CompletedTask;
            },
            new PostgreSqlAsyncResponseTransportOptions(),
            new PostgreSqlSubscriberOptions().UseAckAfterReceive(1, 1, TimeSpan.FromSeconds(5)),
            NullLogger.Instance,
            PostgreSqlSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery(calls), CancellationToken.None);
        Assert.True(entered.Wait(TimeSpan.FromSeconds(5)));
        await dispatcher.HandleAsync(Delivery(calls), CancellationToken.None);
        await dispatcher.HandleAsync(Delivery(calls), CancellationToken.None);

        await WaitUntilAsync(() => calls.Nak == 1);
        Assert.Equal(TimeSpan.FromSeconds(5), calls.LastNakDelay);

        release.Set();
    }

    [Fact]
    public async Task AckAfterReceive_BackgroundFailureWithoutCallback_DeadLetters()
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
            new PostgreSqlSubscriberOptions().UseAckAfterReceive(1, 8, TimeSpan.FromSeconds(5)),
            NullLogger.Instance,
            PostgreSqlSubscriberRole.ResponseIngress);

        await dispatcher.HandleAsync(Delivery(calls), CancellationToken.None);

        await handled.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await WaitUntilAsync(() => calls.DeadLetter == 1);
        Assert.Equal(1, calls.DeadLetter);
    }

    [Fact]
    public async Task AckAfterReceive_BackgroundOperationCanceled_DeadLettersLikeHandlerFailure()
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
            new PostgreSqlSubscriberOptions().UseAckAfterReceive(1, 8, TimeSpan.FromSeconds(5)),
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
            new PostgreSqlSubscriberOptions().UseAckAfterReceive(1, 8, TimeSpan.FromMilliseconds(1)),
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
            new PostgreSqlSubscriberOptions().UseAckAfterReceive(1, 8, TimeSpan.FromMilliseconds(50)),
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
            new PostgreSqlSubscriberOptions { AckMode = PostgreSqlAckMode.AckAfterReceive },
            NullLogger.Instance,
            PostgreSqlSubscriberRole.Worker));

        Assert.Contains(nameof(PostgreSqlSubscriberOptions.BackgroundWorkerCount), ex.Message);
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
        public Calls() => DeadLetterResult = true;

        public int Handler;
        public int Ack;
        public int Nak;
        public int DeadLetter;
        public TaskCompletionSource DeadLettered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public bool DeleteOriginalOnDeadLetter;
        public bool DeadLetterResult;
        public TimeSpan LastNakDelay;
    }
}
