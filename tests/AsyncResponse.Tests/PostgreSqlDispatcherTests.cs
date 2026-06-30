using AsyncResponse.Transports.PostgreSQL;
using Microsoft.Extensions.Logging.Abstractions;
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
        await WaitUntilAsync(() => calls.DeadLetter == 1);

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
        public bool DeleteOriginalOnDeadLetter;
        public bool DeadLetterResult;
        public TimeSpan LastNakDelay;
    }
}
