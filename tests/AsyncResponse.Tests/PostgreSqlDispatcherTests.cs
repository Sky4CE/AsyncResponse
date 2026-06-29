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
    public async Task AckAfterReceive_AcksBeforeBackgroundHandlerRuns()
    {
        var calls = new Calls();
        var handled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var dispatcher = new PostgreSqlMessageDispatcher(
            (_, _) =>
            {
                calls.Handler++;
                Assert.Equal(1, calls.Ack);
                handled.SetResult();
                return Task.CompletedTask;
            },
            new PostgreSqlAsyncResponseTransportOptions(),
            new PostgreSqlSubscriberOptions().UseAckAfterReceive(1, 8, TimeSpan.FromSeconds(5)),
            NullLogger.Instance,
            PostgreSqlSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery(calls), CancellationToken.None);
        await handled.Task.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Equal(1, calls.Ack);
        Assert.Equal(1, calls.Handler);
    }

    private static PostgreSqlTransportDelivery Delivery(Calls calls, int attempt = 1)
        => new(
            Guid.NewGuid(),
            "worker",
            "{}",
            new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase),
            attempt,
            () =>
            {
                calls.Ack++;
                return ValueTask.CompletedTask;
            },
            _ =>
            {
                calls.Nak++;
                return ValueTask.CompletedTask;
            },
            (_, deleteOriginal, _) =>
            {
                calls.DeadLetter++;
                calls.DeleteOriginalOnDeadLetter = deleteOriginal;
                return ValueTask.FromResult(true);
            });

    private sealed class Calls
    {
        public int Handler;
        public int Ack;
        public int Nak;
        public int DeadLetter;
        public bool DeleteOriginalOnDeadLetter;
    }
}
