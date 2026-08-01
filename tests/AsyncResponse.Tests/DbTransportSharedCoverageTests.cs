using AsyncResponse.Transports.MongoDB;
using AsyncResponse.Transports.PostgreSQL;
using AsyncResponse.Transports.SqlServer;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Failure paths of <c>src/Transports/Shared/DbTransportShared.cs</c>, which is
/// <c>&lt;Compile Include&gt;</c>-linked into the MongoDB, PostgreSQL and SQL Server transport
/// packages. The dispatcher compiles separately into each, so every fact runs against all three.
/// </summary>
public sealed class DbTransportSharedCoverageTests
{
    /// <summary>
    /// A lease renewal that throws is a transient store blip, not a lost fence: it is logged and the
    /// heartbeat tries again on the next beat rather than abandoning the in-flight handler.
    /// </summary>
    [Theory]
    [InlineData(Provider.SqlServer)]
    [InlineData(Provider.PostgreSql)]
    [InlineData(Provider.MongoDb)]
    public async Task LeaseRenewal_ThatThrows_IsLoggedAndRetriedOnTheNextBeat(Provider provider)
    {
        var calls = new Calls { RenewThrows = true };
        var logger = new CollectingLogger();

        // Hold the handler until the renewal has faulted twice — proof the loop kept beating
        // instead of dying on the first exception.
        await RunAsync(
            provider,
            logger,
            lockTimeout: TimeSpan.FromMilliseconds(100),
            calls: calls,
            handler: async () =>
            {
                while (Volatile.Read(ref calls.Renew) < 2)
                    await Task.Delay(10);
            });

        Assert.True(calls.Renew >= 2);
        Assert.Equal(1, calls.Ack);
        Assert.Contains(logger.Messages, message => message.StartsWith("Failed to renew the lease of", StringComparison.Ordinal));
    }

    /// <summary>
    /// When the handler fails after an early ACK, the message is already gone from the queue, so a
    /// dead-letter that also fails leaves the failure observable only through logs and the callback —
    /// which is exactly what it must say.
    /// </summary>
    [Theory]
    [InlineData(Provider.SqlServer)]
    [InlineData(Provider.PostgreSql)]
    [InlineData(Provider.MongoDb)]
    public async Task EarlyAck_HandlerFailure_ReportsAnUnrecoverableDeadLetter(Provider provider)
    {
        var calls = new Calls { DeadLetterResult = false };
        var logger = new CollectingLogger();
        var failures = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await RunAsync(
            provider,
            logger,
            lockTimeout: TimeSpan.FromSeconds(30),
            calls: calls,
            handler: () => throw new InvalidOperationException("handler blew up"),
            earlyAck: true,
            onBackgroundFailure: () => failures.TrySetResult());

        await failures.Task.WaitAsync(TimeSpan.FromSeconds(30));
        await Eventually(() => logger.Messages.Any(
            message => message.StartsWith("Failed to dead-letter already-ACKed", StringComparison.Ordinal)));
        Assert.Equal(1, calls.DeadLetter);
    }

    public enum Provider
    {
        SqlServer,
        PostgreSql,
        MongoDb
    }

    /// <summary>
    /// Builds the provider's dispatcher over a delivery wired to <paramref name="calls"/>, handles
    /// one message, and disposes — draining any background worker the early-ACK mode started.
    /// </summary>
    private static async Task RunAsync(
        Provider provider,
        CollectingLogger logger,
        TimeSpan lockTimeout,
        Calls calls,
        Func<Task> handler,
        bool earlyAck = false,
        Action? onBackgroundFailure = null)
    {
        switch (provider)
        {
            case Provider.SqlServer:
            {
                var options = new SqlServerAsyncResponseTransportOptions
                {
                    ConnectionString = "Server=localhost;Database=unused;User ID=sa;Password=unused;TrustServerCertificate=True",
                    LockTimeout = lockTimeout
                };
                var subscriber = new SqlServerSubscriberOptions();
                if (onBackgroundFailure is not null)
                    subscriber.OnBackgroundFailure = _ => { onBackgroundFailure(); return ValueTask.CompletedTask; };
                if (earlyAck)
                    subscriber.UseAckAfterEnqueue(1, 8, TimeSpan.FromMilliseconds(50));

                await using var dispatcher = new SqlServerMessageDispatcher(
                    (_, _) => handler(), options, subscriber, logger, SqlServerSubscriberRole.Worker);
                await dispatcher.HandleAsync(
                    new SqlServerTransportDelivery(
                        Guid.NewGuid(), "worker", "{}", Headers, 1,
                        calls.AckAsync, calls.NakAsync, calls.DeadLetterAsync, calls.RenewAsync),
                    CancellationToken.None);
                return;
            }

            case Provider.PostgreSql:
            {
                var options = new PostgreSqlAsyncResponseTransportOptions { LockTimeout = lockTimeout };
                var subscriber = new PostgreSqlSubscriberOptions();
                if (onBackgroundFailure is not null)
                    subscriber.OnBackgroundFailure = _ => { onBackgroundFailure(); return ValueTask.CompletedTask; };
                if (earlyAck)
                    subscriber.UseAckAfterEnqueue(1, 8, TimeSpan.FromMilliseconds(50));

                await using var dispatcher = new PostgreSqlMessageDispatcher(
                    (_, _) => handler(), options, subscriber, logger, PostgreSqlSubscriberRole.Worker);
                await dispatcher.HandleAsync(
                    new PostgreSqlTransportDelivery(
                        Guid.NewGuid(), "worker", "{}", Headers, 1,
                        calls.AckAsync, calls.NakAsync, calls.DeadLetterAsync, calls.RenewAsync),
                    CancellationToken.None);
                return;
            }

            default:
            {
                var options = new MongoDbAsyncResponseTransportOptions { LockTimeout = lockTimeout };
                var subscriber = new MongoDbSubscriberOptions();
                if (onBackgroundFailure is not null)
                    subscriber.OnBackgroundFailure = _ => { onBackgroundFailure(); return ValueTask.CompletedTask; };
                if (earlyAck)
                    subscriber.UseAckAfterEnqueue(1, 8, TimeSpan.FromMilliseconds(50));

                await using var dispatcher = new MongoDbMessageDispatcher(
                    (_, _) => handler(), options, subscriber, logger, MongoDbSubscriberRole.Worker);
                await dispatcher.HandleAsync(
                    new MongoDbTransportDelivery(
                        Guid.NewGuid(), "worker", "{}", Headers, 1,
                        calls.AckAsync, calls.NakAsync, calls.DeadLetterAsync, calls.RenewAsync),
                    CancellationToken.None);
                return;
            }
        }
    }

    private static readonly Dictionary<string, string> Headers = new(StringComparer.OrdinalIgnoreCase);

    private static async Task Eventually(Func<bool> condition)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        while (!condition())
            await Task.Delay(15, timeout.Token);
    }

    /// <summary>Settlement callbacks shared by all three provider delivery records.</summary>
    private sealed class Calls
    {
        public int Ack;
        public int Renew;
        public int DeadLetter;
        public bool DeadLetterResult = true;
        public bool RenewThrows;

        public ValueTask AckAsync()
        {
            Interlocked.Increment(ref Ack);
            return ValueTask.CompletedTask;
        }

        public ValueTask NakAsync(TimeSpan delay) => ValueTask.CompletedTask;

        public ValueTask<bool> DeadLetterAsync(Exception exception, bool deleteOriginal, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref DeadLetter);
            return ValueTask.FromResult(DeadLetterResult);
        }

        public ValueTask<bool> RenewAsync()
        {
            Interlocked.Increment(ref Renew);
            if (RenewThrows)
                throw new InvalidOperationException("lease store unavailable");

            return ValueTask.FromResult(true);
        }
    }

    private sealed class CollectingLogger : Microsoft.Extensions.Logging.ILogger
    {
        private readonly List<string> _messages = [];
        private readonly object _gate = new();

        public IReadOnlyList<string> Messages
        {
            get { lock (_gate) return _messages.ToArray(); }
        }

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => NullLogger.Instance.BeginScope(state);

        public bool IsEnabled(Microsoft.Extensions.Logging.LogLevel logLevel) => true;

        public void Log<TState>(
            Microsoft.Extensions.Logging.LogLevel logLevel,
            Microsoft.Extensions.Logging.EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            lock (_gate) _messages.Add(formatter(state, exception));
        }
    }
}
