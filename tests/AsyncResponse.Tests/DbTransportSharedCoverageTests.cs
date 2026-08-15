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
    /// A handler that completes before the first beat produces no renewal activity — the beat is
    /// cancelled exception-free before it fires — and the grace wait proves nothing keeps beating
    /// after the ack (no leaked renewal loop). The heartbeat is still ARMED before the handler
    /// runs; see the blocking-handler fact for why that must never be lazy.
    /// </summary>
    [Theory]
    [InlineData(Provider.SqlServer)]
    [InlineData(Provider.PostgreSql)]
    [InlineData(Provider.MongoDb)]
    public async Task FastHandler_AcksWithoutRenewalActivity_AndLeaksNoHeartbeat(Provider provider)
    {
        var calls = new Calls();
        var logger = new CollectingLogger();

        await RunAsync(
            provider,
            logger,
            lockTimeout: TimeSpan.FromMilliseconds(20),
            calls: calls,
            handler: static () => Task.CompletedTask);

        // Several beat intervals of grace: a leaked heartbeat would renew here.
        await Task.Delay(100);
        Assert.Equal(1, calls.Ack);
        Assert.Equal(0, calls.Renew);
    }

    /// <summary>
    /// The heartbeat must be armed BEFORE any user code runs: a handler can burn its whole lease
    /// synchronously (CPU work or blocking I/O with no await), and only an already-armed beat —
    /// firing on a timer thread — can renew under the blocked handler thread. This handler never
    /// yields until it OBSERVES a renewal, so a lazily-armed heartbeat (armed only after the
    /// first incomplete await) fails this fact by timeout.
    /// </summary>
    [Theory]
    [InlineData(Provider.SqlServer)]
    [InlineData(Provider.PostgreSql)]
    [InlineData(Provider.MongoDb)]
    public async Task SynchronouslyBlockingHandler_IsRenewedUnderTheBlockedThread(Provider provider)
    {
        var calls = new Calls();
        var logger = new CollectingLogger();

        await RunAsync(
            provider,
            logger,
            lockTimeout: TimeSpan.FromMilliseconds(100),
            calls: calls,
            handler: () =>
            {
                var blockedUntil = DateTime.UtcNow + TimeSpan.FromSeconds(10);
                while (Volatile.Read(ref calls.Renew) < 1 && DateTime.UtcNow < blockedUntil)
                    Thread.Sleep(10);
                return Task.CompletedTask;
            });

        Assert.True(calls.Renew >= 1, "the lease was never renewed while the handler blocked its thread");
        Assert.Equal(1, calls.Ack);
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

    /// <summary>
    /// A failed handler is released for redelivery with a NAK; a NAK that itself fails (connection
    /// blip during the release) must be swallowed like the guarded ACK — an escape would tear the
    /// whole subscriber down over one poison message, and the drain on the way out dead-letters
    /// unrelated already-ACKed in-flight work. The lease lapses on its own either way.
    /// </summary>
    [Theory]
    [InlineData(Provider.SqlServer)]
    [InlineData(Provider.PostgreSql)]
    [InlineData(Provider.MongoDb)]
    public async Task FailedHandler_WhoseReleaseNakThrows_DoesNotTearDownTheSubscriber(Provider provider)
    {
        var calls = new Calls { NakThrows = true };
        var logger = new CollectingLogger();

        await RunAsync(
            provider,
            logger,
            lockTimeout: TimeSpan.FromSeconds(30),
            calls: calls,
            handler: static () => throw new InvalidOperationException("handler blew up"));

        Assert.Equal(1, calls.Nak);
        Assert.Equal(0, calls.Ack);
        Assert.Contains(logger.Messages, message => message.StartsWith("Failed to NAK", StringComparison.Ordinal)
            && message.Contains("after a failed handler", StringComparison.Ordinal));
    }

    /// <summary>
    /// Same guard on the second bare release: when the attempt cap is reached but the dead-letter
    /// publish fails, the fallback NAK's own failure must not escape either.
    /// </summary>
    [Theory]
    [InlineData(Provider.SqlServer)]
    [InlineData(Provider.PostgreSql)]
    [InlineData(Provider.MongoDb)]
    public async Task FailedDeadLetter_WhoseFallbackNakThrows_DoesNotTearDownTheSubscriber(Provider provider)
    {
        var calls = new Calls { NakThrows = true, DeadLetterResult = false };
        var logger = new CollectingLogger();

        await RunAsync(
            provider,
            logger,
            lockTimeout: TimeSpan.FromSeconds(30),
            calls: calls,
            handler: static () => throw new InvalidOperationException("handler blew up"),
            maxDeliveryAttempts: 1);

        Assert.Equal(1, calls.DeadLetter);
        Assert.Equal(1, calls.Nak);
        Assert.Contains(logger.Messages, message => message.StartsWith("Failed to NAK", StringComparison.Ordinal));
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
        Action? onBackgroundFailure = null,
        int? maxDeliveryAttempts = null)
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
                if (maxDeliveryAttempts is { } sqlCap)
                    subscriber.MaxDeliveryAttempts = sqlCap;
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
                if (maxDeliveryAttempts is { } pgCap)
                    subscriber.MaxDeliveryAttempts = pgCap;
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
                if (maxDeliveryAttempts is { } mongoCap)
                    subscriber.MaxDeliveryAttempts = mongoCap;
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
        public int Nak;
        public int Renew;
        public int DeadLetter;
        public bool DeadLetterResult = true;
        public bool RenewThrows;
        public bool NakThrows;

        public ValueTask AckAsync()
        {
            Interlocked.Increment(ref Ack);
            return ValueTask.CompletedTask;
        }

        public ValueTask NakAsync(TimeSpan delay)
        {
            Interlocked.Increment(ref Nak);
            if (NakThrows)
                throw new InvalidOperationException("release store unavailable");

            return ValueTask.CompletedTask;
        }

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

}
