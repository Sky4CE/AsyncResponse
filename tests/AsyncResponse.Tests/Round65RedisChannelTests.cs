using AsyncResponse.Channels.NATS;
using AsyncResponse.Channels.Redis;
using AsyncResponse.Testing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using StackExchange.Redis;
using System.Net;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Round 65, Redis and NATS channels: a zero / no-responders answer right after this process's
/// connection came back from an outage is not proof that no waiter is live — the waiters the same
/// outage dropped may still be re-subscribing (F-06) — and a wait no longer outlives its own
/// recovery registration (L-09).
/// </summary>
public sealed class Round65RedisChannelTests
{
    // RedisAsyncResponseChannel.RestoredEndPointGrace / NatsAsyncResponseChannel.ReconnectGrace,
    // spelled out so the red-on-old build compiles.
    private static readonly TimeSpan Grace = TimeSpan.FromSeconds(90);

    private readonly ServiceProvider _services = new ServiceCollection().BuildServiceProvider();

    // ------------------------------------------------------------------ F-06 Redis

    /// <summary>
    /// A single-endpoint deployment restarts: every client's connection drops, and each comes back
    /// on its own reconnect schedule. This process reconnected first and its NUMSUB reads 0 — the
    /// waiter in another process has not re-subscribed yet. The probe read that zero as
    /// conclusive the moment ConnectionRestored reset the endpoint, so the lost-subscriber
    /// dispatcher consumed the live waiter's registration. Pre-fix: 0 at once.
    /// </summary>
    [Fact]
    public async Task Redis_ZeroFromAnEndPointBackFromAnOutage_StaysUnknownForTheRestoredGrace()
    {
        var (multiplexer, server, _, _) = RedisMultiplexer();
        var clock = new VirtualTimeProvider();
        var channel = RedisChannel(multiplexer, Mock.Of<IRecoveryStateStore>(), clock);

        Assert.Equal(0, await channel.CountActiveSubscribersAsync("corr")); // healthy: a zero is conclusive

        server.SetupGet(s => s.IsConnected).Returns(false);
        RaiseRedis(multiplexer, server, restored: false);
        clock.Advance(TimeSpan.FromSeconds(5));
        server.SetupGet(s => s.IsConnected).Returns(true);
        RaiseRedis(multiplexer, server, restored: true);

        Assert.Equal(-1, await channel.CountActiveSubscribersAsync("corr"));
        clock.Advance(Grace - TimeSpan.FromSeconds(1));
        Assert.Equal(-1, await channel.CountActiveSubscribersAsync("corr"));
        clock.Advance(TimeSpan.FromSeconds(1));
        Assert.Equal(0, await channel.CountActiveSubscribersAsync("corr"));
    }

    /// <summary>
    /// The end-to-end consequence: inside the window a lost-subscriber publish throws (the ingress
    /// retries and the transport redelivers) instead of settling on "no live waiter". Pre-fix the
    /// publish returned normally — the response, with this callback-less registration, dropped.
    /// </summary>
    [Fact]
    public async Task Redis_LostSubscriberPublishRightAfterAReconnect_ThrowsInsteadOfSettling()
    {
        var (multiplexer, server, subscriber, _) = RedisMultiplexer();
        subscriber
            .Setup(s => s.PublishAsync(It.IsAny<StackExchange.Redis.RedisChannel>(), It.IsAny<RedisValue>(), It.IsAny<CommandFlags>()))
            .ReturnsAsync(0L);
        var store = new Mock<IRecoveryStateStore>();
        store.Setup(s => s.GetAllAsync("corr", It.IsAny<CancellationToken>()))
            .ReturnsAsync([new RecoveryState { CorrelationId = "corr", RegistrationId = Guid.NewGuid() }]);
        var channel = RedisChannel(multiplexer, store.Object, new VirtualTimeProvider());

        server.SetupGet(s => s.IsConnected).Returns(false);
        RaiseRedis(multiplexer, server, restored: false);
        server.SetupGet(s => s.IsConnected).Returns(true);
        RaiseRedis(multiplexer, server, restored: true);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => channel.SetResponse(new OperationResult { Status = OperationStatus.Completed }, "corr"));
        store.Verify(s => s.TryDeleteAsync(It.IsAny<string>(), It.IsAny<Guid>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    /// <summary>
    /// StackExchange.Redis raises ConnectionRestored for the FIRST connection too, when nobody else
    /// is reconnecting: that must not start the grace (every lost-subscriber publish in the first
    /// 90 s of a process would throw).
    /// </summary>
    [Fact]
    public async Task Redis_FirstConnection_StartsNoGrace()
    {
        var (multiplexer, server, _, _) = RedisMultiplexer(connectedAtStart: false);
        var channel = RedisChannel(multiplexer, Mock.Of<IRecoveryStateStore>(), new VirtualTimeProvider());

        server.SetupGet(s => s.IsConnected).Returns(true);
        RaiseRedis(multiplexer, server, restored: true);

        Assert.Equal(0, await channel.CountActiveSubscribersAsync("corr"));
    }

    // ------------------------------------------------------------------ F-06 NATS

    /// <summary>
    /// NATS twin, worse before the fix: the probe had no grace of any kind, so a no-responders
    /// answer right after this connection's reconnect — the waiter's own client still inside its
    /// ReconnectWait — consumed the registration. Pre-fix: 0 at once.
    /// </summary>
    [Fact]
    public async Task Nats_NoRespondersRightAfterAReconnect_StaysUnknownForTheGrace()
    {
        var client = new ReconnectingNatsClient();
        var clock = new VirtualTimeProvider();
        var channel = NatsChannel(client, Mock.Of<IRecoveryStateStore>(), clock);

        Assert.Equal(0, await channel.CountActiveSubscribersAsync("corr")); // no reconnect yet: conclusive

        client.Reconnect();

        Assert.Equal(-1, await channel.CountActiveSubscribersAsync("corr"));
        clock.Advance(Grace - TimeSpan.FromSeconds(1));
        Assert.Equal(-1, await channel.CountActiveSubscribersAsync("corr"));
        clock.Advance(TimeSpan.FromSeconds(1));
        Assert.Equal(0, await channel.CountActiveSubscribersAsync("corr"));
    }

    [Fact]
    public async Task Nats_LostSubscriberPublishRightAfterAReconnect_ThrowsInsteadOfSettling()
    {
        var client = new ReconnectingNatsClient();
        var store = new Mock<IRecoveryStateStore>();
        store.Setup(s => s.GetAllAsync("corr", It.IsAny<CancellationToken>()))
            .ReturnsAsync([new RecoveryState { CorrelationId = "corr", RegistrationId = Guid.NewGuid() }]);
        var channel = NatsChannel(client, store.Object, new VirtualTimeProvider());

        client.Reconnect();

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => channel.SetResponse(new OperationResult { Status = OperationStatus.Completed }, "corr"));
        store.Verify(s => s.TryDeleteAsync(It.IsAny<string>(), It.IsAny<Guid>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    // ------------------------------------------------------------------ L-09 Redis

    /// <summary>
    /// The registration was saved once with RecoveryStateExpiry and never refreshed, so a wait
    /// longer than that (an explicit timeout or DefaultTimeout) lost its recovery for the tail:
    /// a response landing there after the waiter's process died was dropped. Redis keys take any
    /// TTL, so the registration now outlives the wait. Pre-fix: saved with 5 minutes.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Redis_ATimeoutLongerThanTheExpiry_SavesTheRegistrationForTheWholeWait(bool viaDefaultTimeout)
    {
        var (multiplexer, _, _, _) = RedisMultiplexer();
        var store = new Mock<IRecoveryStateStore>();
        var ttls = new List<TimeSpan>();
        store.Setup(s => s.SaveAsync(It.IsAny<string>(), It.IsAny<RecoveryState>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
            .Callback<string, RecoveryState, TimeSpan, CancellationToken>((_, _, ttl, _) => ttls.Add(ttl))
            .Returns(Task.CompletedTask);
        var options = new RedisAsyncResponseOptions
        {
            RecoveryStateExpiry = TimeSpan.FromMinutes(5),
            DefaultTimeout = viaDefaultTimeout ? TimeSpan.FromMinutes(30) : TimeSpan.FromMinutes(1)
        };
        var channel = new RedisAsyncResponseChannel(
            _services.GetRequiredService<IServiceScopeFactory>(),
            multiplexer.Object,
            store.Object,
            Options.Create(options),
            new AsyncResponseContextPropagation([]),
            NullLogger<RedisAsyncResponseChannel>.Instance,
            new NoOpChannelSubscriber(),
            new VirtualTimeProvider());

        await using (var waiter = await channel.CreateResponseWaiter<OperationResult>("corr-long", timeout: viaDefaultTimeout ? null : TimeSpan.FromMinutes(30)))
        {
            Assert.Equal(TimeSpan.FromMinutes(30), Assert.Single(ttls));
        }

        // A wait within the expiry keeps the expiry (it bounds how long a dead waiter stays recoverable).
        ttls.Clear();
        await using (var waiter = await channel.CreateResponseWaiter<OperationResult>("corr-short", timeout: TimeSpan.FromMinutes(1)))
        {
            Assert.Equal(TimeSpan.FromMinutes(5), Assert.Single(ttls));
        }
    }

    // ------------------------------------------------------------------ L-09 NATS

    /// <summary>
    /// NATS cannot outlive the expiry: the KV bucket's MaxAge (= RecoveryStateExpiry) removes the
    /// registration whatever it carries. A longer wait is reported (once per channel) and still
    /// runs — refusing it broke durable-flow steps whose own timeout is longer than the expiry,
    /// which used to run and lose only recovery of their tail. Pre-fix: accepted silently.
    /// </summary>
    [Fact]
    public async Task Nats_ATimeoutLongerThanTheExpiry_IsWarnedOnce_AndTheWaitStillRuns()
    {
        var client = new ReconnectingNatsClient();
        var store = new Mock<IRecoveryStateStore>();
        var logger = new CollectingLogger();
        var channel = NatsChannel(client, store.Object, new VirtualTimeProvider(), logger);

        // The fake's subscribe throws: reaching it proves the wait was not refused up front.
        await Assert.ThrowsAsync<NotSupportedException>(
            () => channel.CreateResponseWaiter<OperationResult>("corr-long", timeout: TimeSpan.FromMinutes(30)));
        await Assert.ThrowsAsync<NotSupportedException>(
            () => channel.CreateResponseWaiter<OperationResult>("corr-long-2", timeout: TimeSpan.FromMinutes(30)));

        Assert.Equal(2, client.SubscribeCount);
        var warning = Assert.Single(logger.Messages, m => m.Contains("outlives its recovery registration", StringComparison.Ordinal));
        Assert.Contains("RecoveryStateExpiry", warning, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Nats_ATimeoutWithinTheExpiry_IsNotWarned()
    {
        var client = new ReconnectingNatsClient();
        var logger = new CollectingLogger();
        var channel = NatsChannel(client, new Mock<IRecoveryStateStore>().Object, new VirtualTimeProvider(), logger);

        await Assert.ThrowsAsync<NotSupportedException>(
            () => channel.CreateResponseWaiter<OperationResult>("corr-short", timeout: TimeSpan.FromMinutes(5)));

        Assert.DoesNotContain(logger.Messages, m => m.Contains("outlives its recovery registration", StringComparison.Ordinal));
    }

    /// <summary>A DefaultTimeout past the expiry still validates (the wait is warned, not refused).</summary>
    [Fact]
    public void Nats_ADefaultTimeoutLongerThanTheExpiry_StillValidates()
    {
        var options = new NatsAsyncResponseChannelOptions
        {
            RecoveryStateExpiry = TimeSpan.FromMinutes(5),
            DefaultTimeout = TimeSpan.FromMinutes(30)
        };

        options.Validate();
    }

    // ------------------------------------------------------------------ helpers

    private RedisAsyncResponseChannel RedisChannel(Mock<IConnectionMultiplexer> multiplexer, IRecoveryStateStore store, TimeProvider clock)
        => new(
            _services.GetRequiredService<IServiceScopeFactory>(),
            multiplexer.Object,
            store,
            Options.Create(new RedisAsyncResponseOptions()),
            new AsyncResponseContextPropagation([]),
            NullLogger<RedisAsyncResponseChannel>.Instance,
            channelSubscriber: null,
            clock);

    private NatsAsyncResponseChannel NatsChannel(ReconnectingNatsClient client, IRecoveryStateStore store, TimeProvider clock, CollectingLogger? logger = null)
        => new(
            _services.GetRequiredService<IServiceScopeFactory>(),
            client,
            store,
            Options.Create(new NatsAsyncResponseChannelOptions { RecoveryStateExpiry = TimeSpan.FromMinutes(5) }),
            new AsyncResponseContextPropagation([]),
            logger?.For<NatsAsyncResponseChannel>() ?? NullLogger<NatsAsyncResponseChannel>.Instance,
            clock);

    /// <summary>One standalone primary (a managed cache's single endpoint), answering NUMSUB 0.</summary>
    private static (Mock<IConnectionMultiplexer> Multiplexer, Mock<IServer> Server, Mock<ISubscriber> Subscriber, EndPoint EndPoint) RedisMultiplexer(bool connectedAtStart = true)
    {
        var endPoint = new IPEndPoint(IPAddress.Parse("10.0.0.1"), 6379);
        var server = new Mock<IServer>();
        server.SetupGet(s => s.IsConnected).Returns(connectedAtStart);
        server.SetupGet(s => s.ServerType).Returns(ServerType.Standalone);
        server.SetupGet(s => s.EndPoint).Returns(endPoint);
        server.Setup(s => s.SubscriptionSubscriberCountAsync(It.IsAny<StackExchange.Redis.RedisChannel>(), It.IsAny<CommandFlags>()))
            .ReturnsAsync(0L);
        var subscriber = new Mock<ISubscriber>();
        var multiplexer = new Mock<IConnectionMultiplexer>();
        multiplexer.Setup(m => m.GetSubscriber(It.IsAny<object?>())).Returns(subscriber.Object);
        multiplexer.Setup(m => m.GetEndPoints(It.IsAny<bool>())).Returns([endPoint]);
        multiplexer.Setup(m => m.GetServer(endPoint, It.IsAny<object?>())).Returns(server.Object);
        return (multiplexer, server, subscriber, endPoint);
    }

    /// <summary>Raises ConnectionFailed / ConnectionRestored for the server's interactive connection, as StackExchange.Redis does.</summary>
    private static void RaiseRedis(Mock<IConnectionMultiplexer> multiplexer, Mock<IServer> server, bool restored)
    {
        if (restored)
        {
            multiplexer.Raise(
                m => m.ConnectionRestored += null,
                new ConnectionFailedEventArgs(multiplexer.Object, server.Object.EndPoint!, ConnectionType.Interactive, ConnectionFailureType.None, null!, "test"));
        }
        else
        {
            multiplexer.Raise(
                m => m.ConnectionFailed += null,
                new ConnectionFailedEventArgs(multiplexer.Object, server.Object.EndPoint!, ConnectionType.Interactive, ConnectionFailureType.SocketClosed, new InvalidOperationException("connection lost"), "test"));
        }
    }

    private sealed class NoOpChannelSubscriber : IRedisChannelSubscriber
    {
        public Task<IRedisChannelSubscription> SubscribeAsync(StackExchange.Redis.RedisChannel channel, Func<StackExchange.Redis.RedisChannel, RedisValue, Task> onMessage)
            => Task.FromResult<IRedisChannelSubscription>(new Subscription());

        public Task UnsubscribeAllAsync(StackExchange.Redis.RedisChannel channel) => Task.CompletedTask;

        private sealed class Subscription : IRedisChannelSubscription
        {
            public ValueTask DisposeAsync() => ValueTask.CompletedTask;
        }
    }

    /// <summary>
    /// A NATS client whose every request finds no responders, and which reports reconnects through
    /// <c>WatchReconnects</c> (the interface member where it exists; before the fix nothing asked).
    /// </summary>
    private sealed class ReconnectingNatsClient : INatsResponseChannelClient
    {
        private Action? _onReconnected;

        public int SubscribeCount { get; private set; }

        public void Reconnect() => _onReconnected?.Invoke();

        public IDisposable? WatchReconnects(Action onReconnected)
        {
            _onReconnected = onReconnected;
            return null;
        }

        public Task<NatsDeliveryOutcome> RequestAsync(string subject, string? payload, bool probe, TimeSpan timeout, CancellationToken cancellationToken)
            => Task.FromResult(NatsDeliveryOutcome.NoResponders);

        public Task<INatsChannelSubscription> SubscribeAsync(string subject, Action<int> onMessagesDropped, CancellationToken cancellationToken)
        {
            SubscribeCount++;
            throw new NotSupportedException("not used by these tests");
        }

        public Task FlushAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    }
}
