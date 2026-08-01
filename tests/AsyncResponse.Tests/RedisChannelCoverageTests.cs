using AsyncResponse.Channels.Redis;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using StackExchange.Redis;
using System.Net;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Redis channel paths the main suite leaves untouched: the re-publish taken when a waiter
/// subscribes between the failed publish and the recovery-state read, and the diagnostics tagging on
/// every publish entry point.
/// </summary>
public sealed class RedisChannelCoverageTests
{
    private readonly Mock<IConnectionMultiplexer> _multiplexer = new();
    private readonly Mock<ISubscriber> _subscriber = new();
    private readonly Mock<IServer> _server = new();
    private readonly Mock<IRecoveryStateStore> _store = new();
    private readonly ServiceProvider _services = new ServiceCollection().BuildServiceProvider();

    /// <summary>PUBSUB NUMSUB result the liveness probe reads; tests flip it mid-publish.</summary>
    private long _liveSubscribers;

    public RedisChannelCoverageTests()
    {
        _multiplexer.Setup(instance => instance.GetSubscriber(It.IsAny<object?>())).Returns(_subscriber.Object);
        _multiplexer.Setup(instance => instance.GetEndPoints(It.IsAny<bool>()))
            .Returns([new DnsEndPoint("localhost", 6379)]);
        _multiplexer.Setup(instance => instance.GetServer(It.IsAny<EndPoint>(), It.IsAny<object?>()))
            .Returns(_server.Object);
        _server.Setup(instance => instance.IsConnected).Returns(true);
        _server.Setup(instance => instance.SubscriptionSubscriberCount(It.IsAny<RedisChannel>(), It.IsAny<CommandFlags>()))
            .Returns(() => _liveSubscribers);

        _store.Setup(store => store.SaveAsync(It.IsAny<string>(), It.IsAny<RecoveryState>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        _store.Setup(store => store.GetAllAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(Array.Empty<RecoveryState>());
        _store.Setup(store => store.TryDeleteAsync(It.IsAny<string>(), It.IsAny<Guid>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);
    }

    /// <summary>
    /// A publish that reaches nobody, followed by a probe that finds a live waiter, means the
    /// snapshot was stale: the publish is retried, and a retry that lands consumes no registration.
    /// </summary>
    [Theory]
    [InlineData(PublishKind.Response)]
    [InlineData(PublishKind.RawJson)]
    [InlineData(PublishKind.Exception)]
    public async Task Publish_RepublishesWhenAWaiterRaced_AndStopsOnceItLands(PublishKind kind)
    {
        // A listener is attached so the subscriber/recovery tags inside the retry block are exercised.
        using var activities = new AsyncResponseActivityCollector();
        // The waiter is live from the probe's point of view all along; the first publish simply
        // raced it, and the second reaches it.
        _liveSubscribers = 1;
        var publishes = 0;
        _subscriber
            .Setup(instance => instance.PublishAsync(It.IsAny<RedisChannel>(), It.IsAny<RedisValue>(), It.IsAny<CommandFlags>()))
            .ReturnsAsync(() => publishes++ == 0 ? 0L : 1L);

        await PublishAsync(CreateChannel(), kind, "corr-republish-lands");

        Assert.Equal(2, publishes);
        // The retry landed, so the registration was read once (before the re-check) and never consumed.
        _store.Verify(store => store.GetAllAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    /// <summary>
    /// A re-publish that still reaches nobody means the waiter really is gone: only then is the
    /// recovery registration consumed.
    /// </summary>
    [Theory]
    [InlineData(PublishKind.Response)]
    [InlineData(PublishKind.RawJson)]
    [InlineData(PublishKind.Exception)]
    public async Task Publish_ConsumesRecoveryOnlyAfterASecondMiss(PublishKind kind)
    {
        using var activities = new AsyncResponseActivityCollector();
        _subscriber
            .Setup(instance => instance.PublishAsync(It.IsAny<RedisChannel>(), It.IsAny<RedisValue>(), It.IsAny<CommandFlags>()))
            .ReturnsAsync(0L);
        // The probe sees a waiter once — enough to force the re-publish — then agrees it is gone.
        var probes = 0;
        _server.Setup(instance => instance.SubscriptionSubscriberCount(It.IsAny<RedisChannel>(), It.IsAny<CommandFlags>()))
            .Returns(() => probes++ == 0 ? 1L : 0L);

        await PublishAsync(CreateChannel(), kind, "corr-republish-misses");

        _subscriber.Verify(
            instance => instance.PublishAsync(It.IsAny<RedisChannel>(), It.IsAny<RedisValue>(), It.IsAny<CommandFlags>()),
            Times.Exactly(2));
        _store.Verify(store => store.GetAllAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Exactly(2));
    }

    /// <summary>Every publish entry point opens an activity and tags the channel and subscriber count.</summary>
    [Fact]
    public async Task PublishPaths_TagTheirActivityWhenAListenerIsAttached()
    {
        using var activities = new AsyncResponseActivityCollector();
        _subscriber
            .Setup(instance => instance.PublishAsync(It.IsAny<RedisChannel>(), It.IsAny<RedisValue>(), It.IsAny<CommandFlags>()))
            .ReturnsAsync(1L);
        var channel = CreateChannel();

        await channel.SetResponse(new OperationResult { Status = OperationStatus.Completed }, "corr-tags");
        await ((IRawAsyncResponsePublisher)channel).SetRawResponseJson("""{"Status":2}""", "corr-tags");
        await channel.SetException(new InvalidOperationException("boom"), "corr-tags");

        foreach (var name in new[] { "asyncresponse.set_response", "asyncresponse.ingress.raw_response", "asyncresponse.set_exception" })
            activities.Single(name, "asyncresponse.channel", "redis");

        var setException = activities.Single("asyncresponse.set_exception", "asyncresponse.channel", "redis");
        Assert.Equal(
            typeof(InvalidOperationException).FullName,
            AsyncResponseActivityCollector.Tag(setException, "asyncresponse.exception_type"));
    }

    /// <summary>The waiter's own activity is tagged at registration, including the effective timeout.</summary>
    [Fact]
    public async Task CreateResponseWaiter_TagsTheWaitActivity()
    {
        using var activities = new AsyncResponseActivityCollector();
        var channel = CreateChannel();

        var waiter = await channel.CreateResponseWaiter<OperationResult>("corr-wait-tags", timeout: TimeSpan.FromSeconds(9));
        // The wait activity is only reported once it stops, which is part of the waiter's cleanup.
        await waiter.DisposeAsync();

        var activity = activities.Single("asyncresponse.wait", "asyncresponse.channel", "redis");
        Assert.Equal(9d, AsyncResponseActivityCollector.Tag(activity, "asyncresponse.timeout_seconds"));
    }

    /// <summary>A disconnected endpoint is skipped rather than probed.</summary>
    [Fact]
    public async Task CountActiveSubscribers_SkipsDisconnectedEndpointsAndSurvivesAProbeFailure()
    {
        var channel = CreateChannel();

        _server.Setup(instance => instance.IsConnected).Returns(false);
        Assert.Equal(0L, await channel.CountActiveSubscribersAsync("corr-disconnected"));

        // A probe that throws is logged and treated as "no information", not propagated.
        _server.Setup(instance => instance.IsConnected).Returns(true);
        _server.Setup(instance => instance.SubscriptionSubscriberCount(It.IsAny<RedisChannel>(), It.IsAny<CommandFlags>()))
            .Throws(new RedisTimeoutException(CommandFlags.None, "probe timed out", CommandStatus.Unknown));
        Assert.Equal(0L, await channel.CountActiveSubscribersAsync("corr-probe-fails"));

        // A blank correlation id short-circuits before any endpoint is touched.
        Assert.Equal(0L, await channel.CountActiveSubscribersAsync(" "));
    }

    public enum PublishKind
    {
        Response,
        RawJson,
        Exception
    }

    private static Task PublishAsync(RedisAsyncResponseChannel channel, PublishKind kind, string correlationId)
        => kind switch
        {
            PublishKind.Response => channel.SetResponse(new OperationResult { Status = OperationStatus.Completed }, correlationId),
            PublishKind.RawJson => ((IRawAsyncResponsePublisher)channel).SetRawResponseJson("""{"Status":2}""", correlationId),
            _ => channel.SetException(new InvalidOperationException("boom"), correlationId)
        };

    private RedisAsyncResponseChannel CreateChannel() => new(
        _services.GetRequiredService<IServiceScopeFactory>(),
        _multiplexer.Object,
        _store.Object,
        Options.Create(new RedisAsyncResponseOptions
        {
            DefaultTimeout = TimeSpan.FromSeconds(5),
            RecoveryStateExpiry = TimeSpan.FromMinutes(5)
        }),
        new AsyncResponseContextPropagation([]),
        NullLogger<RedisAsyncResponseChannel>.Instance,
        new NoopChannelSubscriber());

    /// <summary>
    /// The channel's async subscribe seam. ChannelMessageQueue is sealed, so the real subscriber
    /// cannot run against a mocked ISubscriber; these tests never push messages, so nothing beyond
    /// a well-behaved subscribe/unsubscribe is needed.
    /// </summary>
    private sealed class NoopChannelSubscriber : IRedisChannelSubscriber
    {
        public Task<IRedisChannelSubscription> SubscribeAsync(RedisChannel channel, Func<RedisChannel, RedisValue, Task> onMessage)
            => Task.FromResult<IRedisChannelSubscription>(new Subscription());

        private sealed class Subscription : IRedisChannelSubscription
        {
            public ValueTask DisposeAsync() => ValueTask.CompletedTask;
        }
    }
}
