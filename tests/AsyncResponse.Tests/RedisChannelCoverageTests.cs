using AsyncResponse.Channels.Redis;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using StackExchange.Redis;
using System.Net;
using System.Reflection;
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
        _server.Setup(instance => instance.SubscriptionSubscriberCountAsync(It.IsAny<RedisChannel>(), It.IsAny<CommandFlags>()))
            .ReturnsAsync(() => _liveSubscribers);

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
        _server.Setup(instance => instance.SubscriptionSubscriberCountAsync(It.IsAny<RedisChannel>(), It.IsAny<CommandFlags>()))
            .ReturnsAsync(() => probes++ == 0 ? 1L : 0L);

        await PublishAsync(CreateChannel(), kind, "corr-republish-misses");

        _subscriber.Verify(
            instance => instance.PublishAsync(It.IsAny<RedisChannel>(), It.IsAny<RedisValue>(), It.IsAny<CommandFlags>()),
            Times.Exactly(2));
        _store.Verify(store => store.GetAllAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Exactly(2));
        // The second dispatch consumed the registration only because ITS liveness re-check agreed
        // the waiter is gone — the probe must have been consulted both times.
        Assert.Equal(2, probes);
    }

    /// <summary>
    /// Publishes keep reaching nobody while PUBSUB NUMSUB keeps reporting a live waiter — a
    /// contradiction (subscription landing on another node, or propagation lag). Consuming
    /// recovery registrations on that evidence would strip a live waiter of its recovery arm, so
    /// after the bounded retry the publish leaves all state intact — and surfaces the
    /// non-delivery to the caller instead of silently dropping the payload, so the caller's
    /// retry/redelivery machinery re-attempts once the subscription is visible.
    /// </summary>
    [Theory]
    [InlineData(PublishKind.Response)]
    [InlineData(PublishKind.RawJson)]
    [InlineData(PublishKind.Exception)]
    public async Task Publish_LeavesRecoveryIntactAndThrowsWhileProbeKeepsReportingALiveWaiter(PublishKind kind)
    {
        using var activities = new AsyncResponseActivityCollector();
        _liveSubscribers = 1;
        _subscriber
            .Setup(instance => instance.PublishAsync(It.IsAny<RedisChannel>(), It.IsAny<RedisValue>(), It.IsAny<CommandFlags>()))
            .ReturnsAsync(0L);
        _store.Setup(store => store.GetAllAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync([NewRecoveryState("corr-contradiction")]);

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => PublishAsync(CreateChannel(), kind, "corr-contradiction"));
        Assert.Contains("found no subscribers twice", exception.Message);

        // Bounded: one re-publish, then the failure surfaces with the registration intact instead
        // of looping or silently dropping the payload.
        _subscriber.Verify(
            instance => instance.PublishAsync(It.IsAny<RedisChannel>(), It.IsAny<RedisValue>(), It.IsAny<CommandFlags>()),
            Times.Exactly(2));
        _store.Verify(store => store.TryDeleteAsync(It.IsAny<string>(), It.IsAny<Guid>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public void Validate_RejectsANegativeRemoteStackTraceCap()
    {
        // Regression: Redis was the only channel accepting a negative MaxRemoteStackTraceLength —
        // RemoteStackTrace.Cap treats any non-positive cap as "leave the trace unchanged", so a
        // plausible "unlimited" (-1, or a bad configuration binding) silently disabled the DoS
        // bound the other four channels reject at startup.
        var options = new RedisAsyncResponseOptions { MaxRemoteStackTraceLength = -1 };

        var ex = Assert.Throws<InvalidOperationException>(options.Validate);
        Assert.Contains(nameof(RedisAsyncResponseOptions.MaxRemoteStackTraceLength), ex.Message);

        // Zero (uncapped, the documented escape hatch) and the defaults stay accepted.
        new RedisAsyncResponseOptions { MaxRemoteStackTraceLength = 0 }.Validate();
        new RedisAsyncResponseOptions().Validate();
    }

    [Theory]
    [InlineData(PublishKind.Response)]
    [InlineData(PublishKind.RawJson)]
    [InlineData(PublishKind.Exception)]
    public async Task Publish_ThrowsAndKeepsRegistration_WhenLivenessCannotBeProbed(PublishKind kind)
    {
        // Regression (r24): the lost-subscriber re-check routed through the public probe, which
        // swallowed every failure into 0 — an UNPROBEABLE endpoint read as "no live waiter", so a
        // live waiter's recovery registration was consumed (and its resume/failure callback
        // fired) during a PUBSUB blip while the waiter was still awaiting. An unprobeable result
        // now propagates: the publish throws, the registration stays intact, and the caller's
        // retry/redelivery machinery re-attempts (DB-channel parity).
        using var activities = new AsyncResponseActivityCollector();
        _subscriber
            .Setup(instance => instance.PublishAsync(It.IsAny<RedisChannel>(), It.IsAny<RedisValue>(), It.IsAny<CommandFlags>()))
            .ReturnsAsync(0L);
        _server.Setup(instance => instance.SubscriptionSubscriberCountAsync(It.IsAny<RedisChannel>(), It.IsAny<CommandFlags>()))
            .ThrowsAsync(new RedisTimeoutException(CommandFlags.None, "probe timed out", CommandStatus.Unknown));
        _store.Setup(store => store.GetAllAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync([NewRecoveryState("corr-unprobeable")]);

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => PublishAsync(CreateChannel(), kind, "corr-unprobeable"));
        Assert.Contains("could not be probed", exception.Message);

        _store.Verify(store => store.TryDeleteAsync(It.IsAny<string>(), It.IsAny<Guid>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task CountActiveSubscribers_ObservesItsCancellationToken()
    {
        // Regression (r24): the probe was fully synchronous and never observed its token, so a
        // watchdog scan over up to MaxScanEntries ids could not be interrupted at shutdown and
        // each probe blocked a thread-pool thread. The async probe checks the token per endpoint.
        var channel = CreateChannel();
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => channel.CountActiveSubscribersAsync("corr-cancelled", cts.Token).AsTask());
    }

    [Theory]
    [InlineData("app[prod]")]
    [InlineData("app*")]
    [InlineData("app?x")]
    [InlineData("app prefix")]
    [InlineData(" ")]
    public void KeyPrefix_WithGlobMetacharactersOrWhitespace_IsRejectedAtConstruction(string prefix)
    {
        // Regression (r24): KeyPrefix was never validated — keys are written literally, but the
        // recovery scan uses the prefix inside SCAN MATCH, where * ? [ ] \ are glob
        // metacharacters: a prefix like app[prod] made the watchdog silently find nothing forever
        // (and a '*' swept a foreign deployment's keys). The RedisKeySchema choke point — which
        // both the channel and the recovery store construct through — now rejects it.
        var exception = Assert.Throws<InvalidOperationException>(() => new RedisKeySchema(prefix));
        Assert.Contains("KeyPrefix", exception.Message);
    }

    private static RecoveryState NewRecoveryState(string correlationId) => new()
    {
        RegistrationId = Guid.NewGuid(),
        CorrelationId = correlationId,
        PayloadTypeFullName = typeof(OperationResult).FullName,
        RegisteredAtUtc = DateTime.UtcNow,
        ResumeCallback = new ReflectionCallDto
        {
            ServiceInterfaceFullName = typeof(IRecoverySpy).FullName!,
            MethodName = nameof(IRecoverySpy.OnResume),
            Params = [CallbackParam.ForPlaceholder(PlaceholderType.Payload)]
        },
        FailureCallback = new ReflectionCallDto
        {
            ServiceInterfaceFullName = typeof(IRecoverySpy).FullName!,
            MethodName = nameof(IRecoverySpy.OnFailure),
            Params = [CallbackParam.ForPlaceholder(PlaceholderType.Exception)]
        }
    };

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

    /// <summary>A disconnected endpoint is skipped rather than probed; nothing probed reads as unknown.</summary>
    [Fact]
    public async Task CountActiveSubscribers_SkipsDisconnectedEndpointsAndSurvivesAProbeFailure()
    {
        var channel = CreateChannel();

        // No endpoint could be probed: negative = "unknown" (the watchdog's could-not-probe
        // contract), never 0 — 0 would assert there is definitively no live waiter.
        _server.Setup(instance => instance.IsConnected).Returns(false);
        Assert.Equal(-1L, await channel.CountActiveSubscribersAsync("corr-disconnected"));

        // A probe that throws is logged and treated as "no information", not propagated.
        _server.Setup(instance => instance.IsConnected).Returns(true);
        _server.Setup(instance => instance.SubscriptionSubscriberCountAsync(It.IsAny<RedisChannel>(), It.IsAny<CommandFlags>()))
            .ThrowsAsync(new RedisTimeoutException(CommandFlags.None, "probe timed out", CommandStatus.Unknown));
        Assert.Equal(-1L, await channel.CountActiveSubscribersAsync("corr-probe-fails"));

        // A blank correlation id short-circuits before any endpoint is touched: definitively zero.
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

    [Theory]
    [InlineData(PublishKind.Response)]
    [InlineData(PublishKind.RawJson)]
    [InlineData(PublishKind.Exception)]
    public async Task RecoveryPublish_BoundsTheExecutorRetirementByDisposalDrainTimeout(PublishKind kind)
    {
        // Regression: after routing a response with no live subscriber through recovery, the
        // publish awaited the correlation id's serial-executor retirement — bounded only by the
        // registry's own 30 s + 30 s defaults. A retirement still draining a work item wedged in a
        // user Until predicate therefore stalled the ingress consumer thread per late/duplicate
        // response for up to a minute, with no configured budget applying.
        _liveSubscribers = 0;
        _subscriber
            .Setup(instance => instance.PublishAsync(It.IsAny<RedisChannel>(), It.IsAny<RedisValue>(), It.IsAny<CommandFlags>()))
            .ReturnsAsync(0L);
        var channel = CreateChannel(disposalDrainTimeout: TimeSpan.FromMilliseconds(200));

        // Wedge the correlation id's executor with a work item that never completes.
        var executors = (SerialExecutorRegistry)typeof(RedisAsyncResponseChannel)
            .GetField("_executors", BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(channel)!;
        var channelName = new RedisKeySchema(new RedisAsyncResponseOptions().KeyPrefix).Channel("corr-wedged").ToString()!;
        Assert.True(await executors.EnqueueAsync(channelName, () => new TaskCompletionSource().Task));

        await PublishAsync(channel, kind, "corr-wedged").WaitAsync(TimeSpan.FromSeconds(5));
    }

    private RedisAsyncResponseChannel CreateChannel(TimeSpan? disposalDrainTimeout = null)
    {
        var options = new RedisAsyncResponseOptions
        {
            DefaultTimeout = TimeSpan.FromSeconds(5),
            RecoveryStateExpiry = TimeSpan.FromMinutes(5)
        };
        if (disposalDrainTimeout is { } drain)
            options.DisposalDrainTimeout = drain;

        return new RedisAsyncResponseChannel(
            _services.GetRequiredService<IServiceScopeFactory>(),
            _multiplexer.Object,
            _store.Object,
            Options.Create(options),
            new AsyncResponseContextPropagation([]),
            NullLogger<RedisAsyncResponseChannel>.Instance,
            new NoopChannelSubscriber());
    }

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
