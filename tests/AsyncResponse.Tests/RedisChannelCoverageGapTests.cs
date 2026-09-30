using AsyncResponse.Channels.Redis;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using StackExchange.Redis;
using System.Collections.Concurrent;
using System.Net;
using System.Reflection;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Coverage-gap tests for the Redis channel: the creator's best-effort compensations after a
/// delivery raced registration, a cleanup unsubscribe that outlives its budget, the overload path
/// once the wait is already settled and the messages queued behind an overload, the liveness
/// probe's connection bookkeeping, and the recovery store's promoted-replica scan and blank blob.
/// </summary>
public sealed class RedisChannelCoverageGapTests
{
    private const string TerminalEnvelope =
        """{"SchemaVersion":1,"Success":true,"Payload":{"Status":2,"Message":"inline"},"ExceptionMessage":null,"ExceptionStackTrace":null}""";

    private static readonly TimeSpan HangGuard = TimeSpan.FromSeconds(10);

    private readonly Mock<IConnectionMultiplexer> _multiplexer = new();
    private readonly Mock<IRecoveryStateStore> _store = new();
    private readonly GapChannelSubscriber _subscriber = new();
    private readonly RecordingThrowingLogger<RedisAsyncResponseChannel> _logger = new();

    public RedisChannelCoverageGapTests()
    {
        _multiplexer.Setup(m => m.GetSubscriber(It.IsAny<object?>())).Returns(new Mock<ISubscriber>().Object);
        _store
            .Setup(s => s.SaveAsync(It.IsAny<string>(), It.IsAny<RecoveryState>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        _store
            .Setup(s => s.TryDeleteAsync(It.IsAny<string>(), It.IsAny<Guid>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);
    }

    // ---------------------------------------------------------------- IRedisChannelSubscriber default

    [Fact]
    public async Task ChannelSubscriberDefault_UnsubscribeAll_IsANoOp()
    {
        IRedisChannelSubscriber subscriber = new GapChannelSubscriber();

        var unsubscribe = subscriber.UnsubscribeAllAsync(RedisChannel.Literal("ar:channel:x"));

        Assert.True(unsubscribe.IsCompletedSuccessfully);
        await unsubscribe;
    }

    // ---------------------------------------------------------------- creator compensations

    public static TheoryData<string, string> CompensationUnsubscribeFailures => new()
    {
        { "hang", "Post-registration unsubscribe for channel" },
        { "throw", "Error during unsubscribe-once for channel" }
    };

    /// <summary>
    /// A terminal delivery pumped inside SubscribeAsync ran cleanup before the subscription was
    /// assigned, so the creator unsubscribes afterwards. That compensation is best-effort: a
    /// subscription dispose that hangs past DisposalDrainTimeout, or throws, is logged and the
    /// completed waiter is still returned.
    /// </summary>
    [Theory]
    [MemberData(nameof(CompensationUnsubscribeFailures))]
    public async Task TerminalDeliveryInsideSubscribe_ACompensatingUnsubscribeThatFails_IsLogged_AndTheWaiterStillReturns(string mode, string expectedError)
    {
        var hang = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _subscriber.DisposeBehavior = mode == "hang"
            ? () => new ValueTask(hang.Task)
            : () => ValueTask.FromException(new RedisConnectionException(ConnectionFailureType.SocketClosed, CommandFlags.None, "UNSUBSCRIBE lost", null, CommandStatus.Unknown));
        _subscriber.InvokeOnSubscribe = TerminalEnvelope;
        _subscriber.AfterInvokeOnSubscribe = WaitForCleanupStartedAsync;
        var channel = CreateChannel(disposalDrainTimeout: TimeSpan.FromMilliseconds(100));

        try
        {
            await using var waiter = await channel.CreateResponseWaiter<OperationResult>("corr-compensate-" + mode).WaitAsync(HangGuard);

            Assert.Equal("inline", (await waiter.ResponseTask).Message);
            Assert.True(_logger.HasEntry(LogLevel.Error, expectedError));
        }
        finally
        {
            hang.TrySetResult();
        }
    }

    /// <summary>
    /// A terminal delivery pumped inside SubscribeAsync settles the wait and cleans up before the
    /// registration is written; the creator must not write a callback-armed registration for a
    /// wait that is already settled.
    /// </summary>
    [Fact]
    public async Task TerminalDeliveryInsideSubscribe_DoesNotSaveARecoveryRegistration()
    {
        _subscriber.InvokeOnSubscribe = TerminalEnvelope;
        _subscriber.AfterInvokeOnSubscribe = WaitForCleanupStartedAsync;
        var channel = CreateChannel();

        await using var waiter = await channel.CreateResponseWaiter<OperationResult>("corr-no-save-after-settle").WaitAsync(HangGuard);

        Assert.Equal("inline", (await waiter.ResponseTask).Message);
        _store.Verify(s => s.SaveAsync(It.IsAny<string>(), It.IsAny<RecoveryState>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    /// <summary>
    /// A terminal delivery landing while the recovery registration is being saved orphans it; the
    /// creator's compensating delete is best-effort, so its failure is logged (the registration
    /// then expires by TTL) and the completed waiter is returned.
    /// </summary>
    [Fact]
    public async Task TerminalDeliveryDuringSave_ACompensatingDeleteThatFails_IsLogged_AndTheWaiterStillReturns()
    {
        var saveStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var cleanupDeleteIssued = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var deletes = 0;
        _store
            .Setup(s => s.SaveAsync(It.IsAny<string>(), It.IsAny<RecoveryState>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
            .Returns(async () =>
            {
                saveStarted.TrySetResult();
                await cleanupDeleteIssued.Task;
            });
        _store
            .Setup(s => s.TryDeleteAsync(It.IsAny<string>(), It.IsAny<Guid>(), It.IsAny<CancellationToken>()))
            .Returns(() =>
            {
                if (Interlocked.Increment(ref deletes) == 1)
                {
                    cleanupDeleteIssued.TrySetResult(); // cleanup's pre-save delete
                    return Task.FromResult(true);
                }

                return Task.FromException<bool>(new InvalidOperationException("delete failed"));
            });
        var channel = CreateChannel();

        var waiterTask = channel.CreateResponseWaiter<OperationResult>("corr-compensating-delete");
        await saveStarted.Task.WaitAsync(HangGuard);
        await _subscriber.Handler!(_subscriber.SubscribedChannel, TerminalEnvelope);

        await using var waiter = await waiterTask.WaitAsync(HangGuard);

        Assert.Equal(OperationStatus.Completed, (await waiter.ResponseTask).Status);
        Assert.Equal(2, Volatile.Read(ref deletes));
        Assert.True(_logger.HasEntry(LogLevel.Error, "Post-save recovery-state compensation delete failed for correlationId corr-compensating-delete"));
    }

    // ---------------------------------------------------------------- cleanup

    [Fact]
    public async Task Cleanup_AnUnsubscribeThatOutlivesTheDrainTimeout_IsAbandonedWithAnError()
    {
        var hang = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _subscriber.DisposeBehavior = () => new ValueTask(hang.Task);
        var channel = CreateChannel(disposalDrainTimeout: TimeSpan.FromMilliseconds(100));

        try
        {
            await using var waiter = await channel.CreateResponseWaiter<OperationResult>("corr-hung-unsubscribe");
            await _subscriber.Handler!(_subscriber.SubscribedChannel, TerminalEnvelope);

            Assert.Equal("inline", (await waiter.ResponseTask.WaitAsync(HangGuard)).Message);
            await Eventually(() => _logger.HasEntry(LogLevel.Error, "did not finish within"));
            Assert.Equal(1, _subscriber.Disposals);
        }
        finally
        {
            hang.TrySetResult();
        }
    }

    // ---------------------------------------------------------------- overload

    /// <summary>
    /// Once an overload has faulted the wait, a further message that finds the executor full is a
    /// straggler (dropped, Debug), and a message that was already queued behind the overload is
    /// skipped when it finally runs — its predicate is never called.
    /// </summary>
    [Fact]
    public async Task Overload_AfterTheWaitSettled_DropsStragglers_AndSkipsTheMessagesQueuedBehindIt()
    {
        var channel = CreateChannel(defaultTimeout: TimeSpan.FromSeconds(30));
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var wedged = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var predicateCalls = new ConcurrentQueue<string?>();
        await using var waiter = await channel.CreateResponseWaiter<OperationResult>(
            "corr-overload-gap",
            completionPredicate: async result =>
            {
                predicateCalls.Enqueue(result.Message);
                if (result.Message == "running")
                {
                    started.TrySetResult();
                    await wedged.Task;
                }

                return false;
            },
            timeout: TimeSpan.FromSeconds(30));

        try
        {
            await Publish("running");
            await started.Task.WaitAsync(HangGuard);
            await Publish("queued"); // a real message, queued behind the wedged predicate

            // Fill the rest of the bounded queue so the next delivery finds it full.
            var registry = (SerialExecutorRegistry)typeof(RedisAsyncResponseChannel)
                .GetField("_executors", BindingFlags.Instance | BindingFlags.NonPublic)!
                .GetValue(channel)!;
            var channelName = _subscriber.SubscribedChannel.ToString()!;
            for (var i = 0; i < ExecutorCapacity - 1; i++)
                Assert.Equal(SerialExecutorRegistry.TryEnqueueOutcome.Accepted, registry.TryEnqueue(channelName, () => Task.CompletedTask));

            await Publish("overflow"); // faults the wait as overloaded
            await Assert.ThrowsAsync<AsyncResponseIndeterminateDeliveryException>(() => waiter.ResponseTask.WaitAsync(HangGuard));

            await Publish("straggler"); // the executor is still full: the settled wait drops it
            Assert.True(_logger.HasEntry(LogLevel.Debug, "Dropped a late message on channel"));
        }
        finally
        {
            wedged.TrySetResult();
        }

        await Eventually(() => _logger.HasEntry(LogLevel.Debug, "Dropped a queued message on channel"));
        Assert.Equal(new string?[] { "running" }, predicateCalls.ToArray());
        Assert.Equal(1, _logger.CountEntries(LogLevel.Error, "is overloaded"));
    }

    // ---------------------------------------------------------------- timeout

    /// <summary>
    /// Telemetry cannot change an outcome (round 48): the timeout's cleanup ends the waiter's
    /// <c>asyncresponse.wait</c> span, and a listener whose stopped callback threw used to fault
    /// that one-shot cleanup — logged as a failed timeout, and rethrown by every later dispose of
    /// the waiter, because the faulted cleanup task is cached.
    /// </summary>
    [Fact]
    public async Task Timeout_WithAThrowingWaitSpanListener_TimesOut_AndEveryLaterDisposeSucceeds()
    {
        using var telemetry = new ThrowingSpanStopListener();
        var clock = new AsyncResponse.Testing.VirtualTimeProvider();
        var channel = new RedisAsyncResponseChannel(
            new ServiceCollection().BuildServiceProvider().GetRequiredService<IServiceScopeFactory>(),
            _multiplexer.Object,
            _store.Object,
            Options.Create(new RedisAsyncResponseOptions { DefaultTimeout = TimeSpan.FromSeconds(5), RecoveryStateExpiry = TimeSpan.FromMinutes(5) }),
            new AsyncResponseContextPropagation([]),
            _logger,
            _subscriber,
            clock);

        var waiter = await channel.CreateResponseWaiter<OperationResult>("corr-timeout-cleanup-fault", timeout: TimeSpan.FromMinutes(10));
        var guard = DateTime.UtcNow + HangGuard;
        while (!waiter.ResponseTask.IsCompleted)
        {
            Assert.True(DateTime.UtcNow < guard, "advancing the virtual clock never fired the waiter timeout");
            clock.Advance(TimeSpan.FromMinutes(11));
            await Task.Delay(5);
        }

        await Assert.ThrowsAsync<TimeoutException>(() => waiter.ResponseTask);
        await waiter.DisposeAsync().AsTask().WaitAsync(HangGuard);
        await waiter.DisposeAsync().AsTask().WaitAsync(HangGuard);
        Assert.Equal(1, telemetry.Thrown);
        Assert.False(_logger.HasEntry(LogLevel.Error, "Error handling waiter timeout for correlationId corr-timeout-cleanup-fault"));
    }

    // ---------------------------------------------------------------- liveness probe bookkeeping

    [Fact]
    public async Task ConnectionFailed_OnANonInteractiveConnection_DoesNotStartAFailoverGrace()
    {
        var clock = new AsyncResponse.Testing.VirtualTimeProvider();
        var answering = StandaloneServer("10.0.0.1", connected: true, subscribers: 0);
        var failing = StandaloneServer("10.0.0.2", connected: true, subscribers: 0);
        var channel = CreateProbeChannel(clock, answering, failing);
        failing.SetupGet(s => s.IsConnected).Returns(false);

        // The subscription connection dropping says nothing about the interactive one the probe asks.
        RaiseConnectionFailed(failing, ConnectionType.Subscription);
        Assert.Equal(0, await channel.CountActiveSubscribersAsync("corr"));

        // The interactive one does: within the grace the zero is unknown.
        RaiseConnectionFailed(failing, ConnectionType.Interactive);
        Assert.Equal(-1, await channel.CountActiveSubscribersAsync("corr"));
    }

    [Fact]
    public async Task ConnectionFailed_WhenTheMultiplexerThrowsOnTheReconnectCheck_KeepsTheDisconnection()
    {
        var clock = new AsyncResponse.Testing.VirtualTimeProvider();
        var answering = StandaloneServer("10.0.0.1", connected: true, subscribers: 0);
        var failing = StandaloneServer("10.0.0.2", connected: true, subscribers: 0);
        var channel = CreateProbeChannel(clock, answering, failing);
        var failingEndPoint = failing.Object.EndPoint!;
        failing.SetupGet(s => s.IsConnected).Returns(false);

        // The "is it back already?" check inside the event handler must never throw out of it.
        _multiplexer.Setup(m => m.GetServer(failingEndPoint, It.IsAny<object?>())).Throws(new ObjectDisposedException("multiplexer"));
        RaiseConnectionFailed(failing, ConnectionType.Interactive);
        _multiplexer.Setup(m => m.GetServer(failingEndPoint, It.IsAny<object?>())).Returns(failing.Object);

        // Treated as still down: the disconnection's grace applies.
        Assert.Equal(-1, await channel.CountActiveSubscribersAsync("corr"));
        clock.Advance(RedisAsyncResponseChannel.DisconnectedEndPointGrace);
        Assert.Equal(0, await channel.CountActiveSubscribersAsync("corr"));
    }

    [Fact]
    public async Task CountActiveSubscribers_ForgetsEndPointsTheMultiplexerNoLongerLists()
    {
        var kept = StandaloneServer("10.0.0.1", connected: true, subscribers: 3);
        var dropped = StandaloneServer("10.0.0.2", connected: true, subscribers: 0);
        var channel = CreateProbeChannel(timeProvider: null, kept, dropped);
        var tracked = (ConcurrentDictionary<EndPoint, long>)typeof(RedisAsyncResponseChannel)
            .GetField("_endPointDownSince", BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(channel)!;
        Assert.Equal(2, tracked.Count);

        _multiplexer.Setup(m => m.GetEndPoints(It.IsAny<bool>())).Returns([kept.Object.EndPoint!]);

        Assert.Equal(3, await channel.CountActiveSubscribersAsync("corr"));
        Assert.True(tracked.ContainsKey(kept.Object.EndPoint!));
        Assert.False(tracked.ContainsKey(dropped.Object.EndPoint!));
    }

    // ---------------------------------------------------------------- recovery store

    /// <summary>
    /// After a failover the promoted node can still carry its pre-failover "replica" flag: a
    /// connected node the cluster table names as a slot owner is scanned whatever that flag says
    /// (a connected replica the table does not list as an owner is not).
    /// </summary>
    [Fact]
    public async Task Store_ClusterScan_PromotesAConnectedReplicaTheTableListsAsASlotOwner()
    {
        const string failedOverTable =
            "07c37dfeb235213a872192d90877d0cd55635b91 10.0.0.1:6379@16379 myself,master - 0 0 1 connected 0-8191\n" +
            "67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1 10.0.0.2:6379@16379 master,fail - 0 1426238316232 2 disconnected\n" +
            "292f8b365bb7edb5e285caf0b7e6ddc7265d2f4f 10.0.0.7:6379@16379 master - 0 1426238317741 3 connected 8192-16383\n" +
            "5b2f8b365bb7edb5e285caf0b7e6ddc7265d2f4f 10.0.0.8:6379@16379 slave 07c37dfeb235213a872192d90877d0cd55635b91 0 1426238317741 1 connected\n";
        var multiplexer = new Mock<IConnectionMultiplexer>();
        var database = new Mock<IDatabase>();
        multiplexer.Setup(m => m.GetDatabase(It.IsAny<int>(), It.IsAny<object?>())).Returns(database.Object);
        var shardA = ClusterServer("10.0.0.1", connected: true, replica: false, "ar:recovery:corr-a");
        shardA.Setup(s => s.ClusterNodesRawAsync(It.IsAny<CommandFlags>())).ReturnsAsync(failedOverTable);
        var oldOwner = ClusterServer("10.0.0.2", connected: false, replica: false);
        var promoted = ClusterServer("10.0.0.7", connected: true, replica: true, "ar:recovery:corr-b"); // stale flag
        var replicaOfA = ClusterServer("10.0.0.8", connected: true, replica: true, "ar:recovery:corr-a");
        var servers = new[] { shardA, oldOwner, promoted, replicaOfA };
        var endPoints = servers.Select(server => server.Object.EndPoint!).ToArray();
        multiplexer.Setup(m => m.GetEndPoints(It.IsAny<bool>())).Returns(endPoints);
        multiplexer
            .Setup(m => m.GetServer(It.IsAny<EndPoint>(), It.IsAny<object?>()))
            .Returns<EndPoint, object?>((endPoint, _) => servers[Array.IndexOf(endPoints, endPoint)].Object);
        var time = new TestTimeProvider();
        foreach (var correlationId in new[] { "corr-a", "corr-b" })
        {
            database
                .Setup(d => d.StringGetAsync((RedisKey)$"ar:recovery:{correlationId}", It.IsAny<CommandFlags>()))
                .ReturnsAsync(EnvelopeBlob(correlationId, time.Now + TimeSpan.FromMinutes(10)));
        }

        var store = new RedisRecoveryStateStore(
            multiplexer.Object,
            Options.Create(new RedisAsyncResponseOptions { KeyPrefix = "ar" }),
            NullLogger<RedisRecoveryStateStore>.Instance,
            time);

        var states = new List<RecoveryState>();
        await foreach (var state in store.ScanAsync())
            states.Add(state);

        Assert.Equal(["corr-a", "corr-b"], states.Select(state => state.CorrelationId).OrderBy(id => id, StringComparer.Ordinal));
        promoted.Verify(s => s.KeysAsync(It.IsAny<int>(), It.IsAny<RedisValue>(), It.IsAny<int>(), It.IsAny<long>(), It.IsAny<int>(), It.IsAny<CommandFlags>()), Times.Once);
        replicaOfA.Verify(s => s.KeysAsync(It.IsAny<int>(), It.IsAny<RedisValue>(), It.IsAny<int>(), It.IsAny<long>(), It.IsAny<int>(), It.IsAny<CommandFlags>()), Times.Never);
    }

    /// <summary>A blob of only whitespace is neither shape: it is unreadable, never "no registrations".</summary>
    [Fact]
    public async Task Store_AWhitespaceOnlyBlob_IsUnreadable()
    {
        var multiplexer = new Mock<IConnectionMultiplexer>();
        var database = new Mock<IDatabase>();
        multiplexer.Setup(m => m.GetDatabase(It.IsAny<int>(), It.IsAny<object?>())).Returns(database.Object);
        database
            .Setup(d => d.StringGetAsync((RedisKey)"ar:recovery:corr-blank", It.IsAny<CommandFlags>()))
            .ReturnsAsync((RedisValue)"   \n\t ");
        var logger = new RecordingThrowingLogger<RedisRecoveryStateStore>();
        var store = new RedisRecoveryStateStore(
            multiplexer.Object,
            Options.Create(new RedisAsyncResponseOptions { KeyPrefix = "ar" }),
            logger,
            new TestTimeProvider());

        await Assert.ThrowsAsync<RecoveryStateUnreadableException>(() => store.GetAllAsync("corr-blank"));
        Assert.True(logger.HasEntry(LogLevel.Error, "Failed to deserialize recovery state at ar:recovery:corr-blank"));
    }

    // ---------------------------------------------------------------- helpers

    private const int ExecutorCapacity = 1024;

    private RedisAsyncResponseChannel CreateChannel(TimeSpan? disposalDrainTimeout = null, TimeSpan? defaultTimeout = null)
    {
        var options = new RedisAsyncResponseOptions
        {
            DefaultTimeout = defaultTimeout ?? TimeSpan.FromSeconds(5),
            RecoveryStateExpiry = TimeSpan.FromMinutes(5)
        };
        if (disposalDrainTimeout is { } drain)
            options.DisposalDrainTimeout = drain;

        return new RedisAsyncResponseChannel(
            new ServiceCollection().BuildServiceProvider().GetRequiredService<IServiceScopeFactory>(),
            _multiplexer.Object,
            _store.Object,
            Options.Create(options),
            new AsyncResponseContextPropagation([]),
            _logger,
            _subscriber);
    }

    private RedisAsyncResponseChannel CreateProbeChannel(TimeProvider? timeProvider, params Mock<IServer>[] servers)
    {
        var endPoints = servers.Select(server => server.Object.EndPoint!).ToArray();
        _multiplexer.Setup(m => m.GetEndPoints(It.IsAny<bool>())).Returns(endPoints);
        foreach (var server in servers)
            _multiplexer.Setup(m => m.GetServer(server.Object.EndPoint!, It.IsAny<object?>())).Returns(server.Object);

        return new RedisAsyncResponseChannel(
            new ServiceCollection().BuildServiceProvider().GetRequiredService<IServiceScopeFactory>(),
            _multiplexer.Object,
            _store.Object,
            Options.Create(new RedisAsyncResponseOptions()),
            new AsyncResponseContextPropagation([]),
            new TestLogger<RedisAsyncResponseChannel>(),
            _subscriber,
            timeProvider);
    }

    private static Mock<IServer> StandaloneServer(string address, bool connected, long subscribers)
    {
        var server = new Mock<IServer>();
        server.SetupGet(s => s.IsConnected).Returns(connected);
        server.SetupGet(s => s.ServerType).Returns(ServerType.Standalone);
        server.SetupGet(s => s.EndPoint).Returns(new IPEndPoint(IPAddress.Parse(address), 6379));
        server.Setup(s => s.SubscriptionSubscriberCountAsync(It.IsAny<RedisChannel>(), It.IsAny<CommandFlags>()))
            .ReturnsAsync(subscribers);
        return server;
    }

    private static Mock<IServer> ClusterServer(string address, bool connected, bool replica, params string[] keys)
    {
        var server = new Mock<IServer>();
        server.SetupGet(s => s.IsConnected).Returns(connected);
        server.SetupGet(s => s.IsReplica).Returns(replica);
        server.SetupGet(s => s.ServerType).Returns(ServerType.Cluster);
        server.SetupGet(s => s.EndPoint).Returns(new IPEndPoint(IPAddress.Parse(address), 6379));
        var redisKeys = keys.Select(key => (RedisKey)key).ToArray();
        server
            .Setup(s => s.Keys(It.IsAny<int>(), It.IsAny<RedisValue>(), It.IsAny<int>(), It.IsAny<long>(), It.IsAny<int>(), It.IsAny<CommandFlags>()))
            .Returns(redisKeys);
        server
            .Setup(s => s.KeysAsync(It.IsAny<int>(), It.IsAny<RedisValue>(), It.IsAny<int>(), It.IsAny<long>(), It.IsAny<int>(), It.IsAny<CommandFlags>()))
            .Returns(() => ToAsyncEnumerable(redisKeys));
        return server;
    }

    private static async IAsyncEnumerable<RedisKey> ToAsyncEnumerable(RedisKey[] keys)
    {
        foreach (var key in keys)
        {
            await Task.Yield();
            yield return key;
        }
    }

    private static string EnvelopeBlob(string correlationId, DateTimeOffset expiresAtUtc)
        => "{\"Registrations\":[{\"State\":"
           + System.Text.Json.JsonSerializer.Serialize(new RecoveryState { RegistrationId = Guid.NewGuid(), CorrelationId = correlationId })
           + $",\"ExpiresAtUtc\":\"{expiresAtUtc:O}\"}}]}}";

    private void RaiseConnectionFailed(Mock<IServer> server, ConnectionType connectionType)
        => _multiplexer.Raise(
            m => m.ConnectionFailed += null,
            new ConnectionFailedEventArgs(_multiplexer.Object, server.Object.EndPoint!, connectionType, ConnectionFailureType.SocketClosed, new InvalidOperationException("connection lost"), "test"));

    private Task Publish(string message)
        => _subscriber.Handler!(
            _subscriber.SubscribedChannel,
            $$"""{"SchemaVersion":1,"Success":true,"Payload":{"Status":2,"Message":"{{message}}"},"ExceptionMessage":null,"ExceptionStackTrace":null}""");

    /// <summary>
    /// Holds SubscribeAsync until the inline delivery's cleanup has started (a one-way flag), so
    /// the creator deterministically takes its post-assignment compensation branch.
    /// </summary>
    private static async Task WaitForCleanupStartedAsync(object subscription)
    {
        var cleanupStarted = subscription.GetType().GetProperty("CleanupStarted")!;
        var guard = DateTime.UtcNow + HangGuard;
        while (!(bool)cleanupStarted.GetValue(subscription)!)
        {
            Assert.True(DateTime.UtcNow < guard, "CleanupStarted never became true before SubscribeAsync returned");
            await Task.Yield();
        }
    }

    private static async Task Eventually(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow + HangGuard;
        while (!condition())
        {
            if (DateTime.UtcNow >= deadline)
                throw new TimeoutException("Condition was not satisfied before the timeout.");

            await Task.Delay(10);
        }
    }

    /// <summary>
    /// Subscribe seam fake that leaves <see cref="IRedisChannelSubscriber.UnsubscribeAllAsync"/> to
    /// its default; each subscription's dispose runs <see cref="DisposeBehavior"/>.
    /// </summary>
    private sealed class GapChannelSubscriber : IRedisChannelSubscriber
    {
        private int _disposals;

        public RedisChannel SubscribedChannel { get; private set; }
        public Func<RedisChannel, RedisValue, Task>? Handler { get; private set; }
        public RedisValue? InvokeOnSubscribe { get; set; }
        public Func<object, Task>? AfterInvokeOnSubscribe { get; set; }
        public Func<ValueTask>? DisposeBehavior { get; set; }
        public int Disposals => Volatile.Read(ref _disposals);

        public async Task<IRedisChannelSubscription> SubscribeAsync(RedisChannel channel, Func<RedisChannel, RedisValue, Task> onMessage)
        {
            SubscribedChannel = channel;
            Handler = onMessage;
            if (InvokeOnSubscribe is { } message)
            {
                await onMessage(channel, message);
                if (AfterInvokeOnSubscribe is { } hook)
                    await hook(onMessage.Target!);
            }

            return new Subscription(this);
        }

        private sealed class Subscription(GapChannelSubscriber owner) : IRedisChannelSubscription
        {
            public ValueTask DisposeAsync()
            {
                Interlocked.Increment(ref owner._disposals);
                return owner.DisposeBehavior?.Invoke() ?? ValueTask.CompletedTask;
            }
        }
    }
}
