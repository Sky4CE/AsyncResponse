using AsyncResponse.Channels.NATS;
using AsyncResponse.Testing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using NATS.Client.KeyValueStore;
using System.Text.Json;
using System.Threading.Channels;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Coverage-gap tests for the NATS channel: the registration-timeout path whose subscribe or
/// registration delete outlives it, a consume loop that faults outside its own guard, a drop
/// reported after the wait settled, the creator's failed compensating delete, the KV adapter's
/// conflict/rejection and lost-race arms, the drop router's isolation, and the recovery store's
/// single-flight delete-marker purge.
/// </summary>
public sealed class NatsChannelCoverageGapTests
{
    private static readonly TimeSpan HangGuard = TimeSpan.FromSeconds(10);

    private readonly FakeNatsResponseChannelClient _client = new();
    private readonly Mock<IRecoveryStateStore> _store = new();
    private readonly RecordingThrowingLogger<NatsAsyncResponseChannel> _logger = new();

    public NatsChannelCoverageGapTests()
    {
        _store.Setup(s => s.SaveAsync(It.IsAny<string>(), It.IsAny<RecoveryState>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        _store.Setup(s => s.TryDeleteAsync(It.IsAny<string>(), It.IsAny<Guid>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);
    }

    // ---------------------------------------------------------------- registration timeout

    /// <summary>
    /// The registration budget lapses while the subscribe is still waiting (a reconnecting
    /// connection) and the abandoned registration's delete then outlives the drain budget: the
    /// cleanup waits on it a while longer (Debug), and the subscribe that completes after all is
    /// disposed rather than left installing orphan interest.
    /// </summary>
    [Fact]
    public async Task RegistrationTimeout_ALateSubscribeIsDisposed_AndADeleteThatOutlivesTheDrainIsWaitedOnLonger()
    {
        var clock = new VirtualTimeProvider();
        var subscribeEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var subscribeGate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var deleteGate = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        _client.SubscribeBehavior = async _ =>
        {
            subscribeEntered.TrySetResult();
            await subscribeGate.Task; // ignores its token: completes after the waiter gave up
        };
        _store.Setup(s => s.TryDeleteAsync(It.IsAny<string>(), It.IsAny<Guid>(), It.IsAny<CancellationToken>()))
            .Returns(deleteGate.Task);
        var channel = CreateChannel(drainTimeout: TimeSpan.FromMilliseconds(100), timeProvider: clock);

        try
        {
            var waiterTask = channel.CreateResponseWaiter<OperationResult>("corr-late-subscribe", timeout: TimeSpan.FromSeconds(30));
            await subscribeEntered.Task.WaitAsync(HangGuard);
            clock.Advance(TimeSpan.FromSeconds(31));

            await Assert.ThrowsAsync<TimeoutException>(() => waiterTask.WaitAsync(HangGuard));
            await Eventually(() => _logger.HasEntry(LogLevel.Debug, "the cleanup waits on it a while longer"));
            Assert.Equal(0, _client.SubscriptionDisposeCount);

            subscribeGate.TrySetResult();
            await Eventually(() => _client.SubscriptionDisposeCount == 1);
        }
        finally
        {
            subscribeGate.TrySetResult();
            deleteGate.TrySetResult(true);
        }

        _store.Verify(s => s.SaveAsync(It.IsAny<string>(), It.IsAny<RecoveryState>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    /// <summary>
    /// A subscribe that is cancelled synchronously, once the registration budget has already
    /// lapsed, hands back no pending subscription: the registration still ends as a timeout, with
    /// nothing left to dispose.
    /// </summary>
    [Fact]
    public async Task RegistrationTimeout_ASubscribeCancelledSynchronously_EndsAsATimeoutWithNothingToDispose()
    {
        var clock = new VirtualTimeProvider();
        var client = new Mock<INatsResponseChannelClient>();
        client.Setup(c => c.SubscribeAsync(It.IsAny<string>(), It.IsAny<Action<int>>(), It.IsAny<CancellationToken>()))
            .Callback(() => clock.Advance(TimeSpan.FromSeconds(31)))
            .Throws(new OperationCanceledException("the connection was closed while subscribing"));
        var channel = new NatsAsyncResponseChannel(
            new ServiceCollection().BuildServiceProvider().GetRequiredService<IServiceScopeFactory>(),
            client.Object,
            _store.Object,
            Options.Create(new NatsAsyncResponseChannelOptions { DefaultTimeout = TimeSpan.FromSeconds(5) }),
            new AsyncResponseContextPropagation([]),
            _logger,
            clock);

        var timeout = await Assert.ThrowsAsync<TimeoutException>(() =>
            channel.CreateResponseWaiter<OperationResult>("corr-sync-cancel", timeout: TimeSpan.FromSeconds(30)).WaitAsync(HangGuard));

        Assert.Contains("corr-sync-cancel", timeout.Message, StringComparison.Ordinal);
        Assert.IsAssignableFrom<OperationCanceledException>(timeout.InnerException);
        client.Verify(c => c.FlushAsync(It.IsAny<CancellationToken>()), Times.Never);
        _store.Verify(s => s.SaveAsync(It.IsAny<string>(), It.IsAny<RecoveryState>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    /// <summary>
    /// Telemetry cannot change an outcome (round 48): the cleanup ends the waiter's
    /// <c>asyncresponse.wait</c> span, and a listener whose stopped callback threw used to fault
    /// that one-shot cleanup — cached, so every dispose of the waiter rethrew the listener's error.
    /// </summary>
    [Fact]
    public async Task Dispose_WithAThrowingWaitSpanListener_Succeeds_EveryTime()
    {
        using var telemetry = new ThrowingSpanStopListener();
        var channel = CreateChannel();
        var waiter = await channel.CreateResponseWaiter<OperationResult>("corr-span-dispose", timeout: TimeSpan.FromSeconds(30));

        await waiter.DisposeAsync().AsTask().WaitAsync(HangGuard);
        await waiter.DisposeAsync().AsTask().WaitAsync(HangGuard);

        Assert.Equal(1, telemetry.Thrown);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => waiter.ResponseTask.WaitAsync(HangGuard));
    }

    // ---------------------------------------------------------------- consume loop / overload

    // Logging cannot change an outcome: every log line on the response path goes through SafeLog.
    // A throwing logging provider in the consume loop's catch used to end the loop before it failed
    // the waiter, so a dead subscription surfaced only at the timeout, as indeterminate; one in the
    // message path replaced the response's own outcome with the logger's exception.

    [Fact]
    public async Task ConsumeLoopFailure_WithAThrowingLogger_FaultsTheWaiterAtOnce_WithTheSubscriptionError()
    {
        var logger = new RecordingThrowingLogger<NatsAsyncResponseChannel> { ThrowOnMessageContaining = "Response subscription loop failed" };
        var channel = CreateChannel(logger: logger);
        var waiter = await channel.CreateResponseWaiter<OperationResult>("corr-loop-fault", timeout: TimeSpan.FromMinutes(10));

        _client.FailSubscription(new InvalidOperationException("connection reset"));

        var failure = await Assert.ThrowsAsync<InvalidOperationException>(() => waiter.ResponseTask.WaitAsync(HangGuard));
        Assert.Equal("connection reset", failure.Message);
        await waiter.DisposeAsync().AsTask().WaitAsync(HangGuard);
    }

    [Fact]
    public async Task ErrorResponse_WithAThrowingLogger_FaultsTheWaiterWithTheRemoteFailure()
    {
        var logger = new RecordingThrowingLogger<NatsAsyncResponseChannel> { ThrowOnMessageContaining = "Received error response" };
        var channel = CreateChannel(logger: logger);
        var waiter = await channel.CreateResponseWaiter<OperationResult>("corr-error-log", timeout: TimeSpan.FromMinutes(10));

        _client.Push(JsonSerializer.Serialize(
            new AsyncResponseEnvelope<OperationResult> { Success = false, ExceptionMessage = "remote boom" },
            AsyncResponseEnvelopeOptions<OperationResult>.Instance));

        var failure = await Assert.ThrowsAsync<Exception>(() => waiter.ResponseTask.WaitAsync(HangGuard));
        Assert.Equal("remote boom", failure.Message);
        await waiter.DisposeAsync().AsTask().WaitAsync(HangGuard);
    }

    [Fact]
    public async Task Response_WithAThrowingDebugLogger_StillCompletesTheWaiterWithThePayload()
    {
        var logger = new RecordingThrowingLogger<NatsAsyncResponseChannel> { ThrowOnMessageContaining = "Received response for correlationId" };
        var channel = CreateChannel(logger: logger);
        var waiter = await channel.CreateResponseWaiter<OperationResult>("corr-debug-log", timeout: TimeSpan.FromMinutes(10));

        _client.Push(Envelope("done"));

        Assert.Equal("done", (await waiter.ResponseTask.WaitAsync(HangGuard)).Message);
        await waiter.DisposeAsync().AsTask().WaitAsync(HangGuard);
    }

    [Fact]
    public async Task EmptyMessage_WithAThrowingLogger_IsIgnored_AndTheNextResponseCompletesTheWaiter()
    {
        var logger = new RecordingThrowingLogger<NatsAsyncResponseChannel> { ThrowOnMessageContaining = "Received empty response message" };
        var channel = CreateChannel(logger: logger);
        var waiter = await channel.CreateResponseWaiter<OperationResult>("corr-empty-log", timeout: TimeSpan.FromMinutes(10));

        _client.Push(payload: null);
        _client.Push(Envelope("after-empty"));

        Assert.Equal("after-empty", (await waiter.ResponseTask.WaitAsync(HangGuard)).Message);
        await waiter.DisposeAsync().AsTask().WaitAsync(HangGuard);
    }

    [Fact]
    public async Task MessagesDropped_AfterTheWaitSettled_ChangesNothing()
    {
        var channel = CreateChannel();
        await using var waiter = await channel.CreateResponseWaiter<OperationResult>("corr-drop-after", timeout: TimeSpan.FromSeconds(30));
        _client.Push(Envelope("done"));
        Assert.Equal("done", (await waiter.ResponseTask.WaitAsync(HangGuard)).Message);

        _client.DropMessage(buffered: 7);

        Assert.True(waiter.ResponseTask.IsCompletedSuccessfully);
        Assert.False(_logger.HasEntry(LogLevel.Warning, "is overloaded"));
    }

    // ---------------------------------------------------------------- compensation

    [Fact]
    public async Task TerminalDeliveryDuringSave_ACompensatingDeleteThatFails_IsLogged_AndTheWaiterStillReturns()
    {
        var saveStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var cleanupDeleteIssued = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var deletes = 0;
        _store.Setup(s => s.SaveAsync(It.IsAny<string>(), It.IsAny<RecoveryState>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
            .Returns(async () =>
            {
                saveStarted.TrySetResult();
                await cleanupDeleteIssued.Task;
            });
        _store.Setup(s => s.TryDeleteAsync(It.IsAny<string>(), It.IsAny<Guid>(), It.IsAny<CancellationToken>()))
            .Returns(() =>
            {
                if (Interlocked.Increment(ref deletes) == 1)
                {
                    cleanupDeleteIssued.TrySetResult(); // cleanup's delete, before the save lands
                    return Task.FromResult(true);
                }

                return Task.FromException<bool>(new InvalidOperationException("delete failed"));
            });
        var channel = CreateChannel();

        var waiterTask = channel.CreateResponseWaiter<OperationResult>("corr-nats-compensate");
        await saveStarted.Task.WaitAsync(HangGuard);
        _client.Push(Envelope("settled"));

        await using var waiter = await waiterTask.WaitAsync(HangGuard);

        Assert.Equal("settled", (await waiter.ResponseTask).Message);
        Assert.Equal(2, Volatile.Read(ref deletes));
        Assert.True(_logger.HasEntry(LogLevel.Error, "Post-save recovery-state compensation delete failed for correlationId corr-nats-compensate"));
    }

    [Fact]
    public async Task OverloadDuringSave_AThrowingLoggerOnTheFailedCompensation_DoesNotReplaceTheWaiterWithTheLoggersFault()
    {
        // r3/R3-07: the compensation's LogError was unguarded (Redis wraps it in SafeLog). An
        // overload fault is not a "delivery" (SettledByDelivery is false), so a throwing provider
        // escaped the outer filter and CreateResponseWaiter threw the logger's exception.
        _logger.ThrowOnMessageContaining = "compensation delete failed";
        var saveStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var cleanupDeleteIssued = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var deletes = 0;
        _store.Setup(s => s.SaveAsync(It.IsAny<string>(), It.IsAny<RecoveryState>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
            .Returns(async () =>
            {
                saveStarted.TrySetResult();
                await cleanupDeleteIssued.Task;
            });
        _store.Setup(s => s.TryDeleteAsync(It.IsAny<string>(), It.IsAny<Guid>(), It.IsAny<CancellationToken>()))
            .Returns(() =>
            {
                if (Interlocked.Increment(ref deletes) == 1)
                {
                    cleanupDeleteIssued.TrySetResult();
                    return Task.FromResult(true);
                }

                return Task.FromException<bool>(new InvalidOperationException("delete failed"));
            });
        var channel = CreateChannel();

        var waiterTask = channel.CreateResponseWaiter<OperationResult>("corr-nats-compensate-log");
        await saveStarted.Task.WaitAsync(HangGuard);
        _client.DropMessage(buffered: 16_384);

        await using var waiter = await waiterTask.WaitAsync(HangGuard);
        await Assert.ThrowsAsync<AsyncResponseIndeterminateDeliveryException>(() => waiter.ResponseTask);
    }

    [Fact]
    public async Task TerminalDeliveryBeforeSave_SkipsTheSaveAndItsCompensatingDelete()
    {
        // r3/R3-06: cleanup started before the save, so the save is skipped; the compensating
        // delete (unbounded, tokenless) for a row never written was a wasted KV round trip.
        var cleanupDeleteIssued = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var deletes = 0;
        _client.FlushBehavior = async _ =>
        {
            _client.Push(Envelope("early"));
            await cleanupDeleteIssued.Task.WaitAsync(HangGuard);
        };
        _store.Setup(s => s.TryDeleteAsync(It.IsAny<string>(), It.IsAny<Guid>(), It.IsAny<CancellationToken>()))
            .Returns(() =>
            {
                if (Interlocked.Increment(ref deletes) == 1)
                {
                    cleanupDeleteIssued.TrySetResult();
                    return Task.FromResult(true);
                }

                return Task.FromException<bool>(new InvalidOperationException("delete failed"));
            });
        var channel = CreateChannel();

        await using var waiter = await channel.CreateResponseWaiter<OperationResult>("corr-nats-early").WaitAsync(HangGuard);

        Assert.Equal("early", (await waiter.ResponseTask).Message);
        _store.Verify(s => s.SaveAsync(It.IsAny<string>(), It.IsAny<RecoveryState>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()), Times.Never);
        Assert.False(_logger.HasEntry(LogLevel.Error, "Post-save recovery-state compensation delete failed"));
    }

    [Fact]
    public async Task AbandonedSubscribe_ThatFailsWhileTheLoggerThrows_DoesNotFaultTheFireAndForgetTask()
    {
        // r3/R3-08: DisposeLateSubscriptionAsync is discarded (`_ =`), so its catch must not log
        // unguarded: a throwing provider left the task faulted and unobserved.
        _logger.ThrowOnMessageContaining = "Abandoned subscribe";
        var channel = CreateChannel();
        var method = typeof(NatsAsyncResponseChannel).GetMethod("DisposeLateSubscriptionAsync", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!;
        var pending = Task.FromException<INatsChannelSubscription>(new InvalidOperationException("the subscribe failed late"));

        await ((Task)method.Invoke(channel, [pending, "subj"])!).WaitAsync(HangGuard);
    }

    // ---------------------------------------------------------------- client adapters

    [Fact]
    public void RawRequester_AWatcherThatThrows_NeverFailsTheConnectionsEventLoop()
    {
        var connection = new Mock<INatsConnection>();
        connection.SetupGet(c => c.Opts).Returns(NatsOpts.Default);
        connection.Setup(c => c.GetBoundedChannelOpts(It.IsAny<NatsSubChannelOpts?>())).Returns(new BoundedChannelOptions(4));
        var watched = new NatsSub<string>(connection.Object, Mock.Of<INatsSubscriptionManager>(), "subj", queueGroup: null, opts: null, NatsDefaultSerializer<string>.Default);
        using var requester = new NatsRawRequester(connection.Object);
        var calls = 0;
        using var watch = requester.WatchDrops(watched, _ =>
        {
            calls++;
            throw new InvalidOperationException("waiter reaction failed");
        });

        connection.Raise(c => c.MessageDropped += null, connection.Object, new NatsMessageDroppedEventArgs(watched, 4, "subj", null, null, null));
        connection.Raise(c => c.MessageDropped += null, connection.Object, new NatsMessageDroppedEventArgs(watched, 5, "subj", null, null, null));

        Assert.Equal(2, calls);
    }

    [Fact]
    public async Task KvAdapter_TryCreate_AKeyPurgedBetweenTheWriteAndTheRead_IsAConflict()
    {
        var (adapter, store, leader) = KvAdapter();
        store.Setup(s => s.TryUpdateAsync("k", "v", 0UL, It.IsAny<INatsSerialize<string>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(WrongLastRevision());
        leader.Nothing("k");

        Assert.False(await adapter.TryCreateAsync("k", "v", CancellationToken.None));
    }

    [Fact]
    public async Task KvAdapter_TryCreate_AReReadRejection_IsThrownNotReadAsAConflict()
    {
        var (adapter, store, leader) = KvAdapter();
        var rejection = new NatsJSApiException(new ApiError { Code = 403, ErrCode = 10000, Description = "permission denied" });
        store.Setup(s => s.TryUpdateAsync("k", "v", 0UL, It.IsAny<INatsSerialize<string>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(WrongLastRevision());
        leader.Fails("k", rejection);

        Assert.Same(rejection, await Assert.ThrowsAsync<NatsJSApiException>(() => adapter.TryCreateAsync("k", "v", CancellationToken.None)));
    }

    [Fact]
    public async Task KvAdapter_BucketCreationRaceLost_ButTheBucketIsStillMissing_Rethrows()
    {
        var context = new Mock<INatsKVContext>();
        context.Setup(c => c.GetStoreAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new NatsJSApiException(new ApiError { Code = 404, ErrCode = 10059, Description = "stream not found" }));
        var nameInUse = new NatsJSApiException(new ApiError { Code = 400, ErrCode = 10058, Description = "stream name already in use with a different configuration" });
        context.Setup(c => c.CreateStoreAsync(It.IsAny<NatsKVConfig>(), It.IsAny<CancellationToken>())).ThrowsAsync(nameInUse);
        var adapter = new NatsKvStoreAdapter(context.Object, new NatsAsyncResponseChannelOptions());

        Assert.Same(nameInUse, await Assert.ThrowsAsync<NatsJSApiException>(() => adapter.TryCreateAsync("k", "v", CancellationToken.None)));
        context.Verify(c => c.GetStoreAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Exactly(2));
    }

    [Fact]
    public async Task KvAdapter_ABucketWhoseConfigurationCannotBeRead_IsUsedRegardless()
    {
        var context = new Mock<INatsKVContext>();
        var store = new Mock<INatsKVStore>();
        context.Setup(c => c.GetStoreAsync(It.IsAny<string>(), It.IsAny<CancellationToken>())).ReturnsAsync(store.Object);
        store.Setup(s => s.GetStatusAsync(It.IsAny<CancellationToken>()))
            .ThrowsAsync(new NatsJSApiException(new ApiError { Code = 503, ErrCode = 10008, Description = "JetStream system temporarily unavailable" }));
        store.Setup(s => s.TryUpdateAsync("k", "v", 0UL, It.IsAny<INatsSerialize<string>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new NatsResult<ulong>(1UL));
        var logger = new RecordingThrowingLogger<NatsKvStoreAdapter>();
        var adapter = new NatsKvStoreAdapter(context.Object, new NatsAsyncResponseChannelOptions(), logger);

        Assert.True(await adapter.TryCreateAsync("k", "v", CancellationToken.None));
        Assert.True(logger.HasEntry(LogLevel.Debug, "Could not read the configuration of the NATS recovery bucket"));
        Assert.False(logger.HasEntry(LogLevel.Warning, "already exists with"));
    }

    // ---------------------------------------------------------------- recovery store maintenance

    [Fact]
    public async Task RecoveryStore_AScanWhileAPurgeIsStillRunning_StartsNoSecondPurge()
    {
        var kv = new FakeNatsKvStore();
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var purgeEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        kv.PurgeGate = () =>
        {
            purgeEntered.TrySetResult();
            return release.Task;
        };
        var store = new NatsRecoveryStateStore(kv, Options.Create(new NatsAsyncResponseChannelOptions()), NullLogger<NatsRecoveryStateStore>.Instance);

        try
        {
            await DrainScanAsync(store);
            await purgeEntered.Task.WaitAsync(HangGuard);
            await DrainScanAsync(store); // the first pass still holds the single-flight latch
        }
        finally
        {
            release.TrySetResult();
        }

        await kv.PurgeCompleted.Task.WaitAsync(HangGuard);
        Assert.Single(kv.PurgeRequests);
    }

    [Fact]
    public async Task RecoveryStore_AFailedPurge_IsLoggedAndRetriedByTheNextScan()
    {
        var kv = new FakeNatsKvStore();
        var attempts = 0;
        kv.PurgeGate = () => Interlocked.Increment(ref attempts) == 1
            ? Task.FromException(new InvalidOperationException("purge failed"))
            : Task.CompletedTask;
        var logger = new RecordingThrowingLogger<NatsRecoveryStateStore>();
        var store = new NatsRecoveryStateStore(kv, Options.Create(new NatsAsyncResponseChannelOptions()), logger);

        await DrainScanAsync(store);
        await Eventually(() => logger.HasEntry(LogLevel.Warning, "Purging delete markers from the NATS recovery bucket failed"));

        // The latch was released: the next scan runs the maintenance again.
        await DrainScanAsync(store);
        await Eventually(() => kv.PurgeRequests.Count == 1);
        Assert.Equal(2, Volatile.Read(ref attempts));
    }

    // ---------------------------------------------------------------- helpers

    private NatsAsyncResponseChannel CreateChannel(
        TimeSpan? drainTimeout = null,
        TimeProvider? timeProvider = null,
        ILogger<NatsAsyncResponseChannel>? logger = null)
        => new(
            new ServiceCollection().BuildServiceProvider().GetRequiredService<IServiceScopeFactory>(),
            _client,
            _store.Object,
            Options.Create(new NatsAsyncResponseChannelOptions
            {
                DefaultTimeout = TimeSpan.FromSeconds(5),
                // Above the 10-minute waits several tests use: since round 65 a NATS wait may not
                // outlive its registration (the KV bucket's MaxAge = RecoveryStateExpiry).
                RecoveryStateExpiry = TimeSpan.FromMinutes(15),
                DisposalDrainTimeout = drainTimeout ?? TimeSpan.FromSeconds(5)
            }),
            new AsyncResponseContextPropagation([]),
            logger ?? _logger,
            timeProvider);

    private static string Envelope(string message)
        => JsonSerializer.Serialize(
            new AsyncResponseEnvelope<OperationResult>
            {
                Success = true,
                Payload = new OperationResult { Status = OperationStatus.Completed, Message = message }
            },
            AsyncResponseEnvelopeOptions<OperationResult>.Instance);

    private static (NatsKvStoreAdapter Adapter, Mock<INatsKVStore> Store, NatsKvLeaderStream Leader) KvAdapter()
    {
        var context = new Mock<INatsKVContext>();
        var store = new Mock<INatsKVStore>();
        context.Setup(c => c.GetStoreAsync(It.IsAny<string>(), It.IsAny<CancellationToken>())).ReturnsAsync(store.Object);
        var leader = new NatsKvLeaderStream().Attach(context);
        return (new NatsKvStoreAdapter(context.Object, new NatsAsyncResponseChannelOptions()), store, leader);
    }

    private static NatsResult<ulong> WrongLastRevision()
        => new(new NatsKVWrongLastRevisionException(new ApiError { Code = 400, ErrCode = 10071, Description = "wrong last sequence: 3" }));

    private static async Task DrainScanAsync(NatsRecoveryStateStore store)
    {
        await foreach (var _ in store.ScanAsync())
        {
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
}
