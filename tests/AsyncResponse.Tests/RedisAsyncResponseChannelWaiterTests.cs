using AsyncResponse.Channels.Redis;
using AsyncResponse.Testing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using StackExchange.Redis;
using System.Net;
using System.Reflection;
using System.Text;
using System.Text.Json;
using Xunit;

namespace AsyncResponse.Tests;

public class RedisAsyncResponseChannelWaiterTests
{
    private readonly Mock<IConnectionMultiplexer> _multiplexer = new();
    private readonly Mock<ISubscriber> _subscriber = new();
    private readonly Mock<IRecoveryStateStore> _store = new();
    private readonly FakeRedisChannelSubscriber _channelSubscriber = new();
    private readonly ServiceProvider _services = new ServiceCollection().BuildServiceProvider();

    public RedisAsyncResponseChannelWaiterTests()
    {
        _multiplexer.Setup(m => m.GetSubscriber(It.IsAny<object?>())).Returns(_subscriber.Object);
        _store
            .Setup(s => s.SaveAsync(
                It.IsAny<string>(),
                It.IsAny<RecoveryState>(),
                It.IsAny<TimeSpan>(),
                It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        _store
            .Setup(s => s.TryDeleteAsync(It.IsAny<string>(), It.IsAny<Guid>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);
    }

    /// <summary>
    /// Fake for the channel's async subscribe seam (ChannelMessageQueue is sealed and cannot be
    /// mocked): captures the handler for tests to push messages through, and counts unsubscribes.
    /// </summary>
    private sealed class FakeRedisChannelSubscriber : IRedisChannelSubscriber
    {
        private int _unsubscribeCount;
        private readonly List<Func<RedisChannel, RedisValue, Task>> _handlers = [];

        public RedisChannel SubscribedChannel { get; private set; }
        public Func<RedisChannel, RedisValue, Task>? Handler { get; private set; }

        /// <summary>Every subscription's handler, in subscribe order — one per waiter, as Redis pub/sub fans out.</summary>
        public IReadOnlyList<Func<RedisChannel, RedisValue, Task>> Handlers => _handlers;
        public Exception? SubscribeException { get; set; }
        public Exception? UnsubscribeException { get; set; }
        public RedisValue? InvokeOnSubscribe { get; set; }
        public Action<RedisChannel>? OnSubscribe { get; set; }
        public int UnsubscribeCount => Volatile.Read(ref _unsubscribeCount);

        public async Task<IRedisChannelSubscription> SubscribeAsync(RedisChannel channel, Func<RedisChannel, RedisValue, Task> onMessage)
        {
            if (SubscribeException is not null)
                throw SubscribeException;

            OnSubscribe?.Invoke(channel);
            SubscribedChannel = channel;
            Handler = onMessage;
            _handlers.Add(onMessage);
            if (InvokeOnSubscribe is { } message)
                await onMessage(channel, message);
            return new Subscription(this);
        }

        private sealed class Subscription(FakeRedisChannelSubscriber owner) : IRedisChannelSubscription
        {
            public ValueTask DisposeAsync()
            {
                Interlocked.Increment(ref owner._unsubscribeCount);
                return owner.UnsubscribeException is null
                    ? ValueTask.CompletedTask
                    : ValueTask.FromException(owner.UnsubscribeException);
            }
        }
    }

    [Fact]
    public async Task CreateResponseWaiter_TerminalDeliveryDuringRecoverySave_CompensatesTheOrphanedRegistration()
    {
        // The registration-save race: the message pump is live before the recovery state is
        // saved. A terminal response landing in that window runs cleanup, whose delete no-ops
        // (nothing saved yet); the save then commits an orphaned callback-armed registration that
        // would resurrect recovery for a completed wait on any later publish. The creator must
        // compensate with a second delete after the save when cleanup already started.
        //
        // Determinism: awaiting the handler only awaits executor ADMISSION — processing (and the
        // cleanup it triggers) runs on the executor afterwards. If the save were released on a
        // free-running clock, cleanup could land after the save instead, where its own delete
        // removes the committed row (one delete, no orphan — correct, but a different
        // interleaving than the one this test pins). So the save completes only once cleanup's
        // delete has been OBSERVED: cleanup sets cleanupStarted before that delete, so the
        // post-save check deterministically sees it and must compensate.
        var saveStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var cleanupDeleteIssued = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _store
            .Setup(s => s.SaveAsync(It.IsAny<string>(), It.IsAny<RecoveryState>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
            .Returns(async () =>
            {
                saveStarted.TrySetResult();
                await cleanupDeleteIssued.Task;
            });
        _store
            .Setup(s => s.TryDeleteAsync(It.IsAny<string>(), It.IsAny<Guid>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(true)
            .Callback(() => cleanupDeleteIssued.TrySetResult());
        var channel = CreateChannel();

        var waiterTask = channel.CreateResponseWaiter<OperationResult>("corr-save-race");
        await saveStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));

        // Terminal response arrives while the save is still in flight: waiter completes, cleanup
        // runs its (no-op) delete — which is what releases the parked save.
        await _channelSubscriber.Handler!(
            _channelSubscriber.SubscribedChannel,
            """{"SchemaVersion":1,"Success":true,"Payload":{"Status":2,"Message":"fast"},"ExceptionMessage":null,"ExceptionStackTrace":null}""");

        await using var waiter = await waiterTask.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(OperationStatus.Completed, (await waiter.ResponseTask).Status);

        // One delete from cleanup (pre-save no-op) plus the post-save compensation.
        _store.Verify(
            s => s.TryDeleteAsync("corr-save-race", It.IsAny<Guid>(), It.IsAny<CancellationToken>()),
            Times.AtLeast(2));
    }

    [Fact]
    public async Task CreateResponseWaiter_SaveFailureAfterTerminalSettledTheWait_ReturnsTheCompletedWaiter()
    {
        // A terminal delivery settles the wait while the recovery-state save is in flight, and
        // the save then FAILS. Rethrowing (the plain subscribe-failure contract) would discard a
        // response the waiter already holds — the exact loss the library exists to prevent — and
        // the same interleaving with a healthy store returns the completed waiter. The save is
        // released by failing only once cleanup's delete has been observed, so the failure is
        // deterministically post-settlement.
        var saveStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var cleanupDeleteIssued = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _store
            .Setup(s => s.SaveAsync(It.IsAny<string>(), It.IsAny<RecoveryState>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
            .Returns(async () =>
            {
                saveStarted.TrySetResult();
                await cleanupDeleteIssued.Task;
                throw new InvalidOperationException("recovery save failed");
            });
        _store
            .Setup(s => s.TryDeleteAsync(It.IsAny<string>(), It.IsAny<Guid>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(true)
            .Callback(() => cleanupDeleteIssued.TrySetResult());
        var channel = CreateChannel();

        var waiterTask = channel.CreateResponseWaiter<OperationResult>("corr-save-fail");
        await saveStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));

        await _channelSubscriber.Handler!(
            _channelSubscriber.SubscribedChannel,
            """{"SchemaVersion":1,"Success":true,"Payload":{"Status":2,"Message":"settled"},"ExceptionMessage":null,"ExceptionStackTrace":null}""");

        await using var waiter = await waiterTask.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal("settled", (await waiter.ResponseTask).Message);
    }

    [Fact]
    public async Task CreateResponseWaiter_TerminalDeliveryInsideSubscribe_StillUnsubscribes()
    {
        // Deliveries can start inside SubscribeAsync, before the creator's `subscription` local is
        // assigned. If the message completes the waiter and cleanup runs to completion first, the
        // cleanup's unsubscribe sees a null subscription and the latched cleanup never re-runs —
        // a zombie server-side subscription that makes every future publish/probe report a live
        // waiter, permanently suppressing lost-subscriber recovery for the correlation id. The
        // creator must unsubscribe after the assignment when cleanup already started.
        _channelSubscriber.InvokeOnSubscribe =
            """{"SchemaVersion":1,"Success":true,"Payload":{"Status":2,"Message":"inline"},"ExceptionMessage":null,"ExceptionStackTrace":null}""";
        var channel = CreateChannel();

        await using var waiter = await channel.CreateResponseWaiter<OperationResult>("corr-inline");

        Assert.Equal(OperationStatus.Completed, (await waiter.ResponseTask).Status);

        // Cleanup runs on the executor after handler ADMISSION, so it races the creator: either
        // cleanup's unsubscribe sees the assigned subscription, or it saw null and the creator's
        // post-assignment compensation unsubscribes — and in the overlap BOTH may (unsubscribe is
        // idempotent). The zombie being pinned away is "no unsubscribe ever", so the invariant is
        // eventually-at-least-one, not exactly-one-by-now.
        await Eventually(() => _channelSubscriber.UnsubscribeCount >= 1);
    }

    [Fact]
    public async Task CreateResponseWaiter_CompletesFromSubscribedRedisMessage()
    {
        var channel = CreateChannel();

        await using var waiter = await channel.CreateResponseWaiter<OperationResult>(
            "corr-a",
            timeout: TimeSpan.FromSeconds(5));

        await PublishSuccess(new OperationResult { Status = OperationStatus.Completed, Message = "done" });

        var result = await waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(2));
        Assert.Equal("done", result.Message);
        Assert.Equal("asyncresponse:response:corr-a", _channelSubscriber.SubscribedChannel.ToString());
        _store.Verify(s => s.SaveAsync(
            "corr-a",
            It.Is<RecoveryState>(state => state.CorrelationId == "corr-a"
                && state.PayloadTypeFullName == typeof(OperationResult).FullName),
            It.IsAny<TimeSpan>(),
            It.IsAny<CancellationToken>()), Times.Once);
        await Eventually(() => _channelSubscriber.UnsubscribeCount == 1);
    }

    [Fact]
    public async Task CreateResponseWaiter_ValidatesCorrelationAndUsesRecoveryExpiryAsTimeoutFallback()
    {
        var channel = CreateChannel(new RedisAsyncResponseOptions
        {
            DefaultTimeout = null,
            RecoveryStateExpiry = TimeSpan.FromMinutes(1)
        });

        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            channel.CreateResponseWaiter<OperationResult>(" "));
        await using var waiter = await channel.CreateResponseWaiter<OperationResult>("fallback-timeout");

        Assert.False(waiter.ResponseTask.IsCompleted);
    }

    [Fact]
    public async Task DuplicateTerminalMessages_DoNotReplaceFirstCompletion()
    {
        var channel = CreateChannel();

        await using (var success = await channel.CreateResponseWaiter<OperationResult>("duplicate-success"))
        {
            var handler = _channelSubscriber.Handler!;
            var subscribed = _channelSubscriber.SubscribedChannel;
            var json = JsonSerializer.Serialize(new AsyncResponseEnvelope<OperationResult>
            {
                Success = true,
                Payload = new OperationResult { Status = OperationStatus.Completed, Message = "first" }
            }, AsyncResponseEnvelopeOptions<OperationResult>.Instance);
            await handler(subscribed, json);
            await handler(subscribed, json);
            Assert.Equal("first", (await success.ResponseTask.WaitAsync(TimeSpan.FromSeconds(2))).Message);
        }

        await DuplicateFaultAsync(channel, "duplicate-null", "null", typeof(JsonException));
        await DuplicateFaultAsync(
            channel,
            "duplicate-schema",
            JsonSerializer.Serialize(new AsyncResponseEnvelope<OperationResult>
            {
                SchemaVersion = AsyncResponseEnvelopeSchema.Current + 1,
                Success = true,
                Payload = new OperationResult()
            }, AsyncResponseEnvelopeOptions<OperationResult>.Instance),
            typeof(InvalidOperationException));
        await DuplicateFaultAsync(
            channel,
            "duplicate-remote",
            JsonSerializer.Serialize(new AsyncResponseEnvelope<OperationResult>
            {
                Success = false,
                ExceptionMessage = "remote"
            }, AsyncResponseEnvelopeOptions<OperationResult>.Instance),
            typeof(Exception));
        await DuplicateFaultAsync(channel, "duplicate-malformed", "{not-json", typeof(InvalidDataException));
    }

    [Fact]
    public async Task CreateResponseWaiter_CompletionPredicateCanWaitForLaterMessages()
    {
        var channel = CreateChannel();

        await using var waiter = await channel.CreateResponseWaiter<OperationResult>(
            "corr-a",
            completionPredicate: payload => new ValueTask<bool>(payload.Status == OperationStatus.Completed),
            timeout: TimeSpan.FromSeconds(5));

        await PublishSuccess(new OperationResult { Status = OperationStatus.Running, Message = "still running" });
        await Task.Delay(50);
        Assert.False(waiter.ResponseTask.IsCompleted);

        await PublishSuccess(new OperationResult { Status = OperationStatus.Completed, Message = "done" });

        var result = await waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(2));
        Assert.Equal("done", result.Message);
    }

    [Fact]
    public async Task CreateResponseWaiter_RemoteFailureEnvelopeFaultsWaiter()
    {
        var channel = CreateChannel();

        await using var waiter = await channel.CreateResponseWaiter<OperationResult>(
            "corr-a",
            timeout: TimeSpan.FromSeconds(5));

        await PublishEnvelope(new AsyncResponseEnvelope<OperationResult>
        {
            Success = false,
            ExceptionMessage = "remote failed",
            ExceptionStackTrace = "remote stack"
        });

        var ex = await Assert.ThrowsAsync<Exception>(() => waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(2)));
        Assert.Equal("remote failed", ex.Message);
        Assert.Equal("remote stack", ex.Data["RemoteStackTrace"]);
    }

    [Fact]
    public async Task CreateResponseWaiter_MalformedRedisMessageFaultsWaiter()
    {
        var channel = CreateChannel();

        await using var waiter = await channel.CreateResponseWaiter<OperationResult>(
            "corr-a",
            timeout: TimeSpan.FromSeconds(5));

        await _channelSubscriber.Handler!.Invoke(_channelSubscriber.SubscribedChannel, "{not-json");

        // The body-free parse failure (JsonSafety), not the raw reader's JsonException.
        await Assert.ThrowsAsync<InvalidDataException>(() => waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(2)));
    }

    /// <summary>
    /// Round 36: the reader deserialized the wire bytes directly, so a payload that failed to
    /// convert faulted the waiter with — and logged — the raw System.Text.Json exception, whose
    /// message quotes the inbound dictionary key (<c>Path: $.Payload.Values['…']</c>). Pre-fix
    /// failure: the marker is in the waiter's exception and in the channel's error log.
    /// </summary>
    [Fact]
    public async Task CreateResponseWaiter_MalformedPayload_DoesNotEchoInboundKeysIntoLogsOrTheWaiter()
    {
        var logger = new CollectingLogger();
        var channel = CreateChannel(new RedisAsyncResponseOptions
        {
            DefaultTimeout = TimeSpan.FromSeconds(5),
            RecoveryStateExpiry = TimeSpan.FromMinutes(5)
        }, logger.For<RedisAsyncResponseChannel>());

        await using var waiter = await channel.CreateResponseWaiter<Round36RegressionTests.LeakProbePayload>(
            "corr-leak",
            timeout: TimeSpan.FromSeconds(5));

        await _channelSubscriber.Handler!.Invoke(_channelSubscriber.SubscribedChannel, Round36RegressionTests.LeakingEnvelope);

        var ex = await Assert.ThrowsAnyAsync<Exception>(() => waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(2)));
        Round36RegressionTests.AssertNoMarker(ex, logger);
    }

    /// <summary>
    /// Fixpoint r1 (S3#8). A failure envelope's <c>ExceptionMessage</c> is chosen by whoever
    /// produced it, yet it went raw into a Warning on every delivery and into the wait's activity
    /// status — megabytes of it, with CR/LF forging log lines — while only the stack trace was
    /// capped. The log no longer quotes it (DB-channel parity) and the status carries a capped,
    /// escaped excerpt; the waiter still receives it whole. Pre-fix: the forged line is in the log
    /// and the status.
    /// </summary>
    [Fact]
    public async Task CreateResponseWaiter_RemoteFailure_KeepsItsMessageOutOfTheLog_AndCapsAndEscapesTheStatus()
    {
        using var activities = new AsyncResponseActivityCollector();
        var logger = new CollectingLogger();
        var channel = CreateChannel(new RedisAsyncResponseOptions
        {
            DefaultTimeout = TimeSpan.FromSeconds(5),
            RecoveryStateExpiry = TimeSpan.FromMinutes(5)
        }, logger.For<RedisAsyncResponseChannel>());
        var hostile = "boom\r\nFORGED entry " + new string('x', 100_000);

        await using (var waiter = await channel.CreateResponseWaiter<OperationResult>("corr-hostile", timeout: TimeSpan.FromSeconds(5)))
        {
            await PublishEnvelope(new AsyncResponseEnvelope<OperationResult> { Success = false, ExceptionMessage = hostile });

            var ex = await Assert.ThrowsAnyAsync<Exception>(() => waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(2)));
            Assert.Equal(hostile, ex.Message);
        }

        Assert.DoesNotContain(logger.Entries, entry =>
            entry.Message.Contains("FORGED", StringComparison.Ordinal)
            || entry.Exception?.Message.Contains("FORGED", StringComparison.Ordinal) == true);
        var wait = Assert.Single(activities.All(), activity => activity.OperationName == "asyncresponse.wait");
        Assert.Equal(System.Diagnostics.ActivityStatusCode.Error, wait.Status);
        var status = Assert.IsType<string>(wait.StatusDescription);
        Assert.DoesNotContain('\r', status);
        Assert.DoesNotContain('\n', status);
        Assert.StartsWith("boom\\u000d\\u000aFORGED", status, StringComparison.Ordinal);
        Assert.True(status.Length < 1_000, $"The status quoted {status.Length} characters of the remote message.");
    }

    [Fact]
    public async Task CreateResponseWaiter_NullEnvelopeFaultsWaiter()
    {
        var channel = CreateChannel();

        await using var waiter = await channel.CreateResponseWaiter<OperationResult>(
            "corr-null",
            timeout: TimeSpan.FromSeconds(5));

        await _channelSubscriber.Handler!.Invoke(_channelSubscriber.SubscribedChannel, "null");

        await Assert.ThrowsAsync<JsonException>(() => waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(2)));
    }

    [Fact]
    public async Task CreateResponseWaiter_UnsubscribeFailureDoesNotMaskCompletedResponse()
    {
        _channelSubscriber.UnsubscribeException = new InvalidOperationException("unsubscribe failed");
        var channel = CreateChannel();

        await using var waiter = await channel.CreateResponseWaiter<OperationResult>(
            "corr-cleanup",
            timeout: TimeSpan.FromSeconds(5));

        await PublishSuccess(new OperationResult { Status = OperationStatus.Completed, Message = "done" });

        var result = await waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(2));
        Assert.Equal("done", result.Message);
        await Eventually(() => _channelSubscriber.UnsubscribeCount >= 1);
    }

    [Fact]
    public async Task CreateResponseWaiter_WhenExecutionContextFlowIsSuppressed_StillProcessesMessage()
    {
        var channel = CreateChannel();
        Task<IAsyncResponseWaiter<OperationResult>> waiterTask;
        using (ExecutionContext.SuppressFlow())
        {
            waiterTask = channel.CreateResponseWaiter<OperationResult>(
                "corr-no-context",
                timeout: TimeSpan.FromSeconds(5));
        }

        await using var waiter = await waiterTask;

        await PublishSuccess(new OperationResult { Status = OperationStatus.Completed, Message = "done" });

        var result = await waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(2));
        Assert.Equal("done", result.Message);
    }

    [Fact]
    public async Task CreateResponseWaiter_TimeoutFaultsAndCleansUp()
    {
        var channel = CreateChannel();

        await using var waiter = await channel.CreateResponseWaiter<OperationResult>(
            "corr-timeout",
            timeout: TimeSpan.FromMilliseconds(5));

        await Assert.ThrowsAsync<TimeoutException>(() => waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(2)));
        await Eventually(() => _channelSubscriber.UnsubscribeCount == 1);
        _store.Verify(s => s.TryDeleteAsync("corr-timeout", It.IsAny<Guid>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task CreateResponseWaiter_TimeoutFiresOnTheInjectedClock()
    {
        // Regression: the waiter timeout was armed on a default CancellationTokenSource — the
        // system clock — while the same channel stamped RegisteredAtUtc from the injected
        // TimeProvider. Under a virtual clock a production-sized timeout could never fire
        // (DbChannelShared parity: its timeout CTS is created on the injected clock for exactly
        // this reason).
        var clock = new AsyncResponse.Testing.VirtualTimeProvider();
        var channel = CreateChannel(
            new RedisAsyncResponseOptions
            {
                DefaultTimeout = TimeSpan.FromSeconds(5),
                RecoveryStateExpiry = TimeSpan.FromMinutes(5)
            },
            timeProvider: clock);

        await using var waiter = await channel.CreateResponseWaiter<OperationResult>(
            "corr-virtual-timeout",
            timeout: TimeSpan.FromMinutes(10));

        var guard = TimeProvider.System.GetUtcNow() + TimeSpan.FromSeconds(5);
        while (!waiter.ResponseTask.IsCompleted)
        {
            Assert.True(TimeProvider.System.GetUtcNow() < guard, "advancing the virtual clock never fired the waiter timeout");
            clock.Advance(TimeSpan.FromMinutes(11));
            await Task.Delay(TimeSpan.FromMilliseconds(5));
        }

        // The waiter's own timeout — not a real-time guard — must be what faulted the task.
        var timeout = await Assert.ThrowsAsync<TimeoutException>(() => waiter.ResponseTask);
        Assert.Contains("corr-virtual-timeout", timeout.Message);
    }

    [Fact]
    public async Task WaiterTimeout_DrainsTheExecutorBeforeFaulting_SoAnInFlightDeliveryStillWins()
    {
        // Regression (round 29): the timeout faulted the waiter BEFORE draining the per-correlation
        // executor, so a delivery already inside it — mid Until-predicate, holding a message the
        // publisher was told was delivered — lost the race and a consumed response was reported as
        // a timeout. The terminal exception is now applied after the drain, where TrySet loses.
        var channel = CreateChannel();
        var insidePredicate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releasePredicate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var waiter = await channel.CreateResponseWaiter<OperationResult>(
            "corr-drain-timeout",
            completionPredicate: async _ =>
            {
                insidePredicate.TrySetResult();
                await releasePredicate.Task;
                return true;
            },
            timeout: TimeSpan.FromMilliseconds(50));

        await PublishSuccess(new OperationResult { Status = OperationStatus.Completed, Message = "delivered" });
        await insidePredicate.Task.WaitAsync(TimeSpan.FromSeconds(5));

        // The 50ms timeout fires while the predicate still holds the executor.
        await Task.Delay(300);
        Assert.False(waiter.ResponseTask.IsCompleted, "the waiter was settled before the in-flight delivery had drained");

        releasePredicate.TrySetResult();

        var result = await waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.Equal("delivered", result.Message);
    }

    [Fact]
    public async Task CreateResponseWaiter_StampsRegisteredAtFromTheInjectedTimeProvider()
    {
        // Regression (round 29): the stamp came from DateTime.UtcNow, which no host can substitute.
        // The watchdog judges staleness as "utcNow - RegisteredAtUtc" from whichever host scans, so
        // a skewed stamp made registrations either never age (a stuck flow stays invisible, health
        // stays green) or age instantly (healthy waits page the operator every scan).
        var time = new VirtualTimeProvider(new DateTimeOffset(2031, 5, 4, 3, 2, 1, TimeSpan.Zero));
        RecoveryState? saved = null;
        _store
            .Setup(s => s.SaveAsync(It.IsAny<string>(), It.IsAny<RecoveryState>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
            .Callback((string _, RecoveryState state, TimeSpan _, CancellationToken _) => saved = state)
            .Returns(Task.CompletedTask);

        var channel = CreateChannel(
            new RedisAsyncResponseOptions { DefaultTimeout = TimeSpan.FromSeconds(5), RecoveryStateExpiry = TimeSpan.FromMinutes(5) },
            timeProvider: time);

        await using var waiter = await channel.CreateRecoverableResponseWaiter<OperationResult>(
            "corr-registered-at",
            resumeCallback: new ReflectionCallDto
            {
                ServiceInterfaceFullName = typeof(IAsyncResponsePublisher).FullName!,
                MethodName = "Resume",
                Params = []
            },
            timeout: TimeSpan.FromSeconds(5));

        Assert.NotNull(saved);
        Assert.Equal(time.GetUtcNow().UtcDateTime, saved!.RegisteredAtUtc);
    }

    [Fact]
    public async Task WaiterTimeout_WhenTimeoutHandlingThrows_LogsInsteadOfLeavingAnUnobservedFault()
    {
        // The timeout body runs on a fire-and-forget Task.Run; an exception escaping it (here a
        // logger provider that throws on the timeout warning) must be caught and logged through
        // the error path, not die as an unobserved task fault.
        var logger = new RecordingThrowingLogger<RedisAsyncResponseChannel> { ThrowOnMessageContaining = "Timed out waiting" };
        var channel = CreateChannel(new RedisAsyncResponseOptions
        {
            DefaultTimeout = TimeSpan.FromSeconds(5),
            RecoveryStateExpiry = TimeSpan.FromMinutes(5)
        }, logger);

        await using var waiter = await channel.CreateResponseWaiter<OperationResult>(
            "corr-timeout-throws",
            timeout: TimeSpan.FromMilliseconds(5));

        await Eventually(() => logger.HasEntry(LogLevel.Error, "Error handling waiter timeout"));
    }

    [Fact]
    public async Task CreateResponseWaiter_NonAsciiPayload_RoundTripsThroughTheUtf8BytePath()
    {
        var channel = CreateChannel();

        await using var waiter = await channel.CreateResponseWaiter<OperationResult>(
            "corr-utf8",
            timeout: TimeSpan.FromSeconds(5));

        // Delivered as raw UTF-8 bytes, exactly how StackExchange.Redis hands pub/sub values
        // over; the message covers 2-, 3-, and 4-byte UTF-8 sequences.
        var message = "Grüße 数据 🚀";
        var json = JsonSerializer.Serialize(new AsyncResponseEnvelope<OperationResult>
        {
            Success = true,
            Payload = new OperationResult { Status = OperationStatus.Completed, Message = message }
        }, AsyncResponseEnvelopeOptions<OperationResult>.Instance);
        await _channelSubscriber.Handler!.Invoke(
            _channelSubscriber.SubscribedChannel,
            Encoding.UTF8.GetBytes(json));

        var result = await waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(2));
        Assert.Equal(message, result.Message);
    }

    [Fact]
    public async Task CreateResponseWaiter_SubscribeFailureThrowsAndDeletesRecoveryState()
    {
        var failure = new InvalidOperationException("subscribe failed");
        _channelSubscriber.SubscribeException = failure;
        var channel = CreateChannel();

        // Must throw rather than return a pre-faulted waiter: the builder's contract is that the
        // trigger only runs once the subscription AND recovery state exist, so a registration
        // failure has to surface before any trigger could fire the remote operation.
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            channel.CreateResponseWaiter<OperationResult>(
                "corr-a",
                timeout: TimeSpan.FromSeconds(5)));
        Assert.Same(failure, ex);
        _store.Verify(s => s.SaveAsync(
            It.IsAny<string>(),
            It.IsAny<RecoveryState>(),
            It.IsAny<TimeSpan>(),
            It.IsAny<CancellationToken>()), Times.Never);
        _store.Verify(s => s.TryDeleteAsync("corr-a", It.IsAny<Guid>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task CountActiveSubscribersAsync_UsesMaximumConnectedEndpointCount()
    {
        var endpointA = new DnsEndPoint("redis-a", 6379);
        var endpointB = new DnsEndPoint("redis-b", 6379);
        var endpointC = new DnsEndPoint("redis-c", 6379);
        var serverA = new Mock<IServer>();
        var serverB = new Mock<IServer>();
        var serverC = new Mock<IServer>();
        serverA.SetupGet(s => s.IsConnected).Returns(true);
        serverB.SetupGet(s => s.IsConnected).Returns(false);
        serverC.SetupGet(s => s.IsConnected).Returns(true);
        serverA
            .Setup(s => s.SubscriptionSubscriberCountAsync(It.IsAny<RedisChannel>(), It.IsAny<CommandFlags>()))
            .ReturnsAsync(3);
        serverC
            .Setup(s => s.SubscriptionSubscriberCountAsync(It.IsAny<RedisChannel>(), It.IsAny<CommandFlags>()))
            .ThrowsAsync(new InvalidOperationException("node unavailable"));

        _multiplexer.Setup(m => m.GetEndPoints(It.IsAny<bool>())).Returns([endpointA, endpointB, endpointC]);
        _multiplexer.Setup(m => m.GetServer(endpointA, It.IsAny<object?>())).Returns(serverA.Object);
        _multiplexer.Setup(m => m.GetServer(endpointB, It.IsAny<object?>())).Returns(serverB.Object);
        _multiplexer.Setup(m => m.GetServer(endpointC, It.IsAny<object?>())).Returns(serverC.Object);
        var channel = CreateChannel();

        Assert.Equal(0, await channel.CountActiveSubscribersAsync(" "));
        Assert.Equal(3, await channel.CountActiveSubscribersAsync("corr-a"));
    }

    [Fact]
    public async Task Publishers_WithBlankCorrelationId_AreNoops()
    {
        var channel = CreateChannel();
        var rawPublisher = (IRawAsyncResponsePublisher)channel;

        await channel.SetResponse(new OperationResult { Status = OperationStatus.Completed }, " ");
        await rawPublisher.SetRawResponseJson("""{"Status":2}""", " ");
        await channel.SetException(new InvalidOperationException("missing correlation"), " ");

        _subscriber.Verify(s => s.PublishAsync(
            It.IsAny<RedisChannel>(),
            It.IsAny<RedisValue>(),
            It.IsAny<CommandFlags>()), Times.Never);
    }

    [Fact]
    public async Task Publishers_LogSuccessfulPublishWhenSubscribersArePresent()
    {
        var channel = CreateChannel(new RedisAsyncResponseOptions
        {
            DefaultTimeout = TimeSpan.FromSeconds(5),
            RecoveryStateExpiry = TimeSpan.FromMinutes(5)
        }, new TestLogger<RedisAsyncResponseChannel>());
        var rawPublisher = (IRawAsyncResponsePublisher)channel;
        _subscriber
            .Setup(s => s.PublishAsync(It.IsAny<RedisChannel>(), It.IsAny<RedisValue>(), It.IsAny<CommandFlags>()))
            .ReturnsAsync(1);

        await channel.SetResponse(new OperationResult { Status = OperationStatus.Completed }, "corr-a");
        await rawPublisher.SetRawResponseJson("""{"Status":2}""", "corr-a");
        await channel.SetException(new InvalidOperationException("remote failure"), "corr-a");

        _subscriber.Verify(s => s.PublishAsync(
            It.IsAny<RedisChannel>(),
            It.IsAny<RedisValue>(),
            It.IsAny<CommandFlags>()), Times.Exactly(3));
    }

    [Fact]
    public async Task SetResponse_WhenPublishFails_Propagates()
    {
        var failure = new InvalidOperationException("publish failed");
        _subscriber
            .Setup(s => s.PublishAsync(It.IsAny<RedisChannel>(), It.IsAny<RedisValue>(), It.IsAny<CommandFlags>()))
            .ThrowsAsync(failure);
        var channel = CreateChannel();

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            channel.SetResponse(new OperationResult { Status = OperationStatus.Completed }, "corr-a"));

        Assert.Same(failure, ex);
    }

    [Fact]
    public async Task SetRawResponseJson_WhenPublishFails_Propagates()
    {
        var failure = new InvalidOperationException("publish failed");
        _subscriber
            .Setup(s => s.PublishAsync(It.IsAny<RedisChannel>(), It.IsAny<RedisValue>(), It.IsAny<CommandFlags>()))
            .ThrowsAsync(failure);
        var channel = CreateChannel();
        var rawPublisher = (IRawAsyncResponsePublisher)channel;

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            rawPublisher.SetRawResponseJson("""{"Status":2}""", "corr-a"));

        Assert.Same(failure, ex);
    }

    [Fact]
    public async Task SetException_WhenPublishFails_Propagates()
    {
        var failure = new InvalidOperationException("publish failed");
        _subscriber
            .Setup(s => s.PublishAsync(It.IsAny<RedisChannel>(), It.IsAny<RedisValue>(), It.IsAny<CommandFlags>()))
            .ThrowsAsync(failure);
        var channel = CreateChannel();

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            channel.SetException(new InvalidOperationException("remote failure"), "corr-a"));

        Assert.Same(failure, ex);
    }

    [Fact]
    public async Task CreateResponseWaiter_RegistersExecutorChannelBeforeSubscribing()
    {
        // The subscriber can start delivering before SubscribeAsync returns, and on a correlation
        // id reused within the registry's tombstone lifetime those deliveries are silently dropped
        // for an unregistered channel — so the registration must already be visible when the
        // subscribe call begins.
        var channel = CreateChannel();
        var registry = GetExecutorRegistry(channel);
        bool? registeredAtSubscribeTime = null;
        _channelSubscriber.OnSubscribe = subscribed =>
            registeredAtSubscribeTime = HasExecutorRegistration(registry, subscribed.ToString()!);

        await using var waiter = await channel.CreateResponseWaiter<OperationResult>(
            "corr-order",
            timeout: TimeSpan.FromSeconds(5));

        Assert.True(registeredAtSubscribeTime);
    }

    [Fact]
    public async Task RedisWaiter_CleanupStillUnsubscribesAndRetires_WhenRecoveryDeleteThrows()
    {
        _store
            .Setup(s => s.TryDeleteAsync(It.IsAny<string>(), It.IsAny<Guid>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("recovery store down"));
        var channel = CreateChannel();

        var waiter = await channel.CreateResponseWaiter<OperationResult>(
            "corr-store-down",
            timeout: TimeSpan.FromSeconds(5));
        await waiter.DisposeAsync();

        // A failed recovery-state delete is a network fault; it must not skip the local teardown:
        // the pub/sub subscription is still disposed and the executor channel retired.
        Assert.Equal(1, _channelSubscriber.UnsubscribeCount);
        var registry = GetExecutorRegistry(channel);
        Assert.False(HasExecutorRegistration(registry, _channelSubscriber.SubscribedChannel.ToString()!));
    }

    /// <summary>
    /// Regression (round 33): the channel implemented no <see cref="IAsyncDisposable"/> at all, so
    /// container disposal at host shutdown had nothing to join — an executor retirement scheduled
    /// off a subscription's cleanup was a discarded <c>Task.Run</c> that died with the process.
    /// Pre-fix: the channel was not assignable to <see cref="IAsyncDisposable"/>.
    /// </summary>
    [Fact]
    public void Channel_IsAsyncDisposable_SoHostShutdownJoinsExecutorRetirements()
        => Assert.IsAssignableFrom<IAsyncDisposable>(CreateChannel());

    /// <summary>
    /// Regression (round 33): the executor retirement scheduled from a subscription's cleanup was
    /// a discarded <c>Task.Run(RemoveAsync)</c> and nothing at host shutdown waited for it — a
    /// retirement still draining a user completion predicate was killed mid-flight with the
    /// predicate's side effects half-applied, and no record of it. Retirements are now tracked and
    /// the channel's <c>DisposeAsync</c> joins them: it must NOT return while a retirement is still
    /// inside the predicate, and must return once that predicate has finished. The terminal
    /// message's cleanup schedules the retirement; a second message, admitted while the first was
    /// still in its predicate, runs on the retiring executor and is what the retirement's drain is
    /// parked on. Pre-fix: the cast to <see cref="IAsyncDisposable"/> failed (there was nothing to
    /// join).
    /// </summary>
    [Fact]
    public async Task DisposeAsync_JoinsAnExecutorRetirementStillDrainingACompletionPredicate()
    {
        var channel = CreateChannel();
        var firstStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseSecond = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondFinished = 0;
        var calls = 0;
        await using var waiter = await channel.CreateResponseWaiter<OperationResult>(
            "corr-retire",
            async _ =>
            {
                if (Interlocked.Increment(ref calls) == 1)
                {
                    // Terminal once released: its cleanup retires the executor.
                    firstStarted.TrySetResult();
                    await releaseFirst.Task;
                    return true;
                }

                // Runs on the RETIRING executor: the retirement's drain is parked right here.
                secondStarted.TrySetResult();
                await releaseSecond.Task;
                Volatile.Write(ref secondFinished, 1);
                return true;
            },
            timeout: TimeSpan.FromSeconds(30));
        var payload = new OperationResult { Status = OperationStatus.Completed, Message = "done" };

        await PublishSuccess(payload);
        await firstStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));
        // Admitted behind the first while it is still inside its predicate, so it can only run
        // after the terminal cleanup has scheduled the retirement — on the executor being retired.
        await PublishSuccess(payload);
        releaseFirst.TrySetResult();
        await secondStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal("done", (await waiter.ResponseTask).Message);

        // Through object: the pre-fix channel implemented no IAsyncDisposable to cast to.
        var disposal = ((IAsyncDisposable)(object)channel).DisposeAsync().AsTask();
        await Task.Delay(200);
        Assert.False(
            disposal.IsCompleted,
            "DisposeAsync returned while an executor retirement was still draining a completion predicate");
        Assert.Equal(0, Volatile.Read(ref secondFinished));

        releaseSecond.TrySetResult();
        await disposal.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.Equal(1, Volatile.Read(ref secondFinished));
    }

    [Fact]
    public async Task FanOutSibling_CleanupWhileSurvivorHoldsTheSharedExecutor_DoesNotFaultTheSurvivorAsOverloaded()
    {
        // Two waiters on one correlation id share one serial executor (it is keyed by channel, not
        // by waiter). Pre-fix the first waiter's cleanup retired it unconditionally; while the
        // retirement drained the SURVIVOR's in-flight predicate, the survivor's next message found
        // the executor retiring, TryEnqueue reported Full, and the survivor was faulted as
        // overloaded (indeterminate) with nothing overloaded at all — its response discarded.
        var channel = CreateChannel();
        var survivorInsidePredicate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseSurvivor = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var first = await channel.CreateResponseWaiter<OperationResult>("corr-fanout", timeout: TimeSpan.FromSeconds(30));
        await using var survivor = await channel.CreateResponseWaiter<OperationResult>(
            "corr-fanout",
            completionPredicate: async payload =>
            {
                if (payload.Message != "first")
                    return true;

                survivorInsidePredicate.TrySetResult();
                await releaseSurvivor.Task;
                return false;
            },
            timeout: TimeSpan.FromSeconds(30));
        Assert.Equal(2, _channelSubscriber.Handlers.Count);

        // Redis pub/sub hands the message to both subscriptions; the first waiter completes, the
        // survivor's predicate then holds the shared executor.
        var firstMessage = Envelope(new OperationResult { Status = OperationStatus.Completed, Message = "first" });
        await _channelSubscriber.Handlers[0](_channelSubscriber.SubscribedChannel, firstMessage);
        await _channelSubscriber.Handlers[1](_channelSubscriber.SubscribedChannel, firstMessage);
        Assert.Equal("first", (await first.ResponseTask.WaitAsync(TimeSpan.FromSeconds(5))).Message);
        await survivorInsidePredicate.Task.WaitAsync(TimeSpan.FromSeconds(5));

        // Joins the first waiter's latched cleanup, which schedules its executor retirement.
        await first.DisposeAsync();
        var deadline = DateTime.UtcNow.AddSeconds(10);
        while (PendingRetirements(channel) > 0 && !AnyExecutorRetiring(GetExecutorRegistry(channel)) && DateTime.UtcNow < deadline)
            await Task.Delay(10);

        await _channelSubscriber.Handlers[1](
            _channelSubscriber.SubscribedChannel,
            Envelope(new OperationResult { Status = OperationStatus.Completed, Message = "second" }));
        releaseSurvivor.TrySetResult();

        var result = await survivor.ResponseTask.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.Equal("second", result.Message);
    }

    [Fact]
    public async Task RecoveryRoutedPublish_WhileAWaiterHoldsTheExecutor_DoesNotFaultThatWaiterAsOverloaded()
    {
        // Pre-commit review of fixpoint round 1: the per-waiter cleanups retire the shared
        // executor only once nothing is registered on the channel, but the retirement after a
        // recovery-routed publish still ran unconditionally. A waiter that re-attached in that
        // window (a publish found no subscriber on the endpoint it asked, then routed to recovery)
        // had its executor marked retiring while its predicate held it; its next message read as a
        // full queue and it was faulted as overloaded (indeterminate), its response discarded.
        // The endpoint this publisher asks answers zero: the waiter subscribed elsewhere.
        var channel = CreateProbeChannel(ProbeClusterServer("10.0.0.1", connected: true, subscribers: 0));
        var insidePredicate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releasePredicate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var waiter = await channel.CreateResponseWaiter<OperationResult>(
            "corr-reattached",
            completionPredicate: async payload =>
            {
                if (payload.Message != "first")
                    return true;

                insidePredicate.TrySetResult();
                await releasePredicate.Task;
                return false;
            },
            timeout: TimeSpan.FromSeconds(30));
        _subscriber
            .Setup(s => s.PublishAsync(It.IsAny<RedisChannel>(), It.IsAny<RedisValue>(), It.IsAny<CommandFlags>()))
            .ReturnsAsync(0L);
        _store
            .Setup(s => s.GetAllAsync("corr-reattached", It.IsAny<CancellationToken>()))
            .ReturnsAsync(Array.Empty<RecoveryState>());

        await _channelSubscriber.Handler!(_channelSubscriber.SubscribedChannel, Envelope(new OperationResult { Status = OperationStatus.Running, Message = "first" }));
        await insidePredicate.Task.WaitAsync(TimeSpan.FromSeconds(5));

        // A publish that reached no subscriber routes to recovery, then retires the executor.
        var publish = channel.SetResponse(new OperationResult { Status = OperationStatus.Completed, Message = "lost" }, "corr-reattached");
        var registry = GetExecutorRegistry(channel);
        var deadline = DateTime.UtcNow.AddSeconds(10); // hang guard only: one of the two always happens
        while (!publish.IsCompleted && !AnyExecutorRetiring(registry) && DateTime.UtcNow < deadline)
            await Task.Delay(10);

        await _channelSubscriber.Handler!(_channelSubscriber.SubscribedChannel, Envelope(new OperationResult { Status = OperationStatus.Completed, Message = "second" }));
        releasePredicate.TrySetResult();

        var result = await waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.Equal("second", result.Message);
        await publish.WaitAsync(TimeSpan.FromSeconds(10));
    }

    private static string Envelope(OperationResult payload)
        => JsonSerializer.Serialize(
            new AsyncResponseEnvelope<OperationResult> { Success = true, Payload = payload },
            AsyncResponseEnvelopeOptions<OperationResult>.Instance);

    private static int PendingRetirements(RedisAsyncResponseChannel channel)
        => ((System.Collections.ICollection)typeof(RedisAsyncResponseChannel)
            .GetField("_pendingRetirements", BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(channel)!).Count;

    private static bool AnyExecutorRetiring(object registry)
    {
        var registryType = registry.GetType();
        var gate = registryType.GetField("_gate", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(registry)!;
        var executors = (System.Collections.IDictionary)registryType
            .GetField("_executors", BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(registry)!;
        lock (gate)
        {
            foreach (var entry in executors.Values)
            {
                if ((bool)entry!.GetType().GetProperty("Retiring")!.GetValue(entry)!)
                    return true;
            }
        }

        return false;
    }

    private static object GetExecutorRegistry(RedisAsyncResponseChannel channel)
        => typeof(RedisAsyncResponseChannel)
            .GetField("_executors", BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(channel)!;

    private static bool HasExecutorRegistration(object registry, string channelName)
    {
        var registryType = registry.GetType();
        var gate = registryType.GetField("_gate", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(registry)!;
        var registrations = (Dictionary<string, int>)registryType
            .GetField("_registrations", BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(registry)!;
        lock (gate)
            return registrations.ContainsKey(channelName);
    }

    [Fact]
    public async Task RedisWaiter_DisposeAsyncRunsCleanup()
    {
        var channel = CreateChannel();

        var waiter = await channel.CreateResponseWaiter<OperationResult>(
            "corr-dispose",
            timeout: TimeSpan.FromSeconds(5));

        await waiter.DisposeAsync();

        await Eventually(() => _channelSubscriber.UnsubscribeCount == 1);
        _store.Verify(s => s.TryDeleteAsync("corr-dispose", It.IsAny<Guid>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task SetException_CapsRemoteStackTrace_OnPublish()
    {
        var channel = CreateChannel(new RedisAsyncResponseOptions
        {
            DefaultTimeout = TimeSpan.FromSeconds(5),
            RecoveryStateExpiry = TimeSpan.FromMinutes(5),
            MaxRemoteStackTraceLength = 16
        });
        RedisValue publishedValue = default;
        _subscriber
            .Setup(s => s.PublishAsync(It.IsAny<RedisChannel>(), It.IsAny<RedisValue>(), It.IsAny<CommandFlags>()))
            .Callback<RedisChannel, RedisValue, CommandFlags>((_, value, _) => publishedValue = value)
            .ReturnsAsync(1);

        await channel.SetException(MakeThrownException(), "corr-a");

        using var document = JsonDocument.Parse(publishedValue.ToString());
        var stackTrace = document.RootElement.GetProperty("ExceptionStackTrace").GetString();
        Assert.NotNull(stackTrace);
        Assert.Contains("truncated", stackTrace);
        Assert.True(stackTrace!.Length < 80, $"stack trace was not capped: length {stackTrace.Length}");
    }

    [Fact]
    public async Task SetException_OmitsRemoteStackTrace_WhenDisabled()
    {
        var channel = CreateChannel(new RedisAsyncResponseOptions
        {
            DefaultTimeout = TimeSpan.FromSeconds(5),
            RecoveryStateExpiry = TimeSpan.FromMinutes(5),
            IncludeRemoteStackTrace = false
        });
        RedisValue publishedValue = default;
        _subscriber
            .Setup(s => s.PublishAsync(It.IsAny<RedisChannel>(), It.IsAny<RedisValue>(), It.IsAny<CommandFlags>()))
            .Callback<RedisChannel, RedisValue, CommandFlags>((_, value, _) => publishedValue = value)
            .ReturnsAsync(1);

        await channel.SetException(MakeThrownException(), "corr-a");

        using var document = JsonDocument.Parse(publishedValue.ToString());
        var hasStackTrace = document.RootElement.TryGetProperty("ExceptionStackTrace", out var element)
            && element.ValueKind != JsonValueKind.Null;
        Assert.False(hasStackTrace);
    }

    private static Exception MakeThrownException()
    {
        try
        {
            throw new InvalidOperationException("boom with a real stack trace attached");
        }
        catch (Exception ex)
        {
            return ex;
        }
    }

    private RedisAsyncResponseChannel CreateChannel() => CreateChannel(new RedisAsyncResponseOptions
    {
        DefaultTimeout = TimeSpan.FromSeconds(5),
        RecoveryStateExpiry = TimeSpan.FromMinutes(5)
    });

    private RedisAsyncResponseChannel CreateChannel(
        RedisAsyncResponseOptions options,
        ILogger<RedisAsyncResponseChannel>? logger = null,
        TimeProvider? timeProvider = null) => new(
        _services.GetRequiredService<IServiceScopeFactory>(),
        _multiplexer.Object,
        _store.Object,
        Options.Create(options),
        new AsyncResponseContextPropagation([]),
        logger ?? NullLogger<RedisAsyncResponseChannel>.Instance,
        _channelSubscriber,
        timeProvider);

    [Fact]
    public async Task CreateResponseWaiter_UnsupportedEnvelopeSchema_FaultsWaiter()
    {
        var channel = CreateChannel();
        await using var waiter = await channel.CreateResponseWaiter<OperationResult>(
            "corr-schema",
            timeout: TimeSpan.FromSeconds(5));

        await PublishEnvelope(new AsyncResponseEnvelope<OperationResult>
        {
            SchemaVersion = AsyncResponseEnvelopeSchema.Current + 1,
            Success = true,
            Payload = new OperationResult { Status = OperationStatus.Completed }
        });

        var ex = await Assert.ThrowsAnyAsync<Exception>(() => waiter.ResponseTask);
        Assert.IsType<InvalidOperationException>(ex);
    }

    private Task PublishSuccess(OperationResult payload)
        => PublishEnvelope(new AsyncResponseEnvelope<OperationResult>
        {
            Success = true,
            Payload = payload
        });

    private Task PublishEnvelope(AsyncResponseEnvelope<OperationResult> envelope)
    {
        var json = JsonSerializer.Serialize(envelope, AsyncResponseEnvelopeOptions<OperationResult>.Instance);
        return _channelSubscriber.Handler!.Invoke(_channelSubscriber.SubscribedChannel, json);
    }

    private async Task DuplicateFaultAsync(
        RedisAsyncResponseChannel channel,
        string correlationId,
        RedisValue message,
        Type exceptionType)
    {
        await using var waiter = await channel.CreateResponseWaiter<OperationResult>(correlationId);
        var handler = _channelSubscriber.Handler!;
        var subscribed = _channelSubscriber.SubscribedChannel;
        await handler(subscribed, message);
        await handler(subscribed, message);

        var exception = await Assert.ThrowsAnyAsync<Exception>(() =>
            waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(2)));
        Assert.True(exceptionType.IsAssignableFrom(exception.GetType()));
    }

    private static async Task Eventually(Func<bool> condition)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        while (!condition())
        {
            await Task.Delay(10, timeout.Token);
        }
    }

    [Fact]
    public void ServiceCollectionExtensions_ThrowsWhenNoConnectionStringOrMultiplexer()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddAsyncResponse().WithRedisChannel();

        var provider = services.BuildServiceProvider();
        Assert.Throws<InvalidOperationException>(() => provider.GetRequiredService<RedisAsyncResponseChannel>());
    }

    [Fact]
    public async Task CreateResponseWaiter_SynchronousCompletion_HandlesDisposedCts()
    {
        var channelName = "corr-sync";
        var validEnvelope = new AsyncResponseEnvelope<OperationResult>
        {
            Success = true,
            Payload = new OperationResult { Status = OperationStatus.Completed }
        };
        var json = JsonSerializer.Serialize(validEnvelope, AsyncResponseEnvelopeOptions<OperationResult>.Instance);

        // Deliver the terminal message during SubscribeAsync itself, so cleanup can dispose the
        // timeout CTS before CreateResponseWaiterCore reaches CancelAfter.
        _channelSubscriber.InvokeOnSubscribe = json;
        var channel = CreateChannel(new RedisAsyncResponseOptions(), new TestLogger<RedisAsyncResponseChannel>());

        await using var waiter = await channel.CreateResponseWaiter<OperationResult>(channelName);
        var result = await waiter.ResponseTask;
        Assert.Equal(OperationStatus.Completed, result.Status);
    }

    [Fact]
    public async Task CountActiveSubscribersAsync_HandlesServerException()
    {
        var mockSubscriber = new Mock<ISubscriber>();
        var multiplexer = new Mock<IConnectionMultiplexer>();
        multiplexer.Setup(m => m.GetSubscriber(It.IsAny<object?>())).Returns(mockSubscriber.Object);

        var endPoint = new Mock<EndPoint>().Object;
        multiplexer.Setup(m => m.GetEndPoints(It.IsAny<bool>())).Returns([endPoint]);

        var server = new Mock<IServer>();
        server.SetupGet(s => s.IsConnected).Returns(true);
        server.Setup(s => s.SubscriptionSubscriberCountAsync(It.IsAny<RedisChannel>(), It.IsAny<CommandFlags>()))
            .ThrowsAsync(new InvalidOperationException("Redis command failed"));

        multiplexer.Setup(m => m.GetServer(endPoint, It.IsAny<object?>())).Returns(server.Object);

        var channel = new RedisAsyncResponseChannel(
            _services.GetRequiredService<IServiceScopeFactory>(),
            multiplexer.Object,
            _store.Object,
            Options.Create(new RedisAsyncResponseOptions()),
            new AsyncResponseContextPropagation([]),
            new TestLogger<RedisAsyncResponseChannel>());

        // Nothing could be probed: negative = "unknown" (the watchdog's could-not-probe
        // contract), never 0 — 0 would assert there is definitively no live waiter.
        var count = await channel.CountActiveSubscribersAsync("corr");
        Assert.Equal(-1, count);
    }

    /// <summary>
    /// PUBSUB NUMSUB is node-local and the response channels are key-routed, so the subscription
    /// lives on ONE node. A zero collected while that node was unreachable is the absence of a
    /// waiter on the nodes that answered, not proof there is none.
    /// </summary>
    [Theory]
    // Cluster: the slot owner is unreachable, a sibling primary answers its own node-local zero,
    // and no node table says otherwise: unknown. Read as 0, it consumed a live waiter's recovery
    // registration.
    [InlineData(true, false, false, 0L, -1L)]
    // Outside a cluster the unreachable "primary" is the old one of a failover, which the
    // multiplexer keeps listing (disconnected) until it rejoins: once it has been down for the
    // failover grace, the promoted primary's zero is the whole answer, as it is for the recovery
    // scan. Read as unknown, every lost-subscriber publish threw until the old primary came back
    // or the process restarted.
    [InlineData(false, false, false, 0L, 0L)]
    // Every primary answered: a zero is now the whole answer.
    [InlineData(true, true, false, 0L, 0L)]
    // A replica holds no key-routed subscription, so its absence decides nothing.
    [InlineData(true, false, true, 0L, 0L)]
    // A positive count is proof of a live waiter wherever it was read.
    [InlineData(true, false, false, 3L, 3L)]
    public async Task CountActiveSubscribersAsync_ReportsZeroOnlyWhenEveryPrimaryAnswered(
        bool cluster,
        bool unreachableIsConnected,
        bool unreachableIsReplica,
        long reachableCount,
        long expected)
    {
        var mockSubscriber = new Mock<ISubscriber>();
        var multiplexer = new Mock<IConnectionMultiplexer>();
        multiplexer.Setup(m => m.GetSubscriber(It.IsAny<object?>())).Returns(mockSubscriber.Object);

        var ownerEndPoint = new Mock<EndPoint>().Object;
        var siblingEndPoint = new Mock<EndPoint>().Object;
        multiplexer.Setup(m => m.GetEndPoints(It.IsAny<bool>())).Returns([ownerEndPoint, siblingEndPoint]);

        var serverType = cluster ? ServerType.Cluster : ServerType.Standalone;
        var owner = new Mock<IServer>();
        owner.SetupGet(s => s.IsConnected).Returns(unreachableIsConnected);
        owner.SetupGet(s => s.IsReplica).Returns(unreachableIsReplica);
        owner.SetupGet(s => s.ServerType).Returns(serverType);
        owner.Setup(s => s.SubscriptionSubscriberCountAsync(It.IsAny<RedisChannel>(), It.IsAny<CommandFlags>()))
            .ReturnsAsync(0L);
        multiplexer.Setup(m => m.GetServer(ownerEndPoint, It.IsAny<object?>())).Returns(owner.Object);

        var sibling = new Mock<IServer>();
        sibling.SetupGet(s => s.IsConnected).Returns(true);
        sibling.SetupGet(s => s.ServerType).Returns(serverType);
        sibling.Setup(s => s.SubscriptionSubscriberCountAsync(It.IsAny<RedisChannel>(), It.IsAny<CommandFlags>()))
            .ReturnsAsync(reachableCount);
        multiplexer.Setup(m => m.GetServer(siblingEndPoint, It.IsAny<object?>())).Returns(sibling.Object);

        var clock = new VirtualTimeProvider();
        var channel = new RedisAsyncResponseChannel(
            _services.GetRequiredService<IServiceScopeFactory>(),
            multiplexer.Object,
            _store.Object,
            Options.Create(new RedisAsyncResponseOptions()),
            new AsyncResponseContextPropagation([]),
            new TestLogger<RedisAsyncResponseChannel>(),
            timeProvider: clock);

        // The verdict once any disconnection has outlasted the failover grace (pinned separately).
        await channel.CountActiveSubscribersAsync("corr");
        clock.Advance(RedisAsyncResponseChannel.DisconnectedEndPointGrace);

        Assert.Equal(expected, await channel.CountActiveSubscribersAsync("corr"));
    }

    /// <summary>
    /// A primary that answers with a failure is no more probed than one that never answered:
    /// the exception arm must not leave a zero looking conclusive either.
    /// </summary>
    [Fact]
    public async Task CountActiveSubscribersAsync_PrimaryThatThrows_MakesAZeroUnknown()
    {
        var mockSubscriber = new Mock<ISubscriber>();
        var multiplexer = new Mock<IConnectionMultiplexer>();
        multiplexer.Setup(m => m.GetSubscriber(It.IsAny<object?>())).Returns(mockSubscriber.Object);

        var faultingEndPoint = new Mock<EndPoint>().Object;
        var siblingEndPoint = new Mock<EndPoint>().Object;
        multiplexer.Setup(m => m.GetEndPoints(It.IsAny<bool>())).Returns([faultingEndPoint, siblingEndPoint]);

        var faulting = new Mock<IServer>();
        faulting.SetupGet(s => s.IsConnected).Returns(true);
        faulting.Setup(s => s.SubscriptionSubscriberCountAsync(It.IsAny<RedisChannel>(), It.IsAny<CommandFlags>()))
            .ThrowsAsync(new RedisTimeoutException(CommandFlags.None, "probe timed out", CommandStatus.WaitingInBacklog));
        multiplexer.Setup(m => m.GetServer(faultingEndPoint, It.IsAny<object?>())).Returns(faulting.Object);

        var sibling = new Mock<IServer>();
        sibling.SetupGet(s => s.IsConnected).Returns(true);
        sibling.Setup(s => s.SubscriptionSubscriberCountAsync(It.IsAny<RedisChannel>(), It.IsAny<CommandFlags>()))
            .ReturnsAsync(0L);
        multiplexer.Setup(m => m.GetServer(siblingEndPoint, It.IsAny<object?>())).Returns(sibling.Object);

        var channel = new RedisAsyncResponseChannel(
            _services.GetRequiredService<IServiceScopeFactory>(),
            multiplexer.Object,
            _store.Object,
            Options.Create(new RedisAsyncResponseOptions()),
            new AsyncResponseContextPropagation([]),
            new TestLogger<RedisAsyncResponseChannel>());

        Assert.Equal(-1, await channel.CountActiveSubscribersAsync("corr"));
    }

    // -----------------------------------------------------------------------------------------
    // Fixpoint r1 (S6a#1, GS4#6). The round-42 probe counted every disconnected endpoint without
    // the replica flag as a possible slot owner, while the recovery scan had long excused the
    // same nodes through the cluster's node table. A replica down since process start (never
    // handshaken, so not flagged) or a seed entry the cluster no longer lists therefore made the
    // probe answer "unknown" for every correlation id: every lost-subscriber publish threw and the
    // watchdog saw every registration as unknown, until the node came back or the process
    // restarted. The probe now applies the scan's rule: once every slot owner the table lists has
    // answered, the rest cannot hold a key-routed subscription.
    // -----------------------------------------------------------------------------------------

    private const string ProbeClusterNodeTable =
        "07c37dfeb235213a872192d90877d0cd55635b91 10.0.0.1:6379@16379 myself,master - 0 0 1 connected 0-8191\n" +
        "67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1 10.0.0.2:6379@16379 master - 0 1426238316232 2 connected 8192-16383\n" +
        "292f8b365bb7edb5e285caf0b7e6ddc7265d2f4f 10.0.0.7:6379@16379 slave 67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1 0 1426238317741 2 disconnected\n";

    [Theory]
    // A replica down since before this process started: never handshaken, so not flagged as one.
    [InlineData("10.0.0.7", ProbeClusterNodeTable, 0L)]
    // A configured seed endpoint the cluster no longer lists (decommissioned, DNS gone).
    [InlineData("10.0.0.9", ProbeClusterNodeTable, 0L)]
    // A table that cannot be read leaves the unknown endpoint unknown.
    [InlineData("10.0.0.9", null, -1L)]
    public async Task CountActiveSubscribersAsync_ClusterNodeTable_ExcusesADisconnectedNodeThatCannotOwnTheChannel(
        string disconnectedAddress,
        string? nodeTable,
        long expected)
    {
        var shardA = ProbeClusterServer("10.0.0.1", connected: true, subscribers: 0);
        shardA.Setup(s => s.ClusterNodesRawAsync(It.IsAny<CommandFlags>())).ReturnsAsync(nodeTable);
        var shardB = ProbeClusterServer("10.0.0.2", connected: true, subscribers: 0);
        shardB.Setup(s => s.ClusterNodesRawAsync(It.IsAny<CommandFlags>())).ReturnsAsync(nodeTable);
        var disconnected = ProbeClusterServer(disconnectedAddress, connected: false, subscribers: 0);

        var clock = new VirtualTimeProvider();
        var channel = CreateProbeChannel(clock, shardA, shardB, disconnected);
        await channel.CountActiveSubscribersAsync("corr");
        clock.Advance(RedisAsyncResponseChannel.DisconnectedEndPointGrace);

        Assert.Equal(expected, await channel.CountActiveSubscribersAsync("corr"));
    }

    /// <summary>The table excuses nothing that owns slots: an unreachable shard keeps the zero unknown.</summary>
    [Fact]
    public async Task CountActiveSubscribersAsync_ClusterNodeTable_AnUnreachableSlotOwnerStaysUnknown()
    {
        var shardA = ProbeClusterServer("10.0.0.1", connected: true, subscribers: 0);
        shardA.Setup(s => s.ClusterNodesRawAsync(It.IsAny<CommandFlags>())).ReturnsAsync(ProbeClusterNodeTable);
        var shardB = ProbeClusterServer("10.0.0.2", connected: false, subscribers: 0);
        var downReplica = ProbeClusterServer("10.0.0.7", connected: false, subscribers: 0);

        var channel = CreateProbeChannel(shardA, shardB, downReplica);

        Assert.Equal(-1, await channel.CountActiveSubscribersAsync("corr"));
    }

    /// <summary>
    /// The lost-subscriber publish routes the same verdict: with the never-connected replica
    /// excused, a publish that reaches nobody consumes the registration instead of throwing.
    /// </summary>
    [Fact]
    public async Task SetResponse_ClusterWithANeverConnectedReplica_RoutesTheLostResponseInsteadOfThrowing()
    {
        var shardA = ProbeClusterServer("10.0.0.1", connected: true, subscribers: 0);
        shardA.Setup(s => s.ClusterNodesRawAsync(It.IsAny<CommandFlags>())).ReturnsAsync(ProbeClusterNodeTable);
        var shardB = ProbeClusterServer("10.0.0.2", connected: true, subscribers: 0);
        var neverConnectedReplica = ProbeClusterServer("10.0.0.7", connected: false, subscribers: 0);
        var clock = new VirtualTimeProvider();
        var channel = CreateProbeChannel(clock, shardA, shardB, neverConnectedReplica);
        _subscriber
            .Setup(s => s.PublishAsync(It.IsAny<RedisChannel>(), It.IsAny<RedisValue>(), It.IsAny<CommandFlags>()))
            .ReturnsAsync(0L);
        _store
            .Setup(s => s.GetAllAsync("corr-lost", It.IsAny<CancellationToken>()))
            .ReturnsAsync(Array.Empty<RecoveryState>());

        // Past the failover grace (a replica that never connected is only excused once it has
        // stayed down that long).
        await channel.CountActiveSubscribersAsync("corr-lost");
        clock.Advance(RedisAsyncResponseChannel.DisconnectedEndPointGrace);

        await channel.SetResponse(new OperationResult { Status = OperationStatus.Completed }, "corr-lost");
    }

    /// <summary>
    /// Pre-commit review of fixpoint round 1: a disconnected primary was excused the moment it
    /// went down. That moment IS the failover window — this process already routes to the promoted
    /// node, whose zero only means the waiter's multiplexer (another process) has not moved its
    /// subscription there yet — so the waiter's registration was consumed and its recovery
    /// callback ran while it re-subscribed a moment later: a double resume. The old node is now
    /// excused only after it has stayed down for the grace, and a reconnection starts it over.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task CountActiveSubscribersAsync_JustDisconnectedPrimary_StaysUnknownUntilItHasBeenDownForTheGrace(bool cluster)
    {
        // After the failover: the old owner (10.0.0.2) is down and flagged failed, its slots moved
        // to the promoted replica (10.0.0.7), which answers zero — the waiter has not re-subscribed.
        const string failedOverTable =
            "07c37dfeb235213a872192d90877d0cd55635b91 10.0.0.1:6379@16379 myself,master - 0 0 1 connected 0-8191\n" +
            "67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1 10.0.0.2:6379@16379 master,fail - 0 1426238316232 2 disconnected\n" +
            "292f8b365bb7edb5e285caf0b7e6ddc7265d2f4f 10.0.0.7:6379@16379 master - 0 1426238317741 3 connected 8192-16383\n";
        var serverType = cluster ? ServerType.Cluster : ServerType.Standalone;
        var shardA = ProbeClusterServer("10.0.0.1", connected: true, subscribers: 0);
        shardA.SetupGet(s => s.ServerType).Returns(serverType);
        shardA.Setup(s => s.ClusterNodesRawAsync(It.IsAny<CommandFlags>())).ReturnsAsync(failedOverTable);
        var oldOwnerConnected = false;
        var oldOwner = ProbeClusterServer("10.0.0.2", connected: false, subscribers: 0);
        oldOwner.SetupGet(s => s.IsConnected).Returns(() => oldOwnerConnected);
        oldOwner.SetupGet(s => s.ServerType).Returns(serverType);
        var promoted = ProbeClusterServer("10.0.0.7", connected: true, subscribers: 0);
        promoted.SetupGet(s => s.ServerType).Returns(serverType);
        var clock = new VirtualTimeProvider();
        var channel = CreateProbeChannel(clock, shardA, oldOwner, promoted);

        Assert.Equal(-1, await channel.CountActiveSubscribersAsync("corr"));
        clock.Advance(RedisAsyncResponseChannel.DisconnectedEndPointGrace - TimeSpan.FromSeconds(1));
        Assert.Equal(-1, await channel.CountActiveSubscribersAsync("corr"));
        clock.Advance(TimeSpan.FromSeconds(1));
        Assert.Equal(0, await channel.CountActiveSubscribersAsync("corr"));

        // Only a CONTINUOUS disconnection counts: a node that came back and dropped again is a
        // fresh failover window.
        oldOwnerConnected = true;
        Assert.Equal(0, await channel.CountActiveSubscribersAsync("corr"));
        oldOwnerConnected = false;
        Assert.Equal(-1, await channel.CountActiveSubscribersAsync("corr"));
        clock.Advance(RedisAsyncResponseChannel.DisconnectedEndPointGrace);
        Assert.Equal(0, await channel.CountActiveSubscribersAsync("corr"));
    }

    /// <summary>Inside the grace the lost-subscriber publish throws for a retry instead of consuming the registration.</summary>
    [Fact]
    public async Task SetResponse_PrimaryDisconnectedMomentsAgo_ThrowsForARetryAndKeepsTheRegistration()
    {
        var shardA = ProbeClusterServer("10.0.0.1", connected: true, subscribers: 0);
        shardA.Setup(s => s.ClusterNodesRawAsync(It.IsAny<CommandFlags>())).ReturnsAsync(ProbeClusterNodeTable);
        var shardB = ProbeClusterServer("10.0.0.2", connected: true, subscribers: 0);
        var justDown = ProbeClusterServer("10.0.0.7", connected: false, subscribers: 0);
        var channel = CreateProbeChannel(new VirtualTimeProvider(), shardA, shardB, justDown);
        _subscriber
            .Setup(s => s.PublishAsync(It.IsAny<RedisChannel>(), It.IsAny<RedisValue>(), It.IsAny<CommandFlags>()))
            .ReturnsAsync(0L);
        var registration = new RecoveryState
        {
            RegistrationId = Guid.NewGuid(),
            CorrelationId = "corr-failover",
            ResumeCallback = new ReflectionCallDto { ServiceInterfaceFullName = "Svc", MethodName = "Resume", Params = [] }
        };
        _store
            .Setup(s => s.GetAllAsync("corr-failover", It.IsAny<CancellationToken>()))
            .ReturnsAsync([registration]);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => channel.SetResponse(new OperationResult { Status = OperationStatus.Completed }, "corr-failover"));

        _store.Verify(s => s.TryDeleteAsync(It.IsAny<string>(), It.IsAny<Guid>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    /// <summary>
    /// The endpoints are asked concurrently: one after another, every hung-but-connected node
    /// added its whole command timeout to every probe (twice per lost-subscriber publish, and
    /// again on every ingress retry).
    /// </summary>
    [Fact]
    public async Task CountActiveSubscribersAsync_AsksEveryEndpointBeforeWaitingOnAnyOfThem()
    {
        var hungAnswer = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously);
        var hung = ProbeClusterServer("10.0.0.1", connected: true, subscribers: 0);
        hung.Setup(s => s.SubscriptionSubscriberCountAsync(It.IsAny<RedisChannel>(), It.IsAny<CommandFlags>()))
            .Returns(hungAnswer.Task);
        var other = ProbeClusterServer("10.0.0.2", connected: true, subscribers: 0);
        var channel = CreateProbeChannel(hung, other);

        var probe = channel.CountActiveSubscribersAsync("corr").AsTask();

        other.Verify(s => s.SubscriptionSubscriberCountAsync(It.IsAny<RedisChannel>(), It.IsAny<CommandFlags>()), Times.Once);
        Assert.False(probe.IsCompleted);
        hungAnswer.SetResult(0);
        Assert.Equal(0, await probe);
    }

    private static Mock<IServer> ProbeClusterServer(string address, bool connected, long subscribers)
    {
        var server = new Mock<IServer>();
        server.SetupGet(s => s.IsConnected).Returns(connected);
        server.SetupGet(s => s.ServerType).Returns(ServerType.Cluster);
        server.SetupGet(s => s.EndPoint).Returns(new IPEndPoint(IPAddress.Parse(address), 6379));
        server.Setup(s => s.SubscriptionSubscriberCountAsync(It.IsAny<RedisChannel>(), It.IsAny<CommandFlags>()))
            .ReturnsAsync(subscribers);
        return server;
    }

    private RedisAsyncResponseChannel CreateProbeChannel(params Mock<IServer>[] servers)
        => CreateProbeChannel(timeProvider: null, servers);

    private RedisAsyncResponseChannel CreateProbeChannel(TimeProvider? timeProvider, params Mock<IServer>[] servers)
    {
        var endPoints = servers.Select(server => server.Object.EndPoint!).ToArray();
        _multiplexer.Setup(m => m.GetEndPoints(It.IsAny<bool>())).Returns(endPoints);
        foreach (var server in servers)
            _multiplexer.Setup(m => m.GetServer(server.Object.EndPoint!, It.IsAny<object?>())).Returns(server.Object);

        return new RedisAsyncResponseChannel(
            _services.GetRequiredService<IServiceScopeFactory>(),
            _multiplexer.Object,
            _store.Object,
            Options.Create(new RedisAsyncResponseOptions()),
            new AsyncResponseContextPropagation([]),
            new TestLogger<RedisAsyncResponseChannel>(),
            _channelSubscriber,
            timeProvider);
    }
}
