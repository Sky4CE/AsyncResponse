using AsyncResponse.Transports.Redis;
using Moq;
using StackExchange.Redis;
using System.Reflection;
using System.Runtime.CompilerServices;
using Xunit;

namespace AsyncResponse.Tests;

public class RedisStreamDatabaseAdapterTests
{
    [Fact]
    public async Task StreamMethods_ForwardArgumentsToStackExchangeRedis()
    {
        var values = new[] { new NameValueEntry("payload", "{}") };
        var readEntries = new[] { RedisTransportTests.Entry("1-0", ("payload", "read")) };
        var pending = new[] { Pending("1-0", "consumer-a", 123, 4) };
        var claimed = new[] { RedisTransportTests.Entry("1-0", ("payload", "claimed")) };
        var database = new Mock<IDatabase>(MockBehavior.Strict);
        // The default (ServerDefault) trim strategy forwards to the classic overload — no Redis 8
        // trim-mode token — so `XADD … MAXLEN ~ N` runs on Valkey/Dragonfly as well as Redis 8+.
        database
            .Setup(db => db.StreamAddAsync(
                "stream",
                values,
                (RedisValue?)null,
                123,
                true,
                CommandFlags.None))
            .ReturnsAsync("42-0");
        database
            .Setup(db => db.StreamCreateConsumerGroupAsync(
                "stream",
                "group",
                (RedisValue)StreamPosition.Beginning,
                true,
                CommandFlags.None))
            .ReturnsAsync(true);
        database
            .Setup(db => db.StreamReadGroupAsync(
                "stream",
                "group",
                "consumer",
                StreamPosition.NewMessages,
                7,
                false,
                null,
                CommandFlags.None))
            .ReturnsAsync(readEntries);
        database
            .Setup(db => db.StreamAcknowledgeAsync(
                "stream",
                "group",
                "1-0",
                CommandFlags.None))
            .ReturnsAsync(1);
        database
            .Setup(db => db.StreamPendingMessagesAsync(
                "stream",
                "group",
                11,
                "consumer",
                "0-0",
                "9-0",
                250L,
                CommandFlags.None))
            .ReturnsAsync(pending);
        database
            .Setup(db => db.StreamClaimAsync(
                "stream",
                "group",
                "consumer",
                250L,
                It.Is<RedisValue[]>(ids => ids.SequenceEqual(new RedisValue[] { "1-0" })),
                CommandFlags.None))
            .ReturnsAsync(claimed);
        var claimedIds = new RedisValue[] { "1-0" };
        database
            .Setup(db => db.StreamClaimIdsOnlyAsync(
                "stream",
                "group",
                "consumer",
                0L,
                It.Is<RedisValue[]>(ids => ids.SequenceEqual(new RedisValue[] { "1-0" })),
                CommandFlags.None))
            .ReturnsAsync(claimedIds);
        var adapter = new RedisStreamDatabaseAdapter(database.Object, TimeSpan.FromSeconds(5));

        Assert.Equal("42-0", await adapter.StreamAddAsync("stream", values, 123, true, CancellationToken.None));
        Assert.True(await adapter.StreamCreateConsumerGroupAsync("stream", "group", StreamPosition.Beginning, true, CancellationToken.None));
        Assert.Same(readEntries, await adapter.StreamReadGroupAsync("stream", "group", "consumer", 7, CancellationToken.None));
        Assert.Equal(1, await adapter.StreamAcknowledgeAsync("stream", "group", "1-0", CancellationToken.None));
        Assert.Same(pending, await adapter.StreamPendingMessagesAsync("stream", "group", 11, "consumer", "0-0", "9-0", 250, CancellationToken.None));
        Assert.Same(claimed, await adapter.StreamClaimAsync("stream", "group", "consumer", 250, ["1-0"], CancellationToken.None));
        Assert.Same(claimedIds, await adapter.StreamClaimIdsOnlyAsync("stream", "group", "consumer", 0, ["1-0"], CancellationToken.None));
        database.VerifyAll();
    }

    [Fact]
    public async Task StreamAdd_UsesTokenlessOverload_ForPortabilityAcrossRespServers()
    {
        // Publishing must emit `XADD … MAXLEN ~ N` with no Redis 8 trim-mode token so it runs on
        // Valkey and Dragonfly as well as Redis 8+. The adapter proves this by calling the classic
        // overload (int? maxLength, no StreamTrimMode parameter); the strict mock only accepts that
        // shape, so a regression to the trim-mode overload fails the test.
        var values = new[] { new NameValueEntry("payload", "{}") };
        var database = new Mock<IDatabase>(MockBehavior.Strict);
        database
            .Setup(db => db.StreamAddAsync(
                "stream",
                values,
                (RedisValue?)null,
                int.MaxValue,
                true,
                CommandFlags.None))
            .ReturnsAsync("77-0");

        var adapter = new RedisStreamDatabaseAdapter(database.Object, TimeSpan.FromSeconds(5));

        // A long maxLength above int range clamps into the classic overload rather than overflowing.
        Assert.Equal("77-0", await adapter.StreamAddAsync("stream", values, long.MaxValue, true, CancellationToken.None));
        database.VerifyAll();
    }

    [Fact]
    public async Task StreamMethods_HonorCallerCancellationWhileRedisCommandIsPending()
    {
        var never = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously);
        var database = new Mock<IDatabase>();
        database
            .Setup(db => db.StreamAcknowledgeAsync(
                It.IsAny<RedisKey>(),
                It.IsAny<RedisValue>(),
                It.IsAny<RedisValue>(),
                It.IsAny<CommandFlags>()))
            .Returns(never.Task);
        var adapter = new RedisStreamDatabaseAdapter(database.Object, TimeSpan.FromSeconds(30));
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            adapter.StreamAcknowledgeAsync("stream", "group", "1-0", cts.Token));
    }

    /// <summary>
    /// Fixpoint round 2 (S6a#3). The adapter built each command BEFORE looking at the caller's
    /// token, and StackExchange.Redis queues a command the moment it is called: with a stop already
    /// requested, XREADGROUP and XCLAIM still reached the server, moving entries into the stopping
    /// consumer's pending list while their replies were discarded. A cancelled caller now sends
    /// nothing. Pre-fix: every one of these calls reaches the strict mock and throws a
    /// <see cref="MockException"/> instead of cancelling.
    /// </summary>
    [Fact]
    public async Task StreamMethods_WithAnAlreadyCancelledToken_SendNothing()
    {
        var database = new Mock<IDatabase>(MockBehavior.Strict);
        var adapter = new RedisStreamDatabaseAdapter(database.Object, TimeSpan.FromSeconds(30));
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();
        var token = cts.Token;

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => adapter.StreamReadGroupAsync("stream", "group", "consumer", 1, token));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => adapter.StreamClaimAsync("stream", "group", "consumer", 250, ["1-0"], token));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => adapter.StreamClaimIdsOnlyAsync("stream", "group", "consumer", 0, ["1-0"], token));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => adapter.StreamPendingMessagesAsync("stream", "group", 1, RedisValue.Null, null, null, 250, token));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => adapter.StreamAcknowledgeAsync("stream", "group", "1-0", token));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => adapter.StreamAddAsync("stream", [new NameValueEntry("payload", "{}")], null, true, token));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => adapter.StreamAddOnceAsync("stream", "dedup", TimeSpan.FromMinutes(1), [new NameValueEntry("payload", "{}")], null, true, token));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => adapter.StreamCreateConsumerGroupAsync("stream", "group", StreamPosition.Beginning, true, token));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => adapter.TryDeleteIdleConsumerAsync("stream", "group", "consumer", token));

        Assert.Empty(database.Invocations);
    }

    [Fact]
    public async Task StreamMethods_ApplyOperationTimeoutWhileRedisCommandIsPending()
    {
        var never = new TaskCompletionSource<RedisValue>(TaskCreationOptions.RunContinuationsAsynchronously);
        var database = new Mock<IDatabase>();
        database
            .Setup(db => db.StreamAddAsync(
                It.IsAny<RedisKey>(),
                It.IsAny<NameValueEntry[]>(),
                It.IsAny<RedisValue?>(),
                It.IsAny<int?>(),
                It.IsAny<bool>(),
                It.IsAny<CommandFlags>()))
            .Returns(never.Task);
        var adapter = new RedisStreamDatabaseAdapter(database.Object, TimeSpan.FromMilliseconds(10));

        // An operation timeout (as opposed to caller cancellation) surfaces as TimeoutException so the
        // publish/subscriber retry paths classify it as transient rather than as an intentional cancel.
        await Assert.ThrowsAsync<TimeoutException>(() =>
            adapter.StreamAddAsync("stream", [new NameValueEntry("payload", "{}")], null, true, CancellationToken.None));
    }

    /// <summary>
    /// Fixpoint r1 (S6a#14): the consumer retirement is one atomic script on the stream's key —
    /// delete only while the consumer has no pending entries — and reports whether it deleted.
    /// </summary>
    [Theory]
    [InlineData(0L, true)]
    [InlineData(-1L, false)]
    public async Task TryDeleteIdleConsumer_RunsTheAtomicScriptOnTheStreamKey(long scriptResult, bool deleted)
    {
        var database = new Mock<IDatabase>(MockBehavior.Strict);
        database
            .Setup(db => db.ScriptEvaluateAsync(
                RedisStreamDatabaseAdapter.DeleteIdleConsumerScript,
                It.Is<RedisKey[]>(keys => keys.Length == 1 && keys[0] == "stream"),
                It.Is<RedisValue[]>(values => values.Length == 2 && values[0] == "group" && values[1] == "consumer-a"),
                CommandFlags.None))
            .ReturnsAsync(RedisResult.Create((RedisValue)scriptResult));
        var adapter = new RedisStreamDatabaseAdapter(database.Object, TimeSpan.FromSeconds(5));

        Assert.Equal(deleted, await adapter.TryDeleteIdleConsumerAsync("stream", "group", "consumer-a", CancellationToken.None));
        Assert.Contains("XPENDING", RedisStreamDatabaseAdapter.DeleteIdleConsumerScript, StringComparison.Ordinal);
        Assert.Contains("DELCONSUMER", RedisStreamDatabaseAdapter.DeleteIdleConsumerScript, StringComparison.Ordinal);
    }

    /// <summary>
    /// Fixpoint r1 (S6a#22). A command the adapter gives up on (caller cancellation, or the
    /// operation timeout) keeps running in the multiplexer, and nothing observed it: a fault it
    /// raised later surfaced only as a <see cref="TaskScheduler.UnobservedTaskException"/>. The
    /// abandoned command is now observed. Filtered to this test's own exception, so unrelated
    /// unobserved tasks from other tests cannot leak in.
    /// </summary>
    [Fact]
    public void AbandonedCommand_ThatFaultsLater_IsObserved()
    {
        var marker = $"late redis fault {Guid.NewGuid():N}";
        var unobserved = 0;
        void OnUnobserved(object? sender, UnobservedTaskExceptionEventArgs args)
        {
            if (args.Exception.Flatten().InnerExceptions.Any(inner => inner.Message == marker))
                Interlocked.Increment(ref unobserved);
        }

        TaskScheduler.UnobservedTaskException += OnUnobserved;
        try
        {
            var command = AbandonThenFault(marker);
            for (var pass = 0; pass < 3 && command.IsAlive; pass++)
            {
                GC.Collect();
                GC.WaitForPendingFinalizers();
            }

            GC.Collect();
            GC.WaitForPendingFinalizers();

            // Not collected (an instrumented or debug run can extend lifetimes): no finalizer ran,
            // so the check below would prove nothing either way.
            if (command.IsAlive)
                Assert.Skip("The abandoned command was not collected, so its unobserved-exception check cannot run.");
        }
        finally
        {
            TaskScheduler.UnobservedTaskException -= OnUnobserved;
        }

        Assert.Equal(0, Volatile.Read(ref unobserved));
    }

    /// <summary>
    /// Starts an acknowledge, abandons it by cancelling the caller token, then faults the command —
    /// keeping nothing that references it but a weak handle.
    /// </summary>
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static WeakReference AbandonThenFault(string marker)
    {
        var command = new TaskCompletionSource<long>();
        var database = new Mock<IDatabase>();
        database
            .Setup(db => db.StreamAcknowledgeAsync(It.IsAny<RedisKey>(), It.IsAny<RedisValue>(), It.IsAny<RedisValue>(), It.IsAny<CommandFlags>()))
            .Returns(command.Task);
        var adapter = new RedisStreamDatabaseAdapter(database.Object, TimeSpan.FromMinutes(1));

        using (var cancellation = new CancellationTokenSource())
        {
            var acknowledge = adapter.StreamAcknowledgeAsync("stream", "group", "1-0", cancellation.Token);
            cancellation.Cancel();
            Assert.ThrowsAny<OperationCanceledException>(() => acknowledge.GetAwaiter().GetResult());
        }

        command.SetException(new InvalidOperationException(marker));
        database.Reset();
        database.Invocations.Clear();
        return new WeakReference(command.Task);
    }

    private static StreamPendingMessageInfo Pending(
        RedisValue messageId,
        RedisValue consumerName,
        long idleTimeInMilliseconds,
        int deliveryCount)
        => (StreamPendingMessageInfo)PendingConstructor.Invoke(
            [messageId, consumerName, idleTimeInMilliseconds, deliveryCount]);

    private static readonly ConstructorInfo PendingConstructor =
        typeof(StreamPendingMessageInfo).GetConstructor(
            BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic,
            binder: null,
            [typeof(RedisValue), typeof(RedisValue), typeof(long), typeof(int)],
            modifiers: null)
        ?? throw new InvalidOperationException("StreamPendingMessageInfo constructor was not found.");
}
