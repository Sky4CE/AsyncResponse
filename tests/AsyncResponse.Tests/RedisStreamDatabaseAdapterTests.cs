using AsyncResponse.Transports.Redis;
using Moq;
using StackExchange.Redis;
using System.Reflection;
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
        var adapter = new RedisStreamDatabaseAdapter(database.Object, TimeSpan.FromSeconds(5));

        Assert.Equal("42-0", await adapter.StreamAddAsync("stream", values, 123, true, CancellationToken.None));
        Assert.True(await adapter.StreamCreateConsumerGroupAsync("stream", "group", StreamPosition.Beginning, true, CancellationToken.None));
        Assert.Same(readEntries, await adapter.StreamReadGroupAsync("stream", "group", "consumer", 7, CancellationToken.None));
        Assert.Equal(1, await adapter.StreamAcknowledgeAsync("stream", "group", "1-0", CancellationToken.None));
        Assert.Same(pending, await adapter.StreamPendingMessagesAsync("stream", "group", 11, "consumer", "0-0", "9-0", 250, CancellationToken.None));
        Assert.Same(claimed, await adapter.StreamClaimAsync("stream", "group", "consumer", 250, ["1-0"], CancellationToken.None));
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
