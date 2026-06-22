using AsyncResponse.Transports.Redis;
using StackExchange.Redis;

namespace AsyncResponse.Benchmarks;

internal sealed class BenchmarkRedisStreamDatabase : IRedisStreamDatabase
{
    private int _acks;
    private int _adds;

    public int Acks => Volatile.Read(ref _acks);
    public int Adds => Volatile.Read(ref _adds);

    public Task<RedisValue> StreamAddAsync(
        RedisKey stream,
        NameValueEntry[] values,
        long? maxLength,
        bool useApproximateMaxLength,
        CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref _adds);
        return Task.FromResult<RedisValue>($"{Adds}-0");
    }

    public Task<bool> StreamCreateConsumerGroupAsync(
        RedisKey stream,
        RedisValue groupName,
        RedisValue position,
        bool createStream,
        CancellationToken cancellationToken)
        => Task.FromResult(true);

    public Task<StreamEntry[]> StreamReadGroupAsync(
        RedisKey stream,
        RedisValue groupName,
        RedisValue consumerName,
        int count,
        CancellationToken cancellationToken)
        => Task.FromResult(Array.Empty<StreamEntry>());

    public Task<long> StreamAcknowledgeAsync(
        RedisKey stream,
        RedisValue groupName,
        RedisValue messageId,
        CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref _acks);
        return Task.FromResult(1L);
    }

    public Task<StreamPendingMessageInfo[]> StreamPendingMessagesAsync(
        RedisKey stream,
        RedisValue groupName,
        int count,
        RedisValue consumerName,
        RedisValue? minId,
        RedisValue? maxId,
        long minIdleTimeInMilliseconds,
        CancellationToken cancellationToken)
        => Task.FromResult(Array.Empty<StreamPendingMessageInfo>());

    public Task<StreamEntry[]> StreamClaimAsync(
        RedisKey stream,
        RedisValue groupName,
        RedisValue consumerName,
        long minIdleTimeInMilliseconds,
        RedisValue[] messageIds,
        CancellationToken cancellationToken)
        => Task.FromResult(Array.Empty<StreamEntry>());
}
