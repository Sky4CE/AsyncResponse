using StackExchange.Redis;

namespace AsyncResponse.Channels.Redis;

/// <summary>A live pub/sub subscription; disposing it unsubscribes.</summary>
internal interface IRedisChannelSubscription : IAsyncDisposable;

/// <summary>
/// Async-capable subscribe seam over StackExchange.Redis pub/sub. The channel consumes this instead
/// of <see cref="ISubscriber.Subscribe(RedisChannel, Action{RedisChannel, RedisValue}, CommandFlags)"/>'s
/// synchronous callback — whose handlers the SDK runs on pool threads with no ordering — so
/// messages reach the channel one at a time, in order, off any Redis reader thread. Also the
/// unit-test seam: <see cref="ChannelMessageQueue"/> is sealed with no public constructor, so
/// tests fake this interface rather than the queue.
/// <para>
/// The handler must not wait for downstream capacity: the SDK queue behind this seam is
/// unbounded, so a handler parked on admission does not backpressure the publisher (Redis
/// pub/sub has none), it only lets that queue grow. The channel admits non-blockingly and faults
/// the wait as indeterminate when its bounded buffer is full.
/// </para>
/// </summary>
internal interface IRedisChannelSubscriber
{
    /// <summary>
    /// Subscribes to <paramref name="channel"/>, invoking <paramref name="onMessage"/> for each
    /// message sequentially (a message's task is awaited before the next is delivered).
    /// </summary>
    Task<IRedisChannelSubscription> SubscribeAsync(RedisChannel channel, Func<RedisChannel, RedisValue, Task> onMessage);
}

/// <summary>
/// Production <see cref="IRedisChannelSubscriber"/> over <see cref="ISubscriber"/>: a
/// <see cref="ChannelMessageQueue"/> per subscription, whose <c>OnMessage(Func&lt;…, Task&gt;)</c>
/// loop awaits the handler — preserving per-channel ordering off the reader thread. The queue
/// itself is unbounded (an SDK detail), which is why the channel's handler never waits in it.
/// </summary>
internal sealed class RedisChannelMessageQueueSubscriber(ISubscriber _subscriber) : IRedisChannelSubscriber
{
    /// <summary>Runs the SubscribeAsync operation.</summary>
    public async Task<IRedisChannelSubscription> SubscribeAsync(RedisChannel channel, Func<RedisChannel, RedisValue, Task> onMessage)
    {
        var queue = await _subscriber.SubscribeAsync(channel).ConfigureAwait(false);
        queue.OnMessage(message => onMessage(message.Channel, message.Message));
        return new Subscription(queue);
    }

    private sealed class Subscription(ChannelMessageQueue _queue) : IRedisChannelSubscription
    {
        /// <summary>Releases resources held by this instance.</summary>
        public ValueTask DisposeAsync() => new(_queue.UnsubscribeAsync());
    }
}
