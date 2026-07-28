using StackExchange.Redis;

namespace AsyncResponse.Channels.Redis;

/// <summary>A live pub/sub subscription; disposing it unsubscribes.</summary>
internal interface IRedisChannelSubscription : IAsyncDisposable;

/// <summary>
/// Async-capable subscribe seam over StackExchange.Redis pub/sub. The channel consumes this instead
/// of <see cref="ISubscriber.Subscribe(RedisChannel, Action{RedisChannel, RedisValue}, CommandFlags)"/>'s
/// synchronous callback so message handling can await the per-channel serial executor (bounded
/// backpressure) without sync-over-async blocking a Redis reader thread. Also the unit-test seam:
/// <see cref="ChannelMessageQueue"/> is sealed with no public constructor, so tests fake this
/// interface rather than the queue.
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
/// loop awaits the handler — preserving per-channel ordering while propagating executor
/// backpressure to the queue instead of blocking a reader thread.
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
