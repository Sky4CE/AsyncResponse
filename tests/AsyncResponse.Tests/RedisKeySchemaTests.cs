using AsyncResponse.Channels.Redis;
using StackExchange.Redis;
using System.Reflection;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// The Redis key/channel schema is a storage contract — changing its shape orphans in-flight
/// recovery state — so the prefixing and the recovery-key ↔ correlation-id round-trip are pinned.
/// </summary>
public class RedisKeySchemaTests
{
    [Fact]
    public void RecoveryKey_AndPattern_UsePrefix()
    {
        var schema = new RedisKeySchema("ar");

        Assert.Equal("ar:recovery:c1", schema.RecoveryKey("c1"));
        Assert.Equal("ar:recovery:*", schema.RecoveryKeyPattern);
    }

    [Fact]
    public void Channel_UsesResponsePrefix()
    {
        var schema = new RedisKeySchema("ar");

        Assert.Equal("ar:response:c1", schema.Channel("c1").ToString());
    }

    [Fact]
    public void CorrelationId_RoundTripsThroughRecoveryKey()
    {
        var schema = new RedisKeySchema("ar");

        var key = schema.RecoveryKey("some-correlation-id");

        Assert.Equal("some-correlation-id", schema.CorrelationIdFromRecoveryKey(key));
    }

    [Fact]
    public void Prefix_IsHonored()
    {
        var schema = new RedisKeySchema("tenant-x");

        Assert.Equal("tenant-x:recovery:c1", schema.RecoveryKey("c1"));
        Assert.Equal("tenant-x:response:c1", schema.Channel("c1").ToString());
    }

    /// <summary>
    /// Regression (round 33): the response channel was a plain literal channel. On Redis Cluster a
    /// plain channel lets each client pick its own node, and PUBLISH's integer reply counts only the
    /// subscribers on the node that received it — so a waiter subscribed elsewhere received the
    /// message while the publisher read 0 and either re-published a duplicate into the waiter's
    /// predicate (RetryLive) or fired lost-subscriber recovery for a delivered response. The channel
    /// must be key-routed so SUBSCRIBE and PUBLISH land on the slot owner and the count means what
    /// the publisher reads it as. Pre-fix: <c>IsKeyRouted</c> was false.
    /// </summary>
    [Fact]
    public void Channel_IsKeyRouted_SoAClusterPublishCountsTheWaitersNode()
    {
        var schema = new RedisKeySchema("ar");

        var channel = schema.Channel("c1");

        Assert.True(
            IsKeyRouted(channel),
            "the response channel is not key-routed: on Redis Cluster PUBLISH would count only the receiving node's subscribers");
        // The flag is exactly what WithKeyRouting sets, and nothing a plain literal channel carries.
        Assert.True(IsKeyRouted(new RedisChannel("ar:response:c1", RedisChannel.PatternMode.Literal).WithKeyRouting()));
        Assert.False(IsKeyRouted(new RedisChannel("ar:response:c1", RedisChannel.PatternMode.Literal)));
    }

    /// <summary>
    /// Key routing changes only where a cluster routes the channel, never the channel NAME — the
    /// wire contract every other process subscribes and publishes by — nor its literal mode.
    /// </summary>
    [Fact]
    public void Channel_KeyRouting_LeavesTheChannelNameAndLiteralModeUnchanged()
    {
        var channel = new RedisKeySchema("ar").Channel("c1");

        Assert.Equal("ar:response:c1", channel.ToString());
        Assert.False(channel.IsPattern);
        Assert.False(channel.IsSharded);
    }

    /// <summary>
    /// StackExchange.Redis keeps the routing flag internal (<c>RedisChannel.IsKeyRouted</c>) and
    /// its equality ignores it — a plain and a key-routed channel compare EQUAL — so the flag is
    /// read by reflection; an <c>Assert.Equal</c> against <c>WithKeyRouting()</c> could never fail.
    /// </summary>
    private static bool IsKeyRouted(RedisChannel channel)
    {
        var property = typeof(RedisChannel).GetProperty("IsKeyRouted", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)
            ?? throw new MissingMemberException(typeof(RedisChannel).FullName, "IsKeyRouted");
        return (bool)property.GetValue(channel)!;
    }
}
