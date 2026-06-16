using AsyncResponse.Channels.Redis;
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
}
