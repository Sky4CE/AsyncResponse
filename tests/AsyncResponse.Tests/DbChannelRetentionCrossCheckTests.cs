using AsyncResponse.Channels.MongoDB;
using AsyncResponse.Channels.PostgreSQL;
using AsyncResponse.Channels.SqlServer;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Regression (r24): MessageRetention and DeliveryConfirmationTimeout were validated only
/// independently. A retention at or below the confirmation window let the publish-path pruner
/// delete the message row INSIDE the confirmation wait — and a pruned row is indistinguishable
/// from an acknowledged one in TryClaimForRecoveryAsync's <c>acked_at IS NULL</c> predicate, so
/// the response was reported delivered and lost-response recovery silently skipped, leaving the
/// waiter's registration armed until its TTL. All three DB channels now cross-check the pair.
/// </summary>
public sealed class DbChannelRetentionCrossCheckTests
{
    [Fact]
    public void PostgreSql_RetentionMustExceedTheConfirmationWindow()
    {
        var options = new PostgreSqlAsyncResponseChannelOptions
        {
            MessageRetention = TimeSpan.FromSeconds(2),
            DeliveryConfirmationTimeout = TimeSpan.FromSeconds(5)
        };

        var exception = Assert.Throws<InvalidOperationException>(options.Validate);
        Assert.Contains(nameof(options.MessageRetention), exception.Message);
        Assert.Contains(nameof(options.DeliveryConfirmationTimeout), exception.Message);
    }

    [Fact]
    public void SqlServer_RetentionMustExceedTheConfirmationWindow()
    {
        var options = new SqlServerAsyncResponseChannelOptions
        {
            ConnectionString = "Server=localhost;Database=unused;Integrated Security=true",
            MessageRetention = TimeSpan.FromSeconds(5),
            DeliveryConfirmationTimeout = TimeSpan.FromSeconds(5)
        };

        var exception = Assert.Throws<InvalidOperationException>(options.Validate);
        Assert.Contains(nameof(options.MessageRetention), exception.Message);
    }

    [Fact]
    public void MongoDb_RetentionMustExceedTheConfirmationWindow()
    {
        var options = new MongoDbAsyncResponseChannelOptions
        {
            MessageRetention = TimeSpan.FromSeconds(2),
            DeliveryConfirmationTimeout = TimeSpan.FromSeconds(5)
        };

        var exception = Assert.Throws<InvalidOperationException>(options.Validate);
        Assert.Contains(nameof(options.MessageRetention), exception.Message);
    }

    [Fact]
    public void Defaults_SatisfyTheCrossCheck()
    {
        // 1 h retention vs 5 s confirmation: the shipped defaults stay valid.
        new PostgreSqlAsyncResponseChannelOptions().Validate();
        new SqlServerAsyncResponseChannelOptions
        {
            ConnectionString = "Server=localhost;Database=unused;Integrated Security=true"
        }.Validate();
        new MongoDbAsyncResponseChannelOptions().Validate();
    }
}
