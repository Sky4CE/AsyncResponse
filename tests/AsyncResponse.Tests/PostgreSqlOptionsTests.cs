using AsyncResponse.Channels.PostgreSQL;
using AsyncResponse.Transports.PostgreSQL;
using Microsoft.Extensions.Options;
using Xunit;

namespace AsyncResponse.Tests;

public sealed class PostgreSqlOptionsTests
{
    [Fact]
    public void ChannelOptions_RejectInvalidSqlIdentifier()
    {
        var options = new PostgreSqlAsyncResponseChannelOptions { MessageTable = "bad-name" };

        var ex = Assert.Throws<InvalidOperationException>(options.Validate);
        Assert.Contains(nameof(PostgreSqlAsyncResponseChannelOptions.MessageTable), ex.Message);
    }

    [Fact]
    public void ChannelOptions_RejectHeartbeatIntervalAtOrAboveTimeout()
    {
        var options = new PostgreSqlAsyncResponseChannelOptions
        {
            SubscriberHeartbeatInterval = TimeSpan.FromSeconds(30),
            SubscriberHeartbeatTimeout = TimeSpan.FromSeconds(30)
        };

        var ex = Assert.Throws<InvalidOperationException>(options.Validate);
        Assert.Contains(nameof(PostgreSqlAsyncResponseChannelOptions.SubscriberHeartbeatInterval), ex.Message);
    }

    [Fact]
    public void TransportOptions_RejectInvalidSqlIdentifier()
    {
        var options = new PostgreSqlAsyncResponseTransportOptions { NotificationChannel = "bad-channel" };

        var ex = Assert.Throws<InvalidOperationException>(() => PostgreSqlTransportOptionsValidator.ValidateCommon(options));
        Assert.Contains(nameof(PostgreSqlAsyncResponseTransportOptions.NotificationChannel), ex.Message);
    }

    [Fact]
    public void TransportOptions_RejectQueueNameCollision()
    {
        var options = new PostgreSqlAsyncResponseTransportOptions { ResponseQueue = "worker" };

        var ex = Assert.Throws<InvalidOperationException>(() => PostgreSqlTransportOptionsValidator.ValidateCommon(options));
        Assert.Contains(nameof(PostgreSqlAsyncResponseTransportOptions.WorkerQueue), ex.Message);
    }

    [Fact]
    public void TransportOptions_RejectNonPositiveDeadLetterRetention()
    {
        var options = new PostgreSqlAsyncResponseTransportOptions { DeadLetterRetention = TimeSpan.FromSeconds(-1) };

        var ex = Assert.Throws<InvalidOperationException>(() => PostgreSqlTransportOptionsValidator.ValidateCommon(options));
        Assert.Contains(nameof(PostgreSqlAsyncResponseTransportOptions.DeadLetterRetention), ex.Message);
    }

    [Fact]
    public void ChannelOptions_RejectPublishBaseDelayAboveMax()
    {
        var options = new PostgreSqlAsyncResponseChannelOptions
        {
            PublishRetryBaseDelay = TimeSpan.FromSeconds(2),
            PublishRetryMaxDelay = TimeSpan.FromSeconds(1)
        };

        var ex = Assert.Throws<InvalidOperationException>(options.Validate);
        Assert.Contains(nameof(PostgreSqlAsyncResponseChannelOptions.PublishRetryBaseDelay), ex.Message);
    }

    [Fact]
    public void ChannelOptions_RejectNegativePruneInterval()
    {
        var options = new PostgreSqlAsyncResponseChannelOptions { PruneInterval = TimeSpan.FromSeconds(-1) };

        var ex = Assert.Throws<InvalidOperationException>(options.Validate);
        Assert.Contains(nameof(PostgreSqlAsyncResponseChannelOptions.PruneInterval), ex.Message);
    }

    [Fact]
    public void ReplyTargetProvider_UsesDefaultResponseQueue()
    {
        var provider = new PostgreSqlReplyTargetProvider(Options.Create(new PostgreSqlAsyncResponseTransportOptions
        {
            ResponseQueue = "responses"
        }));

        var target = provider.GetReplyTarget();

        Assert.Equal(PostgreSqlAsyncResponseTransportOptions.TransportName, target.Transport);
        Assert.Equal("responses", target.Address);
        Assert.Equal("responses", target.Properties["queue"]);
        Assert.Equal("asyncresponse_transport_messages", target.Properties["table"]);
    }

    [Fact]
    public void CorrelationExtractor_ReadsHeaderBeforeJsonBody()
    {
        var options = new PostgreSqlAsyncResponseTransportOptions();
        var headers = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            [options.CorrelationIdHeader] = "from-header"
        };

        var correlationId = PostgreSqlCorrelationIdExtractor.Extract(
            headers,
            """{"CorrelationId":"from-body"}""",
            options);

        Assert.Equal("from-header", correlationId);
    }

    [Fact]
    public void SchemaAdvisoryLockKey_AgreesAcrossChannelAndTransport()
    {
        // Channel and transport must take the SAME advisory lock for a shared schema, otherwise they
        // still race each other on CREATE SCHEMA. The keys are computed independently in each package,
        // so this guards against the two implementations drifting apart.
        foreach (var schema in new[] { "public", "async_response", "Tenant_42" })
        {
            Assert.Equal(
                AsyncResponse.Channels.PostgreSQL.PostgreSqlChannelSql.SchemaAdvisoryLockKey(schema),
                AsyncResponse.Transports.PostgreSQL.PostgreSqlTransportStore.SchemaAdvisoryLockKey(schema));
        }

        // Distinct schemas must map to distinct keys so unrelated deployments don't serialize.
        Assert.NotEqual(
            AsyncResponse.Channels.PostgreSQL.PostgreSqlChannelSql.SchemaAdvisoryLockKey("public"),
            AsyncResponse.Channels.PostgreSQL.PostgreSqlChannelSql.SchemaAdvisoryLockKey("other"));
    }

    [Fact]
    public void CorrelationExtractor_ReadsConfiguredJsonPath()
    {
        var options = new PostgreSqlAsyncResponseTransportOptions();

        var correlationId = PostgreSqlCorrelationIdExtractor.Extract(
            headers: null,
            """{"CustomParameters":{"CorrelationId":"from-json"}}""",
            options);

        Assert.Equal("from-json", correlationId);
    }
}
