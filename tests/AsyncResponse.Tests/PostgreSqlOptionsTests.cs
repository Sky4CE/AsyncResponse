using AsyncResponse.Channels.PostgreSQL;
using AsyncResponse.Transports.PostgreSQL;
using Microsoft.Extensions.Options;
using Xunit;

namespace AsyncResponse.Tests;

public sealed class PostgreSqlOptionsTests
{
    [Fact]
    public void ChannelOptions_Validate_PassesForDefaults()
        => new PostgreSqlAsyncResponseChannelOptions().Validate();

    [Fact]
    public void ChannelOptions_RejectInvalidSqlIdentifier()
    {
        var options = new PostgreSqlAsyncResponseChannelOptions { MessageTable = "bad-name" };

        var ex = Assert.Throws<InvalidOperationException>(options.Validate);
        Assert.Contains(nameof(PostgreSqlAsyncResponseChannelOptions.MessageTable), ex.Message);

        AssertChannelInvalid(
            options => options.SchemaName = " ",
            nameof(PostgreSqlAsyncResponseChannelOptions.SchemaName));
        AssertChannelInvalid(
            options => options.SchemaName = "1bad",
            nameof(PostgreSqlAsyncResponseChannelOptions.SchemaName));
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
    public void ChannelOptions_RejectNonPositiveRetentionAndConfirmationSettings()
    {
        AssertChannelInvalid(
            options => options.MessageRetention = TimeSpan.Zero,
            nameof(PostgreSqlAsyncResponseChannelOptions.MessageRetention));
        AssertChannelInvalid(
            options => options.DeliveryConfirmationTimeout = TimeSpan.Zero,
            nameof(PostgreSqlAsyncResponseChannelOptions.DeliveryConfirmationTimeout));
        AssertChannelInvalid(
            options => options.DeliveryConfirmationPollInterval = TimeSpan.Zero,
            nameof(PostgreSqlAsyncResponseChannelOptions.DeliveryConfirmationPollInterval));
        AssertChannelInvalid(
            options => options.ListenerPollInterval = TimeSpan.Zero,
            nameof(PostgreSqlAsyncResponseChannelOptions.ListenerPollInterval));
    }

    [Fact]
    public void ChannelOptions_RejectInvalidWaiterAndEnvelopeSettings()
    {
        AssertChannelInvalid(
            options => options.DefaultTimeout = TimeSpan.Zero,
            nameof(PostgreSqlAsyncResponseChannelOptions.DefaultTimeout));
        AssertChannelInvalid(
            options => options.MaxRemoteStackTraceLength = -1,
            nameof(PostgreSqlAsyncResponseChannelOptions.MaxRemoteStackTraceLength));
        AssertChannelInvalid(
            options => options.PendingMessageBatchSize = 0,
            nameof(PostgreSqlAsyncResponseChannelOptions.PendingMessageBatchSize));
        AssertChannelInvalid(
            options => options.PublishMaxAttempts = 0,
            nameof(PostgreSqlAsyncResponseChannelOptions.PublishMaxAttempts));
    }

    [Fact]
    public void TransportOptions_ValidateCommon_PassesForDefaults()
        => PostgreSqlTransportOptionsValidator.ValidateCommon(new PostgreSqlAsyncResponseTransportOptions());

    [Fact]
    public void TransportOptions_RejectInvalidSqlIdentifier()
    {
        var options = new PostgreSqlAsyncResponseTransportOptions { NotificationChannel = "bad-channel" };

        var ex = Assert.Throws<InvalidOperationException>(() => PostgreSqlTransportOptionsValidator.ValidateCommon(options));
        Assert.Contains(nameof(PostgreSqlAsyncResponseTransportOptions.NotificationChannel), ex.Message);

        AssertTransportInvalid(
            options => options.SchemaName = " ",
            nameof(PostgreSqlAsyncResponseTransportOptions.SchemaName));
        AssertTransportInvalid(
            options => options.SchemaName = "1bad",
            nameof(PostgreSqlAsyncResponseTransportOptions.SchemaName));
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
    public void TransportOptions_RejectNonPositiveAndMisorderedRetrySettings()
    {
        AssertTransportInvalid(
            options => options.LockTimeout = TimeSpan.Zero,
            nameof(PostgreSqlAsyncResponseTransportOptions.LockTimeout));
        AssertTransportInvalid(
            options => options.PublishMaxAttempts = 0,
            nameof(PostgreSqlAsyncResponseTransportOptions.PublishMaxAttempts));
        AssertTransportInvalid(
            options => options.PublishRetryBaseDelay = TimeSpan.FromSeconds(2),
            nameof(PostgreSqlAsyncResponseTransportOptions.PublishRetryBaseDelay));
        AssertTransportInvalid(
            options => options.SubscriberRetryBaseDelay = TimeSpan.FromSeconds(6),
            nameof(PostgreSqlAsyncResponseTransportOptions.SubscriberRetryBaseDelay));
        AssertTransportInvalid(
            options => options.CorrelationIdHeader = " ",
            nameof(PostgreSqlAsyncResponseTransportOptions.CorrelationIdHeader));
    }

    [Fact]
    public void TransportSubscriberOptions_ValidateEarlyAckAndFailureSettings()
    {
        Assert.Throws<InvalidOperationException>(() => PostgreSqlTransportOptionsValidator.ValidateSubscriber(
            new PostgreSqlSubscriberOptions { AckMode = PostgreSqlAckMode.AckAfterReceive },
            "Worker"));
        Assert.Throws<InvalidOperationException>(() => PostgreSqlTransportOptionsValidator.ValidateSubscriber(
            new PostgreSqlSubscriberOptions
            {
                AckMode = PostgreSqlAckMode.AckAfterReceive,
                BackgroundWorkerCount = 1,
                BackgroundQueueCapacity = 0
            },
            "Worker"));
        Assert.Throws<InvalidOperationException>(() => PostgreSqlTransportOptionsValidator.ValidateSubscriber(
            new PostgreSqlSubscriberOptions { BatchSize = 0 },
            "Worker"));
        Assert.Throws<InvalidOperationException>(() => PostgreSqlTransportOptionsValidator.ValidateSubscriber(
            new PostgreSqlSubscriberOptions { MaxDeliveryAttempts = -1 },
            "Worker"));
        Assert.Throws<InvalidOperationException>(() => PostgreSqlTransportOptionsValidator.ValidateSubscriber(
            new PostgreSqlSubscriberOptions { RedeliveryDelay = TimeSpan.Zero },
            "Worker"));
        Assert.Throws<InvalidOperationException>(() => PostgreSqlTransportOptionsValidator.ValidateSubscriber(
            new PostgreSqlSubscriberOptions { AckMode = (PostgreSqlAckMode)999 },
            "Worker"));

        var subscriber = new PostgreSqlSubscriberOptions().UseAckAfterReceive(2, 8, TimeSpan.FromSeconds(3));
        PostgreSqlTransportOptionsValidator.ValidateSubscriber(subscriber, "Worker");
        Assert.Equal(PostgreSqlAckMode.AckAfterReceive, subscriber.AckMode);
        Assert.Equal(2, subscriber.BackgroundWorkerCount);
        Assert.Equal(8, subscriber.BackgroundQueueCapacity);
        Assert.Equal(TimeSpan.FromSeconds(3), subscriber.BackgroundDrainTimeout);
    }

    [Fact]
    public void UseAckAfterReceive_RejectsInvalidArguments()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new PostgreSqlSubscriberOptions().UseAckAfterReceive(0, 8));
        Assert.Throws<ArgumentOutOfRangeException>(() => new PostgreSqlSubscriberOptions().UseAckAfterReceive(2, 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => new PostgreSqlSubscriberOptions().UseAckAfterReceive(2, 8, TimeSpan.Zero));
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
    public void ReplyTargetProvider_ResolvesNamedTargetAndCopiesProperties()
    {
        var options = new PostgreSqlAsyncResponseTransportOptions { SchemaName = "orders" };
        options.AddReplyTarget("regional", "regional_responses");
        options.ReplyTargets["regional"].Properties["tenant"] = "acme";
        var provider = new PostgreSqlReplyTargetProvider(Options.Create(options));

        var target = provider.GetReplyTarget("regional");

        Assert.Equal("regional", target.Name);
        Assert.Equal("regional_responses", target.Address);
        Assert.Equal("regional_responses", target.Properties["queue"]);
        Assert.Equal("orders", target.Properties["schema"]);
        Assert.Equal("acme", target.Properties["tenant"]);
    }

    [Fact]
    public void ReplyTargetProvider_UnknownName_Throws()
    {
        var provider = new PostgreSqlReplyTargetProvider(Options.Create(new PostgreSqlAsyncResponseTransportOptions()));

        var ex = Assert.Throws<InvalidOperationException>(() => provider.GetReplyTarget("missing"));

        Assert.Contains("missing", ex.Message, StringComparison.Ordinal);
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
    public void CorrelationExtractor_ReadsNestedJsonStringAndIsCaseInsensitive()
    {
        var options = new PostgreSqlAsyncResponseTransportOptions
        {
            CorrelationIdJsonPaths = ["CustomParameters.CorrelationId"]
        };

        var correlationId = PostgreSqlCorrelationIdExtractor.Extract(
            headers: null,
            """{"customparameters":"{\"correlationid\":\"from-nested-json-string\"}"}""",
            options);

        Assert.Equal("from-nested-json-string", correlationId);
    }

    [Fact]
    public void CorrelationExtractor_ReturnsNullForInvalidJsonBlankPathsOrBlankMessage()
    {
        var options = new PostgreSqlAsyncResponseTransportOptions();

        Assert.Null(PostgreSqlCorrelationIdExtractor.Extract(null, "{not-json", options));
        Assert.Null(PostgreSqlCorrelationIdExtractor.Extract(null, "", options));
        Assert.Null(PostgreSqlCorrelationIdExtractor.Extract(null, "null", options));

        options.CorrelationIdJsonPaths = [];
        Assert.Null(PostgreSqlCorrelationIdExtractor.Extract(null, """{"CorrelationId":"ignored"}""", options));
    }

    [Fact]
    public void CorrelationExtractor_HandlesUnmatchedBlankPrimitiveAndMalformedNestedPaths()
    {
        var options = new PostgreSqlAsyncResponseTransportOptions
        {
            CorrelationIdJsonPaths =
            [
                "",
                "Missing.Value",
                "CustomParameters.CorrelationId",
                "CorrelationId"
            ]
        };

        Assert.Null(PostgreSqlCorrelationIdExtractor.Extract(
            null,
            """{"CustomParameters":42,"Other":"x"}""",
            options));

        Assert.Null(PostgreSqlCorrelationIdExtractor.Extract(
            null,
            """{"CustomParameters":"{not-json"}""",
            options));

        Assert.Equal("42", PostgreSqlCorrelationIdExtractor.Extract(
            null,
            """{"CorrelationId":42}""",
            options));
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

    [Fact]
    public void PostgreSqlRetry_ClassifiesTransientExceptions()
    {
        Assert.True(PostgreSqlTransportRetry.IsTransient(new TimeoutException()));
        Assert.True(PostgreSqlChannelSql.IsTransient(new TimeoutException()));
        Assert.False(PostgreSqlTransportRetry.IsTransient(new OperationCanceledException()));
        Assert.False(PostgreSqlChannelSql.IsTransient(new OperationCanceledException()));
        Assert.False(PostgreSqlTransportRetry.IsTransient(new InvalidOperationException()));
        Assert.False(PostgreSqlChannelSql.IsTransient(new InvalidOperationException()));
    }

    [Fact]
    public async Task PostgreSqlRetry_RetriesTransientTimeouts()
    {
        var attempts = 0;

        var result = await PostgreSqlTransportRetry.ExecuteAsync(
            _ =>
            {
                attempts++;
                return attempts < 3
                    ? throw new TimeoutException("try again")
                    : Task.FromResult("ok");
            },
            maxAttempts: 3,
            baseDelay: TimeSpan.FromMilliseconds(1),
            maxDelay: TimeSpan.FromMilliseconds(1),
            CancellationToken.None);

        Assert.Equal("ok", result);
        Assert.Equal(3, attempts);
    }

    [Fact]
    public async Task PostgreSqlRetry_DoesNotRetryCancellation()
    {
        var attempts = 0;

        Task<int> Action(CancellationToken _)
        {
            attempts++;
            throw new OperationCanceledException();
        }

        await Assert.ThrowsAsync<OperationCanceledException>(() => PostgreSqlTransportRetry.ExecuteAsync(
            Action,
            maxAttempts: 3,
            baseDelay: TimeSpan.FromMilliseconds(1),
            maxDelay: TimeSpan.FromMilliseconds(1),
            CancellationToken.None));

        Assert.Equal(1, attempts);
    }

    private static void AssertChannelInvalid(
        Action<PostgreSqlAsyncResponseChannelOptions> configure,
        string expectedMessageFragment)
    {
        var options = new PostgreSqlAsyncResponseChannelOptions();
        configure(options);

        var ex = Assert.Throws<InvalidOperationException>(options.Validate);

        Assert.Contains(expectedMessageFragment, ex.Message, StringComparison.Ordinal);
    }

    private static void AssertTransportInvalid(
        Action<PostgreSqlAsyncResponseTransportOptions> configure,
        string expectedMessageFragment)
    {
        var options = new PostgreSqlAsyncResponseTransportOptions();
        configure(options);

        var ex = Assert.Throws<InvalidOperationException>(() => PostgreSqlTransportOptionsValidator.ValidateCommon(options));

        Assert.Contains(expectedMessageFragment, ex.Message, StringComparison.Ordinal);
    }
}
