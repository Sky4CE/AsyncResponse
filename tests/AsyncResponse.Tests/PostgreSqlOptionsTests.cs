using AsyncResponse.Channels.PostgreSQL;
using AsyncResponse.Transports.PostgreSQL;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using System.Reflection;
using System.Text.Json;
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
    public void ChannelOptions_RejectOverLimitIdentifiersAndAcceptTheCap()
    {
        // PostgreSQL silently truncates identifiers past 63 characters, so validation must reject
        // them; exactly 63 is legal and the derived names reserve their own suffix space.
        AssertChannelInvalid(
            options => options.MessageTable = new string('m', 64),
            "limited to 63");
        AssertChannelInvalid(
            options => options.SchemaName = new string('s', 64),
            "limited to 63");

        new PostgreSqlAsyncResponseChannelOptions { MessageTable = new string('m', 63) }.Validate();
    }

    [Fact]
    public void ChannelOptions_RejectEffectiveNamePlanCollisions()
    {
        // A configured table can occupy a derived name outright...
        AssertChannelInvalid(
            options => options.SubscriberTable = $"{options.MessageTable}_ack_seq",
            "ack sequence");

        // ...a table whose name ends exactly where the reserved stem truncates derives ITSELF
        // (63 = 51-char stem + "_expires_idx")...
        AssertChannelInvalid(
            options => options.SubscriberTable = new string('s', 51) + "_expires_idx",
            "expiry index");

        // ...and two max-length tables sharing a 51-char stem derive the SAME expiry-index name.
        AssertChannelInvalid(
            options =>
            {
                options.MessageTable = new string('m', 63);
                options.SubscriberTable = string.Concat(new string('m', 51), new string('x', 12));
            },
            "both resolve to");
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
        // Shared-base knob: enforced via ValidateShared — a bespoke validator that skips the
        // shared guards accepted TimeSpan.Zero here, defeating the promised disposal bound.
        AssertChannelInvalid(
            options => options.DisposalDrainTimeout = TimeSpan.Zero,
            nameof(PostgreSqlAsyncResponseChannelOptions.DisposalDrainTimeout));
        // Over the ~49.7-day BCL timer ceiling: the runtime rejects the value at timer arming,
        // which used to surface only AFTER waiter-registration side effects.
        AssertChannelInvalid(
            options => options.DefaultTimeout = TimeSpan.FromDays(50),
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
    public void TransportOptions_RejectOverLimitIdentifiersAndDerivedIndexCollisions()
    {
        AssertTransportInvalid(
            options => options.MessageTable = new string('q', 64),
            "limited to 63");

        // A queue table whose name ends exactly where the reserved "_claim_idx" stem truncates
        // derives its own name for the claim index (63 = 53-char stem + "_claim_idx").
        AssertTransportInvalid(
            options => options.MessageTable = new string('q', 53) + "_claim_idx",
            "claim index");

        // Exactly at the cap the derived index names stay distinct and the plan validates.
        PostgreSqlTransportOptionsValidator.ValidateCommon(
            new PostgreSqlAsyncResponseTransportOptions { MessageTable = new string('q', 63) });
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
            new PostgreSqlSubscriberOptions { AckMode = PostgreSqlAckMode.AckAfterEnqueue },
            "Worker"));
        Assert.Throws<InvalidOperationException>(() => PostgreSqlTransportOptionsValidator.ValidateSubscriber(
            new PostgreSqlSubscriberOptions
            {
                AckMode = PostgreSqlAckMode.AckAfterEnqueue,
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

        var subscriber = new PostgreSqlSubscriberOptions().UseAckAfterEnqueue(2, 8, TimeSpan.FromSeconds(3));
        PostgreSqlTransportOptionsValidator.ValidateSubscriber(subscriber, "Worker");
        Assert.Equal(PostgreSqlAckMode.AckAfterEnqueue, subscriber.AckMode);
        Assert.Equal(2, subscriber.BackgroundWorkerCount);
        Assert.Equal(8, subscriber.BackgroundQueueCapacity);
        Assert.Equal(TimeSpan.FromSeconds(3), subscriber.BackgroundDrainTimeout);
    }

    [Fact]
    public void UseAckAfterEnqueue_RejectsInvalidArguments()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new PostgreSqlSubscriberOptions().UseAckAfterEnqueue(0, 8));
        Assert.Throws<ArgumentOutOfRangeException>(() => new PostgreSqlSubscriberOptions().UseAckAfterEnqueue(2, 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => new PostgreSqlSubscriberOptions().UseAckAfterEnqueue(2, 8, TimeSpan.Zero));
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
    public void CorrelationExtractor_Throws_WhenTouchedObjectHasExactDuplicateKey()
        // The shared JSON-path walker materializes nothing, but still reproduces this runtime's
        // JsonObject-throws-on-exact-duplicate-key behavior rather than silently resolving to one
        // of the duplicates.
        => Assert.Throws<ArgumentException>(() => PostgreSqlCorrelationIdExtractor.Extract(
            headers: null,
            """{"CorrelationId":"1","CorrelationId":"2"}""",
            new PostgreSqlAsyncResponseTransportOptions()));

    [Fact]
    public void PostgreSqlRetry_ClassifiesTransientExceptions()
    {
        var transientDriverFailure = new NpgsqlException("network", new TimeoutException());
        Assert.True(PostgreSqlTransportRetry.IsTransient(new TimeoutException()));
        Assert.True(PostgreSqlChannelSql.IsTransient(new TimeoutException()));
        Assert.True(PostgreSqlTransportRetry.IsTransient(transientDriverFailure));
        Assert.True(PostgreSqlChannelSql.IsTransient(transientDriverFailure));
        Assert.False(PostgreSqlTransportRetry.IsTransient(new OperationCanceledException()));
        Assert.False(PostgreSqlChannelSql.IsTransient(new OperationCanceledException()));
        Assert.False(PostgreSqlTransportRetry.IsTransient(new InvalidOperationException()));
        Assert.False(PostgreSqlChannelSql.IsTransient(new InvalidOperationException()));
    }

    [Fact]
    public void ChannelSql_HelperBoundaries_HandleIdentifierIndexAndNotifyPayload()
    {
        Assert.True(InvokeChannelSqlStatic<bool>("IsIdentifier", "valid_1"));
        Assert.False(InvokeChannelSqlStatic<bool>("IsIdentifier", ""));
        Assert.False(InvokeChannelSqlStatic<bool>("IsIdentifier", "1bad"));
        Assert.False(InvokeChannelSqlStatic<bool>("IsIdentifier", "bad-name"));

        var indexName = InvokeChannelSqlStatic<string>("IndexName", new string('a', 70), "expires");
        Assert.Equal(63, indexName.Length);

        Assert.Equal("short", InvokeChannelSqlStatic<string>("NotifyPayload", "short"));
        Assert.Equal(string.Empty, InvokeChannelSqlStatic<string>("NotifyPayload", new string('x', 7001)));
    }

    [Fact]
    public void ChannelSql_ShouldPrune_ThrottlesByConfiguredInterval()
    {
        using var dataSource = CreatePostgreSqlDataSource();
        var options = new PostgreSqlAsyncResponseChannelOptions
        {
            PruneInterval = TimeSpan.FromMinutes(10)
        };
        var sql = new PostgreSqlChannelSql(dataSource, Options.Create(options));
        var lastTicks = 0L;

        Assert.True(InvokeShouldPrune(sql, ref lastTicks));
        Assert.NotEqual(0L, lastTicks);
        Assert.False(InvokeShouldPrune(sql, ref lastTicks));

        options.PruneInterval = TimeSpan.Zero;
        Assert.True(InvokeShouldPrune(sql, ref lastTicks));
    }

    [Fact]
    public async Task RecoveryStateStore_SaveAsync_RejectsNonPositiveTtlBeforeSql()
    {
        using var dataSource = CreatePostgreSqlDataSource();
        var store = CreateRecoveryStateStore(dataSource);

        var ex = await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            store.SaveAsync("corr", new RecoveryState(), TimeSpan.Zero));

        Assert.Equal("ttl", ex.ParamName);
        await Assert.ThrowsAsync<ArgumentException>(() => store.TryDeleteAsync("corr", Guid.Empty));
        await Assert.ThrowsAsync<ArgumentException>(() => store.SaveAsync(
            "corr",
            new RecoveryState
            {
                CorrelationId = "corr",
                SchemaVersion = RecoveryStateSchema.Current + 1
            },
            TimeSpan.FromSeconds(1)));
    }

    [Fact]
    public void RecoveryStateStore_DeserializeState_RejectsUnreadableOrMismatchedRecords()
    {
        using var dataSource = CreatePostgreSqlDataSource();
        var store = CreateRecoveryStateStore(dataSource);

        Assert.Null(InvokeDeserializeState(store, "null", "fallback"));
        Assert.Null(InvokeDeserializeState(store, "{not-json", "fallback"));
        Assert.Null(InvokeDeserializeState(
            store,
            JsonSerializer.Serialize(new RecoveryState
            {
                SchemaVersion = RecoveryStateSchema.Current + 1,
                CorrelationId = "future"
            }),
            "fallback"));

        Assert.Null(InvokeDeserializeState(
            store,
            JsonSerializer.Serialize(new RecoveryState { RegistrationId = Guid.NewGuid() }),
            "fallback"));
        Assert.Null(InvokeDeserializeState(
            store,
            JsonSerializer.Serialize(new RecoveryState
            {
                RegistrationId = Guid.NewGuid(),
                CorrelationId = "other"
            }),
            "fallback"));

        var state = InvokeDeserializeState(
            store,
            JsonSerializer.Serialize(new RecoveryState
            {
                RegistrationId = Guid.NewGuid(),
                CorrelationId = "fallback"
            }),
            "fallback");
        Assert.NotNull(state);
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

    [Fact]
    public void ChannelOptions_RejectIntervalsBeyondTheirCeilings()
    {
        // "Passes validation, throws mid-operation" is the failure mode these bounds close: a
        // TimeSpan.MaxValue deadline overflowed AFTER the publisher's insert (reporting failure
        // for a possibly delivered response), and an over-timer-ceiling poll/heartbeat interval
        // threw inside its background loop's own retry delay, killing dispatch.
        AssertChannelInvalid(
            options => options.DeliveryConfirmationTimeout = TimeSpan.MaxValue,
            nameof(PostgreSqlAsyncResponseChannelOptions.DeliveryConfirmationTimeout));
        AssertChannelInvalid(
            options => options.MessageRetention = TimeSpan.MaxValue,
            nameof(PostgreSqlAsyncResponseChannelOptions.MessageRetention));
        AssertChannelInvalid(
            options => options.SubscriberHeartbeatTimeout = TimeSpan.MaxValue,
            nameof(PostgreSqlAsyncResponseChannelOptions.SubscriberHeartbeatTimeout));
        AssertChannelInvalid(
            options => options.ListenerPollInterval = TimeSpan.FromDays(60),
            nameof(PostgreSqlAsyncResponseChannelOptions.ListenerPollInterval));
        AssertChannelInvalid(
            options => options.DeliveryConfirmationPollInterval = TimeSpan.FromDays(60),
            nameof(PostgreSqlAsyncResponseChannelOptions.DeliveryConfirmationPollInterval));
        AssertChannelInvalid(
            options => options.PublishRetryMaxDelay = TimeSpan.FromDays(60),
            nameof(PostgreSqlAsyncResponseChannelOptions.PublishRetryMaxDelay));
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

    private static NpgsqlDataSource CreatePostgreSqlDataSource()
        => NpgsqlDataSource.Create("Host=localhost;Username=postgres;Password=postgres;Database=postgres");

    private static PostgreSqlRecoveryStateStore CreateRecoveryStateStore(NpgsqlDataSource dataSource)
    {
        var sql = new PostgreSqlChannelSql(dataSource, Options.Create(new PostgreSqlAsyncResponseChannelOptions()));
        return new PostgreSqlRecoveryStateStore(sql, NullLogger<PostgreSqlRecoveryStateStore>.Instance);
    }

    private static T InvokeChannelSqlStatic<T>(string name, params object?[] args)
        => (T)typeof(PostgreSqlChannelSql)
            .GetMethod(name, BindingFlags.NonPublic | BindingFlags.Static)!
            .Invoke(null, args)!;

    private static bool InvokeShouldPrune(PostgreSqlChannelSql sql, ref long lastTicks)
    {
        object?[] args = [lastTicks];
        var result = (bool)typeof(PostgreSqlChannelSql)
            .GetMethod("ShouldPrune", BindingFlags.NonPublic | BindingFlags.Instance)!
            .Invoke(sql, args)!;
        lastTicks = (long)args[0]!;
        return result;
    }

    private static RecoveryState? InvokeDeserializeState(
        PostgreSqlRecoveryStateStore store,
        string json,
        string? correlationId)
        => (RecoveryState?)typeof(PostgreSqlRecoveryStateStore)
            .GetMethod("DeserializeState", BindingFlags.NonPublic | BindingFlags.Instance)!
            .Invoke(store, [json, correlationId]);
}
