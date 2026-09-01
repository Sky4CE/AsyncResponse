using AsyncResponse.Channels.SqlServer;
using AsyncResponse.Transports.SqlServer;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Data.SqlClient;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text.Json;
using Xunit;

namespace AsyncResponse.Tests;

public sealed class SqlServerOptionsTests
{
    private const string TestConnectionString =
        "Server=localhost;Database=asyncresponse_tests;User ID=sa;Password=unused;TrustServerCertificate=True";

    [Fact]
    public void ChannelOptions_Validate_PassesForDefaultsWithConnectionString()
        => ChannelOptions().Validate();

    [Fact]
    public void ChannelOptions_RejectMissingConnectionString()
    {
        var ex = Assert.Throws<InvalidOperationException>(new SqlServerAsyncResponseChannelOptions().Validate);
        Assert.Contains(nameof(SqlServerAsyncResponseChannelOptions.ConnectionString), ex.Message);
    }

    [Fact]
    public void ChannelOptions_RejectInvalidSqlIdentifier()
    {
        var options = ChannelOptions();
        options.MessageTable = "bad-name";

        var ex = Assert.Throws<InvalidOperationException>(options.Validate);
        Assert.Contains(nameof(SqlServerAsyncResponseChannelOptions.MessageTable), ex.Message);

        AssertChannelInvalid(
            options => options.SchemaName = " ",
            nameof(SqlServerAsyncResponseChannelOptions.SchemaName));
        AssertChannelInvalid(
            options => options.SchemaName = "1bad",
            nameof(SqlServerAsyncResponseChannelOptions.SchemaName));
    }

    [Fact]
    public void ChannelOptions_RejectOverLimitIdentifiersAndNamePlanCollisions()
    {
        // sysname caps identifiers at 128; exactly 128 is legal and the derived sequence and
        // index names reserve their own suffix space.
        AssertChannelInvalid(
            options => options.MessageTable = new string('m', 129),
            "limited to 128");

        // A configured table must not occupy the derived ack-sequence name — they share the
        // schema-object namespace and CREATE SEQUENCE would fail (or silently be skipped).
        AssertChannelInvalid(
            options => options.SubscriberTable = $"{options.MessageTable}_ack_seq",
            "ack sequence");

        var atCap = ChannelOptions();
        atCap.MessageTable = new string('m', 128);
        atCap.Validate();
    }

    [Fact]
    public void ChannelOptions_RejectHeartbeatIntervalAtOrAboveTimeout()
    {
        var options = ChannelOptions();
        options.SubscriberHeartbeatInterval = TimeSpan.FromSeconds(30);
        options.SubscriberHeartbeatTimeout = TimeSpan.FromSeconds(30);

        var ex = Assert.Throws<InvalidOperationException>(options.Validate);
        Assert.Contains(nameof(SqlServerAsyncResponseChannelOptions.SubscriberHeartbeatInterval), ex.Message);
    }

    [Fact]
    public void ChannelOptions_RejectNonPositiveRetentionAndConfirmationSettings()
    {
        AssertChannelInvalid(
            options => options.MessageRetention = TimeSpan.Zero,
            nameof(SqlServerAsyncResponseChannelOptions.MessageRetention));
        AssertChannelInvalid(
            options => options.DeliveryConfirmationTimeout = TimeSpan.Zero,
            nameof(SqlServerAsyncResponseChannelOptions.DeliveryConfirmationTimeout));
        AssertChannelInvalid(
            options => options.DeliveryConfirmationPollInterval = TimeSpan.Zero,
            nameof(SqlServerAsyncResponseChannelOptions.DeliveryConfirmationPollInterval));
        AssertChannelInvalid(
            options => options.ActivePollInterval = TimeSpan.Zero,
            nameof(SqlServerAsyncResponseChannelOptions.ActivePollInterval));
        AssertChannelInvalid(
            options => options.IdlePollInterval = TimeSpan.Zero,
            nameof(SqlServerAsyncResponseChannelOptions.IdlePollInterval));
    }

    [Fact]
    public void ChannelOptions_RejectActivePollIntervalAboveIdlePollInterval()
    {
        // The adaptive polling contract: the idle interval is the backed-off sweep, so the active
        // interval can never legitimately exceed it.
        var options = ChannelOptions();
        options.ActivePollInterval = TimeSpan.FromSeconds(5);
        options.IdlePollInterval = TimeSpan.FromSeconds(1);

        var ex = Assert.Throws<InvalidOperationException>(options.Validate);
        Assert.Contains(nameof(SqlServerAsyncResponseChannelOptions.ActivePollInterval), ex.Message);
    }

    [Fact]
    public void ChannelOptions_RejectInvalidWaiterAndEnvelopeSettings()
    {
        AssertChannelInvalid(
            options => options.DefaultTimeout = TimeSpan.Zero,
            nameof(SqlServerAsyncResponseChannelOptions.DefaultTimeout));
        // Shared-base knob: enforced via ValidateShared — a bespoke validator that skips the
        // shared guards accepted TimeSpan.Zero here, defeating the promised disposal bound.
        AssertChannelInvalid(
            options => options.DisposalDrainTimeout = TimeSpan.Zero,
            nameof(SqlServerAsyncResponseChannelOptions.DisposalDrainTimeout));
        AssertChannelInvalid(
            options => options.MaxRemoteStackTraceLength = -1,
            nameof(SqlServerAsyncResponseChannelOptions.MaxRemoteStackTraceLength));
        AssertChannelInvalid(
            options => options.PendingMessageBatchSize = 0,
            nameof(SqlServerAsyncResponseChannelOptions.PendingMessageBatchSize));
        AssertChannelInvalid(
            options => options.PublishMaxAttempts = 0,
            nameof(SqlServerAsyncResponseChannelOptions.PublishMaxAttempts));
    }

    [Fact]
    public void TransportOptions_ValidateCommon_PassesForDefaultsWithConnectionString()
        => SqlServerTransportOptionsValidator.ValidateCommon(TransportOptions());

    [Fact]
    public void TransportOptions_RejectMissingConnectionString()
    {
        var ex = Assert.Throws<InvalidOperationException>(
            () => SqlServerTransportOptionsValidator.ValidateCommon(new SqlServerAsyncResponseTransportOptions()));
        Assert.Contains(nameof(SqlServerAsyncResponseTransportOptions.ConnectionString), ex.Message);
    }

    [Fact]
    public void TransportOptions_RejectInvalidSqlIdentifier()
    {
        var options = TransportOptions();
        options.MessageTable = "bad-table";

        var ex = Assert.Throws<InvalidOperationException>(() => SqlServerTransportOptionsValidator.ValidateCommon(options));
        Assert.Contains(nameof(SqlServerAsyncResponseTransportOptions.MessageTable), ex.Message);

        AssertTransportInvalid(
            options => options.SchemaName = " ",
            nameof(SqlServerAsyncResponseTransportOptions.SchemaName));
        AssertTransportInvalid(
            options => options.SchemaName = "1bad",
            nameof(SqlServerAsyncResponseTransportOptions.SchemaName));
    }

    [Theory]
    [InlineData("worker ")]
    [InlineData(" worker")]
    public void TransportOptions_RejectQueueNamesWithSurroundingSpaces(string queueName)
    {
        // Probed on SQL Server 2022: `queue = N'worker'` returns the rows of BOTH 'worker' and
        // 'worker ' — equality pads the shorter operand even under Latin1_General_100_BIN2. The
        // three queues share one table and are told apart only by that column, so names the
        // DATABASE cannot distinguish make the worker and response subscribers consume each
        // other's messages. Ordinal distinctness alone does not see it.
        AssertTransportInvalid(options => options.WorkerQueue = queueName, "space");
        AssertTransportInvalid(options => options.ResponseQueue = queueName, "space");
        AssertTransportInvalid(options => options.DeadLetterQueue = queueName, "space");
    }

    [Fact]
    public void TransportOptions_RejectQueueNamesLongerThanTheColumn()
        => AssertTransportInvalid(
            options => options.WorkerQueue = new string('w', SqlServerTransportOptionsValidator.MaxQueueNameLength + 1),
            "nvarchar(200)");

    [Fact]
    public void TransportOptions_RejectOverLimitIdentifiersAndAcceptTheCap()
    {
        AssertTransportInvalid(
            options => options.MessageTable = new string('q', 129),
            "limited to 128");

        // Exactly at the cap the derived index names reserve suffix space and stay distinct.
        var atCap = TransportOptions();
        atCap.MessageTable = new string('q', 128);
        SqlServerTransportOptionsValidator.ValidateCommon(atCap);
    }

    [Fact]
    public void TransportOptions_RejectQueueNameCollision()
    {
        var options = TransportOptions();
        options.ResponseQueue = "worker";

        var ex = Assert.Throws<InvalidOperationException>(() => SqlServerTransportOptionsValidator.ValidateCommon(options));
        Assert.Contains(nameof(SqlServerAsyncResponseTransportOptions.WorkerQueue), ex.Message);
    }

    [Fact]
    public void TransportOptions_RejectNonPositiveDeadLetterRetention()
    {
        var options = TransportOptions();
        options.DeadLetterRetention = TimeSpan.FromSeconds(-1);

        var ex = Assert.Throws<InvalidOperationException>(() => SqlServerTransportOptionsValidator.ValidateCommon(options));
        Assert.Contains(nameof(SqlServerAsyncResponseTransportOptions.DeadLetterRetention), ex.Message);
    }

    [Fact]
    public void TransportOptions_RejectNonPositiveAndMisorderedRetrySettings()
    {
        AssertTransportInvalid(
            options => options.LockTimeout = TimeSpan.Zero,
            nameof(SqlServerAsyncResponseTransportOptions.LockTimeout));
        AssertTransportInvalid(
            options => options.PublishMaxAttempts = 0,
            nameof(SqlServerAsyncResponseTransportOptions.PublishMaxAttempts));
        AssertTransportInvalid(
            options => options.PublishRetryBaseDelay = TimeSpan.FromSeconds(2),
            nameof(SqlServerAsyncResponseTransportOptions.PublishRetryBaseDelay));
        AssertTransportInvalid(
            options => options.SubscriberRetryBaseDelay = TimeSpan.FromSeconds(6),
            nameof(SqlServerAsyncResponseTransportOptions.SubscriberRetryBaseDelay));
        AssertTransportInvalid(
            options => options.CorrelationIdHeader = " ",
            nameof(SqlServerAsyncResponseTransportOptions.CorrelationIdHeader));
    }

    [Fact]
    public void TransportSubscriberOptions_ValidateEarlyAckAndFailureSettings()
    {
        Assert.Throws<InvalidOperationException>(() => SqlServerTransportOptionsValidator.ValidateSubscriber(
            new SqlServerSubscriberOptions { AckMode = SqlServerAckMode.AckAfterEnqueue },
            "Worker"));
        Assert.Throws<InvalidOperationException>(() => SqlServerTransportOptionsValidator.ValidateSubscriber(
            new SqlServerSubscriberOptions
            {
                AckMode = SqlServerAckMode.AckAfterEnqueue,
                BackgroundWorkerCount = 1,
                BackgroundQueueCapacity = 0
            },
            "Worker"));
        Assert.Throws<InvalidOperationException>(() => SqlServerTransportOptionsValidator.ValidateSubscriber(
            new SqlServerSubscriberOptions { BatchSize = 0 },
            "Worker"));
        Assert.Throws<InvalidOperationException>(() => SqlServerTransportOptionsValidator.ValidateSubscriber(
            new SqlServerSubscriberOptions { MaxDeliveryAttempts = -1 },
            "Worker"));
        Assert.Throws<InvalidOperationException>(() => SqlServerTransportOptionsValidator.ValidateSubscriber(
            new SqlServerSubscriberOptions { RedeliveryDelay = TimeSpan.Zero },
            "Worker"));
        Assert.Throws<InvalidOperationException>(() => SqlServerTransportOptionsValidator.ValidateSubscriber(
            new SqlServerSubscriberOptions { EmptyPollDelay = TimeSpan.Zero },
            "Worker"));
        Assert.Throws<InvalidOperationException>(() => SqlServerTransportOptionsValidator.ValidateSubscriber(
            new SqlServerSubscriberOptions { AckMode = (SqlServerAckMode)999 },
            "Worker"));

        var subscriber = new SqlServerSubscriberOptions().UseAckAfterEnqueue(2, 8, TimeSpan.FromSeconds(3));
        SqlServerTransportOptionsValidator.ValidateSubscriber(subscriber, "Worker");
        Assert.Equal(SqlServerAckMode.AckAfterEnqueue, subscriber.AckMode);
        Assert.Equal(2, subscriber.BackgroundWorkerCount);
        Assert.Equal(8, subscriber.BackgroundQueueCapacity);
        Assert.Equal(TimeSpan.FromSeconds(3), subscriber.BackgroundDrainTimeout);
    }

    [Fact]
    public void UseAckAfterEnqueue_RejectsInvalidArguments()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new SqlServerSubscriberOptions().UseAckAfterEnqueue(0, 8));
        Assert.Throws<ArgumentOutOfRangeException>(() => new SqlServerSubscriberOptions().UseAckAfterEnqueue(2, 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => new SqlServerSubscriberOptions().UseAckAfterEnqueue(2, 8, TimeSpan.Zero));
    }

    [Fact]
    public void ChannelOptions_RejectPublishBaseDelayAboveMax()
    {
        var options = ChannelOptions();
        options.PublishRetryBaseDelay = TimeSpan.FromSeconds(2);
        options.PublishRetryMaxDelay = TimeSpan.FromSeconds(1);

        var ex = Assert.Throws<InvalidOperationException>(options.Validate);
        Assert.Contains(nameof(SqlServerAsyncResponseChannelOptions.PublishRetryBaseDelay), ex.Message);
    }

    [Fact]
    public void ChannelOptions_RejectNegativePruneInterval()
    {
        var options = ChannelOptions();
        options.PruneInterval = TimeSpan.FromSeconds(-1);

        var ex = Assert.Throws<InvalidOperationException>(options.Validate);
        Assert.Contains(nameof(SqlServerAsyncResponseChannelOptions.PruneInterval), ex.Message);
    }

    [Fact]
    public void ReplyTargetProvider_UsesDefaultResponseQueue()
    {
        var options = TransportOptions();
        options.ResponseQueue = "responses";
        var provider = new SqlServerReplyTargetProvider(Options.Create(options));

        var target = provider.GetReplyTarget();

        Assert.Equal(SqlServerAsyncResponseTransportOptions.TransportName, target.Transport);
        Assert.Equal("responses", target.Address);
        Assert.Equal("responses", target.Properties["queue"]);
        Assert.Equal("asyncresponse_transport_messages", target.Properties["table"]);
    }

    [Fact]
    public void ReplyTargetProvider_ResolvesNamedTargetAndCopiesProperties()
    {
        var options = TransportOptions();
        options.SchemaName = "orders";
        options.AddReplyTarget("regional", "regional_responses");
        options.ReplyTargets["regional"].Properties["tenant"] = "acme";
        var provider = new SqlServerReplyTargetProvider(Options.Create(options));

        var target = provider.GetReplyTarget("regional");

        Assert.Equal("regional", target.Name);
        Assert.Equal("regional_responses", target.Address);
        Assert.Equal("regional_responses", target.Properties["queue"]);
        Assert.Equal("orders", target.Properties["schema"]);
        Assert.Equal("acme", target.Properties["tenant"]);
    }

    [Theory]
    [InlineData("regional_responses ")]
    [InlineData(" regional_responses")]
    public void AddReplyTarget_RejectsAQueueNameWithSurroundingSpaces(string queueName)
    {
        // A reply target's queue is a row key in the same nvarchar(200) column as the transport's
        // own three queues, reached by a different route: it is handed to remote publishers as the
        // reply address. A padded name lands rows the exact-matching claim predicate never returns, so
        // the reply is accepted and then never delivered.
        var options = TransportOptions();

        var ex = Assert.Throws<InvalidOperationException>(() => options.AddReplyTarget("regional", queueName));

        Assert.Contains("begins or ends with a space", ex.Message, StringComparison.Ordinal);
        Assert.Contains("regional", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AddReplyTarget_RejectsAnOverlongQueueName()
    {
        // 201 characters does not fit nvarchar(200): unvalidated, the remote publisher's insert
        // fails outright, at reply time, on the far side of the system boundary.
        var options = TransportOptions();

        var ex = Assert.Throws<InvalidOperationException>(() => options.AddReplyTarget(
            "regional",
            new string('r', SqlServerTransportOptionsValidator.MaxQueueNameLength + 1)));

        Assert.Contains("nvarchar(200)", ex.Message, StringComparison.Ordinal);
        Assert.Contains("regional", ex.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("overlong")]
    [InlineData("padded")]
    public void ReplyTargetProvider_RejectsABadQueueName_AddedByMutatingTheDictionary(string violation)
    {
        // ReplyTargets is publicly mutable, so AddReplyTarget's fail-fast is not the only way in;
        // resolution re-checks, and covers both rules the queue-name contract carries.
        var options = TransportOptions();
        options.ReplyTargets["regional"] = new SqlServerReplyTargetOptions
        {
            ResponseQueue = violation == "overlong"
                ? new string('r', SqlServerTransportOptionsValidator.MaxQueueNameLength + 1)
                : "regional_responses "
        };
        var provider = new SqlServerReplyTargetProvider(Options.Create(options));

        var ex = Assert.Throws<InvalidOperationException>(() => provider.GetReplyTarget("regional"));

        Assert.Contains(
            violation == "overlong" ? "nvarchar(200)" : "begins or ends with a space",
            ex.Message,
            StringComparison.Ordinal);
        Assert.Contains("regional", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ReplyTargetProvider_AcceptsAValidNamedQueue()
    {
        // The false-positive guard for the two rejections above: a legal name at the cap must still
        // resolve, or the validation would break every correctly configured reply target.
        var options = TransportOptions();
        var atCap = new string('r', SqlServerTransportOptionsValidator.MaxQueueNameLength);
        options.AddReplyTarget("regional", atCap);
        var provider = new SqlServerReplyTargetProvider(Options.Create(options));

        Assert.Equal(atCap, provider.GetReplyTarget("regional").Address);
    }

    [Fact]
    public void ReplyTargetProvider_UnknownName_Throws()
    {
        var provider = new SqlServerReplyTargetProvider(Options.Create(TransportOptions()));

        var ex = Assert.Throws<InvalidOperationException>(() => provider.GetReplyTarget("missing"));

        Assert.Contains("missing", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void CorrelationExtractor_ReadsHeaderBeforeJsonBody()
    {
        var options = TransportOptions();
        var headers = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            [options.CorrelationIdHeader] = "from-header"
        };

        var correlationId = SqlServerCorrelationIdExtractor.Extract(
            headers,
            """{"CorrelationId":"from-body"}""",
            options);

        Assert.Equal("from-header", correlationId);
    }

    [Fact]
    public void CorrelationExtractor_ReadsNestedJsonStringAndIsCaseInsensitive()
    {
        var options = TransportOptions();
        options.CorrelationIdJsonPaths = ["CustomParameters.CorrelationId"];

        var correlationId = SqlServerCorrelationIdExtractor.Extract(
            headers: null,
            """{"customparameters":"{\"correlationid\":\"from-nested-json-string\"}"}""",
            options);

        Assert.Equal("from-nested-json-string", correlationId);
    }

    [Fact]
    public void CorrelationExtractor_ReturnsNullForInvalidJsonBlankPathsOrBlankMessage()
    {
        var options = TransportOptions();

        Assert.Null(SqlServerCorrelationIdExtractor.Extract(null, "{not-json", options));
        Assert.Null(SqlServerCorrelationIdExtractor.Extract(null, "", options));
        Assert.Null(SqlServerCorrelationIdExtractor.Extract(null, "null", options));

        options.CorrelationIdJsonPaths = [];
        Assert.Null(SqlServerCorrelationIdExtractor.Extract(null, """{"CorrelationId":"ignored"}""", options));
    }

    [Fact]
    public void CorrelationExtractor_HandlesUnmatchedBlankPrimitiveAndMalformedNestedPaths()
    {
        var options = TransportOptions();
        options.CorrelationIdJsonPaths =
        [
            "",
            "Missing.Value",
            "CustomParameters.CorrelationId",
            "CorrelationId"
        ];

        Assert.Null(SqlServerCorrelationIdExtractor.Extract(
            null,
            """{"CustomParameters":42,"Other":"x"}""",
            options));

        Assert.Null(SqlServerCorrelationIdExtractor.Extract(
            null,
            """{"CustomParameters":"{not-json"}""",
            options));

        Assert.Equal("42", SqlServerCorrelationIdExtractor.Extract(
            null,
            """{"CorrelationId":42}""",
            options));
    }

    [Fact]
    public void SchemaLockResource_AgreesAcrossChannelAndTransport()
    {
        // Channel and transport must take the SAME application lock for a shared schema, otherwise they
        // still race each other on CREATE SCHEMA. The resources are computed independently in each
        // package, so this guards against the two implementations drifting apart.
        foreach (var schema in new[] { "dbo", "async_response", "Tenant_42" })
        {
            Assert.Equal(
                AsyncResponse.Channels.SqlServer.SqlServerChannelSql.SchemaLockResource(schema),
                AsyncResponse.Transports.SqlServer.SqlServerTransportStore.SchemaLockResource(schema));
        }

        // Distinct schemas must map to distinct resources so unrelated deployments don't serialize.
        Assert.NotEqual(
            AsyncResponse.Channels.SqlServer.SqlServerChannelSql.SchemaLockResource("dbo"),
            AsyncResponse.Channels.SqlServer.SqlServerChannelSql.SchemaLockResource("other"));
    }

    [Fact]
    public void AddMilliseconds_AgreesAcrossChannelAndTransport_AndStaysOnDatabaseClock()
    {
        // Both packages compute row expiries with the same database-clock expression; drift between
        // them would silently mix clocks for a shared deployment.
        Assert.Equal(
            AsyncResponse.Channels.SqlServer.SqlServerChannelSql.AddMilliseconds("@p"),
            AsyncResponse.Transports.SqlServer.SqlServerTransportStore.AddMilliseconds("@p"));
        Assert.Contains("SYSUTCDATETIME()", AsyncResponse.Channels.SqlServer.SqlServerChannelSql.AddMilliseconds("@p"), StringComparison.Ordinal);
    }

    [Fact]
    public void ChannelPrunes_AreBounded_SoTheyCannotEscalateToATableLock()
    {
        // Regression: the three table-wide prunes ran unbounded, inline on the publish and probe
        // paths. A backlog past SQL Server's ~5,000-lock escalation threshold took a table lock
        // that stalled concurrent delivery claims — long enough for a live waiter's claim to lose
        // to the recovery claim. Every durable-flow store bounds its prune batch; so does the channel.
        // Reflected rather than called: the helper is the fix, so an older build has no bounded
        // statement to hand back and this fact fails there instead of failing to compile.
        var pruneSql = typeof(AsyncResponse.Channels.SqlServer.SqlServerChannelSql)
            .GetMethod("ExpiredPruneSql", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static);
        Assert.NotNull(pruneSql);
        var sql = (string)pruneSql!.Invoke(null, ["[dbo].[ar_messages]"])!;

        Assert.StartsWith("DELETE TOP (1000) FROM [dbo].[ar_messages]", sql, StringComparison.Ordinal);
        Assert.Contains("expires_at <= SYSUTCDATETIME()", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void CorrelationExtractor_ReadsConfiguredJsonPath()
    {
        var options = TransportOptions();

        var correlationId = SqlServerCorrelationIdExtractor.Extract(
            headers: null,
            """{"CustomParameters":{"CorrelationId":"from-json"}}""",
            options);

        Assert.Equal("from-json", correlationId);
    }

    [Fact]
    public void CorrelationExtractor_ReturnsNull_WhenTouchedObjectHasExactDuplicateKey()
        // An object with a duplicate key cannot resolve a property, so the id is simply not in this
        // body: extraction reports "not found" and the ingress acknowledges the message as
        // unroutable. Throwing made it a handler failure, which on RabbitMQ's default cap of 0
        // requeued forever.
        => Assert.Null(SqlServerCorrelationIdExtractor.Extract(
            headers: null,
            """{"CorrelationId":"1","CorrelationId":"2"}""",
            TransportOptions()));

    [Fact]
    public void SqlServerRetry_ClassifiesTransientExceptions()
    {
        Assert.True(SqlServerTransportRetry.IsTransient(new TimeoutException()));
        Assert.True(SqlServerChannelSql.IsTransient(new TimeoutException()));
        Assert.False(SqlServerTransportRetry.IsTransient(new OperationCanceledException()));
        Assert.False(SqlServerChannelSql.IsTransient(new OperationCanceledException()));
        Assert.False(SqlServerTransportRetry.IsTransient(new InvalidOperationException()));
        Assert.False(SqlServerChannelSql.IsTransient(new InvalidOperationException()));
    }

    [Fact]
    public void SqlServerRetry_ClassifiesSqlErrorNumbersAndSeverity()
    {
        var transientNumber = CreateSqlException(1205, severity: 10);
        var transientSeverity = CreateSqlException(50000, severity: 20);
        var permanent = CreateSqlException(50000, severity: 10);

        Assert.True(SqlServerTransportRetry.IsTransient(transientNumber));
        Assert.True(SqlServerChannelSql.IsTransient(transientNumber));
        Assert.True(SqlServerTransportRetry.IsTransient(transientSeverity));
        Assert.True(SqlServerChannelSql.IsTransient(transientSeverity));
        Assert.False(SqlServerTransportRetry.IsTransient(permanent));
        Assert.False(SqlServerChannelSql.IsTransient(permanent));
    }

    [Fact]
    public void ChannelSql_HelperBoundaries_HandleIdentifierAndIndexName()
    {
        Assert.True(InvokeChannelSqlStatic<bool>("IsIdentifier", "valid_1"));
        Assert.False(InvokeChannelSqlStatic<bool>("IsIdentifier", ""));
        Assert.False(InvokeChannelSqlStatic<bool>("IsIdentifier", "1bad"));
        Assert.False(InvokeChannelSqlStatic<bool>("IsIdentifier", "bad-name"));

        // SQL Server identifiers cap at 128 characters (vs PostgreSQL's 63).
        var indexName = InvokeChannelSqlStatic<string>("IndexName", new string('a', 130), "expires");
        Assert.Equal(128, indexName.Length);
    }

    [Fact]
    public void ChannelSql_ShouldPrune_ThrottlesByConfiguredInterval()
    {
        var options = ChannelOptions();
        options.PruneInterval = TimeSpan.FromMinutes(10);
        var sql = new SqlServerChannelSql(Options.Create(options));
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
        var store = CreateRecoveryStateStore();

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
        var store = CreateRecoveryStateStore();

        Assert.Null(InvokeDeserializeState(store, "null", "fallback"));
        Assert.Null(InvokeDeserializeState(store, "{not-json", "fallback"));

        // A row is rejected for four reasons, and only three of them mean "this build cannot read
        // it". The fourth — the stored row carries a DIFFERENT correlation id — is a readable row
        // that belongs to another conversation, surfaced by a legacy case-insensitive collation and
        // correctly refused by the ordinal re-check. Counting it as unreadable turned that refusal
        // into a failed delivery for an id that simply has nothing registered.
        Assert.Equal(1, InvokeDeserializeStateUnreadableCount(store, "{not-json", "fallback"));
        Assert.Equal(
            0,
            InvokeDeserializeStateUnreadableCount(
                store,
                JsonSerializer.Serialize(new RecoveryState { CorrelationId = "OTHER-ID", RegistrationId = Guid.NewGuid() }),
                "other-id"));
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
    public async Task SqlServerRetry_RetriesTransientTimeouts()
    {
        var attempts = 0;

        var result = await SqlServerTransportRetry.ExecuteAsync(
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
    public async Task SqlServerRetry_DoesNotRetryCancellation()
    {
        var attempts = 0;

        Task<int> Action(CancellationToken _)
        {
            attempts++;
            throw new OperationCanceledException();
        }

        await Assert.ThrowsAsync<OperationCanceledException>(() => SqlServerTransportRetry.ExecuteAsync(
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
            nameof(SqlServerAsyncResponseChannelOptions.DeliveryConfirmationTimeout));
        AssertChannelInvalid(
            options => options.MessageRetention = TimeSpan.MaxValue,
            nameof(SqlServerAsyncResponseChannelOptions.MessageRetention));
        AssertChannelInvalid(
            options => options.SubscriberHeartbeatTimeout = TimeSpan.MaxValue,
            nameof(SqlServerAsyncResponseChannelOptions.SubscriberHeartbeatTimeout));
        AssertChannelInvalid(
            options => options.ActivePollInterval = TimeSpan.FromDays(60),
            nameof(SqlServerAsyncResponseChannelOptions.ActivePollInterval));
        AssertChannelInvalid(
            options => options.IdlePollInterval = TimeSpan.FromDays(60),
            nameof(SqlServerAsyncResponseChannelOptions.IdlePollInterval));
        AssertChannelInvalid(
            options => options.DeliveryConfirmationPollInterval = TimeSpan.FromDays(60),
            nameof(SqlServerAsyncResponseChannelOptions.DeliveryConfirmationPollInterval));
    }

    private static void AssertChannelInvalid(
        Action<SqlServerAsyncResponseChannelOptions> configure,
        string expectedMessageFragment)
    {
        var options = ChannelOptions();
        configure(options);

        var ex = Assert.Throws<InvalidOperationException>(options.Validate);

        Assert.Contains(expectedMessageFragment, ex.Message, StringComparison.Ordinal);
    }

    private static void AssertTransportInvalid(
        Action<SqlServerAsyncResponseTransportOptions> configure,
        string expectedMessageFragment)
    {
        var options = TransportOptions();
        configure(options);

        var ex = Assert.Throws<InvalidOperationException>(() => SqlServerTransportOptionsValidator.ValidateCommon(options));

        Assert.Contains(expectedMessageFragment, ex.Message, StringComparison.Ordinal);
    }

    private static SqlServerAsyncResponseChannelOptions ChannelOptions()
        => new() { ConnectionString = TestConnectionString };

    private static SqlServerAsyncResponseTransportOptions TransportOptions()
        => new() { ConnectionString = TestConnectionString };

    private static SqlServerRecoveryStateStore CreateRecoveryStateStore()
    {
        var sql = new SqlServerChannelSql(Options.Create(ChannelOptions()));
        return new SqlServerRecoveryStateStore(sql, NullLogger<SqlServerRecoveryStateStore>.Instance);
    }

    private static T InvokeChannelSqlStatic<T>(string name, params object?[] args)
        => (T)typeof(SqlServerChannelSql)
            .GetMethod(name, BindingFlags.NonPublic | BindingFlags.Static)!
            .Invoke(null, args)!;

    private static bool InvokeShouldPrune(SqlServerChannelSql sql, ref long lastTicks)
    {
        object?[] args = [lastTicks];
        var result = (bool)typeof(SqlServerChannelSql)
            .GetMethod("ShouldPrune", BindingFlags.NonPublic | BindingFlags.Instance)!
            .Invoke(sql, args)!;
        lastTicks = (long)args[0]!;
        return result;
    }

    private static RecoveryState? InvokeDeserializeState(
        SqlServerRecoveryStateStore store,
        string json,
        string? correlationId)
        => (RecoveryState?)typeof(SqlServerRecoveryStateStore)
            .GetMethod("DeserializeState", BindingFlags.NonPublic | BindingFlags.Instance)!
            .Invoke(store, [json, correlationId, 0]);

    /// <summary>
    /// Same call, but reporting how many rows were rejected as UNREADABLE. A row rejected only for
    /// carrying another correlation id must not be counted — that is what keeps a legacy
    /// case-insensitive collation returning somebody else's row from being reported as corruption.
    /// </summary>
    private static int InvokeDeserializeStateUnreadableCount(
        SqlServerRecoveryStateStore store,
        string json,
        string? correlationId)
    {
        var args = new object?[] { json, correlationId, 0 };
        typeof(SqlServerRecoveryStateStore)
            .GetMethod("DeserializeState", BindingFlags.NonPublic | BindingFlags.Instance)!
            .Invoke(store, args);
        return (int)args[2]!;
    }

    private static SqlException CreateSqlException(int number, byte severity)
    {
        var error = (SqlError)RuntimeHelpers.GetUninitializedObject(typeof(SqlError));
        typeof(SqlError).GetFields(BindingFlags.NonPublic | BindingFlags.Instance)
            .Single(field => field.Name.Equals("_number", StringComparison.OrdinalIgnoreCase))
            .SetValue(error, number);
        typeof(SqlError).GetFields(BindingFlags.NonPublic | BindingFlags.Instance)
            .Single(field => field.Name.Equals("_errorClass", StringComparison.OrdinalIgnoreCase))
            .SetValue(error, severity);

        var errors = (SqlErrorCollection)Activator.CreateInstance(typeof(SqlErrorCollection), nonPublic: true)!;
        typeof(SqlErrorCollection).GetMethod("Add", BindingFlags.NonPublic | BindingFlags.Instance)!
            .Invoke(errors, [error]);

        var exception = (SqlException)RuntimeHelpers.GetUninitializedObject(typeof(SqlException));
        typeof(SqlException).GetFields(BindingFlags.NonPublic | BindingFlags.Instance)
            .Single(field => field.FieldType == typeof(SqlErrorCollection))
            .SetValue(exception, errors);
        return exception;
    }
}
