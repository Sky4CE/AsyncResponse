using AsyncResponse.Channels.MongoDB;
using AsyncResponse.DurableFlows.MongoDB;
using AsyncResponse.Transports.MongoDB;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Core.Clusters;
using MongoDB.Driver.Core.Connections;
using MongoDB.Driver.Core.Servers;
using System.Net;
using System.Reflection;
using System.Text.Json;
using Xunit;

namespace AsyncResponse.Tests;

public sealed class MongoDbOptionsTests
{
    [Fact]
    public void ChannelOptions_Validate_PassesForDefaults()
        => new MongoDbAsyncResponseChannelOptions().Validate();

    [Fact]
    public void ChannelOptions_RejectInvalidCollectionName()
    {
        AssertChannelInvalid(
            options => options.MessageCollection = "bad$name",
            nameof(MongoDbAsyncResponseChannelOptions.MessageCollection));
        AssertChannelInvalid(
            options => options.RecoveryStateCollection = " ",
            nameof(MongoDbAsyncResponseChannelOptions.RecoveryStateCollection));
        AssertChannelInvalid(
            options => options.SubscriberCollection = "system.subscribers",
            nameof(MongoDbAsyncResponseChannelOptions.SubscriberCollection));
        // The reserved system namespace also cannot appear INSIDE a dotted name.
        AssertChannelInvalid(
            options => options.SubscriberCollection = "app.system.subscribers",
            nameof(MongoDbAsyncResponseChannelOptions.SubscriberCollection));
    }

    [Fact]
    public void SharedDatabase_CrossComponentCollectionCollision_FailsInEitherStartupOrder()
    {
        // The flow store configured onto the channel's DERIVED "{MessageCollection}_counters"
        // collection: its TTL index would silently delete the ack-sequence counter. Neither
        // component's own validation can see the other, so the container-scoped ownership ledger
        // must fail whichever store is constructed second — in both orders.
        static ServiceProvider Build()
        {
            var client = new Mock<IMongoClient>();
            client.SetupGet(c => c.Settings).Returns(MongoClientSettings.FromConnectionString("mongodb://localhost:27017"));
            var database = new Mock<IMongoDatabase>().WithTestNamespace();
            database.SetupGet(d => d.Client).Returns(client.Object);

            var services = new ServiceCollection();
            services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
            services.AddSingleton(database.Object);
            services.AddAsyncResponse()
                .WithMongoDbChannel(options => options.MessageCollection = "jobs")
                .WithInMemoryTransport()
                .WithMongoDbDurableFlows(options => options.CollectionName = "jobs_counters");
            return services.BuildServiceProvider();
        }

        using (var channelFirst = Build())
        {
            _ = channelFirst.GetRequiredService<MongoDbChannelStore>();
            var ex = Assert.Throws<InvalidOperationException>(channelFirst.GetRequiredService<MongoDbFlowStateStore>);
            Assert.Contains("MongoDB channel", ex.Message, StringComparison.Ordinal);
            Assert.Contains("MongoDB durable-flow store", ex.Message, StringComparison.Ordinal);
            Assert.Contains("jobs_counters", ex.Message, StringComparison.Ordinal);
        }

        using (var flowFirst = Build())
        {
            _ = flowFirst.GetRequiredService<MongoDbFlowStateStore>();
            var ex = Assert.Throws<InvalidOperationException>(flowFirst.GetRequiredService<MongoDbChannelStore>);
            Assert.Contains("MongoDB durable-flow store", ex.Message, StringComparison.Ordinal);
            Assert.Contains("jobs_counters", ex.Message, StringComparison.Ordinal);
        }

        // Distinct collections coexist: both stores resolve.
        var okClient = new Mock<IMongoClient>();
        okClient.SetupGet(c => c.Settings).Returns(MongoClientSettings.FromConnectionString("mongodb://localhost:27017"));
        var okDatabase = new Mock<IMongoDatabase>().WithTestNamespace();
        okDatabase.SetupGet(d => d.Client).Returns(okClient.Object);
        var okServices = new ServiceCollection();
        okServices.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        okServices.AddSingleton(okDatabase.Object);
        okServices.AddAsyncResponse()
            .WithMongoDbChannel(options => options.MessageCollection = "jobs")
            .WithInMemoryTransport()
            .WithMongoDbDurableFlows(options => options.CollectionName = "flows");
        using var ok = okServices.BuildServiceProvider();
        _ = ok.GetRequiredService<MongoDbChannelStore>();
        _ = ok.GetRequiredService<MongoDbFlowStateStore>();
    }

    [Fact]
    public void Stores_RejectEffectiveNamespacesOverTheByteLimit_AtConstruction()
    {
        // The namespace limit spans "database.collection", so only the store — which knows the
        // actual database name — can enforce it. The stores enforce the SHARDED limit (235
        // bytes) conservatively: 255 is only valid while the collection stays unsharded, and a
        // later shard-enable would strand a 236..255-byte namespace. MongoClient/GetDatabase are
        // lazy, so no server is needed. db "tests" (5 bytes) + "." + N-byte collection = N + 6.
        var database = new MongoClient("mongodb://localhost:27017").GetDatabase("tests");

        // 230-char collection: 236-byte namespace — one byte over the sharded limit.
        var direct = Assert.Throws<InvalidOperationException>(() => new MongoDbChannelStore(
            database,
            Options.Create(new MongoDbAsyncResponseChannelOptions { MessageCollection = new string('m', 230) })));
        Assert.Contains("235 bytes", direct.Message, StringComparison.Ordinal);

        // 225-char collection: its own namespace fits (231 bytes) but the DERIVED "_counters"
        // namespace is 240 bytes — the gap static validation could never see.
        var derived = Assert.Throws<InvalidOperationException>(() => new MongoDbChannelStore(
            database,
            Options.Create(new MongoDbAsyncResponseChannelOptions { MessageCollection = new string('m', 225) })));
        Assert.Contains("ack-counter", derived.Message, StringComparison.Ordinal);

        var transport = Assert.Throws<InvalidOperationException>(() => new MongoDbTransportStore(
            database,
            Options.Create(new MongoDbAsyncResponseTransportOptions { MessageCollection = new string('m', 230) })));
        Assert.Contains("235 bytes", transport.Message, StringComparison.Ordinal);

        var flow = Assert.Throws<InvalidOperationException>(() => new MongoDbFlowStateStore(
            database,
            Options.Create(new MongoDbDurableFlowOptions { CollectionName = new string('m', 230) })));
        Assert.Contains("235 bytes", flow.Message, StringComparison.Ordinal);

        // At the boundary (235 exactly) everything constructs: 220 + 6 + 9 ("_counters") = 235
        // for the channel's derived counters namespace, and 229 + 6 = 235 for the flow store.
        using var atLimit = new MongoDbChannelStore(
            database,
            Options.Create(new MongoDbAsyncResponseChannelOptions { MessageCollection = new string('m', 220) }));
        using var flowAtLimit = new MongoDbFlowStateStore(
            database,
            Options.Create(new MongoDbDurableFlowOptions { CollectionName = new string('m', 229) }));
    }

    [Fact]
    public void ChannelOptions_RejectCollectionNameCollision()
    {
        AssertChannelInvalid(
            options => options.MessageCollection = options.RecoveryStateCollection,
            nameof(MongoDbAsyncResponseChannelOptions.RecoveryStateCollection));
    }

    [Fact]
    public void ChannelOptions_RejectCollectionsOccupyingTheDerivedCountersName()
    {
        // "{MessageCollection}_counters" is part of the effective name plan: were the TTL-indexed
        // recovery collection to occupy it, the reaper would silently delete the ack counter and
        // reset the same-tick delivery tie-breaker.
        AssertChannelInvalid(
            options => options.RecoveryStateCollection = $"{options.MessageCollection}_counters",
            "reserved for the ack counter");
        AssertChannelInvalid(
            options => options.SubscriberCollection = $"{options.MessageCollection}_counters",
            "reserved for the ack counter");
    }

    [Fact]
    public void ChannelOptions_RejectHeartbeatIntervalAtOrAboveTimeout()
    {
        var options = new MongoDbAsyncResponseChannelOptions
        {
            SubscriberHeartbeatInterval = TimeSpan.FromSeconds(30),
            SubscriberHeartbeatTimeout = TimeSpan.FromSeconds(30)
        };

        var ex = Assert.Throws<InvalidOperationException>(options.Validate);
        Assert.Contains(nameof(MongoDbAsyncResponseChannelOptions.SubscriberHeartbeatInterval), ex.Message);
    }

    [Fact]
    public void ChannelOptions_RejectNonPositiveRetentionAndConfirmationSettings()
    {
        AssertChannelInvalid(
            options => options.MessageRetention = TimeSpan.Zero,
            nameof(MongoDbAsyncResponseChannelOptions.MessageRetention));
        AssertChannelInvalid(
            options => options.DeliveryConfirmationTimeout = TimeSpan.Zero,
            nameof(MongoDbAsyncResponseChannelOptions.DeliveryConfirmationTimeout));
        AssertChannelInvalid(
            options => options.DeliveryConfirmationPollInterval = TimeSpan.Zero,
            nameof(MongoDbAsyncResponseChannelOptions.DeliveryConfirmationPollInterval));
        AssertChannelInvalid(
            options => options.ListenerPollInterval = TimeSpan.Zero,
            nameof(MongoDbAsyncResponseChannelOptions.ListenerPollInterval));
    }

    [Fact]
    public void ChannelOptions_RejectInvalidWaiterAndEnvelopeSettings()
    {
        AssertChannelInvalid(
            options => options.DefaultTimeout = TimeSpan.Zero,
            nameof(MongoDbAsyncResponseChannelOptions.DefaultTimeout));
        // Shared-base knob: enforced via ValidateShared — a bespoke validator that skips the
        // shared guards accepted TimeSpan.Zero here, defeating the promised disposal bound.
        AssertChannelInvalid(
            options => options.DisposalDrainTimeout = TimeSpan.Zero,
            nameof(MongoDbAsyncResponseChannelOptions.DisposalDrainTimeout));
        AssertChannelInvalid(
            options => options.MaxRemoteStackTraceLength = -1,
            nameof(MongoDbAsyncResponseChannelOptions.MaxRemoteStackTraceLength));
        AssertChannelInvalid(
            options => options.PendingMessageBatchSize = 0,
            nameof(MongoDbAsyncResponseChannelOptions.PendingMessageBatchSize));
        AssertChannelInvalid(
            options => options.PublishMaxAttempts = 0,
            nameof(MongoDbAsyncResponseChannelOptions.PublishMaxAttempts));
    }

    [Fact]
    public void ChannelOptions_RejectPublishBaseDelayAboveMax()
    {
        var options = new MongoDbAsyncResponseChannelOptions
        {
            PublishRetryBaseDelay = TimeSpan.FromSeconds(2),
            PublishRetryMaxDelay = TimeSpan.FromSeconds(1)
        };

        var ex = Assert.Throws<InvalidOperationException>(options.Validate);
        Assert.Contains(nameof(MongoDbAsyncResponseChannelOptions.PublishRetryBaseDelay), ex.Message);
    }

    [Fact]
    public void TransportOptions_ValidateCommon_PassesForDefaults()
        => MongoDbTransportOptionsValidator.ValidateCommon(new MongoDbAsyncResponseTransportOptions());

    [Fact]
    public void TransportOptions_RejectInvalidCollectionName()
    {
        AssertTransportInvalid(
            options => options.MessageCollection = "bad$collection",
            nameof(MongoDbAsyncResponseTransportOptions.MessageCollection));
        AssertTransportInvalid(
            options => options.MessageCollection = " ",
            nameof(MongoDbAsyncResponseTransportOptions.MessageCollection));
        AssertTransportInvalid(
            options => options.MessageCollection = "system.queue",
            nameof(MongoDbAsyncResponseTransportOptions.MessageCollection));
        AssertTransportInvalid(
            options => options.MessageCollection = "app.system.queue",
            nameof(MongoDbAsyncResponseTransportOptions.MessageCollection));
    }

    [Fact]
    public void TransportOptions_RejectQueueNameCollision()
    {
        var options = new MongoDbAsyncResponseTransportOptions { ResponseQueue = "worker" };

        var ex = Assert.Throws<InvalidOperationException>(() => MongoDbTransportOptionsValidator.ValidateCommon(options));
        Assert.Contains(nameof(MongoDbAsyncResponseTransportOptions.WorkerQueue), ex.Message);
    }

    [Fact]
    public void TransportOptions_RejectNonPositiveDeadLetterRetention()
    {
        var options = new MongoDbAsyncResponseTransportOptions { DeadLetterRetention = TimeSpan.FromSeconds(-1) };

        var ex = Assert.Throws<InvalidOperationException>(() => MongoDbTransportOptionsValidator.ValidateCommon(options));
        Assert.Contains(nameof(MongoDbAsyncResponseTransportOptions.DeadLetterRetention), ex.Message);
    }

    [Fact]
    public void TransportOptions_RejectNonPositiveAndMisorderedRetrySettings()
    {
        AssertTransportInvalid(
            options => options.LockTimeout = TimeSpan.Zero,
            nameof(MongoDbAsyncResponseTransportOptions.LockTimeout));
        AssertTransportInvalid(
            options => options.PublishMaxAttempts = 0,
            nameof(MongoDbAsyncResponseTransportOptions.PublishMaxAttempts));
        AssertTransportInvalid(
            options => options.PublishRetryBaseDelay = TimeSpan.FromSeconds(2),
            nameof(MongoDbAsyncResponseTransportOptions.PublishRetryBaseDelay));
        AssertTransportInvalid(
            options => options.SubscriberRetryBaseDelay = TimeSpan.FromSeconds(6),
            nameof(MongoDbAsyncResponseTransportOptions.SubscriberRetryBaseDelay));
        AssertTransportInvalid(
            options => options.CorrelationIdHeader = " ",
            nameof(MongoDbAsyncResponseTransportOptions.CorrelationIdHeader));
    }

    [Fact]
    public void TransportSubscriberOptions_ValidateEarlyAckAndFailureSettings()
    {
        Assert.Throws<InvalidOperationException>(() => MongoDbTransportOptionsValidator.ValidateSubscriber(
            new MongoDbSubscriberOptions { AckMode = MongoDbAckMode.AckAfterEnqueue },
            "Worker"));
        Assert.Throws<InvalidOperationException>(() => MongoDbTransportOptionsValidator.ValidateSubscriber(
            new MongoDbSubscriberOptions
            {
                AckMode = MongoDbAckMode.AckAfterEnqueue,
                BackgroundWorkerCount = 1,
                BackgroundQueueCapacity = 0
            },
            "Worker"));
        Assert.Throws<InvalidOperationException>(() => MongoDbTransportOptionsValidator.ValidateSubscriber(
            new MongoDbSubscriberOptions { BatchSize = 0 },
            "Worker"));
        Assert.Throws<InvalidOperationException>(() => MongoDbTransportOptionsValidator.ValidateSubscriber(
            new MongoDbSubscriberOptions { MaxDeliveryAttempts = -1 },
            "Worker"));
        Assert.Throws<InvalidOperationException>(() => MongoDbTransportOptionsValidator.ValidateSubscriber(
            new MongoDbSubscriberOptions { RedeliveryDelay = TimeSpan.Zero },
            "Worker"));
        Assert.Throws<InvalidOperationException>(() => MongoDbTransportOptionsValidator.ValidateSubscriber(
            new MongoDbSubscriberOptions { AckMode = (MongoDbAckMode)999 },
            "Worker"));

        var subscriber = new MongoDbSubscriberOptions().UseAckAfterEnqueue(2, 8, TimeSpan.FromSeconds(3));
        MongoDbTransportOptionsValidator.ValidateSubscriber(subscriber, "Worker");
        Assert.Equal(MongoDbAckMode.AckAfterEnqueue, subscriber.AckMode);
        Assert.Equal(2, subscriber.BackgroundWorkerCount);
        Assert.Equal(8, subscriber.BackgroundQueueCapacity);
        Assert.Equal(TimeSpan.FromSeconds(3), subscriber.BackgroundDrainTimeout);
    }

    [Fact]
    public void UseAckAfterEnqueue_RejectsInvalidArguments()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new MongoDbSubscriberOptions().UseAckAfterEnqueue(0, 8));
        Assert.Throws<ArgumentOutOfRangeException>(() => new MongoDbSubscriberOptions().UseAckAfterEnqueue(2, 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => new MongoDbSubscriberOptions().UseAckAfterEnqueue(2, 8, TimeSpan.Zero));
    }

    [Fact]
    public void ReplyTargetProvider_UsesDefaultResponseQueue()
    {
        var provider = new MongoDbReplyTargetProvider(Options.Create(new MongoDbAsyncResponseTransportOptions
        {
            ResponseQueue = "responses"
        }));

        var target = provider.GetReplyTarget();

        Assert.Equal(MongoDbAsyncResponseTransportOptions.TransportName, target.Transport);
        Assert.Equal("responses", target.Address);
        Assert.Equal("responses", target.Properties["queue"]);
        Assert.Equal("asyncresponse_transport_messages", target.Properties["collection"]);
    }

    [Fact]
    public void ReplyTargetProvider_ResolvesNamedTargetAndCopiesProperties()
    {
        var options = new MongoDbAsyncResponseTransportOptions { DatabaseName = "orders" };
        options.AddReplyTarget("regional", "regional_responses");
        options.ReplyTargets["regional"].Properties["tenant"] = "acme";
        var provider = new MongoDbReplyTargetProvider(Options.Create(options));

        var target = provider.GetReplyTarget("regional");

        Assert.Equal("regional", target.Name);
        Assert.Equal("regional_responses", target.Address);
        Assert.Equal("regional_responses", target.Properties["queue"]);
        Assert.Equal("orders", target.Properties["database"]);
        Assert.Equal("acme", target.Properties["tenant"]);
    }

    [Fact]
    public void ReplyTargetProvider_UnknownName_Throws()
    {
        var provider = new MongoDbReplyTargetProvider(Options.Create(new MongoDbAsyncResponseTransportOptions()));

        var ex = Assert.Throws<InvalidOperationException>(() => provider.GetReplyTarget("missing"));

        Assert.Contains("missing", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void CorrelationExtractor_ReadsHeaderBeforeJsonBody()
    {
        var options = new MongoDbAsyncResponseTransportOptions();
        var headers = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            [options.CorrelationIdHeader] = "from-header"
        };

        var correlationId = MongoDbCorrelationIdExtractor.Extract(
            headers,
            """{"CorrelationId":"from-body"}""",
            options);

        Assert.Equal("from-header", correlationId);
    }

    [Fact]
    public void CorrelationExtractor_ReadsNestedJsonStringAndIsCaseInsensitive()
    {
        var options = new MongoDbAsyncResponseTransportOptions
        {
            CorrelationIdJsonPaths = ["CustomParameters.CorrelationId"]
        };

        var correlationId = MongoDbCorrelationIdExtractor.Extract(
            headers: null,
            """{"customparameters":"{\"correlationid\":\"from-nested-json-string\"}"}""",
            options);

        Assert.Equal("from-nested-json-string", correlationId);
    }

    [Fact]
    public void CorrelationExtractor_ReturnsNullForInvalidJsonBlankPathsOrBlankMessage()
    {
        var options = new MongoDbAsyncResponseTransportOptions();

        Assert.Null(MongoDbCorrelationIdExtractor.Extract(null, "{not-json", options));
        Assert.Null(MongoDbCorrelationIdExtractor.Extract(null, "", options));
        Assert.Null(MongoDbCorrelationIdExtractor.Extract(null, "null", options));

        options.CorrelationIdJsonPaths = [];
        Assert.Null(MongoDbCorrelationIdExtractor.Extract(null, """{"CorrelationId":"ignored"}""", options));
    }

    [Fact]
    public void CorrelationExtractor_HandlesUnmatchedBlankPrimitiveAndMalformedNestedPaths()
    {
        var options = new MongoDbAsyncResponseTransportOptions
        {
            CorrelationIdJsonPaths =
            [
                "",
                "Missing.Value",
                "CustomParameters.CorrelationId",
                "CorrelationId"
            ]
        };

        Assert.Null(MongoDbCorrelationIdExtractor.Extract(
            null,
            """{"CustomParameters":42,"Other":"x"}""",
            options));

        Assert.Null(MongoDbCorrelationIdExtractor.Extract(
            null,
            """{"CustomParameters":"{not-json"}""",
            options));

        Assert.Equal("42", MongoDbCorrelationIdExtractor.Extract(
            null,
            """{"CorrelationId":42}""",
            options));
    }

    [Fact]
    public void CorrelationExtractor_ReadsConfiguredJsonPath()
    {
        var options = new MongoDbAsyncResponseTransportOptions();

        var correlationId = MongoDbCorrelationIdExtractor.Extract(
            headers: null,
            """{"CustomParameters":{"CorrelationId":"from-json"}}""",
            options);

        Assert.Equal("from-json", correlationId);
    }

    [Fact]
    public void MongoDbRetry_ClassifiesTransientExceptions()
    {
        Assert.True(MongoDbTransportRetry.IsTransient(new TimeoutException()));
        Assert.True(MongoDbChannelStore.IsTransient(new TimeoutException()));
        Assert.False(MongoDbTransportRetry.IsTransient(new OperationCanceledException()));
        Assert.False(MongoDbChannelStore.IsTransient(new OperationCanceledException()));
        Assert.False(MongoDbTransportRetry.IsTransient(new InvalidOperationException()));
        Assert.False(MongoDbChannelStore.IsTransient(new InvalidOperationException()));
    }

    [Fact]
    public void MongoDbRetry_ClassifiesDriverExceptionsAndRetryableLabels()
    {
        var connectionId = new ConnectionId(
            new ServerId(new ClusterId(), new DnsEndPoint("localhost", 27017)));
        var labeled = new MongoException("retryable");
        labeled.AddErrorLabel("RetryableWriteError");
        Exception[] transient =
        [
            new MongoConnectionException(connectionId, "connection"),
            new MongoExecutionTimeoutException(connectionId, "timeout"),
            new MongoNotPrimaryException(connectionId, new BsonDocument(), new BsonDocument()),
            new MongoNodeIsRecoveringException(connectionId, new BsonDocument(), new BsonDocument()),
            labeled
        ];

        foreach (var exception in transient)
        {
            Assert.True(MongoDbTransportRetry.IsTransient(exception));
            Assert.True(MongoDbChannelStore.IsTransient(exception));
        }

        Assert.False(MongoDbTransportRetry.IsTransient(new MongoException("permanent")));
        Assert.False(MongoDbChannelStore.IsTransient(new MongoException("permanent")));
    }

    [Fact]
    public async Task MongoDbRetry_RetriesTransientTimeouts()
    {
        var attempts = 0;

        var result = await MongoDbTransportRetry.ExecuteAsync(
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
    public async Task MongoDbRetry_DoesNotRetryCancellation()
    {
        var attempts = 0;

        Task<int> Action(CancellationToken _)
        {
            attempts++;
            throw new OperationCanceledException();
        }

        await Assert.ThrowsAsync<OperationCanceledException>(() => MongoDbTransportRetry.ExecuteAsync(
            Action,
            maxAttempts: 3,
            baseDelay: TimeSpan.FromMilliseconds(1),
            maxDelay: TimeSpan.FromMilliseconds(1),
            CancellationToken.None));

        Assert.Equal(1, attempts);
    }

    [Fact]
    public void DeadLetterId_IsDeterministicPerSourceAndDistinctAcrossSources()
    {
        var source = Guid.NewGuid();

        // Idempotent dead-lettering depends on the derived id being stable: a crash between the DLQ
        // insert and the original delete redelivers the message, and the second dead-letter must
        // collide (duplicate key) instead of duplicating the DLQ entry.
        Assert.Equal(MongoDbTransportStore.DeadLetterId(source), MongoDbTransportStore.DeadLetterId(source));
        Assert.NotEqual(MongoDbTransportStore.DeadLetterId(source), MongoDbTransportStore.DeadLetterId(Guid.NewGuid()));
        Assert.NotEqual(MongoDbTransportStore.DeadLetterId(source), source);
    }

    [Fact]
    public void RegistrationKey_CombinesCorrelationAndRegistrationIds()
    {
        var registrationId = Guid.NewGuid();

        Assert.Equal($"corr:{registrationId:N}", MongoDbChannelStore.RegistrationKey("corr", registrationId));
        Assert.NotEqual(
            MongoDbChannelStore.RegistrationKey("corr", registrationId),
            MongoDbChannelStore.RegistrationKey("corr", Guid.NewGuid()));
    }

    [Fact]
    public async Task RecoveryStateStore_SaveAsync_RejectsNonPositiveTtlBeforeMongo()
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
    public void ChannelOptions_RejectIntervalsBeyondTheirCeilings()
    {
        // "Passes validation, throws mid-operation" is the failure mode these bounds close: a
        // TimeSpan.MaxValue deadline overflowed AFTER the publisher's insert (reporting failure
        // for a possibly delivered response), and an over-timer-ceiling poll/heartbeat interval
        // threw inside its background loop's own retry delay, killing dispatch.
        AssertChannelInvalid(
            options => options.DeliveryConfirmationTimeout = TimeSpan.MaxValue,
            nameof(MongoDbAsyncResponseChannelOptions.DeliveryConfirmationTimeout));
        AssertChannelInvalid(
            options => options.MessageRetention = TimeSpan.MaxValue,
            nameof(MongoDbAsyncResponseChannelOptions.MessageRetention));
        AssertChannelInvalid(
            options => options.SubscriberHeartbeatTimeout = TimeSpan.MaxValue,
            nameof(MongoDbAsyncResponseChannelOptions.SubscriberHeartbeatTimeout));
        AssertChannelInvalid(
            options => options.ListenerPollInterval = TimeSpan.FromDays(60),
            nameof(MongoDbAsyncResponseChannelOptions.ListenerPollInterval));
        AssertChannelInvalid(
            options => options.DeliveryConfirmationPollInterval = TimeSpan.FromDays(60),
            nameof(MongoDbAsyncResponseChannelOptions.DeliveryConfirmationPollInterval));
        AssertChannelInvalid(
            options => options.PublishRetryMaxDelay = TimeSpan.FromDays(60),
            nameof(MongoDbAsyncResponseChannelOptions.PublishRetryMaxDelay));
    }

    private static void AssertChannelInvalid(
        Action<MongoDbAsyncResponseChannelOptions> configure,
        string expectedMessageFragment)
    {
        var options = new MongoDbAsyncResponseChannelOptions();
        configure(options);

        var ex = Assert.Throws<InvalidOperationException>(options.Validate);

        Assert.Contains(expectedMessageFragment, ex.Message, StringComparison.Ordinal);
    }

    private static void AssertTransportInvalid(
        Action<MongoDbAsyncResponseTransportOptions> configure,
        string expectedMessageFragment)
    {
        var options = new MongoDbAsyncResponseTransportOptions();
        configure(options);

        var ex = Assert.Throws<InvalidOperationException>(() => MongoDbTransportOptionsValidator.ValidateCommon(options));

        Assert.Contains(expectedMessageFragment, ex.Message, StringComparison.Ordinal);
    }

    private static MongoDbRecoveryStateStore CreateRecoveryStateStore()
    {
        // MongoClient/GetDatabase are lazy: no connection is opened until a command runs, so these
        // tests can construct the store without a server.
        var database = new MongoClient("mongodb://localhost:27017").GetDatabase("asyncresponse_tests");
        var store = new MongoDbChannelStore(database, Options.Create(new MongoDbAsyncResponseChannelOptions()));
        return new MongoDbRecoveryStateStore(store, NullLogger<MongoDbRecoveryStateStore>.Instance);
    }

    private static RecoveryState? InvokeDeserializeState(
        MongoDbRecoveryStateStore store,
        string json,
        string? correlationId)
        => (RecoveryState?)typeof(MongoDbRecoveryStateStore)
            .GetMethod("DeserializeState", BindingFlags.NonPublic | BindingFlags.Instance)!
            .Invoke(store, [json, correlationId]);
}
