using System.Reflection;
using AsyncResponse.Channels.MongoDB;
using AsyncResponse.Sample;
using AsyncResponse.Transports.MongoDB;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;
using System.Text.Json;
using Xunit;

namespace AsyncResponse.IntegrationTests;

/// <summary>
/// Drives the MongoDB channel store, transport store, and the channel itself directly against the
/// real single-node replica set — the store contracts (TTL-filtered recovery state, atomic
/// findOneAndUpdate claims, deterministic dead-letter ids) and the change-stream wake, including
/// cross-instance delivery where the publisher and the waiter live in different providers.
/// </summary>
[Collection(IntegrationCollection.Name)]
public sealed class MongoDbDirectIntegrationTests(IntegrationFixture fixture) : IntegrationTestBase(fixture), IDisposable
{
    private readonly MongoClient _client = new(fixture.MongoDbConnectionString);
    private readonly List<string> _databases = [];

    [Fact]
    public async Task ChannelStore_RoundTripsRecoveryStateSubscribersMessagesAndClaims()
    {
        var (database, options) = NewChannelDatabase("channel-store");
        using var store = new MongoDbChannelStore(database, Options.Create(options));
        var recovery = new MongoDbRecoveryStateStore(store, NullLogger<MongoDbRecoveryStateStore>.Instance);
        await store.EnsureCreatedAsync();

        // Recovery state: save/read/scan/delete with per-registration keys.
        var correlationId = NewId("direct-recovery");
        var state = new RecoveryState
        {
            CorrelationId = correlationId,
            PayloadTypeFullName = typeof(OperationResult).FullName
        };
        await recovery.SaveAsync(correlationId, state, TimeSpan.FromSeconds(30));
        Assert.NotEqual(Guid.Empty, state.RegistrationId);

        var stored = Assert.Single(await recovery.GetAllAsync(correlationId));
        Assert.Equal(correlationId, stored.CorrelationId);
        Assert.Equal(state.RegistrationId, stored.RegistrationId);

        var scanned = new List<RecoveryState>();
        await foreach (var scannedState in recovery.ScanAsync())
            scanned.Add(scannedState);
        Assert.Contains(scanned, item => item.CorrelationId == correlationId);

        // A newer-schema entry is rejected on read rather than misinterpreted.
        await store.SaveRecoveryStateAsync(
            "future-state",
            new RecoveryState
            {
                CorrelationId = "future-state",
                RegistrationId = Guid.NewGuid(),
                SchemaVersion = RecoveryStateSchema.Current + 1
            },
            TimeSpan.FromSeconds(30),
            CancellationToken.None);
        Assert.Empty(await recovery.GetAllAsync("future-state"));

        // An unreadable persisted document is skipped, not thrown.
        await database.GetCollection<BsonDocument>(options.RecoveryStateCollection).InsertOneAsync(new BsonDocument
        {
            ["_id"] = $"bad-state:{Guid.NewGuid():N}",
            ["correlation_id"] = "bad-state",
            ["registration_id"] = new BsonBinaryData(Guid.NewGuid(), GuidRepresentation.Standard),
            ["state_json"] = "{not-json",
            ["expires_at"] = DateTime.UtcNow.AddSeconds(30),
            ["registered_at"] = DateTime.UtcNow
        });
        Assert.Empty(await recovery.GetAllAsync("bad-state"));

        Assert.False(await recovery.TryDeleteAsync(correlationId, Guid.NewGuid()));
        Assert.True(await recovery.TryDeleteAsync(correlationId, state.RegistrationId));
        Assert.Empty(await recovery.GetAllAsync(correlationId));

        var deleteState = new RecoveryState { CorrelationId = correlationId };
        await recovery.SaveAsync(correlationId, deleteState, TimeSpan.FromSeconds(30));
        Assert.True(await recovery.TryDeleteAsync(correlationId, deleteState.RegistrationId));
        Assert.Empty(await recovery.GetAllAsync(correlationId));

        // Expiry: the read filter hides expired entries even before the TTL monitor reaps them.
        await recovery.SaveAsync("expired-state", new RecoveryState { CorrelationId = "expired-state" }, TimeSpan.FromMilliseconds(1));
        await Task.Delay(40);
        Assert.Empty(await recovery.GetAllAsync("expired-state"));

        // Subscribers: heartbeat upsert/count/delete with server-side expiry filtering.
        var subscriberCorrelation = NewId("direct-subscriber");
        var registrationId = Guid.NewGuid();
        await store.UpsertSubscriberAsync(subscriberCorrelation, registrationId, "instance-1", TimeSpan.FromSeconds(10), CancellationToken.None);
        Assert.Equal(1, await store.CountActiveSubscribersAsync(subscriberCorrelation, CancellationToken.None));
        await store.DeleteSubscriberAsync(subscriberCorrelation, registrationId, CancellationToken.None);
        Assert.Equal(0, await store.CountActiveSubscribersAsync(subscriberCorrelation, CancellationToken.None));

        var heartbeatA = Guid.NewGuid();
        var heartbeatB = Guid.NewGuid();
        var staleHeartbeat = Guid.NewGuid();
        await store.UpsertSubscriberAsync("heartbeat-a", heartbeatA, "heartbeat-instance", TimeSpan.FromMilliseconds(100), CancellationToken.None);
        await store.UpsertSubscriberAsync("heartbeat-b", heartbeatB, "heartbeat-instance", TimeSpan.FromMilliseconds(100), CancellationToken.None);
        await store.UpsertSubscriberAsync("heartbeat-stale", staleHeartbeat, "heartbeat-instance", TimeSpan.FromMilliseconds(100), CancellationToken.None);
        await Task.Delay(50);
        await store.HeartbeatSubscribersAsync("heartbeat-instance", [heartbeatA, heartbeatB], TimeSpan.FromSeconds(2), CancellationToken.None);
        await Task.Delay(100);
        Assert.Equal(1, await store.CountActiveSubscribersAsync("heartbeat-a", CancellationToken.None));
        Assert.Equal(1, await store.CountActiveSubscribersAsync("heartbeat-b", CancellationToken.None));
        Assert.Equal(0, await store.CountActiveSubscribersAsync("heartbeat-stale", CancellationToken.None));

        // Messages: idempotent insert, server-stamped watermark ordering, and claim arbitration.
        var messageCorrelation = NewId("direct-message");
        var since = await store.GetServerTimeUtcAsync(CancellationToken.None);
        Assert.True((DateTimeOffset.UtcNow - since).Duration() < TimeSpan.FromMinutes(5));

        var messageId = Guid.NewGuid();
        await store.InsertMessageAsync(messageId, messageCorrelation, """{"Success":true}""", TimeSpan.FromMinutes(5), CancellationToken.None);
        await store.InsertMessageAsync(messageId, messageCorrelation, """{"Success":true}""", TimeSpan.FromMinutes(5), CancellationToken.None);
        var messages = await store.LoadMessagesAsync(messageCorrelation, since.AddSeconds(-1), 16, null, null, CancellationToken.None);
        var message = Assert.Single(messages);
        Assert.Equal(messageId, message.Id);

        // Live delivery claims the message; recovery must then lose the arbitration.
        Assert.True(await store.TryClaimForDeliveryAsync(messageId, CancellationToken.None));
        Assert.True(await store.IsMessageAcknowledgedAsync(messageId, CancellationToken.None));
        Assert.False(await store.TryClaimForRecoveryAsync(messageId, CancellationToken.None));

        // And the reverse: once recovery owns a message, live delivery must not double-handle it.
        var recoveryMessageId = Guid.NewGuid();
        await store.InsertMessageAsync(recoveryMessageId, messageCorrelation, """{"Success":true}""", TimeSpan.FromMinutes(5), CancellationToken.None);
        Assert.True(await store.TryClaimForRecoveryAsync(recoveryMessageId, CancellationToken.None));
        Assert.False(await store.TryClaimForDeliveryAsync(recoveryMessageId, CancellationToken.None));

        const int pagedCount = 70;
        var pagedCorrelation = NewId("paged-messages");
        for (var index = 0; index < pagedCount; index++)
            await store.InsertMessageAsync(Guid.NewGuid(), pagedCorrelation, """{"Success":true}""", TimeSpan.FromMinutes(5), CancellationToken.None);
        var paged = new List<MongoDbChannelMessage>();
        DateTimeOffset? afterCreatedAtUtc = null;
        Guid? afterId = null;
        while (true)
        {
            var page = await store.LoadMessagesAsync(pagedCorrelation, since.AddSeconds(-1), 16, afterCreatedAtUtc, afterId, CancellationToken.None);
            paged.AddRange(page);
            if (page.Count < 16)
                break;
            afterCreatedAtUtc = page[^1].CreatedAtUtc;
            afterId = page[^1].Id;
        }
        Assert.Equal(pagedCount, paged.Count);
        Assert.Equal(pagedCount, paged.Select(item => item.Id).Distinct().Count());

        // The change stream observes inserts and surfaces the correlation id — the targeted wake.
        using var watchCts = new CancellationTokenSource();
        var observed = new TaskCompletionSource<string?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var watchTask = Task.Run(() => store.WatchMessagesAsync(cid =>
        {
            if (cid == messageCorrelation)
                observed.TrySetResult(cid);
            return Task.CompletedTask;
        }, watchCts.Token));

        await EventuallyAsync(async () =>
        {
            await store.InsertMessageAsync(Guid.NewGuid(), messageCorrelation, """{"Success":true}""", TimeSpan.FromMinutes(5), CancellationToken.None);
            return observed.Task.IsCompleted;
        });
        Assert.Equal(messageCorrelation, await observed.Task.WaitAsync(TimeSpan.FromSeconds(10)));
        await watchCts.CancelAsync();
        await IgnoreCancellationAsync(watchTask);
    }

    [Fact]
    public async Task TransportStore_PublishClaimAckNakDeadLetterAndWake()
    {
        var (database, options) = NewTransportDatabase("transport-store");
        using var store = new MongoDbTransportStore(database, Options.Create(options));
        await store.EnsureCreatedAsync();

        // Idempotent publish: a retried insert with the same id must not duplicate the job.
        var duplicateId = Guid.NewGuid();
        await store.PublishAsync(duplicateId, options.WorkerQueue, """{"job":"first"}""", null, CancellationToken.None);
        await store.PublishAsync(duplicateId, options.WorkerQueue, """{"job":"first"}""", null, CancellationToken.None);
        await store.PublishAsync(
            Guid.NewGuid(),
            options.WorkerQueue,
            """{"job":"second"}""",
            new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase) { [options.CorrelationIdHeader] = "corr-2" },
            CancellationToken.None);

        // Oldest-first claim; a claimed document is invisible to competing consumers until the lease expires.
        var first = await store.TryClaimAsync(options.WorkerQueue, TimeSpan.FromMinutes(5), CancellationToken.None);
        Assert.NotNull(first);
        Assert.Equal(duplicateId, first!.Id);
        Assert.Equal(1, first.Attempt);

        var second = await store.TryClaimAsync(options.WorkerQueue, TimeSpan.FromMinutes(5), CancellationToken.None);
        Assert.NotNull(second);
        Assert.Equal("corr-2", second!.Headers[options.CorrelationIdHeader]);

        Assert.Null(await store.TryClaimAsync(options.WorkerQueue, TimeSpan.FromMinutes(5), CancellationToken.None));

        // Ack deletes; the queue drains.
        await first.AckAsync();
        await second.AckAsync();
        Assert.Null(await store.TryClaimAsync(options.WorkerQueue, TimeSpan.FromMinutes(5), CancellationToken.None));

        // Nak releases the claim after the redelivery delay, incrementing the attempt count.
        var nakId = Guid.NewGuid();
        await store.PublishAsync(nakId, options.WorkerQueue, """{"job":"nak"}""", null, CancellationToken.None);
        var nakClaim = await store.TryClaimAsync(options.WorkerQueue, TimeSpan.FromMinutes(5), CancellationToken.None);
        await nakClaim!.NakAsync(TimeSpan.FromMilliseconds(50));
        var redelivered = await PollAsync(
            () => store.TryClaimAsync(options.WorkerQueue, TimeSpan.FromMinutes(5), CancellationToken.None),
            claim => claim is not null,
            TimeSpan.FromSeconds(10));
        Assert.NotNull(redelivered);
        Assert.Equal(nakId, redelivered!.Id);
        Assert.Equal(2, redelivered.Attempt);

        // Dead-lettering moves the payload to the DLQ queue with reason headers; the deterministic
        // dead-letter id makes a crash-redelivered dead-letter collide instead of duplicating.
        Assert.True(await redelivered.DeadLetterAsync(new InvalidOperationException("poison"), true, CancellationToken.None));
        Assert.Null(await store.TryClaimAsync(options.WorkerQueue, TimeSpan.FromMinutes(5), CancellationToken.None));

        var deadLetter = await store.TryClaimAsync(options.DeadLetterQueue, TimeSpan.FromMinutes(5), CancellationToken.None);
        Assert.NotNull(deadLetter);
        Assert.Equal(MongoDbTransportStore.DeadLetterId(nakId), deadLetter!.Id);
        Assert.Equal("""{"job":"nak"}""", deadLetter.Payload);
        Assert.Equal("poison", deadLetter.Headers["AR-DeadLetter-Reason"]);
        Assert.Equal(options.WorkerQueue, deadLetter.Headers["AR-DeadLetter-Source-Queue"]);

        // Early-ACK background failures dead-letter without touching the (already deleted) original.
        var backgroundId = Guid.NewGuid();
        await store.PublishAsync(backgroundId, options.WorkerQueue, """{"job":"background"}""", null, CancellationToken.None);
        var backgroundClaim = await store.TryClaimAsync(options.WorkerQueue, TimeSpan.FromMinutes(5), CancellationToken.None);
        await backgroundClaim!.AckAsync();
        Assert.True(await backgroundClaim.DeadLetterAsync(new InvalidOperationException("background boom"), false, CancellationToken.None));

        // The queue change stream wakes subscribers on inserts into their queue.
        using var watchCts = new CancellationTokenSource();
        var woken = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var watchTask = Task.Run(() => store.WatchQueueAsync(options.WorkerQueue, () =>
        {
            woken.TrySetResult();
            return Task.CompletedTask;
        }, watchCts.Token));

        await EventuallyAsync(async () =>
        {
            await store.PublishAsync(Guid.NewGuid(), options.WorkerQueue, """{"job":"wake"}""", null, CancellationToken.None);
            return woken.Task.IsCompleted;
        });
        await woken.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await watchCts.CancelAsync();
        await IgnoreCancellationAsync(watchTask);
    }

    [Fact]
    public async Task Channel_DeliversLiveResponses_WithinAndAcrossProviders()
    {
        var databaseName = NewDatabaseName("channel-e2e");
        var waiterProvider = BuildChannelProvider(databaseName);
        var publisherProvider = BuildChannelProvider(databaseName);
        try
        {
            var subscriber = waiterProvider.GetRequiredService<IAsyncResponseSubscriber>();
            var probe = waiterProvider.GetRequiredService<IActiveSubscriberProbe>();
            var localPublisher = waiterProvider.GetRequiredService<IAsyncResponsePublisher>();
            var remotePublisher = publisherProvider.GetRequiredService<IAsyncResponsePublisher>();
            var remoteRawPublisher = publisherProvider.GetRequiredService<IRawAsyncResponsePublisher>();

            // Same-provider delivery: progress messages keep the waiter alive until the predicate completes it.
            var localCorrelation = NewId("mongo-live-local");
            await using (var waiter = await subscriber.CreateResponseWaiter<OperationResult>(
                localCorrelation,
                payload => new ValueTask<bool>(payload.Status == OperationStatus.Completed),
                TimeSpan.FromSeconds(15)))
            {
                Assert.Equal(1, await probe.CountActiveSubscribersAsync(localCorrelation));
                await localPublisher.SetResponse(new OperationResult { Status = OperationStatus.Running, Message = "progress" }, localCorrelation);
                await Task.Delay(100);
                Assert.False(waiter.ResponseTask.IsCompleted);

                await localPublisher.SetResponse(new OperationResult { Status = OperationStatus.Completed, Message = "done" }, localCorrelation);
                var result = await waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(15));
                Assert.Equal("done", result.Message);
            }

            // Cross-provider delivery: the publisher lives in another provider, so the waiter is woken
            // by the change stream and the publisher confirms delivery through the acked_at claim.
            var remoteCorrelation = NewId("mongo-live-remote");
            await using (var waiter = await subscriber.CreateResponseWaiter<OperationResult>(remoteCorrelation, timeout: TimeSpan.FromSeconds(15)))
            {
                await remotePublisher.SetResponse(new OperationResult { Status = OperationStatus.Completed, Message = "remote" }, remoteCorrelation);
                var result = await waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(15));
                Assert.Equal("remote", result.Message);
            }

            // Raw JSON ingress path across providers.
            var rawCorrelation = NewId("mongo-live-raw");
            await using (var waiter = await subscriber.CreateResponseWaiter<OperationResult>(rawCorrelation, timeout: TimeSpan.FromSeconds(15)))
            {
                await remoteRawPublisher.SetRawResponseJson("""{"Status":2,"Message":"raw"}""", rawCorrelation);
                var result = await waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(15));
                Assert.Equal("raw", result.Message);
            }

            // Exceptions fault the waiter with the remote message.
            var exceptionCorrelation = NewId("mongo-live-exception");
            await using (var waiter = await subscriber.CreateResponseWaiter<OperationResult>(exceptionCorrelation, timeout: TimeSpan.FromSeconds(15)))
            {
                await remotePublisher.SetException(new InvalidOperationException("remote boom"), exceptionCorrelation);
                var ex = await Assert.ThrowsAnyAsync<Exception>(() => waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(15)));
                Assert.Equal("remote boom", ex.Message);
            }

            Assert.Equal(0, await probe.CountActiveSubscribersAsync(NewId("mongo-nobody")));
        }
        finally
        {
            await waiterProvider.DisposeAsync();
            await publisherProvider.DisposeAsync();
        }
    }

    [Fact]
    public async Task Channel_PublisherFallsBackWhenAStoredSubscriberNeverAcknowledges()
    {
        var databaseName = NewDatabaseName("channel-unacked");
        var provider = BuildChannelProvider(databaseName, TimeSpan.FromMilliseconds(50));
        try
        {
            var store = provider.GetRequiredService<MongoDbChannelStore>();
            var publisher = provider.GetRequiredService<IAsyncResponsePublisher>();
            var rawPublisher = provider.GetRequiredService<IRawAsyncResponsePublisher>();

            foreach (var correlationId in new[] { "unacked-response", "unacked-raw", "unacked-exception" })
            {
                await store.UpsertSubscriberAsync(
                    correlationId,
                    Guid.NewGuid(),
                    "missing-process",
                    TimeSpan.FromMinutes(1),
                    CancellationToken.None);
            }

            await publisher.SetResponse(
                new OperationResult { Status = OperationStatus.Completed, Message = "unacked" },
                "unacked-response");
            await rawPublisher.SetRawResponseJson(
                """{"Status":2,"Message":"unacked raw"}""",
                "unacked-raw",
                CancellationToken.None);
            await publisher.SetException(new InvalidOperationException("unacked error"), "unacked-exception");
        }
        finally
        {
            await provider.DisposeAsync();
        }
    }

    [Fact]
    public async Task Channel_DispatchesPagedMessagesAndRejectsUnreadableEnvelopes()
    {
        var databaseName = NewDatabaseName("channel-envelope-edges");
        var provider = BuildChannelProvider(
            databaseName,
            TimeSpan.FromMilliseconds(250),
            options =>
            {
                options.UseChangeStreams = false;
                options.PendingMessageBatchSize = 1;
                options.ListenerPollInterval = TimeSpan.FromMilliseconds(25);
            });
        try
        {
            var store = provider.GetRequiredService<MongoDbChannelStore>();
            var subscriber = provider.GetRequiredService<IAsyncResponseSubscriber>();
            var recoverableSubscriber = provider.GetRequiredService<IRecoverableAsyncResponseSubscriber>();

            var pagedCorrelation = NewId("mongo-paged-dispatch");
            await using (var waiter = await recoverableSubscriber.CreateRecoverableResponseWaiter<OperationResult>(
                pagedCorrelation,
                completionPredicate: payload => new ValueTask<bool>(payload.Status == OperationStatus.Completed),
                timeout: TimeSpan.FromSeconds(10)))
            {
                await store.InsertMessageAsync(
                    Guid.NewGuid(),
                    pagedCorrelation,
                    """{"SchemaVersion":1,"Success":true,"Payload":{"Status":1,"Message":"progress one"}}""",
                    TimeSpan.FromMinutes(1),
                    CancellationToken.None);
                await store.InsertMessageAsync(
                    Guid.NewGuid(),
                    pagedCorrelation,
                    """{"SchemaVersion":1,"Success":true,"Payload":{"Status":1,"Message":"progress two"}}""",
                    TimeSpan.FromMinutes(1),
                    CancellationToken.None);
                await store.InsertMessageAsync(
                    Guid.NewGuid(),
                    pagedCorrelation,
                    """{"SchemaVersion":1,"Success":true,"Payload":{"Status":2,"Message":"complete"}}""",
                    TimeSpan.FromMinutes(1),
                    CancellationToken.None);

                Assert.Equal("complete", (await waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(10))).Message);
            }

            await AssertUnreadableEnvelopeAsync(subscriber, store, "null", typeof(JsonException));
            await AssertUnreadableEnvelopeAsync(
                subscriber,
                store,
                """{"SchemaVersion":999,"Success":true,"Payload":{"Status":2}}""",
                typeof(InvalidOperationException));
            await AssertUnreadableEnvelopeAsync(subscriber, store, "{not-json", typeof(JsonException));
            await AssertUnreadableEnvelopeAsync(
                subscriber,
                store,
                """{"SchemaVersion":1,"Success":false,"ExceptionMessage":"remote boom","ExceptionStackTrace":"remote stack"}""",
                typeof(Exception),
                expectedRemoteStack: "remote stack");
        }
        finally
        {
            await provider.DisposeAsync();
        }
    }

    private ServiceProvider BuildChannelProvider(
        string databaseName,
        TimeSpan? deliveryConfirmationTimeout = null,
        Action<MongoDbAsyncResponseChannelOptions>? configure = null)
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddSingleton<IMongoClient>(_ => new MongoClient(Fixture.MongoDbConnectionString));
        services.AddAsyncResponse().WithMongoDbChannel(options =>
        {
            options.DatabaseName = databaseName;
            options.DefaultTimeout = TimeSpan.FromSeconds(15);
            options.RecoveryStateExpiry = TimeSpan.FromSeconds(30);
            options.MessageRetention = TimeSpan.FromSeconds(30);
            options.DeliveryConfirmationTimeout = deliveryConfirmationTimeout ?? TimeSpan.FromSeconds(2);
            options.DeliveryConfirmationPollInterval = TimeSpan.FromMilliseconds(10);
            options.ListenerPollInterval = TimeSpan.FromMilliseconds(50);
            options.SubscriberHeartbeatInterval = TimeSpan.FromMilliseconds(100);
            options.SubscriberHeartbeatTimeout = TimeSpan.FromSeconds(2);
            configure?.Invoke(options);
        });
        return services.BuildServiceProvider();
    }

    private static async Task AssertUnreadableEnvelopeAsync(
        IAsyncResponseSubscriber subscriber,
        MongoDbChannelStore store,
        string envelopeJson,
        Type expectedExceptionType,
        string? expectedRemoteStack = null)
    {
        var correlationId = $"mongo-unreadable-{Guid.NewGuid():N}";
        await using var waiter = await subscriber.CreateResponseWaiter<OperationResult>(
            correlationId,
            timeout: TimeSpan.FromSeconds(10));
        await store.InsertMessageAsync(
            Guid.NewGuid(),
            correlationId,
            envelopeJson,
            TimeSpan.FromMinutes(1),
            CancellationToken.None);

        var exception = await Assert.ThrowsAnyAsync<Exception>(
            () => waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(10)));
        Assert.IsAssignableFrom(expectedExceptionType, exception);
        if (expectedRemoteStack is not null)
            Assert.Equal(expectedRemoteStack, exception.Data["RemoteStackTrace"]);
    }

    private (IMongoDatabase Database, MongoDbAsyncResponseChannelOptions Options) NewChannelDatabase(string prefix)
    {
        var database = _client.GetDatabase(NewDatabaseName(prefix));
        return (database, new MongoDbAsyncResponseChannelOptions());
    }

    private (IMongoDatabase Database, MongoDbAsyncResponseTransportOptions Options) NewTransportDatabase(string prefix)
    {
        var database = _client.GetDatabase(NewDatabaseName(prefix));
        var options = new MongoDbAsyncResponseTransportOptions
        {
            LockTimeout = TimeSpan.FromMinutes(5),
            DeadLetterRetention = TimeSpan.FromMinutes(5)
        };
        return (database, options);
    }

    private string NewDatabaseName(string prefix)
    {
        var name = $"itest_direct_{prefix.Replace('-', '_')}_{Guid.NewGuid():N}"[..40];
        _databases.Add(name);
        return name;
    }

    [Fact]
    public async Task MongoDbAsyncResponseChannel_CoverInternalEdgeCases()
    {
        var (database, options) = NewChannelDatabase("channel_edges");
        options.DeliveryConfirmationTimeout = TimeSpan.FromMilliseconds(10);
        options.DeliveryConfirmationPollInterval = TimeSpan.FromMilliseconds(5);

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(Options.Create(options));
        var store = new MongoDbChannelStore(database, Options.Create(options));
        services.AddSingleton(store);
        services.AddSingleton(MockRecoveryStore());
        services.AddSingleton(new AsyncResponseContextPropagation([]));
        services.AddSingleton<MongoDbAsyncResponseChannel>();

        await using var provider = services.BuildServiceProvider();
        var channel = provider.GetRequiredService<MongoDbAsyncResponseChannel>();
        
        // Cover EnsureCreatedAsync double call (first/second return)
        await store.EnsureCreatedAsync();
        await store.EnsureCreatedAsync();

        var subscription1 = Subscription(typeof(MongoDbAsyncResponseChannel), "MongoDbSubscription`1", channel, _ => new ValueTask<bool>(true));
        var subscription2 = Subscription(typeof(MongoDbAsyncResponseChannel), "MongoDbSubscription`1", channel, _ => new ValueTask<bool>(true));
        var subscription3 = Subscription(typeof(MongoDbAsyncResponseChannel), "MongoDbSubscription`1", channel, _ => new ValueTask<bool>(true));

        SetField(subscription1.Instance, "_dropped", true);

        var addSubMethod = typeof(MongoDbAsyncResponseChannel).GetMethod("AddSubscription", BindingFlags.Instance | BindingFlags.NonPublic)!;
        addSubMethod.Invoke(channel, ["corr", subscription1.Instance]);
        addSubMethod.Invoke(channel, ["corr", subscription2.Instance]);
        addSubMethod.Invoke(channel, ["corr", subscription3.Instance]);

        // 1. Cover DispatchPendingMessagesAsync where subscriptions.Count == 0
        var channelClean = provider.GetRequiredService<MongoDbAsyncResponseChannel>();
        var subsField = typeof(MongoDbAsyncResponseChannel).GetField("_subscriptions", BindingFlags.Instance | BindingFlags.NonPublic)!;
        var subsDict = (System.Collections.IDictionary)subsField.GetValue(channelClean)!;
        subsDict.Clear();

        addSubMethod.Invoke(channelClean, ["corr-dropped-only", subscription1.Instance]);

        var dispatchPendingMethod = typeof(MongoDbAsyncResponseChannel).GetMethod("DispatchPendingMessagesAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;
        var scope = new HashSet<string> { "corr-dropped-only" };
        await (Task)dispatchPendingMethod.Invoke(channelClean, [scope, CancellationToken.None])!;

        // 2. Cover WaitForAcknowledgementAsync break branch and pollDelay branches
        var beginConfirmationMethod = typeof(MongoDbAsyncResponseChannel).GetMethod("BeginConfirmation", BindingFlags.Instance | BindingFlags.NonPublic)!;
        var tryConfirmMethod = typeof(MongoDbAsyncResponseChannel).GetMethod("TryConfirmDeliveryAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;

        var messageId = Guid.NewGuid();
        await store.InsertMessageAsync(messageId, "corr", "{}", TimeSpan.FromMinutes(1), CancellationToken.None);

        var confirmation = beginConfirmationMethod.Invoke(channel, [messageId])!;
        await (Task)tryConfirmMethod.Invoke(channel, [confirmation, CancellationToken.None])!;

        // 3. Cover DispatchMessageToSubscribersAsync continue branch (dropped & seen & live)
        var messageId2 = Guid.NewGuid();
        await store.InsertMessageAsync(messageId2, "corr", "{}", TimeSpan.FromMinutes(1), CancellationToken.None);

        subscription2.Instance.GetType().GetMethod("MarkSeen", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)!
            .Invoke(subscription2.Instance, [messageId2]);

        var message2 = new MongoDbChannelMessage(messageId2, "corr", "{}", DateTimeOffset.UtcNow);
        var dispatchMethod = typeof(MongoDbAsyncResponseChannel).GetMethod("DispatchMessageToSubscribersAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;

        var subInterfaceType = typeof(MongoDbAsyncResponseChannel).GetNestedType("IMongoDbSubscription", BindingFlags.NonPublic)!;
        var subArray = Array.CreateInstance(subInterfaceType, 3);
        subArray.SetValue(subscription1.Instance, 0); // Dropped
        subArray.SetValue(subscription2.Instance, 1); // Already seen
        subArray.SetValue(subscription3.Instance, 2); // Live (covers ProcessUnderCapturedContextAsync)

        await (Task)dispatchMethod.Invoke(channel, [message2, subArray, CancellationToken.None])!;

        // Start listener to cover background task dispose/cancellation
        var ensureListenerStartedMethod = typeof(MongoDbAsyncResponseChannel).GetMethod("EnsureListenerStarted", BindingFlags.Instance | BindingFlags.NonPublic)!;
        ensureListenerStartedMethod.Invoke(channel, null);

        await channel.DisposeAsync();
        await channelClean.DisposeAsync();
    }

    private static (object Instance, TaskCompletionSource<OperationResult> Completion) Subscription(
        Type channelType,
        string nestedTypeName,
        object channel,
        Func<OperationResult, ValueTask<bool>> predicate)
    {
        var completion = new TaskCompletionSource<OperationResult>(TaskCreationOptions.RunContinuationsAsynchronously);
        var type = channelType.GetNestedType(nestedTypeName, BindingFlags.NonPublic)!
            .MakeGenericType(typeof(OperationResult));
        var instance = Activator.CreateInstance(
            type,
            BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic,
            binder: null,
            [channel, "corr", Guid.NewGuid(), DateTimeOffset.UtcNow, predicate, completion, null, null],
            culture: null)!;
        SetField(instance, "_cleanupStarted", 1);
        return (instance, completion);
    }

    private static void SetField(object target, string name, object value)
        => target.GetType().GetField(name, BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(target, value);

    private static IRecoveryStateStore MockRecoveryStore() => new FakeRecoveryStore();

    private sealed class FakeRecoveryStore : IRecoveryStateStore
    {
        public Task SaveAsync(string correlationId, RecoveryState state, TimeSpan ttl, CancellationToken cancellationToken = default) => Task.CompletedTask;
        public Task<IReadOnlyList<RecoveryState>> GetAllAsync(string correlationId, CancellationToken cancellationToken = default) => Task.FromResult<IReadOnlyList<RecoveryState>>([]);
        public Task<bool> TryDeleteAsync(string correlationId, Guid registrationId, CancellationToken cancellationToken = default) => Task.FromResult(true);
    }

    private static async Task EventuallyAsync(Func<Task<bool>> probe)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        while (!await probe())
        {
            cts.Token.ThrowIfCancellationRequested();
            await Task.Delay(200, cts.Token);
        }
    }

    private static async Task IgnoreCancellationAsync(Task task)
    {
        try
        {
            await task;
        }
        catch (OperationCanceledException)
        {
        }
    }

    public void Dispose()
    {
        foreach (var database in _databases)
        {
            try
            {
                _client.DropDatabase(database);
            }
            catch
            {
                // Cleanup is best effort; the container is discarded with the fixture anyway.
            }
        }

        _client.Dispose();
    }
}
