using AsyncResponse.Channels.MongoDB;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Core.Clusters;
using MongoDB.Driver.Core.Connections;
using MongoDB.Driver.Core.Servers;
using Moq;
using System.Linq.Expressions;
using System.Reflection;
using Xunit;

namespace AsyncResponse.Tests;

public sealed class MongoDbChannelCoverageTests
{
    [Fact]
    public async Task Publishers_HandleBlankLostSubscriberAndStoreFailurePaths()
    {
        var fixture = new ChannelFixture();
        var channel = fixture.Channel;
        var raw = (IRawAsyncResponsePublisher)channel;

        await channel.SetResponse(new OperationResult(), " ");
        await raw.SetRawResponseJson("{}", " ", CancellationToken.None);
        await channel.SetException(new InvalidOperationException("blank"), " ");

        await channel.SetResponse(new OperationResult { Status = OperationStatus.Completed }, "lost-response");
        await raw.SetRawResponse(new OperationResult(), "lost-untyped", CancellationToken.None);
        await raw.SetRawResponseJson("""{"Status":2}""", "lost-raw", CancellationToken.None);
        await channel.SetException(new InvalidOperationException("lost-error"), "lost-exception");

        fixture.Subscribers
            .Setup(c => c.CountDocumentsAsync(
                It.IsAny<FilterDefinition<MongoChannelSubscriberDocument>>(),
                It.IsAny<CountOptions>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("count failed"));

        await Assert.ThrowsAsync<InvalidOperationException>(() => channel.SetResponse(new OperationResult(), "failed-response"));
        await Assert.ThrowsAsync<InvalidOperationException>(() => raw.SetRawResponseJson("{}", "failed-raw", CancellationToken.None));
        await Assert.ThrowsAsync<InvalidOperationException>(() => channel.SetException(new InvalidOperationException(), "failed-exception"));
        Assert.Equal(0, await channel.CountActiveSubscribersAsync("failed-count"));
        Assert.Equal(0, await channel.CountActiveSubscribersAsync(" "));
        await Assert.ThrowsAsync<ArgumentNullException>(() => channel.SetException(null!, "corr"));

        await channel.DisposeAsync();
    }

    [Fact]
    public async Task WaiterCreation_CoversValidationArmingCleanupAndDisposedChannel()
    {
        var fixture = new ChannelFixture();

        await Assert.ThrowsAsync<ArgumentNullException>(
            () => fixture.Channel.CreateResponseWaiter<OperationResult>(" "));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            () => fixture.Channel.CreateResponseWaiter<OperationResult>("timeout", timeout: TimeSpan.Zero));
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            fixture.Channel.CreateRecoverableResponseWaiter<DefaultRecoveryPayload>(
                "default-recovery",
                resumeCallback: new ReflectionCallDto
                {
                    ServiceInterfaceFullName = "Service",
                    MethodName = "Resume",
                    Params = []
                }));

        await using (var waiter = await fixture.Channel.CreateRecoverableResponseWaiter<OperationResult>(
            "armed",
            timeout: TimeSpan.FromSeconds(30)))
        {
            Assert.False(waiter.ResponseTask.IsCompleted);
        }

        fixture.RecoveryState
            .Setup(s => s.SaveAsync(
                "save-failure",
                It.IsAny<RecoveryState>(),
                It.IsAny<TimeSpan>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("save failed"));
        await using (var failed = await fixture.Channel.CreateResponseWaiter<OperationResult>(
            "save-failure",
            timeout: TimeSpan.FromSeconds(30)))
        {
            var error = await Assert.ThrowsAsync<InvalidOperationException>(() => failed.ResponseTask);
            Assert.Equal("save failed", error.Message);
        }

        await fixture.Channel.DisposeAsync();
        await Assert.ThrowsAsync<ObjectDisposedException>(
            () => fixture.Channel.CreateResponseWaiter<OperationResult>("disposed"));
    }

    [Fact]
    public async Task Subscription_ProcessesEveryEnvelopeOutcome_AndMaintainsSeenSet()
    {
        var fixture = new ChannelFixture();

        var success = fixture.Subscription(payload => new ValueTask<bool>(payload.Status == OperationStatus.Completed));
        var firstId = Guid.NewGuid();
        Assert.True(Invoke<bool>(success.Instance, "MarkSeen", firstId));
        Assert.True(Invoke<bool>(success.Instance, "HasSeen", firstId));
        Assert.False(Invoke<bool>(success.Instance, "MarkSeen", firstId));
        Invoke(success.Instance, "PruneSeen", DateTimeOffset.UtcNow.AddMinutes(1));
        Assert.False(Invoke<bool>(success.Instance, "HasSeen", firstId));

        await InvokeTaskAsync(success.Instance, "ProcessAsync", Message("""{"SchemaVersion":1,"Success":true,"Payload":{"Status":1,"Message":"progress"}}"""));
        Assert.False(success.Completion.Task.IsCompleted);
        await InvokeTaskAsync(success.Instance, "ProcessAsync", Message("""{"SchemaVersion":1,"Success":true,"Payload":{"Status":2,"Message":"done"}}"""));
        Assert.Equal("done", (await success.Completion.Task).Message);

        var nullEnvelope = fixture.Subscription(_ => new ValueTask<bool>(true));
        await InvokeTaskAsync(nullEnvelope.Instance, "ProcessAsync", Message("null"));
        await Assert.ThrowsAsync<System.Text.Json.JsonException>(() => nullEnvelope.Completion.Task);

        var futureEnvelope = fixture.Subscription(_ => new ValueTask<bool>(true));
        await InvokeTaskAsync(futureEnvelope.Instance, "ProcessAsync", Message("""{"SchemaVersion":999,"Success":true,"Payload":{"Status":2}}"""));
        await Assert.ThrowsAsync<InvalidOperationException>(() => futureEnvelope.Completion.Task);

        var remoteFailure = fixture.Subscription(_ => new ValueTask<bool>(true));
        await InvokeTaskAsync(remoteFailure.Instance, "ProcessAsync", Message("""{"SchemaVersion":1,"Success":false,"ExceptionMessage":"remote boom","ExceptionStackTrace":"remote stack"}"""));
        var remote = await Assert.ThrowsAsync<Exception>(() => remoteFailure.Completion.Task);
        Assert.Equal("remote stack", remote.Data["RemoteStackTrace"]);

        var malformed = fixture.Subscription(_ => new ValueTask<bool>(true));
        await InvokeTaskAsync(malformed.Instance, "ProcessAsync", Message("{not-json"));
        await Assert.ThrowsAsync<System.Text.Json.JsonException>(() => malformed.Completion.Task);

        var dropped = fixture.Subscription(_ => new ValueTask<bool>(true));
        await InvokeValueTaskAsync(dropped.Instance, "DropLocalAsync", CancellationToken.None);
        await InvokeTaskAsync(dropped.Instance, "ProcessAsync", Message("null"));
        Assert.False(dropped.Completion.Task.IsCompleted);

        await fixture.Channel.DisposeAsync();
    }

    [Fact]
    public async Task Subscription_CleanupIsIdempotent_AndContainsStoreFailures()
    {
        var fixture = new ChannelFixture();
        var subscription = fixture.Subscription(_ => new ValueTask<bool>(true));
        var timeoutRegistrationDisposed = 0;
        SetProperty(subscription.Instance, "TimeoutRegistration", () =>
        {
            timeoutRegistrationDisposed++;
            return ValueTask.CompletedTask;
        });
        SetProperty(subscription.Instance, "TimeoutCancellation", new CancellationTokenSource());
        fixture.Subscribers
            .Setup(c => c.DeleteOneAsync(
                It.IsAny<FilterDefinition<MongoChannelSubscriberDocument>>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("cleanup failed"));

        await InvokeValueTaskAsync(subscription.Instance, "CleanupOnceAsync", true);
        await InvokeValueTaskAsync(subscription.Instance, "CleanupOnceAsync", true);

        Assert.Equal(1, timeoutRegistrationDisposed);
        await fixture.Channel.DisposeAsync();
    }

    [Fact]
    public async Task DropAndDispose_RemoveLiveSubscriptions_AndStopListenerTasks()
    {
        var fixture = new ChannelFixture();
        var dropped = fixture.Subscription(_ => new ValueTask<bool>(false));
        AddSubscription(fixture.Channel, "drop", dropped.Instance);
        await fixture.Channel.DropLocalSubscriptionsAsync();

        var disposed = fixture.Subscription(_ => new ValueTask<bool>(false), "dispose");
        AddSubscription(fixture.Channel, "dispose", disposed.Instance);
        var listenerCts = new CancellationTokenSource();
        var listenerTask = Task.Delay(Timeout.InfiniteTimeSpan, listenerCts.Token);
        SetField(fixture.Channel, "_listenerCts", listenerCts);
        SetField(fixture.Channel, "_listenTask", listenerTask);
        SetField(fixture.Channel, "_dispatchTask", listenerTask);
        SetField(fixture.Channel, "_heartbeatTask", listenerTask);

        await fixture.Channel.DisposeAsync();

        Assert.True(listenerTask.IsCanceled);
    }

    [Fact]
    public async Task Dispatch_SkipsSeenAndDroppedSubscriptions_AndMarksMessagesWhoseClaimWasLost()
    {
        var fixture = new ChannelFixture();
        var active = fixture.Subscription(_ => new ValueTask<bool>(false));
        var message = Message("""{"SchemaVersion":1,"Success":true,"Payload":{"Status":1}}""");
        Invoke(active.Instance, "MarkSeen", message.Id);
        await DispatchAsync(fixture.Channel, message, active.Instance);

        var dropped = fixture.Subscription(_ => new ValueTask<bool>(false));
        await InvokeValueTaskAsync(dropped.Instance, "DropLocalAsync", CancellationToken.None);
        await DispatchAsync(fixture.Channel, Message("null"), dropped.Instance);

        var lostClaim = fixture.Subscription(_ => new ValueTask<bool>(false));
        fixture.Messages
            .Setup(c => c.FindOneAndUpdateAsync(
                It.IsAny<FilterDefinition<MongoChannelMessageDocument>>(),
                It.IsAny<UpdateDefinition<MongoChannelMessageDocument>>(),
                It.IsAny<FindOneAndUpdateOptions<MongoChannelMessageDocument, MongoChannelMessageDocument>>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync((MongoChannelMessageDocument)null!);
        var lostMessage = Message("null");
        await DispatchAsync(fixture.Channel, lostMessage, lostClaim.Instance);
        Assert.True(Invoke<bool>(lostClaim.Instance, "HasSeen", lostMessage.Id));

        await fixture.Channel.DisposeAsync();
    }

    [Fact]
    public async Task ChannelStore_ReplacesConflictingTtlIndex()
    {
        var fixture = new ChannelFixture(autoCreateIndexes: true);
        var indexManager = new Mock<IMongoIndexManager<MongoRecoveryStateDocument>>();
        var conflict = MongoCommandException(85);
        Assert.Equal(85, conflict.Code);
        indexManager
            .SetupSequence(i => i.CreateOneAsync(
                It.IsAny<CreateIndexModel<MongoRecoveryStateDocument>>(),
                It.IsAny<CreateOneIndexOptions>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(conflict)
            .ReturnsAsync("replacement")
            .ReturnsAsync("correlation");
        indexManager
            .Setup(i => i.DropOneAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        fixture.Recovery.SetupGet(c => c.Indexes).Returns(indexManager.Object);
        SetupSuccessfulIndexes(fixture.Messages);
        SetupSuccessfulIndexes(fixture.Subscribers);

        await fixture.Store.EnsureCreatedAsync();

        indexManager.Verify(i => i.DropOneAsync(
            It.IsAny<string>(),
            It.IsAny<CancellationToken>()), Times.Once);
        await fixture.Channel.DisposeAsync();
    }

    [Fact]
    public async Task CollectDispatchScopeAsync_CoversAllBranches()
    {
        var fixture = new ChannelFixture(listenerPollInterval: TimeSpan.FromMilliseconds(1));
        var channel = fixture.Channel;

        var collectMethod = typeof(MongoDbAsyncResponseChannel)
            .GetMethod("CollectDispatchScopeAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;

        await Task.Delay(5);
        var resultNull = await (Task<HashSet<string>?>)collectMethod.Invoke(channel, [CancellationToken.None])!;
        Assert.Null(resultNull);

        var signalMethod = typeof(MongoDbAsyncResponseChannel)
            .GetMethod("SignalDispatcher", BindingFlags.Instance | BindingFlags.NonPublic)!;

        signalMethod.Invoke(channel, [null]);
        var resultSweepNull = await (Task<HashSet<string>?>)collectMethod.Invoke(channel, [CancellationToken.None])!;
        Assert.Null(resultSweepNull);

        // The scoped-collect branch is timing-sensitive by design: CollectDispatchScopeAsync races
        // its poll delay (1ms here) against the signal reader, and under parallel-suite load a
        // descheduled thread can let the delay win with signals still buffered (that is benign in
        // production — a null scope means "sweep everything", a superset). It can also leave the
        // previous assertion's null signal buffered, which reads as a full-sweep marker. Retry
        // until a collect observes the targeted scope; the branch under test is still exercised.
        HashSet<string>? resultScope = null;
        for (var attempt = 0; attempt < 20 && resultScope is null; attempt++)
        {
            signalMethod.Invoke(channel, ["corr-id-1"]);
            signalMethod.Invoke(channel, ["corr-id-2"]);
            resultScope = await (Task<HashSet<string>?>)collectMethod.Invoke(channel, [CancellationToken.None])!;
        }

        Assert.NotNull(resultScope);
        Assert.Contains("corr-id-1", resultScope);
        Assert.Contains("corr-id-2", resultScope);
    }

    [Fact]
    public async Task ProcessUnderCapturedContextAsync_HandlesNullContext()
    {
        var fixture = new ChannelFixture();
        var channel = fixture.Channel;

        using (ExecutionContext.SuppressFlow())
        {
            await using var waiter = await channel.CreateResponseWaiter<OperationResult>("null-context-waiter", timeout: TimeSpan.FromSeconds(30));
            var message = Message("""{"SchemaVersion":1,"Success":true,"Payload":{"Status":2,"Message":"processed"}}""");
            
            var subscriptionsField = typeof(MongoDbAsyncResponseChannel)
                .GetField("_subscriptions", BindingFlags.Instance | BindingFlags.NonPublic)!;
            var subscriptionsDict = (System.Collections.IDictionary)subscriptionsField.GetValue(channel)!;
            
            var group = (System.Collections.IDictionary)subscriptionsDict["null-context-waiter"]!;
            object? subscription = null;
            foreach (System.Collections.DictionaryEntry entry in group)
            {
                subscription = entry.Value;
                break;
            }
            Assert.NotNull(subscription);
            
            var processUnderContextAsyncProperty = subscription.GetType().GetProperty("ProcessUnderContextAsync", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)!;
            var processUnderContextAsync = (Func<MongoDbChannelMessage, Task>)processUnderContextAsyncProperty.GetValue(subscription)!;
            
            await processUnderContextAsync(message);
            
            var result = await waiter.ResponseTask;
            Assert.Equal("processed", result.Message);
        }
    }

    [Fact]
    public async Task ProcessUnderCapturedContextAsync_HandlesNonNullContext()
    {
        var fixture = new ChannelFixture();
        var channel = fixture.Channel;

        await using var waiter = await channel.CreateResponseWaiter<OperationResult>("non-null-context-waiter", timeout: TimeSpan.FromSeconds(30));
        var message = Message("""{"SchemaVersion":1,"Success":true,"Payload":{"Status":2,"Message":"processed-non-null"}}""");
        
        var subscriptionsField = typeof(MongoDbAsyncResponseChannel)
            .GetField("_subscriptions", BindingFlags.Instance | BindingFlags.NonPublic)!;
        var subscriptionsDict = (System.Collections.IDictionary)subscriptionsField.GetValue(channel)!;
        
        var group = (System.Collections.IDictionary)subscriptionsDict["non-null-context-waiter"]!;
        object? subscription = null;
        foreach (System.Collections.DictionaryEntry entry in group)
        {
            subscription = entry.Value;
            break;
        }
        Assert.NotNull(subscription);
        
        var processUnderContextAsyncProperty = subscription.GetType().GetProperty("ProcessUnderContextAsync", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)!;
        var processUnderContextAsync = (Func<MongoDbChannelMessage, Task>)processUnderContextAsyncProperty.GetValue(subscription)!;
        
        await processUnderContextAsync(message);
        
        var result = await waiter.ResponseTask;
        Assert.Equal("processed-non-null", result.Message);
    }

    [Fact]
    public async Task DispatchMessageToSubscribers_WakesPendingConfirmation()
    {
        var fixture = new ChannelFixture();
        var channel = fixture.Channel;

        var messageId = Guid.NewGuid();
        var beginConfirmationMethod = typeof(MongoDbAsyncResponseChannel)
            .GetMethod("BeginConfirmation", BindingFlags.Instance | BindingFlags.NonPublic)!;
        
        var confirmation = beginConfirmationMethod.Invoke(channel, [messageId])!;
        
        var deliveredProperty = confirmation.GetType().GetProperty("Delivered")!;
        var deliveredTask = (Task<bool>)deliveredProperty.GetValue(confirmation)!;
        
        Assert.False(deliveredTask.IsCompleted);

        var active = fixture.Subscription(_ => new ValueTask<bool>(true));
        var message = new MongoDbChannelMessage(messageId, "corr", """{"SchemaVersion":1,"Success":true,"Payload":{"Status":2}}""", DateTimeOffset.UtcNow);

        fixture.Messages
            .Setup(c => c.FindOneAndUpdateAsync(
                It.IsAny<FilterDefinition<MongoChannelMessageDocument>>(),
                It.IsAny<UpdateDefinition<MongoChannelMessageDocument>>(),
                It.IsAny<FindOneAndUpdateOptions<MongoChannelMessageDocument, MongoChannelMessageDocument>>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new MongoChannelMessageDocument());

        await DispatchAsync(channel, message, active.Instance);

        Assert.True(deliveredTask.IsCompleted);
        Assert.True(await deliveredTask);
    }

    [Fact]
    public async Task DispatchPendingMessagesAsync_CoversPagination()
    {
        var fixture = new ChannelFixture(pendingMessageBatchSize: 2);
        var channel = fixture.Channel;

        var correlationId = "paginate-corr";
        var subscription = fixture.Subscription(_ => new ValueTask<bool>(true), correlationId);
        AddSubscription(channel, correlationId, subscription.Instance);

        var doc1 = new MongoChannelMessageDocument { Id = Guid.NewGuid(), CorrelationId = correlationId, EnvelopeJson = "{}", CreatedAtUtc = DateTime.UtcNow };
        var doc2 = new MongoChannelMessageDocument { Id = Guid.NewGuid(), CorrelationId = correlationId, EnvelopeJson = "{}", CreatedAtUtc = DateTime.UtcNow };

        var cursorMock1 = new Mock<IAsyncCursor<MongoChannelMessageDocument>>();
        cursorMock1.SetupSequence(c => c.MoveNextAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(true)
            .ReturnsAsync(false);
        cursorMock1.Setup(c => c.Current)
            .Returns([doc1, doc2]);

        var cursorMock2 = new Mock<IAsyncCursor<MongoChannelMessageDocument>>();
        cursorMock2.SetupSequence(c => c.MoveNextAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(true)
            .ReturnsAsync(false);
        cursorMock2.Setup(c => c.Current)
            .Returns([]);

        fixture.Messages
            .SetupSequence(c => c.FindAsync(
                It.IsAny<FilterDefinition<MongoChannelMessageDocument>>(),
                It.IsAny<FindOptions<MongoChannelMessageDocument, MongoChannelMessageDocument>>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(cursorMock1.Object)
            .ReturnsAsync(cursorMock2.Object);

        fixture.Messages
            .Setup(c => c.FindOneAndUpdateAsync(
                It.IsAny<FilterDefinition<MongoChannelMessageDocument>>(),
                It.IsAny<UpdateDefinition<MongoChannelMessageDocument>>(),
                It.IsAny<FindOneAndUpdateOptions<MongoChannelMessageDocument, MongoChannelMessageDocument>>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(doc1);

        var dispatchMethod = typeof(MongoDbAsyncResponseChannel)
            .GetMethod("DispatchPendingMessagesAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;

        await (Task)dispatchMethod.Invoke(channel, [null, CancellationToken.None])!;

        fixture.Messages.Verify(c => c.FindAsync(
            It.IsAny<FilterDefinition<MongoChannelMessageDocument>>(),
            It.IsAny<FindOptions<MongoChannelMessageDocument, MongoChannelMessageDocument>>(),
            It.IsAny<CancellationToken>()), Times.Exactly(2));
    }

    [Fact]
    public async Task TryConfirmDeliveryAsync_HandlesTimeoutAndAcks()
    {
        var fixture = new ChannelFixture();
        var channel = fixture.Channel;

        var messageId = Guid.NewGuid();
        var beginConfirmationMethod = typeof(MongoDbAsyncResponseChannel)
            .GetMethod("BeginConfirmation", BindingFlags.Instance | BindingFlags.NonPublic)!;
        
        var confirmation = beginConfirmationMethod.Invoke(channel, [messageId])!;

        var anonymousType = typeof(MongoDbChannelStore).Assembly.GetTypes()
            .First(t => t.Name.StartsWith("<>f__AnonymousType") && t.GetProperties().Any(p => p.Name == "AckedAtUtc"));
        var closedAnonymousType = anonymousType.MakeGenericType([typeof(DateTime?)]);

        var acknowledgeAttempts = 0;
        SetupFindAsync(fixture.Messages, closedAnonymousType, () =>
        {
            var listType = typeof(List<>).MakeGenericType(closedAnonymousType);
            var list = Activator.CreateInstance(listType)!;

            acknowledgeAttempts++;
            if (acknowledgeAttempts == 1)
            {
                var anonymousInstance = Activator.CreateInstance(closedAnonymousType, [(DateTime?)DateTime.UtcNow]);
                listType.GetMethod("Add")!.Invoke(list, [anonymousInstance]);
            }
            return list;
        });

        var tryConfirmMethod = typeof(MongoDbAsyncResponseChannel)
            .GetMethod("TryConfirmDeliveryAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;

        await Task.Delay(10);
        var confirmed = await (Task<bool>)tryConfirmMethod.Invoke(channel, [confirmation, CancellationToken.None])!;
        Assert.True(confirmed);

        var messageId2 = Guid.NewGuid();
        var confirmation2 = beginConfirmationMethod.Invoke(channel, [messageId2])!;

        fixture.Messages
            .Setup(c => c.FindOneAndUpdateAsync(
                It.IsAny<FilterDefinition<MongoChannelMessageDocument>>(),
                It.IsAny<UpdateDefinition<MongoChannelMessageDocument>>(),
                It.IsAny<FindOneAndUpdateOptions<MongoChannelMessageDocument, MongoChannelMessageDocument>>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new MongoChannelMessageDocument());

        var confirmed2 = await (Task<bool>)tryConfirmMethod.Invoke(channel, [confirmation2, CancellationToken.None])!;
        Assert.False(confirmed2);
    }

    [Fact]
    public async Task EnsureCreatedAsync_CoversDoubleCheckedLock()
    {
        var fixture = new ChannelFixture(autoCreateIndexes: true);
        var store = fixture.Store;

        var recoveryIndexes = new Mock<IMongoIndexManager<MongoRecoveryStateDocument>>();
        var tcs = new TaskCompletionSource();
        recoveryIndexes
            .Setup(i => i.CreateOneAsync(
                It.IsAny<CreateIndexModel<MongoRecoveryStateDocument>>(),
                It.IsAny<CreateOneIndexOptions>(),
                It.IsAny<CancellationToken>()))
            .Returns(async () =>
            {
                await tcs.Task;
                return "index";
            });
        fixture.Recovery.SetupGet(c => c.Indexes).Returns(recoveryIndexes.Object);

        SetupSuccessfulIndexes(fixture.Messages);
        SetupSuccessfulIndexes(fixture.Subscribers);

        var task1 = store.EnsureCreatedAsync();
        var task2 = store.EnsureCreatedAsync();

        tcs.SetResult();

        await Task.WhenAll(task1, task2);
    }

    [Fact]
    public async Task HeartbeatSubscribersAsync_ReturnsEarlyForEmptyCollection()
    {
        var fixture = new ChannelFixture();
        var store = fixture.Store;

        await store.HeartbeatSubscribersAsync("instance", [], TimeSpan.FromSeconds(30), CancellationToken.None);
        fixture.Subscribers.Verify(
            s => s.UpdateManyAsync(
                It.IsAny<FilterDefinition<MongoChannelSubscriberDocument>>(),
                It.IsAny<UpdateDefinition<MongoChannelSubscriberDocument>>(),
                It.IsAny<UpdateOptions>(),
                It.IsAny<CancellationToken>()),
            Times.Never);
    }

    private sealed class ChannelFixture
    {
        public ChannelFixture(bool autoCreateIndexes = false, TimeSpan? listenerPollInterval = null, int pendingMessageBatchSize = 64)
        {
            var options = Options.Create(new MongoDbAsyncResponseChannelOptions
            {
                AutoCreateIndexes = autoCreateIndexes,
                UseChangeStreams = false,
                ListenerPollInterval = listenerPollInterval ?? TimeSpan.FromHours(1),
                PendingMessageBatchSize = pendingMessageBatchSize,
                DeliveryConfirmationTimeout = TimeSpan.FromMilliseconds(2),
                DeliveryConfirmationPollInterval = TimeSpan.FromMilliseconds(1)
            });
            Database
                .Setup(d => d.GetCollection<MongoRecoveryStateDocument>(
                    It.IsAny<string>(),
                    It.IsAny<MongoCollectionSettings>()))
                .Returns(Recovery.Object);
            Database
                .Setup(d => d.GetCollection<MongoChannelMessageDocument>(
                    It.IsAny<string>(),
                    It.IsAny<MongoCollectionSettings>()))
                .Returns(Messages.Object);
            Database
                .Setup(d => d.GetCollection<MongoChannelSubscriberDocument>(
                    It.IsAny<string>(),
                    It.IsAny<MongoCollectionSettings>()))
                .Returns(Subscribers.Object);
            Database
                .Setup(d => d.RunCommandAsync(
                    It.IsAny<Command<BsonDocument>>(),
                    It.IsAny<ReadPreference>(),
                    It.IsAny<CancellationToken>()))
                .ReturnsAsync(new BsonDocument("localTime", new BsonDateTime(DateTime.UtcNow)));
            Subscribers
                .Setup(c => c.CountDocumentsAsync(
                    It.IsAny<FilterDefinition<MongoChannelSubscriberDocument>>(),
                    It.IsAny<CountOptions>(),
                    It.IsAny<CancellationToken>()))
                .ReturnsAsync(0);
            Subscribers
                .Setup(c => c.DeleteOneAsync(
                    It.IsAny<FilterDefinition<MongoChannelSubscriberDocument>>(),
                    It.IsAny<CancellationToken>()))
                .ReturnsAsync(new DeleteResult.Acknowledged(1));
            Subscribers
                .Setup(c => c.ReplaceOneAsync(
                    It.IsAny<FilterDefinition<MongoChannelSubscriberDocument>>(),
                    It.IsAny<MongoChannelSubscriberDocument>(),
                    It.IsAny<ReplaceOptions>(),
                    It.IsAny<CancellationToken>()))
                .ReturnsAsync(new ReplaceOneResult.Acknowledged(1, 1, BsonNull.Value));
            RecoveryState
                .Setup(s => s.SaveAsync(
                    It.IsAny<string>(),
                    It.IsAny<RecoveryState>(),
                    It.IsAny<TimeSpan>(),
                    It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);
            RecoveryState
                .Setup(s => s.GetAllAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync([]);
            RecoveryState
                .Setup(s => s.TryDeleteAsync(It.IsAny<string>(), It.IsAny<Guid>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(true);
            Store = new MongoDbChannelStore(Database.Object, options);
            var provider = new ServiceCollection().BuildServiceProvider();
            var mockLogger = new Mock<Microsoft.Extensions.Logging.ILogger<MongoDbAsyncResponseChannel>>();
            mockLogger.Setup(l => l.IsEnabled(Microsoft.Extensions.Logging.LogLevel.Debug)).Returns(true);
            Channel = new MongoDbAsyncResponseChannel(
                provider.GetRequiredService<IServiceScopeFactory>(),
                Store,
                RecoveryState.Object,
                options,
                new AsyncResponseContextPropagation([]),
                mockLogger.Object);
        }

        public Mock<IMongoDatabase> Database { get; } = new(MockBehavior.Loose);
        public Mock<IMongoCollection<MongoRecoveryStateDocument>> Recovery { get; } = new(MockBehavior.Loose);
        public Mock<IMongoCollection<MongoChannelMessageDocument>> Messages { get; } = new(MockBehavior.Loose);
        public Mock<IMongoCollection<MongoChannelSubscriberDocument>> Subscribers { get; } = new(MockBehavior.Loose);
        public Mock<IRecoveryStateStore> RecoveryState { get; } = new();
        public MongoDbChannelStore Store { get; }
        public MongoDbAsyncResponseChannel Channel { get; }

        public (object Instance, TaskCompletionSource<OperationResult> Completion) Subscription(
            Func<OperationResult, ValueTask<bool>> predicate,
            string correlationId = "corr")
        {
            var completion = new TaskCompletionSource<OperationResult>(TaskCreationOptions.RunContinuationsAsynchronously);
            var nested = typeof(MongoDbAsyncResponseChannel)
                .GetNestedType("MongoDbSubscription`1", BindingFlags.NonPublic)!
                .MakeGenericType(typeof(OperationResult));
            var instance = Activator.CreateInstance(
                nested,
                BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic,
                binder: null,
                [Channel, correlationId, Guid.NewGuid(), DateTimeOffset.UtcNow, predicate, completion, null, null],
                culture: null)!;
            return (instance, completion);
        }
    }

    private sealed class DefaultRecoveryPayload : IAsyncResponsePayload;

    private static MongoDbChannelMessage Message(string json)
        => new(Guid.NewGuid(), "corr", json, DateTimeOffset.UtcNow);

    private static void AddSubscription(MongoDbAsyncResponseChannel channel, string correlationId, object subscription)
        => typeof(MongoDbAsyncResponseChannel)
            .GetMethod("AddSubscription", BindingFlags.Instance | BindingFlags.NonPublic)!
            .Invoke(channel, [correlationId, subscription]);

    private static async Task DispatchAsync(MongoDbAsyncResponseChannel channel, MongoDbChannelMessage message, params object[] subscriptions)
    {
        var interfaceType = typeof(MongoDbAsyncResponseChannel).GetNestedType("IMongoDbSubscription", BindingFlags.NonPublic)!;
        var array = Array.CreateInstance(interfaceType, subscriptions.Length);
        for (var index = 0; index < subscriptions.Length; index++)
            array.SetValue(subscriptions[index], index);
        await (Task)typeof(MongoDbAsyncResponseChannel)
            .GetMethod("DispatchMessageToSubscribersAsync", BindingFlags.Instance | BindingFlags.NonPublic)!
            .Invoke(channel, [message, array, CancellationToken.None])!;
    }

    private static T Invoke<T>(object target, string method, params object?[] arguments)
        => (T)target.GetType().GetMethod(method)!.Invoke(target, arguments)!;

    private static void Invoke(object target, string method, params object?[] arguments)
        => target.GetType().GetMethod(method)!.Invoke(target, arguments);

    private static Task InvokeTaskAsync(object target, string method, params object?[] arguments)
        => (Task)target.GetType().GetMethod(method)!.Invoke(target, arguments)!;

    private static ValueTask InvokeValueTask(object target, string method, params object?[] arguments)
        => (ValueTask)target.GetType().GetMethod(method)!.Invoke(target, arguments)!;

    private static async Task InvokeValueTaskAsync(object target, string method, params object?[] arguments)
        => await InvokeValueTask(target, method, arguments);

    private static void SetProperty(object target, string name, object value)
        => target.GetType().GetProperty(name)!.SetValue(target, value);

    private static void SetField(object target, string name, object value)
        => target.GetType().GetField(name, BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(target, value);

    private static MongoCommandException MongoCommandException(int code)
    {
        var response = new BsonDocument
        {
            ["ok"] = 0,
            ["code"] = code,
            ["errmsg"] = "index conflict"
        };
        return new MongoCommandException(
            new ConnectionId(new ServerId(new ClusterId(), new System.Net.DnsEndPoint("localhost", 27017))),
            "createIndexes failed",
            new BsonDocument("createIndexes", "collection"),
            response);
    }

    private static void SetupSuccessfulIndexes<TDocument>(Mock<IMongoCollection<TDocument>> collection)
    {
        var indexes = new Mock<IMongoIndexManager<TDocument>>();
        indexes
            .Setup(i => i.CreateOneAsync(
                It.IsAny<CreateIndexModel<TDocument>>(),
                It.IsAny<CreateOneIndexOptions>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync("index");
        collection.SetupGet(c => c.Indexes).Returns(indexes.Object);
    }

    private class DummyCursor<T> : IAsyncCursor<T>
    {
        private readonly List<T> _items;
        private bool _moved;

        public DummyCursor(List<T> items)
        {
            _items = items;
        }

        public IEnumerable<T> Current => _items;

        public bool MoveNext(CancellationToken cancellationToken = default)
        {
            if (_moved) return false;
            _moved = true;
            return true;
        }

        public async Task<bool> MoveNextAsync(CancellationToken cancellationToken = default)
        {
            if (_moved) return false;
            _moved = true;
            return await Task.FromResult(true);
        }

        public void Dispose() {}
    }

    [Fact]
    public void ServiceCollectionExtensions_ThrowsWhenNoDatabaseOrClientOrConnectionString()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddAsyncResponse().WithMongoDbChannel(options =>
        {
            options.DatabaseName = "test-db";
            options.ConnectionString = ""; // Empty
        });

        var provider = services.BuildServiceProvider();
        Assert.Throws<InvalidOperationException>(() => provider.GetRequiredService<MongoDbChannelStore>());
    }

    [Fact]
    public async Task DispatchMessageToSubscribers_CoversDroppedOrSeen()
    {
        var fixture = new ChannelFixture();
        var channel = fixture.Channel;

        var subscription = fixture.Subscription(_ => new ValueTask<bool>(true));
        
        // 1. Mark as dropped
        SetField(subscription.Instance, "_dropped", true);
        
        var message = Message("null");
        await DispatchAsync(channel, message, subscription.Instance);

        // 2. Mark as not dropped, but make MarkSeen return false (by marking it seen manually first)
        SetField(subscription.Instance, "_dropped", false);
        Invoke(subscription.Instance, "MarkSeen", message.Id);
        
        await DispatchAsync(channel, message, subscription.Instance);
    }

    [Fact]
    public async Task DispatchPendingMessagesAsync_CoversScopeFiltering()
    {
        var fixture = new ChannelFixture();
        var channel = fixture.Channel;

        var subscription = fixture.Subscription(_ => new ValueTask<bool>(true), "corr");
        AddSubscription(channel, "corr", subscription.Instance);

        var dispatchMethod = typeof(MongoDbAsyncResponseChannel)
            .GetMethod("DispatchPendingMessagesAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;

        var scope = new HashSet<string> { "other-corr" };
        await (Task)dispatchMethod.Invoke(channel, [scope, CancellationToken.None])!;
    }

    [Fact]
    public async Task MongoDbRecoveryStateStore_ThrowsOnMismatchedCorrelationId()
    {
        var fixture = new ChannelFixture();
        var recoveryStore = new MongoDbRecoveryStateStore(fixture.Store, NullLogger<MongoDbRecoveryStateStore>.Instance);

        var state = new RecoveryState
        {
            CorrelationId = "corr-1",
            RegistrationId = Guid.NewGuid(),
            SchemaVersion = RecoveryStateSchema.Current
        };

        await Assert.ThrowsAsync<ArgumentException>(() => recoveryStore.SaveAsync("corr-2", state, TimeSpan.FromMinutes(5)));
    }

    [Fact]
    public async Task WatchMessagesAsync_ExitsOnEmptyCursor()
    {
        var fixture = new ChannelFixture();
        var store = fixture.Store;

        var cursorMock = new Mock<IChangeStreamCursor<ChangeStreamDocument<MongoChannelMessageDocument>>>();
        cursorMock.SetupSequence(c => c.MoveNextAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(false);

        fixture.Messages
            .Setup(c => c.WatchAsync(
                It.IsAny<PipelineDefinition<ChangeStreamDocument<MongoChannelMessageDocument>, ChangeStreamDocument<MongoChannelMessageDocument>>>(),
                It.IsAny<ChangeStreamOptions>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(cursorMock.Object);

        var called = false;
        await store.WatchMessagesAsync(_ => { called = true; return Task.CompletedTask; }, CancellationToken.None);
        Assert.False(called);
    }

    private static void SetupFindAsync(
        Mock<IMongoCollection<MongoChannelMessageDocument>> messagesMock,
        Type projectionType,
        Func<object> listFactory)
    {
        var filterExpr = Expression.Call(
            typeof(It),
            "IsAny",
            [typeof(FilterDefinition<MongoChannelMessageDocument>)]);
            
        var findOptionsType = typeof(FindOptions<,>).MakeGenericType(typeof(MongoChannelMessageDocument), projectionType);
        var optionsExpr = Expression.Call(
            typeof(It),
            "IsAny",
            [findOptionsType]);
            
        var tokenExpr = Expression.Call(
            typeof(It),
            "IsAny",
            [typeof(CancellationToken)]);
            
        var param = Expression.Parameter(typeof(IMongoCollection<MongoChannelMessageDocument>), "c");
        
        var findAsyncMethod = typeof(IMongoCollection<MongoChannelMessageDocument>)
            .GetMethods()
            .First(m => m.Name == "FindAsync" && m.GetGenericArguments().Length == 1)
            .MakeGenericMethod(projectionType);
            
        var call = Expression.Call(param, findAsyncMethod, filterExpr, optionsExpr, tokenExpr);
        
        var lambdaType = typeof(Func<,>).MakeGenericType(typeof(IMongoCollection<MongoChannelMessageDocument>), findAsyncMethod.ReturnType);
        var lambda = Expression.Lambda(lambdaType, call, param);
        
        var mockType = typeof(Mock<IMongoCollection<MongoChannelMessageDocument>>);
        var setupMethod = mockType.GetMethods()
            .First(m => m.Name == "Setup" && m.IsGenericMethod && m.GetParameters()[0].ParameterType.GetGenericTypeDefinition() == typeof(System.Linq.Expressions.Expression<>))
            .MakeGenericMethod(findAsyncMethod.ReturnType);
            
        var setupPhrase = setupMethod.Invoke(messagesMock, [lambda])!;
        
        var returnsDelegate = CreateReturnsDelegate(projectionType, listFactory);
        
        var returnsMethod = setupPhrase.GetType().GetMethod("Returns", [typeof(Delegate)])!;
        returnsMethod.Invoke(setupPhrase, [returnsDelegate]);
    }

    private static Delegate CreateReturnsDelegate(Type projectionType, Func<object> listFactory)
    {
        var helper = typeof(MongoDbChannelCoverageTests)
            .GetMethod(nameof(ReturnsDelegateHelper), BindingFlags.Static | BindingFlags.NonPublic)!
            .MakeGenericMethod(projectionType);
            
        return (Delegate)helper.Invoke(null, [listFactory])!;
    }

    private static Delegate ReturnsDelegateHelper<TProjection>(Func<object> listFactory)
    {
        Func<FilterDefinition<MongoChannelMessageDocument>, object, CancellationToken, Task<IAsyncCursor<TProjection>>> del = 
            (filter, options, ct) =>
            {
                var list = (List<TProjection>)listFactory();
                var cursor = new DummyCursor<TProjection>(list);
                return Task.FromResult<IAsyncCursor<TProjection>>(cursor);
            };
        return del;
    }
}
