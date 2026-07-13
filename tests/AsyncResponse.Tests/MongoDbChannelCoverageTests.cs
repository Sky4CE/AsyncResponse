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
using System.Diagnostics;
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

    private sealed class ChannelFixture
    {
        public ChannelFixture(bool autoCreateIndexes = false)
        {
            var options = Options.Create(new MongoDbAsyncResponseChannelOptions
            {
                AutoCreateIndexes = autoCreateIndexes,
                UseChangeStreams = false,
                ListenerPollInterval = TimeSpan.FromHours(1),
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
            Channel = new MongoDbAsyncResponseChannel(
                provider.GetRequiredService<IServiceScopeFactory>(),
                Store,
                RecoveryState.Object,
                options,
                new AsyncResponseContextPropagation([]),
                NullLogger<MongoDbAsyncResponseChannel>.Instance);
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
}
