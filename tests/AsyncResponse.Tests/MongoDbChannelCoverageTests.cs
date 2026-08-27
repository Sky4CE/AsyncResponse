using AsyncResponse.Channels.MongoDB;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using System.Collections;
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
        // A failed probe reads as negative "unknown" (watchdog contract); a blank id is
        // definitively zero without touching the store.
        Assert.Equal(-1, await channel.CountActiveSubscribersAsync("failed-count"));
        Assert.Equal(0, await channel.CountActiveSubscribersAsync(" "));
        await Assert.ThrowsAsync<ArgumentNullException>(() => channel.SetException(null!, "corr"));

        await channel.DisposeAsync();
    }

    [Fact]
    public async Task WaiterTimeout_IsArmedOnlyAfterTheSubscriptionIsDiscoverable()
    {
        // Regression (r24): the waiter timeout was armed BEFORE AddSubscription published the
        // subscription. A timer firing in that gap ran cleanup against a map that did not yet
        // hold the entry — the insert then pinned a permanently-dropped subscription (plus its
        // executor registration) that nothing could ever remove, and SQL Server's idle-poll
        // downshift never engaged again. Redis/NATS arm last with a cleanup guard; the DB
        // channels now do too. This test wedges the old gap open deterministically: the
        // registration-path debug log blocks for 200 ms while a 1 ms timeout fires inside it.
        var fixture = new ChannelFixture(logger: new RegistrationBlockingLogger());
        var waiter = await fixture.Channel.CreateResponseWaiter<OperationResult>(
            "corr-arm-last", timeout: TimeSpan.FromMilliseconds(1));
        await Assert.ThrowsAnyAsync<Exception>(() => waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(5)));

        // The timed-out subscription must be fully retired: on the old code the early-armed timer
        // cleaned up before the insert, and the map held a permanently-dropped entry forever.
        var subscriptions = SubscriptionsOf(fixture.Channel);
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(5);
        while (subscriptions.Count > 0)
        {
            Assert.True(DateTime.UtcNow < deadline, "a dropped subscription stayed pinned in the channel's map");
            await Task.Delay(10);
        }

        await fixture.Channel.DisposeAsync();
    }

    private static ICollection SubscriptionsOf(MongoDbAsyncResponseChannel channel)
    {
        for (var type = channel.GetType(); type is not null; type = type.BaseType)
        {
            if (type.GetField("_subscriptions", BindingFlags.NonPublic | BindingFlags.Instance) is { } field)
                return (ICollection)field.GetValue(channel)!;
        }

        throw new InvalidOperationException("_subscriptions field not found on the channel type hierarchy");
    }

    private sealed class RegistrationBlockingLogger : ILogger<MongoDbAsyncResponseChannel>
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            // Wedge open the (old) gap between CancelAfter and AddSubscription: the registration
            // path logs "Waiting for ..." exactly there, and 200 ms dwarfs the 1 ms timeout.
            if (formatter(state, exception).StartsWith("Waiting for", StringComparison.Ordinal))
                Thread.Sleep(200);
        }
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

        // Must throw rather than return a pre-faulted waiter: the builder's contract is that the
        // trigger only runs once the subscription AND recovery state exist, so a registration
        // failure has to surface before any trigger could fire the remote operation.
        var error = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            fixture.Channel.CreateResponseWaiter<OperationResult>(
                "save-failure",
                timeout: TimeSpan.FromSeconds(30)));
        Assert.Equal("save failed", error.Message);
        fixture.RecoveryState.Verify(
            s => s.TryDeleteAsync("save-failure", It.IsAny<Guid>(), It.IsAny<CancellationToken>()),
            Times.Once);

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

    [Theory]
    // Already delivered to the only live subscription — the store deliberately keeps returning it
    // so other processes' waiters can fan out on it.
    [InlineData("seen")]
    // Outside the delivery watermark: acked before this subscription ever existed.
    [InlineData("acked-before-start")]
    public async Task DispatchPendingMessagesAsync_DoesNotEnqueueAMessageNoLiveSubscriptionWouldTake(string reason)
    {
        // Regression (round 29): the sweep enqueued a dispatch work item for EVERY row the store
        // returned and let the item re-check whether anyone wanted it. Because the store keeps
        // returning acked rows, every sweep tick and every targeted signal re-enqueued one item per
        // retained message. A waiter wedged in a slow user Until predicate holds its per-correlation
        // executor, those items pile up against the bounded queue, and the next enqueue parks the
        // PROCESS-WIDE dispatch loop — which walks correlation ids sequentially — so every other
        // waiter stopped receiving and local publishers blocked on the same enqueue.
        var logger = new CollectingLogger();
        var fixture = new ChannelFixture(logger: logger.For<MongoDbAsyncResponseChannel>());
        var channel = fixture.Channel;

        var correlationId = "prefilter-corr";
        var subscription = fixture.Subscription(_ => new ValueTask<bool>(true), correlationId);
        AddSubscription(channel, correlationId, subscription.Instance);

        var document = new MongoChannelMessageDocument
        {
            Id = Guid.NewGuid(),
            CorrelationId = correlationId,
            EnvelopeJson = "{}",
            CreatedAtUtc = DateTime.UtcNow
        };

        if (reason == "seen")
            Invoke(subscription.Instance, "MarkSeen", document.Id);
        else
            document.AckedAtUtc = DateTime.UtcNow.AddMinutes(-5);

        var cursor = new Mock<IAsyncCursor<MongoChannelMessageDocument>>();
        cursor.SetupSequence(c => c.MoveNextAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(true)
            .ReturnsAsync(false);
        cursor.Setup(c => c.Current).Returns([document]);
        fixture.Messages
            .Setup(c => c.FindAsync(
                It.IsAny<FilterDefinition<MongoChannelMessageDocument>>(),
                It.IsAny<FindOptions<MongoChannelMessageDocument, MongoChannelMessageDocument>>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(cursor.Object);

        await (Task)typeof(MongoDbAsyncResponseChannel)
            .GetMethod("DispatchPendingMessagesAsync", BindingFlags.Instance | BindingFlags.NonPublic)!
            .Invoke(channel, [null, CancellationToken.None])!;

        // The executor logs every admitted work item; nothing was admitted.
        Assert.DoesNotContain(
            logger.Messages,
            message => message.Contains("Channel executor enqueued work", StringComparison.Ordinal));
        // ...and the message was never claimed for delivery either.
        fixture.Messages.Verify(
            c => c.FindOneAndUpdateAsync(
                It.IsAny<FilterDefinition<MongoChannelMessageDocument>>(),
                It.IsAny<UpdateDefinition<MongoChannelMessageDocument>>(),
                It.IsAny<FindOneAndUpdateOptions<MongoChannelMessageDocument, MongoChannelMessageDocument>>(),
                It.IsAny<CancellationToken>()),
            Times.Never);
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

        // IsMessageAcknowledgedAsync projects straight to DateTime? (a named projection would
        // lower anonymous types to the trim-unsafe Expression.New overload — see the store), so
        // the mock cursor is a plain typed list: a value on the first attempt means "acked".
        var acknowledgeAttempts = 0;
        SetupFindAsync(fixture.Messages, typeof(DateTime?), () =>
        {
            acknowledgeAttempts++;
            return acknowledgeAttempts == 1
                ? new List<DateTime?> { DateTime.UtcNow }
                : [];
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
            s => s.BulkWriteAsync(
                It.IsAny<IEnumerable<WriteModel<MongoChannelSubscriberDocument>>>(),
                It.IsAny<BulkWriteOptions>(),
                It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task HeartbeatSubscribersAsync_UpsertsEveryLiveRegistration()
    {
        // The heartbeat must be an UPSERT of the process's live registrations: a bare UPDATE
        // no-ops once the TTL reaper deletes the document, leaving a >timeout-stalled waiter
        // permanently invisible to publishers.
        var fixture = new ChannelFixture();
        List<WriteModel<MongoChannelSubscriberDocument>>? writes = null;
        fixture.Subscribers
            .Setup(s => s.BulkWriteAsync(
                It.IsAny<IEnumerable<WriteModel<MongoChannelSubscriberDocument>>>(),
                It.IsAny<BulkWriteOptions>(),
                It.IsAny<CancellationToken>()))
            .Callback<IEnumerable<WriteModel<MongoChannelSubscriberDocument>>, BulkWriteOptions, CancellationToken>(
                (models, _, _) => writes = models.ToList())
            .ReturnsAsync((BulkWriteResult<MongoChannelSubscriberDocument>)null!);
        var registrationA = Guid.NewGuid();
        var registrationB = Guid.NewGuid();

        await fixture.Store.HeartbeatSubscribersAsync(
            "instance-1",
            [("corr-a", registrationA), ("corr-b", registrationB)],
            TimeSpan.FromSeconds(30),
            CancellationToken.None);

        Assert.NotNull(writes);
        Assert.Equal(2, writes!.Count);
        Assert.All(writes, model =>
        {
            var update = Assert.IsType<UpdateOneModel<MongoChannelSubscriberDocument>>(model);
            Assert.True(update.IsUpsert);
        });
    }

    [Fact]
    public async Task Dispatch_SkipsMessagesCreatedBeforeSubscriptionWatermark()
    {
        // Fan-out watermark: the sweep queries with the OLDEST waiter's watermark, so a
        // late-joining waiter on a shared correlation id must not receive (or ack-claim) retained
        // messages created before it registered.
        var fixture = new ChannelFixture();
        var late = fixture.Subscription(_ => new ValueTask<bool>(true));
        var oldMessage = new MongoDbChannelMessage(Guid.NewGuid(), "corr", "null", DateTimeOffset.UtcNow.AddSeconds(-30));

        await DispatchAsync(fixture.Channel, oldMessage, late.Instance);

        Assert.False(late.Completion.Task.IsCompleted);
        Assert.False(Invoke<bool>(late.Instance, "HasSeen", oldMessage.Id));
        fixture.Messages.Verify(
            c => c.FindOneAndUpdateAsync(
                It.IsAny<FilterDefinition<MongoChannelMessageDocument>>(),
                It.IsAny<UpdateDefinition<MongoChannelMessageDocument>>(),
                It.IsAny<FindOneAndUpdateOptions<MongoChannelMessageDocument, MongoChannelMessageDocument>>(),
                It.IsAny<CancellationToken>()),
            Times.Never);

        await fixture.Channel.DisposeAsync();
    }

    [Fact]
    public async Task Dispatch_SkipsMessageAckedInTheSameClockTickAsTheSubscriptionWatermark()
    {
        // Correlation-id reuse: waiter #2 registers so soon after waiter #1's ack that the server
        // clock cannot separate them, so acked_at == started_at exactly. That tie is routine, not
        // measure-zero — server clocks are far coarser than their column precision (SQL Server's
        // SYSUTCDATETIME is datetime2(7) but advances in ~5ms steps; Mongo's $$NOW is 1ms) — and a
        // non-strict watermark hands waiter #2 the response waiter #1 already consumed.
        var fixture = new ChannelFixture();
        var reuse = fixture.Subscription(_ => new ValueTask<bool>(true));
        var startedAt = (DateTimeOffset)reuse.Instance.GetType()
            .GetProperty("StartedAtUtc", BindingFlags.Instance | BindingFlags.Public)!
            .GetValue(reuse.Instance)!;

        // Created inside the 1s creation tolerance, so the ack comparison is the only thing that
        // can exclude it — exactly the state the previous waiter's message is left in.
        var stale = new MongoDbChannelMessage(
            Guid.NewGuid(), "corr", "null", startedAt.AddMilliseconds(-5), startedAt);

        await DispatchAsync(fixture.Channel, stale, reuse.Instance);

        Assert.False(reuse.Completion.Task.IsCompleted);
        Assert.False(Invoke<bool>(reuse.Instance, "HasSeen", stale.Id));

        await fixture.Channel.DisposeAsync();
    }

    [Fact]
    public async Task Dispatch_DeliversMessageAckedAfterTheSubscriptionWatermark()
    {
        // The other side of the strict comparison: an ack stamped after this waiter registered is a
        // live cross-process fan-out delivery, not history, and must still reach it. Pins that the
        // stale-redelivery fix did not simply mute every already-acked message. (At exact equality
        // the sibling test above wins deliberately: a same-tick tie is indistinguishable from
        // history, so even genuine fan-out is excluded there and recovers via the step timeout —
        // see the IsWithinWatermark XML doc for the full trade.)
        var fixture = new ChannelFixture();
        var live = fixture.Subscription(_ => new ValueTask<bool>(true));
        var startedAt = (DateTimeOffset)live.Instance.GetType()
            .GetProperty("StartedAtUtc", BindingFlags.Instance | BindingFlags.Public)!
            .GetValue(live.Instance)!;

        var fanOut = new MongoDbChannelMessage(
            Guid.NewGuid(),
            "corr",
            """{"SchemaVersion":1,"Success":true,"Payload":{"Status":2,"Message":"fan-out"}}""",
            startedAt.AddMilliseconds(-5),
            startedAt.AddMilliseconds(5));

        await DispatchAsync(fixture.Channel, fanOut, live.Instance);

        // The delivery claim is what separates "inside the watermark" from "excluded": the skip
        // path returns before ever touching the collection. Asserting on HasSeen instead would pass
        // either way, because losing the claim also marks the message seen.
        fixture.Messages.Verify(
            c => c.FindOneAndUpdateAsync(
                It.IsAny<FilterDefinition<MongoChannelMessageDocument>>(),
                It.IsAny<UpdateDefinition<MongoChannelMessageDocument>>(),
                It.IsAny<FindOneAndUpdateOptions<MongoChannelMessageDocument, MongoChannelMessageDocument>>(),
                It.IsAny<CancellationToken>()),
            Times.Once);

        await fixture.Channel.DisposeAsync();
    }

    [Fact]
    public async Task WaiterRegistration_WritesSubscriberBeforeRecoveryState()
    {
        // "Recovery state visible ⇒ subscription visible": the lost-subscriber dispatcher's live
        // re-check relies on the subscriber document landing first, so a publisher that sees the
        // state and no subscriber knows the waiter is truly gone (not mid-registration).
        var fixture = new ChannelFixture();
        var order = new List<string>();
        fixture.Subscribers
            .Setup(c => c.UpdateOneAsync(
                It.IsAny<FilterDefinition<MongoChannelSubscriberDocument>>(),
                It.IsAny<UpdateDefinition<MongoChannelSubscriberDocument>>(),
                It.IsAny<UpdateOptions>(),
                It.IsAny<CancellationToken>()))
            .Callback(() => order.Add("subscriber"))
            .ReturnsAsync((UpdateResult)null!);
        fixture.RecoveryState
            .Setup(s => s.SaveAsync(
                "registration-order",
                It.IsAny<RecoveryState>(),
                It.IsAny<TimeSpan>(),
                It.IsAny<CancellationToken>()))
            .Callback(() => order.Add("recovery"))
            .Returns(Task.CompletedTask);

        await using (await fixture.Channel.CreateResponseWaiter<OperationResult>(
            "registration-order",
            timeout: TimeSpan.FromSeconds(30)))
        {
            Assert.Equal(["subscriber", "recovery"], order);
        }

        await fixture.Channel.DisposeAsync();
    }

    // ---------------------------------------------------------------------------------------------
    // Round-26 review regressions in the shared DB channel base. They live here because the fixture
    // above is the only harness whose delivery claim actually succeeds (a mocked findOneAndUpdate),
    // which every one of these paths runs behind.
    // ---------------------------------------------------------------------------------------------

    [Fact]
    public async Task OneThrowingSubscription_DoesNotStrandTheRestOfTheFanOut()
    {
        // Regression (r26): the claim stamps acked_at and trips the publisher's confirmation
        // BEFORE the fan-out loop runs, so the message is already consumed. Pre-fix a throw from
        // one subscription's dispatch propagated straight out of the loop, and every subscription
        // after it never saw a response that IsWithinWatermark now excludes from every later
        // sweep — those waiters hung to their step timeout while the publisher saw success.
        var fixture = new ChannelFixture();
        fixture.Messages
            .Setup(c => c.FindOneAndUpdateAsync(
                It.IsAny<FilterDefinition<MongoChannelMessageDocument>>(),
                It.IsAny<UpdateDefinition<MongoChannelMessageDocument>>(),
                It.IsAny<FindOneAndUpdateOptions<MongoChannelMessageDocument, MongoChannelMessageDocument>>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new MongoChannelMessageDocument());

        var poisoned = fixture.Subscription(_ => new ValueTask<bool>(true));
        var healthy = fixture.Subscription(_ => new ValueTask<bool>(true));

        // Throw from the captured-context wrapper, i.e. OUTSIDE ProcessAsync's own catch — the
        // shape the finding describes (a PushCorrelationId/ExecutionContext.Run fault, or a
        // CleanupOnceAsync throw from CleanupCoreAsync's uncaught finally).
        SetProperty(
            poisoned.Instance,
            "ProcessUnderContextAsync",
            (Func<MongoDbChannelMessage, Task>)(_ => throw new InvalidOperationException("context wrapper exploded")));

        var message = Message("""{"SchemaVersion":1,"Success":true,"Payload":{"Status":2,"Message":"fan-out"}}""");

        // Still reported as a failed dispatch — the fix isolates siblings, it does not swallow.
        var thrown = await Assert.ThrowsAsync<AggregateException>(
            () => DispatchAsync(fixture.Channel, message, poisoned.Instance, healthy.Instance));
        Assert.Contains(thrown.InnerExceptions, ex => ex.Message == "context wrapper exploded");

        // The sibling behind the throw received the already-acked response.
        var delivered = await healthy.Completion.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal("fan-out", delivered.Message);
    }

    [Fact]
    public async Task RecoveryStateRegisteredAt_ComesFromTheServerStampedSubscriptionStart()
    {
        // Regression (r26): RegisteredAtUtc was stamped with DateTime.UtcNow while the delivery
        // watermark on the very next lines was drawn from the store's clock. The watchdog judges
        // staleness as "utcNow - RegisteredAtUtc" from whichever host scans, so a skewed host's
        // registrations either never aged (a stuck flow stayed invisible) or aged instantly
        // (healthy waits paged the operator).
        var serverNow = new DateTime(2031, 5, 4, 3, 2, 1, DateTimeKind.Utc);
        var fixture = new ChannelFixture(serverStampedAt: serverNow);

        RecoveryState? saved = null;
        fixture.RecoveryState
            .Setup(s => s.SaveAsync(It.IsAny<string>(), It.IsAny<RecoveryState>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
            .Callback<string, RecoveryState, TimeSpan, CancellationToken>((_, state, _, _) => saved = state)
            .Returns(Task.CompletedTask);

        await using var waiter = await ((IAsyncResponseSubscriber)fixture.Channel)
            .CreateResponseWaiter<OperationResult>("corr", timeout: TimeSpan.FromMinutes(5));

        Assert.NotNull(saved);
        // Pre-fix this was the app clock (a real "now"), not the distinctive server stamp.
        Assert.Equal(serverNow, saved.RegisteredAtUtc);
    }

    [Fact]
    public async Task WaiterTimeout_FiresOnTheInjectedTimeProvider()
    {
        // Regression (r26): the waiter timeout armed CancellationTokenSource.CancelAfter on a
        // default CTS, which is bound to the system clock. AsyncResponse.Testing's virtual clock
        // therefore could not fire a production-sized timeout on any DB-backed channel — the
        // in-memory channel's ArmTimeout documents that seam as the reason TimeProvider exists.
        var time = new AsyncResponse.Testing.VirtualTimeProvider();
        var fixture = new ChannelFixture(timeProvider: time);

        await using var waiter = await ((IAsyncResponseSubscriber)fixture.Channel)
            .CreateResponseWaiter<OperationResult>("corr", timeout: TimeSpan.FromMinutes(30));

        Assert.False(waiter.ResponseTask.IsCompleted);

        time.Advance(TimeSpan.FromMinutes(31));

        // The MESSAGE is the assertion, not the type: WaitAsync below raises TimeoutException of
        // its own when the task never settles, so a bare ThrowsAsync<TimeoutException> would pass
        // on the pre-fix code for entirely the wrong reason. Only the waiter's own timeout names
        // the correlation id.
        var thrown = await Assert.ThrowsAsync<TimeoutException>(() => waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(5)));
        Assert.Contains("correlationId corr", thrown.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task DisposeAsync_AwaitsExecutorRetirementsStartedByCleanup()
    {
        // Regression (r26): CleanupCoreAsync unlinks the correlation id and THEN fires its
        // executor retirement on the thread pool, so DisposeAsync's own loop never saw it. The
        // host could tear down the logger and exit while that retirement was still inside its
        // 30-second drain budget.
        var fixture = new ChannelFixture();
        fixture.Messages
            .Setup(c => c.FindOneAndUpdateAsync(
                It.IsAny<FilterDefinition<MongoChannelMessageDocument>>(),
                It.IsAny<UpdateDefinition<MongoChannelMessageDocument>>(),
                It.IsAny<FindOneAndUpdateOptions<MongoChannelMessageDocument, MongoChannelMessageDocument>>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new MongoChannelMessageDocument());

        var subscription = fixture.Subscription(_ => new ValueTask<bool>(true));
        AddSubscription(fixture.Channel, "corr", subscription.Instance);

        // A completing delivery runs CleanupCoreAsync, which unlinks the correlation id and then
        // schedules its retirement — so DisposeAsync's own loop over _subscriptions cannot see it.
        await DispatchAsync(
            fixture.Channel,
            Message("""{"SchemaVersion":1,"Success":true,"Payload":{"Status":2}}"""),
            subscription.Instance);
        await subscription.Completion.Task.WaitAsync(TimeSpan.FromSeconds(5));

        var pending = (System.Collections.IDictionary)typeof(MongoDbAsyncResponseChannel)
            .BaseType!.GetField("_pendingRetirements", BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(fixture.Channel)!;

        // Stand in for a retirement still inside its drain budget. Asserting only that the
        // collection ends up EMPTY would be vacuous — it is trivially empty when nothing is
        // tracked at all — so the assertion is that disposal BLOCKS on an outstanding entry.
        var stillDraining = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        pending[stillDraining.Task] = (byte)0;

        var disposal = fixture.Channel.DisposeAsync().AsTask();
        await Task.Delay(100);
        Assert.False(disposal.IsCompleted, "DisposeAsync returned while an executor retirement was still draining");

        stillDraining.TrySetResult();
        await disposal.WaitAsync(TimeSpan.FromSeconds(5));
    }

    private static IMongoCollection<BsonDocument> CountersStampedAt(DateTime drawnAt)
    {
        var counters = new Mock<IMongoCollection<BsonDocument>>(MockBehavior.Loose);
        var seq = 0L;
        counters
            .Setup(c => c.FindOneAndUpdateAsync(
                It.IsAny<FilterDefinition<BsonDocument>>(),
                It.IsAny<UpdateDefinition<BsonDocument>>(),
                It.IsAny<FindOneAndUpdateOptions<BsonDocument, BsonDocument>>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(() => new BsonDocument
            {
                ["_id"] = "ack_seq",
                ["seq"] = Interlocked.Increment(ref seq),
                ["drawn_at"] = new BsonDateTime(drawnAt)
            });
        return counters.Object;
    }

    private sealed class ChannelFixture
    {
        public ChannelFixture(
            bool autoCreateIndexes = false,
            TimeSpan? listenerPollInterval = null,
            int pendingMessageBatchSize = 64,
            ILogger<MongoDbAsyncResponseChannel>? logger = null,
            TimeProvider? timeProvider = null,
            TimeSpan? defaultTimeout = null,
            DateTime? serverStampedAt = null)
        {
            var options = Options.Create(new MongoDbAsyncResponseChannelOptions
            {
                AutoCreateIndexes = autoCreateIndexes,
                UseChangeStreams = false,
                ListenerPollInterval = listenerPollInterval ?? TimeSpan.FromHours(1),
                PendingMessageBatchSize = pendingMessageBatchSize,
                DefaultTimeout = defaultTimeout,
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
            // No hello/localTime fake: the subscription watermark is drawn atomically from the
            // counters collection alone, so a registration that reaches RunCommandAsync again
            // (the old two-round-trip draw) fails loudly on the unmocked call.
            Database.WithCounters();
            // Must land BEFORE the store is constructed: MongoDbChannelStore resolves its
            // collections eagerly, so a later re-setup on the database mock is never seen.
            if (serverStampedAt is { } drawnAt)
            {
                Database
                    .Setup(d => d.GetCollection<BsonDocument>(It.IsAny<string>(), It.IsAny<MongoCollectionSettings>()))
                    .Returns(CountersStampedAt(drawnAt));
            }
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
                logger ?? mockLogger.Object,
                timeProvider);
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
                .BaseType!.GetNestedType("DbSubscription`1", BindingFlags.NonPublic)!
                .MakeGenericType(typeof(OperationResult));
            var instance = Activator.CreateInstance(
                nested,
                BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic,
                binder: null,
                [Channel, correlationId, Guid.NewGuid(), DateTimeOffset.UtcNow, 0L, predicate, completion, null],
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
        var interfaceType = typeof(MongoDbAsyncResponseChannel).BaseType!.GetNestedType("IDbSubscription", BindingFlags.NonPublic)!;
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
    public async Task DispatchPendingMessagesAsync_ScopedScanDeliversThroughTheQueuedWorkItem()
    {
        // End-to-end through the sweep path: the targeted scan finds the signaled id's live
        // waiter, queues the dispatch on the per-correlation serial executor, and the queued work
        // item claims and delivers the message to the waiter.
        var fixture = new ChannelFixture();
        var channel = fixture.Channel;
        var subscription = fixture.Subscription(_ => new ValueTask<bool>(true), "sweep-corr");
        AddSubscription(channel, "sweep-corr", subscription.Instance);

        var document = new MongoChannelMessageDocument
        {
            Id = Guid.NewGuid(),
            CorrelationId = "sweep-corr",
            EnvelopeJson = """{"SchemaVersion":1,"Success":true,"Payload":{"Status":2,"Message":"swept"}}""",
            CreatedAtUtc = DateTime.UtcNow
        };
        fixture.Messages
            .Setup(c => c.FindAsync(
                It.IsAny<FilterDefinition<MongoChannelMessageDocument>>(),
                It.IsAny<FindOptions<MongoChannelMessageDocument, MongoChannelMessageDocument>>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new DummyCursor<MongoChannelMessageDocument>([document]));
        fixture.Messages
            .Setup(c => c.FindOneAndUpdateAsync(
                It.IsAny<FilterDefinition<MongoChannelMessageDocument>>(),
                It.IsAny<UpdateDefinition<MongoChannelMessageDocument>>(),
                It.IsAny<FindOneAndUpdateOptions<MongoChannelMessageDocument, MongoChannelMessageDocument>>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(document);

        var dispatchMethod = typeof(MongoDbAsyncResponseChannel)
            .GetMethod("DispatchPendingMessagesAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;
        await (Task)dispatchMethod.Invoke(channel, [new HashSet<string> { "sweep-corr" }, CancellationToken.None])!;

        var delivered = await subscription.Completion.Task.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.Equal("swept", delivered.Message);
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
