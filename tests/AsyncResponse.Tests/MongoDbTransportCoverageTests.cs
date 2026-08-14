using AsyncResponse.Transports.MongoDB;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;
using Moq;
using System.Reflection;
using Xunit;

namespace AsyncResponse.Tests;

public sealed class MongoDbTransportCoverageTests
{
    [Fact]
    public async Task WorkerTransport_PublishesCorrelatedAndUncorrelatedJobs_AndReportsFailures()
    {
        var documents = new List<MongoTransportMessageDocument>();
        var collection = new Mock<IMongoCollection<MongoTransportMessageDocument>>(MockBehavior.Loose);
        collection
            .Setup(c => c.InsertOneAsync(
                It.IsAny<MongoTransportMessageDocument>(),
                It.IsAny<InsertOneOptions>(),
                It.IsAny<CancellationToken>()))
            .Callback((MongoTransportMessageDocument document, InsertOneOptions _, CancellationToken _) => documents.Add(document))
            .Returns(Task.CompletedTask);
        var options = Options.Create(new MongoDbAsyncResponseTransportOptions
        {
            AutoCreateIndexes = false,
            PublishRetryBaseDelay = TimeSpan.FromMilliseconds(1),
            PublishRetryMaxDelay = TimeSpan.FromMilliseconds(1)
        });
        using var store = CreateStore(collection.Object, options);
        var transport = new MongoDbWorkerTransport(options, store);

        await transport.PublishAsync(Job("corr-mongo"));
        await transport.PublishAsync(Job(null));

        Assert.Equal(2, documents.Count);
        Assert.NotNull(documents[0].Headers);
        Assert.NotNull(documents[1].Headers);
        Assert.Equal("corr-mongo", documents[0].Headers![options.Value.CorrelationIdHeader]);
        Assert.Empty(documents[1].Headers!);
        // Fresh publishes must be claimable regardless of client/server clock skew: the claim
        // filter compares available_at against the server's $$NOW, so inserts stamp epoch.
        Assert.All(documents, d => Assert.Equal(DateTime.UnixEpoch, d.AvailableAtUtc));

        collection
            .Setup(c => c.InsertOneAsync(
                It.IsAny<MongoTransportMessageDocument>(),
                It.IsAny<InsertOneOptions>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("publish failed"));

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => transport.PublishAsync(Job("corr-error")));
        Assert.Equal("publish failed", error.Message);
        await Assert.ThrowsAsync<ArgumentNullException>(() => transport.PublishAsync(null!));
    }

    [Fact]
    public void WorkerTransport_PublicConstructor_UsesTheProvidedDatabase()
    {
        var options = Options.Create(new MongoDbAsyncResponseTransportOptions { AutoCreateIndexes = false });
        var collection = new Mock<IMongoCollection<MongoTransportMessageDocument>>();
        var database = Database(collection.Object);

        Assert.NotNull(new MongoDbWorkerTransport(options, database.Object));
    }

    [Fact]
    public async Task Store_DeadLetterDisabledDeletesOriginal_AndInsertFailureReturnsFalse()
    {
        var message = new MongoTransportMessageDocument
        {
            Id = Guid.NewGuid(),
            Queue = "worker",
            Payload = "{}",
            Headers = new Dictionary<string, string>()
        };
        var collection = new Mock<IMongoCollection<MongoTransportMessageDocument>>(MockBehavior.Loose);
        collection
            .Setup(c => c.FindOneAndUpdateAsync(
                It.IsAny<FilterDefinition<MongoTransportMessageDocument>>(),
                It.IsAny<UpdateDefinition<MongoTransportMessageDocument>>(),
                It.IsAny<FindOneAndUpdateOptions<MongoTransportMessageDocument, MongoTransportMessageDocument>>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(message);
        collection
            .Setup(c => c.DeleteOneAsync(It.IsAny<FilterDefinition<MongoTransportMessageDocument>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new DeleteResult.Acknowledged(1));

        var disabledOptions = Options.Create(new MongoDbAsyncResponseTransportOptions
        {
            AutoCreateIndexes = false,
            DeadLetterEnabled = false
        });
        using (var disabledStore = CreateStore(collection.Object, disabledOptions))
        {
            var delivery = Assert.IsType<MongoDbTransportDelivery>(
                await disabledStore.TryClaimAsync("worker", TimeSpan.FromSeconds(1), CancellationToken.None));
            Assert.True(await delivery.DeadLetterAsync(new InvalidOperationException("ignored"), true, CancellationToken.None));
        }
        collection.Verify(
            c => c.DeleteOneAsync(It.IsAny<FilterDefinition<MongoTransportMessageDocument>>(), It.IsAny<CancellationToken>()),
            Times.Once);

        collection
            .Setup(c => c.InsertOneAsync(
                It.IsAny<MongoTransportMessageDocument>(),
                It.IsAny<InsertOneOptions>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("dlq unavailable"));
        var enabledOptions = Options.Create(new MongoDbAsyncResponseTransportOptions { AutoCreateIndexes = false });
        using var enabledStore = CreateStore(collection.Object, enabledOptions);
        var enabledDelivery = Assert.IsType<MongoDbTransportDelivery>(
            await enabledStore.TryClaimAsync("worker", TimeSpan.FromSeconds(1), CancellationToken.None));

        Assert.False(await enabledDelivery.DeadLetterAsync(
            new InvalidOperationException("poison"),
            false,
            CancellationToken.None));
    }

    [Fact]
    public async Task SubscriberAdapters_ExposeConfiguredRolesAndForwardPayloads()
    {
        var options = Options.Create(new MongoDbAsyncResponseTransportOptions { AutoCreateIndexes = false });
        var collection = new Mock<IMongoCollection<MongoTransportMessageDocument>>(MockBehavior.Loose);
        using var store = CreateStore(collection.Object, options);
        var ingress = new Mock<IAsyncResponseIngress>();
        ingress.Setup(i => i.HandleWorkerMessageAsync("worker-json")).Returns(Task.CompletedTask);
        ingress.Setup(i => i.HandleResponseMessageAsync("response-json", "corr-response")).Returns(Task.CompletedTask);
        var worker = new MongoDbWorkerSubscriber(options, store, ingress.Object, NullLogger<MongoDbWorkerSubscriber>.Instance);
        var response = new MongoDbResponseIngressSubscriber(options, store, ingress.Object, NullLogger<MongoDbResponseIngressSubscriber>.Instance);

        Assert.Equal(options.Value.WorkerQueue, GetProperty<string>(worker, "Queue"));
        Assert.Same(options.Value.WorkerSubscriber, GetProperty<MongoDbSubscriberOptions>(worker, "SubscriberOptions"));
        Assert.Equal(MongoDbSubscriberRole.Worker, GetProperty<MongoDbSubscriberRole>(worker, "Role"));
        await InvokeHandlerAsync(worker, Delivery("worker-json"));

        Assert.Equal(options.Value.ResponseQueue, GetProperty<string>(response, "Queue"));
        Assert.Same(options.Value.ResponseSubscriber, GetProperty<MongoDbSubscriberOptions>(response, "SubscriberOptions"));
        Assert.Equal(MongoDbSubscriberRole.ResponseIngress, GetProperty<MongoDbSubscriberRole>(response, "Role"));
        await InvokeHandlerAsync(response, Delivery(
            "response-json",
            new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                [options.Value.CorrelationIdHeader] = "corr-response"
            }));

        ingress.VerifyAll();
    }

    [Fact]
    public void Registration_UsesSharedClientOrOwnedClient_WhenNoDatabaseIsRegistered()
    {
        var database = new Mock<IMongoDatabase>().WithTestNamespace();
        var client = new Mock<IMongoClient>();
        client.Setup(c => c.GetDatabase("shared_db", It.IsAny<MongoDatabaseSettings>())).Returns(database.Object);
        var sharedServices = Services();
        sharedServices.AddSingleton(client.Object);
        sharedServices.AddAsyncResponse().WithInMemoryChannel().WithMongoDbTransport(options => options.DatabaseName = "shared_db");
        using (var sharedProvider = sharedServices.BuildServiceProvider())
            Assert.NotNull(sharedProvider.GetRequiredService<MongoDbTransportStore>());

        var ownedServices = Services();
        ownedServices.AddAsyncResponse().WithInMemoryChannel().WithMongoDbTransport(options =>
        {
            options.DatabaseName = "owned_db";
            options.ConnectionString = "mongodb://localhost:27017";
        });
        using var ownedProvider = ownedServices.BuildServiceProvider();
        Assert.NotNull(ownedProvider.GetRequiredService<MongoDbTransportStore>());
    }

    private static WorkerJobEnvelope Job(string? correlationId) => new()
    {
        CorrelationId = correlationId,
        Call = new ReflectionCallDto
        {
            ServiceInterfaceFullName = typeof(MongoDbTransportCoverageTests).FullName!,
            MethodName = nameof(Job),
            Params = []
        }
    };

    private static MongoDbTransportDelivery Delivery(
        string payload,
        IReadOnlyDictionary<string, string>? headers = null) => new(
        Guid.NewGuid(),
        "queue",
        payload,
        headers ?? new Dictionary<string, string>(),
        1,
        () => ValueTask.CompletedTask,
        _ => ValueTask.CompletedTask,
        (_, _, _) => ValueTask.FromResult(true),
        () => ValueTask.FromResult(true));

    private static MongoDbTransportStore CreateStore(
        IMongoCollection<MongoTransportMessageDocument> collection,
        IOptions<MongoDbAsyncResponseTransportOptions> options)
        => new(Database(collection).Object, options);

    private static Mock<IMongoDatabase> Database(IMongoCollection<MongoTransportMessageDocument> collection)
    {
        var database = new Mock<IMongoDatabase>(MockBehavior.Loose).WithTestNamespace();
        database
            .Setup(d => d.GetCollection<MongoTransportMessageDocument>(
                It.IsAny<string>(),
                It.IsAny<MongoCollectionSettings>()))
            .Returns(collection);
        return database;
    }

    private static ServiceCollection Services()
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        return services;
    }

    [Fact]
    public async Task WorkerSubscriber_InvalidOptions_FailHostStartupSynchronously()
    {
        // Red-on-old (Hosting 10.0.10+): validation used to sit at the top of ExecuteAsync, which
        // BackgroundService.StartAsync no longer runs inline — StartAsync returned without
        // throwing and the misconfiguration surfaced late or never. Validation now runs in
        // StartAsync so a misconfigured subscriber fails host startup synchronously.
        var options = Options.Create(new MongoDbAsyncResponseTransportOptions
        {
            AutoCreateIndexes = false,
            WorkerSubscriber = { AckMode = MongoDbAckMode.AckAfterEnqueue }
        });
        var collection = new Mock<IMongoCollection<MongoTransportMessageDocument>>(MockBehavior.Loose);
        using var store = CreateStore(collection.Object, options);
        var subscriber = new MongoDbWorkerSubscriber(options, store, Mock.Of<IAsyncResponseIngress>(), NullLogger<MongoDbWorkerSubscriber>.Instance);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => subscriber.StartAsync(CancellationToken.None));
        Assert.Contains("BackgroundWorkerCount", ex.Message, StringComparison.Ordinal);
    }

    private static T GetProperty<T>(object target, string name)
        => (T)target.GetType().GetProperty(name, BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(target)!;

    private static Task InvokeHandlerAsync(object target, MongoDbTransportDelivery delivery)
        => (Task)target.GetType()
            .GetMethod("HandleMessageAsync", BindingFlags.Instance | BindingFlags.NonPublic)!
            .Invoke(target, [delivery, CancellationToken.None])!;
}
