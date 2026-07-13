using AsyncResponse.Channels.MongoDB;
using AsyncResponse.Transports.MongoDB;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using MongoDB.Driver;
using Xunit;

namespace AsyncResponse.Tests;

public sealed class MongoDbRegistrationTests
{
    [Fact]
    public void WithMongoDbChannel_SharesOneInstanceAcrossRoles_AndMarksChannel()
    {
        var provider = Build(builder => builder.WithMongoDbChannel());

        var publisher = provider.GetRequiredService<IAsyncResponsePublisher>();
        var rawPublisher = provider.GetRequiredService<IRawAsyncResponsePublisher>();
        var subscriber = provider.GetRequiredService<IAsyncResponseSubscriber>();
        var recoverableSubscriber = provider.GetRequiredService<IRecoverableAsyncResponseSubscriber>();
        var probe = provider.GetRequiredService<IActiveSubscriberProbe>();
        var builder = provider.GetRequiredService<IAsyncResponseBuilder>();
        var recoverableBuilder = provider.GetRequiredService<IRecoverableAsyncResponseBuilder>();

        Assert.Same(publisher, rawPublisher);
        Assert.Same(publisher, subscriber);
        Assert.Same(publisher, recoverableSubscriber);
        Assert.Same(publisher, probe);
        Assert.Same(builder, recoverableBuilder);
        Assert.IsType<MongoDbAsyncResponseChannel>(publisher);

        var store = provider.GetRequiredService<IRecoveryStateStore>();
        var scanner = provider.GetRequiredService<IRecoveryStateScanner>();
        Assert.Same(store, scanner);
        Assert.IsType<MongoDbRecoveryStateStore>(store);

        Assert.Equal("MongoDB", provider.GetRequiredService<AsyncResponseChannelMarker>().Name);
    }

    [Fact]
    public void WithMongoDbChannel_AppliesConfigureOptions()
    {
        var provider = Build(builder => builder.WithMongoDbChannel(options =>
        {
            options.MessageCollection = "orders_messages";
            options.RecoveryStateCollection = "orders_recovery";
            options.DeliveryConfirmationTimeout = TimeSpan.FromSeconds(7);
        }));

        var options = provider.GetRequiredService<IOptions<MongoDbAsyncResponseChannelOptions>>().Value;
        Assert.Equal("orders_messages", options.MessageCollection);
        Assert.Equal("orders_recovery", options.RecoveryStateCollection);
        Assert.Equal(TimeSpan.FromSeconds(7), options.DeliveryConfirmationTimeout);
    }

    [Fact]
    public void WithMongoDbChannel_WithoutDatabaseOrConnectionString_FailsFastOnResolve()
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddAsyncResponse().WithMongoDbChannel();
        var provider = services.BuildServiceProvider();

        var ex = Assert.Throws<InvalidOperationException>(() => provider.GetRequiredService<MongoDbChannelStore>());
        Assert.Contains(nameof(MongoDbAsyncResponseChannelOptions.DatabaseName), ex.Message);
    }

    [Fact]
    public void WithMongoDbChannel_ResolvesStoreFromSharedClientAndDatabaseName()
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddSingleton<IMongoClient>(new MongoClient("mongodb://localhost:27017"));
        services.AddAsyncResponse().WithMongoDbChannel(options => options.DatabaseName = "asyncresponse_tests");
        var provider = services.BuildServiceProvider();

        Assert.NotNull(provider.GetRequiredService<MongoDbChannelStore>());
    }

    [Fact]
    public void WithMongoDbChannel_ResolvesStoreFromConnectionString()
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddAsyncResponse().WithMongoDbChannel(options =>
        {
            options.ConnectionString = "mongodb://localhost:27017";
            options.DatabaseName = "asyncresponse_tests";
        });
        var provider = services.BuildServiceProvider();

        Assert.NotNull(provider.GetRequiredService<MongoDbChannelStore>());
    }

    [Fact]
    public void WithMongoDbTransport_ReplacesWorkerTransportReplyProvider_AndRegistersHostedServices()
    {
        var provider = Build(builder => builder
            .WithInMemoryChannel()
            .WithMongoDbTransport(options =>
            {
                options.WorkerQueue = "worker_jobs";
                options.ResponseQueue = "responses";
                options.DeadLetterQueue = "dead_letters";
            }));

        Assert.IsType<MongoDbWorkerTransport>(provider.GetRequiredService<IWorkerTransport>());
        Assert.IsType<MongoDbReplyTargetProvider>(provider.GetRequiredService<IAsyncResponseReplyTargetProvider>());
        Assert.Equal("MongoDB", provider.GetRequiredService<AsyncResponseTransportMarker>().Name);

        var hostedServices = provider.GetServices<IHostedService>().ToList();
        Assert.Contains(hostedServices, service => service is MongoDbWorkerSubscriber);
        Assert.Contains(hostedServices, service => service is MongoDbResponseIngressSubscriber);
    }

    [Fact]
    public void WithMongoDbTransport_AppliesConfigureOptions()
    {
        var provider = Build(builder => builder
            .WithInMemoryChannel()
            .WithMongoDbTransport(options =>
            {
                options.MessageCollection = "orders_transport";
                options.WorkerSubscriber.UseAckAfterEnqueue(2, 64);
            }));

        var options = provider.GetRequiredService<IOptions<MongoDbAsyncResponseTransportOptions>>().Value;
        Assert.Equal("orders_transport", options.MessageCollection);
        Assert.Equal(MongoDbAckMode.AckAfterEnqueue, options.WorkerSubscriber.AckMode);
        Assert.Equal(2, options.WorkerSubscriber.BackgroundWorkerCount);
        Assert.Equal(64, options.WorkerSubscriber.BackgroundQueueCapacity);
    }

    [Fact]
    public void WithMongoDbTransport_WithoutDatabaseOrConnectionString_FailsFastOnResolve()
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddAsyncResponse().WithInMemoryChannel().WithMongoDbTransport(_ => { });
        var provider = services.BuildServiceProvider();

        var ex = Assert.Throws<InvalidOperationException>(() => provider.GetRequiredService<MongoDbTransportStore>());
        Assert.Contains(nameof(MongoDbAsyncResponseTransportOptions.DatabaseName), ex.Message);
    }

    [Fact]
    public void WithMongoDbTransport_NullConfigure_Throws()
        => Assert.Throws<ArgumentNullException>(
            () => new ServiceCollection().AddAsyncResponse().WithMongoDbTransport(null!));

    [Fact]
    public async Task MongoDbChannelAndTransport_RegisterExactlyOneMarkerEach()
    {
        var provider = Build(builder => builder
            .WithMongoDbChannel()
            .WithMongoDbTransport(_ => { })
            .WithInMemoryDurableFlows());
        var validator = new AsyncResponseStartupValidator(
            provider.GetServices<AsyncResponseChannelMarker>(),
            provider.GetServices<AsyncResponseTransportMarker>(),
            provider.GetServices<AsyncResponseDurableFlowStoreMarker>());

        await validator.StartAsync(CancellationToken.None);
        await validator.StopAsync(CancellationToken.None);
    }

    private static ServiceProvider Build(Action<AsyncResponseRegistrationBuilder> configure)
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        // MongoClient/GetDatabase are lazy — no connection is opened until a command runs — so DI
        // tests can register a real IMongoDatabase without a server.
        services.AddSingleton(new MongoClient("mongodb://localhost:27017").GetDatabase("asyncresponse_tests"));

        configure(services.AddAsyncResponse());
        return services.BuildServiceProvider();
    }
}
