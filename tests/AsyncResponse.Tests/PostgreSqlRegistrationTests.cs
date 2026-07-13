using AsyncResponse.Channels.PostgreSQL;
using AsyncResponse.Transports.PostgreSQL;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Npgsql;
using Xunit;

namespace AsyncResponse.Tests;

public sealed class PostgreSqlRegistrationTests
{
    [Fact]
    public void WithPostgreSqlChannel_SharesOneInstanceAcrossRoles_AndMarksChannel()
    {
        var provider = Build(builder => builder.WithPostgreSqlChannel());

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
        Assert.IsType<PostgreSqlAsyncResponseChannel>(publisher);

        var store = provider.GetRequiredService<IRecoveryStateStore>();
        var scanner = provider.GetRequiredService<IRecoveryStateScanner>();
        Assert.Same(store, scanner);
        Assert.IsType<PostgreSqlRecoveryStateStore>(store);

        Assert.Equal("PostgreSQL", provider.GetRequiredService<AsyncResponseChannelMarker>().Name);
    }

    [Fact]
    public void WithPostgreSqlChannel_AppliesConfigureOptions()
    {
        var provider = Build(builder => builder.WithPostgreSqlChannel(options =>
        {
            options.SchemaName = "orders";
            options.MessageTable = "orders_messages";
            options.DeliveryConfirmationTimeout = TimeSpan.FromSeconds(7);
        }));

        var options = provider.GetRequiredService<IOptions<PostgreSqlAsyncResponseChannelOptions>>().Value;
        Assert.Equal("orders", options.SchemaName);
        Assert.Equal("orders_messages", options.MessageTable);
        Assert.Equal(TimeSpan.FromSeconds(7), options.DeliveryConfirmationTimeout);
    }

    [Fact]
    public void WithPostgreSqlTransport_ReplacesWorkerTransportReplyProvider_AndRegistersHostedServices()
    {
        var provider = Build(builder => builder
            .WithInMemoryChannel()
            .WithPostgreSqlTransport(options =>
            {
                options.WorkerQueue = "worker_jobs";
                options.ResponseQueue = "responses";
                options.DeadLetterQueue = "dead_letters";
            }));

        Assert.IsType<PostgreSqlWorkerTransport>(provider.GetRequiredService<IWorkerTransport>());
        Assert.IsType<PostgreSqlReplyTargetProvider>(provider.GetRequiredService<IAsyncResponseReplyTargetProvider>());
        Assert.Equal("PostgreSQL", provider.GetRequiredService<AsyncResponseTransportMarker>().Name);

        var hostedServices = provider.GetServices<IHostedService>().ToList();
        Assert.Contains(hostedServices, service => service is PostgreSqlWorkerSubscriber);
        Assert.Contains(hostedServices, service => service is PostgreSqlResponseIngressSubscriber);
    }

    [Fact]
    public void WithPostgreSqlTransport_AppliesConfigureOptions()
    {
        var provider = Build(builder => builder
            .WithInMemoryChannel()
            .WithPostgreSqlTransport(options =>
            {
                options.SchemaName = "orders";
                options.MessageTable = "orders_transport";
                options.WorkerSubscriber.UseAckAfterReceive(2, 64);
            }));

        var options = provider.GetRequiredService<IOptions<PostgreSqlAsyncResponseTransportOptions>>().Value;
        Assert.Equal("orders", options.SchemaName);
        Assert.Equal("orders_transport", options.MessageTable);
        Assert.Equal(PostgreSqlAckMode.AckAfterReceive, options.WorkerSubscriber.AckMode);
        Assert.Equal(2, options.WorkerSubscriber.BackgroundWorkerCount);
        Assert.Equal(64, options.WorkerSubscriber.BackgroundQueueCapacity);
    }

    [Fact]
    public void WithPostgreSqlTransport_NullConfigure_Throws()
        => Assert.Throws<ArgumentNullException>(
            () => new ServiceCollection().AddAsyncResponse().WithPostgreSqlTransport(null!));

    [Fact]
    public async Task PostgreSqlChannelAndTransport_RegisterExactlyOneMarkerEach()
    {
        var provider = Build(builder => builder
            .WithPostgreSqlChannel()
            .WithPostgreSqlTransport(_ => { })
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
        services.AddSingleton<NpgsqlDataSource>(_ => NpgsqlDataSource.Create(
            "Host=localhost;Username=postgres;Password=postgres;Database=asyncresponse_tests;Pooling=false"));

        configure(services.AddAsyncResponse());
        return services.BuildServiceProvider();
    }
}
