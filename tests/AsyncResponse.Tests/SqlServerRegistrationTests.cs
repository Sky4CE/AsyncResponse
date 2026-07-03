using AsyncResponse.Channels.SqlServer;
using AsyncResponse.Transports.SqlServer;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;

namespace AsyncResponse.Tests;

public sealed class SqlServerRegistrationTests
{
    private const string TestConnectionString =
        "Server=localhost;Database=asyncresponse_tests;User ID=sa;Password=unused;TrustServerCertificate=True";

    [Fact]
    public void WithSqlServerChannel_SharesOneInstanceAcrossRoles_AndMarksChannel()
    {
        var provider = Build(builder => builder.WithSqlServerChannel(options => options.ConnectionString = TestConnectionString));

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
        Assert.IsType<SqlServerAsyncResponseChannel>(publisher);

        var store = provider.GetRequiredService<IRecoveryStateStore>();
        var scanner = provider.GetRequiredService<IRecoveryStateScanner>();
        Assert.Same(store, scanner);
        Assert.IsType<SqlServerRecoveryStateStore>(store);

        Assert.Equal("SqlServer", provider.GetRequiredService<AsyncResponseChannelMarker>().Name);
    }

    [Fact]
    public void WithSqlServerChannel_AppliesConfigureOptions()
    {
        var provider = Build(builder => builder.WithSqlServerChannel(options =>
        {
            options.ConnectionString = TestConnectionString;
            options.SchemaName = "orders";
            options.MessageTable = "orders_messages";
            options.DeliveryConfirmationTimeout = TimeSpan.FromSeconds(7);
            options.ActivePollInterval = TimeSpan.FromMilliseconds(100);
            options.IdlePollInterval = TimeSpan.FromSeconds(5);
        }));

        var options = provider.GetRequiredService<IOptions<SqlServerAsyncResponseChannelOptions>>().Value;
        Assert.Equal("orders", options.SchemaName);
        Assert.Equal("orders_messages", options.MessageTable);
        Assert.Equal(TimeSpan.FromSeconds(7), options.DeliveryConfirmationTimeout);
        Assert.Equal(TimeSpan.FromMilliseconds(100), options.ActivePollInterval);
        Assert.Equal(TimeSpan.FromSeconds(5), options.IdlePollInterval);
    }

    [Fact]
    public void WithSqlServerTransport_ReplacesWorkerTransportReplyProvider_AndRegistersHostedServices()
    {
        var provider = Build(builder => builder
            .WithInMemoryChannel()
            .WithSqlServerTransport(options =>
            {
                options.ConnectionString = TestConnectionString;
                options.WorkerQueue = "worker_jobs";
                options.ResponseQueue = "responses";
                options.DeadLetterQueue = "dead_letters";
            }));

        Assert.IsType<SqlServerWorkerTransport>(provider.GetRequiredService<IWorkerTransport>());
        Assert.IsType<SqlServerReplyTargetProvider>(provider.GetRequiredService<IAsyncResponseReplyTargetProvider>());
        Assert.Equal("SqlServer", provider.GetRequiredService<AsyncResponseTransportMarker>().Name);

        var hostedServices = provider.GetServices<IHostedService>().ToList();
        Assert.Contains(hostedServices, service => service is SqlServerWorkerSubscriber);
        Assert.Contains(hostedServices, service => service is SqlServerResponseIngressSubscriber);
    }

    [Fact]
    public void WithSqlServerTransport_AppliesConfigureOptions()
    {
        var provider = Build(builder => builder
            .WithInMemoryChannel()
            .WithSqlServerTransport(options =>
            {
                options.ConnectionString = TestConnectionString;
                options.SchemaName = "orders";
                options.MessageTable = "orders_transport";
                options.WorkerSubscriber.UseAckAfterEnqueue(2, 64);
            }));

        var options = provider.GetRequiredService<IOptions<SqlServerAsyncResponseTransportOptions>>().Value;
        Assert.Equal("orders", options.SchemaName);
        Assert.Equal("orders_transport", options.MessageTable);
        Assert.Equal(SqlServerAckMode.AckAfterEnqueue, options.WorkerSubscriber.AckMode);
        Assert.Equal(2, options.WorkerSubscriber.BackgroundWorkerCount);
        Assert.Equal(64, options.WorkerSubscriber.BackgroundQueueCapacity);
    }

    [Fact]
    public void WithSqlServerTransport_NullConfigure_Throws()
        => Assert.Throws<ArgumentNullException>(
            () => new ServiceCollection().AddAsyncResponse().WithSqlServerTransport(null!));

    [Fact]
    public async Task SqlServerChannelAndTransport_RegisterExactlyOneMarkerEach()
    {
        var provider = Build(builder => builder
            .WithSqlServerChannel(options => options.ConnectionString = TestConnectionString)
            .WithSqlServerTransport(options => options.ConnectionString = TestConnectionString));
        var validator = new AsyncResponseStartupValidator(
            provider.GetServices<AsyncResponseChannelMarker>(),
            provider.GetServices<AsyncResponseTransportMarker>());

        await validator.StartAsync(CancellationToken.None);
        await validator.StopAsync(CancellationToken.None);
    }

    private static ServiceProvider Build(Action<AsyncResponseRegistrationBuilder> configure)
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));

        configure(services.AddAsyncResponse());
        return services.BuildServiceProvider();
    }
}
