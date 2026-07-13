using AsyncResponse.Channels.NATS;
using AsyncResponse.Transports.NATS;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using NATS.Client.Core;
using Xunit;

namespace AsyncResponse.Tests;

public class NatsRegistrationTests
{
    [Fact]
    public void WithNatsChannel_SharesOneInstanceAcrossRoles_AndMarksChannel()
    {
        var provider = Build(builder => builder.WithNatsChannel());

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
        Assert.IsType<NatsAsyncResponseChannel>(publisher);

        var store = provider.GetRequiredService<IRecoveryStateStore>();
        var scanner = provider.GetRequiredService<IRecoveryStateScanner>();
        Assert.Same(store, scanner);
        Assert.IsType<NatsRecoveryStateStore>(store);

        Assert.Equal("NATS", provider.GetRequiredService<AsyncResponseChannelMarker>().Name);
    }

    [Fact]
    public void WithNatsChannel_AppliesConfigureOptions()
    {
        var provider = Build(builder => builder.WithNatsChannel(options =>
        {
            options.SubjectPrefix = "orders";
            options.RecoveryBucket = "orders-recovery";
        }));

        var options = provider.GetRequiredService<IOptions<NatsAsyncResponseChannelOptions>>().Value;
        Assert.Equal("orders", options.SubjectPrefix);
        Assert.Equal("orders-recovery", options.RecoveryBucket);
    }

    [Fact]
    public void WithNatsTransport_ReplacesWorkerTransportReplyProvider_AndRegistersHostedServices()
    {
        var provider = Build(builder => builder
            .WithInMemoryChannel()
            .WithNatsTransport(options => options.SubjectPrefix = "app"));

        Assert.IsType<NatsWorkerTransport>(provider.GetRequiredService<IWorkerTransport>());
        Assert.IsType<NatsReplyTargetProvider>(provider.GetRequiredService<IAsyncResponseReplyTargetProvider>());
        Assert.Equal("NATS", provider.GetRequiredService<AsyncResponseTransportMarker>().Name);

        var hostedServices = provider.GetServices<IHostedService>().ToList();
        Assert.Contains(hostedServices, s => s is NatsWorkerSubscriber);
        Assert.Contains(hostedServices, s => s is NatsResponseIngressSubscriber);
    }

    [Fact]
    public void WithNatsTransport_NullConfigure_Throws()
        => Assert.Throws<ArgumentNullException>(
            () => new ServiceCollection().AddAsyncResponse().WithNatsTransport(null!));

    [Fact]
    public async Task NatsChannelAndTransport_RegisterExactlyOneMarkerEach()
    {
        var provider = Build(builder => builder
            .WithNatsChannel()
            .WithNatsTransport(_ => { })
            .WithInMemoryDurableFlows());
        var validator = new AsyncResponseStartupValidator(
            provider.GetServices<AsyncResponseChannelMarker>(),
            provider.GetServices<AsyncResponseTransportMarker>(),
            provider.GetServices<AsyncResponseDurableFlowStoreMarker>());

        await validator.StartAsync(CancellationToken.None); // one NATS channel + transport + flow store → must not throw
        await validator.StopAsync(CancellationToken.None);
    }

    private static ServiceProvider Build(Action<AsyncResponseRegistrationBuilder> configure)
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));

        var connection = new Mock<INatsConnection>();
        connection.SetupGet(c => c.Opts).Returns(NatsOpts.Default);
        services.AddSingleton(connection.Object);

        configure(services.AddAsyncResponse());
        return services.BuildServiceProvider();
    }
}
