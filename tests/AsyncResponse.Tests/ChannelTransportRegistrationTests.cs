using AsyncResponse.Transports.GooglePubSub;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// What each channel/transport registration wires into DI: the in-memory channel shares one
/// instance across publisher/subscriber/probe and one store across store/scanner; the channel
/// marker drives the single-channel startup rule (multiple channels fail fast); and the Google
/// Pub/Sub transport replaces the worker transport and reply-target provider.
/// </summary>
public class ChannelTransportRegistrationTests
{
    [Fact]
    public void InMemoryChannel_SharesOneInstanceAcrossRoles_AndMarksTheChannel()
    {
        var provider = Build(builder => builder.WithInMemoryChannel());

        var publisher = provider.GetRequiredService<IAsyncResponsePublisher>();
        var subscriber = provider.GetRequiredService<IAsyncResponseSubscriber>();
        var probe = provider.GetRequiredService<IActiveSubscriberProbe>();
        Assert.Same(publisher, subscriber);
        Assert.Same(publisher, probe);
        Assert.IsType<InMemoryAsyncResponseChannel>(publisher);

        var store = provider.GetRequiredService<IRecoveryStateStore>();
        var scanner = provider.GetRequiredService<IRecoveryStateScanner>();
        Assert.Same(store, scanner);
        Assert.IsType<InMemoryRecoveryStateStore>(store);

        Assert.Equal("InMemory", provider.GetRequiredService<AsyncResponseChannelMarker>().Name);
    }

    [Fact]
    public void InMemoryTransport_RegistersTransportAndBackgroundHost()
    {
        var provider = Build(builder => builder.WithInMemoryChannel().WithInMemoryTransport());

        Assert.IsType<InMemoryWorkerTransport>(provider.GetRequiredService<IWorkerTransport>());
        Assert.Contains(provider.GetServices<IHostedService>(), service => service is InMemoryWorkerHost);
    }

    [Fact]
    public async Task MultipleChannels_FailFastAtStartup()
    {
        // Build the validator directly from the markers so we don't construct the (Redis-dependent)
        // watchdog, which would need an IConnectionMultiplexer that this test deliberately omits.
        var provider = Build(builder => builder.WithInMemoryChannel().WithRedisChannel());
        var validator = new AsyncResponseStartupValidator(provider.GetServices<AsyncResponseChannelMarker>());

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => validator.StartAsync(CancellationToken.None));
        Assert.Contains("multiple", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task RedisChannel_RegistersExactlyOneChannelMarker()
    {
        var provider = Build(builder => builder.WithRedisChannel());
        var validator = new AsyncResponseStartupValidator(provider.GetServices<AsyncResponseChannelMarker>());

        await validator.StartAsync(CancellationToken.None); // single "Redis" channel → must not throw
        await validator.StopAsync(CancellationToken.None);

        Assert.Equal("Redis", provider.GetRequiredService<AsyncResponseChannelMarker>().Name);
    }

    [Fact]
    public void GooglePubSubTransport_NullConfigure_Throws()
        => Assert.Throws<ArgumentNullException>(
            () => new ServiceCollection().AddAsyncResponse().WithGooglePubSubTransport(null!));

    [Fact]
    public void GooglePubSubTransport_ReplacesWorkerTransportAndReplyTargetProvider()
    {
        var provider = Build(builder => builder
            .WithInMemoryChannel()
            .WithGooglePubSubTransport(options =>
            {
                options.ProjectId = "proj";
                options.WorkerTopicId = "worker-topic";
            }));

        Assert.IsType<GooglePubSubWorkerTransport>(provider.GetRequiredService<IWorkerTransport>());
        Assert.IsType<GooglePubSubReplyTargetProvider>(provider.GetRequiredService<IAsyncResponseReplyTargetProvider>());
    }

    private static ServiceProvider Build(Action<AsyncResponseRegistrationBuilder> configure)
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        configure(services.AddAsyncResponse());
        return services.BuildServiceProvider();
    }
}
