using AsyncResponse.Channels.Redis;
using AsyncResponse.Transports.GooglePubSub;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using StackExchange.Redis;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// What each channel/transport registration wires into DI: the in-memory channel shares one
/// instance across publisher/subscriber/probe and one store across store/scanner; the channel
/// marker drives the single-channel startup rule (multiple channels fail fast); transport and
/// durable-flow markers drive the same exactly-one rule; and the Google Pub/Sub transport replaces
/// the worker transport and reply-target provider.
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

        Assert.Null(provider.GetService<IRecoverableAsyncResponseBuilder>());
        Assert.Null(provider.GetService<IRecoverableAsyncResponseSubscriber>());
        Assert.Equal("InMemory", provider.GetRequiredService<AsyncResponseChannelMarker>().Name);
    }

    [Fact]
    public void InMemoryTransport_RegistersTransportAndBackgroundHost()
    {
        var provider = Build(builder => builder.WithInMemoryChannel().WithInMemoryTransport());

        Assert.IsType<InMemoryWorkerTransport>(provider.GetRequiredService<IWorkerTransport>());
        Assert.Equal("InMemory", provider.GetRequiredService<AsyncResponseTransportMarker>().Name);
        Assert.Contains(provider.GetServices<IHostedService>(), service => service is InMemoryWorkerHost);
    }

    [Fact]
    public void CoreRegistration_DoesNotEnableDurableFlowsImplicitly()
    {
        var provider = Build(builder => builder.WithInMemoryChannel().WithInMemoryTransport());

        Assert.Null(provider.GetService<IDurableFlows>());
        Assert.Null(provider.GetService<IDurableFlowExecutor>());
        Assert.Null(provider.GetService<IFlowStateStore>());
        Assert.Empty(provider.GetServices<AsyncResponseDurableFlowStoreMarker>());
    }

    [Fact]
    public async Task MultipleChannels_FailFastAtStartup()
    {
        // Build the validator directly from the markers so we don't construct the (Redis-dependent)
        // watchdog, which would need an IConnectionMultiplexer that this test deliberately omits.
        var provider = Build(builder => builder
            .WithInMemoryChannel()
            .WithRedisChannel()
            .WithInMemoryTransport()
            .WithInMemoryDurableFlows());
        var validator = new AsyncResponseStartupValidator(
            provider.GetServices<AsyncResponseChannelMarker>(),
            provider.GetServices<AsyncResponseTransportMarker>(),
            provider.GetServices<AsyncResponseDurableFlowStoreMarker>());

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => validator.StartAsync(CancellationToken.None));
        Assert.Contains("multiple", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task RedisChannel_RegistersExactlyOneChannelMarker()
    {
        var provider = Build(builder => builder
            .WithRedisChannel()
            .WithInMemoryTransport()
            .WithInMemoryDurableFlows());
        var validator = new AsyncResponseStartupValidator(
            provider.GetServices<AsyncResponseChannelMarker>(),
            provider.GetServices<AsyncResponseTransportMarker>(),
            provider.GetServices<AsyncResponseDurableFlowStoreMarker>());

        await validator.StartAsync(CancellationToken.None); // single "Redis" channel → must not throw
        await validator.StopAsync(CancellationToken.None);

        Assert.Equal("Redis", provider.GetRequiredService<AsyncResponseChannelMarker>().Name);
    }

    [Fact]
    public void RedisChannel_AppliesConfigureOptions()
    {
        var provider = Build(builder => builder.WithRedisChannel(options =>
        {
            options.KeyPrefix = "orders";
            options.DefaultTimeout = TimeSpan.FromSeconds(11);
        }));

        var options = provider.GetRequiredService<IOptions<RedisAsyncResponseOptions>>().Value;
        Assert.Equal("orders", options.KeyPrefix);
        Assert.Equal(TimeSpan.FromSeconds(11), options.DefaultTimeout);
    }

    [Fact]
    public void RedisChannel_ResolvedInterfacesShareStoreAndChannelInstances()
    {
        var multiplexer = new Mock<IConnectionMultiplexer>();
        multiplexer
            .Setup(m => m.GetDatabase(It.IsAny<int>(), It.IsAny<object?>()))
            .Returns(Mock.Of<IDatabase>());
        multiplexer
            .Setup(m => m.GetSubscriber(It.IsAny<object?>()))
            .Returns(Mock.Of<ISubscriber>());
        var provider = Build(builder =>
        {
            builder.Services.AddSingleton(multiplexer.Object);
            builder.WithRedisChannel();
        });

        var store = provider.GetRequiredService<IRecoveryStateStore>();
        var scanner = provider.GetRequiredService<IRecoveryStateScanner>();
        var publisher = provider.GetRequiredService<IAsyncResponsePublisher>();
        var rawPublisher = provider.GetRequiredService<IRawAsyncResponsePublisher>();
        var subscriber = provider.GetRequiredService<IAsyncResponseSubscriber>();
        var recoverableSubscriber = provider.GetRequiredService<IRecoverableAsyncResponseSubscriber>();
        var builder = provider.GetRequiredService<IAsyncResponseBuilder>();
        var recoverableBuilder = provider.GetRequiredService<IRecoverableAsyncResponseBuilder>();
        var probe = provider.GetRequiredService<IActiveSubscriberProbe>();

        Assert.Same(store, scanner);
        Assert.IsType<RedisRecoveryStateStore>(store);
        Assert.Same(publisher, rawPublisher);
        Assert.Same(publisher, subscriber);
        Assert.Same(publisher, recoverableSubscriber);
        Assert.Same(builder, recoverableBuilder);
        Assert.Same(publisher, probe);
        Assert.IsType<RedisAsyncResponseChannel>(publisher);
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
            })
            .WithInMemoryDurableFlows());

        Assert.IsType<GooglePubSubWorkerTransport>(provider.GetRequiredService<IWorkerTransport>());
        Assert.IsType<GooglePubSubReplyTargetProvider>(provider.GetRequiredService<IAsyncResponseReplyTargetProvider>());
        Assert.Equal("GooglePubSub", provider.GetRequiredService<AsyncResponseTransportMarker>().Name);
    }

    [Fact]
    public async Task MultipleTransports_FailFastAtStartup()
    {
        var provider = Build(builder => builder
            .WithInMemoryChannel()
            .WithInMemoryTransport()
            .WithGooglePubSubTransport(options =>
            {
                options.ProjectId = "proj";
                options.WorkerTopicId = "worker-topic";
            })
            .WithInMemoryDurableFlows());
        var validator = new AsyncResponseStartupValidator(
            provider.GetServices<AsyncResponseChannelMarker>(),
            provider.GetServices<AsyncResponseTransportMarker>(),
            provider.GetServices<AsyncResponseDurableFlowStoreMarker>());

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => validator.StartAsync(CancellationToken.None));
        Assert.Contains("multiple", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("transport", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task MultipleDurableFlowStores_FailFastAtStartup()
    {
        var provider = Build(builder => builder
            .WithInMemoryChannel()
            .WithInMemoryTransport()
            .WithInMemoryDurableFlows()
            .WithDurableFlows<AlternateFlowStateStore>());
        var validator = new AsyncResponseStartupValidator(
            provider.GetServices<AsyncResponseChannelMarker>(),
            provider.GetServices<AsyncResponseTransportMarker>(),
            provider.GetServices<AsyncResponseDurableFlowStoreMarker>());

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => validator.StartAsync(CancellationToken.None));
        Assert.Contains("multiple", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("durable-flow", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    private static ServiceProvider Build(Action<AsyncResponseRegistrationBuilder> configure)
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        configure(services.AddAsyncResponse());
        return services.BuildServiceProvider();
    }

    private sealed class AlternateFlowStateStore : IFlowStateStore
    {
        public Task<bool> TryCreateAsync(string flowId, FlowState state, TimeSpan ttl, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public Task<FlowState?> LoadAsync(string flowId, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public Task<bool> TryUpdateAsync(string flowId, FlowState state, long expectedRevision, TimeSpan ttl, string? leaseId = null, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public Task<bool> TryAcquireLeaseAsync(string flowId, string leaseId, TimeSpan leaseDuration, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public Task<bool> TryRenewLeaseAsync(string flowId, string leaseId, TimeSpan leaseDuration, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public Task ReleaseLeaseAsync(string flowId, string leaseId, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public Task<bool> TryDeleteAsync(string flowId, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();
    }
}
