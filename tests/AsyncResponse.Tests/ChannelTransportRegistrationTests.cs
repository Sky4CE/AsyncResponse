using AsyncResponse.Channels.Redis;
using AsyncResponse.Transports.AzureServiceBus;
using AsyncResponse.Transports.GooglePubSub;
using AsyncResponse.Transports.Kafka;
using AsyncResponse.Transports.MongoDB;
using AsyncResponse.Transports.NATS;
using AsyncResponse.Transports.PostgreSQL;
using AsyncResponse.Transports.RabbitMQ;
using AsyncResponse.Transports.SQS;
using AsyncResponse.Transports.SqlServer;
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

        // The in-memory channel exposes the full recoverable surface (same registrations as the
        // durable channel packages), backed by the process-local recovery store.
        Assert.Same(publisher, provider.GetRequiredService<IRecoverableAsyncResponseSubscriber>());
        Assert.Same(
            provider.GetRequiredService<IRecoverableAsyncResponseBuilder>(),
            provider.GetRequiredService<IAsyncResponseBuilder>());
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

    [Theory]
    [InlineData("PostgreSQL")]
    [InlineData("SqlServer")]
    [InlineData("MongoDB")]
    [InlineData("Kafka")]
    [InlineData("RabbitMQ")]
    [InlineData("Redis")]
    [InlineData("NATS")]
    [InlineData("AzureServiceBus")]
    [InlineData("GooglePubSub")]
    [InlineData("SQS")]
    public void EveryBrokerTransport_DeclaresWorkerEarlyAckOnItsMarker(string transport)
    {
        // The startup validator's early-ACK-vs-durable-flows veto is only as strong as each
        // transport's marker declaration; this pins the resolved-options factory wiring on all
        // ten so a mis-wired declaration cannot silently disarm the guard.
        var provider = Build(builder => RegisterTransportWithWorkerEarlyAck(builder, transport));

        var marker = provider.GetRequiredService<AsyncResponseTransportMarker>();
        Assert.True(marker.WorkerSubscriberUsesEarlyAck);
        Assert.False(marker.ResponseSubscriberUsesEarlyAck);
        Assert.NotNull(marker.WorkerAckModePath);
        Assert.Contains("WorkerSubscriber", marker.WorkerAckModePath, StringComparison.Ordinal);
        Assert.NotNull(marker.ResponseAckModePath);
        Assert.Contains("ResponseSubscriber", marker.ResponseAckModePath, StringComparison.Ordinal);
    }

    private static void RegisterTransportWithWorkerEarlyAck(AsyncResponseRegistrationBuilder builder, string transport)
    {
        switch (transport)
        {
            case "PostgreSQL":
                builder.WithPostgreSqlTransport(options => options.WorkerSubscriber.UseAckAfterEnqueue(2, 64));
                break;
            case "SqlServer":
                builder.WithSqlServerTransport(options => options.WorkerSubscriber.UseAckAfterEnqueue(2, 64));
                break;
            case "MongoDB":
                builder.WithMongoDbTransport(options => options.WorkerSubscriber.UseAckAfterEnqueue(2, 64));
                break;
            case "Kafka":
                builder.WithKafkaTransport(options => options.WorkerSubscriber.UseAckAfterEnqueue(2, 64));
                break;
            case "RabbitMQ":
                builder.WithRabbitMqTransport(options => options.WorkerSubscriber.UseAckAfterEnqueue(2, 64));
                break;
            case "Redis":
                builder.WithRedisTransport(options => options.WorkerSubscriber.UseAckAfterEnqueue(2, 64));
                break;
            case "NATS":
                builder.WithNatsTransport(options => options.WorkerSubscriber.UseAckAfterEnqueue(2, 64));
                break;
            case "AzureServiceBus":
                builder.WithAzureServiceBusTransport(options => options.WorkerSubscriber.UseAckAfterEnqueue(2, 64));
                break;
            case "GooglePubSub":
                builder.WithGooglePubSubTransport(options => options.WorkerSubscriber.UseAckAfterEnqueue(2, 64));
                break;
            case "SQS":
                builder.WithSqsTransport(options => options.WorkerSubscriber.UseAckAfterEnqueue(2, 64));
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(transport), transport, "Unknown transport case.");
        }
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
            provider.GetServices<AsyncResponseDurableFlowStoreMarker>(),
            provider.GetRequiredService<Microsoft.Extensions.Options.IOptions<AsyncResponseOptions>>());

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
            provider.GetServices<AsyncResponseDurableFlowStoreMarker>(),
            provider.GetRequiredService<Microsoft.Extensions.Options.IOptions<AsyncResponseOptions>>());

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
            provider.GetServices<AsyncResponseDurableFlowStoreMarker>(),
            provider.GetRequiredService<Microsoft.Extensions.Options.IOptions<AsyncResponseOptions>>());

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
            provider.GetServices<AsyncResponseDurableFlowStoreMarker>(),
            provider.GetRequiredService<Microsoft.Extensions.Options.IOptions<AsyncResponseOptions>>());

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => validator.StartAsync(CancellationToken.None));
        Assert.Contains("multiple", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("durable-flow", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void AddAsyncResponseTwice_RegistersHostedServicesOnce()
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddAsyncResponse().WithInMemoryChannel().WithInMemoryTransport().WithInMemoryDurableFlows();
        services.AddAsyncResponse();

        using var provider = services.BuildServiceProvider();
        var hostedServices = provider.GetServices<IHostedService>().ToArray();

        // A second AddAsyncResponse() must not double the validator or watchdog: two watchdogs
        // would duplicate scans and warnings, and two validators are pure waste.
        Assert.Single(hostedServices, service => service is AsyncResponseStartupValidator);
        Assert.Single(hostedServices, service => service is AsyncResponseWatchdog);
    }

    [Theory]
    [InlineData(0, 1, 0, nameof(AsyncResponseWatchdogOptions.Interval))]
    [InlineData(1440, 1, 0, nameof(AsyncResponseWatchdogOptions.Interval))] // 60 d > the timer ceiling: Task.Delay would throw mid-loop
    [InlineData(1, 0, 0, nameof(AsyncResponseWatchdogOptions.StaleAfter))]
    [InlineData(1, 1, -1, nameof(AsyncResponseWatchdogOptions.StartupDelay))]
    [InlineData(1, 1, 86400, nameof(AsyncResponseWatchdogOptions.StartupDelay))] // 60 d > the timer ceiling
    public async Task StartupValidator_RejectsInvalidWatchdogOptions(
        int intervalHours,
        int staleAfterHours,
        int startupDelayMinutes,
        string expectedOption)
    {
        var provider = Build(builder => builder
            .WithInMemoryChannel()
            .WithInMemoryTransport()
            .WithInMemoryDurableFlows());
        var validator = new AsyncResponseStartupValidator(
            provider.GetServices<AsyncResponseChannelMarker>(),
            provider.GetServices<AsyncResponseTransportMarker>(),
            provider.GetServices<AsyncResponseDurableFlowStoreMarker>(),
            Options.Create(new AsyncResponseOptions
            {
                Watchdog = new AsyncResponseWatchdogOptions
                {
                    Interval = TimeSpan.FromHours(intervalHours),
                    StaleAfter = TimeSpan.FromHours(staleAfterHours),
                    StartupDelay = TimeSpan.FromMinutes(startupDelayMinutes)
                }
            }));

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => validator.StartAsync(CancellationToken.None));
        Assert.Contains(expectedOption, ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task StartupValidator_AcceptsZeroStartupDelayAndUncappedStaleThreshold()
    {
        // Zero means "scan immediately" and stays valid; StaleAfter is compare-only (never armed
        // as a timer), so a beyond-timer-ceiling threshold is a legitimate configuration.
        var provider = Build(builder => builder
            .WithInMemoryChannel()
            .WithInMemoryTransport()
            .WithInMemoryDurableFlows());
        var validator = new AsyncResponseStartupValidator(
            provider.GetServices<AsyncResponseChannelMarker>(),
            provider.GetServices<AsyncResponseTransportMarker>(),
            provider.GetServices<AsyncResponseDurableFlowStoreMarker>(),
            Options.Create(new AsyncResponseOptions
            {
                Watchdog = new AsyncResponseWatchdogOptions
                {
                    StartupDelay = TimeSpan.Zero,
                    StaleAfter = TimeSpan.FromDays(60)
                }
            }));

        await validator.StartAsync(CancellationToken.None);
    }

    [Fact]
    public void InMemoryChannel_RejectsNonPositiveSharedChannelOptions()
    {
        using var services = new ServiceCollection().BuildServiceProvider();

        var expiry = Assert.Throws<InvalidOperationException>(() => new InMemoryAsyncResponseChannel(
            services.GetRequiredService<IServiceScopeFactory>(),
            new InMemoryRecoveryStateStore(),
            Options.Create(new InMemoryAsyncResponseOptions { RecoveryStateExpiry = TimeSpan.Zero }),
            new AsyncResponseContextPropagation([]),
            NullLogger<InMemoryAsyncResponseChannel>.Instance));
        Assert.Contains(nameof(AsyncResponseChannelOptions.RecoveryStateExpiry), expiry.Message, StringComparison.Ordinal);

        var timeout = Assert.Throws<InvalidOperationException>(() => new InMemoryAsyncResponseChannel(
            services.GetRequiredService<IServiceScopeFactory>(),
            new InMemoryRecoveryStateStore(),
            Options.Create(new InMemoryAsyncResponseOptions { DefaultTimeout = TimeSpan.Zero }),
            new AsyncResponseContextPropagation([]),
            NullLogger<InMemoryAsyncResponseChannel>.Instance));
        Assert.Contains(nameof(AsyncResponseChannelOptions.DefaultTimeout), timeout.Message, StringComparison.Ordinal);
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
