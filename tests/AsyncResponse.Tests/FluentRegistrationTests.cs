using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// The fluent registration surface: a channel, transport, and durable-flow store are mandatory
/// and enforced at host startup, and the in-memory channel/store satisfy the engine's
/// recovery-state scanner and active-subscriber probe that the (channel-agnostic) watchdog runs on.
/// </summary>
public class FluentRegistrationTests
{
    [Fact]
    public void AsyncResponseOptions_ContainsOnlyGlobalConfiguration()
        => Assert.Null(typeof(AsyncResponseOptions).GetProperty("DurableFlows"));

    [Fact]
    public async Task StartupValidator_NoChannel_ThrowsWithGuidance()
    {
        var provider = Build(builder => builder
            .WithInMemoryTransport()
            .WithInMemoryDurableFlows());
        var validator = StartupValidator(provider);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => validator.StartAsync(CancellationToken.None));
        Assert.Contains("WithInMemoryChannel", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task StartupValidator_NoTransport_ThrowsWithGuidance()
    {
        var provider = Build(builder => builder
            .WithInMemoryChannel()
            .WithInMemoryDurableFlows());
        var validator = StartupValidator(provider);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => validator.StartAsync(CancellationToken.None));
        Assert.Contains("WithInMemoryTransport", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task StartupValidator_NoDurableFlowStore_ThrowsWithGuidance()
    {
        var provider = Build(builder => builder.WithInMemoryChannel().WithInMemoryTransport());
        var validator = StartupValidator(provider);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => validator.StartAsync(CancellationToken.None));
        Assert.Contains("WithInMemoryDurableFlows", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task StartupValidator_WithAllRequiredComponents_Succeeds()
    {
        var provider = Build(builder => builder
            .WithInMemoryChannel()
            .WithInMemoryTransport()
            .WithInMemoryDurableFlows());
        var validator = StartupValidator(provider);

        await validator.StartAsync(CancellationToken.None); // must not throw
        await validator.StopAsync(CancellationToken.None);
    }

    [Fact]
    public void WithInMemoryTransport_WithConfigure_AppliesOptions()
    {
        using var provider = Build(builder => builder.WithInMemoryTransport(o => o.QueueCapacity = 999));
        var options = provider.GetRequiredService<Microsoft.Extensions.Options.IOptions<InMemoryWorkerTransportOptions>>().Value;
        Assert.Equal(999, options.QueueCapacity);
    }

    [Fact]
    public async Task InMemoryChannel_Scanner_YieldsSavedRecoveryState()
    {
        var provider = Build(builder => builder.WithInMemoryChannel());
        var store = provider.GetRequiredService<IRecoveryStateStore>();
        var scanner = provider.GetRequiredService<IRecoveryStateScanner>();

        const string correlationId = "scan-cid";
        await store.SaveAsync(
            correlationId,
            new RecoveryState
            {
                CorrelationId = correlationId,
                PayloadTypeFullName = typeof(OperationResult).FullName,
                RegisteredAtUtc = DateTime.UtcNow
            },
            TimeSpan.FromMinutes(5));

        var scanned = new List<RecoveryState>();
        await foreach (var state in scanner.ScanAsync())
            scanned.Add(state);

        Assert.Contains(scanned, s => s.CorrelationId == correlationId);
    }

    [Fact]
    public async Task InMemoryChannel_Probe_CountsLiveWaiterThenZeroAfterDispose()
    {
        var provider = Build(builder => builder.WithInMemoryChannel());
        var subscriber = provider.GetRequiredService<IAsyncResponseSubscriber>();
        var probe = provider.GetRequiredService<IActiveSubscriberProbe>();

        const string correlationId = "live-cid";
        Assert.Equal(0, await probe.CountActiveSubscribersAsync(correlationId));

        var waiter = await subscriber.CreateResponseWaiter<OperationResult>(correlationId, timeout: TimeSpan.FromSeconds(30));
        try
        {
            Assert.Equal(1, await probe.CountActiveSubscribersAsync(correlationId));
        }
        finally
        {
            await waiter.DisposeAsync();
        }

        Assert.Equal(0, await probe.CountActiveSubscribersAsync(correlationId));
    }

    private static ServiceProvider Build(Action<AsyncResponseRegistrationBuilder> configure)
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        configure(services.AddAsyncResponse());
        return services.BuildServiceProvider();
    }

    private static AsyncResponseStartupValidator StartupValidator(IServiceProvider provider)
        => provider.GetServices<IHostedService>().OfType<AsyncResponseStartupValidator>().Single();
}
