using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using System.Text.Json.Serialization;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// The in-memory transport must deliver what a broker would deliver: the envelope materialized
/// from its wire JSON, never the publisher's live object graph — otherwise tests pass green on
/// <c>[JsonIgnore]</c> state and post-publish mutations that evaporate on every broker-backed
/// transport, and a non-serializable argument surfaces at the worker instead of at the publish.
/// </summary>
public sealed class InMemoryWorkerTransportWireParityTests
{
    public sealed class WireParityPayload
    {
        public string? Name { get; set; }

        [JsonIgnore]
        public string? InProcessOnly { get; set; }
    }

    public interface IWireParityProbe
    {
        Task RunAsync(WireParityPayload payload);
    }

    private sealed class WireParityProbe : IWireParityProbe
    {
        private readonly List<WireParityPayload> _received = [];

        public IReadOnlyList<WireParityPayload> Received
        {
            get { lock (_received) return [.. _received]; }
        }

        public Task RunAsync(WireParityPayload payload)
        {
            lock (_received)
                _received.Add(payload);
            return Task.CompletedTask;
        }
    }

    private static WorkerJobEnvelope Envelope(object? argument, string correlationId) => new()
    {
        Call = new ReflectionCallDto
        {
            ServiceInterfaceFullName = typeof(IWireParityProbe).FullName!,
            MethodName = nameof(IWireParityProbe.RunAsync),
            Params = [CallbackParam.ForValue(argument)]
        },
        CorrelationId = correlationId
    };

    private static (ServiceProvider Provider, InMemoryWorkerTransport Transport, InMemoryWorkerHost Host, WireParityProbe Probe) CreateHost(
        TimeProvider? timeProvider = null)
    {
        var probe = new WireParityProbe();
        var provider = new ServiceCollection()
            .AddSingleton<IWireParityProbe>(probe)
            .BuildServiceProvider();
        var transport = new InMemoryWorkerTransport(
            Microsoft.Extensions.Options.Options.Create(new InMemoryWorkerTransportOptions()), timeProvider);
        var executor = new WorkerJobExecutor(
            provider.GetRequiredService<IServiceScopeFactory>(),
            NullLogger<WorkerJobExecutor>.Instance);
        var host = new InMemoryWorkerHost(transport, executor, NullLogger<InMemoryWorkerHost>.Instance);
        return (provider, transport, host, probe);
    }

    [Fact]
    public async Task JsonIgnoredArgumentState_IsNotDeliveredToTheWorker()
    {
        var (provider, transport, host, probe) = CreateHost();

        await transport.PublishAsync(Envelope(
            new WireParityPayload { Name = "wire", InProcessOnly = "in-process-only" },
            "wire-parity-ignore"));

        // StartAsync then StopAsync: the drain contract executes every accepted job before returning.
        await host.StartAsync(CancellationToken.None);
        await host.StopAsync(CancellationToken.None);

        var received = Assert.Single(probe.Received);
        Assert.Equal("wire", received.Name);
        Assert.Null(received.InProcessOnly);
        await provider.DisposeAsync();
    }

    [Fact]
    public async Task PostPublishMutation_IsInvisibleToTheWorker()
    {
        var (provider, transport, host, probe) = CreateHost();

        var argument = new WireParityPayload { Name = "original" };
        await transport.PublishAsync(Envelope(argument, "wire-parity-mutation"));
        argument.Name = "mutated-after-publish";

        await host.StartAsync(CancellationToken.None);
        await host.StopAsync(CancellationToken.None);

        var received = Assert.Single(probe.Received);
        Assert.Equal("original", received.Name);
        await provider.DisposeAsync();
    }

    [Fact]
    public async Task DelayedJob_PostPublishMutation_IsInvisibleToTheWorker()
    {
        var clock = new AsyncResponse.Testing.VirtualTimeProvider(AsyncResponse.Testing.VirtualTimeProvider.DefaultStartTime);
        var (provider, transport, host, probe) = CreateHost(clock);

        var argument = new WireParityPayload { Name = "original" };
        await ((IDelayedWorkerTransport)transport).PublishAsync(
            Envelope(argument, "wire-parity-delayed"), TimeSpan.FromMinutes(10));
        argument.Name = "mutated-after-publish";

        clock.Advance(TimeSpan.FromMinutes(10));

        await host.StartAsync(CancellationToken.None);
        await host.StopAsync(CancellationToken.None);

        var received = Assert.Single(probe.Received);
        Assert.Equal("original", received.Name);
        await provider.DisposeAsync();
    }

    [Fact]
    public async Task NonSerializableArgument_ThrowsAtPublish_NotAtTheWorker()
    {
        var (provider, transport, host, probe) = CreateHost();

        // A Stream argument is the canonical non-wire-safe value: every broker transport fails it
        // inside PublishAsync when the envelope is serialized. The in-memory transport must fail
        // at the same point instead of executing the job with state no broker could deliver.
        await Assert.ThrowsAnyAsync<Exception>(() =>
            transport.PublishAsync(Envelope(new MemoryStream([1, 2, 3]), "wire-parity-stream")));

        // The failed publish never counts as outstanding, so shutdown does not wait on it.
        Assert.Equal(0, transport.OutstandingJobs);

        await host.StartAsync(CancellationToken.None);
        await host.StopAsync(CancellationToken.None);
        Assert.Empty(probe.Received);
        await provider.DisposeAsync();
    }
}
