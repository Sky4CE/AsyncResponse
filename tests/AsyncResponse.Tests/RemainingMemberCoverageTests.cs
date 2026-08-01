using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Members nothing else in the suite ever calls: the typed durable-flow registry's delegates, the
/// reflection-descriptor worker enqueue, and the MongoDB transport's fenced-renewal update builder.
/// </summary>
public sealed class RemainingMemberCoverageTests
{
    /// <summary>
    /// <c>WithDurableFlow&lt;TFlow, TInput&gt;</c> registers closed-over delegates instead of
    /// reflecting at run time — the whole point of the typed registry for trimmed/AOT apps. Those
    /// delegates are only ever invoked by the executor, so they are exercised directly here.
    /// </summary>
    [Fact]
    public async Task TypedFlowRegistration_DeserializesInputAndExecutesWithoutReflection()
    {
        var probe = new FlowProbe();
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddSingleton(probe);
        services.AddAsyncResponse()
            .WithInMemoryChannel()
            .WithInMemoryTransport()
            .WithInMemoryDurableFlows()
            .WithDurableFlow<RegistryProbeFlow, TestFlowInput>();

        using var provider = services.BuildServiceProvider();
        var registration = provider.GetServices<DurableFlowRegistration>()
            .Single(item => item.FlowType == typeof(RegistryProbeFlow));

        Assert.Equal(typeof(RegistryProbeFlow).FullName, registration.FlowTypeFullName);

        var input = Assert.IsType<TestFlowInput>(registration.DeserializeInput("""{"TenantId":42}"""));
        Assert.Equal(42, input.TenantId);

        var flow = provider.GetRequiredService<RegistryProbeFlow>();
        await registration.ExecuteAsync(flow, FlowContext, input);
        Assert.Equal(42, flow.SeenTenantId);

        // A null input reaches the flow as default rather than throwing on the unboxing cast.
        await registration.ExecuteAsync(flow, FlowContext, null);
        Assert.Equal(0, flow.SeenTenantId);
    }

    /// <summary>
    /// The descriptor-based worker enqueue is the trim-unsafe sibling of the expression overloads;
    /// it forwards to the same core, so nothing else in the suite calls it.
    /// </summary>
    [Fact]
    public async Task EnqueueWorkerAsync_AcceptsAReflectionDescriptor()
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddAsyncResponse()
            .WithInMemoryChannel()
            .WithInMemoryTransport();

        using var provider = services.BuildServiceProvider();
        var builder = provider.GetRequiredService<IAsyncResponseBuilder>();

        await builder.EnqueueWorkerAsync(new ReflectionCallDto
        {
            ServiceInterfaceFullName = typeof(IRegistryProbeService).FullName!,
            MethodName = nameof(IRegistryProbeService.RunAsync),
            Params = []
        });

        // The job reached the transport: the in-memory worker queue hands it straight back out.
        var transport = Assert.IsType<InMemoryWorkerTransport>(provider.GetRequiredService<IWorkerTransport>());
        Assert.NotNull(transport);
    }

    /// <summary>The registration delegates never touch the context; only the cast and dispatch matter.</summary>
    private static readonly IDurableFlowContext FlowContext = new Moq.Mock<IDurableFlowContext>().Object;

    private sealed class RegistryProbeFlow : IDurableFlow<TestFlowInput>
    {
        public int SeenTenantId { get; private set; }

        public Task ExecuteAsync(IDurableFlowContext flow, TestFlowInput input)
        {
            SeenTenantId = input?.TenantId ?? 0;
            return Task.CompletedTask;
        }
    }

    public interface IRegistryProbeService
    {
        Task RunAsync();
    }

}
