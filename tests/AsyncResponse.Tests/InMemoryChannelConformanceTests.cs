using AsyncResponse.Conformance;
using Microsoft.Extensions.DependencyInjection;

namespace AsyncResponse.Tests;

/// <summary>
/// The channel behavioral contract (see <see cref="ChannelConformanceSuite"/>) run against the
/// in-memory channel — pure DI, no infrastructure. The in-memory channel is the reference
/// implementation the contract was calibrated against; the integration-test project runs the same
/// suite against the Redis, NATS, PostgreSQL, SQL Server, and MongoDB channels.
/// </summary>
public sealed class InMemoryChannelConformanceTests : ChannelConformanceSuite
{
    // The in-memory channel completes in-process and synchronously; 2s is already generous.
    protected override TimeSpan Generous => TimeSpan.FromSeconds(2);

    protected override Task<ChannelConformanceHarness> CreateHarnessAsync()
    {
        var services = NewConformanceServices();
        services.AddAsyncResponse().WithInMemoryChannel();
        return Task.FromResult(new ChannelConformanceHarness(services.BuildServiceProvider()));
    }
}
