using AsyncResponse.Channels.Redis;
using Microsoft.Extensions.DependencyInjection;
using StackExchange.Redis;
using Xunit;

namespace AsyncResponse.IntegrationTests;

/// <summary>
/// Round 40: the Redis recovery scan against a real server and a real multiplexer — the two
/// things the unit tests mock. Connected, it must yield every registration across several
/// pipelined read batches; with nothing to connect to, it must FAIL rather than enumerate an
/// empty keyspace (the old behaviour, which turned a Redis outage into a Healthy recovery check).
/// </summary>
[Collection(DataCollection.Name)]
[Trait(Batches.Trait, Batches.Data)]
public sealed class RedisRecoveryScanIntegrationTests(DataBatchFixture fixture)
{
    [Fact]
    public async Task Scan_YieldsEveryRegistration_AcrossPipelinedBatches()
    {
        await using var multiplexer = await ConnectionMultiplexer.ConnectAsync(fixture.RedisConnectionString);
        await using var provider = BuildProvider(multiplexer);
        var store = provider.GetRequiredService<IRecoveryStateStore>();
        var scanner = provider.GetRequiredService<IRecoveryStateScanner>();

        // 300 keys: two full 128-read batches and a remainder, over more than one SCAN page.
        var expected = Enumerable.Range(0, 300).Select(index => $"scan-{index}").ToHashSet(StringComparer.Ordinal);
        foreach (var chunk in expected.Chunk(50))
        {
            await Task.WhenAll(chunk.Select(correlationId => store.SaveAsync(
                correlationId,
                new RecoveryState
                {
                    RegistrationId = Guid.NewGuid(),
                    CorrelationId = correlationId,
                    RegisteredAtUtc = DateTime.UtcNow
                },
                TimeSpan.FromMinutes(2))));
        }

        var scanned = new HashSet<string>(StringComparer.Ordinal);
        await foreach (var state in scanner.ScanAsync())
            scanned.Add(state.CorrelationId!);

        Assert.Equal(expected.Count, scanned.Count);
        Assert.True(expected.SetEquals(scanned));
    }

    [Fact]
    public async Task Scan_WithRedisUnreachable_FailsInsteadOfReportingAnEmptyKeyspace()
    {
        // A real multiplexer that never connected: port 1 has no listener. AbortOnConnectFail is
        // off so it behaves like a deployed application whose Redis went away, not like startup.
        var options = ConfigurationOptions.Parse("127.0.0.1:1");
        options.AbortOnConnectFail = false;
        options.ConnectTimeout = 500;
        options.ConnectRetry = 1;
        await using var multiplexer = await ConnectionMultiplexer.ConnectAsync(options);
        await using var provider = BuildProvider(multiplexer);
        var scanner = provider.GetRequiredService<IRecoveryStateScanner>();

        var failure = await Assert.ThrowsAsync<RedisConnectionException>(async () =>
        {
            await foreach (var _ in scanner.ScanAsync())
            {
            }
        });

        Assert.Contains("no Redis primary is connected", failure.Message, StringComparison.Ordinal);
    }

    private static ServiceProvider BuildProvider(IConnectionMultiplexer multiplexer)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(multiplexer);
        services.AddAsyncResponse().WithRedisChannel(options =>
        {
            // Unique prefix per test: the scan pattern is prefix-scoped, so neither the other
            // suites sharing this Redis nor a rerun can add or hide a registration.
            options.KeyPrefix = $"scan:{Guid.NewGuid():N}";
            options.RecoveryStateExpiry = TimeSpan.FromMinutes(2);
        });
        return services.BuildServiceProvider();
    }
}
