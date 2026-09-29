using AsyncResponse.Channels.NATS;
using AsyncResponse.Channels.Redis;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NATS.Client.Core;
using NATS.Client.KeyValueStore;
using NATS.Net;
using StackExchange.Redis;
using System.Text.Json;
using Xunit;

namespace AsyncResponse.IntegrationTests;

/// <summary>
/// Round 51 against real Redis and NATS servers: the stores that keep every registration of a
/// correlation id in ONE value (F2) — a burst of concurrent registrations, and of concurrent
/// completions, on one id all commit (four immediate attempts let only about four of a burst
/// through) — and a stored value whose expiry cannot be established (F3) is refused as unreadable
/// and left in place, never read as expired.
/// </summary>
[Collection(DataCollection.Name)]
[Trait(Batches.Trait, Batches.Data)]
public sealed class RecoveryFanOutIntegrationTests(DataBatchFixture fixture)
{
    private const int Burst = 48;

    private static RecoveryState Registration(string correlationId) => new()
    {
        RegistrationId = Guid.NewGuid(),
        CorrelationId = correlationId,
        PayloadTypeFullName = typeof(string).FullName,
        RegisteredAtUtc = DateTime.UtcNow
    };

    [Fact]
    public async Task Redis_ABurstOfRegistrationsAndCompletionsOnOneId_AllCommit()
    {
        await using var multiplexer = await ConnectionMultiplexer.ConnectAsync(fixture.RedisConnectionString);
        await using var provider = RedisProvider(multiplexer, out var prefix);
        var store = provider.GetRequiredService<IRecoveryStateStore>();
        var registrations = Enumerable.Range(0, Burst).Select(_ => Registration("burst")).ToList();

        await Task.WhenAll(registrations.Select(registration => store.SaveAsync("burst", registration, TimeSpan.FromMinutes(2))));
        Assert.Equal(Burst, (await store.GetAllAsync("burst")).Count);

        var removed = await Task.WhenAll(registrations.Select(registration => store.TryDeleteAsync("burst", registration.RegistrationId)));
        Assert.All(removed, Assert.True);
        Assert.False(await multiplexer.GetDatabase().KeyExistsAsync($"{prefix}:recovery:burst"));
    }

    [Fact]
    public async Task Redis_AnEntryWithoutItsExpiry_IsRefused_AndLeftInPlace()
    {
        await using var multiplexer = await ConnectionMultiplexer.ConnectAsync(fixture.RedisConnectionString);
        await using var provider = RedisProvider(multiplexer, out var prefix);
        var store = provider.GetRequiredService<IRecoveryStateStore>();
        var key = $"{prefix}:recovery:no-expiry";
        var damaged = $"{{\"Registrations\":[{{\"State\":{JsonSerializer.Serialize(Registration("no-expiry"))}}}]}}";
        await multiplexer.GetDatabase().StringSetAsync(key, damaged, TimeSpan.FromMinutes(2));

        await Assert.ThrowsAsync<RecoveryStateUnreadableException>(() => store.GetAllAsync("no-expiry"));
        await Assert.ThrowsAsync<RecoveryStateUnreadableException>(() => store.SaveAsync("no-expiry", Registration("no-expiry"), TimeSpan.FromMinutes(2)));

        Assert.Equal(damaged, (string?)await multiplexer.GetDatabase().StringGetAsync(key));
    }

    [Fact]
    public async Task Nats_ABurstOfRegistrationsAndCompletionsOnOneId_AllCommit()
    {
        await using var connection = new NatsConnection(new NatsOpts { Url = fixture.NatsConnectionString });
        await connection.ConnectAsync();
        var (kvContext, options, adapter, store) = NatsStore(connection);
        try
        {
            var registrations = Enumerable.Range(0, Burst).Select(_ => Registration("burst")).ToList();

            await Task.WhenAll(registrations.Select(registration => store.SaveAsync("burst", registration, TimeSpan.FromMinutes(2))));
            Assert.Equal(Burst, (await store.GetAllAsync("burst")).Count);

            var removed = await Task.WhenAll(registrations.Select(registration => store.TryDeleteAsync("burst", registration.RegistrationId)));
            Assert.All(removed, Assert.True);
            Assert.Null(await adapter.GetAsync(NatsSubjectSchema.RecoveryKey("burst"), CancellationToken.None));
        }
        finally
        {
            await DeleteBucketAsync(kvContext, options.RecoveryBucket);
        }
    }

    [Fact]
    public async Task Nats_AnEnvelopeWithoutItsExpiry_IsRefused_AndLeftInPlace()
    {
        await using var connection = new NatsConnection(new NatsOpts { Url = fixture.NatsConnectionString });
        await connection.ConnectAsync();
        var (kvContext, options, adapter, store) = NatsStore(connection);
        try
        {
            await store.SaveAsync("no-expiry", Registration("no-expiry"), TimeSpan.FromMinutes(2));
            var key = NatsSubjectSchema.RecoveryKey("no-expiry");
            var entry = (await adapter.GetAsync(key, CancellationToken.None))!.Value;
            var envelope = System.Text.Json.Nodes.JsonNode.Parse(entry.Value)!.AsObject();
            envelope.Remove("ExpiresAtUtc");
            var damaged = envelope.ToJsonString();
            Assert.True(await adapter.TryUpdateAsync(key, damaged, entry.Revision, CancellationToken.None));

            await Assert.ThrowsAsync<RecoveryStateUnreadableException>(() => store.GetAllAsync("no-expiry"));
            var unreadable = await Assert.ThrowsAsync<RecoveryStateScanUnreadableException>(async () =>
            {
                await foreach (var _ in store.ScanAsync())
                {
                }
            });
            Assert.Equal(1, unreadable.UnreadableCount);

            Assert.Equal(damaged, (await adapter.GetAsync(key, CancellationToken.None))!.Value.Value);
        }
        finally
        {
            await DeleteBucketAsync(kvContext, options.RecoveryBucket);
        }
    }

    private static ServiceProvider RedisProvider(IConnectionMultiplexer multiplexer, out string prefix)
    {
        var keyPrefix = $"fanout:{Guid.NewGuid():N}";
        prefix = keyPrefix;
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(multiplexer);
        services.AddAsyncResponse().WithRedisChannel(options =>
        {
            options.KeyPrefix = keyPrefix;
            options.RecoveryStateExpiry = TimeSpan.FromMinutes(2);
        });
        return services.BuildServiceProvider();
    }

    private static (INatsKVContext Context, NatsAsyncResponseChannelOptions Options, NatsKvStoreAdapter Adapter, NatsRecoveryStateStore Store) NatsStore(NatsConnection connection)
    {
        var options = new NatsAsyncResponseChannelOptions
        {
            RecoveryBucket = $"fan-out-{Guid.NewGuid():N}",
            RecoveryStateExpiry = TimeSpan.FromMinutes(5)
        };
        var kvContext = connection.CreateKeyValueStoreContext();
        var adapter = new NatsKvStoreAdapter(kvContext, options);
        return (kvContext, options, adapter, new NatsRecoveryStateStore(adapter, Options.Create(options), NullLogger<NatsRecoveryStateStore>.Instance));
    }

    private static async Task DeleteBucketAsync(INatsKVContext kvContext, string bucket)
    {
        try
        {
            await kvContext.DeleteStoreAsync(bucket, CancellationToken.None);
        }
        catch (Exception)
        {
            // Best effort: a bucket per test, named uniquely, so a leftover harms nothing.
        }
    }
}
