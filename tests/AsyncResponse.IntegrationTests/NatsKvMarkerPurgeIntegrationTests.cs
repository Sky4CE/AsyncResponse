using AsyncResponse.Channels.NATS;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.KeyValueStore;
using NATS.Net;
using Xunit;

namespace AsyncResponse.IntegrationTests;

/// <summary>
/// The NATS recovery bucket's delete-marker maintenance against a real JetStream KV bucket. The
/// unit tests drive it through mocks only, and the conformance runs reach it fire-and-forget from
/// the watchdog scan, where a failure is merely logged — so nothing else would notice if the real
/// path (a metadata-only watch whose empty payloads deserialize as <c>int</c>, then a
/// <c>Filter</c> + <c>Seq = revision + 1</c> stream purge per marker) never purged, or purged the
/// wrong thing. Runs in the data batch beside the NATS channel conformance tests, which own the
/// batch's direct NATS connection.
/// </summary>
[Collection(DataCollection.Name)]
[Trait(Batches.Trait, Batches.Data)]
public sealed class NatsKvMarkerPurgeIntegrationTests(DataBatchFixture fixture)
{
    [Fact]
    public async Task PurgeDeleteMarkersAsync_RemovesOnlyTheMarkers_AndNeverAValueWrittenAfterOne()
    {
        await using var connection = new NatsConnection(new NatsOpts { Url = fixture.NatsConnectionString });
        await connection.ConnectAsync();

        // A bucket of its own: the purge count and the stream's message count are then exact.
        var options = new NatsAsyncResponseChannelOptions
        {
            RecoveryBucket = $"marker-purge-{Guid.NewGuid():N}",
            RecoveryStateExpiry = TimeSpan.FromMinutes(5)
        };
        var kvContext = connection.CreateKeyValueStoreContext();
        var adapter = new NatsKvStoreAdapter(kvContext, options);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var ct = timeout.Token;

        try
        {
            // A: put, delete, put again — its latest entry is a value written AFTER a marker.
            Assert.True(await adapter.TryCreateAsync("key-a", "a1", ct));
            Assert.True(await adapter.TryDeleteAsync("key-a", (await adapter.GetAsync("key-a", ct))!.Value.Revision, ct));
            Assert.True(await adapter.TryCreateAsync("key-a", "a2", ct));

            // B: put, delete — its latest entry is the marker.
            Assert.True(await adapter.TryCreateAsync("key-b", "b1", ct));
            Assert.True(await adapter.TryDeleteAsync("key-b", (await adapter.GetAsync("key-b", ct))!.Value.Revision, ct));

            // C: a live key nobody touches.
            Assert.True(await adapter.TryCreateAsync("key-c", "c1", ct));

            var store = await kvContext.GetStoreAsync(options.RecoveryBucket, ct);
            await Assert.ThrowsAsync<NatsKVKeyDeletedException>(async () => await store.GetEntryAsync<string>("key-b", cancellationToken: ct));

            // Negative: every marker written so far is old enough whatever the container's clock
            // says (the cutoff compares the SERVER's marker timestamps with this process's clock).
            var purged = await adapter.PurgeDeleteMarkersAsync(TimeSpan.FromMinutes(-5), ct);

            Assert.Equal(1, purged);

            // B's marker is gone: the key is now absent, not deleted.
            await Assert.ThrowsAsync<NatsKVKeyNotFoundException>(async () => await store.GetEntryAsync<string>("key-b", cancellationToken: ct));

            // A's value written after its marker survived, and so did the untouched key.
            Assert.Equal("a2", (await adapter.GetAsync("key-a", ct))!.Value.Value);
            Assert.Equal("c1", (await adapter.GetAsync("key-c", ct))!.Value.Value);

            var keys = new List<string>();
            await foreach (var key in adapter.GetKeysAsync(ct))
                keys.Add(key);
            Assert.Equal(["key-a", "key-c"], keys.Order(StringComparer.Ordinal));

            // Exactly the two live values are left on the backing stream.
            var stream = await connection.CreateJetStreamContext().GetStreamAsync("KV_" + options.RecoveryBucket, cancellationToken: ct);
            Assert.Equal(2, stream.Info.State.Messages);

            // A second pass finds nothing left to purge.
            Assert.Equal(0, await adapter.PurgeDeleteMarkersAsync(TimeSpan.FromMinutes(-5), ct));
        }
        finally
        {
            try
            {
                await kvContext.DeleteStoreAsync(options.RecoveryBucket, CancellationToken.None);
            }
            catch (NatsJSApiException)
            {
                // Never created (an earlier step failed); nothing to clean up.
            }
        }
    }
}
