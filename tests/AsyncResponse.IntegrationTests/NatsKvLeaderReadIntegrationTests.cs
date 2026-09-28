using AsyncResponse.Channels.NATS;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.KeyValueStore;
using NATS.Net;
using Xunit;

namespace AsyncResponse.IntegrationTests;

/// <summary>
/// The NATS recovery bucket read through the stream leader's message-get API (round 50) against a
/// real JetStream server. The unit tests script the leader's answers; this pins that the real
/// server answers them the way the adapter reads them — the revision the client reports, a delete
/// and a purge marker read as absence, a server subject-delete marker (<c>Nats-Marker-Reason</c>)
/// too, a create written over a marker, and UTF-8 values intact.
/// </summary>
[Collection(DataCollection.Name)]
[Trait(Batches.Trait, Batches.Data)]
public sealed class NatsKvLeaderReadIntegrationTests(DataBatchFixture fixture)
{
    [Fact]
    public async Task LeaderReads_KeepTheClientsReadingOfEveryEntryShape()
    {
        await using var connection = new NatsConnection(new NatsOpts { Url = fixture.NatsConnectionString });
        await connection.ConnectAsync();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        var ct = timeout.Token;
        var options = new NatsAsyncResponseChannelOptions
        {
            RecoveryBucket = $"leader-read-{Guid.NewGuid():N}",
            RecoveryStateExpiry = TimeSpan.FromMinutes(5)
        };
        var kvContext = connection.CreateKeyValueStoreContext();
        var adapter = new NatsKvStoreAdapter(kvContext, options);

        try
        {
            Assert.Null(await adapter.GetAsync("missing", ct));

            // A value, at the revision the client itself reports for it.
            Assert.True(await adapter.TryCreateAsync("key", "v1", ct));
            var store = await kvContext.GetStoreAsync(options.RecoveryBucket, ct);
            var first = await adapter.GetAsync("key", ct);
            Assert.NotNull(first);
            Assert.Equal("v1", first.Value.Value);
            Assert.Equal((await store.GetEntryAsync<string>("key", cancellationToken: ct)).Revision, first.Value.Revision);
            Assert.False(await adapter.TryCreateAsync("key", "conflict", ct));
            Assert.True(await adapter.TryUpdateAsync("key", "v2", first.Value.Revision, ct));

            // A delete marker is absence, and a create writes over it at the marker's revision.
            var second = (await adapter.GetAsync("key", ct))!.Value;
            Assert.Equal("v2", second.Value);
            Assert.True(await adapter.TryDeleteAsync("key", second.Revision, ct));
            Assert.Null(await adapter.GetAsync("key", ct));
            Assert.True(await adapter.TryCreateAsync("key", "v3", ct));
            Assert.Equal("v3", (await adapter.GetAsync("key", ct))!.Value.Value);

            // A purge marker (KV-Operation: PURGE with a subject rollup) is absence too.
            await store.PurgeAsync("key", cancellationToken: ct);
            Assert.Null(await adapter.GetAsync("key", ct));

            // UTF-8 values round-trip byte for byte.
            Assert.True(await adapter.TryCreateAsync("text", "значение ✓ 値", ct));
            Assert.Equal("значение ✓ 値", (await adapter.GetAsync("text", ct))!.Value.Value);

            // End to end through the recovery store.
            var recovery = new NatsRecoveryStateStore(adapter, Options.Create(options), NullLogger<NatsRecoveryStateStore>.Instance);
            var registration = new RecoveryState
            {
                RegistrationId = Guid.NewGuid(),
                CorrelationId = "leader-read-cid",
                PayloadTypeFullName = typeof(string).FullName,
                RegisteredAtUtc = DateTime.UtcNow
            };
            await recovery.SaveAsync("leader-read-cid", registration, TimeSpan.FromMinutes(5), ct);
            Assert.Equal(registration.RegistrationId, Assert.Single(await recovery.GetAllAsync("leader-read-cid", ct)).RegistrationId);
            Assert.True(await recovery.TryDeleteAsync("leader-read-cid", registration.RegistrationId, ct));
            Assert.Empty(await recovery.GetAllAsync("leader-read-cid", ct));
        }
        finally
        {
            await DeleteBucketAsync(kvContext, options.RecoveryBucket);
        }
    }

    /// <summary>
    /// A bucket with subject-delete markers: when a value outlives the bucket's max age the server
    /// removes it and writes a marker carrying <c>Nats-Marker-Reason: MaxAge</c> and no
    /// <c>KV-Operation</c> — absence, as NATS.Net reads it.
    /// </summary>
    [Fact]
    public async Task LeaderReads_AServerMaxAgeMarker_IsAbsence()
    {
        await using var connection = new NatsConnection(new NatsOpts { Url = fixture.NatsConnectionString });
        await connection.ConnectAsync();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        var ct = timeout.Token;
        var options = new NatsAsyncResponseChannelOptions
        {
            RecoveryBucket = $"leader-marker-{Guid.NewGuid():N}",
            RecoveryStateExpiry = TimeSpan.FromMinutes(5)
        };
        var kvContext = connection.CreateKeyValueStoreContext();

        try
        {
            // Operator-provisioned (an existing bucket is used as it is).
            await kvContext.CreateStoreAsync(
                new NatsKVConfig(options.RecoveryBucket) { MaxAge = TimeSpan.FromSeconds(1), LimitMarkerTTL = TimeSpan.FromSeconds(30) },
                ct);
            var adapter = new NatsKvStoreAdapter(kvContext, options);
            Assert.True(await adapter.TryCreateAsync("key", "short-lived", ct));

            var store = await kvContext.GetStoreAsync(options.RecoveryBucket, ct);
            var stream = await connection.CreateJetStreamContext().GetStreamAsync("KV_" + options.RecoveryBucket, cancellationToken: ct);
            while (true)
            {
                var latest = await stream.GetAsync(new NATS.Client.JetStream.Models.StreamMsgGetRequest { LastBySubj = $"$KV.{options.RecoveryBucket}.key" }, ct);
                if (latest.Message.Hdrs is { } headers && System.Text.Encoding.UTF8.GetString(Convert.FromBase64String(headers)).Contains("Nats-Marker-Reason", StringComparison.Ordinal))
                    break;
                await Task.Delay(200, ct);
            }

            Assert.Null(await adapter.GetAsync("key", ct));
            await Assert.ThrowsAsync<NatsKVKeyDeletedException>(async () => await store.GetEntryAsync<string>("key", cancellationToken: ct));
        }
        finally
        {
            await DeleteBucketAsync(kvContext, options.RecoveryBucket);
        }
    }

    private static async Task DeleteBucketAsync(INatsKVContext kvContext, string bucket)
    {
        try
        {
            await kvContext.DeleteStoreAsync(bucket, CancellationToken.None);
        }
        catch (NatsJSApiException)
        {
            // Never created (an earlier step failed); nothing to clean up.
        }
    }
}
