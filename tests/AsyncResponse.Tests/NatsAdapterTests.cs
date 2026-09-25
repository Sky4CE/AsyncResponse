using AsyncResponse.Channels.NATS;
using AsyncResponse.Transports.NATS;
using Microsoft.Extensions.Logging;
using Moq;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using NATS.Client.KeyValueStore;
using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Xunit;

namespace AsyncResponse.Tests;

public class NatsResponseChannelClientTests
{
    private readonly Mock<INatsRawRequester> _raw = new();

    [Fact]
    public async Task RequestAsync_MapsReplyToReplied()
    {
        _raw.Setup(r => r.RequestAsync("subj", "payload", It.IsAny<NatsHeaders>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        var client = new NatsResponseChannelClient(_raw.Object);

        Assert.Equal(NatsDeliveryOutcome.Replied, await client.RequestAsync("subj", "payload", probe: false, TimeSpan.FromSeconds(1), CancellationToken.None));
    }

    [Fact]
    public async Task RequestAsync_MapsNoRespondersAndNoReply()
    {
        var client = new NatsResponseChannelClient(_raw.Object);

        _raw.Setup(r => r.RequestAsync(It.IsAny<string>(), It.IsAny<string?>(), It.IsAny<NatsHeaders>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new NatsNoRespondersException());
        Assert.Equal(NatsDeliveryOutcome.NoResponders, await client.RequestAsync("s", null, probe: true, TimeSpan.FromSeconds(1), CancellationToken.None));

        _raw.Setup(r => r.RequestAsync(It.IsAny<string>(), It.IsAny<string?>(), It.IsAny<NatsHeaders>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new NatsNoReplyException());
        Assert.Equal(NatsDeliveryOutcome.NoReply, await client.RequestAsync("s", "p", probe: false, TimeSpan.FromSeconds(1), CancellationToken.None));
    }

    [Fact]
    public async Task RequestAsync_PayloadAboveTheServersMaxPayload_IsUnprocessableNotTransient()
    {
        // NatsPayloadTooLargeException is deterministic (the client refuses the message before
        // sending it), but as a NatsException the ingress retried it as transient before
        // escalating. InvalidDataException is the ingress's "escalate now" signal.
        var tooLarge = new NatsPayloadTooLargeException("Payload size 2000000 exceeds server's maximum payload size 1048576");
        _raw.Setup(r => r.RequestAsync(It.IsAny<string>(), It.IsAny<string?>(), It.IsAny<NatsHeaders>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(tooLarge);
        var client = new NatsResponseChannelClient(_raw.Object);

        var ex = await Assert.ThrowsAsync<InvalidDataException>(
            () => client.RequestAsync("subj", "big", probe: false, TimeSpan.FromSeconds(1), CancellationToken.None));
        Assert.Same(tooLarge, ex.InnerException);
        Assert.Contains("max_payload", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task RequestAsync_Probe_SetsProbeHeader()
    {
        NatsHeaders? captured = null;
        _raw.Setup(r => r.RequestAsync(It.IsAny<string>(), It.IsAny<string?>(), It.IsAny<NatsHeaders>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
            .Callback<string, string?, NatsHeaders?, TimeSpan, CancellationToken>((_, _, headers, _, _) => captured = headers)
            .Returns(Task.CompletedTask);
        var client = new NatsResponseChannelClient(_raw.Object);

        await client.RequestAsync("s", null, probe: true, TimeSpan.FromSeconds(1), CancellationToken.None);

        Assert.NotNull(captured);
        Assert.True(captured!.TryGetValue("AR-Probe", out var marker) && marker == "1");
    }

    [Fact]
    public async Task FlushAsync_ForwardsToRaw()
    {
        _raw.Setup(r => r.FlushAsync(It.IsAny<CancellationToken>())).Returns(Task.CompletedTask);
        var client = new NatsResponseChannelClient(_raw.Object);

        await client.FlushAsync(CancellationToken.None);

        _raw.Verify(r => r.FlushAsync(It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task RawRequester_ForwardsRequestWithTimeout()
    {
        var connection = new Mock<INatsConnection>();
        var timeout = TimeSpan.FromSeconds(3);
        NatsSubOpts? capturedReplyOptions = null;
        NatsHeaders? capturedHeaders = null;
        using var cts = new CancellationTokenSource();
        connection
            .Setup(c => c.RequestAsync<string?, string>(
                "subject",
                "payload",
                It.IsAny<NatsHeaders?>(),
                It.IsAny<INatsSerialize<string?>?>(),
                It.IsAny<INatsDeserialize<string>?>(),
                It.IsAny<NatsPubOpts?>(),
                It.IsAny<NatsSubOpts?>(),
                cts.Token))
            .Callback<string, string?, NatsHeaders?, INatsSerialize<string?>?, INatsDeserialize<string>?, NatsPubOpts?, NatsSubOpts?, CancellationToken>(
                (_, _, headers, _, _, _, replyOptions, _) =>
                {
                    capturedHeaders = headers;
                    capturedReplyOptions = replyOptions;
                })
            .ReturnsAsync(new NatsMsg<string>("reply", replyTo: null, 0, headers: null, data: "ack", connection: null));
        var requester = new NatsRawRequester(connection.Object);
        var headers = new NatsHeaders { ["h"] = "v" };

        await requester.RequestAsync("subject", "payload", headers, timeout, cts.Token);

        Assert.Same(headers, capturedHeaders);
        Assert.NotNull(capturedReplyOptions);
        Assert.Equal(timeout, capturedReplyOptions!.Timeout);
    }

    [Fact]
    public async Task RawRequester_PinsThrowIfNoResponders_SoTheLostSubscriberSignalCannotBeInherited()
    {
        // Regression: the reply options carried only the timeout, so whether a no-responders 503
        // THROWS followed the app-supplied connection's RequestReplyMode. Under Direct the sentinel
        // arrives as an ordinary reply that the requester discards — the channel then reports a
        // dead subject as answered (and alive to the liveness probe) and drops the response. The
        // channel's entire lost-subscriber contract rests on the throw, so it is pinned per call.
        var connection = new Mock<INatsConnection>();
        NatsSubOpts? capturedReplyOptions = null;
        connection
            .Setup(c => c.RequestAsync<string?, string>(
                "subject",
                "payload",
                It.IsAny<NatsHeaders?>(),
                It.IsAny<INatsSerialize<string?>?>(),
                It.IsAny<INatsDeserialize<string>?>(),
                It.IsAny<NatsPubOpts?>(),
                It.IsAny<NatsSubOpts?>(),
                It.IsAny<CancellationToken>()))
            .Callback<string, string?, NatsHeaders?, INatsSerialize<string?>?, INatsDeserialize<string>?, NatsPubOpts?, NatsSubOpts?, CancellationToken>(
                (_, _, _, _, _, _, replyOptions, _) => capturedReplyOptions = replyOptions)
            .ReturnsAsync(new NatsMsg<string>("reply", replyTo: null, 0, headers: null, data: "ack", connection: null));
        var requester = new NatsRawRequester(connection.Object);

        await requester.RequestAsync("subject", "payload", headers: null, TimeSpan.FromSeconds(1), CancellationToken.None);

        Assert.NotNull(capturedReplyOptions);
        Assert.True(capturedReplyOptions!.ThrowIfNoResponders);
    }

    [Fact]
    public async Task RawRequester_ForwardsSubscribePublishReplyAndFlush()
    {
        var connection = new Mock<INatsConnection>();
        var sub = new Mock<INatsSub<string>>();
        using var cts = new CancellationTokenSource();
        connection
            .Setup(c => c.SubscribeCoreAsync<string>(
                "subject",
                It.IsAny<string?>(),
                It.IsAny<INatsDeserialize<string>?>(),
                It.IsAny<NatsSubOpts?>(),
                cts.Token))
            .ReturnsAsync(sub.Object);
        connection
            .Setup(c => c.PublishAsync<string>(
                "reply",
                string.Empty,
                It.IsAny<NatsHeaders?>(),
                It.IsAny<string?>(),
                It.IsAny<INatsSerialize<string>?>(),
                It.IsAny<NatsPubOpts?>(),
                cts.Token))
            .Returns(ValueTask.CompletedTask);
        connection
            .Setup(c => c.PingAsync(cts.Token))
            .ReturnsAsync(TimeSpan.FromMilliseconds(1));
        var requester = new NatsRawRequester(connection.Object);

        var subscribed = await requester.SubscribeAsync("subject", cts.Token);
        await requester.PublishReplyAsync("reply", cts.Token);
        await requester.FlushAsync(cts.Token);

        Assert.Same(sub.Object, subscribed);
        connection.Verify(c => c.SubscribeCoreAsync<string>("subject", null, It.IsAny<INatsDeserialize<string>?>(), null, cts.Token), Times.Once);
        connection.Verify(c => c.PublishAsync<string>("reply", string.Empty, null, null, It.IsAny<INatsSerialize<string>?>(), null, cts.Token), Times.Once);
        connection.Verify(c => c.PingAsync(cts.Token), Times.Once);
    }

    [Fact]
    public async Task Subscription_MapsMessages_DetectsProbe_AndRepliesWhenReplyToPresent()
    {
        var channel = Channel.CreateUnbounded<NatsMsg<string>>();
        channel.Writer.TryWrite(new NatsMsg<string>("subj", "reply-1", 0, headers: null, data: "payload-1", connection: null));
        channel.Writer.TryWrite(new NatsMsg<string>("subj", replyTo: null, 0, headers: new NatsHeaders { ["AR-Probe"] = "1" }, data: null, connection: null));
        channel.Writer.TryComplete();

        var sub = new Mock<INatsSub<string>>();
        sub.SetupGet(s => s.Msgs).Returns(channel.Reader);
        sub.Setup(s => s.DisposeAsync()).Returns(ValueTask.CompletedTask);
        _raw.Setup(r => r.SubscribeAsync("subj", It.IsAny<CancellationToken>())).ReturnsAsync(sub.Object);
        _raw.Setup(r => r.PublishReplyAsync(It.IsAny<string>(), It.IsAny<CancellationToken>())).Returns(ValueTask.CompletedTask);
        var client = new NatsResponseChannelClient(_raw.Object);

        await using var subscription = await client.SubscribeAsync("subj", CancellationToken.None);
        var received = new List<NatsInboundResponse>();
        await foreach (var message in subscription.ReadAsync(CancellationToken.None))
        {
            received.Add(message);
            await message.ReplyAsync();
        }

        Assert.Equal(2, received.Count);
        Assert.Equal("payload-1", received[0].Payload);
        Assert.False(received[0].IsProbe);
        Assert.True(received[1].IsProbe);
        // Only the message that carried a reply subject triggers an ack publish.
        _raw.Verify(r => r.PublishReplyAsync("reply-1", It.IsAny<CancellationToken>()), Times.Once);
        _raw.Verify(r => r.PublishReplyAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Once);
    }
}

public class NatsKvStoreAdapterTests
{
    private readonly Mock<INatsKVContext> _context = new();
    private readonly Mock<INatsKVStore> _store = new();

    private NatsKvStoreAdapter CreateAdapter()
    {
        BucketNotFound();
        _context.Setup(c => c.CreateStoreAsync(It.IsAny<NatsKVConfig>(), It.IsAny<CancellationToken>())).ReturnsAsync(_store.Object);
        return new NatsKvStoreAdapter(_context.Object, new NatsAsyncResponseChannelOptions());
    }

    /// <summary>The bucket does not exist: JetStream answers the lookup with a 404 "stream not found".</summary>
    private void BucketNotFound()
        => _context
            .Setup(c => c.GetStoreAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new NatsJSApiException(new ApiError { Code = 404, ErrCode = 10059, Description = "stream not found" }));

    private void StoreStatus(TimeSpan maxAge, int replicas)
        => _store
            .Setup(s => s.GetStatusAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(new NatsKVStatus(
                "asyncresponse-recovery",
                IsCompressed: false,
                LimitMarkerTTL: TimeSpan.Zero,
                new StreamInfo { Config = new StreamConfig("KV_asyncresponse-recovery", ["$KV.asyncresponse-recovery.>"]) { MaxAge = maxAge, NumReplicas = replicas } }));

    [Fact]
    public async Task ExistingBucket_IsOpenedAsItIs_NeverRecreated()
    {
        // Regression: the bucket was opened with a create-only CreateStoreAsync on first use, and
        // JetStream answers a create whose configuration differs from the live bucket with 10058.
        // Raising RecoveryStateExpiry or RecoveryBucketReplicas (or an operator-provisioned bucket)
        // then failed every save and every lost-subscriber read — a full channel outage.
        _context.Setup(c => c.GetStoreAsync("asyncresponse-recovery", It.IsAny<CancellationToken>())).ReturnsAsync(_store.Object);
        _context.Setup(c => c.CreateStoreAsync(It.IsAny<NatsKVConfig>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new NatsJSApiException(new ApiError { Code = 400, ErrCode = 10058, Description = "stream name already in use with a different configuration" }));
        _store.Setup(s => s.TryCreateAsync("k", "v", It.IsAny<INatsSerialize<string>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new NatsResult<ulong>(1UL));
        var adapter = new NatsKvStoreAdapter(_context.Object, new NatsAsyncResponseChannelOptions { RecoveryBucketReplicas = 3 });

        Assert.True(await adapter.TryCreateAsync("k", "v", CancellationToken.None));
        _context.Verify(c => c.CreateStoreAsync(It.IsAny<NatsKVConfig>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task BucketCreationRace_LostToADifferentlyConfiguredPeer_OpensThePeersBucket()
    {
        _context.SetupSequence(c => c.GetStoreAsync("asyncresponse-recovery", It.IsAny<CancellationToken>()))
            .ThrowsAsync(new NatsJSApiException(new ApiError { Code = 404, ErrCode = 10059, Description = "stream not found" }))
            .ReturnsAsync(_store.Object);
        _context.Setup(c => c.CreateStoreAsync(It.IsAny<NatsKVConfig>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new NatsJSApiException(new ApiError { Code = 400, ErrCode = 10058, Description = "stream name already in use with a different configuration" }));
        _store.Setup(s => s.TryCreateAsync("k", "v", It.IsAny<INatsSerialize<string>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new NatsResult<ulong>(1UL));
        var adapter = new NatsKvStoreAdapter(_context.Object, new NatsAsyncResponseChannelOptions());

        Assert.True(await adapter.TryCreateAsync("k", "v", CancellationToken.None));
        _context.Verify(c => c.GetStoreAsync("asyncresponse-recovery", It.IsAny<CancellationToken>()), Times.Exactly(2));
    }

    [Fact]
    public async Task MissingBucket_IsCreatedWithTheConfiguredRetentionAndReplicas()
    {
        BucketNotFound();
        _context.Setup(c => c.CreateStoreAsync(It.IsAny<NatsKVConfig>(), It.IsAny<CancellationToken>())).ReturnsAsync(_store.Object);
        _store.Setup(s => s.TryCreateAsync("k", "v", It.IsAny<INatsSerialize<string>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new NatsResult<ulong>(1UL));
        var adapter = new NatsKvStoreAdapter(
            _context.Object,
            new NatsAsyncResponseChannelOptions { RecoveryStateExpiry = TimeSpan.FromDays(3), RecoveryBucketReplicas = 3 });

        await adapter.TryCreateAsync("k", "v", CancellationToken.None);

        _context.Verify(c => c.CreateStoreAsync(
            It.Is<NatsKVConfig>(cfg => cfg.Bucket == "asyncresponse-recovery" && cfg.MaxAge == TimeSpan.FromDays(3) && cfg.History == 1 && cfg.NumberOfReplicas == 3),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task PurgeDeleteMarkersAsync_PurgesOnlyStaleMarkers_EachBoundedByItsOwnSequence()
    {
        // Each purge is sequence-bounded to the marker and anything older on its subject, so a
        // registration written under the same key after the snapshot survives; only markers past
        // the threshold are purged, and the pass ends with the snapshot instead of watching on.
        var old = DateTimeOffset.UtcNow - TimeSpan.FromHours(2);
        _store.Setup(s => s.WatchAsync<int>(It.IsAny<INatsDeserialize<int>?>(), It.IsAny<NatsKVWatchOpts?>(), It.IsAny<CancellationToken>()))
            .Returns(AsyncEnum(
                new NatsKVEntry<int>("asyncresponse-recovery", "live") { Revision = 5, Delta = 3, Created = old, Operation = NatsKVOperation.Put },
                new NatsKVEntry<int>("asyncresponse-recovery", "stale-del") { Revision = 7, Delta = 2, Created = old, Operation = NatsKVOperation.Del },
                new NatsKVEntry<int>("asyncresponse-recovery", "stale-purge") { Revision = 8, Delta = 1, Created = old, Operation = NatsKVOperation.Purge },
                new NatsKVEntry<int>("asyncresponse-recovery", "fresh-del") { Revision = 9, Delta = 0, Created = DateTimeOffset.UtcNow, Operation = NatsKVOperation.Del },
                new NatsKVEntry<int>("asyncresponse-recovery", "after-snapshot") { Revision = 10, Delta = 0, Created = old, Operation = NatsKVOperation.Del }));
        var jetStream = new Mock<INatsJSContext>();
        var purges = new System.Collections.Concurrent.ConcurrentBag<StreamPurgeRequest>();
        jetStream.Setup(j => j.PurgeStreamAsync("KV_asyncresponse-recovery", It.IsAny<StreamPurgeRequest>(), It.IsAny<CancellationToken>()))
            .Callback<string, StreamPurgeRequest, CancellationToken>((_, request, _) => purges.Add(request))
            .ReturnsAsync(new StreamPurgeResponse { Success = true, Purged = 1 });
        _context.SetupGet(c => c.JetStreamContext).Returns(jetStream.Object);
        var adapter = CreateAdapter();

        var purged = await adapter.PurgeDeleteMarkersAsync(TimeSpan.FromMinutes(30), CancellationToken.None);

        Assert.Equal(2, purged);
        Assert.Equal(
            [("$KV.asyncresponse-recovery.stale-del", 8UL, 0UL), ("$KV.asyncresponse-recovery.stale-purge", 9UL, 0UL)],
            purges.Select(p => (p.Filter, p.Seq, p.Keep)).OrderBy(p => p.Filter).ToArray());
    }

    [Fact]
    public async Task ExistingBucketDrift_IsReportedButNeverRewritten()
    {
        _context.Setup(c => c.GetStoreAsync("asyncresponse-recovery", It.IsAny<CancellationToken>())).ReturnsAsync(_store.Object);
        StoreStatus(maxAge: TimeSpan.FromHours(1), replicas: 3);
        _store.Setup(s => s.TryCreateAsync("k", "v", It.IsAny<INatsSerialize<string>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new NatsResult<ulong>(1UL));
        var logger = new RecordingThrowingLogger<NatsKvStoreAdapter>();
        var adapter = new NatsKvStoreAdapter(
            _context.Object,
            new NatsAsyncResponseChannelOptions { RecoveryStateExpiry = TimeSpan.FromDays(7), RecoveryBucketReplicas = 1 },
            logger);

        await adapter.TryCreateAsync("k", "v", CancellationToken.None);

        Assert.True(logger.HasEntry(LogLevel.Warning, "shorter than RecoveryStateExpiry"));
        Assert.True(logger.HasEntry(LogLevel.Warning, "RecoveryBucketReplicas is 1"));
        _context.Verify(c => c.CreateStoreAsync(It.IsAny<NatsKVConfig>(), It.IsAny<CancellationToken>()), Times.Never);
        _context.Verify(c => c.UpdateStoreAsync(It.IsAny<NatsKVConfig>(), It.IsAny<CancellationToken>()), Times.Never);
        _context.Verify(c => c.CreateOrUpdateStoreAsync(It.IsAny<NatsKVConfig>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task TryCreateAsync_ForwardsToStore_AndCreatesBucketLazilyOnce()
    {
        _store.Setup(s => s.TryCreateAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<INatsSerialize<string>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new NatsResult<ulong>(1UL));
        var adapter = CreateAdapter();

        await adapter.TryCreateAsync("k", "v", CancellationToken.None);
        await adapter.TryCreateAsync("k2", "v2", CancellationToken.None);

        _store.Verify(s => s.TryCreateAsync("k", "v", It.IsAny<INatsSerialize<string>>(), It.IsAny<CancellationToken>()), Times.Once);
        _context.Verify(c => c.CreateStoreAsync(It.IsAny<NatsKVConfig>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task GetAsync_ReturnsValue_OrNullWhenMissingOrDeleted()
    {
        _store.Setup(s => s.GetEntryAsync<string>("hit", It.IsAny<ulong>(), It.IsAny<INatsDeserialize<string>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new NatsKVEntry<string>("bucket", "hit") { Value = "value" });
        _store.Setup(s => s.GetEntryAsync<string>("missing", It.IsAny<ulong>(), It.IsAny<INatsDeserialize<string>>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new NatsKVKeyNotFoundException());
        _store.Setup(s => s.GetEntryAsync<string>("deleted", It.IsAny<ulong>(), It.IsAny<INatsDeserialize<string>>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new NatsKVKeyDeletedException(revision: 1));
        var adapter = CreateAdapter();

        var hit = await adapter.GetAsync("hit", CancellationToken.None);
        Assert.NotNull(hit);
        Assert.Equal("value", hit.Value.Value);
        Assert.Null(await adapter.GetAsync("missing", CancellationToken.None));
        Assert.Null(await adapter.GetAsync("deleted", CancellationToken.None));
    }

    [Fact]
    public async Task GetAsync_NullEntryValue_ReturnsNull()
    {
        _store.Setup(s => s.GetEntryAsync<string>("null", It.IsAny<ulong>(), It.IsAny<INatsDeserialize<string>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new NatsKVEntry<string>("bucket", "null") { Value = null });
        var adapter = CreateAdapter();

        Assert.Null(await adapter.GetAsync("null", CancellationToken.None));
    }

    [Fact]
    public async Task TryCreateAndTryUpdate_ReturnStoreSuccess()
    {
        _store.Setup(s => s.TryCreateAsync("new", "v", It.IsAny<INatsSerialize<string>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new NatsResult<ulong>(1UL));
        _store.Setup(s => s.TryCreateAsync("existing", "v", It.IsAny<INatsSerialize<string>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new NatsResult<ulong>(new InvalidOperationException("exists")));
        _store.Setup(s => s.TryUpdateAsync("hit", "v2", 3UL, It.IsAny<INatsSerialize<string>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new NatsResult<ulong>(2UL));
        _store.Setup(s => s.TryUpdateAsync("stale", "v2", 3UL, It.IsAny<INatsSerialize<string>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new NatsResult<ulong>(new InvalidOperationException("stale")));
        var adapter = CreateAdapter();

        Assert.True(await adapter.TryCreateAsync("new", "v", CancellationToken.None));
        Assert.False(await adapter.TryCreateAsync("existing", "v", CancellationToken.None));
        Assert.True(await adapter.TryUpdateAsync("hit", "v2", 3UL, CancellationToken.None));
        Assert.False(await adapter.TryUpdateAsync("stale", "v2", 3UL, CancellationToken.None));
    }

    [Fact]
    public async Task TryDeleteAsync_MapsRevisionConflictAndMissingKeyToFalse()
    {
        _store.Setup(s => s.DeleteAsync("present", It.IsAny<NatsKVDeleteOpts>(), It.IsAny<CancellationToken>()))
            .Returns(ValueTask.CompletedTask);
        _store.Setup(s => s.DeleteAsync("conflict", It.IsAny<NatsKVDeleteOpts>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new NatsKVWrongLastRevisionException(new NATS.Client.JetStream.Models.ApiError()));
        _store.Setup(s => s.DeleteAsync("missing", It.IsAny<NatsKVDeleteOpts>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new NatsKVKeyNotFoundException());
        var adapter = CreateAdapter();

        Assert.True(await adapter.TryDeleteAsync("present", 3UL, CancellationToken.None));
        Assert.False(await adapter.TryDeleteAsync("conflict", 3UL, CancellationToken.None));
        Assert.False(await adapter.TryDeleteAsync("missing", 3UL, CancellationToken.None));
    }

    [Fact]
    public async Task TryDeleteAsync_TreatsDeletedKeyAsFalse()
    {
        _store.Setup(s => s.DeleteAsync("deleted", It.IsAny<NatsKVDeleteOpts>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new NatsKVKeyDeletedException(revision: 1));
        var adapter = CreateAdapter();

        Assert.False(await adapter.TryDeleteAsync("deleted", 3UL, CancellationToken.None));
    }

    [Fact]
    public async Task GetKeysAsync_StreamsKeys()
    {
        _store.Setup(s => s.GetKeysAsync(It.IsAny<NatsKVWatchOpts>(), It.IsAny<CancellationToken>())).Returns(AsyncEnum("k1", "k2"));
        var adapter = CreateAdapter();

        var keys = new List<string>();
        await foreach (var key in adapter.GetKeysAsync(CancellationToken.None))
            keys.Add(key);

        Assert.Equal(["k1", "k2"], keys);
    }

    private static async IAsyncEnumerable<string> AsyncEnum(params string[] items)
    {
        foreach (var item in items)
        {
            await Task.Yield();
            yield return item;
        }
    }

    private static async IAsyncEnumerable<NatsKVEntry<int>> AsyncEnum(params NatsKVEntry<int>[] items)
    {
        foreach (var item in items)
        {
            await Task.Yield();
            yield return item;
        }
    }
}

public class NatsJetStreamTransportAdapterTests
{
    private readonly Mock<INatsJSContext> _jetStream = new();

    /// <summary>
    /// The stream does not exist: JetStream answers a lookup with a 404 "stream not found", which
    /// is the only answer that lets this transport create one.
    /// </summary>
    private void StreamNotFound(string stream)
        => _jetStream
            .Setup(c => c.GetStreamAsync(stream, It.IsAny<StreamInfoRequest?>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new NatsJSApiException(new ApiError { Code = 404, ErrCode = 10059, Description = "stream not found" }));

    /// <summary>The stream exists with <paramref name="config"/> — an operator's own configuration.</summary>
    private void ExistingStream(string stream, StreamConfig config)
    {
        var info = new Mock<INatsJSStream>();
        info.SetupGet(s => s.Info).Returns(new StreamInfo { Config = config });
        _jetStream
            .Setup(c => c.GetStreamAsync(stream, It.IsAny<StreamInfoRequest?>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(info.Object);
    }

    [Fact]
    public async Task EnsureStreamAsync_CreatesStreamWithSubjectAndLimit()
    {
        StreamNotFound("stream");
        _jetStream.Setup(c => c.CreateStreamAsync(It.IsAny<StreamConfig>(), It.IsAny<CancellationToken>())).ReturnsAsync(Mock.Of<INatsJSStream>());
        var adapter = new NatsJetStreamTransportAdapter(_jetStream.Object);

        await adapter.EnsureStreamAsync("stream", "subj", 100, CancellationToken.None);

        // Created, never "create or update": an existing stream carries an operator's own
        // replicas, limits and placement, and this transport must not write over them.
        _jetStream.Verify(c => c.CreateStreamAsync(
            It.Is<StreamConfig>(cfg => cfg.Name == "stream" && cfg.Subjects!.Contains("subj") && cfg.MaxMsgs == 100),
            It.IsAny<CancellationToken>()), Times.Once);
        _jetStream.Verify(c => c.CreateOrUpdateStreamAsync(It.IsAny<StreamConfig>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task EnsureDeadLetterStreamAsync_CreatesLimitsRetentionEvictOldestStream()
    {
        // Regression (round 31): the dead-letter stream was provisioned with the WORK-QUEUE config
        // (Retention=Workqueue, Discard=New). Nothing ever consumes the dead-letter subject, so
        // work-queue retention removed nothing; once MaxMsgs filled, Discard=New rejected every
        // burial and each over-cap poison message NAK-looped forever (the consumer runs with
        // MaxDeliver=-1 on the premise that the dispatcher bounds attempts). The DLQ must be a
        // bounded evict-oldest archive — Redis's MAXLEN-trimmed dead-letter stream shape.
        StreamNotFound("dead-stream");
        _jetStream.Setup(c => c.CreateStreamAsync(It.IsAny<StreamConfig>(), It.IsAny<CancellationToken>())).ReturnsAsync(Mock.Of<INatsJSStream>());
        var adapter = new NatsJetStreamTransportAdapter(_jetStream.Object);

        await adapter.EnsureDeadLetterStreamAsync("dead-stream", "dead-subj", 100, CancellationToken.None);

        _jetStream.Verify(c => c.CreateStreamAsync(
            It.Is<StreamConfig>(cfg => cfg.Name == "dead-stream"
                && cfg.Subjects!.Contains("dead-subj")
                && cfg.MaxMsgs == 100
                && cfg.Retention == StreamConfigRetention.Limits
                && cfg.Discard == StreamConfigDiscard.Old),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task EnsureDeadLetterStreamAsync_ExistingStreamWithImmutableRetention_WarnsInsteadOfFailingTheSubscriber()
    {
        // JetStream forbids changing an existing stream's retention policy, so a DLQ provisioned
        // by an earlier build (work-queue retention) rejects the update. The old stream still
        // accepts burials until it fills; failing the whole subscriber over it would be worse —
        // keep running and tell the operator how to migrate.
        ExistingStream("dead-stream", new StreamConfig("dead-stream", ["dead-subj"])
        {
            Retention = StreamConfigRetention.Workqueue,
            Discard = StreamConfigDiscard.New,
            MaxMsgs = 100
        });
        var adapter = new NatsJetStreamTransportAdapter(_jetStream.Object);

        await adapter.EnsureDeadLetterStreamAsync("dead-stream", "dead-subj", 100, CancellationToken.None);

        // Left exactly as it is: neither created nor updated.
        _jetStream.Verify(c => c.CreateStreamAsync(It.IsAny<StreamConfig>(), It.IsAny<CancellationToken>()), Times.Never);
        _jetStream.Verify(c => c.CreateOrUpdateStreamAsync(It.IsAny<StreamConfig>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    private void ConsumerNotFound(string stream, string durable)
        => _jetStream
            .Setup(c => c.GetConsumerAsync(stream, durable, It.IsAny<CancellationToken>()))
            .ThrowsAsync(new NatsJSApiException(new ApiError { Code = 404, ErrCode = 10014, Description = "consumer not found" }));

    private void ExistingConsumer(string stream, string durable, ConsumerConfig config)
    {
        var consumer = new Mock<INatsJSConsumer>();
        consumer.SetupGet(c => c.Info).Returns(new ConsumerInfo { StreamName = stream, Name = durable, Config = config });
        _jetStream.Setup(c => c.GetConsumerAsync(stream, durable, It.IsAny<CancellationToken>())).ReturnsAsync(consumer.Object);
    }

    private static ConsumerConfig OperatorConsumer(string durable) => new(durable)
    {
        DurableName = durable,
        AckPolicy = ConsumerConfigAckPolicy.Explicit,
        AckWait = TimeSpan.FromSeconds(30),
        MaxDeliver = -1,
        MaxAckPending = 50
    };

    [Fact]
    public async Task EnsureConsumerAsync_CreatesDurableExplicitAckConsumer()
    {
        ConsumerNotFound("stream", "durable");
        _jetStream.Setup(c => c.CreateConsumerAsync("stream", It.IsAny<ConsumerConfig>(), It.IsAny<CancellationToken>())).ReturnsAsync(Mock.Of<INatsJSConsumer>());
        var adapter = new NatsJetStreamTransportAdapter(_jetStream.Object);

        await adapter.EnsureConsumerAsync("stream", "durable", TimeSpan.FromSeconds(30), 5, CancellationToken.None);

        _jetStream.Verify(c => c.CreateConsumerAsync(
            "stream",
            It.Is<ConsumerConfig>(cfg => cfg.DurableName == "durable" && cfg.AckPolicy == ConsumerConfigAckPolicy.Explicit && cfg.MaxDeliver == -1 && cfg.AckWait == TimeSpan.FromSeconds(30)),
            It.IsAny<CancellationToken>()), Times.Once);
        _jetStream.Verify(c => c.CreateOrUpdateConsumerAsync(It.IsAny<string>(), It.IsAny<ConsumerConfig>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task EnsureConsumerAsync_ExistingConsumer_IsNeverRewritten()
    {
        // Regression: every subscriber attempt (every fast-empty rebuild included) ran a
        // create-or-UPDATE with a minimal config, which replaces the whole configuration — an
        // operator's MaxAckPending, BackOff or metadata reverted on every start, even with
        // CreateStreams off, and a durable differing in an immutable field failed every start.
        ExistingConsumer("stream", "durable", OperatorConsumer("durable"));
        var adapter = new NatsJetStreamTransportAdapter(_jetStream.Object);

        await adapter.EnsureConsumerAsync("stream", "durable", TimeSpan.FromSeconds(30), 5, CancellationToken.None);

        _jetStream.Verify(c => c.CreateOrUpdateConsumerAsync(It.IsAny<string>(), It.IsAny<ConsumerConfig>(), It.IsAny<CancellationToken>()), Times.Never);
        _jetStream.Verify(c => c.CreateConsumerAsync(It.IsAny<string>(), It.IsAny<ConsumerConfig>(), It.IsAny<CancellationToken>()), Times.Never);
        _jetStream.Verify(c => c.UpdateConsumerAsync(It.IsAny<string>(), It.IsAny<ConsumerConfig>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task EnsureConsumerAsync_CreationRaceLostToAPeer_VerifiesThePeersConsumer()
    {
        var consumer = new Mock<INatsJSConsumer>();
        consumer.SetupGet(c => c.Info).Returns(new ConsumerInfo { StreamName = "stream", Name = "durable", Config = OperatorConsumer("durable") });
        _jetStream.SetupSequence(c => c.GetConsumerAsync("stream", "durable", It.IsAny<CancellationToken>()))
            .ThrowsAsync(new NatsJSApiException(new ApiError { Code = 404, ErrCode = 10014, Description = "consumer not found" }))
            .ReturnsAsync(consumer.Object);
        _jetStream.Setup(c => c.CreateConsumerAsync("stream", It.IsAny<ConsumerConfig>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new NatsJSApiException(new ApiError { Code = 400, ErrCode = 10148, Description = "consumer already exists" }));
        var adapter = new NatsJetStreamTransportAdapter(_jetStream.Object);

        await adapter.EnsureConsumerAsync("stream", "durable", TimeSpan.FromSeconds(30), 5, CancellationToken.None);

        _jetStream.Verify(c => c.GetConsumerAsync("stream", "durable", It.IsAny<CancellationToken>()), Times.Exactly(2));
    }

    public static TheoryData<string, Action<ConsumerConfig>> UnworkableConsumers => new()
    {
        { "push consumer", config => config.DeliverSubject = "_INBOX.push" },
        { "ack policy", config => config.AckPolicy = ConsumerConfigAckPolicy.None },
        { "max deliver", config => config.MaxDeliver = 5 }
    };

    [Theory]
    [MemberData(nameof(UnworkableConsumers))]
    public async Task EnsureConsumerAsync_ExistingConsumerThisTransportCannotWorkWith_FailsByName(string fragment, Action<ConsumerConfig> drift)
    {
        var config = OperatorConsumer("durable");
        drift(config);
        ExistingConsumer("stream", "durable", config);
        var adapter = new NatsJetStreamTransportAdapter(_jetStream.Object);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => adapter.EnsureConsumerAsync("stream", "durable", TimeSpan.FromSeconds(30), 5, CancellationToken.None));

        Assert.Contains("'durable'", ex.Message, StringComparison.Ordinal);
        Assert.Contains(fragment, ex.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(5, 30)]   // live at or below a third of the configured value: this used to throw
    [InlineData(30, 120)] // AckWait raised for long handlers, the consumer still on the old value
    [InlineData(120, 30)] // AckWait lowered
    public async Task EnsureConsumerAsync_ExistingConsumerWithADifferentAckWait_ReturnsTheLiveOne_AndWarnsOnce(int liveSeconds, int configuredSeconds)
    {
        // Pre-commit review of fixpoint round 1: a live ack wait at or below a third of the
        // configured one threw — inside the subscriber attempt, so raising AckWait (30 s to 2 min)
        // made the supervisor retry every attempt at Warning forever while nothing consumed. The
        // consumer is still never modified; its own ack wait is returned (the heartbeat follows the
        // shorter one), and the drift is reported once, not on every attempt.
        var config = OperatorConsumer("durable");
        config.AckWait = TimeSpan.FromSeconds(liveSeconds);
        ExistingConsumer("stream", "durable", config);
        var logger = new RecordingThrowingLogger<NatsJetStreamTransportAdapter>();
        var adapter = new NatsJetStreamTransportAdapter(_jetStream.Object, logger);

        var first = await adapter.EnsureConsumerAsync("stream", "durable", TimeSpan.FromSeconds(configuredSeconds), 5, CancellationToken.None);
        var second = await adapter.EnsureConsumerAsync("stream", "durable", TimeSpan.FromSeconds(configuredSeconds), 5, CancellationToken.None);

        Assert.Equal(TimeSpan.FromSeconds(liveSeconds), first);
        Assert.Equal(TimeSpan.FromSeconds(liveSeconds), second);
        Assert.Equal(1, logger.CountEntries(LogLevel.Warning, "already exists with ack wait"));
        _jetStream.Verify(c => c.CreateOrUpdateConsumerAsync(It.IsAny<string>(), It.IsAny<ConsumerConfig>(), It.IsAny<CancellationToken>()), Times.Never);
        _jetStream.Verify(c => c.UpdateConsumerAsync(It.IsAny<string>(), It.IsAny<ConsumerConfig>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task EnsureConsumerAsync_CreatedConsumer_ReturnsTheConfiguredAckWait()
    {
        ConsumerNotFound("stream", "durable");
        _jetStream.Setup(c => c.CreateConsumerAsync("stream", It.IsAny<ConsumerConfig>(), It.IsAny<CancellationToken>())).ReturnsAsync(Mock.Of<INatsJSConsumer>());
        var adapter = new NatsJetStreamTransportAdapter(_jetStream.Object);

        var liveAckWait = await adapter.EnsureConsumerAsync("stream", "durable", TimeSpan.FromSeconds(45), 5, CancellationToken.None);

        Assert.Equal(TimeSpan.FromSeconds(45), liveAckWait);
    }

    [Fact]
    public async Task PublishAsync_ReturnsSequence()
    {
        _jetStream.Setup(c => c.PublishAsync(
                It.IsAny<string>(),
                It.IsAny<string?>(),
                It.IsAny<INatsSerialize<string>>(),
                It.IsAny<NatsJSPubOpts>(),
                It.IsAny<NatsHeaders>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new PubAckResponse { Stream = "s", Seq = 7 });
        var adapter = new NatsJetStreamTransportAdapter(_jetStream.Object);

        var sequence = await adapter.PublishAsync("subj", "payload", headers: null, CancellationToken.None);

        Assert.Equal("7", sequence);
    }

    [Fact]
    public async Task PublishAsync_ForwardsHeaders()
    {
        NatsHeaders? captured = null;
        _jetStream.Setup(c => c.PublishAsync(
                "subj",
                "payload",
                It.IsAny<INatsSerialize<string>>(),
                It.IsAny<NatsJSPubOpts>(),
                It.IsAny<NatsHeaders>(),
                It.IsAny<CancellationToken>()))
            .Callback<string, string?, INatsSerialize<string>, NatsJSPubOpts?, NatsHeaders?, CancellationToken>(
                (_, _, _, _, headers, _) => captured = headers)
            .ReturnsAsync(new PubAckResponse { Stream = "s", Seq = 8 });
        var adapter = new NatsJetStreamTransportAdapter(_jetStream.Object);

        var sequence = await adapter.PublishAsync(
            "subj",
            "payload",
            new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["AR-Correlation-Id"] = "corr-1",
                ["Custom"] = "value"
            },
            CancellationToken.None);

        Assert.Equal("8", sequence);
        Assert.NotNull(captured);
        Assert.Equal("corr-1", captured!["AR-Correlation-Id"]);
        Assert.Equal("value", captured["Custom"]);
    }

    [Fact]
    public async Task PublishAsync_EmptyHeaders_ForwardsNullHeaders()
    {
        NatsHeaders? captured = new();
        _jetStream.Setup(c => c.PublishAsync(
                "subj",
                "payload",
                It.IsAny<INatsSerialize<string>>(),
                It.IsAny<NatsJSPubOpts>(),
                It.IsAny<NatsHeaders>(),
                It.IsAny<CancellationToken>()))
            .Callback<string, string?, INatsSerialize<string>, NatsJSPubOpts?, NatsHeaders?, CancellationToken>(
                (_, _, _, _, headers, _) => captured = headers)
            .ReturnsAsync(new PubAckResponse { Stream = "s", Seq = 9 });
        var adapter = new NatsJetStreamTransportAdapter(_jetStream.Object);

        await adapter.PublishAsync("subj", "payload", new Dictionary<string, string>(), CancellationToken.None);

        Assert.Null(captured);
    }

    [Fact]
    public async Task FetchNoWaitAsync_MapsMessages_AndSettlementDelegatesForward()
    {
        var message = new Mock<INatsJSMsg<string>>();
        message.SetupGet(m => m.Subject).Returns("subj");
        message.SetupGet(m => m.Data).Returns("payload");
        message.SetupGet(m => m.Headers).Returns((NatsHeaders?)null);
        message.SetupGet(m => m.Metadata).Returns((NatsJSMsgMetadata?)null);
        message.Setup(m => m.AckAsync(It.IsAny<AckOpts?>(), It.IsAny<CancellationToken>())).Returns(ValueTask.CompletedTask);
        message.Setup(m => m.AckTerminateAsync(It.IsAny<AckOpts?>(), It.IsAny<CancellationToken>())).Returns(ValueTask.CompletedTask);
        message.Setup(m => m.AckProgressAsync(It.IsAny<AckOpts?>(), It.IsAny<CancellationToken>())).Returns(ValueTask.CompletedTask);

        NatsJSFetchOpts? capturedOpts = null;
        var consumer = new Mock<INatsJSConsumer>();
        consumer.Setup(c => c.FetchNoWaitAsync<string>(It.IsAny<NatsJSFetchOpts>(), It.IsAny<INatsDeserialize<string>>(), It.IsAny<CancellationToken>()))
            .Callback<NatsJSFetchOpts, INatsDeserialize<string>?, CancellationToken>((opts, _, _) => capturedOpts = opts)
            .Returns(AsyncEnum(message.Object));
        _jetStream.Setup(c => c.GetConsumerAsync("stream", "durable", It.IsAny<CancellationToken>())).ReturnsAsync(consumer.Object);
        var adapter = new NatsJetStreamTransportAdapter(_jetStream.Object);

        var deliveries = new List<NatsJobDelivery>();
        await foreach (var delivery in adapter.FetchNoWaitAsync("stream", "durable", 16, CancellationToken.None))
            deliveries.Add(delivery);

        Assert.Equal(16, capturedOpts!.MaxMsgs);
        var single = Assert.Single(deliveries);
        Assert.Equal("subj", single.Subject);
        Assert.Equal("payload", single.Payload);
        Assert.Equal(1, single.NumDelivered); // null metadata defaults to 1

        await single.AckAsync();
        await single.TermAsync();
        // Round 39: the heartbeat's token reaches the SDK call (the settlements above stay
        // deliberately uncancelable — a decision already taken must reach the server).
        using var progressCancellation = new CancellationTokenSource();
        await single.ProgressAsync(progressCancellation.Token);
        // NakAsync(delay) is a NATS.Net extension over the message (not a mockable member), so it
        // cannot be Moq-verified; invoking the delegate still exercises the adapter's nak path, and the
        // extension's internal member call on the loose mock is tolerated.
        try { await single.NakAsync(TimeSpan.FromSeconds(2)); } catch (Exception) { /* extension-over-mock */ }
        message.Verify(m => m.AckAsync(It.IsAny<AckOpts?>(), It.IsAny<CancellationToken>()), Times.Once);
        message.Verify(m => m.AckTerminateAsync(It.IsAny<AckOpts?>(), It.IsAny<CancellationToken>()), Times.Once);
        message.Verify(m => m.AckProgressAsync(It.IsAny<AckOpts?>(), progressCancellation.Token), Times.Once);
        message.Verify(m => m.AckAsync(It.IsAny<AckOpts?>(), CancellationToken.None), Times.Once);
    }

    [Fact]
    public async Task FetchAsync_MapsHeadersAndMetadata_AndCarriesExpires()
    {
        var headers = new NatsHeaders
        {
            ["AR-Correlation-Id"] = "corr-1",
            ["Retry"] = "yes"
        };
        var message = new Mock<INatsJSMsg<string>>();
        message.SetupGet(m => m.Subject).Returns("subj");
        message.SetupGet(m => m.Data).Returns((string?)null);
        message.SetupGet(m => m.Headers).Returns(headers);
        message.SetupGet(m => m.Metadata).Returns(new NatsJSMsgMetadata(
            new NatsJSSequencePair(11, 7),
            4,
            2,
            DateTimeOffset.UtcNow,
            "stream",
            "durable",
            "domain"));

        NatsJSFetchOpts? capturedOpts = null;
        var consumer = new Mock<INatsJSConsumer>();
        consumer.Setup(c => c.FetchAsync<string>(It.IsAny<NatsJSFetchOpts>(), It.IsAny<INatsDeserialize<string>>(), It.IsAny<CancellationToken>()))
            .Callback<NatsJSFetchOpts, INatsDeserialize<string>?, CancellationToken>((opts, _, _) => capturedOpts = opts)
            .Returns(AsyncEnum(message.Object));
        _jetStream.Setup(c => c.GetConsumerAsync("stream", "durable", It.IsAny<CancellationToken>())).ReturnsAsync(consumer.Object);
        var adapter = new NatsJetStreamTransportAdapter(_jetStream.Object);

        var deliveries = new List<NatsJobDelivery>();
        await foreach (var delivery in adapter.FetchAsync("stream", "durable", 1, TimeSpan.FromSeconds(30), CancellationToken.None))
            deliveries.Add(delivery);

        Assert.Equal(1, capturedOpts!.MaxMsgs);
        Assert.Equal(TimeSpan.FromSeconds(30), capturedOpts.Expires);
        var single = Assert.Single(deliveries);
        Assert.Equal(string.Empty, single.Payload);
        Assert.Equal(4, single.NumDelivered);
        Assert.Equal("corr-1", single.Headers["AR-Correlation-Id"]);
        Assert.Equal("yes", single.Headers["Retry"]);
    }

    [Fact]
    public async Task FetchAsync_ReusesTheConsumerLookupAcrossFetches()
    {
        // One consumer-INFO round trip per (stream, durable), not per fetch: the wrapper only
        // carries names for building pull requests, so it stays valid across batches.
        var consumer = new Mock<INatsJSConsumer>();
        consumer.Setup(c => c.FetchNoWaitAsync<string>(It.IsAny<NatsJSFetchOpts>(), It.IsAny<INatsDeserialize<string>>(), It.IsAny<CancellationToken>()))
            .Returns(AsyncEnum());
        consumer.Setup(c => c.FetchAsync<string>(It.IsAny<NatsJSFetchOpts>(), It.IsAny<INatsDeserialize<string>>(), It.IsAny<CancellationToken>()))
            .Returns(AsyncEnum());
        _jetStream.Setup(c => c.GetConsumerAsync("stream", "durable", It.IsAny<CancellationToken>())).ReturnsAsync(consumer.Object);
        var adapter = new NatsJetStreamTransportAdapter(_jetStream.Object);

        await foreach (var _ in adapter.FetchNoWaitAsync("stream", "durable", 16, CancellationToken.None)) { }
        await foreach (var _ in adapter.FetchAsync("stream", "durable", 1, TimeSpan.FromSeconds(1), CancellationToken.None)) { }

        _jetStream.Verify(c => c.GetConsumerAsync("stream", "durable", It.IsAny<CancellationToken>()), Times.Once);
    }

    private static async IAsyncEnumerable<INatsJSMsg<string>> AsyncEnum(params INatsJSMsg<string>[] items)
    {
        foreach (var item in items)
        {
            await Task.Yield();
            yield return item;
        }
    }
}
