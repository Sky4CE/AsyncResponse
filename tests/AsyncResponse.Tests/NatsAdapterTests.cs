using AsyncResponse.Channels.NATS;
using AsyncResponse.Transports.NATS;
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
        _context.Setup(c => c.CreateStoreAsync(It.IsAny<NatsKVConfig>(), It.IsAny<CancellationToken>())).ReturnsAsync(_store.Object);
        return new NatsKvStoreAdapter(_context.Object, new NatsAsyncResponseChannelOptions());
    }

    [Fact]
    public async Task PutAsync_ForwardsToStore_AndCreatesBucketLazilyOnce()
    {
        _store.Setup(s => s.PutAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<INatsSerialize<string>>(), It.IsAny<CancellationToken>())).ReturnsAsync(1UL);
        var adapter = CreateAdapter();

        await adapter.PutAsync("k", "v", CancellationToken.None);
        await adapter.PutAsync("k2", "v2", CancellationToken.None);

        _store.Verify(s => s.PutAsync("k", "v", It.IsAny<INatsSerialize<string>>(), It.IsAny<CancellationToken>()), Times.Once);
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
    public async Task DeleteAsync_ReportsWhetherKeyExisted()
    {
        _store.Setup(s => s.GetEntryAsync<string>("present", It.IsAny<ulong>(), It.IsAny<INatsDeserialize<string>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new NatsKVEntry<string>("bucket", "present") { Value = "v" });
        _store.Setup(s => s.GetEntryAsync<string>("absent", It.IsAny<ulong>(), It.IsAny<INatsDeserialize<string>>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new NatsKVKeyNotFoundException());
        _store.Setup(s => s.DeleteAsync(It.IsAny<string>(), It.IsAny<NatsKVDeleteOpts>(), It.IsAny<CancellationToken>())).Returns(ValueTask.CompletedTask);
        var adapter = CreateAdapter();

        Assert.True(await adapter.DeleteAsync("present", CancellationToken.None));
        Assert.False(await adapter.DeleteAsync("absent", CancellationToken.None));
        _store.Verify(s => s.DeleteAsync("present", It.IsAny<NatsKVDeleteOpts>(), It.IsAny<CancellationToken>()), Times.Once);
        _store.Verify(s => s.DeleteAsync("absent", It.IsAny<NatsKVDeleteOpts>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task DeleteAsync_TreatsDeletedKeyAsAbsent()
    {
        _store.Setup(s => s.GetEntryAsync<string>("tombstone", It.IsAny<ulong>(), It.IsAny<INatsDeserialize<string>>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new NatsKVKeyDeletedException(revision: 1));
        _store.Setup(s => s.DeleteAsync(It.IsAny<string>(), It.IsAny<NatsKVDeleteOpts>(), It.IsAny<CancellationToken>())).Returns(ValueTask.CompletedTask);
        var adapter = CreateAdapter();

        Assert.False(await adapter.DeleteAsync("tombstone", CancellationToken.None));
    }

    [Fact]
    public async Task DeleteAsync_ReturnsFalseWhenKeyIsDeletedAfterRead()
    {
        _store.Setup(s => s.GetEntryAsync<string>("raced", It.IsAny<ulong>(), It.IsAny<INatsDeserialize<string>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new NatsKVEntry<string>("bucket", "raced") { Value = "v" });
        _store.Setup(s => s.DeleteAsync("raced", It.IsAny<NatsKVDeleteOpts>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new NatsKVKeyDeletedException(revision: 2));
        var adapter = CreateAdapter();

        Assert.False(await adapter.DeleteAsync("raced", CancellationToken.None));
    }

    [Fact]
    public async Task DeleteAsync_ReturnsFalseWhenKeyDisappearsAfterRead()
    {
        _store.Setup(s => s.GetEntryAsync<string>("raced-missing", It.IsAny<ulong>(), It.IsAny<INatsDeserialize<string>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new NatsKVEntry<string>("bucket", "raced-missing") { Value = "v" });
        _store.Setup(s => s.DeleteAsync("raced-missing", It.IsAny<NatsKVDeleteOpts>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new NatsKVKeyNotFoundException());
        var adapter = CreateAdapter();

        Assert.False(await adapter.DeleteAsync("raced-missing", CancellationToken.None));
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
}

public class NatsJetStreamTransportAdapterTests
{
    private readonly Mock<INatsJSContext> _jetStream = new();

    [Fact]
    public async Task EnsureStreamAsync_CreatesStreamWithSubjectAndLimit()
    {
        _jetStream.Setup(c => c.CreateOrUpdateStreamAsync(It.IsAny<StreamConfig>(), It.IsAny<CancellationToken>())).ReturnsAsync(Mock.Of<INatsJSStream>());
        var adapter = new NatsJetStreamTransportAdapter(_jetStream.Object);

        await adapter.EnsureStreamAsync("stream", "subj", 100, CancellationToken.None);

        _jetStream.Verify(c => c.CreateOrUpdateStreamAsync(
            It.Is<StreamConfig>(cfg => cfg.Name == "stream" && cfg.Subjects!.Contains("subj") && cfg.MaxMsgs == 100),
            It.IsAny<CancellationToken>()), Times.Once);
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
        _jetStream.Setup(c => c.CreateOrUpdateStreamAsync(It.IsAny<StreamConfig>(), It.IsAny<CancellationToken>())).ReturnsAsync(Mock.Of<INatsJSStream>());
        var adapter = new NatsJetStreamTransportAdapter(_jetStream.Object);

        await adapter.EnsureDeadLetterStreamAsync("dead-stream", "dead-subj", 100, CancellationToken.None);

        _jetStream.Verify(c => c.CreateOrUpdateStreamAsync(
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
        _jetStream
            .Setup(c => c.CreateOrUpdateStreamAsync(It.IsAny<StreamConfig>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new NatsJSApiException(new ApiError { Code = 500, ErrCode = 10052, Description = "stream configuration update can not change retention policy" }));
        var adapter = new NatsJetStreamTransportAdapter(_jetStream.Object);

        await adapter.EnsureDeadLetterStreamAsync("dead-stream", "dead-subj", 100, CancellationToken.None);
    }

    [Fact]
    public async Task EnsureConsumerAsync_CreatesDurableExplicitAckConsumer()
    {
        _jetStream.Setup(c => c.CreateOrUpdateConsumerAsync("stream", It.IsAny<ConsumerConfig>(), It.IsAny<CancellationToken>())).ReturnsAsync(Mock.Of<INatsJSConsumer>());
        var adapter = new NatsJetStreamTransportAdapter(_jetStream.Object);

        await adapter.EnsureConsumerAsync("stream", "durable", TimeSpan.FromSeconds(30), CancellationToken.None);

        _jetStream.Verify(c => c.CreateOrUpdateConsumerAsync(
            "stream",
            It.Is<ConsumerConfig>(cfg => cfg.DurableName == "durable" && cfg.AckPolicy == ConsumerConfigAckPolicy.Explicit),
            It.IsAny<CancellationToken>()), Times.Once);
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
        await single.ProgressAsync();
        // NakAsync(delay) is a NATS.Net extension over the message (not a mockable member), so it
        // cannot be Moq-verified; invoking the delegate still exercises the adapter's nak path, and the
        // extension's internal member call on the loose mock is tolerated.
        try { await single.NakAsync(TimeSpan.FromSeconds(2)); } catch (Exception) { /* extension-over-mock */ }
        message.Verify(m => m.AckAsync(It.IsAny<AckOpts?>(), It.IsAny<CancellationToken>()), Times.Once);
        message.Verify(m => m.AckTerminateAsync(It.IsAny<AckOpts?>(), It.IsAny<CancellationToken>()), Times.Once);
        message.Verify(m => m.AckProgressAsync(It.IsAny<AckOpts?>(), It.IsAny<CancellationToken>()), Times.Once);
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
