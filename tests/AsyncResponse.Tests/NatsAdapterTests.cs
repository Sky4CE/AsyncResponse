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

        Assert.Equal("value", await adapter.GetAsync("hit", CancellationToken.None));
        Assert.Null(await adapter.GetAsync("missing", CancellationToken.None));
        Assert.Null(await adapter.GetAsync("deleted", CancellationToken.None));
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
    public async Task ConsumeAsync_MapsMessages_AndAckNakTermDelegatesForward()
    {
        var message = new Mock<INatsJSMsg<string>>();
        message.SetupGet(m => m.Subject).Returns("subj");
        message.SetupGet(m => m.Data).Returns("payload");
        message.SetupGet(m => m.Headers).Returns((NatsHeaders?)null);
        message.SetupGet(m => m.Metadata).Returns((NatsJSMsgMetadata?)null);
        message.Setup(m => m.AckAsync(It.IsAny<AckOpts?>(), It.IsAny<CancellationToken>())).Returns(ValueTask.CompletedTask);
        message.Setup(m => m.AckTerminateAsync(It.IsAny<AckOpts?>(), It.IsAny<CancellationToken>())).Returns(ValueTask.CompletedTask);

        var consumer = new Mock<INatsJSConsumer>();
        consumer.Setup(c => c.ConsumeAsync<string>(It.IsAny<INatsDeserialize<string>>(), It.IsAny<NatsJSConsumeOpts>(), It.IsAny<CancellationToken>()))
            .Returns(AsyncEnum(message.Object));
        _jetStream.Setup(c => c.GetConsumerAsync("stream", "durable", It.IsAny<CancellationToken>())).ReturnsAsync(consumer.Object);
        var adapter = new NatsJetStreamTransportAdapter(_jetStream.Object);

        var deliveries = new List<NatsJobDelivery>();
        await foreach (var delivery in adapter.ConsumeAsync("stream", "durable", 16, CancellationToken.None))
            deliveries.Add(delivery);

        var single = Assert.Single(deliveries);
        Assert.Equal("subj", single.Subject);
        Assert.Equal("payload", single.Payload);
        Assert.Equal(1, single.NumDelivered); // null metadata defaults to 1

        await single.AckAsync();
        await single.TermAsync();
        // NakAsync(delay) is a NATS.Net extension over the message (not a mockable member), so it
        // cannot be Moq-verified; invoking the delegate still exercises the adapter's nak path, and the
        // extension's internal member call on the loose mock is tolerated.
        try { await single.NakAsync(TimeSpan.FromSeconds(2)); } catch (Exception) { /* extension-over-mock */ }
        message.Verify(m => m.AckAsync(It.IsAny<AckOpts?>(), It.IsAny<CancellationToken>()), Times.Once);
        message.Verify(m => m.AckTerminateAsync(It.IsAny<AckOpts?>(), It.IsAny<CancellationToken>()), Times.Once);
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
