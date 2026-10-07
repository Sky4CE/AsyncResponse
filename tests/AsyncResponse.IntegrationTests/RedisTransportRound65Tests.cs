using AsyncResponse.Transports.Redis;
using StackExchange.Redis;
using Xunit;

namespace AsyncResponse.IntegrationTests;

/// <summary>
/// Round 65 (F-01), against a real server: the worker stream's capacity is enforced by refusal,
/// never by trimming work the worker group has not settled. The append script used
/// <c>XADD … MAXLEN</c>, which trimmed by length alone — past the cap it deleted entries the
/// group had never read and entries pending in a handler, with no dead-letter copy.
/// </summary>
[Collection(BrokersCollection.Name)]
[Trait(Batches.Trait, Batches.Brokers)]
public sealed class RedisTransportRound65Tests(BrokersBatchFixture fixture) : IntegrationTestBase(fixture)
{
    [Fact]
    public async Task FullWorkerStream_WithNoGroupYet_RefusesTheAppend_AndKeepsEveryUnreadEntry()
    {
        using var connection = await ConnectionMultiplexer.ConnectAsync(Fixture.RedisConnectionString);
        var db = connection.GetDatabase();
        var stream = "r65-capacity:" + Guid.NewGuid().ToString("N");
        var adapter = new RedisStreamDatabaseAdapter(db, TimeSpan.FromSeconds(5));
        try
        {
            for (var i = 0; i < 3; i++)
                Assert.False((await AppendAsync(adapter, stream, capacity: 3)).IsNull);

            var refused = await Assert.ThrowsAsync<RedisServerException>(() => AppendAsync(adapter, stream, capacity: 3));
            Assert.StartsWith(RedisStreamDatabaseAdapter.StreamFullErrorCode, refused.Message, StringComparison.Ordinal);
            Assert.True(RedisTransportRetry.IsTransient(refused));
            Assert.Equal(3, await db.StreamLengthAsync(stream));
        }
        finally
        {
            await db.KeyDeleteAsync(stream);
        }
    }

    [Fact]
    public async Task FullWorkerStream_NeverDropsPendingOrUnreadEntries_AndSettledOnesMakeRoom()
    {
        using var connection = await ConnectionMultiplexer.ConnectAsync(Fixture.RedisConnectionString);
        var db = connection.GetDatabase();
        var stream = "r65-capacity:" + Guid.NewGuid().ToString("N");
        var adapter = new RedisStreamDatabaseAdapter(db, TimeSpan.FromSeconds(5));
        try
        {
            await db.StreamCreateConsumerGroupAsync(stream, "group", StreamPosition.Beginning, createStream: true);
            var ids = new List<string>();
            for (var i = 0; i < 3; i++)
                ids.Add((await AppendAsync(adapter, stream, capacity: 3)).ToString());

            // Two pending in a handler, one never read: nothing is settled, so the append is refused.
            Assert.Equal(2, (await db.StreamReadGroupAsync(stream, "group", "consumer", StreamPosition.NewMessages, count: 2)).Length);
            await Assert.ThrowsAsync<RedisServerException>(() => AppendAsync(adapter, stream, capacity: 3));
            Assert.Equal(ids, (await db.StreamRangeAsync(stream)).Select(entry => entry.Id.ToString()));

            // Settlement deletes: the SECOND entry, behind a still-pending first one, leaves at once.
            Assert.Equal(1, await adapter.StreamAcknowledgeAndDeleteAsync(stream, "group", ids[1], default));
            Assert.Equal(2, await db.StreamLengthAsync(stream));
            Assert.False((await AppendAsync(adapter, stream, capacity: 3)).IsNull);
            Assert.Contains(await db.StreamPendingMessagesAsync(stream, "group", 10, RedisValue.Null), pending => pending.MessageId == ids[0]);

            // An entry settled without a delete (an older version only ACKed) is trimmed when the
            // stream is full, and only that one: the unread entry stays.
            await db.StreamAcknowledgeAsync(stream, "group", ids[0]);
            Assert.False((await AppendAsync(adapter, stream, capacity: 3)).IsNull);
            var remaining = (await db.StreamRangeAsync(stream)).Select(entry => entry.Id.ToString()).ToList();
            Assert.DoesNotContain(ids[0], remaining);
            Assert.Contains(ids[2], remaining);
            Assert.Equal(3, remaining.Count);
        }
        finally
        {
            await db.KeyDeleteAsync(stream);
        }
    }

    private static Task<RedisValue> AppendAsync(RedisStreamDatabaseAdapter adapter, string stream, long capacity)
        => adapter.StreamAddOnceAsync(
            stream,
            "{" + stream + "}:publish:" + Guid.NewGuid().ToString("N"),
            TimeSpan.FromMinutes(1),
            [new NameValueEntry("payload", "job")],
            capacity,
            "group",
            default);
}
