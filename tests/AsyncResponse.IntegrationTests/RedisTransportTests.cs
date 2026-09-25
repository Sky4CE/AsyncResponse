using AsyncResponse.Transports.Redis;
using StackExchange.Redis;
using AsyncResponse.Sample;
using System.Net.Http.Json;
using Xunit;

namespace AsyncResponse.IntegrationTests;

/// <summary>
/// The real Redis Streams transport: worker jobs published and consumed over Redis consumer groups,
/// and responses ingested from a Redis response stream into active waiters.
/// </summary>
[Collection(BrokersCollection.Name)]
[Trait(Batches.Trait, Batches.Brokers)]
public sealed class RedisTransportTests(BrokersBatchFixture fixture) : IntegrationTestBase(fixture)
{
    [Fact]
    public async Task WorkerPublish_FailedAppendWithLostErrorReply_CannotBecomeDeduplicatedSuccess()
    {
        using var connection = await ConnectionMultiplexer.ConnectAsync(Fixture.RedisConnectionString);
        var db = connection.GetDatabase();
        var stream = "review-publish:" + Guid.NewGuid().ToString("N");
        var marker = "{" + stream + "}:publish:retry";
        var adapter = new RedisStreamDatabaseAdapter(db, TimeSpan.FromSeconds(5));
        await db.StringSetAsync(stream, "wrong type");
        var lostReply = true;
        try
        {
            // The server executes the operation; only its first error reply is lost. The retry
            // must fail too, never accept a marker written before the failed XADD.
            await Assert.ThrowsAsync<RedisServerException>(() => RedisTransportRetry.ExecuteAsync(async token =>
            {
                try
                {
                    return await adapter.StreamAddOnceAsync(stream, marker, TimeSpan.FromMinutes(1),
                        [new NameValueEntry("payload", "work")], null, true, token);
                }
                catch (RedisServerException) when (lostReply)
                {
                    lostReply = false;
                    throw new TimeoutException("Simulated loss of the executed command's error reply.");
                }
            }, 3, TimeSpan.FromMilliseconds(1), TimeSpan.FromMilliseconds(1), default));
            Assert.False(await db.KeyExistsAsync(marker));

            await db.KeyDeleteAsync(stream);
            Assert.False((await adapter.StreamAddOnceAsync(stream, marker, TimeSpan.FromMinutes(1),
                [new NameValueEntry("payload", "work")], null, true, default)).IsNull);
            Assert.Equal(1, await db.StreamLengthAsync(stream));
        }
        finally
        {
            await db.KeyDeleteAsync(new RedisKey[] { stream, marker });
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task WorkerPublish_SuccessfulAppendWithLostReply_IsStoredOnlyOnce(bool approximate)
    {
        using var connection = await ConnectionMultiplexer.ConnectAsync(Fixture.RedisConnectionString);
        var db = connection.GetDatabase();
        var stream = "review-publish:" + Guid.NewGuid().ToString("N");
        var marker = "{" + stream + "}:publish:retry";
        var adapter = new RedisStreamDatabaseAdapter(db, TimeSpan.FromSeconds(5));
        var first = true;
        try
        {
            var result = await RedisTransportRetry.ExecuteAsync(async token =>
            {
                var id = await adapter.StreamAddOnceAsync(stream, marker, TimeSpan.FromMinutes(1),
                    [new NameValueEntry("payload", "unicode-雪")], 100, approximate, token);
                if (first) { first = false; throw new TimeoutException("Lost successful reply."); }
                return id;
            }, 3, TimeSpan.FromMilliseconds(1), TimeSpan.FromMilliseconds(1), default);
            Assert.True(result.IsNull);
            var entry = Assert.Single(await db.StreamRangeAsync(stream));
            Assert.Equal("unicode-雪", entry.Values[0].Value.ToString());
            Assert.Equal(entry.Id, await db.StringGetAsync(marker));
            Assert.True(await db.KeyTimeToLiveAsync(marker) > TimeSpan.Zero);
        }
        finally
        {
            await db.KeyDeleteAsync(new RedisKey[] { stream, marker });
        }
    }

    /// <summary>
    /// Fixpoint r1 (S6a#14), against a real server: the stop-time consumer retirement deletes a
    /// consumer only while it owns no pending entries (XGROUP DELCONSUMER would discard them), in
    /// one atomic script.
    /// </summary>
    [Fact]
    public async Task ConsumerRetirement_DeletesOnlyAConsumerWithoutPendingEntries()
    {
        using var connection = await ConnectionMultiplexer.ConnectAsync(Fixture.RedisConnectionString);
        var db = connection.GetDatabase();
        var stream = "review-consumers:" + Guid.NewGuid().ToString("N");
        var adapter = new RedisStreamDatabaseAdapter(db, TimeSpan.FromSeconds(5));
        try
        {
            await db.StreamCreateConsumerGroupAsync(stream, "group", StreamPosition.Beginning, createStream: true);
            await db.StreamAddAsync(stream, "payload", "held");
            await db.StreamAddAsync(stream, "payload", "done");

            // "holder" keeps one entry pending; "idle" read one and ACKed it.
            Assert.Single(await db.StreamReadGroupAsync(stream, "group", "holder", StreamPosition.NewMessages, count: 1));
            var done = Assert.Single(await db.StreamReadGroupAsync(stream, "group", "idle", StreamPosition.NewMessages, count: 1));
            await db.StreamAcknowledgeAsync(stream, "group", done.Id);

            Assert.False(await adapter.TryDeleteIdleConsumerAsync(stream, "group", "holder", default));
            Assert.True(await adapter.TryDeleteIdleConsumerAsync(stream, "group", "idle", default));

            var consumers = await db.StreamConsumerInfoAsync(stream, "group");
            var holder = Assert.Single(consumers);
            Assert.Equal("holder", holder.Name.ToString());
            Assert.Equal(1, holder.PendingMessageCount);
        }
        finally
        {
            await db.KeyDeleteAsync(stream);
        }
    }

    [Fact]
    public async Task Config_ReportsDefaultAndEarlyAckRedisModes()
    {
        var defaultConfig = (await Fixture.RedisTransportClient.GetFromJsonAsync<ConfigResponse>("/config"))!;
        var earlyAckConfig = (await Fixture.RedisTransportEarlyAckClient.GetFromJsonAsync<ConfigResponse>("/config"))!;

        Assert.Equal("Redis", defaultConfig.Channel);
        Assert.Equal("Redis", defaultConfig.Transport);
        Assert.Equal("AckAfterHandlerCompletes", defaultConfig.Redis!.WorkerAckMode);
        Assert.Equal("AckAfterHandlerCompletes", defaultConfig.Redis.ResponseAckMode);
        Assert.EndsWith(":transport:worker", defaultConfig.Redis.WorkerStream, StringComparison.Ordinal);
        Assert.EndsWith(":transport:response", defaultConfig.Redis.ResponseStream, StringComparison.Ordinal);

        Assert.Equal("Redis", earlyAckConfig.Channel);
        Assert.Equal("Redis", earlyAckConfig.Transport);
        Assert.Equal("AckAfterEnqueue", earlyAckConfig.Redis!.WorkerAckMode);
        Assert.Equal(4, earlyAckConfig.Redis.WorkerBackgroundWorkerCount);
        Assert.Equal(256, earlyAckConfig.Redis.WorkerBackgroundQueueCapacity);
        Assert.Equal("AckAfterHandlerCompletes", earlyAckConfig.Redis.ResponseAckMode);
    }

    [Fact]
    public async Task WorkerJob_RoundTripsThroughRedis_WithRestoredCorrelationAndTrace()
    {
        var token = NewId("redis-token");
        var trace = NewId("redis-trace");

        var response = await Fixture.RedisTransportClient.PostAsync($"/worker?token={token}&trace={trace}", content: null);
        response.EnsureSuccessStatusCode();
        var correlationId = (await response.Content.ReadFromJsonAsync<WorkerResponse>())!.CorrelationId;

        var call = await WaitForCallAsync(Fixture.RedisTransportClient, $"worker:{token}");
        Assert.Equal("worker", call.Kind);
        Assert.Equal(correlationId, call.CorrelationId);
        Assert.Equal(trace, call.Trace);
    }

    [Fact]
    public async Task WorkerJob_RoundTripsThroughRedis_WithAckAfterEnqueueWorkerSubscriber()
    {
        var token = NewId("redis-early-token");
        var trace = NewId("redis-early-trace");

        var response = await Fixture.RedisTransportEarlyAckClient.PostAsync($"/worker?token={token}&trace={trace}", content: null);
        response.EnsureSuccessStatusCode();
        var correlationId = (await response.Content.ReadFromJsonAsync<WorkerResponse>())!.CorrelationId;

        var call = await WaitForCallAsync(Fixture.RedisTransportEarlyAckClient, $"worker:{token}");
        Assert.Equal("worker", call.Kind);
        Assert.Equal(correlationId, call.CorrelationId);
        Assert.Equal(trace, call.Trace);
    }

    [Fact]
    public async Task WithReplyTarget_ExposesTheRedisResponseStream()
    {
        var response = await Fixture.RedisTransportClient.GetAsync("/reply-target");
        response.EnsureSuccessStatusCode();

        var target = await response.Content.ReadFromJsonAsync<ReplyTargetResult>();
        Assert.Equal("redis", target!.Transport);
        Assert.EndsWith(":transport:response", target.Address, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ResponseIngress_CorrelationViaStreamField_CompletesActiveWaiter()
    {
        var correlationId = await ArmAsync(Fixture.RedisTransportClient, NewId("redis-trace"));

        (await Fixture.RedisTransportClient.PostAsync(
            $"/emit-response?correlationId={correlationId}&status=Completed&useAttribute=true",
            content: null)).EnsureSuccessStatusCode();

        var call = await WaitForCallAsync(Fixture.RedisTransportClient, $"waiter:{correlationId}");
        Assert.Equal("waiter", call.Kind);
        Assert.Equal(OperationStatus.Completed, call.Status);
    }

    [Fact]
    public async Task ResponseIngress_CorrelationViaJsonBody_CompletesActiveWaiter()
    {
        var correlationId = await ArmAsync(Fixture.RedisTransportClient, NewId("redis-trace"));

        (await Fixture.RedisTransportClient.PostAsync(
            $"/emit-response?correlationId={correlationId}&status=Completed&useAttribute=false",
            content: null)).EnsureSuccessStatusCode();

        var call = await WaitForCallAsync(Fixture.RedisTransportClient, $"waiter:{correlationId}");
        Assert.Equal("waiter", call.Kind);
        Assert.Equal(OperationStatus.Completed, call.Status);
    }

    private sealed record WorkerResponse(string CorrelationId);
    private sealed record ReplyTargetResult(string Transport, string Address);
    private sealed record ConfigResponse(string Channel, string Transport, RedisConfig? Redis);
    private sealed record RedisConfig(
        string WorkerStream,
        string ResponseStream,
        string WorkerAckMode,
        int WorkerBackgroundWorkerCount,
        int WorkerBackgroundQueueCapacity,
        string ResponseAckMode);
}
