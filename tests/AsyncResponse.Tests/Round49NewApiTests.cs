using AsyncResponse.Channels.NATS;
using AsyncResponse.Channels.Redis;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using System.Text.Json;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Round 49 pins over the batch-deletion seam (<see cref="IRecoveryStateBatchDeletion"/>) the
/// Redis and NATS recovery stores gained: one conditional rewrite removes exactly the named
/// registrations — never a concurrently added one, never one this build cannot read — and every
/// survivor keeps its own expiry. The behaviour pins that compile against the pre-fix tree are in
/// <see cref="Round49RegressionTests"/>.
/// </summary>
public sealed class Round49NewApiTests
{
    private static RecoveryState Registration(string correlationId, int schemaVersion = RecoveryStateSchema.Current) => new()
    {
        RegistrationId = Guid.NewGuid(),
        CorrelationId = correlationId,
        PayloadTypeFullName = typeof(OperationResult).FullName,
        RegisteredAtUtc = DateTime.UtcNow,
        SchemaVersion = schemaVersion
    };

    private static string RedisEnvelope(params (RecoveryState State, DateTimeOffset ExpiresAtUtc)[] registrations)
        => "{\"Registrations\":["
           + string.Join(",", registrations.Select(registration =>
               $"{{\"State\":{JsonSerializer.Serialize(registration.State)},\"ExpiresAtUtc\":\"{registration.ExpiresAtUtc:O}\"}}"))
           + "]}";

    private static List<(Guid RegistrationId, DateTimeOffset ExpiresAtUtc)> RedisEntries(StatefulRedis redis)
    {
        using var document = JsonDocument.Parse(redis.Value.ToString());
        return document.RootElement.GetProperty("Registrations").EnumerateArray()
            .Select(entry => (entry.GetProperty("State").GetProperty("RegistrationId").GetGuid(), entry.GetProperty("ExpiresAtUtc").GetDateTimeOffset()))
            .ToList();
    }

    [Fact]
    public void TheRedisAndNatsRecoveryStores_OfferBatchDeletion_TheInMemoryStoreKeepsSingleDeletes()
    {
        Assert.IsAssignableFrom<IRecoveryStateBatchDeletion>(new StatefulRedis().CreateStore(new TestTimeProvider()));
        Assert.IsAssignableFrom<IRecoveryStateBatchDeletion>(new NatsRecoveryStateStore(
            new FakeNatsKvStore(), Options.Create(new NatsAsyncResponseChannelOptions()), NullLogger<NatsRecoveryStateStore>.Instance));
        Assert.False(typeof(IRecoveryStateBatchDeletion).IsAssignableFrom(typeof(InMemoryRecoveryStateStore)));
    }

    [Fact]
    public async Task Redis_TryDeleteMany_RemovesExactlyTheNamedRegistrations_KeepingUnreadableSiblingsAndEveryStamp()
    {
        var time = new TestTimeProvider();
        var redis = new StatefulRedis();
        var store = redis.CreateStore(time);
        var a = Registration("corr");
        var b = Registration("corr");
        var future = Registration("corr", RecoveryStateSchema.Current + 1);
        var c = Registration("corr");
        redis.Value = RedisEnvelope(
            (a, time.Now + TimeSpan.FromMinutes(5)),
            (b, time.Now + TimeSpan.FromMinutes(10)),
            (future, time.Now + TimeSpan.FromMinutes(20)),
            (c, time.Now + TimeSpan.FromMinutes(30)));

        var removed = await store.TryDeleteManyAsync("corr", [a.RegistrationId, c.RegistrationId, Guid.NewGuid()]);

        Assert.Equal(2, removed);
        Assert.Equal(1, redis.Transactions);
        Assert.Equal(
            new[] { (b.RegistrationId, time.Now + TimeSpan.FromMinutes(10)), (future.RegistrationId, time.Now + TimeSpan.FromMinutes(20)) },
            RedisEntries(redis));
    }

    [Fact]
    public async Task Redis_TryDeleteMany_ARegistrationAddedConcurrently_SurvivesTheRetry()
    {
        var time = new TestTimeProvider();
        var redis = new StatefulRedis();
        var store = redis.CreateStore(time);
        var a = Registration("corr");
        var b = Registration("corr");
        var added = Registration("corr");
        redis.Value = RedisEnvelope((a, time.Now + TimeSpan.FromMinutes(5)), (b, time.Now + TimeSpan.FromMinutes(5)));
        // Another waiter registers between this delete's read and its EXEC: the condition fails.
        redis.BeforeExecute = () => redis.Value = RedisEnvelope(
            (a, time.Now + TimeSpan.FromMinutes(5)),
            (b, time.Now + TimeSpan.FromMinutes(5)),
            (added, time.Now + TimeSpan.FromMinutes(7)));

        Assert.Equal(2, await store.TryDeleteManyAsync("corr", [a.RegistrationId, b.RegistrationId]));

        Assert.Equal(1, redis.Conflicts);
        var survivor = Assert.Single(RedisEntries(redis));
        Assert.Equal(added.RegistrationId, survivor.RegistrationId);
        Assert.Equal(time.Now + TimeSpan.FromMinutes(7), survivor.ExpiresAtUtc);
    }

    [Fact]
    public async Task Redis_TryDeleteMany_TheLastRegistrations_DeleteTheKey_AndALegacyBlobKeepsItsShape()
    {
        var time = new TestTimeProvider();
        var redis = new StatefulRedis();
        var store = redis.CreateStore(time);
        var a = Registration("corr");
        var b = Registration("corr");
        redis.Value = RedisEnvelope((a, time.Now + TimeSpan.FromMinutes(5)), (b, time.Now + TimeSpan.FromMinutes(5)));

        Assert.Equal(2, await store.TryDeleteManyAsync("corr", [a.RegistrationId, b.RegistrationId]));
        Assert.Equal(1, redis.KeyDeletes);
        Assert.True(redis.Value.IsNull);

        var legacyKept = Registration("legacy");
        var legacyGone = Registration("legacy");
        redis.Value = JsonSerializer.Serialize(new[] { legacyKept, legacyGone });
        Assert.Equal(1, await store.TryDeleteManyAsync("legacy", [legacyGone.RegistrationId]));
        var rewritten = JsonSerializer.Deserialize<List<RecoveryState>>(redis.Value.ToString())!;
        Assert.Equal(legacyKept.RegistrationId, Assert.Single(rewritten).RegistrationId);
    }

    [Fact]
    public async Task Redis_TryDeleteMany_ValidatesItsArguments_AndAnEmptySetCostsNoRoundTrip()
    {
        var redis = new StatefulRedis();
        var store = redis.CreateStore(new TestTimeProvider());

        await Assert.ThrowsAsync<ArgumentException>(() => store.TryDeleteManyAsync(" ", [Guid.NewGuid()]));
        await Assert.ThrowsAsync<ArgumentNullException>(() => store.TryDeleteManyAsync("corr", null!));
        await Assert.ThrowsAsync<ArgumentException>(() => store.TryDeleteManyAsync("corr", [Guid.NewGuid(), Guid.Empty]));
        Assert.Equal(0, await store.TryDeleteManyAsync("corr", []));
        Assert.Equal(0, await store.TryDeleteManyAsync("absent", [Guid.NewGuid()]));

        using var cancelled = new CancellationTokenSource();
        await cancelled.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => store.TryDeleteManyAsync("corr", [Guid.NewGuid()], cancelled.Token));

        Assert.Equal(1, redis.Gets);
        Assert.Equal(0, redis.Transactions);
    }

    [Fact]
    public async Task Redis_TryDeleteMany_EveryAttemptConflicting_RemovesNothing()
    {
        var time = new TestTimeProvider();
        var redis = new StatefulRedis();
        var store = redis.CreateStore(time);
        var a = Registration("corr");
        redis.Value = RedisEnvelope((a, time.Now + TimeSpan.FromMinutes(5)));
        redis.ConflictEveryExecute = true;
        using var noPauses = RecoveryStateContention.SuppressPauses();

        Assert.Equal(0, await store.TryDeleteManyAsync("corr", [a.RegistrationId]));

        Assert.Equal(RecoveryStateContention.MaxAttempts, redis.Conflicts);
        Assert.Equal(a.RegistrationId, Assert.Single(RedisEntries(redis)).RegistrationId);
    }

    private static NatsRecoveryStateStore NatsStore(FakeNatsKvStore kv, TestTimeProvider time)
        => new(kv, Options.Create(new NatsAsyncResponseChannelOptions()), NullLogger<NatsRecoveryStateStore>.Instance, time);

    private static NatsRecoveryStateStore.StoredRecoveryState NatsEnvelope(FakeNatsKvStore kv, string correlationId)
        => JsonSerializer.Deserialize<NatsRecoveryStateStore.StoredRecoveryState>(kv.Entries[NatsSubjectSchema.RecoveryKey(correlationId)])!;

    [Fact]
    public async Task Nats_TryDeleteMany_RemovesExactlyTheNamedRegistrations_KeepingUnreadableSiblingsAndEveryStamp()
    {
        var time = new TestTimeProvider();
        var kv = new FakeNatsKvStore();
        var store = NatsStore(kv, time);
        var a = Registration("corr");
        var b = Registration("corr");
        var future = Registration("corr");
        var c = Registration("corr");
        await store.SaveAsync("corr", a, TimeSpan.FromMinutes(5));
        await store.SaveAsync("corr", b, TimeSpan.FromMinutes(10));
        await store.SaveAsync("corr", future, TimeSpan.FromMinutes(20));
        await store.SaveAsync("corr", c, TimeSpan.FromMinutes(30));
        // What a newer build's save leaves behind: a registration this build cannot interpret.
        var seeded = NatsEnvelope(kv, "corr");
        seeded.States![2].SchemaVersion = RecoveryStateSchema.Current + 1;
        kv.Entries[NatsSubjectSchema.RecoveryKey("corr")] = JsonSerializer.Serialize(seeded);
        kv.PutCount = 0;

        var removed = await store.TryDeleteManyAsync("corr", [a.RegistrationId, c.RegistrationId, Guid.NewGuid()]);

        Assert.Equal(2, removed);
        Assert.Equal(1, kv.PutCount);
        var envelope = NatsEnvelope(kv, "corr");
        Assert.Equal(new[] { b.RegistrationId, future.RegistrationId }, envelope.States!.Select(state => state.RegistrationId));
        Assert.Equal(RecoveryStateSchema.Current + 1, envelope.States![1].SchemaVersion);
        Assert.Equal(new[] { time.Now + TimeSpan.FromMinutes(10), time.Now + TimeSpan.FromMinutes(20) }, envelope.StateExpiries!);
    }

    [Fact]
    public async Task Nats_TryDeleteMany_ARegistrationAddedConcurrently_SurvivesTheRetry()
    {
        var time = new TestTimeProvider();
        var kv = new FakeNatsKvStore();
        var store = NatsStore(kv, time);
        var a = Registration("corr");
        var b = Registration("corr");
        var added = Registration("corr");
        await store.SaveAsync("corr", a, TimeSpan.FromMinutes(5));
        await store.SaveAsync("corr", b, TimeSpan.FromMinutes(5));
        // Another waiter registers between this delete's read and its conditional write.
        kv.AfterGet = _ => store.SaveAsync("corr", added, TimeSpan.FromMinutes(7));

        Assert.Equal(2, await store.TryDeleteManyAsync("corr", [a.RegistrationId, b.RegistrationId]));

        var envelope = NatsEnvelope(kv, "corr");
        Assert.Equal(added.RegistrationId, Assert.Single(envelope.States!).RegistrationId);
        Assert.Equal(time.Now + TimeSpan.FromMinutes(7), Assert.Single(envelope.StateExpiries!));
    }

    [Fact]
    public async Task Nats_TryDeleteMany_TheLastRegistrations_DeleteTheKey_AndExhaustedAttemptsRemoveNothing()
    {
        var time = new TestTimeProvider();
        var kv = new FakeNatsKvStore();
        var store = NatsStore(kv, time);
        var a = Registration("corr");
        var b = Registration("corr");
        await store.SaveAsync("corr", a, TimeSpan.FromMinutes(5));
        await store.SaveAsync("corr", b, TimeSpan.FromMinutes(5));

        kv.ForcedUpdateConflicts = RecoveryStateContention.MaxAttempts;
        using var noPauses = RecoveryStateContention.SuppressPauses();
        Assert.Equal(0, await store.TryDeleteManyAsync("corr", [a.RegistrationId]));
        Assert.Equal(2, NatsEnvelope(kv, "corr").States!.Count);

        Assert.Equal(2, await store.TryDeleteManyAsync("corr", [a.RegistrationId, b.RegistrationId]));
        Assert.Equal(1, kv.DeleteCount);
        Assert.False(kv.Entries.ContainsKey(NatsSubjectSchema.RecoveryKey("corr")));

        await Assert.ThrowsAsync<ArgumentException>(() => store.TryDeleteManyAsync("corr", [Guid.Empty]));
        await Assert.ThrowsAsync<ArgumentNullException>(() => store.TryDeleteManyAsync("corr", null!));
        Assert.Equal(0, await store.TryDeleteManyAsync("corr", []));
    }
}
