using AsyncResponse.Channels.MongoDB;
using AsyncResponse.Channels.NATS;
using AsyncResponse.Channels.Redis;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Serializers;
using MongoDB.Driver;
using Moq;
using StackExchange.Redis;
using System.Text.Json;
using System.Text.Json.Nodes;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Regressions for round 51 (external 82/100 review of 93c3ba4c): behavior pins that compile
/// against the pre-fix tree and fail there. Pins over the option this round introduced
/// (<c>MaxRecoveryRegistrationsPerCorrelationId</c>) live in <see cref="Round51NewApiTests"/>.
/// </summary>
public sealed class Round51RegressionTests
{
    public interface IR51Spy
    {
        Task Resume(OperationResult payload);
        Task Fail(Exception exception);
    }

    internal sealed class R51Spy : IR51Spy
    {
        private int _resumed;
        private int _failed;

        public int Resumed => Volatile.Read(ref _resumed);
        public int Failed => Volatile.Read(ref _failed);

        public Task Resume(OperationResult payload)
        {
            Interlocked.Increment(ref _resumed);
            return Task.CompletedTask;
        }

        public Task Fail(Exception exception)
        {
            Interlocked.Increment(ref _failed);
            return Task.CompletedTask;
        }
    }

    internal static RecoveryState Registration(string correlationId) => new()
    {
        RegistrationId = Guid.NewGuid(),
        CorrelationId = correlationId,
        PayloadTypeFullName = typeof(OperationResult).FullName,
        RegisteredAtUtc = DateTime.UtcNow,
        ResumeCallback = new ReflectionCallDto
        {
            ServiceInterfaceFullName = typeof(IR51Spy).FullName!,
            MethodName = nameof(IR51Spy.Resume),
            Params = [CallbackParam.ForPlaceholder(PlaceholderType.Payload)]
        },
        FailureCallback = new ReflectionCallDto
        {
            ServiceInterfaceFullName = typeof(IR51Spy).FullName!,
            MethodName = nameof(IR51Spy.Fail),
            Params = [CallbackParam.ForPlaceholder(PlaceholderType.Exception)]
        }
    };

    private static readonly OperationResult Terminal = new() { Status = OperationStatus.Completed, Message = "terminal" };

    // ---------------------------------------------------------------------------------------------
    // F1 — the MongoDB recovery lookup read the primary at its default read concern. A primary a
    //      partition has deposed keeps answering until it notices, and a publisher that reached
    //      only it saw none (or only some) of the registrations the new primary had taken: the
    //      dispatcher invoked nothing (or too little), the publish returned, and the transport
    //      acknowledged the response while the missing registrations stayed armed.

    /// <summary>
    /// A MongoDB channel whose recovery collection answers by consistency, which the loose fluent
    /// mocks elsewhere collapse into one source: only a <c>majority</c> read made in a causally
    /// consistent session AFTER that session's majority write was acknowledged returns the current
    /// registrations; every other read — the plain primary handle, a session read with no
    /// acknowledged write before it, a read on any other handle — returns what a deposed primary
    /// would (<see cref="StaleView"/>).
    /// </summary>
    internal sealed class MongoConsistencyHarness : IAsyncDisposable
    {
        private readonly List<IClientSessionHandle> _barrierSessions = [];

        public Mock<IMongoDatabase> Database { get; } = new(MockBehavior.Loose);
        public Mock<IMongoClient> Client { get; } = new(MockBehavior.Loose);
        public Mock<IMongoCollection<MongoRecoveryStateDocument>> Primary { get; } = new(MockBehavior.Loose);
        public Mock<IMongoCollection<MongoRecoveryStateDocument>> Majority { get; } = new(MockBehavior.Loose);
        public Mock<IMongoCollection<MongoChannelMessageDocument>> Messages { get; } = new(MockBehavior.Loose);
        public Mock<IMongoCollection<MongoChannelSubscriberDocument>> Subscribers { get; } = new(MockBehavior.Loose);
        public Mock<IMongoCollection<BsonDocument>> Counters { get; } = new(MockBehavior.Loose);

        public List<string> StaleView { get; } = [];
        public List<string> CurrentView { get; } = [];
        public List<string> Operations { get; } = [];
        public List<ClientSessionOptions?> SessionOptions { get; } = [];
        public List<TimeSpan?> ReadBounds { get; } = [];
        public List<(BsonDocument Filter, BsonDocument Update, bool Upsert)> Barriers { get; } = [];

        /// <summary>Fails every barrier while set.</summary>
        public Exception? BarrierFailure { get; set; }

        /// <summary>Fails the next barriers with these, one each, before <see cref="BarrierFailure"/> is consulted.</summary>
        public Queue<Exception> NextBarrierFailures { get; } = new();

        /// <summary>Fails the session start while set.</summary>
        public Exception? SessionFailure { get; set; }

        /// <summary>Fails the majority read in a session while set (the barrier before it still succeeds).</summary>
        public Exception? MajorityReadFailure { get; set; }
        public int Deletes;

        public MongoDbChannelStore Store { get; }
        public MongoDbRecoveryStateStore RecoveryStore { get; }
        public MongoDbAsyncResponseChannel Channel { get; }
        public R51Spy Spy { get; } = new();

        private readonly ServiceProvider _services;

        public MongoConsistencyHarness()
        {
            Database.SetupGet(d => d.DatabaseNamespace).Returns(new DatabaseNamespace("r51"));
            Database.SetupGet(d => d.Client).Returns(Client.Object);
            Client.SetupGet(c => c.Settings).Returns(MongoClientSettings.FromConnectionString("mongodb://localhost:27017"));
            Client
                .Setup(c => c.StartSessionAsync(It.IsAny<ClientSessionOptions>(), It.IsAny<CancellationToken>()))
                .Returns((ClientSessionOptions options, CancellationToken _) =>
                {
                    SessionOptions.Add(options);
                    return SessionFailure is { } failure
                        ? Task.FromException<IClientSessionHandle>(failure)
                        : Task.FromResult(new Mock<IClientSessionHandle>(MockBehavior.Loose).Object);
                });

            Primary.SelfPinning();
            Majority.SelfPinning();
            // After SelfPinning, so this read concern overrides its catch-all (Moq: last setup wins).
            Primary.Setup(c => c.WithReadConcern(ReadConcern.Majority)).Returns(Majority.Object);

            Primary
                .Setup(c => c.FindAsync(It.IsAny<FilterDefinition<MongoRecoveryStateDocument>>(), It.IsAny<FindOptions<MongoRecoveryStateDocument, string>>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(() => Read("primary", current: false, bound: null));
            Primary
                .Setup(c => c.FindAsync(It.IsAny<IClientSessionHandle>(), It.IsAny<FilterDefinition<MongoRecoveryStateDocument>>(), It.IsAny<FindOptions<MongoRecoveryStateDocument, string>>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync((IClientSessionHandle _, FilterDefinition<MongoRecoveryStateDocument> _, FindOptions<MongoRecoveryStateDocument, string> options, CancellationToken _)
                    => Read("primary-session", current: false, options.MaxTime));
            Majority
                .Setup(c => c.FindAsync(It.IsAny<FilterDefinition<MongoRecoveryStateDocument>>(), It.IsAny<FindOptions<MongoRecoveryStateDocument, string>>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(() => Read("majority", current: false, bound: null));
            Majority
                .Setup(c => c.FindAsync(It.IsAny<IClientSessionHandle>(), It.IsAny<FilterDefinition<MongoRecoveryStateDocument>>(), It.IsAny<FindOptions<MongoRecoveryStateDocument, string>>(), It.IsAny<CancellationToken>()))
                .Returns((IClientSessionHandle session, FilterDefinition<MongoRecoveryStateDocument> _, FindOptions<MongoRecoveryStateDocument, string> options, CancellationToken _)
                    => MajorityReadFailure is { } failure
                        ? Task.FromException<IAsyncCursor<string>>(failure)
                        : Task.FromResult(Read("majority-session", current: _barrierSessions.Contains(session), options.MaxTime)));
            Primary
                .Setup(c => c.DeleteOneAsync(It.IsAny<FilterDefinition<MongoRecoveryStateDocument>>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(() =>
                {
                    Interlocked.Increment(ref Deletes);
                    return new DeleteResult.Acknowledged(1);
                });

            Counters.SelfPinning();
            Counters
                .Setup(c => c.UpdateOneAsync(
                    It.IsAny<IClientSessionHandle>(),
                    It.IsAny<FilterDefinition<BsonDocument>>(),
                    It.IsAny<UpdateDefinition<BsonDocument>>(),
                    It.IsAny<UpdateOptions>(),
                    It.IsAny<CancellationToken>()))
                .Returns((IClientSessionHandle session, FilterDefinition<BsonDocument> filter, UpdateDefinition<BsonDocument> update, UpdateOptions options, CancellationToken _) =>
                {
                    Operations.Add("barrier");
                    Barriers.Add((Render(filter), Render(update), options.IsUpsert));
                    if (NextBarrierFailures.TryDequeue(out var next))
                        return Task.FromException<UpdateResult>(next);
                    if (BarrierFailure is { } failure)
                        return Task.FromException<UpdateResult>(failure);

                    _barrierSessions.Add(session);
                    return Task.FromResult<UpdateResult>(new UpdateResult.Acknowledged(1, 1, BsonNull.Value));
                });

            Messages.SelfPinning();
            Subscribers.SelfPinning();
            Subscribers
                .Setup(c => c.CountDocumentsAsync(It.IsAny<FilterDefinition<MongoChannelSubscriberDocument>>(), It.IsAny<CountOptions>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(0);

            Database.Setup(d => d.GetCollection<MongoRecoveryStateDocument>(It.IsAny<string>(), It.IsAny<MongoCollectionSettings>())).Returns(Primary.Object);
            Database.Setup(d => d.GetCollection<MongoChannelMessageDocument>(It.IsAny<string>(), It.IsAny<MongoCollectionSettings>())).Returns(Messages.Object);
            Database.Setup(d => d.GetCollection<MongoChannelSubscriberDocument>(It.IsAny<string>(), It.IsAny<MongoCollectionSettings>())).Returns(Subscribers.Object);
            Database.Setup(d => d.GetCollection<BsonDocument>(It.IsAny<string>(), It.IsAny<MongoCollectionSettings>())).Returns(Counters.Object);

            var options = Options.Create(new MongoDbAsyncResponseChannelOptions
            {
                AutoCreateIndexes = false,
                UseOwnershipLedger = false,
                UseChangeStreams = false,
                ListenerPollInterval = TimeSpan.FromHours(1)
            });
            Store = new MongoDbChannelStore(Database.Object, options);
            RecoveryStore = new MongoDbRecoveryStateStore(Store, NullLogger<MongoDbRecoveryStateStore>.Instance);

            var services = new ServiceCollection();
            services.AddSingleton<IR51Spy>(Spy);
            _services = services.BuildServiceProvider();
            Channel = new MongoDbAsyncResponseChannel(
                _services.GetRequiredService<IServiceScopeFactory>(),
                Store,
                RecoveryStore,
                options,
                new AsyncResponseContextPropagation([]),
                NullLogger<MongoDbAsyncResponseChannel>.Instance);
        }

        /// <summary>Whether the channel persisted a response (it does only when it routes one live).</summary>
        public bool PersistedAResponse => Messages.Invocations.Any(invocation => invocation.Method.Name == nameof(IMongoCollection<MongoChannelMessageDocument>.FindOneAndUpdateAsync));

        private IAsyncCursor<string> Read(string handle, bool current, TimeSpan? bound)
        {
            Operations.Add(current ? "current-read" : $"stale-read:{handle}");
            ReadBounds.Add(bound);
            return new MongoListCursor<string>((current ? CurrentView : StaleView).ToList());
        }

        private static BsonDocument Render(FilterDefinition<BsonDocument> filter)
            => filter.Render(new RenderArgs<BsonDocument>(BsonDocumentSerializer.Instance, BsonSerializer.SerializerRegistry));

        private static BsonDocument Render(UpdateDefinition<BsonDocument> update)
            => update.Render(new RenderArgs<BsonDocument>(BsonDocumentSerializer.Instance, BsonSerializer.SerializerRegistry)).AsBsonDocument;

        public async ValueTask DisposeAsync()
        {
            await Channel.DisposeAsync();
            Store.Dispose();
            await _services.DisposeAsync();
        }
    }

    /// <summary>
    /// The store's side: the lookup raises a majority barrier and reads the registrations at
    /// <c>majority</c> read concern in the same causally consistent session, bounded by
    /// <c>maxTimeMS</c>. The barrier is a real modification (an <c>$inc</c> upsert): an update
    /// that changes nothing writes no oplog entry, and a deposed primary would acknowledge its
    /// "majority" wait against its own stale history. Pre-fix failure: one plain primary read, the
    /// deposed primary's empty answer.
    /// </summary>
    [Fact]
    public async Task MongoRecoveryLookup_RaisesAMajorityBarrier_ThenReadsMajorityInTheSameCausalSession()
    {
        await using var harness = new MongoConsistencyHarness();
        var registration = Registration("r51-mechanics");
        harness.CurrentView.Add(JsonSerializer.Serialize(registration));

        var found = await harness.RecoveryStore.GetAllAsync("r51-mechanics");

        Assert.Equal(registration.RegistrationId, Assert.Single(found).RegistrationId);
        Assert.Equal(new[] { "barrier", "current-read" }, harness.Operations);
        Assert.True(Assert.Single(harness.SessionOptions)!.CausalConsistency);
        Assert.Equal(MongoDbChannelStore.RecoveryReadBarrierId("r51-mechanics"), Assert.Single(harness.Barriers).Filter["_id"].AsString);
        Assert.StartsWith("recovery_read_barrier_", harness.Barriers[0].Filter["_id"].AsString, StringComparison.Ordinal);
        Assert.Equal(new BsonDocument("$inc", new BsonDocument("seq", 1L)), harness.Barriers[0].Update);
        Assert.True(harness.Barriers[0].Upsert);
        Assert.Equal(TimeSpan.FromSeconds(10), Assert.Single(harness.ReadBounds));
    }

    /// <summary>
    /// Stale-empty: the deposed primary holds none of the registrations. Pre-fix failure: the
    /// publish returned normally with no callback invoked and nothing persisted — acknowledged,
    /// with the registration left armed.
    /// </summary>
    [Fact]
    public async Task MongoLostResponse_OnAPrimaryThatMissedTheRegistration_StillReachesItsCallback()
    {
        await using var harness = new MongoConsistencyHarness();
        harness.CurrentView.Add(JsonSerializer.Serialize(Registration("r51-stale-empty")));

        await harness.Channel.SetResponse(Terminal, "r51-stale-empty");

        Assert.Equal(1, harness.Spy.Resumed);
        Assert.Equal(1, harness.Deletes);
        Assert.DoesNotContain(harness.Operations, operation => operation.StartsWith("stale-read", StringComparison.Ordinal));
    }

    /// <summary>
    /// Stale-partial: the deposed primary holds one of two registrations. Pre-fix failure: only
    /// that one was resumed and consumed; the other stayed armed after the acknowledgement.
    /// </summary>
    [Fact]
    public async Task MongoLostResponse_OnAPrimaryThatMissedOneOfTwoRegistrations_ReachesBoth()
    {
        await using var harness = new MongoConsistencyHarness();
        var older = JsonSerializer.Serialize(Registration("r51-stale-partial"));
        harness.StaleView.Add(older);
        harness.CurrentView.Add(older);
        harness.CurrentView.Add(JsonSerializer.Serialize(Registration("r51-stale-partial")));

        await harness.Channel.SetResponse(Terminal, "r51-stale-partial");

        Assert.Equal(2, harness.Spy.Resumed);
        Assert.Equal(2, harness.Deletes);
    }

    /// <summary>The exception route settles on the same lookup. Pre-fix failure: no failure callback ran.</summary>
    [Fact]
    public async Task MongoLostException_OnAPrimaryThatMissedTheRegistration_StillReachesItsFailureCallback()
    {
        await using var harness = new MongoConsistencyHarness();
        harness.CurrentView.Add(JsonSerializer.Serialize(Registration("r51-stale-exception")));

        await harness.Channel.SetException(new TimeoutException("worker gave up"), "r51-stale-exception");

        Assert.Equal(1, harness.Spy.Failed);
        Assert.Equal(1, harness.Deletes);
    }

    public static TheoryData<string> BarrierFailures => ["replication timeout", "not writable primary"];

    /// <summary>
    /// A barrier that cannot be confirmed fails the publish, the store's error inside: nothing is
    /// read, invoked, consumed or persisted, so the transport redelivers the response instead of
    /// acknowledging it (the wrapper type, and the ingress's handling of it, are pinned in
    /// <see cref="Round51NewApiTests"/>). Pre-fix failure: the publish returned normally on the
    /// deposed primary's empty answer.
    /// </summary>
    [Theory]
    [MemberData(nameof(BarrierFailures))]
    public async Task MongoLostResponse_WhenTheBarrierCannotBeConfirmed_IsNotAcknowledged(string failure)
    {
        await using var harness = new MongoConsistencyHarness();
        harness.CurrentView.Add(JsonSerializer.Serialize(Registration("r51-unconfirmed")));
        harness.BarrierFailure = failure == "replication timeout"
            ? MongoReplicationTimeouts.Write()
            : new MongoNotPrimaryException(MongoReplicationTimeouts.Connection, new BsonDocument("update", "counters"), new BsonDocument { ["ok"] = 0, ["code"] = 10107, ["errmsg"] = "not primary" });

        var thrown = await Assert.ThrowsAnyAsync<Exception>(() => harness.Channel.SetResponse(Terminal, "r51-unconfirmed"));

        Assert.Same(harness.BarrierFailure, thrown.InnerException);
        Assert.Equal(0, harness.Spy.Resumed);
        Assert.Equal(0, harness.Deletes);
        Assert.False(harness.PersistedAResponse);
        Assert.Equal(new[] { "barrier" }, harness.Operations);
    }

    // ---------------------------------------------------------------------------------------------
    // F2 — every waiter registered under one correlation id rewrote the id's whole stored value
    //      on Redis and NATS, unbounded: N waiters cost 1 + 2 + … + N serialized registrations.
    //      The fan-out is now bounded (default 64) and the failure is a clear one, at waiter
    //      creation.

    private const int DefaultFanOutLimit = 64;

    /// <summary>Pre-fix failure: the 65th registration was written, and so on without bound.</summary>
    [Fact]
    public async Task NatsRegistration_PastTheDefaultFanOutLimit_IsRefused_WithTheStoredValueUntouched()
    {
        var kv = new FakeNatsKvStore();
        var store = new NatsRecoveryStateStore(kv, Options.Create(new NatsAsyncResponseChannelOptions()), NullLogger<NatsRecoveryStateStore>.Instance, new TestTimeProvider());
        for (var i = 0; i < DefaultFanOutLimit; i++)
            await store.SaveAsync("r51-fan-out", Registration("r51-fan-out"), TimeSpan.FromMinutes(5));
        var before = kv.Entries[NatsSubjectSchema.RecoveryKey("r51-fan-out")];

        var refused = await Assert.ThrowsAsync<InvalidOperationException>(
            () => store.SaveAsync("r51-fan-out", Registration("r51-fan-out"), TimeSpan.FromMinutes(5)));

        Assert.Contains("MaxRecoveryRegistrationsPerCorrelationId", refused.Message, StringComparison.Ordinal);
        Assert.Contains(nameof(NatsAsyncResponseChannelOptions), refused.Message, StringComparison.Ordinal);
        Assert.Equal(before, kv.Entries[NatsSubjectSchema.RecoveryKey("r51-fan-out")]);
        Assert.Equal(DefaultFanOutLimit, (await store.GetAllAsync("r51-fan-out")).Count);
    }

    /// <summary>Pre-fix failure: the 65th registration was written.</summary>
    [Fact]
    public async Task RedisRegistration_PastTheDefaultFanOutLimit_IsRefused_WithTheStoredValueUntouched()
    {
        var redis = new StatefulRedis();
        var store = redis.CreateStore(new TestTimeProvider());
        for (var i = 0; i < DefaultFanOutLimit; i++)
            await store.SaveAsync("r51-fan-out", Registration("r51-fan-out"), TimeSpan.FromMinutes(5));
        var before = redis.Value;
        redis.ResetCounters();

        var refused = await Assert.ThrowsAsync<InvalidOperationException>(
            () => store.SaveAsync("r51-fan-out", Registration("r51-fan-out"), TimeSpan.FromMinutes(5)));

        Assert.Contains("MaxRecoveryRegistrationsPerCorrelationId", refused.Message, StringComparison.Ordinal);
        Assert.Contains(nameof(RedisAsyncResponseOptions), refused.Message, StringComparison.Ordinal);
        Assert.Equal(0, redis.Transactions + redis.Conflicts);
        Assert.Equal(before, redis.Value);
    }

    /// <summary>
    /// Only live, distinct registrations count: at the limit, re-saving an existing registration
    /// still refreshes it, and once the others have expired a new one registers. Green before and
    /// after the fix — the bound must not have changed either.
    /// </summary>
    [Fact]
    public async Task FanOutLimit_CountsOnlyLiveDistinctRegistrations()
    {
        var time = new TestTimeProvider();
        var kv = new FakeNatsKvStore();
        var nats = new NatsRecoveryStateStore(kv, Options.Create(new NatsAsyncResponseChannelOptions()), NullLogger<NatsRecoveryStateStore>.Instance, time);
        var redis = new StatefulRedis();
        var redisStore = redis.CreateStore(time);
        IRecoveryStateStore[] stores = [nats, redisStore];

        foreach (var store in stores)
        {
            var first = Registration("r51-live-only");
            await store.SaveAsync("r51-live-only", first, TimeSpan.FromMinutes(1));
            for (var i = 1; i < DefaultFanOutLimit; i++)
                await store.SaveAsync("r51-live-only", Registration("r51-live-only"), TimeSpan.FromMinutes(1));

            await store.SaveAsync("r51-live-only", first, TimeSpan.FromMinutes(1));

            time.Advance(TimeSpan.FromMinutes(2));
            await store.SaveAsync("r51-live-only", Registration("r51-live-only"), TimeSpan.FromMinutes(1));
            Assert.Single(await store.GetAllAsync("r51-live-only"));
        }
    }

    /// <summary>
    /// A Redis value behind the recovery store's GET and conditional MULTI/EXEC that is safe under
    /// concurrent callers: EXEC commits only while the value is still the one it was when the
    /// transaction was created, and <see cref="ForcedConflicts"/> fails the next EXECs regardless —
    /// the StatefulRedis of round 49, with a lock. The condition is the one Redis evaluates — the
    /// value the writer's own GET returned — not the value when the transaction is created: other
    /// writers' EXECs run on other threads in between. GET completes synchronously, and the store
    /// goes from it to CreateTransaction without awaiting, so that value is the last one GET handed
    /// out on the creating thread. The latency sits in EXEC, which is where concurrent writers overlap.
    /// </summary>
    private sealed class ContendedRedis
    {
        private readonly object _gate = new();
        private readonly Mock<IConnectionMultiplexer> _multiplexer = new();
        private readonly Mock<IDatabase> _database = new();
        private RedisValue _value = RedisValue.Null;

        [ThreadStatic]
        private static RedisValue _lastRead;

        public int ForcedConflicts;
        public int Conflicts;
        public TimeSpan ExecuteLatency { get; init; }

        public ContendedRedis()
        {
            _multiplexer.Setup(m => m.GetDatabase(It.IsAny<int>(), It.IsAny<object?>())).Returns(_database.Object);
            _database
                .Setup(d => d.StringGetAsync(It.IsAny<RedisKey>(), It.IsAny<CommandFlags>()))
                .ReturnsAsync(() =>
                {
                    lock (_gate)
                    {
                        _lastRead = _value;
                        return _value;
                    }
                });
            _database.Setup(d => d.CreateTransaction(It.IsAny<object?>())).Returns(CreateTransaction);
        }

        public RedisValue Value
        {
            get { lock (_gate) return _value; }
        }

        public RedisRecoveryStateStore CreateStore()
            => new(_multiplexer.Object, Options.Create(new RedisAsyncResponseOptions { KeyPrefix = "ar" }), NullLogger<RedisRecoveryStateStore>.Instance);

        private ITransaction CreateTransaction()
        {
            var readAtCreation = _lastRead;

            var transaction = new Mock<ITransaction>();
            transaction
                .Setup(t => t.ExecuteAsync(It.IsAny<CommandFlags>()))
                .Returns(async () =>
                {
                    if (ExecuteLatency > TimeSpan.Zero)
                        await Task.Delay(ExecuteLatency);
                    else
                        await Task.Yield();

                    lock (_gate)
                    {
                        if (ForcedConflicts > 0 || _value != readAtCreation)
                        {
                            if (ForcedConflicts > 0)
                                ForcedConflicts--;
                            Conflicts++;
                            return false;
                        }

                        foreach (var invocation in transaction.Invocations)
                        {
                            if (invocation.Method.Name == nameof(IDatabase.StringSetAsync))
                                _value = (RedisValue)invocation.Arguments[1]!;
                            else if (invocation.Method.Name == nameof(IDatabase.KeyDeleteAsync))
                                _value = RedisValue.Null;
                        }

                        return true;
                    }
                });
            return transaction.Object;
        }
    }

    public static TheoryData<string> Backends => ["NATS", "Redis"];

    /// <summary>
    /// The attempt budget alone is under test, so the paced pauses between attempts are skipped
    /// (<c>RecoveryStateContention.SuppressPauses</c>, reached by name so this file still compiles
    /// against the pre-fix tree, where there are no pauses to skip).
    /// </summary>
    private static IDisposable SuppressContentionPauses()
    {
        var contention = typeof(AsyncResponseIngress).Assembly.GetType("AsyncResponse.RecoveryStateContention");
        if (contention is null)
            return new NoScope();

        // Present but renamed must fail here, not silently sleep through every pause.
        var suppress = contention.GetMethod("SuppressPauses", System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.NonPublic)
                       ?? throw new MissingMethodException(contention.FullName, "SuppressPauses");
        return (IDisposable)suppress.Invoke(null, null)!;
    }

    private sealed class NoScope : IDisposable
    {
        public void Dispose()
        {
        }
    }

    private static (IRecoveryStateStore Store, Func<string?> Stored) ContendedStore(string backend, TimeSpan latency)
    {
        if (backend == "NATS")
        {
            var kv = new FakeNatsKvStore { GetLatency = latency };
            var nats = new NatsRecoveryStateStore(kv, Options.Create(new NatsAsyncResponseChannelOptions()), NullLogger<NatsRecoveryStateStore>.Instance);
            return (nats, () => kv.Entries.TryGetValue(NatsKey("r51-contended"), out var value) ? value : null);
        }

        var redis = new ContendedRedis { ExecuteLatency = latency };
        return (redis.CreateStore(), () => redis.Value.IsNull ? null : redis.Value.ToString());
    }

    /// <summary>
    /// The waiters of a fan-out register together. Pre-fix failure: a loser retried at once and
    /// collided again, so after four immediate attempts all but a handful of a burst failed with
    /// "could not commit" — against real servers 25 to 28 of 32 concurrent registrations on one id.
    /// </summary>
    [Theory]
    [MemberData(nameof(Backends))]
    public async Task ConcurrentRegistrations_OnOneId_AllCommit(string backend)
    {
        var (store, _) = ContendedStore(backend, TimeSpan.FromMilliseconds(1));

        await Task.WhenAll(Enumerable.Range(0, 16).Select(_ => store.SaveAsync("r51-contended", Registration("r51-contended"), TimeSpan.FromMinutes(5))));

        Assert.Equal(16, (await store.GetAllAsync("r51-contended")).Count);
    }

    /// <summary>
    /// The waiters of a fan-out that completes live remove their registrations together. Pre-fix
    /// failure: most removals exhausted their attempts and left the registration for expiry — armed
    /// for a later response to the same id, though its waiter had its answer.
    /// </summary>
    [Theory]
    [MemberData(nameof(Backends))]
    public async Task ConcurrentCompletions_OnOneId_AllRemoveTheirRegistrations(string backend)
    {
        var (store, stored) = ContendedStore(backend, TimeSpan.FromMilliseconds(1));
        var registrations = Enumerable.Range(0, 16).Select(_ => Registration("r51-contended")).ToList();
        foreach (var registration in registrations)
            await store.SaveAsync("r51-contended", registration, TimeSpan.FromMinutes(5));

        var removed = await Task.WhenAll(registrations.Select(registration => store.TryDeleteAsync("r51-contended", registration.RegistrationId)));

        Assert.All(removed, Assert.True);
        Assert.Null(stored());
    }

    /// <summary>
    /// Deterministic: ten conflicts in a row are ridden out. Pre-fix failure: the fifth attempt was
    /// never made — the save threw after four.
    /// </summary>
    [Theory]
    [MemberData(nameof(Backends))]
    public async Task ARegistration_RidesOutMoreConflictsThanTheOldFourAttempts(string backend)
    {
        var kv = new FakeNatsKvStore();
        var redis = new ContendedRedis();
        IRecoveryStateStore store = backend == "NATS"
            ? new NatsRecoveryStateStore(kv, Options.Create(new NatsAsyncResponseChannelOptions()), NullLogger<NatsRecoveryStateStore>.Instance)
            : redis.CreateStore();
        await store.SaveAsync("r51-conflicts", Registration("r51-conflicts"), TimeSpan.FromMinutes(5));
        kv.ForcedUpdateConflicts = 10;
        redis.ForcedConflicts = 10;
        using var noPauses = SuppressContentionPauses();

        await store.SaveAsync("r51-conflicts", Registration("r51-conflicts"), TimeSpan.FromMinutes(5));

        Assert.Equal(2, (await store.GetAllAsync("r51-conflicts")).Count);
        // The conflicts really happened: every forced one was consumed by an attempt.
        Assert.Equal(0, backend == "NATS" ? kv.ForcedUpdateConflicts : redis.ForcedConflicts);
        if (backend != "NATS")
            Assert.Equal(10, redis.Conflicts);
    }

    // ---------------------------------------------------------------------------------------------
    // F3 — a missing expiry deserialized to DateTimeOffset.MinValue and read as long expired. On
    //      NATS the envelope's stamp decided the whole key: a lookup or a watchdog scan deleted it
    //      and reported nothing, while each registration's own expiry was still in the future. On
    //      Redis the entry was pruned as lapsed: the lookup answered "absent" (the response was
    //      acknowledged) and the next save rewrote the key without it.

    private static string NatsKey(string correlationId) => NatsSubjectSchema.RecoveryKey(correlationId);

    private static (FakeNatsKvStore Kv, NatsRecoveryStateStore Store, TestTimeProvider Time) NatsStore()
    {
        var kv = new FakeNatsKvStore();
        var time = new TestTimeProvider();
        var store = new NatsRecoveryStateStore(kv, Options.Create(new NatsAsyncResponseChannelOptions()), NullLogger<NatsRecoveryStateStore>.Instance, time);
        return (kv, store, time);
    }

    /// <summary>Saves real registrations, then edits the stored envelope — the store's own wire form, damaged in one place.</summary>
    private static async Task<string> SeedNatsAsync(FakeNatsKvStore kv, NatsRecoveryStateStore store, string correlationId, Action<JsonObject> damage, int registrations = 1)
    {
        for (var i = 0; i < registrations; i++)
            await store.SaveAsync(correlationId, Registration(correlationId), TimeSpan.FromMinutes(30));

        var envelope = JsonNode.Parse(kv.Entries[NatsKey(correlationId)])!.AsObject();
        damage(envelope);
        return kv.Entries[NatsKey(correlationId)] = envelope.ToJsonString();
    }

    public static TheoryData<string> NatsExpiryDefects =>
    [
        "no envelope expiry",
        "legacy envelope without its expiry",
        "fewer registration expiries than registrations",
        "envelope expiry before a registration's own",
        "no registration list",
    ];

    private static Action<JsonObject> NatsDamage(string defect, TestTimeProvider time) => defect switch
    {
        "no envelope expiry" => envelope => envelope.Remove("ExpiresAtUtc"),
        "legacy envelope without its expiry" => envelope =>
        {
            envelope.Remove("StateExpiries");
            envelope.Remove("ExpiresAtUtc");
        },
        "fewer registration expiries than registrations" => envelope => envelope["StateExpiries"]!.AsArray().RemoveAt(0),
        "envelope expiry before a registration's own" => envelope => envelope["ExpiresAtUtc"] = (time.Now - TimeSpan.FromMinutes(1)).ToString("O"),
        "no registration list" => envelope => envelope.Remove("States"),
        _ => throw new ArgumentOutOfRangeException(nameof(defect))
    };

    /// <summary>
    /// Pre-fix failures: without its stamp (or with a stamp earlier than its registrations') the
    /// envelope read as expired — the lookup returned nothing and deleted the key; a legacy one
    /// likewise; a short expiry list fell back to the envelope stamp and read as fine; a missing
    /// registration list read as "no registrations". Each is now refused as unreadable, and the
    /// key is left exactly as it was.
    /// </summary>
    [Theory]
    [MemberData(nameof(NatsExpiryDefects))]
    public async Task NatsLookup_AnEnvelopeWhoseExpiryCannotBeEstablished_IsRefusedAndKept(string defect)
    {
        var (kv, store, time) = NatsStore();
        var stored = await SeedNatsAsync(kv, store, "r51-nats-defect", NatsDamage(defect, time), registrations: 2);

        var refused = await Assert.ThrowsAsync<RecoveryStateUnreadableException>(() => store.GetAllAsync("r51-nats-defect"));

        Assert.Equal("r51-nats-defect", refused.CorrelationId);
        Assert.Equal(stored, kv.Entries[NatsKey("r51-nats-defect")]);
        Assert.Equal(0, kv.DeleteCount);
    }

    /// <summary>
    /// A key holding a JSON <c>null</c> — no envelope at all — is unreadable too, never absence
    /// (green before and after: the pre-fix store already refused it, silently).
    /// </summary>
    [Fact]
    public async Task NatsLookup_AKeyHoldingNoEnvelope_IsRefusedAndKept()
    {
        var (kv, store, _) = NatsStore();
        kv.Entries[NatsKey("r51-nats-null")] = "null";

        await Assert.ThrowsAsync<RecoveryStateUnreadableException>(() => store.GetAllAsync("r51-nats-null"));

        Assert.Equal("null", kv.Entries[NatsKey("r51-nats-null")]);
        Assert.Equal(0, kv.DeleteCount);
    }

    /// <summary>
    /// The watchdog scan counts such an envelope as unreadable (health goes Degraded) and deletes
    /// nothing. Pre-fix failure: the scan completed clean with zero registrations after deleting
    /// the key.
    /// </summary>
    [Theory]
    [MemberData(nameof(NatsExpiryDefects))]
    public async Task NatsScan_AnEnvelopeWhoseExpiryCannotBeEstablished_IsReportedNotDeleted(string defect)
    {
        var (kv, store, time) = NatsStore();
        await store.SaveAsync("r51-nats-healthy", Registration("r51-nats-healthy"), TimeSpan.FromMinutes(30));
        var stored = await SeedNatsAsync(kv, store, "r51-nats-scan", NatsDamage(defect, time));

        var scanned = new List<RecoveryState>();
        var unreadable = await Assert.ThrowsAsync<RecoveryStateScanUnreadableException>(async () =>
        {
            await foreach (var state in store.ScanAsync())
                scanned.Add(state);
        });

        Assert.Equal(1, unreadable.UnreadableCount);
        Assert.Equal("r51-nats-healthy", Assert.Single(scanned).CorrelationId);
        Assert.Equal(stored, kv.Entries[NatsKey("r51-nats-scan")]);
        Assert.Equal(0, kv.DeleteCount);
    }

    /// <summary>
    /// A save or a consumed registration's delete must not rewrite the envelope either. Pre-fix
    /// failures: the save read the stamp-less envelope as expired and committed just the new
    /// registration over it; the delete deleted the whole key.
    /// </summary>
    [Fact]
    public async Task NatsWrites_OverAnEnvelopeWithoutItsExpiry_LeaveItUntouched()
    {
        var (kv, store, time) = NatsStore();
        var stored = await SeedNatsAsync(kv, store, "r51-nats-write", NatsDamage("no envelope expiry", time));
        var existing = JsonNode.Parse(stored)!["States"]![0]!["RegistrationId"]!.GetValue<Guid>();

        await Assert.ThrowsAsync<RecoveryStateUnreadableException>(
            () => store.SaveAsync("r51-nats-write", Registration("r51-nats-write"), TimeSpan.FromMinutes(5)));
        Assert.False(await store.TryDeleteAsync("r51-nats-write", existing));

        Assert.Equal(stored, kv.Entries[NatsKey("r51-nats-write")]);
        Assert.Equal(0, kv.DeleteCount);
    }

    /// <summary>
    /// Controls, green before and after: a well-formed envelope whose stamps have all lapsed is
    /// still absence and is still deleted; a legacy envelope (no per-registration expiries) with
    /// its stamp is read; and an envelope stamp LATER than every registration's own is accepted —
    /// it cannot hide a live registration, it only delays the whole-key fast path.
    /// </summary>
    [Fact]
    public async Task NatsControls_GenuinelyExpiredEnvelopesStillDisappear_AndLegacyOrLaterStampsStillRead()
    {
        var (kv, store, time) = NatsStore();
        await store.SaveAsync("r51-nats-lapsed", Registration("r51-nats-lapsed"), TimeSpan.FromMinutes(1));
        time.Advance(TimeSpan.FromMinutes(2));
        Assert.Empty(await store.GetAllAsync("r51-nats-lapsed"));
        Assert.False(kv.Entries.ContainsKey(NatsKey("r51-nats-lapsed")));

        await SeedNatsAsync(kv, store, "r51-nats-legacy", envelope => envelope.Remove("StateExpiries"));
        Assert.Single(await store.GetAllAsync("r51-nats-legacy"));

        await SeedNatsAsync(kv, store, "r51-nats-later", envelope => envelope["ExpiresAtUtc"] = (time.Now + TimeSpan.FromDays(1)).ToString("O"));
        Assert.Single(await store.GetAllAsync("r51-nats-later"));
    }

    private static string RedisEntry(RecoveryState state, DateTimeOffset? expiresAtUtc)
        => expiresAtUtc is { } expires
            ? $"{{\"State\":{JsonSerializer.Serialize(state)},\"ExpiresAtUtc\":\"{expires:O}\"}}"
            : $"{{\"State\":{JsonSerializer.Serialize(state)}}}";

    private static string RedisEnvelope(params string[] entries) => "{\"Registrations\":[" + string.Join(",", entries) + "]}";

    public static TheoryData<string> RedisExpiryDefects =>
    [
        "an entry without its expiry",
        "an entry without its expiry beside a live one",
        "a null entry",
        "no registration list",
        "a JSON null",
    ];

    private static string RedisDamaged(string defect, string correlationId, DateTimeOffset now) => defect switch
    {
        "an entry without its expiry" => RedisEnvelope(RedisEntry(Registration(correlationId), null)),
        "an entry without its expiry beside a live one" => RedisEnvelope(
            RedisEntry(Registration(correlationId), now + TimeSpan.FromMinutes(5)),
            RedisEntry(Registration(correlationId), null)),
        "a null entry" => RedisEnvelope(RedisEntry(Registration(correlationId), now + TimeSpan.FromMinutes(5)), "null"),
        "no registration list" => "{}",
        "a JSON null" => "null",
        _ => throw new ArgumentOutOfRangeException(nameof(defect))
    };

    /// <summary>
    /// Pre-fix failures: the stamp-less entry was pruned as lapsed (so the lookup answered with
    /// nothing, or with the live sibling alone as if it were the whole set), a null entry the
    /// same, and a value with no registration list read as an empty one. Each is now refused.
    /// </summary>
    [Theory]
    [MemberData(nameof(RedisExpiryDefects))]
    public async Task RedisLookup_AValueWhoseExpiryCannotBeEstablished_IsRefused(string defect)
    {
        var time = new TestTimeProvider();
        var redis = new StatefulRedis { Value = RedisDamaged(defect, "r51-redis-defect", time.Now) };
        var store = redis.CreateStore(time);

        var refused = await Assert.ThrowsAsync<RecoveryStateUnreadableException>(() => store.GetAllAsync("r51-redis-defect"));

        Assert.Equal(1, refused.UnreadableCount);
    }

    /// <summary>
    /// A save or a consumed registration's delete must not rewrite such a value: the save would
    /// drop the entry (or invent an expiry for it), and the delete would drop it along with its
    /// target. Pre-fix failures: the save committed the value without the stamp-less entry, and
    /// the delete of its live sibling deleted the key.
    /// </summary>
    [Theory]
    [MemberData(nameof(RedisExpiryDefects))]
    public async Task RedisWrites_OverAValueWhoseExpiryCannotBeEstablished_LeaveItUntouched(string defect)
    {
        var time = new TestTimeProvider();
        var live = Registration("r51-redis-write");
        var damaged = defect == "an entry without its expiry beside a live one"
            ? RedisEnvelope(RedisEntry(live, time.Now + TimeSpan.FromMinutes(5)), RedisEntry(Registration("r51-redis-write"), null))
            : RedisDamaged(defect, "r51-redis-write", time.Now);
        var redis = new StatefulRedis { Value = damaged };
        var store = redis.CreateStore(time);

        await Assert.ThrowsAsync<RecoveryStateUnreadableException>(
            () => store.SaveAsync("r51-redis-write", Registration("r51-redis-write"), TimeSpan.FromMinutes(5)));
        Assert.False(await store.TryDeleteAsync("r51-redis-write", live.RegistrationId));

        Assert.Equal(damaged, redis.Value.ToString());
        Assert.Equal(0, redis.Sets + redis.KeyDeletes);
    }

    /// <summary>
    /// The scan yields the readable registrations and then reports the rest. Pre-fix failure: the
    /// stamp-less entry vanished from the scan, which completed clean.
    /// </summary>
    [Fact]
    public async Task RedisScanRead_AnEntryWithoutItsExpiry_IsCountedUnreadable_BesideTheLiveOne()
    {
        var time = new TestTimeProvider();
        var live = Registration("r51-redis-scan");
        var redis = new StatefulRedis
        {
            Value = RedisEnvelope(RedisEntry(live, time.Now + TimeSpan.FromMinutes(5)), RedisEntry(Registration("r51-redis-scan"), null))
        };
        var store = redis.CreateStore(time);
        var readScanBatch = typeof(RedisRecoveryStateStore).GetMethod("ReadScanBatchAsync", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!;

        var (states, unreadable) = await (Task<(List<RecoveryState>, int)>)readScanBatch.Invoke(store, [new List<string> { "ar:recovery:r51-redis-scan" }, CancellationToken.None])!;

        Assert.Equal(live.RegistrationId, Assert.Single(states).RegistrationId);
        Assert.Equal(1, unreadable);
    }

    /// <summary>
    /// Controls, green before and after: an entry whose own stamp has lapsed is absence (the lookup
    /// does not throw), a save prunes it, and a legacy bare-array value is still read.
    /// </summary>
    [Fact]
    public async Task RedisControls_GenuinelyExpiredEntriesStillDisappear_AndLegacyValuesStillRead()
    {
        var time = new TestTimeProvider();
        var lapsed = Registration("r51-redis-lapsed");
        var redis = new StatefulRedis { Value = RedisEnvelope(RedisEntry(lapsed, time.Now - TimeSpan.FromMinutes(1))) };
        var store = redis.CreateStore(time);

        Assert.Empty(await store.GetAllAsync("r51-redis-lapsed"));
        await store.SaveAsync("r51-redis-lapsed", Registration("r51-redis-lapsed"), TimeSpan.FromMinutes(5));
        Assert.DoesNotContain(lapsed.RegistrationId.ToString(), redis.Value.ToString(), StringComparison.OrdinalIgnoreCase);
        Assert.Single(await store.GetAllAsync("r51-redis-lapsed"));

        redis.Value = JsonSerializer.Serialize(new[] { Registration("r51-redis-legacy") });
        Assert.Single(await store.GetAllAsync("r51-redis-legacy"));
    }

    /// <summary>
    /// End to end through the publisher: the damaged registration is not settled. Pre-fix failure:
    /// the publish returned normally — the transport would acknowledge it — with no callback run,
    /// and on NATS the envelope was deleted with it.
    /// </summary>
    [Theory]
    [InlineData("NATS")]
    [InlineData("Redis")]
    public async Task LostResponse_OverARegistrationWhoseExpiryCannotBeEstablished_IsNotSettled_AndTheRecordIsKept(string backend)
    {
        var spy = new R51Spy();
        var time = new TestTimeProvider();
        IRecoveryStateStore recoveryStore;
        Func<string> stored;
        if (backend == "NATS")
        {
            var kv = new FakeNatsKvStore();
            var nats = new NatsRecoveryStateStore(kv, Options.Create(new NatsAsyncResponseChannelOptions()), NullLogger<NatsRecoveryStateStore>.Instance, time);
            await SeedNatsAsync(kv, nats, "r51-e2e", NatsDamage("no envelope expiry", time));
            recoveryStore = nats;
            stored = () => kv.Entries.TryGetValue(NatsKey("r51-e2e"), out var value) ? value : "(deleted)";
        }
        else
        {
            var redis = new StatefulRedis { Value = RedisEnvelope(RedisEntry(Registration("r51-e2e"), null)) };
            recoveryStore = redis.CreateStore(time);
            stored = () => redis.Value.ToString();
        }

        var before = stored();
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddSingleton<IR51Spy>(spy);
        services.AddSingleton(recoveryStore);
        services.AddAsyncResponse().WithInMemoryChannel();
        await using var provider = services.BuildServiceProvider();
        var publisher = provider.GetRequiredService<IAsyncResponsePublisher>();

        await Assert.ThrowsAsync<RecoveryStateUnreadableException>(() => publisher.SetResponse(Terminal, "r51-e2e"));

        Assert.Equal(0, spy.Resumed + spy.Failed);
        Assert.Equal(before, stored());
    }
}
