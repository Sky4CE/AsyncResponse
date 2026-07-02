using AsyncResponse.Channels.Redis;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using StackExchange.Redis;
using System.Net;
using System.Text.Json;
using Xunit;

namespace AsyncResponse.Tests;

public class RedisRecoveryStateStoreTests
{
    private readonly Mock<IConnectionMultiplexer> _multiplexer = new();
    private readonly Mock<IDatabase> _database = new();
    private readonly RedisRecoveryStateStore _store;
    private readonly List<Mock<ITransaction>> _transactions = new();

    public RedisRecoveryStateStoreTests()
    {
        _multiplexer
            .Setup(m => m.GetDatabase(It.IsAny<int>(), It.IsAny<object?>()))
            .Returns(_database.Object);

        _store = new RedisRecoveryStateStore(
            _multiplexer.Object,
            Options.Create(new RedisAsyncResponseOptions { KeyPrefix = "ar" }),
            NullLogger<RedisRecoveryStateStore>.Instance);
    }

    /// <summary>
    /// Wires <see cref="IDatabase.CreateTransaction"/> to hand out one recording transaction per
    /// CAS attempt; the nth transaction's ExecuteAsync returns the nth result (the last result
    /// repeats), so a <c>false</c> simulates a condition conflict exactly like Redis.
    /// </summary>
    private void SetupTransactions(params bool[] executeResults)
    {
        var next = 0;
        _database
            .Setup(d => d.CreateTransaction(It.IsAny<object?>()))
            .Returns(() =>
            {
                var transaction = new Mock<ITransaction>();
                transaction
                    .Setup(t => t.StringSetAsync(It.IsAny<RedisKey>(), It.IsAny<RedisValue>(), It.IsAny<TimeSpan?>(), It.IsAny<When>(), It.IsAny<CommandFlags>()))
                    .ReturnsAsync(true);
                transaction
                    .Setup(t => t.StringSetAsync(It.IsAny<RedisKey>(), It.IsAny<RedisValue>(), It.IsAny<Expiration>(), It.IsAny<ValueCondition>(), It.IsAny<CommandFlags>()))
                    .ReturnsAsync(true);
                transaction
                    .Setup(t => t.KeyDeleteAsync(It.IsAny<RedisKey>(), It.IsAny<CommandFlags>()))
                    .ReturnsAsync(true);
                var result = executeResults[Math.Min(next++, executeResults.Length - 1)];
                transaction
                    .Setup(t => t.ExecuteAsync(It.IsAny<CommandFlags>()))
                    .ReturnsAsync(result);
                _transactions.Add(transaction);
                return transaction.Object;
            });
    }

    private static RedisValue WrittenValue(Mock<ITransaction> transaction)
        => (RedisValue)Assert.Single(
            transaction.Invocations,
            invocation => invocation.Method.Name == nameof(IDatabase.StringSetAsync)).Arguments[1]!;

    [Fact]
    public async Task SaveAsync_ValidatesAndPersistsJsonWithTtl()
    {
        _database
            .Setup(d => d.StringGetAsync((RedisKey)"ar:recovery:corr-a", It.IsAny<CommandFlags>()))
            .ReturnsAsync(RedisValue.Null);
        SetupTransactions(true);

        var state = new RecoveryState
        {
            CorrelationId = "corr-a",
            PayloadTypeFullName = typeof(OperationResult).FullName,
            RegisteredAtUtc = DateTime.UtcNow
        };

        await _store.SaveAsync("corr-a", state, TimeSpan.FromMinutes(3));

        var transaction = Assert.Single(_transactions);
        Assert.Single(transaction.Invocations, invocation => invocation.Method.Name == nameof(ITransaction.AddCondition));
        var stringSet = Assert.Single(transaction.Invocations, invocation => invocation.Method.Name == nameof(IDatabase.StringSetAsync));
        Assert.Equal("ar:recovery:corr-a", stringSet.Arguments[0]!.ToString());
        Assert.Equal("EX 180", stringSet.Arguments[2]!.ToString());
        var savedStates = JsonSerializer.Deserialize<List<RecoveryState>>(((RedisValue)stringSet.Arguments[1]!).ToString());
        var savedState = Assert.Single(savedStates!);
        Assert.Equal("corr-a", savedState!.CorrelationId);
        Assert.NotEqual(Guid.Empty, savedState.RegistrationId);

        await Assert.ThrowsAsync<ArgumentException>(() => _store.SaveAsync(" ", state, TimeSpan.FromSeconds(1)));
        await Assert.ThrowsAsync<ArgumentNullException>(() => _store.SaveAsync("corr-a", null!, TimeSpan.FromSeconds(1)));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => _store.SaveAsync("corr-a", state, TimeSpan.Zero));

        using var canceled = new CancellationTokenSource();
        await canceled.CancelAsync();
        await Assert.ThrowsAsync<OperationCanceledException>(() =>
            _store.SaveAsync("corr-a", state, TimeSpan.FromSeconds(1), canceled.Token));
    }

    [Fact]
    public async Task GetAsync_ReturnsNullForMissingOrMalformedState()
    {
        _database
            .Setup(d => d.StringGetAsync((RedisKey)"ar:recovery:missing", It.IsAny<CommandFlags>()))
            .ReturnsAsync(RedisValue.Null);
        _database
            .Setup(d => d.StringGetAsync((RedisKey)"ar:recovery:broken", It.IsAny<CommandFlags>()))
            .ReturnsAsync("{not-json");

        Assert.Null(await _store.GetAsync("missing"));
        Assert.Null(await _store.GetAsync("broken"));
        await Assert.ThrowsAsync<ArgumentException>(() => _store.GetAsync(" "));

        using var canceled = new CancellationTokenSource();
        await canceled.CancelAsync();
        await Assert.ThrowsAsync<OperationCanceledException>(() => _store.GetAsync("missing", canceled.Token));
    }

    [Fact]
    public async Task GetAsync_DeserializesStoredState()
    {
        var state = new RecoveryState
        {
            CorrelationId = "corr-a",
            PayloadTypeFullName = typeof(OperationResult).FullName,
            RegisteredAtUtc = DateTime.UtcNow
        };
        _database
            .Setup(d => d.StringGetAsync((RedisKey)"ar:recovery:corr-a", It.IsAny<CommandFlags>()))
            .ReturnsAsync(JsonSerializer.Serialize(state));

        var loaded = await _store.GetAsync("corr-a");

        Assert.NotNull(loaded);
        Assert.Equal("corr-a", loaded!.CorrelationId);
        Assert.Equal(typeof(OperationResult).FullName, loaded.PayloadTypeFullName);
    }

    [Fact]
    public async Task GetAllAsync_DeserializesArrayAndLegacySingleObject()
    {
        var first = new RecoveryState { RegistrationId = Guid.NewGuid(), CorrelationId = "corr-a" };
        var second = new RecoveryState { RegistrationId = Guid.NewGuid(), CorrelationId = "corr-a" };
        _database
            .Setup(d => d.StringGetAsync((RedisKey)"ar:recovery:corr-a", It.IsAny<CommandFlags>()))
            .ReturnsAsync(JsonSerializer.Serialize(new[] { first, second }));
        _database
            .Setup(d => d.StringGetAsync((RedisKey)"ar:recovery:legacy", It.IsAny<CommandFlags>()))
            .ReturnsAsync(JsonSerializer.Serialize(new RecoveryState { CorrelationId = "legacy" }));

        Assert.Equal(2, (await _store.GetAllAsync("corr-a")).Count);
        Assert.Single(await _store.GetAllAsync("legacy"));
    }

    [Fact]
    public async Task TryDeleteAsync_DeletesRecoveryKey()
    {
        RedisKey deletedKey = default;
        _database
            .Setup(d => d.KeyDeleteAsync(It.IsAny<RedisKey>(), It.IsAny<CommandFlags>()))
            .Callback<RedisKey, CommandFlags>((key, _) => deletedKey = key)
            .ReturnsAsync(true);

        Assert.True(await _store.TryDeleteAsync("corr-a"));
        Assert.Equal("ar:recovery:corr-a", deletedKey.ToString());

        await Assert.ThrowsAsync<ArgumentException>(() => _store.TryDeleteAsync(" "));

        using var canceled = new CancellationTokenSource();
        await canceled.CancelAsync();
        await Assert.ThrowsAsync<OperationCanceledException>(() => _store.TryDeleteAsync("corr-a", canceled.Token));
    }

    [Fact]
    public async Task TryDeleteAsync_WithRegistrationId_RemovesOnlyThatRegistration()
    {
        var firstId = Guid.NewGuid();
        var secondId = Guid.NewGuid();
        _database
            .Setup(d => d.StringGetAsync((RedisKey)"ar:recovery:corr-a", It.IsAny<CommandFlags>()))
            .ReturnsAsync(JsonSerializer.Serialize(new[]
            {
                new RecoveryState { RegistrationId = firstId, CorrelationId = "corr-a" },
                new RecoveryState { RegistrationId = secondId, CorrelationId = "corr-a" }
            }));
        SetupTransactions(true);

        Assert.True(await _store.TryDeleteAsync("corr-a", firstId));

        var transaction = Assert.Single(_transactions);
        Assert.Single(transaction.Invocations, invocation => invocation.Method.Name == nameof(ITransaction.AddCondition));
        var remaining = Assert.Single(JsonSerializer.Deserialize<List<RecoveryState>>(WrittenValue(transaction).ToString())!);
        Assert.Equal(secondId, remaining.RegistrationId);
    }

    [Fact]
    public async Task SaveAsync_CompetingWriteBetweenReadAndWrite_RetriesAndKeepsAllRegistrations()
    {
        var first = new RecoveryState { RegistrationId = Guid.NewGuid(), CorrelationId = "corr-a" };
        var competing = new RecoveryState { RegistrationId = Guid.NewGuid(), CorrelationId = "corr-a" };

        // A competing waiter's registration lands between our read and our conditional write: the
        // first transaction fails its condition, and the retry re-reads the merged list. The old
        // read-modify-write overwrote `competing` here.
        _database
            .SetupSequence(d => d.StringGetAsync((RedisKey)"ar:recovery:corr-a", It.IsAny<CommandFlags>()))
            .ReturnsAsync(JsonSerializer.Serialize(new[] { first }))
            .ReturnsAsync(JsonSerializer.Serialize(new[] { first, competing }));
        SetupTransactions(false, true);

        var second = new RecoveryState { RegistrationId = Guid.NewGuid(), CorrelationId = "corr-a" };
        await _store.SaveAsync("corr-a", second, TimeSpan.FromMinutes(3));

        Assert.Equal(2, _transactions.Count);
        var written = JsonSerializer.Deserialize<List<RecoveryState>>(WrittenValue(_transactions[1]).ToString())!;
        Assert.Equal(3, written.Count);
        Assert.Contains(written, s => s.RegistrationId == first.RegistrationId);
        Assert.Contains(written, s => s.RegistrationId == competing.RegistrationId);
        Assert.Contains(written, s => s.RegistrationId == second.RegistrationId);
    }

    [Fact]
    public async Task TryDeleteAsync_CompetingSaveBetweenReadAndDelete_RetriesIntoConditionalUpdate()
    {
        var first = new RecoveryState { RegistrationId = Guid.NewGuid(), CorrelationId = "corr-a" };
        var competing = new RecoveryState { RegistrationId = Guid.NewGuid(), CorrelationId = "corr-a" };

        // First read sees only `first`, so the attempt is a whole-key delete — which must fail its
        // condition because `competing` registered in between; the retry becomes a conditional
        // update that removes only the targeted registration.
        _database
            .SetupSequence(d => d.StringGetAsync((RedisKey)"ar:recovery:corr-a", It.IsAny<CommandFlags>()))
            .ReturnsAsync(JsonSerializer.Serialize(new[] { first }))
            .ReturnsAsync(JsonSerializer.Serialize(new[] { first, competing }));
        SetupTransactions(false, true);

        Assert.True(await _store.TryDeleteAsync("corr-a", first.RegistrationId));

        Assert.Equal(2, _transactions.Count);
        Assert.Single(_transactions[0].Invocations, invocation => invocation.Method.Name == nameof(IDatabase.KeyDeleteAsync));
        var survivor = Assert.Single(JsonSerializer.Deserialize<List<RecoveryState>>(WrittenValue(_transactions[1]).ToString())!);
        Assert.Equal(competing.RegistrationId, survivor.RegistrationId);
    }

    [Fact]
    public async Task SaveAsync_ExhaustedOptimisticAttempts_FallsBackToUnconditionalWrite()
    {
        _database
            .Setup(d => d.StringGetAsync((RedisKey)"ar:recovery:corr-a", It.IsAny<CommandFlags>()))
            .ReturnsAsync(RedisValue.Null);
        SetupTransactions(false);
        _database
            .Setup(d => d.StringSetAsync(
                It.IsAny<RedisKey>(),
                It.IsAny<RedisValue>(),
                It.IsAny<TimeSpan?>(),
                It.IsAny<When>(),
                It.IsAny<CommandFlags>()))
            .ReturnsAsync(true);
        _database
            .Setup(d => d.StringSetAsync(
                It.IsAny<RedisKey>(),
                It.IsAny<RedisValue>(),
                It.IsAny<Expiration>(),
                It.IsAny<ValueCondition>(),
                It.IsAny<CommandFlags>()))
            .ReturnsAsync(true);

        var state = new RecoveryState { RegistrationId = Guid.NewGuid(), CorrelationId = "corr-a" };
        await _store.SaveAsync("corr-a", state, TimeSpan.FromMinutes(3));

        // Registration must never fail the wait: after exhausting conditional attempts, the save
        // degrades to last-writer-wins directly on the database.
        Assert.Equal(4, _transactions.Count);
        var fallback = Assert.Single(_database.Invocations, invocation => invocation.Method.Name == nameof(IDatabase.StringSetAsync));
        var written = Assert.Single(JsonSerializer.Deserialize<List<RecoveryState>>(((RedisValue)fallback.Arguments[1]!).ToString())!);
        Assert.Equal(state.RegistrationId, written.RegistrationId);
    }

    [Fact]
    public async Task TryDeleteAsync_ExhaustedOptimisticAttempts_LeavesRegistrationForExpiry()
    {
        var first = new RecoveryState { RegistrationId = Guid.NewGuid(), CorrelationId = "corr-a" };
        _database
            .Setup(d => d.StringGetAsync((RedisKey)"ar:recovery:corr-a", It.IsAny<CommandFlags>()))
            .ReturnsAsync(JsonSerializer.Serialize(new[] { first }));
        SetupTransactions(false);

        Assert.False(await _store.TryDeleteAsync("corr-a", first.RegistrationId));

        // No unconditional delete or write may run — a lost concurrent registration is worse than
        // one row waiting out its TTL.
        Assert.Equal(4, _transactions.Count);
        Assert.DoesNotContain(_database.Invocations, invocation => invocation.Method.Name == nameof(IDatabase.KeyDeleteAsync));
        Assert.DoesNotContain(_database.Invocations, invocation => invocation.Method.Name == nameof(IDatabase.StringSetAsync));
    }

    [Fact]
    public async Task ScanAsync_YieldsUniqueReadableStatesFromConnectedServers()
    {
        var endpointA = new DnsEndPoint("redis-a", 6379);
        var endpointB = new DnsEndPoint("redis-b", 6379);
        var connected = new Mock<IServer>();
        var disconnected = new Mock<IServer>();

        connected.SetupGet(s => s.IsConnected).Returns(true);
        disconnected.SetupGet(s => s.IsConnected).Returns(false);
        connected
            .Setup(s => s.Keys(
                It.IsAny<int>(),
                It.IsAny<RedisValue>(),
                It.IsAny<int>(),
                It.IsAny<CommandFlags>()))
            .Returns([
                (RedisKey)"ar:recovery:corr-a",
                (RedisKey)"ar:recovery:corr-b",
                (RedisKey)"ar:recovery:corr-a",
                (RedisKey)"ar:recovery:empty",
                (RedisKey)"ar:recovery:broken",
                (RedisKey)"ar:recovery:null-state"
            ]);
        connected
            .Setup(s => s.Keys(
                It.IsAny<int>(),
                It.IsAny<RedisValue>(),
                It.IsAny<int>(),
                It.IsAny<long>(),
                It.IsAny<int>(),
                It.IsAny<CommandFlags>()))
            .Returns([
                (RedisKey)"ar:recovery:corr-a",
                (RedisKey)"ar:recovery:corr-b",
                (RedisKey)"ar:recovery:corr-a",
                (RedisKey)"ar:recovery:empty",
                (RedisKey)"ar:recovery:broken",
                (RedisKey)"ar:recovery:null-state"
            ]);

        _multiplexer.Setup(m => m.GetEndPoints(It.IsAny<bool>())).Returns([endpointA, endpointB]);
        _multiplexer
            .Setup(m => m.GetServer(It.IsAny<EndPoint>(), It.IsAny<object?>()))
            .Returns<EndPoint, object?>((endpoint, _) => ReferenceEquals(endpoint, endpointA) ? connected.Object : disconnected.Object);

        _database
            .Setup(d => d.StringGetAsync((RedisKey)"ar:recovery:corr-a", It.IsAny<CommandFlags>()))
            .ReturnsAsync(JsonSerializer.Serialize(new RecoveryState
            {
                CorrelationId = "explicit-corr",
                PayloadTypeFullName = typeof(OperationResult).FullName,
                RegisteredAtUtc = DateTime.UtcNow
            }));
        _database
            .Setup(d => d.StringGetAsync((RedisKey)"ar:recovery:corr-b", It.IsAny<CommandFlags>()))
            .ReturnsAsync(JsonSerializer.Serialize(new RecoveryState
            {
                PayloadTypeFullName = typeof(OperationResult).FullName,
                RegisteredAtUtc = DateTime.UtcNow
            }));
        _database
            .Setup(d => d.StringGetAsync((RedisKey)"ar:recovery:empty", It.IsAny<CommandFlags>()))
            .ReturnsAsync(RedisValue.Null);
        _database
            .Setup(d => d.StringGetAsync((RedisKey)"ar:recovery:broken", It.IsAny<CommandFlags>()))
            .ReturnsAsync("{not-json");
        _database
            .Setup(d => d.StringGetAsync((RedisKey)"ar:recovery:null-state", It.IsAny<CommandFlags>()))
            .ReturnsAsync("null");

        var states = new List<RecoveryState>();
        await foreach (var state in _store.ScanAsync())
            states.Add(state);

        Assert.Equal(2, states.Count);
        Assert.Contains(states, state => state.CorrelationId == "explicit-corr");
        Assert.Contains(states, state => state.CorrelationId == "corr-b");
    }

    [Fact]
    public async Task ScanAsync_ObservesCancellationInsideServerEnumeration()
    {
        var endpoint = new DnsEndPoint("redis-a", 6379);
        var server = new Mock<IServer>();
        server.SetupGet(s => s.IsConnected).Returns(true);
        server
            .Setup(s => s.Keys(
                It.IsAny<int>(),
                It.IsAny<RedisValue>(),
                It.IsAny<int>(),
                It.IsAny<CommandFlags>()))
            .Returns([(RedisKey)"ar:recovery:corr-a"]);
        server
            .Setup(s => s.Keys(
                It.IsAny<int>(),
                It.IsAny<RedisValue>(),
                It.IsAny<int>(),
                It.IsAny<long>(),
                It.IsAny<int>(),
                It.IsAny<CommandFlags>()))
            .Returns([(RedisKey)"ar:recovery:corr-a"]);

        _multiplexer.Setup(m => m.GetEndPoints(It.IsAny<bool>())).Returns([endpoint]);
        _multiplexer.Setup(m => m.GetServer(It.IsAny<EndPoint>(), It.IsAny<object?>())).Returns(server.Object);

        using var canceled = new CancellationTokenSource();
        await canceled.CancelAsync();

        await Assert.ThrowsAsync<OperationCanceledException>(async () =>
        {
            await foreach (var _ in _store.ScanAsync(canceled.Token))
            {
            }
        });
    }
}
