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

    [Fact]
    public async Task SaveAsync_ValidatesAndPersistsJsonWithTtl()
    {
        RedisValue savedValue = default;
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
                It.IsAny<TimeSpan?>(),
                It.IsAny<bool>(),
                It.IsAny<When>(),
                It.IsAny<CommandFlags>()))
            .ReturnsAsync(true);

        var state = new RecoveryState
        {
            CorrelationId = "corr-a",
            PayloadTypeFullName = typeof(OperationResult).FullName,
            RegisteredAtUtc = DateTime.UtcNow
        };

        await _store.SaveAsync("corr-a", state, TimeSpan.FromMinutes(3));

        var stringSet = Assert.Single(_database.Invocations, invocation => invocation.Method.Name == nameof(IDatabase.StringSetAsync));
        Assert.Equal("ar:recovery:corr-a", stringSet.Arguments[0]!.ToString());
        Assert.Equal("EX 180", stringSet.Arguments[2]!.ToString());
        savedValue = (RedisValue)stringSet.Arguments[1]!;
        var savedState = JsonSerializer.Deserialize<RecoveryState>(savedValue.ToString());
        Assert.Equal("corr-a", savedState!.CorrelationId);

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
