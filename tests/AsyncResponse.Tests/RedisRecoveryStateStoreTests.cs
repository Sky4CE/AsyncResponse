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
    private readonly TestTimeProvider _time = new();
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
            NullLogger<RedisRecoveryStateStore>.Instance,
            _time);
    }

    /// <summary>
    /// Builds the enveloped blob shape the store writes: each registration paired with its own
    /// absolute expiry. Raw JSON (rather than the store's internal types) so shape drift fails
    /// loudly here.
    /// </summary>
    private static string EnvelopeBlob(params (RecoveryState State, DateTimeOffset ExpiresAtUtc)[] registrations)
        => "{\"Registrations\":["
           + string.Join(",", registrations.Select(registration =>
               $"{{\"State\":{JsonSerializer.Serialize(registration.State)},\"ExpiresAtUtc\":\"{registration.ExpiresAtUtc:O}\"}}"))
           + "]}";

    private static JsonElement WrittenRegistration(JsonElement envelope, Guid registrationId)
        => envelope.GetProperty("Registrations").EnumerateArray()
            .Single(entry => entry.GetProperty("State").GetProperty("RegistrationId").GetGuid() == registrationId);

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
            RegistrationId = Guid.NewGuid(),
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
        using var written = JsonDocument.Parse(((RedisValue)stringSet.Arguments[1]!).ToString());
        var registration = Assert.Single(written.RootElement.GetProperty("Registrations").EnumerateArray());
        Assert.Equal("corr-a", registration.GetProperty("State").GetProperty("CorrelationId").GetString());
        Assert.NotEqual(Guid.Empty, registration.GetProperty("State").GetProperty("RegistrationId").GetGuid());
        Assert.Equal(_time.Now + TimeSpan.FromMinutes(3), registration.GetProperty("ExpiresAtUtc").GetDateTimeOffset());

        await Assert.ThrowsAsync<ArgumentException>(() => _store.SaveAsync(" ", state, TimeSpan.FromSeconds(1)));
        await Assert.ThrowsAsync<ArgumentNullException>(() => _store.SaveAsync("corr-a", null!, TimeSpan.FromSeconds(1)));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => _store.SaveAsync("corr-a", state, TimeSpan.Zero));
        await Assert.ThrowsAsync<ArgumentException>(() => _store.TryDeleteAsync("corr-a", Guid.Empty));
        await Assert.ThrowsAsync<ArgumentException>(() => _store.SaveAsync(
            "corr-a",
            new RecoveryState
            {
                CorrelationId = "corr-a",
                SchemaVersion = RecoveryStateSchema.Current + 1
            },
            TimeSpan.FromSeconds(1)));

        using var canceled = new CancellationTokenSource();
        await canceled.CancelAsync();
        await Assert.ThrowsAsync<OperationCanceledException>(() =>
            _store.SaveAsync("corr-a", state, TimeSpan.FromSeconds(1), canceled.Token));
    }

    [Fact]
    public async Task GetAllAsync_ReturnsEmptyForMissingOrMalformedState()
    {
        _database
            .Setup(d => d.StringGetAsync((RedisKey)"ar:recovery:missing", It.IsAny<CommandFlags>()))
            .ReturnsAsync(RedisValue.Null);
        _database
            .Setup(d => d.StringGetAsync((RedisKey)"ar:recovery:broken", It.IsAny<CommandFlags>()))
            .ReturnsAsync("{not-json");

        Assert.Empty(await _store.GetAllAsync("missing"));
        Assert.Empty(await _store.GetAllAsync("broken"));
        await Assert.ThrowsAsync<ArgumentException>(() => _store.GetAllAsync(" "));

        using var canceled = new CancellationTokenSource();
        await canceled.CancelAsync();
        await Assert.ThrowsAsync<OperationCanceledException>(() => _store.GetAllAsync("missing", canceled.Token));
    }

    [Fact]
    public async Task GetAllAsync_DeserializesStoredState()
    {
        var state = new RecoveryState
        {
            RegistrationId = Guid.NewGuid(),
            CorrelationId = "corr-a",
            PayloadTypeFullName = typeof(OperationResult).FullName,
            RegisteredAtUtc = DateTime.UtcNow
        };
        _database
            .Setup(d => d.StringGetAsync((RedisKey)"ar:recovery:corr-a", It.IsAny<CommandFlags>()))
            .ReturnsAsync(JsonSerializer.Serialize(new[] { state }));

        var loaded = Assert.Single(await _store.GetAllAsync("corr-a"));

        Assert.Equal("corr-a", loaded.CorrelationId);
        Assert.Equal(typeof(OperationResult).FullName, loaded.PayloadTypeFullName);
    }

    [Fact]
    public async Task GetAllAsync_DeserializesArrayAndRejectsSingleObject()
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
        Assert.Empty(await _store.GetAllAsync("legacy"));
    }

    [Fact]
    public async Task GetAllAsync_FiltersUnreadableSchemaAndRequiresMatchingCorrelationId()
    {
        _database
            .Setup(d => d.StringGetAsync((RedisKey)"ar:recovery:corr-a", It.IsAny<CommandFlags>()))
            .ReturnsAsync(JsonSerializer.Serialize(new[]
            {
                new RecoveryState
                {
                    RegistrationId = Guid.NewGuid(),
                    SchemaVersion = RecoveryStateSchema.Current + 1,
                    CorrelationId = "corr-a"
                },
                new RecoveryState
                {
                    RegistrationId = Guid.NewGuid(),
                    CorrelationId = "corr-a",
                    PayloadTypeFullName = typeof(OperationResult).FullName,
                    RegisteredAtUtc = DateTime.UtcNow
                }
            }));

        var state = Assert.Single(await _store.GetAllAsync("corr-a"));

        Assert.Equal("corr-a", state.CorrelationId);
    }

    [Fact]
    public async Task GetAllAsync_RejectsNullMissingRegistrationAndMismatchedCorrelationEntries()
    {
        var valid = new RecoveryState
        {
            RegistrationId = Guid.NewGuid(),
            CorrelationId = "corr-a",
            PayloadTypeFullName = "valid"
        };
        RecoveryState?[] stored =
        [
            null,
            new RecoveryState { CorrelationId = "corr-a", PayloadTypeFullName = "missing-id" },
            new RecoveryState { RegistrationId = Guid.NewGuid(), CorrelationId = "different", PayloadTypeFullName = "mismatch" },
            valid
        ];
        _database
            .Setup(d => d.StringGetAsync((RedisKey)"ar:recovery:corr-a", It.IsAny<CommandFlags>()))
            .ReturnsAsync(JsonSerializer.Serialize(stored));

        Assert.Equal(valid.PayloadTypeFullName, Assert.Single(await _store.GetAllAsync("corr-a")).PayloadTypeFullName);
    }

    [Fact]
    public async Task SaveAsync_GeneratesRegistrationIdAndRejectsCorrelationMismatch()
    {
        _database
            .Setup(d => d.StringGetAsync((RedisKey)"ar:recovery:generated", It.IsAny<CommandFlags>()))
            .ReturnsAsync(RedisValue.Null);
        SetupTransactions(true);
        var state = new RecoveryState { CorrelationId = "generated" };

        await _store.SaveAsync("generated", state, TimeSpan.FromMinutes(1));

        Assert.NotEqual(Guid.Empty, state.RegistrationId);
        await Assert.ThrowsAsync<ArgumentException>(() =>
            _store.SaveAsync(
                "expected",
                new RecoveryState { CorrelationId = "different" },
                TimeSpan.FromMinutes(1)));
    }

    [Fact]
    public async Task TryDeleteAsync_DeletesSpecificRegistration()
    {
        var registrationId = Guid.NewGuid();
        _database
            .Setup(d => d.StringGetAsync((RedisKey)"ar:recovery:corr-a", It.IsAny<CommandFlags>()))
            .ReturnsAsync(JsonSerializer.Serialize(new[]
            {
                new RecoveryState { RegistrationId = registrationId, CorrelationId = "corr-a" }
            }));
        SetupTransactions(true);

        Assert.True(await _store.TryDeleteAsync("corr-a", registrationId));
        Assert.Single(_transactions[0].Invocations, invocation => invocation.Method.Name == nameof(IDatabase.KeyDeleteAsync));

        await Assert.ThrowsAsync<ArgumentException>(() => _store.TryDeleteAsync(" ", registrationId));

        using var canceled = new CancellationTokenSource();
        await canceled.CancelAsync();
        await Assert.ThrowsAsync<OperationCanceledException>(() => _store.TryDeleteAsync("corr-a", registrationId, canceled.Token));
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
    public async Task TryDeleteAsync_WithRegistrationId_ReturnsFalseForMissingOrUnknownRegistration()
    {
        var existingId = Guid.NewGuid();
        _database
            .SetupSequence(d => d.StringGetAsync((RedisKey)"ar:recovery:missing", It.IsAny<CommandFlags>()))
            .ReturnsAsync(RedisValue.Null);
        _database
            .SetupSequence(d => d.StringGetAsync((RedisKey)"ar:recovery:corr-a", It.IsAny<CommandFlags>()))
            .ReturnsAsync(JsonSerializer.Serialize(new[]
            {
                new RecoveryState { RegistrationId = existingId, CorrelationId = "corr-a" }
            }));

        Assert.False(await _store.TryDeleteAsync("missing", Guid.NewGuid()));
        Assert.False(await _store.TryDeleteAsync("corr-a", Guid.NewGuid()));

        await Assert.ThrowsAsync<ArgumentException>(() => _store.TryDeleteAsync(" ", existingId));
        using var canceled = new CancellationTokenSource();
        await canceled.CancelAsync();
        await Assert.ThrowsAsync<OperationCanceledException>(() =>
            _store.TryDeleteAsync("corr-a", existingId, canceled.Token));
    }

    [Fact]
    public async Task TryDeleteAsync_WithRegistrationId_DeletesKeyWhenLastRegistrationIsRemoved()
    {
        var registrationId = Guid.NewGuid();
        _database
            .Setup(d => d.StringGetAsync((RedisKey)"ar:recovery:corr-a", It.IsAny<CommandFlags>()))
            .ReturnsAsync(JsonSerializer.Serialize(new[]
            {
                new RecoveryState { RegistrationId = registrationId, CorrelationId = "corr-a" }
            }));
        SetupTransactions(true);

        Assert.True(await _store.TryDeleteAsync("corr-a", registrationId));

        var transaction = Assert.Single(_transactions);
        Assert.Single(transaction.Invocations, invocation => invocation.Method.Name == nameof(IDatabase.KeyDeleteAsync));
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
        using var written = JsonDocument.Parse(WrittenValue(_transactions[1]).ToString());
        var writtenIds = written.RootElement.GetProperty("Registrations").EnumerateArray()
            .Select(entry => entry.GetProperty("State").GetProperty("RegistrationId").GetGuid())
            .ToList();
        Assert.Equal(3, writtenIds.Count);
        Assert.Contains(first.RegistrationId, writtenIds);
        Assert.Contains(competing.RegistrationId, writtenIds);
        Assert.Contains(second.RegistrationId, writtenIds);
    }

    [Fact]
    public async Task SaveAsync_ReplacesExistingStateWithSameRegistrationId()
    {
        var registrationId = Guid.NewGuid();
        _database
            .Setup(d => d.StringGetAsync((RedisKey)"ar:recovery:corr-a", It.IsAny<CommandFlags>()))
            .ReturnsAsync(JsonSerializer.Serialize(new[]
            {
                new RecoveryState
                {
                    RegistrationId = registrationId,
                    CorrelationId = "corr-a",
                    PayloadTypeFullName = typeof(SuccessOnlyPayload).FullName
                }
            }));
        SetupTransactions(true);

        await _store.SaveAsync("corr-a", new RecoveryState
        {
            RegistrationId = registrationId,
            CorrelationId = "corr-a",
            PayloadTypeFullName = typeof(OperationResult).FullName
        }, TimeSpan.FromMinutes(3));

        using var written = JsonDocument.Parse(WrittenValue(Assert.Single(_transactions)).ToString());
        var registration = Assert.Single(written.RootElement.GetProperty("Registrations").EnumerateArray());
        Assert.Equal(registrationId, registration.GetProperty("State").GetProperty("RegistrationId").GetGuid());
        Assert.Equal(typeof(OperationResult).FullName, registration.GetProperty("State").GetProperty("PayloadTypeFullName").GetString());
    }

    [Fact]
    public async Task SaveAsync_KeyTtlIsMaxRemainingAcrossEntries_NotAFreshFullTtl()
    {
        // Registrations for one correlation id share one key, so the key TTL must be the longest
        // remaining ENTRY lifetime. A save that stamped its own full TTL onto the key would
        // truncate a longer-lived sibling here — and the pre-envelope behavior re-extended every
        // sibling on each save, keeping dead registrations recoverable indefinitely.
        var longLived = new RecoveryState { RegistrationId = Guid.NewGuid(), CorrelationId = "corr-a" };
        _database
            .Setup(d => d.StringGetAsync((RedisKey)"ar:recovery:corr-a", It.IsAny<CommandFlags>()))
            .ReturnsAsync(EnvelopeBlob((longLived, _time.Now + TimeSpan.FromMinutes(10))));
        SetupTransactions(true);

        var second = new RecoveryState { RegistrationId = Guid.NewGuid(), CorrelationId = "corr-a" };
        await _store.SaveAsync("corr-a", second, TimeSpan.FromMinutes(5));

        var transaction = Assert.Single(_transactions);
        var stringSet = Assert.Single(transaction.Invocations, invocation => invocation.Method.Name == nameof(IDatabase.StringSetAsync));
        Assert.Equal("EX 600", stringSet.Arguments[2]!.ToString());
        using var written = JsonDocument.Parse(((RedisValue)stringSet.Arguments[1]!).ToString());
        Assert.Equal(2, written.RootElement.GetProperty("Registrations").GetArrayLength());
        Assert.Equal(
            _time.Now + TimeSpan.FromMinutes(10),
            WrittenRegistration(written.RootElement, longLived.RegistrationId).GetProperty("ExpiresAtUtc").GetDateTimeOffset());
        Assert.Equal(
            _time.Now + TimeSpan.FromMinutes(5),
            WrittenRegistration(written.RootElement, second.RegistrationId).GetProperty("ExpiresAtUtc").GetDateTimeOffset());
    }

    [Fact]
    public async Task SaveAsync_PrunesEntriesPastTheirExpiry()
    {
        var expired = new RecoveryState { RegistrationId = Guid.NewGuid(), CorrelationId = "corr-a" };
        var live = new RecoveryState { RegistrationId = Guid.NewGuid(), CorrelationId = "corr-a" };
        _database
            .Setup(d => d.StringGetAsync((RedisKey)"ar:recovery:corr-a", It.IsAny<CommandFlags>()))
            .ReturnsAsync(EnvelopeBlob(
                (expired, _time.Now - TimeSpan.FromMinutes(1)),
                (live, _time.Now + TimeSpan.FromMinutes(10))));
        SetupTransactions(true);

        var fresh = new RecoveryState { RegistrationId = Guid.NewGuid(), CorrelationId = "corr-a" };
        await _store.SaveAsync("corr-a", fresh, TimeSpan.FromMinutes(5));

        var transaction = Assert.Single(_transactions);
        var stringSet = Assert.Single(transaction.Invocations, invocation => invocation.Method.Name == nameof(IDatabase.StringSetAsync));
        Assert.Equal("EX 600", stringSet.Arguments[2]!.ToString());
        using var written = JsonDocument.Parse(((RedisValue)stringSet.Arguments[1]!).ToString());
        var writtenIds = written.RootElement.GetProperty("Registrations").EnumerateArray()
            .Select(entry => entry.GetProperty("State").GetProperty("RegistrationId").GetGuid())
            .ToList();
        Assert.Equal(2, writtenIds.Count);
        Assert.Contains(live.RegistrationId, writtenIds);
        Assert.Contains(fresh.RegistrationId, writtenIds);
        Assert.DoesNotContain(expired.RegistrationId, writtenIds);
    }

    [Fact]
    public async Task GetAllAsync_FiltersEntriesPastTheirExpiry()
    {
        // A sibling's later save keeps the KEY alive past this entry's own lifetime; the entry
        // must still read as absent, or a late response fires recovery callbacks for a
        // registration that lapsed long ago.
        var expired = new RecoveryState { RegistrationId = Guid.NewGuid(), CorrelationId = "corr-a" };
        var live = new RecoveryState { RegistrationId = Guid.NewGuid(), CorrelationId = "corr-a" };
        _database
            .Setup(d => d.StringGetAsync((RedisKey)"ar:recovery:corr-a", It.IsAny<CommandFlags>()))
            .ReturnsAsync(EnvelopeBlob(
                (expired, _time.Now - TimeSpan.FromSeconds(1)),
                (live, _time.Now + TimeSpan.FromMinutes(10))));

        var state = Assert.Single(await _store.GetAllAsync("corr-a"));

        Assert.Equal(live.RegistrationId, state.RegistrationId);
    }

    [Fact]
    public async Task SaveAsync_ReStampsLegacyEntriesWithAFullExpiry()
    {
        // Legacy blobs (a bare state list) carry no per-entry expiry. The first post-upgrade save
        // converts them to the enveloped shape, stamping each with the save's full TTL — the same
        // ceiling every legacy save applied to the whole key.
        var legacy = new RecoveryState { RegistrationId = Guid.NewGuid(), CorrelationId = "corr-a" };
        _database
            .Setup(d => d.StringGetAsync((RedisKey)"ar:recovery:corr-a", It.IsAny<CommandFlags>()))
            .ReturnsAsync(JsonSerializer.Serialize(new[] { legacy }));
        SetupTransactions(true);

        var fresh = new RecoveryState { RegistrationId = Guid.NewGuid(), CorrelationId = "corr-a" };
        await _store.SaveAsync("corr-a", fresh, TimeSpan.FromMinutes(3));

        var transaction = Assert.Single(_transactions);
        var stringSet = Assert.Single(transaction.Invocations, invocation => invocation.Method.Name == nameof(IDatabase.StringSetAsync));
        Assert.Equal("EX 180", stringSet.Arguments[2]!.ToString());
        using var written = JsonDocument.Parse(((RedisValue)stringSet.Arguments[1]!).ToString());
        Assert.Equal(2, written.RootElement.GetProperty("Registrations").GetArrayLength());
        Assert.Equal(
            _time.Now + TimeSpan.FromMinutes(3),
            WrittenRegistration(written.RootElement, legacy.RegistrationId).GetProperty("ExpiresAtUtc").GetDateTimeOffset());
    }

    [Fact]
    public async Task TryDeleteAsync_ShrinksKeyTtlToLongestSurvivingEntry()
    {
        var longLived = new RecoveryState { RegistrationId = Guid.NewGuid(), CorrelationId = "corr-a" };
        var shortLived = new RecoveryState { RegistrationId = Guid.NewGuid(), CorrelationId = "corr-a" };
        _database
            .Setup(d => d.StringGetAsync((RedisKey)"ar:recovery:corr-a", It.IsAny<CommandFlags>()))
            .ReturnsAsync(EnvelopeBlob(
                (longLived, _time.Now + TimeSpan.FromMinutes(10)),
                (shortLived, _time.Now + TimeSpan.FromMinutes(4))));
        SetupTransactions(true);

        Assert.True(await _store.TryDeleteAsync("corr-a", longLived.RegistrationId));

        var transaction = Assert.Single(_transactions);
        var stringSet = Assert.Single(transaction.Invocations, invocation => invocation.Method.Name == nameof(IDatabase.StringSetAsync));
        Assert.Equal("EX 240", stringSet.Arguments[2]!.ToString());
        using var written = JsonDocument.Parse(((RedisValue)stringSet.Arguments[1]!).ToString());
        var survivor = Assert.Single(written.RootElement.GetProperty("Registrations").EnumerateArray());
        Assert.Equal(shortLived.RegistrationId, survivor.GetProperty("State").GetProperty("RegistrationId").GetGuid());
    }

    [Fact]
    public async Task TryDeleteAsync_ExpiredRegistration_ReportsNothingDeleted()
    {
        var expired = new RecoveryState { RegistrationId = Guid.NewGuid(), CorrelationId = "corr-a" };
        _database
            .Setup(d => d.StringGetAsync((RedisKey)"ar:recovery:corr-a", It.IsAny<CommandFlags>()))
            .ReturnsAsync(EnvelopeBlob((expired, _time.Now - TimeSpan.FromMinutes(1))));

        Assert.False(await _store.TryDeleteAsync("corr-a", expired.RegistrationId));

        _database.Verify(d => d.CreateTransaction(It.IsAny<object?>()), Times.Never);
    }

    [Fact]
    public async Task ScanAsync_SkipsEntriesPastTheirExpiry_AndStillYieldsLegacyEntries()
    {
        var endpoint = new DnsEndPoint("redis-a", 6379);
        var server = new Mock<IServer>();
        server.SetupGet(s => s.IsConnected).Returns(true);
        RedisKey[] keys = [(RedisKey)"ar:recovery:corr-a", (RedisKey)"ar:recovery:corr-legacy"];
        server
            .Setup(s => s.Keys(
                It.IsAny<int>(),
                It.IsAny<RedisValue>(),
                It.IsAny<int>(),
                It.IsAny<CommandFlags>()))
            .Returns(keys);
        server
            .Setup(s => s.Keys(
                It.IsAny<int>(),
                It.IsAny<RedisValue>(),
                It.IsAny<int>(),
                It.IsAny<long>(),
                It.IsAny<int>(),
                It.IsAny<CommandFlags>()))
            .Returns(keys);
        _multiplexer.Setup(m => m.GetEndPoints(It.IsAny<bool>())).Returns([endpoint]);
        _multiplexer.Setup(m => m.GetServer(It.IsAny<EndPoint>(), It.IsAny<object?>())).Returns(server.Object);

        var expired = new RecoveryState { RegistrationId = Guid.NewGuid(), CorrelationId = "corr-a" };
        var live = new RecoveryState { RegistrationId = Guid.NewGuid(), CorrelationId = "corr-a" };
        var legacy = new RecoveryState { RegistrationId = Guid.NewGuid(), CorrelationId = "corr-legacy" };
        _database
            .Setup(d => d.StringGetAsync((RedisKey)"ar:recovery:corr-a", It.IsAny<CommandFlags>()))
            .ReturnsAsync(EnvelopeBlob(
                (expired, _time.Now - TimeSpan.FromMinutes(1)),
                (live, _time.Now + TimeSpan.FromMinutes(10))));
        _database
            .Setup(d => d.StringGetAsync((RedisKey)"ar:recovery:corr-legacy", It.IsAny<CommandFlags>()))
            .ReturnsAsync(JsonSerializer.Serialize(new[] { legacy }));

        var states = new List<RecoveryState>();
        await foreach (var state in _store.ScanAsync())
            states.Add(state);

        Assert.Equal(2, states.Count);
        Assert.Contains(states, state => state.RegistrationId == live.RegistrationId);
        Assert.Contains(states, state => state.RegistrationId == legacy.RegistrationId);
        Assert.DoesNotContain(states, state => state.RegistrationId == expired.RegistrationId);
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
    public async Task SaveAsync_ExhaustedOptimisticAttempts_ThrowsWithoutOverwriting()
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
        await Assert.ThrowsAsync<InvalidOperationException>(
            () => _store.SaveAsync("corr-a", state, TimeSpan.FromMinutes(3)));

        // No unconditional write may overwrite registrations committed by competing waiters.
        Assert.Equal(4, _transactions.Count);
        Assert.DoesNotContain(_database.Invocations, invocation => invocation.Method.Name == nameof(IDatabase.StringSetAsync));
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
            .ReturnsAsync(JsonSerializer.Serialize(new[] { new RecoveryState
            {
                RegistrationId = Guid.NewGuid(),
                CorrelationId = "corr-a",
                PayloadTypeFullName = typeof(OperationResult).FullName,
                RegisteredAtUtc = DateTime.UtcNow
            } }));
        _database
            .Setup(d => d.StringGetAsync((RedisKey)"ar:recovery:corr-b", It.IsAny<CommandFlags>()))
            .ReturnsAsync(JsonSerializer.Serialize(new[] { new RecoveryState
            {
                RegistrationId = Guid.NewGuid(),
                CorrelationId = "corr-b",
                PayloadTypeFullName = typeof(OperationResult).FullName,
                RegisteredAtUtc = DateTime.UtcNow
            } }));
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
        Assert.Contains(states, state => state.CorrelationId == "corr-a");
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
