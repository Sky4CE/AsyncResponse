using AsyncResponse.Channels.Redis;
using Microsoft.Extensions.Diagnostics.HealthChecks;
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
    public async Task SaveAsync_UnreadableEnvelope_ThrowsInsteadOfOverwriting()
    {
        // Regression (round 31): per-ENTRY unreadability is carried through the rewrite (the fact
        // below), but a blob whose top-level parse fails deserialized to an EMPTY list — and the
        // CAS condition still held (the value had not changed), so SaveAsync committed just the
        // new registration over registrations it could not even enumerate, destroying every armed
        // callback the blob held. The rewrite must refuse exactly as the read path does.
        _database
            .Setup(d => d.StringGetAsync((RedisKey)"ar:recovery:corr-a", It.IsAny<CommandFlags>()))
            .ReturnsAsync("{not-json");
        SetupTransactions(true);

        var state = new RecoveryState
        {
            RegistrationId = Guid.NewGuid(),
            CorrelationId = "corr-a",
            RegisteredAtUtc = DateTime.UtcNow
        };

        var unreadable = await Assert.ThrowsAsync<RecoveryStateUnreadableException>(
            () => _store.SaveAsync("corr-a", state, TimeSpan.FromMinutes(3)));
        Assert.Equal("corr-a", unreadable.CorrelationId);
        Assert.Empty(_transactions);
    }

    [Fact]
    public async Task SaveAsync_CarriesAnUnreadableSiblingThroughTheRewrite()
    {
        // Regression (round 29): the read path filters out an entry this build cannot INTERPRET
        // (a newer schema version, a null state, a blank registration id) — correct for a read. But
        // save and delete are read-MODIFY-write on a SHARED blob, and rewriting it from the
        // readable subset silently deleted a sibling registration written by a newer host
        // mid-rolling-upgrade: the write path treating "unreadable" as "missing", which is exactly
        // what the read path was hardened to refuse.
        var newerHostsRegistration = Guid.NewGuid();
        _database
            .Setup(d => d.StringGetAsync((RedisKey)"ar:recovery:corr-a", It.IsAny<CommandFlags>()))
            .ReturnsAsync(EnvelopeBlob(
                (new RecoveryState
                {
                    RegistrationId = newerHostsRegistration,
                    CorrelationId = "corr-a",
                    SchemaVersion = RecoveryStateSchema.Current + 1
                },
                _time.Now + TimeSpan.FromMinutes(10))));
        SetupTransactions(true);

        var mine = new RecoveryState { RegistrationId = Guid.NewGuid(), CorrelationId = "corr-a" };
        await _store.SaveAsync("corr-a", mine, TimeSpan.FromMinutes(3));

        var stringSet = Assert.Single(
            Assert.Single(_transactions).Invocations,
            invocation => invocation.Method.Name == nameof(IDatabase.StringSetAsync));
        using var written = JsonDocument.Parse(((RedisValue)stringSet.Arguments[1]!).ToString());

        // Both survive: mine, and the one only a newer build can interpret.
        Assert.Equal(2, written.RootElement.GetProperty("Registrations").GetArrayLength());
        Assert.NotEqual(default, WrittenRegistration(written.RootElement, newerHostsRegistration));
        Assert.NotEqual(default, WrittenRegistration(written.RootElement, mine.RegistrationId));
    }

    [Fact]
    public async Task TryDeleteAsync_CarriesAnUnreadableSiblingThroughTheRewrite()
    {
        // The same rule on the delete path: removing MY registration must not take a sibling this
        // build cannot interpret with it.
        var newerHostsRegistration = Guid.NewGuid();
        var mine = new RecoveryState { RegistrationId = Guid.NewGuid(), CorrelationId = "corr-a" };
        _database
            .Setup(d => d.StringGetAsync((RedisKey)"ar:recovery:corr-a", It.IsAny<CommandFlags>()))
            .ReturnsAsync(EnvelopeBlob(
                (new RecoveryState
                {
                    RegistrationId = newerHostsRegistration,
                    CorrelationId = "corr-a",
                    SchemaVersion = RecoveryStateSchema.Current + 1
                },
                _time.Now + TimeSpan.FromMinutes(10)),
                (mine, _time.Now + TimeSpan.FromMinutes(10))));
        SetupTransactions(true);

        Assert.True(await _store.TryDeleteAsync("corr-a", mine.RegistrationId));

        var stringSet = Assert.Single(
            Assert.Single(_transactions).Invocations,
            invocation => invocation.Method.Name == nameof(IDatabase.StringSetAsync));
        using var written = JsonDocument.Parse(((RedisValue)stringSet.Arguments[1]!).ToString());

        var remaining = Assert.Single(written.RootElement.GetProperty("Registrations").EnumerateArray());
        Assert.Equal(newerHostsRegistration, remaining.GetProperty("State").GetProperty("RegistrationId").GetGuid());
    }

    [Theory]
    [InlineData(true)]
    [InlineData(7L)]
    [InlineData(OperationStatus.Completed)]
    public async Task SaveAsync_AcceptsACallbackArgumentTheChannelsOwnJsonContextNeverSaw(object literal)
    {
        // Regression (round 29): this store serialized through the package-local source-generated
        // context ALONE. A callback argument is CallbackParam.Value, typed object, so it serializes
        // by RUNTIME type — and the generator only emitted what the envelope references
        // transitively (string, int, Guid, DateTime). A perfectly ordinary literal therefore threw
        // NotSupportedException at waiter registration, on this channel only, and the documented
        // trim/AOT seam (AsyncResponseJsonSerialization.RegisterResolver) was bypassed entirely.
        // The context is now CHAINED in front of the library resolver, so the wire format is
        // unchanged and every other type still resolves.
        _database
            .Setup(d => d.StringGetAsync((RedisKey)"ar:recovery:corr-a", It.IsAny<CommandFlags>()))
            .ReturnsAsync(RedisValue.Null);
        SetupTransactions(true);

        var state = new RecoveryState
        {
            RegistrationId = Guid.NewGuid(),
            CorrelationId = "corr-a",
            ResumeCallback = new ReflectionCallDto
            {
                ServiceInterfaceFullName = "Acme.IOrders",
                MethodName = "ResumeAsync",
                Params = [CallbackParam.ForValue(literal)]
            }
        };

        await _store.SaveAsync("corr-a", state, TimeSpan.FromMinutes(3));

        var stringSet = Assert.Single(
            Assert.Single(_transactions).Invocations,
            invocation => invocation.Method.Name == nameof(IDatabase.StringSetAsync));
        Assert.Contains("ResumeAsync", ((RedisValue)stringSet.Arguments[1]!).ToString(), StringComparison.Ordinal);
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

        // No key at all: nobody ever armed a callback here, so an empty list is the truth and the
        // dispatcher may ack.
        Assert.Empty(await _store.GetAllAsync("missing"));

        // A key holding an unparseable blob is the opposite: registrations exist and this build
        // cannot read them. Reporting that as "none" acked a terminal response whose callback never
        // ran, so it fails the delivery instead.
        var unreadable = await Assert.ThrowsAsync<RecoveryStateUnreadableException>(() => _store.GetAllAsync("broken"));
        Assert.Equal("broken", unreadable.CorrelationId);
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
        SetupKeys(server, keys);
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
        SetupKeys(connected, [
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
        SetupKeys(server, [(RedisKey)"ar:recovery:corr-a"]);

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

    // ---------------------------------------------------------------------------------------
    // Round 40: scan completeness and scan cost. Each test arranges BOTH key enumerations (the
    // synchronous one the old scanner used and KeysAsync), so the behavioural ones are red on
    // ca58de0 for what they assert rather than for a missing mock.
    // ---------------------------------------------------------------------------------------

    /// <summary>
    /// With Redis down the old scanner filtered the disconnected server out, enumerated nothing
    /// and completed: the watchdog published "0 registrations, no error" and the recovery health
    /// check flipped from Degraded to Healthy because of the outage.
    /// </summary>
    [Fact]
    public async Task ScanAsync_NoConnectedPrimary_FailsInsteadOfReportingAnEmptyKeyspace()
    {
        var server = new Mock<IServer>();
        server.SetupGet(s => s.IsConnected).Returns(false);
        SetupKeys(server, [(RedisKey)"ar:recovery:corr-a"]);
        _multiplexer.Setup(m => m.GetEndPoints(It.IsAny<bool>())).Returns([new DnsEndPoint("redis-a", 6379)]);
        _multiplexer.Setup(m => m.GetServer(It.IsAny<EndPoint>(), It.IsAny<object?>())).Returns(server.Object);

        var failure = await Assert.ThrowsAsync<RedisConnectionException>(() => DrainScanAsync());

        Assert.Contains("no Redis primary is connected", failure.Message, StringComparison.Ordinal);
        Assert.DoesNotContain(_database.Invocations, invocation => invocation.Method.Name == nameof(IDatabase.StringGetAsync));
    }

    [Fact]
    public async Task ScanAsync_OnlyAReplicaConnected_FailsBecauseThePrimaryIsTheScanTarget()
    {
        var primary = new Mock<IServer>();
        primary.SetupGet(s => s.IsConnected).Returns(false);
        var replica = new Mock<IServer>();
        replica.SetupGet(s => s.IsConnected).Returns(true);
        replica.SetupGet(s => s.IsReplica).Returns(true);
        SetupKeys(replica, [(RedisKey)"ar:recovery:corr-a"]);
        SetupServers(primary, replica);

        await Assert.ThrowsAsync<RedisConnectionException>(() => DrainScanAsync());
    }

    /// <summary>
    /// A cluster shards the keyspace, so the reachable primaries are not the whole answer: the
    /// old scanner returned shard A's registrations as if they were every registration.
    /// </summary>
    [Fact]
    public async Task ScanAsync_ClusterWithAnUnreachablePrimary_FailsInsteadOfReportingThePartialKeyspace()
    {
        var shardA = new Mock<IServer>();
        shardA.SetupGet(s => s.IsConnected).Returns(true);
        shardA.SetupGet(s => s.ServerType).Returns(ServerType.Cluster);
        SetupKeys(shardA, [(RedisKey)"ar:recovery:corr-a"]);
        var shardB = new Mock<IServer>();
        shardB.SetupGet(s => s.IsConnected).Returns(false);
        shardB.SetupGet(s => s.ServerType).Returns(ServerType.Cluster);
        shardB.SetupGet(s => s.EndPoint).Returns(new DnsEndPoint("redis-shard-b", 6379));
        SetupServers(shardA, shardB);
        _database
            .Setup(d => d.StringGetAsync((RedisKey)"ar:recovery:corr-a", It.IsAny<CommandFlags>()))
            .ReturnsAsync(EnvelopeBlob((new RecoveryState { RegistrationId = Guid.NewGuid(), CorrelationId = "corr-a" }, _time.Now + TimeSpan.FromMinutes(10))));

        var failure = await Assert.ThrowsAsync<RedisConnectionException>(() => DrainScanAsync());

        Assert.Contains("redis-shard-b", failure.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// The same topology once the shard is back: the scan is complete again and covers both
    /// shards. A disconnected REPLICA never made it incomplete in the first place.
    /// </summary>
    [Fact]
    public async Task ScanAsync_ClusterAfterReconnection_ScansEveryPrimary_AndIgnoresADisconnectedReplica()
    {
        var shardA = new Mock<IServer>();
        shardA.SetupGet(s => s.IsConnected).Returns(true);
        shardA.SetupGet(s => s.ServerType).Returns(ServerType.Cluster);
        SetupKeys(shardA, [(RedisKey)"ar:recovery:corr-a"]);
        var shardB = new Mock<IServer>();
        var shardBConnected = false;
        shardB.SetupGet(s => s.IsConnected).Returns(() => shardBConnected);
        shardB.SetupGet(s => s.ServerType).Returns(ServerType.Cluster);
        SetupKeys(shardB, [(RedisKey)"ar:recovery:corr-b"]);
        var downReplica = new Mock<IServer>();
        downReplica.SetupGet(s => s.IsConnected).Returns(false);
        downReplica.SetupGet(s => s.IsReplica).Returns(true);
        SetupServers(shardA, shardB, downReplica);
        foreach (var correlationId in new[] { "corr-a", "corr-b" })
        {
            _database
                .Setup(d => d.StringGetAsync((RedisKey)$"ar:recovery:{correlationId}", It.IsAny<CommandFlags>()))
                .ReturnsAsync(EnvelopeBlob((new RecoveryState { RegistrationId = Guid.NewGuid(), CorrelationId = correlationId }, _time.Now + TimeSpan.FromMinutes(10))));
        }

        await Assert.ThrowsAsync<RedisConnectionException>(() => DrainScanAsync());

        shardBConnected = true;
        var states = await DrainScanAsync();

        Assert.Equal(["corr-a", "corr-b"], states.Select(state => state.CorrelationId).OrderBy(id => id, StringComparer.Ordinal));
    }

    /// <summary>
    /// Outside a cluster every primary serves the same dataset: after a failover the multiplexer
    /// still lists the old primary (disconnected, last known as a primary) next to the promoted
    /// one, and that must not read as an incomplete scan.
    /// </summary>
    [Fact]
    public async Task ScanAsync_StandaloneFailover_AConnectedPrimaryIsTheWholeKeyspace()
    {
        var oldPrimary = new Mock<IServer>();
        oldPrimary.SetupGet(s => s.IsConnected).Returns(false);
        var promoted = new Mock<IServer>();
        promoted.SetupGet(s => s.IsConnected).Returns(true);
        SetupKeys(promoted, [(RedisKey)"ar:recovery:corr-a"]);
        SetupServers(oldPrimary, promoted);
        _database
            .Setup(d => d.StringGetAsync((RedisKey)"ar:recovery:corr-a", It.IsAny<CommandFlags>()))
            .ReturnsAsync(EnvelopeBlob((new RecoveryState { RegistrationId = Guid.NewGuid(), CorrelationId = "corr-a" }, _time.Now + TimeSpan.FromMinutes(10))));

        Assert.Single(await DrainScanAsync());
    }

    /// <summary>A value read that fails mid-scan fails the scan: a skipped key would read as "no registration".</summary>
    [Fact]
    public async Task ScanAsync_AFailedValueRead_FailsTheScan()
    {
        var server = new Mock<IServer>();
        server.SetupGet(s => s.IsConnected).Returns(true);
        SetupKeys(server, [(RedisKey)"ar:recovery:corr-a", (RedisKey)"ar:recovery:corr-b"]);
        SetupServers(server);
        _database
            .Setup(d => d.StringGetAsync((RedisKey)"ar:recovery:corr-a", It.IsAny<CommandFlags>()))
            .ReturnsAsync(EnvelopeBlob((new RecoveryState { RegistrationId = Guid.NewGuid(), CorrelationId = "corr-a" }, _time.Now + TimeSpan.FromMinutes(10))));
        _database
            .Setup(d => d.StringGetAsync((RedisKey)"ar:recovery:corr-b", It.IsAny<CommandFlags>()))
            .ThrowsAsync(new RedisTimeoutException(CommandFlags.None, "GET timed out", CommandStatus.Sent));

        await Assert.ThrowsAsync<RedisTimeoutException>(() => DrainScanAsync());
    }

    /// <summary>
    /// The registration reads of one batch are issued back to back and awaited together. The old
    /// scanner awaited each GET before sending the next — one network round trip per key — so
    /// here it would wait forever on the first read: none completes until all five are in flight.
    /// </summary>
    [Fact]
    public async Task ScanAsync_PipelinesTheValueReadsOfABatch_InsteadOfOneRoundTripPerKey()
    {
        var keys = Enumerable.Range(0, 5).Select(index => (RedisKey)$"ar:recovery:corr-{index}").ToArray();
        var server = new Mock<IServer>();
        server.SetupGet(s => s.IsConnected).Returns(true);
        SetupKeys(server, keys);
        SetupServers(server);

        var pending = new List<(TaskCompletionSource<RedisValue> Completion, string CorrelationId)>();
        _database
            .Setup(d => d.StringGetAsync(It.IsAny<RedisKey>(), It.IsAny<CommandFlags>()))
            .Returns<RedisKey, CommandFlags>((key, _) =>
            {
                var completion = new TaskCompletionSource<RedisValue>(TaskCreationOptions.RunContinuationsAsynchronously);
                lock (pending)
                {
                    pending.Add((completion, key.ToString()["ar:recovery:".Length..]));
                    if (pending.Count == keys.Length)
                    {
                        foreach (var (read, correlationId) in pending)
                        {
                            read.SetResult(EnvelopeBlob((
                                new RecoveryState { RegistrationId = Guid.NewGuid(), CorrelationId = correlationId },
                                _time.Now + TimeSpan.FromMinutes(10))));
                        }
                    }
                }

                return completion.Task;
            });

        var states = await DrainScanAsync().WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Equal(keys.Length, states.Count);
    }

    /// <summary>The pipeline is bounded: a keyspace of any size is read a batch at a time.</summary>
    [Fact]
    public async Task ScanAsync_BoundsTheReadsInFlight_AndStillYieldsEveryRegistration()
    {
        var keys = Enumerable.Range(0, 300).Select(index => (RedisKey)$"ar:recovery:corr-{index}").ToArray();
        var server = new Mock<IServer>();
        server.SetupGet(s => s.IsConnected).Returns(true);
        SetupKeys(server, keys);
        SetupServers(server);

        var inFlight = 0;
        var maxInFlight = 0;
        _database
            .Setup(d => d.StringGetAsync(It.IsAny<RedisKey>(), It.IsAny<CommandFlags>()))
            .Returns<RedisKey, CommandFlags>(async (key, _) =>
            {
                var now = Interlocked.Increment(ref inFlight);
                int seen;
                while (now > (seen = Volatile.Read(ref maxInFlight)))
                    Interlocked.CompareExchange(ref maxInFlight, now, seen);

                await Task.Delay(20);
                Interlocked.Decrement(ref inFlight);
                return EnvelopeBlob((
                    new RecoveryState { RegistrationId = Guid.NewGuid(), CorrelationId = key.ToString()["ar:recovery:".Length..] },
                    _time.Now + TimeSpan.FromMinutes(10)));
            });

        var states = await DrainScanAsync().WaitAsync(TimeSpan.FromSeconds(30));

        Assert.Equal(300, states.Select(state => state.CorrelationId).Distinct(StringComparer.Ordinal).Count());
        Assert.InRange(maxInFlight, 2, 128);
    }

    /// <summary>
    /// The reviewer's reproduction end to end — real store, real watchdog, real health check:
    /// one stale registration reads Degraded, and losing Redis must not turn that into Healthy.
    /// </summary>
    [Fact]
    public async Task RedisOutage_KeepsRecoveryHealthDegraded_InsteadOfClearingTheStuckFlowAlarm()
    {
        var connected = true;
        var server = new Mock<IServer>();
        server.SetupGet(s => s.IsConnected).Returns(() => connected);
        SetupKeys(server, [(RedisKey)"ar:recovery:corr-stuck"]);
        SetupServers(server);
        _database
            .Setup(d => d.StringGetAsync((RedisKey)"ar:recovery:corr-stuck", It.IsAny<CommandFlags>()))
            .ReturnsAsync(EnvelopeBlob((
                new RecoveryState
                {
                    RegistrationId = Guid.NewGuid(),
                    CorrelationId = "corr-stuck",
                    PayloadTypeFullName = typeof(OperationResult).FullName,
                    RegisteredAtUtc = DateTime.UtcNow.AddHours(-1)
                },
                _time.Now + TimeSpan.FromMinutes(10))));

        var whileConnected = await ScanHealthAsync();
        Assert.Equal(HealthStatus.Degraded, whileConnected.Status);
        Assert.Contains("look stuck", whileConnected.Description, StringComparison.Ordinal);

        connected = false;
        var duringOutage = await ScanHealthAsync();
        Assert.Equal(HealthStatus.Degraded, duringOutage.Status);
        Assert.Contains("scan failed", duringOutage.Description, StringComparison.Ordinal);

        connected = true;
        Assert.Contains("look stuck", (await ScanHealthAsync()).Description, StringComparison.Ordinal);
    }

    /// <summary>One watchdog scan over <see cref="_store"/>, judged by the recovery health check.</summary>
    private async Task<HealthCheckResult> ScanHealthAsync()
    {
        var state = new AsyncResponseWatchdogState();
        var noLiveWaiter = new Mock<IActiveSubscriberProbe>();
        noLiveWaiter
            .Setup(probe => probe.CountActiveSubscribersAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Returns(ValueTask.FromResult(0L));
        using var watchdog = new AsyncResponseWatchdog(
            [_store],
            [noLiveWaiter.Object],
            state,
            Options.Create(new AsyncResponseOptions
            {
                Watchdog = new AsyncResponseWatchdogOptions
                {
                    Enabled = true,
                    StartupDelay = TimeSpan.Zero,
                    Interval = TimeSpan.FromMinutes(1),
                    StaleAfter = TimeSpan.FromMinutes(1)
                }
            }),
            NullLogger<AsyncResponseWatchdog>.Instance);

        await watchdog.StartAsync(CancellationToken.None);
        try
        {
            var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
            while (state.Latest is null)
            {
                Assert.True(DateTime.UtcNow < deadline, "The watchdog did not publish a snapshot in time.");
                await Task.Delay(20);
            }
        }
        finally
        {
            await watchdog.StopAsync(CancellationToken.None);
        }

        return await new AsyncResponseRecoveryHealthCheck(state).CheckHealthAsync(new HealthCheckContext());
    }

    private async Task<List<RecoveryState>> DrainScanAsync()
    {
        var states = new List<RecoveryState>();
        await foreach (var state in _store.ScanAsync())
            states.Add(state);
        return states;
    }

    private void SetupServers(params Mock<IServer>[] servers)
    {
        var endPoints = servers.Select((_, index) => (EndPoint)new DnsEndPoint($"redis-{index}", 6379)).ToArray();
        _multiplexer.Setup(m => m.GetEndPoints(It.IsAny<bool>())).Returns(endPoints);
        _multiplexer
            .Setup(m => m.GetServer(It.IsAny<EndPoint>(), It.IsAny<object?>()))
            .Returns<EndPoint, object?>((endPoint, _) => servers[Array.IndexOf(endPoints, endPoint)].Object);
    }

    /// <summary>Arranges one keyspace for the asynchronous enumeration and both synchronous overloads.</summary>
    private static void SetupKeys(Mock<IServer> server, RedisKey[] keys)
    {
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
        server
            .Setup(s => s.KeysAsync(
                It.IsAny<int>(),
                It.IsAny<RedisValue>(),
                It.IsAny<int>(),
                It.IsAny<long>(),
                It.IsAny<int>(),
                It.IsAny<CommandFlags>()))
            .Returns(() => ToAsyncEnumerable(keys));
    }

    private static async IAsyncEnumerable<RedisKey> ToAsyncEnumerable(RedisKey[] keys)
    {
        foreach (var key in keys)
        {
            await Task.Yield();
            yield return key;
        }
    }
}
