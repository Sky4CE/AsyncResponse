using AsyncResponse.Channels.NATS;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using System.Text.Json;
using Xunit;

namespace AsyncResponse.Tests;

public class NatsRecoveryStateStoreTests
{
    private readonly FakeNatsKvStore _kv = new();
    private readonly TestTimeProvider _time = new();
    private readonly NatsRecoveryStateStore _store;

    public NatsRecoveryStateStoreTests()
    {
        _store = new NatsRecoveryStateStore(
            _kv,
            Options.Create(new NatsAsyncResponseChannelOptions()),
            NullLogger<NatsRecoveryStateStore>.Instance,
            _time);
    }

    [Fact]
    public async Task SaveAsync_PersistsEnvelopeUnderEncodedKey_AndValidatesArguments()
    {
        var state = new RecoveryState
        {
            CorrelationId = "corr-a",
            PayloadTypeFullName = typeof(OperationResult).FullName,
            RegisteredAtUtc = DateTime.UtcNow
        };

        await _store.SaveAsync("corr-a", state, TimeSpan.FromMinutes(3));

        var key = NatsSubjectSchema.RecoveryKey("corr-a");
        Assert.True(_kv.Entries.ContainsKey(key));
        using var doc = JsonDocument.Parse(_kv.Entries[key]);
        Assert.Equal("corr-a", doc.RootElement.GetProperty("States")[0].GetProperty("CorrelationId").GetString());
        Assert.NotEqual(Guid.Empty, doc.RootElement.GetProperty("States")[0].GetProperty("RegistrationId").GetGuid());
        Assert.Equal(
            (_time.Now + TimeSpan.FromMinutes(3)).UtcDateTime,
            doc.RootElement.GetProperty("ExpiresAtUtc").GetDateTimeOffset().UtcDateTime);

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
        await Assert.ThrowsAsync<OperationCanceledException>(() => _store.SaveAsync("corr-a", state, TimeSpan.FromSeconds(1), canceled.Token));
    }

    [Fact]
    public async Task SaveAsync_CompetingWriteBetweenReadAndWrite_RetriesAndKeepsAllRegistrations()
    {
        var first = new RecoveryState
        {
            CorrelationId = "corr-cas",
            RegistrationId = Guid.NewGuid(),
            PayloadTypeFullName = typeof(OperationResult).FullName
        };
        await _store.SaveAsync("corr-cas", first, TimeSpan.FromMinutes(5));

        // A competing waiter registers between our read and our conditional write. The old
        // read-modify-write overwrote its registration; the CAS loop must retry and keep it.
        var competing = new RecoveryState
        {
            CorrelationId = "corr-cas",
            RegistrationId = Guid.NewGuid(),
            PayloadTypeFullName = typeof(OperationResult).FullName
        };
        _kv.AfterGet = _ => _store.SaveAsync("corr-cas", competing, TimeSpan.FromMinutes(5));

        var second = new RecoveryState
        {
            CorrelationId = "corr-cas",
            RegistrationId = Guid.NewGuid(),
            PayloadTypeFullName = typeof(OperationResult).FullName
        };
        await _store.SaveAsync("corr-cas", second, TimeSpan.FromMinutes(5));

        var states = await _store.GetAllAsync("corr-cas");
        Assert.Equal(3, states.Count);
        Assert.Contains(states, s => s.RegistrationId == first.RegistrationId);
        Assert.Contains(states, s => s.RegistrationId == competing.RegistrationId);
        Assert.Contains(states, s => s.RegistrationId == second.RegistrationId);
    }

    [Fact]
    public async Task SaveAsync_WhenCasKeepsFailing_ThrowsWithoutOverwriting()
    {
        _kv.ForcedCreateConflicts = 4;
        var state = new RecoveryState { CorrelationId = "corr-fallback", RegistrationId = Guid.NewGuid() };

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => _store.SaveAsync("corr-fallback", state, TimeSpan.FromMinutes(5)));

        Assert.Equal(0, _kv.PutCount);
        Assert.Empty(await _store.GetAllAsync("corr-fallback"));
    }

    [Fact]
    public async Task TryDeleteAsync_CompetingSaveBetweenReadAndDelete_RetriesAndKeepsCompetingRegistration()
    {
        var first = new RecoveryState
        {
            CorrelationId = "corr-cas-del",
            RegistrationId = Guid.NewGuid(),
            PayloadTypeFullName = typeof(OperationResult).FullName
        };
        await _store.SaveAsync("corr-cas-del", first, TimeSpan.FromMinutes(5));

        // The delete reads a single-registration list and would delete the whole key; a competing
        // registration lands before its conditional delete, which must fail and retry into a
        // conditional update that removes only the targeted registration.
        var competing = new RecoveryState
        {
            CorrelationId = "corr-cas-del",
            RegistrationId = Guid.NewGuid(),
            PayloadTypeFullName = typeof(OperationResult).FullName
        };
        _kv.AfterGet = _ => _store.SaveAsync("corr-cas-del", competing, TimeSpan.FromMinutes(5));

        Assert.True(await _store.TryDeleteAsync("corr-cas-del", first.RegistrationId));

        var states = await _store.GetAllAsync("corr-cas-del");
        var survivor = Assert.Single(states);
        Assert.Equal(competing.RegistrationId, survivor.RegistrationId);
    }

    [Fact]
    public async Task SaveAsync_RejectsStateWhoseCorrelationIdDoesNotMatchKey()
    {
        await Assert.ThrowsAsync<ArgumentException>(() => _store.SaveAsync(
            "corr-a",
            new RecoveryState { PayloadTypeFullName = typeof(OperationResult).FullName },
            TimeSpan.FromMinutes(5)));
    }

    [Fact]
    public async Task ExpiredCleanup_IsRevisionConditioned_AndSparesAConcurrentFreshRegistration()
    {
        // Regression (r24): the expired-entry cleanup called the UNCONDITIONAL DeleteAsync, so a
        // reader that had just seen the entry expired could wipe a FRESH registration a
        // concurrent SaveAsync committed between the read and the delete — stranding that waiter
        // with a live subscription and no recovery arm. The cleanup now uses the
        // revision-conditioned delete and simply loses on conflict: the new writer owns the key.
        var key = NatsSubjectSchema.RecoveryKey("corr-expired-race");
        await _store.SaveAsync(
            "corr-expired-race",
            new RecoveryState { CorrelationId = "corr-expired-race", RegisteredAtUtc = DateTime.UtcNow },
            TimeSpan.FromMinutes(1));
        _time.Advance(TimeSpan.FromMinutes(2)); // logically expired, physically still present

        // Between GetAllAsync's read (which sees the EXPIRED envelope at revision N) and its
        // best-effort cleanup, a waiter re-registers and CAS-commits a fresh envelope (N+1).
        _kv.AfterGet = async _ => await _store.SaveAsync(
            "corr-expired-race",
            new RecoveryState { CorrelationId = "corr-expired-race", RegisteredAtUtc = DateTime.UtcNow },
            TimeSpan.FromMinutes(30));

        // The reader saw the expired envelope, so it reports nothing — but its cleanup must lose.
        Assert.Empty(await _store.GetAllAsync("corr-expired-race"));

        // The fresh registration survived the racing cleanup and is fully readable.
        Assert.True(_kv.Entries.ContainsKey(key));
        Assert.Single(await _store.GetAllAsync("corr-expired-race"));
    }

    [Fact]
    public async Task GetAllAsync_ReturnsAllRegistrations()
    {
        var first = new RecoveryState { RegistrationId = Guid.NewGuid(), CorrelationId = "corr-a" };
        var second = new RecoveryState { RegistrationId = Guid.NewGuid(), CorrelationId = "corr-a" };
        await _store.SaveAsync("corr-a", first, TimeSpan.FromMinutes(5));
        await _store.SaveAsync("corr-a", second, TimeSpan.FromMinutes(5));

        Assert.Equal(2, (await _store.GetAllAsync("corr-a")).Count);
    }

    [Fact]
    public async Task GetAllAsync_ReturnsEmpty_ForMissingMalformedAndExpired()
    {
        Assert.Empty(await _store.GetAllAsync("missing"));

        _kv.Entries[NatsSubjectSchema.RecoveryKey("broken")] = "{not-json";
        Assert.Empty(await _store.GetAllAsync("broken"));

        await _store.SaveAsync("expired", new RecoveryState { CorrelationId = "expired" }, TimeSpan.FromMinutes(1));
        _time.Advance(TimeSpan.FromMinutes(2));
        Assert.Empty(await _store.GetAllAsync("expired"));
        // Expired entries are deleted on read so they never resurface.
        Assert.False(_kv.Entries.ContainsKey(NatsSubjectSchema.RecoveryKey("expired")));

        await Assert.ThrowsAsync<ArgumentException>(() => _store.GetAllAsync(" "));
    }

    [Fact]
    public async Task TryDeleteAsync_ReportsWhetherRegistrationExisted()
    {
        var registrationId = Guid.NewGuid();
        await _store.SaveAsync(
            "corr-a",
            new RecoveryState { RegistrationId = registrationId, CorrelationId = "corr-a" },
            TimeSpan.FromMinutes(5));

        Assert.True(await _store.TryDeleteAsync("corr-a", registrationId));
        Assert.False(await _store.TryDeleteAsync("corr-a", registrationId));
        await Assert.ThrowsAsync<ArgumentException>(() => _store.TryDeleteAsync(" ", registrationId));
    }

    [Fact]
    public async Task TryDeleteAsync_WithRegistrationId_RemovesOnlyThatRegistration()
    {
        var firstId = Guid.NewGuid();
        var secondId = Guid.NewGuid();
        await _store.SaveAsync("corr-a", new RecoveryState { RegistrationId = firstId, CorrelationId = "corr-a" }, TimeSpan.FromMinutes(5));
        await _store.SaveAsync("corr-a", new RecoveryState { RegistrationId = secondId, CorrelationId = "corr-a" }, TimeSpan.FromMinutes(5));

        Assert.True(await _store.TryDeleteAsync("corr-a", firstId));

        var remaining = Assert.Single(await _store.GetAllAsync("corr-a"));
        Assert.Equal(secondId, remaining.RegistrationId);
    }

    [Fact]
    public async Task TryDeleteAsync_WithRegistrationId_ReturnsFalseForMissingMalformedExpiredAndUnknown()
    {
        Assert.False(await _store.TryDeleteAsync("missing", Guid.NewGuid()));

        _kv.Entries[NatsSubjectSchema.RecoveryKey("broken")] = "{not-json";
        Assert.False(await _store.TryDeleteAsync("broken", Guid.NewGuid()));

        var id = Guid.NewGuid();
        await _store.SaveAsync("expired", new RecoveryState { RegistrationId = id, CorrelationId = "expired" }, TimeSpan.FromMinutes(1));
        _time.Advance(TimeSpan.FromMinutes(2));
        Assert.False(await _store.TryDeleteAsync("expired", id));
        Assert.False(_kv.Entries.ContainsKey(NatsSubjectSchema.RecoveryKey("expired")));

        await _store.SaveAsync("corr-a", new RecoveryState { RegistrationId = id, CorrelationId = "corr-a" }, TimeSpan.FromMinutes(5));
        Assert.False(await _store.TryDeleteAsync("corr-a", Guid.NewGuid()));
    }

    [Fact]
    public async Task TryDeleteAsync_WhenCasKeepsFailing_LeavesRegistrationForExpiry()
    {
        var firstId = Guid.NewGuid();
        var secondId = Guid.NewGuid();
        await _store.SaveAsync("corr-a", new RecoveryState { RegistrationId = firstId, CorrelationId = "corr-a" }, TimeSpan.FromMinutes(5));
        await _store.SaveAsync("corr-a", new RecoveryState { RegistrationId = secondId, CorrelationId = "corr-a" }, TimeSpan.FromMinutes(5));
        _kv.ForcedUpdateConflicts = 4;

        Assert.False(await _store.TryDeleteAsync("corr-a", firstId));

        Assert.Equal(2, (await _store.GetAllAsync("corr-a")).Count);
    }

    [Fact]
    public async Task GetAllAsync_RejectsUnreadableSchemaAndMissingCorrelationId()
    {
        _kv.Entries[NatsSubjectSchema.RecoveryKey("mixed")] = JsonSerializer.Serialize(new NatsRecoveryStateStore.StoredRecoveryState
        {
            States =
            [
                new RecoveryState { PayloadTypeFullName = "old" },
                new RecoveryState { CorrelationId = "mixed", PayloadTypeFullName = "future", SchemaVersion = RecoveryStateSchema.Current + 1 }
            ],
            ExpiresAtUtc = _time.Now + TimeSpan.FromMinutes(5)
        });

        // BOTH stored registrations are unreadable — one has no correlation id, the other a future
        // schema version. Filtering them to an empty list made that indistinguishable from "no
        // registration was ever armed", which the dispatcher answers by acknowledging the response.
        var unreadable = await Assert.ThrowsAsync<RecoveryStateUnreadableException>(() => _store.GetAllAsync("mixed"));
        Assert.Equal("mixed", unreadable.CorrelationId);
        Assert.Equal(2, unreadable.UnreadableCount);
    }

    [Fact]
    public async Task GetAllAndScan_FilterSchemaMismatchCorrelationMismatchAndEmptyStateLists()
    {
        var key = NatsSubjectSchema.RecoveryKey("mixed-complete");
        _kv.Entries[key] = JsonSerializer.Serialize(new NatsRecoveryStateStore.StoredRecoveryState
        {
            States =
            [
                new RecoveryState
                {
                    RegistrationId = Guid.NewGuid(),
                    CorrelationId = "mixed-complete",
                    SchemaVersion = RecoveryStateSchema.Current + 1
                },
                new RecoveryState
                {
                    RegistrationId = Guid.NewGuid(),
                    CorrelationId = "different"
                },
                new RecoveryState
                {
                    RegistrationId = Guid.NewGuid(),
                    CorrelationId = "mixed-complete",
                    PayloadTypeFullName = "valid"
                }
            ],
            ExpiresAtUtc = _time.Now + TimeSpan.FromMinutes(5)
        });
        _kv.Entries[NatsSubjectSchema.RecoveryKey("empty")] = JsonSerializer.Serialize(
            new NatsRecoveryStateStore.StoredRecoveryState
            {
                States = null,
                ExpiresAtUtc = _time.Now + TimeSpan.FromMinutes(5)
            });

        Assert.Equal("valid", Assert.Single(await _store.GetAllAsync("mixed-complete")).PayloadTypeFullName);
        Assert.Empty(await _store.GetAllAsync("empty"));

        var scanned = new List<RecoveryState>();
        await foreach (var state in _store.ScanAsync())
            scanned.Add(state);
        Assert.Equal("valid", Assert.Single(scanned).PayloadTypeFullName);
    }

    [Fact]
    public async Task ExpiredRead_SwallowsBestEffortDeleteFailure()
    {
        await _store.SaveAsync("expired", new RecoveryState { CorrelationId = "expired" }, TimeSpan.FromMinutes(1));
        _time.Advance(TimeSpan.FromMinutes(2));
        _kv.DeleteException = new InvalidOperationException("delete failed");

        Assert.Empty(await _store.GetAllAsync("expired"));
    }

    [Fact]
    public async Task ScanAsync_YieldsLiveEntries_AndSkipsExpired()
    {
        await _store.SaveAsync("corr-live", new RecoveryState { CorrelationId = "corr-live", PayloadTypeFullName = typeof(OperationResult).FullName }, TimeSpan.FromMinutes(10));
        await _store.SaveAsync("corr-expired", new RecoveryState { CorrelationId = "corr-expired" }, TimeSpan.FromMinutes(1));
        _kv.Entries[NatsSubjectSchema.RecoveryKey("corr-broken")] = "{not-json";

        _time.Advance(TimeSpan.FromMinutes(2));

        var states = new List<RecoveryState>();
        await foreach (var state in _store.ScanAsync())
            states.Add(state);

        Assert.Single(states);
        Assert.Equal("corr-live", states[0].CorrelationId);
        Assert.False(_kv.Entries.ContainsKey(NatsSubjectSchema.RecoveryKey("corr-expired")));
    }

    [Fact]
    public async Task ScanAsync_ObservesCancellation()
    {
        await _store.SaveAsync("corr-a", new RecoveryState { CorrelationId = "corr-a" }, TimeSpan.FromMinutes(5));

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
