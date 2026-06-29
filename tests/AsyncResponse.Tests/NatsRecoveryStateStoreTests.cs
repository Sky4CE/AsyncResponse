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
        Assert.Equal("corr-a", doc.RootElement.GetProperty("State").GetProperty("CorrelationId").GetString());
        Assert.Equal("corr-a", doc.RootElement.GetProperty("States")[0].GetProperty("CorrelationId").GetString());
        Assert.NotEqual(Guid.Empty, doc.RootElement.GetProperty("States")[0].GetProperty("RegistrationId").GetGuid());
        Assert.Equal(
            (_time.Now + TimeSpan.FromMinutes(3)).UtcDateTime,
            doc.RootElement.GetProperty("ExpiresAtUtc").GetDateTimeOffset().UtcDateTime);

        await Assert.ThrowsAsync<ArgumentException>(() => _store.SaveAsync(" ", state, TimeSpan.FromSeconds(1)));
        await Assert.ThrowsAsync<ArgumentNullException>(() => _store.SaveAsync("corr-a", null!, TimeSpan.FromSeconds(1)));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => _store.SaveAsync("corr-a", state, TimeSpan.Zero));

        using var canceled = new CancellationTokenSource();
        await canceled.CancelAsync();
        await Assert.ThrowsAsync<OperationCanceledException>(() => _store.SaveAsync("corr-a", state, TimeSpan.FromSeconds(1), canceled.Token));
    }

    [Fact]
    public async Task GetAsync_ReturnsState_AndBackfillsCorrelationIdFromKey()
    {
        await _store.SaveAsync("corr-a", new RecoveryState { PayloadTypeFullName = typeof(OperationResult).FullName }, TimeSpan.FromMinutes(5));

        var loaded = await _store.GetAsync("corr-a");

        Assert.NotNull(loaded);
        Assert.Equal("corr-a", loaded!.CorrelationId);
        Assert.Equal(typeof(OperationResult).FullName, loaded.PayloadTypeFullName);
    }

    [Fact]
    public async Task GetAllAsync_ReturnsAllRegistrations_AndLegacySingleState()
    {
        var first = new RecoveryState { RegistrationId = Guid.NewGuid(), CorrelationId = "corr-a" };
        var second = new RecoveryState { RegistrationId = Guid.NewGuid(), CorrelationId = "corr-a" };
        await _store.SaveAsync("corr-a", first, TimeSpan.FromMinutes(5));
        await _store.SaveAsync("corr-a", second, TimeSpan.FromMinutes(5));

        Assert.Equal(2, (await _store.GetAllAsync("corr-a")).Count);

        _kv.Entries[NatsSubjectSchema.RecoveryKey("legacy")] = JsonSerializer.Serialize(new NatsRecoveryStateStore.StoredRecoveryState
        {
            State = new RecoveryState { CorrelationId = "legacy" },
            ExpiresAtUtc = _time.Now + TimeSpan.FromMinutes(5)
        });

        Assert.Single(await _store.GetAllAsync("legacy"));
    }

    [Fact]
    public async Task GetAsync_ReturnsNull_ForMissingMalformedAndExpired()
    {
        Assert.Null(await _store.GetAsync("missing"));

        _kv.Entries[NatsSubjectSchema.RecoveryKey("broken")] = "{not-json";
        Assert.Null(await _store.GetAsync("broken"));

        await _store.SaveAsync("expired", new RecoveryState { CorrelationId = "expired" }, TimeSpan.FromMinutes(1));
        _time.Advance(TimeSpan.FromMinutes(2));
        Assert.Null(await _store.GetAsync("expired"));
        // Expired entries are deleted on read so they never resurface.
        Assert.False(_kv.Entries.ContainsKey(NatsSubjectSchema.RecoveryKey("expired")));

        await Assert.ThrowsAsync<ArgumentException>(() => _store.GetAsync(" "));
    }

    [Fact]
    public async Task TryDeleteAsync_ReportsWhetherEntryExisted()
    {
        await _store.SaveAsync("corr-a", new RecoveryState { CorrelationId = "corr-a" }, TimeSpan.FromMinutes(5));

        Assert.True(await _store.TryDeleteAsync("corr-a"));
        Assert.False(await _store.TryDeleteAsync("corr-a"));
        await Assert.ThrowsAsync<ArgumentException>(() => _store.TryDeleteAsync(" "));
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
    public async Task ScanAsync_YieldsLiveEntries_SkipsExpired_AndBackfillsCorrelationId()
    {
        await _store.SaveAsync("corr-live", new RecoveryState { PayloadTypeFullName = typeof(OperationResult).FullName }, TimeSpan.FromMinutes(10));
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
