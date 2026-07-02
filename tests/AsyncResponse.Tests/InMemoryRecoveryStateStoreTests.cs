using Xunit;

namespace AsyncResponse.Tests;

public class InMemoryRecoveryStateStoreTests
{
    [Fact]
    public async Task SaveAsync_ValidatesInputsAndCancellation()
    {
        var store = new InMemoryRecoveryStateStore();
        var state = new RecoveryState { CorrelationId = "corr-a", RegisteredAtUtc = DateTime.UtcNow };

        await Assert.ThrowsAsync<ArgumentException>(() => store.SaveAsync(" ", state, TimeSpan.FromSeconds(1)));
        await Assert.ThrowsAsync<ArgumentNullException>(() => store.SaveAsync("corr-a", null!, TimeSpan.FromSeconds(1)));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => store.SaveAsync("corr-a", state, TimeSpan.Zero));

        using var canceled = new CancellationTokenSource();
        await canceled.CancelAsync();
        await Assert.ThrowsAsync<OperationCanceledException>(() =>
            store.SaveAsync("corr-a", state, TimeSpan.FromSeconds(1), canceled.Token));
    }

    [Fact]
    public async Task GetAsync_RemovesExpiredEntriesAndHonorsCancellation()
    {
        var store = new InMemoryRecoveryStateStore();
        await store.SaveAsync(
            "corr-a",
            new RecoveryState { CorrelationId = "corr-a", RegisteredAtUtc = DateTime.UtcNow },
            TimeSpan.FromMilliseconds(1));
        await Task.Delay(20);

        Assert.Null(await store.GetAsync("corr-a"));
        Assert.False(await store.TryDeleteAsync("corr-a"));

        await Assert.ThrowsAsync<ArgumentException>(() => store.GetAsync(" "));
        using var canceled = new CancellationTokenSource();
        await canceled.CancelAsync();
        await Assert.ThrowsAsync<OperationCanceledException>(() => store.GetAsync("corr-a", canceled.Token));
    }

    [Fact]
    public async Task ScanAsync_YieldsOnlyLiveEntriesAndHonorsCancellation()
    {
        var store = new InMemoryRecoveryStateStore();
        await store.SaveAsync(
            "live",
            new RecoveryState { CorrelationId = "live", RegisteredAtUtc = DateTime.UtcNow },
            TimeSpan.FromMinutes(1));
        await store.SaveAsync(
            "expired",
            new RecoveryState { CorrelationId = "expired", RegisteredAtUtc = DateTime.UtcNow },
            TimeSpan.FromMilliseconds(1));
        await Task.Delay(20);

        var states = new List<RecoveryState>();
        await foreach (var state in store.ScanAsync())
            states.Add(state);

        Assert.Single(states);
        Assert.Equal("live", states[0].CorrelationId);

        using var canceled = new CancellationTokenSource();
        await canceled.CancelAsync();
        await Assert.ThrowsAsync<OperationCanceledException>(async () =>
        {
            await foreach (var _ in store.ScanAsync(canceled.Token))
            {
            }
        });
    }

    [Fact]
    public async Task SameCorrelationId_AppendsAndDeletesOneRegistration()
    {
        var store = new InMemoryRecoveryStateStore();
        var firstId = Guid.NewGuid();
        var secondId = Guid.NewGuid();

        await store.SaveAsync(
            "corr-a",
            new RecoveryState { RegistrationId = firstId, CorrelationId = "corr-a", RegisteredAtUtc = DateTime.UtcNow },
            TimeSpan.FromMinutes(1));
        await store.SaveAsync(
            "corr-a",
            new RecoveryState { RegistrationId = secondId, CorrelationId = "corr-a", RegisteredAtUtc = DateTime.UtcNow },
            TimeSpan.FromMinutes(1));

        var states = await store.GetAllAsync("corr-a");
        Assert.Equal(2, states.Count);

        Assert.True(await store.TryDeleteAsync("corr-a", firstId));

        var remaining = Assert.Single(await store.GetAllAsync("corr-a"));
        Assert.Equal(secondId, remaining.RegistrationId);
        Assert.True(await store.TryDeleteAsync("corr-a"));
        Assert.Empty(await store.GetAllAsync("corr-a"));
    }

    [Fact]
    public async Task SameRegistrationId_ReplacesExistingState()
    {
        var store = new InMemoryRecoveryStateStore();
        var registrationId = Guid.NewGuid();

        await store.SaveAsync(
            "corr-a",
            new RecoveryState { RegistrationId = registrationId, CorrelationId = "corr-a", PayloadTypeFullName = "old" },
            TimeSpan.FromMinutes(1));
        await store.SaveAsync(
            "corr-a",
            new RecoveryState { RegistrationId = registrationId, CorrelationId = "corr-a", PayloadTypeFullName = "new" },
            TimeSpan.FromMinutes(1));

        var state = Assert.Single(await store.GetAllAsync("corr-a"));
        Assert.Equal("new", state.PayloadTypeFullName);
    }

    [Fact]
    public async Task SameRegistrationId_ReplacesExistingStateWithinManyBucket()
    {
        var store = new InMemoryRecoveryStateStore();
        var firstId = Guid.NewGuid();
        var secondId = Guid.NewGuid();

        await store.SaveAsync("corr-a", State(firstId, "old-first"), TimeSpan.FromMinutes(1));
        await store.SaveAsync("corr-a", State(secondId, "second"), TimeSpan.FromMinutes(1));
        await store.SaveAsync("corr-a", State(firstId, "new-first"), TimeSpan.FromMinutes(1));

        Assert.Equal(
            ["new-first", "second"],
            (await store.GetAllAsync("corr-a")).Select(state => state.PayloadTypeFullName).Order());
    }

    [Fact]
    public async Task TryDeleteAsync_RemovesOneOfManyRegistrations()
    {
        var store = new InMemoryRecoveryStateStore();
        var firstId = Guid.NewGuid();
        var secondId = Guid.NewGuid();
        var thirdId = Guid.NewGuid();

        await store.SaveAsync("corr-a", State(firstId, "first"), TimeSpan.FromMinutes(1));
        await store.SaveAsync("corr-a", State(secondId, "second"), TimeSpan.FromMinutes(1));
        await store.SaveAsync("corr-a", State(thirdId, "third"), TimeSpan.FromMinutes(1));

        Assert.False(await store.TryDeleteAsync("corr-a", Guid.NewGuid()));
        Assert.True(await store.TryDeleteAsync("corr-a", secondId));

        Assert.Equal(["first", "third"], (await store.GetAllAsync("corr-a")).Select(state => state.PayloadTypeFullName));
    }

    [Fact]
    public async Task GetAllAsync_FiltersUnreadableStatesFromMixedBucket()
    {
        var store = new InMemoryRecoveryStateStore();

        await store.SaveAsync("corr-a", State(Guid.NewGuid(), "old"), TimeSpan.FromMinutes(1));
        await store.SaveAsync(
            "corr-a",
            new RecoveryState
            {
                RegistrationId = Guid.NewGuid(),
                CorrelationId = "corr-a",
                PayloadTypeFullName = "future",
                SchemaVersion = RecoveryStateSchema.Current + 1
            },
            TimeSpan.FromMinutes(1));

        var state = Assert.Single(await store.GetAllAsync("corr-a"));
        Assert.Equal("old", state.PayloadTypeFullName);
        Assert.Equal("old", (await store.GetAsync("corr-a"))!.PayloadTypeFullName);
    }

    [Fact]
    public async Task GetAsync_AndGetAllAsync_ReturnNullOrEmptyWhenOnlyStateIsUnreadable()
    {
        var store = new InMemoryRecoveryStateStore();
        await store.SaveAsync(
            "corr-a",
            new RecoveryState
            {
                RegistrationId = Guid.NewGuid(),
                CorrelationId = "corr-a",
                PayloadTypeFullName = "future",
                SchemaVersion = RecoveryStateSchema.Current + 1
            },
            TimeSpan.FromMinutes(1));

        Assert.Null(await store.GetAsync("corr-a"));
        Assert.Empty(await store.GetAllAsync("corr-a"));
    }

    [Fact]
    public async Task GetAsync_AndGetAllAsync_ReturnNullOrEmptyWhenManyStatesAreUnreadable()
    {
        var store = new InMemoryRecoveryStateStore();
        await store.SaveAsync("corr-a", UnreadableState("future-1"), TimeSpan.FromMinutes(1));
        await store.SaveAsync("corr-a", UnreadableState("future-2"), TimeSpan.FromMinutes(1));

        Assert.Null(await store.GetAsync("corr-a"));
        Assert.Empty(await store.GetAllAsync("corr-a"));
    }

    [Fact]
    public async Task GetAllAsync_PrunesManyExpiredEntriesToEmpty()
    {
        var store = new InMemoryRecoveryStateStore();
        var firstId = Guid.NewGuid();
        var secondId = Guid.NewGuid();

        await store.SaveAsync("corr-a", State(firstId, "first"), TimeSpan.FromMilliseconds(1));
        await store.SaveAsync("corr-a", State(secondId, "second"), TimeSpan.FromMilliseconds(1));
        await Task.Delay(20);

        Assert.Empty(await store.GetAllAsync("corr-a"));
        Assert.False(await store.TryDeleteAsync("corr-a", firstId));
    }

    [Fact]
    public async Task TryDeleteAsync_WithRegistrationId_PrunesExpiredBucketToMissing()
    {
        var store = new InMemoryRecoveryStateStore();
        var firstId = Guid.NewGuid();
        var secondId = Guid.NewGuid();

        await store.SaveAsync("corr-a", State(firstId, "first"), TimeSpan.FromMilliseconds(1));
        await store.SaveAsync("corr-a", State(secondId, "second"), TimeSpan.FromMilliseconds(1));
        await Task.Delay(20);

        Assert.False(await store.TryDeleteAsync("corr-a", firstId));
        Assert.Empty(await store.GetAllAsync("corr-a"));
    }

    [Fact]
    public async Task GetAllAsync_PrunesManyExpiredEntriesToSingleLiveEntry()
    {
        var store = new InMemoryRecoveryStateStore();
        var expiredId = Guid.NewGuid();
        var liveId = Guid.NewGuid();

        await store.SaveAsync("corr-a", State(expiredId, "expired"), TimeSpan.FromMilliseconds(1));
        await store.SaveAsync("corr-a", State(liveId, "live"), TimeSpan.FromMinutes(1));
        await Task.Delay(20);

        var state = Assert.Single(await store.GetAllAsync("corr-a"));

        Assert.Equal(liveId, state.RegistrationId);
        Assert.Equal("live", state.PayloadTypeFullName);
    }

    [Fact]
    public async Task ScanAsync_PrunesManyExpiredEntriesToMultipleLiveEntries()
    {
        var store = new InMemoryRecoveryStateStore();

        await store.SaveAsync("corr-a", State(Guid.NewGuid(), "expired"), TimeSpan.FromMilliseconds(1));
        await store.SaveAsync("corr-a", State(Guid.NewGuid(), "first"), TimeSpan.FromMinutes(1));
        await store.SaveAsync("corr-a", State(Guid.NewGuid(), "second"), TimeSpan.FromMinutes(1));
        await Task.Delay(20);

        var states = new List<RecoveryState>();
        await foreach (var state in store.ScanAsync())
            states.Add(state);

        Assert.Equal(["first", "second"], states.Select(state => state.PayloadTypeFullName).OrderBy(value => value));
    }

    private static RecoveryState State(Guid registrationId, string payloadType)
        => new()
        {
            RegistrationId = registrationId,
            CorrelationId = "corr-a",
            PayloadTypeFullName = payloadType,
            RegisteredAtUtc = DateTime.UtcNow
        };

    private static RecoveryState UnreadableState(string payloadType)
        => new()
        {
            RegistrationId = Guid.NewGuid(),
            CorrelationId = "corr-a",
            PayloadTypeFullName = payloadType,
            SchemaVersion = RecoveryStateSchema.Current + 1,
            RegisteredAtUtc = DateTime.UtcNow
        };
}
