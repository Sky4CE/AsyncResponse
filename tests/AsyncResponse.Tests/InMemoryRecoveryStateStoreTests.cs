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
}
