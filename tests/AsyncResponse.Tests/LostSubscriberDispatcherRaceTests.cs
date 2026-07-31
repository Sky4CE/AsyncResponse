using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// The lost-subscriber dispatcher's snapshot-race re-check must run before the empty-state early
/// return. A waiter registers its subscription before saving its recovery state, so an empty
/// snapshot can mean "the waiter is about to go live": the publisher counted zero subscribers,
/// the waiter then registered, and its recovery state is not visible yet. Returning without
/// RetryLive there drops the response — it is neither published live nor routed to recovery.
/// </summary>
public sealed class LostSubscriberDispatcherRaceTests
{
    private static LostSubscriberCallbackDispatcher CreateDispatcher()
    {
        var provider = new ServiceCollection().BuildServiceProvider();
        return new LostSubscriberCallbackDispatcher(
            provider.GetRequiredService<IServiceScopeFactory>(),
            new AsyncResponseContextPropagation([]),
            NullLogger<LostSubscriberCallbackDispatcher>.Instance);
    }

    [Fact]
    public async Task DispatchLostResponses_EmptySnapshotButLiveSubscriber_ReturnsRetryLive()
    {
        var store = new EmptyRecoveryStateStore();
        var dispatcher = CreateDispatcher();

        var result = await dispatcher.DispatchLostResponses(
            store,
            "cid-live-race",
            new OperationResult(),
            "channel-live-race",
            CancellationToken.None,
            hasLiveSubscriber: () => new ValueTask<bool>(true));

        Assert.True(result.RetryLive);
        Assert.False(result.CallbackInvoked);
    }

    [Fact]
    public async Task DispatchLostExceptions_EmptySnapshotButLiveSubscriber_ReturnsRetryLive()
    {
        var store = new EmptyRecoveryStateStore();
        var dispatcher = CreateDispatcher();

        var result = await dispatcher.DispatchLostExceptions(
            store,
            "cid-live-race",
            new InvalidOperationException("remote failure"),
            "channel-live-race",
            CancellationToken.None,
            hasLiveSubscriber: () => new ValueTask<bool>(true));

        Assert.True(result.RetryLive);
        Assert.False(result.CallbackInvoked);
    }

    [Fact]
    public async Task DispatchLostResponses_EmptySnapshotAndNoLiveSubscriber_DoesNotRetryLive()
    {
        var store = new EmptyRecoveryStateStore();
        var dispatcher = CreateDispatcher();

        var result = await dispatcher.DispatchLostResponses(
            store,
            "cid-truly-lost",
            new OperationResult(),
            "channel-truly-lost",
            CancellationToken.None,
            hasLiveSubscriber: () => new ValueTask<bool>(false));

        Assert.False(result.RetryLive);
        Assert.False(result.CallbackInvoked);
    }

    private sealed class EmptyRecoveryStateStore : IRecoveryStateStore
    {
        public Task SaveAsync(string correlationId, RecoveryState state, TimeSpan ttl, CancellationToken cancellationToken = default)
            => Task.CompletedTask;

        public Task<IReadOnlyList<RecoveryState>> GetAllAsync(string correlationId, CancellationToken cancellationToken = default)
            => Task.FromResult<IReadOnlyList<RecoveryState>>([]);

        public Task<bool> TryDeleteAsync(string correlationId, Guid registrationId, CancellationToken cancellationToken = default)
            => Task.FromResult(false);
    }
}
