using Xunit;

namespace AsyncResponse.Tests;

public class AsyncResponsePayloadReflectionTests
{
    [Fact]
    public void OverridesShouldResumeOnRecovery_RejectsNullType()
        => Assert.Throws<ArgumentNullException>(() => AsyncResponsePayloadReflection.OverridesShouldResumeOnRecovery(null!));

    [Fact]
    public void OverridesShouldResumeOnRecovery_ReturnsFalseForInterfaceAndNonPayloadTypes()
    {
        Assert.False(AsyncResponsePayloadReflection.OverridesShouldResumeOnRecovery(typeof(IAsyncResponsePayload)));
        Assert.False(AsyncResponsePayloadReflection.OverridesShouldResumeOnRecovery(typeof(string)));
    }

    [Fact]
    public void OverridesShouldResumeOnRecovery_DetectsDefaultInterfaceImplementation()
        => Assert.False(AsyncResponsePayloadReflection.OverridesShouldResumeOnRecovery(typeof(DefaultRecoveryPayload)));

    [Fact]
    public void DefaultShouldResumeOnRecovery_ReturnsFalse()
    {
        IAsyncResponsePayload payload = new DefaultRecoveryPayload();

        Assert.False(payload.ShouldResumeOnRecovery());
    }

    [Fact]
    public void OverridesShouldResumeOnRecovery_DetectsConcreteOverride()
        => Assert.True(AsyncResponsePayloadReflection.OverridesShouldResumeOnRecovery(typeof(OperationResult)));

    [Fact]
    public async Task RecoveryStateStore_DefaultMethods_UseSingleEntryBehavior()
    {
        IRecoveryStateStore store = new LegacyRecoveryStateStore(new RecoveryState { CorrelationId = "corr" });

        var state = Assert.Single(await store.GetAllAsync("corr"));
        Assert.Equal("corr", state.CorrelationId);
        Assert.True(await store.TryDeleteAsync("corr", Guid.NewGuid()));
    }

    private sealed class DefaultRecoveryPayload : IAsyncResponsePayload;

    private sealed class LegacyRecoveryStateStore(RecoveryState? _state) : IRecoveryStateStore
    {
        public Task SaveAsync(string correlationId, RecoveryState state, TimeSpan ttl, CancellationToken cancellationToken = default)
            => Task.CompletedTask;

        public Task<RecoveryState?> GetAsync(string correlationId, CancellationToken cancellationToken = default)
            => Task.FromResult(_state);

        public Task<bool> TryDeleteAsync(string correlationId, CancellationToken cancellationToken = default)
            => Task.FromResult(true);
    }
}
