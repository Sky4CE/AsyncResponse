using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Upper bounds on durable-flow intervals: "passes validation, throws mid-operation" was the
/// failure mode — a <see cref="TimeSpan.MaxValue"/> StateExpiry overflowed the in-memory store's
/// expiry stamp, and over-timer-ceiling lease/step intervals failed inside the renewal loop or at
/// waiter arming, after side effects.
/// </summary>
public sealed class DurableFlowOptionsValidationTests
{
    [Fact]
    public void ValidateOptions_RejectsUnrepresentableIntervals()
    {
        Assert.Throws<InvalidOperationException>(() => FlowStateConcurrency.ValidateOptions(
            new DurableFlowOptions { StateExpiry = TimeSpan.MaxValue }));
        Assert.Throws<InvalidOperationException>(() => FlowStateConcurrency.ValidateOptions(
            new DurableFlowOptions { DefaultStepTimeout = TimeSpan.FromDays(60) }));
        Assert.Throws<InvalidOperationException>(() => FlowStateConcurrency.ValidateOptions(
            new DurableFlowOptions { ExecutionLeaseDuration = TimeSpan.FromDays(60) }));
        Assert.Throws<InvalidOperationException>(() => FlowStateConcurrency.ValidateOptions(
            new DurableFlowOptions { ProgressPersistenceInterval = TimeSpan.FromDays(60) }));

        // The defaults remain valid.
        FlowStateConcurrency.ValidateOptions(new DurableFlowOptions());
    }

    [Fact]
    public async Task InMemoryFlowStateStore_SaturatesHugeTtlsInsteadOfThrowing()
    {
        // The store is also called with caller-supplied ttls (not just validated options), and
        // every external store saturates this arithmetic — the in-memory one threw.
        var store = new InMemoryFlowStateStore();
        Assert.True(await store.TryCreateAsync("huge-ttl", new FlowState { FlowId = "huge-ttl" }, TimeSpan.MaxValue));
        Assert.NotNull(await store.LoadAsync("huge-ttl"));
    }
}
