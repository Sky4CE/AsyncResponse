using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Off the parallel path: the test below records one measurement on EVERY instrument of the
/// process-wide meter, the rarely used ones included, and an exact-count assertion elsewhere
/// that listens to one of them unscoped would count it.
/// </summary>
[CollectionDefinition(nameof(Round48NewApiTests), DisableParallelization = true)]
public sealed class Round48NewApiTestsCollection;

/// <summary>
/// Review of 2026-09-27 (whole repository at ba8e8e70) — the pins that need the API this round
/// added: the guarded recorders of <see cref="AsyncResponseDiagnostics"/>. The regressions
/// written against the pre-existing API are in <see cref="Round48RegressionTests"/>.
/// </summary>
[Collection(nameof(Round48NewApiTests))]
public sealed class Round48NewApiTests
{
    [Fact]
    public void EveryMeasurement_IsRecordedThroughTheGuard()
    {
        // The guard is central: every recorder of the library's meter, not the two that were
        // bitten. One listener on the whole meter, throwing for this test's flow only.
        using var metrics = new Round48ThrowingMetricsListener(instrument: null);
        var histogram = AsyncResponseDiagnostics.Meter.CreateHistogram<double>("asyncresponse.test.round48_guard");

        AsyncResponseDiagnostics.RecordLostSubscriber("response", RecoveryAction.KeepWaiting, callbackInvoked: false);
        AsyncResponseDiagnostics.RecordLostSubscriber("exception", action: null, callbackInvoked: true, mixed: true);
        AsyncResponseDiagnostics.RecordWaiterOverload("redis");
        AsyncResponseDiagnostics.RecordWaiterTimeout("inmemory");
        AsyncResponseDiagnostics.RecordWorkerOutcome("executed");
        AsyncResponseDiagnostics.RecordTypeResolutionFailure("payload");
        AsyncResponseDiagnostics.RecordUnroutableResponse();
        AsyncResponseDiagnostics.RecordOversizedInboundMessage("worker");
        AsyncResponseDiagnostics.RecordFlowStatePruned("postgresql", 3);
        AsyncResponseDiagnostics.RecordFlowStatePruneFailure("postgresql");
        AsyncResponseDiagnostics.RecordFlowStatePruneBudgetExhausted("postgresql");
        AsyncResponseDiagnostics.RecordFlowStateCheckpoint("postgresql", 1024);
        AsyncResponseDiagnostics.RecordInMemoryOverflowRejection();
        AsyncResponseDiagnostics.RecordInMemoryDelayedRejection();
        AsyncResponseDiagnostics.RecordFlowOwnJobRedelivery("waiting");
        AsyncResponseDiagnostics.Record(histogram, 1.5, new KeyValuePair<string, object?>("asyncresponse.channel", "test"));

        // One per call above: each reached the listener, and none of its exceptions came back.
        Assert.Equal(16, metrics.Thrown);
    }
}
