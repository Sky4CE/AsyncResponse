namespace AsyncResponse;

/// <summary>
/// Internal companion to <see cref="IDurableFlowExecutionObserver"/> for observers that must know
/// how a timer step actually waits. <see cref="IDurableFlowExecutionObserver.OnStepWaitingAsync"/>
/// fires before the engine decides between suspending (a delayed wake-up job) and waiting in
/// process, so an observer can only predict that choice from the remainder and the in-process
/// threshold — and the prediction is wrong for a skew-forced wake-up, which waits out a remainder
/// above the threshold in process rather than re-suspend and lose its skew proof. The test
/// harness's quiesce probe implements this so it counts such a wait as parked instead of as a job
/// still running user code.
/// </summary>
internal interface IInProcessTimerParkObserver
{
    /// <summary>
    /// A timer step of <paramref name="flowId"/> is about to wait in process until
    /// <paramref name="waitEndsAtUtc"/> (one hop: the end of this delivery's wait, not necessarily
    /// the step's due time), holding its worker slot.
    /// </summary>
    void OnTimerParkedInProcess(string flowId, string stepName, DateTime waitEndsAtUtc);
}
