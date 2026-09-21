namespace AsyncResponse;

// An interruption of the attempt, not a failure of the step: flow code that filters
// DurableFlowInterruptedException (or OperationCanceledException) out of its catch-all lets a park
// unwind to the executor instead of compensating for a run that merely went to sleep.
internal sealed class DurableFlowSuspendedException(string message) : DurableFlowInterruptedException(message);
