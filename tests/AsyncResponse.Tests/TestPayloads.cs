namespace AsyncResponse.Tests;

public enum OperationStatus
{
    Unknown = 0,
    Running = 1,
    Completed = 2,
    Failed = 3
}

/// <summary>A typical remote-operation payload with explicit lost-subscriber recovery semantics.</summary>
public sealed class OperationResult : IAsyncResponsePayload
{
    public OperationStatus Status { get; set; }
    public string? Message { get; set; }

    // Recovery routing only — resume on the in-flight/successful states, fail on Failed (and any
    // unrecognized state). Live completion is decided by the waiter's Until predicate, not here.
    public bool ShouldResumeOnRecovery() => Status is OperationStatus.Completed or OperationStatus.Running;
}

/// <summary>A payload only ever published on success paths (failures go through SetException).</summary>
public sealed class SuccessOnlyPayload : IAsyncResponsePayload
{
    public string? Message { get; set; }

    public bool ShouldResumeOnRecovery() => true;
}
