namespace AsyncResponse.Tests;

public enum OperationStatus
{
    Unknown = 0,
    Running = 1,
    Completed = 2,
    Failed = 3
}

/// <summary>
/// A typical remote-operation payload with explicit lost-subscriber recovery semantics: the remote
/// side streams Running progress before its terminal result, so a Running message on the recovery
/// path is a checkpoint to wait past, not an outcome. Live completion is decided by the waiter's
/// Until predicate, not here.
/// </summary>
public sealed class OperationResult : IAsyncResponsePayload
{
    public OperationStatus Status { get; set; }
    public string? Message { get; set; }

    public RecoveryAction OnRecovery() => Status switch
    {
        OperationStatus.Completed => RecoveryAction.Resume,
        OperationStatus.Running => RecoveryAction.KeepWaiting,
        _ => RecoveryAction.Fail,
    };
}

/// <summary>A payload only ever published on success paths (failures go through SetException).</summary>
public sealed class SuccessOnlyPayload : IAsyncResponsePayload
{
    public string? Message { get; set; }

    public RecoveryAction OnRecovery() => RecoveryAction.Resume;
}
