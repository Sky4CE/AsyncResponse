namespace AsyncResponse.Tests;

public enum OperationStatus
{
    Unknown = 0,
    Running = 1,
    Completed = 2,
    Failed = 3
}

/// <summary>A typical remote-operation payload with explicit domain semantics.</summary>
public sealed class OperationResult : IAsyncResponsePayload
{
    public OperationStatus Status { get; set; }
    public string? Message { get; set; }

    public AsyncResponseOutcome ClassifyOutcome() => Status switch
    {
        OperationStatus.Completed => AsyncResponseOutcome.Succeeded,
        OperationStatus.Running => AsyncResponseOutcome.InProgress,
        OperationStatus.Failed => AsyncResponseOutcome.Failed,
        _ => AsyncResponseOutcome.Unknown
    };
}

/// <summary>A payload only ever published on success paths (failures go through SetException).</summary>
public sealed class SuccessOnlyPayload : IAsyncResponsePayload
{
    public string? Message { get; set; }

    public AsyncResponseOutcome ClassifyOutcome() => AsyncResponseOutcome.Succeeded;
}
