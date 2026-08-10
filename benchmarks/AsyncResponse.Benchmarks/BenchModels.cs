namespace AsyncResponse.Benchmarks;

public enum BenchStatus
{
    Unknown = 0,
    Running = 1,
    Completed = 2,
    Failed = 3
}

/// <summary>Minimal payload used across the benchmarks and the stress harness.</summary>
public sealed class BenchPayload : IAsyncResponsePayload
{
    public BenchStatus Status { get; set; }
    public string? Message { get; set; }

    public RecoveryAction OnRecovery()
        => Status is BenchStatus.Completed or BenchStatus.Running ? RecoveryAction.Resume : RecoveryAction.Fail;
}

/// <summary>Second payload shape used to exercise conversion/type-dispatch paths.</summary>
public sealed class BenchPayloadWithMetadata : IAsyncResponsePayload
{
    public BenchStatus Status { get; set; }
    public string? Message { get; set; }
    public Dictionary<string, string>? Metadata { get; set; }

    public RecoveryAction OnRecovery()
        => Status is BenchStatus.Completed or BenchStatus.Running ? RecoveryAction.Resume : RecoveryAction.Fail;
}

/// <summary>A no-op worker target for the callback/reflection benchmarks.</summary>
public interface IBenchWorker
{
    Task DoWorkAsync(int id);
    Task DoWorkWithPayloadAsync(BenchPayload payload, string correlationId);
    ValueTask DoValueWorkAsync(int id);
}

public sealed class BenchWorker : IBenchWorker
{
    public Task DoWorkAsync(int id) => Task.CompletedTask;

    public Task DoWorkWithPayloadAsync(BenchPayload payload, string correlationId) => Task.CompletedTask;

    public ValueTask DoValueWorkAsync(int id) => ValueTask.CompletedTask;
}
