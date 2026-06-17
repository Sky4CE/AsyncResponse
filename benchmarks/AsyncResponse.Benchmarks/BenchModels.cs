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

    public bool ShouldResumeOnRecovery() => Status is BenchStatus.Completed or BenchStatus.Running;
}

/// <summary>A no-op worker target for the callback/reflection benchmarks.</summary>
public interface IBenchWorker
{
    Task DoWorkAsync(int id);
}

public sealed class BenchWorker : IBenchWorker
{
    public Task DoWorkAsync(int id) => Task.CompletedTask;
}
