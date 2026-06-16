using System.Collections.Concurrent;

namespace AsyncResponse.IntegrationTests.App;

public enum ItestStatus
{
    Unknown = 0,
    Running = 1,
    Completed = 2,
    Failed = 3
}

/// <summary>Response payload used by the integration tests; classifies its own domain outcome.</summary>
public sealed class ItestPayload : IAsyncResponsePayload
{
    public ItestStatus Status { get; set; }
    public string? Message { get; set; }

    public AsyncResponseOutcome ClassifyOutcome() => Status switch
    {
        ItestStatus.Completed => AsyncResponseOutcome.Succeeded,
        ItestStatus.Running => AsyncResponseOutcome.InProgress,
        ItestStatus.Failed => AsyncResponseOutcome.Failed,
        _ => AsyncResponseOutcome.Unknown
    };
}

/// <summary>Ambient trace id the app sets per flow; carried across serialized hops by <see cref="ItestTracePropagator"/>.</summary>
public static class ItestTraceContext
{
    private static readonly AsyncLocal<string?> _trace = new();

    public static string? Current => _trace.Value;
    public static void Set(string? value) => _trace.Value = value;

    internal static readonly IDisposable NoScope = new NullScope();

    internal static IDisposable Push(string? value)
    {
        var previous = _trace.Value;
        _trace.Value = value;
        return new Scope(previous);
    }

    private sealed class Scope(string? _previous) : IDisposable
    {
        public void Dispose() => _trace.Value = _previous;
    }

    private sealed class NullScope : IDisposable
    {
        public void Dispose() { }
    }
}

/// <summary>Carries <see cref="ItestTraceContext"/> across the serialized worker/recovery hops.</summary>
public sealed class ItestTracePropagator : IAsyncResponseContextPropagator
{
    public const string Key = "itest.trace";

    public void Capture(IDictionary<string, string> carrier)
    {
        if (ItestTraceContext.Current is { } trace)
            carrier[Key] = trace;
    }

    public IDisposable Restore(IReadOnlyDictionary<string, string> carrier)
        => carrier.TryGetValue(Key, out var trace)
            ? ItestTraceContext.Push(trace)
            : ItestTraceContext.NoScope;
}

/// <summary>One recorded invocation observed by the flow service (worker job, recovery callback, or waiter result).</summary>
public sealed record ItestCall(string Kind, string? CorrelationId, string? Trace, ItestStatus? Status, string? Detail);

/// <summary>
/// The flow service worker jobs and lost-subscriber callbacks are dispatched to (by full name, via DI),
/// recording each invocation so tests can await and assert it.
/// </summary>
public interface IItestFlowService
{
    Task ProcessWorkAsync(string token);
    Task ResumeAsync(ItestPayload payload, string correlationId);
    Task FailAsync(Exception exception, string correlationId);
}

public sealed class ItestFlowService : IItestFlowService
{
    private readonly ConcurrentQueue<ItestCall> _calls = new();
    private readonly ConcurrentDictionary<string, TaskCompletionSource<ItestCall>> _waiters = new(StringComparer.Ordinal);

    public IReadOnlyCollection<ItestCall> Calls => _calls;

    public Task ProcessWorkAsync(string token)
    {
        Record($"worker:{token}", new ItestCall("worker", AsyncResponseContext.CorrelationId, ItestTraceContext.Current, null, token));
        return Task.CompletedTask;
    }

    public Task ResumeAsync(ItestPayload payload, string correlationId)
    {
        Record($"resume:{correlationId}", new ItestCall("resume", correlationId, ItestTraceContext.Current, payload.Status, payload.Message));
        return Task.CompletedTask;
    }

    public Task FailAsync(Exception exception, string correlationId)
    {
        var detail = exception is AsyncResponseDomainFailureException domain
            ? $"domain:{domain.Outcome}"
            : exception.GetType().Name;
        Record($"fail:{correlationId}", new ItestCall("fail", correlationId, ItestTraceContext.Current, null, detail));
        return Task.CompletedTask;
    }

    /// <summary>Records the terminal outcome of an active waiter (completed payload or fault).</summary>
    public void RecordWaiterResult(string correlationId, Task<ItestPayload> task)
    {
        var call = task.Status == TaskStatus.RanToCompletion
            ? new ItestCall("waiter", correlationId, ItestTraceContext.Current, task.Result.Status, task.Result.Message)
            : new ItestCall("waiter-faulted", correlationId, null, null, task.Exception?.GetBaseException().GetType().Name);
        Record($"waiter:{correlationId}", call);
    }

    /// <summary>Awaits the recorded call for <paramref name="key"/> (e.g. <c>worker:{token}</c>, <c>resume:{cid}</c>).</summary>
    public Task<ItestCall> WaitForAsync(string key)
        => _waiters.GetOrAdd(key, _ => NewSource()).Task;

    public void Clear()
    {
        while (_calls.TryDequeue(out _))
        {
        }

        _waiters.Clear();
    }

    private void Record(string key, ItestCall call)
    {
        _calls.Enqueue(call);
        _waiters.GetOrAdd(key, _ => NewSource()).TrySetResult(call);
    }

    private static TaskCompletionSource<ItestCall> NewSource() => new(TaskCreationOptions.RunContinuationsAsynchronously);
}
