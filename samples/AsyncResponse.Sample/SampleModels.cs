using System.Collections.Concurrent;

namespace AsyncResponse.Sample;

public enum OperationStatus
{
    Unknown = 0,
    Running = 1,
    Completed = 2,
    Failed = 3
}

/// <summary>
/// The response payload the simulated remote system sends back. <see cref="ShouldResumeOnRecovery"/>
/// is consulted only on the lost-subscriber recovery path (live completion is owned by the waiter's
/// <c>Until</c> predicate): resume on the successful/in-flight states, fail on Failed and anything
/// unrecognized.
/// </summary>
public sealed class OperationResult : IAsyncResponsePayload
{
    public OperationStatus Status { get; set; }
    public string? Message { get; set; }

    public bool ShouldResumeOnRecovery() => Status is OperationStatus.Completed or OperationStatus.Running;
}

/// <summary>One invocation observed by <see cref="SampleFlowService"/> — a worker job, a recovery
/// callback, or an active waiter's terminal result. Recorded so integration tests can await and
/// assert it over HTTP (see the <c>/calls</c> endpoint).</summary>
public sealed record FlowCall(string Kind, string? CorrelationId, string? Trace, string? Tenant, OperationStatus? Status, string? Detail);

/// <summary>
/// Records flow invocations (worker jobs, recovery callbacks, waiter results) and lets a caller
/// long-poll for one by key. This is the observability seam that lets the integration tests assert
/// — over HTTP — that an out-of-process callback actually fired with the expected restored context.
/// </summary>
public sealed class FlowRecorder
{
    private readonly ConcurrentQueue<FlowCall> _calls = new();
    private readonly ConcurrentDictionary<string, TaskCompletionSource<FlowCall>> _waiters = new(StringComparer.Ordinal);

    public IReadOnlyCollection<FlowCall> Calls => _calls;

    public void Record(string key, FlowCall call)
    {
        _calls.Enqueue(call);
        _waiters.GetOrAdd(key, _ => NewSource()).TrySetResult(call);
    }

    /// <summary>Records the terminal outcome of an active waiter (completed payload or fault).</summary>
    public void RecordWaiterResult(string correlationId, Task<OperationResult> task)
    {
        var call = task.Status == TaskStatus.RanToCompletion
            ? new FlowCall("waiter", correlationId, SampleTraceContext.Current, SampleTenantContext.Current, task.Result.Status, task.Result.Message)
            : new FlowCall("waiter-faulted", correlationId, null, null, null, task.Exception?.GetBaseException().GetType().Name);
        Record($"waiter:{correlationId}", call);
    }

    /// <summary>Awaits the recorded call for <paramref name="key"/> (e.g. <c>worker:{token}</c>, <c>resume:{cid}</c>).</summary>
    public Task<FlowCall> WaitForAsync(string key) => _waiters.GetOrAdd(key, _ => NewSource()).Task;

    public void Clear()
    {
        while (_calls.TryDequeue(out _))
        {
        }

        _waiters.Clear();
    }

    private static TaskCompletionSource<FlowCall> NewSource() => new(TaskCreationOptions.RunContinuationsAsynchronously);
}

/// <summary>
/// The "flow service" of the sample. Lost-subscriber callbacks and worker jobs are dispatched to
/// this interface by full name through the DI container — exactly how production resume/fail
/// handlers work. Every method both logs (so the demo is observable in the console/dashboard) and
/// records to <see cref="FlowRecorder"/> (so integration tests can assert it).
/// </summary>
public interface ISampleFlowService
{
    /// <summary>A fire-and-forget background worker job; <paramref name="token"/> identifies the unit of work.</summary>
    Task ProcessWorkAsync(string token);

    /// <summary>Lost-subscriber resume: a successful/in-progress response arrived after the waiter was lost.</summary>
    Task ResumeFlowAsync(string flowName, OperationResult payload, string correlationId);

    /// <summary>Lost-subscriber failure: a failed response or technical error arrived after the waiter was lost.</summary>
    Task FailFlowAsync(Exception exception, string correlationId);
}

public sealed class SampleFlowService(FlowRecorder _recorder, ILogger<SampleFlowService> _logger) : ISampleFlowService
{
    public Task ProcessWorkAsync(string token)
    {
        _logger.LogInformation(
            "WORKER: processing {Token} (correlationId: {CorrelationId}, traceId: {TraceId}, tenant: {Tenant})…",
            token, AsyncResponseContext.CorrelationId, SampleTraceContext.Current, SampleTenantContext.Current);
        _recorder.Record($"worker:{token}", new FlowCall(
            "worker", AsyncResponseContext.CorrelationId, SampleTraceContext.Current, SampleTenantContext.Current, null, token));
        return Task.CompletedTask;
    }

    public Task ResumeFlowAsync(string flowName, OperationResult payload, string correlationId)
    {
        _logger.LogWarning(
            "RECOVERY (resume): flow '{FlowName}' got a {Status} response after its waiter was lost " +
            "(correlationId: {CorrelationId}, traceId: {TraceId}, tenant: {Tenant}, message: {Message}).",
            flowName, payload.Status, correlationId, SampleTraceContext.Current, SampleTenantContext.Current, payload.Message);
        _recorder.Record($"resume:{correlationId}", new FlowCall(
            "resume", correlationId, SampleTraceContext.Current, SampleTenantContext.Current, payload.Status, payload.Message));
        return Task.CompletedTask;
    }

    public Task FailFlowAsync(Exception exception, string correlationId)
    {
        var detail = exception is AsyncResponseDomainFailureException
            ? "domain-failure"
            : exception.GetType().Name;

        _logger.LogError(exception,
            "RECOVERY (failure): correlationId {CorrelationId} (traceId: {TraceId}, tenant: {Tenant}) failed after its waiter was lost — {Detail}.",
            correlationId, SampleTraceContext.Current, SampleTenantContext.Current, detail);
        _recorder.Record($"fail:{correlationId}", new FlowCall(
            "fail", correlationId, SampleTraceContext.Current, SampleTenantContext.Current, null, detail));
        return Task.CompletedTask;
    }
}

/// <summary>
/// A tiny ambient "trace id" the sample sets per request, standing in for a real trace/principal.
/// It flows automatically to in-process work (the in-memory worker, the response handler) via
/// <see cref="System.Threading.ExecutionContext"/>; to survive the serialized hops (lost-subscriber
/// recovery, broker-backed workers) it needs <see cref="SampleTracePropagator"/>.
/// </summary>
public static class SampleTraceContext
{
    private static readonly AsyncLocal<string?> _traceId = new();

    public static string? Current => _traceId.Value;

    public static void Set(string? traceId) => _traceId.Value = traceId;

    internal static readonly IDisposable NoScope = new NullScope();

    internal static IDisposable Push(string? traceId)
    {
        var previous = _traceId.Value;
        _traceId.Value = traceId;
        return new Scope(previous);
    }

    private sealed class Scope(string? _previous) : IDisposable
    {
        public void Dispose() => _traceId.Value = _previous;
    }

    private sealed class NullScope : IDisposable
    {
        public void Dispose() { }
    }
}

/// <summary>
/// Sample <see cref="IAsyncResponseContextPropagator"/> that carries the ambient trace id across the
/// serialization boundary into worker jobs and lost-subscriber recovery callbacks. A real one would
/// also restore an <c>ILogger.BeginScope</c> in <see cref="Restore"/> so emitted logs carry the
/// trace id — that is what the returned <see cref="IDisposable"/> is for.
/// </summary>
public sealed class SampleTracePropagator : IAsyncResponseContextPropagator
{
    private const string Key = "sample.traceId";

    public void Capture(IDictionary<string, string> carrier)
    {
        if (SampleTraceContext.Current is { } traceId)
            carrier[Key] = traceId;
    }

    public IDisposable Restore(IReadOnlyDictionary<string, string> carrier)
        => carrier.TryGetValue(Key, out var traceId)
            ? SampleTraceContext.Push(traceId)
            : SampleTraceContext.NoScope;
}

/// <summary>
/// A second ambient value (tenant) the sample sets per request, demonstrating how multiple
/// propagators compose — each registered with its own <c>.WithContextPropagator&lt;T&gt;()</c> and
/// namespacing its own carrier key.
/// </summary>
public static class SampleTenantContext
{
    private static readonly AsyncLocal<string?> _tenant = new();

    public static string? Current => _tenant.Value;

    public static void Set(string? tenant) => _tenant.Value = tenant;

    internal static readonly IDisposable NoScope = new NullScope();

    internal static IDisposable Push(string? tenant)
    {
        var previous = _tenant.Value;
        _tenant.Value = tenant;
        return new Scope(previous);
    }

    private sealed class Scope(string? _previous) : IDisposable
    {
        public void Dispose() => _tenant.Value = _previous;
    }

    private sealed class NullScope : IDisposable
    {
        public void Dispose() { }
    }
}

/// <summary>Sample propagator carrying the tenant id; registered alongside <see cref="SampleTracePropagator"/>.</summary>
public sealed class SampleTenantPropagator : IAsyncResponseContextPropagator
{
    private const string Key = "sample.tenant";

    public void Capture(IDictionary<string, string> carrier)
    {
        if (SampleTenantContext.Current is { } tenant)
            carrier[Key] = tenant;
    }

    public IDisposable Restore(IReadOnlyDictionary<string, string> carrier)
        => carrier.TryGetValue(Key, out var tenant)
            ? SampleTenantContext.Push(tenant)
            : SampleTenantContext.NoScope;
}
