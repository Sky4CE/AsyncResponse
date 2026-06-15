namespace AsyncResponse.Sample;

public enum OperationStatus
{
    Unknown = 0,
    Running = 1,
    Completed = 2,
    Failed = 3
}

/// <summary>
/// The response payload the simulated remote system sends back. The classifier mirrors the
/// semantics the active waiter applies: Completed succeeds, Running keeps waiting, Failed fails,
/// anything else is conservatively unknown.
/// </summary>
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

/// <summary>
/// The "flow service" of the sample application. Lost-subscriber callbacks and worker jobs are
/// dispatched to this interface by full name through the DI container — exactly how production
/// resume/fail handlers work.
/// </summary>
public interface ISampleFlowService
{
    /// <summary>A background worker job (fire-and-forget demo).</summary>
    Task ProcessOrderAsync(int orderId);

    /// <summary>Lost-subscriber resume: a successful/in-progress response arrived after a "crash".</summary>
    Task ResumeFlowAsync(string flowName, OperationResult payload, string correlationId);

    /// <summary>Lost-subscriber failure: a failed response or technical error arrived after a "crash".</summary>
    Task FailFlowAsync(Exception exception, string correlationId);
}

public sealed class SampleFlowService(ILogger<SampleFlowService> _logger) : ISampleFlowService
{
    public async Task ProcessOrderAsync(int orderId)
    {
        _logger.LogInformation("WORKER: processing order {OrderId} (correlationId: {CorrelationId}, traceId: {TraceId}, tenant: {Tenant})…",
            orderId, AsyncResponseContext.CorrelationId, SampleTraceContext.Current, SampleTenantContext.Current);
        await Task.Delay(1_000);
        _logger.LogInformation("WORKER: order {OrderId} processed.", orderId);
    }

    public Task ResumeFlowAsync(string flowName, OperationResult payload, string correlationId)
    {
        _logger.LogWarning(
            "RECOVERY (resume): flow '{FlowName}' got a {Status} response after its waiter was lost " +
            "(correlationId: {CorrelationId}, traceId: {TraceId}, tenant: {Tenant}, message: {Message}). A real flow would resume or re-register here.",
            flowName, payload.Status, correlationId, SampleTraceContext.Current, SampleTenantContext.Current, payload.Message);
        return Task.CompletedTask;
    }

    public Task FailFlowAsync(Exception exception, string correlationId)
    {
        if (exception is AsyncResponseDomainFailureException domainFailure)
        {
            _logger.LogError(
                "RECOVERY (failure): correlationId {CorrelationId} (traceId: {TraceId}, tenant: {Tenant}) reported domain outcome {Outcome} after its waiter was lost. Payload: {Payload}. A real flow would mark itself failed (retriable) here.",
                correlationId, SampleTraceContext.Current, SampleTenantContext.Current, domainFailure.Outcome, domainFailure.PayloadJson);
        }
        else
        {
            _logger.LogError(exception,
                "RECOVERY (failure): correlationId {CorrelationId} (traceId: {TraceId}, tenant: {Tenant}) failed technically after its waiter was lost.",
                correlationId, SampleTraceContext.Current, SampleTenantContext.Current);
        }

        return Task.CompletedTask;
    }
}

/// <summary>
/// A tiny ambient "trace id" the sample sets per request, standing in for a real trace/tenant/
/// principal. It flows automatically to in-process work (the in-memory worker, the response
/// handler) via <see cref="System.Threading.ExecutionContext"/>; to survive the serialized hops
/// (lost-subscriber recovery, broker-backed workers) it needs <see cref="SampleTracePropagator"/>.
/// </summary>
public static class SampleTraceContext
{
    private static readonly AsyncLocal<string?> _traceId = new();

    public static string? Current => _traceId.Value;

    public static void Set(string traceId) => _traceId.Value = traceId;

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
/// Sample <see cref="IAsyncResponseContextPropagator"/> that carries the ambient trace id across
/// the serialization boundary into worker jobs and lost-subscriber recovery callbacks. A real one
/// would also restore an <c>ILogger.BeginScope</c> in <see cref="Restore"/> so emitted logs carry
/// the trace id — that is what the returned <see cref="IDisposable"/> is for.
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
/// namespacing its own carrier key. Like the trace id, it flows in-process via
/// <see cref="System.Threading.ExecutionContext"/> and across serialized hops via
/// <see cref="SampleTenantPropagator"/>.
/// </summary>
public static class SampleTenantContext
{
    private static readonly AsyncLocal<string?> _tenant = new();

    public static string? Current => _tenant.Value;

    public static void Set(string tenant) => _tenant.Value = tenant;

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
