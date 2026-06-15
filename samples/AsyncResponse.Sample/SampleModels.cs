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
        _logger.LogInformation("WORKER: processing order {OrderId} (correlationId: {CorrelationId})…",
            orderId, AsyncResponseContext.CorrelationId);
        await Task.Delay(1_000);
        _logger.LogInformation("WORKER: order {OrderId} processed.", orderId);
    }

    public Task ResumeFlowAsync(string flowName, OperationResult payload, string correlationId)
    {
        _logger.LogWarning(
            "RECOVERY (resume): flow '{FlowName}' got a {Status} response after its waiter was lost " +
            "(correlationId: {CorrelationId}, message: {Message}). A real flow would resume or re-register here.",
            flowName, payload.Status, correlationId, payload.Message);
        return Task.CompletedTask;
    }

    public Task FailFlowAsync(Exception exception, string correlationId)
    {
        if (exception is AsyncResponseDomainFailureException domainFailure)
        {
            _logger.LogError(
                "RECOVERY (failure): correlationId {CorrelationId} reported domain outcome {Outcome} after its waiter was lost. Payload: {Payload}. A real flow would mark itself failed (retriable) here.",
                correlationId, domainFailure.Outcome, domainFailure.PayloadJson);
        }
        else
        {
            _logger.LogError(exception,
                "RECOVERY (failure): correlationId {CorrelationId} failed technically after its waiter was lost.",
                correlationId);
        }

        return Task.CompletedTask;
    }
}
