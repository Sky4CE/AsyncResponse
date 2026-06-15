using System.Text.Json;

namespace AsyncResponse.Sample;

public enum RemoteBehavior
{
    /// <summary>Report progress, then complete successfully.</summary>
    Succeed,

    /// <summary>Report progress, then fail with a domain error (Status = Failed).</summary>
    FailDomain,

    /// <summary>Take a long time before completing — used to demonstrate timeouts.</summary>
    Slow
}

/// <summary>
/// Plays the role of a remote system <em>plus</em> the message broker bridge: it receives a
/// "request", works on it, and delivers raw-JSON responses into
/// <see cref="IAsyncResponseIngress.HandleResponseMessageAsync"/> — exactly the integration
/// point a real broker subscription (Pub/Sub, RabbitMQ, Kafka, webhook) would call.
/// </summary>
public sealed class RemoteWorkSimulator(IAsyncResponseIngress _ingress, ILogger<RemoteWorkSimulator> _logger)
{
    public void Start(string correlationId, RemoteBehavior behavior)
    {
        _logger.LogInformation("REMOTE: accepted request {CorrelationId} ({Behavior}).", correlationId, behavior);

        _ = Task.Run(async () =>
        {
            try
            {
                // Intermediate progress message.
                await Task.Delay(TimeSpan.FromMilliseconds(400));
                await DeliverAsync(correlationId, new OperationResult
                {
                    Status = OperationStatus.Running,
                    Message = "remote operation in progress…"
                });

                // Terminal message.
                await Task.Delay(behavior == RemoteBehavior.Slow ? TimeSpan.FromSeconds(15) : TimeSpan.FromMilliseconds(600));
                await DeliverAsync(correlationId, new OperationResult
                {
                    Status = behavior == RemoteBehavior.FailDomain ? OperationStatus.Failed : OperationStatus.Completed,
                    Message = behavior == RemoteBehavior.FailDomain ? "remote validation failed" : "remote operation completed"
                });
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "REMOTE: simulation for {CorrelationId} crashed.", correlationId);
            }
        });
    }

    /// <summary>Delivers one raw-JSON terminal/progress message, like a broker consumer would.</summary>
    public Task DeliverAsync(string correlationId, OperationResult result)
    {
        var json = JsonSerializer.Serialize(result);
        _logger.LogInformation("BROKER: delivering message for {CorrelationId}: {Json}", correlationId, json);
        return _ingress.HandleResponseMessageAsync(json, correlationId);
    }
}
