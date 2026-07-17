using Microsoft.Extensions.Options;
using MongoDB.Driver;
using System.Diagnostics;
using System.Text.Json;

namespace AsyncResponse.Transports.MongoDB;

/// <summary>Publishes <see cref="WorkerJobEnvelope"/> messages to the MongoDB worker queue.</summary>
public sealed class MongoDbWorkerTransport : IWorkerTransport
{
    private readonly MongoDbAsyncResponseTransportOptions _options;
    private readonly MongoDbTransportStore _store;

    /// <summary>Creates a MongoDB worker transport over the host's shared database.</summary>
    public MongoDbWorkerTransport(
        IOptions<MongoDbAsyncResponseTransportOptions> options,
        IMongoDatabase database)
        : this(options, new MongoDbTransportStore(database, options))
    {
    }

    internal MongoDbWorkerTransport(
        IOptions<MongoDbAsyncResponseTransportOptions> options,
        MongoDbTransportStore store)
    {
        _options = options.Value;
        MongoDbTransportOptionsValidator.ValidateCommon(_options);
        _store = store;
    }

    /// <inheritdoc />
    public async Task PublishAsync(WorkerJobEnvelope job, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(job);

        using var activity = AsyncResponseDiagnostics.StartActivity(
            "asyncresponse.worker.publish",
            ActivityKind.Producer,
            job.CorrelationId);
        activity?.SetTag("asyncresponse.transport", "mongodb");
        activity?.SetTag("messaging.system", "mongodb");
        activity?.SetTag("messaging.destination.name", _options.WorkerQueue);
        AsyncResponseDiagnostics.SetReplyTarget(activity, job.ReplyTarget);
        AsyncResponseDiagnostics.SetWorker(activity, job.Call);

        try
        {
            var headers = string.IsNullOrWhiteSpace(job.CorrelationId)
                ? null
                : new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
                {
                    [_options.CorrelationIdHeader] = job.CorrelationId!
                };

            var payload = AsyncResponseJson.Serialize(job);
            // Stable id outside the retry loop so a retried publish is idempotent rather than enqueuing
            // the same worker job twice.
            var messageId = Guid.NewGuid();
            await MongoDbTransportRetry.ExecuteAsync(
                async token =>
                {
                    await _store.PublishAsync(messageId, _options.WorkerQueue, payload, headers, token).ConfigureAwait(false);
                    return true;
                },
                _options.PublishMaxAttempts,
                _options.PublishRetryBaseDelay,
                _options.PublishRetryMaxDelay,
                cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            AsyncResponseDiagnostics.SetError(activity, ex);
            throw;
        }
    }
}

internal static class MongoDbTransportRetry
{
    public static Task<T> ExecuteAsync<T>(
        Func<CancellationToken, Task<T>> action,
        int maxAttempts,
        TimeSpan baseDelay,
        TimeSpan maxDelay,
        CancellationToken cancellationToken)
        => AsyncResponseRetry.ExecuteAsync(action, IsTransient, maxAttempts, baseDelay, maxDelay, cancellationToken);

    public static bool IsTransient(Exception exception)
        => exception is not OperationCanceledException
           && (exception is MongoConnectionException
               or MongoNotPrimaryException
               or MongoNodeIsRecoveringException
               or MongoExecutionTimeoutException
               or TimeoutException
               || (exception is MongoException mongoException && mongoException.HasErrorLabel("RetryableWriteError")));
}
