using AsyncResponse.Internal;
using Microsoft.Extensions.Options;
using System.Diagnostics;
using System.Text.Json;

namespace AsyncResponse.Transports.SqlServer;

/// <summary>Publishes <see cref="WorkerJobEnvelope"/> messages to the SQL Server worker queue.</summary>
public sealed class SqlServerWorkerTransport : IWorkerTransport, IDelayedWorkerTransport
{
    private readonly SqlServerAsyncResponseTransportOptions _options;
    private readonly SqlServerTransportStore _store;

    /// <summary>Creates a SQL Server worker transport over the configured connection string.</summary>
    public SqlServerWorkerTransport(IOptions<SqlServerAsyncResponseTransportOptions> options)
        : this(options, new SqlServerTransportStore(options))
    {
    }

    internal SqlServerWorkerTransport(
        IOptions<SqlServerAsyncResponseTransportOptions> options,
        SqlServerTransportStore store)
    {
        _options = options.Value;
        SqlServerTransportOptionsValidator.ValidateCommon(_options);
        _store = store;
    }

    /// <inheritdoc />
    public Task PublishAsync(WorkerJobEnvelope job, CancellationToken cancellationToken = default)
        => PublishCoreAsync(job, delay: null, cancellationToken);

    /// <inheritdoc cref="IDelayedWorkerTransport.MaxPublishDelay"/>
    /// <remarks>The due time is a database timestamp; no per-hop cap applies.</remarks>
    public TimeSpan MaxPublishDelay => AsyncResponseChannelOptions.MaxPersistenceTtl;

    /// <inheritdoc cref="IDelayedWorkerTransport.PublishAsync(WorkerJobEnvelope, TimeSpan, CancellationToken)"/>
    public Task PublishAsync(WorkerJobEnvelope job, TimeSpan delay, CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfGreaterThan(delay, MaxPublishDelay);
        return PublishCoreAsync(job, delay > TimeSpan.Zero ? delay : null, cancellationToken);
    }

    private async Task PublishCoreAsync(WorkerJobEnvelope job, TimeSpan? delay, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(job);

        using var activity = AsyncResponseDiagnostics.StartActivity(
            "asyncresponse.worker.publish",
            ActivityKind.Producer,
            job.CorrelationId);
        activity?.SetTag("asyncresponse.transport", "sqlserver");
        activity?.SetTag("messaging.system", "sqlserver");
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
            if (delay is { } delayTag)
                activity?.SetTag("asyncresponse.worker.delay_seconds", delayTag.TotalSeconds);
            await SqlServerTransportRetry.ExecuteAsync(
                async token =>
                {
                    await _store.PublishAsync(messageId, _options.WorkerQueue, payload, headers, token, delay).ConfigureAwait(false);
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

internal static class SqlServerTransportRetry
{
    public static Task<T> ExecuteAsync<T>(
        Func<CancellationToken, Task<T>> action,
        int maxAttempts,
        TimeSpan baseDelay,
        TimeSpan maxDelay,
        CancellationToken cancellationToken)
        => AsyncResponseRetry.ExecuteAsync(action, IsTransient, maxAttempts, baseDelay, maxDelay, cancellationToken);

    public static bool IsTransient(Exception exception) => SqlServerTransientFaults.IsTransient(exception);
}
