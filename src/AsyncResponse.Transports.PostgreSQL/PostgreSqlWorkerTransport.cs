using Microsoft.Extensions.Options;
using Npgsql;
using System.Diagnostics;
using System.Text.Json;

namespace AsyncResponse.Transports.PostgreSQL;

/// <summary>Publishes <see cref="WorkerJobEnvelope"/> messages to the PostgreSQL worker queue.</summary>
public sealed class PostgreSqlWorkerTransport : IWorkerTransport
{
    private readonly PostgreSqlAsyncResponseTransportOptions _options;
    private readonly PostgreSqlTransportStore _store;

    /// <summary>Creates a PostgreSQL worker transport over the host's shared data source.</summary>
    public PostgreSqlWorkerTransport(
        IOptions<PostgreSqlAsyncResponseTransportOptions> options,
        NpgsqlDataSource dataSource)
        : this(options, new PostgreSqlTransportStore(dataSource, options))
    {
    }

    internal PostgreSqlWorkerTransport(
        IOptions<PostgreSqlAsyncResponseTransportOptions> options,
        PostgreSqlTransportStore store)
    {
        _options = options.Value;
        PostgreSqlTransportOptionsValidator.ValidateCommon(_options);
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
        activity?.SetTag("asyncresponse.transport", "postgresql");
        activity?.SetTag("messaging.system", "postgresql");
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

            var payload = JsonSerializer.Serialize(job);
            await PostgreSqlTransportRetry.ExecuteAsync(
                async token =>
                {
                    await _store.PublishAsync(_options.WorkerQueue, payload, headers, token).ConfigureAwait(false);
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

internal static class PostgreSqlTransportRetry
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
           && (exception is NpgsqlException { IsTransient: true } || exception is TimeoutException);
}
