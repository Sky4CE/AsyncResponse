using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Options;
using System.Diagnostics;
using System.Text.Json;

namespace AsyncResponse.Transports.SqlServer;

/// <summary>Publishes <see cref="WorkerJobEnvelope"/> messages to the SQL Server worker queue.</summary>
public sealed class SqlServerWorkerTransport : IWorkerTransport
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
    public async Task PublishAsync(WorkerJobEnvelope job, CancellationToken cancellationToken = default)
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
            await SqlServerTransportRetry.ExecuteAsync(
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

internal static class SqlServerTransportRetry
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
           && (exception is SqlException sqlException && SqlServerTransientFaults.IsTransient(sqlException)
               || exception is TimeoutException);
}

/// <summary>
/// Classifies SQL Server errors worth retrying. <see cref="SqlException"/> exposes no public
/// transient flag, so this mirrors the error numbers Microsoft's own retry guidance and the
/// SqlClient configurable-retry defaults treat as transient, plus severity ≥ 20 (broken connection).
/// </summary>
internal static class SqlServerTransientFaults
{
    private static readonly HashSet<int> TransientErrorNumbers =
    [
        -2,    // client-side command timeout
        20,    // instance does not support encryption
        64,    // connection lost during login
        121,   // transport semaphore timeout
        233,   // no process on the other end of the pipe
        997,   // overlapped I/O in progress
        1204,  // lock resources exhausted
        1205,  // deadlock victim
        1222,  // lock request timeout
        4060,  // database unavailable
        4221,  // readable secondary timeout
        10053, // transport-level connection abort
        10054, // transport-level connection reset
        10060, // network unreachable / connect timeout
        10928, // Azure SQL resource limit reached
        10929, // Azure SQL minimum guarantee exceeded
        40143, // Azure SQL connection failure
        40197, // Azure SQL service processing error
        40501, // Azure SQL service busy
        40540, // Azure SQL service unavailable
        40613, // Azure SQL database unavailable
        49918, // cannot process request, not enough resources
        49919, // cannot process create/update request
        49920  // cannot process request, too many operations
    ];

    public static bool IsTransient(SqlException exception)
    {
        if (exception.Class >= 20)
            return true;

        foreach (SqlError error in exception.Errors)
        {
            if (TransientErrorNumbers.Contains(error.Number))
                return true;
        }

        return TransientErrorNumbers.Contains(exception.Number);
    }
}
