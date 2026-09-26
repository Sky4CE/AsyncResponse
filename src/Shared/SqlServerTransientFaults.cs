using Microsoft.Data.SqlClient;

namespace AsyncResponse.Internal;

/// <summary>
/// Classifies SQL Server errors worth retrying. <see cref="SqlException"/> exposes no public
/// transient flag, so this mirrors the error numbers Microsoft's own retry guidance and the
/// SqlClient configurable-retry defaults treat as transient, plus severity ≥ 20 (broken connection).
/// Source-linked into the SQL Server channel and transport packages (separate packages cannot share
/// compiled code), so both retry the same set: a number curated in one copy and not the other would
/// make the same fault permanent on one side of the same deployment.
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

    /// <summary>
    /// The predicate the retry loops pass to <c>AsyncResponseRetry</c>. Cancellation is excluded
    /// first: a token trip is the caller's decision, never a fault to retry.
    /// </summary>
    public static bool IsTransient(Exception exception)
        => exception is not OperationCanceledException
           && (exception is SqlException sqlException && IsTransient(sqlException)
               || exception is TimeoutException);

    /// <summary>
    /// Opens <paramref name="connection"/>, rethrowing SqlClient's pool-exhaustion failure as a
    /// (transient) <see cref="TimeoutException"/>. SqlClient reports "Timeout expired … obtaining a
    /// connection from the pool" as a plain <see cref="InvalidOperationException"/> — the same type
    /// as every usage error, so <see cref="IsTransient(Exception)"/> cannot accept it — and a
    /// sustained exhaustion then never counted toward the retry policies or the database channel's
    /// outage breaker: each sweep waited out the connect timeout once per correlation id, adding
    /// eight more contenders to the exhausted pool each time. Npgsql raises the same condition as a
    /// transient <c>NpgsqlException</c> over a <see cref="TimeoutException"/>.
    /// </summary>
    public static async Task OpenAsync(SqlConnection connection, CancellationToken cancellationToken)
    {
        var startedAt = System.Diagnostics.Stopwatch.GetTimestamp();
        try
        {
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (InvalidOperationException ex) when (IsPoolTimeout(ex, connection.ConnectionString, System.Diagnostics.Stopwatch.GetElapsedTime(startedAt)))
        {
            throw new TimeoutException(ex.Message, ex);
        }
    }

    /// <summary>
    /// Whether <paramref name="exception"/>, thrown by an open that took <paramref name="elapsed"/>,
    /// is SqlClient's pool-wait timeout. Judged by the open having waited out the whole connect
    /// timeout with pooling on, never by the message: SqlClient's resource text is localized. Every
    /// other <see cref="InvalidOperationException"/> an open throws (a missing connection string, a
    /// connection already open, a disposed one) is immediate.
    /// </summary>
    internal static bool IsPoolTimeout(InvalidOperationException exception, string connectionString, TimeSpan elapsed)
    {
        if (exception is ObjectDisposedException)
            return false;

        try
        {
            var builder = new SqlConnectionStringBuilder(connectionString);
            // Connect Timeout = 0 waits for good, so nothing times out; the timers behind the pool
            // wait can fire a hair before a stopwatch started just ahead of the open agrees.
            return builder.Pooling
                   && builder.ConnectTimeout > 0
                   && elapsed + PoolTimeoutTolerance >= TimeSpan.FromSeconds(builder.ConnectTimeout);
        }
        catch (ArgumentException)
        {
            return false;
        }
    }

    private static readonly TimeSpan PoolTimeoutTolerance = TimeSpan.FromMilliseconds(100);

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
