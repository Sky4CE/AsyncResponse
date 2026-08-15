using Npgsql;

namespace AsyncResponse.Internal;

/// <summary>
/// Classifies PostgreSQL errors worth retrying. Npgsql already curates the set through
/// <see cref="NpgsqlException.IsTransient"/> (connection breaks, serialization/deadlock SQLSTATEs,
/// admin shutdowns), so this only adds the driver-independent client-side timeout and excludes
/// cancellation. Source-linked into the PostgreSQL channel and transport packages (separate
/// packages cannot share compiled code), so both retry the same set.
/// </summary>
internal static class PostgreSqlTransientFaults
{
    /// <summary>
    /// The predicate the retry loops pass to <c>AsyncResponseRetry</c>. Cancellation is excluded
    /// first: a token trip is the caller's decision, never a fault to retry.
    /// </summary>
    public static bool IsTransient(Exception exception)
        => exception is not OperationCanceledException
           && (exception is NpgsqlException { IsTransient: true } || exception is TimeoutException);
}
