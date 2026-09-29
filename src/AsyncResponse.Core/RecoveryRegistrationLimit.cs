namespace AsyncResponse;

/// <summary>
/// The bound on live recovery registrations under one correlation id, for the stores that keep
/// every registration of an id in ONE stored value (the Redis and NATS channels). Each new waiter
/// reads, parses and rewrites that whole value, so registering N waiters on one id writes
/// 1 + 2 + … + N registrations — quadratic in the fan-out — and each rewrite nears the server's
/// value-size limit (1 MB by default on NATS). The database channels store one row per
/// registration and have no such bound.
/// </summary>
internal static class RecoveryRegistrationLimit
{
    /// <summary>
    /// The default bound, set from measurement (RecoveryRegistrationGrowthBenchmarks, with a
    /// registration of about 750 bytes — both callbacks, one argument, a trace context): 64 waiters
    /// on one id write about 1.5 MB in total, end with a 47 KB value, and allocate about 8 MB with a
    /// gen-2 collection every few fan-outs; 256 write about 24 MB and allocate about 124 MB, with
    /// eleven gen-2 collections per fan-out, since each rewrite's strings then sit on the
    /// large-object heap. The 1 MB NATS payload leaves room for registrations of up to about 16 KB
    /// each at this bound.
    /// </summary>
    public const int Default = 64;

    /// <summary>The option's name, as it appears on both providers' options.</summary>
    public const string OptionName = "MaxRecoveryRegistrationsPerCorrelationId";

    /// <summary>Rejects a bound below one at startup: with it, no recoverable waiter could ever register.</summary>
    public static void Validate(int value, string optionsName)
    {
        if (value < 1)
        {
            throw new InvalidOperationException(
                $"{optionsName}.{OptionName} must be at least 1; it is {value}.");
        }
    }

    /// <summary>
    /// The failure a save reports when the id already holds <paramref name="existing"/> live
    /// registrations. It reaches the caller from waiter creation, before the trigger runs, so no
    /// work was dispatched for the waiter that could not register.
    /// </summary>
    public static InvalidOperationException Exceeded(string correlationId, int existing, string optionsName)
        => new(
            $"Correlation id '{correlationId}' already has {existing} live recovery registration(s), the limit set by " +
            $"{optionsName}.{OptionName}. Every registration under one correlation id is stored in one value that each " +
            "new waiter rewrites whole, so the cost of registering grows with the square of the fan-out. Raise the limit, " +
            "or use a database channel (PostgreSQL, SQL Server, MongoDB), which stores one row per registration, for wider fan-out.");
}
