namespace AsyncResponse;

/// <summary>
/// Options for the process-local response channel registered by
/// <c>AddAsyncResponse().WithInMemoryChannel()</c>. Waiters, subscriptions, and recovery state
/// all live in memory and disappear when the process exits.
/// </summary>
// Derives from the DURABLE options despite holding nothing durable: the wire channels serialize
// failures (message + capped stack trace, type erased), and the in-memory channel reproduces that
// so tests exercise the same failure shape production sees — including these two policy knobs.
public sealed class InMemoryAsyncResponseOptions : DurableAsyncResponseChannelOptions
{
    /// <summary>Runs the InMemoryAsyncResponseOptions operation.</summary>
    public InMemoryAsyncResponseOptions()
    {
        // Process-local recovery state is not durable (entries are lost on exit), so a short default
        // keeps stale state from lingering — unlike the durable channels' 7-day default.
        RecoveryStateExpiry = TimeSpan.FromMinutes(30);
    }

    /// <summary>
    /// Validates the options, throwing an actionable <see cref="InvalidOperationException"/> on
    /// misconfiguration — the same guard set every durable channel applies, so a configuration
    /// that passes the in-memory harness cannot fail host startup on a real channel.
    /// </summary>
    internal void Validate()
    {
        ValidateShared(nameof(InMemoryAsyncResponseOptions));
        // RemoteStackTrace.Cap treats a non-positive cap as "no cap", so without this the bound
        // on remote traces was silently disabled here while all five wire channels reject it.
        if (MaxRemoteStackTraceLength < 0)
            throw new InvalidOperationException($"{nameof(InMemoryAsyncResponseOptions)}.{nameof(MaxRemoteStackTraceLength)} must not be negative.");
    }
}
