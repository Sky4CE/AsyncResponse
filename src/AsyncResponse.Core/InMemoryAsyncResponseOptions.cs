namespace AsyncResponse;

/// <summary>
/// Options for the process-local response channel registered by
/// <c>AddAsyncResponse().WithInMemoryChannel()</c>. Waiters, subscriptions, and recovery state
/// all live in memory and disappear when the process exits.
/// </summary>
public sealed class InMemoryAsyncResponseOptions : AsyncResponseChannelOptions
{
    /// <summary>Runs the InMemoryAsyncResponseOptions operation.</summary>
    public InMemoryAsyncResponseOptions()
    {
        // Process-local recovery state is not durable (entries are lost on exit), so a short default
        // keeps stale state from lingering — unlike the durable channels' 7-day default.
        RecoveryStateExpiry = TimeSpan.FromMinutes(30);
    }
}
