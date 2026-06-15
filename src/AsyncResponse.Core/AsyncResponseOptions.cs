namespace AsyncResponse;

/// <summary>
/// Engine-level options for AsyncResponse, configured by <c>AddAsyncResponse()</c>. These apply
/// regardless of which channel or transport is registered. Channel-specific settings (per-waiter
/// timeouts, recovery-state expiry, key prefixes) live on the channel's own options — see
/// <see cref="InMemoryAsyncResponseOptions"/> and the Redis package's <c>RedisAsyncResponseOptions</c>.
/// </summary>
public sealed class AsyncResponseOptions
{
    /// <summary>
    /// Settings for the built-in recovery-state watchdog, which runs by default and periodically
    /// scans the configured recovery store for flows that look stuck (persisted recovery state
    /// with no live waiter). Set <see cref="AsyncResponseWatchdogOptions.Enabled"/> to
    /// <c>false</c> to turn it off — for example in all but one host when several hosts share one
    /// durable store, to avoid duplicate scans and warnings.
    /// </summary>
    public AsyncResponseWatchdogOptions Watchdog { get; set; } = new();
}
