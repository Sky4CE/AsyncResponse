namespace AsyncResponse;

/// <summary>
/// Options for the process-local response channel registered by
/// <c>AddAsyncResponse().WithInMemoryChannel()</c>. Waiters, subscriptions, and recovery state
/// all live in memory and disappear when the process exits.
/// </summary>
public sealed class InMemoryAsyncResponseOptions
{
    /// <summary>
    /// How long process-local recovery state lives. This is not durable persistence: entries are
    /// lost when the process exits. Default: 30 minutes.
    /// </summary>
    public TimeSpan RecoveryStateExpiry { get; set; } = TimeSpan.FromMinutes(30);

    /// <summary>
    /// Default timeout applied to waiters that do not specify <c>WithTimeout</c>. When
    /// <c>null</c> (the default), <see cref="RecoveryStateExpiry"/> is used — waits are never
    /// infinite.
    /// </summary>
    public TimeSpan? DefaultTimeout { get; set; }
}
