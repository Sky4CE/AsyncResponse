namespace AsyncResponse;

/// <summary>
/// Options for the transport-agnostic, in-memory AsyncResponse implementation registered by
/// <c>AddAsyncResponse()</c>.
/// </summary>
public sealed class AsyncResponseOptions
{
    /// <summary>
    /// How long process-local recovery state lives. This is not durable persistence: entries are
    /// lost when the process exits. Default: 30 minutes.
    /// </summary>
    public TimeSpan RecoveryStateExpiry { get; set; } = TimeSpan.FromMinutes(30);

    /// <summary>
    /// Default timeout applied to waiters that do not specify <c>WithTimeout</c>. When
    /// <c>null</c> (the default), <see cref="RecoveryStateExpiry"/> is used.
    /// </summary>
    public TimeSpan? DefaultTimeout { get; set; }
}
