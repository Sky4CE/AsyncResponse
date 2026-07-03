namespace AsyncResponse;

/// <summary>Options for durable flows (<see cref="IDurableFlows"/>), set via <c>AddAsyncResponse(o => o.DurableFlows...)</c>.</summary>
public sealed class DurableFlowOptions
{
    /// <summary>
    /// How long persisted flow state lives; the TTL is refreshed on every checkpoint, so it bounds
    /// the *idle* time of a run, not its total duration. Must comfortably exceed the longest gap
    /// between checkpoints (typically the longest awaited step). Default: 7 days.
    /// </summary>
    public TimeSpan StateExpiry { get; set; } = TimeSpan.FromDays(7);

    /// <summary>
    /// Default timeout for awaited steps that don't pass one explicitly. <c>null</c> uses the
    /// configured channel's default wait timeout.
    /// </summary>
    public TimeSpan? DefaultStepTimeout { get; set; }
}
