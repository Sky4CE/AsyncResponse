namespace AsyncResponse;

/// <summary>
/// Common options for durable flows (<see cref="IDurableFlows"/>), configured on the selected
/// <c>With*DurableFlows(...)</c> registration. Provider option types derive from this class so
/// flow-engine and state-store settings live in one configuration block.
/// </summary>
public class DurableFlowOptions
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

    /// <summary>
    /// Distributed execution-lease duration. A worker renews the lease while flow code is running;
    /// another replica may take over after this interval if the worker disappears. Default: 1 minute.
    /// </summary>
    public TimeSpan ExecutionLeaseDuration { get; set; } = TimeSpan.FromMinutes(1);

    /// <summary>
    /// How often an active execution renews its lease. Must be shorter than
    /// <see cref="ExecutionLeaseDuration"/>. Default: 20 seconds.
    /// </summary>
    public TimeSpan ExecutionLeaseRenewInterval { get; set; } = TimeSpan.FromSeconds(20);

    /// <summary>
    /// Minimum interval between persistence writes made only by <c>ReportProgressAsync</c>.
    /// Reports inside the interval update the in-memory flow state and are coalesced into the next
    /// checkpoint or flow outcome. Set to zero to persist every report. Default: 1 second.
    /// </summary>
    public TimeSpan ProgressPersistenceInterval { get; set; } = TimeSpan.FromSeconds(1);
}
