namespace AsyncResponse;

/// <summary>
/// The ledger retention floor (<see cref="FlowState.RetainUntilUtc"/>) at the write sites.
/// <para>
/// A park longer than the ordinary idle <c>StateExpiry</c> — a timer sleep, an awaited step's
/// window, a child flow's own park — needs the parked run's ledger AND every ancestor waiting on
/// it to outlive the wait. The TTL stamped by the park's own save covers that only until the
/// next write: every store recomputes expiry as "now + ttl", and the writers that can race a
/// park (an ancestor's replay re-parking on a stale child snapshot, an executor's per-attempt
/// save, a recovery or operator mutation) know nothing about the wait and stamp the plain
/// <c>StateExpiry</c>. The floor rides in the ledger itself, so whoever writes the ledger next
/// carries it forward: the TTL a write stamps is raised to reach the floor. That is what makes
/// a descendant's extension of an ancestor durable across the ancestor's own checkpoints, and
/// what lets the extension prove — by re-reading the ancestor — that a concurrent write which
/// beat its compare-and-swap left adequate retention behind.
/// </para>
/// <para>
/// Terminal runs ignore the floor: they have no wait in progress, and a failed run should not
/// be retained for the length of the sleep it never finished.
/// </para>
/// </summary>
internal static class FlowStateRetention
{
    /// <summary>
    /// The TTL a write must stamp for <paramref name="state"/>: <paramref name="requested"/>,
    /// raised to reach the state's retention floor when the run is live and the floor is further
    /// out. Saturated at the persistence ceiling (clock skew between replicas could otherwise push
    /// a floor stamped elsewhere a hair past it).
    /// </summary>
    public static TimeSpan EffectiveTtl(FlowState state, TimeSpan requested, DateTime nowUtc)
    {
        if (state.RetainUntilUtc is not { } floor || IsTerminal(state.Status))
            return requested;

        var needed = floor - nowUtc;
        if (needed <= requested)
            return requested;

        return needed > AsyncResponseChannelOptions.MaxPersistenceTtl
            ? AsyncResponseChannelOptions.MaxPersistenceTtl
            : needed;
    }

    /// <summary>
    /// Raises the floor of <paramref name="state"/> to <paramref name="nowUtc"/> +
    /// <paramref name="ttl"/> when that is further out than the current one (never lowers it) and
    /// returns the instant the floor now sits at.
    /// </summary>
    public static DateTime RaiseFloor(FlowState state, DateTime nowUtc, TimeSpan ttl)
    {
        var until = FloorAt(nowUtc, ttl);
        if (state.RetainUntilUtc is not { } floor || until > floor)
            state.RetainUntilUtc = until;

        return state.RetainUntilUtc!.Value;
    }

    /// <summary>The instant a floor stamped now for <paramref name="ttl"/> sits at (saturating).</summary>
    public static DateTime FloorAt(DateTime nowUtc, TimeSpan ttl) => AddSaturating(nowUtc, ttl);

    /// <summary>Whether the floor of <paramref name="state"/> already reaches <paramref name="until"/>.</summary>
    public static bool Covers(FlowState state, DateTime until)
        => state.RetainUntilUtc is { } floor && floor >= until;

    private static bool IsTerminal(FlowRunStatus status)
        => status is FlowRunStatus.Succeeded or FlowRunStatus.Failed;

    private static DateTime AddSaturating(DateTime instant, TimeSpan ttl)
        => ttl > DateTime.MaxValue - instant ? DateTime.MaxValue : instant + ttl;
}
