namespace AsyncResponse;

/// <summary>
/// Common options for durable flows (<see cref="IDurableFlows"/>), configured on the selected
/// <c>With*DurableFlows(...)</c> registration. Provider option types derive from this class so
/// flow-engine and state-store settings live in one configuration block.
/// </summary>
public class DurableFlowOptions
{
    /// <summary>
    /// The portable flow-id length contract in UTF-16 characters: the longest final flow id every
    /// bundled state store accepts (SQL Server, MySQL, Oracle, and EF Core declare <c>flow_id</c>
    /// as a 400-character column). Every id is validated when its state is created — root ids
    /// passed to <see cref="IDurableFlows.StartAsync{TFlow,TInput}"/>, composed child ids
    /// (<c>{parentId}:{stepName}</c>), and scheduled occurrence ids
    /// (<c>sched:{name}:{timestamp}</c>, validated at registration) — so an id cannot work on one
    /// store and fail on another, or work as a root and fail once a suffix is appended.
    /// <para>
    /// Length is only one of the three portability rules; see <see cref="MaxFlowIdBytes"/> and the
    /// character restrictions documented with it.
    /// </para>
    /// </summary>
    public const int MaxFlowIdLength = 400;

    /// <summary>
    /// The portable flow-id size contract in UTF-8 <em>bytes</em> — the Cosmos DB id limit, which
    /// a 400-character id cannot be assumed to satisfy: characters outside the Basic Latin range
    /// cost two to four bytes each, so 400 CJK characters are 1200 bytes.
    /// <para>
    /// Ids must also avoid <c>/</c>, <c>\</c>, <c>?</c> and <c>#</c> (Cosmos rejects them outright)
    /// and control characters. Ids are compared ORDINALLY everywhere, and the relational stores
    /// pin a binary collation on the column so the database agrees — two ids differing only in
    /// case are two different flows.
    /// </para>
    /// </summary>
    public const int MaxFlowIdBytes = 1023;

    /// <summary>
    /// How long persisted flow state lives; the TTL is refreshed on every checkpoint, so it bounds
    /// the *idle* time of a run, not its total duration. Must comfortably exceed the longest gap
    /// between checkpoints (typically the longest awaited step) — the default is deliberately
    /// double the 7-day default step-timeout chain (<see cref="DefaultStepTimeout"/> →
    /// channel <c>DefaultTimeout</c> → <c>RecoveryStateExpiry</c>), so a step that waits out the
    /// full default timeout still faults and checkpoints before its ledger can expire, instead of
    /// racing it. Default: 14 days.
    /// </summary>
    public TimeSpan StateExpiry { get; set; } = TimeSpan.FromDays(14);

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

    /// <summary>
    /// Timer remainders at or under this threshold wait in process (under the execution lease)
    /// instead of suspending the run for a delayed wake-up job — a broker round-trip for a
    /// two-second sleep costs more than it frees. Longer remainders suspend when the registered
    /// transport supports native delayed delivery (<see cref="IDelayedWorkerTransport"/>); on
    /// transports without it every timer waits in process regardless of this value. Zero always
    /// prefers suspension. Default: 10 seconds.
    /// </summary>
    public TimeSpan TimerInProcessThreshold { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Ledger size, in bytes (estimated from the serialized input, step results, values, and
    /// context), past which the executor logs a warning naming the flow — once when the threshold
    /// is first crossed and again at each doubling, so a long run logs a handful of times, not once
    /// per step. Every checkpoint rewrites the <em>whole</em> ledger, so persistence cost grows
    /// with each completed step: a run of N steps with similar result sizes serializes about N²/2
    /// step-results over its lifetime, and the store's <c>MaxStateBytes</c> (or the provider's
    /// item cap) is the hard limit. The warning is the early signal to keep step results small
    /// (persist large data yourself and pass references) or to partition a long history into child
    /// flows. <c>null</c> disables it. Default: 512 KiB — under the smallest bundled hard cap
    /// (DynamoDB's 350 KB item, whose store defaults <c>MaxStateBytes</c> to 350 000) users should
    /// lower it accordingly.
    /// </summary>
    public long? LedgerSizeWarningBytes { get; set; } = 512 * 1024;

    /// <summary>
    /// Accepts the risk of running the worker subscriber in early ACK (<c>AckAfterEnqueue</c>)
    /// while durable flows are registered, suppressing the startup error. Durable-flow wake-ups
    /// ride the worker queue and rely on broker redelivery for crash recovery; with early ACK, a
    /// process crash after the ACK but before execution strands the run as <c>Running</c> with no
    /// lease and no queued job, and only an operator <c>ResumeAsync(flowId)</c> can revive it.
    /// Leave <c>false</c> (the default) unless that loss mode is acceptable. Default: false.
    /// </summary>
    public bool AllowEarlyAckWorkerSubscriber { get; set; }
}
