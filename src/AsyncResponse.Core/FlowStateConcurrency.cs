using Microsoft.Extensions.Logging;

namespace AsyncResponse;

/// <summary>Coordinates atomic flow creation, optimistic updates, and one active executor per flow id.</summary>
internal static class FlowStateConcurrency
{
    private const int MaxUpdateAttempts = 8;

    public static Task<bool> TryCreateAsync(
        IFlowStateStore store,
        string flowId,
        FlowState state,
        TimeSpan ttl,
        CancellationToken cancellationToken = default)
    {
        EnsurePortableFlowId(flowId);

        state.Revision = 0;
        return store.TryCreateAsync(flowId, state, ttl, cancellationToken);
    }

    /// <summary>
    /// Throws <see cref="ArgumentException"/> for an id that fails <see cref="FlowIdNotPortable"/>.
    /// Called by every create, and by <c>IDurableFlows.StartAsync</c> BEFORE it publishes the start
    /// job — the publish is the start's commit point, so a job for an id no store would accept must
    /// never leave the process.
    /// </summary>
    internal static void EnsurePortableFlowId(string flowId)
    {
        if (FlowIdNotPortable(flowId) is { } rejection)
            throw new ArgumentException(rejection, nameof(flowId));
    }

    /// <summary>
    /// Whether an existing ledger describes the same start as the requested one: same flow type,
    /// same input type (both by type identity — <see cref="TypeNameIdentity"/> — so the assembly
    /// versions inside a generic name do not count), and semantically identical input. The one idempotency test
    /// for flow ids, shared by the starter (which reports a mismatch to its caller as
    /// <see cref="DurableFlowIdConflictException"/>) and the executor's start target (which drops
    /// the job on a mismatch) so the two can never disagree about what "the same run" means.
    /// <para>
    /// Input is compared by VALUE (<paramref name="inputEquivalent"/>:
    /// <see cref="FlowStateJson.InputEquivalent{TInput}"/> on the starter; on the executor the
    /// registration's round trip, or for a flow executed by reflection the same round trip through
    /// the input type resolved from the ledger once it passed the flow-contract check), as child
    /// starts already are: inputs are written with their nulls and defaults, so a member added to
    /// the input type since the ledger was written made an idempotent re-start — the scheduler's
    /// startup re-drive of a never-executed occurrence, a caller's retry across a deploy — differ in
    /// shape, report a conflict, and drop the published start job with the run stuck behind it. The
    /// JSON shape decides only when no delegate is passed, or the executor's reflection path cannot
    /// resolve the input type or it fails that check.
    /// </para>
    /// </summary>
    internal static bool IsSameStart(
        FlowState existing,
        string? flowTypeName,
        string? inputTypeName,
        string? inputJson,
        Func<string?, string, bool>? inputEquivalent = null)
        => TypeNameIdentity.Same(existing.FlowTypeName, flowTypeName)
            && TypeNameIdentity.Same(existing.InputTypeName, inputTypeName)
            && (inputEquivalent ?? FlowStateJson.JsonEquivalent)(existing.InputJson, inputJson ?? string.Empty);

    /// <summary>
    /// Enforces the portable flow-id contract on every final id at creation — the single door all
    /// creates walk through. Three independent limits, because the stores disagree about what an
    /// id may be, and an id that works on one store and fails on another is not portable:
    /// <list type="bullet">
    /// <item>length in UTF-16 code units, for the 400-unit <c>flow_id</c> columns (SQL Server,
    /// MySQL, Oracle, EF Core);</item>
    /// <item>length in UTF-8 <em>bytes</em>, for Cosmos DB, whose 1023-byte id limit a 400-unit id
    /// exceeds once the characters are non-ASCII (up to three bytes per unit, four for a
    /// surrogate pair);</item>
    /// <item>the characters themselves — Cosmos rejects <c>/</c>, <c>\</c>, <c>?</c> and <c>#</c>
    /// in an id, and control characters break every store's diagnostics;</item>
    /// <item>no surrounding spaces — SQL Server pads the shorter operand of an equality
    /// comparison (binary collations included) and MySQL's <c>utf8mb4_bin</c> is PAD SPACE, so
    /// <c>flow</c> and <c>flow&#160;</c> are ONE key to those databases while the engine treats
    /// them as two runs.</item>
    /// </list>
    /// Case is deliberately NOT folded here: ids are compared ordinally throughout, and the
    /// relational stores pin a binary collation on the column so the database agrees.
    /// Returns the rejection message, or <c>null</c> when the id is portable.
    /// </summary>
    internal static string? FlowIdNotPortable(string flowId)
    {
        // Length-guarded before the [0]/[^1] probe below: this is the single door every create
        // walks through and its contract is to RETURN a rejection, so an empty id must not throw
        // IndexOutOfRangeException out of the very method whose job is to explain bad ids.
        if (flowId.Length == 0)
            return "Flow id is empty. A run needs an id to be addressable by its wake-ups, child flows, and recovery callbacks.";

        if (flowId.Length > DurableFlowOptions.MaxFlowIdLength)
        {
            return $"Flow id '{Excerpt(flowId)}' is {flowId.Length} UTF-16 code units; the portable maximum is " +
                $"{DurableFlowOptions.MaxFlowIdLength} ({nameof(DurableFlowOptions)}.{nameof(DurableFlowOptions.MaxFlowIdLength)} — the flow_id " +
                "column length in the SQL Server, MySQL, Oracle, and EF Core stores). " + BudgetGuidance;
        }

        // Checked BEFORE the byte count, which would otherwise be measured against the U+FFFD an
        // encoder substitutes rather than against the id the caller passed.
        if (PortableText.IndexOfIllFormedUtf16(flowId) is var illFormed and >= 0)
            return PortableText.IllFormedUtf16Rejection("Flow id", Excerpt(flowId), flowId[illFormed], illFormed);

        var utf8Bytes = System.Text.Encoding.UTF8.GetByteCount(flowId);
        if (utf8Bytes > DurableFlowOptions.MaxFlowIdBytes)
        {
            return $"Flow id '{Excerpt(flowId)}' is {utf8Bytes} UTF-8 bytes; the portable maximum is " +
                $"{DurableFlowOptions.MaxFlowIdBytes} ({nameof(DurableFlowOptions)}.{nameof(DurableFlowOptions.MaxFlowIdBytes)} — the Cosmos DB " +
                "id limit). A non-ASCII character costs up to three bytes (four for a surrogate pair), so a count of characters " +
                "does not bound the byte length. " + BudgetGuidance;
        }

        if (flowId[0] == ' ' || flowId[^1] == ' ')
        {
            return $"Flow id '{Excerpt(flowId)}' begins or ends with a space. SQL Server pads the shorter operand of an equality " +
                "comparison — binary collations included — and MySQL's utf8mb4_bin is PAD SPACE, so an id with trailing spaces is " +
                "the SAME key as one without to those stores, while the engine compares them ordinally and treats them as two " +
                "different flows. Trim the id.";
        }

        foreach (var character in flowId)
        {
            if (character is '/' or '\\' or '?' or '#' || char.IsControl(character))
            {
                return $"Flow id '{Excerpt(flowId)}' contains the character '{(char.IsControl(character) ? $"\\u{(int)character:x4}" : character.ToString())}', " +
                    "which is not portable: Cosmos DB rejects '/', '\\', '?' and '#' in an id, and control characters corrupt " +
                    "diagnostics. Use a separator the stores agree on, such as ':' or '-'.";
            }
        }

        return null;
    }

    private const string BudgetGuidance =
        "Budget root ids for growth: child flows append \":{stepName}\" to the parent id, and scheduled flows wrap the schedule " +
        "name as \"sched:{name}:{timestamp}\".";

    // Escaped, not merely truncated: the ids quoted here are rejected because they are malformed,
    // and a start job's id is written by whoever can publish to the worker stream — quoted raw, a
    // CR/LF inside it wrote its own log line (the correlation-id twins were switched in round 42).
    private static string Excerpt(string flowId) => DiagnosticText.EscapedExcerpt(flowId);

    public static async Task<FlowExecutionLease?> TryAcquireExecutionLeaseAsync(
        IFlowStateStore store,
        string flowId,
        DurableFlowOptions options,
        ILogger logger,
        TimeProvider? timeProvider = null,
        CancellationToken cancellationToken = default,
        string? jobTag = null)
    {
        ValidateOptions(options);

        var clock = timeProvider ?? TimeProvider.System;

        // The job driving this execution is recorded IN the lease id, so it lands atomically with
        // the acquire on every store and comes back through ObserveLeaseAsync: a later delivery of
        // that same job can then tell it is contending with its own first delivery (see
        // FlowLeaseContention). Without a job identity this is the plain 32-character id.
        var leaseId = FlowLeaseContention.NewLeaseId(jobTag);

        // Stamp the deadline BEFORE the call, not after it returns. The store starts the lease when
        // it executes the command; every millisecond after that — network latency, a delayed
        // continuation, a GC pause between the response arriving and this line running — is lease
        // time already spent. Anchoring afterwards handed that whole interval back to the client as
        // if it were still owned, so a worker could believe it held a 60s lease 20s past the point
        // another replica was free to take it. Anchoring first is conservative in the safe
        // direction: the client's deadline can only be EARLIER than the server's.
        var deadline = FlowExecutionLease.DeadlineFrom(clock, options.ExecutionLeaseDuration);

        if (!await store.TryAcquireLeaseAsync(
                flowId,
                leaseId,
                options.ExecutionLeaseDuration,
                cancellationToken).ConfigureAwait(false))
            return null;

        // The constructor is throw-free after the option bounds above: it only assigns fields,
        // records the pre-call deadline, and starts the renewal loop (whose first Task.Delay faults
        // the loop task, never the constructor). Were that ever to change, lease expiry is the
        // backstop for the persisted row.
        return new FlowExecutionLease(store, flowId, leaseId, options, logger, clock, deadline);
    }

    public static async Task<bool> MutateAsync(
        IFlowStateStore store,
        string flowId,
        TimeSpan ttl,
        TimeProvider? timeProvider,
        Func<FlowState, bool> mutate,
        CancellationToken cancellationToken = default)
    {
        for (var attempt = 0; attempt < MaxUpdateAttempts; attempt++)
        {
            var state = await store.LoadAsync(flowId, cancellationToken).ConfigureAwait(false);
            if (state is null)
                return false;

            if (!mutate(state))
                return true;

            var expectedRevision = state.Revision;
            state.Revision = checked(expectedRevision + 1);
            var nowUtc = (timeProvider ?? TimeProvider.System).GetUtcNow().UtcDateTime;
            state.UpdatedAtUtc = nowUtc;
            if (await store.TryUpdateAsync(
                    flowId,
                    state,
                    expectedRevision,
                    // A lease-bypassing write (recovery, failure signal, operator) never shrinks a
                    // live run's ledger under a park it knows nothing about.
                    FlowStateRetention.EffectiveTtl(state, ttl, nowUtc),
                    leaseId: null,
                    cancellationToken).ConfigureAwait(false))
                return true;
        }

        throw new InvalidOperationException(
            $"Durable flow '{flowId}' changed repeatedly while applying a recovery update; retry the operation.");
    }

    internal static void ValidateOptions(DurableFlowOptions options)
    {
        // Upper bounds close the "passes validation, throws mid-operation" gap — but only on the
        // knobs that actually reach the failing sink. StateExpiry and ExecutionLeaseDuration become
        // "now + value" stamps (store TTLs and lease deadlines) and never arm a timer themselves,
        // so they get the persistence bound; DefaultStepTimeout and ExecutionLeaseRenewInterval arm
        // BCL timers, so they get the timer ceiling; ProgressPersistenceInterval is only ever
        // compared against elapsed time (DurableFlowContext.ReportProgressAsync), so any
        // non-negative value is representable — a 60-day lease or progress throttle is a valid
        // configuration and must not fail startup.
        AsyncResponseChannelOptions.EnsurePersistedTtl(options.StateExpiry, nameof(DurableFlowOptions), nameof(options.StateExpiry));
        if (options.DefaultStepTimeout is { } defaultStepTimeout)
            AsyncResponseChannelOptions.EnsureTimerBacked(defaultStepTimeout, nameof(DurableFlowOptions), nameof(options.DefaultStepTimeout));
        AsyncResponseChannelOptions.EnsurePersistedTtl(options.ExecutionLeaseDuration, nameof(DurableFlowOptions), nameof(options.ExecutionLeaseDuration));
        AsyncResponseChannelOptions.EnsureTimerBacked(options.ExecutionLeaseRenewInterval, nameof(DurableFlowOptions), nameof(options.ExecutionLeaseRenewInterval));
        if (options.LedgerSizeWarningBytes is { } ledgerWarning && ledgerWarning <= 0)
        {
            throw new InvalidOperationException(
                $"{nameof(DurableFlowOptions)}.{nameof(options.LedgerSizeWarningBytes)} must be positive, or null to disable the warning (got {ledgerWarning}).");
        }
        if (options.MaxRetainedSteps is <= 0)
            throw new InvalidOperationException($"{nameof(DurableFlowOptions)}.{nameof(options.MaxRetainedSteps)} must be positive, or null to disable the budget.");
        if (options.ExecutionLeaseRenewInterval >= options.ExecutionLeaseDuration)
        {
            throw new InvalidOperationException(
                $"{nameof(DurableFlowOptions)}.{nameof(options.ExecutionLeaseRenewInterval)} must be shorter than " +
                $"{nameof(DurableFlowOptions.ExecutionLeaseDuration)}.");
        }
        // Compared against elapsed time only (the contention poll arms pollDelay-sized timers), so
        // any positive value is representable; zero or less would cap the store-driven wait below
        // "no wait at all", which is a misconfiguration rather than a way to disable the feature.
        if (options.MaxLeaseContentionWait <= TimeSpan.Zero)
            throw new InvalidOperationException($"{nameof(DurableFlowOptions)}.{nameof(options.MaxLeaseContentionWait)} must be positive (got {options.MaxLeaseContentionWait}).");
        if (options.ProgressPersistenceInterval < TimeSpan.Zero)
            throw new InvalidOperationException($"{nameof(DurableFlowOptions)}.{nameof(options.ProgressPersistenceInterval)} cannot be negative.");
        // Timer remainders at or under the threshold arm an in-process Task.Delay, so the knob is
        // timer-backed; zero legitimately means "always suspend".
        AsyncResponseChannelOptions.EnsureTimerBackedAllowZero(options.TimerInProcessThreshold, nameof(DurableFlowOptions), nameof(options.TimerInProcessThreshold));
        // Here with the rest, not only in the lazily built starter: a worker-only host never
        // resolves it, and zero (read as "always hand over", like TimerInProcessThreshold's zero)
        // turned every in-process timer into a hot publish/acquire/save loop.
        options.ValidateInProcessPark();
    }
}

/// <summary>One distributed durable-flow execution lease.</summary>
internal sealed class FlowExecutionLease : IAsyncDisposable
{
    private readonly IFlowStateStore _store;
    private readonly string _flowId;
    private readonly string _leaseId;
    private readonly DurableFlowOptions _options;
    private readonly ILogger _logger;
    private readonly TimeProvider _timeProvider;
    private readonly CancellationTokenSource _stop = new();
    private readonly CancellationTokenSource _lost = new();
    private readonly Task _deadline;
    // Renewal alone can be paused (a park about to publish its wake-up) and restarted (that
    // publish failed), so its loop and stop signal are replaceable; _stop still ends both loops.
    private CancellationTokenSource _renewalStop;
    private Task _renewal;
    // DateTime ticks so the renewal loop's writes and the execution path's reads tear-free on
    // 32-bit runtimes and order via Volatile.
    private long _validUntilUtcTicks;
    private int _disposed;
    // 1 once the lease has been released — by a committed park, or by disposal. See EndForParkAsync.
    private int _ended;
    private Task<bool>? _end;

    /// <summary>
    /// Longest single wait the deadline watcher arms. ExecutionLeaseDuration is validated as a
    /// PERSISTENCE bound, not a timer bound (see <see cref="FlowStateConcurrency.ValidateOptions"/>) —
    /// a 60-day lease is a legal configuration — so the watcher sleeps in chunks and re-reads the
    /// deadline rather than handing an out-of-range delay to a BCL timer.
    /// </summary>
    private static readonly TimeSpan MaxDeadlineChunk = TimeSpan.FromDays(1);

    /// <summary>
    /// Budget for joining the renewal and deadline loops on disposal. The renewal loop can be
    /// stuck inside a store call that ignores its cancellation token (a wedged connection, a
    /// database that accepts the request and never answers — the exact case the deadline watcher
    /// exists for); an unbounded join there wedged the whole worker job. Past the budget the
    /// loops are abandoned: the deadline watcher has already marked the lease lost and the
    /// server-side lease expires on its own.
    /// </summary>
    private static readonly TimeSpan DisposeJoinLimit = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Budget for the final lease release on disposal. The release is one conditional write, so
    /// ten seconds is generous; past it the call is abandoned (cancelled, its outcome observed)
    /// and the server-side lease expires on its own — the same recovery the abandoned renewal
    /// loops rely on. Separate from <see cref="DisposeJoinLimit"/> because the two hang for
    /// different reasons: the loops are joined first and are usually idle, while the release is
    /// a fresh store call that a wedged connection can hold indefinitely even after a clean join.
    /// </summary>
    private static readonly TimeSpan ReleaseLimit = TimeSpan.FromSeconds(10);

    /// <param name="store">The flow state store the lease was acquired through.</param>
    /// <param name="flowId">The flow the lease protects.</param>
    /// <param name="leaseId">The identity of this lease within the flow's row.</param>
    /// <param name="options">Validated durable-flow options.</param>
    /// <param name="logger">Sink for renewal and deadline watcher events.</param>
    /// <param name="timeProvider">Clock used for deadline computation; <see cref="TimeProvider.System"/> when omitted.</param>
    /// <param name="acquiredDeadlineUtcTicks">
    /// The conservative deadline for the lease this instance was handed, captured BEFORE the
    /// acquire call went out. Omitted only by callers that construct a lease without an acquire
    /// round trip (tests), where "now + duration" is exact.
    /// </param>
    public FlowExecutionLease(
        IFlowStateStore store,
        string flowId,
        string leaseId,
        DurableFlowOptions options,
        ILogger logger,
        TimeProvider? timeProvider = null,
        long? acquiredDeadlineUtcTicks = null)
    {
        _store = store;
        _flowId = flowId;
        _leaseId = leaseId;
        _options = options;
        _logger = logger;
        _timeProvider = timeProvider ?? TimeProvider.System;
        Volatile.Write(
            ref _validUntilUtcTicks,
            acquiredDeadlineUtcTicks ?? DeadlineFrom(_timeProvider, options.ExecutionLeaseDuration));
        _renewalStop = CancellationTokenSource.CreateLinkedTokenSource(_stop.Token);
        _renewal = RenewLoopAsync(_renewalStop.Token, options.ExecutionLeaseRenewInterval);
        _deadline = DeadlineLoopAsync();
    }

    /// <summary>
    /// "Now + duration" in UTC ticks, saturating instead of overflowing: <c>ExecutionLeaseDuration</c>
    /// is bounded as a persistence TTL, not a timer, so a 60-day lease near <see cref="DateTime.MaxValue"/>
    /// is a legal configuration that must not throw here.
    /// </summary>
    internal static long DeadlineFrom(TimeProvider timeProvider, TimeSpan duration)
        => FlowStateRetention.AddSaturating(timeProvider.GetUtcNow().UtcDateTime, duration).Ticks;

    public CancellationToken LostToken => _lost.Token;

    /// <summary>
    /// Whether this lease can still fence a write. Callers holding a claimed, unrecorded result
    /// use it to choose the lease-less persistence path BEFORE surfacing the takeover signal.
    /// </summary>
    public bool IsLost => _lost.IsCancellationRequested
        || Volatile.Read(ref _ended) != 0
        || _timeProvider.GetUtcNow().UtcDateTime.Ticks >= Volatile.Read(ref _validUntilUtcTicks);

    /// <summary>
    /// Throws when the lease is lost. <paramref name="cause"/> (e.g. the exception that made the
    /// caller check) is attached as the inner exception so the real failure is not discarded.
    /// <para>
    /// A passed deadline counts as lost even before any renewal fails: the renewal loop only
    /// observes loss on a store round-trip, so a stop-the-world pause (GC, VM freeze, debugger)
    /// longer than the lease lets another worker take over while this side has seen nothing —
    /// its next step body would then run concurrently with the new holder's. Checkpoints are
    /// lease-fenced; side effects are fenced only by this guard, so it is conservative near the
    /// boundary by design: retrying from the checkpoint is always safe, a concurrent step is not.
    /// </para>
    /// </summary>
    public void ThrowIfLost(Exception? cause = null)
    {
        // Released on purpose (see EndForParkAsync), not lost: nothing is fenced by it any more,
        // and the run's successor may already hold a lease of its own.
        if (Volatile.Read(ref _ended) != 0)
            throw new InvalidOperationException($"Durable flow '{_flowId}' has released its execution lease (the run parked, or the execution ended); this execution checkpoints nothing more.", cause);

        if (!_lost.IsCancellationRequested
            && _timeProvider.GetUtcNow().UtcDateTime.Ticks < Volatile.Read(ref _validUntilUtcTicks))
            return;

        MarkLost();
        throw new InvalidOperationException($"Durable flow '{_flowId}' lost its execution lease; the worker will retry from the last checkpoint.", cause);
    }

    public async Task SaveAsync(FlowState state, TimeSpan ttl, CancellationToken cancellationToken = default, Exception? cause = null)
    {
        ThrowIfLost(cause);
        var expectedRevision = state.Revision;
        state.Revision = checked(expectedRevision + 1);
        var nowUtc = _timeProvider.GetUtcNow().UtcDateTime;
        state.UpdatedAtUtc = nowUtc;

        try
        {
            if (await _store.TryUpdateAsync(
                    _flowId,
                    state,
                    expectedRevision,
                    // Every checkpoint carries the ledger's retention floor forward (see
                    // FlowStateRetention): the executor's per-attempt save and an ancestor's
                    // re-park stamp the plain StateExpiry, and used to shrink a ledger a
                    // descendant had extended for a wait still in progress.
                    FlowStateRetention.EffectiveTtl(state, ttl, nowUtc),
                    _leaseId,
                    cancellationToken).ConfigureAwait(false))
                return;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // The CALLER cancelled the write — flow code's own token on a progress, value, park or
            // breadcrumb save. That says nothing about the lease: marking it lost here turned every
            // later context call into a misdiagnosed "lost its execution lease" and skipped the
            // attempt's failure checkpoint. Should the write have committed after all, the next
            // fenced save's compare-and-swap rejects and diagnoses it.
            state.Revision = expectedRevision;
            throw;
        }
        catch
        {
            state.Revision = expectedRevision;
            MarkLost();

            // The store exception propagates; keep the failure this save was recording from
            // vanishing with it.
            if (cause is not null)
                _logger.LogWarning(cause, "Durable flow '{FlowId}' failed to checkpoint; the failure it was recording is attached here and the store error propagates.", _flowId);
            throw;
        }

        state.Revision = expectedRevision;
        MarkLost();
        throw await CreateSaveRejectedExceptionAsync(expectedRevision, cause, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Builds the exception for a rejected checkpoint write. The store's compare-and-swap only
    /// returns <c>false</c>, so the reason is diagnosed with a best-effort re-read: a revision
    /// conflict — a concurrent lease-bypassing writer such as <c>RecoverAsync</c>, <c>FailAsync</c>,
    /// or an operator parking the run — is reported as such instead of as a lost lease, which sent
    /// operators hunting phantom lease problems. Behavior is unchanged either way: the lease is
    /// abandoned (<see cref="MarkLost"/> already ran) and the delivery retries from the last
    /// checkpoint; <paramref name="cause"/> rides along as the inner exception so the failure that
    /// triggered the save is not discarded.
    /// </summary>
    private async Task<InvalidOperationException> CreateSaveRejectedExceptionAsync(
        long expectedRevision,
        Exception? cause,
        CancellationToken cancellationToken)
    {
        var reason = "its execution lease was no longer held (expired or taken over)";
        try
        {
            var current = await _store.LoadAsync(_flowId, cancellationToken).ConfigureAwait(false);
            if (current is null)
                reason = "its ledger entry is gone (expired or deleted)";
            else if (current.Revision != expectedRevision)
                reason = $"a concurrent write advanced the ledger (revision {expectedRevision} -> {current.Revision}: a recovery, failure signal, or operator status change won the race)";
        }
        catch
        {
            // Best-effort diagnosis only — the rejection itself is what matters.
        }

        return new InvalidOperationException(
            $"Durable flow '{_flowId}' could not checkpoint because {reason}; the worker abandons this execution and the delivery retries from the last checkpoint.",
            cause);
    }

    /// <param name="stop">Ends the loop.</param>
    /// <param name="firstWait">
    /// How long before the first renewal: a full interval for a freshly acquired lease, zero for a
    /// renewal restarted after a pause (see <see cref="ResumeRenewal"/>).
    /// </param>
    private async Task RenewLoopAsync(CancellationToken stop, TimeSpan firstWait)
    {
        // A FAILED beat retries on a short backoff instead of waiting out another full interval
        // (the database transports' lock renewal does the same): with the default 60 s lease and
        // 20 s interval, two failed beats — a 25-second store blip — lost a healthy lease, and one
        // renewal that took 20 s to time out lost it on its own. Floored at a millisecond: a zero
        // wait would spin.
        var interval = _options.ExecutionLeaseRenewInterval;
        var retryInterval = TimeSpan.FromTicks(Math.Max(
            TimeSpan.TicksPerMillisecond,
            Math.Min(Math.Min(TimeSpan.TicksPerSecond, _options.ExecutionLeaseDuration.Ticks / 10), interval.Ticks)));
        var wait = firstWait;
        var failing = false;
        while (!stop.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(wait, _timeProvider, stop).ConfigureAwait(false);

                // Same anchoring rule as acquisition: the renewed lease starts when the store runs
                // the command, so the deadline is measured from before the call, not from whenever
                // the answer gets back here. Published only on success, so a failed renewal never
                // extends anything.
                var renewedDeadline = DeadlineFrom(_timeProvider, _options.ExecutionLeaseDuration);

                if (!await _store.TryRenewLeaseAsync(
                        _flowId,
                        _leaseId,
                        _options.ExecutionLeaseDuration,
                        stop).ConfigureAwait(false))
                {
                    MarkLost();
                    return;
                }

                Volatile.Write(ref _validUntilUtcTicks, renewedDeadline);
                wait = interval;
                failing = false;
            }
            catch (OperationCanceledException) when (stop.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                // Only the first failure of a streak is a warning: at the retry cadence a store
                // outage would otherwise log one per second per running flow.
                _logger.Log(
                    failing ? LogLevel.Debug : LogLevel.Warning,
                    ex,
                    "Failed to renew durable flow {FlowId} execution lease; retrying every {RetryInterval} until it succeeds or the lease expires.",
                    _flowId,
                    retryInterval);
                failing = true;
                if (_timeProvider.GetUtcNow().UtcDateTime.Ticks >= Volatile.Read(ref _validUntilUtcTicks))
                {
                    MarkLost();
                    return;
                }

                wait = retryInterval;
            }
        }
    }

    /// <summary>
    /// Cancels <see cref="LostToken"/> when the lease deadline passes, on a clock of its own.
    /// <para>
    /// <see cref="RenewLoopAsync"/> cannot be trusted to do this: it only learns the lease is gone
    /// by completing a store round-trip, so a renewal call that hangs — a wedged connection, a
    /// database that accepts the request and never answers — leaves the token live indefinitely
    /// while the server-side lease expires and another replica takes the flow over. Checkpoints
    /// stay fenced regardless (<see cref="ThrowIfLost"/> and the lease-fenced CAS both check the
    /// clock), but anything watching the TOKEN — a step body, a linked operation — saw nothing.
    /// This loop closes that gap: it re-reads the deadline each pass, so a successful renewal
    /// simply pushes it out, and it fires whether or not the renewal path is responsive.
    /// </para>
    /// </summary>
    private async Task DeadlineLoopAsync()
    {
        try
        {
            while (!_stop.IsCancellationRequested && !_lost.IsCancellationRequested)
            {
                var remaining = new DateTime(Volatile.Read(ref _validUntilUtcTicks), DateTimeKind.Utc)
                    - _timeProvider.GetUtcNow().UtcDateTime;

                if (remaining <= TimeSpan.Zero)
                {
                    _logger.LogWarning(
                        "Durable flow {FlowId} execution lease reached its deadline without a successful renewal; abandoning this execution.",
                        _flowId);
                    MarkLost();
                    return;
                }

                await Task.Delay(
                    remaining < MaxDeadlineChunk ? remaining : MaxDeadlineChunk,
                    _timeProvider,
                    _stop.Token).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (_stop.IsCancellationRequested)
        {
            // Normal completion: the execution finished and disposal stopped the watcher.
        }
    }

    private void MarkLost()
    {
        try
        {
            _lost.Cancel();
        }
        catch (ObjectDisposedException)
        {
            // Disposal won the race.
        }
    }

    /// <summary>
    /// Stops renewing and waits for a renewal already in flight to land, so that from here on the
    /// lease in the store never changes again on this execution's account. A park calls it after
    /// its checkpoint and BEFORE it publishes the wake-up: that wake-up judges the lease it finds
    /// by whether it changes (<see cref="FlowLeaseContention"/>), and a renewal landing after its
    /// first look reads as a live holder executing a different job — proof enough to acknowledge
    /// it as a duplicate, although it is the park's own continuation. The deadline watcher keeps
    /// running. Bounded like disposal: past the budget the stuck renewal is left behind.
    /// </summary>
    internal async Task PauseRenewalAsync()
    {
        _renewalStop.Cancel();
        try
        {
            await _renewal.WaitAsync(DisposeJoinLimit, _timeProvider).ConfigureAwait(false);
        }
        catch (TimeoutException)
        {
            _logger.LogWarning(
                "Durable flow {FlowId} execution lease renewal did not stop within {DisposeJoinLimit} before the run parked; parking anyway.",
                _flowId,
                DisposeJoinLimit);
        }
    }

    /// <summary>
    /// Restarts the renewal <see cref="PauseRenewalAsync"/> stopped — the park's publish failed, so
    /// the execution goes on (as the retriable failure it is) and still owns the run. The first
    /// renewal is due at once, not a full interval later: the publish ran through the builder's
    /// retry ladder with nothing renewing, so the lease may have little left — waiting another
    /// interval let it lapse, and the attempt's failure checkpoint was refused with it.
    /// </summary>
    internal void ResumeRenewal()
    {
        if (Volatile.Read(ref _ended) != 0 || _stop.IsCancellationRequested || _lost.IsCancellationRequested || !_renewal.IsCompleted)
            return;

        _renewalStop.Dispose();
        _renewalStop = CancellationTokenSource.CreateLinkedTokenSource(_stop.Token);
        _renewal = RenewLoopAsync(_renewalStop.Token, TimeSpan.Zero);
    }

    /// <summary>
    /// Releases the lease as soon as a park has committed (checkpoint written, wake-up published),
    /// instead of when the executor disposes it — which happens only after the flow body has
    /// unwound: every user <c>finally</c>, <c>await using</c> and <c>catch (OperationCanceledException)</c>
    /// on the way out, since a park is a cancellation. Until then the store still showed a held
    /// lease, and the wake-up the park had just published (a child finishing at once, a timer
    /// hand-over) could only wait behind it; had it seen one more renewal it would have been
    /// acknowledged as a duplicate of a live holder whose own job, far from being redelivered if
    /// the holder died, was about to be acknowledged too. Released, the lease is free for the
    /// wake-up on its next poll. Idempotent with disposal, which afterwards only frees resources.
    /// Every later use of this lease to fence a write throws (see <see cref="ThrowIfLost"/>).
    /// </summary>
    internal Task EndForParkAsync() => EndAsync();

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        if (!await EndAsync().ConfigureAwait(false))
        {
            // Loops abandoned: leave the cancellation sources undisposed for them.
            return;
        }

        _stop.Dispose();
        _renewalStop.Dispose();
        _lost.Dispose();
    }

    // Called on the execution path only (a park, then disposal after the body returned), never
    // concurrently: the first call does the work and every later one observes its outcome.
    private Task<bool> EndAsync() => _end ??= EndCoreAsync();

    /// <summary>Stops both loops and releases the lease; <c>false</c> when the loops had to be abandoned.</summary>
    private async Task<bool> EndCoreAsync()
    {
        Volatile.Write(ref _ended, 1);
        _stop.Cancel();
        try
        {
            // Bounded join (see DisposeJoinLimit): both loops swallow their own exceptions, so an
            // abandoned task cannot fault unobserved.
            await Task.WhenAll(_renewal, _deadline).WaitAsync(DisposeJoinLimit, _timeProvider).ConfigureAwait(false);
        }
        catch (TimeoutException)
        {
            // The store call the renewal loop is stuck in ignores cancellation, so releasing the
            // lease through the same store would hang this disposal all over again. Skip the
            // release (the server-side lease expires) and leave the cancellation sources
            // undisposed for the abandoned loops.
            _logger.LogWarning(
                "Durable flow {FlowId} execution lease loops did not stop within {DisposeJoinLimit}; abandoning them (the lease will expire server-side).",
                _flowId,
                DisposeJoinLimit);
            return false;
        }

        // Bounded release (see ReleaseLimit), with a token the store can honor. An unbounded,
        // uncancelable release kept a FINISHED execution's disposal — and with it the executor's
        // `await using`, the job's DI scope, the worker slot, and the transport acknowledgement —
        // pending for as long as a wedged store took to answer, which can be forever.
        var releaseCancellation = new CancellationTokenSource();
        Task? release = null;
        try
        {
            release = _store.ReleaseLeaseAsync(_flowId, _leaseId, releaseCancellation.Token);
            await release.WaitAsync(ReleaseLimit, _timeProvider).ConfigureAwait(false);
            releaseCancellation.Dispose();
        }
        catch (TimeoutException) when (release is { IsCompleted: false })
        {
            // The budget lapsed with the store still silent (a TimeoutException thrown BY the
            // store completes the task first and takes the branch below). Cancel what can be
            // cancelled, observe whatever the abandoned call eventually does, and move on: the
            // server-side lease expires on its own, exactly as when the renewal loops are abandoned.
            releaseCancellation.Cancel();
            _logger.LogWarning(
                "Durable flow {FlowId} execution lease release did not complete within {ReleaseLimit}; abandoning it (the lease will expire server-side).",
                _flowId,
                ReleaseLimit);
            ObserveAbandonedRelease(release, releaseCancellation);
        }
        catch (Exception ex)
        {
            releaseCancellation.Dispose();
            _logger.LogWarning(ex, "Failed to release durable flow {FlowId} execution lease; it will expire.", _flowId);
        }

        return true;
    }

    /// <summary>
    /// Attaches the one continuation an abandoned release needs: its eventual fault is observed
    /// (and logged, so a store that finally answers with an error is not an unobserved-task
    /// event) and the cancellation source it still holds is disposed only once it can no longer
    /// be touched.
    /// </summary>
    private void ObserveAbandonedRelease(Task release, CancellationTokenSource releaseCancellation)
        => _ = release.ContinueWith(
            (task, state) =>
            {
                var (lease, cancellation) = ((FlowExecutionLease, CancellationTokenSource))state!;
                if (task.IsFaulted)
                {
                    lease._logger.LogWarning(
                        task.Exception?.GetBaseException(),
                        "The abandoned release of durable flow {FlowId}'s execution lease eventually failed; the lease expires server-side.",
                        lease._flowId);
                }
                else
                {
                    lease._logger.LogDebug(
                        "The abandoned release of durable flow {FlowId}'s execution lease eventually completed ({Status}).",
                        lease._flowId,
                        task.Status);
                }

                cancellation.Dispose();
            },
            (this, releaseCancellation),
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);
}
