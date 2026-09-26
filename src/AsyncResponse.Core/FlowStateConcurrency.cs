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

    // A checkpoint its caller cancelled mid-write, whose outcome is unknown until
    // ResolveUncertainSaveAsync reads the ledger back. Execution path only, like every save.
    private UncertainSave? _uncertainSave;

    /// <summary>
    /// What a caller-cancelled checkpoint would have left in the store had it committed: the
    /// revision it expected (it wrote the next one) and the fields that tell this execution's
    /// write from another writer's at the same revision.
    /// </summary>
    private sealed record UncertainSave(long ExpectedRevision, DateTime UpdatedAtUtc, FlowRunStatus Status, string? LastMessage);

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
    internal static readonly TimeSpan DisposeJoinLimit = TimeSpan.FromSeconds(30);

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

        // A token cancelled before the call writes nothing, so there is no outcome to settle later.
        cancellationToken.ThrowIfCancellationRequested();

        // An earlier checkpoint whose caller cancelled it mid-write may have committed after all:
        // settled first, or this write's compare-and-swap would be judged against a revision the
        // store moved past on this execution's own account, and the execution abandoned as a lost
        // race (see ResolveUncertainSaveAsync).
        if (_uncertainSave is not null)
        {
            try
            {
                await ResolveUncertainSaveCoreAsync(state).ConfigureAwait(false);
            }
            catch
            {
                // A settle read that failed leaves this checkpoint unwritten like any failed write,
                // and marks the lease lost like one: every throwing save does, so the executor takes
                // its lost-lease path and runs no failure-path save after it. Left live, that save
                // settled on a retry and persisted the attempt's terminal status with the store
                // error as its message — a Succeeded run no parent was ever told about.
                MarkLost();
                if (cause is not null)
                    LogCauseOfFailedCheckpoint(cause);
                throw;
            }
        }

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
        catch (Exception ex) when (cancellationToken.IsCancellationRequested)
        {
            // The CALLER cancelled the write — flow code's own token on a progress, value, park or
            // breadcrumb save. That says nothing about the lease: marking it lost here turned every
            // later context call into a misdiagnosed "lost its execution lease" and skipped the
            // attempt's failure checkpoint. Judged by the TOKEN, not the exception's type:
            // SqlClient and ODP.NET report a cancellation that interrupts a running command as
            // their own exception ("Operation cancelled by user", ORA-01013), and only one raised
            // before the command ran as OperationCanceledException.
            //
            // Nor does it say the write did not happen: the server may have applied it before the
            // cancellation reached it (an attention that arrives after the autocommit UPDATE, an
            // HTTP request the store already holds). The outcome is recorded as UNKNOWN and settled
            // by a read before the next step body or checkpoint (see ResolveUncertainSaveAsync) —
            // carrying on as if it had failed ran the next step's side effect before a
            // compare-and-swap that could only fail, and blamed the wrong writer.
            state.Revision = expectedRevision;
            _uncertainSave = new UncertainSave(expectedRevision, nowUtc, state.Status, state.LastMessage);
            if (ex is OperationCanceledException)
                throw;
            throw new OperationCanceledException(
                $"Durable flow '{_flowId}' checkpoint was cancelled by its caller; whether it committed is settled before the next step runs.",
                ex,
                cancellationToken);
        }
        catch
        {
            state.Revision = expectedRevision;
            MarkLost();

            // The store exception propagates; keep the failure this save was recording from
            // vanishing with it.
            if (cause is not null)
                LogCauseOfFailedCheckpoint(cause);
            throw;
        }

        state.Revision = expectedRevision;
        MarkLost();
        throw await CreateSaveRejectedExceptionAsync(expectedRevision, cause, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Settles a checkpoint whose caller cancelled it mid-write (see <see cref="SaveAsync"/>) with
    /// one current read, before anything else runs on this execution's account: the context calls
    /// it ahead of every step body, and every checkpoint ahead of its own write.
    /// <list type="bullet">
    /// <item>The ledger still at the revision the write expected: it did not commit. Carry on.</item>
    /// <item>The ledger at the revision it wrote, carrying its stamp: it committed. That revision is
    /// adopted, so the next compare-and-swap is judged against what this execution wrote itself.</item>
    /// <item>Anything else — another writer (a recovery, a failure signal, an operator status
    /// change) moved the ledger, or it is gone: the lease is marked lost and the execution
    /// abandoned, as for any lost race, with the cancelled checkpoint named.</item>
    /// </list>
    /// Read through <see cref="IFlowStateStore.LoadCurrentAsync"/>, since a lagging copy would show
    /// the expected revision for a write that did commit, and bounded by <see cref="LostToken"/>. A
    /// failed read propagates with the outcome still unknown, and the next caller settles it again;
    /// inside a checkpoint it fails that checkpoint, which marks the lease lost like any failed
    /// checkpoint write.
    /// </summary>
    internal Task ResolveUncertainSaveAsync(FlowState state)
        => _uncertainSave is null ? Task.CompletedTask : ResolveUncertainSaveCoreAsync(state);

    private async Task ResolveUncertainSaveCoreAsync(FlowState state)
    {
        var uncertain = _uncertainSave!;
        ThrowIfLost();

        // Not the caller's token: the caller's cancellation is what left the outcome unknown, and
        // this read is what settles it. The lease's own token, though: a wedged store must not
        // pin the step past the lease deadline, when nothing here may run any more anyway.
        var current = await _store.LoadCurrentAsync(_flowId, LostToken).ConfigureAwait(false);
        _uncertainSave = null;
        if (current?.Revision == uncertain.ExpectedRevision)
            return;

        if (current is not null
            && current.Revision == uncertain.ExpectedRevision + 1
            && current.UpdatedAtUtc?.Ticks == uncertain.UpdatedAtUtc.Ticks
            && current.Status == uncertain.Status
            && string.Equals(current.LastMessage, uncertain.LastMessage, StringComparison.Ordinal))
        {
            state.Revision = current.Revision;
            return;
        }

        MarkLost();
        throw new InvalidOperationException(
            $"Durable flow '{_flowId}' could not confirm the checkpoint its caller cancelled mid-write (revision {uncertain.ExpectedRevision} -> {uncertain.ExpectedRevision + 1}): " +
            (current is null
                ? "its ledger entry is gone (expired or deleted)"
                : $"the ledger now reads revision {current.Revision}, written by someone else (a recovery, failure signal, or operator status change)") +
            "; the worker abandons this execution and the delivery retries from the last checkpoint.");
    }

    private void LogCauseOfFailedCheckpoint(Exception cause)
        => SafeLog.Try(
            (Logger: _logger, Cause: cause, FlowId: _flowId),
            static s => s.Logger.LogWarning(s.Cause, "Durable flow '{FlowId}' failed to checkpoint; the failure it was recording is attached here and the store error propagates.", s.FlowId));

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
        // wait would spin. The retries back off from there (see RenewalRetryDelay).
        var interval = _options.ExecutionLeaseRenewInterval;
        var retryInterval = TimeSpan.FromTicks(Math.Max(
            TimeSpan.TicksPerMillisecond,
            Math.Min(Math.Min(TimeSpan.TicksPerSecond, _options.ExecutionLeaseDuration.Ticks / 10), interval.Ticks)));
        var wait = firstWait;
        var failures = 0;
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
                failures = 0;
            }
            catch (Exception) when (stop.IsCancellationRequested)
            {
                // Stopped — by a park's pause or by the end of the execution — whatever the call in
                // flight threw on the way out: a renewal cancelled mid-command surfaces as the
                // driver's own exception (SqlClient, ODP.NET), and is no renewal failure.
                return;
            }
            catch (Exception ex)
            {
                failures++;
                var left = new DateTime(Volatile.Read(ref _validUntilUtcTicks), DateTimeKind.Utc) - _timeProvider.GetUtcNow().UtcDateTime;
                if (left > TimeSpan.Zero)
                    wait = RenewalRetryDelay(failures, retryInterval, interval, left);
                else
                    MarkLost();

                // Decided first, logged second, and guarded: a logging provider that throws ended
                // this loop, and nothing renewed the lease for the rest of the execution. Only the
                // first failure of a streak is a warning, or a store outage would log one per retry
                // per running flow.
                SafeLog.Try(
                    (Lease: this, Error: ex, First: failures == 1, Lost: left <= TimeSpan.Zero, Wait: wait),
                    static s =>
                    {
                        var level = s.First ? LogLevel.Warning : LogLevel.Debug;
                        if (s.Lost)
                            s.Lease._logger.Log(level, s.Error, "Failed to renew durable flow {FlowId} execution lease; it has reached its deadline, and this execution is abandoned.", s.Lease._flowId);
                        else
                            s.Lease._logger.Log(level, s.Error, "Failed to renew durable flow {FlowId} execution lease; retrying in {RetryDelay}, backing off, until it succeeds or the lease expires.", s.Lease._flowId, s.Wait);
                    });

                if (left <= TimeSpan.Zero)
                    return;
            }
        }
    }

    /// <summary>
    /// The wait before the next renewal attempt after <paramref name="failures"/> consecutive
    /// failed ones: half-jitter exponential backoff from <paramref name="retryInterval"/>, capped at
    /// the renew interval and at half the time the lease has <paramref name="left"/> — never under
    /// <paramref name="retryInterval"/>. A fixed one-second cadence multiplied the store's renewal
    /// load about twentyfold, per running flow, for as long as the store was failing; backing off
    /// alone would have stretched the last waits past the deadline and lost a lease a blip ending
    /// just before it could have kept. Capped at half of what is left, the attempts keep landing
    /// before the deadline, closer together as it nears.
    /// </summary>
    internal static TimeSpan RenewalRetryDelay(int failures, TimeSpan retryInterval, TimeSpan interval, TimeSpan left)
    {
        var cap = Math.Max(retryInterval.Ticks, Math.Min(interval.Ticks, left.Ticks / 2));
        return AsyncResponseRetry.Backoff(failures, retryInterval, TimeSpan.FromTicks(cap));
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
                    // Marked first, logged second and guarded: a throwing logging provider left
                    // LostToken live past the deadline, the one thing this loop exists to fire.
                    MarkLost();
                    SafeLog.Try(this, static lease => lease._logger.LogWarning(
                        "Durable flow {FlowId} execution lease reached its deadline without a successful renewal; abandoning this execution.",
                        lease._flowId));
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
    /// running. Bounded like disposal.
    /// </summary>
    /// <returns>
    /// <c>false</c> when the renewal did not stop within <see cref="DisposeJoinLimit"/> — a store
    /// call that ignores its cancellation token is still in flight and may yet land, so the caller
    /// must not publish anything that judges this lease by whether it changes.
    /// </returns>
    internal async Task<bool> PauseRenewalAsync()
    {
        _renewalStop.Cancel();
        try
        {
            await _renewal.WaitAsync(DisposeJoinLimit, _timeProvider).ConfigureAwait(false);
        }
        catch (TimeoutException)
        {
            // Past the budget the verdict stands even should the renewal land a moment later: it
            // was still in flight when the budget ran out.
            return false;
        }
        catch
        {
            // The loop swallows its own failures; one that faulted anyway has still ended, which is
            // all a pause needs.
        }

        return true;
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
        var loops = Task.WhenAll(_renewal, _deadline);
        try
        {
            // Bounded join (see DisposeJoinLimit): both loops swallow their own exceptions, so an
            // abandoned task cannot fault unobserved.
            await loops.WaitAsync(DisposeJoinLimit, _timeProvider).ConfigureAwait(false);
        }
        catch (TimeoutException) when (!loops.IsCompleted)
        {
            // The store call the renewal loop is stuck in ignores cancellation, so releasing the
            // lease through the same store would hang this disposal all over again. Skip the
            // release (the server-side lease expires) and leave the cancellation sources
            // undisposed for the abandoned loops.
            SafeLog.Try(this, static lease => lease._logger.LogWarning(
                "Durable flow {FlowId} execution lease loops did not stop within {DisposeJoinLimit}; abandoning them (the lease will expire server-side).",
                lease._flowId,
                DisposeJoinLimit));
            return false;
        }
        catch
        {
            // A loop that faulted anyway (they swallow their own failures) has still ended: the
            // release below must not be skipped for it, nor its fault thrown out of a disposal
            // that follows a run already saved and notified.
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
            ObserveAbandonedRelease(release, releaseCancellation);
            SafeLog.Try(this, static lease => lease._logger.LogWarning(
                "Durable flow {FlowId} execution lease release did not complete within {ReleaseLimit}; abandoning it (the lease will expire server-side).",
                lease._flowId,
                ReleaseLimit));
        }
        catch (Exception ex)
        {
            releaseCancellation.Dispose();
            SafeLog.Try((Lease: this, Error: ex), static s => s.Lease._logger.LogWarning(s.Error, "Failed to release durable flow {FlowId} execution lease; it will expire.", s.Lease._flowId));
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
                cancellation.Dispose();
                SafeLog.Try((Lease: lease, Task: task), static s =>
                {
                    if (s.Task.IsFaulted)
                    {
                        s.Lease._logger.LogWarning(
                            s.Task.Exception?.GetBaseException(),
                            "The abandoned release of durable flow {FlowId}'s execution lease eventually failed; the lease expires server-side.",
                            s.Lease._flowId);
                    }
                    else
                    {
                        s.Lease._logger.LogDebug(
                            "The abandoned release of durable flow {FlowId}'s execution lease eventually completed ({Status}).",
                            s.Lease._flowId,
                            s.Task.Status);
                    }
                });
            },
            (this, releaseCancellation),
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);
}
