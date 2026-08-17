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
        if (FlowIdNotPortable(flowId) is { } rejection)
            throw new ArgumentException(rejection, nameof(flowId));

        state.Revision = 0;
        return store.TryCreateAsync(flowId, state, ttl, cancellationToken);
    }

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

    private static string Excerpt(string flowId) => PortableText.Excerpt(flowId);

    public static async Task<FlowExecutionLease?> TryAcquireExecutionLeaseAsync(
        IFlowStateStore store,
        string flowId,
        DurableFlowOptions options,
        ILogger logger,
        TimeProvider? timeProvider = null,
        CancellationToken cancellationToken = default)
    {
        ValidateOptions(options);

        var clock = timeProvider ?? TimeProvider.System;
        var leaseId = Guid.NewGuid().ToString("N");

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
            state.UpdatedAtUtc = (timeProvider ?? TimeProvider.System).GetUtcNow().UtcDateTime;
            if (await store.TryUpdateAsync(
                    flowId,
                    state,
                    expectedRevision,
                    ttl,
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
        if (options.ExecutionLeaseRenewInterval >= options.ExecutionLeaseDuration)
        {
            throw new InvalidOperationException(
                $"{nameof(DurableFlowOptions)}.{nameof(options.ExecutionLeaseRenewInterval)} must be shorter than " +
                $"{nameof(DurableFlowOptions.ExecutionLeaseDuration)}.");
        }
        if (options.ProgressPersistenceInterval < TimeSpan.Zero)
            throw new InvalidOperationException($"{nameof(DurableFlowOptions)}.{nameof(options.ProgressPersistenceInterval)} cannot be negative.");
        // Timer remainders at or under the threshold arm an in-process Task.Delay, so the knob is
        // timer-backed; zero legitimately means "always suspend".
        AsyncResponseChannelOptions.EnsureTimerBackedAllowZero(options.TimerInProcessThreshold, nameof(DurableFlowOptions), nameof(options.TimerInProcessThreshold));
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
    private readonly Task _renewal;
    private readonly Task _deadline;
    // DateTime ticks so the renewal loop's writes and the execution path's reads tear-free on
    // 32-bit runtimes and order via Volatile.
    private long _validUntilUtcTicks;
    private int _disposed;

    /// <summary>
    /// Longest single wait the deadline watcher arms. ExecutionLeaseDuration is validated as a
    /// PERSISTENCE bound, not a timer bound (see <see cref="FlowStateConcurrency.ValidateOptions"/>) —
    /// a 60-day lease is a legal configuration — so the watcher sleeps in chunks and re-reads the
    /// deadline rather than handing an out-of-range delay to a BCL timer.
    /// </summary>
    private static readonly TimeSpan MaxDeadlineChunk = TimeSpan.FromDays(1);

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
        _renewal = RenewLoopAsync();
        _deadline = DeadlineLoopAsync();
    }

    /// <summary>
    /// "Now + duration" in UTC ticks, saturating instead of overflowing: <c>ExecutionLeaseDuration</c>
    /// is bounded as a persistence TTL, not a timer, so a 60-day lease near <see cref="DateTime.MaxValue"/>
    /// is a legal configuration that must not throw here.
    /// </summary>
    internal static long DeadlineFrom(TimeProvider timeProvider, TimeSpan duration)
    {
        var now = timeProvider.GetUtcNow().UtcDateTime;
        return duration > DateTime.MaxValue - now ? DateTime.MaxValue.Ticks : now.Add(duration).Ticks;
    }

    public CancellationToken LostToken => _lost.Token;

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
        state.UpdatedAtUtc = _timeProvider.GetUtcNow().UtcDateTime;

        try
        {
            if (await _store.TryUpdateAsync(
                    _flowId,
                    state,
                    expectedRevision,
                    ttl,
                    _leaseId,
                    cancellationToken).ConfigureAwait(false))
                return;
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

    private async Task RenewLoopAsync()
    {
        while (!_stop.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(_options.ExecutionLeaseRenewInterval, _timeProvider, _stop.Token).ConfigureAwait(false);

                // Same anchoring rule as acquisition: the renewed lease starts when the store runs
                // the command, so the deadline is measured from before the call, not from whenever
                // the answer gets back here. Published only on success, so a failed renewal never
                // extends anything.
                var renewedDeadline = DeadlineFrom(_timeProvider, _options.ExecutionLeaseDuration);

                if (!await _store.TryRenewLeaseAsync(
                        _flowId,
                        _leaseId,
                        _options.ExecutionLeaseDuration,
                        _stop.Token).ConfigureAwait(false))
                {
                    MarkLost();
                    return;
                }

                Volatile.Write(ref _validUntilUtcTicks, renewedDeadline);
            }
            catch (OperationCanceledException) when (_stop.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to renew durable flow {FlowId} execution lease; retrying before expiry.", _flowId);
                if (_timeProvider.GetUtcNow().UtcDateTime.Ticks >= Volatile.Read(ref _validUntilUtcTicks))
                {
                    MarkLost();
                    return;
                }
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

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        _stop.Cancel();
        await _renewal.ConfigureAwait(false);
        await _deadline.ConfigureAwait(false);

        try
        {
            await _store.ReleaseLeaseAsync(_flowId, _leaseId, CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to release durable flow {FlowId} execution lease; it will expire.", _flowId);
        }

        _stop.Dispose();
        _lost.Dispose();
    }
}
