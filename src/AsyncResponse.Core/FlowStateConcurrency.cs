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
    /// <item>length in UTF-16 characters, for the 400-character <c>flow_id</c> columns (SQL
    /// Server, MySQL, Oracle, EF Core);</item>
    /// <item>length in UTF-8 <em>bytes</em>, for Cosmos DB, whose 1023-byte id limit a 400-
    /// character id made of three-byte characters (CJK, most emoji) exceeds at 1200;</item>
    /// <item>the characters themselves — Cosmos rejects <c>/</c>, <c>\</c>, <c>?</c> and <c>#</c>
    /// in an id, and control characters break every store's diagnostics.</item>
    /// </list>
    /// Case is deliberately NOT folded here: ids are compared ordinally throughout, and the
    /// relational stores pin a binary collation on the column so the database agrees.
    /// Returns the rejection message, or <c>null</c> when the id is portable.
    /// </summary>
    internal static string? FlowIdNotPortable(string flowId)
    {
        if (flowId.Length > DurableFlowOptions.MaxFlowIdLength)
        {
            return $"Flow id '{Excerpt(flowId)}' is {flowId.Length} characters; the portable maximum is " +
                $"{DurableFlowOptions.MaxFlowIdLength} ({nameof(DurableFlowOptions)}.{nameof(DurableFlowOptions.MaxFlowIdLength)} — the flow_id " +
                "column length in the SQL Server, MySQL, Oracle, and EF Core stores). " + BudgetGuidance;
        }

        var utf8Bytes = System.Text.Encoding.UTF8.GetByteCount(flowId);
        if (utf8Bytes > DurableFlowOptions.MaxFlowIdBytes)
        {
            return $"Flow id '{Excerpt(flowId)}' is {utf8Bytes} UTF-8 bytes; the portable maximum is " +
                $"{DurableFlowOptions.MaxFlowIdBytes} ({nameof(DurableFlowOptions)}.{nameof(DurableFlowOptions.MaxFlowIdBytes)} — the Cosmos DB " +
                "id limit). Non-ASCII characters cost up to three bytes each, so a character count alone does not bound it. " + BudgetGuidance;
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

    private static string Excerpt(string flowId)
        => flowId.Length <= 40 ? flowId : string.Concat(flowId.AsSpan(0, 40), "…");

    public static async Task<FlowExecutionLease?> TryAcquireExecutionLeaseAsync(
        IFlowStateStore store,
        string flowId,
        DurableFlowOptions options,
        ILogger logger,
        TimeProvider? timeProvider = null,
        CancellationToken cancellationToken = default)
    {
        ValidateOptions(options);

        var leaseId = Guid.NewGuid().ToString("N");
        if (!await store.TryAcquireLeaseAsync(
                flowId,
                leaseId,
                options.ExecutionLeaseDuration,
                cancellationToken).ConfigureAwait(false))
            return null;

        // The constructor is throw-free after the option bounds above: it only assigns fields,
        // computes a bounded "now + lease" stamp, and starts the renewal loop (whose first
        // Task.Delay faults the loop task, never the constructor). Were that ever to change,
        // lease expiry is the backstop for the persisted row.
        return new FlowExecutionLease(store, flowId, leaseId, options, logger, timeProvider ?? TimeProvider.System);
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
    private DateTime _validUntilUtc;
    private int _disposed;

    public FlowExecutionLease(
        IFlowStateStore store,
        string flowId,
        string leaseId,
        DurableFlowOptions options,
        ILogger logger,
        TimeProvider? timeProvider = null)
    {
        _store = store;
        _flowId = flowId;
        _leaseId = leaseId;
        _options = options;
        _logger = logger;
        _timeProvider = timeProvider ?? TimeProvider.System;
        _validUntilUtc = _timeProvider.GetUtcNow().UtcDateTime.Add(options.ExecutionLeaseDuration);
        _renewal = RenewLoopAsync();
    }

    public CancellationToken LostToken => _lost.Token;

    /// <summary>
    /// Throws when the lease is lost. <paramref name="cause"/> (e.g. the exception that made the
    /// caller check) is attached as the inner exception so the real failure is not discarded.
    /// </summary>
    public void ThrowIfLost(Exception? cause = null)
    {
        if (_lost.IsCancellationRequested)
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
                if (!await _store.TryRenewLeaseAsync(
                        _flowId,
                        _leaseId,
                        _options.ExecutionLeaseDuration,
                        _stop.Token).ConfigureAwait(false))
                {
                    MarkLost();
                    return;
                }

                _validUntilUtc = _timeProvider.GetUtcNow().UtcDateTime.Add(_options.ExecutionLeaseDuration);
            }
            catch (OperationCanceledException) when (_stop.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to renew durable flow {FlowId} execution lease; retrying before expiry.", _flowId);
                if (_timeProvider.GetUtcNow().UtcDateTime >= _validUntilUtc)
                {
                    MarkLost();
                    return;
                }
            }
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
