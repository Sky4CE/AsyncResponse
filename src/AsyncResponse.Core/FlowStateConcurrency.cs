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
        state.Revision = 0;
        return store.TryCreateAsync(flowId, state, ttl, cancellationToken);
    }

    public static async Task<FlowExecutionLease?> TryAcquireExecutionLeaseAsync(
        IFlowStateStore store,
        string flowId,
        DurableFlowOptions options,
        ILogger logger,
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

        try
        {
            return new FlowExecutionLease(store, flowId, leaseId, options, logger);
        }
        catch (Exception constructionFailure)
        {
            // The persisted lease exists but no local owner does — without this release the flow
            // is unexecutable until the lease expires. Defensive: with the option bounds above,
            // no known input makes the constructor throw; if something ever does, the acquired
            // lease must not leak. Best-effort — lease expiry remains the backstop.
            try
            {
                await store.ReleaseLeaseAsync(flowId, leaseId, CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception releaseFailure)
            {
                logger.LogWarning(releaseFailure,
                    "Failed to release the execution lease for flow {FlowId} after lease construction failed; it expires on its own.",
                    flowId);
            }

            logger.LogError(constructionFailure, "Execution-lease construction failed for flow {FlowId}; the acquired lease was released.", flowId);
            throw;
        }
    }

    public static async Task<bool> MutateAsync(
        IFlowStateStore store,
        string flowId,
        TimeSpan ttl,
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
            state.UpdatedAtUtc = DateTime.UtcNow;
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
        // Upper bounds close the "passes validation, throws mid-operation" gap: a
        // TimeSpan.MaxValue StateExpiry overflowed the in-memory store's expiry stamp, an
        // over-ceiling lease/renewal interval failed inside the renewal loop's timers, and an
        // unbounded step timeout would be rejected only at waiter arming — after side effects.
        AsyncResponseChannelOptions.EnsurePersistedTtl(options.StateExpiry, nameof(DurableFlowOptions), nameof(options.StateExpiry));
        if (options.DefaultStepTimeout is { } defaultStepTimeout)
            AsyncResponseChannelOptions.EnsureTimerBacked(defaultStepTimeout, nameof(DurableFlowOptions), nameof(options.DefaultStepTimeout));
        AsyncResponseChannelOptions.EnsureTimerBacked(options.ExecutionLeaseDuration, nameof(DurableFlowOptions), nameof(options.ExecutionLeaseDuration));
        if (options.ExecutionLeaseRenewInterval <= TimeSpan.Zero
            || options.ExecutionLeaseRenewInterval >= options.ExecutionLeaseDuration)
        {
            throw new InvalidOperationException(
                $"{nameof(DurableFlowOptions)}.{nameof(options.ExecutionLeaseRenewInterval)} must be positive and shorter than " +
                $"{nameof(DurableFlowOptions.ExecutionLeaseDuration)}.");
        }
        if (options.ProgressPersistenceInterval < TimeSpan.Zero)
            throw new InvalidOperationException($"{nameof(DurableFlowOptions)}.{nameof(options.ProgressPersistenceInterval)} cannot be negative.");
        if (options.ProgressPersistenceInterval > AsyncResponseChannelOptions.MaxTimerBackedTimeout)
            throw new InvalidOperationException(
                $"{nameof(DurableFlowOptions)}.{nameof(options.ProgressPersistenceInterval)} must be at most " +
                $"{AsyncResponseChannelOptions.MaxTimerBackedTimeout.TotalDays:0.#} days (the .NET timer ceiling).");
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
        ILogger logger)
    {
        _store = store;
        _flowId = flowId;
        _leaseId = leaseId;
        _options = options;
        _logger = logger;
        _validUntilUtc = DateTime.UtcNow.Add(options.ExecutionLeaseDuration);
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
        state.UpdatedAtUtc = DateTime.UtcNow;

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
                await Task.Delay(_options.ExecutionLeaseRenewInterval, _stop.Token).ConfigureAwait(false);
                if (!await _store.TryRenewLeaseAsync(
                        _flowId,
                        _leaseId,
                        _options.ExecutionLeaseDuration,
                        _stop.Token).ConfigureAwait(false))
                {
                    MarkLost();
                    return;
                }

                _validUntilUtc = DateTime.UtcNow.Add(_options.ExecutionLeaseDuration);
            }
            catch (OperationCanceledException) when (_stop.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to renew durable flow {FlowId} execution lease; retrying before expiry.", _flowId);
                if (DateTime.UtcNow >= _validUntilUtc)
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
