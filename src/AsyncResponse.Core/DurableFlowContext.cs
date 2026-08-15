using Microsoft.Extensions.Logging;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;

namespace AsyncResponse;

/// <summary>
/// Runtime <see cref="IDurableFlowContext"/> bound to one execution of one flow run. Owns the
/// checkpointed-flow mechanics so flow code doesn't have to: step guards, result memoization, the
/// pending-correlation-id breadcrumb, fresh-start vs re-attach, and the durable resume/failure
/// callbacks that point back at the flow executor.
/// <para>
/// Not thread-safe by design: a flow body runs sequentially, and <c>until</c> predicates run on
/// the channel's dispatch path only while the flow itself is parked awaiting that same step.
/// </para>
/// </summary>
internal sealed class DurableFlowContext : IDurableFlowContext
{
    private readonly FlowState _state;
    private readonly IFlowStateStore _store;
    private readonly IAsyncResponseBuilder _builder;
    private readonly AsyncResponseContextPropagation _propagation;
    private readonly DurableFlowOptions _options;
    private readonly IAsyncResponseSubscriber _subscriber;
    private readonly IRecoverableAsyncResponseSubscriber? _recoverableSubscriber;
    private readonly ILogger _logger;
    private readonly FlowExecutionLease _lease;
    private readonly TimeProvider _timeProvider;
    private readonly IDurableFlowExecutionObserver[] _observers;
    private readonly IWorkerTransport? _workerTransport;
    private readonly TimeSpan? _channelDefaultWaitTimeout;
    private bool _suspended;
    private bool _progressDirty;
    private DateTime _lastPersistenceUtc;

    /// <summary>
    /// How many ancestors a long park refreshes (see <see cref="ExtendAncestorLedgersAsync"/>);
    /// far beyond any sane child-flow nesting, small enough to bound a corrupted parent cycle.
    /// </summary>
    private const int MaxAncestorLedgerDepth = 16;

    /// <summary>Creates the context for one execution of the given run.</summary>
    public DurableFlowContext(
        FlowState state,
        IFlowStateStore store,
        IAsyncResponseBuilder builder,
        AsyncResponseContextPropagation propagation,
        DurableFlowOptions options,
        IAsyncResponseSubscriber subscriber,
        IRecoverableAsyncResponseSubscriber? recoverableSubscriber,
        ILogger logger,
        FlowExecutionLease lease,
        TimeProvider? timeProvider = null,
        IDurableFlowExecutionObserver[]? observers = null,
        IWorkerTransport? workerTransport = null,
        TimeSpan? channelDefaultWaitTimeout = null)
    {
        _state = state;
        _store = store;
        _builder = builder;
        _propagation = propagation;
        _options = options;
        _subscriber = subscriber;
        _recoverableSubscriber = recoverableSubscriber;
        _logger = logger;
        _lease = lease;
        _timeProvider = timeProvider ?? TimeProvider.System;
        _observers = observers ?? [];
        _workerTransport = workerTransport;
        _channelDefaultWaitTimeout = channelDefaultWaitTimeout;
    }

    private DateTime UtcNow => _timeProvider.GetUtcNow().UtcDateTime;

    /// <summary>
    /// Invokes every registered execution observer. Observers run on the execution path by
    /// contract: an observer exception fails this execution attempt exactly like a step failure
    /// (AsyncResponse.Testing injects deterministic crashes through precisely this).
    /// </summary>
    private async ValueTask NotifyStepAsync(
        Func<IDurableFlowExecutionObserver, DurableFlowStepEvent, ValueTask> invoke,
        string stepName,
        DurableFlowStepKind kind,
        string? correlationId = null,
        DateTime? wakeAtUtc = null)
    {
        if (_observers.Length == 0)
            return;

        var stepEvent = new DurableFlowStepEvent(FlowId, stepName, kind, correlationId, wakeAtUtc);
        foreach (var observer in _observers)
            await invoke(observer, stepEvent).ConfigureAwait(false);
    }

    internal bool IsSuspended => _suspended;

    /// <inheritdoc />
    public string FlowId => _state.FlowId!;

    /// <inheritdoc />
    public async Task StepAsync(string name, Func<Task> step, CancellationToken cancellationToken = default)
    {
        ThrowIfSuspended();
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(step);

        var checkpoint = GetStep(name);
        if (checkpoint.Completed)
            return;

        cancellationToken.ThrowIfCancellationRequested();
        await NotifyStepAsync(static (o, e) => o.OnStepStartingAsync(e), name, DurableFlowStepKind.Local).ConfigureAwait(false);
        await step().ConfigureAwait(false);
        _lease.ThrowIfLost();
        await CompleteStepAsync(name, checkpoint, resultJson: null, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<TResult> StepAsync<TResult>(string name, Func<Task<TResult>> step, CancellationToken cancellationToken = default)
    {
        ThrowIfSuspended();
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(step);

        var checkpoint = GetStep(name);
        if (checkpoint.Completed)
            return DeserializeResult<TResult>(checkpoint.ResultJson);

        cancellationToken.ThrowIfCancellationRequested();
        await NotifyStepAsync(static (o, e) => o.OnStepStartingAsync(e), name, DurableFlowStepKind.Local).ConfigureAwait(false);
        var result = await step().ConfigureAwait(false);
        _lease.ThrowIfLost();
        await CompleteStepAsync(name, checkpoint, AsyncResponseJson.Serialize(result), cancellationToken).ConfigureAwait(false);
        return result;
    }

    /// <inheritdoc />
    public Task DelayAsync(string name, TimeSpan delay, CancellationToken cancellationToken = default)
    {
        ThrowIfSuspended();
        ArgumentException.ThrowIfNullOrWhiteSpace(name);

        var checkpoint = GetStep(name);
        if (checkpoint.Completed)
            return Task.CompletedTask;

        // The due time anchors at the FIRST execution that reaches this step and is checkpointed;
        // replays (crash, redeploy, chunked wake-up) wait out the remainder, never restart. The
        // checkpointed instant therefore wins outright — the argument is not even looked at on a
        // replay, exactly as in DelayUntilAsync: an edit to the delay while a run is mid-sleep
        // must not change that run, and least of all fail it (validating the new argument here
        // turned a parked, perfectly valid timer into a terminal failure on resume).
        if (checkpoint.WakeAtUtc is { } persisted)
            return DelayCoreAsync(name, checkpoint, persisted, cancellationToken);

        // Fresh arrival: validate the requested span BEFORE the UtcNow.Add below, so
        // TimeSpan.MaxValue (or any absurd span) surfaces as the terminal sleep-ceiling failure
        // rather than the DateTime overflow the addition would throw first.
        var requested = delay > TimeSpan.Zero ? delay : TimeSpan.Zero;
        ThrowIfSleepBeyondLedger(name, requested);
        return DelayCoreAsync(name, checkpoint, UtcNow.Add(requested), cancellationToken);
    }

    /// <inheritdoc />
    public Task DelayUntilAsync(string name, DateTimeOffset wakeAtUtc, CancellationToken cancellationToken = default)
    {
        ThrowIfSuspended();
        ArgumentException.ThrowIfNullOrWhiteSpace(name);

        var checkpoint = GetStep(name);
        if (checkpoint.Completed)
            return Task.CompletedTask;

        // The checkpointed instant wins over the argument on replay, so a code edit that changes
        // the target while a run is mid-sleep cannot double- or under-sleep that run.
        return DelayCoreAsync(name, checkpoint, checkpoint.WakeAtUtc ?? wakeAtUtc.UtcDateTime, cancellationToken);
    }

    private async Task DelayCoreAsync(string name, FlowStepState checkpoint, DateTime wakeAtUtc, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        await NotifyStepAsync(static (o, e) => o.OnStepStartingAsync(e), name, DurableFlowStepKind.Timer, wakeAtUtc: wakeAtUtc).ConfigureAwait(false);

        var remaining = wakeAtUtc - UtcNow;
        var firstPass = checkpoint.WakeAtUtc is null;

        // The ledger bound is settled at the FIRST arm, against the options in force then; a
        // replay never re-litigates the checkpointed sleep against the CURRENT options — raising
        // StateExpiry mid-sleep shrinks the recomputed bound and would terminally fail a parked
        // timer that was valid when it was armed, exactly the class of failure the
        // argument-ignoring contract above rules out.
        if (firstPass)
            ThrowIfSleepBeyondLedger(name, remaining);

        // The skew proof rides the wake-up that carried it, and that wake-up targets the ONE step
        // whose due time is already persisted — the parked timer this delivery re-executes.
        // Claiming it here (one-shot) scopes the forced-early fallback to that step alone: an
        // unrelated later timer in the same replay suspends normally instead of inheriting an
        // exemption that would pin it in process for its full remainder, or fail it on the
        // 49.7-day ceiling below.
        var forcedEarly = !firstPass && WorkerJobSkewScope.TryConsumeForcedEarlyExecution();

        if (firstPass)
        {
            // Persist the breadcrumb BEFORE any wake-up can exist, with a TTL that covers the whole
            // sleep plus the normal idle margin — a run must never out-sleep its own ledger.
            checkpoint.WakeAtUtc = wakeAtUtc;
            checkpoint.Faulted = false;
            checkpoint.Message = remaining > TimeSpan.Zero ? $"Sleeping until {wakeAtUtc:O}." : null;
            await SaveForSleepAsync(remaining, cancellationToken).ConfigureAwait(false);
        }

        if (remaining > TimeSpan.Zero)
        {
            await NotifyStepAsync(static (o, e) => o.OnStepWaitingAsync(e), name, DurableFlowStepKind.Timer, wakeAtUtc: wakeAtUtc).ConfigureAwait(false);

            // MaxPublishDelay <= zero marks a transport whose delayed capability is unavailable in
            // the current configuration (an SQS FIFO worker queue): suspend-then-throw would strand
            // the run as "sleeping" with no wake-up, so treat it as not delayed-capable at all.
            //
            // The skew marker rules out suspension for a different reason: this delivery only
            // happened because the executor proved (over consecutive hops) that the transport's
            // delay gate and the stamping clock disagree. Suspending again would enqueue a FRESH
            // wake-up whose stall counters start at zero, discarding that proof and looping
            // forever — so the remainder is waited out in process instead, which honors the due
            // time. That fallback needs no new envelope and is the same one non-delayed transports
            // always take.
            if (remaining > _options.TimerInProcessThreshold
                && !forcedEarly
                && _workerTransport is IDelayedWorkerTransport delayedTransport
                && delayedTransport.MaxPublishDelay > TimeSpan.Zero)
            {
                // Suspend instead of waiting here: the delayed wake-up job re-executes the flow at
                // (or chunked toward) the due time, and this run holds no worker, lease, or memory
                // while it sleeps. Mirrors the child-flow suspension ordering: persist, enqueue,
                // throw — a crash between the persist and the enqueue leaves this job unacked, so
                // broker redelivery re-runs the step and re-enqueues the wake-up.
                await SuspendForTimerAsync(name, wakeAtUtc, remaining, cancellationToken).ConfigureAwait(false);
                throw new InvalidOperationException("Unreachable.");
            }

            if (remaining > AsyncResponseChannelOptions.MaxTimerBackedTimeout)
            {
                throw new DurableFlowFailedException(
                    $"Timer step '{name}' of flow '{FlowId}' sleeps for {remaining.TotalDays:0.#} days, which exceeds the " +
                    $"{AsyncResponseChannelOptions.MaxTimerBackedTimeout.TotalDays:0.#}-day .NET timer ceiling, and " +
                    (forcedEarly
                        ? "this wake-up was released early because the transport's delay gate and the publishing clock disagree, so " +
                          "re-suspending would loop instead of sleeping. Fix the clock skew between the application and the broker/database."
                        : $"the registered worker transport has no native delayed delivery ({nameof(IDelayedWorkerTransport)}) to suspend on. " +
                          "Use a delayed-capable transport (in-memory, Azure Service Bus, SQS, PostgreSQL, SQL Server, MongoDB) for sleeps this long."));
            }

            if (!firstPass)
            {
                // Replayed execution about to resume the sleep in process: the executor's
                // unconditional per-attempt save reset the ledger TTL to StateExpiry, and every
                // store recomputes expiry from "now" — a resumed sleep longer than StateExpiry
                // would out-sleep its own ledger and be silently dropped mid-wait. Re-extend to
                // cover the remainder (the suspend path re-extends every pass in SuspendForTimerAsync).
                await SaveForSleepAsync(remaining, cancellationToken).ConfigureAwait(false);
            }

            await WaitInProcessAsync(remaining, cancellationToken).ConfigureAwait(false);
            _lease.ThrowIfLost();
        }

        await CompleteStepAsync(name, checkpoint, resultJson: null, CancellationToken.None, kind: DurableFlowStepKind.Timer).ConfigureAwait(false);
    }

    /// <summary>
    /// In-process timer wait under the execution lease — the fallback for transports without
    /// delayed delivery and for sub-threshold remainders. Cancellation (caller token, host
    /// shutdown via lease loss) deliberately leaves the checkpoint untouched: the persisted due
    /// time is the breadcrumb, and the redelivered execution waits out the remainder — the timer
    /// itself cannot fault.
    /// </summary>
    private async Task WaitInProcessAsync(TimeSpan remaining, CancellationToken cancellationToken)
    {
        using var linked = cancellationToken.CanBeCanceled
            ? CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _lease.LostToken)
            : null;

        try
        {
            await Task.Delay(remaining, _timeProvider, linked?.Token ?? _lease.LostToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException ex)
        {
            _lease.ThrowIfLost(ex);
            throw;
        }
    }

    private async Task SuspendForTimerAsync(string name, DateTime wakeAtUtc, TimeSpan remaining, CancellationToken cancellationToken)
    {
        _suspended = true;
        _state.LastMessage = $"Flow {FlowId} sleeping until {wakeAtUtc:O} at step '{name}'.";
        await SaveForSleepAsync(remaining, cancellationToken).ConfigureAwait(false);

        var id = FlowId;
        await _builder.EnqueueWorkerAsync<IDurableFlowExecutor>(executor => executor.ExecuteAsync(id), remaining).ConfigureAwait(false);
        throw new DurableFlowSuspendedException(_state.LastMessage);
    }

    /// <summary>
    /// The longest sleep a run's ledger can survive: the persistence ceiling minus the configured
    /// <see cref="DurableFlowOptions.StateExpiry"/>, so the TTL stamped by
    /// <see cref="SaveForSleepAsync"/> (<c>sleep + StateExpiry</c>) always fits the ceiling with
    /// the full idle margin intact. Allowing sleeps up to the ceiling itself would stamp a TTL
    /// that expires exactly at the due instant — any wake latency or store clock skew then finds
    /// the flow state already gone and strands the run unfinished.
    /// </summary>
    private void ThrowIfSleepBeyondLedger(string name, TimeSpan sleep)
    {
        var maxSleep = AsyncResponseChannelOptions.MaxPersistenceTtl - _options.StateExpiry;
        if (sleep <= maxSleep)
            return;

        throw new DurableFlowFailedException(
            $"Timer step '{name}' of flow '{FlowId}' sleeps for {sleep.TotalDays:0} days; the maximum is " +
            $"{maxSleep.TotalDays:0} days — the {AsyncResponseChannelOptions.MaxPersistenceTtl.TotalDays:0}-day persistence ceiling minus " +
            $"{nameof(DurableFlowOptions)}.{nameof(DurableFlowOptions.StateExpiry)} ({_options.StateExpiry.TotalDays:0.#} days) — because ledger TTL " +
            "stamps are computed as \"now + sleep + StateExpiry\" and the ledger must outlive its own wake-up.");
    }

    /// <summary>
    /// Checkpoint save whose TTL covers a known wait window (a timer's sleep, or an awaited step's
    /// timeout): <c>remaining + StateExpiry</c>, saturated at the persistence ceiling. The ordinary
    /// <see cref="SaveAsync"/> TTL bounds <em>idle</em> time between checkpoints; a run parked on a
    /// timer or an awaited response is idle by design for the whole window — and because
    /// <see cref="ThrowIfSleepBeyondLedger"/> caps every sleep at ceiling − StateExpiry (and step
    /// timeouts are timer-bounded far below it), the sum here always carries the full StateExpiry
    /// margin past the due instant.
    /// </summary>
    private async Task SaveForSleepAsync(TimeSpan remaining, CancellationToken cancellationToken)
    {
        var margin = AsyncResponseChannelOptions.MaxPersistenceTtl - _options.StateExpiry;
        var ttl = remaining <= TimeSpan.Zero
            ? _options.StateExpiry
            : remaining >= margin
                ? AsyncResponseChannelOptions.MaxPersistenceTtl
                : remaining + _options.StateExpiry;
        await SaveAsync(cancellationToken, ttl: ttl).ConfigureAwait(false);

        // A parked ancestor's row must survive this run's whole wait, not just its own idle
        // margin: nothing refreshes an ancestor while it waits on this chain (lease renewal only
        // stamps the lease columns), so a descendant parking beyond the ancestor's StateExpiry
        // silently expired the ancestor and the eventual completion wake-up found no state.
        if (ttl > _options.StateExpiry && _state.ParentFlowId is not null)
            await ExtendAncestorLedgersAsync(ttl, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Best-effort TTL refresh of the ancestor chain when this run parks for a window its own
    /// plain <see cref="DurableFlowOptions.StateExpiry"/> would not cover. Only
    /// <see cref="FlowRunStatus.Running"/> ancestors are stamped — a terminal or
    /// operator-suspended run is not waiting on this chain, and an absent row is never
    /// resurrected (the walk stops there and the expired-ancestor failure surfaces on wake-up,
    /// as before). Failures log and return: this run's own checkpoint already latched, and
    /// faulting the park over ancestor insurance would re-run the step for a write the next long
    /// park retries anyway.
    /// </summary>
    private async Task ExtendAncestorLedgersAsync(TimeSpan ttl, CancellationToken cancellationToken)
    {
        var ancestorId = _state.ParentFlowId;
        for (var depth = 0; ancestorId is not null && depth < MaxAncestorLedgerDepth; depth++)
        {
            string? next = null;
            var running = false;
            try
            {
                var found = await FlowStateConcurrency.MutateAsync(
                    _store,
                    ancestorId,
                    ttl,
                    _timeProvider,
                    state =>
                    {
                        next = state.ParentFlowId;
                        running = state.Status == FlowRunStatus.Running;
                        return running;
                    },
                    cancellationToken).ConfigureAwait(false);
                if (!found)
                {
                    _logger.LogWarning(
                        "Flow {FlowId} parked for {Ttl} but ancestor flow {AncestorFlowId} has no state (expired or deleted); its chain keeps the current expiry.",
                        FlowId, ttl, ancestorId);
                    return;
                }
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(
                    ex,
                    "Flow {FlowId} could not extend ancestor flow {AncestorFlowId}'s ledger TTL for its {Ttl} park; the ancestor keeps its current expiry.",
                    FlowId, ancestorId, ttl);
                return;
            }

            if (!running)
                return;
            ancestorId = next;
        }
    }

    /// <inheritdoc />
    public Task<TResponse> AwaitStepAsync<TResponse>(
        string name,
        Func<string, Task> trigger,
        TimeSpan? timeout = null,
        CancellationToken cancellationToken = default) where TResponse : IAsyncResponsePayload
        => AwaitStepCoreAsync<TResponse>(name, trigger, until: null, timeout, cancellationToken);

    /// <inheritdoc />
    public Task<TResponse> AwaitStepAsync<TResponse>(
        string name,
        Func<string, Task> trigger,
        Func<TResponse, bool> until,
        TimeSpan? timeout = null,
        CancellationToken cancellationToken = default) where TResponse : IAsyncResponsePayload
    {
        ArgumentNullException.ThrowIfNull(until);
        return AwaitStepCoreAsync<TResponse>(name, trigger, payload => new ValueTask<bool>(until(payload)), timeout, cancellationToken);
    }

    /// <inheritdoc />
    public Task<TResponse> AwaitStepAsync<TResponse>(
        string name,
        Func<string, Task> trigger,
        Func<TResponse, Task<bool>> until,
        TimeSpan? timeout = null,
        CancellationToken cancellationToken = default) where TResponse : IAsyncResponsePayload
    {
        ArgumentNullException.ThrowIfNull(until);
        return AwaitStepCoreAsync<TResponse>(name, trigger, payload => new ValueTask<bool>(until(payload)), timeout, cancellationToken);
    }

    /// <inheritdoc />
    public Task ReportProgressAsync(string message, CancellationToken cancellationToken = default)
    {
        ThrowIfSuspended();
        _state.LastMessage = message;
        var now = UtcNow;
        if (_options.ProgressPersistenceInterval <= TimeSpan.Zero
            || now - _lastPersistenceUtc >= _options.ProgressPersistenceInterval)
            return SaveAsync(cancellationToken);

        _progressDirty = true;
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public TValue? GetValue<TValue>(string key)
    {
        ThrowIfSuspended();
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        return _state.Values is not null && _state.Values.TryGetValue(key, out var json)
            ? JsonSafety.SafeDeserialize<TValue>(json)
            : default;
    }

    /// <inheritdoc />
    public Task SetValueAsync<TValue>(string key, TValue value, CancellationToken cancellationToken = default)
    {
        ThrowIfSuspended();
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        var values = _state.Values ??= new Dictionary<string, string>(StringComparer.Ordinal);
        values[key] = AsyncResponseJson.Serialize(value);
        return SaveAsync(cancellationToken);
    }

    /// <inheritdoc />
    public async Task<FlowState> AwaitChildFlowAsync<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicMethods | DynamicallyAccessedMemberTypes.Interfaces)] TFlow, TInput>(
        string name,
        TInput input,
        string? flowId = null,
        bool failOnChildFailure = true,
        CancellationToken cancellationToken = default)
        where TFlow : class, IDurableFlow<TInput>
    {
        ThrowIfSuspended();
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(input);
        if (flowId is not null)
            ArgumentException.ThrowIfNullOrWhiteSpace(flowId);

        var checkpoint = GetStep(name);
        var requestedChildFlowId = flowId ?? $"{FlowId}:{name}";
        var breadcrumb = checkpoint.ChildFlowId;
        if (breadcrumb is not null && !string.Equals(breadcrumb, requestedChildFlowId, StringComparison.Ordinal))
        {
            throw new DurableFlowFailedException(
                $"Step '{name}' of flow '{FlowId}' is already bound to child flow id '{breadcrumb}', " +
                $"but this execution requested '{requestedChildFlowId}'. A durable step must keep the same child id on every replay.");
        }

        var childFlowId = breadcrumb ?? requestedChildFlowId;
        if (FlowStateConcurrency.FlowIdNotPortable(childFlowId) is { } rejection)
        {
            // Deterministic on every replay, so terminal rather than retriable: the composed id
            // can never become portable, and the constrained stores would reject the child row
            // anyway after a full budget of wasted redeliveries.
            throw new DurableFlowFailedException(
                $"Step '{name}' of flow '{FlowId}' composed a non-portable child flow id. {rejection}");
        }

        var inputJson = AsyncResponseJson.Serialize(input);
        if (checkpoint.Completed)
        {
            var completedChild = DeserializeResult<FlowState>(checkpoint.ResultJson)
                ?? throw new DurableFlowFailedException(
                    $"Completed child step '{name}' of flow '{FlowId}' has no child-state snapshot.");
            ThrowIfChildMismatched<TFlow, TInput>(completedChild, childFlowId, name, inputJson);
            ThrowIfChildFailed(completedChild, failOnChildFailure);
            return completedChild;
        }

        await NotifyStepAsync(static (o, e) => o.OnStepStartingAsync(e), name, DurableFlowStepKind.ChildFlow).ConfigureAwait(false);

        var child = await _store.LoadAsync(childFlowId, cancellationToken).ConfigureAwait(false);
        if (child is null)
        {
            if (breadcrumb is not null)
            {
                // The breadcrumb is persisted only after the child state exists, so a missing child
                // here means its ledger expired (StateExpiry) or was deleted while this parent was
                // suspended. Its outcome is unknowable; re-running it blind would re-execute side
                // effects of a possibly-completed run. Fail deterministically instead.
                throw new DurableFlowFailedException(
                    $"Child flow '{childFlowId}' has no state (expired or deleted) while parent flow '{FlowId}' was waiting on step '{name}'. " +
                    "Its outcome is unknown, so it is not re-run automatically. Size DurableFlowOptions.StateExpiry beyond the longest child idle time, " +
                    "or start a new parent run to re-execute the work.");
            }

            // Create the child BEFORE persisting the breadcrumb: "breadcrumb exists" must always
            // imply "child state existed", which keeps the expired-child check above sound. A crash
            // between the two writes is safe — the child id is deterministic, so the re-delivered
            // parent execution loads this child instead of re-creating it.
            child = CreateChildState<TFlow, TInput>(childFlowId, name, inputJson);
            if (await FlowStateConcurrency.TryCreateAsync(
                    _store,
                    childFlowId,
                    child,
                    _options.StateExpiry,
                    cancellationToken).ConfigureAwait(false))
            {
                _logger.LogDebug("Flow {FlowId} started child flow {ChildFlowId} for step '{Step}'.", FlowId, childFlowId, name);
            }
            else
            {
                child = await _store.LoadAsync(childFlowId, cancellationToken).ConfigureAwait(false)
                    ?? throw new InvalidOperationException($"Child flow '{childFlowId}' was created concurrently but could not be loaded.");
                ThrowIfChildMismatched<TFlow, TInput>(child, childFlowId, name, inputJson);
            }
        }
        else
        {
            ThrowIfChildMismatched<TFlow, TInput>(child, childFlowId, name, inputJson);
        }

        if (breadcrumb is null)
        {
            checkpoint.ChildFlowId = childFlowId;
            checkpoint.Faulted = false;
            checkpoint.Message = $"Waiting for child flow '{childFlowId}'.";
            await SaveAsync(cancellationToken).ConfigureAwait(false);
        }

        switch (child.Status)
        {
            case FlowRunStatus.Succeeded:
                await CompleteStepAsync(name, checkpoint, FlowStateJson.SerializeSnapshot(child), cancellationToken, kind: DurableFlowStepKind.ChildFlow).ConfigureAwait(false);
                return child;

            case FlowRunStatus.Failed:
                checkpoint.Message = child.LastMessage;
                await CompleteStepAsync(name, checkpoint, FlowStateJson.SerializeSnapshot(child), cancellationToken, faulted: true, kind: DurableFlowStepKind.ChildFlow).ConfigureAwait(false);
                ThrowIfChildFailed(child, failOnChildFailure);
                return child;

            default:
                await NotifyStepAsync(static (o, e) => o.OnStepWaitingAsync(e), name, DurableFlowStepKind.ChildFlow).ConfigureAwait(false);
                await SuspendForChildAsync(childFlowId, cancellationToken).ConfigureAwait(false);
                throw new InvalidOperationException("Unreachable.");
        }
    }

    private async Task<TResponse> AwaitStepCoreAsync<TResponse>(
        string name,
        Func<string, Task> trigger,
        Func<TResponse, ValueTask<bool>>? until,
        TimeSpan? timeout,
        CancellationToken cancellationToken) where TResponse : IAsyncResponsePayload
    {
        ThrowIfSuspended();
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(trigger);

        var checkpoint = GetStep(name);
        if (checkpoint.Completed)
            return DeserializeResult<TResponse>(checkpoint.ResultJson);

        // Re-attach when a previous execution already triggered this step and died waiting; start
        // fresh when there is no breadcrumb or the last attempt faulted (steps are idempotent).
        var reattach = checkpoint.PendingCorrelationId is not null && !checkpoint.Faulted;
        var correlationId = reattach
            ? checkpoint.PendingCorrelationId!
            : AsyncResponseContext.GenerateCorrelationId();
        var stepTimeout = timeout ?? _options.DefaultStepTimeout;
        // The window the ledger must outlive: the resolved step timeout, or — for a timeout-less
        // wait — the channel's declared default waiter timeout, which the channel arms on the
        // waiter below anyway. Null only when the channel declares nothing; such waits keep the
        // historical plain-TTL stamp and re-arm-in-full replays.
        var waitWindow = stepTimeout ?? _channelDefaultWaitTimeout;

        await NotifyStepAsync(static (o, e) => o.OnStepStartingAsync(e), name, DurableFlowStepKind.Awaited, correlationId).ConfigureAwait(false);

        if (reattach && checkpoint.AwaitDeadlineUtc is { } awaitDeadline)
        {
            // The deadline persisted at the FIRST arm is the step's fault clock across
            // executions: a replay arms the REMAINDER, never a fresh full window. Recomputing the
            // window per attempt let every redelivery inside the timeout restart both the fault
            // clock and the ledger TTL from zero — a remote system that never answers produced a
            // run that neither completed nor alarmed, kept alive indefinitely.
            var remainingWindow = awaitDeadline - UtcNow;
            if (remainingWindow <= TimeSpan.Zero)
            {
                // The window elapsed while no execution was live. Recovery may have consumed the
                // response and completed the step in that gap — prefer its checkpoint over a
                // fault (same authority argument as the post-registration short-circuit below).
                if (await TryShortCircuitRecoveredCheckpointAsync(name, checkpoint).ConfigureAwait(false))
                    return DeserializeResult<TResponse>(checkpoint.ResultJson);

                // Settle exactly like the live timeout the waiter would have produced: the fault
                // is recorded so the next execution restarts the step fresh, and the exception
                // propagates as retriable for the transport's bounded redelivery.
                var timedOut = new TimeoutException(
                    $"Timed out waiting for response for correlationId {correlationId}: the await deadline {awaitDeadline:O} " +
                    "elapsed while no execution was live.");
                checkpoint.Faulted = true;
                checkpoint.Message = timedOut.Message;
                await SaveAsync(CancellationToken.None, cause: timedOut).ConfigureAwait(false);
                throw timedOut;
            }

            stepTimeout = remainingWindow;
            waitWindow = remainingWindow;
        }

        var waiter = await CreateWaiterAsync(correlationId, until, stepTimeout, name).ConfigureAwait(false);
        var triggerCompleted = reattach;
        try
        {
            if (reattach && await TryShortCircuitRecoveredCheckpointAsync(name, checkpoint).ConfigureAwait(false))
            {
                // Lost-subscriber recovery checkpointed this step between our state load and the
                // waiter registration. Its wake-up delivery will find OUR lease alive and ack as
                // a duplicate, so nothing would ever wake the parked wait — take the checkpointed
                // result now instead of waiting out the full step timeout for a response that was
                // already consumed. (Recovery always checkpoints BEFORE enqueueing its wake-up,
                // so a completed persisted checkpoint here is authoritative.)
                return DeserializeResult<TResponse>(checkpoint.ResultJson);
            }

            if (!reattach)
            {
                // Persist the breadcrumb AFTER the registration exists and BEFORE the send:
                // "breadcrumb persisted" therefore implies "someone is listening", so a crash on
                // either side of the send re-attaches (or times out and restarts the idempotent
                // step) — never a lost run, never a double-send.
                checkpoint.PendingCorrelationId = correlationId;
                checkpoint.PendingPayloadTypeFullName = typeof(TResponse).FullName;
                checkpoint.Faulted = false;
                checkpoint.Message = null;
                // The fault clock survives crashes and redeliveries only through this stamp (see
                // the re-attach deadline branch above); null when the effective window is unknown,
                // and such waits keep the recompute-per-attempt behavior.
                checkpoint.AwaitDeadlineUtc = waitWindow is { } window ? UtcNow.Add(window) : null;
                // The ledger must outlive the wait it records, exactly as SaveForSleepAsync covers
                // a timer's sleep: with a wait window longer than StateExpiry, a plain-TTL stamp
                // expires the row (and with it the lease renewal's anchor) mid-wait — the lease is
                // marked lost against a row that no longer exists and the run is unrecoverable. A
                // wait whose window is unknown keeps the plain stamp: bounding open-ended idleness
                // is what StateExpiry is documented to do.
                if (waitWindow is { } armWindow)
                    await SaveForSleepAsync(armWindow, cancellationToken).ConfigureAwait(false);
                else
                    await SaveAsync(cancellationToken).ConfigureAwait(false);

                await trigger(correlationId).ConfigureAwait(false);
                triggerCompleted = true;
            }
            else
            {
                // Replayed execution re-attaching to an in-flight wait: the executor's
                // unconditional per-attempt save reset the ledger TTL to StateExpiry, so a wait
                // window longer than StateExpiry would out-live its own ledger and strand the
                // run mid-wait — re-extend to cover the wait, exactly as the fresh path above
                // and the timer path's replay branch do. With a persisted deadline the window is
                // the REMAINDER (shrunk above); a legacy ledger without one re-extends (and
                // re-arms) the full window — its fault clock restarts, the pre-deadline behavior.
                // A window-less re-attach keeps the plain stamp the executor already wrote.
                if (waitWindow is { } replayWindow)
                    await SaveForSleepAsync(replayWindow, cancellationToken).ConfigureAwait(false);

                if (_logger.IsEnabled(LogLevel.Debug))
                {
                    _logger.LogDebug(
                        "Flow {FlowId} step '{Step}' re-attaching to in-flight correlationId {CorrelationId}.",
                        FlowId, name, correlationId);
                }
            }

            await NotifyStepAsync(static (o, e) => o.OnStepWaitingAsync(e), name, DurableFlowStepKind.Awaited, correlationId).ConfigureAwait(false);

            var response = await WaitForResponseAsync(waiter.ResponseTask, cancellationToken).ConfigureAwait(false);

            checkpoint.PendingCorrelationId = null;
            checkpoint.PendingPayloadTypeFullName = null;
            // Deliberately NOT the caller's token: once the response is claimed from the channel
            // it exists nowhere else, so the completion checkpoint must not be interruptible — a
            // cancellation here used to leave `pending` set with the response already consumed,
            // and the redelivered execution re-attached to a correlation id nothing could answer.
            await CompleteStepAsync(name, checkpoint, AsyncResponseJson.Serialize(response), CancellationToken.None, kind: DurableFlowStepKind.Awaited, correlationId: correlationId).ConfigureAwait(false);
            return response;
        }
        catch (OperationCanceledException ex) when (triggerCompleted)
        {
            // A lost lease surfaces as cancellation of the linked wait; convert it with the wait
            // failure attached so the takeover signal does not discard the real cause.
            _lease.ThrowIfLost(ex);

            // SETTLE the handoff before deciding. A point-in-time IsCompletedSuccessfully check
            // raced the channel's dispatch: the response could win the task a moment after the
            // check, leaving a consumed response behind a still-pending ledger. Disposing the
            // waiter cancels its response task unless something already completed it (the channel
            // contract since the dispose-cancels fix), so after this await the task is TERMINAL
            // and the decision below is the race's single authoritative outcome. The finally's
            // second dispose is a no-op behind the subscription's cleanup latch.
            await waiter.DisposeAsync().ConfigureAwait(false);

            if (waiter.ResponseTask.IsCompletedSuccessfully)
            {
                // Delivery won the settlement: the channel claimed and acked that message — it
                // exists nowhere else, and re-attaching to its consumed correlation id would park
                // the run until the step timeout. The checkpoint therefore wins over the
                // cancellation: persist the received payload and return it; the caller's token
                // gets its say again at the next step boundary.
                var received = waiter.ResponseTask.Result;
                checkpoint.PendingCorrelationId = null;
                await CompleteStepAsync(name, checkpoint, AsyncResponseJson.Serialize(received), CancellationToken.None, kind: DurableFlowStepKind.Awaited, correlationId: correlationId).ConfigureAwait(false);
                return received;
            }

            if (waiter.ResponseTask.IsFaulted)
            {
                // The wait FAULTED — a throwing Until predicate (possibly between the catch
                // filter and the settlement), or the disposal drain abandoning a wedged delivery
                // as AsyncResponseIndeterminateDeliveryException. Either way the message may be
                // consumed: restart the idempotent step fresh, exactly like the general fault
                // path below. The checkpoint records the fault's own message (not the
                // cancellation's) so the ledger says WHY the step restarts.
                var fault = waiter.ResponseTask.Exception?.GetBaseException();
                checkpoint.Faulted = true;
                checkpoint.Message = fault?.Message ?? ex.Message;
                await SaveAsync(CancellationToken.None, cause: fault ?? ex).ConfigureAwait(false);
                throw;
            }

            // Cancellation won the settlement (the task is now canceled; nothing was delivered).
            // WAIT-SIDE cancellation is infrastructure, not a step verdict: the channel cancels
            // in-flight waiters when it is disposed at host shutdown, and the caller's token
            // means "stop this execution", not "the step failed" — the remote operation is still
            // in flight. The persisted breadcrumb must survive untouched so the redelivered
            // execution RE-ATTACHES to the same correlation id; marking the checkpoint faulted
            // here turned every graceful shutdown mid-await into a fresh-correlation restart that
            // re-sent the remote request. (A response that never arrives still faults via the
            // step timeout.)
            //
            // The filter keeps this branch away from TRIGGER-thrown cancellation (an HttpClient
            // timeout surfaces as TaskCanceledException): the request may never have left the
            // process, so that case falls through to the fault path below and restarts fresh.
            throw;
        }
        catch (Exception ex)
        {
            // Timeout, trigger failure (including trigger-thrown cancellation), or a faulted
            // wait: record it so the next execution restarts this step fresh instead of
            // re-attaching to a dead correlation id. The original failure rides along as `cause`
            // so a rejected save cannot displace it.
            checkpoint.Faulted = true;
            checkpoint.Message = ex.Message;
            await SaveAsync(CancellationToken.None, cause: ex).ConfigureAwait(false);
            throw;
        }
        finally
        {
            await waiter.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Re-reads the persisted step checkpoint after the re-attach waiter registration exists and,
    /// when recovery already completed the step, syncs the in-memory ledger so the caller can
    /// short-circuit. Best-effort: a store read failure logs and falls through to the normal wait
    /// (the behavior before this check existed) rather than faulting the step.
    /// </summary>
    private async Task<bool> TryShortCircuitRecoveredCheckpointAsync(string name, FlowStepState checkpoint)
    {
        try
        {
            var persisted = await _store.LoadAsync(FlowId).ConfigureAwait(false);
            if (persisted?.Steps is null
                || !persisted.Steps.TryGetValue(name, out var persistedStep)
                || !persistedStep.Completed)
            {
                return false;
            }

            // Adopt ONLY while the persisted run is still Running. The revision sync below makes
            // the next checkpoint's CAS succeed, so adopting the revision of a writer that ALSO
            // transitioned the run (RecoverAsync's escalation into FailAsync marking it Failed, an
            // operator parking it) would let this execution's next save write the stale in-memory
            // Status/LastMessage over that transition — resurrecting a terminally Failed run.
            // Falling through keeps the stale revision, so the next save loses the CAS and the
            // delivery abandons and retries: the documented outcome for losing a concurrent write.
            if (persisted.Status is not FlowRunStatus.Running)
                return false;

            // Sync the ledger revision too: the recovery write that completed this step advanced
            // it, and a stale in-memory revision would fail the NEXT checkpoint's compare-and-swap
            // — aborting every execution that took this short-circuit as a phantom "concurrent
            // write" and forcing a pointless redelivery.
            _state.Revision = persisted.Revision;
            checkpoint.Completed = true;
            checkpoint.ResultJson = persistedStep.ResultJson;
            checkpoint.PendingCorrelationId = null;
            checkpoint.PendingPayloadTypeFullName = null;
            checkpoint.Faulted = false;
            checkpoint.Message = persistedStep.Message;
            checkpoint.CompletedAtUtc = persistedStep.CompletedAtUtc;
            return true;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "Flow {FlowId} step '{Step}' could not re-read its checkpoint before re-attaching; continuing with the normal wait.",
                FlowId, name);
            return false;
        }
    }

    private async Task<IAsyncResponseWaiter<TResponse>> CreateWaiterAsync<TResponse>(
        string correlationId,
        Func<TResponse, ValueTask<bool>>? until,
        TimeSpan? timeout,
        string stepName) where TResponse : IAsyncResponsePayload
    {
        if (_recoverableSubscriber is not null)
        {
            // The durable safety net: a response landing while no process is executing this flow
            // checkpoints the terminal payload and re-enqueues the run, or terminally fails it —
            // the same at-least-once, idempotency-required contract as hand-registered callbacks.
            var flowId = FlowId;
            Expression<Func<IDurableFlowExecutor, Task>> resume = executor => executor.RecoverAsync(
                flowId,
                Placeholder.Payload<TResponse>()!,
                Placeholder.CorrelationId());
            Expression<Func<IDurableFlowExecutor, Task>> failure = executor => executor.FailAsync(flowId, Placeholder.Exception());

            return await _recoverableSubscriber.CreateRecoverableResponseWaiter(
                correlationId,
                CallbackExpressionConverter.ToReflectionCall(resume),
                CallbackExpressionConverter.ToReflectionCall(failure),
                until,
                timeout).ConfigureAwait(false);
        }

        _logger.LogDebug(
            "Flow {FlowId} step '{Step}': the configured channel exposes no recoverable subscriber; lost-subscriber recovery is unavailable for this wait.",
            FlowId, stepName);

        return await _subscriber.CreateResponseWaiter(correlationId, until, timeout).ConfigureAwait(false);
    }

    private FlowState CreateChildState<TFlow, TInput>(string flowId, string parentStepName, string inputJson)
    {
        var now = UtcNow;
        return new FlowState
        {
            FlowId = flowId,
            FlowTypeName = typeof(TFlow).FullName,
            InputTypeName = typeof(TInput).FullName,
            InputJson = inputJson,
            Status = FlowRunStatus.Running,
            LastMessage = $"Child flow started by {FlowId}.",
            CreatedAtUtc = now,
            UpdatedAtUtc = now,
            ParentFlowId = FlowId,
            ParentStepName = parentStepName,
            Context = _propagation.Capture()
        };
    }

    private Task EnqueueChildAsync(string childFlowId)
    {
        var id = childFlowId;
        return _builder.EnqueueWorkerAsync<IDurableFlowExecutor>(executor => executor.ExecuteAsync(id));
    }

    private async Task SuspendForChildAsync(string childFlowId, CancellationToken cancellationToken)
    {
        // Persist the suspension BEFORE the child becomes runnable: once the child is enqueued it
        // can complete and re-execute this parent on another worker at any moment, and a save after
        // that point would clobber the re-execution's newer checkpoints with this stale snapshot.
        // The executor therefore does NOT save again on the suspension path.
        _suspended = true;
        _state.LastMessage = $"Flow {FlowId} suspended waiting for child flow {childFlowId}.";
        await SaveAsync(cancellationToken).ConfigureAwait(false);
        await EnqueueChildAsync(childFlowId).ConfigureAwait(false);
        throw new DurableFlowSuspendedException(_state.LastMessage);
    }

    private void ThrowIfSuspended()
    {
        _lease.ThrowIfLost();
        if (_suspended)
            throw new DurableFlowSuspendedException(_state.LastMessage ?? $"Flow {FlowId} is suspended.");
    }

    private static void ThrowIfChildFailed(FlowState child, bool failOnChildFailure)
    {
        if (failOnChildFailure && child.Status == FlowRunStatus.Failed)
            throw new DurableFlowFailedException($"Child flow '{child.FlowId}' failed: {child.LastMessage ?? "no message"}");
    }

    private void ThrowIfChildMismatched<TFlow, TInput>(
        FlowState child,
        string childFlowId,
        string stepName,
        string requestedInputJson)
    {
        // A child id is owned by exactly one parent: the notification that resumes a suspended
        // parent follows the child's single ParentFlowId, so a second parent awaiting the same id
        // would suspend and never wake. Reject collisions loudly instead of parking forever.
        if (!string.Equals(child.ParentFlowId, FlowId, StringComparison.Ordinal))
        {
            var owner = child.ParentFlowId is null ? "a run not started by AwaitChildFlowAsync" : $"parent flow '{child.ParentFlowId}'";
            throw new DurableFlowFailedException(
                $"Step '{stepName}' of flow '{FlowId}' awaits child flow id '{childFlowId}', but that id belongs to {owner}. " +
                "Child flow ids are exclusive to the parent that started them — pass a flowId that is unique per parent run " +
                "(the default '{parentFlowId}:{stepName}' id is always safe).");
        }

        if (!string.Equals(child.FlowId, childFlowId, StringComparison.Ordinal)
            || !string.Equals(child.ParentStepName, stepName, StringComparison.Ordinal))
        {
            throw new DurableFlowFailedException(
                $"Child flow id '{childFlowId}' is bound to a different child step than '{stepName}' of parent flow '{FlowId}'. " +
                "A child id is exclusive to one parent step.");
        }

        if (!string.Equals(child.FlowTypeName, typeof(TFlow).FullName, StringComparison.Ordinal))
        {
            throw new DurableFlowFailedException(
                $"Step '{stepName}' of flow '{FlowId}' awaits child flow id '{childFlowId}' as {typeof(TFlow).FullName}, " +
                $"but the persisted run is {child.FlowTypeName}. The flowId collides with a different flow — use a unique child id.");
        }

        if (!string.Equals(child.InputTypeName, typeof(TInput).FullName, StringComparison.Ordinal)
            || !FlowStateJson.JsonEquivalent(child.InputJson, requestedInputJson))
        {
            throw new DurableFlowFailedException(
                $"Step '{stepName}' of flow '{FlowId}' requested child flow id '{childFlowId}' with a different input " +
                "type or value than the persisted child. Replays must use semantically identical child input.");
        }
    }

    private FlowStepState GetStep(string name)
    {
        var steps = _state.Steps ??= new Dictionary<string, FlowStepState>(StringComparer.Ordinal);
        if (!steps.TryGetValue(name, out var step))
        {
            step = new FlowStepState();
            steps[name] = step;
        }

        return step;
    }

    private async Task CompleteStepAsync(
        string name,
        FlowStepState step,
        string? resultJson,
        CancellationToken cancellationToken,
        bool faulted = false,
        DurableFlowStepKind kind = DurableFlowStepKind.Local,
        string? correlationId = null)
    {
        step.Completed = true;
        step.ResultJson = resultJson;
        step.PendingCorrelationId = null;
        // Cleared together with the breadcrumb on EVERY settlement path (the FlowState contract):
        // a stale declared-type name on a completed step would mislead the next recovery pass.
        step.PendingPayloadTypeFullName = null;
        // A memoized failed child keeps Faulted = true so operators can spot the failure on the
        // step itself instead of digging through ResultJson.
        step.Faulted = faulted;
        step.CompletedAtUtc = UtcNow;
        _state.LastMessage = faulted ? $"Step '{name}' completed (child flow failed)." : $"Step '{name}' completed.";
        await SaveAsync(cancellationToken).ConfigureAwait(false);

        if (_logger.IsEnabled(LogLevel.Debug))
            _logger.LogDebug("Flow {FlowId} step '{Step}' completed.", FlowId, name);

        await NotifyStepAsync(static (o, e) => o.OnStepCompletedAsync(e), name, kind, correlationId, step.WakeAtUtc).ConfigureAwait(false);
    }

    internal Task FlushProgressAsync()
        => _progressDirty ? SaveAsync(CancellationToken.None) : Task.CompletedTask;

    private async Task SaveAsync(CancellationToken cancellationToken, Exception? cause = null, TimeSpan? ttl = null)
    {
        _state.UpdatedAtUtc = UtcNow;
        await _lease.SaveAsync(_state, ttl ?? _options.StateExpiry, cancellationToken, cause).ConfigureAwait(false);

        _progressDirty = false;
        _lastPersistenceUtc = UtcNow;
    }

    private async Task<TResponse> WaitForResponseAsync<TResponse>(Task<TResponse> responseTask, CancellationToken cancellationToken)
    {
        if (!cancellationToken.CanBeCanceled)
            return await responseTask.WaitAsync(_lease.LostToken).ConfigureAwait(false);

        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _lease.LostToken);
        return await responseTask.WaitAsync(linked.Token).ConfigureAwait(false);
    }

    private static TResult DeserializeResult<TResult>(string? resultJson)
        => resultJson is null ? default! : JsonSafety.SafeDeserialize<TResult>(resultJson)!;
}
