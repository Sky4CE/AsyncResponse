using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System.Diagnostics.CodeAnalysis;

namespace AsyncResponse;

/// <summary>
/// Executes durable flow runs. Its methods are the durable targets behind every flow: worker jobs
/// carry <see cref="ExecuteAsync"/>, and awaited steps register <see cref="RecoverAsync"/> /
/// <see cref="FailAsync"/> as their lost-subscriber callbacks — invoked by whichever process
/// receives a late response, possibly a different deployment.
/// <para>
/// <b>Naming contract:</b> like all recovery callbacks, these targets are persisted as
/// interface/method name strings and live in stores for up to the configured expiry. The
/// interface and method names must stay stable across deployments.
/// </para>
/// </summary>
public interface IDurableFlowExecutor
{
    /// <summary>
    /// Runs the flow body for <paramref name="flowId"/> from the top: completed steps skip via
    /// their checkpoints, the in-flight awaited step re-attaches. No-op for terminal runs.
    /// </summary>
    Task ExecuteAsync(string flowId);

    /// <summary>
    /// Lost-subscriber resume target: re-enqueues <see cref="ExecuteAsync"/> on the worker
    /// transport (never runs the flow inline on a publisher's dispatch path).
    /// </summary>
    Task ResumeAsync(string flowId);

    /// <summary>
    /// Lost-subscriber success target: checkpoints the terminal payload into the matching pending
    /// step before re-enqueueing execution, so recovery does not wait for a consumed correlation id.
    /// </summary>
    Task RecoverAsync(string flowId, object payload, string correlationId);

    /// <summary>Lost-subscriber failure target: marks the run terminally <see cref="FlowRunStatus.Failed"/>.</summary>
    Task FailAsync(string flowId, Exception exception);
}

/// <inheritdoc cref="IDurableFlowExecutor" />
internal sealed class DurableFlowExecutor : IDurableFlowExecutor
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly IAsyncResponseBuilder _builder;
    private readonly IAsyncResponseSubscriber _subscriber;
    private readonly IRecoverableAsyncResponseSubscriber? _recoverableSubscriber;
    private readonly AsyncResponseContextPropagation _propagation;
    private readonly DurableFlowOptions _options;
    private readonly ILogger<DurableFlowExecutor> _logger;
    private readonly TimeProvider _timeProvider;
    private readonly IDurableFlowExecutionObserver[] _observers;
    private readonly IWorkerTransport? _workerTransport;
    private readonly TimeSpan? _channelDefaultWaitTimeout;
    private readonly Dictionary<string, DurableFlowRegistration> _registrations;
    private readonly CancellationToken _hostStopping;

    /// <summary>Creates the flow executor.</summary>
    public DurableFlowExecutor(
        IServiceScopeFactory scopeFactory,
        IAsyncResponseBuilder builder,
        IAsyncResponseSubscriber subscriber,
        IRecoverableAsyncResponseSubscriber? recoverableSubscriber,
        AsyncResponseContextPropagation propagation,
        DurableFlowOptions options,
        ILogger<DurableFlowExecutor> logger,
        IEnumerable<DurableFlowRegistration>? registrations = null,
        Microsoft.Extensions.Hosting.IHostApplicationLifetime? hostLifetime = null,
        TimeProvider? timeProvider = null,
        IEnumerable<IDurableFlowExecutionObserver>? observers = null,
        IWorkerTransport? workerTransport = null,
        TimeSpan? channelDefaultWaitTimeout = null)
    {
        _workerTransport = workerTransport;
        _channelDefaultWaitTimeout = channelDefaultWaitTimeout;
        _scopeFactory = scopeFactory;
        _builder = builder;
        _subscriber = subscriber;
        _recoverableSubscriber = recoverableSubscriber;
        _propagation = propagation;
        _options = options;
        FlowStateConcurrency.ValidateOptions(_options);
        _logger = logger;
        _timeProvider = timeProvider ?? TimeProvider.System;
        _observers = observers?.ToArray() ?? [];
        _hostStopping = hostLifetime?.ApplicationStopping ?? CancellationToken.None;
        _registrations = new Dictionary<string, DurableFlowRegistration>(StringComparer.Ordinal);
        foreach (var registration in registrations ?? [])
        {
            // Last registration wins, matching DI's usual override semantics.
            _registrations[registration.FlowTypeFullName] = registration;
        }
    }

    /// <inheritdoc />
    public async Task ExecuteAsync(string flowId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(flowId);

        await using var scope = _scopeFactory.CreateAsyncScope();
        var store = scope.ServiceProvider.GetRequiredService<IFlowStateStore>();

        await using var lease = await AcquireExecutionLeaseWithRetryAsync(store, flowId).ConfigureAwait(false);
        if (lease is null)
            return;

        var state = await store.LoadAsync(flowId).ConfigureAwait(false);
        if (state is null)
        {
            _logger.LogWarning("Durable flow {FlowId} has no state (unknown, expired, or unreadable); nothing to execute.", flowId);
            return;
        }

        if (state.Status != FlowRunStatus.Running)
        {
            _logger.LogDebug("Durable flow {FlowId} is already {Status}; skipping execution.", flowId, state.Status);
            // Re-notify terminal runs on duplicate deliveries: the ORIGINAL delivery's
            // run-finished notification (below, outside the try/catch) may itself have thrown and
            // caused this redelivery — skipping here would lose the terminal event forever.
            // Observer delivery is at-least-once by contract; implementations tolerate duplicates.
            if (state.Status is FlowRunStatus.Succeeded or FlowRunStatus.Failed)
                await NotifyRunFinishedAsync(state).ConfigureAwait(false);
            await NotifyParentAsync(state).ConfigureAwait(false);
            return;
        }

        using var activity = AsyncResponseDiagnostics.StartActivity("asyncresponse.flow.execute");
        activity?.SetTag("asyncresponse.flow_id", flowId);
        activity?.SetTag("asyncresponse.flow_type", state.FlowTypeName);

        state.Attempts++;
        await lease.SaveAsync(state, _options.StateExpiry).ConfigureAwait(false);

        // The run may be resumed by a different deployment than the one that started it: restore
        // the ambient context captured at start before any flow code runs.
        using var ambientScope = _propagation.Restore(state.Context);

        try
        {
            var suspended = await InvokeFlowAsync(scope.ServiceProvider, store, state, lease).ConfigureAwait(false);
            if (suspended)
            {
                // The context persisted the suspended state BEFORE enqueueing the child; saving here
                // could overwrite newer checkpoints written by a parent re-execution the child has
                // already triggered on another worker.
                _logger.LogDebug("Durable flow {FlowId} suspended: {Message}", flowId, state.LastMessage);
                return;
            }

            state.Status = FlowRunStatus.Succeeded;
            state.LastMessage = "Flow completed.";
            await lease.SaveAsync(state, _options.StateExpiry).ConfigureAwait(false);

            _logger.LogInformation("Durable flow {FlowId} completed successfully (attempt {Attempts}).", flowId, state.Attempts);
        }
        catch (DurableFlowSuspendedException ex)
        {
            // Same as the IsSuspended return above: the suspended state is already persisted, and a
            // save here races the child-triggered parent re-execution.
            _logger.LogDebug("Durable flow {FlowId} suspended: {Message}", flowId, ex.Message);
            return;
        }
        catch (DurableFlowFailedException ex)
        {
            // Terminal by declaration: mark failed and swallow so the transport acks the job.
            state.Status = FlowRunStatus.Failed;
            state.LastMessage = ex.Message;
            await lease.SaveAsync(state, _options.StateExpiry, cause: ex).ConfigureAwait(false);

            AsyncResponseDiagnostics.SetError(activity, ex);
            _logger.LogWarning(ex, "Durable flow {FlowId} failed terminally: {Message}", flowId, ex.Message);
        }
        catch (Exception ex) when (lease.LostToken.IsCancellationRequested)
        {
            AsyncResponseDiagnostics.SetError(activity, ex);
            throw;
        }
        catch (Exception ex)
        {
            state.LastMessage = ex.Message;
            await lease.SaveAsync(state, _options.StateExpiry, cause: ex).ConfigureAwait(false);

            AsyncResponseDiagnostics.SetError(activity, ex);

            // Retriable: propagate so the worker transport redelivers the run with bounded
            // attempts and dead-letters it when they are exhausted — the "run is stuck" alarm.
            throw;
        }

        // The terminal outcome is persisted above; notify OUTSIDE the try/catch so a throwing
        // observer (throwing is the documented crash-injection contract) cannot re-enter those
        // catches and rewrite a Succeeded run as Failed — or overwrite a terminal ledger message
        // with a telemetry error — "the run's outcome is never at stake". A throw from here
        // propagates for redelivery; the replay sees the terminal status, RE-NOTIFIES (so the
        // event that just failed is not lost), and acks.
        await NotifyRunFinishedAsync(state).ConfigureAwait(false);
        await NotifyParentAsync(state).ConfigureAwait(false);
    }

    /// <summary>
    /// Acquires the execution lease for <paramref name="flowId"/>, retrying while the current
    /// holder's lease window elapses. Returns <c>null</c> when this delivery is safe to ack without
    /// executing (flow terminal/absent, or the lease is held by a demonstrably live worker).
    /// <para>
    /// A held lease alone is NOT proof this delivery is a duplicate: the holder may have died
    /// inside its unexpired lease window, and acking would drop the only wake-up the flow has —
    /// wake-ups would silently become at-most-once and the <see cref="FlowRunStatus.Running"/> run
    /// would strand. A dead holder's lease expires within <see cref="DurableFlowOptions.ExecutionLeaseDuration"/>,
    /// so polling for one full duration + renew interval guarantees this delivery either takes over
    /// (checkpoints make the re-run idempotent) or proves the holder alive.
    /// </para>
    /// </summary>
    private async Task<FlowExecutionLease?> AcquireExecutionLeaseWithRetryAsync(IFlowStateStore store, string flowId)
    {
        var lease = await FlowStateConcurrency.TryAcquireExecutionLeaseAsync(
            store,
            flowId,
            _options,
            _logger,
            _timeProvider).ConfigureAwait(false);
        if (lease is not null)
            return lease;

        // Poll ceiling: a lease can only stay held past its duration if the holder renewed it, so
        // one full duration + renew interval of failed acquires proves the holder is alive. The 2s
        // poll delay is capped by the renew interval so short test-sized leases still get polled.
        var deadline = _timeProvider.GetUtcNow().UtcDateTime + _options.ExecutionLeaseDuration + _options.ExecutionLeaseRenewInterval;
        var pollDelay = _options.ExecutionLeaseRenewInterval < TimeSpan.FromSeconds(2)
            ? _options.ExecutionLeaseRenewInterval
            : TimeSpan.FromSeconds(2);

        while (true)
        {
            // Between attempts, look at the state itself: a terminal or absent flow needs no
            // execution, and reporting it accurately beats a misleading "already executing" log.
            var state = await store.LoadAsync(flowId).ConfigureAwait(false);
            if (state is null)
            {
                _logger.LogWarning("Durable flow {FlowId} has no state (unknown, expired, or unreadable); nothing to execute.", flowId);
                return null;
            }

            if (state.Status != FlowRunStatus.Running)
            {
                _logger.LogDebug("Durable flow {FlowId} is already {Status}; skipping duplicate delivery.", flowId, state.Status);
                // Same at-least-once re-notify as ExecuteAsync's terminal early return: the prior
                // delivery may have died in the run-finished notification itself.
                if (state.Status is FlowRunStatus.Succeeded or FlowRunStatus.Failed)
                    await NotifyRunFinishedAsync(state).ConfigureAwait(false);
                await NotifyParentAsync(state).ConfigureAwait(false);
                return null;
            }

            lease = await FlowStateConcurrency.TryAcquireExecutionLeaseAsync(
                store,
                flowId,
                _options,
                _logger,
                _timeProvider).ConfigureAwait(false);
            if (lease is not null)
                return lease;

            if (_timeProvider.GetUtcNow().UtcDateTime >= deadline)
                break;

            try
            {
                await Task.Delay(pollDelay, _timeProvider, _hostStopping).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Host shutdown must not leave this delivery parked in the poll — but acking it
                // would silently drop the flow's only wake-up. Propagate as cancellation so the
                // transport treats the job as not executed and redelivers it after restart.
                throw new OperationCanceledException(
                    $"Host is stopping; durable flow '{flowId}' wake-up is abandoned for redelivery.");
            }
        }

        // The lease survived a full duration + renew window, so the holder is alive and renewing.
        // Every execution is driven by a worker job that stays unacked until its handler completes,
        // so if that live holder crashes later the broker redelivers its own job — this delivery is
        // genuinely redundant and safe to ack. The retry loop above exists purely to cover
        // deliveries that arrive inside a DEAD holder's unexpired lease window, which broker
        // redelivery alone cannot cover.
        _logger.LogDebug(
            "Durable flow {FlowId} is executing on another live worker (lease renewed through the full wait window); skipping duplicate delivery.",
            flowId);
        return null;
    }

    /// <inheritdoc />
    public async Task ResumeAsync(string flowId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(flowId);

        await using var scope = _scopeFactory.CreateAsyncScope();
        var store = scope.ServiceProvider.GetRequiredService<IFlowStateStore>();

        var state = await store.LoadAsync(flowId).ConfigureAwait(false);
        if (state is null)
        {
            _logger.LogWarning("Durable flow {FlowId} cannot resume: no state (unknown, expired, or unreadable).", flowId);
            return;
        }

        if (state.Status != FlowRunStatus.Running)
        {
            _logger.LogDebug("Durable flow {FlowId} is already {Status}; ignoring resume.", flowId, state.Status);
            return;
        }

        _logger.LogDebug("Durable flow {FlowId} resuming via worker transport.", flowId);
        await _builder.EnqueueWorkerAsync<IDurableFlowExecutor>(executor => executor.ExecuteAsync(flowId)).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task RecoverAsync(string flowId, object payload, string correlationId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(flowId);
        ArgumentNullException.ThrowIfNull(payload);
        ArgumentException.ThrowIfNullOrWhiteSpace(correlationId);

        await using var scope = _scopeFactory.CreateAsyncScope();
        var store = scope.ServiceProvider.GetRequiredService<IFlowStateStore>();
        var checkpointed = false;
        var running = false;
        var lastStatus = FlowRunStatus.Running;
        string? recoveredStep = null;

        var found = await FlowStateConcurrency.MutateAsync(
            store,
            flowId,
            _options.StateExpiry,
            _timeProvider,
            state =>
            {
                checkpointed = false;
                recoveredStep = null;
                lastStatus = state.Status;
                running = state.Status == FlowRunStatus.Running;

                // Suspended runs still CHECKPOINT the recovered terminal payload — the response
                // exists nowhere else once this callback returns — but are never woken (see
                // below): suspension means an operator took manual control, and ResumeAsync
                // continues from the checkpoint instead of re-running the remote step.
                var checkpointable = running || state.Status == FlowRunStatus.Suspended;
                if (!checkpointable || state.Steps is null)
                    return false;

                var pending = state.Steps.FirstOrDefault(pair =>
                    string.Equals(pair.Value.PendingCorrelationId, correlationId, StringComparison.Ordinal));
                if (pending.Value is null)
                    return false;

                pending.Value.Completed = true;
                pending.Value.ResultJson = SerializeRecoveredResult(payload, pending.Value.PendingPayloadTypeFullName);
                pending.Value.PendingCorrelationId = null;
                pending.Value.PendingPayloadTypeFullName = null;
                pending.Value.Faulted = false;
                pending.Value.Message = "Terminal response recovered after subscriber loss.";
                pending.Value.CompletedAtUtc = _timeProvider.GetUtcNow().UtcDateTime;
                state.LastMessage = $"Step '{pending.Key}' recovered after subscriber loss.";
                checkpointed = true;
                recoveredStep = pending.Key;
                return true;
            }).ConfigureAwait(false);

        if (!found)
        {
            _logger.LogWarning("Durable flow {FlowId} cannot recover response {CorrelationId}: no state found.", flowId, correlationId);
            return;
        }

        if (!checkpointed)
        {
            if (!running)
            {
                _logger.LogDebug("Durable flow {FlowId} is {Status}; ignoring recovered correlationId {CorrelationId}.", flowId, lastStatus, correlationId);
                return;
            }

            // Still Running with no matching pending step: a previous delivery may have crashed
            // between checkpointing this response and enqueueing the run, making this redelivery
            // the only remaining wake-up. Re-enqueue instead of dropping — it is idempotent, and
            // worst case the job finds a live holder's lease and acks as a duplicate.
            _logger.LogDebug("Durable flow {FlowId} has no pending step for recovered correlationId {CorrelationId}; re-enqueueing execution to cover a checkpoint/enqueue crash window.", flowId, correlationId);
            await _builder.EnqueueWorkerAsync<IDurableFlowExecutor>(executor => executor.ExecuteAsync(flowId)).ConfigureAwait(false);
            return;
        }

        // The completion recorded here is the ONLY chance observers get to see this step finish:
        // the replayed execution short-circuits the now-memoized step without notifying. Notified
        // before the wake-up is enqueued so observers (e.g. the Testing probe's step waiters) see
        // the completion before the resumed run races past it. Best-effort by necessity: once the
        // checkpoint settles, the pending correlation id is gone, and a redelivered RecoverAsync
        // can no longer tell which step this response completed — an observer that throws here
        // loses the event (unlike run-finished, which re-derives from the persisted status).
        await NotifyStepCompletedAsync(flowId, recoveredStep!, correlationId).ConfigureAwait(false);

        if (!running)
        {
            _logger.LogInformation(
                "Durable flow {FlowId} checkpointed recovered correlationId {CorrelationId} while Suspended; not waking the run — ResumeAsync continues from the checkpoint.",
                flowId, correlationId);
            return;
        }

        _logger.LogDebug("Durable flow {FlowId} checkpointed recovered correlationId {CorrelationId}; resuming.", flowId, correlationId);
        await _builder.EnqueueWorkerAsync<IDurableFlowExecutor>(executor => executor.ExecuteAsync(flowId)).ConfigureAwait(false);
    }

    private async Task NotifyStepCompletedAsync(string flowId, string stepName, string correlationId)
    {
        if (_observers.Length == 0)
            return;

        var stepEvent = new DurableFlowStepEvent(flowId, stepName, DurableFlowStepKind.Awaited, correlationId, WakeAtUtc: null);
        foreach (var observer in _observers)
            await observer.OnStepCompletedAsync(stepEvent).ConfigureAwait(false);
    }

    /// <summary>
    /// Serializes a recovered terminal payload for the step checkpoint AS THE STEP'S DECLARED
    /// response type (recorded at await time): replay deserializes the checkpoint as that declared
    /// type, and the recovered payload's runtime type may be a <c>[JsonPolymorphic]</c> derived
    /// whose runtime-type serialization omits the discriminator — breaking every replay against an
    /// abstract declared base, or silently truncating against a concrete one. Falls back to the
    /// runtime type when the recorded name is absent (ledgers written before it existed) or no
    /// longer resolves to a compatible type.
    /// </summary>
    private static string SerializeRecoveredResult(object payload, string? declaredTypeFullName)
    {
        var declaredType = declaredTypeFullName is null
            ? null
            : PayloadRecoveryClassifier.ResolvePayloadType(declaredTypeFullName);

        return declaredType is not null && declaredType.IsInstanceOfType(payload)
            ? AsyncResponseJson.Serialize(payload, declaredType)
            : AsyncResponseJson.Serialize(payload, payload.GetType());
    }

    /// <inheritdoc />
    public async Task FailAsync(string flowId, Exception exception)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(flowId);
        ArgumentNullException.ThrowIfNull(exception);

        await using var scope = _scopeFactory.CreateAsyncScope();
        var store = scope.ServiceProvider.GetRequiredService<IFlowStateStore>();

        FlowState? updated = null;
        var failedNow = false;
        var found = await FlowStateConcurrency.MutateAsync(
            store,
            flowId,
            _options.StateExpiry,
            _timeProvider,
            state =>
            {
                updated = state;
                failedNow = false;
                if (state.Status != FlowRunStatus.Running)
                    return false;

                state.Status = FlowRunStatus.Failed;
                state.LastMessage = exception.Message;
                failedNow = true;
                return true;
            }).ConfigureAwait(false);

        if (!found || updated is null)
        {
            _logger.LogWarning("Durable flow {FlowId} cannot be failed: no state (unknown, expired, or unreadable).", flowId);
            return;
        }

        if (!failedNow)
        {
            _logger.LogDebug("Durable flow {FlowId} is already {Status}; ignoring failure signal.", flowId, updated.Status);
            // At-least-once re-notify, as in ExecuteAsync: the prior delivery of this failure
            // signal may have marked the run and then died in its own run-finished notification.
            if (updated.Status is FlowRunStatus.Succeeded or FlowRunStatus.Failed)
                await NotifyRunFinishedAsync(updated).ConfigureAwait(false);
            await NotifyParentAsync(updated).ConfigureAwait(false);
            return;
        }

        // Run-finished observers BEFORE the parent wake-up, matching ExecuteAsync and the
        // already-terminal branch above: enqueueing the parent first would let it resume and
        // finish before this run's terminal event is recorded, and an observer throw after the
        // parent was already notified would re-enqueue the parent a second time on redelivery.
        await NotifyRunFinishedAsync(updated).ConfigureAwait(false);
        await NotifyParentAsync(updated).ConfigureAwait(false);

        _logger.LogWarning(exception, "Durable flow {FlowId} failed via lost-subscriber routing: {Message}", flowId, exception.Message);
    }

    private async Task<bool> InvokeFlowAsync(
        IServiceProvider serviceProvider,
        IFlowStateStore store,
        FlowState state,
        FlowExecutionLease lease)
    {
        var context = new DurableFlowContext(
            state,
            store,
            _builder,
            _propagation,
            _options,
            _subscriber,
            _recoverableSubscriber,
            _logger,
            lease,
            _timeProvider,
            _observers,
            _workerTransport,
            _channelDefaultWaitTimeout);

        // Statically-typed path for flows registered via WithDurableFlow<TFlow, TInput>(): no
        // type-name resolution, no MakeGenericType, no MethodInfo.Invoke — the path trimmed and
        // Native AOT apps rely on.
        if (state.FlowTypeName is not null && _registrations.TryGetValue(state.FlowTypeName, out var registration))
        {
            // Fail closed exactly like the reflection fallback: a run persisted with a different
            // input type than the registration's TInput would otherwise silently parse the old
            // payload as the new type — renamed or added members become defaults — and execute
            // the remaining steps against wrong input. Null tolerated: ledgers written before the
            // stamp existed carry no input type name.
            if (state.InputTypeName is not null
                && !string.Equals(state.InputTypeName, registration.InputTypeFullName, StringComparison.Ordinal))
            {
                throw new InvalidOperationException(
                    $"Durable flow type '{state.FlowTypeName}' is registered with input type '{registration.InputTypeFullName}', " +
                    $"but the persisted run carries input type '{state.InputTypeName}'; the flow state was written by an " +
                    "incompatible flow definition.");
            }

            var flow = ResolveFlowFromDi(serviceProvider, registration.FlowType);
            var input = state.InputJson is null ? null : registration.DeserializeInput(state.InputJson);
            await registration.ExecuteAsync(flow, context, input).ConfigureAwait(false);
            await context.FlushProgressAsync().ConfigureAwait(false);
            return context.IsSuspended;
        }

        return await InvokeFlowByReflectionAsync(serviceProvider, state, context).ConfigureAwait(false);
    }

    [UnconditionalSuppressMessage("Trimming", "IL2026",
        Justification = "Reflection fallback for flows not registered via WithDurableFlow<TFlow, TInput>(). In a trimmed app an " +
                        "unregistered flow fails closed here with an actionable error telling the operator to register it; nothing " +
                        "is silently misexecuted.")]
    [UnconditionalSuppressMessage("Trimming", "IL2075",
        Justification = "Same fallback contract: the flow type and IDurableFlow<TInput> instantiation exist whenever the app " +
                        "actually defines and starts the flow; otherwise resolution fails closed with guidance.")]
    [UnconditionalSuppressMessage("AOT", "IL3050",
        Justification = "MakeGenericType over the flow's input type re-materializes an interface instantiation the user's flow " +
                        "class already implements statically; flows whose types were trimmed fail closed with guidance to use " +
                        "WithDurableFlow<TFlow, TInput>().")]
    private async Task<bool> InvokeFlowByReflectionAsync(
        IServiceProvider serviceProvider,
        FlowState state,
        DurableFlowContext context)
    {
        var flowType = ResolveType(state.FlowTypeName, "flow");
        var inputType = ResolveType(state.InputTypeName, "input");
        var input = state.InputJson is null ? null : JsonSafety.SafeDeserialize(state.InputJson, inputType);

        var contract = typeof(IDurableFlow<>).MakeGenericType(inputType);

        var flow = ResolveFlowFromDi(serviceProvider, flowType);
        if (!contract.IsInstanceOfType(flow))
        {
            throw new InvalidOperationException(
                $"Durable flow type '{flowType.FullName}' does not implement IDurableFlow<{inputType.Name}> " +
                "matching the persisted input type; the flow state was written by an incompatible flow definition.");
        }

        var execute = contract.GetMethod(nameof(IDurableFlow<object>.ExecuteAsync))!;
        try
        {
            await ((Task)execute.Invoke(flow, [context, input])!).ConfigureAwait(false);
            await context.FlushProgressAsync().ConfigureAwait(false);
            return context.IsSuspended;
        }
        catch (System.Reflection.TargetInvocationException ex) when (ex.InnerException is not null)
        {
            // A synchronously-thrown flow exception arrives wrapped; unwrap so terminal
            // DurableFlowFailedException handling (and user-visible stack traces) see the real one.
            System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(ex.InnerException).Throw();
            throw;
        }
    }

    private static object ResolveFlowFromDi(IServiceProvider serviceProvider, Type flowType)
    {
        try
        {
            return serviceProvider.GetRequiredService(flowType);
        }
        catch (InvalidOperationException ex)
        {
            throw new InvalidOperationException(
                $"Durable flow type '{flowType.FullName}' is not registered in DI. Register it with " +
                $"WithDurableFlow<{flowType.Name}, TInput>() (or services.AddScoped<{flowType.Name}>()) so the flow can be " +
                "resolved on execute and resume.", ex);
        }
    }

    private static Type ResolveType(string? fullName, string kind)
    {
        if (string.IsNullOrWhiteSpace(fullName))
            throw new InvalidOperationException($"The persisted flow state carries no {kind} type name; it was written by an incompatible producer.");

        return ReflectionExtensions.ResolveServiceType(fullName)
            ?? throw new InvalidOperationException(
                $"Cannot resolve {kind} type '{fullName}'. For plugin/collectible-assembly scenarios register a resolver " +
                $"via {nameof(AsyncResponseTypeResolution)}.{nameof(AsyncResponseTypeResolution.RegisterAssembly)}.");
    }

    /// <summary>
    /// Observer hook for terminal transitions. Fires AFTER the terminal save: an observer that
    /// throws here (crash injection) fails a delivery whose run is already terminal, so the
    /// redelivered execution re-notifies and acks — the run's outcome is never at stake, and the
    /// terminal event is delivered at least once.
    /// </summary>
    private async Task NotifyRunFinishedAsync(FlowState state)
    {
        if (_observers.Length == 0)
            return;

        var runEvent = new DurableFlowRunEvent(state.FlowId!, state.Status, state.LastMessage);
        foreach (var observer in _observers)
            await observer.OnRunFinishedAsync(runEvent).ConfigureAwait(false);
    }

    private Task NotifyParentAsync(FlowState state)
    {
        if (string.IsNullOrWhiteSpace(state.ParentFlowId))
            return Task.CompletedTask;

        // A suspended child cannot unblock its parent — the parent would only re-attach and go
        // back to waiting. The terminal transition after an operator un-suspends notifies then.
        if (state.Status == FlowRunStatus.Suspended)
            return Task.CompletedTask;

        var parentFlowId = state.ParentFlowId;
        _logger.LogInformation(
            "Durable child flow {FlowId} reached {Status}; resuming parent flow {ParentFlowId} step '{ParentStepName}'.",
            state.FlowId,
            state.Status,
            parentFlowId,
            state.ParentStepName);

        return _builder.EnqueueWorkerAsync<IDurableFlowExecutor>(executor => executor.ExecuteAsync(parentFlowId));
    }
}
