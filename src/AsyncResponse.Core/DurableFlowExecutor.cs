using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System.Text.Json;

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

    /// <summary>Creates the flow executor.</summary>
    public DurableFlowExecutor(
        IServiceScopeFactory scopeFactory,
        IAsyncResponseBuilder builder,
        IAsyncResponseSubscriber subscriber,
        IRecoverableAsyncResponseSubscriber? recoverableSubscriber,
        AsyncResponseContextPropagation propagation,
        DurableFlowOptions options,
        ILogger<DurableFlowExecutor> logger)
    {
        _scopeFactory = scopeFactory;
        _builder = builder;
        _subscriber = subscriber;
        _recoverableSubscriber = recoverableSubscriber;
        _propagation = propagation;
        _options = options;
        FlowStateConcurrency.ValidateOptions(_options);
        _logger = logger;
    }

    /// <inheritdoc />
    public async Task ExecuteAsync(string flowId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(flowId);

        await using var scope = _scopeFactory.CreateAsyncScope();
        var store = scope.ServiceProvider.GetRequiredService<IFlowStateStore>();

        await using var lease = await FlowStateConcurrency.TryAcquireExecutionLeaseAsync(
            store,
            flowId,
            _options,
            _logger).ConfigureAwait(false);
        if (lease is null)
        {
            _logger.LogDebug("Durable flow {FlowId} is already executing on another worker; skipping duplicate delivery.", flowId);
            return;
        }

        var state = await store.LoadAsync(flowId).ConfigureAwait(false);
        if (state is null)
        {
            _logger.LogWarning("Durable flow {FlowId} has no state (unknown, expired, or unreadable); nothing to execute.", flowId);
            return;
        }

        if (state.Status != FlowRunStatus.Running)
        {
            _logger.LogDebug("Durable flow {FlowId} is already {Status}; skipping execution.", flowId, state.Status);
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
            await lease.SaveAsync(state, _options.StateExpiry).ConfigureAwait(false);

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
            await lease.SaveAsync(state, _options.StateExpiry).ConfigureAwait(false);

            AsyncResponseDiagnostics.SetError(activity, ex);

            // Retriable: propagate so the worker transport redelivers the run with bounded
            // attempts and dead-letters it when they are exhausted — the "run is stuck" alarm.
            throw;
        }

        await NotifyParentAsync(state).ConfigureAwait(false);
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

        var found = await FlowStateConcurrency.MutateAsync(
            store,
            flowId,
            _options.StateExpiry,
            state =>
            {
                checkpointed = false;
                if (state.Status != FlowRunStatus.Running || state.Steps is null)
                    return false;

                var pending = state.Steps.FirstOrDefault(pair =>
                    string.Equals(pair.Value.PendingCorrelationId, correlationId, StringComparison.Ordinal));
                if (pending.Value is null)
                    return false;

                pending.Value.Completed = true;
                pending.Value.ResultJson = JsonSerializer.Serialize(payload, payload.GetType());
                pending.Value.PendingCorrelationId = null;
                pending.Value.Faulted = false;
                pending.Value.Message = "Terminal response recovered after subscriber loss.";
                pending.Value.CompletedAtUtc = DateTime.UtcNow;
                state.LastMessage = $"Step '{pending.Key}' recovered after subscriber loss.";
                checkpointed = true;
                return true;
            }).ConfigureAwait(false);

        if (!found)
        {
            _logger.LogWarning("Durable flow {FlowId} cannot recover response {CorrelationId}: no state found.", flowId, correlationId);
            return;
        }

        if (!checkpointed)
        {
            _logger.LogDebug("Durable flow {FlowId} has no pending step for recovered correlationId {CorrelationId}; ignoring duplicate.", flowId, correlationId);
            return;
        }

        _logger.LogDebug("Durable flow {FlowId} checkpointed recovered correlationId {CorrelationId}; resuming.", flowId, correlationId);
        await _builder.EnqueueWorkerAsync<IDurableFlowExecutor>(executor => executor.ExecuteAsync(flowId)).ConfigureAwait(false);
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
            await NotifyParentAsync(updated).ConfigureAwait(false);
            return;
        }

        await NotifyParentAsync(updated).ConfigureAwait(false);

        _logger.LogWarning(exception, "Durable flow {FlowId} failed via lost-subscriber routing: {Message}", flowId, exception.Message);
    }

    private async Task<bool> InvokeFlowAsync(
        IServiceProvider serviceProvider,
        IFlowStateStore store,
        FlowState state,
        FlowExecutionLease lease)
    {
        var flowType = ResolveType(state.FlowTypeName, "flow");
        var inputType = ResolveType(state.InputTypeName, "input");
        var input = state.InputJson is null ? null : JsonSafety.SafeDeserialize(state.InputJson, inputType);

        var contract = typeof(IDurableFlow<>).MakeGenericType(inputType);

        object flow;
        try
        {
            flow = serviceProvider.GetRequiredService(flowType);
        }
        catch (InvalidOperationException ex)
        {
            throw new InvalidOperationException(
                $"Durable flow type '{flowType.FullName}' is not registered in DI. Register the class itself " +
                $"(e.g. services.AddScoped<{flowType.Name}>()) so the flow can be resolved on execute and resume.", ex);
        }

        if (!contract.IsInstanceOfType(flow))
        {
            throw new InvalidOperationException(
                $"Durable flow type '{flowType.FullName}' does not implement IDurableFlow<{inputType.Name}> " +
                "matching the persisted input type; the flow state was written by an incompatible flow definition.");
        }

        var context = new DurableFlowContext(
            state,
            store,
            _builder,
            _propagation,
            _options,
            _subscriber,
            _recoverableSubscriber,
            _logger,
            lease);
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

    private static Type ResolveType(string? fullName, string kind)
    {
        if (string.IsNullOrWhiteSpace(fullName))
            throw new InvalidOperationException($"The persisted flow state carries no {kind} type name; it was written by an incompatible producer.");

        return ReflectionExtensions.ResolveServiceType(fullName)
            ?? throw new InvalidOperationException(
                $"Cannot resolve {kind} type '{fullName}'. For plugin/collectible-assembly scenarios register a resolver " +
                $"via {nameof(AsyncResponseTypeResolution)}.{nameof(AsyncResponseTypeResolution.RegisterAssembly)}.");
    }

    private Task NotifyParentAsync(FlowState state)
    {
        if (string.IsNullOrWhiteSpace(state.ParentFlowId))
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
