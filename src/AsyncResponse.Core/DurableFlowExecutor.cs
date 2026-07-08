using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace AsyncResponse;

/// <summary>
/// Executes durable flow runs. Its methods are the durable targets behind every flow: worker jobs
/// carry <see cref="ExecuteAsync"/>, and awaited steps register <see cref="ResumeAsync"/> /
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
        IOptions<AsyncResponseOptions> options,
        ILogger<DurableFlowExecutor> logger)
    {
        _scopeFactory = scopeFactory;
        _builder = builder;
        _subscriber = subscriber;
        _recoverableSubscriber = recoverableSubscriber;
        _propagation = propagation;
        _options = options.Value.DurableFlows;
        _logger = logger;
    }

    /// <inheritdoc />
    public async Task ExecuteAsync(string flowId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(flowId);

        await using var scope = _scopeFactory.CreateAsyncScope();
        var store = scope.ServiceProvider.GetRequiredService<IFlowStateStore>();

        var state = await store.LoadAsync(flowId).ConfigureAwait(false);
        if (state is null)
        {
            _logger.LogWarning("Durable flow {FlowId} has no state (unknown, expired, or unreadable); nothing to execute.", flowId);
            return;
        }

        if (state.Status != FlowRunStatus.Running)
        {
            _logger.LogDebug("Durable flow {FlowId} is already {Status}; skipping execution.", flowId, state.Status);
            return;
        }

        using var activity = AsyncResponseDiagnostics.StartActivity("asyncresponse.flow.execute");
        activity?.SetTag("asyncresponse.flow_id", flowId);
        activity?.SetTag("asyncresponse.flow_type", state.FlowTypeName);

        state.Attempts++;
        state.UpdatedAtUtc = DateTime.UtcNow;
        await store.SaveAsync(flowId, state, _options.StateExpiry).ConfigureAwait(false);

        // The run may be resumed by a different deployment than the one that started it: restore
        // the ambient context captured at start before any flow code runs.
        using var ambientScope = _propagation.Restore(state.Context);

        try
        {
            await InvokeFlowAsync(scope.ServiceProvider, store, state).ConfigureAwait(false);

            state.Status = FlowRunStatus.Succeeded;
            state.LastMessage = "Flow completed.";
            await SaveAsync(store, state).ConfigureAwait(false);

            _logger.LogInformation("Durable flow {FlowId} completed successfully (attempt {Attempts}).", flowId, state.Attempts);
        }
        catch (DurableFlowFailedException ex)
        {
            // Terminal by declaration: mark failed and swallow so the transport acks the job.
            state.Status = FlowRunStatus.Failed;
            state.LastMessage = ex.Message;
            await SaveAsync(store, state).ConfigureAwait(false);

            AsyncResponseDiagnostics.SetError(activity, ex);
            _logger.LogWarning(ex, "Durable flow {FlowId} failed terminally: {Message}", flowId, ex.Message);
        }
        catch (Exception ex)
        {
            state.LastMessage = ex.Message;
            await SaveAsync(store, state).ConfigureAwait(false);

            AsyncResponseDiagnostics.SetError(activity, ex);

            // Retriable: propagate so the worker transport redelivers the run with bounded
            // attempts and dead-letters it when they are exhausted — the "run is stuck" alarm.
            throw;
        }
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

        _logger.LogInformation("Durable flow {FlowId} resuming via worker transport.", flowId);
        await _builder.EnqueueWorkerAsync<IDurableFlowExecutor>(executor => executor.ExecuteAsync(flowId)).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task FailAsync(string flowId, Exception exception)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(flowId);
        ArgumentNullException.ThrowIfNull(exception);

        await using var scope = _scopeFactory.CreateAsyncScope();
        var store = scope.ServiceProvider.GetRequiredService<IFlowStateStore>();

        var state = await store.LoadAsync(flowId).ConfigureAwait(false);
        if (state is null)
        {
            _logger.LogWarning("Durable flow {FlowId} cannot be failed: no state (unknown, expired, or unreadable).", flowId);
            return;
        }

        if (state.Status != FlowRunStatus.Running)
        {
            _logger.LogDebug("Durable flow {FlowId} is already {Status}; ignoring failure signal.", flowId, state.Status);
            return;
        }

        state.Status = FlowRunStatus.Failed;
        state.LastMessage = exception.Message;
        await SaveAsync(store, state).ConfigureAwait(false);

        _logger.LogWarning(exception, "Durable flow {FlowId} failed via lost-subscriber routing: {Message}", flowId, exception.Message);
    }

    private async Task InvokeFlowAsync(IServiceProvider serviceProvider, IFlowStateStore store, FlowState state)
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

        var context = new DurableFlowContext(state, store, _options, _subscriber, _recoverableSubscriber, _logger);
        var execute = contract.GetMethod(nameof(IDurableFlow<object>.ExecuteAsync))!;
        try
        {
            await ((Task)execute.Invoke(flow, [context, input])!).ConfigureAwait(false);
        }
        catch (System.Reflection.TargetInvocationException ex) when (ex.InnerException is not null)
        {
            // A synchronously-thrown flow exception arrives wrapped; unwrap so terminal
            // DurableFlowFailedException handling (and user-visible stack traces) see the real one.
            System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(ex.InnerException).Throw();
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

    private Task SaveAsync(IFlowStateStore store, FlowState state)
    {
        state.UpdatedAtUtc = DateTime.UtcNow;
        return store.SaveAsync(state.FlowId!, state, _options.StateExpiry);
    }
}
