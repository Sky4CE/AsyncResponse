using Microsoft.Extensions.Logging;
using System.Linq.Expressions;
using System.Text.Json;

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
    private bool _suspended;

    /// <summary>Creates the context for one execution of the given run.</summary>
    public DurableFlowContext(
        FlowState state,
        IFlowStateStore store,
        IAsyncResponseBuilder builder,
        AsyncResponseContextPropagation propagation,
        DurableFlowOptions options,
        IAsyncResponseSubscriber subscriber,
        IRecoverableAsyncResponseSubscriber? recoverableSubscriber,
        ILogger logger)
    {
        _state = state;
        _store = store;
        _builder = builder;
        _propagation = propagation;
        _options = options;
        _subscriber = subscriber;
        _recoverableSubscriber = recoverableSubscriber;
        _logger = logger;
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

        await step().ConfigureAwait(false);
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

        var result = await step().ConfigureAwait(false);
        await CompleteStepAsync(name, checkpoint, JsonSerializer.Serialize(result), cancellationToken).ConfigureAwait(false);
        return result;
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
        return SaveAsync(cancellationToken);
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
        values[key] = JsonSerializer.Serialize(value);
        return SaveAsync(cancellationToken);
    }

    /// <inheritdoc />
    public async Task<FlowState> AwaitChildFlowAsync<TFlow, TInput>(
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

        var checkpoint = GetStep(name);
        if (checkpoint.Completed)
        {
            var completedChild = DeserializeResult<FlowState>(checkpoint.ResultJson);
            ThrowIfChildFailed(completedChild, failOnChildFailure);
            return completedChild;
        }

        var childFlowId = checkpoint.ChildFlowId;
        if (string.IsNullOrWhiteSpace(childFlowId))
        {
            childFlowId = string.IsNullOrWhiteSpace(flowId) ? $"{FlowId}:{name}" : flowId;
            checkpoint.ChildFlowId = childFlowId;
            checkpoint.Faulted = false;
            checkpoint.Message = $"Waiting for child flow '{childFlowId}'.";
            await SaveAsync(cancellationToken).ConfigureAwait(false);
        }

        var child = await _store.LoadAsync(childFlowId, cancellationToken).ConfigureAwait(false);
        if (child is null)
        {
            child = CreateChildState<TFlow, TInput>(childFlowId, name, input);
            await _store.SaveAsync(childFlowId, child, _options.StateExpiry, cancellationToken).ConfigureAwait(false);
            _logger.LogInformation("Flow {FlowId} started child flow {ChildFlowId} for step '{Step}'.", FlowId, childFlowId, name);
        }

        switch (child.Status)
        {
            case FlowRunStatus.Succeeded:
                await CompleteStepAsync(name, checkpoint, FlowStateJson.Serialize(child), cancellationToken).ConfigureAwait(false);
                return child;

            case FlowRunStatus.Failed:
                checkpoint.Faulted = true;
                checkpoint.Message = child.LastMessage;
                await CompleteStepAsync(name, checkpoint, FlowStateJson.Serialize(child), cancellationToken).ConfigureAwait(false);
                ThrowIfChildFailed(child, failOnChildFailure);
                return child;

            default:
                await EnqueueChildAsync(childFlowId).ConfigureAwait(false);
                Suspend($"Flow {FlowId} suspended waiting for child flow {childFlowId}.");
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

        var waiter = await CreateWaiterAsync(correlationId, until, stepTimeout, name).ConfigureAwait(false);
        try
        {
            if (!reattach)
            {
                // Persist the breadcrumb AFTER the registration exists and BEFORE the send:
                // "breadcrumb persisted" therefore implies "someone is listening", so a crash on
                // either side of the send re-attaches (or times out and restarts the idempotent
                // step) — never a lost run, never a double-send.
                checkpoint.PendingCorrelationId = correlationId;
                checkpoint.Faulted = false;
                checkpoint.Message = null;
                await SaveAsync(cancellationToken).ConfigureAwait(false);

                await trigger(correlationId).ConfigureAwait(false);
            }
            else if (_logger.IsEnabled(LogLevel.Information))
            {
                _logger.LogInformation(
                    "Flow {FlowId} step '{Step}' re-attaching to in-flight correlationId {CorrelationId}.",
                    FlowId, name, correlationId);
            }

            var response = await waiter.ResponseTask.ConfigureAwait(false);

            checkpoint.PendingCorrelationId = null;
            await CompleteStepAsync(name, checkpoint, JsonSerializer.Serialize(response), cancellationToken).ConfigureAwait(false);
            return response;
        }
        catch (Exception ex)
        {
            // Timeout, trigger failure, or a faulted wait: record it so the next execution
            // restarts this step fresh instead of re-attaching to a dead correlation id.
            checkpoint.Faulted = true;
            checkpoint.Message = ex.Message;
            await SaveAsync(CancellationToken.None).ConfigureAwait(false);
            throw;
        }
        finally
        {
            await waiter.DisposeAsync().ConfigureAwait(false);
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
            // re-enqueues the run (resume) or terminally fails it (failure) — the same at-least-once,
            // idempotency-required contract as hand-registered recovery callbacks.
            var flowId = FlowId;
            Expression<Func<IDurableFlowExecutor, Task>> resume = executor => executor.ResumeAsync(flowId);
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

    private FlowState CreateChildState<TFlow, TInput>(string flowId, string parentStepName, TInput input)
    {
        var now = DateTime.UtcNow;
        return new FlowState
        {
            FlowId = flowId,
            FlowTypeName = typeof(TFlow).FullName,
            InputTypeName = typeof(TInput).FullName,
            InputJson = JsonSerializer.Serialize(input),
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

    private void Suspend(string message)
    {
        _suspended = true;
        _state.LastMessage = message;
        throw new DurableFlowSuspendedException(message);
    }

    private void ThrowIfSuspended()
    {
        if (_suspended)
            throw new DurableFlowSuspendedException(_state.LastMessage ?? $"Flow {FlowId} is suspended.");
    }

    private static void ThrowIfChildFailed(FlowState child, bool failOnChildFailure)
    {
        if (failOnChildFailure && child.Status == FlowRunStatus.Failed)
            throw new DurableFlowFailedException($"Child flow '{child.FlowId}' failed: {child.LastMessage ?? "no message"}");
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

    private async Task CompleteStepAsync(string name, FlowStepState step, string? resultJson, CancellationToken cancellationToken)
    {
        step.Completed = true;
        step.ResultJson = resultJson;
        step.PendingCorrelationId = null;
        step.Faulted = false;
        step.CompletedAtUtc = DateTime.UtcNow;
        _state.LastMessage = $"Step '{name}' completed.";
        await SaveAsync(cancellationToken).ConfigureAwait(false);

        if (_logger.IsEnabled(LogLevel.Information))
            _logger.LogInformation("Flow {FlowId} step '{Step}' completed.", FlowId, name);
    }

    private Task SaveAsync(CancellationToken cancellationToken)
    {
        _state.UpdatedAtUtc = DateTime.UtcNow;
        return _store.SaveAsync(FlowId, _state, _options.StateExpiry, cancellationToken);
    }

    private static TResult DeserializeResult<TResult>(string? resultJson)
        => resultJson is null ? default! : JsonSafety.SafeDeserialize<TResult>(resultJson)!;
}
