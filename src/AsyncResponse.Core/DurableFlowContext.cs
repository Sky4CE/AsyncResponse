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
    private bool _suspended;
    private bool _progressDirty;
    private DateTime _lastPersistenceUtc;

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
        FlowExecutionLease lease)
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
        var result = await step().ConfigureAwait(false);
        _lease.ThrowIfLost();
        await CompleteStepAsync(name, checkpoint, AsyncResponseJson.Serialize(result), cancellationToken).ConfigureAwait(false);
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
        var now = DateTime.UtcNow;
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
                await CompleteStepAsync(name, checkpoint, FlowStateJson.SerializeSnapshot(child), cancellationToken).ConfigureAwait(false);
                return child;

            case FlowRunStatus.Failed:
                checkpoint.Message = child.LastMessage;
                await CompleteStepAsync(name, checkpoint, FlowStateJson.SerializeSnapshot(child), cancellationToken, faulted: true).ConfigureAwait(false);
                ThrowIfChildFailed(child, failOnChildFailure);
                return child;

            default:
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

        var waiter = await CreateWaiterAsync(correlationId, until, stepTimeout, name).ConfigureAwait(false);
        var triggerCompleted = reattach;
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
                triggerCompleted = true;
            }
            else if (_logger.IsEnabled(LogLevel.Debug))
            {
                _logger.LogDebug(
                    "Flow {FlowId} step '{Step}' re-attaching to in-flight correlationId {CorrelationId}.",
                    FlowId, name, correlationId);
            }

            var response = await WaitForResponseAsync(waiter.ResponseTask, cancellationToken).ConfigureAwait(false);

            checkpoint.PendingCorrelationId = null;
            // Deliberately NOT the caller's token: once the response is claimed from the channel
            // it exists nowhere else, so the completion checkpoint must not be interruptible — a
            // cancellation here used to leave `pending` set with the response already consumed,
            // and the redelivered execution re-attached to a correlation id nothing could answer.
            await CompleteStepAsync(name, checkpoint, AsyncResponseJson.Serialize(response), CancellationToken.None).ConfigureAwait(false);
            return response;
        }
        catch (OperationCanceledException ex) when (triggerCompleted && !waiter.ResponseTask.IsFaulted)
        {
            // A lost lease surfaces as cancellation of the linked wait; convert it with the wait
            // failure attached so the takeover signal does not discard the real cause.
            _lease.ThrowIfLost(ex);

            if (waiter.ResponseTask.IsCompletedSuccessfully)
            {
                // The cancellation RACED a delivered response: Task.WaitAsync can complete as
                // canceled even though the source task finished first. The channel has already
                // claimed and acked that message — it exists nowhere else, and re-attaching to
                // its consumed correlation id would park the run until the step timeout. The
                // checkpoint therefore wins over the cancellation: persist the received payload
                // and return it; the caller's token gets its say again at the next step boundary.
                var received = waiter.ResponseTask.Result;
                checkpoint.PendingCorrelationId = null;
                await CompleteStepAsync(name, checkpoint, AsyncResponseJson.Serialize(received), CancellationToken.None).ConfigureAwait(false);
                return received;
            }

            // WAIT-SIDE cancellation with nothing delivered is infrastructure, not a step
            // verdict: the channel cancels in-flight waiters when it is disposed at host
            // shutdown, and the caller's token means "stop this execution", not "the step
            // failed" — the remote operation is still in flight. The persisted breadcrumb must
            // survive untouched so the redelivered execution RE-ATTACHES to the same correlation
            // id; marking the checkpoint faulted here turned every graceful shutdown mid-await
            // into a fresh-correlation restart that re-sent the remote request. (A response that
            // never arrives still faults via the step timeout.)
            //
            // The filter is what keeps this narrow: cancellation thrown BY THE TRIGGER (an
            // HttpClient timeout surfaces as TaskCanceledException) means the request may never
            // have left the process — re-attaching would park the run on a correlation id nobody
            // answers — and a response task that FAULTED with a cancellation (a throwing Until
            // predicate) already consumed its message. Both fall through to the fault path below
            // and restart the idempotent step fresh.
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
        var now = DateTime.UtcNow;
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

    private async Task CompleteStepAsync(string name, FlowStepState step, string? resultJson, CancellationToken cancellationToken, bool faulted = false)
    {
        step.Completed = true;
        step.ResultJson = resultJson;
        step.PendingCorrelationId = null;
        // A memoized failed child keeps Faulted = true so operators can spot the failure on the
        // step itself instead of digging through ResultJson.
        step.Faulted = faulted;
        step.CompletedAtUtc = DateTime.UtcNow;
        _state.LastMessage = faulted ? $"Step '{name}' completed (child flow failed)." : $"Step '{name}' completed.";
        await SaveAsync(cancellationToken).ConfigureAwait(false);

        if (_logger.IsEnabled(LogLevel.Debug))
            _logger.LogDebug("Flow {FlowId} step '{Step}' completed.", FlowId, name);
    }

    internal Task FlushProgressAsync()
        => _progressDirty ? SaveAsync(CancellationToken.None) : Task.CompletedTask;

    private async Task SaveAsync(CancellationToken cancellationToken, Exception? cause = null)
    {
        _state.UpdatedAtUtc = DateTime.UtcNow;
        await _lease.SaveAsync(_state, _options.StateExpiry, cancellationToken, cause).ConfigureAwait(false);

        _progressDirty = false;
        _lastPersistenceUtc = DateTime.UtcNow;
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
