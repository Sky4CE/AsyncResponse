using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System.Diagnostics.CodeAnalysis;

namespace AsyncResponse;

/// <inheritdoc cref="IDurableFlows" />
internal sealed class DurableFlowService : IDurableFlows
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly IAsyncResponseBuilder _builder;
    private readonly AsyncResponseContextPropagation _propagation;
    private readonly DurableFlowOptions _options;
    private readonly ILogger<DurableFlowService> _logger;
    private readonly TimeProvider _timeProvider;

    /// <summary>Creates the durable-flows starter.</summary>
    public DurableFlowService(
        IServiceScopeFactory scopeFactory,
        IAsyncResponseBuilder builder,
        AsyncResponseContextPropagation propagation,
        DurableFlowOptions options,
        ILogger<DurableFlowService> logger,
        TimeProvider? timeProvider = null)
    {
        _scopeFactory = scopeFactory;
        _builder = builder;
        _propagation = propagation;
        _options = options;
        FlowStateConcurrency.ValidateOptions(_options);
        _logger = logger;
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    /// <inheritdoc />
    public async Task<string> StartAsync<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicMethods | DynamicallyAccessedMemberTypes.Interfaces)] TFlow, TInput>(
        TInput input,
        string? flowId = null,
        CancellationToken cancellationToken = default)
        where TFlow : class, IDurableFlow<TInput>
    {
        ArgumentNullException.ThrowIfNull(input);
        cancellationToken.ThrowIfCancellationRequested();
        if (flowId is null)
            flowId = $"flow-{AsyncResponseContext.GenerateCorrelationId()}";
        else
            ArgumentException.ThrowIfNullOrWhiteSpace(flowId);

        // Every id is validated BEFORE anything is published: the publish below is the start's
        // commit point, and a job for an id every store would reject must never leave the process.
        FlowStateConcurrency.EnsurePortableFlowId(flowId);

        await using var scope = _scopeFactory.CreateAsyncScope();
        var store = scope.ServiceProvider.GetRequiredService<IFlowStateStore>();

        var now = _timeProvider.GetUtcNow().UtcDateTime;
        var inputJson = AsyncResponseJson.Serialize(input);
        var state = new FlowState
        {
            FlowId = flowId,
            FlowTypeName = typeof(TFlow).FullName,
            InputTypeName = typeof(TInput).FullName,
            InputJson = inputJson,
            Status = FlowRunStatus.Running,
            LastMessage = "Flow started.",
            CreatedAtUtc = now,
            UpdatedAtUtc = now,
            Revision = 0,
            Context = _propagation.Capture()
        };

        // PUBLISH FIRST, then create. The worker job carries the whole initial ledger, and
        // IDurableFlowExecutor.CreateAndExecuteAsync creates the ledger itself (insert-if-absent)
        // before executing — so the publish is the single durable commit point of a start:
        //  - a crash before the publish leaves nothing behind (the caller sees a fault and retries);
        //  - a crash after the publish leaves a job whose execution creates and runs the flow.
        // The previous order (create, then publish) had an unrecoverable gap: a process dying
        // between the two left a committed Running ledger with Attempts = 0 that nothing would
        // ever execute, and IFlowStateStore has no enumeration for a reconciler to go find it.
        // The publish still runs the retry ladder the ingress uses, and a publish that fails for
        // good surfaces the id (DurableFlowNotDispatchedException) — now with nothing persisted.
        var id = flowId;
        var initialStateJson = FlowStateJson.Serialize(state);
        await PublishStartAsync(
            executor => executor.CreateAndExecuteAsync(id, initialStateJson),
            id,
            cancellationToken).ConfigureAwait(false);

        // The starter's own create keeps the caller-facing contract: the ledger exists by the time
        // StartAsync returns (GetStateAsync / ResumeAsync right after a start see it), and a
        // conflicting reuse of an explicit id is reported to THIS caller. Losing the create race —
        // to the executor that already picked the job up, or to a concurrent identical start — is
        // the expected shape, not an error. A store fault here no longer matters for the run: the
        // job is published and the executor creates the ledger; the caller gets the id.
        bool created;
        try
        {
            created = await FlowStateConcurrency.TryCreateAsync(
                store,
                flowId,
                state,
                _options.StateExpiry,
                cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
        {
            _logger.LogWarning(
                ex,
                "Durable flow {FlowId} start job is published but the starter could not write the ledger; the executor creates it when the job is picked up.",
                flowId);
            return flowId;
        }

        if (created)
        {
            _logger.LogInformation("Started durable flow {FlowId} ({FlowType}).", flowId, typeof(TFlow).Name);
            return flowId;
        }

        var existing = await store.LoadAsync(flowId, cancellationToken).ConfigureAwait(false);
        if (existing is null)
        {
            // Lost the create to a ledger that has since expired: the published job's create wins
            // the next time round. Nothing for the caller to do.
            _logger.LogWarning("Durable flow {FlowId} start job is published; the existing ledger is expired and the executor re-creates it.", flowId);
            return flowId;
        }

        // Throws DurableFlowIdConflictException for different work; the executor drops the
        // already-published job on the same test.
        EnsureIdempotentStart<TFlow, TInput>(existing, inputJson, flowId);

        // A semantically identical retry: the published job re-enqueues the existing run
        // (completed steps skip) instead of creating a duplicate.
        _logger.LogInformation("Durable flow {FlowId} already exists; the start job re-enqueues the existing run instead of creating a duplicate.", flowId);
        return flowId;
    }

    /// <summary>
    /// Publishes a start job through the ingress's retry ladder. A publish that still fails
    /// surfaces as <see cref="DurableFlowNotDispatchedException"/> carrying the id: nothing was
    /// persisted, so the caller simply retries the start (idempotent with the same id).
    /// </summary>
    private async Task PublishStartAsync(
        System.Linq.Expressions.Expression<Func<IDurableFlowExecutor, Task>> job,
        string flowId,
        CancellationToken cancellationToken)
    {
        try
        {
            await AsyncResponseRetry.ExecuteAsync(
                async token =>
                {
                    await _builder.EnqueueWorkerAsync(job, token).ConfigureAwait(false);
                    return true;
                },
                // Only the CALLER's cancellation ends the ladder. An OperationCanceledException
                // whose token is not the caller's is a transport or SDK timeout — brokers surface
                // those as TaskCanceledException all the time — and that is exactly the transient
                // shape this retry exists for. Excluding the whole exception type meant the most
                // common recoverable publish failure got zero retries. An envelope over the
                // ingress's size budget is deterministic (the same input serializes to the same
                // length): no attempt can succeed, so it is not retried either.
                isTransient: ex => ex is not WorkerJobTooLargeException
                    && (ex is not OperationCanceledException || !cancellationToken.IsCancellationRequested),
                maxAttempts: 4,
                baseDelay: TimeSpan.FromMilliseconds(250),
                maxDelay: TimeSpan.FromSeconds(2),
                cancellationToken,
                _timeProvider).ConfigureAwait(false);
        }
        catch (WorkerJobTooLargeException ex)
        {
            // Not a dispatch failure to retry: the start job carries the initial ledger, and this
            // input serializes past what the consuming ingress accepts — it would be acknowledged
            // there without ever executing. Surfaced as itself (nothing was persisted) so the
            // caller can shrink the input or move it behind a claim check.
            _logger.LogError(
                ex,
                "Durable flow {FlowId} could not be started: its start job ({SerializedLength} UTF-16 code units) exceeds the ingress budget of {Limit}. Nothing was persisted.",
                flowId,
                ex.SerializedLength,
                ex.Limit);
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "Durable flow {FlowId} could not be started: its worker job was not published after retries. Nothing was persisted; retry the start (idempotent with this id).",
                flowId);
            throw new DurableFlowNotDispatchedException(flowId, ex);
        }
    }

    /// <inheritdoc />
    public async Task ResumeAsync(string flowId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(flowId);

        await using var scope = _scopeFactory.CreateAsyncScope();
        var store = scope.ServiceProvider.GetRequiredService<IFlowStateStore>();

        var state = await store.LoadAsync(flowId, cancellationToken).ConfigureAwait(false)
            ?? throw new InvalidOperationException($"No flow state found for '{flowId}' (unknown, expired, or unreadable).");

        if (state.Status != FlowRunStatus.Running)
        {
            _logger.LogDebug("Durable flow {FlowId} is already {Status}; ignoring resume.", flowId, state.Status);
            return;
        }

        var id = flowId;
        await _builder.EnqueueWorkerAsync<IDurableFlowExecutor>(
            executor => executor.ExecuteAsync(id),
            cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<FlowState?> GetStateAsync(string flowId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(flowId);

        await using var scope = _scopeFactory.CreateAsyncScope();
        var store = scope.ServiceProvider.GetRequiredService<IFlowStateStore>();
        return await store.LoadAsync(flowId, cancellationToken).ConfigureAwait(false);
    }

    private static void EnsureIdempotentStart<TFlow, TInput>(
        FlowState existing,
        string requestedInputJson,
        string flowId)
    {
        if (FlowStateConcurrency.IsSameStart(existing, typeof(TFlow).FullName, typeof(TInput).FullName, requestedInputJson))
            return;

        throw new DurableFlowIdConflictException(
            $"Durable flow id '{flowId}' is already bound to a different flow type or input. " +
            "Idempotent retries must use the same TFlow, TInput, and semantically identical input value.");
    }

}
