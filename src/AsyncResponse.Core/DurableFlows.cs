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
            Context = _propagation.Capture()
        };

        if (await FlowStateConcurrency.TryCreateAsync(
                store,
                flowId,
                state,
                _options.StateExpiry,
                cancellationToken).ConfigureAwait(false))
        {
            _logger.LogInformation("Started durable flow {FlowId} ({FlowType}).", flowId, typeof(TFlow).Name);
        }
        else
        {
            var existing = await store.LoadAsync(flowId, cancellationToken).ConfigureAwait(false)
                ?? throw new InvalidOperationException(
                    $"Durable flow '{flowId}' already exists but its ledger is expired or unreadable.");
            EnsureIdempotentStart<TFlow, TInput>(existing, inputJson, flowId);

            // A semantically identical retry re-enqueues the existing run; completed steps skip.
            _logger.LogInformation("Durable flow {FlowId} already exists; re-enqueueing instead of creating a duplicate.", flowId);
        }

        // The ledger is committed; from here the run EXISTS and is Running. If the wake-up never
        // gets published, nothing in the system will ever execute it — IFlowStateStore has no
        // enumeration, so no reconciler can go find it either. Retry the publish through the same
        // ladder the ingress uses, and if it still fails, surface the flow id rather than the bare
        // transport fault: with the id, a caller can re-drive the start idempotently; without it
        // (the generated-id case) the run is simply lost.
        var id = flowId;
        try
        {
            await AsyncResponseRetry.ExecuteAsync(
                async token =>
                {
                    await _builder.EnqueueWorkerAsync<IDurableFlowExecutor>(
                        executor => executor.ExecuteAsync(id),
                        token).ConfigureAwait(false);
                    return true;
                },
                // Only the CALLER's cancellation ends the ladder. An OperationCanceledException
                // whose token is not the caller's is a transport or SDK timeout — brokers surface
                // those as TaskCanceledException all the time — and that is exactly the transient
                // shape this retry exists for. Excluding the whole exception type meant the most
                // common recoverable publish failure got zero retries and went straight to an
                // orphaned Running ledger.
                isTransient: ex => ex is not OperationCanceledException || !cancellationToken.IsCancellationRequested,
                maxAttempts: 4,
                baseDelay: TimeSpan.FromMilliseconds(250),
                maxDelay: TimeSpan.FromSeconds(2),
                cancellationToken,
                _timeProvider).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "Durable flow {FlowId} was persisted but its worker job could not be published; the run exists with no wake-up. Retry the start with this id to re-enqueue it.",
                id);
            throw new DurableFlowNotDispatchedException(id, ex);
        }

        return flowId;
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
        var sameFlowType = string.Equals(existing.FlowTypeName, typeof(TFlow).FullName, StringComparison.Ordinal);
        var sameInputType = string.Equals(existing.InputTypeName, typeof(TInput).FullName, StringComparison.Ordinal);
        if (sameFlowType && sameInputType && FlowStateJson.JsonEquivalent(existing.InputJson, requestedInputJson))
            return;

        throw new DurableFlowIdConflictException(
            $"Durable flow id '{flowId}' is already bound to a different flow type or input. " +
            "Idempotent retries must use the same TFlow, TInput, and semantically identical input value.");
    }

}
