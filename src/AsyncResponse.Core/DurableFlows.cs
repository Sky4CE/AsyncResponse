using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Text.Json;

namespace AsyncResponse;

/// <inheritdoc cref="IDurableFlows" />
internal sealed class DurableFlows : IDurableFlows
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly IAsyncResponseBuilder _builder;
    private readonly AsyncResponseContextPropagation _propagation;
    private readonly DurableFlowOptions _options;
    private readonly ILogger<DurableFlows> _logger;

    /// <summary>Creates the durable-flows starter.</summary>
    public DurableFlows(
        IServiceScopeFactory scopeFactory,
        IAsyncResponseBuilder builder,
        AsyncResponseContextPropagation propagation,
        IOptions<AsyncResponseOptions> options,
        ILogger<DurableFlows> logger)
    {
        _scopeFactory = scopeFactory;
        _builder = builder;
        _propagation = propagation;
        _options = options.Value.DurableFlows;
        _logger = logger;
    }

    /// <inheritdoc />
    public async Task<string> StartAsync<TFlow, TInput>(
        TInput input,
        string? flowId = null,
        CancellationToken cancellationToken = default)
        where TFlow : class, IDurableFlow<TInput>
    {
        ArgumentNullException.ThrowIfNull(input);
        flowId = string.IsNullOrWhiteSpace(flowId)
            ? $"flow-{AsyncResponseContext.GenerateCorrelationId()}"
            : flowId;

        await using var scope = _scopeFactory.CreateAsyncScope();
        var store = scope.ServiceProvider.GetRequiredService<IFlowStateStore>();

        var existing = await store.LoadAsync(flowId, cancellationToken).ConfigureAwait(false);
        if (existing is null)
        {
            var now = DateTime.UtcNow;
            var state = new FlowState
            {
                FlowId = flowId,
                FlowTypeName = typeof(TFlow).FullName,
                InputTypeName = typeof(TInput).FullName,
                InputJson = JsonSerializer.Serialize(input),
                Status = FlowRunStatus.Running,
                LastMessage = "Flow started.",
                CreatedAtUtc = now,
                UpdatedAtUtc = now,
                Context = _propagation.Capture()
            };

            await store.SaveAsync(flowId, state, _options.StateExpiry, cancellationToken).ConfigureAwait(false);
            _logger.LogInformation("Started durable flow {FlowId} ({FlowType}).", flowId, typeof(TFlow).Name);
        }
        else
        {
            // Idempotent start: a caller-supplied id that already exists just re-enqueues the run
            // (completed steps skip), so retried API calls and operator kicks are safe.
            _logger.LogInformation("Durable flow {FlowId} already exists; re-enqueueing instead of creating a duplicate.", flowId);
        }

        var id = flowId;
        await _builder.EnqueueWorkerAsync<IDurableFlowExecutor>(executor => executor.ExecuteAsync(id)).ConfigureAwait(false);
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
        await _builder.EnqueueWorkerAsync<IDurableFlowExecutor>(executor => executor.ExecuteAsync(id)).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<FlowState?> GetStateAsync(string flowId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(flowId);

        await using var scope = _scopeFactory.CreateAsyncScope();
        var store = scope.ServiceProvider.GetRequiredService<IFlowStateStore>();
        return await store.LoadAsync(flowId, cancellationToken).ConfigureAwait(false);
    }
}
