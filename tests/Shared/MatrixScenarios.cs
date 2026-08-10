using Microsoft.Extensions.DependencyInjection;
using System.Collections.Concurrent;
using Xunit;

namespace AsyncResponse.Conformance;

/// <summary>
/// The scenarios every <see cref="MatrixCell"/> runs. Each one drives all three provider axes in a
/// single pass — the durable-flow store holds the checkpoints, the worker transport carries the flow
/// execution, and the response channel carries the awaited remote reply — because that interaction is
/// the thing the cross product exists to prove. Depth within a single axis belongs to that axis's own
/// conformance suite (<see cref="ChannelConformanceSuite"/>, <see cref="TransportConformanceSuite"/>,
/// <see cref="FlowStoreConformanceSuite"/>), which run per provider rather than per combination.
/// </summary>
public static class MatrixScenarios
{
    /// <summary>
    /// Budget for one cell. The slowest combinations pair a polling database channel with a polling
    /// database transport and an Oracle store, each contributing its own latency; the budget bounds
    /// waits and is never asserted against.
    /// </summary>
    private static readonly TimeSpan CellBudget = TimeSpan.FromSeconds(90);

    /// <summary>
    /// The complete round trip: start a flow, let the worker transport deliver its execution, run a
    /// memoized local step, trigger a remote operation and durably await its reply over the response
    /// channel, then read the finished state back out of the durable-flow store.
    /// <para>
    /// This is the fact that fails when two providers do not compose — a transport whose worker
    /// context loses the correlation id, a channel whose waiter never re-attaches after the flow
    /// suspends, a store that cannot round-trip the step map the executor wrote.
    /// </para>
    /// </summary>
    public static async Task DurableFlowCompletesAsync(MatrixCell cell, MatrixBackends backends)
    {
        await using var harness = await CreateHarnessAsync(cell, backends);
        var probe = harness.Provider.GetRequiredService<MatrixFlowProbe>();

        var flowId = await harness.Flows.StartAsync<MatrixFlow, MatrixFlowInput>(
            new MatrixFlowInput { Token = 7 });

        // The trigger fires on the worker, wherever the transport put it. Its correlation id is the
        // handshake: the test plays the remote system and answers over the channel.
        var correlationId = await probe.TriggerFor(flowId).WaitAsync(CellBudget);
        await harness.Publisher.SetResponse(
            new MatrixResult { Status = MatrixStatus.Running, Message = "progress" }, correlationId);
        await harness.Publisher.SetResponse(
            new MatrixResult { Status = MatrixStatus.Completed, Message = "done" }, correlationId);

        await probe.CompletionFor(flowId).WaitAsync(CellBudget);

        // Read through the store rather than the executor's in-flight copy: this proves the whole
        // serialize → persist → deserialize round trip for the cell's store.
        var state = await EventuallyStateAsync(
            harness, flowId, s => s.Status == FlowRunStatus.Succeeded);

        Assert.Equal(FlowRunStatus.Succeeded, state.Status);
        Assert.True(state.Steps!["compute-stamp"].Completed);
        Assert.Contains("107", state.Steps["compute-stamp"].ResultJson);
        Assert.True(state.Steps["remote-op"].Completed);
        Assert.Null(state.Steps["remote-op"].PendingCorrelationId);
        Assert.True(state.Steps["notify"].Completed);
        Assert.Equal("done", state.Values!["final-message"].Trim('"'));
    }

    /// <summary>
    /// The terminal-failure path across all three axes: the remote operation answers with a failed
    /// payload, the flow turns that into <see cref="DurableFlowFailedException"/>, and the run must be
    /// persisted as terminally failed — not retried by the transport, not left Running in the store.
    /// A resume kick afterwards must leave it terminal.
    /// </summary>
    public static async Task DomainFailureMarksRunFailedAsync(MatrixCell cell, MatrixBackends backends)
    {
        await using var harness = await CreateHarnessAsync(cell, backends);
        var probe = harness.Provider.GetRequiredService<MatrixFlowProbe>();

        var flowId = await harness.Flows.StartAsync<MatrixFlow, MatrixFlowInput>(
            new MatrixFlowInput { Token = 3 });

        var correlationId = await probe.TriggerFor(flowId).WaitAsync(CellBudget);
        await harness.Publisher.SetResponse(
            new MatrixResult { Status = MatrixStatus.Failed, Message = "remote rejected" }, correlationId);

        var state = await EventuallyStateAsync(harness, flowId, s => s.Status == FlowRunStatus.Failed);
        Assert.Equal(FlowRunStatus.Failed, state.Status);
        Assert.Contains("remote rejected", state.LastMessage, StringComparison.Ordinal);

        // Terminal runs stay terminal. A resume kick is exactly what a crash-recovery scan issues, and
        // it must not restart a failed run on any store.
        await harness.Flows.ResumeAsync(flowId);
        await Task.Delay(TimeSpan.FromMilliseconds(750));
        var after = await harness.Flows.GetStateAsync(flowId);
        Assert.Equal(FlowRunStatus.Failed, after!.Status);
    }

    /// <summary>
    /// The transport-plus-channel half on its own: a fire-and-forget worker job must arrive with the
    /// enqueuing correlation id and ambient context restored, and a response published from inside the
    /// job must reach a waiter parked on the channel. This isolates the wire axes from the store, so a
    /// failure here says the pairing is broken rather than the flow executor.
    /// </summary>
    public static async Task WorkerJobRestoresContextAndAnswersWaiterAsync(MatrixCell cell, MatrixBackends backends)
    {
        await using var harness = await CreateHarnessAsync(cell, backends);
        var correlationId = harness.Names.NewCorrelationId("worker");

        await using var waiter = await harness.Subscriber.CreateResponseWaiter<MatrixResult>(
            correlationId, timeout: CellBudget);

        // Enqueue through the public offload surface with the correlation id ambient, exactly as an
        // application does: the transport must carry it across the wire and restore it on the worker.
        AsyncResponseContext.SetCorrelationId(correlationId);
        try
        {
            await harness.Provider.GetRequiredService<IAsyncResponseBuilder>()
                .EnqueueWorkerAsync<IMatrixWorkerService>(service => service.RunAsync(41));
        }
        finally
        {
            AsyncResponseContext.ClearCorrelationId();
        }

        var result = await waiter.ResponseTask.WaitAsync(CellBudget);
        Assert.Equal(MatrixStatus.Completed, result.Status);

        // The worker echoes the correlation id it observed. Equality proves the transport restored the
        // enqueuing context rather than the job inventing a fresh one.
        Assert.Equal(correlationId, result.Message);
    }

    private static async Task<MatrixHarness> CreateHarnessAsync(MatrixCell cell, MatrixBackends backends)
        => await MatrixHarness.CreateAsync(
            cell,
            backends,
            services =>
            {
                services.AddSingleton<MatrixFlowProbe>();
                services.AddScoped<MatrixFlow>();
                services.AddSingleton<IMatrixWorkerService, MatrixWorkerService>();
            },
            options =>
            {
                // Flow wake-ups ride the worker queue, so the early-ACK trade-off has to be accepted
                // explicitly for any cell whose transport is configured that way. The matrix runs the
                // default ACK mode, but accepting it here keeps the knob from deciding which cells
                // can exist.
                options.AllowEarlyAckWorkerSubscriber = true;
                options.ExecutionLeaseDuration = TimeSpan.FromMinutes(2);
            },
            // Records the statically-typed execution route, which is what the executor prefers over
            // resolving the persisted type name by reflection.
            builder => builder.WithDurableFlow<MatrixFlow, MatrixFlowInput>());

    /// <summary>
    /// Polls the store until the run reaches the expected shape. Stores differ in read-after-write
    /// visibility (DynamoDB rounds its TTL to whole seconds, Cosmos is eventually consistent by
    /// default), so the assertion polls rather than reading once.
    /// </summary>
    private static async Task<FlowState> EventuallyStateAsync(
        MatrixHarness harness, string flowId, Func<FlowState, bool> predicate)
    {
        var deadline = DateTime.UtcNow + CellBudget;
        FlowState? last = null;
        while (DateTime.UtcNow < deadline)
        {
            last = await harness.Flows.GetStateAsync(flowId);
            if (last is not null && predicate(last))
                return last;

            await Task.Delay(TimeSpan.FromMilliseconds(100));
        }

        Assert.Fail(
            $"Cell {harness.Cell} did not reach the expected flow state within {CellBudget}. " +
            $"Last observed: status={last?.Status.ToString() ?? "<null>"}, message={last?.LastMessage ?? "<null>"}.");
        throw new InvalidOperationException("unreachable");
    }
}

/// <summary>Input for <see cref="MatrixFlow"/>; one serializable record, as the flow contract requires.</summary>
public sealed class MatrixFlowInput
{
    /// <summary>Seeds the memoized step result, so the assertion proves the value survived the store.</summary>
    public int Token { get; set; }
}

/// <summary>Terminal states the matrix's simulated remote operation can report.</summary>
public enum MatrixStatus
{
    Unknown = 0,
    Running = 1,
    Completed = 2,
    Failed = 3
}

/// <summary>
/// The matrix's response payload. <see cref="OnRecovery"/> keeps a progress message from consuming a
/// lost-subscriber registration — classifying Running as Resume would let the terminal response
/// arrive with nothing left to route against.
/// </summary>
public sealed class MatrixResult : IAsyncResponsePayload
{
    public MatrixStatus Status { get; set; }

    public string? Message { get; set; }

    /// <inheritdoc />
    public RecoveryAction OnRecovery() => Status switch
    {
        MatrixStatus.Completed => RecoveryAction.Resume,
        MatrixStatus.Running => RecoveryAction.KeepWaiting,
        _ => RecoveryAction.Fail
    };
}

/// <summary>
/// The flow every cell runs: a memoized local step, a durably awaited remote operation, a persisted
/// value, and a final step. Between them they touch every part of the store contract the executor
/// uses — step map, result memoization, pending-correlation breadcrumb, value bag.
/// </summary>
public sealed class MatrixFlow(MatrixFlowProbe probe) : IDurableFlow<MatrixFlowInput>
{
    /// <inheritdoc />
    public async Task ExecuteAsync(IDurableFlowContext flow, MatrixFlowInput input)
    {
        var stamp = await flow.StepAsync("compute-stamp", () => Task.FromResult(input.Token + 100));

        var result = await flow.AwaitStepAsync<MatrixResult>(
            "remote-op",
            correlationId =>
            {
                probe.TriggerFired(flow.FlowId, correlationId);
                return Task.CompletedTask;
            },
            payload => payload.Status is MatrixStatus.Completed or MatrixStatus.Failed);

        if (result.Status == MatrixStatus.Failed)
            throw new DurableFlowFailedException(result.Message ?? "the remote operation failed");

        await flow.SetValueAsync("final-message", result.Message);
        await flow.SetValueAsync("stamp", stamp);
        await flow.StepAsync("notify", () =>
        {
            probe.Completed(flow.FlowId);
            return Task.CompletedTask;
        });
    }
}

/// <summary>
/// The test's side of the flow handshake: it learns the correlation id the flow generated (so the
/// test can play the remote system) and when the flow finished. Keyed by flow id because a cell may
/// run more than one flow.
/// </summary>
public sealed class MatrixFlowProbe
{
    private readonly ConcurrentDictionary<string, TaskCompletionSource<string>> _triggers = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, TaskCompletionSource> _completions = new(StringComparer.Ordinal);

    /// <summary>Completes with the correlation id once the flow's awaited step has triggered.</summary>
    public Task<string> TriggerFor(string flowId) => Slot(_triggers, flowId).Task;

    /// <summary>Completes once the flow's final step has run.</summary>
    public Task CompletionFor(string flowId) => Slot(_completions, flowId).Task;

    internal void TriggerFired(string flowId, string correlationId)
        // TrySet, not Set: a redelivered or resumed run re-enters the trigger, and the first
        // correlation id is the one the breadcrumb pinned.
        => Slot(_triggers, flowId).TrySetResult(correlationId);

    internal void Completed(string flowId) => Slot(_completions, flowId).TrySetResult();

    private static TaskCompletionSource<T> Slot<T>(
        ConcurrentDictionary<string, TaskCompletionSource<T>> map, string flowId)
        => map.GetOrAdd(flowId, _ => new TaskCompletionSource<T>(TaskCreationOptions.RunContinuationsAsynchronously));

    private static TaskCompletionSource Slot(
        ConcurrentDictionary<string, TaskCompletionSource> map, string flowId)
        => map.GetOrAdd(flowId, _ => new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));
}

/// <summary>
/// A service whose method is offloaded to the worker transport. Resolved by name on the worker side,
/// which is why it is registered by interface — the same shape an application uses.
/// </summary>
public interface IMatrixWorkerService
{
    /// <summary>Offloaded entry point; the attribute-free equivalent of the sample's worker endpoint.</summary>
    Task RunAsync(int token);
}

/// <summary>
/// Echoes the correlation id it was invoked with back over the response channel. Equality between
/// that id and the enqueuing one is what proves the transport restored the context.
/// </summary>
public sealed class MatrixWorkerService(IAsyncResponsePublisher publisher) : IMatrixWorkerService
{
    /// <inheritdoc />
    public async Task RunAsync(int token)
    {
        // Restoring the correlation id is the transport's job and the thing this worker exists to
        // report; failing here rather than publishing to a null id makes a transport that lost it say
        // so at the point it was lost.
        var correlationId = AsyncResponseContext.CorrelationId
            ?? throw new InvalidOperationException(
                "The worker ran with no ambient correlation id: the transport did not restore it.");

        await publisher.SetResponse(
            new MatrixResult { Status = MatrixStatus.Completed, Message = correlationId },
            correlationId);
    }
}
