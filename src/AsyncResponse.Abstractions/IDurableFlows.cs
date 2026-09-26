using System.Diagnostics.CodeAnalysis;

namespace AsyncResponse;

/// <summary>
/// Starts and manages durable flows (<see cref="IDurableFlow{TInput}"/>). Registered only when the
/// application explicitly selects an atomic flow-state store, for example with
/// <c>WithInMemoryDurableFlows()</c> or a provider-backed durable-flow package.
/// </summary>
public interface IDurableFlows
{
    /// <summary>
    /// Creates a flow run and enqueues its execution on the worker transport. Returns the flow id.
    /// The publish of the start job is the commit point: the job carries the initial ledger and its
    /// execution creates the run if the starter's own ledger write never happened, so a process that
    /// dies mid-start leaves either nothing or a run that executes — never a committed ledger that
    /// nothing will ever wake. Built-in stores validate deterministic creation constraints before
    /// publication. Normally the ledger exists when this method returns; after a transient store
    /// fault it may remain absent until the published job creates it.
    /// <para>
    /// Pass a non-empty <paramref name="flowId"/> to make the start idempotent: starting an id that
    /// already exists with the same flow type and semantically identical input re-enqueues the
    /// existing run (which skips completed steps). Reusing the id for different work is rejected —
    /// before anything is published when the existing ledger can be read up front.
    /// </para>
    /// <para>
    /// The publish of the start job is the start's commit point. Once it succeeded, a cancelled
    /// <paramref name="cancellationToken"/> no longer interrupts the method, and a store failure
    /// while creating or reading the ledger is logged, not thrown — the job creates and runs the
    /// flow when it is picked up — so the id is returned. Two outcomes still throw after the
    /// publish, because the published job will not run the flow either: a
    /// <see cref="DurableFlowIdConflictException"/> when the id turns out to be bound to different
    /// work only at the create that follows the publish (a concurrent start with other input won
    /// the race, or the up-front read failed; the job is dropped on the same test when it runs),
    /// and a deterministic rejection of the initial ledger by a store whose
    /// <see cref="IFlowStateStore.ValidateCreate"/> did not catch it before the publish
    /// (<see cref="FlowStateTooLargeException"/>, <see cref="ArgumentException"/>; the job fails
    /// the same way and is dead-lettered by the worker transport).
    /// </para>
    /// </summary>
    /// <typeparam name="TFlow">The flow class; must be registered in DI and resolvable by its persisted type name.</typeparam>
    /// <typeparam name="TInput">The flow input, persisted as JSON with the flow state.</typeparam>
    /// <exception cref="ArgumentException"><paramref name="flowId"/> is empty or whitespace.</exception>
    /// <exception cref="InvalidOperationException"><paramref name="flowId"/> already belongs to different work.</exception>
    /// <exception cref="DurableFlowNotDispatchedException">
    /// The start job could not be published to the worker transport, even after retries. Nothing
    /// was persisted — the publish is the start's commit point (the job carries the initial ledger
    /// and its execution creates the run), so no orphaned <see cref="FlowRunStatus.Running"/> ledger
    /// is left behind. <see cref="DurableFlowNotDispatchedException.FlowId"/> carries the id the
    /// start would have used (including a generated one) so a retry can stay idempotent:
    /// <code>
    /// try
    /// {
    ///     return await flows.StartAsync&lt;ProvisioningFlow, ProvisionRequest&gt;(request);
    /// }
    /// catch (DurableFlowNotDispatchedException ex)
    /// {
    ///     // Idempotent: if the publish did land after all, the same id dedupes against that run.
    ///     return await flows.StartAsync&lt;ProvisioningFlow, ProvisionRequest&gt;(request, ex.FlowId);
    /// }
    /// </code>
    /// </exception>
    /// <exception cref="OperationCanceledException">
    /// <paramref name="cancellationToken"/> was cancelled before the start job was published. It
    /// ends the publish retry ladder as a cancellation, not as
    /// <see cref="DurableFlowNotDispatchedException"/>. A cancellation that interrupts the publish
    /// is still ambiguous — the job may have landed — so the exception names the flow id in its
    /// message and carries it (including a generated one) in <c>Exception.Data["FlowId"]</c>:
    /// retry with that id and the start stays idempotent.
    /// </exception>
    /// <exception cref="FlowStateTooLargeException">
    /// The initial ledger exceeds the selected store's size budget. Built-in stores reject this
    /// before the start job is published. Keep large inputs in application storage and pass keys.
    /// </exception>
    /// <exception cref="WorkerJobTooLargeException">
    /// The start job — which carries the serialized initial ledger, input included — exceeds the
    /// ingress's <c>AsyncResponseOptions.MaxInboundMessageChars</c> budget; the consuming ingress
    /// would acknowledge it without ever executing it. Deterministic, so not retried and not
    /// wrapped; nothing was persisted. Shrink the input or pass a reference to it.
    /// </exception>
    Task<string> StartAsync<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicMethods | DynamicallyAccessedMemberTypes.Interfaces)] TFlow, TInput>(
        TInput input,
        string? flowId = null,
        CancellationToken cancellationToken = default)
        where TFlow : class, IDurableFlow<TInput>;

    /// <summary>
    /// Re-enqueues execution of an existing run — completed steps are skipped and the in-flight
    /// awaited step re-attaches. No-op for runs that already succeeded or failed.
    /// </summary>
    Task ResumeAsync(string flowId, CancellationToken cancellationToken = default);

    /// <summary>Loads a snapshot of the flow run's state, or <c>null</c> when unknown or expired.</summary>
    Task<FlowState?> GetStateAsync(string flowId, CancellationToken cancellationToken = default);
}
