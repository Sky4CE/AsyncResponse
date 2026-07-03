namespace AsyncResponse;

/// <summary>
/// The step context handed to <see cref="IDurableFlow{TInput}.ExecuteAsync"/>. Every method
/// checkpoints into the flow's persisted state, which is what makes the flow safe to re-run from
/// the top after a crash, redeploy, or redelivery:
/// <list type="bullet">
/// <item><see cref="StepAsync(string, Func{Task}, CancellationToken)"/> — a local unit of work,
/// executed once; on re-runs a completed step is skipped (and its memoized result returned).</item>
/// <item><see cref="AwaitStepAsync{TResponse}(string, Func{string, Task}, TimeSpan?, CancellationToken)"/>
/// — trigger a remote operation and await its response. The library owns the correlation-id
/// breadcrumb: a re-run while the operation is still in flight <em>re-attaches</em> to the same
/// wait instead of re-triggering, and lost-subscriber recovery callbacks are registered
/// automatically (resume = re-run the flow; failure = fail the run).</item>
/// </list>
/// Step names are persisted — keep them stable and unique within the flow. Inserting, reordering,
/// or removing steps is an ordinary code edit; in-flight runs pick the changes up on resume.
/// </summary>
public interface IDurableFlowContext
{
    /// <summary>The flow run id — also usable as an idempotency key for step side effects.</summary>
    string FlowId { get; }

    /// <summary>
    /// Runs a local step exactly once per flow run: skipped when its checkpoint says it already
    /// completed. The body must be idempotent for the crash window between completing and the
    /// checkpoint being persisted (at-least-once).
    /// </summary>
    Task StepAsync(string name, Func<Task> step, CancellationToken cancellationToken = default);

    /// <summary>
    /// Runs a local step exactly once per flow run and memoizes its JSON-serialized result in the
    /// flow state: re-runs return the stored value without re-executing, so values that must stay
    /// stable across resumes (computed dates, generated ids) belong in a step.
    /// </summary>
    Task<TResult> StepAsync<TResult>(string name, Func<Task<TResult>> step, CancellationToken cancellationToken = default);

    /// <summary>
    /// Triggers a remote operation and durably awaits its response. The first response completes
    /// the wait. See the overloads with <c>until</c> for progress-aware waits.
    /// <para>
    /// The trigger receives the correlation id to hand to the remote system, and runs only after
    /// the subscription, recovery callbacks, and the persisted breadcrumb exist — a crash at any
    /// point either re-attaches to the in-flight wait or restarts the step; the response can never
    /// arrive before someone is listening. On completion the terminal payload is memoized, so
    /// re-runs return it without waiting again.
    /// </para>
    /// </summary>
    Task<TResponse> AwaitStepAsync<TResponse>(
        string name,
        Func<string, Task> trigger,
        TimeSpan? timeout = null,
        CancellationToken cancellationToken = default) where TResponse : IAsyncResponsePayload;

    /// <summary>
    /// Triggers a remote operation and awaits the first response for which <paramref name="until"/>
    /// returns <c>true</c>; responses for which it returns <c>false</c> are progress messages and
    /// keep the wait open.
    /// </summary>
    Task<TResponse> AwaitStepAsync<TResponse>(
        string name,
        Func<string, Task> trigger,
        Func<TResponse, bool> until,
        TimeSpan? timeout = null,
        CancellationToken cancellationToken = default) where TResponse : IAsyncResponsePayload;

    /// <summary>
    /// Triggers a remote operation and awaits the first response for which <paramref name="until"/>
    /// returns <c>true</c>, with an asynchronous predicate (e.g. to report progress).
    /// </summary>
    Task<TResponse> AwaitStepAsync<TResponse>(
        string name,
        Func<string, Task> trigger,
        Func<TResponse, Task<bool>> until,
        TimeSpan? timeout = null,
        CancellationToken cancellationToken = default) where TResponse : IAsyncResponsePayload;

    /// <summary>
    /// Persists an operator-facing progress message on the flow state
    /// (<see cref="FlowState.LastMessage"/>). Safe to call from <c>until</c> predicates.
    /// </summary>
    Task ReportProgressAsync(string message, CancellationToken cancellationToken = default);

    /// <summary>Reads a value from the flow's persisted key/value bag, or <c>default</c> when absent.</summary>
    TValue? GetValue<TValue>(string key);

    /// <summary>Persists a JSON-serializable value in the flow's key/value bag.</summary>
    Task SetValueAsync<TValue>(string key, TValue value, CancellationToken cancellationToken = default);
}
