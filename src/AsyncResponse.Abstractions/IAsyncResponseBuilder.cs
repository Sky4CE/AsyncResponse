using System.Linq.Expressions;

namespace AsyncResponse;

/// <summary>
/// Entry point for configuring an asynchronous response waiter and for offloading background
/// worker jobs. This is the API application code is expected to use.
/// </summary>
public interface IAsyncResponseBuilder
{
    /// <summary>
    /// Begins configuring an async-response waiter that <em>attaches</em> to an existing
    /// correlation id — the remote operation was already started (by an earlier run whose step
    /// is being resumed, or by a different system entirely) and this flow only needs to wait
    /// for its outcome.
    /// <para>
    /// Returns an <see cref="IAsyncResponseAttachedBuilder{T}"/>, whose <c>WaitAsync</c> takes
    /// <em>no</em> trigger: re-sending an operation that is already running would double-fire
    /// it, so the type does not offer that option. Flows that start the operation themselves
    /// use <see cref="For{T}()"/>, where the builder generates the correlation id and the
    /// trigger is required.
    /// </para>
    /// </summary>
    /// <typeparam name="T">The expected response payload type.</typeparam>
    /// <param name="correlationId">The unique identifier of the in-flight operation to attach to.</param>
    IAsyncResponseAttachedBuilder<T> For<T>(string correlationId) where T : IAsyncResponsePayload;

    /// <summary>
    /// Begins configuring an async-response waiter with a freshly generated correlation id,
    /// created through <see cref="AsyncResponseContext.CreateCorrelationId"/> so it is also
    /// available ambiently to the outgoing request. Combine with
    /// <see cref="IAsyncResponseTriggeredBuilder{T}.WaitAsync(Func{string, Task})"/> so simple
    /// flows never handle the correlation id themselves:
    /// <c>builder.For&lt;T&gt;().WaitAsync(correlationId =&gt; SendRequest(correlationId))</c>.
    /// <para>
    /// Returns a <see cref="IAsyncResponseTriggeredBuilder{T}"/>, whose <c>WaitAsync</c>
    /// <em>requires</em> the trigger: a generated correlation id is known to nobody else, so a
    /// wait-only call could never receive a response — that mistake is a compile error here.
    /// Flows that persist the correlation id as a recovery breadcrumb do so inside the trigger,
    /// which receives the id once the subscription and recovery state exist.
    /// </para>
    /// </summary>
    /// <typeparam name="T">The expected response payload type.</typeparam>
    IAsyncResponseTriggeredBuilder<T> For<T>() where T : IAsyncResponsePayload;

    /// <summary>Publishes a work descriptor to the configured <see cref="IWorkerTransport"/>.</summary>
    Task EnqueueWorkerAsync(ReflectionCallDto work);

    /// <summary>Enqueues a synchronous worker operation expressed as a lambda: <c>svc => svc.DoWork(args)</c>.</summary>
    Task EnqueueWorkerAsync<TService>(Expression<Action<TService>> work);

    /// <summary>Enqueues an asynchronous worker operation expressed as a lambda: <c>svc => svc.DoWorkAsync(args)</c>.</summary>
    Task EnqueueWorkerAsync<TService>(Expression<Func<TService, Task>> work);

    /// <summary>Enqueues an asynchronous worker operation returning <see cref="ValueTask"/>.</summary>
    Task EnqueueWorkerAsync<TService>(Expression<Func<TService, ValueTask>> work);
}

/// <summary>
/// Builder returned by <see cref="IAsyncResponseBuilder.For{T}(string)"/>, i.e. for waiters that
/// <em>attach</em> to an operation already started elsewhere (an earlier run, another system).
/// Its <see cref="WaitAsync"/> takes no trigger — the operation is already in flight, so there
/// is nothing to send.
/// </summary>
/// <typeparam name="T">The expected response payload type.</typeparam>
public interface IAsyncResponseAttachedBuilder<T> where T : IAsyncResponsePayload
{
    /// <summary>
    /// Registers the lost-subscriber <em>resume</em> callback: invoked when a response whose
    /// domain outcome is <see cref="AsyncResponseOutcome.Succeeded"/> or
    /// <see cref="AsyncResponseOutcome.InProgress"/> arrives while no waiter is listening
    /// (typically after a redeploy). The callback usually resumes or re-registers the flow.
    /// </summary>
    IAsyncResponseAttachedBuilder<T> OnLostSubscriberResume(ReflectionCallDto callback);

    /// <summary>
    /// Expression-based overload of <see cref="OnLostSubscriberResume(ReflectionCallDto)"/>:
    /// <c>svc => svc.ResumeAsync(literalArg, Placeholder.Payload&lt;T&gt;(), Placeholder.CorrelationId())</c>.
    /// Literal arguments are captured by value; <see cref="Placeholder"/> markers are substituted
    /// when the callback fires.
    /// </summary>
    IAsyncResponseAttachedBuilder<T> OnLostSubscriberResume<TService>(Expression<Func<TService, Task>> callback);

    /// <summary>
    /// Registers the lost-subscriber <em>failure</em> callback: invoked when an exception
    /// envelope — or a payload whose domain outcome is <see cref="AsyncResponseOutcome.Failed"/>
    /// or <see cref="AsyncResponseOutcome.Unknown"/> — arrives while no waiter is listening.
    /// Domain failures are delivered as <see cref="AsyncResponseDomainFailureException"/>.
    /// </summary>
    IAsyncResponseAttachedBuilder<T> OnLostSubscriberFailure(ReflectionCallDto callback);

    /// <summary>
    /// Expression-based overload of <see cref="OnLostSubscriberFailure(ReflectionCallDto)"/>:
    /// <c>svc => svc.FailAsync(literalArg, Placeholder.Exception(), Placeholder.CorrelationId())</c>.
    /// </summary>
    IAsyncResponseAttachedBuilder<T> OnLostSubscriberFailure<TService>(Expression<Func<TService, Task>> callback);

    /// <summary>
    /// Specifies how long to wait before the waiter faults with a <see cref="TimeoutException"/>.
    /// When not specified, the transport's default applies (for the Redis transport: the
    /// recovery-state expiry) — waits are never infinite, so a response that never arrives fails
    /// the flow instead of leaving it stuck.
    /// </summary>
    IAsyncResponseAttachedBuilder<T> WithTimeout(TimeSpan timeout);

    /// <summary>
    /// Configures a completion predicate. The waiter keeps receiving payloads until the
    /// predicate returns <c>true</c> — use it to consume intermediate progress messages and
    /// decide which payload is terminal.
    /// </summary>
    IAsyncResponseAttachedBuilder<T> Until(Func<T, bool> predicate);

    /// <inheritdoc cref="Until(Func{T, bool})"/>
    IAsyncResponseAttachedBuilder<T> Until(Func<T, Task<bool>> predicate);

    /// <summary>
    /// Subscribes, awaits the terminal response (or error/timeout), and unsubscribes
    /// automatically. There is deliberately no trigger parameter: this builder attaches to an
    /// operation that is already running, started by an earlier run or a different system —
    /// re-sending it from here would double-fire it. Flows that start the operation themselves
    /// use <see cref="IAsyncResponseBuilder.For{T}()"/>, where the trigger is required.
    /// </summary>
    /// <returns>
    /// The payload of type <typeparamref name="T"/> if successful; otherwise the task faults
    /// with an exception or a <see cref="TimeoutException"/>.
    /// </returns>
    Task<T> WaitAsync();
}

/// <summary>
/// Builder returned by <see cref="IAsyncResponseBuilder.For{T}()"/>, i.e. for waiters whose
/// correlation id was generated by the builder rather than supplied by the flow.
/// <para>
/// A generated correlation id exists nowhere else — no remote system, no persisted flow state —
/// so this flow must be the one that starts the remote operation: <c>WaitAsync</c> requires the
/// trigger, making "wait on a channel nobody will ever publish to" unrepresentable. Flows that
/// attach to an operation already started elsewhere use
/// <see cref="IAsyncResponseBuilder.For{T}(string)"/>, whose <c>WaitAsync</c> takes no trigger.
/// </para>
/// </summary>
/// <typeparam name="T">The expected response payload type.</typeparam>
public interface IAsyncResponseTriggeredBuilder<T> where T : IAsyncResponsePayload
{
    /// <inheritdoc cref="IAsyncResponseAttachedBuilder{T}.OnLostSubscriberResume(ReflectionCallDto)"/>
    IAsyncResponseTriggeredBuilder<T> OnLostSubscriberResume(ReflectionCallDto callback);

    /// <inheritdoc cref="IAsyncResponseAttachedBuilder{T}.OnLostSubscriberResume{TService}(Expression{Func{TService, Task}})"/>
    IAsyncResponseTriggeredBuilder<T> OnLostSubscriberResume<TService>(Expression<Func<TService, Task>> callback);

    /// <inheritdoc cref="IAsyncResponseAttachedBuilder{T}.OnLostSubscriberFailure(ReflectionCallDto)"/>
    IAsyncResponseTriggeredBuilder<T> OnLostSubscriberFailure(ReflectionCallDto callback);

    /// <inheritdoc cref="IAsyncResponseAttachedBuilder{T}.OnLostSubscriberFailure{TService}(Expression{Func{TService, Task}})"/>
    IAsyncResponseTriggeredBuilder<T> OnLostSubscriberFailure<TService>(Expression<Func<TService, Task>> callback);

    /// <inheritdoc cref="IAsyncResponseAttachedBuilder{T}.WithTimeout(TimeSpan)"/>
    IAsyncResponseTriggeredBuilder<T> WithTimeout(TimeSpan timeout);

    /// <inheritdoc cref="IAsyncResponseAttachedBuilder{T}.Until(Func{T, bool})"/>
    IAsyncResponseTriggeredBuilder<T> Until(Func<T, bool> predicate);

    /// <inheritdoc cref="IAsyncResponseAttachedBuilder{T}.Until(Func{T, Task{bool}})"/>
    IAsyncResponseTriggeredBuilder<T> Until(Func<T, Task<bool>> predicate);

    /// <summary>
    /// Subscribes, runs the required trigger (the action that starts the remote operation),
    /// awaits the terminal response, and unsubscribes automatically. The trigger runs strictly
    /// <em>after</em> the subscription and recovery state exist, which closes the race where a
    /// fast first response arrives before anyone is listening. If the trigger throws, the
    /// registration is torn down and the exception propagates: the operation never started, so
    /// nothing is left armed. Rule of thumb: never send the request yourself — pass the send
    /// (and any recovery-breadcrumb persistence) as the trigger.
    /// </summary>
    /// <param name="trigger">The action that starts the remote operation. Required.</param>
    Task<T> WaitAsync(Func<Task> trigger);

    /// <summary>
    /// Same as <see cref="WaitAsync(Func{Task})"/>, with the generated correlation id passed
    /// into the trigger: <c>For&lt;T&gt;().WaitAsync(correlationId =&gt; SendRequest(correlationId))</c>.
    /// Use this overload when the trigger persists the correlation id (e.g. into flow state)
    /// before sending.
    /// </summary>
    /// <param name="trigger">The action that starts the remote operation; receives the correlation id. Required.</param>
    Task<T> WaitAsync(Func<string, Task> trigger);
}
