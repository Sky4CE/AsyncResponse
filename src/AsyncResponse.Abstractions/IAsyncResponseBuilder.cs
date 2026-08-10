using System.Diagnostics.CodeAnalysis;
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
    /// <see cref="IAsyncResponseTriggeredBuilder{T}.WaitAsync(Func{AsyncResponseRequestContext, Task})"/>
    /// so simple flows never handle the correlation id themselves:
    /// <c>builder.For&lt;T&gt;().WaitAsync(context =&gt; SendRequest(context.CorrelationId))</c>.
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

    /// <summary>
    /// Publishes a work descriptor to the configured <see cref="IWorkerTransport"/>. Prefer the
    /// expression-based overloads: they keep the target service's methods rooted under trimming,
    /// which a hand-written descriptor cannot.
    /// </summary>
    [RequiresUnreferencedCode("The descriptor names its target service and method as strings, resolved by reflection when the " +
                              "job executes; trimming may have removed them. Use the expression-based EnqueueWorkerAsync<TService> " +
                              "overloads, which root the service's public methods automatically.")]
    Task EnqueueWorkerAsync(ReflectionCallDto work, CancellationToken cancellationToken = default);

    /// <summary>Enqueues a synchronous worker operation expressed as a lambda: <c>svc => svc.DoWork(args)</c>.</summary>
    Task EnqueueWorkerAsync<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicMethods)] TService>(Expression<Action<TService>> work, CancellationToken cancellationToken = default);

    /// <summary>Enqueues an asynchronous worker operation expressed as a lambda: <c>svc => svc.DoWorkAsync(args)</c>.</summary>
    Task EnqueueWorkerAsync<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicMethods)] TService>(Expression<Func<TService, Task>> work, CancellationToken cancellationToken = default);

    /// <summary>Enqueues an asynchronous worker operation returning <see cref="ValueTask"/>.</summary>
    Task EnqueueWorkerAsync<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicMethods)] TService>(Expression<Func<TService, ValueTask>> work, CancellationToken cancellationToken = default);

    /// <summary>
    /// Publishes a work descriptor that becomes deliverable only after <paramref name="delay"/>.
    /// Requires the registered transport to support native delayed delivery
    /// (<see cref="IDelayedWorkerTransport"/>) — see that interface for which transports do and how
    /// longer-than-cap delays are chunked. A non-positive delay publishes immediately. Inside a
    /// durable flow prefer <c>IDurableFlowContext.DelayAsync</c>, which works on every transport.
    /// </summary>
    [RequiresUnreferencedCode("The descriptor names its target service and method as strings, resolved by reflection when the " +
                              "job executes; trimming may have removed them. Use the expression-based EnqueueWorkerAsync<TService> " +
                              "overloads, which root the service's public methods automatically.")]
    Task EnqueueWorkerAsync(ReflectionCallDto work, TimeSpan delay, CancellationToken cancellationToken = default);

    /// <summary>Delayed variant of the synchronous-lambda overload; see <see cref="EnqueueWorkerAsync(ReflectionCallDto, TimeSpan, CancellationToken)"/> for delay semantics.</summary>
    Task EnqueueWorkerAsync<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicMethods)] TService>(Expression<Action<TService>> work, TimeSpan delay, CancellationToken cancellationToken = default);

    /// <summary>Delayed variant of the async-lambda overload; see <see cref="EnqueueWorkerAsync(ReflectionCallDto, TimeSpan, CancellationToken)"/> for delay semantics.</summary>
    Task EnqueueWorkerAsync<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicMethods)] TService>(Expression<Func<TService, Task>> work, TimeSpan delay, CancellationToken cancellationToken = default);

    /// <summary>Delayed variant of the <see cref="ValueTask"/>-lambda overload; see <see cref="EnqueueWorkerAsync(ReflectionCallDto, TimeSpan, CancellationToken)"/> for delay semantics.</summary>
    Task EnqueueWorkerAsync<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicMethods)] TService>(Expression<Func<TService, ValueTask>> work, TimeSpan delay, CancellationToken cancellationToken = default);
}

/// <summary>
/// Builder exposed by durable response-channel packages that can recover a response that arrives
/// after the original waiter disappeared. Use this when the flow registers lost-subscriber resume
/// or failure callbacks; simple in-memory channels only expose <see cref="IAsyncResponseBuilder"/>.
/// </summary>
public interface IRecoverableAsyncResponseBuilder : IAsyncResponseBuilder
{
    /// <inheritdoc cref="IAsyncResponseBuilder.For{T}(string)"/>
    new IRecoverableAsyncResponseAttachedBuilder<T> For<T>(string correlationId) where T : IAsyncResponsePayload;

    /// <inheritdoc cref="IAsyncResponseBuilder.For{T}()"/>
    new IRecoverableAsyncResponseTriggeredBuilder<T> For<T>() where T : IAsyncResponsePayload;
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
    /// Specifies how long to wait before the waiter faults with a <see cref="TimeoutException"/>.
    /// When not specified, the response channel's default applies (for the built-in channels:
    /// the recovery-state expiry) — waits are never infinite, so a response that never arrives
    /// fails the flow instead of leaving it stuck.
    /// </summary>
    IAsyncResponseAttachedBuilder<T> WithTimeout(TimeSpan timeout);

    /// <summary>
    /// Marks this wait as using the default async-response reply target registered by a transport
    /// package. The target is exposed through <see cref="AsyncResponseRequestContext"/> and
    /// <see cref="AsyncResponseContext.ReplyTarget"/> while the trigger executes.
    /// </summary>
    IAsyncResponseAttachedBuilder<T> WithReplyTarget();

    /// <summary>
    /// Marks this wait as using a named async-response reply target registered by a transport
    /// package.
    /// </summary>
    IAsyncResponseAttachedBuilder<T> WithReplyTarget(string name);

    /// <summary>
    /// Marks this wait as using an explicitly supplied async-response reply target.
    /// </summary>
    IAsyncResponseAttachedBuilder<T> WithReplyTarget(AsyncResponseReplyTarget replyTarget);

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
    /// <inheritdoc cref="IAsyncResponseAttachedBuilder{T}.WithTimeout(TimeSpan)"/>
    IAsyncResponseTriggeredBuilder<T> WithTimeout(TimeSpan timeout);

    /// <inheritdoc cref="IAsyncResponseAttachedBuilder{T}.WithReplyTarget()"/>
    IAsyncResponseTriggeredBuilder<T> WithReplyTarget();

    /// <inheritdoc cref="IAsyncResponseAttachedBuilder{T}.WithReplyTarget(string)"/>
    IAsyncResponseTriggeredBuilder<T> WithReplyTarget(string name);

    /// <inheritdoc cref="IAsyncResponseAttachedBuilder{T}.WithReplyTarget(AsyncResponseReplyTarget)"/>
    IAsyncResponseTriggeredBuilder<T> WithReplyTarget(AsyncResponseReplyTarget replyTarget);

    /// <inheritdoc cref="IAsyncResponseAttachedBuilder{T}.Until(Func{T, bool})"/>
    IAsyncResponseTriggeredBuilder<T> Until(Func<T, bool> predicate);

    /// <inheritdoc cref="IAsyncResponseAttachedBuilder{T}.Until(Func{T, Task{bool}})"/>
    IAsyncResponseTriggeredBuilder<T> Until(Func<T, Task<bool>> predicate);

    /// <summary>
    /// Subscribes, runs the required trigger (the action that starts the remote operation),
    /// awaits the terminal response, and unsubscribes automatically. The trigger receives an
    /// <see cref="AsyncResponseRequestContext"/> carrying the generated correlation id and the
    /// selected reply target (if any): read <see cref="AsyncResponseRequestContext.CorrelationId"/>
    /// to persist a recovery breadcrumb or stamp the outgoing request, read
    /// <see cref="AsyncResponseRequestContext.ReplyTarget"/> for reply-to metadata, or ignore the
    /// argument entirely (<c>_ =&gt; Send()</c>) when the ambient <see cref="AsyncResponseContext"/>
    /// is enough.
    /// <para>
    /// The trigger runs strictly <em>after</em> the subscription and recovery state exist, which
    /// closes the race where a fast first response arrives before anyone is listening. If the
    /// trigger throws, the registration is torn down and the exception propagates: the operation
    /// never started, so nothing is left armed. Rule of thumb: never send the request yourself —
    /// pass the send (and any recovery-breadcrumb persistence) as the trigger.
    /// </para>
    /// </summary>
    /// <param name="trigger">The action that starts the remote operation; receives the request context. Required.</param>
    Task<T> WaitAsync(Func<AsyncResponseRequestContext, Task> trigger);
}

/// <summary>
/// Recoverable attached builder returned by <see cref="IRecoverableAsyncResponseBuilder.For{T}(string)"/>.
/// It adds durable lost-subscriber callbacks to the ordinary attached wait API.
/// </summary>
/// <typeparam name="T">The expected response payload type.</typeparam>
public interface IRecoverableAsyncResponseAttachedBuilder<T> : IAsyncResponseAttachedBuilder<T>
    where T : IAsyncResponsePayload
{
    /// <summary>
    /// Registers the lost-subscriber <em>resume</em> callback: invoked when a response payload whose
    /// <see cref="IAsyncResponsePayload.OnRecovery"/> returns <see cref="RecoveryAction.Resume"/>
    /// arrives while no waiter is listening (typically after a redeploy). The callback usually
    /// resumes or re-registers the flow, and receives the payload materialized as the registered
    /// payload type — never raw broker JSON.
    /// </summary>
    [RequiresUnreferencedCode("The callback names its target service and method as strings, resolved by reflection when it fires after a " +
                              "subscriber loss; trimming may have removed them. Use the expression-based overload, which roots the service's " +
                              "public methods automatically.")]
    IRecoverableAsyncResponseAttachedBuilder<T> OnLostSubscriberResume(ReflectionCallDto callback);

    /// <summary>
    /// Expression-based overload of <see cref="OnLostSubscriberResume(ReflectionCallDto)"/>:
    /// <c>svc => svc.ResumeAsync(literalArg, Placeholder.Payload&lt;T&gt;(), Placeholder.CorrelationId())</c>.
    /// Literal arguments are captured by value; <see cref="Placeholder"/> markers are substituted
    /// when the callback fires.
    /// </summary>
    IRecoverableAsyncResponseAttachedBuilder<T> OnLostSubscriberResume<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicMethods)] TService>(Expression<Func<TService, Task>> callback);

    /// <summary>
    /// Registers the lost-subscriber <em>failure</em> callback: invoked when an exception
    /// envelope — or a payload whose <see cref="IAsyncResponsePayload.OnRecovery"/>
    /// returns <see cref="RecoveryAction.Fail"/> (or that cannot be classified) — arrives while no
    /// waiter is listening. Domain failures are delivered as
    /// <see cref="AsyncResponseDomainFailureException"/>.
    /// </summary>
    [RequiresUnreferencedCode("The callback names its target service and method as strings, resolved by reflection when it fires after a " +
                              "subscriber loss; trimming may have removed them. Use the expression-based overload, which roots the service's " +
                              "public methods automatically.")]
    IRecoverableAsyncResponseAttachedBuilder<T> OnLostSubscriberFailure(ReflectionCallDto callback);

    /// <summary>
    /// Expression-based overload of <see cref="OnLostSubscriberFailure(ReflectionCallDto)"/>:
    /// <c>svc => svc.FailAsync(literalArg, Placeholder.Exception(), Placeholder.CorrelationId())</c>.
    /// </summary>
    IRecoverableAsyncResponseAttachedBuilder<T> OnLostSubscriberFailure<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicMethods)] TService>(Expression<Func<TService, Task>> callback);

    /// <inheritdoc cref="IAsyncResponseAttachedBuilder{T}.WithTimeout(TimeSpan)"/>
    new IRecoverableAsyncResponseAttachedBuilder<T> WithTimeout(TimeSpan timeout);

    /// <inheritdoc cref="IAsyncResponseAttachedBuilder{T}.WithReplyTarget()"/>
    new IRecoverableAsyncResponseAttachedBuilder<T> WithReplyTarget();

    /// <inheritdoc cref="IAsyncResponseAttachedBuilder{T}.WithReplyTarget(string)"/>
    new IRecoverableAsyncResponseAttachedBuilder<T> WithReplyTarget(string name);

    /// <inheritdoc cref="IAsyncResponseAttachedBuilder{T}.WithReplyTarget(AsyncResponseReplyTarget)"/>
    new IRecoverableAsyncResponseAttachedBuilder<T> WithReplyTarget(AsyncResponseReplyTarget replyTarget);

    /// <inheritdoc cref="IAsyncResponseAttachedBuilder{T}.Until(Func{T, bool})"/>
    new IRecoverableAsyncResponseAttachedBuilder<T> Until(Func<T, bool> predicate);

    /// <inheritdoc cref="IAsyncResponseAttachedBuilder{T}.Until(Func{T, Task{bool}})"/>
    new IRecoverableAsyncResponseAttachedBuilder<T> Until(Func<T, Task<bool>> predicate);
}

/// <summary>
/// Recoverable triggered builder returned by <see cref="IRecoverableAsyncResponseBuilder.For{T}()"/>.
/// It preserves the trigger-required terminal while exposing durable lost-subscriber callbacks.
/// </summary>
/// <typeparam name="T">The expected response payload type.</typeparam>
public interface IRecoverableAsyncResponseTriggeredBuilder<T> : IAsyncResponseTriggeredBuilder<T>
    where T : IAsyncResponsePayload
{
    /// <inheritdoc cref="IRecoverableAsyncResponseAttachedBuilder{T}.OnLostSubscriberResume(ReflectionCallDto)"/>
    [RequiresUnreferencedCode("The callback names its target service and method as strings, resolved by reflection when it fires after a " +
                              "subscriber loss; trimming may have removed them. Use the expression-based overload, which roots the service's " +
                              "public methods automatically.")]
    IRecoverableAsyncResponseTriggeredBuilder<T> OnLostSubscriberResume(ReflectionCallDto callback);

    /// <inheritdoc cref="IRecoverableAsyncResponseAttachedBuilder{T}.OnLostSubscriberResume{TService}(Expression{Func{TService, Task}})"/>
    IRecoverableAsyncResponseTriggeredBuilder<T> OnLostSubscriberResume<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicMethods)] TService>(Expression<Func<TService, Task>> callback);

    /// <inheritdoc cref="IRecoverableAsyncResponseAttachedBuilder{T}.OnLostSubscriberFailure(ReflectionCallDto)"/>
    [RequiresUnreferencedCode("The callback names its target service and method as strings, resolved by reflection when it fires after a " +
                              "subscriber loss; trimming may have removed them. Use the expression-based overload, which roots the service's " +
                              "public methods automatically.")]
    IRecoverableAsyncResponseTriggeredBuilder<T> OnLostSubscriberFailure(ReflectionCallDto callback);

    /// <inheritdoc cref="IRecoverableAsyncResponseAttachedBuilder{T}.OnLostSubscriberFailure{TService}(Expression{Func{TService, Task}})"/>
    IRecoverableAsyncResponseTriggeredBuilder<T> OnLostSubscriberFailure<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicMethods)] TService>(Expression<Func<TService, Task>> callback);

    /// <inheritdoc cref="IAsyncResponseTriggeredBuilder{T}.WithTimeout(TimeSpan)"/>
    new IRecoverableAsyncResponseTriggeredBuilder<T> WithTimeout(TimeSpan timeout);

    /// <inheritdoc cref="IAsyncResponseTriggeredBuilder{T}.WithReplyTarget()"/>
    new IRecoverableAsyncResponseTriggeredBuilder<T> WithReplyTarget();

    /// <inheritdoc cref="IAsyncResponseTriggeredBuilder{T}.WithReplyTarget(string)"/>
    new IRecoverableAsyncResponseTriggeredBuilder<T> WithReplyTarget(string name);

    /// <inheritdoc cref="IAsyncResponseTriggeredBuilder{T}.WithReplyTarget(AsyncResponseReplyTarget)"/>
    new IRecoverableAsyncResponseTriggeredBuilder<T> WithReplyTarget(AsyncResponseReplyTarget replyTarget);

    /// <inheritdoc cref="IAsyncResponseTriggeredBuilder{T}.Until(Func{T, bool})"/>
    new IRecoverableAsyncResponseTriggeredBuilder<T> Until(Func<T, bool> predicate);

    /// <inheritdoc cref="IAsyncResponseTriggeredBuilder{T}.Until(Func{T, Task{bool}})"/>
    new IRecoverableAsyncResponseTriggeredBuilder<T> Until(Func<T, Task<bool>> predicate);
}
