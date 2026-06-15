using System.Linq.Expressions;

namespace AsyncResponse;

/// <summary>
/// Entry point for configuring an asynchronous response waiter and for offloading background
/// worker jobs. This is the API application code is expected to use.
/// </summary>
public interface IAsyncResponseBuilder
{
    /// <summary>
    /// Begins configuring an async-response waiter for payloads of type <typeparamref name="T"/>,
    /// using the specified correlation id. Use this overload when the flow needs the correlation
    /// id beforehand — to persist it as a recovery breadcrumb, or to re-attach to an existing one.
    /// </summary>
    /// <typeparam name="T">The expected response payload type.</typeparam>
    /// <param name="correlationId">The unique identifier that ties the request to its response channel.</param>
    IAsyncResponseBuilder<T> For<T>(string correlationId) where T : IAsyncResponsePayload;

    /// <summary>
    /// Begins configuring an async-response waiter with a freshly generated correlation id,
    /// created through <see cref="AsyncResponseContext.CreateCorrelationId"/> so it is also
    /// available ambiently to the outgoing request. Combine with
    /// <see cref="IAsyncResponseBuilder{T}.WaitAsync(Func{string, Task})"/> so simple flows never
    /// handle the correlation id themselves:
    /// <c>builder.For&lt;T&gt;().WaitAsync(correlationId =&gt; SendRequest(correlationId))</c>.
    /// </summary>
    /// <typeparam name="T">The expected response payload type.</typeparam>
    IAsyncResponseBuilder<T> For<T>() where T : IAsyncResponsePayload;

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
/// Generic, chainable builder for configuring and awaiting an async response of type
/// <typeparamref name="T"/>.
/// </summary>
/// <typeparam name="T">The expected response payload type.</typeparam>
public interface IAsyncResponseBuilder<T> where T : IAsyncResponsePayload
{
    /// <summary>
    /// Registers the lost-subscriber <em>resume</em> callback: invoked when a response whose
    /// domain outcome is <see cref="AsyncResponseOutcome.Succeeded"/> or
    /// <see cref="AsyncResponseOutcome.InProgress"/> arrives while no waiter is listening
    /// (typically after a redeploy). The callback usually resumes or re-registers the flow.
    /// </summary>
    IAsyncResponseBuilder<T> OnLostSubscriberResume(ReflectionCallDto callback);

    /// <summary>
    /// Expression-based overload of <see cref="OnLostSubscriberResume(ReflectionCallDto)"/>:
    /// <c>svc => svc.ResumeAsync(literalArg, Placeholder.Payload&lt;T&gt;(), Placeholder.CorrelationId())</c>.
    /// Literal arguments are captured by value; <see cref="Placeholder"/> markers are substituted
    /// when the callback fires.
    /// </summary>
    IAsyncResponseBuilder<T> OnLostSubscriberResume<TService>(Expression<Func<TService, Task>> callback);

    /// <summary>
    /// Registers the lost-subscriber <em>failure</em> callback: invoked when an exception
    /// envelope — or a payload whose domain outcome is <see cref="AsyncResponseOutcome.Failed"/>
    /// or <see cref="AsyncResponseOutcome.Unknown"/> — arrives while no waiter is listening.
    /// Domain failures are delivered as <see cref="AsyncResponseDomainFailureException"/>.
    /// </summary>
    IAsyncResponseBuilder<T> OnLostSubscriberFailure(ReflectionCallDto callback);

    /// <summary>
    /// Expression-based overload of <see cref="OnLostSubscriberFailure(ReflectionCallDto)"/>:
    /// <c>svc => svc.FailAsync(literalArg, Placeholder.Exception(), Placeholder.CorrelationId())</c>.
    /// </summary>
    IAsyncResponseBuilder<T> OnLostSubscriberFailure<TService>(Expression<Func<TService, Task>> callback);

    /// <summary>
    /// Specifies how long to wait before the waiter faults with a <see cref="TimeoutException"/>.
    /// When not specified, the transport's default applies (for the Redis transport: the
    /// recovery-state expiry) — waits are never infinite, so a response that never arrives fails
    /// the flow instead of leaving it stuck.
    /// </summary>
    IAsyncResponseBuilder<T> WithTimeout(TimeSpan timeout);

    /// <summary>
    /// Configures a completion predicate. The waiter keeps receiving payloads until the
    /// predicate returns <c>true</c> — use it to consume intermediate progress messages and
    /// decide which payload is terminal.
    /// </summary>
    IAsyncResponseBuilder<T> Until(Func<T, bool> predicate);

    /// <inheritdoc cref="Until(Func{T, bool})"/>
    IAsyncResponseBuilder<T> Until(Func<T, Task<bool>> predicate);

    /// <summary>
    /// Subscribes, optionally triggers the remote operation, awaits the terminal response
    /// (or error/timeout), and unsubscribes automatically. This is the terminal operation for
    /// virtually every flow.
    /// <para>
    /// The trigger — the action that sends the request / starts the remote job — runs strictly
    /// <em>after</em> the subscription and the recovery state exist, which closes the race where
    /// a fast first response arrives before anyone is listening. If the trigger throws, the
    /// subscription and recovery state are torn down and the exception propagates: the operation
    /// never started, so nothing is left armed. Rule of thumb: never send the request yourself —
    /// pass the send as the trigger.
    /// </para>
    /// <para>
    /// Pass <c>null</c> (or no argument) when there is nothing to trigger: the request was
    /// already sent — by an earlier run whose step is being re-attached after a resume, or by a
    /// different system entirely. Only the flow can know this (typically from its persisted
    /// state); the transport cannot detect it.
    /// </para>
    /// </summary>
    /// <param name="trigger">The action that starts the remote operation, or <c>null</c> to only wait.</param>
    /// <returns>
    /// The payload of type <typeparamref name="T"/> if successful; otherwise the task faults
    /// with an exception or a <see cref="TimeoutException"/>.
    /// </returns>
    Task<T> WaitAsync(Func<Task>? trigger = null);

    /// <summary>
    /// Same as <see cref="WaitAsync(Func{Task})"/>, with the waiter's correlation id passed into
    /// the trigger — convenient with <see cref="IAsyncResponseBuilder.For{T}()"/> so simple
    /// flows never handle the correlation id themselves.
    /// </summary>
    /// <param name="trigger">The action that starts the remote operation; receives the correlation id.</param>
    Task<T> WaitAsync(Func<string, Task> trigger);

    /// <summary>
    /// Advanced escape hatch: subscribes and returns the waiter for manual lifetime control —
    /// e.g. registering recovery callbacks without awaiting in place, or selecting over several
    /// waiters. Dispose it to cancel the subscription and clear the persisted recovery state.
    /// No trigger runs here; if you send a request after this returns, the subscription already
    /// exists, so the ordering stays safe. Prefer <see cref="WaitAsync(Func{Task})"/>.
    /// </summary>
    Task<IAsyncResponseWaiter<T>> BuildWaiterAsync();
}
