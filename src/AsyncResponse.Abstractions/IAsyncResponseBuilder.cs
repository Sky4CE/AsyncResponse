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
    /// using the specified correlation id.
    /// </summary>
    /// <typeparam name="T">The expected response payload type.</typeparam>
    /// <param name="correlationId">The unique identifier that ties the request to its response channel.</param>
    IAsyncResponseBuilder<T> For<T>(string correlationId) where T : IAsyncResponsePayload;

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
    /// Registers the action that triggers the remote operation (sends the request, starts the
    /// job). It runs <em>after</em> the subscription and recovery state exist, which closes the
    /// race where a fast first response arrives before anyone is listening. If the trigger
    /// throws, the waiter is disposed and the exception propagates.
    /// <para>Rule of thumb: never send the request before the waiter exists — express the send
    /// as the trigger instead of calling it beforehand.</para>
    /// </summary>
    IAsyncResponseBuilder<T> TriggeredBy(Func<Task> trigger);

    /// <summary>
    /// Subscribes (running the <see cref="TriggeredBy"/> action, if any, right after) and
    /// returns the waiter for manual lifetime control. Dispose it to cancel the subscription
    /// and clear the persisted recovery state.
    /// </summary>
    Task<IAsyncResponseWaiter<T>> BuildWaiterAsync();

    /// <summary>
    /// Subscribes, triggers, awaits the terminal response (or error/timeout), and disposes the
    /// waiter automatically.
    /// </summary>
    Task<T> BuildAndWaitAsync();
}
