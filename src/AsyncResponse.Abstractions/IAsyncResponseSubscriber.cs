namespace AsyncResponse;

/// <summary>
/// Low-level entry point for subscribing to asynchronous responses by correlation id.
/// Application code should prefer the fluent <see cref="IAsyncResponseBuilder"/>.
/// </summary>
public interface IAsyncResponseSubscriber
{
    /// <summary>
    /// Subscribes to the response channel associated with <paramref name="correlationId"/>,
    /// and returns a disposable <see cref="IAsyncResponseWaiter{T}"/> for manual lifetime control.
    /// Disposing the waiter cancels the subscription, clears the stored recovery state, and — when
    /// no terminal signal was observed yet — cancels <see cref="IAsyncResponseWaiter{T}.ResponseTask"/>.
    /// </summary>
    /// <typeparam name="T">The expected response payload type.</typeparam>
    /// <param name="correlationId">The unique identifier linking the request to its response channel.</param>
    /// <param name="completionPredicate">
    /// Optional predicate deciding whether a received payload completes the wait; when
    /// <c>null</c>, the first payload completes it. Return <c>false</c> to keep waiting
    /// (intermediate/progress messages).
    /// </param>
    /// <param name="timeout">
    /// Optional timeout after which the waiter faults with a <see cref="TimeoutException"/>.
    /// When <c>null</c>, the response channel's default timeout applies — waits are never infinite.
    /// </param>
    Task<IAsyncResponseWaiter<T>> CreateResponseWaiter<T>(
        string correlationId,
        Func<T, ValueTask<bool>>? completionPredicate = null,
        TimeSpan? timeout = null
    ) where T : IAsyncResponsePayload;
}

/// <summary>
/// Low-level subscriber capability exposed by durable response channels that can route a late
/// response to stored lost-subscriber callbacks. Application code should usually prefer
/// <see cref="IRecoverableAsyncResponseBuilder"/>.
/// </summary>
public interface IRecoverableAsyncResponseSubscriber : IAsyncResponseSubscriber
{
    /// <summary>
    /// Subscribes to the response channel, stores durable recovery callbacks, and returns a
    /// disposable <see cref="IAsyncResponseWaiter{T}"/> for manual lifetime control.
    /// </summary>
    /// <typeparam name="T">The expected response payload type.</typeparam>
    /// <param name="correlationId">The unique identifier linking the request to its response channel.</param>
    /// <param name="resumeCallback">
    /// Optional callback invoked when a successful/in-progress response arrives with no live
    /// subscriber (see <see cref="RecoveryState.ResumeCallback"/>).
    /// </param>
    /// <param name="failureCallback">
    /// Optional callback invoked when a failed response or exception arrives with no live
    /// subscriber (see <see cref="RecoveryState.FailureCallback"/>).
    /// </param>
    /// <param name="completionPredicate">
    /// Optional predicate deciding whether a received payload completes the wait; when
    /// <c>null</c>, the first payload completes it. Return <c>false</c> to keep waiting
    /// (intermediate/progress messages).
    /// </param>
    /// <param name="timeout">
    /// Optional timeout after which the waiter faults with a <see cref="TimeoutException"/>.
    /// When <c>null</c>, the response channel's default timeout applies — waits are never infinite.
    /// </param>
    Task<IAsyncResponseWaiter<T>> CreateRecoverableResponseWaiter<T>(
        string correlationId,
        ReflectionCallDto? resumeCallback = null,
        ReflectionCallDto? failureCallback = null,
        Func<T, ValueTask<bool>>? completionPredicate = null,
        TimeSpan? timeout = null
    ) where T : IAsyncResponsePayload;
}
