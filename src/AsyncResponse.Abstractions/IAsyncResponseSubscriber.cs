namespace AsyncResponse;

/// <summary>
/// Low-level entry point for subscribing to asynchronous responses by correlation id.
/// Application code should prefer the fluent <see cref="IAsyncResponseBuilder"/>.
/// </summary>
public interface IAsyncResponseSubscriber
{
    /// <summary>
    /// Subscribes to the response channel associated with <paramref name="correlationId"/>,
    /// stores the recovery callbacks, and returns a disposable
    /// <see cref="IAsyncResponseWaiter{T}"/> for manual lifetime control. Disposing the waiter
    /// cancels the subscription and clears the stored recovery state.
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
    Task<IAsyncResponseWaiter<T>> CreateResponseWaiter<T>(
        string correlationId,
        ReflectionCallDto? resumeCallback = null,
        ReflectionCallDto? failureCallback = null,
        Func<T, ValueTask<bool>>? completionPredicate = null,
        TimeSpan? timeout = null
    ) where T : IAsyncResponsePayload;
}
