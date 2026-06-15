using System.Linq.Expressions;

namespace AsyncResponse;

/// <inheritdoc cref="IAsyncResponseBuilder"/>
internal sealed class AsyncResponseBuilder(
    IAsyncResponseSubscriber _subscriber,
    IWorkerTransport? _workerTransport = null) : IAsyncResponseBuilder
{
    /// <inheritdoc />
    public IAsyncResponseBuilder<T> For<T>(string correlationId) where T : IAsyncResponsePayload
        => new AsyncResponseBuilder<T>(
            _subscriber,
            !string.IsNullOrWhiteSpace(correlationId)
                ? correlationId
                : throw new ArgumentNullException(nameof(correlationId), "CorrelationId must not be empty or whitespace."));

    /// <inheritdoc />
    public Task EnqueueWorkerAsync(ReflectionCallDto work)
    {
        ArgumentNullException.ThrowIfNull(work);

        var transport = _workerTransport ?? throw new InvalidOperationException(
            "No IWorkerTransport is registered. Add AddInProcessWorkerTransport() for in-process execution, " +
            "or register a broker-backed IWorkerTransport implementation.");

        return transport.PublishAsync(new WorkerJobEnvelope
        {
            Call = work,
            CorrelationId = AsyncResponseContext.CorrelationId
        });
    }

    /// <inheritdoc />
    public Task EnqueueWorkerAsync<TService>(Expression<Action<TService>> work)
        => EnqueueWorkerAsync(CallbackExpressionConverter.ToReflectionCall(work));

    /// <inheritdoc />
    public Task EnqueueWorkerAsync<TService>(Expression<Func<TService, Task>> work)
        => EnqueueWorkerAsync(CallbackExpressionConverter.ToReflectionCall(work));

    /// <inheritdoc />
    public Task EnqueueWorkerAsync<TService>(Expression<Func<TService, ValueTask>> work)
        => EnqueueWorkerAsync(CallbackExpressionConverter.ToReflectionCall(work));
}

/// <inheritdoc cref="IAsyncResponseBuilder{T}" />
internal sealed class AsyncResponseBuilder<T> : IAsyncResponseBuilder<T> where T : IAsyncResponsePayload
{
    private readonly IAsyncResponseSubscriber _subscriber;
    private readonly string _correlationId;
    private ReflectionCallDto? _resumeCallback;
    private ReflectionCallDto? _failureCallback;
    private Func<T, ValueTask<bool>>? _completionPredicate;
    private TimeSpan? _timeout;
    private Func<Task>? _trigger;

    internal AsyncResponseBuilder(IAsyncResponseSubscriber subscriber, string correlationId)
    {
        _subscriber = subscriber;
        _correlationId = correlationId;
    }

    /// <inheritdoc />
    public IAsyncResponseBuilder<T> OnLostSubscriberResume(ReflectionCallDto callback)
    {
        _resumeCallback = callback ?? throw new ArgumentNullException(nameof(callback));
        return this;
    }

    /// <inheritdoc />
    public IAsyncResponseBuilder<T> OnLostSubscriberResume<TService>(Expression<Func<TService, Task>> callback)
        => OnLostSubscriberResume(CallbackExpressionConverter.ToReflectionCall(callback));

    /// <inheritdoc />
    public IAsyncResponseBuilder<T> OnLostSubscriberFailure(ReflectionCallDto callback)
    {
        _failureCallback = callback ?? throw new ArgumentNullException(nameof(callback));
        return this;
    }

    /// <inheritdoc />
    public IAsyncResponseBuilder<T> OnLostSubscriberFailure<TService>(Expression<Func<TService, Task>> callback)
        => OnLostSubscriberFailure(CallbackExpressionConverter.ToReflectionCall(callback));

    /// <inheritdoc />
    public IAsyncResponseBuilder<T> WithTimeout(TimeSpan timeout)
    {
        if (timeout <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(timeout), "Timeout must be greater than zero.");

        _timeout = timeout;
        return this;
    }

    /// <inheritdoc />
    public IAsyncResponseBuilder<T> Until(Func<T, bool> predicate)
    {
        _completionPredicate = predicate != null
            ? payload => new ValueTask<bool>(predicate(payload))
            : throw new ArgumentNullException(nameof(predicate));
        return this;
    }

    /// <inheritdoc />
    public IAsyncResponseBuilder<T> Until(Func<T, Task<bool>> predicate)
    {
        _completionPredicate = predicate != null
            ? payload => new ValueTask<bool>(predicate(payload))
            : throw new ArgumentNullException(nameof(predicate));
        return this;
    }

    /// <inheritdoc />
    public IAsyncResponseBuilder<T> TriggeredBy(Func<Task> trigger)
    {
        _trigger = trigger ?? throw new ArgumentNullException(nameof(trigger));
        return this;
    }

    /// <inheritdoc />
    public async Task<IAsyncResponseWaiter<T>> BuildWaiterAsync()
    {
        var waiter = await _subscriber.CreateResponseWaiter<T>(
            _correlationId,
            _resumeCallback,
            _failureCallback,
            _completionPredicate,
            _timeout
        ).ConfigureAwait(false);

        if (_trigger is null)
            return waiter;

        // Subscribe-before-send by construction: the trigger runs only once the subscription and
        // the recovery state exist, so the first response can never race the registration.
        try
        {
            await _trigger().ConfigureAwait(false);
        }
        catch
        {
            // The operation never started: tear down the subscription and the recovery state.
            await waiter.DisposeAsync().ConfigureAwait(false);
            throw;
        }

        return waiter;
    }

    /// <inheritdoc />
    public async Task<T> BuildAndWaitAsync()
    {
        await using var waiter = await BuildWaiterAsync().ConfigureAwait(false);
        return await waiter.ResponseTask.ConfigureAwait(false);
    }
}
