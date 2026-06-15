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
    public IAsyncResponseTriggeredBuilder<T> For<T>() where T : IAsyncResponsePayload
        => new AsyncResponseBuilder<T>(_subscriber, AsyncResponseContext.CreateCorrelationId());

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
internal sealed class AsyncResponseBuilder<T> : IAsyncResponseBuilder<T>, IAsyncResponseTriggeredBuilder<T> where T : IAsyncResponsePayload
{
    private readonly IAsyncResponseSubscriber _subscriber;
    private readonly string _correlationId;
    private ReflectionCallDto? _resumeCallback;
    private ReflectionCallDto? _failureCallback;
    private Func<T, ValueTask<bool>>? _completionPredicate;
    private TimeSpan? _timeout;

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
    public Task<T> WaitAsync(Func<Task>? trigger = null)
        => WaitCoreAsync(trigger);

    /// <inheritdoc />
    public Task<T> WaitAsync(Func<string, Task> trigger)
    {
        ArgumentNullException.ThrowIfNull(trigger);
        return WaitCoreAsync(() => trigger(_correlationId));
    }

    /// <inheritdoc />
    public Task<IAsyncResponseWaiter<T>> BuildWaiterAsync()
        => _subscriber.CreateResponseWaiter<T>(
            _correlationId,
            _resumeCallback,
            _failureCallback,
            _completionPredicate,
            _timeout
        );

    private async Task<T> WaitCoreAsync(Func<Task>? trigger)
    {
        await using var waiter = await BuildWaiterAsync().ConfigureAwait(false);

        // Subscribe-before-send by construction: the trigger runs only once the subscription and
        // the recovery state exist, so the first response can never race the registration. A
        // failing trigger means the operation never started — the waiter (and with it the
        // recovery state) is torn down by the await-using disposal as the exception propagates.
        if (trigger != null)
            await trigger().ConfigureAwait(false);

        return await waiter.ResponseTask.ConfigureAwait(false);
    }

    // -----------------------------------------------------------------------------------------
    // IAsyncResponseTriggeredBuilder<T> — the builder handed out by For<T>() (generated
    // correlation id). Same shared state and behavior; only the static return type differs, so
    // the trigger-required WaitAsync terminal is preserved through the fluent chain. The shared
    // WaitAsync/BuildWaiterAsync implementations above satisfy both interfaces.

    IAsyncResponseTriggeredBuilder<T> IAsyncResponseTriggeredBuilder<T>.OnLostSubscriberResume(ReflectionCallDto callback)
    {
        OnLostSubscriberResume(callback);
        return this;
    }

    IAsyncResponseTriggeredBuilder<T> IAsyncResponseTriggeredBuilder<T>.OnLostSubscriberResume<TService>(Expression<Func<TService, Task>> callback)
    {
        OnLostSubscriberResume(callback);
        return this;
    }

    IAsyncResponseTriggeredBuilder<T> IAsyncResponseTriggeredBuilder<T>.OnLostSubscriberFailure(ReflectionCallDto callback)
    {
        OnLostSubscriberFailure(callback);
        return this;
    }

    IAsyncResponseTriggeredBuilder<T> IAsyncResponseTriggeredBuilder<T>.OnLostSubscriberFailure<TService>(Expression<Func<TService, Task>> callback)
    {
        OnLostSubscriberFailure(callback);
        return this;
    }

    IAsyncResponseTriggeredBuilder<T> IAsyncResponseTriggeredBuilder<T>.WithTimeout(TimeSpan timeout)
    {
        WithTimeout(timeout);
        return this;
    }

    IAsyncResponseTriggeredBuilder<T> IAsyncResponseTriggeredBuilder<T>.Until(Func<T, bool> predicate)
    {
        Until(predicate);
        return this;
    }

    IAsyncResponseTriggeredBuilder<T> IAsyncResponseTriggeredBuilder<T>.Until(Func<T, Task<bool>> predicate)
    {
        Until(predicate);
        return this;
    }
}
