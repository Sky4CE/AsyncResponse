using System.Linq.Expressions;

namespace AsyncResponse;

/// <inheritdoc cref="IAsyncResponseBuilder"/>
internal sealed class AsyncResponseBuilder(
    IAsyncResponseSubscriber _subscriber,
    IWorkerTransport? _workerTransport = null,
    IAsyncResponseReplyTargetProvider? _replyTargetProvider = null,
    AsyncResponseContextPropagation? _propagation = null) : IAsyncResponseBuilder
{
    /// <inheritdoc />
    public IAsyncResponseAttachedBuilder<T> For<T>(string correlationId) where T : IAsyncResponsePayload
        => new AsyncResponseBuilder<T>(
            _subscriber,
            _replyTargetProvider,
            !string.IsNullOrWhiteSpace(correlationId)
                ? correlationId
                : throw new ArgumentNullException(nameof(correlationId), "CorrelationId must not be empty or whitespace."));

    /// <inheritdoc />
    public IAsyncResponseTriggeredBuilder<T> For<T>() where T : IAsyncResponsePayload
        => new AsyncResponseBuilder<T>(_subscriber, _replyTargetProvider, AsyncResponseContext.CreateCorrelationId());

    /// <inheritdoc />
    public Task EnqueueWorkerAsync(ReflectionCallDto work)
    {
        ArgumentNullException.ThrowIfNull(work);

        var transport = _workerTransport ?? throw new InvalidOperationException(
            "No IWorkerTransport is registered. Call .WithInMemoryTransport() for in-process execution, " +
            ".WithGooglePubSubTransport(...) for Google Pub/Sub, " +
            "or register a broker-backed IWorkerTransport implementation.");

        return transport.PublishAsync(new WorkerJobEnvelope
        {
            Call = work,
            CorrelationId = AsyncResponseContext.CorrelationId,
            ReplyTarget = AsyncResponseContext.ReplyTarget,
            Context = _propagation?.Capture()
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

/// <inheritdoc cref="IAsyncResponseAttachedBuilder{T}" />
internal sealed class AsyncResponseBuilder<T> : IAsyncResponseAttachedBuilder<T>, IAsyncResponseTriggeredBuilder<T> where T : IAsyncResponsePayload
{
    private readonly IAsyncResponseSubscriber _subscriber;
    private readonly IAsyncResponseReplyTargetProvider? _replyTargetProvider;
    private readonly string _correlationId;
    private ReflectionCallDto? _resumeCallback;
    private ReflectionCallDto? _failureCallback;
    private Func<T, ValueTask<bool>>? _completionPredicate;
    private TimeSpan? _timeout;
    private bool _useReplyTarget;
    private string? _replyTargetName;
    private AsyncResponseReplyTarget? _replyTarget;

    internal AsyncResponseBuilder(
        IAsyncResponseSubscriber subscriber,
        IAsyncResponseReplyTargetProvider? replyTargetProvider,
        string correlationId)
    {
        _subscriber = subscriber;
        _replyTargetProvider = replyTargetProvider;
        _correlationId = correlationId;
    }

    /// <inheritdoc />
    public IAsyncResponseAttachedBuilder<T> OnLostSubscriberResume(ReflectionCallDto callback)
    {
        _resumeCallback = callback ?? throw new ArgumentNullException(nameof(callback));
        return this;
    }

    /// <inheritdoc />
    public IAsyncResponseAttachedBuilder<T> OnLostSubscriberResume<TService>(Expression<Func<TService, Task>> callback)
        => OnLostSubscriberResume(CallbackExpressionConverter.ToReflectionCall(callback));

    /// <inheritdoc />
    public IAsyncResponseAttachedBuilder<T> OnLostSubscriberFailure(ReflectionCallDto callback)
    {
        _failureCallback = callback ?? throw new ArgumentNullException(nameof(callback));
        return this;
    }

    /// <inheritdoc />
    public IAsyncResponseAttachedBuilder<T> OnLostSubscriberFailure<TService>(Expression<Func<TService, Task>> callback)
        => OnLostSubscriberFailure(CallbackExpressionConverter.ToReflectionCall(callback));

    /// <inheritdoc />
    public IAsyncResponseAttachedBuilder<T> WithTimeout(TimeSpan timeout)
    {
        if (timeout <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(timeout), "Timeout must be greater than zero.");

        _timeout = timeout;
        return this;
    }

    /// <inheritdoc />
    public IAsyncResponseAttachedBuilder<T> WithReplyTarget()
    {
        _useReplyTarget = true;
        _replyTargetName = null;
        _replyTarget = null;
        return this;
    }

    /// <inheritdoc />
    public IAsyncResponseAttachedBuilder<T> WithReplyTarget(string name)
    {
        _useReplyTarget = true;
        _replyTargetName = !string.IsNullOrWhiteSpace(name)
            ? name
            : throw new ArgumentException("Reply target name cannot be null or whitespace.", nameof(name));
        _replyTarget = null;
        return this;
    }

    /// <inheritdoc />
    public IAsyncResponseAttachedBuilder<T> WithReplyTarget(AsyncResponseReplyTarget replyTarget)
    {
        ArgumentNullException.ThrowIfNull(replyTarget);
        ValidateReplyTarget(replyTarget);

        _useReplyTarget = true;
        _replyTargetName = null;
        _replyTarget = replyTarget;
        return this;
    }

    /// <inheritdoc />
    public IAsyncResponseAttachedBuilder<T> Until(Func<T, bool> predicate)
    {
        _completionPredicate = predicate != null
            ? payload => new ValueTask<bool>(predicate(payload))
            : throw new ArgumentNullException(nameof(predicate));
        return this;
    }

    /// <inheritdoc />
    public IAsyncResponseAttachedBuilder<T> Until(Func<T, Task<bool>> predicate)
    {
        _completionPredicate = predicate != null
            ? payload => new ValueTask<bool>(predicate(payload))
            : throw new ArgumentNullException(nameof(predicate));
        return this;
    }

    /// <inheritdoc cref="IAsyncResponseAttachedBuilder{T}.WaitAsync" />
    public Task<T> WaitAsync()
        => WaitCoreAsync((Func<AsyncResponseRequestContext, Task>?)null);

    /// <inheritdoc cref="IAsyncResponseTriggeredBuilder{T}.WaitAsync(Func{AsyncResponseRequestContext, Task})" />
    public Task<T> WaitAsync(Func<AsyncResponseRequestContext, Task> trigger)
    {
        ArgumentNullException.ThrowIfNull(trigger);
        return WaitCoreAsync(trigger);
    }

    private Task<IAsyncResponseWaiter<T>> CreateWaiterAsync()
        => _subscriber.CreateResponseWaiter<T>(
            _correlationId,
            _resumeCallback,
            _failureCallback,
            _completionPredicate,
            _timeout
        );

    private async Task<T> WaitCoreAsync(Func<AsyncResponseRequestContext, Task>? trigger)
    {
        await using var waiter = await CreateWaiterAsync().ConfigureAwait(false);
        var replyTarget = ResolveReplyTarget();
        var requestContext = new AsyncResponseRequestContext(_correlationId, replyTarget);

        // Subscribe-before-send by construction: the trigger runs only once the subscription and
        // the recovery state exist, so the first response can never race the registration. A
        // failing trigger means the operation never started — the waiter (and with it the
        // recovery state) is torn down by the await-using disposal as the exception propagates.
        if (trigger != null)
        {
            using var contextScope = AsyncResponseContext.PushContext(_correlationId, replyTarget);
            await trigger(requestContext).ConfigureAwait(false);
        }

        return await waiter.ResponseTask.ConfigureAwait(false);
    }

    private AsyncResponseReplyTarget? ResolveReplyTarget()
    {
        if (!_useReplyTarget)
            return null;

        var replyTarget = _replyTarget
            ?? (_replyTargetProvider ?? throw new InvalidOperationException(
                "No async-response reply target provider is registered. Register a transport package " +
                "that provides reply targets, such as .WithGooglePubSubTransport(...), or pass an " +
                "explicit AsyncResponseReplyTarget to .WithReplyTarget(...)."))
            .GetReplyTarget(_replyTargetName);

        ValidateReplyTarget(replyTarget);
        return replyTarget;
    }

    private static void ValidateReplyTarget(AsyncResponseReplyTarget replyTarget)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(replyTarget.Name);
        ArgumentException.ThrowIfNullOrWhiteSpace(replyTarget.Transport);
        ArgumentException.ThrowIfNullOrWhiteSpace(replyTarget.Address);
    }

    // -----------------------------------------------------------------------------------------
    // IAsyncResponseTriggeredBuilder<T> — the builder handed out by For<T>() (generated
    // correlation id). Same shared state and behavior; only the static return type differs, so
    // the trigger-required WaitAsync terminal is preserved through the fluent chain. The public
    // WaitAsync(Func<AsyncResponseRequestContext, Task>) overload above satisfies its terminal.

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

    IAsyncResponseTriggeredBuilder<T> IAsyncResponseTriggeredBuilder<T>.WithReplyTarget()
    {
        WithReplyTarget();
        return this;
    }

    IAsyncResponseTriggeredBuilder<T> IAsyncResponseTriggeredBuilder<T>.WithReplyTarget(string name)
    {
        WithReplyTarget(name);
        return this;
    }

    IAsyncResponseTriggeredBuilder<T> IAsyncResponseTriggeredBuilder<T>.WithReplyTarget(AsyncResponseReplyTarget replyTarget)
    {
        WithReplyTarget(replyTarget);
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
