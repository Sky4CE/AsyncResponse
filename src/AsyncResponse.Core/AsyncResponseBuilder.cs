using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;

namespace AsyncResponse;

internal abstract class AsyncResponseBuilderBase(
    IWorkerTransport? _workerTransport = null,
    IAsyncResponseReplyTargetProvider? _replyTargetProvider = null,
    AsyncResponseContextPropagation? _propagation = null)
{
    protected IAsyncResponseReplyTargetProvider? ReplyTargetProvider => _replyTargetProvider;

    /// <summary>Validates the supplied options.</summary>
    protected static string ValidateCorrelationId(string correlationId)
        => !string.IsNullOrWhiteSpace(correlationId)
            ? correlationId
            : throw new ArgumentNullException(nameof(correlationId), "CorrelationId must not be empty or whitespace.");

    /// <inheritdoc cref="IAsyncResponseBuilder.EnqueueWorkerAsync(ReflectionCallDto, CancellationToken)" />
    [RequiresUnreferencedCode("The descriptor names its target service and method as strings, resolved by reflection when the " +
                              "job executes; trimming may have removed them. Use the expression-based EnqueueWorkerAsync<TService> " +
                              "overloads, which root the service's public methods automatically.")]
    public Task EnqueueWorkerAsync(ReflectionCallDto work, CancellationToken cancellationToken = default)
        => EnqueueWorkerCoreAsync(work, cancellationToken);

    // Shared by the annotation-free expression overloads (whose TService is rooted via
    // DynamicallyAccessedMembers) and the RequiresUnreferencedCode DTO overload above.
    private async Task EnqueueWorkerCoreAsync(ReflectionCallDto work, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(work);
        cancellationToken.ThrowIfCancellationRequested();

        using var activity = AsyncResponseDiagnostics.StartActivity(
            "asyncresponse.enqueue_worker",
            ActivityKind.Producer,
            AsyncResponseContext.CorrelationId);
        AsyncResponseDiagnostics.SetWorker(activity, work);
        AsyncResponseDiagnostics.SetReplyTarget(activity, AsyncResponseContext.ReplyTarget);

        try
        {
            var transport = _workerTransport ?? throw new InvalidOperationException(
                "No IWorkerTransport is registered. Call .WithInMemoryTransport() for in-process execution, " +
                ".WithGooglePubSubTransport(...) for Google Pub/Sub, " +
                "or install another full AsyncResponse transport package.");

            await transport.PublishAsync(
                new WorkerJobEnvelope
                {
                    Call = work,
                    CorrelationId = AsyncResponseContext.CorrelationId,
                    ReplyTarget = AsyncResponseContext.ReplyTarget,
                    Context = _propagation?.Capture()
                },
                cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            AsyncResponseDiagnostics.SetError(activity, ex);
            throw;
        }
    }

    /// <inheritdoc cref="IAsyncResponseBuilder.EnqueueWorkerAsync{TService}(Expression{Action{TService}}, CancellationToken)" />
    public Task EnqueueWorkerAsync<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicMethods)] TService>(Expression<Action<TService>> work, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return EnqueueWorkerCoreAsync(CallbackExpressionConverter.ToReflectionCall(work), cancellationToken);
    }

    /// <inheritdoc cref="IAsyncResponseBuilder.EnqueueWorkerAsync{TService}(Expression{Func{TService, Task}}, CancellationToken)" />
    public Task EnqueueWorkerAsync<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicMethods)] TService>(Expression<Func<TService, Task>> work, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return EnqueueWorkerCoreAsync(CallbackExpressionConverter.ToReflectionCall(work), cancellationToken);
    }

    /// <inheritdoc cref="IAsyncResponseBuilder.EnqueueWorkerAsync{TService}(Expression{Func{TService, ValueTask}}, CancellationToken)" />
    public Task EnqueueWorkerAsync<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicMethods)] TService>(Expression<Func<TService, ValueTask>> work, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return EnqueueWorkerCoreAsync(CallbackExpressionConverter.ToReflectionCall(work), cancellationToken);
    }
}

/// <inheritdoc cref="IAsyncResponseBuilder"/>
internal sealed class AsyncResponseBuilder(
    IAsyncResponseSubscriber _subscriber,
    IWorkerTransport? workerTransport = null,
    IAsyncResponseReplyTargetProvider? replyTargetProvider = null,
    AsyncResponseContextPropagation? propagation = null)
    : AsyncResponseBuilderBase(workerTransport, replyTargetProvider, propagation),
        IAsyncResponseBuilder
{
    /// <inheritdoc />
    public IAsyncResponseAttachedBuilder<T> For<T>(string correlationId) where T : IAsyncResponsePayload
        => new AsyncResponseBuilder<T>(_subscriber, ReplyTargetProvider, ValidateCorrelationId(correlationId));

    /// <inheritdoc />
    public IAsyncResponseTriggeredBuilder<T> For<T>() where T : IAsyncResponsePayload
        => new AsyncResponseBuilder<T>(_subscriber, ReplyTargetProvider, AsyncResponseContext.CreateCorrelationId());
}

/// <inheritdoc cref="IRecoverableAsyncResponseBuilder"/>
internal sealed class RecoverableAsyncResponseBuilder(
    IRecoverableAsyncResponseSubscriber _subscriber,
    IWorkerTransport? workerTransport = null,
    IAsyncResponseReplyTargetProvider? replyTargetProvider = null,
    AsyncResponseContextPropagation? propagation = null)
    : AsyncResponseBuilderBase(workerTransport, replyTargetProvider, propagation),
        IRecoverableAsyncResponseBuilder
{
    /// <inheritdoc />
    public IRecoverableAsyncResponseAttachedBuilder<T> For<T>(string correlationId) where T : IAsyncResponsePayload
        => new RecoverableAsyncResponseBuilder<T>(_subscriber, ReplyTargetProvider, ValidateCorrelationId(correlationId));

    /// <inheritdoc />
    public IRecoverableAsyncResponseTriggeredBuilder<T> For<T>() where T : IAsyncResponsePayload
        => new RecoverableAsyncResponseBuilder<T>(_subscriber, ReplyTargetProvider, AsyncResponseContext.CreateCorrelationId());

    IAsyncResponseAttachedBuilder<T> IAsyncResponseBuilder.For<T>(string correlationId)
        => For<T>(correlationId);

    IAsyncResponseTriggeredBuilder<T> IAsyncResponseBuilder.For<T>()
        => For<T>();
}

/// <inheritdoc cref="IAsyncResponseAttachedBuilder{T}" />
internal class AsyncResponseBuilder<T> : IAsyncResponseAttachedBuilder<T>, IAsyncResponseTriggeredBuilder<T>
    where T : IAsyncResponsePayload
{
    private readonly IAsyncResponseSubscriber _subscriber;
    private readonly IAsyncResponseReplyTargetProvider? _replyTargetProvider;
    protected readonly string _correlationId;
    protected Func<T, ValueTask<bool>>? _completionPredicate;
    protected TimeSpan? _timeout;
    private bool _useReplyTarget;
    private string? _replyTargetName;
    private AsyncResponseReplyTarget? _replyTarget;
    private int _consumed;

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

    /// <summary>Creates the requested resource.</summary>
    protected virtual Task<IAsyncResponseWaiter<T>> CreateWaiterAsync()
        => _subscriber.CreateResponseWaiter<T>(_correlationId, _completionPredicate, _timeout);

    private async Task<T> WaitCoreAsync(Func<AsyncResponseRequestContext, Task>? trigger)
    {
        // Builders are single-use: reuse would re-register the SAME correlation id and re-fire
        // the trigger — exactly the double-send the attached/triggered split exists to prevent.
        if (Interlocked.Exchange(ref _consumed, 1) != 0)
        {
            throw new InvalidOperationException(
                "This async-response builder has already been awaited. Builders are single-use — " +
                "call For<T>() again so every wait gets its own correlation id and registration.");
        }

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

/// <inheritdoc cref="IRecoverableAsyncResponseAttachedBuilder{T}" />
internal sealed class RecoverableAsyncResponseBuilder<T> :
    AsyncResponseBuilder<T>,
    IRecoverableAsyncResponseAttachedBuilder<T>,
    IRecoverableAsyncResponseTriggeredBuilder<T>
    where T : IAsyncResponsePayload
{
    private readonly IRecoverableAsyncResponseSubscriber _subscriber;
    private ReflectionCallDto? _resumeCallback;
    private ReflectionCallDto? _failureCallback;

    internal RecoverableAsyncResponseBuilder(
        IRecoverableAsyncResponseSubscriber subscriber,
        IAsyncResponseReplyTargetProvider? replyTargetProvider,
        string correlationId)
        : base(subscriber, replyTargetProvider, correlationId)
    {
        _subscriber = subscriber;
    }

    /// <summary>Creates the requested resource.</summary>
    protected override Task<IAsyncResponseWaiter<T>> CreateWaiterAsync()
        => _subscriber.CreateRecoverableResponseWaiter<T>(
            _correlationId,
            _resumeCallback,
            _failureCallback,
            _completionPredicate,
            _timeout);

    private void SetResumeCallback(ReflectionCallDto callback)
        => _resumeCallback = callback ?? throw new ArgumentNullException(nameof(callback));

    private void SetFailureCallback(ReflectionCallDto callback)
        => _failureCallback = callback ?? throw new ArgumentNullException(nameof(callback));

    [RequiresUnreferencedCode("The callback names its target service and method as strings, resolved by reflection when it fires after a " +
                              "subscriber loss; trimming may have removed them. Use the expression-based overload, which roots the service's " +
                              "public methods automatically.")]
    IRecoverableAsyncResponseAttachedBuilder<T> IRecoverableAsyncResponseAttachedBuilder<T>.OnLostSubscriberResume(ReflectionCallDto callback)
    {
        SetResumeCallback(callback);
        return this;
    }

    IRecoverableAsyncResponseAttachedBuilder<T> IRecoverableAsyncResponseAttachedBuilder<T>.OnLostSubscriberResume<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicMethods)] TService>(Expression<Func<TService, Task>> callback)
    {
        SetResumeCallback(CallbackExpressionConverter.ToReflectionCall(callback));
        return this;
    }

    [RequiresUnreferencedCode("The callback names its target service and method as strings, resolved by reflection when it fires after a " +
                              "subscriber loss; trimming may have removed them. Use the expression-based overload, which roots the service's " +
                              "public methods automatically.")]
    IRecoverableAsyncResponseAttachedBuilder<T> IRecoverableAsyncResponseAttachedBuilder<T>.OnLostSubscriberFailure(ReflectionCallDto callback)
    {
        SetFailureCallback(callback);
        return this;
    }

    IRecoverableAsyncResponseAttachedBuilder<T> IRecoverableAsyncResponseAttachedBuilder<T>.OnLostSubscriberFailure<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicMethods)] TService>(Expression<Func<TService, Task>> callback)
    {
        SetFailureCallback(CallbackExpressionConverter.ToReflectionCall(callback));
        return this;
    }

    IRecoverableAsyncResponseAttachedBuilder<T> IRecoverableAsyncResponseAttachedBuilder<T>.WithTimeout(TimeSpan timeout)
    {
        WithTimeout(timeout);
        return this;
    }

    IRecoverableAsyncResponseAttachedBuilder<T> IRecoverableAsyncResponseAttachedBuilder<T>.WithReplyTarget()
    {
        WithReplyTarget();
        return this;
    }

    IRecoverableAsyncResponseAttachedBuilder<T> IRecoverableAsyncResponseAttachedBuilder<T>.WithReplyTarget(string name)
    {
        WithReplyTarget(name);
        return this;
    }

    IRecoverableAsyncResponseAttachedBuilder<T> IRecoverableAsyncResponseAttachedBuilder<T>.WithReplyTarget(AsyncResponseReplyTarget replyTarget)
    {
        WithReplyTarget(replyTarget);
        return this;
    }

    IRecoverableAsyncResponseAttachedBuilder<T> IRecoverableAsyncResponseAttachedBuilder<T>.Until(Func<T, bool> predicate)
    {
        Until(predicate);
        return this;
    }

    IRecoverableAsyncResponseAttachedBuilder<T> IRecoverableAsyncResponseAttachedBuilder<T>.Until(Func<T, Task<bool>> predicate)
    {
        Until(predicate);
        return this;
    }

    [RequiresUnreferencedCode("The callback names its target service and method as strings, resolved by reflection when it fires after a " +
                              "subscriber loss; trimming may have removed them. Use the expression-based overload, which roots the service's " +
                              "public methods automatically.")]
    IRecoverableAsyncResponseTriggeredBuilder<T> IRecoverableAsyncResponseTriggeredBuilder<T>.OnLostSubscriberResume(ReflectionCallDto callback)
    {
        SetResumeCallback(callback);
        return this;
    }

    IRecoverableAsyncResponseTriggeredBuilder<T> IRecoverableAsyncResponseTriggeredBuilder<T>.OnLostSubscriberResume<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicMethods)] TService>(Expression<Func<TService, Task>> callback)
    {
        SetResumeCallback(CallbackExpressionConverter.ToReflectionCall(callback));
        return this;
    }

    [RequiresUnreferencedCode("The callback names its target service and method as strings, resolved by reflection when it fires after a " +
                              "subscriber loss; trimming may have removed them. Use the expression-based overload, which roots the service's " +
                              "public methods automatically.")]
    IRecoverableAsyncResponseTriggeredBuilder<T> IRecoverableAsyncResponseTriggeredBuilder<T>.OnLostSubscriberFailure(ReflectionCallDto callback)
    {
        SetFailureCallback(callback);
        return this;
    }

    IRecoverableAsyncResponseTriggeredBuilder<T> IRecoverableAsyncResponseTriggeredBuilder<T>.OnLostSubscriberFailure<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicMethods)] TService>(Expression<Func<TService, Task>> callback)
    {
        SetFailureCallback(CallbackExpressionConverter.ToReflectionCall(callback));
        return this;
    }

    IRecoverableAsyncResponseTriggeredBuilder<T> IRecoverableAsyncResponseTriggeredBuilder<T>.WithTimeout(TimeSpan timeout)
    {
        WithTimeout(timeout);
        return this;
    }

    IRecoverableAsyncResponseTriggeredBuilder<T> IRecoverableAsyncResponseTriggeredBuilder<T>.WithReplyTarget()
    {
        WithReplyTarget();
        return this;
    }

    IRecoverableAsyncResponseTriggeredBuilder<T> IRecoverableAsyncResponseTriggeredBuilder<T>.WithReplyTarget(string name)
    {
        WithReplyTarget(name);
        return this;
    }

    IRecoverableAsyncResponseTriggeredBuilder<T> IRecoverableAsyncResponseTriggeredBuilder<T>.WithReplyTarget(AsyncResponseReplyTarget replyTarget)
    {
        WithReplyTarget(replyTarget);
        return this;
    }

    IRecoverableAsyncResponseTriggeredBuilder<T> IRecoverableAsyncResponseTriggeredBuilder<T>.Until(Func<T, bool> predicate)
    {
        Until(predicate);
        return this;
    }

    IRecoverableAsyncResponseTriggeredBuilder<T> IRecoverableAsyncResponseTriggeredBuilder<T>.Until(Func<T, Task<bool>> predicate)
    {
        Until(predicate);
        return this;
    }
}
