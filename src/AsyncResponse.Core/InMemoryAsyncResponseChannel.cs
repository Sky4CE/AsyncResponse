using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace AsyncResponse;

/// <summary>
/// Process-local response channel registered by <c>AddAsyncResponse().WithInMemoryChannel()</c>.
/// It provides the async-response programming model without Redis or another broker-backed channel.
/// Waiters, subscriptions, and recovery state are all in memory and disappear when the process
/// exits.
/// </summary>
internal sealed class InMemoryAsyncResponseChannel : IAsyncResponsePublisher, IRawAsyncResponsePublisher, IAsyncResponseSubscriber, IActiveSubscriberProbe
{
    private readonly ConcurrentDictionary<string, SubscriptionGroup> _subscriptions = new(StringComparer.Ordinal);
    private readonly IRecoveryStateStore _recoveryStateStore;
    private readonly InMemoryAsyncResponseOptions _options;
    private readonly LostSubscriberCallbackDispatcher _lostSubscriberDispatcher;
    private readonly AsyncResponseContextPropagation _propagation;
    private readonly ILogger<InMemoryAsyncResponseChannel> _logger;

    /// <summary>Creates a process-local async-response channel.</summary>
    public InMemoryAsyncResponseChannel(
        IServiceScopeFactory scopeFactory,
        IRecoveryStateStore recoveryStateStore,
        IOptions<InMemoryAsyncResponseOptions> options,
        AsyncResponseContextPropagation propagation,
        ILogger<InMemoryAsyncResponseChannel> logger)
    {
        _recoveryStateStore = recoveryStateStore;
        _options = options.Value;
        _options.ValidateShared(nameof(InMemoryAsyncResponseOptions));
        _propagation = propagation;
        _logger = logger;
        _lostSubscriberDispatcher = new LostSubscriberCallbackDispatcher(scopeFactory, propagation, logger);
    }

    /// <inheritdoc />
    public async Task<IAsyncResponseWaiter<T>> CreateResponseWaiter<T>(
        string correlationId,
        Func<T, ValueTask<bool>>? completionPredicate = null,
        TimeSpan? timeout = null) where T : IAsyncResponsePayload
    {
        if (string.IsNullOrWhiteSpace(correlationId))
            throw new ArgumentNullException(nameof(correlationId), "CorrelationId must not be empty or whitespace.");

        var hasCustomPredicate = completionPredicate is not null;
        completionPredicate ??= static _ => new ValueTask<bool>(true);
        timeout ??= _options.DefaultTimeout ?? _options.RecoveryStateExpiry;

        if (timeout <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(timeout), "Timeout must be greater than zero.");

        var activity = AsyncResponseDiagnostics.StartActivity("asyncresponse.wait", correlationId: correlationId);
        activity?.SetTag("asyncresponse.channel", "inmemory");
        AsyncResponseDiagnostics.SetPayloadType(activity, typeof(T));
        activity?.SetTag("asyncresponse.timeout_seconds", timeout.Value.TotalSeconds);

        var subscription = new Subscription<T>(
            owner: this,
            correlationId,
            timeout.Value,
            completionPredicate,
            activity,
            // Only restore the subscribe-time ambient context during dispatch when there is a user
            // completion predicate to run under it. With the default (always-complete) predicate,
            // nothing on the dispatch path observes ambient context, so capturing it would only buy
            // a per-dispatch ExecutionContext.Run plus its capturing closure. The waiter's own
            // continuation flows its own context regardless (RunContinuationsAsynchronously).
            hasCustomPredicate ? ExecutionContext.Capture() : null);

        AddSubscription(correlationId, subscription);

        try
        {
            await _recoveryStateStore.SaveAsync(
                correlationId,
                new RecoveryState
                {
                    RegistrationId = subscription.Id,
                    CorrelationId = correlationId,
                    PayloadTypeFullName = typeof(T).FullName,
                    RegisteredAtUtc = DateTime.UtcNow,
                    Context = _propagation.Capture()
                },
                _options.RecoveryStateExpiry).ConfigureAwait(false);

            if (subscription.CleanupStarted)
                await _recoveryStateStore.TryDeleteAsync(correlationId, subscription.Id).ConfigureAwait(false);
            else
                subscription.ArmTimeout();

            if (_logger.IsEnabled(LogLevel.Debug))
                _logger.LogDebug("Waiting for response on correlationId {CorrelationId} with timeout {Timeout}.", correlationId, timeout.Value);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to create in-memory waiter for correlationId {CorrelationId}.", correlationId);
            AsyncResponseDiagnostics.SetError(activity, ex);
            await subscription.CleanupOnceAsync().ConfigureAwait(false);

            // Rethrow instead of returning a pre-faulted waiter: the builder's contract is that
            // the trigger runs only once the subscription AND recovery state exist. A returned
            // waiter would still let the trigger fire the remote operation with no registration
            // left to receive (or recover) its response. Cleanup cancels ResponseTask, so no
            // pending task is left behind.
            throw;
        }

        return new InMemoryAsyncResponseWaiter<T>(subscription.ResponseTask, subscription.CleanupOnceAsync);
    }

    /// <inheritdoc />
    public Task SetResponse<T>(T response, string correlationId, CancellationToken cancellationToken = default) where T : IAsyncResponsePayload
        => SetResponseCore(response, correlationId, cancellationToken);

    Task IRawAsyncResponsePublisher.SetRawResponse(object? response, string correlationId, CancellationToken cancellationToken)
        => SetResponseCore(response, correlationId, cancellationToken);

    Task IRawAsyncResponsePublisher.SetRawResponseJson(string responseJson, string correlationId, CancellationToken cancellationToken)
        => SetRawResponseJsonCore(new RawJsonResponse(responseJson), correlationId, cancellationToken);

    // Intentionally duplicated with SetRawResponseJsonCore: this is a microbenchmarked publish
    // hot path. Earlier generic/delegate/helper refactors made the code prettier but measurably
    // regressed latency and throughput, so keep the typed path inline unless benchmarks prove out.
    private async Task SetResponseCore<T>(T response, string correlationId, CancellationToken cancellationToken)
    {
        using var activity = AsyncResponseDiagnostics.StartActivity("asyncresponse.set_response", ActivityKind.Producer);
        activity?.SetTag("asyncresponse.channel", "inmemory");
        AsyncResponseDiagnostics.SetPayloadType(activity, typeof(T));

        AsyncResponseDiagnostics.SetCorrelationId(activity, correlationId);
        if (string.IsNullOrWhiteSpace(correlationId))
        {
            _logger.LogWarning("CorrelationId is null; cannot publish the response.");
            AsyncResponseDiagnostics.SetError(activity, "correlation_id_null", "CorrelationId is null; cannot publish the response.");
            return;
        }

        try
        {
            var subscribers = SnapshotSubscribers(correlationId);
            activity?.SetTag("asyncresponse.subscribers", subscribers.Count);
            if (subscribers.Count == 0)
            {
                var result = await _lostSubscriberDispatcher
                    .DispatchLostResponses(
                        _recoveryStateStore,
                        correlationId,
                        response,
                        ChannelName(correlationId),
                        cancellationToken,
                        hasLiveSubscriber: () => new ValueTask<bool>(SnapshotSubscribers(correlationId).Count > 0))
                    .ConfigureAwait(false);

                if (!result.RetryLive)
                {
                    AsyncResponseDiagnostics.SetLostSubscriberRoute(activity, result.ShouldResume);
                    AsyncResponseDiagnostics.RecordLostSubscriber("response", result.ShouldResume, result.CallbackInvoked);
                    activity?.SetTag("asyncresponse.recovery.callback_invoked", result.CallbackInvoked);

                    return;
                }

                // A waiter registered between the snapshot and the recovery-state read — deliver
                // live instead of consuming its registration.
                subscribers = SnapshotSubscribers(correlationId);
                activity?.SetTag("asyncresponse.subscribers", subscribers.Count);
            }

            await DispatchResponsesAsync(subscribers, response).ConfigureAwait(false);

            if (_logger.IsEnabled(LogLevel.Debug))
                _logger.LogDebug("Published response for correlationId {CorrelationId}. PayloadType: {PayloadType}. Subscribers: {SubscriberCount}.", correlationId, typeof(T), subscribers.Count);
        }
        catch (Exception ex)
        {
            AsyncResponseDiagnostics.SetError(activity, ex);
            throw;
        }
    }

    // Intentionally duplicated with SetResponseCore: raw ingress has different dispatch and
    // recovery materialization costs, and keeping the branch inline avoids hot-path indirection.
    private async Task SetRawResponseJsonCore(RawJsonResponse response, string correlationId, CancellationToken cancellationToken)
    {
        using var activity = AsyncResponseDiagnostics.StartActivity("asyncresponse.ingress.raw_response", ActivityKind.Producer);
        activity?.SetTag("asyncresponse.channel", "inmemory");

        AsyncResponseDiagnostics.SetCorrelationId(activity, correlationId);
        if (string.IsNullOrWhiteSpace(correlationId))
        {
            _logger.LogWarning("CorrelationId is null; cannot publish the raw response.");
            AsyncResponseDiagnostics.SetError(activity, "correlation_id_null", "CorrelationId is null; cannot publish the raw response.");
            return;
        }

        try
        {
            var subscribers = SnapshotSubscribers(correlationId);
            activity?.SetTag("asyncresponse.subscribers", subscribers.Count);
            if (subscribers.Count == 0)
            {
                var result = await _lostSubscriberDispatcher
                    .DispatchLostResponses(
                        _recoveryStateStore,
                        correlationId,
                        response.DeserializeUntyped(),
                        ChannelName(correlationId),
                        cancellationToken,
                        hasLiveSubscriber: () => new ValueTask<bool>(SnapshotSubscribers(correlationId).Count > 0))
                    .ConfigureAwait(false);

                if (!result.RetryLive)
                {
                    AsyncResponseDiagnostics.SetLostSubscriberRoute(activity, result.ShouldResume);
                    AsyncResponseDiagnostics.RecordLostSubscriber("response", result.ShouldResume, result.CallbackInvoked);
                    activity?.SetTag("asyncresponse.recovery.callback_invoked", result.CallbackInvoked);

                    return;
                }

                // A waiter registered between the snapshot and the recovery-state read — deliver
                // live instead of consuming its registration.
                subscribers = SnapshotSubscribers(correlationId);
                activity?.SetTag("asyncresponse.subscribers", subscribers.Count);
            }

            await DispatchRawJsonResponsesAsync(subscribers, response).ConfigureAwait(false);

            if (_logger.IsEnabled(LogLevel.Debug))
                _logger.LogDebug("Published raw response for correlationId {CorrelationId}. Subscribers: {SubscriberCount}.", correlationId, subscribers.Count);
        }
        catch (Exception ex)
        {
            AsyncResponseDiagnostics.SetError(activity, ex);
            throw;
        }
    }

    /// <inheritdoc />
    public async Task SetException(Exception exception, string correlationId, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(exception);

        using var activity = AsyncResponseDiagnostics.StartActivity("asyncresponse.set_exception", ActivityKind.Producer);
        activity?.SetTag("asyncresponse.channel", "inmemory");
        activity?.SetTag("asyncresponse.exception_type", exception.GetType().FullName ?? exception.GetType().Name);

        AsyncResponseDiagnostics.SetCorrelationId(activity, correlationId);
        if (string.IsNullOrWhiteSpace(correlationId))
        {
            _logger.LogWarning("CorrelationId is null; cannot publish the exception. Exception: {ExceptionMessage}", exception.Message);
            AsyncResponseDiagnostics.SetError(activity, "correlation_id_null", "CorrelationId is null; cannot publish the exception.");
            return;
        }

        try
        {
            var subscribers = SnapshotSubscribers(correlationId);
            activity?.SetTag("asyncresponse.subscribers", subscribers.Count);
            if (subscribers.Count == 0)
            {
                var result = await _lostSubscriberDispatcher
                    .DispatchLostExceptions(
                        _recoveryStateStore,
                        correlationId,
                        exception,
                        ChannelName(correlationId),
                        cancellationToken,
                        hasLiveSubscriber: () => new ValueTask<bool>(SnapshotSubscribers(correlationId).Count > 0))
                    .ConfigureAwait(false);

                if (!result.RetryLive)
                {
                    activity?.SetTag("asyncresponse.recovery.callback_invoked", result.CallbackInvoked);
                    AsyncResponseDiagnostics.RecordLostSubscriber("exception", shouldResume: false, result.CallbackInvoked);

                    return;
                }

                // A waiter registered between the snapshot and the recovery-state read — deliver
                // live instead of consuming its registration.
                subscribers = SnapshotSubscribers(correlationId);
                activity?.SetTag("asyncresponse.subscribers", subscribers.Count);
            }

            await DispatchExceptionsAsync(subscribers, exception).ConfigureAwait(false);

            if (_logger.IsEnabled(LogLevel.Debug))
                _logger.LogDebug("Published exception for correlationId {CorrelationId}. Subscribers: {SubscriberCount}.", correlationId, subscribers.Count);
        }
        catch (Exception ex)
        {
            AsyncResponseDiagnostics.SetError(activity, ex);
            throw;
        }
    }

    /// <inheritdoc />
    public ValueTask<long> CountActiveSubscribersAsync(string correlationId, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(correlationId))
            return new ValueTask<long>(0L);

        long count = _subscriptions.TryGetValue(correlationId, out var subscribers) ? subscribers.Count : 0L;
        return new ValueTask<long>(count);
    }

    private void AddSubscription(string correlationId, SubscriptionBase subscription)
    {
        while (true)
        {
            var group = _subscriptions.GetOrAdd(correlationId, static _ => new SubscriptionGroup());
            if (group.TryAdd(subscription))
                return;

            _subscriptions.TryRemove(new KeyValuePair<string, SubscriptionGroup>(correlationId, group));
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private SubscriptionSnapshot SnapshotSubscribers(string correlationId)
        => _subscriptions.TryGetValue(correlationId, out var subscribers)
            ? subscribers.Snapshot()
            : default;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Task DispatchResponsesAsync(SubscriptionSnapshot subscribers, object? response)
    {
        if (subscribers.Single is { } single)
            return single.DispatchResponseAsync(response);

        return DispatchManyAsync(subscribers.Many, static (subscriber, state) => subscriber.DispatchResponseAsync(state), response);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Task DispatchRawJsonResponsesAsync(SubscriptionSnapshot subscribers, RawJsonResponse response)
    {
        if (subscribers.Single is { } single)
            return single.DispatchRawJsonResponseAsync(response);

        return DispatchManyAsync(subscribers.Many, static (subscriber, state) => subscriber.DispatchRawJsonResponseAsync(state), response);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Task DispatchExceptionsAsync(SubscriptionSnapshot subscribers, Exception exception)
    {
        if (subscribers.Single is { } single)
            return single.DispatchExceptionAsync(exception);

        return DispatchManyAsync(subscribers.Many, static (subscriber, state) => subscriber.DispatchExceptionAsync(state), exception);
    }

    private static Task DispatchManyAsync<TState>(
        SubscriptionBase[]? subscribers,
        Func<SubscriptionBase, TState, Task> dispatch,
        TState state)
    {
        if (subscribers is null || subscribers.Length == 0)
            return Task.CompletedTask;

        Task? firstPending = null;
        List<Task>? pending = null;
        for (var i = 0; i < subscribers.Length; i++)
        {
            var task = dispatch(subscribers[i], state);
            if (task.IsCompletedSuccessfully)
                continue;

            if (firstPending is null)
            {
                firstPending = task;
                continue;
            }

            (pending ??= [firstPending]).Add(task);
        }

        return pending is not null
            ? Task.WhenAll(pending)
            : firstPending ?? Task.CompletedTask;
    }

    private void RemoveSubscription(string correlationId, Guid subscriptionId)
    {
        if (!_subscriptions.TryGetValue(correlationId, out var subscribers))
            return;

        if (subscribers.Remove(subscriptionId))
            _subscriptions.TryRemove(new KeyValuePair<string, SubscriptionGroup>(correlationId, subscribers));
    }

    private static string ChannelName(string correlationId) => $"inmemory:response:{correlationId}";

    private sealed class SubscriptionGroup
    {
        private readonly object _gate = new();
        private SubscriptionBase? _single;
        private List<SubscriptionBase>? _many;
        private bool _closed;

        public int Count
        {
            get
            {
                lock (_gate)
                    return _single is not null ? 1 : _many?.Count ?? 0;
            }
        }

        /// <summary>Adds a subscription to this correlation-id group.</summary>
        public bool TryAdd(SubscriptionBase subscription)
        {
            lock (_gate)
            {
                if (_closed)
                    return false;

                if (_single is null && _many is null)
                {
                    _single = subscription;
                    return true;
                }

                if (_many is null)
                {
                    _many = [_single!, subscription];
                    _single = null;
                    return true;
                }

                _many.Add(subscription);
                return true;
            }
        }

        /// <summary>Removes a subscription and returns whether the group became empty.</summary>
        public bool Remove(Guid subscriptionId)
        {
            lock (_gate)
            {
                if (_single?.Id == subscriptionId)
                {
                    _single = null;
                    _closed = true;
                    return true;
                }

                if (_many is null)
                    return false;

                for (var i = 0; i < _many.Count; i++)
                {
                    if (_many[i].Id != subscriptionId)
                        continue;

                    _many.RemoveAt(i);
                    // _many is only ever created with two entries and collapses to _single at one,
                    // so it can never reach zero here — the group-empty signal is produced solely
                    // by the _single removal path above.
                    if (_many.Count == 1)
                    {
                        _single = _many[0];
                        _many = null;
                    }

                    return false;
                }

                return false;
            }
        }

        /// <summary>Captures the current subscriptions for lock-free dispatch outside the group lock.</summary>
        public SubscriptionSnapshot Snapshot()
        {
            lock (_gate)
            {
                if (_single is not null)
                    return SubscriptionSnapshot.ForSingle(_single);

                if (_many is { Count: > 0 })
                    return SubscriptionSnapshot.ForMany(_many.ToArray());

                return default;
            }
        }
    }

    private readonly struct SubscriptionSnapshot
    {
        private SubscriptionSnapshot(SubscriptionBase? single, SubscriptionBase[]? many)
        {
            Single = single;
            Many = many;
        }

        public SubscriptionBase? Single { get; }
        public SubscriptionBase[]? Many { get; }
        public int Count
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => Single is not null ? 1 : Many?.Length ?? 0;
        }

        /// <summary>Creates a snapshot containing one subscription.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static SubscriptionSnapshot ForSingle(SubscriptionBase single) => new(single, null);

        /// <summary>Creates a snapshot containing multiple subscriptions.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static SubscriptionSnapshot ForMany(SubscriptionBase[] many) => new(null, many);
    }

    private abstract class SubscriptionBase
    {
        private readonly InMemoryAsyncResponseChannel _owner;
        private readonly CancellationTokenSource _timeoutCts;
        private readonly Activity? _activity;
        private readonly object _cleanupSync = new();
        private SemaphoreSlim? _dispatchWaiters;
        private CancellationTokenRegistration _timeoutRegistration;
        private Task? _cleanupTask;
        private int _dispatching;
        private int _dispatchWaiterCount;
        private int _terminal;
        private int _cleanupStarted;

        /// <summary>Creates the common state for an in-memory waiter subscription.</summary>
        protected SubscriptionBase(InMemoryAsyncResponseChannel owner, string correlationId, TimeSpan timeout, Activity? activity)
        {
            _owner = owner;
            CorrelationId = correlationId;
            Timeout = timeout;
            _activity = activity;
            _timeoutCts = new CancellationTokenSource();
        }

        /// <summary>Per-waiter registration id used for subscription and recovery-state cleanup.</summary>
        public Guid Id { get; } = Guid.NewGuid();
        protected string CorrelationId { get; }
        protected Activity? WaitActivity => _activity;
        private TimeSpan Timeout { get; }
        public bool CleanupStarted
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => Volatile.Read(ref _cleanupStarted) != 0;
        }

        /// <summary>Arms the subscription timeout after registration has succeeded.</summary>
        public void ArmTimeout()
        {
            if (CleanupStarted)
                return;

            try
            {
                _timeoutRegistration = _timeoutCts.Token.Register(static state =>
                {
                    _ = ((SubscriptionBase)state!).TimeoutAsync();
                }, this);

                if (CleanupStarted)
                {
                    _timeoutRegistration.Dispose();
                    return;
                }

                _timeoutCts.CancelAfter(Timeout);
            }
            catch (ObjectDisposedException)
            {
                // A response or explicit disposal can clean up between the guard and timer arming.
            }
        }

        /// <summary>Dispatches a typed or materializable response to this subscription.</summary>
        public abstract Task DispatchResponseAsync(object? response);

        /// <summary>Dispatches a raw JSON response to this subscription.</summary>
        public abstract Task DispatchRawJsonResponseAsync(RawJsonResponse response);

        /// <summary>Faults this subscription with a published exception.</summary>
        public Task DispatchExceptionAsync(Exception exception)
            => DispatchSerialAsync(
                exception,
                static (subscription, state) => subscription.DispatchExceptionCoreAsync(state));

        private Task DispatchExceptionCoreAsync(Exception exception)
        {
            if (CleanupStarted)
                return Task.CompletedTask;

            if (!TryBeginTerminal())
                return Task.CompletedTask;

            AsyncResponseDiagnostics.SetError(_activity, exception);
            TrySetException(exception);
            return CleanupOnceAsTask();
        }

        /// <summary>
        /// Serializes every signal for one waiter. The uncontended path uses only an interlocked
        /// owner bit; the semaphore is created lazily if concurrent publishers actually contend.
        /// </summary>
        protected Task DispatchSerialAsync<TState>(
            TState state,
            Func<SubscriptionBase, TState, Task> dispatch)
        {
            if (Interlocked.CompareExchange(ref _dispatching, 1, 0) != 0)
                return WaitAndDispatchAsync(this, state, dispatch);

            Task task;
            try
            {
                task = dispatch(this, state);
            }
            catch
            {
                ReleaseDispatch();
                throw;
            }

            if (task.IsCompletedSuccessfully)
            {
                ReleaseDispatch();
                return task;
            }

            return ReleaseAfterDispatchAsync(this, task);
        }

        private static async Task WaitAndDispatchAsync<TState>(
            SubscriptionBase subscription,
            TState state,
            Func<SubscriptionBase, TState, Task> dispatch)
        {
            Interlocked.Increment(ref subscription._dispatchWaiterCount);
            try
            {
                var waiters = LazyInitializer.EnsureInitialized(
                    ref subscription._dispatchWaiters,
                    static () => new SemaphoreSlim(0));
                while (Interlocked.CompareExchange(ref subscription._dispatching, 1, 0) != 0)
                    await waiters.WaitAsync().ConfigureAwait(false);
            }
            finally
            {
                Interlocked.Decrement(ref subscription._dispatchWaiterCount);
            }

            try
            {
                await dispatch(subscription, state).ConfigureAwait(false);
            }
            finally
            {
                subscription.ReleaseDispatch();
            }
        }

        private static async Task ReleaseAfterDispatchAsync(SubscriptionBase subscription, Task task)
        {
            try
            {
                await task.ConfigureAwait(false);
            }
            finally
            {
                subscription.ReleaseDispatch();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ReleaseDispatch()
        {
            Volatile.Write(ref _dispatching, 0);
            if (Volatile.Read(ref _dispatchWaiterCount) > 0)
                Volatile.Read(ref _dispatchWaiters)?.Release();
        }

        /// <summary>Runs subscription, recovery-state, timeout, and activity cleanup once.</summary>
        public ValueTask CleanupOnceAsync()
        {
            Task cleanupTask;
            lock (_cleanupSync)
            {
                cleanupTask = _cleanupTask ??= StartCleanupAsync();
            }

            return cleanupTask.IsCompletedSuccessfully
                ? ValueTask.CompletedTask
                : new ValueTask(cleanupTask);
        }

        private async Task StartCleanupAsync()
        {
            Volatile.Write(ref _cleanupStarted, 1);

            // A waiter disposed before any terminal signal must not leave ResponseTask pending
            // forever for callers that hold it directly. Cancellation is a no-op after a normal
            // completion, timeout, or fault.
            TrySetCanceled();

            try
            {
                // Delete the recovery state BEFORE removing the subscription. In the reverse order
                // a publish landing in the window sees "no subscriber, state present" and fires a
                // spurious recovery callback for a wait that already reached a terminal state. In
                // this order the window shows a subscriber that drops the message (CleanupStarted)
                // — a late or duplicate terminal message is droppable; a resurrected recovery
                // callback is not.
                await _owner._recoveryStateStore.TryDeleteAsync(CorrelationId, Id).ConfigureAwait(false);
            }
            finally
            {
                _owner.RemoveSubscription(CorrelationId, Id);
                await _timeoutRegistration.DisposeAsync().ConfigureAwait(false);
                _timeoutCts.Dispose();
                _activity?.Dispose();
            }
        }

        /// <summary>Marks this subscription as terminal if no terminal signal has won yet.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected bool TryBeginTerminal()
            => Interlocked.Exchange(ref _terminal, 1) == 0;

        /// <summary>Stores the timeout exception on the concrete waiter task.</summary>
        protected abstract void SetTimeoutException(Exception exception);

        /// <summary>Attempts to fault the concrete waiter task.</summary>
        public abstract void TrySetException(Exception exception);

        /// <summary>Attempts to cancel the concrete waiter task (dispose before any terminal signal).</summary>
        public abstract void TrySetCanceled();

        /// <summary>Returns cleanup as a task for dispatch paths that already operate on <see cref="Task"/>.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected Task CleanupOnceAsTask()
        {
            var cleanup = CleanupOnceAsync();
            return cleanup.IsCompletedSuccessfully ? Task.CompletedTask : cleanup.AsTask();
        }

        private Task TimeoutAsync()
            => DispatchSerialAsync(
                0,
                static (subscription, _) => subscription.TimeoutCoreAsync());

        private Task TimeoutCoreAsync()
        {
            if (CleanupStarted)
                return Task.CompletedTask;

            if (!TryBeginTerminal())
                return Task.CompletedTask;

            _owner._logger.LogWarning("Timed out waiting for response for correlationId {CorrelationId}.", CorrelationId);
            AsyncResponseDiagnostics.RecordWaiterTimeout("inmemory");

            var exception = new TimeoutException($"Timed out waiting for response for correlationId {CorrelationId}.");
            AsyncResponseDiagnostics.SetError(_activity, "timeout", exception.Message);
            SetTimeoutException(exception);
            return CleanupOnceAsTask();
        }
    }

    private sealed class Subscription<T> : SubscriptionBase where T : IAsyncResponsePayload
    {
        private readonly Func<T, ValueTask<bool>> _completionPredicate;
        private readonly ExecutionContext? _capturedContext;
        private readonly TaskCompletionSource<T> _tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Creates a typed in-memory waiter subscription.</summary>
        public Subscription(
            InMemoryAsyncResponseChannel owner,
            string correlationId,
            TimeSpan timeout,
            Func<T, ValueTask<bool>> completionPredicate,
            Activity? activity,
            ExecutionContext? capturedContext)
            : base(owner, correlationId, timeout, activity)
        {
            _completionPredicate = completionPredicate;
            _capturedContext = capturedContext;
        }

        public Task<T> ResponseTask => _tcs.Task;

        /// <inheritdoc />
        public override Task DispatchResponseAsync(object? response)
            => DispatchSerialAsync(
                response,
                static (subscription, state) => ((Subscription<T>)subscription).DispatchResponseUnserializedAsync(state));

        private Task DispatchResponseUnserializedAsync(object? response)
        {
            if (CleanupStarted)
                return Task.CompletedTask;

            // Restore the waiter's subscribe-time ambient context (trace, principal, …) so the
            // completion predicate and any logging run under it, even when the response is delivered
            // on a foreign thread such as a broker ingress callback.
            if (_capturedContext is null)
                return DispatchResponseCoreAsync(response);

            Task? dispatch = null;
            ExecutionContext.Run(_capturedContext, _ => dispatch = DispatchResponseCoreAsync(response), null);
            return dispatch!;
        }

        /// <inheritdoc />
        public override Task DispatchRawJsonResponseAsync(RawJsonResponse response)
            => DispatchSerialAsync(
                response,
                static (subscription, state) => ((Subscription<T>)subscription).DispatchRawJsonResponseUnserializedAsync(state));

        private Task DispatchRawJsonResponseUnserializedAsync(RawJsonResponse response)
        {
            if (CleanupStarted)
                return Task.CompletedTask;

            if (_capturedContext is null)
                return DispatchRawJsonResponseCoreAsync(response);

            Task? dispatch = null;
            ExecutionContext.Run(_capturedContext, _ => dispatch = DispatchRawJsonResponseCoreAsync(response), null);
            return dispatch!;
        }

        private Task DispatchResponseCoreAsync(object? response)
        {
            try
            {
                var payload = response is T typed
                    ? typed
                    : response.As<T>();

                var completion = _completionPredicate(payload);
                if (!completion.IsCompletedSuccessfully)
                    return AwaitCompletionPredicateAsync(completion, payload);

                var finished = completion.Result;
                if (!finished || !TryBeginTerminal())
                    return Task.CompletedTask;

                _tcs.TrySetResult(payload);
                return CleanupOnceAsTask();
            }
            catch (Exception ex)
            {
                if (!TryBeginTerminal())
                    return Task.CompletedTask;

                AsyncResponseDiagnostics.SetError(WaitActivity, ex);
                _tcs.TrySetException(ex);
                return CleanupOnceAsTask();
            }
        }

        private Task DispatchRawJsonResponseCoreAsync(RawJsonResponse response)
        {
            try
            {
                return DispatchPayloadAsync(response.Deserialize<T>()!);
            }
            catch (Exception ex)
            {
                return FaultAsync(ex);
            }
        }

        // Raw ingress has to materialize JSON before it can run the same completion semantics as
        // the typed path. Keep this separate so typed publishers stay on the shorter inline path.
        private Task DispatchPayloadAsync(T payload)
        {
            try
            {
                var completion = _completionPredicate(payload);
                if (!completion.IsCompletedSuccessfully)
                    return AwaitCompletionPredicateAsync(completion, payload);

                var finished = completion.Result;
                if (!finished || !TryBeginTerminal())
                    return Task.CompletedTask;

                _tcs.TrySetResult(payload);
                return CleanupOnceAsTask();
            }
            catch (Exception ex)
            {
                return FaultAsync(ex);
            }
        }

        private Task FaultAsync(Exception exception)
        {
            if (!TryBeginTerminal())
                return Task.CompletedTask;

            AsyncResponseDiagnostics.SetError(WaitActivity, exception);
            _tcs.TrySetException(exception);
            return CleanupOnceAsTask();
        }

        private async Task AwaitCompletionPredicateAsync(ValueTask<bool> completion, T payload)
        {
            try
            {
                var finished = await completion.ConfigureAwait(false);
                if (!finished || !TryBeginTerminal())
                    return;

                _tcs.TrySetResult(payload);
                await CleanupOnceAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                if (!TryBeginTerminal())
                    return;

                AsyncResponseDiagnostics.SetError(WaitActivity, ex);
                _tcs.TrySetException(ex);
                await CleanupOnceAsync().ConfigureAwait(false);
            }
        }

        /// <inheritdoc />
        protected override void SetTimeoutException(Exception exception)
            => _tcs.TrySetException(exception);

        /// <inheritdoc />
        public override void TrySetException(Exception exception)
            => _tcs.TrySetException(exception);

        /// <inheritdoc />
        public override void TrySetCanceled()
            => _tcs.TrySetCanceled();
    }
}

internal sealed class InMemoryAsyncResponseWaiter<T>(
    Task<T> _responseTask,
    Func<ValueTask> _cleanupAsync) : IAsyncResponseWaiter<T> where T : IAsyncResponsePayload
{
    public Task<T> ResponseTask => _responseTask;

    /// <inheritdoc />
    public ValueTask DisposeAsync()
        => _cleanupAsync();
}
