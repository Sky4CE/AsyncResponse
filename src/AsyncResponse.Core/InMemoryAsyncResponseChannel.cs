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

    public InMemoryAsyncResponseChannel(
        IServiceScopeFactory scopeFactory,
        IRecoveryStateStore recoveryStateStore,
        IOptions<InMemoryAsyncResponseOptions> options,
        AsyncResponseContextPropagation propagation,
        ILogger<InMemoryAsyncResponseChannel> logger)
    {
        _recoveryStateStore = recoveryStateStore;
        _options = options.Value;
        _propagation = propagation;
        _logger = logger;
        _lostSubscriberDispatcher = new LostSubscriberCallbackDispatcher(scopeFactory, propagation, logger);
    }

    public async Task<IAsyncResponseWaiter<T>> CreateResponseWaiter<T>(
        string correlationId,
        ReflectionCallDto? resumeCallback = null,
        ReflectionCallDto? failureCallback = null,
        Func<T, ValueTask<bool>>? completionPredicate = null,
        TimeSpan? timeout = null) where T : IAsyncResponsePayload
    {
        if (string.IsNullOrWhiteSpace(correlationId))
            throw new ArgumentNullException(nameof(correlationId), "CorrelationId must not be empty or whitespace.");

        completionPredicate ??= _ => new ValueTask<bool>(true);
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
            ExecutionContext.Capture());

        AddSubscription(correlationId, subscription);

        try
        {
            await _recoveryStateStore.SaveAsync(
                correlationId,
                new RecoveryState
                {
                    ResumeCallback = resumeCallback,
                    FailureCallback = failureCallback,
                    CorrelationId = correlationId,
                    PayloadTypeFullName = typeof(T).FullName,
                    RegisteredAtUtc = DateTime.UtcNow,
                    Context = _propagation.Capture()
                },
                _options.RecoveryStateExpiry).ConfigureAwait(false);

            if (subscription.CleanupStarted)
                await _recoveryStateStore.TryDeleteAsync(correlationId).ConfigureAwait(false);
            else
                subscription.ArmTimeout();

            if (_logger.IsEnabled(LogLevel.Information))
                _logger.LogInformation("Waiting for response on correlationId {CorrelationId} with timeout {Timeout}.", correlationId, timeout.Value);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to create in-memory waiter for correlationId {CorrelationId}.", correlationId);
            AsyncResponseDiagnostics.SetError(activity, ex);
            subscription.TrySetException(ex);
            await subscription.CleanupOnceAsync().ConfigureAwait(false);
        }

        return new InMemoryAsyncResponseWaiter<T>(subscription.ResponseTask, subscription.CleanupOnceAsync);
    }

    public Task SetResponse<T>(T response, string? correlationId = null) where T : IAsyncResponsePayload
        => SetResponseCore(response, correlationId);

    Task IRawAsyncResponsePublisher.SetRawResponse(object? response, string? correlationId)
        => SetResponseCore(response, correlationId);

    Task IRawAsyncResponsePublisher.SetRawResponseJson(string responseJson, string? correlationId)
        => SetRawResponseJsonCore(new RawJsonResponse(responseJson), correlationId);

    // Intentionally duplicated with SetRawResponseJsonCore: this is a microbenchmarked publish
    // hot path. Earlier generic/delegate/helper refactors made the code prettier but measurably
    // regressed latency and throughput, so keep the typed path inline unless benchmarks prove out.
    private async Task SetResponseCore<T>(T response, string? correlationId)
    {
        using var activity = AsyncResponseDiagnostics.StartActivity("asyncresponse.set_response", ActivityKind.Producer);
        activity?.SetTag("asyncresponse.channel", "inmemory");
        AsyncResponseDiagnostics.SetPayloadType(activity, typeof(T));

        correlationId ??= AsyncResponseContext.CorrelationId;
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
                var recoveryState = await _recoveryStateStore.GetAsync(correlationId).ConfigureAwait(false);
                var result = await _lostSubscriberDispatcher
                    .DispatchLostResponse(recoveryState, response, ChannelName(correlationId))
                    .ConfigureAwait(false);

                AsyncResponseDiagnostics.SetLostSubscriberRoute(activity, result.ShouldResume);
                activity?.SetTag("asyncresponse.recovery.callback_invoked", result.CallbackInvoked);

                if (result.CallbackInvoked)
                    await _recoveryStateStore.TryDeleteAsync(correlationId).ConfigureAwait(false);

                return;
            }

            await DispatchResponsesAsync(subscribers, response).ConfigureAwait(false);

            if (_logger.IsEnabled(LogLevel.Information))
                _logger.LogInformation("Published response for correlationId {CorrelationId}. PayloadType: {PayloadType}. Subscribers: {SubscriberCount}.", correlationId, typeof(T), subscribers.Count);
        }
        catch (Exception ex)
        {
            AsyncResponseDiagnostics.SetError(activity, ex);
            throw;
        }
    }

    // Intentionally duplicated with SetResponseCore: raw ingress has different dispatch and
    // recovery materialization costs, and keeping the branch inline avoids hot-path indirection.
    private async Task SetRawResponseJsonCore(RawJsonResponse response, string? correlationId)
    {
        using var activity = AsyncResponseDiagnostics.StartActivity("asyncresponse.ingress.raw_response", ActivityKind.Producer);
        activity?.SetTag("asyncresponse.channel", "inmemory");

        correlationId ??= AsyncResponseContext.CorrelationId;
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
                var recoveryState = await _recoveryStateStore.GetAsync(correlationId).ConfigureAwait(false);
                var result = await _lostSubscriberDispatcher
                    .DispatchLostResponse(recoveryState, response.DeserializeUntyped(), ChannelName(correlationId))
                    .ConfigureAwait(false);

                AsyncResponseDiagnostics.SetLostSubscriberRoute(activity, result.ShouldResume);
                activity?.SetTag("asyncresponse.recovery.callback_invoked", result.CallbackInvoked);

                if (result.CallbackInvoked)
                    await _recoveryStateStore.TryDeleteAsync(correlationId).ConfigureAwait(false);

                return;
            }

            await DispatchRawJsonResponsesAsync(subscribers, response).ConfigureAwait(false);

            if (_logger.IsEnabled(LogLevel.Information))
                _logger.LogInformation("Published raw response for correlationId {CorrelationId}. Subscribers: {SubscriberCount}.", correlationId, subscribers.Count);
        }
        catch (Exception ex)
        {
            AsyncResponseDiagnostics.SetError(activity, ex);
            throw;
        }
    }

    public async Task SetException(Exception exception, string? correlationId = null)
    {
        ArgumentNullException.ThrowIfNull(exception);

        using var activity = AsyncResponseDiagnostics.StartActivity("asyncresponse.set_exception", ActivityKind.Producer);
        activity?.SetTag("asyncresponse.channel", "inmemory");
        activity?.SetTag("asyncresponse.exception_type", exception.GetType().FullName ?? exception.GetType().Name);

        correlationId ??= AsyncResponseContext.CorrelationId;
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
                var recoveryState = await _recoveryStateStore.GetAsync(correlationId).ConfigureAwait(false);
                var invoked = await _lostSubscriberDispatcher
                    .DispatchLostException(recoveryState, exception, ChannelName(correlationId))
                    .ConfigureAwait(false);

                activity?.SetTag("asyncresponse.recovery.callback_invoked", invoked);

                if (invoked)
                    await _recoveryStateStore.TryDeleteAsync(correlationId).ConfigureAwait(false);

                return;
            }

            await DispatchExceptionsAsync(subscribers, exception).ConfigureAwait(false);

            _logger.LogInformation("Published exception for correlationId {CorrelationId}. Subscribers: {SubscriberCount}.", correlationId, subscribers.Count);
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
                    if (_many.Count == 1)
                    {
                        _single = _many[0];
                        _many = null;
                    }
                    else if (_many.Count == 0)
                    {
                        _many = null;
                        _closed = true;
                        return true;
                    }

                    return false;
                }

                return false;
            }
        }

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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static SubscriptionSnapshot ForSingle(SubscriptionBase single) => new(single, null);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static SubscriptionSnapshot ForMany(SubscriptionBase[] many) => new(null, many);
    }

    private abstract class SubscriptionBase
    {
        private readonly InMemoryAsyncResponseChannel _owner;
        private readonly CancellationTokenSource _timeoutCts;
        private readonly Activity? _activity;
        private readonly object _cleanupSync = new();
        private CancellationTokenRegistration _timeoutRegistration;
        private Task? _cleanupTask;
        private int _terminal;
        private int _cleanupStarted;

        protected SubscriptionBase(InMemoryAsyncResponseChannel owner, string correlationId, TimeSpan timeout, Activity? activity)
        {
            _owner = owner;
            CorrelationId = correlationId;
            Timeout = timeout;
            _activity = activity;
            _timeoutCts = new CancellationTokenSource();
        }

        public Guid Id { get; } = Guid.NewGuid();
        protected string CorrelationId { get; }
        protected Activity? WaitActivity => _activity;
        private TimeSpan Timeout { get; }
        public bool CleanupStarted
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => Volatile.Read(ref _cleanupStarted) != 0;
        }

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

        public abstract Task DispatchResponseAsync(object? response);

        public abstract Task DispatchRawJsonResponseAsync(RawJsonResponse response);

        public Task DispatchExceptionAsync(Exception exception)
        {
            if (!TryBeginTerminal())
                return Task.CompletedTask;

            AsyncResponseDiagnostics.SetError(_activity, exception);
            TrySetException(exception);
            return CleanupOnceAsTask();
        }

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
            try
            {
                _owner.RemoveSubscription(CorrelationId, Id);
                await _owner._recoveryStateStore.TryDeleteAsync(CorrelationId).ConfigureAwait(false);
            }
            finally
            {
                await _timeoutRegistration.DisposeAsync().ConfigureAwait(false);
                _timeoutCts.Dispose();
                _activity?.Dispose();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected bool TryBeginTerminal()
            => Interlocked.Exchange(ref _terminal, 1) == 0;

        protected abstract void SetTimeoutException(Exception exception);

        public abstract void TrySetException(Exception exception);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected Task CleanupOnceAsTask()
        {
            var cleanup = CleanupOnceAsync();
            return cleanup.IsCompletedSuccessfully ? Task.CompletedTask : cleanup.AsTask();
        }

        private Task TimeoutAsync()
        {
            if (!TryBeginTerminal())
                return Task.CompletedTask;

            _owner._logger.LogWarning("Timed out waiting for response for correlationId {CorrelationId}.", CorrelationId);

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

        public override Task DispatchResponseAsync(object? response)
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

        public override Task DispatchRawJsonResponseAsync(RawJsonResponse response)
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

        protected override void SetTimeoutException(Exception exception)
            => _tcs.TrySetException(exception);

        public override void TrySetException(Exception exception)
            => _tcs.TrySetException(exception);
    }
}

internal sealed class InMemoryAsyncResponseWaiter<T>(
    Task<T> _responseTask,
    Func<ValueTask> _cleanupAsync) : IAsyncResponseWaiter<T> where T : IAsyncResponsePayload
{
    public Task<T> ResponseTask => _responseTask;

    public void Dispose()
        => _cleanupAsync().AsTask().GetAwaiter().GetResult();

    public ValueTask DisposeAsync()
        => _cleanupAsync();
}
