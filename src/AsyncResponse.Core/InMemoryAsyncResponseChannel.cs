using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Collections.Concurrent;

namespace AsyncResponse;

/// <summary>
/// Process-local response channel registered by <c>AddAsyncResponse().WithInMemoryChannel()</c>.
/// It provides the async-response programming model without Redis or another broker-backed channel.
/// Waiters, subscriptions, and recovery state are all in memory and disappear when the process
/// exits.
/// </summary>
internal sealed class InMemoryAsyncResponseChannel : IAsyncResponsePublisher, IAsyncResponseSubscriber, IActiveSubscriberProbe
{
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<Guid, SubscriptionBase>> _subscriptions = new(StringComparer.Ordinal);
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

        var subscription = new Subscription<T>(
            owner: this,
            correlationId,
            timeout.Value,
            completionPredicate,
            ExecutionContext.Capture());

        subscription.ArmTimeout();

        var subscribers = _subscriptions.GetOrAdd(
            correlationId,
            _ => new ConcurrentDictionary<Guid, SubscriptionBase>());

        subscribers[subscription.Id] = subscription;

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

            _logger.LogInformation("Waiting for response on correlationId {CorrelationId} with timeout {Timeout}.", correlationId, timeout.Value);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to create in-memory waiter for correlationId {CorrelationId}.", correlationId);
            subscription.TrySetException(ex);
            await subscription.CleanupOnceAsync().ConfigureAwait(false);
        }

        return new InMemoryAsyncResponseWaiter<T>(subscription.ResponseTask, subscription.CleanupOnceAsync);
    }

    public async Task SetResponse<T>(T response, string? correlationId = null)
    {
        correlationId ??= AsyncResponseContext.CorrelationId;
        if (string.IsNullOrWhiteSpace(correlationId))
        {
            _logger.LogWarning("CorrelationId is null; cannot publish the response.");
            return;
        }

        var subscribers = SnapshotSubscribers(correlationId);
        if (subscribers.Length == 0)
        {
            var recoveryState = await _recoveryStateStore.GetAsync(correlationId).ConfigureAwait(false);
            var result = await _lostSubscriberDispatcher
                .DispatchLostResponse(recoveryState, response, ChannelName(correlationId))
                .ConfigureAwait(false);

            if (result.CallbackInvoked)
                await _recoveryStateStore.TryDeleteAsync(correlationId).ConfigureAwait(false);

            return;
        }

        await DispatchResponsesAsync(subscribers, response).ConfigureAwait(false);

        _logger.LogInformation("Published response for correlationId {CorrelationId}. PayloadType: {PayloadType}. Subscribers: {SubscriberCount}.", correlationId, typeof(T), subscribers.Length);
    }

    public async Task SetException(Exception exception, string? correlationId = null)
    {
        ArgumentNullException.ThrowIfNull(exception);

        correlationId ??= AsyncResponseContext.CorrelationId;
        if (string.IsNullOrWhiteSpace(correlationId))
        {
            _logger.LogWarning("CorrelationId is null; cannot publish the exception. Exception: {ExceptionMessage}", exception.Message);
            return;
        }

        var subscribers = SnapshotSubscribers(correlationId);
        if (subscribers.Length == 0)
        {
            var recoveryState = await _recoveryStateStore.GetAsync(correlationId).ConfigureAwait(false);
            var invoked = await _lostSubscriberDispatcher
                .DispatchLostException(recoveryState, exception, ChannelName(correlationId))
                .ConfigureAwait(false);

            if (invoked)
                await _recoveryStateStore.TryDeleteAsync(correlationId).ConfigureAwait(false);

            return;
        }

        await DispatchExceptionsAsync(subscribers, exception).ConfigureAwait(false);

        _logger.LogInformation("Published exception for correlationId {CorrelationId}. Subscribers: {SubscriberCount}.", correlationId, subscribers.Length);
    }

    /// <inheritdoc />
    public ValueTask<long> CountActiveSubscribersAsync(string correlationId, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(correlationId))
            return new ValueTask<long>(0L);

        long count = _subscriptions.TryGetValue(correlationId, out var subscribers) ? subscribers.Count : 0L;
        return new ValueTask<long>(count);
    }

    private SubscriptionBase[] SnapshotSubscribers(string correlationId)
        => _subscriptions.TryGetValue(correlationId, out var subscribers)
            ? subscribers.Values.ToArray()
            : [];

    private static Task DispatchResponsesAsync(SubscriptionBase[] subscribers, object? response)
    {
        if (subscribers.Length == 1)
            return subscribers[0].DispatchResponseAsync(response);

        var tasks = new Task[subscribers.Length];
        for (var i = 0; i < subscribers.Length; i++)
            tasks[i] = subscribers[i].DispatchResponseAsync(response);

        return Task.WhenAll(tasks);
    }

    private static Task DispatchExceptionsAsync(SubscriptionBase[] subscribers, Exception exception)
    {
        if (subscribers.Length == 1)
            return subscribers[0].DispatchExceptionAsync(exception);

        var tasks = new Task[subscribers.Length];
        for (var i = 0; i < subscribers.Length; i++)
            tasks[i] = subscribers[i].DispatchExceptionAsync(exception);

        return Task.WhenAll(tasks);
    }

    private void RemoveSubscription(string correlationId, Guid subscriptionId)
    {
        if (!_subscriptions.TryGetValue(correlationId, out var subscribers))
            return;

        subscribers.TryRemove(subscriptionId, out _);
        if (subscribers.IsEmpty)
            _subscriptions.TryRemove(correlationId, out _);
    }

    private static string ChannelName(string correlationId) => $"inmemory:response:{correlationId}";

    private abstract class SubscriptionBase
    {
        private readonly InMemoryAsyncResponseChannel _owner;
        private readonly CancellationTokenSource _timeoutCts;
        private CancellationTokenRegistration _timeoutRegistration;
        private int _terminal;
        private int _cleanupStarted;

        protected SubscriptionBase(InMemoryAsyncResponseChannel owner, string correlationId, TimeSpan timeout)
        {
            _owner = owner;
            CorrelationId = correlationId;
            Timeout = timeout;
            _timeoutCts = new CancellationTokenSource(timeout);
        }

        public Guid Id { get; } = Guid.NewGuid();
        protected string CorrelationId { get; }
        private TimeSpan Timeout { get; }
        public bool CleanupStarted => Volatile.Read(ref _cleanupStarted) != 0;

        public void ArmTimeout()
        {
            _timeoutRegistration = _timeoutCts.Token.Register(static state =>
            {
                _ = ((SubscriptionBase)state!).TimeoutAsync();
            }, this);
        }

        public abstract Task DispatchResponseAsync(object? response);

        public Task DispatchExceptionAsync(Exception exception)
        {
            if (!TryBeginTerminal())
                return Task.CompletedTask;

            TrySetException(exception);
            return CleanupOnceAsTask();
        }

        public async ValueTask CleanupOnceAsync()
        {
            if (Interlocked.Exchange(ref _cleanupStarted, 1) != 0)
                return;

            try
            {
                _owner.RemoveSubscription(CorrelationId, Id);
                await _owner._recoveryStateStore.TryDeleteAsync(CorrelationId).ConfigureAwait(false);
            }
            finally
            {
                await _timeoutRegistration.DisposeAsync().ConfigureAwait(false);
                _timeoutCts.Dispose();
            }
        }

        protected bool TryBeginTerminal()
            => Interlocked.Exchange(ref _terminal, 1) == 0;

        protected abstract void SetTimeoutException(Exception exception);

        public abstract void TrySetException(Exception exception);

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

            SetTimeoutException(new TimeoutException($"Timed out waiting for response for correlationId {CorrelationId}."));
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
            ExecutionContext? capturedContext)
            : base(owner, correlationId, timeout)
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

                _tcs.TrySetException(ex);
                return CleanupOnceAsTask();
            }
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
