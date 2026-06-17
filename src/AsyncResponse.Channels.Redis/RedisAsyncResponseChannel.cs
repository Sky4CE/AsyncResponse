using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StackExchange.Redis;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text.Json;

namespace AsyncResponse.Channels.Redis;

/// <summary>
/// Redis-backed response channel:
/// <list type="bullet">
/// <item><description>Publishes responses to Redis pub/sub channels keyed by correlation id.</description></item>
/// <item><description>Subscribes waiters to those channels with per-channel serialized handling.</description></item>
/// <item><description>Persists <see cref="RecoveryState"/> so responses arriving after the waiter
/// died (e.g. a redeploy) are routed through the lost-subscriber dispatcher, which asks the payload's
/// ShouldResumeOnRecovery and invokes the resume or failure callback.</description></item>
/// </list>
/// </summary>
internal sealed class RedisAsyncResponseChannel : IAsyncResponsePublisher, IAsyncResponseSubscriber, IActiveSubscriberProbe
{
    /// <summary>OpenTelemetry-compatible activity source for the AsyncResponse library.</summary>
    internal static readonly ActivitySource ActivitySource = new("AsyncResponse");

    private readonly ISubscriber _subscriber;
    private readonly IConnectionMultiplexer _multiplexer;
    private readonly IRecoveryStateStore _recoveryStateStore;
    private readonly AsyncResponseContextPropagation _propagation;
    private readonly LostSubscriberCallbackDispatcher _lostSubscriberDispatcher;
    private readonly RedisKeySchema _keys;
    private readonly RedisAsyncResponseOptions _options;
    private readonly ILogger<RedisAsyncResponseChannel> _logger;

    private readonly ConcurrentDictionary<string, ChannelSerialExecutor> _executors = new(StringComparer.Ordinal);

    public RedisAsyncResponseChannel(
        IServiceScopeFactory scopeFactory,
        IConnectionMultiplexer multiplexer,
        IRecoveryStateStore recoveryStateStore,
        IOptions<RedisAsyncResponseOptions> options,
        AsyncResponseContextPropagation propagation,
        ILogger<RedisAsyncResponseChannel> logger)
    {
        _subscriber = multiplexer.GetSubscriber();
        _multiplexer = multiplexer;
        _recoveryStateStore = recoveryStateStore;
        _propagation = propagation;
        _options = options.Value;
        _keys = new RedisKeySchema(_options.KeyPrefix);
        _logger = logger;
        _lostSubscriberDispatcher = new LostSubscriberCallbackDispatcher(scopeFactory, propagation, logger);
    }

    // ---------------------------------------------------------------------------------------
    // IAsyncResponseSubscriber

    /// <inheritdoc/>
    public async Task<IAsyncResponseWaiter<T>> CreateResponseWaiter<T>(
        string correlationId,
        ReflectionCallDto? resumeCallback = null,
        ReflectionCallDto? failureCallback = null,
        Func<T, ValueTask<bool>>? completionPredicate = null,
        TimeSpan? timeout = null) where T : IAsyncResponsePayload
    {
        if (string.IsNullOrWhiteSpace(correlationId))
            throw new ArgumentNullException(nameof(correlationId), "CorrelationId must not be empty or whitespace.");

        // Recovery callbacks only make sense if the payload can say whether a late response should
        // resume or fail the flow. On this durable channel that decision is real (it survives a
        // redeploy), so require the override rather than letting the conservative default silently
        // route every recovered response to the failure callback. The in-memory channel, which
        // cannot recover across a process restart, is deliberately not subject to this check.
        if ((resumeCallback is not null || failureCallback is not null)
            && !AsyncResponsePayloadReflection.OverridesShouldResumeOnRecovery(typeof(T)))
        {
            throw new InvalidOperationException(
                $"Payload type '{typeof(T)}' registers lost-subscriber recovery callbacks on the Redis channel " +
                $"but does not override {nameof(IAsyncResponsePayload)}.{nameof(IAsyncResponsePayload.ShouldResumeOnRecovery)}(). " +
                "Override it to declare which responses resume the flow (return true) versus fail it (return false); " +
                "the durable channel needs this to route a response that arrives after the waiter was lost.");
        }

        // default: first envelope completes the wait
        completionPredicate ??= _ => new ValueTask<bool>(true);

        // Default timeout aligned with the recovery-state expiry: an infinite wait is never
        // meaningful, because once the recovery state expires the correlation id has no recovery
        // anyway. Timing out routes the flow through its normal failure handling instead of
        // leaving it stuck forever.
        timeout ??= _options.DefaultTimeout ?? _options.RecoveryStateExpiry;

        var storedCorrelationId = correlationId;
        // Capture the subscribe-time ExecutionContext so app AsyncLocals (trace, principal, logging
        // scope) flow into the message handler, which runs on a foreign Redis subscriber thread.
        var capturedContext = ExecutionContext.Capture();
        var channel = _keys.Channel(correlationId);

        var activity = ActivitySource.StartActivity("asyncresponse.wait");
        activity?.SetTag("asyncresponse.correlation_id", correlationId);
        activity?.SetTag("asyncresponse.timeout_seconds", timeout.Value.TotalSeconds);

        _logger.LogInformation("Waiting for response on correlationId {CorrelationId} with timeout {Timeout}.", correlationId, timeout.Value);

        var tcs = new TaskCompletionSource<T>(TaskCreationOptions.RunContinuationsAsynchronously);

        // Single-use cancellation token implementing the timeout. The timer is armed only
        // after subscribe + recovery-state save succeeds, but the callback is registered before
        // subscribing so a very fast terminal message can still clean up safely.
        var cancellationTokenSource = new CancellationTokenSource();
        CancellationTokenRegistration timeoutRegistration = default;

        // -------------------------------------------------------------------------
        // Local: CleanupOnceAsync
        // Ensures unsubscribe, recovery-state delete, timeout disposal, and executor cleanup
        // happen once no matter whether completion, timeout, or waiter disposal got there first.
        int cleanupStarted = 0;
        async ValueTask CleanupOnceAsync(Action<RedisChannel, RedisValue> redisHandler)
        {
            if (Interlocked.Exchange(ref cleanupStarted, 1) != 0)
                return;

            try
            {
                await _subscriber.UnsubscribeAsync(channel, redisHandler).ConfigureAwait(false);
                await _recoveryStateStore.TryDeleteAsync(correlationId).ConfigureAwait(false);

                // Schedule the disposal on the thread pool; do not await directly to prevent
                // deadlocks with work currently running on the executor.
                _ = Task.Run(async () => await RemoveExecutorAsync(channel.ToString()!).ConfigureAwait(false));

                _logger.LogDebug("Unsubscribed from channel {Channel}.", channel.ToString()!);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error during unsubscribe-once for channel {Channel}.", channel.ToString()!);
            }
            finally
            {
                await timeoutRegistration.DisposeAsync().ConfigureAwait(false);
                cancellationTokenSource.Dispose();
                activity?.Dispose();
            }
        }

        // -------------------------------------------------------------------------
        // Local: ProcessRedisMessageAsync
        // Deserializes and handles a single incoming envelope, completes the TCS when terminal.
        async Task ProcessRedisMessageAsync(RedisChannel messageChannel, RedisValue messageValue, Action<RedisChannel, RedisValue> redisHandler)
        {
            _logger.LogDebug("Received message on channel {Channel}.", messageChannel.ToString()!);

            bool finished = false;
            try
            {
                var envelope = JsonSerializer.Deserialize<AsyncResponseEnvelope<T>>(messageValue.ToString(), AsyncResponseEnvelopeOptions<T>.Instance);

                if (envelope == null)
                {
                    _logger.LogError("Failed to deserialize envelope for correlationId {CorrelationId}.", correlationId);

                    finished = true;
                    activity?.SetTag("error.type", "deserialize_failure");
                    var deserializationError = new JsonException($"Failed to deserialize envelope for correlationId {correlationId}.");
                    if (!tcs.TrySetException(deserializationError))
                        _logger.LogWarning(deserializationError, "TaskCompletionSource already completed for correlationId {CorrelationId}.", correlationId);
                }
                else if (!envelope.Success)
                {
                    finished = true;
                    var remoteFailure = new Exception(envelope.ExceptionMessage ?? "Unknown error during asynchronous processing.");
                    if (!string.IsNullOrEmpty(envelope.ExceptionStackTrace))
                    {
                        remoteFailure.Data["RemoteStackTrace"] = envelope.ExceptionStackTrace;
                    }

                    _logger.LogWarning("Received error response for correlationId {CorrelationId}: {ErrorMessage}", correlationId, envelope.ExceptionMessage);
                    activity?.SetTag("error.type", "remote_failure");
                    if (!tcs.TrySetException(remoteFailure))
                        _logger.LogWarning(remoteFailure, "TaskCompletionSource already completed for correlationId {CorrelationId}.", correlationId);
                }
                else
                {
                    _logger.LogInformation("Received response for correlationId {CorrelationId}.", correlationId);

                    finished = await completionPredicate(envelope.Payload!).ConfigureAwait(false);

                    if (finished && !tcs.TrySetResult(envelope.Payload!))
                        _logger.LogWarning("TaskCompletionSource already completed for correlationId {CorrelationId}.", correlationId);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing message on channel {Channel} for correlationId {CorrelationId}.", messageChannel.ToString()!, correlationId);

                finished = true;
                if (!tcs.TrySetException(ex))
                    _logger.LogWarning(ex, "TaskCompletionSource already completed for correlationId {CorrelationId}.", correlationId);
            }
            finally
            {
                // Unsubscription also happens on dispose, but doing it immediately after the
                // terminal message releases resources sooner.
                if (finished)
                    await CleanupOnceAsync(redisHandler).ConfigureAwait(false);
            }
        }

        // -------------------------------------------------------------------------
        // Local: RedisHandler
        // Receives raw Redis pub/sub messages and enqueues them on the per-channel executor.
        void RedisHandler(RedisChannel messageChannel, RedisValue messageValue)
        {
            _ = GetExecutor(messageChannel.ToString()!)
                  .Enqueue(() => ProcessUnderCapturedContextAsync(messageChannel, messageValue, RedisHandler))
                  .ContinueWith(t =>
                  {
                      if (t.IsFaulted)
                          _logger.LogError(t.Exception!, "Enqueue faulted for channel {Channel}.", messageChannel.ToString()!);
                      else if (!t.Result)
                          _logger.LogWarning("Executor rejected message for channel {Channel}.", messageChannel.ToString()!);
                  });
        }

        // -------------------------------------------------------------------------
        // Local: ProcessUnderCapturedContextAsync
        // Restores the waiter's subscribe-time ExecutionContext (app AsyncLocals: trace, principal,
        // logging scope) plus the correlation id before processing — the Redis subscriber callback
        // runs on a foreign thread-pool thread that never had them.
        Task ProcessUnderCapturedContextAsync(RedisChannel messageChannel, RedisValue messageValue, Action<RedisChannel, RedisValue> redisHandler)
        {
            async Task ProcessAsync()
            {
                using var correlationScope = AsyncResponseContext.PushCorrelationId(storedCorrelationId);
                await ProcessRedisMessageAsync(messageChannel, messageValue, redisHandler).ConfigureAwait(false);
            }

            if (capturedContext is null)
                return ProcessAsync();

            Task? task = null;
            ExecutionContext.Run(capturedContext, _ => task = ProcessAsync(), null);
            return task!;
        }

        timeoutRegistration = cancellationTokenSource.Token.Register(() =>
        {
            _ = Task.Run(async () =>
            {
                _logger.LogWarning("Timed out waiting for response for correlationId {CorrelationId}.", correlationId);
                tcs.TrySetException(new TimeoutException($"Timed out waiting for response for correlationId {correlationId}."));
                await CleanupOnceAsync(RedisHandler).ConfigureAwait(false);
            });
        });

        try
        {
            await _subscriber.SubscribeAsync(channel, RedisHandler).ConfigureAwait(false);
            var recoveryState = new RecoveryState
            {
                ResumeCallback = resumeCallback,
                FailureCallback = failureCallback,
                CorrelationId = correlationId,
                PayloadTypeFullName = typeof(T).FullName,
                RegisteredAtUtc = DateTime.UtcNow,
                Context = _propagation.Capture()
            };
            await _recoveryStateStore.SaveAsync(correlationId, recoveryState, _options.RecoveryStateExpiry).ConfigureAwait(false);
            _logger.LogDebug("Subscribed to channel {Channel} for correlationId {CorrelationId}.", channel.ToString()!, correlationId);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to subscribe to channel {Channel} for correlationId {CorrelationId}.", channel.ToString()!, correlationId);
            activity?.SetTag("error.type", "subscribe_failure");
            tcs.TrySetException(ex);
            await CleanupOnceAsync(RedisHandler).ConfigureAwait(false);
            // Return an already-faulted waiter.
            return new RedisAsyncResponseWaiter<T>(tcs.Task, () => CleanupOnceAsync(RedisHandler));
        }

        try
        {
            if (Volatile.Read(ref cleanupStarted) == 0)
                cancellationTokenSource.CancelAfter(timeout.Value);
        }
        catch (ObjectDisposedException)
        {
            // A response completed and cleaned up between the check and CancelAfter.
        }

        return new RedisAsyncResponseWaiter<T>(tcs.Task, () => CleanupOnceAsync(RedisHandler));
    }

    // ---------------------------------------------------------------------------------------
    // IAsyncResponsePublisher

    /// <inheritdoc/>
    public async Task SetResponse<T>(T response, string? correlationId = null)
    {
        using var activity = ActivitySource.StartActivity("asyncresponse.set_response");

        // When no correlation id is provided, fall back to the ambient context.
        correlationId ??= AsyncResponseContext.CorrelationId;
        activity?.SetTag("asyncresponse.correlation_id", correlationId);

        if (string.IsNullOrWhiteSpace(correlationId))
        {
            _logger.LogWarning("CorrelationId is null; cannot publish the response.");
            activity?.SetTag("error.type", "correlation_id_null");
            return;
        }

        var channel = _keys.Channel(correlationId);
        try
        {
            var envelope = new AsyncResponseEnvelope<T>
            {
                Success = true,
                Payload = response
            };
            var json = JsonSerializer.Serialize(envelope);
            long numSubscribers = await _subscriber.PublishAsync(channel, json).ConfigureAwait(false);
            activity?.SetTag("asyncresponse.subscribers", numSubscribers);

            if (numSubscribers == 0)
            {
                // Nobody was listening (the waiter died, e.g. with a redeploy): hand the response
                // over to the lost-subscriber dispatcher, which asks the payload whether to resume
                // the flow or fail it, and invokes the matching callback.
                var recoveryState = await _recoveryStateStore.GetAsync(correlationId).ConfigureAwait(false);

                var dispatchResult = await _lostSubscriberDispatcher.DispatchLostResponse(recoveryState, response, channel.ToString()!).ConfigureAwait(false);
                activity?.SetTag("asyncresponse.lost_subscriber_route", dispatchResult.ShouldResume switch
                {
                    true => "Resume",
                    false => "Fail",
                    _ => "Unclassified"
                });

                if (dispatchResult.CallbackInvoked)
                    await _recoveryStateStore.TryDeleteAsync(correlationId).ConfigureAwait(false);

                await RemoveExecutorAsync(channel.ToString()!).ConfigureAwait(false);
            }
            else
            {
                _logger.LogInformation("Published response for correlationId {CorrelationId} on channel {Channel}. PayloadType: {PayloadType}. Subscribers: {SubscriberCount}.", correlationId, channel.ToString()!, typeof(T), numSubscribers);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to publish response for correlationId {CorrelationId} on channel {Channel}.", correlationId, channel.ToString()!);
            activity?.SetTag("error.type", ex.GetType().Name);
            throw;
        }
    }

    /// <inheritdoc/>
    public async Task SetException(Exception exception, string? correlationId = null)
    {
        using var activity = ActivitySource.StartActivity("asyncresponse.set_exception");

        correlationId ??= AsyncResponseContext.CorrelationId;
        activity?.SetTag("asyncresponse.correlation_id", correlationId);

        if (string.IsNullOrWhiteSpace(correlationId))
        {
            _logger.LogWarning("CorrelationId is null; cannot publish the exception. Exception: {ExceptionMessage}", exception.Message);
            activity?.SetTag("error.type", "correlation_id_null");
            return;
        }

        var channel = _keys.Channel(correlationId);
        try
        {
            var envelope = new AsyncResponseEnvelope<object>
            {
                Success = false,
                ExceptionMessage = exception.Message,
                ExceptionStackTrace = exception.StackTrace,
                Payload = null
            };
            var json = JsonSerializer.Serialize(envelope);
            long numSubscribers = await _subscriber.PublishAsync(channel, json).ConfigureAwait(false);
            activity?.SetTag("asyncresponse.subscribers", numSubscribers);

            if (numSubscribers == 0)
            {
                // Nobody was listening: exception envelopes always go to the failure callback.
                var recoveryState = await _recoveryStateStore.GetAsync(correlationId).ConfigureAwait(false);

                var callbackInvoked = await _lostSubscriberDispatcher.DispatchLostException(recoveryState, exception, channel.ToString()!).ConfigureAwait(false);

                if (callbackInvoked)
                    await _recoveryStateStore.TryDeleteAsync(correlationId).ConfigureAwait(false);

                await RemoveExecutorAsync(channel.ToString()!).ConfigureAwait(false);
            }
            else
            {
                _logger.LogInformation("Published exception response for correlationId {CorrelationId} on channel {Channel}. Subscribers: {SubscriberCount}.", correlationId, channel.ToString()!, numSubscribers);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to publish exception response for correlationId {CorrelationId} on channel {Channel}.", correlationId, channel.ToString()!);
            activity?.SetTag("error.type", ex.GetType().Name);
            throw;
        }
    }

    // ---------------------------------------------------------------------------------------
    // IActiveSubscriberProbe

    /// <inheritdoc/>
    public ValueTask<long> CountActiveSubscribersAsync(string correlationId, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(correlationId))
            return new ValueTask<long>(0L);

        var channel = _keys.Channel(correlationId);

        // Subscriptions live on whichever node the client subscribed through, so the live count is
        // the maximum reported across all connected endpoints.
        long subscribers = 0;
        foreach (var endPoint in _multiplexer.GetEndPoints())
        {
            var server = _multiplexer.GetServer(endPoint);
            if (!server.IsConnected)
                continue;

            try
            {
                subscribers = Math.Max(subscribers, server.SubscriptionSubscriberCount(channel));
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Failed to read subscriber count for channel {Channel}.", channel.ToString()!);
            }
        }

        return new ValueTask<long>(subscribers);
    }

    private ChannelSerialExecutor GetExecutor(string channel) =>
        _executors.GetOrAdd(channel, ch => new ChannelSerialExecutor(_logger, ch));

    private async ValueTask RemoveExecutorAsync(string channel)
    {
        if (_executors.TryRemove(channel, out var executor))
        {
            await executor.DisposeAsync().ConfigureAwait(false);
        }
    }
}
