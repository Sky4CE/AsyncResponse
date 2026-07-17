using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StackExchange.Redis;
using System.Buffers;
using System.Diagnostics;
using System.Text;
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
internal sealed class RedisAsyncResponseChannel : IAsyncResponsePublisher, IRawAsyncResponsePublisher, IRecoverableAsyncResponseSubscriber, IActiveSubscriberProbe
{

    private readonly ISubscriber _subscriber;
    private readonly IConnectionMultiplexer _multiplexer;
    private readonly IRecoveryStateStore _recoveryStateStore;
    private readonly AsyncResponseContextPropagation _propagation;
    private readonly LostSubscriberCallbackDispatcher _lostSubscriberDispatcher;
    private readonly RedisKeySchema _keys;
    private readonly RedisAsyncResponseOptions _options;
    private readonly ILogger<RedisAsyncResponseChannel> _logger;

    private readonly SerialExecutorRegistry _executors;

    /// <summary>Creates a Redis-backed async-response channel.</summary>
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
        _executors = new SerialExecutorRegistry(logger);
    }

    // ---------------------------------------------------------------------------------------
    // IAsyncResponseSubscriber / IRecoverableAsyncResponseSubscriber

    /// <inheritdoc/>
    public Task<IAsyncResponseWaiter<T>> CreateResponseWaiter<T>(
        string correlationId,
        Func<T, ValueTask<bool>>? completionPredicate = null,
        TimeSpan? timeout = null) where T : IAsyncResponsePayload
        => CreateResponseWaiterCore(
            correlationId,
            resumeCallback: null,
            failureCallback: null,
            completionPredicate,
            timeout);

    /// <inheritdoc/>
    public Task<IAsyncResponseWaiter<T>> CreateRecoverableResponseWaiter<T>(
        string correlationId,
        ReflectionCallDto? resumeCallback = null,
        ReflectionCallDto? failureCallback = null,
        Func<T, ValueTask<bool>>? completionPredicate = null,
        TimeSpan? timeout = null) where T : IAsyncResponsePayload
        => CreateResponseWaiterCore(
            correlationId,
            resumeCallback,
            failureCallback,
            completionPredicate,
            timeout);

    private async Task<IAsyncResponseWaiter<T>> CreateResponseWaiterCore<T>(
        string correlationId,
        ReflectionCallDto? resumeCallback,
        ReflectionCallDto? failureCallback,
        Func<T, ValueTask<bool>>? completionPredicate,
        TimeSpan? timeout) where T : IAsyncResponsePayload
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

        var activity = AsyncResponseDiagnostics.StartActivity("asyncresponse.wait", correlationId: correlationId);
        activity?.SetTag("asyncresponse.channel", "redis");
        AsyncResponseDiagnostics.SetPayloadType(activity, typeof(T));
        activity?.SetTag("asyncresponse.timeout_seconds", timeout.Value.TotalSeconds);

        if (_logger.IsEnabled(LogLevel.Debug))
            _logger.LogDebug("Waiting for response on correlationId {CorrelationId} with timeout {Timeout}.", correlationId, timeout.Value);

        var tcs = new TaskCompletionSource<T>(TaskCreationOptions.RunContinuationsAsynchronously);
        var registrationId = Guid.NewGuid();

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
                await _recoveryStateStore.TryDeleteAsync(correlationId, registrationId).ConfigureAwait(false);

                // Schedule the disposal on the thread pool; do not await directly to prevent
                // deadlocks with work currently running on the executor.
                _ = Task.Run(async () => await _executors.RemoveAsync(channel.ToString()!).ConfigureAwait(false));

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
                var envelope = JsonSerializer.Deserialize(messageValue.ToString(), AsyncResponseEnvelopeJson.TypeInfo<T>());

                if (envelope == null)
                {
                    _logger.LogError("Failed to deserialize envelope for correlationId {CorrelationId}.", correlationId);

                    finished = true;
                    var deserializationError = new JsonException($"Failed to deserialize envelope for correlationId {correlationId}.");
                    AsyncResponseDiagnostics.SetError(activity, "deserialize_failure", deserializationError.Message);
                    if (!tcs.TrySetException(deserializationError))
                        _logger.LogWarning(deserializationError, "TaskCompletionSource already completed for correlationId {CorrelationId}.", correlationId);
                }
                else if (!AsyncResponseEnvelopeSchema.IsReadable(envelope.SchemaVersion))
                {
                    finished = true;
                    var schemaError = new InvalidOperationException(
                        $"Response envelope for correlationId {correlationId} has schema version {envelope.SchemaVersion}, " +
                        $"which this build does not support (current: {AsyncResponseEnvelopeSchema.Current}).");
                    AsyncResponseDiagnostics.SetError(activity, "schema_mismatch", schemaError.Message);
                    if (!tcs.TrySetException(schemaError))
                        _logger.LogWarning(schemaError, "TaskCompletionSource already completed for correlationId {CorrelationId}.", correlationId);
                }
                else if (!envelope.Success)
                {
                    finished = true;
                    var remoteFailure = new Exception(envelope.ExceptionMessage ?? "Unknown error during asynchronous processing.");
                    if (!string.IsNullOrEmpty(envelope.ExceptionStackTrace))
                    {
                        // Cap on receive too: the publish-side cap only bounds traces we emit, not what
                        // a remote we do not control can push at us.
                        remoteFailure.Data["RemoteStackTrace"] = RemoteStackTrace.Cap(envelope.ExceptionStackTrace, _options.MaxRemoteStackTraceLength);
                    }

                    _logger.LogWarning("Received error response for correlationId {CorrelationId}: {ErrorMessage}", correlationId, envelope.ExceptionMessage);
                    AsyncResponseDiagnostics.SetError(activity, "remote_failure", remoteFailure.Message);
                    if (!tcs.TrySetException(remoteFailure))
                        _logger.LogWarning(remoteFailure, "TaskCompletionSource already completed for correlationId {CorrelationId}.", correlationId);
                }
                else
                {
                    if (_logger.IsEnabled(LogLevel.Debug))
                        _logger.LogDebug("Received response for correlationId {CorrelationId}.", correlationId);

                    finished = await completionPredicate(envelope.Payload!).ConfigureAwait(false);

                    if (finished && !tcs.TrySetResult(envelope.Payload!))
                        _logger.LogWarning("TaskCompletionSource already completed for correlationId {CorrelationId}.", correlationId);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing message on channel {Channel} for correlationId {CorrelationId}.", messageChannel.ToString()!, correlationId);

                finished = true;
                AsyncResponseDiagnostics.SetError(activity, ex);
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
            // The registry coordinates create/enqueue/retire under one lock, so the message is never
            // enqueued onto an executor that is concurrently being torn down (no lost messages) and a
            // correlation-id reused mid-drain never produces two live executors for one channel.
            _executors.Enqueue(
                messageChannel.ToString()!,
                () => ProcessUnderCapturedContextAsync(messageChannel, messageValue, RedisHandler));
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
                AsyncResponseDiagnostics.SetError(activity, "timeout", $"Timed out waiting for response for correlationId {correlationId}.");
                AsyncResponseDiagnostics.RecordWaiterTimeout("redis");
                tcs.TrySetException(new TimeoutException($"Timed out waiting for response for correlationId {correlationId}."));
                await CleanupOnceAsync(RedisHandler).ConfigureAwait(false);
            });
        });

        try
        {
            await _subscriber.SubscribeAsync(channel, RedisHandler).ConfigureAwait(false);
            var recoveryState = new RecoveryState
            {
                RegistrationId = registrationId,
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
            AsyncResponseDiagnostics.SetError(activity, "subscribe_failure", ex.Message);
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
    public Task SetResponse<T>(T response, string correlationId, CancellationToken cancellationToken = default) where T : IAsyncResponsePayload
        => SetResponseCore(response, correlationId, cancellationToken);

    Task IRawAsyncResponsePublisher.SetRawResponse(object? response, string correlationId, CancellationToken cancellationToken)
        => SetResponseCore(response, correlationId, cancellationToken);

    Task IRawAsyncResponsePublisher.SetRawResponseJson(string responseJson, string correlationId, CancellationToken cancellationToken)
        => SetRawResponseJsonCore(responseJson, correlationId, cancellationToken);

    // Intentionally duplicated with SetRawResponseJsonCore: this publish method is a latency hot
    // path, and earlier shared helper/delegate refactors regressed throughput in benchmarks.
    // Keep the typed Redis path inline unless a benchmark run proves a refactor is free.
    private async Task SetResponseCore<T>(T response, string correlationId, CancellationToken cancellationToken)
    {
        using var activity = AsyncResponseDiagnostics.StartActivity("asyncresponse.set_response", ActivityKind.Producer);
        activity?.SetTag("asyncresponse.channel", "redis");
        AsyncResponseDiagnostics.SetPayloadType(activity, typeof(T));

        // When no correlation id is provided, fall back to the ambient context.
        AsyncResponseDiagnostics.SetCorrelationId(activity, correlationId);

        if (string.IsNullOrWhiteSpace(correlationId))
        {
            _logger.LogWarning("CorrelationId is null; cannot publish the response.");
            AsyncResponseDiagnostics.SetError(activity, "correlation_id_null", "CorrelationId is null; cannot publish the response.");
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
            var json = AsyncResponseEnvelopeJson.Serialize(envelope);
            long numSubscribers = await _subscriber.PublishAsync(channel, json).ConfigureAwait(false);
            activity?.SetTag("asyncresponse.subscribers", numSubscribers);

            if (numSubscribers == 0)
            {
                // Nobody was listening (the waiter died, e.g. with a redeploy): hand the response
                // over to the lost-subscriber dispatcher, which asks the payload whether to resume
                // the flow or fail it, and invokes the matching callback.
                var dispatchResult = await _lostSubscriberDispatcher
                    .DispatchLostResponses(_recoveryStateStore, correlationId, response, channel.ToString()!, cancellationToken)
                    .ConfigureAwait(false);
                AsyncResponseDiagnostics.SetLostSubscriberRoute(activity, dispatchResult.ShouldResume);
                AsyncResponseDiagnostics.RecordLostSubscriber("response", dispatchResult.ShouldResume, dispatchResult.CallbackInvoked);
                activity?.SetTag("asyncresponse.recovery.callback_invoked", dispatchResult.CallbackInvoked);

                await _executors.RemoveAsync(channel.ToString()!).ConfigureAwait(false);
            }
            else
            {
                if (_logger.IsEnabled(LogLevel.Debug))
                    _logger.LogDebug("Published response for correlationId {CorrelationId} on channel {Channel}. PayloadType: {PayloadType}. Subscribers: {SubscriberCount}.", correlationId, channel.ToString()!, typeof(T), numSubscribers);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to publish response for correlationId {CorrelationId} on channel {Channel}.", correlationId, channel.ToString()!);
            AsyncResponseDiagnostics.SetError(activity, ex);
            throw;
        }
    }

    // Intentionally duplicated with SetResponseCore: raw ingress uses pre-serialized payload JSON
    // and a different lost-subscriber materialization path, so avoiding shared indirection matters.
    private async Task SetRawResponseJsonCore(string responseJson, string correlationId, CancellationToken cancellationToken)
    {
        using var activity = AsyncResponseDiagnostics.StartActivity("asyncresponse.ingress.raw_response", ActivityKind.Producer);
        activity?.SetTag("asyncresponse.channel", "redis");

        AsyncResponseDiagnostics.SetCorrelationId(activity, correlationId);

        if (string.IsNullOrWhiteSpace(correlationId))
        {
            _logger.LogWarning("CorrelationId is null; cannot publish the raw response.");
            AsyncResponseDiagnostics.SetError(activity, "correlation_id_null", "CorrelationId is null; cannot publish the raw response.");
            return;
        }

        var channel = _keys.Channel(correlationId);
        try
        {
            var json = SerializeRawSuccessEnvelope(responseJson);
            long numSubscribers = await _subscriber.PublishAsync(channel, json).ConfigureAwait(false);
            activity?.SetTag("asyncresponse.subscribers", numSubscribers);

            if (numSubscribers == 0)
            {
                var response = new RawJsonResponse(responseJson).DeserializeUntyped();

                var dispatchResult = await _lostSubscriberDispatcher
                    .DispatchLostResponses(_recoveryStateStore, correlationId, response, channel.ToString()!, cancellationToken)
                    .ConfigureAwait(false);
                AsyncResponseDiagnostics.SetLostSubscriberRoute(activity, dispatchResult.ShouldResume);
                AsyncResponseDiagnostics.RecordLostSubscriber("response", dispatchResult.ShouldResume, dispatchResult.CallbackInvoked);
                activity?.SetTag("asyncresponse.recovery.callback_invoked", dispatchResult.CallbackInvoked);

                await _executors.RemoveAsync(channel.ToString()!).ConfigureAwait(false);
            }
            else
            {
                if (_logger.IsEnabled(LogLevel.Debug))
                    _logger.LogDebug("Published raw response for correlationId {CorrelationId} on channel {Channel}. Subscribers: {SubscriberCount}.", correlationId, channel.ToString()!, numSubscribers);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to publish raw response for correlationId {CorrelationId} on channel {Channel}.", correlationId, channel.ToString()!);
            AsyncResponseDiagnostics.SetError(activity, ex);
            throw;
        }
    }

    /// <inheritdoc/>
    public async Task SetException(Exception exception, string correlationId, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(exception);

        using var activity = AsyncResponseDiagnostics.StartActivity("asyncresponse.set_exception", ActivityKind.Producer);
        activity?.SetTag("asyncresponse.channel", "redis");
        activity?.SetTag("asyncresponse.exception_type", exception.GetType().FullName ?? exception.GetType().Name);

        AsyncResponseDiagnostics.SetCorrelationId(activity, correlationId);

        if (string.IsNullOrWhiteSpace(correlationId))
        {
            _logger.LogWarning("CorrelationId is null; cannot publish the exception. Exception: {ExceptionMessage}", exception.Message);
            AsyncResponseDiagnostics.SetError(activity, "correlation_id_null", "CorrelationId is null; cannot publish the exception.");
            return;
        }

        var channel = _keys.Channel(correlationId);
        try
        {
            var envelope = new AsyncResponseEnvelope<object>
            {
                Success = false,
                ExceptionMessage = exception.Message,
                ExceptionStackTrace = RemoteStackTrace.ForWire(exception.StackTrace, _options.IncludeRemoteStackTrace, _options.MaxRemoteStackTraceLength),
                Payload = null
            };
            var json = AsyncResponseEnvelopeJson.Serialize(envelope);
            long numSubscribers = await _subscriber.PublishAsync(channel, json).ConfigureAwait(false);
            activity?.SetTag("asyncresponse.subscribers", numSubscribers);

            if (numSubscribers == 0)
            {
                // Nobody was listening: exception envelopes always go to the failure callback.
                var callbackInvoked = await _lostSubscriberDispatcher
                    .DispatchLostExceptions(_recoveryStateStore, correlationId, exception, channel.ToString()!, cancellationToken)
                    .ConfigureAwait(false);
                activity?.SetTag("asyncresponse.recovery.callback_invoked", callbackInvoked);
                AsyncResponseDiagnostics.RecordLostSubscriber("exception", shouldResume: false, callbackInvoked);

                await _executors.RemoveAsync(channel.ToString()!).ConfigureAwait(false);
            }
            else if (_logger.IsEnabled(LogLevel.Debug))
            {
                _logger.LogDebug("Published exception response for correlationId {CorrelationId} on channel {Channel}. Subscribers: {SubscriberCount}.", correlationId, channel.ToString()!, numSubscribers);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to publish exception response for correlationId {CorrelationId} on channel {Channel}.", correlationId, channel.ToString()!);
            AsyncResponseDiagnostics.SetError(activity, ex);
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

    private static string SerializeRawSuccessEnvelope(string payloadJson)
    {
        JsonSafety.ThrowIfClearlyNotJson(payloadJson);

        var buffer = new ArrayBufferWriter<byte>();
        using (var writer = new Utf8JsonWriter(buffer))
        {
            writer.WriteStartObject();
            writer.WriteNumber("SchemaVersion", AsyncResponseEnvelopeSchema.Current);
            writer.WriteBoolean("Success", true);
            writer.WritePropertyName("Payload");
            writer.WriteRawValue(payloadJson);
            writer.WriteNull("ExceptionMessage");
            writer.WriteNull("ExceptionStackTrace");
            writer.WriteEndObject();
        }

        return Encoding.UTF8.GetString(buffer.WrittenSpan);
    }
}
