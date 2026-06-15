using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StackExchange.Redis;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text.Json;

namespace AsyncResponse.Redis;

/// <summary>
/// Redis-backed async-response transport:
/// <list type="bullet">
/// <item><description>Publishes responses to Redis pub/sub channels keyed by correlation id.</description></item>
/// <item><description>Subscribes waiters to those channels with per-channel serialized handling.</description></item>
/// <item><description>Persists <see cref="RecoveryState"/> so responses arriving after the waiter
/// died (e.g. a redeploy) are routed through the lost-subscriber dispatcher, which classifies the
/// payload's domain outcome and invokes the resume or failure callback.</description></item>
/// <item><description>Acts as the transport-neutral <see cref="IAsyncResponseIngress"/> for
/// broker-delivered messages.</description></item>
/// </list>
/// </summary>
internal sealed class RedisAsyncResponseTransport : IAsyncResponsePublisher, IAsyncResponseSubscriber, IAsyncResponseIngress
{
    private const string SERVICE_NAME = nameof(RedisAsyncResponseTransport);

    /// <summary>OpenTelemetry-compatible activity source for the AsyncResponse library.</summary>
    internal static readonly ActivitySource ActivitySource = new("AsyncResponse");

    private readonly IConnectionMultiplexer _multiplexer;
    private readonly ISubscriber _subscriber;
    private readonly IDatabase _database;
    private readonly WorkerJobExecutor _workerJobExecutor;
    private readonly LostSubscriberCallbackDispatcher _lostSubscriberDispatcher;
    private readonly RedisKeySchema _keys;
    private readonly RedisAsyncResponseOptions _options;
    private readonly ILogger<RedisAsyncResponseTransport> _logger;

    private readonly ConcurrentDictionary<string, ChannelSerialExecutor> _executors = new(StringComparer.Ordinal);

    public RedisAsyncResponseTransport(
        IServiceScopeFactory scopeFactory,
        IConnectionMultiplexer multiplexer,
        WorkerJobExecutor workerJobExecutor,
        IOptions<RedisAsyncResponseOptions> options,
        ILogger<RedisAsyncResponseTransport> logger)
    {
        _multiplexer = multiplexer;
        _subscriber = multiplexer.GetSubscriber();
        _database = multiplexer.GetDatabase();
        _workerJobExecutor = workerJobExecutor;
        _options = options.Value;
        _keys = new RedisKeySchema(_options.KeyPrefix);
        _logger = logger;
        _lostSubscriberDispatcher = new LostSubscriberCallbackDispatcher(scopeFactory, logger);
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
        const string MethodName = nameof(CreateResponseWaiter);

        if (string.IsNullOrWhiteSpace(correlationId))
            throw new ArgumentNullException(nameof(correlationId), "CorrelationId must not be empty or whitespace.");

        // default: first envelope completes the wait
        completionPredicate ??= _ => new ValueTask<bool>(true);

        // Default timeout aligned with the recovery-state expiry: an infinite wait is never
        // meaningful, because once the recovery state expires the correlation id has no recovery
        // anyway. Timing out routes the flow through its normal failure handling instead of
        // leaving it stuck forever.
        timeout ??= _options.DefaultTimeout ?? _options.RecoveryStateExpiry;

        var storedCorrelationId = correlationId;
        var channel = _keys.Channel(correlationId);
        var recoveryKey = _keys.RecoveryKey(correlationId);

        var activity = ActivitySource.StartActivity("asyncresponse.wait");
        activity?.SetTag("asyncresponse.correlation_id", correlationId);
        activity?.SetTag("asyncresponse.timeout_seconds", timeout.Value.TotalSeconds);

        _logger.LogInformation("{ServiceName}: {MethodName} Waiting for response on correlationId {CorrelationId} with timeout {Timeout}.",
            SERVICE_NAME, MethodName, correlationId, timeout.Value);

        var tcs = new TaskCompletionSource<T>(TaskCreationOptions.RunContinuationsAsynchronously);

        // Single-use cancellation token implementing the timeout.
        var cancellationTokenSource = new CancellationTokenSource(timeout.Value);

        // -------------------------------------------------------------------------
        // Local: UnsubscribeOnceAsync
        // Ensures we unsubscribe only once, delete the recovery state, and schedule executor cleanup.
        int unsubscribedLocal = 0;
        async Task UnsubscribeOnceAsync(Action<RedisChannel, RedisValue> redisHandler)
        {
            if (Interlocked.Exchange(ref unsubscribedLocal, 1) != 0)
                return;

            try
            {
                await _subscriber.UnsubscribeAsync(channel, redisHandler).ConfigureAwait(false);
                var recoveryKeyRemoved = await _database.KeyDeleteAsync(recoveryKey).ConfigureAwait(false);
                if (!recoveryKeyRemoved)
                    _logger.LogWarning("{ServiceName}: Failed to delete recovery state {RecoveryKey} for channel {Channel}.",
                        SERVICE_NAME, recoveryKey, channel.ToString());

                // Schedule the disposal on the thread pool; do not await directly to prevent
                // deadlocks with work currently running on the executor.
                _ = Task.Run(async () => await RemoveExecutorAsync(channel.ToString()!).ConfigureAwait(false));

                _logger.LogDebug("{ServiceName}: Unsubscribed from channel {Channel}.", SERVICE_NAME, channel.ToString());
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "{ServiceName}: Error during unsubscribe-once for channel {Channel}.", SERVICE_NAME, channel.ToString());
            }
            finally
            {
                cancellationTokenSource.Dispose();
                activity?.Dispose();
            }
        }

        // -------------------------------------------------------------------------
        // Local: ProcessRedisMessageAsync
        // Deserializes and handles a single incoming envelope, completes the TCS when terminal.
        async Task ProcessRedisMessageAsync(RedisChannel messageChannel, RedisValue messageValue, Action<RedisChannel, RedisValue> redisHandler)
        {
            const string LocalMethodName = nameof(ProcessRedisMessageAsync);

            _logger.LogDebug("{ServiceName}: {MethodName} Received message on channel {Channel}.", SERVICE_NAME, LocalMethodName, messageChannel);

            bool finished = false;
            try
            {
                var envelope = JsonSerializer.Deserialize<AsyncResponseEnvelope<T>>(messageValue.ToString(), AsyncResponseEnvelopeOptions<T>.Instance);

                if (envelope == null)
                {
                    _logger.LogError("{ServiceName}: {MethodName} Failed to deserialize envelope for correlationId {CorrelationId}.",
                        SERVICE_NAME, LocalMethodName, correlationId);

                    finished = true;
                    activity?.SetTag("error.type", "deserialize_failure");
                    var deserializationError = new JsonException($"Failed to deserialize envelope for correlationId {correlationId}.");
                    if (!tcs.TrySetException(deserializationError))
                        _logger.LogWarning(deserializationError, "{ServiceName}: {MethodName} TaskCompletionSource already completed for correlationId {CorrelationId}.",
                            SERVICE_NAME, LocalMethodName, correlationId);
                }
                else if (!envelope.Success)
                {
                    finished = true;
                    var remoteFailure = new Exception(envelope.ExceptionMessage ?? "Unknown error during asynchronous processing.");
                    if (!string.IsNullOrEmpty(envelope.ExceptionStackTrace))
                    {
                        remoteFailure.Data["RemoteStackTrace"] = envelope.ExceptionStackTrace;
                    }

                    _logger.LogWarning("{ServiceName}: {MethodName} Received error response for correlationId {CorrelationId}: {ErrorMessage}",
                        SERVICE_NAME, LocalMethodName, correlationId, envelope.ExceptionMessage);
                    activity?.SetTag("error.type", "remote_failure");
                    if (!tcs.TrySetException(remoteFailure))
                        _logger.LogWarning(remoteFailure, "{ServiceName}: {MethodName} TaskCompletionSource already completed for correlationId {CorrelationId}.",
                            SERVICE_NAME, LocalMethodName, correlationId);
                }
                else
                {
                    _logger.LogInformation("{ServiceName}: {MethodName} Received response for correlationId {CorrelationId}.",
                        SERVICE_NAME, LocalMethodName, correlationId);

                    finished = await completionPredicate(envelope.Payload!).ConfigureAwait(false);

                    if (finished && !tcs.TrySetResult(envelope.Payload!))
                        _logger.LogWarning("{ServiceName}: {MethodName} TaskCompletionSource already completed for correlationId {CorrelationId}.",
                            SERVICE_NAME, LocalMethodName, correlationId);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "{ServiceName}: {MethodName} Error processing message on channel {Channel} for correlationId {CorrelationId}.",
                    SERVICE_NAME, LocalMethodName, messageChannel.ToString(), correlationId);

                finished = true;
                if (!tcs.TrySetException(ex))
                    _logger.LogWarning(ex, "{ServiceName}: {MethodName} TaskCompletionSource already completed for correlationId {CorrelationId}.",
                        SERVICE_NAME, LocalMethodName, correlationId);
            }
            finally
            {
                // Unsubscription also happens on dispose, but doing it immediately after the
                // terminal message releases resources sooner.
                if (finished)
                    await UnsubscribeOnceAsync(redisHandler).ConfigureAwait(false);
            }
        }

        // -------------------------------------------------------------------------
        // Local: RedisHandler
        // Receives raw Redis pub/sub messages and enqueues them on the per-channel executor.
        void RedisHandler(RedisChannel messageChannel, RedisValue messageValue)
        {
            // Restore the ambient correlation id for the handling scope.
            AsyncResponseContext.SetCorrelationId(storedCorrelationId);

            _ = GetExecutor(messageChannel.ToString()!)
                  .Enqueue(() => ProcessRedisMessageAsync(messageChannel, messageValue, RedisHandler))
                  .ContinueWith(t =>
                  {
                      if (t.IsFaulted)
                          _logger.LogError(t.Exception, "{ServiceName}: Enqueue faulted for {Channel}", SERVICE_NAME, messageChannel.ToString());
                      else if (!t.Result)
                          _logger.LogWarning("{ServiceName}: Executor rejected message for {Channel}", SERVICE_NAME, messageChannel.ToString());
                  });
        }

        try
        {
            await _subscriber.SubscribeAsync(channel, RedisHandler).ConfigureAwait(false);
            var recoveryState = new RecoveryState
            {
                ResumeCallback = resumeCallback,
                FailureCallback = failureCallback,
                CorrelationId = correlationId,
                PayloadTypeFullName = typeof(T).FullName,
                RegisteredAtUtc = DateTime.UtcNow
            };
            await _database.StringSetAsync(recoveryKey, JsonSerializer.Serialize(recoveryState), _options.RecoveryStateExpiry).ConfigureAwait(false);
            _logger.LogDebug("{ServiceName}: {MethodName} Subscribed to channel {Channel} for correlationId {CorrelationId}.",
                SERVICE_NAME, MethodName, channel.ToString(), correlationId);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "{ServiceName}: {MethodName} Failed to subscribe to channel {Channel} for correlationId {CorrelationId}.",
                SERVICE_NAME, MethodName, channel.ToString(), correlationId);
            activity?.SetTag("error.type", "subscribe_failure");
            activity?.Dispose();
            await _database.KeyDeleteAsync(recoveryKey).ConfigureAwait(false);
            await RemoveExecutorAsync(channel.ToString()!).ConfigureAwait(false);
            tcs.TrySetException(ex);
            // Return an already-faulted waiter.
            return new RedisAsyncResponseWaiter<T>(_subscriber, channel, recoveryKey, RedisHandler, tcs.Task, _database, _logger, cancellationTokenSource, RemoveExecutorAsync);
        }

        // Set up the timeout.
        cancellationTokenSource.Token.Register(async () =>
        {
            _logger.LogWarning("{ServiceName}: {MethodName} Timed out waiting for response for correlationId {CorrelationId}.",
                SERVICE_NAME, MethodName, correlationId);
            tcs.TrySetException(new TimeoutException($"Timed out waiting for response for correlationId {correlationId}."));
            await UnsubscribeOnceAsync(RedisHandler).ConfigureAwait(false);
        });

        return new RedisAsyncResponseWaiter<T>(_subscriber, channel, recoveryKey, RedisHandler, tcs.Task, _database, _logger, cancellationTokenSource, RemoveExecutorAsync);
    }

    // ---------------------------------------------------------------------------------------
    // IAsyncResponsePublisher

    /// <inheritdoc/>
    public async Task SetResponse<T>(T response, string? correlationId = null)
    {
        const string MethodName = nameof(SetResponse);

        using var activity = ActivitySource.StartActivity("asyncresponse.set_response");

        // When no correlation id is provided, fall back to the ambient context.
        correlationId ??= AsyncResponseContext.CorrelationId;
        activity?.SetTag("asyncresponse.correlation_id", correlationId);

        if (string.IsNullOrWhiteSpace(correlationId))
        {
            _logger.LogWarning("{ServiceName}: {MethodName} CorrelationId is null. Cannot publish the response.", SERVICE_NAME, MethodName);
            activity?.SetTag("error.type", "correlation_id_null");
            return;
        }

        var channel = _keys.Channel(correlationId);
        var recoveryKey = _keys.RecoveryKey(correlationId);
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
                // over to the lost-subscriber dispatcher, which classifies the payload's domain
                // state and decides between the resume and failure callbacks.
                var recoveryState = await GetRecoveryStateAsync(recoveryKey).ConfigureAwait(false);

                var dispatchResult = await _lostSubscriberDispatcher.DispatchLostResponse(recoveryState, response, channel.ToString()!).ConfigureAwait(false);
                activity?.SetTag("asyncresponse.lost_subscriber_outcome", dispatchResult.Outcome?.ToString() ?? "Unclassified");

                if (dispatchResult.CallbackInvoked)
                    await DeleteRecoveryStateAsync(recoveryKey, channel, MethodName).ConfigureAwait(false);

                await RemoveExecutorAsync(channel.ToString()!).ConfigureAwait(false);
            }
            else
            {
                _logger.LogInformation("{ServiceName}: {MethodName} Published response for correlationId {CorrelationId} on channel {Channel}. PayloadType: {PayloadType}. Subscribers: {SubscriberCount}.",
                    SERVICE_NAME, MethodName, correlationId, channel.ToString(), typeof(T), numSubscribers);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "{ServiceName}: {MethodName} Failed to publish response for correlationId {CorrelationId} on channel {Channel}.",
                SERVICE_NAME, MethodName, correlationId, channel.ToString());
            activity?.SetTag("error.type", ex.GetType().Name);
            throw;
        }
    }

    /// <inheritdoc/>
    public async Task SetException(Exception exception, string? correlationId = null)
    {
        const string MethodName = nameof(SetException);

        using var activity = ActivitySource.StartActivity("asyncresponse.set_exception");

        correlationId ??= AsyncResponseContext.CorrelationId;
        activity?.SetTag("asyncresponse.correlation_id", correlationId);

        if (string.IsNullOrWhiteSpace(correlationId))
        {
            _logger.LogWarning("{ServiceName}: {MethodName} CorrelationId is null. Cannot publish the exception. Exception: {ExceptionMessage}",
                SERVICE_NAME, MethodName, exception.Message);
            activity?.SetTag("error.type", "correlation_id_null");
            return;
        }

        var channel = _keys.Channel(correlationId);
        var recoveryKey = _keys.RecoveryKey(correlationId);
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
                var recoveryState = await GetRecoveryStateAsync(recoveryKey).ConfigureAwait(false);

                var callbackInvoked = await _lostSubscriberDispatcher.DispatchLostException(recoveryState, exception, channel.ToString()!).ConfigureAwait(false);

                if (callbackInvoked)
                    await DeleteRecoveryStateAsync(recoveryKey, channel, MethodName).ConfigureAwait(false);

                await RemoveExecutorAsync(channel.ToString()!).ConfigureAwait(false);
            }
            else
            {
                _logger.LogInformation("{ServiceName}: {MethodName} Published exception response for correlationId {CorrelationId} on channel {Channel}. Subscribers: {SubscriberCount}.",
                    SERVICE_NAME, MethodName, correlationId, channel.ToString(), numSubscribers);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "{ServiceName}: {MethodName} Failed to publish exception response for correlationId {CorrelationId} on channel {Channel}.",
                SERVICE_NAME, MethodName, correlationId, channel.ToString());
            activity?.SetTag("error.type", ex.GetType().Name);
            throw;
        }
    }

    // ---------------------------------------------------------------------------------------
    // IAsyncResponseIngress

    /// <inheritdoc/>
    public async Task HandleResponseMessageAsync(string messageJson, string? correlationId = null)
    {
        const string MethodName = nameof(HandleResponseMessageAsync);

        try
        {
            _logger.LogDebug("{ServiceName}: {MethodName} Received inbound response message: {Message}.", SERVICE_NAME, MethodName, messageJson);

            // The ingress deliberately makes only a transport-level decision: the message parses
            // as JSON → it is a response payload, delivered untyped and uninterpreted. A payload
            // whose domain state is failed is still a valid response that active waiters consume
            // through their Until(...) predicates; domain-state classification happens only in
            // the lost-subscriber fallback, because "nobody is listening" is only knowable after
            // publishing.
            var response = JsonSafety.SafeDeserialize<object?>(messageJson);

            await SetResponse(response, correlationId).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            // Safety net: an unparseable message (no payload to deliver) — or a failing
            // lost-subscriber resume — is escalated to the failure path.
            _logger.LogError(ex, "{ServiceName}: {MethodName} An error occurred while processing the inbound message. ErrorMessage: {ErrorMessage}",
                SERVICE_NAME, MethodName, ex.Message);
            try
            {
                await SetException(ex, correlationId).ConfigureAwait(false);
            }
            catch (Exception innerEx)
            {
                _logger.LogError(innerEx, "{ServiceName}: {MethodName} Failed to publish the exception for the inbound message. ErrorMessage: {ErrorMessage}; InnerErrorMessage: {InnerErrorMessage}",
                    SERVICE_NAME, MethodName, ex.Message, innerEx.Message);
            }
        }
    }

    /// <inheritdoc/>
    public async Task HandleWorkerMessageAsync(string messageJson)
    {
        const string MethodName = nameof(HandleWorkerMessageAsync);

        try
        {
            _logger.LogDebug("{ServiceName}: {MethodName} Received worker job: {Payload}", SERVICE_NAME, MethodName, messageJson);

            var job = JsonSafety.SafeDeserialize<WorkerJobEnvelope>(messageJson)
                ?? throw new InvalidDataException("Worker message deserialized to null.");

            await _workerJobExecutor.ExecuteAsync(job).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            // Intentionally no rethrow: broker subscription loops must stay alive.
            _logger.LogError(ex, "{ServiceName}: {MethodName} Worker job execution failed.", SERVICE_NAME, MethodName);
        }
    }

    // ---------------------------------------------------------------------------------------
    // Internals

    private async Task<RecoveryState?> GetRecoveryStateAsync(string recoveryKey)
    {
        var value = await _database.StringGetAsync(recoveryKey).ConfigureAwait(false);
        if (value.IsNullOrEmpty)
            return null;

        try
        {
            return JsonSerializer.Deserialize<RecoveryState>(value.ToString());
        }
        catch (JsonException ex)
        {
            _logger.LogError(ex, "{ServiceName}: Failed to deserialize recovery state at {RecoveryKey}.", SERVICE_NAME, recoveryKey);
            return null;
        }
    }

    /// <summary>Deletes the recovery state after its callback has been consumed by the dispatcher.</summary>
    private async Task DeleteRecoveryStateAsync(string recoveryKey, RedisChannel channel, string methodName)
    {
        var recoveryKeyRemoved = await _database.KeyDeleteAsync(recoveryKey).ConfigureAwait(false);
        if (!recoveryKeyRemoved)
            _logger.LogWarning("{ServiceName}: {MethodName} Failed to delete recovery state {RecoveryKey} for channel {Channel}.",
                SERVICE_NAME, methodName, recoveryKey, channel.ToString());
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
