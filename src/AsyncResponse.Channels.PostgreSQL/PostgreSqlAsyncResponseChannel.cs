using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Buffers;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;

namespace AsyncResponse.Channels.PostgreSQL;

/// <summary>
/// PostgreSQL-backed response channel using <c>LISTEN/NOTIFY</c> for active waiter wakeups and
/// PostgreSQL tables for durable recovery state.
/// </summary>
internal sealed class PostgreSqlAsyncResponseChannel :
    IAsyncResponsePublisher,
    IRawAsyncResponsePublisher,
    IRecoverableAsyncResponseSubscriber,
    IActiveSubscriberProbe,
    IAsyncDisposable
{
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<Guid, IPostgreSqlSubscription>> _subscriptions = new(StringComparer.Ordinal);
    private readonly Channel<bool> _signals = Channel.CreateUnbounded<bool>(new UnboundedChannelOptions
    {
        SingleReader = true,
        SingleWriter = false
    });

    private readonly PostgreSqlChannelSql _sql;
    private readonly IRecoveryStateStore _recoveryStateStore;
    private readonly AsyncResponseContextPropagation _propagation;
    private readonly LostSubscriberCallbackDispatcher _lostSubscriberDispatcher;
    private readonly PostgreSqlAsyncResponseChannelOptions _options;
    private readonly ILogger<PostgreSqlAsyncResponseChannel> _logger;
    private readonly SerialExecutorRegistry _executors;
    private readonly string _instanceId = $"{Environment.MachineName}-{Environment.ProcessId}-{Guid.NewGuid():N}";

    private readonly object _listenerGate = new();
    private CancellationTokenSource? _listenerCts;
    private Task? _listenTask;
    private Task? _dispatchTask;
    private bool _disposed;

    /// <summary>Creates a PostgreSQL-backed async-response channel.</summary>
    public PostgreSqlAsyncResponseChannel(
        IServiceScopeFactory scopeFactory,
        PostgreSqlChannelSql sql,
        IRecoveryStateStore recoveryStateStore,
        IOptions<PostgreSqlAsyncResponseChannelOptions> options,
        AsyncResponseContextPropagation propagation,
        ILogger<PostgreSqlAsyncResponseChannel> logger)
    {
        _sql = sql;
        _recoveryStateStore = recoveryStateStore;
        _propagation = propagation;
        _options = options.Value;
        _options.Validate();
        _logger = logger;
        _lostSubscriberDispatcher = new LostSubscriberCallbackDispatcher(scopeFactory, propagation, logger);
        _executors = new SerialExecutorRegistry(logger);
    }

    /// <inheritdoc />
    public Task<IAsyncResponseWaiter<T>> CreateResponseWaiter<T>(
        string correlationId,
        Func<T, ValueTask<bool>>? completionPredicate = null,
        TimeSpan? timeout = null) where T : IAsyncResponsePayload
        => CreateResponseWaiterCore(correlationId, null, null, completionPredicate, timeout);

    /// <inheritdoc />
    public Task<IAsyncResponseWaiter<T>> CreateRecoverableResponseWaiter<T>(
        string correlationId,
        ReflectionCallDto? resumeCallback = null,
        ReflectionCallDto? failureCallback = null,
        Func<T, ValueTask<bool>>? completionPredicate = null,
        TimeSpan? timeout = null) where T : IAsyncResponsePayload
        => CreateResponseWaiterCore(correlationId, resumeCallback, failureCallback, completionPredicate, timeout);

    private async Task<IAsyncResponseWaiter<T>> CreateResponseWaiterCore<T>(
        string correlationId,
        ReflectionCallDto? resumeCallback,
        ReflectionCallDto? failureCallback,
        Func<T, ValueTask<bool>>? completionPredicate,
        TimeSpan? timeout) where T : IAsyncResponsePayload
    {
        if (string.IsNullOrWhiteSpace(correlationId))
            throw new ArgumentNullException(nameof(correlationId), "CorrelationId must not be empty or whitespace.");

        if ((resumeCallback is not null || failureCallback is not null)
            && !AsyncResponsePayloadReflection.OverridesShouldResumeOnRecovery(typeof(T)))
        {
            throw new InvalidOperationException(
                $"Payload type '{typeof(T)}' registers lost-subscriber recovery callbacks on the PostgreSQL channel " +
                $"but does not override {nameof(IAsyncResponsePayload)}.{nameof(IAsyncResponsePayload.ShouldResumeOnRecovery)}(). " +
                "Override it to declare which responses resume the flow (return true) versus fail it (return false); " +
                "the durable channel needs this to route a response that arrives after the waiter was lost.");
        }

        completionPredicate ??= _ => new ValueTask<bool>(true);
        timeout ??= _options.DefaultTimeout ?? _options.RecoveryStateExpiry;
        if (timeout <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(timeout), "Timeout must be greater than zero.");

        await _sql.EnsureCreatedAsync().ConfigureAwait(false);
        EnsureListenerStarted();

        var storedCorrelationId = correlationId;
        var capturedContext = ExecutionContext.Capture();

        var activity = AsyncResponseDiagnostics.StartActivity("asyncresponse.wait", correlationId: correlationId);
        activity?.SetTag("asyncresponse.channel", "postgresql");
        AsyncResponseDiagnostics.SetPayloadType(activity, typeof(T));
        activity?.SetTag("asyncresponse.timeout_seconds", timeout.Value.TotalSeconds);

        var tcs = new TaskCompletionSource<T>(TaskCreationOptions.RunContinuationsAsynchronously);
        var registrationId = Guid.NewGuid();
        var subscription = new PostgreSqlSubscription<T>(
            this,
            correlationId,
            registrationId,
            DateTimeOffset.UtcNow,
            completionPredicate,
            tcs,
            capturedContext,
            activity);

        AddSubscription(correlationId, subscription);

        var timeoutCts = new CancellationTokenSource();
        CancellationTokenRegistration timeoutRegistration = default;
        subscription.TimeoutRegistration = () => timeoutRegistration.DisposeAsync();
        subscription.TimeoutCancellation = timeoutCts;

        timeoutRegistration = timeoutCts.Token.Register(() =>
        {
            _ = Task.Run(async () =>
            {
                _logger.LogWarning("Timed out waiting for PostgreSQL response for correlationId {CorrelationId}.", correlationId);
                AsyncResponseDiagnostics.SetError(activity, "timeout", $"Timed out waiting for response for correlationId {correlationId}.");
                AsyncResponseDiagnostics.RecordWaiterTimeout("postgresql");
                tcs.TrySetException(new TimeoutException($"Timed out waiting for response for correlationId {correlationId}."));
                await subscription.CleanupOnceAsync(deleteRecoveryState: true).ConfigureAwait(false);
            });
        });

        try
        {
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
            await _sql.UpsertSubscriberAsync(correlationId, registrationId, _instanceId, _options.SubscriberHeartbeatTimeout, CancellationToken.None).ConfigureAwait(false);

            subscription.StartHeartbeat();
            timeoutCts.CancelAfter(timeout.Value);
            SignalDispatcher();

            if (_logger.IsEnabled(LogLevel.Information))
                _logger.LogInformation("Waiting for PostgreSQL response on correlationId {CorrelationId} with timeout {Timeout}.", correlationId, timeout.Value);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to create PostgreSQL waiter for correlationId {CorrelationId}.", correlationId);
            AsyncResponseDiagnostics.SetError(activity, "subscribe_failure", ex.Message);
            tcs.TrySetException(ex);
            await subscription.CleanupOnceAsync(deleteRecoveryState: true).ConfigureAwait(false);
        }

        Task ProcessUnderCapturedContextAsync(PostgreSqlChannelMessage message)
        {
            async Task Process()
            {
                using var correlationScope = AsyncResponseContext.PushCorrelationId(storedCorrelationId);
                await subscription.ProcessAsync(message).ConfigureAwait(false);
            }

            if (capturedContext is null)
                return Process();

            Task? task = null;
            ExecutionContext.Run(capturedContext, _ => task = Process(), null);
            return task!;
        }

        subscription.ProcessUnderContextAsync = ProcessUnderCapturedContextAsync;

        return new PostgreSqlAsyncResponseWaiter<T>(tcs.Task, () => subscription.CleanupOnceAsync(deleteRecoveryState: true));
    }

    /// <inheritdoc />
    public Task SetResponse<T>(T response, string correlationId, CancellationToken cancellationToken = default) where T : IAsyncResponsePayload
        => SetResponseCore(response, correlationId, cancellationToken);

    Task IRawAsyncResponsePublisher.SetRawResponse(object? response, string correlationId, CancellationToken cancellationToken)
        => SetResponseCore(response, correlationId, cancellationToken);

    Task IRawAsyncResponsePublisher.SetRawResponseJson(string responseJson, string correlationId, CancellationToken cancellationToken)
        => SetRawResponseJsonCore(responseJson, correlationId, cancellationToken);

    private async Task SetResponseCore<T>(T response, string correlationId, CancellationToken cancellationToken)
    {
        using var activity = AsyncResponseDiagnostics.StartActivity("asyncresponse.set_response", ActivityKind.Producer);
        activity?.SetTag("asyncresponse.channel", "postgresql");
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
            var subscribers = await CountActiveSubscribersAsync(correlationId, cancellationToken).ConfigureAwait(false);
            activity?.SetTag("asyncresponse.subscribers", subscribers);
            if (subscribers <= 0)
            {
                var dispatchResult = await _lostSubscriberDispatcher
                    .DispatchLostResponses(_recoveryStateStore, correlationId, response, ChannelName(correlationId), cancellationToken)
                    .ConfigureAwait(false);
                AsyncResponseDiagnostics.SetLostSubscriberRoute(activity, dispatchResult.ShouldResume);
                AsyncResponseDiagnostics.RecordLostSubscriber("response", dispatchResult.ShouldResume, dispatchResult.CallbackInvoked);
                activity?.SetTag("asyncresponse.recovery.callback_invoked", dispatchResult.CallbackInvoked);
                return;
            }

            var envelope = new AsyncResponseEnvelope<T> { Success = true, Payload = response };
            var json = JsonSerializer.Serialize(envelope, AsyncResponseEnvelopeOptions<T>.Instance);
            var messageId = await _sql.InsertMessageAsync(correlationId, json, _options.MessageRetention, cancellationToken).ConfigureAwait(false);
            SignalDispatcher();

            if (!await WaitForAcknowledgementAsync(messageId, cancellationToken).ConfigureAwait(false))
            {
                var dispatchResult = await _lostSubscriberDispatcher
                    .DispatchLostResponses(_recoveryStateStore, correlationId, response, ChannelName(correlationId), cancellationToken)
                    .ConfigureAwait(false);
                AsyncResponseDiagnostics.SetLostSubscriberRoute(activity, dispatchResult.ShouldResume);
                AsyncResponseDiagnostics.RecordLostSubscriber("response", dispatchResult.ShouldResume, dispatchResult.CallbackInvoked);
                activity?.SetTag("asyncresponse.recovery.callback_invoked", dispatchResult.CallbackInvoked);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to publish PostgreSQL response for correlationId {CorrelationId}.", correlationId);
            AsyncResponseDiagnostics.SetError(activity, ex);
            throw;
        }
    }

    private async Task SetRawResponseJsonCore(string responseJson, string correlationId, CancellationToken cancellationToken)
    {
        using var activity = AsyncResponseDiagnostics.StartActivity("asyncresponse.ingress.raw_response", ActivityKind.Producer);
        activity?.SetTag("asyncresponse.channel", "postgresql");
        AsyncResponseDiagnostics.SetCorrelationId(activity, correlationId);

        if (string.IsNullOrWhiteSpace(correlationId))
        {
            _logger.LogWarning("CorrelationId is null; cannot publish the raw response.");
            AsyncResponseDiagnostics.SetError(activity, "correlation_id_null", "CorrelationId is null; cannot publish the raw response.");
            return;
        }

        try
        {
            var subscribers = await CountActiveSubscribersAsync(correlationId, cancellationToken).ConfigureAwait(false);
            activity?.SetTag("asyncresponse.subscribers", subscribers);
            if (subscribers <= 0)
            {
                var response = new RawJsonResponse(responseJson).DeserializeUntyped();
                var dispatchResult = await _lostSubscriberDispatcher
                    .DispatchLostResponses(_recoveryStateStore, correlationId, response, ChannelName(correlationId), cancellationToken)
                    .ConfigureAwait(false);
                AsyncResponseDiagnostics.SetLostSubscriberRoute(activity, dispatchResult.ShouldResume);
                AsyncResponseDiagnostics.RecordLostSubscriber("response", dispatchResult.ShouldResume, dispatchResult.CallbackInvoked);
                activity?.SetTag("asyncresponse.recovery.callback_invoked", dispatchResult.CallbackInvoked);
                return;
            }

            var messageId = await _sql.InsertMessageAsync(correlationId, SerializeRawSuccessEnvelope(responseJson), _options.MessageRetention, cancellationToken).ConfigureAwait(false);
            SignalDispatcher();

            if (!await WaitForAcknowledgementAsync(messageId, cancellationToken).ConfigureAwait(false))
            {
                var response = new RawJsonResponse(responseJson).DeserializeUntyped();
                var dispatchResult = await _lostSubscriberDispatcher
                    .DispatchLostResponses(_recoveryStateStore, correlationId, response, ChannelName(correlationId), cancellationToken)
                    .ConfigureAwait(false);
                AsyncResponseDiagnostics.SetLostSubscriberRoute(activity, dispatchResult.ShouldResume);
                AsyncResponseDiagnostics.RecordLostSubscriber("response", dispatchResult.ShouldResume, dispatchResult.CallbackInvoked);
                activity?.SetTag("asyncresponse.recovery.callback_invoked", dispatchResult.CallbackInvoked);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to publish PostgreSQL raw response for correlationId {CorrelationId}.", correlationId);
            AsyncResponseDiagnostics.SetError(activity, ex);
            throw;
        }
    }

    /// <inheritdoc />
    public async Task SetException(Exception exception, string correlationId, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(exception);

        using var activity = AsyncResponseDiagnostics.StartActivity("asyncresponse.set_exception", ActivityKind.Producer);
        activity?.SetTag("asyncresponse.channel", "postgresql");
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
            var subscribers = await CountActiveSubscribersAsync(correlationId, cancellationToken).ConfigureAwait(false);
            activity?.SetTag("asyncresponse.subscribers", subscribers);
            if (subscribers <= 0)
            {
                var invoked = await _lostSubscriberDispatcher
                    .DispatchLostExceptions(_recoveryStateStore, correlationId, exception, ChannelName(correlationId), cancellationToken)
                    .ConfigureAwait(false);
                activity?.SetTag("asyncresponse.recovery.callback_invoked", invoked);
                AsyncResponseDiagnostics.RecordLostSubscriber("exception", shouldResume: false, invoked);
                return;
            }

            var envelope = new AsyncResponseEnvelope<object>
            {
                Success = false,
                ExceptionMessage = exception.Message,
                ExceptionStackTrace = RemoteStackTrace.ForWire(exception.StackTrace, _options.IncludeRemoteStackTrace, _options.MaxRemoteStackTraceLength),
                Payload = null
            };
            var json = JsonSerializer.Serialize(envelope, AsyncResponseEnvelopeOptions<object>.Instance);
            var messageId = await _sql.InsertMessageAsync(correlationId, json, _options.MessageRetention, cancellationToken).ConfigureAwait(false);
            SignalDispatcher();

            if (!await WaitForAcknowledgementAsync(messageId, cancellationToken).ConfigureAwait(false))
            {
                var invoked = await _lostSubscriberDispatcher
                    .DispatchLostExceptions(_recoveryStateStore, correlationId, exception, ChannelName(correlationId), cancellationToken)
                    .ConfigureAwait(false);
                activity?.SetTag("asyncresponse.recovery.callback_invoked", invoked);
                AsyncResponseDiagnostics.RecordLostSubscriber("exception", shouldResume: false, invoked);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to publish PostgreSQL exception response for correlationId {CorrelationId}.", correlationId);
            AsyncResponseDiagnostics.SetError(activity, ex);
            throw;
        }
    }

    /// <inheritdoc />
    public async ValueTask<long> CountActiveSubscribersAsync(string correlationId, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(correlationId))
            return 0L;

        try
        {
            return await _sql.CountActiveSubscribersAsync(correlationId, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogDebug(ex, "Failed to count PostgreSQL subscribers for correlationId {CorrelationId}.", correlationId);
            return 0L;
        }
    }

    /// <summary>
    /// Drops local subscriptions while leaving recovery state intact. Used by the sample app to
    /// simulate a redeploy for lost-subscriber integration tests.
    /// </summary>
    internal async Task DropLocalSubscriptionsAsync(CancellationToken cancellationToken = default)
    {
        foreach (var (correlationId, group) in _subscriptions.ToArray())
        {
            foreach (var subscription in group.Values.ToArray())
            {
                await subscription.DropLocalAsync(cancellationToken).ConfigureAwait(false);
                group.TryRemove(subscription.Id, out _);
            }

            if (group.IsEmpty)
                _subscriptions.TryRemove(correlationId, out _);

            await _executors.RemoveAsync(ChannelName(correlationId)).ConfigureAwait(false);
        }
    }

    private void AddSubscription(string correlationId, IPostgreSqlSubscription subscription)
    {
        var group = _subscriptions.GetOrAdd(correlationId, _ => new ConcurrentDictionary<Guid, IPostgreSqlSubscription>());
        group[subscription.Id] = subscription;
    }

    private void RemoveSubscription(string correlationId, Guid registrationId)
    {
        if (!_subscriptions.TryGetValue(correlationId, out var group))
            return;

        group.TryRemove(registrationId, out _);
        if (group.IsEmpty)
            _subscriptions.TryRemove(correlationId, out _);
    }

    private void EnsureListenerStarted()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(PostgreSqlAsyncResponseChannel));

        lock (_listenerGate)
        {
            if (_listenerCts is not null)
                return;

            _listenerCts = new CancellationTokenSource();
            _listenTask = Task.Run(() => ListenLoopAsync(_listenerCts.Token));
            _dispatchTask = Task.Run(() => DispatchLoopAsync(_listenerCts.Token));
        }
    }

    private async Task ListenLoopAsync(CancellationToken cancellationToken)
    {
        var failures = 0;
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await _sql.ExecuteListenAsync(() =>
                {
                    SignalDispatcher();
                    return Task.CompletedTask;
                }, cancellationToken).ConfigureAwait(false);
                failures = 0;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                failures++;
                var delay = AsyncResponseRetry.Backoff(failures, TimeSpan.FromMilliseconds(100), TimeSpan.FromSeconds(5));
                _logger.LogWarning(ex, "PostgreSQL LISTEN loop failed; retrying in {Delay}.", delay);
                await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private async Task DispatchLoopAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await DispatchPendingMessagesAsync(cancellationToken).ConfigureAwait(false);
                await WaitForSignalOrDelayAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "PostgreSQL response dispatch loop failed; retrying after poll delay.");
                await Task.Delay(_options.ListenerPollInterval, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private async Task DispatchPendingMessagesAsync(CancellationToken cancellationToken)
    {
        foreach (var (correlationId, group) in _subscriptions.ToArray())
        {
            var subscriptions = group.Values.Where(static s => !s.Dropped).ToArray();
            if (subscriptions.Length == 0)
                continue;

            var since = subscriptions.Min(static s => s.StartedAtUtc).AddSeconds(-1);
            var messages = await _sql.LoadMessagesAsync(correlationId, since, _options.PendingMessageBatchSize, cancellationToken).ConfigureAwait(false);
            foreach (var message in messages)
            {
                await _sql.AcknowledgeMessageAsync(message.Id, cancellationToken).ConfigureAwait(false);
                _executors.Enqueue(
                    ChannelName(correlationId),
                    () => DispatchMessageToSubscribersAsync(message, subscriptions));
            }
        }
    }

    private async Task DispatchMessageToSubscribersAsync(PostgreSqlChannelMessage message, IReadOnlyList<IPostgreSqlSubscription> subscriptions)
    {
        foreach (var subscription in subscriptions)
        {
            if (subscription.Dropped || !subscription.MarkSeen(message.Id))
                continue;

            await subscription.ProcessUnderContextAsync(message).ConfigureAwait(false);
        }
    }

    private async Task WaitForSignalOrDelayAsync(CancellationToken cancellationToken)
    {
        var delay = Task.Delay(_options.ListenerPollInterval, cancellationToken);
        var signal = _signals.Reader.WaitToReadAsync(cancellationToken).AsTask();
        var completed = await Task.WhenAny(delay, signal).ConfigureAwait(false);
        if (completed == signal)
        {
            await signal.ConfigureAwait(false);
            while (_signals.Reader.TryRead(out _))
            {
            }
        }
    }

    private void SignalDispatcher() => _signals.Writer.TryWrite(true);

    private async Task<bool> WaitForAcknowledgementAsync(Guid messageId, CancellationToken cancellationToken)
    {
        var deadline = DateTimeOffset.UtcNow + _options.DeliveryConfirmationTimeout;
        while (DateTimeOffset.UtcNow < deadline)
        {
            if (await _sql.IsMessageAcknowledgedAsync(messageId, cancellationToken).ConfigureAwait(false))
                return true;

            var remaining = deadline - DateTimeOffset.UtcNow;
            if (remaining <= TimeSpan.Zero)
                break;

            var delay = remaining < _options.DeliveryConfirmationPollInterval
                ? remaining
                : _options.DeliveryConfirmationPollInterval;
            await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
        }

        return await _sql.IsMessageAcknowledgedAsync(messageId, cancellationToken).ConfigureAwait(false);
    }

    private string ChannelName(string correlationId) => $"{_options.NotificationChannel}:{correlationId}";

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

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        _disposed = true;
        CancellationTokenSource? cts;
        Task? listenTask;
        Task? dispatchTask;
        lock (_listenerGate)
        {
            cts = _listenerCts;
            listenTask = _listenTask;
            dispatchTask = _dispatchTask;
            _listenerCts = null;
            _listenTask = null;
            _dispatchTask = null;
        }

        if (cts is not null)
        {
            await cts.CancelAsync().ConfigureAwait(false);
            try
            {
                await Task.WhenAll(new[] { listenTask, dispatchTask }.OfType<Task>()).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
            }
            cts.Dispose();
        }

        foreach (var (correlationId, group) in _subscriptions.ToArray())
        {
            foreach (var subscription in group.Values.ToArray())
                await subscription.CleanupOnceAsync(deleteRecoveryState: false).ConfigureAwait(false);
            await _executors.RemoveAsync(ChannelName(correlationId)).ConfigureAwait(false);
        }
    }

    private interface IPostgreSqlSubscription
    {
        Guid Id { get; }
        DateTimeOffset StartedAtUtc { get; }
        bool Dropped { get; }
        Func<PostgreSqlChannelMessage, Task> ProcessUnderContextAsync { get; set; }
        bool MarkSeen(Guid messageId);
        Task ProcessAsync(PostgreSqlChannelMessage message);
        ValueTask CleanupOnceAsync(bool deleteRecoveryState);
        ValueTask DropLocalAsync(CancellationToken cancellationToken);
    }

    private sealed class PostgreSqlSubscription<T> : IPostgreSqlSubscription where T : IAsyncResponsePayload
    {
        private readonly PostgreSqlAsyncResponseChannel _owner;
        private readonly string _correlationId;
        private readonly Func<T, ValueTask<bool>> _completionPredicate;
        private readonly TaskCompletionSource<T> _tcs;
        private readonly Activity? _activity;
        private readonly HashSet<Guid> _seen = [];
        private readonly object _seenGate = new();
        private readonly CancellationTokenSource _heartbeatCts = new();
        private int _cleanupStarted;
        private volatile bool _dropped;

        public PostgreSqlSubscription(
            PostgreSqlAsyncResponseChannel owner,
            string correlationId,
            Guid registrationId,
            DateTimeOffset startedAtUtc,
            Func<T, ValueTask<bool>> completionPredicate,
            TaskCompletionSource<T> tcs,
            ExecutionContext? _,
            Activity? activity)
        {
            _owner = owner;
            _correlationId = correlationId;
            Id = registrationId;
            StartedAtUtc = startedAtUtc;
            _completionPredicate = completionPredicate;
            _tcs = tcs;
            _activity = activity;
            ProcessUnderContextAsync = ProcessAsync;
        }

        public Guid Id { get; }
        public DateTimeOffset StartedAtUtc { get; }
        public bool Dropped => _dropped;
        public Func<ValueTask>? TimeoutRegistration { get; set; }
        public CancellationTokenSource? TimeoutCancellation { get; set; }
        public Func<PostgreSqlChannelMessage, Task> ProcessUnderContextAsync { get; set; }

        public bool MarkSeen(Guid messageId)
        {
            lock (_seenGate)
            {
                return _seen.Add(messageId);
            }
        }

        public void StartHeartbeat()
            => _ = Task.Run(HeartbeatLoopAsync);

        private async Task HeartbeatLoopAsync()
        {
            try
            {
                while (!_heartbeatCts.IsCancellationRequested)
                {
                    await Task.Delay(_owner._options.SubscriberHeartbeatInterval, _heartbeatCts.Token).ConfigureAwait(false);
                    await _owner._sql.UpsertSubscriberAsync(
                        _correlationId,
                        Id,
                        _owner._instanceId,
                        _owner._options.SubscriberHeartbeatTimeout,
                        _heartbeatCts.Token).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException)
            {
            }
            catch (Exception ex)
            {
                _owner._logger.LogDebug(ex, "PostgreSQL subscriber heartbeat failed for correlationId {CorrelationId}.", _correlationId);
            }
        }

        public async Task ProcessAsync(PostgreSqlChannelMessage message)
        {
            if (_dropped)
                return;

            var finished = false;
            try
            {
                var envelope = JsonSerializer.Deserialize<AsyncResponseEnvelope<T>>(message.EnvelopeJson, AsyncResponseEnvelopeOptions<T>.Instance);
                if (envelope is null)
                {
                    finished = true;
                    var error = new JsonException($"Failed to deserialize envelope for correlationId {_correlationId}.");
                    AsyncResponseDiagnostics.SetError(_activity, "deserialize_failure", error.Message);
                    _tcs.TrySetException(error);
                }
                else if (!AsyncResponseEnvelopeSchema.IsReadable(envelope.SchemaVersion))
                {
                    finished = true;
                    var error = new InvalidOperationException(
                        $"Response envelope for correlationId {_correlationId} has schema version {envelope.SchemaVersion}, " +
                        $"newer than this build supports ({AsyncResponseEnvelopeSchema.Current}); it was produced by a newer deployment.");
                    AsyncResponseDiagnostics.SetError(_activity, "schema_mismatch", error.Message);
                    _tcs.TrySetException(error);
                }
                else if (!envelope.Success)
                {
                    finished = true;
                    var remoteFailure = new Exception(envelope.ExceptionMessage ?? "Unknown error during asynchronous processing.");
                    if (!string.IsNullOrEmpty(envelope.ExceptionStackTrace))
                        remoteFailure.Data["RemoteStackTrace"] = RemoteStackTrace.Cap(envelope.ExceptionStackTrace, _owner._options.MaxRemoteStackTraceLength);
                    AsyncResponseDiagnostics.SetError(_activity, "remote_failure", remoteFailure.Message);
                    _tcs.TrySetException(remoteFailure);
                }
                else
                {
                    finished = await _completionPredicate(envelope.Payload!).ConfigureAwait(false);
                    if (finished)
                        _tcs.TrySetResult(envelope.Payload!);
                }
            }
            catch (Exception ex)
            {
                finished = true;
                _owner._logger.LogError(ex, "Error processing PostgreSQL response for correlationId {CorrelationId}.", _correlationId);
                AsyncResponseDiagnostics.SetError(_activity, ex);
                _tcs.TrySetException(ex);
            }
            finally
            {
                if (finished)
                    await CleanupOnceAsync(deleteRecoveryState: true).ConfigureAwait(false);
            }
        }

        public async ValueTask CleanupOnceAsync(bool deleteRecoveryState)
        {
            if (Interlocked.Exchange(ref _cleanupStarted, 1) != 0)
                return;

            try
            {
                _dropped = true;
                await _heartbeatCts.CancelAsync().ConfigureAwait(false);
                _owner.RemoveSubscription(_correlationId, Id);
                await _owner._sql.DeleteSubscriberAsync(_correlationId, Id, CancellationToken.None).ConfigureAwait(false);
                if (deleteRecoveryState)
                    await _owner._recoveryStateStore.TryDeleteAsync(_correlationId, Id).ConfigureAwait(false);
                await _owner._executors.RemoveAsync(_owner.ChannelName(_correlationId)).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _owner._logger.LogError(ex, "Error during PostgreSQL waiter cleanup for correlationId {CorrelationId}.", _correlationId);
            }
            finally
            {
                if (TimeoutRegistration is not null)
                    await TimeoutRegistration().ConfigureAwait(false);
                TimeoutCancellation?.Dispose();
                _heartbeatCts.Dispose();
                _activity?.Dispose();
            }
        }

        public async ValueTask DropLocalAsync(CancellationToken cancellationToken)
        {
            _dropped = true;
            await _heartbeatCts.CancelAsync().ConfigureAwait(false);
            await _owner._sql.DeleteSubscriberAsync(_correlationId, Id, cancellationToken).ConfigureAwait(false);
        }
    }
}
