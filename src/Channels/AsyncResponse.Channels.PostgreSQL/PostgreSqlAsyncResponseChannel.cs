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

    // A signal carries the correlation id to scan (targeted), or null to scan every subscribed
    // correlation id (the periodic missed-notification safety net).
    private readonly Channel<string?> _signals = Channel.CreateBounded<string?>(new BoundedChannelOptions(1024)
    {
        SingleReader = true,
        SingleWriter = false,
        FullMode = BoundedChannelFullMode.DropOldest
    });

    // Maps a just-published message id to a completion the local dispatch loop trips the instant it
    // delivers the message to a live waiter. Same-process delivery (the overwhelmingly common case)
    // is confirmed without polling the database; cross-process delivery falls back to polling acked_at.
    private readonly ConcurrentDictionary<Guid, TaskCompletionSource<bool>> _pendingConfirmations = new();

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
    private Task? _heartbeatTask;
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

        // Watermark from the database clock, not the app clock: the dispatch loop filters pending
        // messages with created_at >= started, and mixing an app-side timestamp with DB-side created_at
        // would silently drop live deliveries under clock skew.
        var startedAtUtc = await _sql.GetServerTimeUtcAsync(CancellationToken.None).ConfigureAwait(false);

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
            startedAtUtc,
            completionPredicate,
            tcs,
            capturedContext,
            activity);

        var timeoutCts = new CancellationTokenSource();
        CancellationTokenRegistration timeoutRegistration = default;
        subscription.TimeoutRegistration = () => timeoutRegistration.DisposeAsync();
        subscription.TimeoutCancellation = timeoutCts;

        timeoutRegistration = timeoutCts.Token.Register(
            OnWaiterTimeout,
            new WaiterTimeoutState<T>(this, subscription, activity, correlationId, tcs));

        // Wire the captured-context delegate before the subscription becomes discoverable, so a
        // response already stored for this correlation id is processed with the caller's context.
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
            // Subscriber row BEFORE recovery state: "recovery state visible ⇒ subscription
            // visible" is the invariant the lost-subscriber dispatcher's live re-check relies on.
            // In the reverse order a publisher could see the state, see no subscriber, and consume
            // the registration while this waiter is milliseconds from being live.
            await _sql.UpsertSubscriberAsync(correlationId, registrationId, _instanceId, _options.SubscriberHeartbeatTimeout, CancellationToken.None).ConfigureAwait(false);
            await _recoveryStateStore.SaveAsync(correlationId, recoveryState, _options.RecoveryStateExpiry).ConfigureAwait(false);

            timeoutCts.CancelAfter(timeout.Value);

            if (_logger.IsEnabled(LogLevel.Debug))
                _logger.LogDebug("Waiting for PostgreSQL response on correlationId {CorrelationId} with timeout {Timeout}.", correlationId, timeout.Value);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to create PostgreSQL waiter for correlationId {CorrelationId}.", correlationId);
            AsyncResponseDiagnostics.SetError(activity, "subscribe_failure", ex.Message);
            await subscription.CleanupOnceAsync(deleteRecoveryState: true).ConfigureAwait(false);

            // Rethrow instead of returning a pre-faulted waiter: the builder's contract is that
            // the trigger runs only once the subscription AND recovery state exist. A returned
            // waiter would still let the trigger fire the remote operation with no registration
            // left to receive (or recover) its response. Cleanup cancels the response task, so no
            // pending task is left behind.
            tcs.TrySetCanceled();
            throw;
        }

        // Publish the subscription only once it is fully armed (heartbeat + timeout + context
        // delegate), then signal a scan targeted at this correlation id so any already-stored
        // response is delivered promptly without a full-table sweep.
        AddSubscription(correlationId, subscription);
        SignalDispatcher(correlationId);

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
            var subscribers = await _sql.CountActiveSubscribersAsync(correlationId, cancellationToken).ConfigureAwait(false);
            activity?.SetTag("asyncresponse.subscribers", subscribers);
            if (subscribers <= 0)
            {
                var dispatchResult = await _lostSubscriberDispatcher
                    .DispatchLostResponses(
                        _recoveryStateStore,
                        correlationId,
                        response,
                        ChannelName(correlationId),
                        cancellationToken,
                        hasLiveSubscriber: () => HasLiveSubscriberAsync(correlationId, cancellationToken))
                    .ConfigureAwait(false);
                if (!dispatchResult.RetryLive)
                {
                    AsyncResponseDiagnostics.SetLostSubscriberRoute(activity, dispatchResult.ShouldResume);
                    AsyncResponseDiagnostics.RecordLostSubscriber("response", dispatchResult.ShouldResume, dispatchResult.CallbackInvoked);
                    activity?.SetTag("asyncresponse.recovery.callback_invoked", dispatchResult.CallbackInvoked);
                    return;
                }

                // A waiter registered between the count and the recovery-state read — publish live
                // instead of consuming its registration.
            }

            var envelope = new AsyncResponseEnvelope<T> { Success = true, Payload = response };
            var json = AsyncResponseEnvelopeJson.Serialize(envelope);
            var messageId = Guid.NewGuid();
            using var confirmation = BeginConfirmation(messageId);
            await PublishMessageAsync(messageId, correlationId, json, cancellationToken).ConfigureAwait(false);

            if (!await TryConfirmDeliveryAsync(confirmation, cancellationToken).ConfigureAwait(false))
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
            var subscribers = await _sql.CountActiveSubscribersAsync(correlationId, cancellationToken).ConfigureAwait(false);
            activity?.SetTag("asyncresponse.subscribers", subscribers);
            if (subscribers <= 0)
            {
                var response = new RawJsonResponse(responseJson).DeserializeUntyped();
                var dispatchResult = await _lostSubscriberDispatcher
                    .DispatchLostResponses(
                        _recoveryStateStore,
                        correlationId,
                        response,
                        ChannelName(correlationId),
                        cancellationToken,
                        hasLiveSubscriber: () => HasLiveSubscriberAsync(correlationId, cancellationToken))
                    .ConfigureAwait(false);
                if (!dispatchResult.RetryLive)
                {
                    AsyncResponseDiagnostics.SetLostSubscriberRoute(activity, dispatchResult.ShouldResume);
                    AsyncResponseDiagnostics.RecordLostSubscriber("response", dispatchResult.ShouldResume, dispatchResult.CallbackInvoked);
                    activity?.SetTag("asyncresponse.recovery.callback_invoked", dispatchResult.CallbackInvoked);
                    return;
                }

                // A waiter registered between the count and the recovery-state read — publish live
                // instead of consuming its registration.
            }

            var messageId = Guid.NewGuid();
            using var confirmation = BeginConfirmation(messageId);
            await PublishMessageAsync(messageId, correlationId, SerializeRawSuccessEnvelope(responseJson), cancellationToken).ConfigureAwait(false);

            if (!await TryConfirmDeliveryAsync(confirmation, cancellationToken).ConfigureAwait(false))
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
            var subscribers = await _sql.CountActiveSubscribersAsync(correlationId, cancellationToken).ConfigureAwait(false);
            activity?.SetTag("asyncresponse.subscribers", subscribers);
            if (subscribers <= 0)
            {
                var dispatchResult = await _lostSubscriberDispatcher
                    .DispatchLostExceptions(
                        _recoveryStateStore,
                        correlationId,
                        exception,
                        ChannelName(correlationId),
                        cancellationToken,
                        hasLiveSubscriber: () => HasLiveSubscriberAsync(correlationId, cancellationToken))
                    .ConfigureAwait(false);
                if (!dispatchResult.RetryLive)
                {
                    activity?.SetTag("asyncresponse.recovery.callback_invoked", dispatchResult.CallbackInvoked);
                    AsyncResponseDiagnostics.RecordLostSubscriber("exception", shouldResume: false, dispatchResult.CallbackInvoked);
                    return;
                }

                // A waiter registered between the count and the recovery-state read — publish live
                // instead of consuming its registration.
            }

            var envelope = new AsyncResponseEnvelope<object>
            {
                Success = false,
                ExceptionMessage = exception.Message,
                ExceptionStackTrace = RemoteStackTrace.ForWire(exception.StackTrace, _options.IncludeRemoteStackTrace, _options.MaxRemoteStackTraceLength),
                Payload = null
            };
            var json = AsyncResponseEnvelopeJson.Serialize(envelope);
            var messageId = Guid.NewGuid();
            using var confirmation = BeginConfirmation(messageId);
            await PublishMessageAsync(messageId, correlationId, json, cancellationToken).ConfigureAwait(false);

            if (!await TryConfirmDeliveryAsync(confirmation, cancellationToken).ConfigureAwait(false))
            {
                // No live re-check here: TryClaimForRecoveryAsync already won the message for the
                // recovery path, so live delivery of it is no longer possible.
                var dispatchResult = await _lostSubscriberDispatcher
                    .DispatchLostExceptions(_recoveryStateStore, correlationId, exception, ChannelName(correlationId), cancellationToken)
                    .ConfigureAwait(false);
                activity?.SetTag("asyncresponse.recovery.callback_invoked", dispatchResult.CallbackInvoked);
                AsyncResponseDiagnostics.RecordLostSubscriber("exception", shouldResume: false, dispatchResult.CallbackInvoked);
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

    /// <summary>
    /// Re-probes waiter liveness for the lost-subscriber dispatcher's snapshot-race re-check,
    /// using the same active-subscriber count the publish path consulted.
    /// </summary>
    private async ValueTask<bool> HasLiveSubscriberAsync(string correlationId, CancellationToken cancellationToken)
        => await _sql.CountActiveSubscribersAsync(correlationId, cancellationToken).ConfigureAwait(false) > 0;

    private void AddSubscription(string correlationId, IPostgreSqlSubscription subscription)
    {
        var group = _subscriptions.GetOrAdd(correlationId, _ => new ConcurrentDictionary<Guid, IPostgreSqlSubscription>());
        group[subscription.Id] = subscription;
        _executors.OnSubscriptionRegistered(ChannelName(correlationId));
    }

    private void RemoveSubscription(string correlationId, Guid registrationId)
    {
        if (!_subscriptions.TryGetValue(correlationId, out var group))
            return;

        if (group.TryRemove(registrationId, out _))
            _executors.OnSubscriptionRetired(ChannelName(correlationId));
        if (group.IsEmpty)
            _subscriptions.TryRemove(correlationId, out _);
    }

    private void EnsureListenerStarted()
    {
        lock (_listenerGate)
        {
            // Checked under the same gate DisposeAsync sets it under: a racing registration must
            // never recreate the CTS and loops after disposal tore them down.
            if (_disposed)
                throw new ObjectDisposedException(nameof(PostgreSqlAsyncResponseChannel));

            if (_listenerCts is not null)
                return;

            var listenerCts = new CancellationTokenSource();
            _listenerCts = listenerCts;
            _listenTask = Task.Run(() => ListenLoopAsync(listenerCts.Token));
            _dispatchTask = Task.Run(() => DispatchLoopAsync(listenerCts.Token));
            _heartbeatTask = Task.Run(() => HeartbeatLoopAsync(listenerCts.Token));
        }
    }

    private async Task HeartbeatLoopAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(_options.SubscriberHeartbeatInterval, cancellationToken).ConfigureAwait(false);
                var registrations = SnapshotActiveRegistrations();
                if (registrations.Count > 0)
                {
                    await _sql.HeartbeatSubscribersAsync(
                        _instanceId,
                        registrations,
                        _options.SubscriberHeartbeatTimeout,
                        cancellationToken).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "PostgreSQL subscriber heartbeat failed; retrying for all local waiters.");
            }
        }
    }

    private List<(string CorrelationId, Guid RegistrationId)> SnapshotActiveRegistrations()
    {
        // Full (correlation id, registration id) pairs: the heartbeat UPSERTs the subscriber rows,
        // so it needs everything required to re-create a row the pruner has already deleted.
        var registrations = new List<(string CorrelationId, Guid RegistrationId)>();
        foreach (var (correlationId, group) in _subscriptions)
        {
            foreach (var subscription in group.Values)
            {
                if (!subscription.Dropped)
                    registrations.Add((correlationId, subscription.Id));
            }
        }

        return registrations;
    }

    private async Task ListenLoopAsync(CancellationToken cancellationToken)
    {
        var failures = 0;
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await _sql.ExecuteListenAsync(payload =>
                {
                    SignalDispatcher(string.IsNullOrEmpty(payload) ? null : payload);
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
                var scope = await CollectDispatchScopeAsync(cancellationToken).ConfigureAwait(false);
                await DispatchPendingMessagesAsync(scope, cancellationToken).ConfigureAwait(false);
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

    /// <summary>
    /// Waits for the next dispatch trigger and returns its scope. <c>null</c> means scan every
    /// subscribed correlation id — a full sweep requested explicitly (a null signal) or by the
    /// periodic poll that is the missed-notification safety net. A non-null set scans only the
    /// signaled correlation ids, so a flood of notifications never forces a scan of every waiter.
    /// </summary>
    private async Task<HashSet<string>?> CollectDispatchScopeAsync(CancellationToken cancellationToken)
    {
        var delay = Task.Delay(_options.ListenerPollInterval, cancellationToken);
        var signal = _signals.Reader.WaitToReadAsync(cancellationToken).AsTask();
        var completed = await Task.WhenAny(delay, signal).ConfigureAwait(false);
        if (completed == delay)
            return null;

        await signal.ConfigureAwait(false);

        var scope = new HashSet<string>(StringComparer.Ordinal);
        var fullSweep = false;
        while (_signals.Reader.TryRead(out var correlationId))
        {
            if (string.IsNullOrEmpty(correlationId))
                fullSweep = true;
            else
                scope.Add(correlationId);
        }

        return fullSweep || scope.Count == 0 ? null : scope;
    }

    private async Task DispatchPendingMessagesAsync(HashSet<string>? scope, CancellationToken cancellationToken)
    {
        foreach (var (correlationId, group) in _subscriptions)
        {
            if (scope is not null && !scope.Contains(correlationId))
                continue;

            var subscriptions = new List<IPostgreSqlSubscription>(group.Count);
            foreach (var subscription in group.Values)
            {
                if (!subscription.Dropped)
                    subscriptions.Add(subscription);
            }
            if (subscriptions.Count == 0)
                continue;

            var since = subscriptions.Min(static s => s.StartedAtUtc).AddSeconds(-1);
            var seenCutoff = DateTimeOffset.UtcNow - _options.MessageRetention - TimeSpan.FromMinutes(1);
            foreach (var subscription in subscriptions)
                subscription.PruneSeen(seenCutoff);

            DateTimeOffset? afterCreatedAtUtc = null;
            Guid? afterId = null;
            while (true)
            {
                var messages = await _sql.LoadMessagesAsync(
                    correlationId,
                    since,
                    _options.PendingMessageBatchSize,
                    afterCreatedAtUtc,
                    afterId,
                    cancellationToken).ConfigureAwait(false);
                foreach (var message in messages)
                {
                    await _executors.EnqueueAsync(
                        ChannelName(correlationId),
                        () => DispatchMessageToSubscribersAsync(message, subscriptions, cancellationToken),
                        cancellationToken).ConfigureAwait(false);
                }

                if (messages.Count < _options.PendingMessageBatchSize)
                    break;

                var last = messages[^1];
                afterCreatedAtUtc = last.CreatedAtUtc;
                afterId = last.Id;
            }
        }
    }

    private async Task PublishMessageAsync(
        Guid messageId,
        string correlationId,
        string envelopeJson,
        CancellationToken cancellationToken)
    {
        await _sql.InsertMessageAsync(messageId, correlationId, envelopeJson, _options.MessageRetention, cancellationToken)
            .ConfigureAwait(false);
        await TryDispatchLocalSubscribersAsync(
            new PostgreSqlChannelMessage(messageId, correlationId, envelopeJson, DateTimeOffset.UtcNow),
            cancellationToken).ConfigureAwait(false);
        SignalDispatcher(correlationId);
    }

    private async Task DispatchMessageToSubscribersAsync(
        PostgreSqlChannelMessage message,
        IReadOnlyList<IPostgreSqlSubscription> subscriptions,
        CancellationToken cancellationToken)
    {
        // Only subscriptions that are still live, inside their delivery watermark, and have not
        // already processed this message. Skipping when there is nothing to deliver also avoids a
        // redundant claim on every re-sweep.
        var hasTargets = false;
        foreach (var subscription in subscriptions)
        {
            if (!subscription.Dropped && IsWithinWatermark(subscription, message) && !subscription.HasSeen(message.Id))
            {
                hasTargets = true;
                break;
            }
        }
        if (!hasTargets)
            return;

        // Take the message for live delivery. The claim sets acked_at unless the publisher already
        // routed it to recovery (recovery_claimed); losing the claim means recovery owns it, so it is
        // not delivered to the waiter and handled a second time.
        if (!await _sql.TryClaimForDeliveryAsync(message.Id, cancellationToken).ConfigureAwait(false))
        {
            foreach (var subscription in subscriptions)
            {
                if (!subscription.Dropped)
                    subscription.MarkSeen(message.Id);
            }
            return;
        }

        // Wake the publisher immediately if it is waiting in this process — no acked_at polling needed.
        if (_pendingConfirmations.TryGetValue(message.Id, out var confirmation))
            confirmation.TrySetResult(true);

        foreach (var subscription in subscriptions)
        {
            if (subscription.Dropped || !IsWithinWatermark(subscription, message) || !subscription.MarkSeen(message.Id))
                continue;

            await subscription.ProcessUnderContextAsync(message).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Per-subscription delivery watermark. The sweep queries with the OLDEST waiter's watermark on
    /// a shared correlation id, so without this filter a late-joining waiter would receive retained
    /// messages created before it registered. Same 1s tolerance as the query watermark.
    /// </summary>
    private static bool IsWithinWatermark(IPostgreSqlSubscription subscription, PostgreSqlChannelMessage message)
        => message.CreatedAtUtc >= subscription.StartedAtUtc.AddSeconds(-1);

    private async Task TryDispatchLocalSubscribersAsync(PostgreSqlChannelMessage message, CancellationToken cancellationToken)
    {
        if (!_subscriptions.TryGetValue(message.CorrelationId, out var group))
            return;

        var subscriptions = new List<IPostgreSqlSubscription>(group.Count);
        foreach (var subscription in group.Values)
        {
            if (!subscription.Dropped)
                subscriptions.Add(subscription);
        }
        if (subscriptions.Count == 0)
            return;

        // Same-process fast path: skips the LISTEN round trip but still runs on the per-correlation
        // serial executor — completion predicates are guaranteed serial, in-order invocation on every
        // channel, and a direct dispatch here could otherwise run concurrently with a sweep-enqueued
        // dispatch of a different message for the same subscription. MarkSeen keeps the sweep from
        // double-processing this message.
        await _executors.EnqueueAsync(
            ChannelName(message.CorrelationId),
            new LocalDispatchWorkItem(this, message, subscriptions, cancellationToken).InvokeAsync,
            cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Registers an in-process delivery completion for a message id. Disposing it removes the entry,
    /// so a publish that throws or completes never leaks the registration.
    /// </summary>
    private PendingConfirmation BeginConfirmation(Guid messageId)
    {
        var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        _pendingConfirmations[messageId] = tcs;
        return new PendingConfirmation(this, messageId, tcs);
    }

    /// <summary>
    /// Confirms a published response reached a live waiter. Returns <c>true</c> once a waiter has
    /// acknowledged it; on confirmation timeout, atomically claims the message for the lost-subscriber
    /// path and returns <c>false</c> only if that claim wins — so the recovery callback and a
    /// slow-but-live waiter are mutually exclusive.
    /// </summary>
    private async Task<bool> TryConfirmDeliveryAsync(PendingConfirmation confirmation, CancellationToken cancellationToken)
    {
        if (await WaitForAcknowledgementAsync(confirmation, cancellationToken).ConfigureAwait(false))
            return true;

        return !await _sql.TryClaimForRecoveryAsync(confirmation.MessageId, cancellationToken).ConfigureAwait(false);
    }

    private void SignalDispatcher(string? correlationId = null) => _signals.Writer.TryWrite(correlationId);

    private async Task<bool> WaitForAcknowledgementAsync(PendingConfirmation confirmation, CancellationToken cancellationToken)
    {
        var deadline = DateTimeOffset.UtcNow + _options.DeliveryConfirmationTimeout;
        while (DateTimeOffset.UtcNow < deadline)
        {
            var remaining = deadline - DateTimeOffset.UtcNow;
            if (remaining <= TimeSpan.Zero)
                break;

            var pollDelay = remaining < _options.DeliveryConfirmationPollInterval
                ? remaining
                : _options.DeliveryConfirmationPollInterval;

            // Fast path: an in-process delivery trips the completion and we return without a query.
            await Task.WhenAny(confirmation.Delivered, Task.Delay(pollDelay, cancellationToken)).ConfigureAwait(false);
            if (confirmation.Delivered.IsCompletedSuccessfully)
                return true;

            // Slow path: a delivery in another process only set acked_at, so poll for it.
            if (await _sql.IsMessageAcknowledgedAsync(confirmation.MessageId, cancellationToken).ConfigureAwait(false))
                return true;
        }

        return confirmation.Delivered.IsCompletedSuccessfully
            || await _sql.IsMessageAcknowledgedAsync(confirmation.MessageId, cancellationToken).ConfigureAwait(false);
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

    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    private static void OnWaiterTimeout(object? state)
        => ((IWaiterTimeoutState)state!).Schedule();

    private async Task HandleWaiterTimeoutAsync<T>(
        PostgreSqlSubscription<T> subscription,
        Activity? activity,
        string correlationId,
        TaskCompletionSource<T> tcs) where T : IAsyncResponsePayload
    {
        _logger.LogWarning("Timed out waiting for PostgreSQL response for correlationId {CorrelationId}.", correlationId);
        AsyncResponseDiagnostics.SetError(activity, "timeout", $"Timed out waiting for response for correlationId {correlationId}.");
        AsyncResponseDiagnostics.RecordWaiterTimeout("postgresql");
        tcs.TrySetException(new TimeoutException($"Timed out waiting for response for correlationId {correlationId}."));
        await subscription.CleanupOnceAsync(deleteRecoveryState: true).ConfigureAwait(false);
    }

    private interface IWaiterTimeoutState
    {
        void Schedule();
    }

    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    private sealed class WaiterTimeoutState<T>(
        PostgreSqlAsyncResponseChannel owner,
        PostgreSqlSubscription<T> subscription,
        Activity? activity,
        string correlationId,
        TaskCompletionSource<T> tcs) : IWaiterTimeoutState where T : IAsyncResponsePayload
    {
        public void Schedule()
            => _ = Task.Run(() => owner.HandleWaiterTimeoutAsync(subscription, activity, correlationId, tcs));
    }

    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    private sealed class LocalDispatchWorkItem(
        PostgreSqlAsyncResponseChannel owner,
        PostgreSqlChannelMessage message,
        IReadOnlyList<IPostgreSqlSubscription> subscriptions,
        CancellationToken cancellationToken)
    {
        public async Task InvokeAsync()
        {
            try
            {
                await owner.DispatchMessageToSubscribersAsync(message, subscriptions, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
            {
                owner._logger.LogDebug(
                    ex,
                    "Local PostgreSQL response dispatch failed for correlationId {CorrelationId}; listener retry will pick it up.",
                    message.CorrelationId);
            }
        }
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        CancellationTokenSource? cts;
        Task? listenTask;
        Task? dispatchTask;
        Task? heartbeatTask;
        lock (_listenerGate)
        {
            // Set under the gate so EnsureListenerStarted can never observe "not disposed" and
            // then recreate the CTS/loops this teardown is about to stop.
            _disposed = true;
            cts = _listenerCts;
            listenTask = _listenTask;
            dispatchTask = _dispatchTask;
            heartbeatTask = _heartbeatTask;
            _listenerCts = null;
            _listenTask = null;
            _dispatchTask = null;
            _heartbeatTask = null;
        }

        if (cts is not null)
        {
            await cts.CancelAsync().ConfigureAwait(false);
            try
            {
                await Task.WhenAll(new[] { listenTask, dispatchTask, heartbeatTask }.OfType<Task>()).ConfigureAwait(false);
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

    /// <summary>Scopes an in-process delivery completion; <see cref="Dispose"/> unregisters it.</summary>
    private readonly struct PendingConfirmation(
        PostgreSqlAsyncResponseChannel owner,
        Guid messageId,
        TaskCompletionSource<bool> tcs) : IDisposable
    {
        public Guid MessageId => messageId;
        public Task<bool> Delivered => tcs.Task;
        public void Dispose() => owner._pendingConfirmations.TryRemove(messageId, out _);
    }

    private interface IPostgreSqlSubscription
    {
        Guid Id { get; }
        DateTimeOffset StartedAtUtc { get; }
        bool Dropped { get; }
        Func<PostgreSqlChannelMessage, Task> ProcessUnderContextAsync { get; set; }
        bool HasSeen(Guid messageId);
        bool MarkSeen(Guid messageId);
        void PruneSeen(DateTimeOffset cutoffUtc);
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
        private readonly Queue<(Guid Id, DateTimeOffset SeenAtUtc)> _seenOrder = [];
        private readonly object _seenGate = new();
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

        public bool HasSeen(Guid messageId)
        {
            lock (_seenGate)
            {
                return _seen.Contains(messageId);
            }
        }

        public bool MarkSeen(Guid messageId)
        {
            lock (_seenGate)
            {
                if (!_seen.Add(messageId))
                    return false;

                // Use the local observation time, not the database creation time. This keeps the
                // pruning queue monotonic and avoids immediate eviction when app and DB clocks differ.
                _seenOrder.Enqueue((messageId, DateTimeOffset.UtcNow));
                return true;
            }
        }

        public void PruneSeen(DateTimeOffset cutoffUtc)
        {
            lock (_seenGate)
            {
                while (_seenOrder.TryPeek(out var entry) && entry.SeenAtUtc < cutoffUtc)
                {
                    _seenOrder.Dequeue();
                    _seen.Remove(entry.Id);
                }
            }
        }

        public async Task ProcessAsync(PostgreSqlChannelMessage message)
        {
            if (_dropped)
                return;

            var finished = false;
            try
            {
                var envelope = JsonSerializer.Deserialize(message.EnvelopeJson, AsyncResponseEnvelopeJson.TypeInfo<T>());
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
                        $"which this build does not support (current: {AsyncResponseEnvelopeSchema.Current}).");
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

                // Delete the recovery state BEFORE removing the subscription (locally and in the
                // subscriber table). In the reverse order a publish landing in the window sees
                // "no subscriber, state present" and fires a spurious recovery callback for a wait
                // that already reached a terminal state. In this order the window shows a
                // subscriber that drops the message — a late or duplicate terminal message is
                // droppable; a resurrected recovery callback is not. (Shutdown/redeploy paths pass
                // deleteRecoveryState: false and keep the state for lost-subscriber recovery.)
                if (deleteRecoveryState)
                    await _owner._recoveryStateStore.TryDeleteAsync(_correlationId, Id).ConfigureAwait(false);
                _owner.RemoveSubscription(_correlationId, Id);
                await _owner._sql.DeleteSubscriberAsync(_correlationId, Id, CancellationToken.None).ConfigureAwait(false);

                // Schedule the executor retirement on the thread pool; do not await directly —
                // dispatch-loop deliveries run this cleanup ON the executor, and RemoveAsync waits
                // for the executor's drain loop to finish, which would be a circular await.
                var channelName = _owner.ChannelName(_correlationId);
                _ = Task.Run(async () => await _owner._executors.RemoveAsync(channelName).ConfigureAwait(false));
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
                _activity?.Dispose();
            }
        }

        public async ValueTask DropLocalAsync(CancellationToken cancellationToken)
        {
            _dropped = true;
            await _owner._sql.DeleteSubscriberAsync(_correlationId, Id, cancellationToken).ConfigureAwait(false);
        }
    }
}
