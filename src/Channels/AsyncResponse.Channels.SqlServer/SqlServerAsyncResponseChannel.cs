using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Buffers;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;

namespace AsyncResponse.Channels.SqlServer;

/// <summary>
/// Microsoft SQL Server-backed response channel using an adaptive polling sweep for active waiter
/// wakeups (SQL Server has no <c>LISTEN/NOTIFY</c>) and SQL Server tables for durable recovery state.
/// Same-process deliveries bypass the sweep entirely; cross-process deliveries are picked up within
/// <see cref="SqlServerAsyncResponseChannelOptions.ActivePollInterval"/>, and the sweep backs off to
/// <see cref="SqlServerAsyncResponseChannelOptions.IdlePollInterval"/> while no waiters are subscribed.
/// </summary>
internal sealed class SqlServerAsyncResponseChannel :
    IAsyncResponsePublisher,
    IRawAsyncResponsePublisher,
    IRecoverableAsyncResponseSubscriber,
    IActiveSubscriberProbe,
    IAsyncDisposable
{
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<Guid, ISqlServerSubscription>> _subscriptions = new(StringComparer.Ordinal);

    // A signal carries the correlation id to scan (targeted), or null to scan every subscribed
    // correlation id (the periodic sweep safety net).
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

    private readonly SqlServerChannelSql _sql;
    private readonly IRecoveryStateStore _recoveryStateStore;
    private readonly AsyncResponseContextPropagation _propagation;
    private readonly LostSubscriberCallbackDispatcher _lostSubscriberDispatcher;
    private readonly SqlServerAsyncResponseChannelOptions _options;
    private readonly ILogger<SqlServerAsyncResponseChannel> _logger;
    private readonly SerialExecutorRegistry _executors;
    private readonly string _instanceId = $"{Environment.MachineName}-{Environment.ProcessId}-{Guid.NewGuid():N}";

    private readonly object _dispatcherGate = new();
    private CancellationTokenSource? _dispatcherCts;
    private Task? _dispatchTask;
    private Task? _heartbeatTask;
    private bool _disposed;

    /// <summary>Creates a SQL Server-backed async-response channel.</summary>
    public SqlServerAsyncResponseChannel(
        IServiceScopeFactory scopeFactory,
        SqlServerChannelSql sql,
        IRecoveryStateStore recoveryStateStore,
        IOptions<SqlServerAsyncResponseChannelOptions> options,
        AsyncResponseContextPropagation propagation,
        ILogger<SqlServerAsyncResponseChannel> logger)
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
                $"Payload type '{typeof(T)}' registers lost-subscriber recovery callbacks on the SQL Server channel " +
                $"but does not override {nameof(IAsyncResponsePayload)}.{nameof(IAsyncResponsePayload.ShouldResumeOnRecovery)}(). " +
                "Override it to declare which responses resume the flow (return true) versus fail it (return false); " +
                "the durable channel needs this to route a response that arrives after the waiter was lost.");
        }

        completionPredicate ??= _ => new ValueTask<bool>(true);
        timeout ??= _options.DefaultTimeout ?? _options.RecoveryStateExpiry;
        if (timeout <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(timeout), "Timeout must be greater than zero.");

        await _sql.EnsureCreatedAsync().ConfigureAwait(false);
        EnsureDispatcherStarted();

        // Watermark from the database clock, not the app clock: the dispatch loop filters pending
        // messages with created_at >= started, and mixing an app-side timestamp with DB-side created_at
        // would silently drop live deliveries under clock skew.
        var startedAtUtc = await _sql.GetServerTimeUtcAsync(CancellationToken.None).ConfigureAwait(false);

        var storedCorrelationId = correlationId;
        var capturedContext = ExecutionContext.Capture();

        var activity = AsyncResponseDiagnostics.StartActivity("asyncresponse.wait", correlationId: correlationId);
        activity?.SetTag("asyncresponse.channel", "sqlserver");
        AsyncResponseDiagnostics.SetPayloadType(activity, typeof(T));
        activity?.SetTag("asyncresponse.timeout_seconds", timeout.Value.TotalSeconds);

        var tcs = new TaskCompletionSource<T>(TaskCreationOptions.RunContinuationsAsynchronously);
        var registrationId = Guid.NewGuid();
        var subscription = new SqlServerSubscription<T>(
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
        Task ProcessUnderCapturedContextAsync(SqlServerChannelMessage message)
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

        var armed = false;
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

            timeoutCts.CancelAfter(timeout.Value);
            armed = true;

            if (_logger.IsEnabled(LogLevel.Debug))
                _logger.LogDebug("Waiting for SQL Server response on correlationId {CorrelationId} with timeout {Timeout}.", correlationId, timeout.Value);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to create SQL Server waiter for correlationId {CorrelationId}.", correlationId);
            AsyncResponseDiagnostics.SetError(activity, "subscribe_failure", ex.Message);
            tcs.TrySetException(ex);
            await subscription.CleanupOnceAsync(deleteRecoveryState: true).ConfigureAwait(false);
        }

        if (armed)
        {
            // Publish the subscription only once it is fully armed (heartbeat + timeout + context
            // delegate), then signal a scan targeted at this correlation id so any already-stored
            // response is delivered promptly without a full-table sweep.
            AddSubscription(correlationId, subscription);
            SignalDispatcher(correlationId);
        }

        return new SqlServerAsyncResponseWaiter<T>(tcs.Task, () => subscription.CleanupOnceAsync(deleteRecoveryState: true));
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
        activity?.SetTag("asyncresponse.channel", "sqlserver");
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
                    .DispatchLostResponses(_recoveryStateStore, correlationId, response, ChannelName(correlationId), cancellationToken)
                    .ConfigureAwait(false);
                AsyncResponseDiagnostics.SetLostSubscriberRoute(activity, dispatchResult.ShouldResume);
                AsyncResponseDiagnostics.RecordLostSubscriber("response", dispatchResult.ShouldResume, dispatchResult.CallbackInvoked);
                activity?.SetTag("asyncresponse.recovery.callback_invoked", dispatchResult.CallbackInvoked);
                return;
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
            _logger.LogError(ex, "Failed to publish SQL Server response for correlationId {CorrelationId}.", correlationId);
            AsyncResponseDiagnostics.SetError(activity, ex);
            throw;
        }
    }

    private async Task SetRawResponseJsonCore(string responseJson, string correlationId, CancellationToken cancellationToken)
    {
        using var activity = AsyncResponseDiagnostics.StartActivity("asyncresponse.ingress.raw_response", ActivityKind.Producer);
        activity?.SetTag("asyncresponse.channel", "sqlserver");
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
                    .DispatchLostResponses(_recoveryStateStore, correlationId, response, ChannelName(correlationId), cancellationToken)
                    .ConfigureAwait(false);
                AsyncResponseDiagnostics.SetLostSubscriberRoute(activity, dispatchResult.ShouldResume);
                AsyncResponseDiagnostics.RecordLostSubscriber("response", dispatchResult.ShouldResume, dispatchResult.CallbackInvoked);
                activity?.SetTag("asyncresponse.recovery.callback_invoked", dispatchResult.CallbackInvoked);
                return;
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
            _logger.LogError(ex, "Failed to publish SQL Server raw response for correlationId {CorrelationId}.", correlationId);
            AsyncResponseDiagnostics.SetError(activity, ex);
            throw;
        }
    }

    /// <inheritdoc />
    public async Task SetException(Exception exception, string correlationId, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(exception);

        using var activity = AsyncResponseDiagnostics.StartActivity("asyncresponse.set_exception", ActivityKind.Producer);
        activity?.SetTag("asyncresponse.channel", "sqlserver");
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
            var json = AsyncResponseEnvelopeJson.Serialize(envelope);
            var messageId = Guid.NewGuid();
            using var confirmation = BeginConfirmation(messageId);
            await PublishMessageAsync(messageId, correlationId, json, cancellationToken).ConfigureAwait(false);

            if (!await TryConfirmDeliveryAsync(confirmation, cancellationToken).ConfigureAwait(false))
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
            _logger.LogError(ex, "Failed to publish SQL Server exception response for correlationId {CorrelationId}.", correlationId);
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
            _logger.LogDebug(ex, "Failed to count SQL Server subscribers for correlationId {CorrelationId}.", correlationId);
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

    private void AddSubscription(string correlationId, ISqlServerSubscription subscription)
    {
        var group = _subscriptions.GetOrAdd(correlationId, _ => new ConcurrentDictionary<Guid, ISqlServerSubscription>());
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

    private void EnsureDispatcherStarted()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(SqlServerAsyncResponseChannel));

        lock (_dispatcherGate)
        {
            if (_dispatcherCts is not null)
                return;

            var dispatcherCts = new CancellationTokenSource();
            _dispatcherCts = dispatcherCts;
            _dispatchTask = Task.Run(() => DispatchLoopAsync(dispatcherCts.Token));
            _heartbeatTask = Task.Run(() => HeartbeatLoopAsync(dispatcherCts.Token));
        }
    }

    private async Task HeartbeatLoopAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(_options.SubscriberHeartbeatInterval, cancellationToken).ConfigureAwait(false);
                var registrationIds = SnapshotActiveRegistrationIds();
                if (registrationIds.Count > 0)
                {
                    await _sql.HeartbeatSubscribersAsync(
                        _instanceId,
                        registrationIds,
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
                _logger.LogWarning(ex, "SQL Server subscriber heartbeat failed; retrying for all local waiters.");
            }
        }
    }

    private List<Guid> SnapshotActiveRegistrationIds()
    {
        var registrationIds = new List<Guid>();
        foreach (var group in _subscriptions.Values)
        {
            foreach (var subscription in group.Values)
            {
                if (!subscription.Dropped)
                    registrationIds.Add(subscription.Id);
            }
        }

        return registrationIds;
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
                _logger.LogWarning(ex, "SQL Server response dispatch loop failed; retrying after poll delay.");
                await Task.Delay(CurrentPollInterval(), cancellationToken).ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// The adaptive sweep cadence: tight while any waiter is subscribed (cross-process deliveries
    /// must land promptly), backed off while the channel is idle so an inactive application does not
    /// keep polling the database at delivery latency.
    /// </summary>
    private TimeSpan CurrentPollInterval()
        => _subscriptions.IsEmpty ? _options.IdlePollInterval : _options.ActivePollInterval;

    /// <summary>
    /// Waits for the next dispatch trigger and returns its scope. <c>null</c> means scan every
    /// subscribed correlation id — a full sweep requested explicitly (a null signal) or by the
    /// periodic poll that recovers cross-process deliveries. A non-null set scans only the signaled
    /// correlation ids, so a flood of local signals never forces a scan of every waiter.
    /// </summary>
    private async Task<HashSet<string>?> CollectDispatchScopeAsync(CancellationToken cancellationToken)
    {
        var delay = Task.Delay(CurrentPollInterval(), cancellationToken);
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

            var subscriptions = new List<ISqlServerSubscription>(group.Count);
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
            new SqlServerChannelMessage(messageId, correlationId, envelopeJson, DateTimeOffset.UtcNow),
            cancellationToken).ConfigureAwait(false);
        SignalDispatcher(correlationId);
    }

    private async Task DispatchMessageToSubscribersAsync(
        SqlServerChannelMessage message,
        IReadOnlyList<ISqlServerSubscription> subscriptions,
        CancellationToken cancellationToken)
    {
        // Only subscriptions that are still live and have not already processed this message. Skipping
        // when there is nothing to deliver also avoids a redundant claim on every re-sweep.
        var hasTargets = false;
        foreach (var subscription in subscriptions)
        {
            if (!subscription.Dropped && !subscription.HasSeen(message.Id))
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
            if (subscription.Dropped || !subscription.MarkSeen(message.Id))
                continue;

            await subscription.ProcessUnderContextAsync(message).ConfigureAwait(false);
        }
    }

    private async Task TryDispatchLocalSubscribersAsync(SqlServerChannelMessage message, CancellationToken cancellationToken)
    {
        if (!_subscriptions.TryGetValue(message.CorrelationId, out var group))
            return;

        var subscriptions = new List<ISqlServerSubscription>(group.Count);
        foreach (var subscription in group.Values)
        {
            if (!subscription.Dropped)
                subscriptions.Add(subscription);
        }
        if (subscriptions.Count == 0)
            return;

        // Same-process fast path: skips the sweep latency but still runs on the per-correlation
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

    private string ChannelName(string correlationId) => $"{_options.SchemaName}.{_options.MessageTable}:{correlationId}";

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
        SqlServerSubscription<T> subscription,
        Activity? activity,
        string correlationId,
        TaskCompletionSource<T> tcs) where T : IAsyncResponsePayload
    {
        _logger.LogWarning("Timed out waiting for SQL Server response for correlationId {CorrelationId}.", correlationId);
        AsyncResponseDiagnostics.SetError(activity, "timeout", $"Timed out waiting for response for correlationId {correlationId}.");
        AsyncResponseDiagnostics.RecordWaiterTimeout("sqlserver");
        tcs.TrySetException(new TimeoutException($"Timed out waiting for response for correlationId {correlationId}."));
        await subscription.CleanupOnceAsync(deleteRecoveryState: true).ConfigureAwait(false);
    }

    private interface IWaiterTimeoutState
    {
        void Schedule();
    }

    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    private sealed class WaiterTimeoutState<T>(
        SqlServerAsyncResponseChannel owner,
        SqlServerSubscription<T> subscription,
        Activity? activity,
        string correlationId,
        TaskCompletionSource<T> tcs) : IWaiterTimeoutState where T : IAsyncResponsePayload
    {
        public void Schedule()
            => _ = Task.Run(() => owner.HandleWaiterTimeoutAsync(subscription, activity, correlationId, tcs));
    }

    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    private sealed class LocalDispatchWorkItem(
        SqlServerAsyncResponseChannel owner,
        SqlServerChannelMessage message,
        IReadOnlyList<ISqlServerSubscription> subscriptions,
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
                    "Local SQL Server response dispatch failed for correlationId {CorrelationId}; the sweep retry will pick it up.",
                    message.CorrelationId);
            }
        }
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        _disposed = true;
        CancellationTokenSource? cts;
        Task? dispatchTask;
        Task? heartbeatTask;
        lock (_dispatcherGate)
        {
            cts = _dispatcherCts;
            dispatchTask = _dispatchTask;
            heartbeatTask = _heartbeatTask;
            _dispatcherCts = null;
            _dispatchTask = null;
            _heartbeatTask = null;
        }

        if (cts is not null)
        {
            await cts.CancelAsync().ConfigureAwait(false);
            try
            {
                await Task.WhenAll(new[] { dispatchTask, heartbeatTask }.OfType<Task>()).ConfigureAwait(false);
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
        SqlServerAsyncResponseChannel owner,
        Guid messageId,
        TaskCompletionSource<bool> tcs) : IDisposable
    {
        public Guid MessageId => messageId;
        public Task<bool> Delivered => tcs.Task;
        public void Dispose() => owner._pendingConfirmations.TryRemove(messageId, out _);
    }

    private interface ISqlServerSubscription
    {
        Guid Id { get; }
        DateTimeOffset StartedAtUtc { get; }
        bool Dropped { get; }
        Func<SqlServerChannelMessage, Task> ProcessUnderContextAsync { get; set; }
        bool HasSeen(Guid messageId);
        bool MarkSeen(Guid messageId);
        void PruneSeen(DateTimeOffset cutoffUtc);
        Task ProcessAsync(SqlServerChannelMessage message);
        ValueTask CleanupOnceAsync(bool deleteRecoveryState);
        ValueTask DropLocalAsync(CancellationToken cancellationToken);
    }

    private sealed class SqlServerSubscription<T> : ISqlServerSubscription where T : IAsyncResponsePayload
    {
        private readonly SqlServerAsyncResponseChannel _owner;
        private readonly string _correlationId;
        private readonly Func<T, ValueTask<bool>> _completionPredicate;
        private readonly TaskCompletionSource<T> _tcs;
        private readonly Activity? _activity;
        private readonly HashSet<Guid> _seen = [];
        private readonly Queue<(Guid Id, DateTimeOffset SeenAtUtc)> _seenOrder = [];
        private readonly object _seenGate = new();
        private int _cleanupStarted;
        private volatile bool _dropped;

        public SqlServerSubscription(
            SqlServerAsyncResponseChannel owner,
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
        public Func<SqlServerChannelMessage, Task> ProcessUnderContextAsync { get; set; }

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

        public async Task ProcessAsync(SqlServerChannelMessage message)
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
                _owner._logger.LogError(ex, "Error processing SQL Server response for correlationId {CorrelationId}.", _correlationId);
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
                _owner.RemoveSubscription(_correlationId, Id);
                await _owner._sql.DeleteSubscriberAsync(_correlationId, Id, CancellationToken.None).ConfigureAwait(false);
                if (deleteRecoveryState)
                    await _owner._recoveryStateStore.TryDeleteAsync(_correlationId, Id).ConfigureAwait(false);

                // Schedule the executor retirement on the thread pool; do not await directly —
                // dispatch-loop deliveries run this cleanup ON the executor, and RemoveAsync waits
                // for the executor's drain loop to finish, which would be a circular await.
                var channelName = _owner.ChannelName(_correlationId);
                _ = Task.Run(async () => await _owner._executors.RemoveAsync(channelName).ConfigureAwait(false));
            }
            catch (Exception ex)
            {
                _owner._logger.LogError(ex, "Error during SQL Server waiter cleanup for correlationId {CorrelationId}.", _correlationId);
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
