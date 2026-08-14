using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System.Buffers;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;

namespace AsyncResponse.Channels;

// Shared source for the database-backed response channels (PostgreSQL, SQL Server, MongoDB),
// mirroring the DurableFlows shared-store pattern: each channel csproj pulls this file in via
// <Compile Include="..\Shared\DbChannelShared.cs" />, so the base class compiles INTO each
// provider assembly against that provider's concrete seam types. The seam is bound per project
// with three global using aliases (declared at the top of the provider's channel file):
//
//   DbChannelStore   -> the provider's store/SQL adapter (e.g. PostgreSqlChannelSql)
//   DbChannelMessage -> the provider's channel-message record (e.g. PostgreSqlChannelMessage)
//   DbChannelOptions -> the provider's options class (e.g. PostgreSqlAsyncResponseChannelOptions)
//
// Because the aliases resolve to concrete sealed types at compile time, store calls on the
// per-message paths stay direct (no interface dispatch, no delegate indirection) — see the
// benchmark note in RedisAsyncResponseChannel.SetResponseCore for why that matters. The only
// virtual seams are the four hooks below, which cover exactly what the three providers genuinely
// do differently: the channel-name format, the sweep cadence, the optional wake listener, and the
// provider waiter type.

/// <summary>
/// Provider-agnostic machinery for the database-backed response channels: waiter registration and
/// recovery-state bookkeeping, publish with delivery confirmation, the signal-driven dispatch
/// sweep, the subscriber heartbeat, and subscription lifecycle/cleanup. Derived channels supply
/// the wake mechanism (LISTEN/NOTIFY, adaptive polling, change streams), the channel-name format,
/// and the provider waiter type via the protected hooks.
/// </summary>
internal abstract class DbAsyncResponseChannelBase :
    IAsyncResponsePublisher,
    IRawAsyncResponsePublisher,
    IRecoverableAsyncResponseSubscriber,
    IActiveSubscriberProbe,
    IAsyncDisposable
{
    private protected readonly ConcurrentDictionary<string, ConcurrentDictionary<Guid, IDbSubscription>> _subscriptions = new(StringComparer.Ordinal);

    // A signal carries the correlation id to scan (targeted), or null to scan every subscribed
    // correlation id (the periodic sweep that is the missed-wake safety net).
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

    private protected readonly DbChannelStore _store;
    private readonly IRecoveryStateStore _recoveryStateStore;
    private readonly AsyncResponseContextPropagation _propagation;
    private readonly LostSubscriberCallbackDispatcher _lostSubscriberDispatcher;
    private protected readonly DbChannelOptions _options;
    private protected readonly ILogger _logger;
    private readonly SerialExecutorRegistry _executors;
    private readonly string _instanceId = $"{Environment.MachineName}-{Environment.ProcessId}-{Guid.NewGuid():N}";

    // Provider text used in diagnostics. The emitted strings must stay byte-identical to the
    // pre-consolidation per-provider channels — tests and dashboards match on them.
    private readonly string _channelTypeName;
    private readonly string _providerName;
    private readonly string _activityTag;
    private readonly string _subscriberRecordNoun;
    private readonly string _localDispatchRetryHint;

    private readonly object _listenerGate = new();
    private protected CancellationTokenSource? _listenerCts;
    private protected Task? _listenTask;
    private protected Task? _dispatchTask;
    private protected Task? _heartbeatTask;
    private bool _disposed;

    /// <summary>Creates the shared machinery for a database-backed async-response channel.</summary>
    protected DbAsyncResponseChannelBase(
        IServiceScopeFactory scopeFactory,
        DbChannelStore store,
        IRecoveryStateStore recoveryStateStore,
        DbChannelOptions options,
        AsyncResponseContextPropagation propagation,
        ILogger logger,
        string channelTypeName,
        string providerName,
        string activityTag,
        string subscriberRecordNoun,
        string localDispatchRetryHint)
    {
        _store = store;
        _recoveryStateStore = recoveryStateStore;
        _propagation = propagation;
        _options = options;
        _options.Validate();
        _logger = logger;
        _channelTypeName = channelTypeName;
        _providerName = providerName;
        _activityTag = activityTag;
        _subscriberRecordNoun = subscriberRecordNoun;
        _localDispatchRetryHint = localDispatchRetryHint;
        _lostSubscriberDispatcher = new LostSubscriberCallbackDispatcher(scopeFactory, propagation, logger);
        _executors = new SerialExecutorRegistry(logger);
    }

    /// <summary>
    /// The per-correlation channel name used as the serial-executor key and the lost-subscriber
    /// channel label. Formats differ per provider (notification channel, schema.table, collection).
    /// </summary>
    protected abstract string ChannelName(string correlationId);

    /// <summary>
    /// The dispatch sweep cadence. Fixed (<c>ListenerPollInterval</c>) for the providers with a push
    /// wake; adaptive (active/idle) for SQL Server where the sweep IS the cross-process wake.
    /// </summary>
    protected abstract TimeSpan CurrentPollInterval();

    /// <summary>
    /// Starts the provider's wake listener loop (LISTEN/NOTIFY, change stream), or returns
    /// <c>null</c> when the provider has none and relies on the dispatch sweep alone.
    /// </summary>
    protected virtual Task? StartWakeListener(CancellationToken cancellationToken) => null;

    /// <summary>Wraps the response task in the provider's waiter type.</summary>
    protected abstract IAsyncResponseWaiter<T> CreateWaiter<T>(Task<T> responseTask, Func<ValueTask> cleanupAsync)
        where T : IAsyncResponsePayload;

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
        CorrelationIdGuard.ThrowIfUnusable(correlationId);

        if ((resumeCallback is not null || failureCallback is not null)
            && !AsyncResponsePayloadReflection.OverridesOnRecovery(typeof(T)))
        {
            throw new InvalidOperationException(
                $"Payload type '{typeof(T)}' registers lost-subscriber recovery callbacks on the {_providerName} channel " +
                $"but does not override {nameof(IAsyncResponsePayload)}.{nameof(IAsyncResponsePayload.OnRecovery)}(). " +
                "Override it to declare what each response does to the flow — RecoveryAction.Resume, " +
                "RecoveryAction.Fail, or RecoveryAction.KeepWaiting for non-terminal checkpoints; the durable " +
                "channel needs this to route a response that arrives after the waiter was lost.");
        }

        completionPredicate ??= _ => new ValueTask<bool>(true);
        timeout ??= _options.DefaultTimeout ?? _options.RecoveryStateExpiry;
        // BEFORE any side effect: an unsupported resolved timeout (non-positive, or past the
        // ~49.7-day BCL timer ceiling) used to throw only at timer arming — after the
        // subscription and recovery state existed, leaking both — and zero used to slip through
        // on some channels entirely, insta-timing-out a fully registered waiter.
        AsyncResponseChannelOptions.EnsureWaiterTimeoutSupported(timeout.Value);

        // Refuse BEFORE any store round trip: EnsureCreatedAsync now validates manually managed
        // schemas over the network, and a disposed channel must fail with ObjectDisposedException,
        // not with whatever that connection attempt throws. EnsureListenerStarted below re-checks
        // under the gate, so a dispose racing this early check still cannot start listeners.
        ThrowIfDisposed();
        await _store.EnsureCreatedAsync().ConfigureAwait(false);
        EnsureListenerStarted();

        // Watermark from the database server's clock, not the app clock: the dispatch loop filters
        // pending messages with created_at >= started, and mixing an app-side timestamp with the
        // server-stamped created_at would silently drop live deliveries under clock skew. The
        // same round trip draws this subscription's position in the store's monotonic ack
        // sequence — the exact ordering IsWithinWatermark uses to separate "acked before this
        // waiter existed" (history) from "acked to a group including this waiter" (fan-out),
        // which no pair of same-tick timestamps can distinguish.
        var (startedAtUtc, startedSeq) = await _store.GetSubscriptionStartAsync(CancellationToken.None).ConfigureAwait(false);

        var storedCorrelationId = correlationId;
        var capturedContext = ExecutionContext.Capture();

        var activity = AsyncResponseDiagnostics.StartActivity("asyncresponse.wait", correlationId: correlationId);
        activity?.SetTag("asyncresponse.channel", _activityTag);
        AsyncResponseDiagnostics.SetPayloadType(activity, typeof(T));
        activity?.SetTag("asyncresponse.timeout_seconds", timeout.Value.TotalSeconds);

        var tcs = new TaskCompletionSource<T>(TaskCreationOptions.RunContinuationsAsynchronously);
        var registrationId = Guid.NewGuid();
        var subscription = new DbSubscription<T>(
            this,
            correlationId,
            registrationId,
            startedAtUtc,
            startedSeq,
            completionPredicate,
            tcs,
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
        Task ProcessUnderCapturedContextAsync(DbChannelMessage message)
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
            // Subscriber record BEFORE recovery state: "recovery state visible ⇒ subscription
            // visible" is the invariant the lost-subscriber dispatcher's live re-check relies on.
            // In the reverse order a publisher could see the state, see no subscriber, and consume
            // the registration while this waiter is milliseconds from being live.
            await _store.UpsertSubscriberAsync(correlationId, registrationId, _instanceId, _options.SubscriberHeartbeatTimeout, CancellationToken.None).ConfigureAwait(false);
            await _recoveryStateStore.SaveAsync(correlationId, recoveryState, _options.RecoveryStateExpiry).ConfigureAwait(false);

            timeoutCts.CancelAfter(timeout.Value);

            if (_logger.IsEnabled(LogLevel.Debug))
                _logger.LogDebug("Waiting for {Provider} response on correlationId {CorrelationId} with timeout {Timeout}.", _providerName, correlationId, timeout.Value);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to create {Provider} waiter for correlationId {CorrelationId}.", _providerName, correlationId);
            AsyncResponseDiagnostics.SetError(activity, "subscribe_failure", ex.Message);
            await subscription.DrainThenCleanupAsync(deleteRecoveryState: true).ConfigureAwait(false);

            // Rethrow instead of returning a pre-faulted waiter: the builder's contract is that
            // the trigger runs only once the subscription AND recovery state exist. A returned
            // waiter would still let the trigger fire the remote operation with no registration
            // left to receive (or recover) its response. Cleanup cancels the response task, so no
            // pending task is left behind.
            throw;
        }

        // Publish the subscription only once it is fully armed (heartbeat + timeout + context
        // delegate), then signal a scan targeted at this correlation id so any already-stored
        // response is delivered promptly without a full sweep.
        AddSubscription(correlationId, subscription);
        SignalDispatcher(correlationId);

        return CreateWaiter<T>(tcs.Task, () => subscription.DrainThenCleanupAsync(deleteRecoveryState: true));
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
        activity?.SetTag("asyncresponse.channel", _activityTag);
        AsyncResponseDiagnostics.SetPayloadType(activity, typeof(T));
        AsyncResponseDiagnostics.SetCorrelationId(activity, correlationId);

        if (CorrelationIdGuard.IsUnpublishable(correlationId, _logger, activity, "the response"))
            return;

        try
        {
            var subscribers = await _store.CountActiveSubscribersAsync(correlationId, cancellationToken).ConfigureAwait(false);
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
                    AsyncResponseDiagnostics.SetLostSubscriberRoute(activity, dispatchResult.Action, dispatchResult.RouteMixed);
                    AsyncResponseDiagnostics.RecordLostSubscriber("response", dispatchResult.Action, dispatchResult.CallbackInvoked, dispatchResult.RouteMixed);
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
                AsyncResponseDiagnostics.SetLostSubscriberRoute(activity, dispatchResult.Action, dispatchResult.RouteMixed);
                AsyncResponseDiagnostics.RecordLostSubscriber("response", dispatchResult.Action, dispatchResult.CallbackInvoked, dispatchResult.RouteMixed);
                activity?.SetTag("asyncresponse.recovery.callback_invoked", dispatchResult.CallbackInvoked);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to publish {Provider} response for correlationId {CorrelationId}.", _providerName, correlationId);
            AsyncResponseDiagnostics.SetError(activity, ex);
            throw;
        }
    }

    private async Task SetRawResponseJsonCore(string responseJson, string correlationId, CancellationToken cancellationToken)
    {
        using var activity = AsyncResponseDiagnostics.StartActivity("asyncresponse.ingress.raw_response", ActivityKind.Producer);
        activity?.SetTag("asyncresponse.channel", _activityTag);
        AsyncResponseDiagnostics.SetCorrelationId(activity, correlationId);

        if (CorrelationIdGuard.IsUnpublishable(correlationId, _logger, activity, "the raw response", dropContractViolations: true))
            return;

        try
        {
            var subscribers = await _store.CountActiveSubscribersAsync(correlationId, cancellationToken).ConfigureAwait(false);
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
                    AsyncResponseDiagnostics.SetLostSubscriberRoute(activity, dispatchResult.Action, dispatchResult.RouteMixed);
                    AsyncResponseDiagnostics.RecordLostSubscriber("response", dispatchResult.Action, dispatchResult.CallbackInvoked, dispatchResult.RouteMixed);
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
                AsyncResponseDiagnostics.SetLostSubscriberRoute(activity, dispatchResult.Action, dispatchResult.RouteMixed);
                AsyncResponseDiagnostics.RecordLostSubscriber("response", dispatchResult.Action, dispatchResult.CallbackInvoked, dispatchResult.RouteMixed);
                activity?.SetTag("asyncresponse.recovery.callback_invoked", dispatchResult.CallbackInvoked);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to publish {Provider} raw response for correlationId {CorrelationId}.", _providerName, correlationId);
            AsyncResponseDiagnostics.SetError(activity, ex);
            throw;
        }
    }

    /// <inheritdoc />
    public async Task SetException(Exception exception, string correlationId, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(exception);

        using var activity = AsyncResponseDiagnostics.StartActivity("asyncresponse.set_exception", ActivityKind.Producer);
        activity?.SetTag("asyncresponse.channel", _activityTag);
        activity?.SetTag("asyncresponse.exception_type", exception.GetType().FullName ?? exception.GetType().Name);
        AsyncResponseDiagnostics.SetCorrelationId(activity, correlationId);

        if (CorrelationIdGuard.IsUnpublishable(correlationId, _logger, activity, "the exception", exception))
            return;

        try
        {
            var subscribers = await _store.CountActiveSubscribersAsync(correlationId, cancellationToken).ConfigureAwait(false);
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
                    AsyncResponseDiagnostics.RecordLostSubscriber("exception", action: RecoveryAction.Fail, dispatchResult.CallbackInvoked);
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
                AsyncResponseDiagnostics.RecordLostSubscriber("exception", action: RecoveryAction.Fail, dispatchResult.CallbackInvoked);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to publish {Provider} exception response for correlationId {CorrelationId}.", _providerName, correlationId);
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
            return await _store.CountActiveSubscribersAsync(correlationId, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogDebug(ex, "Failed to count {Provider} subscribers for correlationId {CorrelationId}.", _providerName, correlationId);
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
                // Retire the registry registration too (as RemoveSubscription does): a leftover
                // refcount would defeat the tombstone set by the RemoveAsync below, letting a
                // later delivery recreate an executor nothing ever retires.
                if (group.TryRemove(subscription.Id, out _))
                    _executors.OnSubscriptionRetired(ChannelName(correlationId));
            }

            UnlinkIfEmpty(correlationId, group);

            await _executors.RemoveAsync(ChannelName(correlationId)).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Re-probes waiter liveness for the lost-subscriber dispatcher's snapshot-race re-check,
    /// using the same active-subscriber count the publish path consulted.
    /// </summary>
    private async ValueTask<bool> HasLiveSubscriberAsync(string correlationId, CancellationToken cancellationToken)
        => await _store.CountActiveSubscribersAsync(correlationId, cancellationToken).ConfigureAwait(false) > 0;

    private protected void AddSubscription(string correlationId, IDbSubscription subscription)
    {
        // Register with the executor registry BEFORE publishing into the subscription map: every
        // dispatch path consults the map and then enqueues, so a delivery racing a visible-but-
        // unregistered subscription on a correlation id reused within the tombstone lifetime would
        // be silently dropped. In the reversed window (registered, not yet visible) the delivery
        // just waits for the next sweep or falls back to lost-subscriber recovery.
        _executors.OnSubscriptionRegistered(ChannelName(correlationId));
        while (true)
        {
            var group = _subscriptions.GetOrAdd(correlationId, _ => new ConcurrentDictionary<Guid, IDbSubscription>());
            group[subscription.Id] = subscription;

            // A concurrent RemoveSubscription may have unlinked this group between the GetOrAdd
            // and the insert above (its emptiness check cannot see the in-flight insert). If the
            // group this subscription landed in is no longer the mapped one, move it to the live
            // group so it stays reachable to every dispatch path.
            if (_subscriptions.TryGetValue(correlationId, out var current) && ReferenceEquals(current, group))
                return;

            group.TryRemove(subscription.Id, out _);
        }
    }

    private void RemoveSubscription(string correlationId, Guid registrationId)
    {
        if (!_subscriptions.TryGetValue(correlationId, out var group))
            return;

        if (group.TryRemove(registrationId, out _))
            _executors.OnSubscriptionRetired(ChannelName(correlationId));
        UnlinkIfEmpty(correlationId, group);
    }

    // Unlinks an emptied subscription group from the map without orphaning a concurrent
    // registration: the emptiness read and the map removal cannot be one atomic step, so a
    // waiter registered for a reused correlation id in that window would land in an unreachable
    // group and time out despite its response being published. Unlink only our exact group,
    // then re-link (or merge) anything a racing AddSubscription slipped into it.
    private void UnlinkIfEmpty(string correlationId, ConcurrentDictionary<Guid, IDbSubscription> group)
    {
        if (!group.IsEmpty)
            return;

        if (!((ICollection<KeyValuePair<string, ConcurrentDictionary<Guid, IDbSubscription>>>)_subscriptions)
                .Remove(new KeyValuePair<string, ConcurrentDictionary<Guid, IDbSubscription>>(correlationId, group)))
            return;

        if (group.IsEmpty)
            return;

        var merged = _subscriptions.GetOrAdd(correlationId, group);
        if (ReferenceEquals(merged, group))
            return;

        foreach (var entry in group)
            merged[entry.Key] = entry.Value;
    }

    private void ThrowIfDisposed()
    {
        lock (_listenerGate)
        {
            if (_disposed)
                throw new ObjectDisposedException(_channelTypeName);
        }
    }

    private protected void EnsureListenerStarted()
    {
        lock (_listenerGate)
        {
            // Checked under the same gate DisposeAsync sets it under: a racing registration must
            // never recreate the CTS and loops after disposal tore them down.
            if (_disposed)
                throw new ObjectDisposedException(_channelTypeName);

            if (_listenerCts is not null)
                return;

            var listenerCts = new CancellationTokenSource();
            _listenerCts = listenerCts;
            _listenTask = StartWakeListener(listenerCts.Token);
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
                    try
                    {
                        await _store.HeartbeatSubscribersAsync(
                            _instanceId,
                            registrations,
                            _options.SubscriberHeartbeatTimeout,
                            cancellationToken).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                    {
                        return;
                    }
                    catch (Exception ex)
                    {
                        // The round still compensates below: SQL Server commits per-batch,
                        // MongoDB bulk-writes unordered, and any provider can fail after some
                        // upserts landed — a registration dropped mid-round may already be
                        // resurrected even though the round as a whole threw. Skipping the
                        // re-check on failure left exactly those rows phantom until TTL.
                        _logger.LogWarning(ex, "{Provider} subscriber heartbeat failed; retrying for all local waiters.", _providerName);
                    }

                    await DeleteRegistrationsDroppedDuringHeartbeatAsync(registrations, cancellationToken).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "{Provider} subscriber heartbeat failed; retrying for all local waiters.", _providerName);
            }
        }
    }

    private List<(string CorrelationId, Guid RegistrationId)> SnapshotActiveRegistrations()
    {
        // Full (correlation id, registration id) pairs: the heartbeat UPSERTs the subscriber
        // records, so it needs everything required to re-create one the store's expiry pruning
        // (relational pruner / TTL reaper) has already deleted.
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

    /// <summary>
    /// Closes the heartbeat/cleanup race: a subscription can be dropped (and its subscriber row
    /// deleted) AFTER the snapshot above was taken but BEFORE the round's upsert landed — the
    /// upsert then resurrects the deleted row, and until it ages out past the heartbeat timeout
    /// every publisher counts a live waiter that no longer exists, suppressing lost-subscriber
    /// recovery for the correlation id. Both cleanup paths set <c>Dropped</c> BEFORE issuing
    /// their delete, which makes this post-round re-check airtight: either the drop is visible
    /// here and the compensating delete below lands after the resurrecting upsert, or the drop
    /// happened after this check — and then the cleanup's own delete is ordered after the upsert
    /// and removes the row itself. Best-effort like the cleanup delete: a failed compensation
    /// ages out via the heartbeat timeout.
    /// </summary>
    private async Task DeleteRegistrationsDroppedDuringHeartbeatAsync(
        List<(string CorrelationId, Guid RegistrationId)> heartbeaten,
        CancellationToken cancellationToken)
    {
        foreach (var (correlationId, registrationId) in heartbeaten)
        {
            if (IsRegistrationLive(correlationId, registrationId))
                continue;

            _logger.LogDebug(
                "Deleting {Provider} subscriber {RegistrationId} for correlationId {CorrelationId}: it was dropped while a heartbeat round was in flight.",
                _providerName, registrationId, correlationId);
            try
            {
                await _store.DeleteSubscriberAsync(correlationId, registrationId, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex,
                    "Failed to delete {Provider} subscriber {SubscriberRecord} for correlationId {CorrelationId} after its heartbeat-round drop; it ages out via the heartbeat timeout.",
                    _providerName, _subscriberRecordNoun, correlationId);
            }
        }
    }

    private bool IsRegistrationLive(string correlationId, Guid registrationId)
        => _subscriptions.TryGetValue(correlationId, out var group)
           && group.TryGetValue(registrationId, out var subscription)
           && !subscription.Dropped;

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
                _logger.LogWarning(ex, "{Provider} response dispatch loop failed; retrying after poll delay.", _providerName);
                await Task.Delay(CurrentPollInterval(), cancellationToken).ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// Waits for the next dispatch trigger and returns its scope. <c>null</c> means scan every
    /// subscribed correlation id — a full sweep requested explicitly (a null signal) or by the
    /// periodic poll that is the missed-wake / cross-process-delivery safety net. A non-null set
    /// scans only the signaled correlation ids, so a flood of wake signals never forces a scan of
    /// every waiter.
    /// </summary>
    /// <summary>Returned for a poll tick whose full sweep is not yet due: scan nothing. Never mutated.</summary>
    private static readonly HashSet<string> EmptyDispatchScope = [];

    private DateTimeOffset _lastFullSweepUtc;

    private protected async Task<HashSet<string>?> CollectDispatchScopeAsync(CancellationToken cancellationToken)
    {
        // The WhenAny loser is cancelled via the per-iteration linked source: an abandoned
        // WaitToReadAsync would otherwise stay parked in the channel's blocked-reader list until
        // the next signal — one per poll interval, accumulating without bound on an idle channel.
        using var iteration = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var delay = Task.Delay(CurrentPollInterval(), iteration.Token);
        var signal = _signals.Reader.WaitToReadAsync(iteration.Token).AsTask();
        var completed = await Task.WhenAny(delay, signal).ConfigureAwait(false);
        iteration.Cancel();
        if (completed == delay)
        {
            // The timer sweep costs one store query per subscribed correlation id, so with W
            // waiters an idle channel pays W queries per poll tick. FullSweepInterval bounds that:
            // a tick whose sweep is not yet due scans nothing (signaled scans are unaffected —
            // they arrive through the signal branch below with their own scope).
            if (_options.FullSweepInterval is { } fullSweepInterval)
            {
                var now = DateTimeOffset.UtcNow;
                if (now - _lastFullSweepUtc < fullSweepInterval)
                    return EmptyDispatchScope;
                _lastFullSweepUtc = now;
            }

            return null;
        }

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

        if (fullSweep || scope.Count == 0)
        {
            // A signal-driven full sweep does the timer sweep's work; stamping it defers the next
            // timer sweep by a full interval instead of re-scanning everything twice in a row.
            _lastFullSweepUtc = DateTimeOffset.UtcNow;
            return null;
        }

        return scope;
    }

    private protected async Task DispatchPendingMessagesAsync(HashSet<string>? scope, CancellationToken cancellationToken)
    {
        foreach (var (correlationId, group) in _subscriptions)
        {
            if (scope is not null && !scope.Contains(correlationId))
                continue;

            var subscriptions = new List<IDbSubscription>(group.Count);
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
                var messages = await _store.LoadMessagesAsync(
                    correlationId,
                    since,
                    _options.PendingMessageBatchSize,
                    afterCreatedAtUtc,
                    afterId,
                    cancellationToken).ConfigureAwait(false);
                foreach (var message in messages)
                {
                    // The store was asked for ONE exact correlation id, but "exact" is the
                    // database's opinion: a case-insensitive (or accent-insensitive) column
                    // collation — the SQL Server default in most deployments — answers a query for
                    // "FOO" with the rows of "foo". Delivering those would hand one waiter another
                    // waiter's response, so the id is re-checked ordinally here, where the
                    // library's own comparison rules apply. This also covers pre-existing tables
                    // created before the collation was pinned in the DDL.
                    if (!string.Equals(message.CorrelationId, correlationId, StringComparison.Ordinal))
                    {
                        _logger.LogError(
                            "The {Provider} channel store returned a message for correlationId '{ReturnedCorrelationId}' when asked for '{RequestedCorrelationId}'. " +
                            "The correlation-id column is not using a case-sensitive/binary collation, so distinct correlation ids collide in the database. " +
                            "The message was NOT delivered to the wrong waiter. Re-create the AsyncResponse tables (or ALTER the correlation_id columns) with a binary collation.",
                            _providerName, message.CorrelationId, correlationId);
                        continue;
                    }

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
        // The insert itself carries the remote wake where the provider has one (a NOTIFY rides the
        // PostgreSQL insert; MongoDB change streams observe it) and the SQL Server sweep polls it
        // up. Only the local fast path and a targeted local signal are needed on top. The store
        // returns the SERVER-stamped created_at for the local fast-path message: subscription
        // watermarks are server-clock, and an app-clock timestamp here silently disabled the fast
        // path whenever the app clock ran more than the 1s tolerance behind the database — delivery
        // then quietly degraded to sweep latency on every publish.
        var createdAtUtc = await _store.InsertMessageAsync(messageId, correlationId, envelopeJson, _options.MessageRetention, cancellationToken)
            .ConfigureAwait(false);
        await TryDispatchLocalSubscribersAsync(
            new DbChannelMessage(messageId, correlationId, envelopeJson, createdAtUtc),
            cancellationToken).ConfigureAwait(false);
        SignalDispatcher(correlationId);
    }

    private protected async Task DispatchMessageToSubscribersAsync(
        DbChannelMessage message,
        IReadOnlyList<IDbSubscription> subscriptions,
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
        //
        // Claim-then-dispatch is deliberate — keep this ordering. The in-process handoff is
        // at-most-once by design: a crash between the claim and the waiter's continuation can only
        // lose delivery to waiters in THIS dying process, which no ordering could save (their
        // continuations die with it), while pre-registered fan-out waiters in other processes
        // still receive the acked message (IsWithinWatermark admits acked_at > started_at).
        // Dispatch-then-ack behind an expiring claim would re-open the stale-redelivery wrong-data
        // bug the strict acked exclusion in IsWithinWatermark closes. Durability across process
        // death belongs to the layer above: flow re-execution, publish-time recovery routing, and
        // the step timeout.
        if (!await _store.TryClaimForDeliveryAsync(message.Id, cancellationToken).ConfigureAwait(false))
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
    /// <para>
    /// The creation-time tolerance alone re-admits history: a message created inside the 1s skew
    /// window may have already been delivered and acked for a PREVIOUS waiter that reused the
    /// correlation id, and per-subscription seen-tracking cannot dedupe what a different
    /// subscription processed. A message acked before this subscription existed is history, not
    /// delivery — waiters that legitimately participate in a delivery (including cross-process
    /// fan-out) were registered before its claim stamped <c>acked_at</c>. The acked comparison is
    /// deliberately strict, with no skew tolerance: under skew, strictness can only make a waiter
    /// whose registration raced another process's in-flight ack keep waiting for its own response,
    /// whereas a tolerance would re-open the stale-redelivery window this check closes.
    /// </para>
    /// <para>
    /// The comparison must be STRICTLY greater, and that is load-bearing rather than stylistic: a
    /// server clock's resolution is far coarser than its column precision, so equal timestamps are
    /// routine, not a measure-zero tie. SQL Server stamps <c>datetime2(7)</c> from
    /// <c>SYSUTCDATETIME()</c> — 100ns precision, but the clock behind it advances in ~5ms ticks
    /// (measured: 30,344 samples over 300ms yielded 61 distinct values, mean gap 4.9ms), and
    /// MongoDB's <c>$$NOW</c> is millisecond-resolution. A waiter that reuses a correlation id
    /// within one tick of the previous waiter's ack therefore registers at exactly
    /// <c>acked_at</c>, and a non-strict comparison hands it the response its predecessor already
    /// consumed. Registration is ordered strictly after that ack in real time and the clock is
    /// non-decreasing, so <c>acked_at &lt;= started_at</c> always holds for history and the strict
    /// form excludes it deterministically — not probabilistically.
    /// </para>
    /// <para>
    /// The same-tick equality is symmetric — it can also be a genuine cross-process fan-out
    /// delivery (this waiter registered and another process's claim stamped <c>acked_at</c>
    /// inside one clock tick) — and no timestamp can separate the two cases. The store's
    /// monotonic ack sequence arbitrates that tie (and only that tie): every delivery claim
    /// stamps <c>acked_seq</c> drawn from the same monotonic source the subscription drew
    /// <c>StartedSeq</c> from at registration. The arbitration is conservative-exact — exact
    /// whenever the claim's draw was not stalled across ticks; the stalled-draw residual below
    /// resolves as history.
    /// </para>
    /// <para>
    /// The sequence deliberately does NOT outrank truthful (unequal) timestamps. A claim's
    /// sequence value is drawn BEFORE the claim becomes visible — MongoDB draws from a separate
    /// counter document, SQL Server in a <c>DECLARE</c> ahead of an <c>UPDATE</c> that may block
    /// on a row lock, and even PostgreSQL's <c>nextval</c> evaluates before the commit — so a
    /// claim can draw <c>41</c>, stall, and land AFTER a waiter registered at <c>42</c>. Ranking
    /// the sequence above timestamps would exclude that delivery as history even though
    /// <c>acked_at</c> truthfully post-dates the registration tick. With timestamps primary, the
    /// stalled claim lands in a LATER tick and is delivered by the timestamp rule; the sequence
    /// is consulted only when the tick is identical.
    /// </para>
    /// <para>
    /// Inside the tie the sequence can never replay history: a claim visible before a same-tick
    /// registration drew its value before that visibility, hence before the registration's own
    /// draw — <c>acked_seq &lt; StartedSeq</c> — and is excluded. The only residual conservatism
    /// is a claim whose draw-to-execution stall ends exactly in the registration's tick: it
    /// resolves as history, which is the same verdict the timestamp-only rule gave every tie —
    /// never worse, and exact whenever draws are not stalled (the overwhelmingly common case).
    /// Rows acked by a pre-sequence build carry no <c>acked_seq</c> and keep the old at-most-once
    /// tie resolution (excluded fan-out recovers through its step timeout and the
    /// idempotent-restart contract).
    /// </para>
    /// </summary>
    private static bool IsWithinWatermark(IDbSubscription subscription, DbChannelMessage message)
    {
        if (message.CreatedAtUtc < subscription.StartedAtUtc.AddSeconds(-1))
            return false;

        if (message.AckedAtUtc is null)
            return true;

        // Timestamps are primary: strictly later tick = delivered, strictly earlier = history.
        if (message.AckedAtUtc > subscription.StartedAtUtc)
            return true;
        if (message.AckedAtUtc < subscription.StartedAtUtc)
            return false;

        // Same-tick tie: the monotonic ack sequence arbitrates when the claim carries one.
        if (message.AckedSeq is { } ackedSeq)
            return ackedSeq > subscription.StartedSeq;

        // Legacy tie (row acked by a pre-sequence build): the old conservative resolution.
        return false;
    }

    private async Task TryDispatchLocalSubscribersAsync(DbChannelMessage message, CancellationToken cancellationToken)
    {
        if (!_subscriptions.TryGetValue(message.CorrelationId, out var group))
            return;

        var subscriptions = new List<IDbSubscription>(group.Count);
        foreach (var subscription in group.Values)
        {
            if (!subscription.Dropped)
                subscriptions.Add(subscription);
        }
        if (subscriptions.Count == 0)
            return;

        // Same-process fast path: skips the wake round trip / sweep latency but still runs on the
        // per-correlation serial executor — completion predicates are guaranteed serial, in-order
        // invocation on every channel, and a direct dispatch here could otherwise run concurrently
        // with a sweep-enqueued dispatch of a different message for the same subscription. MarkSeen
        // keeps the sweep from double-processing this message.
        await _executors.EnqueueAsync(
            ChannelName(message.CorrelationId),
            new LocalDispatchWorkItem(this, message, subscriptions, cancellationToken).InvokeAsync,
            cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Registers an in-process delivery completion for a message id. Disposing it removes the entry,
    /// so a publish that throws or completes never leaks the registration.
    /// </summary>
    private protected PendingConfirmation BeginConfirmation(Guid messageId)
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
    private protected async Task<bool> TryConfirmDeliveryAsync(PendingConfirmation confirmation, CancellationToken cancellationToken)
    {
        if (await WaitForAcknowledgementAsync(confirmation, cancellationToken).ConfigureAwait(false))
            return true;

        return !await _store.TryClaimForRecoveryAsync(confirmation.MessageId, cancellationToken).ConfigureAwait(false);
    }

    private protected void SignalDispatcher(string? correlationId = null) => _signals.Writer.TryWrite(correlationId);

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
            if (await _store.IsMessageAcknowledgedAsync(confirmation.MessageId, cancellationToken).ConfigureAwait(false))
                return true;
        }

        return confirmation.Delivered.IsCompletedSuccessfully
            || await _store.IsMessageAcknowledgedAsync(confirmation.MessageId, cancellationToken).ConfigureAwait(false);
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

    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    private static void OnWaiterTimeout(object? state)
        => ((IWaiterTimeoutState)state!).Schedule();

    private async Task HandleWaiterTimeoutAsync<T>(
        DbSubscription<T> subscription,
        Activity? activity,
        string correlationId,
        TaskCompletionSource<T> tcs) where T : IAsyncResponsePayload
    {
        _logger.LogWarning("Timed out waiting for {Provider} response for correlationId {CorrelationId}.", _providerName, correlationId);
        AsyncResponseDiagnostics.SetError(activity, "timeout", $"Timed out waiting for response for correlationId {correlationId}.");
        AsyncResponseDiagnostics.RecordWaiterTimeout(_activityTag);
        tcs.TrySetException(new TimeoutException($"Timed out waiting for response for correlationId {correlationId}."));
        await subscription.DrainThenCleanupAsync(deleteRecoveryState: true).ConfigureAwait(false);
    }

    private interface IWaiterTimeoutState
    {
        void Schedule();
    }

    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    private sealed class WaiterTimeoutState<T>(
        DbAsyncResponseChannelBase owner,
        DbSubscription<T> subscription,
        Activity? activity,
        string correlationId,
        TaskCompletionSource<T> tcs) : IWaiterTimeoutState where T : IAsyncResponsePayload
    {
        public void Schedule()
            => _ = Task.Run(async () =>
            {
                try
                {
                    await owner.HandleWaiterTimeoutAsync(subscription, activity, correlationId, tcs).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    // Fire-and-forget: nothing awaits this task, so an escaped fault would vanish.
                    owner._logger.LogError(ex, "Error handling {Provider} waiter timeout for correlationId {CorrelationId}.", owner._providerName, correlationId);
                }
            });
    }

    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    private sealed class LocalDispatchWorkItem(
        DbAsyncResponseChannelBase owner,
        DbChannelMessage message,
        IReadOnlyList<IDbSubscription> subscriptions,
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
                    "Local {Provider} response dispatch failed for correlationId {CorrelationId}; {RetryHint}.",
                    owner._providerName,
                    message.CorrelationId,
                    owner._localDispatchRetryHint);
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
                await subscription.DrainThenCleanupAsync(deleteRecoveryState: false).ConfigureAwait(false);
            await _executors.RemoveAsync(ChannelName(correlationId)).ConfigureAwait(false);
        }
    }

    /// <summary>Scopes an in-process delivery completion; <see cref="Dispose"/> unregisters it.</summary>
    private protected readonly struct PendingConfirmation(
        DbAsyncResponseChannelBase owner,
        Guid messageId,
        TaskCompletionSource<bool> tcs) : IDisposable
    {
        public Guid MessageId => messageId;
        public Task<bool> Delivered => tcs.Task;
        public void Dispose() => owner._pendingConfirmations.TryRemove(messageId, out _);
    }

    private protected interface IDbSubscription
    {
        Guid Id { get; }
        DateTimeOffset StartedAtUtc { get; }
        long StartedSeq { get; }
        bool Dropped { get; }
        Func<DbChannelMessage, Task> ProcessUnderContextAsync { get; set; }
        bool HasSeen(Guid messageId);
        bool MarkSeen(Guid messageId);
        void PruneSeen(DateTimeOffset cutoffUtc);
        Task ProcessAsync(DbChannelMessage message);
        ValueTask CleanupOnceAsync(bool deleteRecoveryState);
        ValueTask DrainThenCleanupAsync(bool deleteRecoveryState);
        ValueTask DropLocalAsync(CancellationToken cancellationToken);
    }

    private sealed class DbSubscription<T> : IDbSubscription where T : IAsyncResponsePayload
    {
        private readonly DbAsyncResponseChannelBase _owner;
        private readonly string _correlationId;
        private readonly Func<T, ValueTask<bool>> _completionPredicate;
        private readonly TaskCompletionSource<T> _tcs;
        private readonly Activity? _activity;
        private readonly HashSet<Guid> _seen = [];
        private readonly Queue<(Guid Id, DateTimeOffset SeenAtUtc)> _seenOrder = [];
        private readonly object _seenGate = new();
        private int _cleanupStarted;
        private volatile bool _dropped;
        private readonly object _cleanupGate = new();
        private Task? _cleanupTask;

        public DbSubscription(
            DbAsyncResponseChannelBase owner,
            string correlationId,
            Guid registrationId,
            DateTimeOffset startedAtUtc,
            long startedSeq,
            Func<T, ValueTask<bool>> completionPredicate,
            TaskCompletionSource<T> tcs,
            Activity? activity)
        {
            _owner = owner;
            _correlationId = correlationId;
            Id = registrationId;
            StartedAtUtc = startedAtUtc;
            StartedSeq = startedSeq;
            _completionPredicate = completionPredicate;
            _tcs = tcs;
            _activity = activity;
            ProcessUnderContextAsync = ProcessAsync;
        }

        public Guid Id { get; }
        public DateTimeOffset StartedAtUtc { get; }
        public long StartedSeq { get; }
        public bool Dropped => _dropped;
        public Func<ValueTask>? TimeoutRegistration { get; set; }
        public CancellationTokenSource? TimeoutCancellation { get; set; }
        public Func<DbChannelMessage, Task> ProcessUnderContextAsync { get; set; }

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

        public async Task ProcessAsync(DbChannelMessage message)
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
                _owner._logger.LogError(ex, "Error processing {Provider} response for correlationId {CorrelationId}.", _owner._providerName, _correlationId);
                AsyncResponseDiagnostics.SetError(_activity, ex);
                _tcs.TrySetException(ex);
            }
            finally
            {
                if (finished)
                    await CleanupOnceAsync(deleteRecoveryState: true).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Task-latched so EVERY caller completes only when the one real cleanup has finished —
        /// a fire-once flag alone would let a disposing waiter racing the timeout return before
        /// the response task was settled.
        /// </summary>
        public ValueTask CleanupOnceAsync(bool deleteRecoveryState)
        {
            Task task;
            lock (_cleanupGate)
            {
                task = _cleanupTask ??= CleanupCoreAsync(deleteRecoveryState);
            }

            return task.IsCompletedSuccessfully ? ValueTask.CompletedTask : new ValueTask(task);
        }

        /// <summary>
        /// Dispose-path cleanup: DRAINS the per-correlation serial executor before settling. A
        /// delivery may be mid <c>Until</c>-predicate holding a message the claim already acked;
        /// the marker work item completes only after that in-flight item finished, so by the time
        /// cleanup cancels, the task is either settled by the delivery or genuinely undelivered —
        /// never a cancellation stealing a consumed response. Must NOT be called from dispatch
        /// code (which runs ON the executor): the dispatch-triggered cleanup calls
        /// <see cref="CleanupOnceAsync"/> directly, its task already settled.
        /// <para>
        /// The drain is bounded by <c>DisposalDrainTimeout</c> — a single budget covering marker
        /// ADMISSION too, since a full bounded queue behind a wedged item blocks the enqueue
        /// itself. A lapsed budget must not fall back to the cleanup's cancel: the wedged delivery
        /// holds a message the claim already consumed, and "canceled" would tell a re-attaching
        /// caller nothing was delivered. It faults the task with the explicit indeterminate
        /// contract instead, routing durable flows to a fresh idempotent restart. An enqueue
        /// suppressed by the registry's tombstone is the opposite case — the retired executor
        /// finished everything it ever admitted, so nothing is in flight and the plain cancel
        /// below is truthful.
        /// </para>
        /// </summary>
        public async ValueTask DrainThenCleanupAsync(bool deleteRecoveryState)
        {
            if (Volatile.Read(ref _cleanupStarted) == 0)
            {
                var drainTimeout = _owner._options.DisposalDrainTimeout;
                var drained = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                try
                {
                    using var budget = new CancellationTokenSource(drainTimeout);
                    var accepted = await _owner._executors.EnqueueAsync(_owner.ChannelName(_correlationId), () =>
                    {
                        drained.TrySetResult();
                        return Task.CompletedTask;
                    }, budget.Token).ConfigureAwait(false);
                    if (accepted)
                        await drained.Task.WaitAsync(budget.Token).ConfigureAwait(false);
                }
                catch (Exception drainEx)
                {
                    // Budget lapse — or an unforeseen drain failure: either way the marker never
                    // ran, so an in-flight delivery cannot be ruled out (only accepted=false
                    // proves the executor finished everything). Settlement unproven means the
                    // cleanup's cancel below would be a false "nothing was delivered" — fault
                    // with the explicit indeterminate contract instead. A TrySetResult from the
                    // late-finishing dispatch loses against this and is dropped; its cleanup
                    // call is a no-op behind the latch.
                    _owner._logger.LogWarning(
                        "Disposal drain for {Provider} correlationId {CorrelationId} did not prove settlement within {DrainTimeout}; faulting the waiter as indeterminate.",
                        _owner._providerName, _correlationId, drainTimeout);
                    AsyncResponseDiagnostics.SetError(_activity, "indeterminate_delivery", "Disposal drain did not prove settlement.");
                    if (drainEx is not OperationCanceledException)
                        _owner._logger.LogDebug(drainEx, "Dispatch drain failed for correlationId {CorrelationId}.", _correlationId);
                    _tcs.TrySetException(new AsyncResponseIndeterminateDeliveryException(_correlationId, drainTimeout));
                }
            }

            await CleanupOnceAsync(deleteRecoveryState).ConfigureAwait(false);
        }

        private async Task CleanupCoreAsync(bool deleteRecoveryState)
        {
            // The flag is kept alongside the task latch: dispatch cores and white-box tests gate
            // on it, and a pre-set flag (test isolation) must keep skipping the network cleanup.
            if (Interlocked.Exchange(ref _cleanupStarted, 1) != 0)
                return;

            // A waiter disposed before any terminal signal must not leave ResponseTask pending
            // forever for callers that hold it directly — the timeout dies with this cleanup, so
            // nothing else could ever complete the task. This also covers channel DisposeAsync at
            // host shutdown, which runs this cleanup over every in-flight subscription and would
            // otherwise hang still-awaiting WaitAsync callers. Cancellation is a no-op after a
            // normal completion, timeout, fault, or a delivery drained by DrainThenCleanupAsync.
            _tcs.TrySetCanceled();

            try
            {
                _dropped = true;

                try
                {
                    // Delete the recovery state BEFORE removing the subscription (locally and in the
                    // subscriber store). In the reverse order a publish landing in the window sees
                    // "no subscriber, state present" and fires a spurious recovery callback for a wait
                    // that already reached a terminal state. In this order the window shows a
                    // subscriber that drops the message — a late or duplicate terminal message is
                    // droppable; a resurrected recovery callback is not. (Shutdown/redeploy paths pass
                    // deleteRecoveryState: false and keep the state for lost-subscriber recovery.)
                    if (deleteRecoveryState)
                        await _owner._recoveryStateStore.TryDeleteAsync(_correlationId, Id).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    // Best-effort: the state expires on its own, and a transient store failure must
                    // not skip the local teardown below.
                    _owner._logger.LogError(ex, "Failed to delete {Provider} recovery state for correlationId {CorrelationId}.", _owner._providerName, _correlationId);
                }

                try
                {
                    await _owner._store.DeleteSubscriberAsync(_correlationId, Id, CancellationToken.None).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    // Best-effort: an orphaned subscriber record ages out via the heartbeat timeout.
                    _owner._logger.LogError(ex, "Failed to delete {Provider} subscriber {SubscriberRecord} for correlationId {CorrelationId}.", _owner._providerName, _owner._subscriberRecordNoun, _correlationId);
                }
            }
            finally
            {
                // Purely local teardown runs no matter which network call above failed — the
                // cleanup latch is already set, so a skipped removal would leak the subscription
                // map entry and the executor until process exit.
                _owner.RemoveSubscription(_correlationId, Id);

                // Schedule the executor retirement on the thread pool; do not await directly —
                // dispatch-loop deliveries run this cleanup ON the executor, and RemoveAsync waits
                // for the executor's drain loop to finish, which would be a circular await.
                var channelName = _owner.ChannelName(_correlationId);
                _ = Task.Run(async () =>
                {
                    try
                    {
                        await _owner._executors.RemoveAsync(channelName).ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        _owner._logger.LogError(ex, "Failed to retire the executor for channel {Channel}.", channelName);
                    }
                });

                if (TimeoutRegistration is not null)
                    await TimeoutRegistration().ConfigureAwait(false);
                TimeoutCancellation?.Dispose();
                _activity?.Dispose();
            }
        }

        public async ValueTask DropLocalAsync(CancellationToken cancellationToken)
        {
            _dropped = true;
            await _owner._store.DeleteSubscriberAsync(_correlationId, Id, cancellationToken).ConfigureAwait(false);
        }
    }
}
