using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System.Buffers;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
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
    //
    // Bounded but never lossy: a write that finds the channel full raises _fullSweepRequested
    // instead of evicting anything. DropOldest silently discarded the EARLIEST queued signals —
    // during one long pass a burst of wakes filled the channel, and the targeted wakes and
    // backpressure rescans it evicted then waited for the next throttled full sweep, past the
    // publisher's delivery-confirmation budget, so their responses were claimed for
    // lost-subscriber recovery under live waiters. A full sweep covers every id a refused signal
    // could have named.
    private readonly Channel<string?> _signals = Channel.CreateBounded<string?>(new BoundedChannelOptions(1024)
    {
        SingleReader = true,
        SingleWriter = false,
        FullMode = BoundedChannelFullMode.Wait
    });

    // 1 when a signal was refused by a full channel, or a targeted pass the breaker cut short; the
    // next dispatch pass then sweeps in full.
    private int _fullSweepRequested;

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

    // The listener CTS's token, captured when the loops start (a disposed CTS refuses .Token, the
    // captured struct stays usable): what the same-process fast path's work items run on, so they
    // stop with the channel rather than with whichever publisher queued them. None until then.
    private CancellationToken _dispatchToken;
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
        string localDispatchRetryHint,
        TimeProvider? timeProvider = null)
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
        _timeProvider = timeProvider ?? TimeProvider.System;
        _lostSubscriberDispatcher = new LostSubscriberCallbackDispatcher(scopeFactory, propagation, logger, _timeProvider);
        _executors = new SerialExecutorRegistry(logger, timeProvider: _timeProvider);
    }

    /// <summary>
    /// The engine's clock. Waiter timeouts and the delivery-confirmation wait arm on it rather
    /// than on the wall clock, so AsyncResponse.Testing's virtual clock can fire production-sized
    /// timeouts instantly here exactly as it already does on the in-memory channel — previously
    /// these were the only channels whose timeout paths a virtual-clock test could not reach.
    /// </summary>
    private protected readonly TimeProvider _timeProvider;

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

        // Clock-injected: CancelAfter on a default CTS is bound to the system clock, so a virtual
        // clock could never fire a production-sized waiter timeout on this channel.
        var timeoutCts = new CancellationTokenSource(Timeout.InfiniteTimeSpan, _timeProvider);
        CancellationTokenRegistration timeoutRegistration = default;
        subscription.TimeoutRegistration = () => timeoutRegistration.DisposeAsync();
        subscription.TimeoutCancellation = timeoutCts;

        timeoutRegistration = timeoutCts.Token.Register(
            OnWaiterTimeout,
            new WaiterTimeoutState<T>(this, subscription, activity, correlationId));

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
                // The SERVER-stamped subscription start, not the app clock. The watchdog judges
                // staleness as "utcNow - RegisteredAtUtc" from whichever host scans, so an
                // app-clock stamp made a skewed host's registrations either never age (skew
                // ahead: a genuinely stuck flow stays invisible) or age instantly (skew behind:
                // healthy waits page the operator). This is the same clock the delivery watermark
                // above is drawn from, and for the same reason.
                RegisteredAtUtc = startedAtUtc.UtcDateTime,
                Context = _propagation.Capture()
            };
            // Subscriber record BEFORE recovery state: "recovery state visible ⇒ subscription
            // visible" is the invariant the lost-subscriber dispatcher's live re-check relies on.
            // In the reverse order a publisher could see the state, see no subscriber, and consume
            // the registration while this waiter is milliseconds from being live.
            await _store.UpsertSubscriberAsync(correlationId, registrationId, _instanceId, _options.SubscriberHeartbeatTimeout, CancellationToken.None).ConfigureAwait(false);
            await _recoveryStateStore.SaveAsync(correlationId, recoveryState, _options.RecoveryStateExpiry).ConfigureAwait(false);

            if (_logger.IsEnabled(LogLevel.Debug))
            {
                SafeLog.Try(
                    (Logger: _logger, Provider: _providerName, CorrelationId: correlationId, Timeout: timeout.Value),
                    static state => state.Logger.LogDebug("Waiting for {Provider} response on correlationId {CorrelationId} with timeout {Timeout}.", state.Provider, state.CorrelationId, state.Timeout));
            }
        }
        catch (Exception ex)
        {
            // Clean up first, report second: a logging provider that throws (Microsoft.Extensions.
            // Logging rethrows a provider's failure) used to skip the cleanup below — leaving a
            // subscriber row publishers counted as a live waiter nobody holds, and the wait's
            // activity never ended — and replaced the failure the caller receives.
            AsyncResponseDiagnostics.SetError(activity, "subscribe_failure", ex.Message);
            await subscription.DrainThenCleanupAsync(deleteRecoveryState: true).ConfigureAwait(false);
            SafeLog.Try(
                (Logger: _logger, Error: ex, Provider: _providerName, CorrelationId: correlationId),
                static state => state.Logger.LogError(state.Error, "Failed to create {Provider} waiter for correlationId {CorrelationId}.", state.Provider, state.CorrelationId));

            // Rethrow instead of returning a pre-faulted waiter: the builder's contract is that
            // the trigger runs only once the subscription AND recovery state exist. A returned
            // waiter would still let the trigger fire the remote operation with no registration
            // left to receive (or recover) its response. Cleanup cancels the response task, so no
            // pending task is left behind.
            throw;
        }

        // Publish the subscription only once it is fully armed (heartbeat + context delegate),
        // then signal a scan targeted at this correlation id so any already-stored response is
        // delivered promptly without a full sweep.
        AddSubscription(correlationId, subscription);
        SignalDispatcher(correlationId);

        // Arm the waiter timeout only AFTER the subscription is discoverable (Redis/NATS parity):
        // a timer that fired before AddSubscription would run cleanup against a map that does not
        // hold the entry yet, and the insert above would then pin a permanently-dropped
        // subscription (plus its executor registration) that nothing can ever remove again.
        try
        {
            if (!subscription.CleanupStarted)
                timeoutCts.CancelAfter(timeout.Value);
        }
        catch (ObjectDisposedException)
        {
            // A response completed and cleaned up between the check and CancelAfter.
        }

        return CreateWaiter<T>(tcs.Task, () => subscription.DrainThenCleanupAsync(deleteRecoveryState: true));
    }

    /// <inheritdoc />
    public Task SetResponse<T>(T response, string correlationId, CancellationToken cancellationToken = default) where T : IAsyncResponsePayload
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
            var envelope = new AsyncResponseEnvelope<T> { Success = true, Payload = response };
            await PublishResponseWithRecoveryAsync(
                activity,
                correlationId,
                AsyncResponseEnvelopeJson.Serialize(envelope),
                typedResponse: response,
                rawResponseJson: null,
                cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            // SafeLog: a throwing logging provider must not replace the exception the caller
            // (the ingress's retry classification among them) is about to receive.
            SafeLog.Try(
                (Logger: _logger, Error: ex, Provider: _providerName, CorrelationId: correlationId),
                static state => state.Logger.LogError(state.Error, "Failed to publish {Provider} response for correlationId {CorrelationId}.", state.Provider, state.CorrelationId));
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
            await PublishResponseWithRecoveryAsync<object>(
                activity,
                correlationId,
                SerializeRawSuccessEnvelope(responseJson),
                typedResponse: null,
                rawResponseJson: responseJson,
                cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            SafeLog.Try(
                (Logger: _logger, Error: ex, Provider: _providerName, CorrelationId: correlationId),
                static state => state.Logger.LogError(state.Error, "Failed to publish {Provider} raw response for correlationId {CorrelationId}.", state.Provider, state.CorrelationId));
            AsyncResponseDiagnostics.SetError(activity, ex);
            throw;
        }
    }

    /// <summary>
    /// The publish-with-recovery protocol shared by <see cref="SetResponseCore{T}"/> and
    /// <see cref="SetRawResponseJsonCore"/>, which carried lockstep copies of it (and the raw
    /// copy parsed its body twice when the RetryLive branch fell through to a failed delivery
    /// confirmation). The recovery payload is <paramref name="typedResponse"/> when
    /// <paramref name="rawResponseJson"/> is null; otherwise the raw body is deserialized
    /// lazily — once — on the cold branches that dispatch to recovery, so the delivered-live
    /// path never parses it. Generic so the typed path hands the dispatcher the publisher's
    /// DECLARED <typeparamref name="TPayload"/> — erasing to <c>object</c> made the recovery
    /// wire form the RUNTIME-type serialization, diverging from the declared-type envelope this
    /// method just wrote (and from Redis/NATS/in-memory) whenever the runtime type carries
    /// members the declared contract does not.
    /// </summary>
    private async Task PublishResponseWithRecoveryAsync<TPayload>(
        Activity? activity,
        string correlationId,
        string envelopeJson,
        TPayload? typedResponse,
        string? rawResponseJson,
        CancellationToken cancellationToken)
    {
        object? rawRecoveryPayload = null;
        var rawRecoveryPayloadMaterialized = false;

        // One dispatch shape per payload source, so generic inference binds the declared type on
        // the typed path and object on the raw path — never object for both.
        Task<LostSubscriberDispatchResult> DispatchToRecoveryAsync(Func<ValueTask<bool>>? hasLiveSubscriber)
        {
            if (rawResponseJson is null)
            {
                return _lostSubscriberDispatcher.DispatchLostResponses(
                    _recoveryStateStore, correlationId, typedResponse, ChannelName(correlationId), cancellationToken, hasLiveSubscriber);
            }

            if (!rawRecoveryPayloadMaterialized)
            {
                rawRecoveryPayload = new RawJsonResponse(rawResponseJson).DeserializeUntyped();
                rawRecoveryPayloadMaterialized = true;
            }

            return _lostSubscriberDispatcher.DispatchLostResponses(
                _recoveryStateStore, correlationId, rawRecoveryPayload, ChannelName(correlationId), cancellationToken, hasLiveSubscriber);
        }

        var subscribers = await _store.CountActiveSubscribersAsync(correlationId, cancellationToken).ConfigureAwait(false);
        activity?.SetTag("asyncresponse.subscribers", subscribers);
        if (subscribers <= 0 && !HasLocalLiveSubscription(correlationId))
        {
            var dispatchResult = await DispatchToRecoveryAsync(
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
        await PublishMessageAsync(messageId, correlationId, envelopeJson, cancellationToken).ConfigureAwait(false);

        if (!await TryConfirmDeliveryAsync(confirmation, cancellationToken).ConfigureAwait(false))
        {
            var dispatchResult = await DispatchToRecoveryAsync(hasLiveSubscriber: null).ConfigureAwait(false);
            AsyncResponseDiagnostics.SetLostSubscriberRoute(activity, dispatchResult.Action, dispatchResult.RouteMixed);
            AsyncResponseDiagnostics.RecordLostSubscriber("response", dispatchResult.Action, dispatchResult.CallbackInvoked, dispatchResult.RouteMixed);
            activity?.SetTag("asyncresponse.recovery.callback_invoked", dispatchResult.CallbackInvoked);
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
            if (subscribers <= 0 && !HasLocalLiveSubscription(correlationId))
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
            SafeLog.Try(
                (Logger: _logger, Error: ex, Provider: _providerName, CorrelationId: correlationId),
                static state => state.Logger.LogError(state.Error, "Failed to publish {Provider} exception response for correlationId {CorrelationId}.", state.Provider, state.CorrelationId));
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
            SafeLog.Try(
                (Logger: _logger, Error: ex, Provider: _providerName, CorrelationId: correlationId),
                static state => state.Logger.LogDebug(state.Error, "Failed to count {Provider} subscribers for correlationId {CorrelationId}.", state.Provider, state.CorrelationId));
            // Negative = "could not be probed" (the watchdog's documented unknown-liveness
            // contract): returning 0 would assert there is definitively no live waiter, flagging
            // every over-threshold registration stale during a transient probe outage.
            return -1L;
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
    /// using the same liveness rule the publish path consulted: a live local subscription, or
    /// the store's active-subscriber count.
    /// </summary>
    private async ValueTask<bool> HasLiveSubscriberAsync(string correlationId, CancellationToken cancellationToken)
        => HasLocalLiveSubscription(correlationId)
           || await _store.CountActiveSubscribersAsync(correlationId, cancellationToken).ConfigureAwait(false) > 0;

    /// <summary>
    /// Whether THIS process holds a live (not dropped) subscription for the id — proof of a live
    /// waiter whatever the subscriber store says. The store's row lapses once heartbeats have
    /// failed for longer than the heartbeat timeout (a database outage), and a publish in that
    /// window counted no subscriber, consumed the registration and fired the recovery callback
    /// under a waiter still waiting in this very process. Publishing live instead hands the
    /// response to it through the same-process fast path.
    /// </summary>
    private bool HasLocalLiveSubscription(string correlationId)
    {
        if (!_subscriptions.TryGetValue(correlationId, out var group))
            return false;

        foreach (var subscription in group.Values)
        {
            if (!subscription.Dropped)
                return true;
        }

        return false;
    }

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

    // Executor retirements started off the cleanup path (see CleanupCoreAsync). Keyed by the task
    // itself and self-evicting, so a long-lived channel never accumulates completed entries.
    private readonly ConcurrentDictionary<Task, byte> _pendingRetirements = new();

    private void TrackRetirement(Task retirement)
    {
        _pendingRetirements[retirement] = 0;
        _ = retirement.ContinueWith(
            static (completed, state) => ((ConcurrentDictionary<Task, byte>)state!).TryRemove(completed, out _),
            _pendingRetirements,
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);
    }

    /// <summary>
    /// The effective minimum interval between full safety-net sweeps, <c>null</c> to sweep on
    /// every poll tick. Defaults to the configured <c>FullSweepInterval</c>; a provider overrides
    /// it when its push wake is not carrying delivery, because the throttled sweep is then the
    /// ONLY cross-process wake and a throttle equal to the delivery-confirmation timeout routed
    /// live waiters' responses into lost-subscriber recovery.
    /// </summary>
    protected virtual TimeSpan? CurrentFullSweepInterval() => _options.FullSweepInterval;

    /// <summary>
    /// The full-sweep interval while a provider's push wake is expected but not established (a
    /// <c>LISTEN</c> or change stream that is down or reconnecting):
    /// <c>min(FullSweepInterval, DeliveryConfirmationTimeout / 4)</c>, or every tick when
    /// <c>FullSweepInterval</c> is null.
    /// <para>
    /// The sweep is then the only cross-process wake, so the regular throttle — whose 5 s default
    /// equals the publisher's confirmation budget — would let responses be claimed for
    /// lost-subscriber recovery under live waiters. But sweeping on EVERY tick held up to
    /// <see cref="FullSweepParallelism"/> pooled connections back to back for as long as the
    /// wake stayed down, which is exactly when the database (an outage, an exhausted pool — the
    /// conditions that drop the wake) can least afford it, and the listener reconnecting through
    /// the same pool competed with it. A quarter of the confirmation budget bounds the wait for
    /// the next sweep and leaves three quarters for the sweep itself (which warns past half the
    /// budget) and the delivery claim; on defaults that is one sweep per 1.25 s instead of per
    /// 250 ms tick.
    /// </para>
    /// </summary>
    private protected TimeSpan? WakeDownFullSweepInterval()
    {
        if (_options.FullSweepInterval is not { } configured)
            return null;

        var floor = _options.DeliveryConfirmationTimeout / 4;
        return floor < configured ? floor : configured;
    }

    /// <summary>The longest reconnect backoff of a wake listener (the PostgreSQL <c>LISTEN</c> loop, the MongoDB change-stream loop).</summary>
    private protected static readonly TimeSpan WakeListenerMaxRetryDelay = TimeSpan.FromSeconds(5);

    /// <summary>
    /// How long an established wake listener must stay up before its failure starts a new run of
    /// reconnect backoff instead of continuing the current one: the backoff's cap, the healthy-run
    /// rule <c>SubscriberSupervisor</c> applies to transport subscribers. Resetting on every
    /// successful connect removed the backoff's bound on a server or proxy that accepts the
    /// <c>LISTEN</c> (or opens the stream) and then drops the session — a crash-looping server, a
    /// short <c>idle_session_timeout</c>: it reconnected every 50–100 ms for good, each time
    /// requesting an immediate full sweep and logging a warning. Never resetting escalated every
    /// flap of a long-lived process toward the cap. A field only so a test can shorten it.
    /// </summary>
    private protected readonly TimeSpan _wakeListenerHealthyRun = WakeListenerMaxRetryDelay;

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
            _dispatchToken = listenerCts.Token;
            _listenTask = StartWakeListener(listenerCts.Token);
            _dispatchTask = Task.Run(() => DispatchLoopAsync(listenerCts.Token));
            _heartbeatTask = Task.Run(() => HeartbeatLoopAsync(listenerCts.Token));
        }
    }

    // The REAL clock, deliberately — here, in the dispatch loop's poll and rescan delays, in the
    // history-reconciliation interval, in the late-commit lookback window and in seen-set aging —
    // although waiter timeouts and the delivery-confirmation wait arm on _timeProvider. These
    // loops keep pace with state that lives in the database and moves in real time whatever clock
    // the process was handed: subscriber rows expire on the SERVER's clock, and another process's
    // response becomes visible when ITS transaction commits. A heartbeat parked on a virtual clock
    // that a test never advances lets the rows of live waiters expire server-side (their responses
    // then route to lost-subscriber recovery), and a parked poll never delivers a cross-process
    // response at all. What the injected clock owns is time the process itself defines.
    private async Task HeartbeatLoopAsync(CancellationToken cancellationToken)
    {
        // Consecutive failed rounds. A failed round is retried on a short backoff (capped at one
        // second, and never later than the regular interval) instead of a full interval: the
        // subscriber rows lapse SubscriberHeartbeatTimeout after the last successful round, so
        // across a database outage longer than that every row expires, and a loop parked in a
        // full-interval delay then left a window of up to one interval after the database came
        // back in which every publish saw no live subscriber and routed live waiters' responses
        // to lost-subscriber recovery. Same shape as the DB transports' lease-renewal retry.
        var failures = 0;
        // Stopwatch timestamp of the last failure logged at Warning. The retry runs about once a
        // second through an outage, and a Warning per retry was ten times the volume of the old
        // full-interval cadence for no extra information: the first failure of a run warns, later
        // ones at most once per heartbeat interval, the rest at Debug.
        long? lastFailureWarningAt = null;
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(failures == 0 ? _options.SubscriberHeartbeatInterval : HeartbeatRetryDelay(failures), cancellationToken).ConfigureAwait(false);
                var registrations = SnapshotActiveRegistrations();
                if (registrations.Count == 0)
                    failures = 0;
                else
                {
                    try
                    {
                        await _store.HeartbeatSubscribersAsync(
                            _instanceId,
                            registrations,
                            _options.SubscriberHeartbeatTimeout,
                            cancellationToken).ConfigureAwait(false);
                        failures = 0;
                    }
                    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                    {
                        return;
                    }
                    catch (Exception ex)
                    {
                        failures++;
                        // The round still compensates below: SQL Server commits per-batch,
                        // MongoDB bulk-writes unordered, and any provider can fail after some
                        // upserts landed — a registration dropped mid-round may already be
                        // resurrected even though the round as a whole threw. Skipping the
                        // re-check on failure left exactly those rows phantom until TTL.
                        //
                        // SafeLog on every log line of this loop: a logging provider that throws
                        // skipped the compensation below and then ended the loop in the outer
                        // catch, whose own warning threw too — the subscriber rows of every live
                        // waiter then lapsed and their responses routed to lost-subscriber
                        // recovery until the process restarted.
                        if (failures == 1
                            || lastFailureWarningAt is not { } warnedAt
                            || Stopwatch.GetElapsedTime(warnedAt) >= _options.SubscriberHeartbeatInterval)
                        {
                            lastFailureWarningAt = Stopwatch.GetTimestamp();
                            SafeLog.Try(
                                (Logger: _logger, Error: ex, Provider: _providerName, Failures: failures),
                                static state => state.Logger.LogWarning(
                                    state.Error,
                                    "{Provider} subscriber heartbeat failed ({ConsecutiveFailures} consecutive); retrying for all local waiters.",
                                    state.Provider, state.Failures));
                        }
                        else
                        {
                            SafeLog.Try(
                                (Logger: _logger, Error: ex, Provider: _providerName, Failures: failures),
                                static state => state.Logger.LogDebug(
                                    state.Error,
                                    "{Provider} subscriber heartbeat retry {ConsecutiveFailures} failed; retrying for all local waiters (warnings are limited to one per heartbeat interval).",
                                    state.Provider, state.Failures));
                        }
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
                // Distinct from the inner catch's message, which reports a failed store upsert
                // that the drop compensation below it still runs. Reaching HERE means the round's
                // own bookkeeping broke — the snapshot, or the compensating deletes — so subscriber
                // rows dropped mid-round stay resurrected and suppress lost-subscriber recovery for
                // their correlation ids until the heartbeat timeout. Different cause, different
                // operator response, so it must not read identically.
                SafeLog.Try(
                    (Logger: _logger, Error: ex, Provider: _providerName),
                    static state => state.Logger.LogWarning(
                        state.Error,
                        "{Provider} subscriber heartbeat round failed outside the store upsert (snapshot or drop compensation); subscriber records dropped during this round may linger until the heartbeat timeout.",
                        state.Provider));
            }
        }
    }

    private TimeSpan HeartbeatRetryDelay(int failures)
    {
        var backoff = AsyncResponseRetry.Backoff(failures, HeartbeatRetryBaseDelay, HeartbeatRetryMaxDelay);
        return backoff < _options.SubscriberHeartbeatInterval ? backoff : _options.SubscriberHeartbeatInterval;
    }

    private static readonly TimeSpan HeartbeatRetryBaseDelay = TimeSpan.FromMilliseconds(100);
    private static readonly TimeSpan HeartbeatRetryMaxDelay = TimeSpan.FromSeconds(1);

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

            SafeLog.Try(
                (Logger: _logger, Provider: _providerName, RegistrationId: registrationId, CorrelationId: correlationId),
                static state => state.Logger.LogDebug(
                    "Deleting {Provider} subscriber {RegistrationId} for correlationId {CorrelationId}: it was dropped while a heartbeat round was in flight.",
                    state.Provider, state.RegistrationId, state.CorrelationId));
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
                SafeLog.Try(
                    (Logger: _logger, Error: ex, Provider: _providerName, Record: _subscriberRecordNoun, CorrelationId: correlationId),
                    static state => state.Logger.LogError(state.Error,
                        "Failed to delete {Provider} subscriber {SubscriberRecord} for correlationId {CorrelationId} after its heartbeat-round drop; it ages out via the heartbeat timeout.",
                        state.Provider, state.Record, state.CorrelationId));
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
                // SafeLog: this one loop serves every correlation id of the channel, and a logging
                // provider that throws used to end it here for good — every live waiter then timed
                // out while publishers' confirmations lapsed into lost-subscriber recovery.
                SafeLog.Try(
                    (Logger: _logger, Error: ex, Provider: _providerName),
                    static state => state.Logger.LogWarning(state.Error, "{Provider} response dispatch loop failed; retrying after poll delay.", state.Provider));
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
    /// <para>
    /// The poll deadline is ABSOLUTE and judged after either wake source. It used to be a fresh
    /// <c>Task.Delay</c> per pass that only counted when it won the race, so a steady stream of
    /// targeted signals cancelled every delay and the full sweep never ran: a response published
    /// from another process with no local signal — every cross-process response on SQL Server,
    /// any missed or dropped notification elsewhere — sat undelivered for as long as unrelated
    /// local traffic continued.
    /// </para>
    /// </summary>
    private protected async Task<HashSet<string>?> CollectDispatchScopeAsync(CancellationToken cancellationToken)
    {
        // Armed on the first pass rather than at construction: the loop starts lazily, and a
        // deadline measured from the constructor would already be overdue by then.
        _pollArmedAt ??= Stopwatch.GetTimestamp();

        var signalled = false;
        var untilPoll = CurrentPollInterval() - Stopwatch.GetElapsedTime(_pollArmedAt.Value);
        var pollDue = untilPoll <= TimeSpan.Zero;
        if (!pollDue)
        {
            // The WhenAny loser is cancelled via the per-iteration linked source: an abandoned
            // WaitToReadAsync would otherwise stay parked in the channel's blocked-reader list until
            // the next signal — one per poll interval, accumulating without bound on an idle channel.
            using var iteration = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            var delay = Task.Delay(untilPoll, iteration.Token);
            var signal = _signals.Reader.WaitToReadAsync(iteration.Token).AsTask();
            var completed = await Task.WhenAny(delay, signal).ConfigureAwait(false);
            iteration.Cancel();
            if (completed == signal)
            {
                await signal.ConfigureAwait(false);
                signalled = true;
            }
            else
            {
                // The timer is the authority for its own tick: it may fire a hair before the
                // stopwatch agrees, and that tick must not degrade into an empty pass.
                pollDue = true;
            }
        }

        // A signal does not excuse the poll. Re-read the interval while judging it: a signal from
        // a new waiter is what re-arms SQL Server's tight active cadence, and that waiter's first
        // poll must not wait out the idle interval.
        if (pollDue || Stopwatch.GetElapsedTime(_pollArmedAt.Value) >= CurrentPollInterval())
        {
            _pollArmedAt = Stopwatch.GetTimestamp();

            // The timer sweep costs one store query per subscribed correlation id, so with W
            // waiters an idle channel pays W queries per poll tick. FullSweepInterval bounds that:
            // a tick whose sweep is not yet due scans only what was signalled (possibly nothing).
            // Provider-resolved: a provider whose push wake is off or unavailable has no other
            // cross-process delivery path and must sweep every tick.
            if (CurrentFullSweepInterval() is not { } fullSweepInterval
                || _lastFullSweepAt is not { } lastFullSweepAt
                || Stopwatch.GetElapsedTime(lastFullSweepAt) >= fullSweepInterval
                || IsRequestedSweepRetryDue(lastFullSweepAt))
            {
                // Queued signals stay queued: the sweep covers their correlation ids, and the
                // next pass re-scans them as a cheap targeted scope instead of this pass having
                // to reason about signals written while the sweep was running. A refused signal
                // is covered by this sweep too, and so is a request the breaker left unfinished.
                _lastFullSweepAt = Stopwatch.GetTimestamp();
                _fullSweepServesRequest = Interlocked.Exchange(ref _fullSweepRequested, 0) != 0 || _requestedSweepRetryArmed;
                _requestedSweepRetryArmed = false;
                return null;
            }
        }

        var scope = new HashSet<string>(StringComparer.Ordinal);
        var fullSweep = false;
        for (var read = 0; read < MaxSignalsPerPass && _signals.Reader.TryRead(out var correlationId); read++)
        {
            if (string.IsNullOrEmpty(correlationId))
                fullSweep = true;
            else
                scope.Add(correlationId);
        }

        // Judged after the drain: a signal refused while the channel was full named an id that
        // is in none of the entries just read, so only a full sweep is sure to cover it.
        if (Interlocked.Exchange(ref _fullSweepRequested, 0) != 0)
            fullSweep = true;

        if (fullSweep || (signalled && scope.Count == 0))
        {
            // A signal-driven full sweep does the timer sweep's work; stamping it defers the next
            // timer sweep by a full interval instead of re-scanning everything twice in a row.
            _lastFullSweepAt = Stopwatch.GetTimestamp();
            _fullSweepServesRequest = true;
            _requestedSweepRetryArmed = false;
            return null;
        }

        return scope.Count == 0 ? EmptyDispatchScope : scope;
    }

    /// <summary>
    /// Whether a requested full sweep the outage breaker cut short is due again: at the wake-down
    /// floor (<see cref="WakeDownFullSweepInterval"/>) after it, never sooner.
    /// <para>
    /// A requested sweep carries work nothing else will come back for — the signals a full channel
    /// refused, the consumed wakes of a targeted pass the breaker stopped, the notifications
    /// published while no push wake was up — so leaving its unvisited ids to the regular throttle
    /// (5 s by default while the push wake is up, the publisher's whole confirmation budget) let
    /// their responses be claimed for lost-subscriber recovery under live waiters. Requesting it
    /// again outright made the next poll tick sweep in full, and every tick after it while the
    /// outage lasted: up to 8 connection attempts per 250 ms tick, past both sweep throttles, on
    /// a database that was down or out of connections. The floor is the cadence the channel
    /// already sweeps at whenever its push wake is down; a sweep that trips again is retried at
    /// it again, and one that does not trip ends the retry.
    /// </para>
    /// </summary>
    private bool IsRequestedSweepRetryDue(long lastFullSweepAt)
        => _requestedSweepRetryArmed
           && WakeDownFullSweepInterval() is { } floor
           && Stopwatch.GetElapsedTime(lastFullSweepAt) >= floor;

    /// <summary>Returned for a poll tick whose full sweep is not yet due: scan nothing. Never mutated.</summary>
    private static readonly HashSet<string> EmptyDispatchScope = [];

    /// <summary>
    /// Most signals one pass folds into its scope. The channel holds this many, so a pass still
    /// takes everything that was queued when it started; the bound only stops it from chasing
    /// writers that refill the channel as fast as it drains, which would keep the dispatch — and
    /// the poll deadline behind it — waiting on the drain.
    /// </summary>
    private const int MaxSignalsPerPass = 1024;

    // Stopwatch timestamps, not wall-clock stamps: both are interval deadlines, and a system
    // clock stepping backwards must not postpone a sweep. Touched only by the dispatch loop.
    private long? _pollArmedAt;
    private long? _lastFullSweepAt;

    // Whether the full sweep CollectDispatchScopeAsync last returned serves a request (a null
    // signal, _fullSweepRequested, or the retry below) rather than only the timer; and whether a
    // requested sweep the breaker cut short is waiting for its retry (IsRequestedSweepRetryDue).
    // Touched only by the dispatch loop.
    private bool _fullSweepServesRequest;
    private bool _requestedSweepRetryArmed;

    private protected async Task DispatchPendingMessagesAsync(HashSet<string>? scope, CancellationToken cancellationToken)
    {
        // Per-correlation isolation: one id's failure (a transient fault, a poisoned row) used to
        // abort the pass at that id, in the same order every time, so every id enumerated after
        // it lost its delivery on every pass until the failing waiter went away. Each id is now
        // dispatched on its own, and the pass still reports a failure afterwards so the loop logs
        // it and backs off as before.
        //
        // Isolation must not turn an outage into one failing store call per waiter per pass,
        // though: SweepFailures trips a breaker once the first ids of a pass all failed
        // transiently (the store is down, not one id poisoned), the rest of the pass is left to
        // the next one, and the reported exception carries only the first few failures.
        if (scope is not null)
        {
            // The not-yet-due poll tick: nothing to scan, and nothing to allocate for it.
            if (scope.Count == 0)
                return;

            // A publish signals exactly one correlation id, so a targeted scan must cost
            // O(scope), not O(live waiters): enumerating the whole registry made every publish
            // quadratic under load, and the not-yet-due poll tick (an empty scope) paid the same
            // walk to match nothing. The failure record is allocated by the first failure (this
            // runs on every publish and registration), so the settle order is counted here until then.
            SweepFailures? failures = null;
            var attempted = 0;
            var settled = 0;
            foreach (var correlationId in scope)
            {
                attempted++;
                if (!_subscriptions.TryGetValue(correlationId, out var group))
                    continue;

                var (queried, failure) = await TryDispatchPendingCorrelationAsync(correlationId, group, cancellationToken).ConfigureAwait(false);
                // Every subscription dropped (waiters mid-cleanup): nothing was asked of the store,
                // so the outcome says nothing about it and takes no place in the breaker's wave.
                if (!queried)
                    continue;

                settled++;
                if (failure is null && failures is null)
                    continue;

                failures ??= new SweepFailures(settledBefore: settled - 1);
                if (failures.Record(correlationId, failure))
                {
                    // These ids' signals are consumed: only a full sweep is sure to come back
                    // for the ones this pass skips.
                    Interlocked.Exchange(ref _fullSweepRequested, 1);
                    break;
                }
            }

            if (failures is { Tripped: false })
                RescanTransientlyFailed(failures, cancellationToken);
            failures?.ThrowIfAny(_providerName, notAttempted: scope.Count - attempted);
            return;
        }

        // The full sweep costs at least one store query per subscribed correlation id. Walked
        // sequentially its duration grew as waiters × round trip, and past the publisher's
        // delivery-confirmation budget every response only this sweep delivers (every
        // cross-process response on SQL Server; a lost wake elsewhere) was claimed for
        // lost-subscriber recovery under its live waiter. Distinct ids share no scan state (each
        // group owns its cursor, the seen sets lock, admission is non-blocking) and each id's own
        // executor keeps its delivery order, so a bounded number run side by side.
        var startedAt = Stopwatch.GetTimestamp();
        var servesRequest = _fullSweepServesRequest;
        _fullSweepServesRequest = false;
        var visited = 0;
        var sweepFailures = new SweepFailures();
        // The breaker stops only the scheduling of further ids; the ids already in flight finish
        // on the loop's own token.
        using var breaker = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        try
        {
            await Parallel.ForEachAsync(
                _subscriptions,
                new ParallelOptions { MaxDegreeOfParallelism = FullSweepParallelism, CancellationToken = breaker.Token },
                async (entry, _) =>
                {
                    // Judged per id as well as through the token: a worker that took its next id
                    // just as the breaker tripped leaves it alone.
                    if (sweepFailures.Tripped)
                        return;
                    var (queried, failure) = await TryDispatchPendingCorrelationAsync(entry.Key, entry.Value, cancellationToken).ConfigureAwait(false);
                    // No live subscription, no store call: see the targeted pass above. Such an id
                    // settles at once while failing ones wait out a connect timeout, so counted as
                    // a success it landed early in the first wave and kept the breaker from ever
                    // tripping during the outage it exists for.
                    if (!queried)
                        return;
                    Interlocked.Increment(ref visited);
                    if (sweepFailures.Record(entry.Key, failure))
                        breaker.Cancel();
                }).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (sweepFailures.Tripped && !cancellationToken.IsCancellationRequested)
        {
            // Tripped: the unvisited ids wait for a later sweep, reported below.
        }

        // An idle channel keeps ticking over an empty map; only sweeps that visited a waiter
        // count — and one with any failure measured the failure (a connect or server-selection
        // timeout per failed id), not the sweep, whether or not it tripped: recorded, it advised
        // an operator in a database outage to reduce the number of waiters.
        if (visited > 0 && !sweepFailures.HasFailures)
            ReportFullSweep(Stopwatch.GetElapsedTime(startedAt), visited);
        if (sweepFailures.Tripped)
        {
            // The ids this sweep did not reach: a timer sweep's wait for the next one, but a
            // requested sweep's are retried at the wake-down floor (IsRequestedSweepRetryDue).
            if (servesRequest)
                _requestedSweepRetryArmed = true;
        }
        else
            RescanTransientlyFailed(sweepFailures, cancellationToken);
        sweepFailures.ThrowIfAny(_providerName, notAttempted: sweepFailures.Tripped ? Math.Max(0, _subscriptions.Count - Volatile.Read(ref visited)) : 0);
    }

    /// <summary>
    /// One correlation id's pass; returns its failure instead of throwing it (cancellation of the
    /// loop still propagates), and whether it queried the store at all — <c>false</c> when every
    /// subscription of the id is already dropped.
    /// </summary>
    private async Task<(bool Queried, Exception? Failure)> TryDispatchPendingCorrelationAsync(
        string correlationId,
        ConcurrentDictionary<Guid, IDbSubscription> group,
        CancellationToken cancellationToken)
    {
        try
        {
            return (await DispatchPendingCorrelationAsync(correlationId, group, cancellationToken).ConfigureAwait(false), null);
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
        {
            return (true, ex);
        }
    }

    /// <summary>
    /// Schedules a rescan of every id whose pass failed transiently in a pass the breaker did not
    /// trip. That pass consumed the id's wake — a NOTIFY, a change event, a local publish — and a
    /// failure left nothing to come back for it but the next full sweep: with the push wake up,
    /// <c>FullSweepInterval</c> (5 s by default, the publisher's whole confirmation budget) away,
    /// so a response behind one dead pooled connection after a failover was claimed for
    /// lost-subscriber recovery under its live waiter. A tripped pass is covered by the full
    /// sweep it requests; a non-transient failure (a poisoned row) would only fail again.
    /// </summary>
    private void RescanTransientlyFailed(SweepFailures failures, CancellationToken cancellationToken)
    {
        foreach (var correlationId in failures.TransientlyFailedIds)
            ScheduleFailureRescan(correlationId, cancellationToken);
    }

    /// <summary>
    /// The per-correlation outcomes of one dispatch pass: reported as one exception once the pass
    /// is over, and a breaker that trips when the first <see cref="FullSweepParallelism"/> ids to
    /// settle all failed with a transient store fault.
    /// <para>
    /// Per-id isolation made a database outage cost one failing store call (a connection attempt,
    /// up to its timeout) per subscribed correlation id per pass, and one exception per id in the
    /// Warning the loop logs — at 1,000 waiters, megabytes of stack traces every poll tick where
    /// the pre-isolation loop had logged one. A first wave in which every id failed transiently
    /// says the store is down, not that one id is poisoned: the rest of the pass is skipped, and only
    /// the first <see cref="MaxReportedFailures"/> failures travel with the exception. A poisoned
    /// id or a non-transient fault never trips it, so the isolation still holds for those.
    /// </para>
    /// </summary>
    private sealed class SweepFailures(int settledBefore = 0)
    {
        private readonly List<Exception> _reported = [];
        private int _count;
        // Ids that settled before this record was created (the targeted pass allocates it on its
        // first failure), so the breaker still judges the first ids to settle.
        private int _settled = settledBefore;
        private int _leadingTransientFailures;
        private volatile bool _tripped;
        private List<string>? _transientlyFailed;

        public bool Tripped => _tripped;

        /// <summary>Whether any settled id failed, tripped or not.</summary>
        public bool HasFailures
        {
            get { lock (_reported) return _count > 0; }
        }

        /// <summary>The ids whose pass failed with a transient store fault; read once the pass is over.</summary>
        public IReadOnlyList<string> TransientlyFailedIds
        {
            get { lock (_reported) return _transientlyFailed?.ToArray() ?? []; }
        }

        /// <summary>
        /// Records one settled id that queried the store (<c>null</c> = dispatched); <c>true</c>
        /// when this outcome tripped the breaker.
        /// </summary>
        public bool Record(string correlationId, Exception? failure)
        {
            // One lock for the settle order AND the trip decision: no id can settle between the
            // wave's last failure and the trip, so an id that starts after it sees Tripped, and at
            // most one id per other worker is still in flight when it trips.
            lock (_reported)
            {
                // Settle order, not start order: the breaker judges the first ids to come back.
                var order = ++_settled;
                if (failure is null)
                    return false;

                _count++;
                if (_reported.Count < MaxReportedFailures)
                    _reported.Add(failure);

                var transient = DbChannelStore.IsTransient(failure);
                if (transient)
                    (_transientlyFailed ??= []).Add(correlationId);

                // Reaching the wave size counts only transient failures among the first ids to
                // settle, so it means every one of them failed transiently — a success or a
                // non-transient failure in that wave leaves the count short for good.
                if (_tripped
                    || order > FullSweepParallelism
                    || !transient
                    || ++_leadingTransientFailures < FullSweepParallelism)
                {
                    return false;
                }

                _tripped = true;
                return true;
            }
        }

        public void ThrowIfAny(string providerName, int notAttempted)
        {
            lock (_reported)
            {
                if (_count == 0)
                    return;
                if (_count == 1 && !_tripped)
                    ExceptionDispatchInfo.Capture(_reported[0]).Throw();

                var attached = _count > _reported.Count
                    ? $" (the first {_reported.Count} attached, and {_count - _reported.Count} more)"
                    : string.Empty;
                var rest = Tripped
                    ? $"; the first {FullSweepParallelism} to settle all failed transiently, so the store looks unavailable and the remaining {notAttempted} were left to a later pass"
                    : "; every other id was still dispatched";
                throw new AggregateException($"The {providerName} dispatch pass failed for {_count} correlation ids{attached}{rest}.", _reported);
            }
        }
    }

    /// <summary>Most per-correlation failures one pass's exception carries; the message counts the rest.</summary>
    private const int MaxReportedFailures = 3;

    /// <summary>Most correlation ids one full sweep dispatches concurrently (each holds one store connection while it runs).</summary>
    private const int FullSweepParallelism = 8;

    private static readonly Histogram<double> FullSweepDuration = AsyncResponseDiagnostics.Meter.CreateHistogram<double>(
        "asyncresponse.channel.sweep.duration",
        unit: "s",
        description: "Duration of one database-channel full dispatch sweep over every subscribed correlation id. A sweep approaching DeliveryConfirmationTimeout lets cross-process responses be claimed for lost-subscriber recovery under live waiters.");

    // Stopwatch timestamp of the last slow-sweep warning; touched only by the dispatch loop.
    private long? _lastSlowSweepWarningAt;

    private void ReportFullSweep(TimeSpan duration, int correlationIds)
    {
        FullSweepDuration.Record(duration.TotalSeconds, new KeyValuePair<string, object?>("asyncresponse.channel", _activityTag));

        // Half the confirmation budget: past it, a response the sweep reaches last is claimed for
        // recovery before its waiter is handed it. Rate-limited — a persistently slow sweep runs
        // every poll tick on SQL Server, and one line a minute says as much as one per tick.
        if (duration <= _options.DeliveryConfirmationTimeout / 2
            || (_lastSlowSweepWarningAt is { } lastWarning && Stopwatch.GetElapsedTime(lastWarning) < SlowSweepWarningInterval))
        {
            return;
        }

        _lastSlowSweepWarningAt = Stopwatch.GetTimestamp();
        SafeLog.Try(
            (Logger: _logger, Provider: _providerName, Count: correlationIds, Duration: duration, Budget: _options.DeliveryConfirmationTimeout),
            static state => state.Logger.LogWarning(
                "{Provider} full dispatch sweep over {CorrelationIdCount} correlation ids took {Duration}, more than half the {DeliveryConfirmationTimeout} delivery-confirmation timeout. " +
                "Cross-process responses only the sweep delivers can be claimed for lost-subscriber recovery under live waiters; reduce the number of concurrent waiters per process or raise DeliveryConfirmationTimeout.",
                state.Provider, state.Count, state.Duration, state.Budget));
    }

    private static readonly TimeSpan SlowSweepWarningInterval = TimeSpan.FromMinutes(1);

    // The group owns its scan progress: removing the last subscription also makes the cursor
    // collectible, without another per-correlation registry or a cleanup race on reused ids.
    private readonly ConditionalWeakTable<ConcurrentDictionary<Guid, IDbSubscription>, DispatchScan> _dispatchScans = new();
    private const int MaxForwardPagesPerPass = 16;
    private static readonly Guid LastMessageId = new("ffffffff-ffff-ffff-ffff-ffffffffffff");

    private sealed class MessageCursor
    {
        public DateTimeOffset? CreatedAtUtc;
        public Guid? Id;
        public void Advance(DbChannelMessage message) { CreatedAtUtc = message.CreatedAtUtc; Id = message.Id; }
    }

    // Cursor positions compared by creation time only: the stores order ids by their own binary
    // rules, which Guid.CompareTo does not reproduce, so within one tick all that is knowable is
    // whether the last row read is a different one.
    private static bool IsBehind(MessageCursor cursor, MessageCursor reference)
        => cursor.CreatedAtUtc is { } at && reference.CreatedAtUtc is { } referenceAt && at < referenceAt;

    private static bool IsAhead(MessageCursor cursor, MessageCursor reference)
        => cursor.CreatedAtUtc is { } at
           && (reference.CreatedAtUtc is not { } referenceAt || at > referenceAt || (at == referenceAt && cursor.Id != reference.Id));

    private sealed class DispatchScan
    {
        public HashSet<Guid> Registrations = [];
        public MessageCursor Forward = new();
        public bool ForwardCaughtUp;
        // Stopwatch timestamp of the last pass whose forward read moved past every row seen
        // before it, or whose lookback revisit the executor refused — what opens the late-commit
        // lookback window (see LateCommitLookback).
        public long? ForwardAdvancedAt;
        // Stopwatch timestamp of the last pass that revisited the whole lookback window: at most
        // one per poll interval (capped at the lookback) does (see LookbackRescanDelay).
        public long? WindowRevisitedAt;
        public MessageCursor? Reconciliation;
        public DateTimeOffset? ReconciliationEndUtc;
        public Guid? ReconciliationEndId;
        // Stopwatch timestamp the reconciliation interval runs from: a pure interval over
        // commits that land in real time, so a wall clock stepping back must not postpone it and
        // a virtual clock nobody advances must not suspend it.
        public long ReconciledAt;
        public int RewindRequested;
        // Ids this scan admitted to the executor whose work item has not finished yet. Rows are
        // marked seen only when their work item runs, so without this the lookback window, the
        // last-tick revisit and reconciliation re-read a row that is still queued and queued it
        // again on every pass — behind a slow Until predicate, duplicates that took executor
        // slots ahead of new rows. Dropped (never cleared) on a reset: a new registration set
        // must re-admit what the old one queued, since a queued item delivers only to the
        // subscriptions it captured; each item removes itself from the set it was added to.
        // Created by the first admission: a scan is built and reset once per correlation id's
        // lifecycle, and most admit a row or two.
        public ConcurrentDictionary<Guid, byte>? Queued;
    }

    /// <summary>
    /// How far behind its last row a caught-up forward read looks while the cursor is fresh:
    /// half the delivery-confirmation budget, at most two seconds.
    /// <para>
    /// A row's <c>created_at</c> is stamped when its INSERT runs, not when it commits. A response
    /// whose insert was stamped before a row this scan already read, but whose transaction
    /// committed after that read, lands BEHIND the cursor, where no forward read sees it again —
    /// only history reconciliation, which runs one page per pass a whole interval after the last
    /// one (5 s by default, the same as the publisher's confirmation budget). Its publisher in
    /// another process then won the recovery claim first: the lost-subscriber callback fired
    /// under a live waiter, and reconciliation later lost the delivery claim and dropped the row.
    /// A commit slower than this window from its stamp still waits for reconciliation.
    /// </para>
    /// </summary>
    private TimeSpan LateCommitLookback()
    {
        var half = _options.DeliveryConfirmationTimeout / 2;
        return half < MaxLateCommitLookback ? half : MaxLateCommitLookback;
    }

    private static readonly TimeSpan MaxLateCommitLookback = TimeSpan.FromSeconds(2);

    /// <summary>
    /// How long until a pass may revisit the whole lookback window again, the last full revisit
    /// having run <paramref name="sinceRevisit"/> ago; <c>null</c> when this pass may. The throttle is the
    /// poll interval, but never longer than the lookback: the window is open for only twice the
    /// lookback, and a poll interval of a few seconds (allowed, and unrelated to the confirmation
    /// budget the lookback derives from) put a throttled late commit's rescan past its close — it
    /// then waited for history reconciliation, and its publisher's confirmation lapsed into
    /// lost-subscriber recovery under a live waiter. The rescan is due when the throttle ends, not
    /// a whole interval after the throttled pass.
    /// </summary>
    internal static TimeSpan? LookbackRescanDelay(TimeSpan pollInterval, TimeSpan lookback, TimeSpan sinceRevisit)
    {
        var throttle = pollInterval < lookback ? pollInterval : lookback;
        return sinceRevisit < throttle ? throttle - sinceRevisit : null;
    }

    /// <summary>One correlation id's pass; <c>false</c> when no subscription of it is live, so the store was not queried.</summary>
    private async Task<bool> DispatchPendingCorrelationAsync(
        string correlationId,
        ConcurrentDictionary<Guid, IDbSubscription> group,
        CancellationToken cancellationToken)
    {
        // The oldest watermark is folded into the pass that builds the list: this runs per
        // correlation id on every sweep tick AND on every publish's targeted scan, so a separate
        // LINQ Min() was one enumerator allocation and one delegate call per element on the
        // dispatch hot path, over a list this loop has in hand anyway.
        var subscriptions = new List<IDbSubscription>(group.Count);
        var oldestStartedAtUtc = DateTimeOffset.MaxValue;
        foreach (var subscription in group.Values)
        {
            if (subscription.Dropped)
                continue;

            subscriptions.Add(subscription);
            if (subscription.StartedAtUtc < oldestStartedAtUtc)
                oldestStartedAtUtc = subscription.StartedAtUtc;
        }
        if (subscriptions.Count == 0)
            return false;

        var since = oldestStartedAtUtc.AddSeconds(-1);
        // Seen entries age out on the real monotonic clock (see MarkSeen), a little past the
        // server-side retention they stand in for.
        var seenMaxAge = _options.MessageRetention + TimeSpan.FromMinutes(1);
        foreach (var subscription in subscriptions)
            subscription.PruneSeen(seenMaxAge);

        var scan = _dispatchScans.GetOrCreateValue(group);
        var registrations = subscriptions.Select(subscription => subscription.Id).ToHashSet();
        if (!scan.Registrations.SetEquals(registrations) || Interlocked.Exchange(ref scan.RewindRequested, 0) != 0)
        {
            scan.Registrations = registrations;
            scan.Forward = new MessageCursor();
            scan.ForwardCaughtUp = false;
            scan.ForwardAdvancedAt = null;
            scan.WindowRevisitedAt = null;
            scan.Reconciliation = null;
            scan.ReconciledAt = Stopwatch.GetTimestamp();
            scan.Queued = null;
        }

        // Normal polls and targeted signals continue after the last admitted page. A new waiter
        // resets progress so its own watermark, not another waiter's seen set, decides fan-out.
        // A snapshot, not the live cursor: a walk that continues from an earlier pass advances
        // the cursor object in place, and this pass must still see where it started.
        var previousForward = new MessageCursor { CreatedAtUtc = scan.Forward.CreatedAtUtc, Id = scan.Forward.Id };
        if (scan.ForwardCaughtUp && scan.Forward.CreatedAtUtc is { } lastTick && lastTick > DateTimeOffset.MinValue)
        {
            // A database clock tick can contain several random ids. A newly committed message
            // in the LAST tick must not wait for historical reconciliation merely because its
            // id sorts before the previous message. Revisit that tick, not the entire history.
            // The provider may truncate the sub-tick timestamp to milliseconds/microseconds;
            // the maximum id excludes rows at that preceding, truncated timestamp.
            //
            // While the cursor is fresh, revisit a whole lookback window behind it instead: a
            // late commit stamped inside it becomes visible within the window's length of the
            // read that moved the cursor past it. The box stays open for TWICE the window, since
            // the wake for such a commit lands only after the commit itself; after that, idle
            // polls fall back to the single last tick, so they stay cheap. Re-read rows are
            // screened before admission: processed ones by the seen sets, admitted ones still
            // waiting in the executor by the scan's queued set, so none is enqueued twice.
            //
            // The whole window is revisited at most once per poll interval — and at least once per
            // lookback (see LookbackRescanDelay). Every publish and every wake signals a pass, and
            // a steady stream keeps the window open, so re-reading it on each pass cost about 2R
            // rows per pass — 2R² per second for an id receiving R responses a second (an Until
            // progress stream), envelopes included for rows still queued — where one-row reads had
            // done before. A pass inside the throttle revisits the last tick only and schedules a
            // rescan of the id for when the throttle is over, so a late commit whose own wake was
            // the last signal is still found while the window is open.
            var lookback = LateCommitLookback();
            var revisitWindow = false;
            if (scan.ForwardAdvancedAt is { } advancedAt
                && Stopwatch.GetElapsedTime(advancedAt) < lookback + lookback
                && lastTick - DateTimeOffset.MinValue > lookback)
            {
                if (scan.WindowRevisitedAt is { } revisitedAt
                    && LookbackRescanDelay(CurrentPollInterval(), lookback, Stopwatch.GetElapsedTime(revisitedAt)) is { } rescanIn)
                    ScheduleLookbackRescan(correlationId, rescanIn, cancellationToken);
                else
                {
                    scan.WindowRevisitedAt = Stopwatch.GetTimestamp();
                    revisitWindow = true;
                }
            }
            var revisitFrom = revisitWindow ? lastTick - lookback : lastTick.AddTicks(-1);
            scan.Forward = new MessageCursor { CreatedAtUtc = revisitFrom, Id = LastMessageId };
        }
        scan.ForwardCaughtUp = false;
        var forwardReadAny = false;
        for (var page = 0; page < MaxForwardPagesPerPass; page++)
        {
            var (more, admitted, _) = await DispatchPageAsync(scan.Forward).ConfigureAwait(false);
            if (!admitted)
            {
                // A refused page inside the revisit window: put the cursor back where this pass
                // found it, caught up, as if the revisit had not run. Left at the window's start
                // and no longer caught up, every rescan re-read the whole window from there —
                // whether or not the window was still open — and re-offered the same rows to the
                // executor that had just refused them. The refused rows are behind the restored
                // cursor only if they were inside the window, and the next pass revisits it again
                // while it is open — so the refusal keeps it open: a restored cursor does not
                // advance, and a window left to age out while refusals lasted (an executor full for
                // longer than twice the lookback) stranded a refused late row behind it for
                // reconciliation. Re-reads are screened by the seen and queued sets.
                if (IsBehind(scan.Forward, previousForward))
                {
                    scan.Forward = previousForward;
                    scan.ForwardCaughtUp = true;
                    scan.ForwardAdvancedAt = Stopwatch.GetTimestamp();
                }
                return true;
            }
            if (!more)
            {
                scan.ForwardCaughtUp = true;
                break;
            }
            if (page == MaxForwardPagesPerPass - 1)
                ScheduleBackpressureRescan(correlationId, cancellationToken);
        }
        // Expired/pruned tail, or a lookback revisit that ended inside the range already read: do
        // not walk backward on idle polls.
        if (!forwardReadAny || IsBehind(scan.Forward, previousForward))
            scan.Forward = previousForward;
        else if (IsAhead(scan.Forward, previousForward))
            scan.ForwardAdvancedAt = Stopwatch.GetTimestamp();

        // Creation keys are NOT commit order: a transaction can become visible behind the
        // cursor, even with the same timestamp and a lower id, and another process may already
        // have acknowledged it. Reconcile retained history periodically, one page per pass.
        // Both unacked and acked rows participate; filtering acked rows would break fan-out.
        if (scan.Reconciliation is null
            && Stopwatch.GetElapsedTime(scan.ReconciledAt) >= _options.HistoryReconciliationInterval
            && scan.Forward.Id is not null)
        {
            scan.Reconciliation = new MessageCursor();
            scan.ReconciliationEndUtc = scan.Forward.CreatedAtUtc;
            scan.ReconciliationEndId = scan.Forward.Id;
        }
        if (scan.Reconciliation is { } reconciliation)
        {
            var (more, admitted, reachedEnd) = await DispatchPageAsync(reconciliation, reconcile: true).ConfigureAwait(false);
            if (admitted && (!more || reachedEnd))
            {
                scan.Reconciliation = null;
                scan.ReconciledAt = Stopwatch.GetTimestamp();
            }
            else
                ScheduleBackpressureRescan(correlationId, cancellationToken);
        }

        return true;

        async Task<(bool More, bool Admitted, bool ReachedEnd)> DispatchPageAsync(MessageCursor cursor, bool reconcile = false)
        {
            var messages = await _store.LoadMessagesAsync(
                correlationId, since, _options.PendingMessageBatchSize,
                cursor.CreatedAtUtc, cursor.Id, cancellationToken).ConfigureAwait(false);

            // Acknowledged rows stay eligible for fan-out, but travel header-only. Hydrate
            // only those a live subscription still needs, then enqueue in page order.
            List<DbChannelMessage>? eligible = null;
            List<Guid>? headerOnly = null;
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
                    // SafeLog: a throwing provider here failed the whole page, on every pass.
                    SafeLog.Try(
                        (Logger: _logger, Provider: _providerName, Returned: message.CorrelationId, Requested: correlationId),
                        static state => state.Logger.LogError(
                            "The {Provider} channel store returned a message for correlationId '{ReturnedCorrelationId}' when asked for '{RequestedCorrelationId}'. " +
                            "The correlation-id column is not using a case-sensitive/binary collation, so distinct correlation ids collide in the database. " +
                            "The message was NOT delivered to the wrong waiter. Re-create the AsyncResponse tables or collections (or alter the correlation_id columns) with a binary collation — on MongoDB, the simple collation.",
                            state.Provider, state.Returned, state.Requested));
                    continue;
                }

                // Reconciliation, the lookback window and last-tick overlap revisit seen headers
                // and rows still queued from an earlier pass; keep those out of the executor
                // queue. The work item re-checks after admission as well.
                if (scan.Queued?.ContainsKey(message.Id) == true || !WouldDeliverToAnySubscription(message, subscriptions))
                    continue;

                (eligible ??= []).Add(message);
                if (message.EnvelopeJson is null)
                    (headerOnly ??= []).Add(message.Id);
            }

            if (eligible is not null
                && !await EnqueueEligibleAsync(
                    correlationId,
                    eligible,
                    headerOnly,
                    subscriptions,
                    scan.Queued ??= new ConcurrentDictionary<Guid, byte>(concurrencyLevel: 1, capacity: eligible.Count),
                    cancellationToken).ConfigureAwait(false))
            {
                return (false, false, false); // Retry this page: never advance past refused work.
            }

            if (messages.Count > 0)
            {
                cursor.Advance(messages[^1]);
                if (!reconcile)
                    forwardReadAny = true;
            }
            var reachedEnd = reconcile && messages.Any(message =>
                message.Id == scan.ReconciliationEndId || message.CreatedAtUtc > scan.ReconciliationEndUtc);
            return (messages.Count == _options.PendingMessageBatchSize, true, reachedEnd);
        }
    }

    /// <summary>
    /// Second pass of one sweep page: hydrates the header-only rows among <paramref name="eligible"/>
    /// and admits every row to the correlation id's executor in page order. Returns <c>false</c>
    /// when the executor is full (the page's remaining rows are left in the store, in order, and
    /// a rescan is scheduled), which ends the correlation id's scan for this sweep.
    /// </summary>
    private async Task<bool> EnqueueEligibleAsync(
        string correlationId,
        List<DbChannelMessage> eligible,
        List<Guid>? headerOnly,
        List<IDbSubscription> subscriptions,
        ConcurrentDictionary<Guid, byte> queued,
        CancellationToken cancellationToken)
    {
        Dictionary<Guid, DbChannelMessage>? hydrated = null;
        if (headerOnly is not null)
        {
            var loaded = await _store.LoadMessagesByIdAsync(correlationId, headerOnly, cancellationToken).ConfigureAwait(false);
            hydrated = new Dictionary<Guid, DbChannelMessage>(loaded.Count);
            foreach (var message in loaded)
            {
                // The by-id read is exact on the id (unique), but a hydrated row must carry its
                // envelope: a store that answered header-only here would hand the waiter nothing.
                if (message.EnvelopeJson is not null)
                    hydrated[message.Id] = message;
            }
        }

        foreach (var message in eligible)
        {
            var deliverable = message;
            if (message.EnvelopeJson is null)
            {
                // Pruned or expired between the page read and the hydration: nothing to deliver
                // now; a row that is still there is re-evaluated by the next sweep.
                if (hydrated is null || !hydrated.TryGetValue(message.Id, out deliverable))
                    continue;
            }

            // Work-item class, not a lambda: a queued closure would chain display classes
            // pinning this paging frame (batch list, cursors, watermark) for as long as the
            // item sits in the executor's bounded queue.
            //
            // NON-BLOCKING admission. This loop is the process-wide dispatch sweep and walks
            // correlation ids sequentially, so waiting for ONE correlation id's executor
            // capacity here (the old EnqueueAsync) parked delivery for every other waiter in
            // the process: a waiter wedged in a slow Until predicate, fed a backlog of NEW
            // progress messages (the pre-filter above only screens consumed history), filled
            // its 1024-slot executor and the sweep then blocked on slot 1025 without ever
            // querying the next correlation id. Its per-correlation backpressure became shared
            // delivery blockage — unrelated remote/polled responses timed out behind it. At
            // capacity the rest of this correlation id's messages are left unclaimed in the
            // store, in order (nothing later is enqueued ahead of them), and a rescan of just
            // this id is scheduled for when the executor has had a poll interval to drain.
            //
            // Marked queued BEFORE the admission: an admitted item can run and unmark itself
            // before TryEnqueue even returns, and a mark set after that would never be cleared.
            queued.TryAdd(deliverable.Id, 0);
            var outcome = _executors.TryEnqueue(
                ChannelName(correlationId),
                new LocalDispatchWorkItem(this, deliverable, subscriptions, cancellationToken, queued).InvokeAsync);
            if (outcome != SerialExecutorRegistry.TryEnqueueOutcome.Accepted)
                queued.TryRemove(deliverable.Id, out _);
            if (outcome == SerialExecutorRegistry.TryEnqueueOutcome.Full)
            {
                ScheduleBackpressureRescan(correlationId, cancellationToken);
                return false;
            }
        }

        return true;
    }

    /// <summary>
    /// Would any live subscription actually take this message? Used both as the sweep's
    /// pre-enqueue filter and as the dispatch work item's own guard, so the two can never drift.
    /// </summary>
    private static bool WouldDeliverToAnySubscription(DbChannelMessage message, IReadOnlyList<IDbSubscription> subscriptions)
    {
        foreach (var subscription in subscriptions)
        {
            if (!subscription.Dropped && IsWithinWatermark(subscription, message) && !subscription.HasSeen(message.Id))
                return true;
        }

        return false;
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
        // returns the fast-path message with the SERVER-stamped created_at: subscription
        // watermarks are server-clock, and an app-clock timestamp here silently disabled the fast
        // path whenever the app clock ran more than the 1s tolerance behind the database — delivery
        // then quietly degraded to sweep latency on every publish. On an idempotent duplicate (a
        // publish retry) it is the ORIGINAL row, settlement columns included: fabricating
        // AckedAtUtc = null here bypassed IsWithinWatermark's acked-history exclusion, and a retry
        // landing after another process had claimed and acked the first attempt replayed that
        // consumed response to a waiter registered since — the sweep path never had the problem
        // because LoadMessagesAsync reads acked_at.
        var message = await _store.InsertMessageAsync(messageId, correlationId, envelopeJson, _options.MessageRetention, cancellationToken)
            .ConfigureAwait(false);
        TryDispatchLocalSubscribers(message);
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
        if (!WouldDeliverToAnySubscription(message, subscriptions))
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

        // Per-subscription isolation is load-bearing, not defensive tidiness. The claim above
        // already stamped acked_at and tripped the publisher's confirmation, so this message is
        // consumed: IsWithinWatermark excludes it from every later sweep and the lost-subscriber
        // path will never see it. If one subscription's dispatch throws OUTSIDE ProcessAsync's own
        // catch — a fault in the captured-context wrapper, or CleanupOnceAsync throwing from
        // CleanupCoreAsync's uncaught finally, which replaces the swallowed exception — letting it
        // propagate would strand every subscription after it in this fan-out until its step
        // timeout, with the response gone. Record the first fault and rethrow only after every
        // sibling has had the message, so the dispatch is still reported as failed.
        List<Exception>? failures = null;
        foreach (var subscription in subscriptions)
        {
            if (subscription.Dropped || !IsWithinWatermark(subscription, message) || !subscription.MarkSeen(message.Id))
                continue;

            try
            {
                await subscription.ProcessUnderContextAsync(message).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
            {
                (failures ??= []).Add(ex);
                // SafeLog: a throwing provider here stranded every sibling after this one.
                SafeLog.Try(
                    (Logger: _logger, Error: ex, Provider: _providerName, message.CorrelationId),
                    static state => state.Logger.LogError(
                        state.Error,
                        "Delivering the {Provider} response for correlationId {CorrelationId} to one waiter failed; the remaining waiters for this correlation id still receive it.",
                        state.Provider,
                        state.CorrelationId));
            }
        }

        // Always aggregated, never a bare rethrow of failures[0]: a rethrow would reset that
        // exception's stack trace, and AggregateException carries every inner one intact.
        if (failures is not null)
            throw new AggregateException($"Delivering the response for correlationId {message.CorrelationId} failed for {failures.Count} waiter(s).", failures);
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

    private void TryDispatchLocalSubscribers(DbChannelMessage message)
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
        //
        // NON-BLOCKING admission, like the sweep's: the publisher used to AWAIT executor capacity
        // here, and behind a sibling waiter's executor retirement that meant waiting out its
        // drain (up to 30 s + 30 s behind a slow Until predicate) — a stalled serial ingress. The
        // row is already stored, so a full or mid-retirement executor just leaves it to the
        // targeted signal the publish sends next; the sweep admits it in store order. The work
        // item runs on the CHANNEL's dispatch token, never the publisher's: a publisher that gave
        // up after its response was stored must not abort the local delivery of it (the claim
        // then threw, and every such item was logged as an executor error).
        _ = _executors.TryEnqueue(
            ChannelName(message.CorrelationId),
            new LocalDispatchWorkItem(this, message, subscriptions, _dispatchToken).InvokeAsync);
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

        // Retried on the insert's policy. The claim runs AFTER the row is stored, so a single
        // transient fault here failed the publish, and the ingress's own retry then re-published
        // the response under a NEW message id — a duplicate an Until waiter received twice. It is
        // idempotent: it sets recovery_claimed only while acked_at is null, delivery claims
        // refuse a recovery-claimed row, so a retry after a lost reply finds its own claim.
        return !await AsyncResponseRetry.ExecuteAsync(
            token => _store.TryClaimForRecoveryAsync(confirmation.MessageId, token),
            DbChannelStore.IsTransient,
            _options.PublishMaxAttempts,
            _options.PublishRetryBaseDelay,
            _options.PublishRetryMaxDelay,
            cancellationToken).ConfigureAwait(false);
    }

    private protected void SignalDispatcher(string? correlationId = null)
    {
        // Every process receives every wake on the database (each NOTIFY, each change event)
        // whether or not it holds a waiter for the id, and only a local subscription can use one:
        // a targeted scan of an unsubscribed id reads nothing. Dropping those here — the single
        // choke point every wake source goes through — keeps the cluster-wide wake rate out of
        // the bounded channel. A registration racing this check is safe: a new waiter signals its
        // own id right after it becomes discoverable (CreateResponseWaiterCore).
        if (!string.IsNullOrEmpty(correlationId) && !_subscriptions.ContainsKey(correlationId))
            return;

        if (!_signals.Writer.TryWrite(correlationId))
            Interlocked.Exchange(ref _fullSweepRequested, 1);
    }

    /// <summary>
    /// Correlation ids whose executor was at capacity during a sweep and that have a rescan
    /// pending. One pending rescan per id: a saturated id is re-signalled once per poll interval,
    /// not once per sweep that found it full.
    /// </summary>
    private readonly ConcurrentDictionary<string, byte> _backpressureRescans = new(StringComparer.Ordinal);

    /// <summary>
    /// Re-signals a targeted scan of <paramref name="correlationId"/> after one poll interval —
    /// the time the sweep would otherwise have waited for the saturated executor, spent letting
    /// every other correlation id deliver instead. The messages themselves stay in the store
    /// (unclaimed, unseen) until that scan enqueues them, in their original order.
    /// </summary>
    private void ScheduleBackpressureRescan(string correlationId, CancellationToken cancellationToken)
    {
        if (!_backpressureRescans.TryAdd(correlationId, 0))
            return;

        // Scheduled before the log line: a logging provider that throws must not leave the id's
        // dedupe entry behind with no rescan to clear it, which barred every later one.
        _ = RescanAfterDelayAsync(correlationId, CurrentPollInterval(), _backpressureRescans, cancellationToken);

        if (_logger.IsEnabled(LogLevel.Debug))
        {
            SafeLog.Try(
                (Logger: _logger, Provider: _providerName, CorrelationId: correlationId),
                static state => state.Logger.LogDebug(
                    "{Provider} dispatch for correlationId {CorrelationId} is at executor capacity; the remaining messages are left in the store and this id is rescanned after the poll interval.",
                    state.Provider, state.CorrelationId));
        }
    }

    /// <summary>
    /// Correlation ids with a lookback-window rescan pending (<see cref="ScheduleLookbackRescan"/>).
    /// Separate from <see cref="_backpressureRescans"/>: a backpressure rescan a whole poll interval
    /// out must not postpone one due when the window's throttle ends.
    /// </summary>
    private readonly ConcurrentDictionary<string, byte> _lookbackRescans = new(StringComparer.Ordinal);

    /// <summary>
    /// Re-signals a targeted scan of <paramref name="correlationId"/> after <paramref name="delay"/>,
    /// when the lookback window's revisit throttle ends (see <see cref="LookbackRescanDelay"/>). One
    /// pending rescan per id: the throttle ends at the same instant for every pass it held back.
    /// </summary>
    private void ScheduleLookbackRescan(string correlationId, TimeSpan delay, CancellationToken cancellationToken)
    {
        if (!_lookbackRescans.TryAdd(correlationId, 0))
            return;

        // Scheduled before the log line, for the same reason as the backpressure rescan.
        _ = RescanAfterDelayAsync(correlationId, delay, _lookbackRescans, cancellationToken);

        if (_logger.IsEnabled(LogLevel.Debug))
        {
            SafeLog.Try(
                (Logger: _logger, Provider: _providerName, CorrelationId: correlationId, Delay: delay),
                static state => state.Logger.LogDebug(
                    "{Provider} dispatch for correlationId {CorrelationId} revisited its late-commit lookback window moments ago; the window is revisited again in {Delay}.",
                    state.Provider, state.CorrelationId, state.Delay));
        }
    }

    /// <summary>
    /// Correlation ids whose pass failed transiently and that have a rescan pending
    /// (<see cref="ScheduleFailureRescan"/>). Separate from <see cref="_backpressureRescans"/>:
    /// a pending failure rescan must not postpone a backpressure rescan due sooner.
    /// </summary>
    private readonly ConcurrentDictionary<string, byte> _failureRescans = new(StringComparer.Ordinal);

    /// <summary>
    /// Re-signals a targeted scan of <paramref name="correlationId"/>, whose pass failed
    /// transiently, after the wake-down floor (<see cref="WakeDownFullSweepInterval"/>) — never
    /// after the poll interval: an outage too small to trip the breaker (fewer than 8 waiters)
    /// would otherwise retry every failed id on every 250 ms tick, past both sweep throttles.
    /// Only while the full-sweep throttle is longer than that floor; otherwise the next full sweep
    /// comes back for the id first. One pending rescan per id.
    /// </summary>
    private void ScheduleFailureRescan(string correlationId, CancellationToken cancellationToken)
    {
        if (WakeDownFullSweepInterval() is not { } delay
            || CurrentFullSweepInterval() is not { } fullSweepInterval
            || fullSweepInterval <= delay
            || !_failureRescans.TryAdd(correlationId, 0))
        {
            return;
        }

        _ = RescanAfterDelayAsync(correlationId, delay, _failureRescans, cancellationToken);
    }

    private async Task RescanAfterDelayAsync(
        string correlationId,
        TimeSpan delay,
        ConcurrentDictionary<string, byte> pending,
        CancellationToken cancellationToken)
    {
        try
        {
            await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Listener stopping: nothing to rescan for.
        }
        finally
        {
            pending.TryRemove(correlationId, out _);
        }

        if (!cancellationToken.IsCancellationRequested)
            SignalDispatcher(correlationId);
    }

    private async Task<bool> WaitForAcknowledgementAsync(PendingConfirmation confirmation, CancellationToken cancellationToken)
    {
        // MONOTONIC, on the injected clock: the confirmation budget is a pure interval, and it used
        // to be a wall-clock deadline (GetUtcNow() + timeout). A system clock stepped forward while
        // a publish waited here — an NTP correction, a VM resumed or migrated — made `remaining`
        // non-positive at once, the loop body never ran, and TryConfirmDeliveryAsync went straight
        // to TryClaimForRecoveryAsync: the message was claimed for lost-subscriber recovery under
        // a live waiter the dispatch loop was about to deliver it to. This is the same rule the
        // poll deadlines below already follow (_pollArmedAt); TimeProvider's timestamp keeps the
        // wait drivable by a virtual clock, which a raw Stopwatch would not.
        var startedAt = _timeProvider.GetTimestamp();
        TimeSpan Remaining() => _options.DeliveryConfirmationTimeout - _timeProvider.GetElapsedTime(startedAt);

        // One `remaining` computation drives both the loop condition and the poll delay: the old
        // shape tested the deadline twice, one line apart, so the code read as if two different
        // conditions mattered when the second could only ever agree with the first.
        //
        // The fast-path wait is a WaitAsync on the confirmation rather than a fresh Task.Delay
        // raced by WhenAny. WhenAny abandoned its loser every iteration, so the overwhelmingly
        // common same-process delivery left a live timer entry and a registration on the caller's
        // token behind on every publish; WaitAsync tears its timer down when the confirmation
        // wins. A lapsed poll interval surfaces as TimeoutException, which is the loop condition,
        // not a failure.
        for (var remaining = Remaining();
             remaining > TimeSpan.Zero;
             remaining = Remaining())
        {
            var pollDelay = remaining < _options.DeliveryConfirmationPollInterval
                ? remaining
                : _options.DeliveryConfirmationPollInterval;

            try
            {
                // Fast path: an in-process delivery trips the completion and we return without a query.
                return await confirmation.Delivered.WaitAsync(pollDelay, _timeProvider, cancellationToken).ConfigureAwait(false);
            }
            catch (TimeoutException)
            {
                // Nothing local within this poll interval; fall through to the store check.
            }

            // Slow path: a delivery in another process only set acked_at, so poll for it.
            if (await IsAcknowledgedToleratingTransientFaultsAsync(confirmation.MessageId, cancellationToken).ConfigureAwait(false))
                return true;
        }

        return confirmation.Delivered.IsCompletedSuccessfully
            || await IsAcknowledgedToleratingTransientFaultsAsync(confirmation.MessageId, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// One acknowledgement poll, reading a transient store fault as "not acknowledged yet". The
    /// poll runs AFTER the response row is stored, and one transient fault among its ~100 polls
    /// failed the whole publish — the ingress then re-published the response under a new
    /// message id, a duplicate for any Until waiter that had already received the first copy.
    /// Nothing is lost by polling on: the recovery claim at the deadline arbitrates atomically.
    /// </summary>
    private async Task<bool> IsAcknowledgedToleratingTransientFaultsAsync(Guid messageId, CancellationToken cancellationToken)
    {
        try
        {
            return await _store.IsMessageAcknowledgedAsync(messageId, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (DbChannelStore.IsTransient(ex))
        {
            SafeLog.Try(
                (Logger: _logger, Error: ex, Provider: _providerName, MessageId: messageId),
                static state => state.Logger.LogDebug(state.Error, "{Provider} delivery-confirmation poll for message {MessageId} failed transiently; polling on.", state.Provider, state.MessageId));
            return false;
        }
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
        string correlationId) where T : IAsyncResponsePayload
    {
        // SafeLog: a throwing provider here skipped the cleanup below, so the waiter never
        // completed and its subscription, subscriber row and executor stayed registered for good.
        SafeLog.Try(
            (Logger: _logger, Provider: _providerName, CorrelationId: correlationId),
            static state => state.Logger.LogWarning("Timed out waiting for {Provider} response for correlationId {CorrelationId}.", state.Provider, state.CorrelationId));
        AsyncResponseDiagnostics.SetError(activity, "timeout", $"Timed out waiting for response for correlationId {correlationId}.");
        AsyncResponseDiagnostics.RecordWaiterTimeout(_activityTag);
        await subscription.DrainThenCleanupAsync(
            deleteRecoveryState: true,
            new TimeoutException($"Timed out waiting for response for correlationId {correlationId}.")).ConfigureAwait(false);
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
        string correlationId) : IWaiterTimeoutState where T : IAsyncResponsePayload
    {
        public void Schedule()
            => _ = Task.Run(async () =>
            {
                try
                {
                    await owner.HandleWaiterTimeoutAsync(subscription, activity, correlationId).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    // Fire-and-forget: nothing awaits this task, so an escaped fault would vanish.
                    SafeLog.Try(
                        (Logger: owner._logger, Error: ex, Provider: owner._providerName, CorrelationId: correlationId),
                        static state => state.Logger.LogError(state.Error, "Error handling {Provider} waiter timeout for correlationId {CorrelationId}.", state.Provider, state.CorrelationId));
                }
            });
    }

    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    private sealed class LocalDispatchWorkItem(
        DbAsyncResponseChannelBase owner,
        DbChannelMessage message,
        IReadOnlyList<IDbSubscription> subscriptions,
        CancellationToken cancellationToken,
        ConcurrentDictionary<Guid, byte>? queued = null)
    {
        public async Task InvokeAsync()
        {
            try
            {
                await InvokeCoreAsync().ConfigureAwait(false);
            }
            finally
            {
                // Settled (delivered, seen, or failed into a rewind): a later read may admit the
                // row again, and the seen sets decide from here on.
                queued?.TryRemove(message.Id, out _);
            }
        }

        private async Task InvokeCoreAsync()
        {
            try
            {
                await owner.DispatchMessageToSubscribersAsync(message, subscriptions, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                // The channel is stopping — its dispatch token is the only token a work item runs
                // on. Not a delivery failure and nothing left to rescan for; escaping, it reached
                // ChannelSerialExecutor, which logged every item still queued at a graceful
                // shutdown as an executor error.
                SafeLog.Try(
                    (Logger: owner._logger, Provider: owner._providerName, message.CorrelationId),
                    static state => state.Logger.LogDebug(
                        "Local {Provider} response dispatch for correlationId {CorrelationId} stopped with the channel.",
                        state.Provider,
                        state.CorrelationId));
            }
            catch (Exception ex)
            {
                if (owner._subscriptions.TryGetValue(message.CorrelationId, out var group)
                    && owner._dispatchScans.TryGetValue(group, out var scan))
                {
                    Interlocked.Exchange(ref scan.RewindRequested, 1);
                    owner.ScheduleBackpressureRescan(message.CorrelationId, cancellationToken);
                }
                SafeLog.Try(
                    (Logger: owner._logger, Error: ex, Provider: owner._providerName, message.CorrelationId, Hint: owner._localDispatchRetryHint),
                    static state => state.Logger.LogDebug(
                        state.Error,
                        "Local {Provider} response dispatch failed for correlationId {CorrelationId}; {RetryHint}.",
                        state.Provider,
                        state.CorrelationId,
                        state.Hint));
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
            catch (Exception ex)
            {
                // A loop that died of anything but the cancellation is reported, never rethrown:
                // escaping here skipped every waiter's cleanup below, so their response tasks never
                // settled and their executors were never retired at host stop.
                SafeLog.Try(
                    (Logger: _logger, Error: ex, Provider: _providerName),
                    static state => state.Logger.LogError(state.Error, "A {Provider} channel background loop had failed before disposal; cleaning up its waiters anyway.", state.Provider));
            }
            cts.Dispose();
        }

        foreach (var (correlationId, group) in _subscriptions.ToArray())
        {
            foreach (var subscription in group.Values.ToArray())
                await subscription.DrainThenCleanupAsync(deleteRecoveryState: false).ConfigureAwait(false);
            await _executors.RemoveAsync(ChannelName(correlationId)).ConfigureAwait(false);
        }

        // Retirements for subscriptions that cleaned themselves up (a response landing during
        // shutdown) unlink their correlation id before scheduling, so the loop above never sees
        // them. Awaiting them here is what makes disposal mean "every executor is retired" rather
        // than "every executor still in the map is retired". Their bodies swallow, so this cannot
        // throw; the drain budgets inside RemoveAsync bound how long it can take.
        var retirements = _pendingRetirements.Keys.ToArray();
        if (retirements.Length > 0)
            await Task.WhenAll(retirements).ConfigureAwait(false);
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
        void PruneSeen(TimeSpan maxAge);
        Task ProcessAsync(DbChannelMessage message);
        ValueTask CleanupOnceAsync(bool deleteRecoveryState);
        ValueTask DrainThenCleanupAsync(bool deleteRecoveryState, Exception? terminalIfUndelivered = null);
        ValueTask DropLocalAsync(CancellationToken cancellationToken);
    }

    /// <summary>Most characters of a remote failure's message the waiter's activity status quotes.</summary>
    private const int RemoteFailureStatusMaxLength = 512;

    private sealed class DbSubscription<T> : IDbSubscription where T : IAsyncResponsePayload
    {
        private readonly DbAsyncResponseChannelBase _owner;
        private readonly string _correlationId;
        private readonly Func<T, ValueTask<bool>> _completionPredicate;
        private readonly TaskCompletionSource<T> _tcs;
        private readonly Activity? _activity;
        private readonly HashSet<Guid> _seen = [];
        private readonly Queue<(Guid Id, long SeenAt)> _seenOrder = [];
        private readonly object _seenGate = new();
        private int _cleanupStarted;
        private volatile bool _dropped;

        /// <summary>True once cleanup began — the arm-last waiter-timeout guard reads this.</summary>
        internal bool CleanupStarted => Volatile.Read(ref _cleanupStarted) != 0;
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
                // A monotonic Stopwatch stamp, not the injected clock: an entry stands in for a row
                // the SERVER retains for MessageRetention of real time, so a wall clock stepped
                // forward — or a virtual clock a test advanced past the retention — must not evict
                // the seen set of a live waiter while the rows are still there to be re-read (the
                // last-tick overlap and reconciliation re-delivered them). PruneSeen ages entries
                // on the same Stopwatch.
                _seenOrder.Enqueue((messageId, Stopwatch.GetTimestamp()));
                return true;
            }
        }

        public void PruneSeen(TimeSpan maxAge)
        {
            lock (_seenGate)
            {
                while (_seenOrder.TryPeek(out var entry) && Stopwatch.GetElapsedTime(entry.SeenAt) >= maxAge)
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
                // JsonSafety, not the raw reader: a parse failure is logged below and handed to the
                // waiter, and the reader's own message quotes inbound property names and dictionary
                // keys (docs/security.md, "never logs a message body"). Size and position only.
                // A header-only sweep row never reaches delivery: the sweep hydrates it first.
                var envelopeJson = message.EnvelopeJson
                    ?? throw new InvalidOperationException($"The {_owner._providerName} channel message {message.Id} reached delivery without its envelope.");
                var envelope = JsonSafety.SafeDeserialize(envelopeJson, AsyncResponseEnvelopeJson.TypeInfo<T>());
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
                    // The status quotes a capped, escaped excerpt: the message is REMOTE text (up to
                    // the inbound size limit, CR/LF included), the same exposure the stack trace
                    // cap exists for. The waiter's exception still carries it verbatim.
                    AsyncResponseDiagnostics.SetError(_activity, "remote_failure", DiagnosticText.EscapedExcerpt(remoteFailure.Message, RemoteFailureStatusMaxLength));
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
                // Faulted before the log: a throwing provider otherwise left the cleanup below to
                // cancel the waiter instead of failing it with the error.
                AsyncResponseDiagnostics.SetError(_activity, ex);
                _tcs.TrySetException(ex);
                SafeLog.Try(
                    (Logger: _owner._logger, Error: ex, Provider: _owner._providerName, CorrelationId: _correlationId),
                    static state => state.Logger.LogError(state.Error, "Error processing {Provider} response for correlationId {CorrelationId}.", state.Provider, state.CorrelationId));
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
        public async ValueTask DrainThenCleanupAsync(bool deleteRecoveryState, Exception? terminalIfUndelivered = null)
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
                    // call is a no-op behind the latch. Faulted BEFORE the logs: a logging
                    // provider that throws must not leave the cleanup's cancel to report a
                    // consumed response as "nothing was delivered".
                    AsyncResponseDiagnostics.SetError(_activity, "indeterminate_delivery", "Disposal drain did not prove settlement.");
                    _tcs.TrySetException(new AsyncResponseIndeterminateDeliveryException(_correlationId, drainTimeout));
                    SafeLog.Try(
                        (Logger: _owner._logger, Provider: _owner._providerName, CorrelationId: _correlationId, DrainTimeout: drainTimeout, Error: drainEx),
                        static state =>
                        {
                            state.Logger.LogWarning(
                                "Disposal drain for {Provider} correlationId {CorrelationId} did not prove settlement within {DrainTimeout}; faulting the waiter as indeterminate.",
                                state.Provider, state.CorrelationId, state.DrainTimeout);
                            if (state.Error is not OperationCanceledException)
                                state.Logger.LogDebug(state.Error, "Dispatch drain failed for correlationId {CorrelationId}.", state.CorrelationId);
                        });
                }
            }

            // Settle AFTER the drain, never before it. A delivery already inside the per-correlation
            // executor may hold a message the claim acked — the publisher was told "delivered" and
            // the watermark excludes it from every later sweep, so it exists nowhere else. Faulting
            // first let a timeout beat that in-flight delivery and report a consumed response as a
            // timeout; TrySet loses here if the delivery won, which is the whole point. (A lapsed
            // drain budget has already faulted the task as indeterminate above, and TrySet is a
            // no-op behind it.)
            if (terminalIfUndelivered is not null)
                _tcs.TrySetException(terminalIfUndelivered);

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
                    // not skip the local teardown below — nor may a logging provider that throws
                    // skip the subscriber delete after it.
                    SafeLog.Try(
                        (Logger: _owner._logger, Error: ex, Provider: _owner._providerName, CorrelationId: _correlationId),
                        static state => state.Logger.LogError(state.Error, "Failed to delete {Provider} recovery state for correlationId {CorrelationId}.", state.Provider, state.CorrelationId));
                }

                try
                {
                    await _owner._store.DeleteSubscriberAsync(_correlationId, Id, CancellationToken.None).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    // Best-effort: an orphaned subscriber record ages out via the heartbeat timeout.
                    SafeLog.Try(
                        (Logger: _owner._logger, Error: ex, Provider: _owner._providerName, Record: _owner._subscriberRecordNoun, CorrelationId: _correlationId),
                        static state => state.Logger.LogError(state.Error, "Failed to delete {Provider} subscriber {SubscriberRecord} for correlationId {CorrelationId}.", state.Provider, state.Record, state.CorrelationId));
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
                // TRACKED, though: RemoveSubscription above already unlinked this correlation id,
                // so DisposeAsync's own retirement loop will not see it, and an untracked
                // retirement could still be inside its 30-second drain budget when the host tears
                // down the logger and exits — logging into a disposed logger, or being killed
                // mid-drain. DisposeAsync awaits whatever is still outstanding here. Retired only
                // once no sibling subscription on this correlation id remains registered: the
                // executor is shared, and the last cleanup out retires it.
                var channelName = _owner.ChannelName(_correlationId);
                _owner.TrackRetirement(Task.Run(async () =>
                {
                    try
                    {
                        await _owner._executors.RetireIfUnreferencedAsync(channelName).ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        // SafeLog: DisposeAsync awaits this task and relies on it never faulting.
                        SafeLog.Try(
                            (Logger: _owner._logger, Error: ex, Channel: channelName),
                            static state => state.Logger.LogError(state.Error, "Failed to retire the executor for channel {Channel}.", state.Channel));
                    }
                }));

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
