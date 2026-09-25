// Binds the shared database-channel machinery (src/Channels/Shared/DbChannelShared.cs, compiled
// into this project) to this provider's concrete seam types. See the note atop the shared file.
global using DbChannelStore = AsyncResponse.Channels.MongoDB.MongoDbChannelStore;
global using DbChannelMessage = AsyncResponse.Channels.MongoDB.MongoDbChannelMessage;
global using DbChannelOptions = AsyncResponse.Channels.MongoDB.MongoDbAsyncResponseChannelOptions;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Diagnostics;

namespace AsyncResponse.Channels.MongoDB;

/// <summary>
/// MongoDB-backed response channel using change streams for active waiter wakeups and TTL-indexed
/// collections for durable recovery state. Requires the server to run as a replica set (a
/// single-node replica set is sufficient) for change-stream wakes; without one the channel degrades
/// to interval polling.
/// </summary>
internal sealed class MongoDbAsyncResponseChannel : DbAsyncResponseChannelBase
{
    /// <summary>Creates a MongoDB-backed async-response channel.</summary>
    public MongoDbAsyncResponseChannel(
        IServiceScopeFactory scopeFactory,
        MongoDbChannelStore store,
        IRecoveryStateStore recoveryStateStore,
        IOptions<MongoDbAsyncResponseChannelOptions> options,
        AsyncResponseContextPropagation propagation,
        ILogger<MongoDbAsyncResponseChannel> logger,
        TimeProvider? timeProvider = null)
        : base(
            scopeFactory,
            store,
            recoveryStateStore,
            options.Value,
            propagation,
            logger,
            channelTypeName: nameof(MongoDbAsyncResponseChannel),
            providerName: "MongoDB",
            activityTag: "mongodb",
            subscriberRecordNoun: "document",
            localDispatchRetryHint: "listener retry will pick it up",
            timeProvider)
    {
    }

    /// <inheritdoc />
    protected override string ChannelName(string correlationId) => $"{_options.MessageCollection}:{correlationId}";

    /// <inheritdoc />
    protected override TimeSpan CurrentPollInterval() => _options.ListenerPollInterval;

    // True only while a change stream is open: set once the watch cursor opens, cleared as soon
    // as the stream fails or ends, until the next successful open. Never set when the server
    // reports change streams unsupported (a standalone). Read on every poll tick, so volatile.
    private volatile bool _watching;

    // Set once the server reports change streams unsupported (a standalone): the channel then
    // polls for good, like UseChangeStreams = false. Read on every poll tick, so volatile.
    private volatile bool _changeStreamsUnavailable;

    /// <summary>
    /// The throttled sweep applies only while change streams carry normal delivery. With
    /// <see cref="MongoDbAsyncResponseChannelOptions.UseChangeStreams"/> off, once the server has
    /// reported them unsupported, and while a stream is down (before it first opens, and across
    /// every re-open after a transient failure) the sweep is the ONLY cross-process wake — and
    /// the 5s default equalled <c>DeliveryConfirmationTimeout</c>, so the publisher gave up,
    /// claimed the message for recovery and fired the lost-subscriber callback a beat before the
    /// healthy waiter's sweep found it. Polling for good (the option off, a standalone server) it
    /// sweeps on every tick, as the <c>UseChangeStreams</c> doc promises; with a stream only
    /// down until it re-opens, at the wake-down interval (a quarter of the confirmation budget).
    /// </summary>
    protected override TimeSpan? CurrentFullSweepInterval()
        => !_options.UseChangeStreams || _changeStreamsUnavailable ? null
            : _watching ? _options.FullSweepInterval
            : WakeDownFullSweepInterval();

    /// <inheritdoc />
    protected override Task? StartWakeListener(CancellationToken cancellationToken)
        => _options.UseChangeStreams
            ? Task.Run(() => ListenLoopAsync(cancellationToken))
            : Task.CompletedTask;

    /// <inheritdoc />
    protected override IAsyncResponseWaiter<T> CreateWaiter<T>(Task<T> responseTask, Func<ValueTask> cleanupAsync)
        => new MongoDbAsyncResponseWaiter<T>(responseTask, cleanupAsync);

    /// <summary>
    /// A change stream (re)opened: it carries delivery again, but a re-opened stream has no
    /// resume point, so the inserts made while none was open were never observed — sweep every
    /// waiter once now instead of leaving them to the next throttled sweep.
    /// </summary>
    private void OnWakeListenerEstablished()
    {
        _watching = true;
        SignalDispatcher();
    }

    private async Task ListenLoopAsync(CancellationToken cancellationToken)
    {
        var failures = 0;
        while (!cancellationToken.IsCancellationRequested)
        {
            // Stopwatch timestamp of this attempt's opened stream, if it got that far.
            long? openedAt = null;
            try
            {
                await _store.WatchMessagesAsync(
                    payload =>
                    {
                        SignalDispatcher(string.IsNullOrEmpty(payload) ? null : payload);
                        return Task.CompletedTask;
                    },
                    cancellationToken,
                    () =>
                    {
                        openedAt = Stopwatch.GetTimestamp();
                        OnWakeListenerEstablished();
                    }).ConfigureAwait(false);
                // The stream ended (an invalidate event): no wake until the next open.
                _watching = false;
                failures = 0;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex) when (MongoDbChannelStore.IsChangeStreamUnsupported(ex))
            {
                // Standalone server: change streams need a replica set. The dispatch loop's
                // ListenerPollInterval sweep still delivers, so degrade to polling instead of
                // retry-spamming an error the server will keep returning. Never watching again,
                // so the sweep throttle stays lifted (see CurrentFullSweepInterval): it is now the
                // only wake.
                _changeStreamsUnavailable = true;
                _watching = false;
                _logger.LogWarning(
                    ex,
                    "MongoDB change streams are unavailable (the server is not a replica set); response wakes fall back to {PollInterval} polling.",
                    _options.ListenerPollInterval);
                return;
            }
            catch (Exception ex)
            {
                _watching = false;
                // A stream that stayed open a healthy run ends the failure run, so it reconnects
                // on the first backoff step (see _wakeListenerHealthyRun).
                if (openedAt is { } since && Stopwatch.GetElapsedTime(since) >= _wakeListenerHealthyRun)
                    failures = 0;
                failures++;
                var delay = AsyncResponseRetry.Backoff(failures, TimeSpan.FromMilliseconds(100), WakeListenerMaxRetryDelay);
                _logger.LogWarning(ex, "MongoDB change-stream loop failed; retrying in {Delay}.", delay);
                await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
            }
        }
    }
}
