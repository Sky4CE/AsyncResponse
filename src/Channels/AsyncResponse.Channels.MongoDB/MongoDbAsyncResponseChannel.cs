// Binds the shared database-channel machinery (src/Channels/Shared/DbChannelShared.cs, compiled
// into this project) to this provider's concrete seam types. See the note atop the shared file.
global using DbChannelStore = AsyncResponse.Channels.MongoDB.MongoDbChannelStore;
global using DbChannelMessage = AsyncResponse.Channels.MongoDB.MongoDbChannelMessage;
global using DbChannelOptions = AsyncResponse.Channels.MongoDB.MongoDbAsyncResponseChannelOptions;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

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

    // Set once the server reports change streams unsupported (a standalone). Read on every poll
    // tick, so volatile.
    private volatile bool _changeStreamsUnavailable;

    /// <summary>
    /// The throttled sweep applies only while change streams carry normal delivery. With
    /// <see cref="MongoDbAsyncResponseChannelOptions.UseChangeStreams"/> off, or once the server
    /// has reported them unsupported, the sweep is the ONLY cross-process wake — and the 5s
    /// default equalled <c>DeliveryConfirmationTimeout</c>, so the publisher gave up, claimed the
    /// message for recovery and fired the lost-subscriber callback a beat before the healthy
    /// waiter's sweep found it. Every tick, as the <c>UseChangeStreams</c> doc promises.
    /// </summary>
    protected override TimeSpan? CurrentFullSweepInterval()
        => _options.UseChangeStreams && !_changeStreamsUnavailable ? _options.FullSweepInterval : null;

    /// <inheritdoc />
    protected override Task? StartWakeListener(CancellationToken cancellationToken)
        => _options.UseChangeStreams
            ? Task.Run(() => ListenLoopAsync(cancellationToken))
            : Task.CompletedTask;

    /// <inheritdoc />
    protected override IAsyncResponseWaiter<T> CreateWaiter<T>(Task<T> responseTask, Func<ValueTask> cleanupAsync)
        => new MongoDbAsyncResponseWaiter<T>(responseTask, cleanupAsync);

    private async Task ListenLoopAsync(CancellationToken cancellationToken)
    {
        var failures = 0;
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await _store.WatchMessagesAsync(payload =>
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
            catch (Exception ex) when (MongoDbChannelStore.IsChangeStreamUnsupported(ex))
            {
                // Standalone server: change streams need a replica set. The dispatch loop's
                // ListenerPollInterval sweep still delivers, so degrade to polling instead of
                // retry-spamming an error the server will keep returning. Flagged so the sweep
                // throttle lifts (see CurrentFullSweepInterval): it is now the only wake.
                _changeStreamsUnavailable = true;
                _logger.LogWarning(
                    ex,
                    "MongoDB change streams are unavailable (the server is not a replica set); response wakes fall back to {PollInterval} polling.",
                    _options.ListenerPollInterval);
                return;
            }
            catch (Exception ex)
            {
                failures++;
                var delay = AsyncResponseRetry.Backoff(failures, TimeSpan.FromMilliseconds(100), TimeSpan.FromSeconds(5));
                _logger.LogWarning(ex, "MongoDB change-stream loop failed; retrying in {Delay}.", delay);
                await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
            }
        }
    }
}
