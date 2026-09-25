using Microsoft.Extensions.Logging;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using NATS.Client.KeyValueStore;
using NATS.Net;
using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace AsyncResponse.Channels.NATS;

/// <summary>Outcome of a response publish/probe over NATS request/reply.</summary>
internal enum NatsDeliveryOutcome
{
    /// <summary>A waiter acknowledged the message — confirmed delivery / confirmed live subscriber.</summary>
    Replied,

    /// <summary>
    /// Interest existed but no ack arrived within the timeout. A publish treats it as delivered
    /// (usually a live subscriber whose ack was slow — but also a subscriber whose host died without
    /// closing its connection, see <see cref="NatsAsyncResponseChannelOptions.DeliveryConfirmationTimeout"/>);
    /// a probe reports it as unprobeable.
    /// </summary>
    NoReply,

    /// <summary>NATS reported no responders: nobody is subscribed, so the lost-subscriber fallback must run.</summary>
    NoResponders
}

/// <summary>A response message received by a waiter, decoupled from the NATS client types for testability.</summary>
/// <param name="Payload">The raw JSON body, or <c>null</c> for an empty (e.g. probe) message.</param>
/// <param name="IsProbe">Whether the message is a liveness probe rather than a real response.</param>
/// <param name="ReplyAsync">Acknowledges receipt to the publisher (a no-op when the message has no reply subject).</param>
internal readonly record struct NatsInboundResponse(string? Payload, bool IsProbe, Func<ValueTask> ReplyAsync);

/// <summary>A live subscription to a correlation id's response subject.</summary>
internal interface INatsChannelSubscription : IAsyncDisposable
{
    /// <summary>Streams inbound response messages until the subscription is disposed or the token is cancelled.</summary>
    IAsyncEnumerable<NatsInboundResponse> ReadAsync(CancellationToken cancellationToken);
}

/// <summary>
/// Thin abstraction over the NATS Core operations the response channel needs. Confines the NATS.Net
/// API surface to one place so the channel logic is unit-testable against a fake/mock.
/// </summary>
internal interface INatsResponseChannelClient
{
    /// <summary>
    /// Publishes <paramref name="payload"/> to <paramref name="subject"/> as a request, returning
    /// whether any waiter was listening. A <paramref name="probe"/> request carries no payload and is
    /// answered by waiters without being treated as a response.
    /// </summary>
    Task<NatsDeliveryOutcome> RequestAsync(string subject, string? payload, bool probe, TimeSpan timeout, CancellationToken cancellationToken);

    /// <summary>Establishes a subscription to <paramref name="subject"/>; awaiting the result guarantees the subscription is registered.</summary>
    Task<INatsChannelSubscription> SubscribeAsync(string subject, CancellationToken cancellationToken);

    /// <summary>Round-trips to the server so previously issued subscriptions are guaranteed processed before the caller proceeds.</summary>
    Task FlushAsync(CancellationToken cancellationToken);
}

/// <summary>Header marking a request as a liveness probe rather than a response payload.</summary>
internal static class NatsChannelHeaders
{
    public const string Probe = "AR-Probe";
}

/// <summary>
/// The minimal raw NATS operations the response channel cannot express through mockable interface
/// members: <c>RequestAsync</c> and <c>PingAsync</c> are NATS.Net extension methods, and a reply is a
/// Core publish. Isolating them here keeps <see cref="NatsResponseChannelClient"/> fully unit-testable;
/// only this tiny shim wraps the un-mockable network calls.
/// </summary>
internal interface INatsRawRequester
{
    /// <summary>Sends a request and awaits a reply, throwing <c>NatsNoRespondersException</c>/<c>NatsNoReplyException</c> on no responders / no timely reply.</summary>
    Task RequestAsync(string subject, string? payload, NatsHeaders? headers, TimeSpan timeout, CancellationToken cancellationToken);

    /// <summary>Establishes (and awaits registration of) a Core subscription to <paramref name="subject"/>.</summary>
    Task<INatsSub<string>> SubscribeAsync(string subject, CancellationToken cancellationToken);

    /// <summary>Publishes an empty acknowledgement to <paramref name="replyTo"/>.</summary>
    ValueTask PublishReplyAsync(string replyTo, CancellationToken cancellationToken);

    /// <summary>Round-trips to the server (a ping) so prior subscriptions are guaranteed processed.</summary>
    Task FlushAsync(CancellationToken cancellationToken);
}

/// <summary>Production <see cref="INatsRawRequester"/> over a NATS <see cref="INatsConnection"/>.</summary>
internal sealed class NatsRawRequester(INatsConnection _connection) : INatsRawRequester
{
    /// <summary>Runs the RequestAsync operation.</summary>
    public async Task RequestAsync(string subject, string? payload, NatsHeaders? headers, TimeSpan timeout, CancellationToken cancellationToken)
        => _ = await _connection.RequestAsync<string?, string>(
            subject,
            payload,
            headers: headers,
            // Pinned per call: the channel's lost-subscriber routing and liveness probe both rest on
            // a no-responders 503 THROWING. Left unset, that follows the app-supplied connection's
            // RequestReplyMode, and under Direct the sentinel arrives as an ordinary (discarded)
            // reply — a dead subject then looks answered and the response is dropped.
            replyOpts: new NatsSubOpts { Timeout = timeout, ThrowIfNoResponders = true },
            cancellationToken: cancellationToken).ConfigureAwait(false);

    /// <summary>Runs the SubscribeAsync operation.</summary>
    public async Task<INatsSub<string>> SubscribeAsync(string subject, CancellationToken cancellationToken)
        => await _connection.SubscribeCoreAsync<string>(subject, cancellationToken: cancellationToken).ConfigureAwait(false);

    /// <summary>Publishes the supplied message.</summary>
    public ValueTask PublishReplyAsync(string replyTo, CancellationToken cancellationToken)
        => _connection.PublishAsync(replyTo, string.Empty, cancellationToken: cancellationToken);

    /// <summary>Runs the FlushAsync operation.</summary>
    public async Task FlushAsync(CancellationToken cancellationToken)
        => await _connection.PingAsync(cancellationToken).ConfigureAwait(false);
}

/// <summary>
/// Production <see cref="INatsResponseChannelClient"/>: maps NATS request/reply outcomes and wraps Core
/// subscriptions, delegating the raw (un-mockable) calls to an <see cref="INatsRawRequester"/>.
/// </summary>
internal sealed class NatsResponseChannelClient(INatsRawRequester _raw) : INatsResponseChannelClient
{
    /// <summary>Runs the RequestAsync operation.</summary>
    public async Task<NatsDeliveryOutcome> RequestAsync(string subject, string? payload, bool probe, TimeSpan timeout, CancellationToken cancellationToken)
    {
        NatsHeaders? headers = probe ? new NatsHeaders { [NatsChannelHeaders.Probe] = "1" } : null;

        try
        {
            await _raw.RequestAsync(subject, payload, headers, timeout, cancellationToken).ConfigureAwait(false);
            return NatsDeliveryOutcome.Replied;
        }
        catch (NatsNoRespondersException)
        {
            // The definitive "nobody is listening" signal — the server answered immediately because no
            // subscription has interest in the subject.
            return NatsDeliveryOutcome.NoResponders;
        }
        catch (NatsNoReplyException)
        {
            // Interest existed but no ack arrived within the timeout. A publish treats it as
            // delivered (normally a live subscriber whose ack was slow; the same answer comes from a
            // subscription whose host died without closing its connection, which the server keeps
            // until its ping timeout). A probe reports it as unprobeable. The channel interprets
            // this per call site.
            return NatsDeliveryOutcome.NoReply;
        }
        catch (NatsPayloadTooLargeException ex)
        {
            // The client refuses a message above the server's max_payload (1 MiB by default) before
            // sending it — deterministically, on every attempt. As a NatsException it looked like a
            // transient connection error, so the ingress ran its whole retry ladder for a response
            // that can never be delivered on this channel before escalating it. InvalidDataException
            // is the ingress's "unprocessable, escalate now" signal. The message carries sizes only.
            throw new InvalidDataException(
                $"The response for subject '{subject}' exceeds the NATS server's max_payload and cannot be delivered over the NATS channel: {ex.Message}",
                ex);
        }
    }

    /// <summary>Runs the SubscribeAsync operation.</summary>
    public async Task<INatsChannelSubscription> SubscribeAsync(string subject, CancellationToken cancellationToken)
    {
        var subscription = await _raw.SubscribeAsync(subject, cancellationToken).ConfigureAwait(false);
        return new NatsChannelSubscription(subscription, _raw);
    }

    /// <summary>Runs the FlushAsync operation.</summary>
    public Task FlushAsync(CancellationToken cancellationToken) => _raw.FlushAsync(cancellationToken);

    private sealed class NatsChannelSubscription(INatsSub<string> _subscription, INatsRawRequester _raw) : INatsChannelSubscription
    {
        /// <summary>Runs the ReadAsync operation.</summary>
        public async IAsyncEnumerable<NatsInboundResponse> ReadAsync([EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await foreach (var message in _subscription.Msgs.ReadAllAsync(cancellationToken).ConfigureAwait(false))
            {
                var isProbe = message.Headers is { } headers
                    && headers.TryGetValue(NatsChannelHeaders.Probe, out var marker)
                    && marker == "1";

                var replyTo = message.ReplyTo;
                ValueTask Reply()
                    => string.IsNullOrEmpty(replyTo)
                        ? ValueTask.CompletedTask
                        : _raw.PublishReplyAsync(replyTo, CancellationToken.None);

                yield return new NatsInboundResponse(message.Data, isProbe, Reply);
            }
        }

        /// <summary>Releases resources held by this instance.</summary>
        public ValueTask DisposeAsync() => _subscription.DisposeAsync();
    }
}

/// <summary>
/// Thin abstraction over the NATS JetStream Key-Value operations the recovery store needs, confining
/// the NATS.Net API surface to one place so the recovery store is unit-testable against a fake/mock.
/// The backing bucket is created lazily on first use.
/// </summary>
internal interface INatsKvStore
{
    /// <summary>Creates <paramref name="key"/> only when absent; <c>false</c> when it already exists.</summary>
    Task<bool> TryCreateAsync(string key, string value, CancellationToken cancellationToken);

    /// <summary>Replaces <paramref name="key"/> only while its revision still equals <paramref name="expectedRevision"/>; <c>false</c> on a conflict.</summary>
    Task<bool> TryUpdateAsync(string key, string value, ulong expectedRevision, CancellationToken cancellationToken);

    /// <summary>Returns the stored entry (value plus revision) for <paramref name="key"/>, or <c>null</c> when absent or deleted.</summary>
    Task<NatsKvEntry?> GetAsync(string key, CancellationToken cancellationToken);

    /// <summary>Deletes <paramref name="key"/> only while its revision still equals <paramref name="expectedRevision"/>; <c>false</c> on a conflict.</summary>
    Task<bool> TryDeleteAsync(string key, ulong expectedRevision, CancellationToken cancellationToken);

    /// <summary>Streams the live (non-deleted) keys in the bucket.</summary>
    IAsyncEnumerable<string> GetKeysAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Removes the delete markers older than <paramref name="olderThan"/> that removals left in the
    /// bucket, returning how many messages were purged. Never removes a value written after a marker.
    /// </summary>
    Task<long> PurgeDeleteMarkersAsync(TimeSpan olderThan, CancellationToken cancellationToken);
}

/// <summary>A stored value together with the KV revision it was read at, for optimistic conditional writes.</summary>
internal readonly record struct NatsKvEntry(string Value, ulong Revision);

/// <summary>
/// Production <see cref="INatsKvStore"/> over a NATS JetStream Key-Value bucket. The bucket
/// (<c>{RecoveryBucket}</c>, backed by stream <c>KV_{RecoveryBucket}</c>) is created on first use,
/// when it does not exist yet, with a <c>MaxAge</c> ceiling equal to
/// <see cref="AsyncResponseChannelOptions.RecoveryStateExpiry"/>; an existing bucket is used as it is.
/// </summary>
internal sealed class NatsKvStoreAdapter(INatsKVContext _kvContext, NatsAsyncResponseChannelOptions _options, ILogger? _logger = null) : INatsKvStore
{
    // JetStream ApiError.ErrCode for "stream name already in use with a different configuration".
    private const int StreamNameInUseErrCode = 10058;

    private readonly SemaphoreSlim _initGate = new(1, 1);
    private INatsKVStore? _store;

    /// <summary>Runs the TryCreateAsync operation.</summary>
    public async Task<bool> TryCreateAsync(string key, string value, CancellationToken cancellationToken)
    {
        var store = await GetStoreAsync(cancellationToken).ConfigureAwait(false);
        var result = await store.TryCreateAsync(key, value, cancellationToken: cancellationToken).ConfigureAwait(false);
        return result.Success;
    }

    /// <summary>Runs the TryUpdateAsync operation.</summary>
    public async Task<bool> TryUpdateAsync(string key, string value, ulong expectedRevision, CancellationToken cancellationToken)
    {
        var store = await GetStoreAsync(cancellationToken).ConfigureAwait(false);
        var result = await store.TryUpdateAsync(key, value, expectedRevision, cancellationToken: cancellationToken).ConfigureAwait(false);
        return result.Success;
    }

    /// <summary>Runs the GetAsync operation.</summary>
    public async Task<NatsKvEntry?> GetAsync(string key, CancellationToken cancellationToken)
    {
        var store = await GetStoreAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var entry = await store.GetEntryAsync<string>(key, cancellationToken: cancellationToken).ConfigureAwait(false);
            return entry.Value is null ? null : new NatsKvEntry(entry.Value, entry.Revision);
        }
        catch (NatsKVKeyNotFoundException)
        {
            return null;
        }
        catch (NatsKVKeyDeletedException)
        {
            return null;
        }
    }

    /// <summary>Runs the TryDeleteAsync operation.</summary>
    public async Task<bool> TryDeleteAsync(string key, ulong expectedRevision, CancellationToken cancellationToken)
    {
        var store = await GetStoreAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await store.DeleteAsync(
                key,
                new NatsKVDeleteOpts { Revision = expectedRevision },
                cancellationToken: cancellationToken).ConfigureAwait(false);
            return true;
        }
        catch (NatsKVWrongLastRevisionException)
        {
            return false;
        }
        catch (NatsKVKeyNotFoundException)
        {
            return false;
        }
        catch (NatsKVKeyDeletedException)
        {
            return false;
        }
    }

    /// <summary>Runs the GetKeysAsync operation.</summary>
    public async IAsyncEnumerable<string> GetKeysAsync([EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var store = await GetStoreAsync(cancellationToken).ConfigureAwait(false);
        await foreach (var key in store.GetKeysAsync(cancellationToken: cancellationToken).ConfigureAwait(false))
            yield return key;
    }

    /// <summary>
    /// Bounded number of marker purges in flight at once: one JetStream round trip each, so a
    /// serial pass over a busy bucket's markers would take many times longer than it needs to.
    /// </summary>
    private const int MarkerPurgeConcurrency = 8;

    /// <summary>
    /// Removing a key's last registration writes a KV delete marker, and with <c>History = 1</c>
    /// that marker stays until the bucket's <c>MaxAge</c> — so storage, server subject state and
    /// every watchdog scan (which walks the markers too) grew with throughput × RecoveryStateExpiry.
    /// Each marker is purged with a SEQUENCE-bounded subject purge: only the marker and anything
    /// older on its subject go, so a registration a waiter wrote under the same key after the marker
    /// always survives. (NATS.Net's own PurgeDeletesAsync purges the whole subject, which races with
    /// exactly that re-registration, and collects every marker into memory before purging any.)
    /// </summary>
    public async Task<long> PurgeDeleteMarkersAsync(TimeSpan olderThan, CancellationToken cancellationToken)
    {
        var store = await GetStoreAsync(cancellationToken).ConfigureAwait(false);
        var stream = "KV_" + _options.RecoveryBucket;
        var subjectPrefix = "$KV." + _options.RecoveryBucket + ".";
        // Marker timestamps are the server's; the threshold dwarfs any clock skew between the two.
        var cutoff = DateTimeOffset.UtcNow - olderThan;
        long purged = 0;

        await Parallel.ForEachAsync(
            StaleDeleteMarkersAsync(store, cutoff, cancellationToken),
            new ParallelOptions { MaxDegreeOfParallelism = MarkerPurgeConcurrency, CancellationToken = cancellationToken },
            async (marker, token) =>
            {
                var response = await _kvContext.JetStreamContext.PurgeStreamAsync(
                    stream,
                    new StreamPurgeRequest { Filter = subjectPrefix + marker.Key, Seq = marker.Revision + 1 },
                    token).ConfigureAwait(false);
                Interlocked.Add(ref purged, response.Purged);
            }).ConfigureAwait(false);

        return purged;
    }

    /// <summary>
    /// The markers in a point-in-time snapshot of the bucket (the latest entry per key, metadata
    /// only), older than <paramref name="cutoff"/>. Ends once the snapshot is exhausted instead of
    /// watching for later updates.
    /// </summary>
    private static async IAsyncEnumerable<(string Key, ulong Revision)> StaleDeleteMarkersAsync(
        INatsKVStore store,
        DateTimeOffset cutoff,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var opts = new NatsKVWatchOpts { MetaOnly = true, IgnoreDeletes = false, OnNoData = static _ => new ValueTask<bool>(true) };
        await foreach (var entry in store.WatchAsync<int>(opts: opts, cancellationToken: cancellationToken).ConfigureAwait(false))
        {
            if (entry.Operation is NatsKVOperation.Del or NatsKVOperation.Purge && entry.Created <= cutoff)
                yield return (entry.Key, entry.Revision);

            if (entry.Delta == 0)
                yield break;
        }
    }

    private async ValueTask<INatsKVStore> GetStoreAsync(CancellationToken cancellationToken)
    {
        if (_store is not null)
            return _store;

        await _initGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            _store ??= await OpenOrCreateStoreAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _initGate.Release();
        }

        return _store;
    }

    /// <summary>
    /// Opens the recovery bucket, creating it only when it does not exist. A create-only
    /// <c>CreateStoreAsync</c> ran on every first use, and JetStream answers a create whose
    /// configuration differs from the live bucket with 10058 "stream name already in use with a
    /// different configuration" — so raising <see cref="AsyncResponseChannelOptions.RecoveryStateExpiry"/>
    /// or <see cref="NatsAsyncResponseChannelOptions.RecoveryBucketReplicas"/>, or an operator
    /// pre-creating the bucket with its own replica count, failed every save and every
    /// lost-subscriber read: a full channel outage after a routine configuration change. An
    /// existing bucket is used as it is; drift that matters is reported, never overwritten (the
    /// transport's streams follow the same rule).
    /// </summary>
    private async Task<INatsKVStore> OpenOrCreateStoreAsync(CancellationToken cancellationToken)
    {
        var existing = await TryGetStoreAsync(cancellationToken).ConfigureAwait(false);
        if (existing is null)
        {
            try
            {
                // Creating with a configuration identical to the live one is a JetStream no-op, so
                // replicas of one deployment racing here all succeed.
                return await _kvContext.CreateStoreAsync(
                    new NatsKVConfig(_options.RecoveryBucket)
                    {
                        MaxAge = _options.RecoveryStateExpiry,
                        History = 1,
                        NumberOfReplicas = _options.RecoveryBucketReplicas
                    },
                    cancellationToken).ConfigureAwait(false);
            }
            catch (NatsJSApiException ex) when (ex.Error.ErrCode == StreamNameInUseErrCode)
            {
                // A peer configured differently (mid-rollout) won the creation race: from here on
                // it is an existing bucket like any other.
                existing = await TryGetStoreAsync(cancellationToken).ConfigureAwait(false);
                if (existing is null)
                    throw;
            }
        }

        await ReportDriftAsync(existing, cancellationToken).ConfigureAwait(false);
        return existing;
    }

    private async Task<INatsKVStore?> TryGetStoreAsync(CancellationToken cancellationToken)
    {
        try
        {
            return await _kvContext.GetStoreAsync(_options.RecoveryBucket, cancellationToken).ConfigureAwait(false);
        }
        catch (NatsJSApiException ex) when (ex.Error.Code == 404)
        {
            return null; // "stream not found" — the only answer that means the bucket may be created
        }
    }

    /// <summary>
    /// Reports the differences between a live bucket and these options that change behaviour.
    /// Advisory: a bucket whose configuration cannot be read is used regardless.
    /// </summary>
    private async Task ReportDriftAsync(INatsKVStore store, CancellationToken cancellationToken)
    {
        if (_logger is null)
            return;

        StreamConfig config;
        try
        {
            config = (await store.GetStatusAsync(cancellationToken).ConfigureAwait(false)).Info.Config;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogDebug(ex, "Could not read the configuration of the NATS recovery bucket {Bucket}; skipping its drift check.", _options.RecoveryBucket);
            return;
        }

        // MaxAge is the bucket's garbage-collection ceiling. Below RecoveryStateExpiry it silently
        // shortens recovery: a registration is collected before its own expiry, and a response
        // arriving after that finds no callback to resume or fail the flow.
        if (config.MaxAge > TimeSpan.Zero && config.MaxAge < _options.RecoveryStateExpiry)
        {
            _logger.LogWarning(
                "The NATS recovery bucket {Bucket} already exists with max age {MaxAge}, shorter than RecoveryStateExpiry ({RecoveryStateExpiry}): registrations are collected before they expire, so a response arriving later than {MaxAge} finds no recovery callback. " +
                "An existing bucket is never modified by this library — raise the bucket's max age, or lower RecoveryStateExpiry.",
                _options.RecoveryBucket,
                config.MaxAge,
                _options.RecoveryStateExpiry,
                config.MaxAge);
        }

        if (config.NumReplicas != _options.RecoveryBucketReplicas)
        {
            _logger.LogWarning(
                "The NATS recovery bucket {Bucket} already exists with {Replicas} replica(s); RecoveryBucketReplicas is {DesiredReplicas}. " +
                "An existing bucket is never modified by this library — apply the change to the bucket yourself, or align the option with it.",
                _options.RecoveryBucket,
                config.NumReplicas,
                _options.RecoveryBucketReplicas);
        }
    }
}
