using Microsoft.Extensions.Logging;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;

namespace AsyncResponse.Transports.NATS;

/// <summary>A worker/response message pulled from a JetStream consumer, decoupled from NATS client types for testability.</summary>
/// <param name="Subject">The subject the message was published to.</param>
/// <param name="Payload">The raw JSON body.</param>
/// <param name="Headers">The message headers (correlation id, etc.).</param>
/// <param name="NumDelivered">How many times JetStream has delivered this message (1 on first delivery).</param>
/// <param name="AckAsync">Acknowledges the message so it is not redelivered.</param>
/// <param name="NakAsync">Negatively acknowledges the message, requesting redelivery after the given delay.</param>
/// <param name="TermAsync">Terminates the message so JetStream stops redelivering it (used after dead-lettering).</param>
internal sealed record NatsJobDelivery(
    string Subject,
    string Payload,
    IReadOnlyDictionary<string, string> Headers,
    long NumDelivered,
    Func<ValueTask> AckAsync,
    Func<TimeSpan, ValueTask> NakAsync,
    Func<ValueTask> TermAsync)
{
    /// <summary>
    /// Signals "working on it" (JetStream in-progress) so the server resets this delivery's
    /// AckWait window without settling it or bumping its delivery count. An init property with a
    /// no-op default rather than a positional parameter so out-of-package constructions stay
    /// source-compatible. The token is the batch's renewal cancellation: a heartbeat still in
    /// flight when the batch settles (or the subscriber stops) must abort with it, not hold the
    /// batch — the SDK call it wraps takes the token for exactly that.
    /// </summary>
    public Func<CancellationToken, ValueTask> ProgressAsync { get; init; } = static _ => ValueTask.CompletedTask;
}

/// <summary>
/// Thin abstraction over the NATS JetStream operations the transport needs, confining the NATS.Net
/// API surface to one place so the worker transport, dispatcher, and subscribers are unit-testable
/// against a fake/mock.
/// </summary>
internal interface INatsJetStreamTransport
{
    /// <summary>
    /// Creates the stream capturing <paramref name="subject"/> when it does not exist. An existing
    /// stream is verified, never rewritten.
    /// </summary>
    Task EnsureStreamAsync(string stream, string subject, long? maxMessages, CancellationToken cancellationToken);

    /// <summary>
    /// Ensures the dead-letter stream exists, with retention suited to a stream nothing consumes.
    /// </summary>
    Task EnsureDeadLetterStreamAsync(string stream, string subject, long? maxMessages, CancellationToken cancellationToken);

    /// <summary>
    /// Creates the durable explicit-ack pull consumer on <paramref name="stream"/> when it does not
    /// exist. An existing consumer is verified, never rewritten — including that it delivers
    /// <paramref name="subject"/>, the subject this transport publishes to. Returns the ack wait the
    /// live consumer actually runs with — <paramref name="ackWait"/> for one this call created, the
    /// consumer's own (its shortest BackOff step when that is shorter) for an existing one — which
    /// is what the in-progress heartbeat must beat.
    /// </summary>
    Task<TimeSpan> EnsureConsumerAsync(string stream, string subject, string durable, TimeSpan ackWait, int maxDeliveryAttempts, CancellationToken cancellationToken);

    /// <summary>Publishes <paramref name="payload"/> to <paramref name="subject"/> via JetStream and returns the assigned sequence.</summary>
    Task<string> PublishAsync(string subject, string payload, IReadOnlyDictionary<string, string>? headers, CancellationToken cancellationToken);

    // The two fetch members carry throwing default implementations so out-of-package fakes that
    // never drive the subscriber read loop keep compiling; every implementation the subscribers
    // actually consume overrides them.

    /// <summary>
    /// Fetches up to <paramref name="maxMessages"/> already-available messages from the durable
    /// consumer and completes immediately (JetStream no-wait fetch) — the batch-drain half of the
    /// subscriber loop.
    /// </summary>
    IAsyncEnumerable<NatsJobDelivery> FetchNoWaitAsync(string stream, string durable, int maxMessages, CancellationToken cancellationToken)
        => throw new NotSupportedException($"{GetType()} does not implement {nameof(FetchNoWaitAsync)}.");

    /// <summary>
    /// Fetches up to <paramref name="maxMessages"/> messages, waiting up to
    /// <paramref name="expires"/> for them to arrive — the idle long-poll half of the subscriber
    /// loop. Completes without error when the wait expires with fewer messages.
    /// </summary>
    IAsyncEnumerable<NatsJobDelivery> FetchAsync(string stream, string durable, int maxMessages, TimeSpan expires, CancellationToken cancellationToken)
        => throw new NotSupportedException($"{GetType()} does not implement {nameof(FetchAsync)}.");
}

/// <summary>Production <see cref="INatsJetStreamTransport"/> over a NATS <see cref="INatsJSContext"/>.</summary>
internal sealed class NatsJetStreamTransportAdapter(INatsJSContext _jetStream, ILogger? _logger = null, int _streamReplicas = 1) : INatsJetStreamTransport
{
    // JetStream ApiError.ErrCode for "stream name already in use with a different configuration".
    private const int StreamNameInUseErrCode = 10058;

    /// <summary>Ensures the required resource exists.</summary>
    public Task EnsureStreamAsync(string stream, string subject, long? maxMessages, CancellationToken cancellationToken)
    {
        var config = new StreamConfig(stream, [subject])
        {
            MaxMsgs = maxMessages ?? -1,
            // Work-queue retention removes each message once it is acked, so the stream only ever
            // holds the unprocessed backlog. Limits retention kept acked messages forever, letting
            // MaxMsgs eviction silently discard the oldest *unprocessed* jobs once the cap filled
            // up with already-acked traffic.
            Retention = StreamConfigRetention.Workqueue,
            // If the unprocessed backlog itself reaches MaxMsgs, refuse new publishes (a failed
            // PubAck the publisher's retry/exception path surfaces) instead of silently evicting
            // the oldest pending jobs.
            Discard = StreamConfigDiscard.New,
            NumReplicas = _streamReplicas
        };
        return EnsureStreamAsync(stream, subject, config, retentionIsRequired: true, cancellationToken);
    }

    /// <summary>Ensures the dead-letter stream exists.</summary>
    public Task EnsureDeadLetterStreamAsync(string stream, string subject, long? maxMessages, CancellationToken cancellationToken)
    {
        // NOT the work-queue config above: nothing ever consumes (so nothing ever acks) the
        // dead-letter subject, which means work-queue retention removes nothing and Discard=New
        // then rejects every burial once MaxMsgs fills — each over-cap poison message NAK-looping
        // forever because its burial can never be accepted. Limits retention with Discard=Old
        // makes the DLQ a bounded evict-oldest archive, the same shape as Redis's MAXLEN-trimmed
        // dead-letter stream.
        var config = new StreamConfig(stream, [subject])
        {
            MaxMsgs = maxMessages ?? -1,
            Retention = StreamConfigRetention.Limits,
            Discard = StreamConfigDiscard.Old,
            NumReplicas = _streamReplicas
        };

        // Retention is NOT required here: a DLQ provisioned by an earlier build (work-queue
        // retention) cannot be changed in place — JetStream makes retention immutable — and it
        // still accepts burials until it fills; failing the whole subscriber over it would be
        // worse. Keep running and tell the operator how to migrate.
        return EnsureStreamAsync(stream, subject, config, retentionIsRequired: false, cancellationToken);
    }

    /// <summary>
    /// Creates the stream when it is missing and otherwise leaves it exactly as it is. This ran as
    /// a create-or-UPDATE with the minimal config above, and a JetStream update replaces the whole
    /// configuration: every subscriber start (and every first publish) reset whatever an operator
    /// had tuned on the live stream — replicas back to 1, max age / max bytes / max message size
    /// back to unlimited, the duplicate window back to its default. An existing stream is only
    /// checked for what this transport cannot work without; the rest of any drift is reported,
    /// never overwritten.
    /// </summary>
    private async Task EnsureStreamAsync(string stream, string subject, StreamConfig desired, bool retentionIsRequired, CancellationToken cancellationToken)
    {
        var existing = await TryGetStreamConfigAsync(stream, cancellationToken).ConfigureAwait(false);
        if (existing is null)
        {
            try
            {
                // Creating with a configuration identical to the live one is a JetStream no-op, so
                // replicas of one deployment racing here all succeed.
                await _jetStream.CreateStreamAsync(desired, cancellationToken).ConfigureAwait(false);
                return;
            }
            catch (NatsJSApiException ex) when (ex.Error.ErrCode == StreamNameInUseErrCode)
            {
                // A peer configured differently (mid-rollout) won the creation race: from here on
                // it is an existing stream like any other.
                existing = await TryGetStreamConfigAsync(stream, cancellationToken).ConfigureAwait(false);
                if (existing is null)
                    throw;
            }
        }

        VerifyExistingStream(stream, subject, existing, desired, retentionIsRequired);
    }

    private async Task<StreamConfig?> TryGetStreamConfigAsync(string stream, CancellationToken cancellationToken)
    {
        try
        {
            var info = await _jetStream.GetStreamAsync(stream, cancellationToken: cancellationToken).ConfigureAwait(false);
            return info.Info.Config;
        }
        catch (NatsJSApiException ex) when (ex.Error.Code == 404)
        {
            return null; // "stream not found" — the only answer that means the stream may be created
        }
    }

    private void VerifyExistingStream(string stream, string subject, StreamConfig existing, StreamConfig desired, bool retentionIsRequired)
    {
        // Nothing this transport publishes would be stored: every publish would fail with "no
        // response from stream" and the consumer would sit on an empty (or someone else's) stream.
        if (existing.Subjects is null || !existing.Subjects.Any(captured => SubjectCaptures(captured, subject)))
        {
            throw new InvalidOperationException(
                $"NATS stream '{stream}' already exists but does not capture subject '{subject}' " +
                $"(it captures: {(existing.Subjects is { Count: > 0 } subjects ? string.Join(", ", subjects) : "none")}). " +
                "An existing stream is never modified by this transport: add the subject to the stream, or configure a different stream name.");
        }

        if (existing.Retention != desired.Retention)
        {
            if (retentionIsRequired)
            {
                // Without work-queue retention acked jobs are never removed: the stream fills with
                // finished work until MaxMsgs rejects every publish (or, with Discard=Old, evicts
                // jobs nobody has run yet). Retention cannot be changed on a live stream, so there
                // is nothing to repair here — fail, as the rejected update did before.
                throw new InvalidOperationException(
                    $"NATS stream '{stream}' already exists with {existing.Retention} retention; this transport requires {desired.Retention} retention, " +
                    "and JetStream does not allow changing the retention policy of an existing stream. " +
                    "Delete the stream (after draining it) so this host can recreate it, or configure a different stream name.");
            }

            _logger?.LogWarning(
                "The NATS dead-letter stream {Stream} has {Retention} retention instead of limits retention (an existing stream's retention policy is immutable). " +
                "It keeps its current configuration; to migrate, delete and let this host recreate it (its messages are dead letters — export first if needed).",
                stream,
                existing.Retention);
        }

        if (existing.Discard != desired.Discard || existing.MaxMsgs != desired.MaxMsgs)
        {
            _logger?.LogWarning(
                "NATS stream {Stream} already exists with discard={Discard}, max_msgs={MaxMsgs}; this host is configured for discard={DesiredDiscard}, max_msgs={DesiredMaxMsgs}. " +
                "An existing stream is never modified by this transport — apply the change to the stream yourself, or align the transport options with it.",
                stream,
                existing.Discard,
                existing.MaxMsgs,
                desired.Discard,
                desired.MaxMsgs);
        }
    }

    /// <summary>NATS subject matching: <c>*</c> stands for exactly one token, a trailing <c>&gt;</c> for one or more.</summary>
    internal static bool SubjectCaptures(string captured, string subject)
    {
        var capturedTokens = captured.Split('.');
        var subjectTokens = subject.Split('.');
        for (var i = 0; i < capturedTokens.Length; i++)
        {
            if (capturedTokens[i] == ">")
                return i < subjectTokens.Length;

            if (i >= subjectTokens.Length)
                return false;

            if (capturedTokens[i] != "*" && !string.Equals(capturedTokens[i], subjectTokens[i], StringComparison.Ordinal))
                return false;
        }

        return capturedTokens.Length == subjectTokens.Length;
    }

    /// <summary>
    /// Creates the consumer when it is missing and otherwise leaves it exactly as it is. This ran
    /// as a create-or-UPDATE with the minimal config below on every subscriber attempt (every
    /// fast-empty rebuild included), even with CreateStreams off, and a JetStream update replaces
    /// the whole configuration: an operator's MaxAckPending, BackOff or metadata reverted on every
    /// start, and an operator-provisioned durable differing in an immutable field failed every
    /// start forever. An existing consumer is only checked for what this transport cannot work
    /// without (the streams follow the same rule), and its own ack wait is returned so the
    /// heartbeat follows the consumer rather than the options.
    /// </summary>
    public async Task<TimeSpan> EnsureConsumerAsync(string stream, string subject, string durable, TimeSpan ackWait, int maxDeliveryAttempts, CancellationToken cancellationToken)
    {
        var existing = await TryGetConsumerConfigAsync(stream, durable, cancellationToken).ConfigureAwait(false);
        if (existing is null)
        {
            var config = new ConsumerConfig(durable)
            {
                DurableName = durable,
                AckPolicy = ConsumerConfigAckPolicy.Explicit,
                AckWait = ackWait,
                // Redelivery attempts are bounded by the dispatcher (via NumDelivered + Terminate), so the
                // consumer itself is left unlimited rather than silently swallowing the last attempt.
                MaxDeliver = -1
            };

            try
            {
                // Create-only: creating with a configuration identical to the live one is a
                // JetStream no-op, so replicas of one deployment racing here all succeed.
                await _jetStream.CreateConsumerAsync(stream, config, cancellationToken).ConfigureAwait(false);
                return ackWait;
            }
            catch (NatsJSApiException)
            {
                // A peer configured differently won the creation race: from here on it is an
                // existing consumer like any other. Anything else is still missing, so rethrow.
                existing = await TryGetConsumerConfigAsync(stream, durable, cancellationToken).ConfigureAwait(false);
                if (existing is null)
                    throw;
            }
        }

        VerifyExistingConsumer(stream, subject, durable, existing, maxDeliveryAttempts);

        // The server reports its resolved ack wait (30 s when none was set); a non-positive value
        // would only come from a non-conforming server, and then the options are all there is.
        var liveAckWait = existing.AckWait > TimeSpan.Zero ? existing.AckWait : ackWait;

        // A BackOff consumer enforces BackOff[n] as the ack wait of its n-th redelivery (the last
        // step for every later one) and reports only BackOff[0] as its AckWait. A non-ascending
        // BackOff — [30s, 5s] — gave a redelivered message a 5 s window while the heartbeat renewed
        // every 10 s from the reported 30 s, so it lapsed under a live handler and redelivered to a
        // peer. The heartbeat must beat the shortest window any delivery can get.
        if (existing.Backoff is { Count: > 0 } backoff)
        {
            foreach (var step in backoff)
            {
                if (step > TimeSpan.Zero && step < liveAckWait)
                    liveAckWait = step;
            }
        }

        if (liveAckWait != ackWait)
            ReportAckWaitDrift(stream, durable, liveAckWait, ackWait);

        return liveAckWait;
    }

    // Consumers whose ack-wait drift has been reported, with the live value reported: this runs on
    // every subscriber attempt (fast-empty rebuilds included), and one warning per drift is enough.
    private readonly ConcurrentDictionary<(string Stream, string Durable), TimeSpan> _reportedAckWaitDrift = new();

    /// <summary>
    /// A live ack wait that differs from <see cref="NatsAsyncResponseTransportOptions.AckWait"/> is
    /// not an error: the heartbeat renews at a third of the SHORTER of the two, so it always lands
    /// inside the consumer's real window. This was a throw when the live value sat at or below a
    /// third of the configured one — so raising AckWait (30 s to 2 min, for long handlers) failed
    /// every subscriber attempt inside the supervisor, which retried it at Warning forever while
    /// nothing consumed; and just above that line the heartbeat ran from the configured value
    /// against the shorter live window, one stall away from redelivering under live handlers.
    /// </summary>
    private void ReportAckWaitDrift(string stream, string durable, TimeSpan liveAckWait, TimeSpan ackWait)
    {
        var key = (stream, durable);
        if (_reportedAckWaitDrift.TryGetValue(key, out var reported) && reported == liveAckWait)
            return;

        _reportedAckWaitDrift[key] = liveAckWait;
        _logger?.LogWarning(
            "NATS consumer {Consumer} on stream {Stream} already exists with ack wait {AckWait} (its shortest BackOff step, when it has a shorter one); this host is configured for {DesiredAckWait}. " +
            "The in-progress heartbeat follows the shorter of the two. An existing consumer is never modified by this transport — apply the change to the consumer yourself, or align AckWait with it.",
            durable,
            stream,
            liveAckWait,
            ackWait);
    }

    private async Task<ConsumerConfig?> TryGetConsumerConfigAsync(string stream, string durable, CancellationToken cancellationToken)
    {
        try
        {
            var consumer = await _jetStream.GetConsumerAsync(stream, durable, cancellationToken).ConfigureAwait(false);
            return consumer.Info.Config;
        }
        catch (NatsJSApiException ex) when (ex.Error.Code == 404)
        {
            return null; // "consumer (or stream) not found" — the only answer that means it may be created
        }
    }

    private static void VerifyExistingConsumer(string stream, string subject, string durable, ConsumerConfig existing, int maxDeliveryAttempts)
    {
        const string NeverModified = "An existing consumer is never modified by this transport: fix it, or delete it so this host recreates it, or configure a different consumer name.";

        // Every fetch here is a pull request, and every settlement an explicit ack/nak/term.
        if (!string.IsNullOrEmpty(existing.DeliverSubject))
        {
            throw new InvalidOperationException(
                $"NATS consumer '{durable}' on stream '{stream}' already exists as a push consumer (deliver subject '{existing.DeliverSubject}'); this transport needs a pull consumer. {NeverModified}");
        }

        if (existing.AckPolicy != ConsumerConfigAckPolicy.Explicit)
        {
            throw new InvalidOperationException(
                $"NATS consumer '{durable}' on stream '{stream}' already exists with ack policy {existing.AckPolicy}; this transport settles every message explicitly and needs ack policy {ConsumerConfigAckPolicy.Explicit}. {NeverModified}");
        }

        // The dispatcher bounds attempts itself: it dead-letters a delivery arriving past
        // MaxDeliveryAttempts before running it. A server-side MaxDeliver at or below that cap
        // stops redelivering first, so a message whose attempts all died with the process sits
        // unacknowledged on the stream forever, never dead-lettered.
        if (existing.MaxDeliver > 0 && (maxDeliveryAttempts <= 0 || existing.MaxDeliver <= maxDeliveryAttempts))
        {
            throw new InvalidOperationException(
                $"NATS consumer '{durable}' on stream '{stream}' already exists with max deliver {existing.MaxDeliver}, which stops redelivery before MaxDeliveryAttempts " +
                $"({(maxDeliveryAttempts <= 0 ? "unlimited" : maxDeliveryAttempts)}) can dead-letter the message; it needs unlimited (-1) or more than MaxDeliveryAttempts. {NeverModified}");
        }

        // A headers-only consumer delivers every message without its body. The ingress reads an
        // empty body as unparsable and acknowledges it without dispatch, so every job was removed
        // from the work-queue stream with no NAK and no dead-letter copy.
        if (existing.HeadersOnly)
        {
            throw new InvalidOperationException(
                $"NATS consumer '{durable}' on stream '{stream}' already exists as headers-only; this transport needs every message's body. {NeverModified}");
        }

        // A consumer filtered to other subjects never delivers what this transport publishes: its
        // long polls expire empty as if the stream were idle, with no error and no warning, while
        // the jobs pile up on the stream.
        var filters = new List<string>();
        if (!string.IsNullOrEmpty(existing.FilterSubject))
            filters.Add(existing.FilterSubject);
        if (existing.FilterSubjects is not null)
            filters.AddRange(existing.FilterSubjects.Where(filter => !string.IsNullOrEmpty(filter)));

        if (filters.Count > 0 && !filters.Any(filter => SubjectCaptures(filter, subject)))
        {
            throw new InvalidOperationException(
                $"NATS consumer '{durable}' on stream '{stream}' already exists filtered to {string.Join(", ", filters)}, which does not deliver subject '{subject}' " +
                $"this transport publishes to. {NeverModified}");
        }
    }

    /// <summary>Publishes the supplied message.</summary>
    public async Task<string> PublishAsync(string subject, string payload, IReadOnlyDictionary<string, string>? headers, CancellationToken cancellationToken)
    {
        var ack = await _jetStream.PublishAsync(
            subject,
            payload,
            headers: ToHeaders(headers),
            cancellationToken: cancellationToken).ConfigureAwait(false);

        EnsureAccepted(ack);

        return ack.Seq.ToString();
    }

    /// <summary>
    /// Accepts a JetStream publish ack. Deliberately NOT <c>ack.EnsureSuccess()</c>: that treats
    /// <c>PubAck.Duplicate</c> as a failure and throws NatsJSDuplicateMessageException.
    /// Duplicate=true means JetStream already holds this Nats-Msg-Id — the SUCCESS case for the
    /// stable id the worker transport stamps outside its retry loop, precisely so a retry after a
    /// lost PubAck is deduplicated rather than enqueuing the same worker job twice. Throwing there
    /// burned the whole retry ladder (every attempt gets the same answer — it is a JetStream
    /// decision, not a blip) and reported a publish failure for a job that is queued and WILL run,
    /// so the caller re-published under a fresh id and the job executed twice. Only a real API
    /// error is a failure.
    /// </summary>
    internal static void EnsureAccepted(PubAckResponse ack)
    {
        if (ack.Error is not null)
            throw new NatsJSApiException(ack.Error);
    }

    /// <summary>Runs the FetchNoWaitAsync operation.</summary>
    public async IAsyncEnumerable<NatsJobDelivery> FetchNoWaitAsync(
        string stream,
        string durable,
        int maxMessages,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var consumer = await GetConsumerAsync(stream, durable, cancellationToken).ConfigureAwait(false);
        var fetchOpts = new NatsJSFetchOpts { MaxMsgs = maxMessages };

        await foreach (var message in consumer.FetchNoWaitAsync<string>(opts: fetchOpts, cancellationToken: cancellationToken).ConfigureAwait(false))
            yield return ToDelivery(message);
    }

    /// <summary>Runs the FetchAsync operation.</summary>
    public async IAsyncEnumerable<NatsJobDelivery> FetchAsync(
        string stream,
        string durable,
        int maxMessages,
        TimeSpan expires,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var consumer = await GetConsumerAsync(stream, durable, cancellationToken).ConfigureAwait(false);
        var fetchOpts = new NatsJSFetchOpts { MaxMsgs = maxMessages, Expires = expires };

        await foreach (var message in consumer.FetchAsync<string>(opts: fetchOpts, cancellationToken: cancellationToken).ConfigureAwait(false))
            yield return ToDelivery(message);
    }

    // The consumer wrapper only carries names for building pull requests, so it stays valid across
    // subscriber rebuilds (EnsureConsumerAsync recreates the durable if it was deleted server-side)
    // and is cached to avoid a consumer-INFO round trip per fetch. Only a SUCCESSFUL lookup is
    // cached, so a transient failure is not replayed forever.
    private readonly ConcurrentDictionary<(string Stream, string Durable), INatsJSConsumer> _consumers = new();

    private async ValueTask<INatsJSConsumer> GetConsumerAsync(string stream, string durable, CancellationToken cancellationToken)
    {
        if (_consumers.TryGetValue((stream, durable), out var cached))
            return cached;

        var consumer = await _jetStream.GetConsumerAsync(stream, durable, cancellationToken).ConfigureAwait(false);
        return _consumers.GetOrAdd((stream, durable), consumer);
    }

    private static NatsJobDelivery ToDelivery(INatsJSMsg<string> message)
    {
        var numDelivered = (long)(message.Metadata?.NumDelivered ?? 1);
        var captured = message;

        return new NatsJobDelivery(
            captured.Subject,
            captured.Data ?? string.Empty,
            FromHeaders(captured.Headers),
            numDelivered,
            () => captured.AckAsync(cancellationToken: CancellationToken.None),
            delay => captured.NakAsync(delay: delay, cancellationToken: CancellationToken.None),
            () => captured.AckTerminateAsync(cancellationToken: CancellationToken.None))
        {
            // Unlike the settlements above (deliberately uncancelable: a settlement decision
            // already taken must reach the server), a progress heartbeat is advisory — the
            // renewal loop's token cancels one that stalls, so the batch never waits on it.
            ProgressAsync = cancellationToken => captured.AckProgressAsync(cancellationToken: cancellationToken)
        };
    }

    private static NatsHeaders? ToHeaders(IReadOnlyDictionary<string, string>? headers)
    {
        if (headers is null || headers.Count == 0)
            return null;

        var natsHeaders = new NatsHeaders();
        foreach (var (key, value) in headers)
            natsHeaders[key] = value;
        return natsHeaders;
    }

    private static IReadOnlyDictionary<string, string> FromHeaders(NatsHeaders? headers)
    {
        if (headers is null || headers.Count == 0)
            return EmptyHeaders;

        var result = new Dictionary<string, string>(headers.Count, StringComparer.OrdinalIgnoreCase);
        foreach (var key in headers.Keys)
            result[key] = headers[key].ToString();
        return result;
    }

    private static readonly IReadOnlyDictionary<string, string> EmptyHeaders =
        new Dictionary<string, string>(0, StringComparer.OrdinalIgnoreCase);
}

/// <summary>Bounded exponential-backoff retry for transient NATS failures, mirroring the other transports.</summary>
internal static class NatsTransportRetry
{
    /// <summary>Runs this background operation until cancellation is requested.</summary>
    public static Task<T> ExecuteAsync<T>(
        Func<CancellationToken, Task<T>> action,
        int maxAttempts,
        TimeSpan baseDelay,
        TimeSpan maxDelay,
        CancellationToken cancellationToken)
        => AsyncResponseRetry.ExecuteAsync(action, IsTransient, maxAttempts, baseDelay, maxDelay, cancellationToken);

    /// <summary>Runs the IsTransient operation.</summary>
    public static bool IsTransient(Exception exception)
    {
        // A JetStream API request that the server ANSWERED with an error is a decision, not a
        // blip: "stream name already in use", "consumer config would change an immutable field",
        // "no permission". Those repeat identically on every attempt, so retrying only delays the
        // report — unless the server itself said it was temporarily unable (5xx, e.g. 503 while a
        // meta-leader election settles). Everything else in the NatsException family — no
        // responders, no API response, connection loss — is the transient case.
        if (exception is NatsJSApiException api)
            return api.Error.Code >= 500;

        // The client refuses a message above the server's max_payload before sending it, on every
        // attempt alike: a decision, not a blip, even though it is a NatsException too.
        if (exception is NatsPayloadTooLargeException)
            return false;

        return exception is NatsException or TimeoutException && exception is not OperationCanceledException;
    }
}
