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
    /// source-compatible.
    /// </summary>
    public Func<ValueTask> ProgressAsync { get; init; } = static () => ValueTask.CompletedTask;
}

/// <summary>
/// Thin abstraction over the NATS JetStream operations the transport needs, confining the NATS.Net
/// API surface to one place so the worker transport, dispatcher, and subscribers are unit-testable
/// against a fake/mock.
/// </summary>
internal interface INatsJetStreamTransport
{
    /// <summary>Idempotently creates or updates the stream capturing <paramref name="subject"/>.</summary>
    Task EnsureStreamAsync(string stream, string subject, long? maxMessages, CancellationToken cancellationToken);

    /// <summary>Idempotently creates or updates a durable explicit-ack consumer on <paramref name="stream"/>.</summary>
    Task EnsureConsumerAsync(string stream, string durable, TimeSpan ackWait, CancellationToken cancellationToken);

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
internal sealed class NatsJetStreamTransportAdapter(INatsJSContext _jetStream) : INatsJetStreamTransport
{
    /// <summary>Ensures the required resource exists.</summary>
    public async Task EnsureStreamAsync(string stream, string subject, long? maxMessages, CancellationToken cancellationToken)
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
            Discard = StreamConfigDiscard.New
        };
        await _jetStream.CreateOrUpdateStreamAsync(config, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Ensures the required resource exists.</summary>
    public async Task EnsureConsumerAsync(string stream, string durable, TimeSpan ackWait, CancellationToken cancellationToken)
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
        await _jetStream.CreateOrUpdateConsumerAsync(stream, config, cancellationToken).ConfigureAwait(false);
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
            ProgressAsync = () => captured.AckProgressAsync(cancellationToken: CancellationToken.None)
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

        return exception is NatsException or TimeoutException && exception is not OperationCanceledException;
    }
}
