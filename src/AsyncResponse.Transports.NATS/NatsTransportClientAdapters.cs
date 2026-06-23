using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
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
    Func<ValueTask> TermAsync);

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

    /// <summary>Continuously consumes messages from the durable consumer on <paramref name="stream"/>.</summary>
    IAsyncEnumerable<NatsJobDelivery> ConsumeAsync(string stream, string durable, int batchSize, CancellationToken cancellationToken);
}

/// <summary>Production <see cref="INatsJetStreamTransport"/> over a NATS <see cref="INatsJSContext"/>.</summary>
internal sealed class NatsJetStreamTransportAdapter(INatsJSContext _jetStream) : INatsJetStreamTransport
{
    public async Task EnsureStreamAsync(string stream, string subject, long? maxMessages, CancellationToken cancellationToken)
    {
        var config = new StreamConfig(stream, [subject])
        {
            MaxMsgs = maxMessages ?? -1,
            Retention = StreamConfigRetention.Limits
        };
        await _jetStream.CreateOrUpdateStreamAsync(config, cancellationToken).ConfigureAwait(false);
    }

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

    public async Task<string> PublishAsync(string subject, string payload, IReadOnlyDictionary<string, string>? headers, CancellationToken cancellationToken)
    {
        var ack = await _jetStream.PublishAsync(
            subject,
            payload,
            headers: ToHeaders(headers),
            cancellationToken: cancellationToken).ConfigureAwait(false);
        ack.EnsureSuccess();
        return ack.Seq.ToString();
    }

    public async IAsyncEnumerable<NatsJobDelivery> ConsumeAsync(
        string stream,
        string durable,
        int batchSize,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var consumer = await _jetStream.GetConsumerAsync(stream, durable, cancellationToken).ConfigureAwait(false);
        var consumeOpts = new NatsJSConsumeOpts { MaxMsgs = batchSize };

        await foreach (var message in consumer.ConsumeAsync<string>(opts: consumeOpts, cancellationToken: cancellationToken).ConfigureAwait(false))
        {
            var numDelivered = (long)(message.Metadata?.NumDelivered ?? 1);
            var captured = message;

            yield return new NatsJobDelivery(
                captured.Subject,
                captured.Data ?? string.Empty,
                FromHeaders(captured.Headers),
                numDelivered,
                () => captured.AckAsync(cancellationToken: CancellationToken.None),
                delay => captured.NakAsync(delay: delay, cancellationToken: CancellationToken.None),
                () => captured.AckTerminateAsync(cancellationToken: CancellationToken.None));
        }
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
    public static Task<T> ExecuteAsync<T>(
        Func<CancellationToken, Task<T>> action,
        int maxAttempts,
        TimeSpan baseDelay,
        TimeSpan maxDelay,
        CancellationToken cancellationToken)
        => AsyncResponseRetry.ExecuteAsync(action, IsTransient, maxAttempts, baseDelay, maxDelay, cancellationToken);

    public static bool IsTransient(Exception exception)
        => exception is NatsException or TimeoutException && exception is not OperationCanceledException;
}
