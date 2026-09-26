using Microsoft.Extensions.Options;
using System.Diagnostics;
using System.Text;
using System.Text.Json;

namespace AsyncResponse.Transports.Kafka;

/// <summary>
/// Publishes <see cref="WorkerJobEnvelope"/> messages to a Kafka topic.
/// </summary>
/// <remarks>
/// The correlation id is carried in a message header and doubles as the partition key, so all jobs
/// of one flow stay ordered within their partition; jobs without a correlation id are spread
/// round-robin. The producer is idempotent with <c>acks=all</c>, and transient broker failures are
/// retried with bounded exponential backoff before the exception is returned to the caller.
/// </remarks>
public sealed class KafkaWorkerTransport : IWorkerTransport
{
    private readonly KafkaAsyncResponseTransportOptions _options;
    private readonly IKafkaProducerClient _producer;
    private readonly KafkaTransportTopicSchema _topics;
    private int _messageMaxBytes;

    /// <summary>Runs the KafkaWorkerTransport operation.</summary>
    public KafkaWorkerTransport(IOptions<KafkaAsyncResponseTransportOptions> options)
        : this(options, new KafkaProducerClientAdapter(KafkaTransportOptionsValidatorWithValue(options)))
    {
    }

    internal KafkaWorkerTransport(
        IOptions<KafkaAsyncResponseTransportOptions> options,
        IKafkaProducerClient producer)
    {
        _options = options.Value;
        KafkaTransportOptionsValidator.ValidateCommon(_options);
        _producer = producer;
        _topics = new KafkaTransportTopicSchema(_options);
    }

    private static KafkaAsyncResponseTransportOptions KafkaTransportOptionsValidatorWithValue(
        IOptions<KafkaAsyncResponseTransportOptions> options)
    {
        KafkaTransportOptionsValidator.ValidateCommon(options.Value);
        return options.Value;
    }

    /// <summary>Publishes the supplied message.</summary>
    public async Task PublishAsync(WorkerJobEnvelope job, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(job);

        using var activity = AsyncResponseDiagnostics.StartActivity(
            "asyncresponse.worker.publish",
            ActivityKind.Producer,
            job.CorrelationId);
        activity?.SetTag("asyncresponse.transport", "kafka");
        activity?.SetTag("messaging.system", "kafka");
        activity?.SetTag("messaging.destination.name", _topics.WorkerTopic);
        AsyncResponseDiagnostics.SetReplyTarget(activity, job.ReplyTarget);
        AsyncResponseDiagnostics.SetWorker(activity, job.Call);

        try
        {
            var payload = Encoding.UTF8.GetBytes(AsyncResponseJson.Serialize(job));
            var headers = CreateMessageHeaders(job.CorrelationId, _options);
            var key = string.IsNullOrWhiteSpace(job.CorrelationId) ? null : job.CorrelationId;
            ThrowIfCannotBeBuried(key, payload, headers);
            var result = await KafkaTransportRetry.ExecuteAsync(
                token => _producer.PublishAsync(
                    _topics.WorkerTopic,
                    key,
                    payload,
                    headers,
                    token),
                _options.PublishMaxAttempts,
                _options.PublishRetryBaseDelay,
                _options.PublishRetryMaxDelay,
                cancellationToken).ConfigureAwait(false);

            activity?.SetTag("messaging.kafka.destination.partition", result.Partition);
            activity?.SetTag("messaging.kafka.message.offset", result.Offset);
        }
        catch (Exception ex)
        {
            AsyncResponseDiagnostics.SetError(activity, ex);
            throw;
        }
    }

    /// <summary>
    /// Refuses a job whose dead-letter copy could never fit the producer's <c>message.max.bytes</c>
    /// (from <see cref="KafkaAsyncResponseTransportOptions.ConfigureProducer"/>, default 1,000,000):
    /// the copy carries the record plus the burial headers, and only the two exception-text headers
    /// can be shortened. A job within that margin of the limit was accepted here, failed its
    /// handler, and could then never be buried — librdkafka rejects the copy locally on every
    /// attempt — so the worker subscriber restarted on it for ever, re-running the handler each
    /// time, with its partition stalled. Checked only while dead-lettering is enabled (without it
    /// nothing is ever copied, and librdkafka still enforces the limit itself).
    /// </summary>
    private void ThrowIfCannotBeBuried(string? key, byte[] payload, IReadOnlyList<KafkaTransportHeader> headers)
    {
        if (!_options.DeadLetterEnabled)
            return;

        var recordBytes = KafkaMessageDispatcher.EstimateRecordSize(key, payload, headers);
        var burialBytes = KafkaMessageDispatcher.BurialOverheadBytes(_topics.WorkerTopic, _options.WorkerConsumerGroup);
        var limit = MessageMaxBytes;
        if (recordBytes + burialBytes > limit)
            throw new KafkaRecordTooLargeException(_topics.WorkerTopic, recordBytes, burialBytes, limit);
    }

    /// <summary>The producer's <c>message.max.bytes</c>, resolved on first publish.</summary>
    private int MessageMaxBytes
    {
        get
        {
            var resolved = Volatile.Read(ref _messageMaxBytes);
            if (resolved == 0)
            {
                resolved = KafkaProducerClientAdapter.ResolveMessageMaxBytes(_options);
                Volatile.Write(ref _messageMaxBytes, resolved);
            }

            return resolved;
        }
    }

    internal static IReadOnlyList<KafkaTransportHeader> CreateMessageHeaders(
        string? correlationId,
        KafkaAsyncResponseTransportOptions options)
    {
        var correlationHeader = KafkaTransportOptionsValidator.Required(
            options.CorrelationIdHeader,
            nameof(options.CorrelationIdHeader));

        return string.IsNullOrWhiteSpace(correlationId)
            ? []
            : [KafkaTransportHeader.Utf8(correlationHeader, correlationId)];
    }
}

/// <summary>
/// A worker job refused before it was produced: the record plus the headers its dead-letter copy
/// would add exceed the producer's <c>message.max.bytes</c>, so a failed run of it could never be
/// buried. Nothing was published; the refusal is deterministic, so it is not retried.
/// </summary>
internal sealed class KafkaRecordTooLargeException(string topic, long recordBytes, long burialBytes, int messageMaxBytes)
    : InvalidOperationException(
        $"The worker job for Kafka topic '{topic}' encodes to about {recordBytes} bytes (key, value and headers), and its dead-letter copy " +
        $"would add up to {burialBytes} bytes of burial headers — over the producer's message.max.bytes ({messageMaxBytes}), so a failed " +
        "run of it could never be buried and the worker subscriber would restart on it for ever. Nothing was published. Put large " +
        "arguments behind a claim check (persist the data and pass a reference), or raise MessageMaxBytes through " +
        $"{nameof(KafkaAsyncResponseTransportOptions)}.{nameof(KafkaAsyncResponseTransportOptions.ConfigureProducer)} (and the topics' " +
        "max.message.bytes on the broker).")
{
    public long RecordBytes { get; } = recordBytes;
    public long BurialBytes { get; } = burialBytes;
    public int MessageMaxBytes { get; } = messageMaxBytes;
}
