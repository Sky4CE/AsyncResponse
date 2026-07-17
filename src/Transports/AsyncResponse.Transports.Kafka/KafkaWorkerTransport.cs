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
            var result = await KafkaTransportRetry.ExecuteAsync(
                token => _producer.PublishAsync(
                    _topics.WorkerTopic,
                    string.IsNullOrWhiteSpace(job.CorrelationId) ? null : job.CorrelationId,
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
