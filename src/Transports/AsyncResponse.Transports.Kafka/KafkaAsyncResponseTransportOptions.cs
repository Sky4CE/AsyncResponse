using Confluent.Kafka;

namespace AsyncResponse.Transports.Kafka;

/// <summary>
/// Options for the Apache Kafka AsyncResponse transport. Built on classic consumer groups: manual
/// offset management, in-process bounded retries (offsets cannot NACK a single message), and
/// dead-letter topics. Ordering is per-partition, so consumer parallelism equals the partition
/// count and a slow message delays its partition (head-of-line blocking); size
/// <see cref="TopicNumPartitions"/> accordingly.
/// </summary>
public sealed class KafkaAsyncResponseTransportOptions
{
    public const string TransportName = "kafka";

    /// <summary>
    /// Comma-separated Kafka bootstrap servers (for example <c>broker-1:9092,broker-2:9092</c>).
    /// Required.
    /// </summary>
    public string? BootstrapServers { get; set; }

    /// <summary>
    /// Optional Kafka <c>client.id</c> applied to the producer, consumers, and admin client. When
    /// null, the package generates a stable process-local id containing machine name and process id.
    /// </summary>
    public string? ClientId { get; set; }

    /// <summary>
    /// Prefix used when a topic name is not explicitly configured. The default worker topic is
    /// <c>{TopicPrefix}.transport.worker</c> and the default response topic is
    /// <c>{TopicPrefix}.transport.response</c>. Use a unique prefix per app/environment when several
    /// deployments share one cluster.
    /// </summary>
    public string TopicPrefix { get; set; } = "asyncresponse";

    /// <summary>
    /// Kafka topic used by <see cref="KafkaWorkerTransport"/> to publish worker jobs. When null,
    /// <see cref="TopicPrefix"/> determines the topic name.
    /// </summary>
    public string? WorkerTopic { get; set; }

    /// <summary>Consumer group used by the hosted worker subscriber.</summary>
    public string WorkerConsumerGroup { get; set; } = "asyncresponse-workers";

    /// <summary>Worker topic handling options.</summary>
    public KafkaSubscriberOptions WorkerSubscriber { get; } = new();

    /// <summary>
    /// Kafka topic remote systems can produce response payloads to. The hosted response-ingress
    /// subscriber reads this topic and forwards payloads into <see cref="IAsyncResponseIngress"/>.
    /// When null, <see cref="TopicPrefix"/> determines the topic name.
    /// </summary>
    public string? ResponseTopic { get; set; }

    /// <summary>Consumer group used by the hosted response-ingress subscriber.</summary>
    public string ResponseConsumerGroup { get; set; } = "asyncresponse-responses";

    /// <summary>Response topic handling options.</summary>
    public KafkaSubscriberOptions ResponseSubscriber { get; } = new();

    /// <summary>
    /// Creates the worker, response, and dead-letter topics on subscriber startup using
    /// <see cref="TopicNumPartitions"/> and <see cref="TopicReplicationFactor"/>. Existing topics
    /// are left untouched. Disable when your infra team owns topic provisioning.
    /// </summary>
    public bool CreateTopics { get; set; } = true;

    /// <summary>
    /// Partition count used when <see cref="CreateTopics"/> provisions a missing topic. Partitions
    /// are the unit of consumer parallelism and per-message ordering. <c>-1</c> uses the broker
    /// default (<c>num.partitions</c>). Default: <c>8</c>.
    /// </summary>
    public int TopicNumPartitions { get; set; } = 8;

    /// <summary>
    /// Replication factor used when <see cref="CreateTopics"/> provisions a missing topic.
    /// <c>-1</c> uses the broker default (<c>default.replication.factor</c>), which is correct for
    /// single-broker development clusters and lets production clusters keep their own policy.
    /// </summary>
    public short TopicReplicationFactor { get; set; } = -1;

    /// <summary>
    /// Enables dead-lettering when a message exhausts
    /// <see cref="KafkaSubscriberOptions.MaxDeliveryAttempts"/> in-process retries, a background
    /// handler fails after early ACK, or a message cannot be parsed into a delivery. The failing
    /// message is produced to the dead-letter topic with failure-detail headers and its offset is
    /// committed so the partition keeps moving.
    /// </summary>
    public bool DeadLetterEnabled { get; set; } = true;

    /// <summary>
    /// Explicit dead-letter topic receiving poison messages from all subscribed topics. When null,
    /// each source topic dead-letters to <c>{sourceTopic}{DeadLetterTopicSuffix}</c>.
    /// </summary>
    public string? DeadLetterTopic { get; set; }

    /// <summary>
    /// Suffix appended to a source topic to derive its dead-letter topic when
    /// <see cref="DeadLetterTopic"/> is not configured. Default: <c>.deadletter</c>.
    /// </summary>
    public string DeadLetterTopicSuffix { get; set; } = ".deadletter";

    /// <summary>The logical reply target name used by <c>WithReplyTarget()</c>. Default: <c>default</c>.</summary>
    public string DefaultReplyTargetName { get; set; } = "default";

    /// <summary>
    /// Named reply targets exposed to Core through <see cref="IAsyncResponseReplyTargetProvider"/>.
    /// When empty, the resolved <see cref="ResponseTopic"/> becomes the default target.
    /// </summary>
    public Dictionary<string, KafkaReplyTargetOptions> ReplyTargets { get; } = new(StringComparer.Ordinal);

    /// <summary>
    /// Kafka message header carrying the AsyncResponse correlation id. Response messages may omit
    /// it when the id is present in the JSON body via <see cref="CorrelationIdJsonPaths"/>.
    /// </summary>
    public string CorrelationIdHeader { get; set; } = "correlationId";

    /// <summary>
    /// JSON paths inspected when a response message does not carry the correlation id in
    /// <see cref="CorrelationIdHeader"/>. Paths are case-insensitive and support nested JSON strings.
    /// </summary>
    public string[] CorrelationIdJsonPaths { get; set; } =
    [
        "CorrelationId",
        "CustomParameters",
        "CustomParameters.CorrelationId",
        "PubSubParams.CustomParameters",
        "PubSubParams.CustomParameters.CorrelationId",
        "DagJsonParameters.CorrelationId"
    ];

    /// <summary>
    /// How often stored offsets are auto-committed to the broker. Subscribers store an offset only
    /// once its message is fully resolved (handled, enqueued for early ACK, or dead-lettered), so a
    /// crash inside this window redelivers at-least-once rather than losing work. Maps to the
    /// consumer's <c>auto.commit.interval.ms</c>. Default: <c>5s</c>.
    /// </summary>
    public TimeSpan OffsetCommitInterval { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>Timeout applied to administrative operations such as topic creation.</summary>
    public TimeSpan OperationTimeout { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>Maximum attempts for Kafka produce calls. Set to 1 to disable publish retries.</summary>
    public int PublishMaxAttempts { get; set; } = 3;

    /// <summary>Initial delay before retrying a failed produce call.</summary>
    public TimeSpan PublishRetryBaseDelay { get; set; } = TimeSpan.FromMilliseconds(50);

    /// <summary>Maximum delay between produce retry attempts.</summary>
    public TimeSpan PublishRetryMaxDelay { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>Initial delay after a subscriber loop Kafka failure.</summary>
    public TimeSpan SubscriberRetryBaseDelay { get; set; } = TimeSpan.FromMilliseconds(100);

    /// <summary>Maximum delay after repeated subscriber loop Kafka failures.</summary>
    public TimeSpan SubscriberRetryMaxDelay { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// The hosting shutdown budget that must contain
    /// <see cref="KafkaSubscriberOptions.BackgroundDrainTimeout"/> when a subscriber uses
    /// <see cref="KafkaAckMode.AckAfterEnqueue"/>.
    /// </summary>
    public TimeSpan? HostShutdownTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Optional last-chance hook over the producer configuration (compression, linger, batch size,
    /// security settings, …) after the package applies its defaults.
    /// </summary>
    public Action<ProducerConfig>? ConfigureProducer { get; set; }

    /// <summary>
    /// Optional last-chance hook over each subscriber's consumer configuration (fetch sizes,
    /// <c>max.poll.interval.ms</c>, security settings, …) after the package applies its defaults.
    /// The package relies on <c>enable.auto.commit=true</c> with <c>enable.auto.offset.store=false</c>
    /// for its manual offset management; overriding those breaks delivery guarantees.
    /// </summary>
    public Action<ConsumerConfig>? ConfigureConsumer { get; set; }

    /// <summary>Optional last-chance hook over the admin client configuration used for topic creation.</summary>
    public Action<AdminClientConfig>? ConfigureAdminClient { get; set; }

    /// <summary>Adds or replaces a named Kafka reply target.</summary>
    public KafkaAsyncResponseTransportOptions AddReplyTarget(string name, string responseTopic)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentException.ThrowIfNullOrWhiteSpace(responseTopic);

        ReplyTargets[name] = new KafkaReplyTargetOptions { ResponseTopic = responseTopic };
        return this;
    }
}

/// <summary>Options for one named Kafka async-response reply target.</summary>
public sealed class KafkaReplyTargetOptions
{
    /// <summary>Kafka topic remote systems should produce response payloads to.</summary>
    public string? ResponseTopic { get; set; }

    /// <summary>Consumer group that receives responses for this target. Optional metadata.</summary>
    public string? ConsumerGroup { get; set; }

    /// <summary>Additional values copied to the transport-neutral reply target.</summary>
    public Dictionary<string, string> Properties { get; } = new(StringComparer.Ordinal);
}
