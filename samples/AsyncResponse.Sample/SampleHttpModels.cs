namespace AsyncResponse.Sample;

// HTTP response DTOs for the sample's endpoints. These were anonymous types before the sample
// became Native AOT-publishable; anonymous types have no source-generated JSON metadata, so each
// shape is a named record registered in SampleHttpJsonContext. Property names are PascalCase and
// rely on the ASP.NET web default camelCase naming policy at runtime, which keeps the JSON
// byte-identical to the previous anonymous-type output (the integration tests assert these
// shapes over HTTP).

/// <summary>Terminal outcome of the basic request/response round-trip.</summary>
public sealed record StatusMessageResult(OperationStatus Status, string? Message);

/// <summary>Fault details returned when a waiter is expected to fault (ambient-exception demo).</summary>
public sealed record FaultResult(bool Faulted, string ExceptionType, string? Detail);

/// <summary>The reply target a trigger observed (reply-target demo).</summary>
public sealed record ReplyTargetResult(string? Transport, string? Address);

/// <summary>A correlation id returned by fire-and-forget style endpoints (/worker, /arm).</summary>
public sealed record CorrelationResult(string CorrelationId);

/// <summary>Terminal outcome of an attached wait, including the correlation id it attached to (/attach).</summary>
public sealed record AttachOutcomeResult(string CorrelationId, OperationStatus Status, string? Message);

/// <summary>Count of recovery registrations removed by the test-hygiene endpoints.</summary>
public sealed record DeletedResult(int Deleted);

/// <summary>JSON shape of the /healthz report (status plus per-check entries).</summary>
public sealed record HealthReportResponse(string Status, IReadOnlyList<HealthCheckEntryResponse> Checks);

/// <summary>One health-check entry inside <see cref="HealthReportResponse"/>.</summary>
public sealed record HealthCheckEntryResponse(string Name, string Status, string? Description, IReadOnlyDictionary<string, object>? Data);

// ---------------------------------------------------------------------------------------------
// /config response: the providers this instance resolved from configuration. One record per
// provider section; property order and names mirror the previous anonymous shapes exactly.

/// <summary>SQS transport section of the /config response.</summary>
public sealed record SqsConfig(
    string? WorkerQueue,
    string? ResponseQueue,
    bool CreateQueues,
    int MaxReceiveCount,
    string WorkerAckMode,
    int WorkerBackgroundWorkerCount,
    int WorkerBackgroundQueueCapacity,
    string ResponseAckMode,
    int ResponseBackgroundWorkerCount,
    int ResponseBackgroundQueueCapacity);

/// <summary>Azure Service Bus transport section of the /config response.</summary>
public sealed record AzureServiceBusConfig(
    string? WorkerQueue,
    string? ResponseQueue,
    string WorkerAckMode,
    int WorkerBackgroundWorkerCount,
    int WorkerBackgroundQueueCapacity,
    int WorkerMaxDeliveryAttempts,
    string ResponseAckMode,
    int ResponseBackgroundWorkerCount,
    int ResponseBackgroundQueueCapacity,
    int ResponseMaxDeliveryAttempts);

/// <summary>Google Pub/Sub transport section of the /config response.</summary>
public sealed record PubSubConfig(
    string WorkerAckMode,
    int WorkerBackgroundWorkerCount,
    int WorkerBackgroundQueueCapacity,
    string ResponseAckMode,
    int ResponseBackgroundWorkerCount,
    int ResponseBackgroundQueueCapacity);

/// <summary>Kafka transport section of the /config response.</summary>
public sealed record KafkaConfig(
    string? WorkerTopic,
    string? WorkerConsumerGroup,
    string? ResponseTopic,
    string? ResponseConsumerGroup,
    string? DeadLetterTopic,
    string WorkerAckMode,
    int WorkerBackgroundWorkerCount,
    int WorkerBackgroundQueueCapacity,
    int WorkerMaxDeliveryAttempts,
    string ResponseAckMode,
    int ResponseBackgroundWorkerCount,
    int ResponseBackgroundQueueCapacity,
    int ResponseMaxDeliveryAttempts);

/// <summary>RabbitMQ transport section of the /config response.</summary>
public sealed record RabbitMqConfig(
    string? WorkerExchange,
    string? WorkerQueue,
    string? WorkerRoutingKey,
    string? ResponseExchange,
    string? ResponseQueue,
    string? ResponseRoutingKey,
    string WorkerAckMode,
    int WorkerBackgroundWorkerCount,
    int WorkerBackgroundQueueCapacity,
    string ResponseAckMode,
    int ResponseBackgroundWorkerCount,
    int ResponseBackgroundQueueCapacity);

/// <summary>Redis Streams transport section of the /config response.</summary>
public sealed record RedisTransportConfig(
    string? WorkerStream,
    string? WorkerConsumerGroup,
    string? ResponseStream,
    string? ResponseConsumerGroup,
    string? DeadLetterStream,
    string WorkerAckMode,
    int WorkerBackgroundWorkerCount,
    int WorkerBackgroundQueueCapacity,
    int WorkerMaxDeliveryAttempts,
    string ResponseAckMode,
    int ResponseBackgroundWorkerCount,
    int ResponseBackgroundQueueCapacity,
    int ResponseMaxDeliveryAttempts);

/// <summary>NATS channel/transport section of the /config response.</summary>
public sealed record NatsConfig(
    string? SubjectPrefix,
    string? RecoveryBucket,
    string? WorkerSubject,
    string? ResponseSubject,
    string WorkerAckMode,
    int WorkerBackgroundWorkerCount,
    int WorkerBackgroundQueueCapacity,
    string ResponseAckMode);

/// <summary>PostgreSQL channel/transport section of the /config response.</summary>
public sealed record PostgresConfig(
    string? SchemaName,
    string? RecoveryStateTable,
    string? ChannelMessageTable,
    string? TransportMessageTable,
    string? WorkerQueue,
    string? ResponseQueue,
    string WorkerAckMode,
    int WorkerBackgroundWorkerCount,
    int WorkerBackgroundQueueCapacity,
    string ResponseAckMode);

/// <summary>SQL Server channel/transport section of the /config response.</summary>
public sealed record SqlServerConfig(
    string? SchemaName,
    string? RecoveryStateTable,
    string? ChannelMessageTable,
    string? TransportMessageTable,
    string? WorkerQueue,
    string? ResponseQueue,
    string WorkerAckMode,
    int WorkerBackgroundWorkerCount,
    int WorkerBackgroundQueueCapacity,
    string ResponseAckMode);

/// <summary>MongoDB channel/transport section of the /config response.</summary>
public sealed record MongoDbConfig(
    string? RecoveryStateCollection,
    string? ChannelMessageCollection,
    string? SubscriberCollection,
    string? TransportMessageCollection,
    string? WorkerQueue,
    string? ResponseQueue,
    string WorkerAckMode,
    int WorkerBackgroundWorkerCount,
    int WorkerBackgroundQueueCapacity,
    string ResponseAckMode);

/// <summary>
/// The /config response. Property names are chosen so the runtime camelCase policy reproduces the
/// previous anonymous-member spelling exactly (e.g. <c>Sqlserver</c> → <c>sqlserver</c>,
/// <c>Rabbitmq</c> → <c>rabbitmq</c>).
/// </summary>
public sealed record ConfigResponse(
    string Channel,
    string Transport,
    AzureServiceBusConfig? AzureServiceBus,
    SqsConfig? Sqs,
    PubSubConfig? Pubsub,
    KafkaConfig? Kafka,
    RabbitMqConfig? Rabbitmq,
    RedisTransportConfig? Redis,
    NatsConfig? Nats,
    PostgresConfig? Postgres,
    SqlServerConfig? Sqlserver,
    MongoDbConfig? Mongodb);

/// <summary>
/// Raw response body used by the /emit-response helpers when the correlation id travels in the
/// JSON body (exercising the ingress extractor's JSON-path fallback). PascalCase on the wire,
/// exactly like the anonymous shape it replaces.
/// </summary>
public sealed record RawResponseBody(string CorrelationId, int Status, string? Message);
