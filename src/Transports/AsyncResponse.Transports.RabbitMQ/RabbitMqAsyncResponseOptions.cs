namespace AsyncResponse.Transports.RabbitMQ;

/// <summary>
/// Options for the RabbitMQ AsyncResponse transport.
/// </summary>
public sealed class RabbitMqAsyncResponseOptions
{
    public const string TransportName = "rabbitmq";

    /// <summary>
    /// AMQP connection string. When set, it wins over the individual host/user/password settings.
    /// Default local RabbitMQ: <c>amqp://guest:guest@localhost:5672/</c>.
    /// </summary>
    public string? ConnectionString { get; set; }

    /// <summary>RabbitMQ host name used when <see cref="ConnectionString"/> is not set.</summary>
    public string HostName { get; set; } = "localhost";

    /// <summary>RabbitMQ AMQP port used when <see cref="ConnectionString"/> is not set.</summary>
    public int Port { get; set; } = 5672;

    /// <summary>RabbitMQ virtual host used when <see cref="ConnectionString"/> is not set.</summary>
    public string VirtualHost { get; set; } = "/";

    /// <summary>RabbitMQ user name used when <see cref="ConnectionString"/> is not set.</summary>
    public string UserName { get; set; } = "guest";

    /// <summary>RabbitMQ password used when <see cref="ConnectionString"/> is not set.</summary>
    public string Password { get; set; } = "guest";

    /// <summary>Client-provided connection name shown in RabbitMQ management UI and logs.</summary>
    public string ClientProvidedName { get; set; } = "AsyncResponse";

    /// <summary>Enables the RabbitMQ client's automatic connection recovery. Default: <c>true</c>.</summary>
    public bool AutomaticRecoveryEnabled { get; set; } = true;

    /// <summary>Enables automatic topology recovery. Default: <c>true</c>.</summary>
    public bool TopologyRecoveryEnabled { get; set; } = true;

    /// <summary>
    /// How long the client waits between automatic recovery attempts. Must be strictly positive:
    /// the RabbitMQ client uses the value directly in its recovery loop. Default: 5 seconds. The
    /// hosted subscribers' own restart retries back off separately, governed by
    /// <see cref="SubscriberRetryBaseDelay"/>/<see cref="SubscriberRetryMaxDelay"/>.
    /// </summary>
    public TimeSpan NetworkRecoveryInterval { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>Requested AMQP heartbeat interval.</summary>
    public TimeSpan RequestedHeartbeat { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>Declares exchanges, queues, and bindings from this package before publish/consume. Default: <c>true</c>.</summary>
    public bool DeclareTopology { get; set; } = true;

    /// <summary>RabbitMQ direct exchange used to publish worker jobs.</summary>
    public string WorkerExchange { get; set; } = "asyncresponse.worker";

    /// <summary>Durable queue consumed by the worker subscriber hosted service.</summary>
    public string WorkerQueue { get; set; } = "asyncresponse.worker";

    /// <summary>Routing key used to bind and publish worker jobs.</summary>
    public string WorkerRoutingKey { get; set; } = "asyncresponse.worker";

    /// <summary>Worker queue handling options.</summary>
    public RabbitMqSubscriberOptions WorkerSubscriber { get; } = new();

    /// <summary>RabbitMQ direct exchange remote systems publish response messages to.</summary>
    public string ResponseExchange { get; set; } = "asyncresponse.response";

    /// <summary>Durable queue consumed by the response-ingress hosted service.</summary>
    public string ResponseQueue { get; set; } = "asyncresponse.response";

    /// <summary>Routing key used to bind and publish response messages.</summary>
    public string ResponseRoutingKey { get; set; } = "asyncresponse.response";

    /// <summary>Response queue handling options.</summary>
    public RabbitMqSubscriberOptions ResponseSubscriber { get; } = new();

    /// <summary>
    /// Optional dead-letter exchange. When set (and <see cref="DeclareTopology"/> is enabled), the worker and
    /// response queues are declared with <c>x-dead-letter-exchange</c> so messages rejected without requeue
    /// (see <see cref="RabbitMqSubscriberOptions.MaxDeliveryAttempts"/>) are routed here instead of dropped.
    /// Changing this on a queue that already exists requires recreating the queue (RabbitMQ rejects a redeclare
    /// with different arguments).
    /// </summary>
    public string? DeadLetterExchange { get; set; }

    /// <summary>
    /// Optional dead-letter queue declared and bound to <see cref="DeadLetterExchange"/>. Leave null to manage
    /// the dead-letter queue externally. It is declared only together with <see cref="DeadLetterExchange"/>:
    /// set on its own (a broker policy supplies the dead-letter exchange) it must already exist — it is still
    /// where a capped message is parked unless <see cref="ParkQueue"/> names another queue, and a park that
    /// finds no queue fails loudly and the delivery is requeued. Because it is bound to the dead-letter
    /// exchange it receives <em>every</em> dead-lettered message: when that exchange also feeds a TTL-retry
    /// queue, each retry hop leaves a copy here, including hops of messages that later succeed — use
    /// <see cref="ParkQueue"/> for that topology.
    /// </summary>
    public string? DeadLetterQueue { get; set; }

    /// <summary>
    /// Optional queue that receives only the messages the delivery cap parks (see
    /// <see cref="RabbitMqSubscriberOptions.MaxDeliveryAttempts"/>): a capped message that has already ridden
    /// the dead-letter cycle is published here through the default exchange and ACKed. Declared durable and
    /// deliberately <em>unbound</em> when <see cref="DeclareTopology"/> is enabled — with or without a
    /// <see cref="DeadLetterExchange"/> — so, unlike <see cref="DeadLetterQueue"/>, it never collects the
    /// retry hops of a TTL-retry cycle. When null, capped messages are parked in <see cref="DeadLetterQueue"/>.
    /// </summary>
    public string? ParkQueue { get; set; }

    /// <summary>
    /// Routing key used both for the <c>x-dead-letter-routing-key</c> argument and for binding
    /// <see cref="DeadLetterQueue"/>. When null, dead-lettered messages keep their original routing key and the
    /// dead-letter queue is bound with the source queue's routing key.
    /// </summary>
    public string? DeadLetterRoutingKey { get; set; }

    /// <summary>The logical reply target name used by <c>WithReplyTarget()</c>. Default: <c>default</c>.</summary>
    public string DefaultReplyTargetName { get; set; } = "default";

    /// <summary>
    /// Named reply targets exposed to Core through <see cref="IAsyncResponseReplyTargetProvider"/>.
    /// When empty, <see cref="ResponseExchange"/> + <see cref="ResponseRoutingKey"/> become the default target.
    /// </summary>
    public Dictionary<string, RabbitMqReplyTargetOptions> ReplyTargets { get; } = new(StringComparer.Ordinal);

    /// <summary>Message header that carries the AsyncResponse correlation id. Default: <c>correlationId</c>.</summary>
    public string CorrelationIdHeader { get; set; } = "correlationId";

    /// <summary>
    /// JSON paths inspected when a response message does not carry the correlation id as a
    /// message property/header. Paths are case-insensitive and support nested JSON strings.
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

    /// <summary>Initial delay after a subscriber loop failure.</summary>
    public TimeSpan SubscriberRetryBaseDelay { get; set; } = TimeSpan.FromMilliseconds(250);

    /// <summary>Maximum delay after repeated subscriber loop failures.</summary>
    public TimeSpan SubscriberRetryMaxDelay { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Bounds the connection close while hosted consumers/publishers stop. The close completes in
    /// milliseconds when healthy; when it does not, the connection is abandoned anyway, so keep
    /// this short — it counts against the host's shutdown budget. Default: <c>5s</c>.
    /// </summary>
    public TimeSpan ShutdownTimeout { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// The hosting shutdown budget that must contain RabbitMQ channel shutdown plus
    /// <see cref="RabbitMqSubscriberOptions.BackgroundDrainTimeout"/> when a subscriber uses
    /// <see cref="RabbitMqAckMode.AckAfterEnqueue"/>.
    /// </summary>
    public TimeSpan? HostShutdownTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// The broker's <c>consumer_timeout</c>, mirrored here because the client cannot read it: RabbitMQ
    /// closes a channel whose delivery stays unacknowledged longer than that (<c>PRECONDITION_FAILED</c>)
    /// and requeues the delivery, so a worker handler still running past it — in
    /// <see cref="RabbitMqAckMode.AckAfterHandlerCompletes"/> the delivery stays unacknowledged for the
    /// handler's whole run — has its job redelivered to another consumer mid-run. The value is advertised
    /// through <see cref="IWorkerTransportInFlightLimit.MaxInFlightDuration"/> so durable-flow timers that
    /// wait in process plan their waits inside it; nothing in this package enforces it. Default:
    /// <c>30 minutes</c>, the broker's default. Keep it equal to (or below) the broker's setting; set
    /// <c>null</c> only when the broker's <c>consumer_timeout</c> is disabled. Must be positive.
    /// </summary>
    public TimeSpan? BrokerConsumerTimeout { get; set; } = TimeSpan.FromMinutes(30);

    /// <summary>Adds or replaces a named RabbitMQ reply target.</summary>
    public RabbitMqAsyncResponseOptions AddReplyTarget(
        string name,
        string exchange,
        string routingKey)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentException.ThrowIfNullOrWhiteSpace(exchange);
        ArgumentException.ThrowIfNullOrWhiteSpace(routingKey);

        ReplyTargets[name] = new RabbitMqReplyTargetOptions
        {
            Exchange = exchange,
            RoutingKey = routingKey
        };

        return this;
    }
}

/// <summary>Options for one named RabbitMQ async-response reply target.</summary>
public sealed class RabbitMqReplyTargetOptions
{
    /// <summary>Exchange remote systems should publish responses to.</summary>
    public string? Exchange { get; set; }

    /// <summary>Routing key remote systems should publish responses with.</summary>
    public string? RoutingKey { get; set; }

    /// <summary>Queue that receives responses for this target. Optional; included as metadata for consumers.</summary>
    public string? Queue { get; set; }

    /// <summary>Additional values copied to the transport-neutral reply target.</summary>
    public Dictionary<string, string> Properties { get; } = new(StringComparer.Ordinal);
}
