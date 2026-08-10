namespace AsyncResponse.Conformance;

/// <summary>
/// What a worker transport can be asked to do. The transports do not offer the same knobs, and
/// pretending otherwise is how a conformance suite ends up either untestable or dishonest:
/// <list type="bullet">
/// <item>Eight transports expose <c>MaxDeliveryAttempts</c> and retry in process or by rescheduling
/// the row. SQS and Google Pub/Sub do not — redelivery there is the broker's own redrive policy and
/// ack deadline, configured on the queue/subscription rather than by the library.</item>
/// <item>Seven expose <c>DeadLetterEnabled</c> and route exhausted messages to a destination the
/// library creates. RabbitMQ uses a dead-letter exchange, and Service Bus, SQS, and Pub/Sub have
/// native dead-letter queues — so the assertion has to look somewhere different.</item>
/// </list>
/// A fact that needs an absent capability skips with the reason named, rather than silently not
/// existing for that transport. <see cref="TransportConformanceSuite"/> asserts the capability set
/// itself, so this stays a description of the library rather than a wish.
/// </summary>
public sealed record TransportCapabilities
{
    /// <summary>
    /// A failed handler is redelivered, and the redelivery is <em>bounded</em> — by a
    /// <c>MaxDeliveryAttempts</c> knob on the subscriber options for most transports, by the
    /// in-process retry budget for the in-memory queue, or by the queue's own redrive policy on SQS.
    /// Where the bound comes from does not change the contract: a transient failure is retried, and a
    /// permanently failing message eventually stops being delivered.
    /// </summary>
    public bool BoundedRedelivery { get; init; }

    /// <summary>
    /// The transport creates and routes to its own dead-letter destination, toggled by
    /// <c>DeadLetterEnabled</c>. False when the broker owns dead-lettering natively.
    /// </summary>
    public bool LibraryDeadLetter { get; init; }

    /// <summary>
    /// The broker dead-letters exhausted messages itself (SQS redrive policy, Service Bus DLQ,
    /// Pub/Sub dead-letter topic, RabbitMQ dead-letter exchange). Exactly one of this and
    /// <see cref="LibraryDeadLetter"/> is true for every transport that dead-letters at all.
    /// </summary>
    public bool BrokerDeadLetter { get; init; }

    /// <summary>
    /// The highest <c>MaxDeliveryAttempts</c> the transport can actually enforce, or null when any
    /// value works. Only RabbitMQ constrains this: a plain <c>basic.nack</c> requeue does not
    /// increment the broker's <c>x-death</c> header, so attempts past the second are uncountable
    /// unless the application declares a TTL-retry dead-letter cycle the library does not own. The
    /// subscriber logs a startup warning when a higher value is configured; above the cap a poison
    /// message requeues forever, which the contract would otherwise report as a library defect
    /// rather than the documented trade-off it is (docs/troubleshooting.md).
    /// </summary>
    public int? MaxCountableDeliveryAttempts { get; init; }

    /// <summary>
    /// The lowest delivery bound the transport will accept. Only Google Pub/Sub sets one: a
    /// <c>DeadLetterPolicy</c> rejects a <c>MaxDeliveryAttempts</c> below 5. The contract has to
    /// assert against the bound that is actually in force, not the one it asked for — asserting the
    /// smaller number reports the deliveries between them as a runaway poison message.
    /// </summary>
    public int MinDeliveryAttempts { get; init; } = 1;

    /// <summary>
    /// The largest single argument the payload contract sends through this transport, chosen to sit
    /// under the broker's hard message ceiling with room for the envelope around it.
    /// <para>
    /// The ceilings are real and enforced by the broker, not by the library: SQS and Service Bus
    /// (standard tier, which the emulator implements) both reject anything over 256 KiB outright, NATS
    /// defaults to a 1 MiB max payload, and Kafka to a 1 MiB message. AsyncResponse has no claim-check
    /// — a payload over the ceiling fails the publish — so the contract pins the supported size per
    /// transport rather than asserting one number everywhere.
    /// </para>
    /// </summary>
    public int LargePayloadBytes { get; init; } = 256 * 1024;

    /// <summary>The transport supports the early-ACK subscriber mode (<c>AckAfterEnqueue</c>).</summary>
    public bool EarlyAck { get; init; }

    /// <summary>
    /// The transport resolves a reply target an external system can publish a response to. The
    /// in-memory transport has no addressable destination.
    /// </summary>
    public bool ReplyTarget { get; init; }

    /// <summary>
    /// The transport survives its consumer going away and back — messages published while no
    /// subscriber is running are still delivered when one returns. False only for the in-memory
    /// transport, whose queue lives and dies with the host.
    /// </summary>
    public bool DurableWhileConsumerIsDown { get; init; }

    /// <summary>The capability set for one transport.</summary>
    public static TransportCapabilities For(MatrixTransport transport) => transport switch
    {
        MatrixTransport.InMemory => new TransportCapabilities
        {
            // The in-process queue retries a failing job with backoff up to
            // InMemoryWorkerTransportOptions.MaxDeliveryAttempts and then drops it — deliberately, so
            // durable-flow wake-ups riding this queue get the same redelivery contract a broker gives
            // them. What it genuinely lacks is a dead-letter destination, an early-ACK mode, an
            // addressable reply target, and any life beyond the host.
            BoundedRedelivery = true,
            EarlyAck = false,
            ReplyTarget = false,
            DurableWhileConsumerIsDown = false
        },

        MatrixTransport.Redis or MatrixTransport.Nats or MatrixTransport.PostgreSql or
        MatrixTransport.SqlServer or MatrixTransport.MongoDb or MatrixTransport.Kafka
            => new TransportCapabilities
            {
                BoundedRedelivery = true,
                LibraryDeadLetter = true,
                EarlyAck = true,
                ReplyTarget = true,
                DurableWhileConsumerIsDown = true
            },

        // RabbitMQ bounds redelivery in the library but dead-letters through a dead-letter exchange
        // the broker owns.
        MatrixTransport.RabbitMq => new TransportCapabilities
        {
            BoundedRedelivery = true,
            BrokerDeadLetter = true,
            MaxCountableDeliveryAttempts = 2,
            EarlyAck = true,
            ReplyTarget = true,
            DurableWhileConsumerIsDown = true
        },

        // Service Bus counts deliveries itself against the queue's MaxDeliveryCount and moves the
        // message to its own $DeadLetterQueue; the library's attempt bound rides on top.
        MatrixTransport.AzureServiceBus => new TransportCapabilities
        {
            BoundedRedelivery = true,
            BrokerDeadLetter = true,
            // Standard tier — what the emulator implements — caps a message at 256 KiB including its
            // AMQP properties. Premium raises it to 100 MB, but the contract has to pass on both.
            LargePayloadBytes = 128 * 1024,
            EarlyAck = true,
            ReplyTarget = true,
            DurableWhileConsumerIsDown = true
        },

        // SQS and Pub/Sub own redelivery entirely — visibility timeout plus redrive policy, and ack
        // deadline plus dead-letter policy. The library exposes no attempt bound for either.
        // SQS bounds redelivery through the queue rather than the subscriber: CreateQueues provisions
        // a "-dlq" and a native redrive policy at MaxReceiveCount, so a poison message stops being
        // delivered exactly the way it does everywhere else — the knob just lives on the queue.
        MatrixTransport.Sqs => new TransportCapabilities
        {
            BoundedRedelivery = true,
            BrokerDeadLetter = true,
            // SQS rejects anything over 262,144 bytes for the whole message; 128 KiB of argument
            // leaves room for the envelope and the message attributes around it.
            LargePayloadBytes = 128 * 1024,
            EarlyAck = true,
            ReplyTarget = true,
            DurableWhileConsumerIsDown = true
        },

        // Pub/Sub bounds redelivery furthest from the library of all of them: the package exposes
        // neither a delivery cap nor a dead-letter destination on purpose, because on GCP that is the
        // subscription's own DeadLetterPolicy — infrastructure the application owns, and which the
        // subscriber logs a startup warning about when it cannot see one. The contract still holds;
        // the harness provisions the subscription with that policy, exactly as the warning prescribes.
        MatrixTransport.GooglePubSub => new TransportCapabilities
        {
            BoundedRedelivery = true,
            BrokerDeadLetter = true,
            MinDeliveryAttempts = 5,
            EarlyAck = true,
            ReplyTarget = true,
            DurableWhileConsumerIsDown = true
        },

        _ => throw new ArgumentOutOfRangeException(nameof(transport), transport, null)
    };
}
