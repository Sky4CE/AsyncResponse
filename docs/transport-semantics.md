# Transport semantics

All ten broker transports implement the same delivery contract — a safe ack-after-handler
default, opt-in early ACK behind a bounded in-process queue, bounded shutdown drain, and
dead-lettering (or an explicit delegation to the broker's native mechanism). But each transport
maps that contract onto its broker's own settlement model, so *what an ACK is*, *who counts
delivery attempts*, *where poison messages go*, and *what shutdown waits for* differ per
transport — deliberately. An offset commit is not a peek-lock settle; a visibility timeout is
not a row lease. This page is the single reference for those differences, derived from each
transport's options, dispatcher, and subscriber source rather than from broker documentation.

Per-option defaults and registration syntax live in the
[configuration guide](configuration.md#transport-options); this page covers behavior.

## Identical everywhere

These hold for every transport in the matrix, verified per package:

- **Ack mode default.** Every subscriber options type has
  `AckMode = <Transport>AckMode.AckAfterHandlerCompletes`. Early ACK is opt-in via the same
  method on all ten:
  `UseAckAfterEnqueue(backgroundWorkerCount, backgroundQueueCapacity, backgroundDrainTimeout = null)`
  — both counts must be explicit positive values, there are no defaults to fall into.
- **Early ACK on the worker queue is vetoed at startup.** Durable-flow wake-ups ride the worker
  queue and rely on broker redelivery for crash recovery, so a crash after an early ACK but
  before execution strands the run as `Running` with nothing left to wake it (see
  [durable flows — what happens when things die](durable-flows.md#what-happens-when-things-die)).
  Startup throws for `WorkerSubscriber` early ACK unless
  `DurableFlowOptions.AllowEarlyAckWorkerSubscriber = true` accepts the risk; `ResponseSubscriber`
  early ACK logs a startup warning instead — it is at-most-once response delivery: a crash after
  the ACK destroys the broker's only copy, the waiter burns its full timeout and fails, and a
  durable flow then restarts the timed-out step and re-sends its request (idempotent triggers
  required, which the recovery contract already demands). Nothing strands.
- **`OnBackgroundFailure`.** Every subscriber options type exposes
  `Func<<Transport>BackgroundFailureContext, ValueTask>? OnBackgroundFailure`, invoked when a
  handler fails after the message was already settled (see the
  [context table](#what-onbackgroundfailure-receives) for what each transport reports).
- **`BackgroundDrainTimeout` = 20 s.** The maximum time to wait for queued and running
  background handlers while a hosted subscriber stops.
- **`HostShutdownTimeout` = 30 s.** A mirror of `Microsoft.Extensions.Hosting`
  `HostOptions.ShutdownTimeout` (whose real default is also 30 s). When a subscriber opts into
  early ACK, startup validation sums the transport's worst-case shutdown spend and **throws
  `InvalidOperationException`** if it exceeds this mirror — because a drain truncated by the
  host silently loses already-ACKed work. Equality passes; set the mirror to `null` only when
  the budget is validated externally.
- **Drain is a bounded wait, not a cancellation.** On stop the dispatcher closes the queue and
  waits up to `BackgroundDrainTimeout` for the background workers to finish the queued and
  in-flight handlers naturally — it does not cancel handlers to meet the deadline. Only after
  the timeout expires is cancellation signalled to whatever is still running, a warning logged
  ("already ACKed work may be interrupted by host shutdown"), and shutdown proceeds. In
  ack-after-handler mode there is nothing ACKed-but-unprocessed to drain; the subscriber loop
  simply stops with the host token.

## Legend

- **ack-after-handler / early ACK** — the two `AckMode` values
  (`AckAfterHandlerCompletes` / `AckAfterEnqueue`).
- **broker / store / in-process / native** — who counts delivery attempts: the broker's own
  delivery metadata, the transport's queue table/collection, the package's retry loop, or the
  broker's redrive/dead-letter policy with no app-level counter at all.
- **declared / delegated** — whether the package provisions the dead-letter destination
  (`CreateTopics`/`CreateStreams`/`DeclareTopology`/DDL-style) or expects infrastructure to
  own it.
- **drain** — `BackgroundDrainTimeout` (20 s default); **+ close 5 s** — the transport also
  spends its `ShutdownTimeout` (5 s default) on a bounded close/join, and startup validation
  counts both against `HostShutdownTimeout`. Transports without the close component have no
  `ShutdownTimeout` option at all.
- **—** — not applicable to that transport.
- Unqualified option names are per-subscriber (`WorkerSubscriber.` / `ResponseSubscriber.`);
  `MaxDeliveryAttempts` defaults to 5 with `0` = unlimited unless the row says otherwise.

## The matrix

| Transport | Ack semantics (default mode) | Attempt counting | Dead-letter destination | After a failure post-early-ACK | Shutdown drain budget | Lock/lease renewal |
|---|---|---|---|---|---|---|
| **AzureServiceBus** | peek-lock: complete on success, abandon on failure | broker `DeliveryCount`; dead-letter at `MaxDeliveryAttempts` (`0` = defer to the entity's `MaxDeliveryCount`) | native dead-letter subqueue (broker built-in, nothing to declare) | log + `OnBackgroundFailure`; the lock is settled, no DLQ write possible | drain + close 5 s (receiver/sender close, renewal-task join) | `LockRenewalInterval` 30 s, on by default; `null` disables |
| **GooglePubSub** | streaming pull: ACK on success, NACK on failure | native — subscription retry policy + `DeadLetterPolicy` `maxDeliveryAttempts`; no app counter by design | subscription `DeadLetterPolicy` (delegated to infra) | log + `OnBackgroundFailure`; already ACKed, no DLQ write possible | drain + close 5 s (subscriber-client stop) | — (the Pub/Sub client manages the ack deadline) |
| **Kafka** | manual offset store, auto-committed every `OffsetCommitInterval` 5 s; offsets cannot NACK one message | in-process retries with backoff (100 ms → 5 s); counted per process delivery — a restart before the commit resets the count | `{topic}.deadletter` (or one `DeadLetterTopic`); declared by `CreateTopics` (default on) | retried in-process, then log + `OnBackgroundFailure` + produced to the DLQ topic | drain only | — (stay under `max.poll.interval.ms` instead) |
| **MongoDB** | claimed document: delete on success, reschedule after `RedeliveryDelay` 5 s on failure | store — the `findOneAndUpdate` claim increments the attempt | `deadletter` logical queue in the same collection; `DeadLetterEnabled` default on, optional `DeadLetterRetention` | log + `OnBackgroundFailure` + DLQ document | drain + close 5 s (change-stream listen join) | automatic fenced renewal at `LockTimeout`/2 (server-clock `$$NOW`, `lock_id` fence) |
| **NATS** | JetStream explicit ack: ACK on success, NAK + `RedeliveryDelay` 5 s on failure | broker `NumDelivered`; at `MaxDeliveryAttempts` the message is ACKed + dead-lettered | `{prefix}.transport.deadletter` subject/stream; declared by `CreateStreams` (default on) | log + `OnBackgroundFailure` + published to the DLQ subject | drain only | — (`AckWait` 30 s must cover the handler) |
| **PostgreSQL** | claimed row: delete on success, reschedule after `RedeliveryDelay` 5 s on failure | store — the `FOR UPDATE SKIP LOCKED` claim increments `attempts` | `deadletter` logical queue in the same table; `DeadLetterEnabled` default on, optional `DeadLetterRetention` | log + `OnBackgroundFailure` + DLQ row | drain + close 5 s (LISTEN task join) | automatic fenced renewal at `LockTimeout`/2 (`lock_id` fence) |
| **RabbitMQ** | per-delivery `basic.ack`; `basic.nack` + requeue on failure | broker `x-death` header + `redelivered` flag; **`MaxDeliveryAttempts` default `0` = unlimited**; values > 2 need a TTL-retry DLX cycle (see notes) | optional `DeadLetterExchange` (default `null` → exhausted messages are **dropped**); declared when set and `DeclareTopology` is on | log + `OnBackgroundFailure`; the package writes no DLX message | drain + close 5 s (connection close) | — (unacked deliveries hold no expiring lock) |
| **Redis** | consumer group: `XACK` on success; a failed entry stays in the PEL and is reclaimed after `PendingMessageMinIdleTime` 30 s | broker — PEL delivery count (`XPENDING`) + 1 at claim; at `MaxDeliveryAttempts` the entry is dead-lettered + `XACK`ed | `{prefix}:transport:deadletter` stream (`XADD` auto-creates it); `DeadLetterEnabled` default on | log + `OnBackgroundFailure` + `XADD` to the DLQ stream | drain only | — (`PendingMessageMinIdleTime` must exceed the slowest handler) |
| **SQS** | visibility settle: delete on success; failure lets the visibility timeout lapse (or shortens it to `RedeliveryDelay`) | native `ApproximateReceiveCount` + redrive `maxReceiveCount`; no app counter by design | native redrive DLQ; delegated — or declared by `CreateQueues` (default **off**): `{queue}-dlq` + `MaxReceiveCount` 5 | log + `OnBackgroundFailure`; already deleted, no DLQ write possible | drain only | opt-in `VisibilityRenewalInterval` (default off); suppressed per message once the failure path schedules `RedeliveryDelay` |
| **SqlServer** | claimed row: delete on success, reschedule after `RedeliveryDelay` 5 s on failure | store — the `UPDLOCK/READPAST` claim increments `attempts` | `deadletter` logical queue in the same table; `DeadLetterEnabled` default on, optional `DeadLetterRetention` | log + `OnBackgroundFailure` + DLQ row | drain only | automatic fenced renewal at `LockTimeout`/2 (`lock_id` fence) |

## Delayed delivery (`IDelayedWorkerTransport`)

Delayed worker jobs — `EnqueueWorkerAsync(..., delay)` and the wake-ups behind suspended
durable-flow timers — are a per-transport capability. Envelopes carry their absolute due time
(`NotBeforeUtc`); the shared worker-job executor re-publishes any early delivery for the
remainder, which is how capped transports chunk long delays with no transport-specific code.

| Transport | Native delayed delivery | Per-hop cap | Mechanism / caveats |
|---|---|---|---|
| **InMemory** | ✅ | — | `TimeProvider` timer wheel (virtual-clock aware in tests); delayed jobs die with the process, logged at shutdown |
| **AzureServiceBus** | ✅ | — | scheduled messages (`ScheduledEnqueueTime`); broker-held, survives restarts |
| **SQS** | ✅ | 15 min (chunked) | `DelaySeconds`; standard queues only — per-message delay on a FIFO queue is rejected loudly |
| **PostgreSQL** | ✅ | — | insert with `available_at = now() + delay` (database clock); pickup latency ≤ `EmptyPollDelay` |
| **SqlServer** | ✅ | — | insert with `available_at = SYSUTCDATETIME() + delay`; pickup latency ≤ `EmptyPollDelay` |
| **MongoDB** | ✅ | — | insert stamps a client-computed `available_at` atomically; skew-early deliveries corrected by the `NotBeforeUtc` guard |
| **Kafka, RabbitMQ, GooglePubSub, Redis, NATS** | — | — | no native mechanism; flow timers wait in process under the lease, bare delayed enqueue throws with guidance |

## What `OnBackgroundFailure` receives

Each transport reports the failure with its own context type — the broker-native coordinates of
the already-settled message plus the handler exception:

| Transport | Context type | Fields |
|---|---|---|
| AzureServiceBus | `AzureServiceBusBackgroundFailureContext` | queue, subscriber role, sequence number, message id, correlation id, exception |
| GooglePubSub | `GooglePubSubBackgroundFailureContext` | subscription id, subscriber role, the full `PubsubMessage` (+ message id), exception — no correlation id property |
| Kafka | `KafkaBackgroundFailureContext` | topic, consumer group, subscriber role, partition, offset, correlation id, exception |
| MongoDB | `MongoDbBackgroundFailureContext` | queue, subscriber role, attempt, correlation id, exception |
| NATS | `NatsBackgroundFailureContext` | subject, consumer, subscriber role, `NumDelivered`, correlation id, exception |
| PostgreSQL | `PostgreSqlBackgroundFailureContext` | queue, subscriber role, attempt, correlation id, exception |
| RabbitMQ | `RabbitMqBackgroundFailureContext` | queue, subscriber role, exchange, routing key, delivery tag, exception — no correlation id property |
| Redis | `RedisBackgroundFailureContext` | stream, consumer group, subscriber role, entry id, correlation id, exception |
| SQS | `SqsBackgroundFailureContext` | queue, subscriber role, message id, receive count, correlation id, exception |
| SqlServer | `SqlServerBackgroundFailureContext` | queue, subscriber role, attempt, correlation id, exception |

## Notes per transport

Only cells that need more than a phrase.

### Azure Service Bus

- A receive batch is processed sequentially, so the last message in a batch waits up to
  `MaxMessagesPerReceive × handler latency` before settlement. `LockRenewalInterval` (30 s,
  cancellable per beat) renews the peek-lock of every unsettled batch message — including the
  one in the handler — so slow handlers do not hit `MessageLockLostException` redeliveries of
  already-processed messages
  ([troubleshooting](troubleshooting.md#azure-service-bus-messagelocklostexception-redeliveries-of-already-processed-messages)).
  Renewal failures are logged and processing continues — the message simply redelivers,
  preserving at-least-once. Ignored in early ACK (the message is already completed); the
  renewal task's join at shutdown is bounded by `ShutdownTimeout`.
- Every abandon burns broker `DeliveryCount`, which also counts toward the *entity's*
  `MaxDeliveryCount` policy. `MaxDeliveryAttempts = 0` disables the package-level dead-letter
  decision and leaves poison handling entirely to that broker policy.
- In early ACK the receive loop waits for background-queue capacity before receiving, so
  queue-full abandons cannot burn `DeliveryCount` in steady state.

### Google Pub/Sub

- The transport intentionally has no `MaxDeliveryAttempts` and no library-managed dead-letter:
  attempts and dead-lettering are the subscription's `RetryPolicy` and `DeadLetterPolicy`,
  configured in GCP. A failed handler NACKs and Pub/Sub redelivers per those policies.
- In early ACK, the streaming pull's flow control is bounded to `BackgroundQueueCapacity`
  (`maxOutstandingElementCount`), and a full background queue **parks the delivery callback
  until capacity frees instead of NACKing** — a queue-full NACK would burn a
  `DeadLetterPolicy` delivery attempt on a healthy, never-executed message. A NACK is returned
  only when the enqueue fails during shutdown/dispose, so the message redelivers elsewhere.

### Kafka

- Kafka offsets cannot NACK a single message, so redelivery is in-process: a failing handler is
  retried with backoff (`HandlerRetryBaseDelay` 100 ms → `HandlerRetryMaxDelay` 5 s) up to
  `MaxDeliveryAttempts`, stalling that partition while it retries (classic consumer-group
  semantics — size `TopicNumPartitions` for parallelism). Keep the worst-case retry budget
  under the consumer's `max.poll.interval.ms` or the broker evicts the consumer mid-retry
  ([troubleshooting](troubleshooting.md#kafka-the-broker-evicts-the-consumer-mid-retry)).
- Attempts are counted per process delivery: a consumer restart before the offset commit
  resets the count. The message that exhausts its attempts is produced to the dead-letter
  topic with failure-detail headers and its offset committed, so the partition keeps moving.
  The same retry-then-dead-letter path runs for background failures after an early ACK.
- In early ACK, a full background queue pauses consumption on all assigned partitions
  (re-checked every `BackpressurePollDelay` 50 ms) rather than dropping or re-fetching.

### RabbitMQ

- The broker does not count plain `basic.nack` requeues: the resolved attempt is
  `max(x-death count, redelivered ? 1 : 0) + 1`, which never exceeds 2 on its own. A
  `MaxDeliveryAttempts` above 2 therefore only takes effect when the dead-letter path forms a
  TTL-retry cycle that re-delivers the message (each dead-letter hop increments `x-death`);
  without such a cycle it behaves like 2 and logs a startup warning
  ([troubleshooting](troubleshooting.md#rabbitmq-startup-warns-about-maxdeliveryattempts-or-a-poison-message-loops-forever)).
- The default `MaxDeliveryAttempts = 0` means unlimited requeues — a poison message hot-loops
  until a cap (with a `DeadLetterExchange`) is configured. With a cap but no
  `DeadLetterExchange`, the exhausted message is rejected without requeue and **dropped**.
- In early ACK, a full background queue NACKs the delivery with requeue, so it redelivers
  instead of waiting.

### Redis

- New entries arrive via `XREADGROUP` at attempt 1. A separate reclaim loop scans the
  pending-entries list every `PendingClaimInterval` (5 s) and claims entries idle longer than
  `PendingMessageMinIdleTime` (30 s) with `XCLAIM`, so a crashed consumer's in-flight work is
  retried by a peer; the attempt is the PEL delivery count + 1.
- In early ACK, a full background queue leaves the entry un-ACKed in the PEL for the reclaim
  loop to retry — nothing is NACKed because Redis has no NACK.

### NATS

- Attempts are the broker's `NumDelivered` for the JetStream consumer; the consumer is durable,
  so counts survive subscriber restarts (unlike Kafka's in-process counter).
- In early ACK, a full background queue pauses pulling; a message still waiting in the queue
  when the subscriber stops is NAKed so JetStream redelivers it — backpressure itself never
  churns redeliveries.
- There is no in-progress ack extension: `AckWait` (30 s) must exceed the slowest handler or
  the server redelivers mid-handling.

### SQS

- Settlement is the visibility timeout: `VisibilityTimeout = null` uses the queue's setting,
  and it must exceed the slowest handler
  ([troubleshooting](troubleshooting.md#sqs-duplicate-executions-or-fifo-settings-that-dont-apply)).
  `RedeliveryDelay` optionally shortens a *failed* message's remaining invisibility via
  `ChangeMessageVisibility`; accounting stays native either way — every receive increments
  `ApproximateReceiveCount` and the queue's redrive policy dead-letters after
  `maxReceiveCount`.
- `VisibilityRenewalInterval` is off by default because extending visibility silently overrides
  redrive timing operators tune on the queue, and on FIFO queues an extended message keeps its
  whole message group blocked if the consumer wedges. When enabled, it requires
  `VisibilityTimeout` to be set and shorter renewal beats; once the failure path schedules
  `RedeliveryDelay` for a message, the renewal sweep suppresses that message so a late renewal
  cannot overwrite the shortened redelivery. Ignored in early ACK.
- `CreateQueues` is the only provisioning default that is **off** — production queues (and
  their redrive policies) are usually owned by infrastructure code.

### PostgreSQL, SQL Server, MongoDB

- The claim is atomic (`FOR UPDATE SKIP LOCKED` / `UPDLOCK, ROWLOCK, READPAST` /
  `findOneAndUpdate`) and increments the attempt counter in the store, so attempts survive
  process restarts and are visible in the queue table/collection.
- While a handler runs in ack-after-handler mode, a heartbeat renews the claim's lease every
  `LockTimeout`/2, fenced by the claim's `lock_id` (MongoDB additionally evaluates the lease
  against the server clock via `$$NOW`, so client clock skew cannot fence messages in or out).
  If the fence no longer matches — the lease lapsed and a peer re-claimed the row/document —
  renewal stops and the fenced ack/NAK no-ops for the stale claim: at-least-once is preserved,
  and the loss is logged. Renewal *failures* (transient DB errors) are logged and retried next
  beat.
- The dead-letter queue is rows/documents in the same table/collection under the
  `DeadLetterQueue` logical name; it has no consumer by default, so set `DeadLetterRetention`
  if entries should be pruned instead of kept for manual inspection. DDL is owned by
  `AutoCreateSchema` (PostgreSQL, SQL Server) / `AutoCreateIndexes` (MongoDB) — disable when
  migrations own it.
- Only PostgreSQL and MongoDB spend a `ShutdownTimeout` at stop (bounding the LISTEN /
  change-stream task join). SQL Server has no push channel to join and therefore no
  `ShutdownTimeout` option; it budgets only the drain.
