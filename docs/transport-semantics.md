# Transport semantics

All ten broker transports implement the same delivery contract — a safe ack-after-handler
default, opt-in early ACK behind a bounded in-process queue, a bounded shutdown drain, and
dead-lettering (or an explicit delegation to the broker's native mechanism). Each transport maps
that contract onto its broker's own settlement model, so *what an ACK is*, *who counts delivery
attempts*, *where poison messages go*, and *what shutdown waits for* differ per transport — an
offset commit is not a peek-lock settle, and a visibility timeout is not a row lease. This page is
the single reference for those differences, derived from each transport's source rather than from
broker documentation.

Per-option defaults and registration syntax live in the
[configuration guide](configuration.md#transport-options); this page covers behavior.

## Identical everywhere

These hold for every transport in the matrix:

- **Ack mode default.** Every subscriber options type has
  `AckMode = <Transport>AckMode.AckAfterHandlerCompletes`. Early ACK is opt-in via the same
  method on all ten:
  `UseAckAfterEnqueue(backgroundWorkerCount, backgroundQueueCapacity, backgroundDrainTimeout = null)`
  — both counts must be explicit positive values; there are no defaults to fall into.
- **Early ACK on the worker queue is vetoed at startup.** Durable-flow wake-ups ride the worker
  queue and rely on broker redelivery for crash recovery, so a crash after an early ACK but
  before execution strands the run as `Running` with nothing left to wake it (see
  [durable flows — what happens when things die](durable-flows.md#what-happens-when-things-die)).
  Startup throws for `WorkerSubscriber` early ACK unless
  `DurableFlowOptions.AllowEarlyAckWorkerSubscriber = true` accepts the risk. `ResponseSubscriber`
  early ACK only logs a startup warning: it makes response delivery at-most-once — a crash after
  the ACK destroys the broker's only copy, the waiter times out, and a durable flow restarts the
  timed-out step and re-sends its request (idempotent triggers required, as the recovery contract
  already demands). Nothing strands.
- **`OnBackgroundFailure`.** Every subscriber options type exposes
  `Func<<Transport>BackgroundFailureContext, ValueTask>? OnBackgroundFailure`, invoked when a
  handler fails after the message was already settled (see the
  [context table](#what-onbackgroundfailure-receives)).
- **The background queue outlives a reconnect.** The early-ACK dispatcher belongs to the hosted
  service, not to one connect-and-consume attempt, and only the host stopping drains it. A routine
  receive-loop fault (a claim timeout, a deadlock victim, a broker blip) rebuilds the connection
  while already-ACKed work keeps running.
- **`BackgroundDrainTimeout` = 20 s, split three to one.** The maximum wait for queued and running
  background handlers while a hosted subscriber stops. Every transport spends three quarters on the
  handlers and reserves one quarter for the entries still queued when that lapses. Kafka, RabbitMQ,
  Redis, NATS and the database transports dead-letter them and report each through
  `OnBackgroundFailure`; on Kafka, RabbitMQ, Redis and NATS the stop itself does this (a background
  worker may be stuck in a long handler) and writes each copy *before* awaiting the callback, so a
  slow callback cannot hold back the only durable record. SQS, Azure Service Bus and Google Pub/Sub
  cannot dead-letter a message the broker already forgot, so there the stop reports each
  still-queued entry through `OnBackgroundFailure`. Whatever the reserve does not cover is logged
  at Error with its count — it is lost at process exit.
- **`HostShutdownTimeout` = 30 s.** A mirror of `Microsoft.Extensions.Hosting`
  `HostOptions.ShutdownTimeout` (whose default is also 30 s). Startup validation sums the
  transport's worst-case shutdown spend and **throws `InvalidOperationException`** if it exceeds
  this mirror, because a drain truncated by the host silently loses already-ACKed work. The check
  runs for early-ACK subscribers on every transport, and in both ack modes on Azure Service Bus,
  SQS and Google Pub/Sub (whose ack-after-handler stop also spends `ShutdownTimeout`). When
  **both** subscribers use early ACK, NATS and the database transports sum both roles' spends: the
  host stops hosted services one after another (`HostOptions.ServicesStopConcurrently = false` by
  default), and hosted services registered after them spend from the same budget, which
  validation cannot see. Equality passes; set the mirror to `null` only when the budget is
  validated externally.
- **Drain is a bounded wait, not a cancellation.** On stop the dispatcher closes the queue and
  waits up to the handler share of `BackgroundDrainTimeout` for the queued and in-flight handlers
  to finish naturally. Only then is cancellation signalled to whatever still runs and a warning
  logged ("already ACKed work may be interrupted by host shutdown"). In ack-after-handler mode
  there is nothing ACKed-but-unprocessed to drain and the subscriber loop simply stops with the
  host token — except that Kafka waits for detached handlers still in their partition's current
  assignment (and commits their offsets), RabbitMQ waits, bounded, for the handler running in its
  delivery callback so that its ACK lands before the channel closes, and Google Pub/Sub waits,
  bounded, for its running handlers before it stops the client (see their notes).
- **A host-stop hand-back is a stop, not a failure.** When the host begins stopping, a durable
  flow waiting in process on a timer is normally *handed over*: checkpointed, an immediate wake-up
  published, its delivery acknowledged. When that cannot happen — the publish failed, the wait
  began on a host already stopping, or a lease-contention wait is cut short by the stop — the
  delivery is handed back instead (`DurableFlowInterruptedException`). The host fires
  `ApplicationStopping` *before* it stops any hosted service, so this arrives while the
  subscriber's own token is still live; every ack-after-handler dispatcher recognizes it by type
  and hands the delivery back for redelivery — never counted as a failure, retried or
  dead-lettered:

  | Transport | Ack-after-handler hand-back |
  |---|---|
  | RabbitMQ, Kafka, Redis, Service Bus, database transports | left unsettled (lock/lease lapses, or the channel close requeues) |
  | NATS | NAKed with no delay, for a live peer |
  | SQS | visibility shortened to `HostShutdownTimeout` when `VisibilityTimeout` is longer |
  | Google Pub/Sub | held, then NACKed once when its client stops |

  The redelivery still spends a delivery attempt wherever the broker or store counts them, so a
  hand-back at the attempt cap is dead-lettered on redelivery without running (on Service Bus
  only against the entity's own `MaxDeliveryCount`). Under early ACK the broker has already
  forgotten the message: where the transport can dead-letter (Kafka, RabbitMQ, Redis, NATS and
  the database transports) the hand-back is copied to the dead-letter destination with a reason
  starting `handed_back_after_commit` — replaying it is safe, the run resumes from its checkpoint —
  then logged as a Warning and reported through `OnBackgroundFailure`. When no copy is written (no
  destination configured, or the write failed) — and always on Azure Service Bus, SQS and Google
  Pub/Sub — it is logged at Error: the wake-up is lost unless the report records it, so resume the
  flow explicitly. Application code must never throw `DurableFlowInterruptedException` itself.
- **Worker intake stops at `ApplicationStopping`.** When the host registers
  `IHostApplicationLifetime`, every worker subscriber — except Google Pub/Sub in
  `AckAfterHandlerCompletes`, which keeps starting deliveries until its own stop — takes no new
  delivery from `ApplicationStopping` on, so the wake-ups a stopping host's hand-overs publish go
  to a live replica instead of coming straight back. A delivery already received but not started
  is handed back without running: left unacknowledged on RabbitMQ, Kafka and Redis; made visible
  again on SQS; abandoned on Service Bus; NAKed with no delay on NATS and the database transports;
  NACKed at client stop on Pub/Sub. Without a registered lifetime, the first hand-back is the stop
  signal. Response subscribers are never gated and keep serving waiters. The gate cannot tell a
  flow wake-up from a plain job: on a single replica, or when the whole fleet stops, queued worker
  jobs wait for the next start — a request waiting on one times out, and a recoverable request's
  response is recovered once the job runs.

## Legend

- **ack-after-handler / early ACK** — the two `AckMode` values
  (`AckAfterHandlerCompletes` / `AckAfterEnqueue`).
- **broker / store / in-process / native** — who counts delivery attempts: the broker's own
  delivery metadata, the transport's queue table/collection, the package's retry loop, or the
  broker's redrive/dead-letter policy with no app-level counter at all.
- **declared / delegated** — whether the package provisions the dead-letter destination
  (`CreateTopics`/`CreateStreams`/`CreateQueues`/`DeclareTopology`/DDL) or expects
  infrastructure to own it.
- **drain** — `BackgroundDrainTimeout` (20 s default); **+ close 5 s** — the transport also
  spends its `ShutdownTimeout` (5 s default) on a bounded close/join, and startup validation
  counts it against `HostShutdownTimeout` (twice for RabbitMQ, and for Service Bus with lock
  renewal on). Transports without the close component have no `ShutdownTimeout` option.
- **—** — not applicable to that transport.
- Unqualified option names are per-subscriber (`WorkerSubscriber.` / `ResponseSubscriber.`);
  `MaxDeliveryAttempts` defaults to 5 with `0` = unlimited unless the row says otherwise.

## The matrix

| Transport | Ack semantics (default mode) | Attempt counting | Dead-letter destination | After a failure post-early-ACK | Shutdown drain budget | Lock/lease renewal |
|---|---|---|---|---|---|---|
| **AzureServiceBus** | peek-lock: complete on success, abandon on failure | broker `DeliveryCount`; dead-letter at `MaxDeliveryAttempts` (`0` = defer to the entity's `MaxDeliveryCount`) | native dead-letter subqueue (nothing to declare) | log + `OnBackgroundFailure`; no DLQ write possible | drain + close 5 s (renewal-task join, then receiver close; validated in both ack modes — two `ShutdownTimeout` terms with renewal on, one with it off) | `LockRenewalInterval` 10 s, on by default; `null` disables |
| **GooglePubSub** | streaming pull: ACK on success, NACK on failure | native — subscription retry policy + `DeadLetterPolicy` `maxDeliveryAttempts`; no app counter by design | subscription `DeadLetterPolicy` (delegated) | log + `OnBackgroundFailure`; no DLQ write possible | drain + close 5 s (subscriber-client stop; validated in both ack modes) | — (the Pub/Sub client manages the ack deadline) |
| **Kafka** | manual offset store, auto-committed every `OffsetCommitInterval` 5 s; offsets cannot NACK one message | in-process retries with backoff (100 ms → 5 s); counted per process delivery — a restart before the commit resets the count | `{topic}.deadletter` (or one `DeadLetterTopic`); declared by `CreateTopics` (default on) | retried in-process, then produced to the DLQ topic + log + `OnBackgroundFailure` | drain only | — (stay under `max.poll.interval.ms` instead) |
| **MongoDB** | claimed document: delete on success, reschedule after `RedeliveryDelay` 5 s on failure | store — the `findOneAndUpdate` claim increments the attempt | `deadletter` logical queue in the same collection; `DeadLetterEnabled` default on, optional `DeadLetterRetention` | DLQ document + log + `OnBackgroundFailure` | drain + close 5 s (change-stream listen join) | automatic fenced renewal at `LockTimeout`/3 (server-clock `$$NOW`, `lock_id` fence) |
| **NATS** | JetStream explicit ack: ACK on success, NAK + `RedeliveryDelay` 5 s on failure | broker `NumDelivered`; at `MaxDeliveryAttempts` the message is dead-lettered and settled — including one whose earlier attempts never settled, refused before execution | `{prefix}.transport.deadletter` subject/stream (capped at `DeadLetterStreamMaxMessages` 100,000); declared by `CreateStreams` (default on) | published to the DLQ subject + log + `OnBackgroundFailure` | drain only | automatic in-progress heartbeat every `AckWait`/3 across the in-flight batch (not configurable) |
| **PostgreSQL** | claimed row: delete on success, reschedule after `RedeliveryDelay` 5 s on failure | store — the `FOR UPDATE SKIP LOCKED` claim increments `attempts` | `deadletter` logical queue in the same table; `DeadLetterEnabled` default on, optional `DeadLetterRetention` | DLQ row + log + `OnBackgroundFailure` | drain + close 5 s (LISTEN task join) | automatic fenced renewal at `LockTimeout`/3 (`lock_id` fence) |
| **RabbitMQ** | per-delivery `basic.ack`; `basic.nack` + requeue on failure | broker `x-death` header + `redelivered` flag, judged before the handler runs; **`MaxDeliveryAttempts` default `0` = unlimited**; values > 2 need a TTL-retry DLX cycle (see notes) | optional `DeadLetterExchange` (default `null` → exhausted messages are **dropped**); declared when set and `DeclareTopology` is on; a message capped after riding the DLX cycle is parked in `ParkQueue` (else `DeadLetterQueue`) | published to the `DeadLetterExchange` when configured + log + `OnBackgroundFailure`; a copy that returns through the DLX and fails again is parked instead | drain + close 5 s ×2 (consumer cancel, then channel/connection close) | — (unacked deliveries hold no expiring lock) |
| **Redis** | consumer group: `XACK` on success; a failed entry stays in the PEL and is reclaimed after `PendingMessageMinIdleTime` 30 s | broker — PEL delivery count (`XPENDING`) + 1 at claim; at `MaxDeliveryAttempts` the entry is dead-lettered + `XACK`ed | `{prefix}:transport:deadletter` stream (`XADD` auto-creates it; trimmed at `DeadLetterStreamMaxLength` 100,000); `DeadLetterEnabled` default on | `XADD` to the DLQ stream + log + `OnBackgroundFailure` | drain only | automatic idle-reset heartbeat (`XCLAIM … JUSTID`, no delivery-count bump) every `PendingMessageMinIdleTime`/3 while an entry is in flight (not configurable) |
| **SQS** | visibility settle: delete on success; failure lets the visibility timeout lapse (or shortens it to `RedeliveryDelay`) | native `ApproximateReceiveCount` + redrive `maxReceiveCount`; no app counter by design | native redrive DLQ; delegated — or declared by `CreateQueues` (default **off**): `{queue}-dlq` + `MaxReceiveCount` 5 | log + `OnBackgroundFailure`; no DLQ write possible | drain + close 5 s (visibility-renewal join on the final batch — spent with renewal off too; validated in both ack modes) | opt-in `VisibilityRenewalInterval` (default off) |
| **SqlServer** | claimed row: delete on success, reschedule after `RedeliveryDelay` 5 s on failure | store — the `UPDLOCK/READPAST` claim increments `attempts` | `deadletter` logical queue in the same table; `DeadLetterEnabled` default on, optional `DeadLetterRetention` | DLQ row + log + `OnBackgroundFailure` | drain only | automatic fenced renewal at `LockTimeout`/3 (`lock_id` fence) |

## Delayed delivery (`IDelayedWorkerTransport`)

Delayed worker jobs — `EnqueueWorkerAsync(..., delay)` and the wake-ups behind suspended
durable-flow timers — are a per-transport capability. Envelopes carry their absolute due time
(`NotBeforeUtc`); the shared worker-job executor re-publishes any early delivery for the
remainder, which is how capped transports chunk long delays with no transport-specific code. Any
single delay is capped at 3,650 days.

| Transport | Native delayed delivery | Per-hop cap | Mechanism / caveats |
|---|---|---|---|
| **InMemory** | ✅ | ~49.7 days (chunked) | `TimeProvider` timer wheel; the per-hop cap is the .NET timer ceiling (virtual-clock aware in tests); delayed jobs die with the process, logged at shutdown; at most `DelayedJobCapacity` (4096) held at once — external publishers wait, in-job publishes are rejected |
| **AzureServiceBus** | ✅ | — | scheduled messages (`ScheduledEnqueueTime`); broker-held, survives restarts |
| **SQS** | ✅ | 15 min (chunked) | `DelaySeconds`; standard queues only — a FIFO worker queue advertises no delay capability (`MaxPublishDelay` = zero), so flow timers wait in process and a delayed enqueue fails fast at publish |
| **PostgreSQL** | ✅ | — | insert with `available_at = now() + delay` (database clock); pickup latency ≤ `EmptyPollDelay` |
| **SqlServer** | ✅ | — | insert with `available_at = SYSUTCDATETIME() + delay`; pickup latency ≤ `EmptyPollDelay` |
| **MongoDB** | ✅ | — | insert stamps `available_at` server-relative (`$$NOW + delay`) via an atomic upsert pipeline, so client clock skew cannot shift it; an insert due more than a second out does not wake the queue's change-stream watchers; pickup latency ≤ `EmptyPollDelay` |
| **Kafka, RabbitMQ, GooglePubSub, Redis, NATS** | — | — | no native mechanism; flow timers wait in process under the lease, and a bare delayed enqueue throws with guidance |

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
| Redis | `RedisBackgroundFailureContext` | stream, consumer group, subscriber role, entry id (`MessageId`), correlation id, exception |
| SQS | `SqsBackgroundFailureContext` | queue, subscriber role, message id, receive count, correlation id, exception |
| SqlServer | `SqlServerBackgroundFailureContext` | queue, subscriber role, attempt, correlation id, exception |

## Notes per transport

Only what needs more than a table cell. The shared hand-back, intake-gate and drain rules above
are not repeated here except where a transport differs.

### Azure Service Bus

- **One message at a time in ack-after-handler mode (worker).** Service Bus locks — and, when a
  lock lapses, counts — every message a receive hands over, so a batch would let a
  process-killing handler spend its batch-mates' `DeliveryCount` and keep them locked while idle
  peers wait. `MaxMessagesPerReceive` applies to early ACK and to the response subscriber, whose
  handler is not always short: a response whose waiter is gone runs that correlation's recovery
  callbacks inline, retries included, so with `ResponseSubscriber.LockRenewalInterval = null` slow
  callbacks can let the locks of the batch-mates behind them lapse and a peer runs those callbacks
  too (callbacks are at-least-once by contract; keep renewal on where they can be slow).
- **Host stop.** From `ApplicationStopping` the worker subscriber stops receiving, and a batch
  received as the stop began is abandoned rather than dispatched (under early ACK, never completed
  first). A delivery the engine hands back is left locked, not abandoned.
- **Lock renewal.** `LockRenewalInterval` (10 s, cancellable per beat) renews the peek-lock of the
  message in the handler, so slow handlers do not hit `MessageLockLostException` redeliveries of
  already-processed messages
  ([troubleshooting](troubleshooting.md#azure-service-bus-messagelocklostexception-redeliveries-of-already-processed-messages)).
  It must be under 5 minutes (the `LockDuration` maximum; Azure's default is 60 s) and is ignored
  in early ACK. The renewal is not ended by the host stop — a running handler keeps its lock until
  it returns — while messages of the batch that never started are abandoned so a peer takes them
  at once. Renewal failures are logged and processing continues (the message simply redelivers);
  a lock reported lost (`MessageLockLost`) is logged once and not renewed again; a renewal racing
  the handler's own Complete/Abandon/DeadLetter is skipped, and one failing behind a settle is
  logged at Debug.
- **Shutdown budget.** The renewal join (bounded by `ShutdownTimeout`) runs before the receiver
  close, so in ack-after-handler mode `HostShutdownTimeout` must fit `2 × ShutdownTimeout` with
  renewal on and `1 ×` with `LockRenewalInterval = null`; in early ACK it must fit
  `BackgroundDrainTimeout + ShutdownTimeout` (the hand-back of unstarted messages and the close
  share that one term).
- **In-flight ceiling with renewal off.** The worker transport then advertises the 5-minute
  `LockDuration` maximum to the durable-flow engine — only an upper bound, since the transport
  never reads the entity's real `LockDuration` (60 s by default) — and the worker subscriber warns
  at startup.
- `PrefetchCount` (default 0) buffers locked messages the renewal heartbeat never reaches. In
  ack-after-handler mode keep `PrefetchCount × handler latency` well under `LockDuration`, or
  leave it at 0; in early ACK the buffer ages the same way while the background queue is
  saturated, so keep it at 0 or well under what the workers drain within one `LockDuration`.
  Startup warns when it is positive in either mode.
- In early ACK the receive loop waits for background-queue capacity before receiving, so
  queue-full abandons cannot burn `DeliveryCount` in steady state.
- Every abandon burns broker `DeliveryCount`, which also counts toward the *entity's*
  `MaxDeliveryCount`. `MaxDeliveryAttempts = 0` disables the package-level dead-letter decision
  and leaves poison handling entirely to that broker policy.
- Queue names are compared case-insensitively in the worker/response and reply-target collision
  guards: Service Bus entity names are case-insensitive, so `Jobs` and `jobs` are one entity.
- A message the transport cannot project — `ServiceBusReceivedMessage.Body` throws
  `NotSupportedException` for an AMQP **Value** or **Sequence** body, which JMS and raw AMQP
  producers send — is dead-lettered on its own with reason `AsyncResponseUnsupportedBody`; the rest
  of the batch is unaffected. If that dead-letter fails, the lock lapses and the broker redelivers.
- Dead-letter descriptions are truncated to 4,096 characters: Service Bus rejects a longer one
  client-side with `ArgumentOutOfRangeException`, which would otherwise leave the message
  un-dead-letterable until the entity's own `MaxDeliveryCount`.

### Google Pub/Sub

- The transport intentionally has no `MaxDeliveryAttempts` and no library-managed dead-letter:
  attempts and dead-lettering are the subscription's `RetryPolicy` and `DeadLetterPolicy`,
  configured in GCP (startup warns when it cannot see a `DeadLetterPolicy`). A failed handler
  NACKs and Pub/Sub redelivers per those policies.
- In early ACK, streaming-pull flow control is bounded to `BackgroundQueueCapacity`
  (`maxOutstandingElementCount`), and a full background queue **parks the delivery callback until
  capacity frees instead of NACKing** — a queue-full NACK would burn a `DeadLetterPolicy` attempt
  on a healthy, never-executed message. A NACK is returned only when the enqueue fails during
  shutdown/dispose. From `ApplicationStopping` the early-ACK worker acknowledges nothing new: the
  streaming pull cannot stop short of the client stop, so each delivery arriving then — and any
  parked in the queue-full wait — is **held unstarted** and handed back with the client stop, at
  the cost of one `DeadLetterPolicy` attempt each.
- The ack-after-handler worker is not gated at `ApplicationStopping`: it keeps starting deliveries
  until its own stop (holding every delivery after the first hand-back would cost non-flow jobs an
  attempt each).
- **Stop in ack-after-handler mode drains first, then stops the client.** The subscriber waits for
  the running handlers for up to `BackgroundDrainTimeout` — shortened to what
  `HostShutdownTimeout` still leaves after `ShutdownTimeout`, measured from `ApplicationStopping`
  because hosted services stop one after another — and only then stops the client
  (`NackImmediately`, bounded by `ShutdownTimeout`). The SDK's stop hands every message still in
  leasing back at once for any timeout under its 30-second hard-stop window, dropping the eventual
  ACK of a handler still running, so it must come second. A handler parked on an awaited
  durable-flow response is not interrupted by the host stop: unless its response arrives, it costs
  the drain its whole bound and is redelivered.
- **Deliveries arriving during the drain, and hand-backs, are held — not NACKed.** An immediate
  NACK would be redelivered straight back to the still-running stream, looping through the
  `DeadLetterPolicy`'s attempts. Held deliveries keep their flow-control slots, so the pull stalls
  once the slots are full; each handler finishing during the drain frees a slot the pull refills
  with a delivery that is then held. Each held delivery costs one `DeadLetterPolicy` attempt (plus
  the `RetryPolicy` delay) when the client stop hands it back — at most one refill per slot freed,
  per replica stop. Size `MaxOutstandingMessages` (default 1,000) and the policy's
  `maxDeliveryAttempts` for rolling deploys accordingly.
- Reply targets name the correlation attribute (`correlationIdAttribute`) a remote producer must
  set. A correlation id over the 1,024-byte attribute-value limit is published without the
  attribute (the worker path reads the id from the body).

### Kafka

- **Redelivery is in-process.** Kafka offsets cannot NACK a single message, so a failing handler is
  retried with backoff (`HandlerRetryBaseDelay` 100 ms → `HandlerRetryMaxDelay` 5 s) up to
  `MaxDeliveryAttempts`, stalling that partition — and only that partition — while it retries
  (size `TopicNumPartitions`, default 8, for parallelism). Attempts are counted per process
  delivery: a consumer restart before the offset commit resets the count. The message that
  exhausts its attempts is produced to the dead-letter topic with failure-detail headers and its
  offset stored, so the partition keeps moving.
- **Early ACK and `MaxDeliveryAttempts = 0`.** The same retry-then-dead-letter path runs for
  background failures after an early ACK, but `0` means unlimited retries only in
  ack-after-handler mode; under early ACK it means a **single** attempt (the offset is already
  committed, so unbounded retries would wedge the background worker), after which the message is
  dead-lettered and surfaced via `OnBackgroundFailure`. An early-ACK hand-back ends the ladder at
  once and is dead-lettered as `handed_back_after_commit` even when it arrives after the drain
  budget lapsed.
- **A long handler never stalls the poll loop.** In ack-after-handler mode a handler is awaited
  inline for `DetachHandlerAfter` (default 1 s); one still running past that is detached: its
  partition is paused (nothing is fetched or buffered for it), the handler and its retry ladder
  run on the thread pool, and the poll thread keeps polling — other partitions keep flowing,
  `max.poll.interval.ms` is honored, and rebalance callbacks fire. The poll thread (the only
  thread that touches the consumer) stores the offset and resumes the partition within one
  `BackpressurePollDelay` of the handler settling. Detached handlers for different partitions run
  concurrently. A stop waits for detached handlers still in their partition's current assignment
  and commits their offsets
  ([troubleshooting](troubleshooting.md#kafka-rebalances-or-duplicate-runs-while-long-handlers-execute)).
- **Detached handlers are revocation-aware.** Every revoke (or loss) of a partition advances its
  assignment generation and lifts the pause taken behind a detached handler (librdkafka would
  otherwise keep it across a revoke and re-assignment, and the partition would never fetch again).
  The package keeps librdkafka's default assignor (`range,roundrobin`, the eager protocol), which
  revokes *every* partition on every rebalance and usually hands most of them straight back. The
  first message of the new assignment (it starts at the group's committed offset) decides what
  happens to a handler still running from before the revoke:
  - **past the running message** — another member committed beyond it: the handler is orphaned.
    Its offset is never stored (Kafka's offset commit is not monotonic, so a store would rewind the
    group past a peer's commits), nothing of the new assignment is held behind it, and a stop
    neither waits for it nor stores it; its outcome is only logged. The partition's current owner
    re-consumes the message (at-least-once).
  - **at or before the running message** — the handler keeps the partition, the redelivered
    messages wait behind it, and those it has already settled are dropped instead of running a
    second copy alongside the first.

  A handler that finishes before any message of the new assignment arrives is treated as
  orphaned, so its message runs once more afterwards (at-least-once, never concurrently).
- **Early-ACK backpressure.** A full background queue pauses consumption on all assigned
  partitions (re-checked every `BackpressurePollDelay` 50 ms) rather than dropping or
  re-fetching. The pause holds across a rebalance (librdkafka keeps it on a partition revoked and
  handed back) and is lifted once the queue has room — on a partition unassigned at that moment,
  as soon as it is assigned again.
- **Worker intake at host stop.** The assignment is paused while the poll loop keeps polling for
  group liveness; a record fetched before the pause is neither started nor stored (the partition's
  next owner redelivers it), and messages held behind a detached handler are not started (the
  running handler still settles and stores its offset). A handed-back message's partition is
  parked for the rest of the stop, so no later offset of it is stored past the unsettled message.
  Two at-least-once edges remain (never a
  loss): a detached handler whose partition is revoked and re-assigned during the stop cannot be
  re-adopted, so its message runs again on the next owner; and a malformed message held behind a
  detached handler is buried off the poll thread, so a revoke during that burial can leave a
  second dead-letter copy.
- **The early-ACK queue outlives the consumer.** One dispatcher serves every consumer the
  supervisor builds; a poll-loop fault rebuilds the consumer while the queued, already-committed
  work keeps running. At stop, the reserved quarter dead-letters still-queued messages first, then
  reports each buried one through `OnBackgroundFailure`, waiting for each callback only while the
  reserve lasts.
- **Malformed messages stay in partition order.** A message that cannot be projected (empty
  payload, unresolvable correlation id) is produced to the dead-letter topic and its offset stored,
  ignoring the stopping token like every other settlement. Consumed behind a detached handler of
  the same partition, it is held like a valid delivery and buried in its turn, so the partition is
  never committed past the unfinished message. At most one burial blocks the poll thread per poll:
  a malformed message held behind a detached handler is buried detached, like a handler. A
  `StoreOffset` that throws because a rebalance revoked the partition is logged at Information and
  the message redelivers to the new owner.
- **Dead-letter copies replace, never stack, burial headers, and fit the producer's size limit.**
  A record replayed from a dead-letter topic (one carrying both `sourceTopic` and `sourceOffset`)
  has its earlier `sourceTopic`/`reason`/`exception*` set replaced; any other record keeps all of
  its own headers. `exceptionMessage` (then `exceptionType`) is shortened so the copy stays under
  the producer's `message.max.bytes` (from `ConfigureProducer`, default 1,000,000). The other burial
  headers cannot be shortened, so while `DeadLetterEnabled` the worker transport refuses at publish
  a job whose record plus their widest size would exceed the limit (an `InvalidOperationException`
  naming the sizes; nothing is published). A durable-flow start refused this way surfaces as
  `DurableFlowNotDispatchedException`; do not retry it — raise the producer's `message.max.bytes`
  (or shrink the input). A record that still leaves no room (a foreign one, or one published under
  a larger limit) fails its burial with an error naming the size and `ConfigureProducer`.
- **Dead-letter produces are time-bounded.** Every dead-letter produce's retry ladder is bounded to
  a quarter of the effective `max.poll.interval.ms` (`MaxPollInterval`, default 5 min, or the
  value `ConfigureConsumer` sets), so an undeliverable dead-letter topic (auto-create off, a
  leaderless partition, an over-sized payload) cannot evict the consumer mid-burial.
- **A burial that fails for good faults the subscriber** (ack-after-handler mode and the
  malformed-message discard). Kafka commits a partition *position*, so leaving the failed offset
  unstored protects nothing — the next successful settlement would commit past it. Instead the poll
  loop throws, the consumer closes without storing past the message, and the supervisor rebuilds
  it after its backoff (`SubscriberRetryBaseDelay` → `SubscriberRetryMaxDelay`); the restarted
  consumer re-runs the handler up to `MaxDeliveryAttempts` and retries the burial — a loud,
  bounded-rate loop that parks **every** partition of that subscriber at the poison message until
  the dead-letter topic is fixed or `message.max.bytes` is raised (each restart logs which).
  Messages committed at enqueue (early ACK) are outside this rule: their burial failure is logged
  and surfaced through `OnBackgroundFailure`.
- **A poll-loop failure tears down within `FaultDrainTimeout`** (default 5 s; `0` abandons at
  once). The consumer is closed and rebuilt by the supervisor after its backoff; detached handlers
  that settle within the budget get their offsets stored and committed by the close. The rest are
  abandoned: their offsets stay unstored and their messages redeliver on the rebuilt consumer —
  possibly while the abandoned handler still runs (a durable flow's lease makes that a no-op; a
  plain worker job must be idempotent); their retry ladders are cancelled and each eventual
  outcome is logged. A graceful stop is bounded by the host's shutdown budget instead.
- **Publish retries are for transient errors only.** `Local_MsgTimedOut` (raised only after
  librdkafka already retried for `message.timeout.ms`), message/record-size and topic/cluster
  authorization errors fail the publish at once instead of burning `PublishMaxAttempts`.
- **Consumer groups are not derived from `TopicPrefix`.** Deployments sharing a cluster need
  their own `WorkerConsumerGroup`/`ResponseConsumerGroup` as well as their own prefix: a shared
  group rebalances every member whenever any one of them joins or leaves.

### RabbitMQ

- **Attempt counting.** The broker does not count plain `basic.nack` requeues: the resolved
  attempt is `max(x-death count, redelivered ? 1 : 0) + 1`, which never exceeds 2 on its own. A
  `MaxDeliveryAttempts` above 2 therefore only takes effect when the dead-letter path forms a
  TTL-retry cycle that re-delivers the message (each dead-letter hop increments `x-death`; once
  `x-death` is present every retry below the cap rejects without requeue so the cycle does the
  counting). Without such a cycle it behaves like 2 and logs a startup warning
  ([troubleshooting](troubleshooting.md#rabbitmq-startup-warns-about-maxdeliveryattempts-or-a-poison-message-loops-forever)).
- **The cap is judged before the handler runs** as well: a delivery whose previous attempt ended
  without an exception (the process was killed mid-handler and the broker requeued it with
  `redelivered` set) is dead-lettered (or parked) without executing.
- **At the cap with `x-death` present the message is parked, terminally.** It is copied to
  `ParkQueue` — or, without one, `DeadLetterQueue` — through the default exchange (bypassing the
  cycling `DeadLetterExchange`) and ACKed; with neither configured it is ACKed and **dropped** with
  an error log. `ParkQueue` is declared unbound, so unlike `DeadLetterQueue` it never collects the
  hops of messages still riding the cycle. A parked copy carries the original headers, `x-death`
  included — strip it when replaying.
- **Publisher confirmations on the subscriber channel.** With `DeadLetterExchange`,
  `DeadLetterQueue` or `ParkQueue` set, a park or dead-letter copy the broker cannot route (a
  missing queue, a full `reject-publish` queue) fails loudly: the delivery is NACKed with requeue
  after the `SubscriberRetryBaseDelay`/`SubscriberRetryMaxDelay` backoff and the park retries on
  redelivery.
- **Unlimited by default.** `MaxDeliveryAttempts = 0` means unlimited requeues — a poison message
  hot-loops until a cap (with a `DeadLetterExchange`) is configured. With a cap but no
  `DeadLetterExchange`, the exhausted message is rejected without requeue and **dropped** (a reject
  before the handler ran is logged at Error). On RabbitMQ 4.x **quorum queues** the broker enforces
  its own `delivery-limit` (20 by default) regardless: past it the message is dead-lettered, or
  dropped when no dead-letter exchange is set — configure one, or raise/disable `delivery-limit`
  by policy.
- **`consumer_timeout` counts from the send, not from the handler.** The broker closes a channel
  whose delivery stays unacknowledged longer than its `consumer_timeout` (mirrored by
  `BrokerConsumerTimeout`, 30 minutes by default) and requeues every unacknowledged delivery.
  Deliveries are handled one at a time, so prefetched ones age while they wait: the worker
  transport advertises `BrokerConsumerTimeout / PrefetchCount` (default `PrefetchCount` 16) as its
  in-flight ceiling, and durable-flow timers park in process for at most half of that per
  delivery. `PrefetchCount = 1` gives timers the whole timeout. The advertised value is floored at
  **one minute** (capped at `BrokerConsumerTimeout` when that is shorter) so a large prefetch
  cannot turn timers into a republish storm. `PrefetchCount = 0` (AMQP "unlimited") is rejected at
  startup. When the share falls below the floor — `PrefetchCount` above 30 at the default timeout —
  the worker subscriber warns at startup in ack-after-handler mode.
- **Early-ACK backpressure.** A full background queue parks the delivery on the bounded
  in-process channel until capacity frees; a NACK with requeue is sent only when that enqueue fails
  during shutdown/dispose (or the channel it arrived on dies). A parked delivery — and every
  prefetched one behind it — stays unacknowledged and keeps ageing against `consumer_timeout`:
  with long background jobs, size `BackgroundQueueCapacity`/`PrefetchCount` so sustained
  saturation stays well inside it, or disable the timeout for that queue (`x-consumer-timeout`).
- **The early-ACK queue outlives the channel.** One dispatcher serves every channel the supervisor
  builds, so a broker restart or channel close does not drop already-ACKed work, and a later
  background failure publishes its dead-letter copy through the live channel. A failed job's copy
  that comes back through the dead-letter exchange and fails again is parked in
  `ParkQueue`/`DeadLetterQueue` (dropped with an error when neither is set) — for handler failures
  only; a returned copy handed back at host stop, or lapsing in a drain, goes to the dead-letter
  exchange again.
- **A graceful stop does not cut off the running handler (ack-after-handler).** The stop cancels
  the consumer and waits for the handler still running in its delivery callback — up to
  `BackgroundDrainTimeout`, shortened (not validated) to what `HostShutdownTimeout` leaves after
  the two `ShutdownTimeout` spends — so its ACK lands before the channel closes; deliveries the
  client had already buffered are not started and are redelivered. Past the bound the close goes
  ahead and the running delivery is redelivered. When `HostShutdownTimeout` leaves no wait at all
  (for example 10 s against two default 5 s spends) the subscriber says so once at startup
  (Information). A consumer cancel that fails or outlives `ShutdownTimeout` is logged and the stop
  still drains before closing; a delivery arriving once the early-ACK drain has begun is left
  unacknowledged for the close to requeue.
- **Shutdown budget.** Shutdown spends `ShutdownTimeout` twice — consumer cancel, then channel and
  connection close after the drain — and early-ACK validation sums both plus
  `BackgroundDrainTimeout` against `HostShutdownTimeout`. `ShutdownTimeout` must be positive and
  timer-backed in both ack modes.
- **A host-stop hand-back comes back `redelivered`.** The channel close requeues the unacknowledged
  delivery with `redelivered` set — attempt 2 on the next host (unless `x-death` counts further).
  With `MaxDeliveryAttempts = 1` that is past the cap, so the redelivery is rejected before its
  handler runs and the flow's wake-up is lost; the same happens to any delivery requeued under a
  running handler. The worker subscriber therefore warns at startup for `MaxDeliveryAttempts = 1`
  in ack-after-handler mode — use 2 or more (or 0). Deliveries held by the host-stop intake gate
  come back as attempt 2 for the same reason; under early ACK, a delivery waiting for a free
  background slot at `ApplicationStopping` is NACKed back once. Under early ACK a hand-back's
  dead-letter copy carries an `AR-DeadLetter-Reason` starting `handed_back_after_commit:`.
- A correlation id longer than an AMQP short string (255 UTF-8 bytes — as few as ~86 characters
  above U+0800) travels in the `CorrelationIdHeader` header only; the native `correlation-id`
  property is left unset.

### Redis

- **The Redis *channel* (pub/sub) is fire-and-forget, and its per-wait buffer is bounded.** A
  publisher is never backpressured and the StackExchange.Redis subscription queue is unbounded, so
  the channel buffers at most 1,024 responses per correlation id behind the wait's serial
  processing (an `Until` predicate runs one message at a time). A response that finds that buffer
  full faults the wait with the overload form of `AsyncResponseIndeterminateDeliveryException` (a
  terminal response may be among the queued or refused ones), unsubscribes, and counts
  `asyncresponse.channel.overloaded_waits` — never buffered without bound, never silently dropped.
  Durable flows restart the (idempotent) step. Where a backlog must be lossless, use a database
  channel (PostgreSQL, SQL Server, MongoDB), whose backlog stays server-side.
- Requires **Redis 6.2+** (or a compatible server): the reclaim loop uses `XPENDING … IDLE`, and
  on an older server every subscriber attempt fails on it.
- **Delivery and reclaim.** New entries arrive via `XREADGROUP` at attempt 1. A reclaim loop scans
  the pending-entries list every `PendingClaimInterval` (5 s, monotonic clock) and `XCLAIM`s
  entries idle longer than `PendingMessageMinIdleTime` (30 s), so a crashed consumer's in-flight
  work is retried by a peer; the attempt is the PEL delivery count + 1. The ack-after-handler
  reclaim loop re-reads each candidate's pending entry right before claiming it, so the attempt
  reflects claims a peer made meanwhile.
- **One entry at a time in ack-after-handler mode.** Redis counts a delivery when `XREADGROUP` or
  `XCLAIM` hands an entry over, not when a handler starts. So `AckAfterHandlerCompletes` reads one
  entry at a time, and although `XPENDING` lists up to `PendingClaimBatchSize` (16) reclaim
  candidates, each is `XCLAIM`ed only right before it runs: nothing queued behind a
  process-crashing handler has its count bumped without running, and nothing is pinned behind a
  handler that runs for hours. Early ACK keeps `BatchSize` (16) reads and batch claims, clamped to
  the dispatcher's free capacity so backpressure pauses consumption instead of spending attempts
  (Redis has no NACK).
- **Heartbeat.** While an entry is in flight, a heartbeat re-claims it (and, in an early-ACK batch,
  the entries still waiting their turn) with `XCLAIM … JUSTID` every `PendingMessageMinIdleTime`/3,
  resetting the idle clock without bumping the delivery count. It is not tied to the stop signal
  and ends only when the loop lets go of the entry.
- **Stop.** Nothing new starts and nothing more is read or claimed (the stream adapter checks the
  stop before sending a command, since Redis queues a command the moment it is called); the rest
  of a batch, and any reclaim candidate not yet claimed, stays pending for a peer. From
  `ApplicationStopping` the worker subscriber reads, claims and starts nothing (an early-ACK entry
  is never enqueued and ACKed after it). A hand-back leaves the entry pending with no failure log
  of its own; after the first hand-back the subscriber reads and claims nothing more until its own
  stop.
- **Early-ACK drain lapse.** Entries still queued when the handler share lapses are dead-lettered
  with reason `drain_budget_lapsed_after_ack` before any callback.
- **Dead-letter failures leave the entry pending.** A dead-letter `XADD` that fails (MISCONF/OOM, a
  timeout, `WRONGTYPE` on the dead-letter key) is logged and leaves the entry pending for the next
  reclaim cycle on every burial path — at-cap and pre-execution burials of both ACK modes and the
  discard of an unparsable entry — instead of faulting the subscriber. Discarding an unparsable
  entry is a settlement and ignores cancellation. With `DeadLetterEnabled = false` a burial is only
  the `XACK`: the entry is dropped with no copy, logged at Error.
- **`StreamMaxLength` (default `100000`) evicts unprocessed work.** Worker publishes append with
  `XADD … MAXLEN ~ N`, and Redis trims by length alone, whatever the consumer group has read: past
  the cap the oldest entries are deleted — including jobs never read and jobs pending in a
  handler — with no dead-letter copy. On Redis 6.2 `XCLAIM` answers a trimmed-while-pending id with
  a nil entry, which is ACKed by its pending id with a Warning (7.0+ drops such an id from the
  pending list itself). Settlement is `XACK` only, so processed entries stay in the stream until
  trimmed. Size the cap well above the deepest backlog an outage can build (throughput × longest
  worker outage), or set `null` to disable trimming and bound the stream operationally. The library
  trims only its own worker publishes; producers writing the **response** stream must apply
  `XADD … MAXLEN ~` themselves.
- **Worker publishes are idempotent across their retry window.** `XADD` has no natural identity,
  so each publish runs one same-slot Lua script (`EVAL`/`EVALSHA` must be permitted by the ACL):
  it appends with `XADD` and only then writes a TTL-bound success marker
  (`{<worker stream>}:publish:<id>`, hash-tagged to the stream's cluster slot, TTL ≈ 2× the retry
  window) holding the entry id. A failed append leaves no marker; a retry after a lost reply finds
  the marker and appends nothing. The script uses the portable `MAXLEN` syntax (validated on Redis
  and Valkey). Besides connection faults and timeouts, the retry covers the replies of a cluster in
  transition — `TRYAGAIN`, `CLUSTERDOWN`, `LOADING`, `MASTERDOWN`, `READONLY` — which the server
  raises before running anything; every other server error fails the publish at once. This is not
  exactly-once execution: handlers must still tolerate redelivery.
- **An acknowledged write is only as durable as your Redis deployment.** A worker publish (a flow
  wake-up included) returns once the primary has run the append script, and the Redis channel's
  recovery registrations once the primary has applied the `SET`; neither waits for a replica
  (`WAIT`) or for the AOF to reach disk. A primary that fails over inside the replication lag — or
  restarts from a snapshot or a non-fsynced AOF — can roll back an acknowledged job or
  registration. Run replicas with AOF persistence (`appendfsync everysec` or `always`) where that
  matters; `WAIT` would narrow the window but not close it. (The MongoDB transport, by contrast,
  waits for a bounded `w: "majority"` — see [its notes](#postgresql-sql-server-mongodb).)
- Flow timers wait in process on Redis (no delayed delivery), holding one delivery for the whole
  remainder under the heartbeat. For long timers set `DurableFlowOptions.MaxInProcessParkDuration`:
  each hop then continues under a fresh delivery with a fresh delivery count, so repeated heartbeat
  outages over a multi-day wait cannot let peers' reclaims spend the attempts that bury the live
  holder's job.
- A subscriber that stops for good deletes its own generated consumer from the group
  (`XGROUP DELCONSUMER`) — only while that consumer owns no pending entries, checked atomically in
  one script. A crashed process's consumer is left behind; a configured `ConsumerName` is never
  deleted.

### NATS

- **The NATS *channel* never loses a response silently to its client buffer.** NATS.Net buffers
  each wait's inbound responses in a bounded subscription channel (the connection's
  `SubPendingChannelCapacity`, 16,384 by default) and drops the newest once it is full. The channel
  listens for the client's `MessageDropped` event: the wait is faulted with the overload form of
  `AsyncResponseIndeterminateDeliveryException`, ended (registration deleted, subscription closed,
  remaining buffer skipped), and counted in `asyncresponse.channel.overloaded_waits`. Durable flows
  restart the (idempotent) step.
- **Attempts are the broker's `NumDelivered`** for the durable JetStream consumer, so counts survive
  subscriber restarts. The consumer is created with `MaxDeliver = -1` because the dispatcher bounds
  attempts itself — including a delivery whose earlier attempts never settled (process killed
  mid-handler, a failed NAK): past the cap it is dead-lettered and TERMed before the handler runs.
- **Dead-letter republish.** It drops every inbound `Nats-*` header (JetStream publish directives
  and server metadata of the *live* publish — `Nats-Msg-Id` would dedupe a second burial, and
  `Nats-Expected-*`, `Nats-Rollup` or `Nats-TTL` would make the DLQ stream refuse it). The
  `AR-DeadLetter-Reason` header is capped at 512 characters so a long exception message cannot
  push the burial past `max_payload`.
- **The dead-letter stream uses limits retention with evict-oldest discard**, capped at
  `DeadLetterStreamMaxMessages` (100,000; `null` disables) — a bounded archive, unlike the work
  streams' work-queue retention, since nothing consumes it and a full work-queue stream would
  reject every burial. JetStream cannot change an existing stream's retention in place: a DLQ
  stream created with another retention keeps it, with a startup warning explaining how to migrate
  (export its dead letters if needed, delete it, and let the subscriber recreate it).
- **Fetching.** Consumption fetches what is already available (`FetchNoWaitAsync`), or long-polls
  for a single message (`FetchAsync`) when idle, and dispatches serially. In ack-after-handler mode
  every fetch takes exactly **one** message whatever `BatchSize` says (JetStream counts a delivery
  when it hands a message over); early ACK fetches up to `BatchSize` (16).
- **In-progress heartbeat.** While a message is in flight, a heartbeat signals in-progress
  (`ProgressAsync`) for it — and, in early ACK, for every still-unsettled message of the batch —
  every third of the shorter of `AckWait` (30 s) and the live consumer's ack wait, so `AckWait`
  only has to survive one heartbeat round-trip, not the slowest handler. The heartbeat is advisory:
  it carries the batch's cancellation token, and the batch joins it for at most one interval — a
  heartbeat wedged on a dead socket is abandoned with a warning, and unsettled deliveries fall back
  to the server's own `AckWait`. Settlements stay uncancelable.
- **Stop and hand-back.** A stop NAKs a batch's unstarted messages with no delay. From
  `ApplicationStopping` the worker subscriber ends an idle long poll at once and fetches nothing
  more; a batch's unstarted messages — including one waiting for room in the early-ACK queue — are
  NAKed with no delay, and nothing is ACKed at enqueue after that point. A hand-back also stops
  fetching until the subscriber's own stop.
- **Early-ACK backpressure.** A full background queue pauses pulling; a message still waiting for
  room when the subscriber stops is NAKed so JetStream redelivers it — backpressure itself never
  churns redeliveries.
- **Existing consumers are never modified** — whatever `CreateStreams` says — so operator-tuned
  settings (`MaxAckPending`, `BackOff`, metadata) survive every start. An existing consumer this
  transport cannot work with is refused by name: a push consumer, an ack policy other than
  explicit, a finite max deliver at or below `MaxDeliveryAttempts` (any finite one when it is 0), a
  headers-only consumer, or one whose filter subject(s) do not capture the transport's subject. A
  refusal does not fail host startup: every subscriber attempt fails with that error (logged as a
  warning, retried with backoff) until the consumer is fixed or deleted. An existing consumer's own
  ack wait is honored — the heartbeat renews inside the shorter of it and `AckWait`; on a consumer
  with a `BackOff`, the live ack wait is its shortest step (the server reports only `BackOff[0]`).
  The drift is logged once as a warning; apply the new value yourself (`nats consumer edit`) to
  get the longer window.
- Header-first correlation reads NATS headers through NATS.Net's default ASCII header encoding,
  which turns every non-ASCII character into `?`. A header that is exactly the `?`-mangled form of
  the body's correlation id yields the body's id, and the worker transport does not stamp the
  header for a non-ASCII id. Remote producers publishing non-ASCII ids should rely on the body path
  (or configure a UTF-8 header encoding on both connections).
- NATS refuses a message above the server's `max_payload` (1 MiB by default) before sending it;
  the worker publish fails such a job on the first attempt instead of running its retry ladder.

### SQS

- **Settlement is the visibility timeout.** `VisibilityTimeout = null` uses the queue's setting, and
  it must exceed the slowest handler
  ([troubleshooting](troubleshooting.md#sqs-duplicate-executions-or-fifo-settings-that-dont-apply)).
  `RedeliveryDelay` optionally shortens a *failed* message's remaining invisibility via
  `ChangeMessageVisibility`; accounting stays native either way — every receive increments
  `ApproximateReceiveCount` and the queue's redrive policy dead-letters after `maxReceiveCount`.
- **One message at a time in ack-after-handler mode (worker).** SQS counts — and starts the
  visibility clock of — every message a receive hands over, so a batch would let a
  process-killing handler bump its batch-mates' receive counts, spend the later positions'
  visibility, and on FIFO run a failed message's same-group batch-mates ahead of its redelivery.
  `MaxMessagesPerReceive` (default 10) applies to early ACK and to the response subscriber, whose
  handler can run slow recovery callbacks inline: set `ResponseSubscriber.VisibilityTimeout` and
  `VisibilityRenewalInterval` where they can be slow. `ReceiveWaitTime = 0` (short polling) backs
  off on the subscriber retry schedule between empty receives.
- **Host stop.** From `ApplicationStopping` the worker subscriber stops receiving, and a batch
  received as the stop began is made visible again rather than dispatched (under early ACK, never
  deleted first). A handed-back delivery is not released at once — replicas stopping alongside
  could bounce it — but its visibility is shortened to `HostShutdownTimeout` (30 s when `null`)
  when `VisibilityTimeout` is longer.
- **`VisibilityRenewalInterval` is off by default**, because extending visibility silently overrides
  redrive timing operators tune on the queue, and on FIFO queues an extended message keeps its
  whole message group blocked if the consumer wedges. When enabled it requires `VisibilityTimeout`
  to be set and longer than the renewal interval, and is ignored in early ACK. Renewal and
  failure-path visibility changes serialize per receipt: once the failure path schedules
  `RedeliveryDelay`, that change is applied after any already-started renewal and the sweep stops
  renewing the message, so the shortened redelivery stays the last change. Waiting for a wedged
  renewal is bounded by `ShutdownTimeout` and host cancellation; if it fails, the failure is logged
  and the original visibility timeout governs redelivery. A renewal that fails — including the AWS
  SDK's client-side HTTP timeout (`TaskCanceledException`) — is logged and the sweep continues;
  the heartbeat ends with the batch, not at the host stop, and unstarted messages of the batch are
  made visible again at once.
- **12-hour in-flight ceiling.** SQS never keeps a message invisible for more than 12 hours from
  its receive, so the heartbeat clamps its last extension, logs one warning ("reached the 12-hour
  SQS in-flight ceiling") and stops renewing; SQS then redelivers the message regardless. The
  worker transport advertises those 12 hours as its in-flight ceiling to the durable-flow engine
  with renewal on; with renewal off it advertises `VisibilityTimeout` — or, when that is unset too,
  still 12 hours, because the transport never reads the queue's own visibility timeout (30 s unless
  configured). The worker subscriber warns at startup in that last configuration: set
  `WorkerSubscriber.VisibilityTimeout` to the queue's value when durable flows run on SQS.
- **FIFO worker queues** group jobs by correlation id; every job without one — durable-flow start,
  resume and wake-up jobs among them, unless the flow was started inside a request scope — shares
  the single `FifoMessageGroupIdFallback` group, which SQS delivers strictly one at a time across
  all consumers. FIFO also keeps flow timers in process, so one flow parked on a timer or an awaited
  step holds that group — and every other flow's jobs — for as long as it waits (the worker
  subscriber warns at startup). Prefer a standard worker queue for durable flows. The fallback must
  itself be a valid `MessageGroupId` (startup validates it).
- **`CreateQueues` is the only provisioning default that is off** — production queues and their
  redrive policies are usually owned by infrastructure code. When on, converging an existing
  `.fifo` queue re-applies only its mutable attributes (`FifoQueue` is create-only). Startup
  validates the queue names it would create — dead-letter names derived with
  `DeadLetterQueueSuffix` included — against the SQS name rule (80 characters of letters, digits,
  `-` and `_`, `.fifo` counted). Provisioning retries only what can still succeed (throttling, 5xx,
  an endpoint not yet accepting connections, eventual consistency right after a create, credentials
  or the instance-metadata endpoint not ready, the SDK's client-side timeout); a deterministic
  rejection such as AccessDenied fails startup at once.
- **Queue strings may be names or URLs — not ARNs.** A `WorkerQueue` or `ResponseQueue` that is not
  a URL must pass the SQS name rule at startup whether or not `CreateQueues` is on. The
  worker/response, derived dead-letter and reply-target collision guards compare two names, or two
  normalized URLs, exactly, and fail startup (or `GetReplyTarget`) on a match. A name and a URL
  sharing the queue name may be one queue or two (the URL may be another account's), so the worker
  subscriber only **warns** about such a pair; configure both sides as URLs to make the comparison
  exact.
- `CorrelationIdAttribute` resolution is case-**sensitive**, unlike every other transport's
  case-insensitive inbound header lookup: AWS message attribute names are case-sensitive, so
  `CorrelationId` and `correlationId` can coexist on one message. Startup validates the name
  against the SQS attribute-name rule (at most 256 ASCII letters, digits, `_`, `-` and `.`; no
  leading, trailing or consecutive periods; no `AWS.`/`Amazon.` prefix in any casing).

### PostgreSQL, SQL Server, MongoDB

- **Claims.** The claim is atomic (`FOR UPDATE SKIP LOCKED` / `UPDLOCK, ROWLOCK, READPAST` /
  `findOneAndUpdate`) and increments the attempt counter in the store, so attempts survive
  process restarts and are visible in the queue table/collection.
- **Lease renewal.** While a handler runs in ack-after-handler mode, a heartbeat renews the claim's
  lease every `LockTimeout`/3 (`LockTimeout` default 30 s), fenced by the claim's `lock_id`
  (MongoDB also evaluates the lease against the server clock via `$$NOW`, so client clock skew
  cannot fence messages in or out). If the fence no longer matches — the lease lapsed and a peer
  re-claimed the row — renewal stops and the fenced ack/NAK no-ops for the stale claim
  (at-least-once preserved, loss logged). Renewal failures are logged and retried on a backoff
  that starts short (a second, or `LockTimeout`/10 when shorter) and doubles (half-jittered) up to
  the beat interval; while the lease is in hand one retry always lands short of its end. Each
  renew attempt is bounded by the beat interval, so a renew hung on a black-holed pooled connection
  or a failover is abandoned and retried on a fresh connection inside the lease. The heartbeat is
  **not** joined before settlement (every settlement is fenced by `lock_id`) and **not** tied to
  the subscriber's stop: the handler takes no token and runs on through the host's stop budget,
  and the heartbeat keeps its lease until the handler actually ends.
- **Host stop.** A hand-back leaves the delivery **unsettled**: no "failed on attempt" warning, no
  NAK, no dead-letter write of its own; the lease lapses and the row is redelivered as its next
  attempt. From `ApplicationStopping` the worker subscriber claims nothing more — not even the rest
  of the batch in hand — and a claim already in flight when the stop began (or an early-ACK claim
  parked on a full background queue) is released at once (a NAK with no delay; its attempt is
  spent) without being started or early-ACKed.
- **Early-ACK park.** A claim parked on a full background queue keeps its lease renewed; if that
  heartbeat reports the lease **lost** — or its renewals still fail once `LockTimeout` has passed
  since the last one that landed — the park drops the delivery instead of running a job a peer may
  own. The lease's age is judged again when a slot frees, so a GC or VM pause cannot enqueue it
  either.
- **`MaxDeliveryAttempts` is enforced before the handler runs**, not only after it throws. A
  delivery that ends any other way (the process dies mid-handler, the lease lapses while the
  database is unreachable) has already had `attempts + 1` stamped by the claim, so a message that
  arrives past the cap is dead-lettered without executing (and released for retry if the
  dead-letter write itself fails). `MaxDeliveryAttempts = 0` still means unlimited.
- **Dead-lettering is fenced by the same `lock_id`** as the ack and NAK: if a peer re-claimed the
  row, the burial no-ops and reports failure rather than copying a message still live under its
  new owner. MongoDB, with no cross-document transaction, writes the DLQ document first under a
  deterministic id and deliberately **keeps** it when the fenced delete does not match — at worst a
  spurious, retention-prunable DLQ entry.
- **The dead-letter queue** is rows/documents in the same table/collection under the
  `DeadLetterQueue` logical name; nothing consumes it, so set `DeadLetterRetention` if entries
  should be pruned instead of kept for inspection. The prune runs after a publish, at most once a
  minute per process (monotonic clock), in bounded batches of 1,000 (SQL Server
  `DELETE TOP (1000)`, PostgreSQL a `ctid … LIMIT 1000` batch, MongoDB one `deleteMany` over up to
  1,000 ids) so it never escalates to a table lock or outruns the command timeout; it keeps
  draining full batches for up to 2 s and warns when it stops with rows remaining. A prune failure
  (or the publisher's token firing mid-prune) is logged and never fails the already-committed
  publish. DDL is owned by `AutoCreateSchema` (PostgreSQL, SQL Server) / `AutoCreateIndexes`
  (MongoDB) — disable when migrations own it.
- **Early-ACK drain reserve.** The already-ACKed entries still queued when the handler share
  lapses are dead-lettered (unless `DeadLetterEnabled = false`) and reported via
  `OnBackgroundFailure` by the background workers within the reserved quarter — their
  rows/documents were deleted by the early ACK.
- **Shutdown components.** Only PostgreSQL and MongoDB spend a `ShutdownTimeout` at stop (bounding
  the LISTEN / change-stream task join). SQL Server has no push channel to join and therefore no
  `ShutdownTimeout` option.
- **MongoDB write concerns.** The transport pins its collection handle to the primary (so a
  `secondaryPreferred` client cannot route the change-stream wake to a lagging secondary) and
  writes its inserts — publishes and dead letters — with `w: "majority"` bounded by a `wtimeout`
  (an inherited `wtimeoutMS` or `journal` is kept; 10 s otherwise), so a failover cannot roll back
  a publish the caller saw succeed. A lapsed `wtimeout` means the insert was applied on the primary
  and only the majority acknowledgement timed out: the write counts as done, logs a Warning, and is
  never retried (a retry could re-create a job a subscriber already ran and deleted). Restore the
  replica set's secondaries (or remove the arbiter) rather than lowering the write concern. Lease
  writes (claim, renew, NAK) and deletes (ack, burial, prune) use an explicit `w: 1`, keeping the
  inherited `journal`: rolling one back only makes the document claimable again, like a lapsed
  lease.
- **MongoDB claim order and indexes.** The claim takes documents in the claim index's own order
  (`queue, available_at, created_at`); immediate publishes stamp `available_at` with the server's
  `$$NOW`, so a NAKed or delayed document queues by when it became due and a claim walks the index
  instead of sorting the due backlog in memory. With `AutoCreateIndexes = true` an equivalent
  index under another name is accepted — unless it is hidden (`hidden: true`): unhide or drop it.
  With `AutoCreateIndexes = false` a one-time read-only check **warns** when no index leads on
  `queue`. The claim and the dead-letter prune pin the **simple** (binary) collation, so a
  collection with a case- or accent-folding default collation cannot let one subscriber claim
  another logical queue's documents; an index built under a folding collation cannot serve those
  queries, so the claim scans there.
- **MongoDB foreign documents.** The claim reads the stamped document as raw BSON: a document the
  transport cannot read — a driver-generated `ObjectId` `_id`, a `payload` written as an embedded
  document, any mistyped field — is **dead-lettered on sight** (fenced by `lock_id`,
  payload/headers kept, reason in `AR-DeadLetter-Reason`) and the claim moves on. Foreign producers
  should write a `binData` UUID `_id` (subtype 4), a string `payload`, and the `[{k, v}]` header
  array the transport itself writes.
