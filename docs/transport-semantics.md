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
  background handlers while a hosted subscriber stops. The database transports split it —
  three quarters for the handlers, one quarter reserved for dead-lettering what is still queued
  when that lapses (see [their notes](#postgresql-sql-server-mongodb)).
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
  counts both against `HostShutdownTimeout` — twice over for RabbitMQ (consumer cancel, then the
  channel/connection close after the drain), and for Azure Service Bus in both ack modes (in
  ack-after-handler mode the renewal-task join and the receiver close are two terms when
  `LockRenewalInterval` is set, one when it is `null`). Transports without the close component
  have no `ShutdownTimeout` option at all.
- **—** — not applicable to that transport.
- Unqualified option names are per-subscriber (`WorkerSubscriber.` / `ResponseSubscriber.`);
  `MaxDeliveryAttempts` defaults to 5 with `0` = unlimited unless the row says otherwise.

## The matrix

| Transport | Ack semantics (default mode) | Attempt counting | Dead-letter destination | After a failure post-early-ACK | Shutdown drain budget | Lock/lease renewal |
|---|---|---|---|---|---|---|
| **AzureServiceBus** | peek-lock: complete on success, abandon on failure | broker `DeliveryCount`; dead-letter at `MaxDeliveryAttempts` (`0` = defer to the entity's `MaxDeliveryCount`) | native dead-letter subqueue (broker built-in, nothing to declare) | log + `OnBackgroundFailure`; the lock is settled, no DLQ write possible | drain + close 5 s (renewal-task join, then receiver/sender close; validated in both ack modes — two `ShutdownTimeout` terms with renewal on, one with it off) | `LockRenewalInterval` 10 s, on by default; `null` disables |
| **GooglePubSub** | streaming pull: ACK on success, NACK on failure | native — subscription retry policy + `DeadLetterPolicy` `maxDeliveryAttempts`; no app counter by design | subscription `DeadLetterPolicy` (delegated to infra) | log + `OnBackgroundFailure`; already ACKed, no DLQ write possible | drain + close 5 s (subscriber-client stop) | — (the Pub/Sub client manages the ack deadline) |
| **Kafka** | manual offset store, auto-committed every `OffsetCommitInterval` 5 s; offsets cannot NACK one message | in-process retries with backoff (100 ms → 5 s); counted per process delivery — a restart before the commit resets the count | `{topic}.deadletter` (or one `DeadLetterTopic`); declared by `CreateTopics` (default on) | retried in-process, then log + `OnBackgroundFailure` + produced to the DLQ topic | drain only | — (stay under `max.poll.interval.ms` instead) |
| **MongoDB** | claimed document: delete on success, reschedule after `RedeliveryDelay` 5 s on failure | store — the `findOneAndUpdate` claim increments the attempt | `deadletter` logical queue in the same collection; `DeadLetterEnabled` default on, optional `DeadLetterRetention` | log + `OnBackgroundFailure` + DLQ document | drain + close 5 s (change-stream listen join) | automatic fenced renewal at `LockTimeout`/2 (server-clock `$$NOW`, `lock_id` fence) |
| **NATS** | JetStream explicit ack: ACK on success, NAK + `RedeliveryDelay` 5 s on failure | broker `NumDelivered`; at `MaxDeliveryAttempts` the message is ACKed + dead-lettered — including a delivery whose earlier attempts never settled (process killed mid-handler), refused before execution | `{prefix}.transport.deadletter` subject/stream; declared by `CreateStreams` (default on) | log + `OnBackgroundFailure` + published to the DLQ subject | drain only | automatic in-progress heartbeat every `AckWait`/3 across the in-flight batch (not configurable) |
| **PostgreSQL** | claimed row: delete on success, reschedule after `RedeliveryDelay` 5 s on failure | store — the `FOR UPDATE SKIP LOCKED` claim increments `attempts` | `deadletter` logical queue in the same table; `DeadLetterEnabled` default on, optional `DeadLetterRetention` | log + `OnBackgroundFailure` + DLQ row | drain + close 5 s (LISTEN task join) | automatic fenced renewal at `LockTimeout`/2 (`lock_id` fence) |
| **RabbitMQ** | per-delivery `basic.ack`; `basic.nack` + requeue on failure | broker `x-death` header + `redelivered` flag, judged before the handler runs; **`MaxDeliveryAttempts` default `0` = unlimited**; values > 2 need a TTL-retry DLX cycle, and at the cap a message that has ridden it is parked terminally (see notes) | optional `DeadLetterExchange` (default `null` → exhausted messages are **dropped**); declared when set and `DeclareTopology` is on; a message capped after riding the DLX cycle is parked in `DeadLetterQueue` via the default exchange (ACKed and dropped when none is configured) | log + `OnBackgroundFailure` + published to the `DeadLetterExchange` when one is configured (the early ACK already foreclosed the native reject-without-requeue route; with a DLX configured the subscriber channel enables publisher confirmations, so an unroutable copy fails loudly instead of logging a false success) | drain + close 5 s ×2 (consumer cancel, then channel/connection close) | — (unacked deliveries hold no expiring lock) |
| **Redis** | consumer group: `XACK` on success; a failed entry stays in the PEL and is reclaimed after `PendingMessageMinIdleTime` 30 s | broker — PEL delivery count (`XPENDING`) + 1 at claim; at `MaxDeliveryAttempts` the entry is dead-lettered + `XACK`ed | `{prefix}:transport:deadletter` stream (`XADD` auto-creates it); `DeadLetterEnabled` default on | log + `OnBackgroundFailure` + `XADD` to the DLQ stream | drain only | — (`PendingMessageMinIdleTime` must exceed the slowest handler) |
| **SQS** | visibility settle: delete on success; failure lets the visibility timeout lapse (or shortens it to `RedeliveryDelay`) | native `ApproximateReceiveCount` + redrive `maxReceiveCount`; no app counter by design | native redrive DLQ; delegated — or declared by `CreateQueues` (default **off**): `{queue}-dlq` + `MaxReceiveCount` 5 | log + `OnBackgroundFailure`; already deleted, no DLQ write possible | drain + up to `ShutdownTimeout` joining the visibility-renewal task on the final batch | opt-in `VisibilityRenewalInterval` (default off); suppressed per message once the failure path schedules `RedeliveryDelay` |
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
| **SQS** | ✅ | 15 min (chunked) | `DelaySeconds`; standard queues only — a FIFO worker queue advertises no delay capability (`MaxPublishDelay` = zero), so flow timers fall back in process and a delayed enqueue fails fast at publish |
| **PostgreSQL** | ✅ | — | insert with `available_at = now() + delay` (database clock); pickup latency ≤ `EmptyPollDelay` |
| **SqlServer** | ✅ | — | insert with `available_at = SYSUTCDATETIME() + delay`; pickup latency ≤ `EmptyPollDelay` |
| **MongoDB** | ✅ | — | insert stamps `available_at` server-relative (`$$NOW + delay`) via an atomic upsert pipeline, so client clock skew cannot shift it |
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
  `MaxMessagesPerReceive × handler latency` before settlement. `LockRenewalInterval` (10 s,
  cancellable per beat) renews the peek-lock of every unsettled batch message — including the
  one in the handler — so slow handlers do not hit `MessageLockLostException` redeliveries of
  already-processed messages
  ([troubleshooting](troubleshooting.md#azure-service-bus-messagelocklostexception-redeliveries-of-already-processed-messages)).
  Renewal failures are logged and processing continues — the message simply redelivers,
  preserving at-least-once. Ignored in early ACK (the message is already completed); the
  renewal task's join at shutdown is bounded by `ShutdownTimeout` and runs before the receiver
  close, so in ack-after-handler mode startup validation requires `HostShutdownTimeout` to fit
  `2 × ShutdownTimeout` with renewal on (`1 ×` with `LockRenewalInterval = null`).
- Every abandon burns broker `DeliveryCount`, which also counts toward the *entity's*
  `MaxDeliveryCount` policy. `MaxDeliveryAttempts = 0` disables the package-level dead-letter
  decision and leaves poison handling entirely to that broker policy.
- In early ACK the receive loop waits for background-queue capacity before receiving, so
  queue-full abandons cannot burn `DeliveryCount` in steady state.
- A message the transport cannot project — `ServiceBusReceivedMessage.Body` throws
  `NotSupportedException` for an AMQP **Value** or **Sequence** body, which is what a JMS or raw
  AMQP producer sends — is dead-lettered on its own with reason `AsyncResponseUnsupportedBody`.
  The rest of the batch is unaffected. If that dead-letter itself fails, the lock simply lapses and
  the broker redelivers.
- Dead-letter descriptions are truncated to 4096 characters. Service Bus rejects a longer one with
  `ArgumentOutOfRangeException` client-side, which is indistinguishable from a lost lock at the
  call site — so a handler whose exception message ran long (a serializer dump, a wrapped SQL error,
  an HTTP body) could not be dead-lettered at all and re-ran until the entity's own
  `MaxDeliveryCount`.

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
  The same retry-then-dead-letter path runs for background failures after an early ACK, with
  one difference in what `MaxDeliveryAttempts = 0` means: unlimited in-process retries in
  ack-after-handler mode, but a **single** attempt under early ACK — the offset is already
  committed, and retrying a committed message forever wedged the background worker with no
  record — after which the message is dead-lettered and surfaced via `OnBackgroundFailure`.
- In early ACK, a full background queue pauses consumption on all assigned partitions
  (re-checked every `BackpressurePollDelay` 50 ms) rather than dropping or re-fetching.
- A message that cannot be projected at all (empty payload, unresolvable correlation id) is
  produced to the dead-letter topic and its offset stored, ignoring the stopping token like every
  other settlement path — a shutdown landing mid-burial would leave the poison message neither
  buried nor committed. A `StoreOffset` that throws because a rebalance revoked the partition is
  logged rather than faulting the poll loop; the message simply redelivers.
- Every dead-letter produce runs on the poll thread, so its retry ladder is bounded to a quarter
  of `MaxPollInterval`: an undeliverable dead-letter topic (auto-create off, a leaderless
  partition, an over-sized payload) would otherwise wait out librdkafka's `message.timeout.ms`
  per attempt, overrun `max.poll.interval.ms`, and evict the consumer mid-burial. A burial that
  runs out of budget leaves the offset unstored and is retried after the next restart/rebalance.

### RabbitMQ

- The broker does not count plain `basic.nack` requeues: the resolved attempt is
  `max(x-death count, redelivered ? 1 : 0) + 1`, which never exceeds 2 on its own. A
  `MaxDeliveryAttempts` above 2 therefore only takes effect when the dead-letter path forms a
  TTL-retry cycle that re-delivers the message (each dead-letter hop increments `x-death`; once
  `x-death` is present every retry below the cap rejects without requeue so the cycle is what
  counts it — a plain requeue never advances `x-death`);
  without such a cycle it behaves like 2 and logs a startup warning
  ([troubleshooting](troubleshooting.md#rabbitmq-startup-warns-about-maxdeliveryattempts-or-a-poison-message-loops-forever)).
- **At the cap with `x-death` present the message is parked, terminally.** Rejecting it again
  would only re-enter the cycle at its TTL rate forever, so it is copied to `DeadLetterQueue`
  through the default exchange (bypassing the cycling `DeadLetterExchange`) and ACKed — or, with
  no `DeadLetterQueue` configured, ACKed and **dropped** with an error log. A failed copy leaves
  the delivery un-ACKed, so the broker redelivers it and the park retries.
- The cap is judged **before** the handler runs as well (NATS and database-transport parity): a
  delivery whose previous attempt ended without a thrown exception — the process was killed
  mid-handler and the broker requeued it with `redelivered` set — is dead-lettered (or parked)
  without executing, instead of crash-looping every replica in turn.
- The default `MaxDeliveryAttempts = 0` means unlimited requeues — a poison message hot-loops
  until a cap (with a `DeadLetterExchange`) is configured. With a cap but no
  `DeadLetterExchange`, the exhausted message is rejected without requeue and **dropped**.
- In early ACK, a full background queue parks the delivery on the bounded in-process channel
  until capacity frees; a NACK with requeue is sent only when that enqueue fails during
  shutdown/dispose, so backpressure itself never churns redeliveries.
- Shutdown spends `ShutdownTimeout` twice — cancelling the consumer, then closing the channel
  and connection after the background drain — and startup validation sums both plus
  `BackgroundDrainTimeout` against `HostShutdownTimeout`.

### Redis

- New entries arrive via `XREADGROUP` at attempt 1. A separate reclaim loop scans the
  pending-entries list every `PendingClaimInterval` (5 s) and claims entries idle longer than
  `PendingMessageMinIdleTime` (30 s) with `XCLAIM`, so a crashed consumer's in-flight work is
  retried by a peer; the attempt is the PEL delivery count + 1.
- In early ACK, `XREADGROUP` reads and `XCLAIM` pending claims are clamped to the dispatcher's
  free capacity (Azure Service Bus/SQS parity), so an entry is never read only to be deferred
  into the PEL with a bumped delivery count — backpressure pauses consumption instead of
  spending attempts, and nothing is NACKed because Redis has no NACK.
- A dead-letter `XADD` that fails (MISCONF/OOM, a timeout, `WRONGTYPE` on the dead-letter key)
  is logged and leaves the entry pending for the next reclaim cycle instead of faulting the
  subscriber. On Redis 5/6, `XCLAIM` answers with a nil entry for an id trimmed while still
  pending; that tombstone is ACKed by its pending id so it drains rather than being
  re-dead-lettered every claim cycle. Discarding an unparsable entry is a settlement and ignores
  cancellation like every other one.
- Worker publishes are idempotent across their retry window: `XADD` has no natural identity (the
  entry id is server-generated), so a retry after an ambiguous timeout — the command was abandoned
  client-side while the server kept running it — used to append the same job twice. Each publish
  now commits a short-lived dedup marker (`{<worker stream>}:publish:<id>`, hash-tagged to the
  stream's cluster slot, TTL ≈ 2× the retry window) atomically with the append; a retry that finds
  the marker appends nothing.

### NATS

- Attempts are the broker's `NumDelivered` for the JetStream consumer; the consumer is durable,
  so counts survive subscriber restarts (unlike Kafka's in-process counter). The consumer is
  created with `MaxDeliver = -1` because the dispatcher itself bounds attempts — including a
  delivery whose earlier attempts never settled (process killed mid-handler, a failed NAK): past
  the cap it is dead-lettered and TERMed before the handler runs, instead of redelivering after
  every `AckWait` forever.
- The dead-letter republish drops the inbound `Nats-Msg-Id`: that id identifies the *live* publish,
  and carrying it over would make a second burial of the same message a deduplicated publish inside
  the DLQ stream's duplicate window — which the caller reads as a DLQ failure and answers with a
  NAK, looping until the window passes.
- The dead-letter stream is provisioned with **limits retention and evict-oldest discard** — a
  bounded archive, unlike the work streams' work-queue retention. Nothing consumes the DLQ
  subject, so work-queue retention would never remove anything and a full stream would reject
  every burial (each over-cap poison message then NAK-looping forever under `MaxDeliver = -1`).
  JetStream cannot change an existing stream's retention in place: a DLQ stream provisioned by an
  earlier version keeps its configuration, with a startup warning explaining how to migrate
  (delete it and let the subscriber recreate it; export its dead letters first if needed).
- In early ACK, a full background queue pauses pulling; a message still waiting in the queue
  when the subscriber stops is NAKed so JetStream redelivers it — backpressure itself never
  churns redeliveries. Once the drain budget lapses at shutdown, entries still queued (already
  ACKed, so JetStream will not redeliver them) are dead-lettered and surfaced via
  `OnBackgroundFailure` instead of being executed past the budget or lost at process exit.
- Consumption fetches a bounded batch (`FetchNoWaitAsync`, draining whatever is already buffered,
  or a single-message long-poll `FetchAsync` when idle) and dispatches it serially. While a batch
  is in flight, a background heartbeat signals in-progress (`ProgressAsync`) for every still-
  unsettled message roughly every `AckWait`/3 (two chances to land a renewal inside every `AckWait`
  window even when one sweep is delayed), so `AckWait` (30 s) only has to survive one heartbeat
  round-trip — it no longer needs to exceed the slowest handler, and before batching this was
  effectively the slowest handler **× `BatchSize`** (16 by default).

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
  cannot overwrite the shortened redelivery. Ignored in early ACK. A renewal that fails — including
  the AWS SDK's own client-side HTTP timeout, which surfaces as `TaskCanceledException` — is logged
  and the sweep continues with the rest of the batch; only the subscriber's own stop ends it.
- `CreateQueues` is the only provisioning default that is **off** — production queues (and
  their redrive policies) are usually owned by infrastructure code. When on, converging an
  existing `.fifo` queue re-applies only its mutable attributes: the create-only `FifoQueue`
  attribute is skipped, since `SetQueueAttributes` rejects it (which previously failed host
  startup once the provisioning retries were exhausted).
- `CorrelationIdAttribute` resolution is case-**sensitive**, unlike every other transport's
  case-insensitive inbound header lookup — AWS message attribute names are themselves
  case-sensitive, so `CorrelationId` and `correlationId` can coexist as two distinct attributes on
  one message; a case-folding lookup would alias them. The outbound publish path is also ordinal.

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
  beat. At settlement the dispatcher joins the heartbeat for at most `LockTimeout`: a renew that
  never returns (a degraded database mid-command) is abandoned with a warning rather than holding
  the ack for a full connect+command timeout. While the subscriber is **stopping** the join is
  skipped altogether — the lease lapses on its own — so `LockTimeout`, a term no shutdown-budget
  validator counts, is never spent on the stop path.
- The MongoDB transport pins its collection handle to the primary (channel and flow-store
  parity), so a `secondaryPreferred` client cannot route the change-stream wake to a lagging
  secondary. With `AutoCreateIndexes = false` it runs a one-time read-only check and **warns**
  when no index leads on `queue` — every claim would otherwise scan the collection on every poll
  tick with nothing to show for it. The claim and the dead-letter prune pin the **simple**
  (binary) collation, so an operator-created collection with a case- or accent-folding default
  collation cannot let one subscriber claim another logical queue's documents (the worker
  subscriber previously claimed response documents, which the ingress then dropped and ACKed
  with no dead-letter record). The trade-off: an index built under a folding collation cannot
  serve a simple-collation query, so the claim scans there — a collection with the default
  (simple) collation is unaffected.
- `MaxDeliveryAttempts` is enforced **before** the handler runs, not only after it throws. A
  delivery that ends any other way — the process dies mid-handler, the lease lapses while the
  database is unreachable at settlement — never reaches the post-failure check, and the claim has
  already stamped `attempts + 1`, so the row comes back over the cap forever. A message that
  arrives past the cap is dead-lettered without executing (and released for retry if the
  dead-letter write itself fails). `MaxDeliveryAttempts = 0` still means unlimited.
- Dead-lettering is fenced by the same `lock_id` as the ack and NAK: if the lease lapsed and a peer
  re-claimed the row, the burial no-ops and reports failure rather than writing a DLQ copy of a
  message that is still live under its new owner (on MongoDB the DLQ document is written first,
  under a deterministic id, and removed again when the fenced delete does not match).
- The dead-letter queue is rows/documents in the same table/collection under the
  `DeadLetterQueue` logical name; it has no consumer by default, so set `DeadLetterRetention`
  if entries should be pruned instead of kept for manual inspection (SQL Server prunes
  `DELETE TOP (1000)` rows per throttle window — an unbounded delete over a large backlog
  escalated to a table lock that `READPAST` cannot skip, stalling every claim, ACK and lease
  renewal behind it; a bigger backlog drains over successive windows). DDL is owned by
  `AutoCreateSchema` (PostgreSQL, SQL Server) / `AutoCreateIndexes` (MongoDB) — disable when
  migrations own it.
- Under early ACK, `BackgroundDrainTimeout` is split: three quarters for the queued and running
  handlers, one quarter reserved for dead-lettering — and reporting via `OnBackgroundFailure` —
  the already-ACKed entries still queued when that drain budget lapses. Their rows/documents were
  deleted by the early ACK, so without the reserve they were simply lost at process exit.
- Only PostgreSQL and MongoDB spend a `ShutdownTimeout` at stop (bounding the LISTEN /
  change-stream task join). SQL Server has no push channel to join and therefore no
  `ShutdownTimeout` option; it budgets only the drain.
