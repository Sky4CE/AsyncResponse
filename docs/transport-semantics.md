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

## Publication and visibility failure boundaries

Redis worker publication uses a same-slot Lua operation (`EVAL`/`EVALSHA` must be permitted
by the broker ACL). It appends with `XADD` before writing a TTL-bound success marker containing
the stream entry ID. A failed append leaves no marker, so a timeout hiding that error cannot
turn the retry into success. A lost successful reply reuses the marker without a second append.
The operation uses the portable `MAXLEN` syntax; Redis and Valkey run the integration regression.
This does not provide exactly-once execution: handlers must still tolerate redelivery.

For SQS with visibility renewal enabled, renewal and failure-path visibility changes serialize
per receipt. The retry delay is applied after any already-started renewal, so it remains the
last change. Waiting for a wedged renewal is bounded by `ShutdownTimeout` and host cancellation;
if the wait fails, the failure is logged and the original visibility timeout governs redelivery.

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
- **The background queue outlives a reconnect.** The early-ACK dispatcher belongs to the hosted
  service, not to one connect-and-consume attempt, and only the host stopping disposes (drains)
  it. Scoped to an attempt — as it was — any routine fault in the receive loop (a claim timeout,
  a deadlock victim, a broker blip; for the database transports a poll that runs several times a
  second) ran the *stop-time* drain on a host that was not stopping: consumption paused for the
  whole budget and healthy work the broker had already been told to forget was dead-lettered as
  "drain budget lapsed" — or, when the dead-letter write needed the same failing dependency,
  survived only as an error log line.
- **`BackgroundDrainTimeout` = 20 s.** The maximum time to wait for queued and running
  background handlers while a hosted subscriber stops. The database transports, NATS, RabbitMQ,
  and Kafka split it — three quarters for the handlers, one quarter reserved for dead-lettering
  what is still queued when that lapses (see [their notes](#postgresql-sql-server-mongodb) and
  [NATS](#nats)); NATS, RabbitMQ, and Kafka route those entries from the stop itself, so a worker
  stuck in a long handler cannot keep them from being recorded. SQS, Azure Service Bus and Google
  Pub/Sub split it the same way, and in the reserve the dispose itself reports every entry still
  queued through `OnBackgroundFailure` (they cannot be dead-lettered: the broker already forgot
  them), logging the loss at Error with its count — the background workers may all still be busy,
  so leaving it to them lost those entries at process exit with no callback.
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
  simply stops with the host token — except that Kafka waits for its detached handlers still in
  their partition's current assignment (and commits their offsets), RabbitMQ waits, bounded, for the handler
  still running in its delivery callback so that job's ACK lands before the channel closes, and
  Google Pub/Sub waits, bounded, for its running handlers before it stops the client (see their
  notes).
- **A host-stop hand-back is a stop, not a failure.** When the host begins stopping, a durable
  flow waiting in process on a timer is normally *handed over*: checkpointed, an immediate wake-up
  published, its delivery acknowledged. When that cannot happen — the publish failed, the wait
  began on a host already stopping, or a lease-contention wait is cut short by the stop — the
  delivery is handed back instead (`DurableFlowInterruptedException`), and because the host fires
  `ApplicationStopping` *before* it stops any hosted service, this arrives while the worker
  subscriber's own token is still live. Every transport's ack-after-handler dispatcher recognizes
  it by type: the delivery is left for redelivery (unsettled — Google Pub/Sub NACKs it back once,
  when its client stops), never counted as a failure, retried or dead-lettered, exactly as if the
  subscriber's own stop had cancelled it. The redelivery still spends a delivery attempt where the
  broker or store counts them, so a hand-back at the attempt cap is dead-lettered on redelivery
  without running (on Service Bus only against the entity's own `MaxDeliveryCount`). Under early
  ACK the broker has already forgotten the message: the hand-back is logged as a warning, reported
  through `OnBackgroundFailure`, and — where the transport can dead-letter (Kafka, RabbitMQ, Redis,
  NATS and the database transports) — copied to the dead-letter destination with a reason starting
  `handed_back_after_commit`; on Kafka, RabbitMQ, Redis and NATS it is logged as an error instead
  when no destination is configured (the database transports log the warning even with
  `DeadLetterEnabled = false`, where no copy is written).
  Application code must never throw `DurableFlowInterruptedException` itself. On Kafka, which
  commits a position, the handed-back message's partition is also parked for the rest of the
  stop, so no later offset of it is stored past the unsettled message.

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
| **MongoDB** | claimed document: delete on success, reschedule after `RedeliveryDelay` 5 s on failure | store — the `findOneAndUpdate` claim increments the attempt | `deadletter` logical queue in the same collection; `DeadLetterEnabled` default on, optional `DeadLetterRetention` | log + `OnBackgroundFailure` + DLQ document | drain + close 5 s (change-stream listen join) | automatic fenced renewal at `LockTimeout`/3 (server-clock `$$NOW`, `lock_id` fence) |
| **NATS** | JetStream explicit ack: ACK on success, NAK + `RedeliveryDelay` 5 s on failure | broker `NumDelivered`; at `MaxDeliveryAttempts` the message is ACKed + dead-lettered — including a delivery whose earlier attempts never settled (process killed mid-handler), refused before execution | `{prefix}.transport.deadletter` subject/stream; declared by `CreateStreams` (default on) | log + `OnBackgroundFailure` + published to the DLQ subject | drain only | automatic in-progress heartbeat every `AckWait`/3 across the in-flight batch (not configurable) |
| **PostgreSQL** | claimed row: delete on success, reschedule after `RedeliveryDelay` 5 s on failure | store — the `FOR UPDATE SKIP LOCKED` claim increments `attempts` | `deadletter` logical queue in the same table; `DeadLetterEnabled` default on, optional `DeadLetterRetention` | log + `OnBackgroundFailure` + DLQ row | drain + close 5 s (LISTEN task join) | automatic fenced renewal at `LockTimeout`/3 (`lock_id` fence) |
| **RabbitMQ** | per-delivery `basic.ack`; `basic.nack` + requeue on failure | broker `x-death` header + `redelivered` flag, judged before the handler runs; **`MaxDeliveryAttempts` default `0` = unlimited**; values > 2 need a TTL-retry DLX cycle, and at the cap a message that has ridden it is parked terminally (see notes) | optional `DeadLetterExchange` (default `null` → exhausted messages are **dropped**); declared when set and `DeclareTopology` is on; a message capped after riding the DLX cycle is parked in `ParkQueue` (else `DeadLetterQueue`) via the default exchange (ACKed and dropped when neither is configured) | log + `OnBackgroundFailure` + published to the `DeadLetterExchange` when one is configured (the early ACK already foreclosed the native reject-without-requeue route); a copy that comes back through the DLX and fails again is parked instead. With `DeadLetterExchange`, `DeadLetterQueue` or `ParkQueue` set the subscriber channel enables publisher confirmations, so an unroutable copy or park fails loudly instead of logging a false success | drain + close 5 s ×2 (consumer cancel, then channel/connection close); ack-after-handler waits for the running handler within what the host budget leaves | — (unacked deliveries hold no expiring lock) |
| **Redis** | consumer group: `XACK` on success; a failed entry stays in the PEL and is reclaimed after `PendingMessageMinIdleTime` 30 s | broker — PEL delivery count (`XPENDING`) + 1 at claim; at `MaxDeliveryAttempts` the entry is dead-lettered + `XACK`ed | `{prefix}:transport:deadletter` stream (`XADD` auto-creates it); `DeadLetterEnabled` default on | log + `OnBackgroundFailure` + `XADD` to the DLQ stream | drain only | automatic idle-reset heartbeat (`XCLAIM … JUSTID`, no delivery-count bump) every `PendingMessageMinIdleTime`/3 while an entry is in flight (not configurable) |
| **SQS** | visibility settle: delete on success; failure lets the visibility timeout lapse (or shortens it to `RedeliveryDelay`) | native `ApproximateReceiveCount` + redrive `maxReceiveCount`; no app counter by design | native redrive DLQ; delegated — or declared by `CreateQueues` (default **off**): `{queue}-dlq` + `MaxReceiveCount` 5 | log + `OnBackgroundFailure`; already deleted, no DLQ write possible | drain + up to `ShutdownTimeout` joining the visibility-renewal task on the final batch | opt-in `VisibilityRenewalInterval` (default off); suppressed per message once the failure path schedules `RedeliveryDelay` |
| **SqlServer** | claimed row: delete on success, reschedule after `RedeliveryDelay` 5 s on failure | store — the `UPDLOCK/READPAST` claim increments `attempts` | `deadletter` logical queue in the same table; `DeadLetterEnabled` default on, optional `DeadLetterRetention` | log + `OnBackgroundFailure` + DLQ row | drain only | automatic fenced renewal at `LockTimeout`/3 (`lock_id` fence) |

## Delayed delivery (`IDelayedWorkerTransport`)

Delayed worker jobs — `EnqueueWorkerAsync(..., delay)` and the wake-ups behind suspended
durable-flow timers — are a per-transport capability. Envelopes carry their absolute due time
(`NotBeforeUtc`); the shared worker-job executor re-publishes any early delivery for the
remainder, which is how capped transports chunk long delays with no transport-specific code.

| Transport | Native delayed delivery | Per-hop cap | Mechanism / caveats |
|---|---|---|---|
| **InMemory** | ✅ | ~49.7 days (chunked) | `TimeProvider` timer wheel; the per-hop cap is the .NET timer ceiling (virtual-clock aware in tests); delayed jobs die with the process, logged at shutdown; at most `DelayedJobCapacity` (4096) held at once — external publishers wait, in-job publishes are rejected |
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

- The worker subscriber in ack-after-handler mode receives **one message at a time**: Service Bus
  locks — and, when a lock lapses, counts — every message a receive hands over, so a batch worked
  serially let a process-killing handler take its batch-mates' `DeliveryCount` with it on every
  crash, and a long handler kept them locked while idle peers waited. `MaxMessagesPerReceive`
  applies to early ACK and to the response subscriber (whose handler is the library's own).
  Once the flow engine hands a delivery back at `ApplicationStopping` (left locked, not
  abandoned), the subscriber stops receiving until its own stop, so the wake-ups the engine hands
  over reach a replica still running instead of being taken, and locked, by this stopping host.
  `LockRenewalInterval` (10 s, cancellable per beat) renews the peek-lock of the message in
  the handler, so slow handlers do not hit `MessageLockLostException` redeliveries of
  already-processed messages
  ([troubleshooting](troubleshooting.md#azure-service-bus-messagelocklostexception-redeliveries-of-already-processed-messages)).
  The renewal is not ended by the host stop — the handler still running keeps its lock until it
  returns — and once stopping, nothing new starts: messages of the batch that never started are
  abandoned so a peer takes them at once. Renewal failures are logged and processing continues —
  the message simply redelivers, preserving at-least-once; a lock reported lost
  (`MessageLockLost`) is logged once and not renewed again. `LockRenewalInterval` must be under
  5 minutes, the `LockDuration` maximum (Azure's default is 60 s). Ignored in early ACK (the
  message is already completed); the renewal task's join at shutdown is bounded by
  `ShutdownTimeout` and runs before the receiver close — which is budgeted whenever the stop
  ends the receive loop — so in ack-after-handler mode startup validation requires
  `HostShutdownTimeout` to fit `2 × ShutdownTimeout` with renewal on (`1 ×` with
  `LockRenewalInterval = null`, and in early ACK alongside `BackgroundDrainTimeout`: without a
  renewal join to overlap, abandoning a batch's unstarted messages and the receiver close share
  that one `ShutdownTimeout`). With renewal off the worker transport advertises the
  5-minute `LockDuration` maximum as its in-flight ceiling to the durable-flow engine.
- `PrefetchCount` (default 0) buffers locked messages the renewal heartbeat never reaches; in
  ack-after-handler mode keep `PrefetchCount × handler latency` well under `LockDuration`, or
  leave it at 0 (startup warns when it is positive).
- Queue names are compared case-insensitively in the worker/response and reply-target collision
  guards: Service Bus entity names are case-insensitive, so `Jobs` and `jobs` are one entity.
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
- On stop in ack-after-handler mode the subscriber first **drains the handlers already running**
  for up to `BackgroundDrainTimeout` (default 20 s) — shortened to what `HostShutdownTimeout`
  still leaves after `ShutdownTimeout`, measured from `ApplicationStopping` because hosted
  services stop one after another — and only then stops the client (`NackImmediately`, bounded
  by `ShutdownTimeout`). The SDK's stop hands every message still in leasing back at once for
  any timeout under its 30-second hard-stop window, stopping the lease extension of a job whose
  handler is still running and dropping its eventual ACK — so calling it first redelivered every
  in-flight job to a peer on every deploy. A handler parked on an awaited durable-flow response
  is not interrupted by the host stop: unless its response arrives, it costs the drain its whole
  bound and is redelivered. Deliveries arriving during the drain — and deliveries the flow engine hands back
  at `ApplicationStopping` — are **held, not NACKed**, and handed back with the client stop: the
  streaming pull runs until then, so an immediate NACK was redelivered straight back, often to
  the same stream, in a loop that spent a `DeadLetterPolicy`'s delivery attempts; held, they keep
  their flow-control slots, so the pull stalls once the slots are full. Startup validates `ShutdownTimeout` against
  `HostShutdownTimeout` in both ack modes.
- Reply targets name the correlation attribute (`correlationIdAttribute`) a remote producer must
  set, like every other broker transport's `correlationId*` property. A correlation id over the
  1024-byte attribute-value limit is published without the attribute (the worker path reads the
  id from the body).

### Kafka

- Kafka offsets cannot NACK a single message, so redelivery is in-process: a failing handler is
  retried with backoff (`HandlerRetryBaseDelay` 100 ms → `HandlerRetryMaxDelay` 5 s) up to
  `MaxDeliveryAttempts`, stalling that partition — and only that partition — while it retries
  (classic consumer-group semantics — size `TopicNumPartitions` for parallelism).
- **A long handler never stalls the poll loop.** In ack-after-handler mode a handler is awaited
  inline for `DetachHandlerAfter` (default 1 s); one still running past that is detached: its
  partition is paused (Kafka's own ordering primitive — nothing is fetched for it, nothing is
  buffered in-process), the handler and its retry ladder run on the thread pool, and the poll
  thread keeps polling — so the consumer's other partitions keep flowing, `max.poll.interval.ms`
  is honored, and rebalance callbacks fire. The poll thread (the only thread that touches the
  consumer) stores the offset and resumes the partition once the handler settles, within one
  `BackpressurePollDelay`; a stop waits for detached handlers still in their partition's current
  assignment and commits their offsets (one revoked since it detached and not handed straight back
  is not waited for). Detached
  handlers for different partitions run concurrently. Before this, the poll thread awaited the
  whole handler, and a durable-flow step awaiting a remote response for longer than the interval
  got the consumer evicted, its partitions rebalanced, and the same job redelivered to a peer
  ([troubleshooting](troubleshooting.md#kafka-rebalances-or-duplicate-runs-while-long-handlers-execute)).
- Attempts are counted per process delivery: a consumer restart before the offset commit
  resets the count. The message that exhausts its attempts is produced to the dead-letter
  topic with failure-detail headers and its offset committed, so the partition keeps moving.
  The same retry-then-dead-letter path runs for background failures after an early ACK, with
  one difference in what `MaxDeliveryAttempts = 0` means: unlimited in-process retries in
  ack-after-handler mode, but a **single** attempt under early ACK — the offset is already
  committed, and retrying a committed message forever wedged the background worker with no
  record — after which the message is dead-lettered and surfaced via `OnBackgroundFailure`. A
  flow's host-stop hand-back (`DurableFlowInterruptedException`) of an early-ACK job ends the
  ladder at once and is not a failure: it is logged as a Warning, surfaced via
  `OnBackgroundFailure` and dead-lettered with reason `handed_back_after_commit` (the offset is
  already committed, so the copy is the only way to replay it; the flow's checkpoints make that
  replay safe). With `DeadLetterEnabled = false` no copy is written: the hand-back is logged at
  Error instead, because the wake-up is lost unless `OnBackgroundFailure` records it.
- In early ACK, a full background queue pauses consumption on all assigned partitions
  (re-checked every `BackpressurePollDelay` 50 ms) rather than dropping or re-fetching.
- A message that cannot be projected at all (empty payload, unresolvable correlation id) is
  produced to the dead-letter topic and its offset stored, ignoring the stopping token like every
  other settlement path — a shutdown landing mid-burial would leave the poison message neither
  buried nor committed. **In partition order:** consumed behind a detached handler of the same
  partition (a rebalance handing the partition back with its pause reset delivers the next
  record), it is held exactly like a valid delivery and buried in its turn once the handler
  settles — storing its offset at once committed the partition *past* the unfinished message, and
  a crash after that commit skipped the valid job for good with only the malformed record's copy
  in the dead-letter topic. A `StoreOffset` that throws because a rebalance revoked the partition
  is logged (at Information — it is routine in a consumer group) rather than faulting the poll
  loop; the message simply redelivers to the partition's new owner.
- **Detached handlers are revocation-aware.** Every revoke (or loss) of a partition advances its
  assignment generation and lifts the pause taken behind a detached handler: librdkafka itself
  keeps an application pause across a revoke and a re-assignment, so a partition paused behind a
  detached handler used to come back to the same member still paused and never fetch again (the
  early-ACK backpressure pause is left to the poll loop, which lifts it once the queue has room).
  The package keeps librdkafka's default assignor (`range,roundrobin`, the eager protocol), which
  revokes *every* partition on every rebalance — any member joining or leaving — and usually hands
  most of them straight back. The first message of the new assignment (it starts at the group's
  committed offset) decides what happens to a handler still running from before the revoke:
  - **past the running message** — another member committed beyond it: the handler is orphaned.
    Its offset is never stored (librdkafka accepts a store for a partition that is assigned
    *again*, and Kafka's offset commit is not monotonic, so that store rewound the group past a
    peer's commits), nothing of the new assignment is held behind it, and a stop neither waits for
    it nor stores it; its outcome is only logged. The partition's current owner re-consumes the
    message (at-least-once).
  - **at or before the running message** — nobody moved past it: the handler keeps the partition,
    the redelivered messages wait behind it, and those it has already settled are dropped instead
    of running a second copy of the same message alongside the first.

  A handler that finishes before any message of the new assignment arrives is treated as
  orphaned, so its message runs once more afterwards (at-least-once, never concurrently).
- **The early-ACK queue outlives the consumer.** In `AckAfterEnqueue` mode one dispatcher serves
  every consumer the supervisor builds; a poll-loop fault rebuilds the consumer while the queued,
  already-committed work keeps running, and only a host stop drains it. (Created per consumer, a
  transient consume error paused every partition for the drain budget and then dead-lettered the
  queued work unstarted — or lost it when the fault was the dead-letter topic itself.)
- **Dead-letter copies replace, never stack, burial headers, and fit the producer's size limit.**
  A record replayed from a dead-letter topic (one carrying both `sourceTopic` and `sourceOffset`)
  has its earlier `sourceTopic`/`reason`/`exception*` set replaced; any other record keeps all of
  its own headers, including ones named `reason` or `attempts`. `exceptionMessage` (then
  `exceptionType`) is shortened so the copy stays under the producer's `message.max.bytes` (from
  `ConfigureProducer`, default 1,000,000).
- **Publish retries are for transient errors only.** `Local_MsgTimedOut` (raised only after
  librdkafka already retried for `message.timeout.ms`), message/record-size and topic/cluster
  authorization errors fail the publish at once instead of burning `PublishMaxAttempts`.
- **Consumer groups are not derived from `TopicPrefix`.** Deployments sharing a cluster need
  their own `WorkerConsumerGroup`/`ResponseConsumerGroup` as well as their own prefix: a shared
  group rebalances every member whenever any one of them joins or leaves.
- **A poll-loop failure tears down within `FaultDrainTimeout`** (default 5 s; `0` abandons at
  once). When a consume fails — a dropped broker connection, a burial that failed for good — the
  consumer is closed and rebuilt by the supervisor after its backoff; detached handlers that
  settle within the budget get their offsets stored and committed by the close, exactly as after
  a stop. The rest are abandoned: their offsets stay unstored, their messages redeliver on the
  rebuilt consumer — possibly while the abandoned handler is still running, which is the
  at-least-once contract every handler on this transport already carries (a durable flow's lease
  makes the redelivery a no-op; a plain worker job must be idempotent) — the session's
  cancellation token stops their retry ladders, and each one's eventual outcome is logged. Before
  the bound, the teardown waited for every detached handler without limit, so a transient broker
  failure disabled the whole subscriber for as long as an unrelated long handler took and the
  configured reconnect policy never ran. A graceful stop is not bounded here; the host's shutdown
  budget bounds it.
- Every dead-letter produce's retry ladder is bounded to a quarter of `MaxPollInterval`: the
  malformed-message discard runs it on the poll thread, and an undeliverable dead-letter topic
  (auto-create off, a leaderless partition, an over-sized payload) would otherwise wait out
  librdkafka's `message.timeout.ms` per attempt, overrun `max.poll.interval.ms`, and evict the
  consumer mid-burial; the ack-after-handler burial runs inside the (possibly detached) handler
  task and keeps the same bound so a partition is not parked on it either.
- **A burial that fails for good faults the subscriber** (ack-after-handler mode and the
  malformed-message discard). Kafka commits a partition *position*, not per-record
  acknowledgements, so merely leaving the failed message's offset unstored protects nothing: the
  next successful settlement on the same partition stores a higher offset and the auto-committer
  commits past the failed message, which a restart then skips with no dead-letter copy anywhere.
  Instead the poll loop throws, the consumer closes without ever storing past the message, and the
  supervisor rebuilds it after its backoff (`SubscriberRetryBaseDelay` → `SubscriberRetryMaxDelay`);
  the restarted consumer re-consumes from the committed position, re-runs the handler up to
  `MaxDeliveryAttempts`, and retries the burial — a loud, bounded-rate loop that parks **every**
  partition of that subscriber at the poison message until the dead-letter topic is fixed (each
  restart logs the failure). That is the at-least-once outcome; the previous swallow was a silent
  loss. Messages already committed at enqueue time (early ACK) are outside this rule: their
  burial failure is logged and surfaced through `OnBackgroundFailure`, because Kafka will not
  redeliver them either way.

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
  would only re-enter the cycle at its TTL rate forever, so it is copied to `ParkQueue` — or,
  without one, `DeadLetterQueue` — through the default exchange (bypassing the cycling
  `DeadLetterExchange`) and ACKed; with neither configured it is ACKed and **dropped** with an
  error log. `ParkQueue` is declared unbound, so unlike `DeadLetterQueue` it never collects the
  hops of messages still riding the cycle. The subscriber channel runs with publisher
  confirmations whenever a park or dead-letter copy can be published, so a copy the broker cannot
  route (a missing queue, a full `reject-publish` queue) fails loudly: the delivery is then NACKed
  with requeue after the `SubscriberRetryBaseDelay`/`SubscriberRetryMaxDelay` backoff, and the
  park retries on redelivery. A parked copy carries the original headers, `x-death` included —
  strip it when replaying (see [troubleshooting](troubleshooting.md#rabbitmq-startup-warns-about-maxdeliveryattempts-or-a-poison-message-loops-forever)).
- The cap is judged **before** the handler runs as well (NATS and database-transport parity): a
  delivery whose previous attempt ended without a thrown exception — the process was killed
  mid-handler and the broker requeued it with `redelivered` set — is dead-lettered (or parked)
  without executing, instead of crash-looping every replica in turn.
- The default `MaxDeliveryAttempts = 0` means unlimited requeues — a poison message hot-loops
  until a cap (with a `DeadLetterExchange`) is configured. With a cap but no
  `DeadLetterExchange`, the exhausted message is rejected without requeue and **dropped** (a
  reject before the handler ran is logged at Error in that case). On RabbitMQ 4.x **quorum
  queues** the broker enforces its own `delivery-limit` (20 by default) regardless: past it the
  message is dead-lettered, or dropped when no dead-letter exchange is set, so "unlimited" is
  bounded there — configure a dead-letter exchange, or raise/disable `delivery-limit` by policy.
- **`consumer_timeout` counts from the send, not from the handler.** The broker closes a channel
  whose delivery stays unacknowledged longer than its `consumer_timeout` (mirrored by
  `BrokerConsumerTimeout`, 30 minutes by default) and requeues every unacknowledged delivery.
  Deliveries are handled one at a time, so prefetched ones age while they wait: the worker
  transport therefore advertises `BrokerConsumerTimeout / PrefetchCount` as its in-flight
  ceiling, and durable-flow timers park in process for at most half of that per delivery.
  `PrefetchCount = 1` gives timers the whole timeout. The advertised value is never less than
  **one minute** (nor more than `BrokerConsumerTimeout` itself, when that is shorter): an
  unfloored share turned a large prefetch into a republish storm (1,000 → a 0.9 s timer hop).
  `PrefetchCount = 0`, AMQP's "unlimited" (which the subscriber rejects at startup), advertises
  that floor. When the share falls below the floor — `PrefetchCount` above 30 at the default
  30-minute timeout — the worker subscriber logs a startup warning with durable flows
  registered: the last buffered delivery can then outlive `consumer_timeout` when most of the
  buffer is parked flows.
- In early ACK, a full background queue parks the delivery on the bounded in-process channel
  until capacity frees; a NACK with requeue is sent only when that enqueue fails during
  shutdown/dispose (or the channel it arrived on dies), so backpressure itself never churns
  redeliveries. A parked delivery — and every prefetched one behind it — stays unacknowledged
  meanwhile and keeps ageing against `consumer_timeout`: with long background jobs, size
  `BackgroundQueueCapacity`/`PrefetchCount` so sustained saturation stays well inside it, or
  disable the timeout for that queue (`x-consumer-timeout`), or the broker closes the channel.
- **The early-ACK queue outlives the channel.** One dispatcher serves every channel the
  supervisor builds: a broker restart, network blip or channel-level close rebuilds the channel
  while the queued, already-ACKed work keeps running, and a background failure afterwards
  publishes its dead-letter copy through the live channel. Only a host stop drains it. A failed
  job's copy that comes back through the dead-letter exchange (a TTL-retry queue bound to it)
  and fails again is parked in `ParkQueue`/`DeadLetterQueue` instead of copied into the cycle a
  second time (dropped with an error when neither is set).
- **A graceful stop does not cut off the running handler (ack-after-handler).** The stop cancels
  the consumer, waits for the handler still running in its delivery callback — up to
  `BackgroundDrainTimeout`, shortened to what `HostShutdownTimeout` leaves after the two
  `ShutdownTimeout` spends, never validated — so its ACK lands before the channel closes, and
  starts none of the deliveries the client had already buffered (they stay unacknowledged and
  are redelivered unstarted). Past the bound the close goes ahead and the running delivery is
  redelivered. When `HostShutdownTimeout` leaves no wait at all (for example 10 s against the
  two default 5 s spends) the subscriber says so once, at startup (Information), and a stop
  closes without waiting. A consumer cancel that fails or outlives `ShutdownTimeout` is logged
  and the stop still drains (both ack modes) before it closes the channel; a delivery that still
  arrives once the early-ACK drain has begun is left unacknowledged for the close to requeue
  (a NACK would hand it straight back to the still-registered consumer).
- **A host-stop hand-back comes back `redelivered`.** In ack-after-handler mode the flow
  engine's hand-back (`DurableFlowInterruptedException`) leaves the delivery unacknowledged, and
  the channel close requeues it with `redelivered` set — attempt 2 on the next host (unless
  `x-death` already counts further). With
  `MaxDeliveryAttempts = 1` that is past the cap, so the redelivery is rejected (dead-lettered,
  or dropped without a dead-letter exchange) before its handler runs, and the flow's wake-up is
  lost; the same happens to any delivery requeued under a running handler. The worker subscriber
  logs a startup warning for `MaxDeliveryAttempts = 1` when durable flows are registered — use 2
  or more (or 0). In early ACK the delivery was already acknowledged and is never redelivered:
  a hand-back is logged at Warning (not Error), reported through `OnBackgroundFailure`, and
  copied to the dead-letter exchange with an `AR-DeadLetter-Reason` that starts with
  `handed_back_after_commit:` — replaying it is safe, the run resumes from its checkpoint.
  Without a `DeadLetterExchange` (the default) no copy is written: the hand-back is logged at
  Error instead, because the wake-up is lost unless `OnBackgroundFailure` records it.
- Shutdown spends `ShutdownTimeout` twice — cancelling the consumer, then closing the channel
  and connection after the background drain — and startup validation sums both plus
  `BackgroundDrainTimeout` against `HostShutdownTimeout` (early ACK).
- A correlation id longer than an AMQP short string (255 UTF-8 bytes — as few as ~86 characters
  above U+0800) travels in the `CorrelationIdHeader` header only; the native `correlation-id`
  property, which the client cannot encode past that, is left unset.

### Redis

- **The Redis *channel* (pub/sub) is fire-and-forget, and its per-wait buffer is bounded.** A
  publisher is never backpressured, and the StackExchange.Redis queue behind a subscription is
  unbounded, so the channel buffers at most `1,024` responses per correlation id behind the wait's
  serial processing (an `Until` predicate runs one message at a time). A response that finds that
  buffer full faults the wait with the overload form of
  `AsyncResponseIndeterminateDeliveryException` (a terminal response may be among the queued or the
  refused ones), unsubscribes, and counts `asyncresponse.channel.overloaded_waits` — never buffered
  without bound, never silently dropped. Durable flows treat the fault like the disposal-drain form
  and restart the (idempotent) step. Where a backlog must be lossless, use a database channel
  (PostgreSQL, SQL Server, MongoDB): its backlog stays server-side and the dispatch sweep admits it
  as capacity frees.

- Requires **Redis 6.2+** (or a compatible server): the reclaim loop's `XPENDING … IDLE` form
  arrived in 6.2, and on an older server every subscriber attempt fails on it before reading.
- New entries arrive via `XREADGROUP` at attempt 1. A separate reclaim loop scans the
  pending-entries list every `PendingClaimInterval` (5 s, on the monotonic clock) and claims
  entries idle longer than `PendingMessageMinIdleTime` (30 s) with `XCLAIM`, so a crashed
  consumer's in-flight work is retried by a peer; the attempt is the PEL delivery count + 1.
- Redis counts a delivery when `XREADGROUP` or `XCLAIM` hands an entry over, not when a handler
  starts. So `AckAfterHandlerCompletes` reads **one entry at a time**, and although `XPENDING`
  lists up to `PendingClaimBatchSize` reclaim candidates, each is `XCLAIM`ed only right before it
  runs: nothing queued behind a handler that crashes the process has its count bumped toward the
  dead-letter cap without ever running, and nothing is pinned behind a handler that runs for hours
  (a flow timer waiting in process). Early ACK keeps `BatchSize` reads and batch claims.
- While an entry is in flight, a heartbeat re-claims it (and, in an early-ACK batch, the entries
  still waiting their turn) with `XCLAIM … JUSTID` every `PendingMessageMinIdleTime`/3, resetting
  the idle clock without bumping the delivery count, so a slow handler is not stolen by a peer's
  reclaim. The heartbeat is not tied to the stop signal — a handler takes no token and outlives
  it — and ends only when the loop lets go of the entry. On stop nothing new starts: the rest of
  a batch, and any reclaim candidate not yet claimed, stays pending (Redis has no NACK) for a peer.
- In early ACK, `XREADGROUP` reads and `XCLAIM` pending claims are clamped to the dispatcher's
  free capacity (Azure Service Bus/SQS parity), so an entry is never read only to be deferred
  into the PEL with a bumped delivery count — backpressure pauses consumption instead of
  spending attempts, and nothing is NACKed because Redis has no NACK.
- A host-stop hand-back from the flow engine (`DurableFlowInterruptedException`) is recognised by
  type, since it arrives while the subscriber's own token is still live: the entry stays pending,
  unsettled, with no failure log and no dead-letter write of its own (the redelivery still counts:
  a hand-back at `MaxDeliveryAttempts` is dead-lettered on redelivery without running). An
  early-ACK entry was already ACKed, so Redis can never redeliver it: its hand-back is logged as a
  warning, reported through `OnBackgroundFailure`, and dead-lettered with reason
  `handed_back_after_commit` (replaying the copy is safe — the run resumes from its last
  checkpoint); with `DeadLetterEnabled = false` no copy can be written, so it is logged as an error
  instead — the wake-up is lost unless your `OnBackgroundFailure` records it. After the first
  hand-back the subscriber reads and claims nothing more until its own stop, so the rest of the stop
  window's wake-ups stay unread (or pending, unclaimed) for a live peer instead of being handed back
  one by one. (The engine raises it only while the host is stopping; application code must never
  throw it, or the subscriber stops consuming until the process restarts.)
- The ack-after-handler reclaim loop re-reads each candidate's pending entry (`XPENDING` for that
  one id) right before claiming it, so the attempt number reflects claims a peer made while this
  consumer worked through the earlier candidates.
- A dead-letter `XADD` that fails (MISCONF/OOM, a timeout, `WRONGTYPE` on the dead-letter key)
  is logged and leaves the entry pending for the next reclaim cycle instead of faulting the
  subscriber — on every burial path: the at-cap and pre-execution burials of both ACK modes and
  the discard of an unparsable entry. On Redis 6.2, `XCLAIM` answers with a nil entry for an id
  trimmed while still pending; that tombstone is ACKed by its pending id so it drains rather than
  being re-dead-lettered every claim cycle (7.0+ drops such an id from the pending list itself).
  Either way the job is gone — see `StreamMaxLength` below. Discarding an unparsable entry is a
  settlement and ignores cancellation like every other one.
- **`StreamMaxLength` (default `100000`) evicts unprocessed work.** Worker publishes append with
  `XADD … MAXLEN ~ N`, and Redis trims by length alone, whatever the consumer group has read: past
  the cap the oldest entries are deleted — including jobs never read and jobs pending in a
  handler — with no dead-letter copy (a trimmed pending entry surfaces at most as the Warning
  above). Settlement is `XACK` only, so already-processed entries also stay in the stream until
  trimmed. Size the cap well above the deepest backlog an outage can build (throughput × longest
  worker outage), or set `null` to disable trimming and bound the stream operationally. The
  library trims only its own worker publishes; producers writing the **response** stream must
  apply `XADD … MAXLEN ~` themselves.
- Flow timers wait in process on Redis (no delayed delivery), holding one delivery for the whole
  remainder under the heartbeat. For long timers set `DurableFlowOptions.MaxInProcessParkDuration`:
  each hop then continues under a fresh delivery with a fresh delivery count, so repeated
  heartbeat outages over a multi-day wait cannot let peers' reclaims spend the attempts that bury
  the live holder's job.
- When a subscriber stops for good, it deletes its own generated consumer from the group
  (`XGROUP DELCONSUMER`) — only while that consumer owns no pending entries, checked atomically in
  one script — so consumer lists no longer grow by one entry per process start. A crashed
  process's consumer is left behind; a configured `ConsumerName` is never deleted.
- Worker publishes are idempotent across their retry window: `XADD` has no natural identity (the
  entry id is server-generated), so a retry after an ambiguous timeout — the command was abandoned
  client-side while the server kept running it — used to append the same job twice. Each publish
  now commits a short-lived dedup marker (`{<worker stream>}:publish:<id>`, hash-tagged to the
  stream's cluster slot, TTL ≈ 2× the retry window) atomically with the append; a retry that finds
  the marker appends nothing. Besides connection faults and timeouts, the retry covers the replies
  of a cluster in transition — `TRYAGAIN` (the two-key append during its slot's migration),
  `CLUSTERDOWN`, `LOADING`, `MASTERDOWN`, `READONLY` — which the server raises before running
  anything; every other server error fails the publish at once.

### NATS

- Attempts are the broker's `NumDelivered` for the JetStream consumer; the consumer is durable,
  so counts survive subscriber restarts (unlike Kafka's in-process counter). The consumer is
  created with `MaxDeliver = -1` because the dispatcher itself bounds attempts — including a
  delivery whose earlier attempts never settled (process killed mid-handler, a failed NAK): past
  the cap it is dead-lettered and TERMed before the handler runs, instead of redelivering after
  every `AckWait` forever.
- The dead-letter republish drops every inbound `Nats-*` header: those are JetStream publish
  directives and server metadata that belonged to the *live* publish. `Nats-Msg-Id` would make a
  second burial of the same message a deduplicated publish inside the DLQ stream's duplicate
  window, and a producer's `Nats-Expected-Stream` / `-Last-Sequence` / `-Last-Subject-Sequence`,
  `Nats-Rollup` or `Nats-TTL` would make the DLQ stream refuse the burial — either way the caller
  reads a DLQ failure and answers with a NAK, looping (forever, under `MaxDeliver = -1`). The
  `AR-DeadLetter-Reason` header is capped at 512 characters for the same reason: an unbounded
  exception message could push the burial past `max_payload`.
- The dead-letter stream is provisioned with **limits retention and evict-oldest discard** — a
  bounded archive, unlike the work streams' work-queue retention. Nothing consumes the DLQ
  subject, so work-queue retention would never remove anything and a full stream would reject
  every burial (each over-cap poison message then NAK-looping forever under `MaxDeliver = -1`).
  JetStream cannot change an existing stream's retention in place: a DLQ stream provisioned by an
  earlier version keeps its configuration, with a startup warning explaining how to migrate
  (delete it and let the subscriber recreate it; export its dead letters first if needed).
- In early ACK, a full background queue pauses pulling; a message still waiting in the queue
  when the subscriber stops is NAKed so JetStream redelivers it — backpressure itself never
  churns redeliveries. Once three quarters of the drain budget lapse at shutdown, the dispatcher
  itself dead-letters the entries still queued (already ACKed, so JetStream will not redeliver
  them) and surfaces them via `OnBackgroundFailure` within the remaining quarter, instead of
  executing them past the budget or losing them at process exit — it cannot leave that to the
  background workers, since queued entries mean every worker is inside a handler that takes no
  token. Whatever the reserve cannot bury is logged as an error with its count. With **both** the
  worker and the response subscriber in early ACK, startup validation checks the **sum** of their
  drains against `HostShutdownTimeout`: the host stops the two hosted services one after the
  other inside one shutdown budget (hosted services registered after them stop first and spend
  from it too, which validation cannot see).
- A flow handing its delivery back because the host is stopping (`DurableFlowInterruptedException`)
  is treated like the subscriber's own shutdown even while the subscriber's token is still live
  (the engine reacts to `ApplicationStopping`, which fires before any hosted service stops): the
  delivery is left unsettled for redelivery after `AckWait`, with no NAK, failure log or
  dead-letter. JetStream still counts that delivery attempt. In early ACK the message was already
  ACKed, so JetStream can never redeliver it: the hand-back is logged as a warning, reported through
  `OnBackgroundFailure`, and dead-lettered with an `AR-DeadLetter-Reason` starting
  `handed_back_after_commit` (replaying the copy is safe — the run resumes from its last
  checkpoint). With dead-lettering disabled no copy can be written, so it is logged as an error
  instead: the wake-up is lost unless your `OnBackgroundFailure` records it. After the first
  hand-back the subscriber fetches nothing more until its own stop; what a batch had not started — a
  message waiting for room in the early-ACK queue included — is NAKed with no delay for a live peer.
  (The engine raises it only while the host is stopping; application code must never throw it, or
  the subscriber stops consuming until the process restarts.)
- Consumption fetches what is already available (`FetchNoWaitAsync`), or long-polls for a single
  message (`FetchAsync`) when idle, and dispatches serially. In the default ack-after-handler mode
  every fetch takes exactly **one** message, whatever `BatchSize` says: JetStream counts a
  delivery when it hands a message over, so messages prefetched behind a handler that kills the
  process would lose an attempt without ever running. Early ACK settles each message as it is
  accepted, so it fetches up to `BatchSize` (16 by default). While a message is in flight, a
  background heartbeat signals in-progress (`ProgressAsync`) for it — and, in early ACK, for every
  still-unsettled message of the batch — roughly every `AckWait`/3 (the live consumer's ack wait
  when that is shorter; two chances to land a renewal
  inside every `AckWait` window even when one sweep is delayed), so `AckWait` (30 s) only has to
  survive one heartbeat round-trip, not the slowest handler. A stop cuts a batch short: messages
  that never started are NAKed with no delay so a surviving replica takes them at once. The
  heartbeat is advisory,
  unlike the settlements (which stay deliberately uncancelable): it carries the batch's
  cancellation token into the SDK call, so a heartbeat still in flight when the batch settles is
  aborted, and the batch joins the heartbeat loop for at most one heartbeat interval — a heartbeat
  wedged on a dead socket is abandoned with a warning rather than holding the loop after every
  message in the batch has settled (which left no further batch fetched and a stop never
  completing); unsettled deliveries then fall back to the server's own `AckWait`.
- The durable consumers are created when missing and never modified — whatever `CreateStreams`
  says — so settings an operator tuned on a live consumer (`MaxAckPending`, `BackOff`, metadata)
  survive every subscriber start. An existing consumer this transport cannot work with is refused
  by name: a push consumer, an ack policy other than explicit, or a finite max deliver at or below
  `MaxDeliveryAttempts` — any finite one when it is 0 (the server would stop redelivering before the dispatcher could bury the
  message). A refusal does not fail host startup: every subscriber attempt fails with that error,
  logged as a warning and retried with backoff, and the subscriber consumes nothing until the
  consumer is fixed or deleted (the next attempt then recreates it). An existing consumer's own ack
  wait is used as it is: the in-progress heartbeat renews at a third of the **shorter** of the live
  ack wait and `AckWait`, so raising `AckWait` without editing the consumer keeps renewing inside
  the window the server actually enforces. The drift is logged once as a warning; apply the new
  value to the consumer yourself (`nats consumer edit`) to get the longer window.
- Header-first correlation reads NATS headers through NATS.Net's default ASCII header encoding,
  which turns every non-ASCII character into `?`. A header that is exactly the `?`-mangled form of
  the body's correlation id yields the body's id instead, and the worker transport does not stamp
  the header for a non-ASCII id at all. Remote producers publishing responses with non-ASCII ids
  should rely on the body path (or configure a UTF-8 header encoding on both connections).
- NATS refuses a message above the server's `max_payload` (1 MiB by default) before sending it;
  the worker publish fails such a job on the first attempt instead of running its retry ladder.

### SQS

- Settlement is the visibility timeout: `VisibilityTimeout = null` uses the queue's setting,
  and it must exceed the slowest handler
  ([troubleshooting](troubleshooting.md#sqs-duplicate-executions-or-fifo-settings-that-dont-apply)).
  `RedeliveryDelay` optionally shortens a *failed* message's remaining invisibility via
  `ChangeMessageVisibility`; accounting stays native either way — every receive increments
  `ApproximateReceiveCount` and the queue's redrive policy dead-letters after
  `maxReceiveCount`.
- The worker subscriber in ack-after-handler mode receives **one message at a time**: SQS counts
  — and starts the visibility clock of — every message a receive hands over, so a batch worked
  serially let a process-killing handler bump its batch-mates' receive counts toward the redrive
  policy on every crash, spent the later positions' visibility (and 12-hour ceiling) while they
  waited, and on FIFO ran a failed message's same-group batch-mates ahead of its redelivery.
  `MaxMessagesPerReceive` applies to early ACK and to the response subscriber (whose handler is
  the library's own). `ReceiveWaitTime = 0` (short polling) backs off on the subscriber retry
  schedule between empty receives instead of re-polling once per round trip. Once the flow
  engine hands a delivery back at `ApplicationStopping` (its visibility left untouched), the
  subscriber stops receiving until its own stop, so the wake-ups the engine hands over reach a
  replica still running instead of being taken, and hidden, by this stopping host.
- `VisibilityRenewalInterval` is off by default because extending visibility silently overrides
  redrive timing operators tune on the queue, and on FIFO queues an extended message keeps its
  whole message group blocked if the consumer wedges. When enabled, it requires
  `VisibilityTimeout` to be set and shorter renewal beats; once the failure path schedules
  `RedeliveryDelay` for a message, the renewal sweep suppresses that message so a late renewal
  cannot overwrite the shortened redelivery. Ignored in early ACK. A renewal that fails — including
  the AWS SDK's own client-side HTTP timeout, which surfaces as `TaskCanceledException` — is logged
  and the sweep continues with the rest of the batch; the heartbeat ends when the batch does, not
  at the host stop (the handler still running keeps its visibility until it returns, and messages
  of the batch that never started — including behind a handler that exited through the stop's
  cancellation — are made visible again at once).
- SQS never keeps a message invisible for more than **12 hours** from its receive and rejects a
  `ChangeMessageVisibility` that would cross it, so the heartbeat clamps its last extension to
  what is left, logs one warning ("reached the 12-hour SQS in-flight ceiling") and stops renewing
  that message; SQS then redelivers it however alive its handler is. With renewal on, the worker
  transport advertises those 12 hours as its in-flight ceiling to the durable-flow engine; with
  renewal off, `VisibilityTimeout` — or, when that is unset too, still the 12-hour upper bound,
  because the queue's own visibility timeout (30 s unless configured) is a value the transport
  never reads. The worker subscriber warns at startup in that last configuration: set
  `WorkerSubscriber.VisibilityTimeout` to the queue's value when durable flows run on SQS.
- A **FIFO** worker queue groups jobs by correlation id; every job without one — durable-flow
  start, resume and wake-up jobs among them, unless the flow was started inside a request scope —
  shares the single `FifoMessageGroupIdFallback` group, which SQS delivers strictly one at a time
  across all consumers. FIFO also keeps flow timers in process, so one flow parked on a timer or an
  awaited step holds that group — and every other flow's jobs — for as long as it waits (the
  worker subscriber warns at startup). Prefer a standard worker queue for durable flows. The
  fallback must itself be a valid `MessageGroupId` (startup validates it).
- `CreateQueues` is the only provisioning default that is **off** — production queues (and
  their redrive policies) are usually owned by infrastructure code. When on, converging an
  existing `.fifo` queue re-applies only its mutable attributes: the create-only `FifoQueue`
  attribute is skipped, since `SetQueueAttributes` rejects it (which previously failed host
  startup once the provisioning retries were exhausted). Startup validates the queue names it
  would create — dead-letter names derived with `DeadLetterQueueSuffix` included — against the SQS
  name rule (80 characters of letters, digits, `-` and `_`, `.fifo` counted), and provisioning
  retries only what can still succeed (throttling, 5xx, an endpoint not yet accepting
  connections, eventual consistency right after a create, credentials or the instance-metadata
  endpoint not ready yet, the SDK's own client-side timeout); a deterministic rejection such as
  AccessDenied fails startup at once instead of after the whole retry budget.
- Queue strings may be names or URLs. The worker/response, derived dead-letter and reply-target
  collision guards compare two names, or two URLs (normalized: scheme, host and default port
  folded), exactly, and fail startup — or `GetReplyTarget` — on a match. A name and a URL that
  share the queue name (`jobs` and `https://sqs.…/123456789012/jobs`) may be one queue (the name
  resolves in the client's own account and region) or two (the URL is another account's), which
  the transport cannot tell without a `GetQueueUrl` call — so the worker subscriber **warns**
  about such a pair at startup instead of failing. Configure both sides as URLs to make the
  comparison exact.
- `CorrelationIdAttribute` resolution is case-**sensitive**, unlike every other transport's
  case-insensitive inbound header lookup — AWS message attribute names are themselves
  case-sensitive, so `CorrelationId` and `correlationId` can coexist as two distinct attributes on
  one message; a case-folding lookup would alias them. The outbound publish path is also ordinal.

### PostgreSQL, SQL Server, MongoDB

- The claim is atomic (`FOR UPDATE SKIP LOCKED` / `UPDLOCK, ROWLOCK, READPAST` /
  `findOneAndUpdate`) and increments the attempt counter in the store, so attempts survive
  process restarts and are visible in the queue table/collection.
- While a handler runs in ack-after-handler mode, a heartbeat renews the claim's lease every
  `LockTimeout`/3, fenced by the claim's `lock_id` (MongoDB additionally evaluates the lease
  against the server clock via `$$NOW`, so client clock skew cannot fence messages in or out).
  If the fence no longer matches — the lease lapsed and a peer re-claimed the row/document —
  renewal stops and the fenced ack/NAK no-ops for the stale claim: at-least-once is preserved,
  and the loss is logged. Renewal *failures* (transient DB errors) are logged and retried on a
  short backoff (a second, or `LockTimeout`/10 when shorter), so one failed beat still renews with
  most of the lease left. Each renew attempt is **bounded** by the beat interval (the stores
  cancel the connect and command, and SQL Server also sets the command timeout): a renew hung on a
  black-holed pooled connection or a failover is abandoned and retried on a fresh connection
  inside the lease, instead of outliving it at the provider's own command timeout (30 s on
  Npgsql and SqlClient, none on MongoDB). The heartbeat is **not** joined before settlement —
  every settlement is fenced by `lock_id`, so a beat still in flight is a no-op against it — and
  it is **not** tied to the subscriber's stop: the handler takes no token and runs on through the
  host's stop budget, so the heartbeat keeps its lease until the handler actually ends (otherwise
  a peer, such as a new replica mid-deploy, claimed and ran the row concurrently).
- A delivery the durable-flow engine hands back because the host is stopping
  (`DurableFlowInterruptedException`, which arrives before this subscriber's own stop token is
  cancelled) is left **unsettled** exactly like the subscriber's own shutdown: no "failed on
  attempt" warning, no NAK, no dead-letter write of its own. The lease lapses and the row is
  redelivered after the restart — as its next attempt, so a hand-back at the cap is dead-lettered
  on that redelivery without running.
- Under early ACK, a claim that parks on a full background queue keeps its lease renewed for the
  park's duration; if that heartbeat reports the lease **lost** (a peer re-claimed the row), the
  park drops the delivery instead of enqueueing and running a job a peer already owns.
- The MongoDB transport pins its collection handle to the primary (channel and flow-store
  parity), so a `secondaryPreferred` client cannot route the change-stream wake to a lagging
  secondary, and writes its publishes, dead letters and deletes (ack, burial, prune) with
  `w: "majority"` bounded by a `wtimeout` (an inherited `wtimeoutMS` or `journal` is kept; 10 s
  otherwise) — under an inherited `w: 1` a failover could roll back a job, or a durable flow's
  wake-up, whose publish the caller had already seen succeed. A lapsed `wtimeout` surfaces as a
  failed (retried) publish whose write may still have been applied: the idempotent insert absorbs
  the retry. The lease writes — claim, renew, NAK — keep the connection's own write concern: a
  rolled-back lease write only makes the document claimable again, exactly like a lapsed lease,
  whereas a claim that applied but reported a `wtimeout` would burn a delivery attempt for a job
  that never ran, and one acknowledged only after the replication wait could return after its own
  lease had expired. The claim reads the stamped document as raw BSON: a
  document the transport cannot read — a foreign producer's driver-generated `ObjectId` `_id`, a
  `payload` written as an embedded document, any mistyped field — is **dead-lettered on sight**
  (fenced by the claim's `lock_id`, payload/headers kept, reason in `AR-DeadLetter-Reason`) and
  the claim moves on, instead of throwing after the claim stamp and tearing the subscriber down
  every `LockTimeout` forever. Foreign producers should write a `binData` UUID `_id` (subtype 4),
  a string `payload`, and the `[{k, v}]` header array the transport itself writes. With
  `AutoCreateIndexes = false` it runs a one-time read-only check and **warns**
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
  message that is still live under its new owner. On MongoDB, which has no cross-document
  transaction to rely on, the DLQ document is written first under a deterministic id and is
  deliberately **kept** when the fenced delete does not match: a peer that reached the cap may
  have buried into that same document, and the worst a kept copy can be is a spurious,
  retention-prunable DLQ entry for a message whose new owner later succeeds.
- The dead-letter queue is rows/documents in the same table/collection under the
  `DeadLetterQueue` logical name; it has no consumer by default, so set `DeadLetterRetention`
  if entries should be pruned instead of kept for manual inspection. The prune runs after a
  publish, at most once a minute per process (on the monotonic clock, so a wall-clock step cannot
  suspend it) — on MongoDB as one `deleteMany`, on PostgreSQL and SQL Server in bounded batches
  of 1,000 rows (SQL Server `DELETE TOP (1000)`, PostgreSQL a
  `ctid … LIMIT 1000` batch — an unbounded delete escalated to a table lock that `READPAST`
  cannot skip on SQL Server, and on PostgreSQL outran the command timeout over a large backlog,
  rolled back, and never shrank it), draining further batches for up to 2 s while each comes
  back full and warning when it stops with rows remaining. A prune failure — or the publisher's
  token firing mid-prune — is logged and never fails the publish it follows, which had already
  committed. DDL is owned by `AutoCreateSchema` (PostgreSQL, SQL Server) / `AutoCreateIndexes`
  (MongoDB) — disable when migrations own it.
- Under early ACK, `BackgroundDrainTimeout` is split: three quarters for the queued and running
  handlers, one quarter reserved for dead-lettering — and reporting via `OnBackgroundFailure` —
  the already-ACKed entries still queued when that drain budget lapses. Their rows/documents were
  deleted by the early ACK, so without the reserve they were simply lost at process exit.
- Only PostgreSQL and MongoDB spend a `ShutdownTimeout` at stop (bounding the LISTEN /
  change-stream task join). SQL Server has no push channel to join and therefore no
  `ShutdownTimeout` option; it budgets only the drain. When **both** subscribers use early ACK,
  the shutdown-budget check sums both roles' spends: the host stops hosted services one after
  another (the default `HostOptions.ServicesStopConcurrently = false`), so the response
  subscriber's drain runs to completion before the worker's stop begins.
