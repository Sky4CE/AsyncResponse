# Troubleshooting

[← Back to README](../README.md)

Symptom → cause → fix for the gotchas that surface most often. Every entry links to the page that
owns the full story — this page is the map, not the territory.

**On this page**

- [Channels](#channels)
- [Transports](#transports)
- [Durable flows](#durable-flows)
- [Trimming / Native AOT](#trimming--native-aot)
- [Contributing](#contributing)

## Channels

### MongoDB waiters wake slowly, or the driver complains about change streams

- **Symptom:** MongoDB channel/transport works, but response wakes arrive at polling cadence — or
  startup logs a change-stream error.
- **Cause:** MongoDB change streams require a **replica set**. Against a standalone server the
  channel falls back to `ListenerPollInterval` polling (the transport to `EmptyPollDelay`), which
  is correct but slower. In that mode — and with `UseChangeStreams = false` — the channel sweeps
  on every tick, ignoring `FullSweepInterval`: the throttled sweep would be its only cross-process
  wake, and the 5 s default equalled `DeliveryConfirmationTimeout`, so a cross-process response
  could route to recovery before its waiter had even looked.
- **Fix:** run a replica set — single-node is sufficient — and include `directConnection=true` in
  the connection string when connecting to a single-node replica set. See the MongoDB rows in
  [channel options](configuration.md#channel-options).

### Garnet returns `unknown command` for `XADD` / `XREADGROUP`

- **Symptom:** the Redis transport fails against Garnet with `unknown command` errors on stream
  commands.
- **Cause:** Garnet does not implement Redis Streams. It is validated as a **channel-only** server.
- **Fix:** keep Garnet for the channel and run the transport on a streams-capable server (Redis,
  Valkey, Dragonfly) or another transport entirely. See
  [Redis-compatible servers](configuration.md#redis-compatible-servers).

## Transports

### Azure Service Bus: `MessageLockLostException` redeliveries of already-processed messages

- **Symptom:** handlers complete, yet messages reappear and Service Bus reports lost locks.
- **Cause:** the peek-lock budget. A handler that outlives the queue's lock duration (60 s by
  default) loses its lock unless it is renewed — and prefetched messages (`PrefetchCount > 0`) are
  locked while they wait in the client buffer, where no renewal reaches them.
- **Fix:** keep `WorkerSubscriber.LockRenewalInterval` on (the default) for long handlers, or keep
  handler latency well under the lock duration; leave `PrefetchCount` at 0 unless handlers are fast.
  (In ack-after-handler mode the worker subscriber receives one message at a time, so batch size no longer
  adds to the budget.) See [transport options](configuration.md#transport-options).

### SQS: duplicate executions, or FIFO settings that don't apply

- **Symptom:** already-processed messages run again; or `MessageGroupId` ordering never engages.
- **Cause:** the visibility budget, same shape as the Service Bus lock budget — a handler must
  finish within the message's visibility timeout (the queue's own, 30 s unless configured, when
  `WorkerSubscriber.VisibilityTimeout` is unset). FIFO behavior is opt-in by **queue naming**, not
  an option flag.
- **Fix:** keep handler latency under the visibility timeout (raise
  `WorkerSubscriber.VisibilityTimeout`, or use visibility renewal for long handlers — up to the
  12-hour SQS in-flight maximum), and name the queue `*.fifo` to opt into FIFO publishing. When
  durable flows run on SQS, set `VisibilityTimeout` explicitly (the startup warning says so) and
  prefer a standard worker queue: on FIFO every uncorrelated job shares one serial message group.
  See [transport options](configuration.md#transport-options).

### Kafka: rebalances or duplicate runs while long handlers execute

- **Symptom:** `Application maximum poll interval (…ms) exceeded` from librdkafka, rebalances, and
  a worker job or flow step that ran twice — once here and once on the peer the partition moved
  to — while a handler was still running.
- **Cause:** a consumer that stops polling for `max.poll.interval.ms` (default 5 minutes) is
  evicted from its group. Before round 37 the poll thread awaited the whole handler, so a
  durable-flow step awaiting a remote response or sleeping on a timer for longer than the
  interval — or a long in-process retry ladder — did exactly that. Now a handler still running
  after `WorkerSubscriber.DetachHandlerAfter` (default 1 s) is detached: its partition is paused,
  the handler runs on, and the poll thread keeps polling; the offset is stored once the handler
  settles. The symptom can therefore only remain when the inline budget itself is raised toward
  the interval (validation allows up to half of it, minus `PollTimeout`), or when a
  `ConfigureConsumer` hook overrides `MaxPollIntervalMs` below what the library configured.
- **Fix:** leave `DetachHandlerAfter` at its default and do not override `MaxPollIntervalMs` in
  `ConfigureConsumer`; set `MaxPollInterval` on the subscriber options instead, which validates
  the inline budget against it. See [transport options](configuration.md#transport-options) and
  [transport semantics](transport-semantics.md#kafka).

### Kafka: `Abandoning detached Kafka handler …` after a broker failure

- **Symptom:** a warning naming a message whose handler was still running when the poll loop
  failed, then the subscriber reconnects and the same message is handled again — for a durable
  flow, a second execution that acknowledges as a duplicate (the lease is held); for a plain
  worker job, a second run.
- **Cause:** the consume failed (a dropped connection, a burial that failed for good) and the
  fault teardown waited `WorkerSubscriber.FaultDrainTimeout` (default 5 s) for the detached
  handlers; this one did not settle in time, so its offset was left unstored and the rebuilt
  consumer re-consumed it. The handler's eventual outcome is logged (`Abandoned Kafka handler …
  completed/failed/stopped …`). Before the bound, the reconnect waited for every detached handler
  with no limit and a transient broker failure parked the subscriber behind one long step.
- **Fix:** nothing, if the handler is idempotent — this is the transport's at-least-once
  contract. Raise `FaultDrainTimeout` when handlers reliably settle within a known window and
  you would rather delay the reconnect than redeliver; make plain worker jobs idempotent
  regardless. See [transport semantics](transport-semantics.md#kafka).

### `WorkerJobTooLargeException` from `EnqueueWorkerAsync` or `StartAsync`

- **Symptom:** the publish throws `WorkerJobTooLargeException` naming the envelope's serialized
  length and `AsyncResponseOptions.MaxInboundMessageChars`; nothing was published, no ledger exists.
- **Cause:** the serialized worker envelope — arguments, captured context, and for a flow start
  the whole initial ledger, input included — exceeds what the consuming ingress accepts. The
  ingress acknowledges such a message *without executing it* (an oversized message never gets
  smaller, so redelivering it would hot-loop), so before this check the transport took the job,
  the ingress dropped it, and the caller held a flow id for a `Running` run nothing would ever
  execute. JSON escaping counts: quotes, non-ASCII and control characters serialize to several
  times their length. A delayed job is measured as its largest re-published hop: when an early
  delivery is re-published for the remaining delay, the stamped remainder makes the envelope up
  to 24 characters longer.
- **Fix:** put the large argument behind a claim check — persist it yourself and pass a reference
  (see the [durable-flows ledger budgets](durable-flows.md#supported-ledger-budgets)) — rather
  than raising the limit; if you do raise it, raise it identically on every producer and consumer
  of the deployment.

### Redis: a wait faults with `AsyncResponseIndeterminateDeliveryException` saying responses "arrived faster than the wait could process them"

- **Symptom:** the waiter faults with the overload form of the exception (`BufferedMessages` =
  1,024), the log carries `Wait for correlationId … is overloaded`, and
  `asyncresponse.channel.overloaded_waits` counts up for `channel=redis`.
- **Cause:** responses for one correlation id — typically a progress-message flood — arrived
  faster than the wait's serial processing (its `Until` predicate) consumed them, and the bounded
  per-wait buffer filled. Redis pub/sub cannot backpressure the publisher, and the SDK queue
  behind the subscription is unbounded, so the channel refuses the next response instead of
  buffering it without bound. The refused or queued responses may include the terminal one, which
  is why the wait is faulted as indeterminate rather than completed or timed out.
- **Fix:** make the predicate cheap (no I/O per progress message), publish fewer progress
  messages, or move the wait to a database channel, whose backlog stays server-side and is
  admitted as capacity frees. Durable flows restart the awaiting step on this fault automatically;
  plain waiters should treat the step as indeterminate and restart it rather than re-attach.

### Kafka: the subscriber restarts repeatedly, each time naming a message it "could not dead-letter"

- **Symptom:** `Kafka subscriber failed for topic … retrying in …` on a backoff cadence, each
  preceded by `Failed to dead-letter Kafka message …@{offset}`; that subscriber's partitions stop
  advancing.
- **Cause:** a message exhausted `MaxDeliveryAttempts` (or could not be parsed) and its produce to
  the dead-letter topic keeps failing — the topic does not exist with auto-create off, its
  partition is leaderless, the payload exceeds the broker's message cap. The library faults the
  subscriber on purpose: leaving the offset unstored and consuming on would let the next
  settlement commit past the message, losing it with no record (see
  [transport semantics](transport-semantics.md#kafka)).
- **Fix:** fix the dead-letter topic (create it, size `message.max.bytes`, restore its leader).
  The next restart buries the message and the partition moves. Do not raise `MaxDeliveryAttempts`
  to "get past" it — the handler is re-run per restart regardless.

### RabbitMQ: startup warns about `MaxDeliveryAttempts`, or a poison message loops forever

- **Symptom:** a startup warning about delivery attempts, or a failing message that redelivers
  endlessly instead of dead-lettering.
- **Cause:** `MaxDeliveryAttempts` defaults to `0` (unlimited): a persistently failing message
  requeues forever rather than being silently dropped — deliberate for a durability-focused
  default, but it means poison protection is opt-in. Additionally, the broker does not count
  plain `basic.nack` requeues, so `MaxDeliveryAttempts` values above 2 need a TTL-retry
  dead-letter cycle to make attempts countable. Without one the cap is *clamped to 2* — the
  message is rejected on its second delivery rather than requeued forever, which is what the
  startup warning is telling you. Once a message carries `x-death` (it has been dead-lettered at
  least once), every further retry below the cap rejects **without** requeue so the cycle is what
  counts it — a plain requeue never advances `x-death`. The cap is judged before the handler
  runs too: a delivery whose previous attempt ended without a thrown exception (the process was
  killed mid-handler; the broker requeued it with `redelivered`) is dead-lettered without
  executing rather than crash-looping every replica.
- **Fix:** for production, set a positive `MaxDeliveryAttempts` **and** configure
  `DeadLetterExchange` (so capped-out messages are preserved, not dropped). `DeclareTopology`
  declares the dead-letter *wiring* (`x-dead-letter-exchange` on the worker and response queues),
  not the retry cycle: to make a cap above 2 reachable, declare a dead-letter queue with
  `x-message-ttl` that dead-letters back to the source exchange in your own topology. The cap
  then **terminates** that cycle: a message reaching it with `x-death` present is parked — copied
  to `ParkQueue` (or, without one, `DeadLetterQueue`) through the default exchange (bypassing the
  cycling exchange) and ACKed, or ACKed and dropped with an error log when neither is configured —
  instead of re-entering the cycle at its TTL rate forever. Prefer `ParkQueue` with a TTL-retry
  cycle: it is declared unbound, so it holds only parked messages, whereas `DeadLetterQueue` is
  bound to the dead-letter exchange and also collects a copy of every retry hop. A park the broker
  cannot route (the queue is missing, a `reject-publish` queue is full) fails under publisher
  confirms and is NACKed with requeue after the subscriber backoff, so it retries until the queue
  is fixed — watch for `Failed to park capped RabbitMQ delivery`. Otherwise keep
  `MaxDeliveryAttempts` at 2 or below and silence the warning. See
  [transport options](configuration.md#transport-options).
- **Quorum queues (RabbitMQ 4.x):** the broker applies its own `delivery-limit` — 20 by default —
  whatever `MaxDeliveryAttempts` says, and past it dead-letters the message, or **drops** it when
  no dead-letter exchange is set. `MaxDeliveryAttempts = 0` is therefore not "forever" on a quorum
  queue (the vhost's default queue type may make the worker queue one): configure a dead-letter
  exchange, or raise/disable `delivery-limit` by policy.
- **Replaying a parked or dead-lettered message:** the copy keeps the original headers, `x-death`
  included, so a message republished into the worker queue as-is resolves its attempt from that
  count. The check before the handler is strict (`attempt > cap`): a copy whose count puts it
  exactly at the cap runs its handler **once more** and, if it fails, is parked; one past the cap
  (with `MaxDeliveryAttempts = 1`, every copy carrying `x-death`) is parked again without its
  handler running. Strip `x-death` (and the `AR-DeadLetter-*` headers)
  when replaying so the replay starts from attempt 1, or, for a durable flow, call
  `ResumeAsync(flowId)` instead of replaying its wake-up.
- **`MaxDeliveryAttempts = 1` in `AckAfterHandlerCompletes`:** a startup warning (durable-flow jobs
  ride the worker queue). Every delivery the broker requeues on its own — a flow handed back at host
  stop, a delivery prefetched but not yet started when host stop began, a channel closed under a
  running handler — comes back `redelivered`, resolves to attempt 2 and is rejected before its
  handler runs, so a flow's wake-up can be lost on a routine deploy. Use 2 or more (or 0).

## Durable flows

### A flow is stuck `Running`

- **Symptom:** `GetStateAsync` reports `Running`, but nothing progresses.
- **Cause:** the worker job carrying the flow id dead-lettered (a retriable failure exhausted the
  transport's delivery attempts), or the owning process died and its execution lease has not
  expired yet. A run with `Attempts == 0` was never picked up: its wake-up is queued behind a busy
  worker, or was lost in transit (an early-ACK worker subscriber, a broker that dropped it).
- **Fix:** check the transport's dead-letter queue first — the DLQ entry is the alarm. Replay it
  (on RabbitMQ strip its `x-death` header first — see
  [above](#rabbitmq-startup-warns-about-maxdeliveryattempts-or-a-poison-message-loops-forever))
  or call `ResumeAsync(flowId)` to re-enqueue the run. After a crash, expect up to the crashed
  owner's `ExecutionLeaseDuration` — the value it was *running with*, which a later deployment may
  have changed — before another replica may take the run over; the redelivered wake-up waits for
  that persisted expiry on its own. See
  [what happens when things die](durable-flows.md#what-happens-when-things-die).

### A flow job fails with `DurableFlowLeaseContendedException`

- **Symptom:** a worker job for a flow retries (and may dead-letter) with *"could not acquire the
  execution lease and cannot prove the run is executing elsewhere"*.
- **Cause:** the wake-up found the execution lease held and waited, and either saw neither the
  lease come free nor proof of a live holder (the lease being renewed or taken over), or proved a
  live holder that is executing *this same job*. The engine never acknowledges a wake-up in either
  state — it may be the run's only one. The reason in the message says which case it is:
  *"the flow state store does not report leases"* — an application-owned `IFlowStateStore` that
  does not implement `ObserveLeaseAsync`, so a duplicate of a long-running execution cannot be
  recognized as one; *"although the store reports no holder"* — `ObserveLeaseAsync` kept reporting
  no lease while every acquire failed for this host's whole lease window, so the store's lease
  report and its acquire disagree (a store or decorator bug); *"neither changed nor became
  acquirable … past that expiry"* — the store still refuses the lease a full lease window after
  the expiry it reports, which points at clock skew between this host and the store (or a store
  bug); *"its persisted expiry lies further out than that budget lets one delivery wait"* — the
  expiry the store reports is more than `MaxLeaseContentionWait` (default 1 hour) away, which is
  either a deployment that really issues leases that long, or a store clock (or expiry column) far
  ahead of this host; or *"held by a live execution of this same worker job"* — the broker
  redelivered a job whose handler is still running because an in-flight ceiling lapsed under it
  (Pub/Sub `MaxTotalAckExtension`, RabbitMQ `consumer_timeout`, the SQS 12-hour visibility cap, a
  Kafka rebalance), and the transport cannot re-publish it delayed past the holder's lease.
- **Fix:** implement `ObserveLeaseAsync` in the custom store (and forward it from any decorator
  around a built-in one); fix time synchronization; raise `MaxLeaseContentionWait` when leases
  longer than it are intended; for the same-job case, keep a single in-process wait shorter than
  the broker's in-flight ceiling (`DurableFlowOptions.MaxInProcessParkDuration`) or raise the
  ceiling. The redelivered job completes the run once
  the lease is free — a dead-lettered one needs a replay or `ResumeAsync(flowId)`. See
  [lease contention](durable-flow-state-stores.md#lease-contention-and-deployments-that-change-the-lease-duration).

### A flow logs a `LedgerSizeWarningBytes` warning

- **Symptom:** `Durable flow {id} ledger is roughly N bytes over K step(s), past the … threshold`,
  once and then again each time the size doubles.
- **Cause:** step results (and values) accumulate in the ledger, and every checkpoint rewrites the
  whole ledger — a run of N similar steps serializes about N²/2 step-results over its lifetime and
  eventually hits the store's `MaxStateBytes` cap.
- **Fix:** keep large results out of the ledger (persist them yourself and pass references),
  partition a long history into child flows, or — if the sizes are expected — raise
  `DurableFlowOptions.LedgerSizeWarningBytes` (set `null` to disable). On DynamoDB lower it: the
  store's 350 KB `MaxStateBytes` default (headroom under DynamoDB's 400 KB item cap) sits under the
  512 KiB default. See
  [ledger growth](durable-flows.md#storage-where-flow-state-lives).

### Every attempt of an awaited step fails with an `OnRecovery` error

- **Symptom:** a flow never gets past its first `AwaitStepAsync`; each delivery throws
  `InvalidOperationException` naming a payload type and `OnRecovery`, and the run eventually
  dead-letters (or, on the in-memory transport, is dropped after its retry budget).
- **Cause:** durable flows register lost-subscriber recovery callbacks on *every* awaited step, and
  a payload that does not override `IAsyncResponsePayload.OnRecovery()` cannot be classified when
  it arrives with no live waiter — so waiter creation fails fast rather than guessing. Every
  channel enforces this, the in-memory one included; code written before that was uniform can hit
  it the first time it runs in-memory.
- **Fix:** override `OnRecovery()` on the payload — terminal success → `Resume`, terminal failure
  → `Fail`, progress/checkpoint payloads → `KeepWaiting` (which is what keeps a progress message
  from consuming the registration the terminal response still needs). See
  [recovery.md](recovery.md).

### Flow state exceeds the store's size limit

- **Symptom:** a checkpoint fails with an error naming the flow, its state size, and the limit.
- **Cause:** step results and values-bag entries are persisted in the flow ledger; large payloads
  grow the state past the store's `MaxStateBytes` cap.
- **Fix:** keep large payloads in your own storage and pass **references** (ids, URIs) through
  steps instead of the data itself. See
  [where flow state lives](durable-flows.md#storage-where-flow-state-lives).

## Trimming / Native AOT

### A trimmed app fails serialization, naming a type and a registration call

- **Symptom:** in a trimmed/Native AOT app, serializing a payload/flow input fails with an error
  telling you which type is unregistered and which call to make.
- **Cause:** the trimmed app removes the reflection serializer link; payload types need
  source-generated JSON metadata.
- **Fix:** add the type to your `JsonSerializerContext` and register it once at startup with
  `AsyncResponseJsonSerialization.RegisterResolver(MyAppJsonContext.Default)`. See
  [what you do in a trimmed / Native AOT app](aot.md#what-you-do-in-a-trimmed--native-aot-app).

## Contributing

### The build fails with `RS0016` / `RS0017` after adding a public member

- **Symptom:** a clean-looking change fails the build with public-API analyzer diagnostics.
- **Cause:** every package tracks its public surface with `Microsoft.CodeAnalysis.PublicApiAnalyzers`;
  new public members must be recorded in that package's `PublicAPI.Unshipped.txt`.
- **Fix:** apply the IDE's "Add to public API" code fix, or run `dotnet format analyzers`. This is
  the API-review gate, not a broken build. See
  [CONTRIBUTING.md](../CONTRIBUTING.md#adding-public-api).
