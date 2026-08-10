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
  is correct but slower.
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
- **Cause:** the peek-lock budget. A receive batch is processed sequentially, so the last message
  in a batch waits up to `MaxMessagesPerReceive × handler latency` before settlement — past the
  queue's lock duration, the lock is gone.
- **Fix:** keep that product well under the queue's lock duration, lower `MaxMessagesPerReceive`,
  or enable the transport's lock-renewal option for long handlers. See
  [transport options](configuration.md#transport-options).

### SQS: duplicate executions, or FIFO settings that don't apply

- **Symptom:** already-processed messages run again; or `MessageGroupId` ordering never engages.
- **Cause:** the visibility budget, same shape as the Service Bus lock budget — a sequentially
  processed batch must finish within the queue's visibility timeout. FIFO behavior is opt-in by
  **queue naming**, not an option flag.
- **Fix:** keep `MaxMessagesPerReceive × handler latency` under the visibility timeout (raise
  `WorkerSubscriber.VisibilityTimeout`, lower the batch size, or use visibility renewal for long
  handlers), and name the queue `*.fifo` to opt into FIFO publishing. See
  [transport options](configuration.md#transport-options).

### Kafka: the broker evicts the consumer mid-retry

- **Symptom:** rebalances and consumer evictions while a failing message is being retried.
- **Cause:** in-process retries happen inside one poll cycle, so the worst-case budget
  `MaxDeliveryAttempts × HandlerRetryMaxDelay` can exceed the consumer's `max.poll.interval.ms`
  (default 5 minutes) — the broker then considers the consumer dead.
- **Fix:** keep the retry budget well under `max.poll.interval.ms`, or raise the interval via
  `ConfigureConsumer`. See [transport options](configuration.md#transport-options).

### RabbitMQ: startup warns about `MaxDeliveryAttempts`, or a poison message loops forever

- **Symptom:** a startup warning about delivery attempts, or a failing message that redelivers
  endlessly instead of dead-lettering.
- **Cause:** `MaxDeliveryAttempts` defaults to `0` (unlimited): a persistently failing message
  requeues forever rather than being silently dropped — deliberate for a durability-focused
  default, but it means poison protection is opt-in. Additionally, the broker does not count
  plain `basic.nack` requeues, so `MaxDeliveryAttempts` values above 2 need the TTL-retry
  dead-letter cycle to make attempts countable.
- **Fix:** for production, set a positive `MaxDeliveryAttempts` **and** configure
  `DeadLetterExchange` (so capped-out messages are preserved, not dropped); let the package
  declare the cycle (`DeclareTopology` on), declare it in your own topology when infra owns it,
  or keep `MaxDeliveryAttempts` at 2 or below. See
  [transport options](configuration.md#transport-options).

## Durable flows

### A flow is stuck `Running`

- **Symptom:** `GetStateAsync` reports `Running`, but nothing progresses.
- **Cause:** the worker job carrying the flow id dead-lettered (a retriable failure exhausted the
  transport's delivery attempts), or the owning process died and its execution lease has not
  expired yet.
- **Fix:** check the transport's dead-letter queue first — the DLQ entry is the alarm. Replay it
  or call `ResumeAsync(flowId)` to re-enqueue the run. After a crash, expect up to
  `ExecutionLeaseDuration` before another replica may take the run over. See
  [what happens when things die](durable-flows.md#what-happens-when-things-die).

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
