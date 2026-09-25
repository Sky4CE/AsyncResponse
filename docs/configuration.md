# Configuration

[← Back to README](../README.md)

`AddAsyncResponse()` registers the channel-agnostic engine but **selects no channel, transport, or
durable-flow store** — chain
exactly one channel (`.WithInMemoryChannel()`, `.WithRedisChannel()`, `.WithNatsChannel()`,
`.WithPostgreSqlChannel(...)`, `.WithSqlServerChannel(...)`, or `.WithMongoDbChannel(...)`) and
exactly one transport (`.WithInMemoryTransport()`, `.WithRedisTransport(...)`,
`.WithAzureServiceBusTransport(...)`, `.WithGooglePubSubTransport(...)`,
`.WithRabbitMqTransport(...)`, `.WithSqsTransport(...)`, `.WithKafkaTransport(...)`,
`.WithNatsTransport(...)`, `.WithPostgreSqlTransport(...)`, `.WithSqlServerTransport(...)`, or
`.WithMongoDbTransport(...)`), and exactly one flow store (`.WithInMemoryDurableFlows()`, a
`.With*DurableFlows(...)` provider, or `.WithDurableFlows<TStore>()`). An app that starts without any
one of the three fails fast with setup guidance. The recovery watchdog is part of the engine and
runs by default for whichever channel you choose.

This page is the consolidated options reference: engine options, durable-flow store package
options, channel options, transport options, and the per-transport delivery semantics behind them.
For setup code rather than option lookup, use the
[channel, transport, and flow-store examples](provider-examples.md).

**On this page**

- [Engine options (`AsyncResponseOptions`)](#engine-options-asyncresponseoptions)
- [Durable-flow state store package options](#durable-flow-state-store-package-options)
- [Channel options](#channel-options)
- [Transport options](#transport-options) — including per-transport ACK/redelivery semantics
- [Redis-compatible servers](#redis-compatible-servers)

```csharp
builder.Services.AddAsyncResponse(options =>
{
    options.Watchdog.Interval = TimeSpan.FromHours(6);
    options.Watchdog.StaleAfter = TimeSpan.FromHours(24);
    options.Watchdog.StartupDelay = TimeSpan.FromMinutes(5);
    // options.Watchdog.Enabled = false;                   // e.g. all but one host per Redis
})
.WithRedisChannel(options =>
{
    options.KeyPrefix = "myapp";                            // isolate apps/environments
    options.RecoveryStateExpiry = TimeSpan.FromDays(7);     // how long recovery survives
    options.DefaultTimeout = TimeSpan.FromHours(12);        // default per-waiter timeout
})
.WithInMemoryTransport()                                    // or .WithAzureServiceBusTransport(...) / .WithRabbitMqTransport(...)
.WithInMemoryDurableFlows(options =>
{
    options.StateExpiry = TimeSpan.FromDays(14);             // idle TTL, refreshed at checkpoints
    options.ExecutionLeaseDuration = TimeSpan.FromMinutes(1);
    options.ExecutionLeaseRenewInterval = TimeSpan.FromSeconds(20);
});
```

## Engine options (`AsyncResponseOptions`)

Configured through the `AddAsyncResponse(options => …)` callback.

| Option | Default | Purpose |
|---|---|---|
| `Watchdog.Enabled` | `true` | Run the recovery watchdog in this host. Disable in all but one host when several share one store, so its scan and warnings aren't duplicated. |
| `Watchdog.Interval` | 6 hours | How often the watchdog scans persisted recovery state. |
| `Watchdog.StaleAfter` | 24 hours | Age at which an entry with no live waiter is reported stale. |
| `Watchdog.MaxScanEntries` | 100 000 | Upper bound on recovery entries one scan buffers before probing (unique correlation ids + individual correlation-less entries — a memory bound, not a flow count). Larger stores are reported for the buffered subset only: the report carries `Truncated`, the recovery health check degrades, the `asyncresponse.recovery.scan_truncated` gauge reads 1, and a warning is logged. |
| `Watchdog.ProbeConcurrency` | 8 | Upper bound on liveness probes one scan runs concurrently — each probe is its own round trip to the channel, and a scan issues one per buffered entry, so probing strictly sequentially would serialize up to `MaxScanEntries` round trips. Must be at least 1. |
| `Watchdog.StartupDelay` | 5 minutes | Delay before the first scan after host start. |
| `Watchdog.IntervalJitter` | 10% of `Interval` | Random extra delay added to the startup delay and to every interval wait, so replicas deployed together do not scan in lockstep and fire their liveness probes at the same instant. Set to `TimeSpan.Zero` for an exactly-periodic scan. Cannot exceed `Interval`. |
| `MaxInboundMessageChars` | 8 Mi | Largest inbound message the ingress will process, in UTF-16 code units. Larger messages are acknowledged without dispatch, with an error log and the `asyncresponse.ingress.oversized_messages` counter — an oversized message never gets smaller, so redelivering it would hot-loop. Also enforced **producer-side**: every `EnqueueWorkerAsync` overload, and `IDurableFlows.StartAsync` (whose start job carries the initial ledger, input included), measures the serialized envelope and throws `WorkerJobTooLargeException` before publishing when it would exceed this budget — so an oversized job fails in the caller's stack instead of being acknowledged unexecuted by a consumer while the caller holds a flow id. The producer's own value stands in for the consumer's: keep it identical across the processes of one deployment. `null` removes the limit; a non-positive value is rejected at startup. A memory guard, not a business rule: put large payloads behind a claim check rather than raising it. |

The watchdog values in the [example above](#configuration) are exactly these defaults — shown so
you can see which knobs exist, not because they need changing.

See [recovery.md](recovery.md) for the watchdog in context and [security.md](security.md) for
`.AuthorizeCallbacks(...)` and type-resolution registration, which are also chained off
`AddAsyncResponse()`.

## Durable-flow state store package options

`AddAsyncResponse()` does not choose a flow store. Complete registration with exactly one
`AsyncResponse.DurableFlows.*` provider, `.WithInMemoryDurableFlows()` for a process-local setup, or
`.WithDurableFlows<TStore>()` for an application-owned atomic store. Every variant accepts the
common flow-engine options in its own callback; provider variants add store-specific properties to
that same options object. Configure everything in that one callback: a second registration for
the same store (a provider variant followed by `.WithDurableFlows<TStore>()` to "adjust" a common
setting) is rejected at startup, because the engine would consume only the last one.

### Common durable-flow options

| Option | Default | Purpose |
|---|---|---|
| `StateExpiry` | 14 days | Idle TTL for persisted flow state; refreshed on every checkpoint, so it bounds the gap *between* checkpoints, not total run duration. Deliberately double the 7-day default step-timeout chain so a silent step faults before its ledger expires. Also bounds the longest single `DelayAsync`/`DelayUntilAsync` sleep: the 3650-day persistence ceiling **minus** this value (default → 3636 days), so a sleeping ledger's TTL always outlives its own wake-up by the full idle margin. |
| `MaxFlowIdLength` (const) | 400 | Portable flow-id length in characters — the `flow_id` column length in the SQL Server, MySQL, Oracle, and EF Core stores. Every final id (root, composed child `:{stepName}`, scheduled `sched:{name}:{timestamp}`) is validated at creation, so an id cannot work on one store and fail on another. |
| `MaxFlowIdBytes` (const) | 1023 | Portable flow-id size in UTF-8 bytes — the Cosmos DB id limit, which 400 multi-byte characters exceed. Ids must also avoid `/`, `\`, `?`, `#` (Cosmos rejects them) and control characters, and are compared ordinally (a binary collation is pinned on the relational columns). |
| `DefaultStepTimeout` | `null` (channel default) | Default timeout for `AwaitStepAsync` steps that don't pass one explicitly. |
| `ExecutionLeaseDuration` | 1 minute | How long one store lease owns a flow execution before another replica may take over after owner loss. Safe to change between deployments: a wake-up behind a lease written under the previous value waits for that lease's persisted expiry, not for this host's window ([details](durable-flow-state-stores.md#lease-contention-and-deployments-that-change-the-lease-duration)). |
| `ExecutionLeaseRenewInterval` | 20 seconds | Renewal cadence; must be positive and shorter than `ExecutionLeaseDuration`. |
| `MaxLeaseContentionWait` | 1 hour | The longest one wake-up stays parked behind another worker's lease **because of the expiry the store reports for it**. A contended wake-up waits the holder's *persisted* expiry out, and that expiry is data this host does not control — a store clock hours ahead, or an expiry read back shifted, would otherwise park the delivery (and its worker slot) for as long as the bad value says. Past the budget the wake-up fails with `DurableFlowLeaseContendedException` and the transport redelivers it. This host's own window (`ExecutionLeaseDuration` + `ExecutionLeaseRenewInterval`) is always waited, whatever this is set to. Raise it if a deployment legitimately issues leases longer than an hour. Must be positive. |
| `ProgressPersistenceInterval` | 1 second | Minimum interval between writes caused only by progress reports. Faster updates are coalesced into the next checkpoint (a run's outcome records its own final message); zero writes every report. |
| `MaxRetainedSteps` | 256 (`null` disables) | Maximum distinct checkpoints in one flow ledger. A new step beyond the limit fails terminally before its side effects. Existing checkpoints can still replay. Partition long histories into bounded child flows; raise or disable only after measuring write amplification. |
| `LedgerSizeWarningBytes` | 512 KiB (`null` disables) | Estimated ledger size past which the executor logs a warning naming the flow — once when first crossed, again at each doubling. Every checkpoint rewrites the whole ledger, so persistence cost grows with each completed step until `MaxStateBytes` (or the provider's item cap) refuses a checkpoint — the attempt is then retried until the transport dead-letters it, and a step whose checkpoint was refused re-runs on each redelivery; this is the early signal to keep step results small or partition into child flows. Lower it on DynamoDB (its store's `MaxStateBytes` defaults to 350 000, under DynamoDB's 400 KB item cap). Must be positive. |
| `TimerInProcessThreshold` | 10 seconds | Timer remainders (`flow.DelayAsync`) at or under this wait in process under the execution lease; longer remainders suspend the run behind a delayed wake-up job when the transport supports native delayed delivery. Zero always prefers suspension; on transports without delayed delivery every timer waits in process regardless. See [timers-and-scheduling.md](timers-and-scheduling.md). |
| `MaxInProcessParkDuration` | `null` (derive from the transport) | The longest a durable timer holds ONE worker delivery while it waits in process (no native delayed delivery, or a remainder under `TimerInProcessThreshold`). Some brokers cap how long a delivery may stay in flight however alive its handler is — Google Pub/Sub's `MaxTotalAckExtension`, RabbitMQ's `consumer_timeout`, the SQS 12-hour visibility ceiling — and past it the broker hands the same job to a second consumer while the first is still sleeping. Longer sleeps are therefore waited in hops: park, checkpoint, publish an immediate wake-up, end the delivery; the replay resumes the same timer under a fresh delivery. `null` derives the hop from the transport (half of any ceiling it advertises, never more than the delivery has left of it; when it advertises none, the ~49.7-day timer ceiling); a value can only shorten that hop, or supply one for a transport the library cannot read a ceiling from. Must be positive and at most ~49.7 days. Awaited-response steps are not hopped — see [timers-and-scheduling.md](timers-and-scheduling.md). |
| `MaxStateBytes` | DynamoDB 350 000 · Cosmos 1 900 000 · MongoDB 15 000 000 · `null` (unlimited) elsewhere | Serialized-ledger size budget checked on every create/checkpoint. An oversized write fails with a diagnosable error (flow id, size, limit) instead of the raw provider error — defaults sit under each provider's hard item/document cap; the refused attempt is retried until the transport dead-letters it (see `LedgerSizeWarningBytes`). On Cosmos the budget is enforced on the **complete document** as it is sent (the ledger JSON is embedded as a string and escaped a second time, so a ledger well under the budget can produce a document over Cosmos's 2 MB item cap); the ledger JSON alone is checked first as the cheap pre-check. Keep large payloads in your own storage and pass references (see the ledger-size note in [durable-flows.md](durable-flows.md#child-flows)). |

Configure these on the selected store, for example:

```csharp
.WithPostgreSqlDurableFlows(options =>
{
    options.StateExpiry = TimeSpan.FromDays(14);             // common engine option
    options.ExecutionLeaseDuration = TimeSpan.FromMinutes(1); // common engine option
    options.ConnectionString = connectionString;              // PostgreSQL store option
    options.SchemaName = "public";
})
// Cron-scheduled flows are registered on the same builder: five-field cron (validated here),
// optional time zone, and a deterministic input factory (it must produce the same value on
// every replica for the same occurrence).
.WithScheduledFlow<NightlyReportFlow, ReportInput>(
    "nightly-report", "0 6 * * *",
    occurrence => new ReportInput(occurrence),
    schedule => schedule.TimeZone = TimeZoneInfo.FindSystemTimeZoneById("Europe/Berlin"));
```

### Provider-specific options

| Package | Provider-specific options (in addition to the common options above) |
|---|---|
| `SqlServer` | `ConnectionString`, `SchemaName`, `TableName`, `AutoCreateSchema`, `PruneInterval`, `PruneBudget` |
| `PostgreSQL` | `ConnectionString` or registered `NpgsqlDataSource`, `SchemaName`, `TableName`, `AutoCreateSchema`, `PruneInterval`, `PruneBudget` |
| `MySql` | `ConnectionString`, `TableName`, `AutoCreateSchema`, `PruneInterval`, `PruneBudget` |
| `Sqlite` | `ConnectionString`, `TableName`, `AutoCreateSchema`, `PruneInterval`, `PruneBudget` |
| `Oracle` | `ConnectionString`, `TableName`, `AutoCreateSchema`, `PruneInterval`, `PruneBudget` |
| `MongoDB` | `ConnectionString` or registered `IMongoDatabase`/`IMongoClient`, `DatabaseName`, `CollectionName`, `AutoCreateIndexes` |
| `Cosmos` | `ConnectionString` or registered `CosmosClient`, `DatabaseName`, `ContainerName`, `PartitionKeyPath`, `AutoCreateContainer`, `Throughput` |
| `DynamoDB` | registered/default `IAmazonDynamoDB`, `TableName`, `AutoCreateTable`, `EnableTimeToLive`, `TimeToLiveAttributeName` |
| `EFCore` | application `DbContext` mapping via `ConfigureAsyncResponseDurableFlows(...)`, `PruneInterval`, `PruneBudget`; schema changes are owned by your EF migrations |

The SQL stores prune expired rows opportunistically on flow creation, throttled by `PruneInterval`
(default 5 minutes; zero or negative prunes on every save). Each prune deletes in batches of 1000
rows and keeps going while batches come back full, for at most `PruneBudget` (default 2 seconds;
zero keeps the historical single batch) — the create that triggered it waits, so the budget also
bounds that create's added latency. A prune that fails is skipped until the next interval, never
failing the `StartAsync` it rides on, and the outcome is reported either way: deleted rows,
failures, and a lapsed budget with rows remaining land on the `AsyncResponse` meter
(`asyncresponse.flow_state.pruned_rows` / `prune_failures` / `prune_budget_exhausted`, tagged by
provider) and the store's logger at Warning. MongoDB, Cosmos, and DynamoDB use native TTL instead. All packages register their store as a singleton and reuse a
host-registered client when one exists. See
[durable-flow-state-stores.md](durable-flow-state-stores.md) for
package examples, lifetimes, cleanup mechanics, and schema ownership guidance.

## Channel options

Channel options are common where noted and channel-specific otherwise. They are set through the
channel registration callback (`.WithRedisChannel(options => …)`, `.WithNatsChannel(options => …)`,
`.WithPostgreSqlChannel(options => …)`, `.WithSqlServerChannel(options => …)`,
`.WithMongoDbChannel(options => …)`).

Every channel has a complete registration in [provider-examples.md](provider-examples.md#channel-examples).

| Option | Channels | Default | Purpose |
|---|---|---|---|
| `KeyPrefix` | Redis | `asyncresponse` | Isolate apps/environments sharing one Redis. **Persisted — treat as a deployment contract.** Response pub/sub channels are **key-routed** (`RedisChannel.WithKeyRouting`), so on Redis Cluster `SUBSCRIBE` and `PUBLISH` for one correlation id land on the same node and `PUBLISH`'s subscriber count — the lost-subscriber signal — is meaningful; cluster mode is supported. On Redis Cluster do **not** hash-tag the channel's prefix: its recovery keys are single-key transactions and its channels are spread per correlation id, so a tag (`{app}`) only pins every response channel and recovery key to one slot — one node carrying all of the channel's traffic. (The transport's own `KeyPrefix` is the opposite case: a hash tag there co-locates its derived keys, the idempotent-publish marker included — see the transport options.) |
| `SubjectPrefix` | NATS | `asyncresponse` | Response subjects: `{prefix}.response.{cid}`. A dotted prefix (`my.app`) is fine; a value that would yield an empty subject token — a leading or trailing `.`, or `..` — is rejected at startup (nats-server refuses such subjects with an error the client never surfaces, so the failure previously appeared as silent no-responders at runtime). |
| `RecoveryBucket` | NATS | `asyncresponse-recovery` | JetStream KV bucket for recovery state. **Give every application or environment sharing one NATS system its own bucket as well as its own `SubjectPrefix`:** registrations are keyed by correlation id only, not by the prefix, so deployments sharing a bucket see each other's registrations (each watchdog reports the other's long waits as stale; a correlation id both use can have its registration consumed by the wrong deployment). A non-default `SubjectPrefix` with the default bucket logs a startup warning. An existing bucket is used as it is — never recreated or rewritten — and a warning when the channel first opens it reports a max age shorter than `RecoveryStateExpiry` (registrations collected before they expire) or a replica count different from `RecoveryBucketReplicas`. The watchdog scan also purges the KV delete markers completed waiters leave behind once they are 30 minutes old; with the watchdog disabled they stay until the bucket's max age. |
| `RecoveryBucketReplicas` | NATS | `1` | Replica count the recovery bucket is created with (1–5, validated at startup); use 3 on a clustered JetStream so recovery state survives a node loss. Only applied when the bucket is created. |
| `PresenceProbeTimeout` | NATS | 2 seconds | How long a presence ping waits for a waiter to answer — for the watchdog's liveness probe and for the re-check a publish makes before routing a response to recovery. Only NATS no-responders reads as "no live waiter"; a subscriber that does not answer in time (a waiter busy in a slow `Until` predicate) is reported as unprobeable and never has its registration consumed. |
| `SchemaName` | PostgreSQL, SQL Server | `public` / `dbo` | Schema that contains the channel tables. |
| `ConnectionString` | SQL Server, MongoDB | — | SQL Server: connection string; must point at an existing database (the package creates schema/tables, never the database). MongoDB: optional — the package prefers a host-registered `IMongoDatabase` (or `IMongoClient` + `DatabaseName`); against a single-node replica set include `directConnection=true`. |
| `DatabaseName` | MongoDB | — | Database used when no `IMongoDatabase` is registered. |
| `RecoveryStateTable` / `RecoveryStateCollection` | PostgreSQL, SQL Server, MongoDB | `asyncresponse_recovery_state` | Durable lost-subscriber recovery registrations, one row/document per waiter. MongoDB expires them natively via a TTL index. |
| `MessageTable` / `MessageCollection` | PostgreSQL, SQL Server, MongoDB | `asyncresponse_channel_messages` | Stored response envelopes loaded after `LISTEN/NOTIFY` wakeups (PostgreSQL), by the adaptive polling sweep (SQL Server), or after change-stream wakeups (MongoDB). The sweep's page carries the envelope only for rows nobody has acknowledged; acknowledged history comes back header-only and is fetched by id only when a live subscription still has to receive it. |
| `SubscriberTable` / `SubscriberCollection` | PostgreSQL, SQL Server, MongoDB | `asyncresponse_channel_subscribers` | Live waiter heartbeat rows/documents used for subscriber counts and delivery confirmation. |
| `NotificationChannel` | PostgreSQL | `asyncresponse_channel_notify` | PostgreSQL `LISTEN/NOTIFY` channel; must be a simple identifier. |
| `AutoCreateSchema` / `AutoCreateIndexes` | PostgreSQL, SQL Server, MongoDB | `true` | Create schema/tables/indexes (or TTL + lookup indexes on MongoDB) on first use; set `false` when migrations/provisioning own DDL. With `AutoCreateIndexes = false`, MongoDB runs a one-time read-only check instead and **warns** (never throws) if the TTL or correlation-id lookup index is missing — indexes affect retention/performance, not correctness. |
| `UseOwnershipLedger` | MongoDB | `true` | Claim the effective collections in the reserved `asyncresponse_ownership` collection at first use so two components misconfigured onto one collection fail startup instead of corrupting each other's data. Independent of `AutoCreateIndexes`. Set `false` only for least-privilege deployments that cannot write the ledger collection and audit their collection layout externally. The same option exists on the MongoDB transport and durable-flow options. |
| `UseChangeStreams` | MongoDB | `true` | Wake waiters with a change stream on the message collection (requires a replica set; single-node is sufficient). When disabled — or when the server is standalone, or while a stream is down between re-opens — waiters fall back to `ListenerPollInterval` polling with the full sweep on **every** tick: `FullSweepInterval` is ignored there, because the sweep is then the only cross-process wake. Each (re)opened stream triggers one immediate full sweep. The channel needs MongoDB **4.4+** (its sweep projection uses an aggregation expression in a `find` projection). |
| `MessageRetention` | PostgreSQL, SQL Server, MongoDB | 1 hour | How long response envelope rows/documents remain available for missed notification / cross-process sweep recovery. MongoDB reaps them natively via a TTL index. |
| `DeliveryConfirmationTimeout` | PostgreSQL, SQL Server, MongoDB | 5 seconds | How long a publisher waits for live waiter confirmation before routing to lost-subscriber recovery. Measured on a monotonic clock, so a system clock stepped while a publish waits (NTP correction, VM resume) neither shortens nor stretches it. |
| `DeliveryConfirmationTimeout` | NATS | 5 seconds | How long a publish waits for a subscribed waiter to acknowledge the response. **The opposite outcome from the database channels:** a subscriber that does not acknowledge in time is treated as *delivered* — the publish succeeds and recovery is not consulted. Right for a live waiter whose acknowledgement was slow; but a waiter whose host died or was partitioned without closing its connection keeps its subscription on the server until the server's ping timeout (about four minutes by default), and a response published in that window is lost — the flow notices only when its own wait times out. Only NATS no-responders (nobody subscribed) routes to recovery. |
| `DeliveryConfirmationPollInterval` | PostgreSQL, SQL Server, MongoDB | 50 ms | Poll cadence for cross-process delivery confirmation. |
| `ListenerPollInterval` | PostgreSQL, MongoDB | 250 ms | Missed-notification safety scan interval (and the wake cadence when MongoDB change streams are unavailable). The deadline is absolute — targeted wake-ups (a notification, a local publish, a new waiter) are served in between and never postpone it, so neither this tick nor the `FullSweepInterval` sweep behind it can be starved by traffic. |
| `FullSweepInterval` | PostgreSQL, SQL Server, MongoDB | 5 s (PostgreSQL, MongoDB) / `null` (SQL Server) | Throttle for the full missed-notification sweep while push (`LISTEN/NOTIFY`, change streams) carries normal delivery; `null` sweeps on every poll tick, which SQL Server keeps because polling *is* its delivery. Whenever push is not carrying delivery the throttled sweep would be the only cross-process wake, and its 5 s default equals `DeliveryConfirmationTimeout`: MongoDB polling for good (`UseChangeStreams = false`, a standalone server) sweeps every poll tick, and while push is only down (PostgreSQL: before the first `LISTEN` and from a listen-connection failure until the next successful `LISTEN`; MongoDB: a stream that is down until it re-opens) the sweep runs every `min(FullSweepInterval, DeliveryConfirmationTimeout / 4)` — 1.25 s on defaults — rather than on every tick. Full sweeps dispatch up to 8 correlation ids concurrently, skip the rest of a pass once its first 8 ids all failed transiently (a database outage), and record `asyncresponse.channel.sweep.duration`. |
| `ActivePollInterval` / `IdlePollInterval` | SQL Server | 250 ms / 2 s | Adaptive polling wake (SQL Server has no `LISTEN/NOTIFY`): sweep cadence while waiters are subscribed, and the backed-off cadence while idle. Same-process deliveries never wait for the sweep, and never delay it either: the poll deadline is absolute, so sustained local traffic cannot hold back a response published from another process. |
| `PendingMessageBatchSize` | PostgreSQL, SQL Server, MongoDB | 64 | Keyset-page size. Each correlation gets up to 16 forward pages and one history page per pass, then continues after a poll interval. Forward cursors persist between scans. |
| `HistoryReconciliationInterval` | PostgreSQL, SQL Server, MongoDB | 5 s | Minimum delay after a completed historical pass before starting another. One retained-history page per dispatch pass discovers late commits behind the forward cursor, including already-acknowledged fan-out responses. Large histories add poll intervals; this is not a hard discovery deadline. New subscriptions and failed claims rewind immediately. Measured on the real monotonic clock. Commits landing within a short lookback window (half of `DeliveryConfirmationTimeout`, at most 2 s) behind a fresh cursor are found by the next forward pass without waiting for this. |
| `SubscriberHeartbeatInterval` / `SubscriberHeartbeatTimeout` | PostgreSQL, SQL Server, MongoDB | 10s / 30s | Heartbeat cadence and liveness window. One channel-level loop batches the process's current active registrations per interval; abandoned rows/documents are not renewed. A failed round is retried on a short backoff (at most 1 s) instead of a full interval; the first failure of a run logs a warning, later ones at most one per interval. |
| `RecoveryStateExpiry` | Redis, NATS, PostgreSQL, SQL Server, MongoDB | 7 days | How long durable recovery state survives. Also the default wait timeout backstop. Don't set below your longest flow duration. While `DefaultTimeout` is unset it doubles as the waiter-timeout fallback and is then capped at ~49.7 days (the .NET timer ceiling); with `DefaultTimeout` configured it is a pure persistence TTL and may exceed the ceiling (e.g. 90-day retention), hard-capped at 10 years so "now + expiry" stamps can never overflow date arithmetic. `DefaultTimeout` and `DisposalDrainTimeout` always carry the timer cap, and every waiter's resolved timeout — explicit values included — must be positive and under it, rejected before the waiter registers anything. |
| `DefaultTimeout` | all | `RecoveryStateExpiry` | Default per-waiter timeout when a flow doesn't call `WithTimeout`. |
| `DisposalDrainTimeout` | all | 30 seconds | How long ending a wait — disposing the waiter, or its own timeout firing — drains a delivery already in flight (an `Until` predicate mid-run) before abandoning it. A drained delivery settles the waiter as delivered; a lapsed budget faults it with `AsyncResponseIndeterminateDeliveryException` — never a cancellation (or, on the timeout path, a `TimeoutException`), which would invite re-attaching to a possibly-consumed correlation id. |
| `IncludeRemoteStackTrace` | Redis, NATS, PostgreSQL, SQL Server, MongoDB | `true` | Whether the remote exception's stack trace travels on the wire (`Exception.Data["RemoteStackTrace"]`). See [security.md](security.md). |
| `MaxRemoteStackTraceLength` | Redis, NATS, PostgreSQL, SQL Server, MongoDB | `16384` | Length cap (chars) applied to the remote stack trace on both publish and receive. |

**Clock note for the database channels** (PostgreSQL, SQL Server, MongoDB): stored envelopes and
each waiter's delivery watermark are both stamped on the *database* clock — the publish path
returns the server-stamped `created_at` so even the same-process fast path compares like clocks —
which keeps app-host clock skew out of the delivery decision entirely. The 1 s watermark tolerance
covers the database clock's own granularity across statements, not app↔database skew. App clocks
only stamp non-delivery metadata (e.g. recovery-registration age for the watchdog's staleness
report), where ordinary NTP sync is ample.

**NATS permissions and payload ceiling.** The NATS channel's user needs to publish and subscribe on
`{SubjectPrefix}.response.>` (responses, presence pings), publish and subscribe on the
connection's inbox prefix (`_INBOX.>` by default — replies to its own requests, and the
acknowledgement a waiter publishes to the requester's reply inbox for every response and presence
ping; `allow_responses` also grants that publish), publish on `$KV.{RecoveryBucket}.>`
(registration writes), and use the JetStream API (`$JS.API.…`) for the bucket's stream
`KV_{RecoveryBucket}`: stream info, stream create (unless the bucket is provisioned out of band),
direct get, stream purge (the delete-marker maintenance), and the ephemeral consumer create/delete
behind the watchdog's key listing.

> **Upgrading: grant `$JS.API.STREAM.INFO.KV_{RecoveryBucket}`.** The channel now looks the
> recovery bucket up before it would create it (an existing bucket is opened as it is, never
> re-created), so its user needs stream-info permission on the bucket's stream — even when the
> bucket already exists and even when `STREAM.CREATE` is granted. nats-server drops a request it
> does not permit instead of answering it, so a deployment missing this permission does not get
> an error it can read: the lookup times out, and **every waiter registration fails** after the
> upgrade. Add `$JS.API.STREAM.INFO.KV_{RecoveryBucket}` to the user's publish allow-list before
> rolling out; without `$JS.API.STREAM.PURGE.KV_{RecoveryBucket}`, the watchdog logs a warning on
> every scan and delete markers stay until the bucket's max age.

nats-server refuses a subscription it does not permit with an error the client never surfaces, so
a missing `subscribe` permission on the response subjects looks like "every waiter lost" at runtime — each response takes the lost-subscriber path while its live
waiter runs to its timeout. A message larger than the server's `max_payload` (1 MiB by default)
cannot travel over the channel at all: the channel reports such a response as unprocessable at once
(the ingress then fails the waiter or runs its failure callback instead of retrying), so keep
responses — or the configured `max_payload` — sized accordingly.

## Transport options

Transport options are set through the transport registration callback. Each transport package owns
its own option type; the common shapes are summarized here. See
[Install and run](../README.md#install-and-run) for the local minimum and
[transport examples](provider-examples.md#transport-examples) for every provider.

The in-memory transport is configured directly on registration:

```csharp
.WithInMemoryTransport(options =>
{
    options.QueueCapacity = 1_024;          // default; PublishAsync waits when full
    options.WorkerCount = 1;                // default; increase for independent parallel jobs
    options.InJobOverflowCapacity = 4_096;  // default; follow-up publishes held past QueueCapacity, then rejected
    options.DelayedJobCapacity = 4_096;     // default; delayed jobs held at once — external publishers wait, in-job ones are rejected
})
```

`QueueCapacity` is backpressure on *external* producers only. A publish made from **inside** a
running job — a durable flow starting a child, or a child waking its parent — never waits for
capacity: the workers are the only consumers, so a worker parking on a full queue would be waiting
on itself (with the default `WorkerCount = 1`, permanently). Follow-up work is a continuation of a
job the queue already admitted, so it is accepted past the bound into an **in-job overflow**, and a
worker that finishes a job runs whatever is waiting there at that moment before it takes anything
new off the queue — what those follow-ups publish in turn waits for the next round, so a chain of
follow-ups (a job re-enqueueing itself, a sequential child orchestration) cannot starve the jobs
already queued; it still counts toward the shutdown drain, so nothing is lost at exit. (Draining the
overflow only *through* the queue starved it: a bounded channel hands a slot freed by a read
straight to a producer already parked in `PublishAsync`, so under sustained external load the
overflow never drained at all and follow-ups — a child flow's start, a parent's wake-up — were
eventually rejected at the bound. External producers stay parked while follow-ups run, which is
the backpressure the capacity exists to apply.) The overflow is bounded by `InJobOverflowCapacity` (default 4096; `0` allows none;
negative is rejected at startup): past it a follow-up publish throws `InvalidOperationException` —
the publishing job fails and is redelivered by the retry ladder below, so make in-job publishes
idempotent. Every held job retains its materialized envelope and captured execution context, and an
unbounded overflow let a runaway fan-out exhaust memory with the configured queue capacity giving
no signal. The current depth is the `asyncresponse.worker.inmemory_overflow_depth` gauge;
rejections count on `asyncresponse.worker.inmemory_overflow_rejections`.

**Delayed jobs** — `EnqueueWorkerAsync(..., delay)` and the wake-ups behind suspended flow timers
— are neither queued nor overflow while they wait on their due time, so neither bound covered
them: every scheduled publish retained its envelope and captured execution context against no
limit, and a burst's timers firing at once started that many channel writes pending outside the
bounded queue. `DelayedJobCapacity` (default 4096; must be positive) bounds the jobs held at once
— waiting on their timer, or fired and waiting for queue room. At the bound a delayed publish from
outside a job waits (honoring its cancellation token) until a scheduled job enters the queue; one
made from inside a running job — a flow parking on a timer — never waits, for the same reason as
the overflow, and throws `InvalidOperationException` instead: the publishing job fails and is
redelivered, so make it idempotent. Size it above the number of flows you expect to be sleeping
at once on this transport. The current count is the `asyncresponse.worker.inmemory_delayed_jobs`
gauge; in-job rejections count on `asyncresponse.worker.inmemory_delayed_rejections`.

Failed jobs retry with backoff (`RetryBaseDelay` 100 ms → `RetryMaxDelay` 5 s) up to
`MaxDeliveryAttempts` (default 5; `0` = unlimited). Retries keep running during the shutdown drain,
each backoff capped at `RetryBaseDelay` so a failing job cannot park a worker behind it; a job
with unlimited attempts that fails during the drain is dropped (logged) so the jobs queued behind
it still drain. A job interrupted *by* the stop — a durable flow handing its delivery back with
`DurableFlowInterruptedException` — is not treated as a failure: it is abandoned without a retry
or a `dropped` outcome (the interruption records no `failed` outcome either), with one warning
naming the explicit `ResumeAsync` its flow needs after the restart — this queue cannot redeliver
it. Any other cancellation a job throws (its own `HttpClient` timeout, say) is an ordinary failure
and is retried, during the drain too.

| Option | Transports | Purpose |
|---|---|---|
| `KeyPrefix` / `SubjectPrefix` / `SchemaName` / `TopicPrefix` | Redis / NATS / PostgreSQL / SQL Server / Kafka | Namespace for worker and response streams/subjects/tables/topics. On Redis Cluster give the Redis transport a hash-tagged prefix (`{app}`) so every derived key — the idempotent-publish marker included — shares one slot; a prefix or stream name whose braces do not form one well-formed tag is rejected at startup. NATS additionally caps every resolved subject and JetStream stream/consumer name at 255 characters, validated at startup before any subscriber connects, and rejects a prefix or explicit subject that would yield an empty token (a leading/trailing `.` or `..`) — a dotted prefix such as `my.app` remains valid. |
| `StreamReplicas` | NATS | Replica count (JetStream `num_replicas`, 1–5, validated at startup) for the worker, response and dead-letter streams the transport creates; use an odd count (3 or 5) on a clustered JetStream. Only applied when a stream is created — an existing stream keeps its own. Default `1`. |
| `ConnectionString` | Azure Service Bus | Service Bus namespace connection string. Omit when you register your own singleton `ServiceBusClient`. |
| `ServiceUrl` / `Region` / `AccessKey` / `SecretKey` | SQS | Endpoint and credentials. All optional: omit everything to use the AWS SDK default chain, set `ServiceUrl` for LocalStack or a proxy, or register your own singleton `Amazon.SQS.IAmazonSQS` (e.g. via `AWSSDK.Extensions.NETCore.Setup`) and the package reuses it. |
| `ConnectionString` | SQL Server | SQL Server connection string; must point at an existing database (the package creates schema/table/indexes, never the database). |
| `BootstrapServers` | Kafka | Comma-separated broker list. The package speaks the Kafka protocol via `Confluent.Kafka`, so Redpanda, Amazon MSK, WarpStream, Aiven, and Confluent Cloud all work. |
| `WorkerTopic` / `ResponseTopic` + `WorkerConsumerGroup` / `ResponseConsumerGroup` | Kafka | Topics for worker jobs and response ingress (default `{TopicPrefix}.transport.worker` / `.response`) and the consumer group per role. The groups default to the fixed `asyncresponse-workers` / `asyncresponse-responses` — they are **not** derived from `TopicPrefix` — so deployments sharing a cluster need their own group names too: otherwise every member change in one deployment (a deploy, a scale event, a crash) rebalances the consumers of all of them. |
| `CreateTopics` / `TopicNumPartitions` / `TopicReplicationFactor` | Kafka | Provision missing topics on subscriber startup. Partitions are the unit of consumer parallelism and ordering; `-1` uses broker defaults. |
| `OffsetCommitInterval` | Kafka | Auto-commit cadence for offsets stored after each resolved message; a crash inside the window redelivers at-least-once. |
| `BackpressurePollDelay` | Kafka | The short poll slice used while the poll thread waits on in-process work: capacity re-checks while consumption is paused under a full `AckAfterEnqueue` queue, and completion checks while `AckAfterHandlerCompletes` handlers run detached (a finished handler's offset is stored and its partition resumed within one slice). Default 50 ms. |
| `MaxPollInterval` | Kafka | Maximum gap between consumer polls before the broker evicts the consumer from its group and rebalances its partitions (the librdkafka `max.poll.interval.ms`); default 5 minutes. The poll thread's longest gap is `DetachHandlerAfter` plus `PollTimeout`, and startup validation requires that sum to fit within half of it. Handler execution time and the in-process retry ladder no longer count: a handler that outlives `DetachHandlerAfter` runs detached while polling continues. Under the classic group protocol it must also be at least the consumer's `session.timeout.ms` (librdkafka default 45 s) — librdkafka refuses to build such a consumer — so startup validation checks the resolved consumer configuration (after `ConfigureConsumer`): lower `SessionTimeoutMs` there to use a shorter interval. |
| `DetachHandlerAfter` | Kafka | In `AckAfterHandlerCompletes` mode, how long the poll thread waits for a message's handler inline before detaching it. Within the budget a fast handler settles as before (offset stored, next message consumed, no pause). Past it the message's partition is paused — its order holds with nothing buffered in-process — the handler and its retry ladder continue on the thread pool, and the poll thread keeps polling: the consumer's other partitions keep flowing, `MaxPollInterval` is honored, rebalance callbacks fire. The poll thread stores the offset and resumes the partition once the handler settles (checked every `BackpressurePollDelay`); a stop waits for detached handlers still in their partition's current assignment so their offsets are committed (one whose partition a rebalance revoked since it detached and did not hand straight back is not waited for, and its offset is never stored). This is what lets a durable-flow step await a remote response or sleep on a timer for minutes without the consumer being evicted from its group. Default 1 s; `0` detaches every handler at once. |
| `FaultDrainTimeout` | Kafka | In `AckAfterHandlerCompletes` mode, how long a subscriber whose poll loop *failed* (a consume error, a dropped broker connection, a burial that failed for good) waits for its detached handlers before it closes the consumer and the supervisor rebuilds it. Handlers that settle within the budget get their offsets stored and committed by the close, as after a stop; the rest are abandoned — offsets unstored, messages redelivered on the rebuilt consumer (possibly while the abandoned handler still runs: handlers are at-least-once), retry ladders stopped, outcomes logged. Without it the teardown waited for every detached handler with no limit, so a transient broker failure disabled the subscriber for as long as an unrelated long handler took and the reconnect policy (`SubscriberRetryBaseDelay` → `SubscriberRetryMaxDelay`) never ran. A graceful stop is bounded by the host's shutdown budget instead. Default 5 s; `0` abandons at once. |
| `DeadLetterTopic` / `DeadLetterTopicSuffix` | Kafka | Explicit dead-letter topic, or the suffix appended per source topic (default `.deadletter` → `{topic}.deadletter`). |
| `ConfigureProducer` / `ConfigureConsumer` / `ConfigureAdminClient` | Kafka | Last-chance hooks over the Confluent client configs (security, compression, fetch tuning, …). |
| `WorkerQueue` / `ResponseQueue` | Azure Service Bus | Service Bus queues used for worker jobs and response ingress; they must be distinct — compared case-insensitively, since Service Bus entity names are (`Jobs` and `jobs` are one queue). |
| `WorkerQueue` / `ResponseQueue` | SQS | Queues for worker jobs and response ingress (distinct); each accepts a queue name (resolved once via `GetQueueUrl`) or a full queue URL. The distinctness check compares two names, or two URLs (normalized), exactly; a name and a URL sharing the queue name may be another account's queue, so that pair only warns at startup (configure both as URLs to make it exact). A name/URL ending in `.fifo` opts into FIFO publishing: the correlation id becomes the `MessageGroupId` (jobs sharing a correlation id stay ordered) and every message carries a unique `MessageDeduplicationId`. Every job **without** a correlation id — durable-flow start, resume and wake-up jobs among them, unless the flow was started inside a request scope — shares the single `FifoMessageGroupIdFallback` group, which SQS delivers strictly one at a time across all consumers; with FIFO's in-process timers one parked flow then holds every other flow's jobs (the worker subscriber warns at startup). Prefer a standard worker queue for durable flows. |
| `CreateQueues` / `DeadLetterQueueSuffix` / `MaxReceiveCount` | SQS | Provision the queues on startup, each with a native dead-letter queue (`{queue}-dlq`) wired through a redrive policy: SQS counts receives (`ApproximateReceiveCount`) and moves a message to the DLQ after `MaxReceiveCount`. Off by default — point at existing queues in production. Converging an existing `.fifo` queue re-applies only the mutable attributes: the create-only `FifoQueue` attribute is skipped, since `SetQueueAttributes` rejects it (previously host startup failed once the provisioning retries were exhausted). Startup validates every name it would create — derived dead-letter names included — against the SQS rule (80 characters of letters, digits, `-`, `_`, `.fifo` counted), and provisioning retries only transient failures (credentials or the instance-metadata endpoint not ready yet and the SDK's client-side timeout count as transient), so a deterministic rejection fails startup at once. |
| `WorkerSubscriber.VisibilityTimeout` / `RedeliveryDelay` | SQS | Per-receive visibility timeout (`null` uses the queue's setting) and the optional shortened invisibility applied via `ChangeMessageVisibility` when a handler fails; `null` lets the visibility timeout expire naturally. With renewal off, set `VisibilityTimeout` to the queue's value when durable flows run on SQS: unset, the transport cannot know the queue's timeout and advertises the 12-hour SQS maximum to the engine as its in-flight ceiling (the worker subscriber warns at startup). |
| `MessageTable` / `MessageCollection` | PostgreSQL, SQL Server, MongoDB | Single queue table/collection containing worker, response-ingress, and dead-letter rows/documents. |
| `ConnectionString` / `DatabaseName` | MongoDB | Optional — the package prefers a host-registered `IMongoDatabase` (or `IMongoClient` + `DatabaseName`). Against a single-node replica set include `directConnection=true`. |
| `WorkerQueue` / `ResponseQueue` / `DeadLetterQueue` | PostgreSQL, SQL Server, MongoDB | Logical queue names stored in the queue table/collection. They must be distinct. |
| `NotificationChannel` | PostgreSQL | `LISTEN/NOTIFY` channel that wakes PostgreSQL subscribers after publishes (a NAK sends no wake: the released row is not due yet). A publish notifies with the queue name as the payload, and each subscriber wakes only for its own queue; a foreign producer (reply target) should notify with the queue name, or with an empty payload, which wakes every subscriber. SQL Server has no equivalent: same-process publishes wake subscribers through an in-process signal, and cross-process rows are picked up within `EmptyPollDelay`. |
| `UseChangeStreamWake` | MongoDB | Wake idle subscribers with a change stream on the queue collection (requires a replica set). When disabled — or when the server is standalone — subscribers fall back to `EmptyPollDelay` polling. |
| `LockTimeout` | PostgreSQL, SQL Server, MongoDB | How long a claimed row/document stays locked (leased) before another subscriber may retry it. While a handler runs, the subscriber renews the claim automatically every third of this (fenced by `lock_id`; a failed renew retries on a short backoff, and each attempt is bounded by that third so a hung connection cannot outlive the lease), so one slow handler is not redelivered mid-execution — including while the host is stopping and the handler is still running. |
| `WorkerSubscriber.LockRenewalInterval` | Azure Service Bus | Peek-lock renewal cadence for the message whose handler is running (default 10 s; `null` disables; must be under the 5-minute `LockDuration` maximum). Keeps slow handlers from losing their lock mid-run, and keeps renewing past the host stop until the handler returns — see the lock-budget note below. |
| `WorkerSubscriber.PrefetchCount` | Azure Service Bus | Messages the receiver buffers locally (default 0). Buffered messages are locked but never renewed, so in `AckAfterHandlerCompletes` keep `PrefetchCount × handler latency` well under the queue's `LockDuration` or leave it at 0; startup warns when it is positive in that mode. |
| `WorkerSubscriber.VisibilityRenewalInterval` | SQS | Opt-in visibility heartbeat for the message whose handler is running (default `null` = off; requires `VisibilityTimeout` set and a shorter interval). It keeps renewing past the host stop until the handler returns, and clamps its last extension to the 12-hour SQS in-flight maximum, then logs one warning and stops — past it SQS redelivers regardless. Off by default because extending visibility overrides queue-tuned redrive timing, and on FIFO queues an extended message keeps its whole message group blocked if the consumer wedges. |
| `MaxMessagesPerReceive` / `ReceiveWaitTime` | Azure Service Bus, SQS | Receive-loop batch size (the worker subscriber in `AckAfterHandlerCompletes` receives one message at a time instead, since the broker locks and counts every message a receive hands over whether or not its handler starts; early ACK and the response subscriber keep the batch) and long-poll timeout for queue subscribers. SQS caps them at 10 messages and 20 seconds (the defaults); an SQS `ReceiveWaitTime` of 0 (short polling) backs off between empty receives. |
| `WorkerSubscriber.UseAckAfterEnqueue(...)` | all broker transports | Opt-in early-ACK dispatch for long-running workers: bounded in-process queue, configurable worker count, capacity, and drain timeout. Every broker transport exposes the same method name; the message is ACKed once it is accepted into the bounded in-process queue, before the handler runs. Durable-flow wake-ups ride this queue and lose broker redelivery under early ACK, so startup throws unless `DurableFlowOptions.AllowEarlyAckWorkerSubscriber = true` explicitly accepts the risk (see [transport-semantics.md](transport-semantics.md)). |
| `WorkerSubscriber.BackgroundDrainTimeout` | all broker transports | Bounded wait (default 20 s; `UseAckAfterEnqueue`'s optional third argument) for queued and running background handlers while the subscriber stops — a wait, not a cancellation. On PostgreSQL, SQL Server, MongoDB, NATS, RabbitMQ, and Kafka it is **split**: three quarters for the handlers, one quarter reserved for dead-lettering (and reporting via `OnBackgroundFailure`) the already-ACKed entries still queued when that lapses, so they are recorded instead of lost at process exit (NATS, RabbitMQ, and Kafka route them from the stop itself, on the still-open connection, channel or producer, even when every worker is still busy). SQS, Azure Service Bus and Google Pub/Sub split it the same way and report every entry still queued through `OnBackgroundFailure` in the reserve (no dead-letter write is possible for a deleted/completed/ACKed message), with an Error log counting them. On RabbitMQ in `AckAfterHandlerCompletes` mode it also bounds how long a stopping subscriber waits for the handler still running in its delivery callback, so that job's ACK lands before the channel closes instead of the close requeueing it for another consumer — shortened to what `HostShutdownTimeout` leaves after the two `ShutdownTimeout` spends, never validated (a stop that cannot wait only costs a redelivery; when nothing is left the subscriber says so once at startup and stops without waiting). Google Pub/Sub bounds its running-handler drain the same way in `AckAfterHandlerCompletes` before it stops the client — at most `BackgroundDrainTimeout`, shortened to what `HostShutdownTimeout` still leaves after `ShutdownTimeout`, measured from `ApplicationStopping` — without a startup notice; unless its response arrives, a handler parked on an awaited flow response costs the stop that whole bound. |
| `WorkerSubscriber.MaxDeliveryAttempts` | all broker transports except Google Pub/Sub and SQS | Redeliveries before dead-lettering. Google Pub/Sub and SQS perform redelivery natively — bound attempts with the subscription's `DeadLetterPolicy` (Pub/Sub) or the queue's redrive policy `maxReceiveCount` (SQS, provisioned by `CreateQueues` or your infra). On RabbitMQ, values above 2 require a TTL-retry dead-letter cycle (plain `basic.nack` requeues are not counted by the broker) and log a startup warning otherwise; the cap is judged before the handler runs, and a message that reaches it after riding that cycle (`x-death` present) is parked in `ParkQueue` — or `DeadLetterQueue` when no `ParkQueue` is set, or ACKed and dropped when neither is — so the cycle terminates; a park that fails (with publisher confirms: the queue is missing, a `reject-publish` queue is full) is NACKed with requeue after the `SubscriberRetryBaseDelay`/`SubscriberRetryMaxDelay` backoff, so the park is retried. A reject before the handler ran is logged (Error when no `DeadLetterExchange` is configured, since the broker then drops it). A worker cap of `1` with durable flows registered logs a startup warning: a delivery the broker requeues on its own — a flow handed back at host stop, a channel closed under a running handler — comes back `redelivered` as attempt 2 and is rejected unrun. Negative values fail startup. On RabbitMQ 4.x quorum queues the broker also applies its own `delivery-limit` (20 by default) whatever this option says, so `0` is not "forever" there: configure a dead-letter exchange, or raise/disable `delivery-limit` by policy. On Kafka, attempts are in-process retries with backoff (`HandlerRetryBaseDelay`/`HandlerRetryMaxDelay`) counted per process delivery — offsets cannot NACK a single message; `0` is unlimited retries under `AckAfterHandlerCompletes` but a **single** attempt under `AckAfterEnqueue` (the offset is already committed; the message is dead-lettered and surfaced via `OnBackgroundFailure`). |
| `SubscriberRetryBaseDelay` / `SubscriberRetryMaxDelay` | all broker transports | Bounded backoff for restarting a failed hosted subscriber (streaming-pull/long-poll/consumer fault, transient auth/startup errors). On RabbitMQ, `NetworkRecoveryInterval` no longer paces subscriber restarts — these options govern that backoff instead. |
| `WorkerSubscriber.OnBackgroundFailure` | all broker transports | Hook for operator-visible metrics, alerting, or a durable dead-letter path when a background handler fails after early ACK. |
| `HostShutdownTimeout` | all broker transports | Must accommodate the transport's shutdown spend: `BackgroundDrainTimeout` (default 20 s), plus `ShutdownTimeout` (default 5 s) on the transports that bound a close/listen join with it (Azure Service Bus, RabbitMQ, Google Pub/Sub, PostgreSQL, MongoDB, SQS) — RabbitMQ counts `ShutdownTimeout` **twice** (consumer cancel, then the channel/connection close after the drain; its `AckAfterHandlerCompletes` in-flight wait is shortened to fit instead of validated), and Azure Service Bus is validated in `AckAfterHandlerCompletes` too: `2 × ShutdownTimeout` when `LockRenewalInterval` is set (lock-renewal join, then receiver close), `1 ×` when it is `null` (with no renewal join to overlap, handing a batch's unstarted messages back and the receiver close share it — in early ACK too). Stock defaults fit the .NET host's 30 s default for one early-ACK subscriber; on NATS, PostgreSQL, SQL Server, and MongoDB, enabling early ACK on **both** `WorkerSubscriber` and `ResponseSubscriber` sums both roles' spends (the host stops hosted services one after another by default; any hosted service registered after them also stops first and spends from the same budget, which startup validation cannot see), so raise `HostOptions.ShutdownTimeout` — or shorten the drains — for that combination. Mirror any custom `HostOptions.ShutdownTimeout` here so startup validation checks the real budget. SQS's `ShutdownTimeout` bounds its visibility-renewal task join after each batch (including the final one while the subscriber stops) rather than a one-time close. Google Pub/Sub validates `ShutdownTimeout` in `AckAfterHandlerCompletes` too: on stop it first waits for the handlers already running — for at most `BackgroundDrainTimeout`, shortened to what the host budget still leaves after `ShutdownTimeout` (measured from `ApplicationStopping`, so a subscriber stopped after others does not assume the whole budget) — then stops the client within `ShutdownTimeout`. |
| `DeclareTopology` | RabbitMQ | Declare durable exchanges/queues/bindings (`true`) or leave topology to your infra team (`false`). |
| `DeadLetterExchange` / `DeadLetterQueue` / `DeadLetterRoutingKey` / `ParkQueue` | RabbitMQ | The dead-letter exchange the worker and response queues are declared with (`x-dead-letter-exchange`), the queue bound to it, and the routing key (blank: dead-lettered messages keep their original key). `ParkQueue` receives only what the delivery cap parks — declared unbound, so it never collects the hops of a TTL-retry cycle, unlike `DeadLetterQueue` — and wins over `DeadLetterQueue` as the park destination. The subscriber channel requests publisher confirms whenever `DeadLetterExchange`, `DeadLetterQueue` or `ParkQueue` is set (it then publishes dead-letter copies or parks; `DeadLetterRoutingKey` alone publishes nothing), so a park or copy the broker cannot route fails loudly instead of being logged as done. In `AckAfterEnqueue` a failed job's copy goes to the dead-letter exchange; a copy that comes back through it (a TTL-retry queue bound to the exchange) and fails again is parked instead of copied into the cycle a second time — or dropped with an error when neither `ParkQueue` nor `DeadLetterQueue` is set. A durable flow handed back at host stop in `AckAfterEnqueue` is logged at Warning, reported through `OnBackgroundFailure` and copied to the dead-letter exchange with an `AR-DeadLetter-Reason` starting `handed_back_after_commit:` (safe to replay: the run resumes from its checkpoint); without a `DeadLetterExchange` no copy is written and the hand-back is logged at Error — the wake-up is lost unless `OnBackgroundFailure` records it. |
| `BrokerConsumerTimeout` | RabbitMQ | Mirror of the broker's `consumer_timeout` (default 30 minutes, the broker's default): past it RabbitMQ closes the channel and requeues every unacknowledged delivery. Keep it equal to (or below) the broker's setting; `null` only when the broker's timeout is disabled; must be positive. In `AckAfterHandlerCompletes` mode the worker transport advertises it divided by `WorkerSubscriber.PrefetchCount` as its in-flight ceiling — the broker starts the clock when it sends a delivery, and prefetched deliveries age while they wait their turn — so durable-flow timers wait in process for at most half of that per delivery (see [timers-and-scheduling.md](timers-and-scheduling.md)). The advertised value is never less than one minute (nor more than the timeout itself, when shorter), so a large prefetch cannot shrink timer hops to seconds; `PrefetchCount = 0` (AMQP "unlimited", rejected by the subscriber) advertises that floor. With durable flows registered, a share below the floor — `PrefetchCount` above 30 at the default timeout — logs a startup warning, since the last buffered delivery can then outlive `consumer_timeout`. Set `WorkerSubscriber.PrefetchCount = 1` for workers whose timers should use the whole timeout. |
| `CorrelationIdAttribute` / `CorrelationIdHeader` / `CorrelationIdProperty` | Pub/Sub / SQS / RabbitMQ / Kafka / NATS / PostgreSQL / SQL Server / MongoDB / Azure Service Bus | Broker metadata key used to resolve the correlation id before falling back to JSON body paths. On Kafka the correlation id also becomes the message key, keeping jobs that share a correlation id ordered within a partition; on FIFO SQS queues it becomes the `MessageGroupId` with the same effect. Durable-flow jobs carry no correlation id unless the flow was started inside a request scope, so this orders them per caller, not per flow — and on FIFO SQS every uncorrelated job shares one `FifoMessageGroupIdFallback` group. |
| `CorrelationIdJsonPaths` | broker transports | JSON paths inspected when metadata does not carry the correlation id. Every transport also unwraps nested JSON strings at those paths (a JSON-string-encoded object or array is parsed and descended into). |
| `DeadLetterEnabled` / `DeadLetterRetention` | Redis / NATS / PostgreSQL / SQL Server / MongoDB / Kafka | Whether poison messages are preserved. `DeadLetterRetention` exists only on PostgreSQL, SQL Server, and MongoDB (row/document retention, pruned after a publish at most once a minute per process — PostgreSQL and SQL Server in bounded 1,000-row batches drained for up to 2 s, MongoDB in one `deleteMany`; a prune failure never fails the publish); Redis and NATS bound their dead-letter streams via `DeadLetterStreamMaxLength` / `DeadLetterStreamMaxMessages` instead, and Kafka's `.deadletter` topic uses broker retention. See [transport-semantics.md](transport-semantics.md) for the full per-transport dead-letter matrix. |

A worker handler failure propagates out of the ingress to the transport dispatcher, which owns the
retry decision: in `AckAfterHandlerCompletes` the delivery is NACKed/abandoned and redelivered up
to `MaxDeliveryAttempts`, then dead-lettered; after an early ACK the failure is reported through
`OnBackgroundFailure` (and written to the transport's own dead-letter queue where one exists). A
failing worker never completes the waiter by itself — publish a failure response from the worker's
error handling when the flow should fail fast instead of waiting out its timeout.

Azure Service Bus uses peek-lock settlement. In `AckAfterHandlerCompletes`, a successful handler
completes the message, failures abandon it until `MaxDeliveryAttempts`, then dead-letter it through
Service Bus. In `AckAfterEnqueue`, the message is completed as soon as it enters the bounded
background queue; later handler failures cannot be broker-dead-lettered because the lock is gone, so
use `OnBackgroundFailure` for metrics, alerts, or a custom durable failure path. Mind the peek-lock
budget: in `AckAfterHandlerCompletes` the worker subscriber receives one message at a time, and by default
renews the peek-lock of the message in the handler every `WorkerSubscriber.LockRenewalInterval`
(10 s), so slow handlers no longer hit `MessageLockLostException` redeliveries of already-processed
messages; if you disable renewal (`LockRenewalInterval = null`), keep the handler latency well under
the queue's lock duration (60 s by default, 5 minutes at most) — the worker transport then
advertises the 5-minute maximum to the durable-flow engine as its in-flight ceiling. Keep
`PrefetchCount` at 0 unless handlers are fast: prefetched messages are locked but never renewed.

AWS SQS uses visibility-timeout settlement with long-poll `ReceiveMessage` (up to 10 messages and
20 seconds per call). In `AckAfterHandlerCompletes`, a successful handler deletes the message;
failures leave it invisible until the visibility timeout expires (or shorten the wait with
`RedeliveryDelay` via `ChangeMessageVisibility`), SQS redelivers it with an incremented
`ApproximateReceiveCount`, and the queue's redrive policy dead-letters it after `maxReceiveCount`
receives — redelivery accounting and the DLQ are fully native, so there is no app-level
`MaxDeliveryAttempts`. In `AckAfterEnqueue`, the message is deleted as soon as it enters the bounded
background queue; later handler failures cannot be redelivered because the message is gone, so use
`OnBackgroundFailure` for metrics, alerts, or a custom durable failure path. Mind the visibility
budget the same way as the Service Bus lock budget: in `AckAfterHandlerCompletes` the worker subscriber
receives one message at a time, so keep the handler latency under the queue's visibility timeout (or
set `WorkerSubscriber.VisibilityTimeout` higher) to avoid duplicate executions of already-processed
messages — or opt into the `WorkerSubscriber.VisibilityRenewalInterval` heartbeat, which re-extends
the running message's invisibility up to the 12-hour SQS maximum (see the option table for why it is
off by default). FIFO queues are opt-in by naming the queue `*.fifo`; prefer a standard worker queue
for durable flows (see the `WorkerQueue` row).

Kafka is built on classic consumer groups with manual offset management (`enable.auto.commit=true` +
`enable.auto.offset.store=false`; an offset is stored only once its message is fully resolved). Two
consequences to plan for: ordering is per-partition, so consumer parallelism equals the partition
count — size `TopicNumPartitions` accordingly — and a slow or retrying message delays its partition
(head-of-line blocking) and nothing else: a handler still running after `DetachHandlerAfter`
(default 1 s) is detached — its partition paused, the handler and its retry ladder running on, the
poll thread polling — so neither a long handler nor a long retry ladder can overrun `MaxPollInterval`
(default 5 minutes) and get the consumer evicted. Only `DetachHandlerAfter + PollTimeout` must fit
within half of it, which startup validation enforces (see the option table above).
In `AckAfterEnqueue`, the offset is stored at enqueue time and partition fetching pauses while the
bounded in-process queue is full; later handler failures are retried in-process, then reported via
`OnBackgroundFailure` and produced to the dead-letter topic with failure-detail headers. The message
that exhausts its attempts is always dead-lettered *and committed* so the partition keeps moving.
The early-ACK queue belongs to the hosted subscriber, not to one consumer: a poll-loop fault rebuilds
the consumer while the queued, already-committed work keeps running, and only a host stop drains it.

Redis Streams uses consumer groups. Each subscriber reads new entries with `XREADGROUP`; an entry
stays in the group's pending-entries list until the handler acknowledges it with `XACK`. A separate
reclaim loop takes over entries idle longer than `PendingMessageMinIdleTime` (`XPENDING` + `XCLAIM`),
so a crashed consumer's in-flight work is retried by a peer rather than stranded. `MaxDeliveryAttempts`
is counted from the stream delivery count; on exhaustion the entry is written to the dead-letter stream
and ACKed off the source. Because Redis counts a delivery when it hands an entry over, the default
`AckAfterHandlerCompletes` reads one entry at a time and claims each reclaim candidate right before it
runs (`BatchSize` and batch claims apply to `AckAfterEnqueue`). `AckAfterEnqueue` ACKs at enqueue time
and reports later handler failures through `OnBackgroundFailure`. Streams are trimmed with plain
`XADD … MAXLEN ~ N` (no Redis 8 trim-mode token), which keeps the transport portable across
Redis-compatible servers (see below) — but trimming is by length alone and **deletes unread and
pending jobs** once the worker stream exceeds `StreamMaxLength`; size it above the deepest backlog an
outage can build (see [transport semantics](transport-semantics.md#redis)).

RabbitMQ uses publisher confirms and mandatory routing on publish, and per-message `basic.ack` /
`basic.nack` on consume, with a dead-letter exchange for poison messages. Because the broker does not
count plain `basic.nack`-requeues, a `WorkerSubscriber.MaxDeliveryAttempts` above 2 requires a
TTL-retry dead-letter cycle (declared when `DeclareTopology` is on); values above 2 without that cycle
log a startup warning. `AckAfterEnqueue` ACKs after the bounded enqueue and routes later failures to
`OnBackgroundFailure`. The early-ACK queue belongs to the hosted subscriber, not to one connection:
a channel fault or broker restart rebuilds the channel while the queued, already-ACKed work keeps
running, and only a host stop drains it. A correlation id longer than an AMQP short string
(255 UTF-8 bytes) travels in the `CorrelationIdHeader` header only, not in the native
`correlation-id` property.

Google Pub/Sub uses streaming pull with the client library extending the ack deadline while a handler
runs. Redelivery and dead-lettering are **native**, like SQS: bound them with the subscription's
`DeadLetterPolicy` and `maxDeliveryAttempts` rather than an app-level `MaxDeliveryAttempts`. A failed
handler NACKs the message for Pub/Sub to redeliver. In `AckAfterEnqueue` the client's flow control
is bounded to the background queue capacity (`MaxOutstandingElementCount`), and a full queue parks
the delivery callback until capacity frees instead of NACKing — so backpressure cannot burn a
subscription `DeadLetterPolicy`'s delivery attempts; `AckAfterEnqueue` ACKs after enqueue and reports
later failures through `OnBackgroundFailure`.

NATS JetStream uses explicit acknowledgement. A successful handler `ACK`s; a failure `NAK`s with a
delay so JetStream redelivers after a backoff, and a message that reaches `MaxDeliveryAttempts` is
written to the dead-letter stream. Consumers are durable, so a restarted subscriber resumes from its
last acknowledged position. `AckAfterEnqueue` ACKs as soon as the message enters the bounded queue;
when that queue is full the consume loop pauses until capacity frees (a NAK is sent only on
shutdown/cancellation, so backpressure does not churn redeliveries).

The PostgreSQL and SQL Server transports are table-backed queues: a publish is an idempotent
`INSERT`, and each subscriber claims up to a batch of rows, one atomic single-row claim at a time — PostgreSQL with
`FOR UPDATE SKIP LOCKED`, SQL Server with `UPDLOCK, ROWLOCK, READPAST` (the same skip-locked effect) —
so competing subscribers never claim the same row. PostgreSQL wakes subscribers with `LISTEN/NOTIFY`;
SQL Server has no equivalent, so it wakes same-process subscribers through an in-process signal and
picks up cross-process rows within its adaptive poll interval. A claimed row stays locked for
`LockTimeout` before another subscriber may retry it, and a row that reaches `MaxDeliveryAttempts` is
moved to the dead-letter queue (rows in the same table). See [postgresql.md](postgresql.md) for the
PostgreSQL table layout, delivery-confirmation details, and connection-string tuning, and
[sqlserver.md](sqlserver.md) for the SQL Server pair (adaptive polling wake, `UPDLOCK/READPAST`
claims, application-lock DDL, and operational notes).

MongoDB is the document analogue of that design: a publish is an idempotent insert (a retried
publish with the same id collides on `_id` and is treated as success), and competing subscribers
claim documents atomically with `findOneAndUpdate` — the claim increments the attempt count and
stamps a `lock_id` fence plus a `locked_until` lease evaluated against the server clock (`$$NOW`),
so publisher/consumer clock skew never fences messages in or out. The claim and the dead-letter
prune pin the simple (binary) collation, so a collection created with a case- or accent-folding
default collation cannot let one subscriber claim another logical queue's documents — at the cost
that an index built under a folding collation cannot serve the claim, which then scans (a
collection with the default collation is unaffected). Acks and NAKs are fenced by the
`lock_id`, and a document that reaches `MaxDeliveryAttempts` is moved to the dead-letter queue
(documents in the same collection) under an id derived deterministically from the source message,
so a crash between the dead-letter insert and the original delete cannot duplicate the DLQ entry.
Subscribers are woken by a change stream on the queue collection when the server is a replica set
(single-node is sufficient — the same requirement the MongoDB channel has for its response wake);
on a standalone server both degrade gracefully to interval polling. The channel stores response
envelopes, recovery registrations, and waiter heartbeats in TTL-indexed collections, so MongoDB
itself reaps expired documents — there is no application-side pruning.

Every MongoDB store (channel, transport, durable flows) also claims its effective collections —
derived ones such as the channel's `{MessageCollection}_counters` included — in a small reserved
`asyncresponse_ownership` collection at first use: one tiny document per collection, so two
components (in the same process or different hosts) misconfigured onto the same collection fail
startup with an error naming both claimants instead of silently corrupting each other's data.
The claim is independent of `AutoCreateIndexes` — disabling index DDL must not disable collision
protection — so it still runs when auto-creation is off. A least-privilege deployment whose
principal cannot write `asyncresponse_ownership` opts out with `UseOwnershipLedger = false` on the
channel, transport and durable-flow options, and audits its collection layout externally. Effective
namespaces (`database.collection`, UTF-8 bytes) are validated against MongoDB's sharded limit
(235 bytes) at store construction.

### Redis-compatible servers

The Redis channel (`AsyncResponse.Channels.Redis`) and Redis Streams transport
(`AsyncResponse.Transports.Redis`) talk RESP through `StackExchange.Redis` and use only widely
implemented commands, so they run unchanged on Redis-compatible servers. The command surface is:

| Component | Commands used | Portability |
|---|---|---|
| Channel | `SUBSCRIBE`/`PUBLISH` + `PUBSUB NUMSUB` (the "is anyone listening?" lost-subscriber probe), `GET`/`SET EX`/`DEL`, `SCAN`, `WATCH`/`MULTI`/`EXEC` for the recovery-state CAS, and on Redis Cluster `CLUSTER NODES` (read by the recovery scan, and by the liveness probe only when a primary-flagged node has been disconnected for longer than the probe's 90 s failover grace) | pub/sub, strings, `SCAN`, transactions — universally supported |
| Transport | Streams: `XADD` (with `MAXLEN ~` approximate trim, no Redis 8 trim-mode token), `XGROUP CREATE`, `XREADGROUP`, `XPENDING … IDLE`, `XCLAIM` (incl. `JUSTID` for the in-flight heartbeat), `XACK`; `EVAL`/`EVALSHA` for the idempotent worker publish (`GET`/`XADD`/`SET` inside) and the stop-time consumer retirement (`XPENDING`/`XGROUP DELCONSUMER` inside) — the broker ACL must allow scripts | requires Redis Streams + consumer groups, Redis 6.2+ (`XPENDING … IDLE`) |

| Server | Channel | Transport | Notes |
|---|---|---|---|
| **Redis** 6.2+/8 | ✅ | ✅ | reference implementation |
| **Valkey** 7.2 / 8 | ✅ | ✅ | drop-in; the full Redis-backed integration suite passes against it end-to-end in CI |
| **Dragonfly** | ✅ | ✅ | RESP-compatible (channel + Streams transport commands validated against a live server); connect directly |
| **Garnet** 1.0 | ✅ | ❌ | implements pub/sub, strings, and `SCAN`, so it works as a **channel**, but has no stream commands, so it cannot back the Streams **transport** |
| **ElastiCache / MemoryDB / Azure Managed Redis** | ✅ | ✅ | managed Redis/Valkey — same command surface; cluster mode included (the channel key-routes its pub/sub channels so `SUBSCRIBE` and `PUBLISH` meet on one node; give the transport a hash-tagged `KeyPrefix` so its keys share one slot) |

No configuration change is needed — point `IConnectionMultiplexer` at the server. A scheduled CI
matrix reruns the whole Redis-backed integration suite against **Valkey** to hold this claim; Valkey
is a true drop-in for the Aspire test harness (it shares the redis container launch contract).
**Dragonfly** is RESP-compatible and validated by running the real channel + transport against a live
server, but its container entrypoint differs from the redis image, so it is not exercised through that
Aspire harness — connect to it the same way you would any Redis. **Garnet** is validated as a channel
only; pairing it with the Redis transport fails fast because the stream commands are absent.
