# Configuration

[← Back to README](../README.md)

`AddAsyncResponse()` registers the channel-agnostic engine but **selects no channel, transport, or
durable-flow store** — chain exactly one channel (`.WithInMemoryChannel()`, `.WithRedisChannel()`,
`.WithNatsChannel()`, `.WithPostgreSqlChannel(...)`, `.WithSqlServerChannel(...)`, or
`.WithMongoDbChannel(...)`), exactly one transport (`.WithInMemoryTransport()`,
`.WithRedisTransport(...)`, `.WithAzureServiceBusTransport(...)`, `.WithGooglePubSubTransport(...)`,
`.WithRabbitMqTransport(...)`, `.WithSqsTransport(...)`, `.WithKafkaTransport(...)`,
`.WithNatsTransport(...)`, `.WithPostgreSqlTransport(...)`, `.WithSqlServerTransport(...)`, or
`.WithMongoDbTransport(...)`), and exactly one flow store (`.WithInMemoryDurableFlows()`, a
`.With*DurableFlows(...)` provider, or `.WithDurableFlows<TStore>()`). An app that starts without any
one of the three fails fast with setup guidance. The recovery watchdog is part of the engine and
runs by default for whichever channel you choose.

This page is the consolidated options reference: engine options, durable-flow store options,
channel options, and transport options with short per-transport notes. Full delivery semantics
live in [transport-semantics.md](transport-semantics.md); copy/paste registrations in
[provider-examples.md](provider-examples.md).

**On this page**

- [Engine options (`AsyncResponseOptions`)](#engine-options-asyncresponseoptions)
- [Durable-flow state store package options](#durable-flow-state-store-package-options)
- [Channel options](#channel-options)
- [Transport options](#transport-options)
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
| `Watchdog.StaleAfter` | 24 hours | Age at which an entry with no live waiter is reported stale. Must be positive. |
| `Watchdog.MaxScanEntries` | 100 000 | Upper bound on recovery entries one scan buffers before probing (unique correlation ids plus individual correlation-less entries — a memory bound, not a flow count). A larger store is reported for the buffered subset only: the report carries `Truncated`, the recovery health check degrades, the `asyncresponse.recovery.scan_truncated` gauge reads 1, and a warning is logged. |
| `Watchdog.ProbeConcurrency` | 8 | Liveness probes one scan runs concurrently; each is a round trip to the channel, one per buffered entry. Must be at least 1. |
| `Watchdog.StartupDelay` | 5 minutes | Delay before the first scan after host start. |
| `Watchdog.IntervalJitter` | 10% of `Interval` | Random extra delay added to the startup delay and every interval wait, so replicas deployed together do not probe in lockstep. `TimeSpan.Zero` makes the scan exactly periodic. Cannot exceed `Interval`. |
| `MaxInboundMessageChars` | 8 Mi | Largest inbound message the ingress processes, in UTF-16 code units. A larger one is acknowledged without dispatch (it never gets smaller, so redelivery would hot-loop), with an error log and the `asyncresponse.ingress.oversized_messages` counter. Also enforced **producer-side**: `EnqueueWorkerAsync` and `IDurableFlows.StartAsync` (whose start job carries the initial ledger) throw `WorkerJobTooLargeException` before publishing when the serialized envelope — plus the few characters a re-published hop adds — would exceed it, so an oversized job fails in the caller instead of being dropped by a consumer. The producer uses its own value as the consumer's, so keep it identical across a deployment. `null` removes the limit; a non-positive value is rejected at startup. A memory guard, not a business rule: put large payloads behind a claim check rather than raising it. |

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
| `StateExpiry` | 14 days | Idle TTL for persisted flow state, refreshed on every checkpoint — it bounds the gap *between* checkpoints, not total run duration. Double the 7-day default step-timeout chain, so a silent step faults before its ledger expires; while `DefaultStepTimeout` is unset, startup rejects a value that does not exceed the channel's effective default waiter timeout (`DefaultTimeout`, else `RecoveryStateExpiry`). Also bounds the longest single `DelayAsync`/`DelayUntilAsync` sleep: the 3650-day persistence ceiling **minus** this value (default → 3636 days). |
| `MaxFlowIdLength` (const) | 400 | Portable flow-id length in characters — the `flow_id` column length in the SQL Server, MySQL, Oracle, and EF Core stores. Every final id (root, composed child `{parentId}:{stepName}`, scheduled `sched:{name}:{timestamp}`) is validated at creation, so an id cannot work on one store and fail on another. |
| `MaxFlowIdBytes` (const) | 1023 | Portable flow-id size in UTF-8 bytes — the Cosmos DB id limit, which 400 multi-byte characters exceed. Ids must also avoid `/`, `\`, `?`, `#` (Cosmos rejects them) and control characters, and are compared ordinally (a binary collation is pinned on the relational columns). |
| `DefaultStepTimeout` | `null` (channel default) | Default timeout for `AwaitStepAsync` steps that don't pass one explicitly. |
| `ExecutionLeaseDuration` | 1 minute | How long one store lease owns a flow execution before another replica may take over after owner loss. Safe to change between deployments: a wake-up behind a lease written under the previous value waits for that lease's persisted expiry ([details](durable-flow-state-stores.md#lease-contention-and-deployments-that-change-the-lease-duration)). |
| `ExecutionLeaseRenewInterval` | 20 seconds | Renewal cadence; must be positive and shorter than `ExecutionLeaseDuration`. A failed renewal is retried on a jittered exponential backoff capped at this interval and at half the time the lease has left, so retries land before the lease's deadline. |
| `MaxLeaseContentionWait` | 1 hour | Longest one wake-up stays parked behind another worker's lease **because of the expiry the store reports for it** — a store clock hours ahead, or a shifted expiry, would otherwise park the delivery (and its worker slot) indefinitely. Past it the wake-up fails with `DurableFlowLeaseContendedException` and the transport redelivers it. This host's own window (`ExecutionLeaseDuration` + `ExecutionLeaseRenewInterval`) is always waited. Raise it if a deployment legitimately issues longer leases. Must be positive. |
| `ProgressPersistenceInterval` | 1 second | Minimum interval between writes caused only by progress reports. Faster updates are coalesced into the next checkpoint (a run's outcome records its own final message); zero writes every report. |
| `MaxRetainedSteps` | 256 (`null` disables) | Maximum distinct checkpoints in one flow ledger. A new step beyond the limit fails terminally before its side effects; existing checkpoints still replay. Partition long histories into bounded child flows; raise or disable only after measuring write amplification. |
| `LedgerSizeWarningBytes` | 512 KiB; 256 KiB on DynamoDB (`null` disables) | Ledger size past which the executor logs a warning naming the flow — once when first crossed, again at each doubling. The size is the UTF-8 bytes the store measured for the write, the same bytes `MaxStateBytes` judges (a custom store that measures nothing is judged by a lower-bound estimate). Every checkpoint rewrites the whole ledger, so cost grows with each step until `MaxStateBytes` (or the provider's item cap) refuses a checkpoint; this is the early signal to keep step results small or partition into child flows. Must be positive. A threshold at or above the store's `MaxStateBytes` could never fire before the cap: left at its default it is lowered to three quarters of the cap, and set explicitly the bundled stores refuse it at startup. |
| `TimerInProcessThreshold` | 10 seconds | Timer remainders (`flow.DelayAsync`) at or under this wait in process under the execution lease; longer ones suspend the run behind a delayed wake-up job when the transport supports native delayed delivery. Zero always prefers suspension; on transports without delayed delivery every timer waits in process. See [timers-and-scheduling.md](timers-and-scheduling.md). |
| `MaxInProcessParkDuration` | `null` (derive from the transport) | Longest a durable timer holds ONE worker delivery while it waits in process. Some brokers cap how long a delivery may stay in flight however alive its handler is (Google Pub/Sub's `MaxTotalAckExtension`, RabbitMQ's `consumer_timeout`, the SQS 12-hour visibility ceiling), so longer sleeps are waited in hops: park, checkpoint, publish an immediate wake-up, end the delivery. `null` derives the hop from the transport (half of any ceiling it advertises, never more than the delivery has left; otherwise the ~49.7-day timer ceiling); a value can only shorten that hop or supply one for a transport whose ceiling the library cannot read. Must be positive and at most ~49.7 days. Awaited-response steps are not hopped — see [timers-and-scheduling.md](timers-and-scheduling.md). |
| `AllowEarlyAckWorkerSubscriber` | `false` | Accepts running the worker subscriber in early ACK (`UseAckAfterEnqueue`) while durable flows are registered; otherwise startup throws. Under early ACK a crash after the ACK, or a wake-up handed back at host stop, strands the run until an operator `ResumeAsync(flowId)` (see [transport-semantics.md](transport-semantics.md)). |
| `MaxStateBytes` (provider stores) | DynamoDB 350 000 · Cosmos 1 900 000 · MongoDB 15 000 000 · `null` (unlimited) on the SQL and EF Core stores | Serialized-ledger size budget checked on every create/checkpoint; not available on the in-memory store or `.WithDurableFlows<TStore>()`. An oversized write fails with a diagnosable error (flow id, size, limit) instead of the raw provider error; defaults sit under each provider's hard item cap. The refused attempt is retried under the transport's redelivery bound — the step whose checkpoint was refused re-runs each time — and dead-lettered where the transport has a dead-letter destination (RabbitMQ's default is unlimited requeues on classic queues; the in-memory transport drops the job after `MaxDeliveryAttempts`). On Cosmos the budget applies to the **complete document** as sent (the ledger JSON is embedded as an escaped string, so a ledger under the budget can still exceed Cosmos's 2 MB item cap). Keep large payloads in your own storage and pass references (see [ledger size and cost](durable-flows.md#ledger-size-and-cost)). |

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
| `SqlServer` | `ConnectionString`, `SchemaName`, `TableName`, `AutoCreateSchema`, `PruneInterval`, `PruneBudget`, `MaxStateBytes` |
| `PostgreSQL` | `ConnectionString` or registered `NpgsqlDataSource`, `SchemaName`, `TableName`, `AutoCreateSchema`, `PruneInterval`, `PruneBudget`, `MaxStateBytes` |
| `MySql` | `ConnectionString`, `TableName`, `AutoCreateSchema`, `PruneInterval`, `PruneBudget`, `MaxStateBytes` |
| `Sqlite` | `ConnectionString`, `TableName`, `AutoCreateSchema`, `PruneInterval`, `PruneBudget`, `MaxStateBytes` |
| `Oracle` | `ConnectionString`, `TableName`, `AutoCreateSchema`, `PruneInterval`, `PruneBudget`, `MaxStateBytes` |
| `MongoDB` | `ConnectionString` or registered `IMongoDatabase`/`IMongoClient`, `DatabaseName`, `CollectionName`, `AutoCreateIndexes`, `UseOwnershipLedger`, `MaxStateBytes` |
| `Cosmos` | `ConnectionString` or registered `CosmosClient`, `DatabaseName`, `ContainerName`, `PartitionKeyPath`, `AutoCreateContainer`, `Throughput`, `MaxStateBytes`, `AllowUnsafeAccountConfiguration` (default `false`: the store refuses an account whose reads run below Session consistency or that has more than one write region; set it only for the emulator and tests — see [account requirements](durable-flow-state-stores.md#account-requirements)) |
| `DynamoDB` | registered/default `IAmazonDynamoDB`, `TableName`, `AutoCreateTable`, `EnableTimeToLive`, `TimeToLiveAttributeName`, `MaxStateBytes` |
| `EFCore` | `.WithEFCoreDurableFlows<TContext>()` with the mapping applied in your `DbContext` via `modelBuilder.ConfigureAsyncResponseDurableFlows(...)`; `PruneInterval`, `PruneBudget`, `MaxStateBytes`; schema changes are owned by your EF migrations |

The SQL and EF Core stores prune expired rows opportunistically on flow creation, throttled by
`PruneInterval` (default 5 minutes; zero or negative prunes on every save). Each prune deletes
1000-row batches while they come back full, for at most `PruneBudget` (default 2 seconds; zero =
one batch) — the triggering create waits, so the budget also bounds its added latency. A failed
prune is skipped until the next interval and never fails the `StartAsync` it rides on. Deleted
rows, failures, and a lapsed budget with rows remaining are counted on the `AsyncResponse` meter
(`asyncresponse.flow_state.pruned_rows` / `prune_failures` / `prune_budget_exhausted`, tagged by
provider) and logged at Warning. MongoDB, Cosmos, and DynamoDB use native TTL instead. All packages
register their store as a singleton and reuse a host-registered client when one exists. See
[durable-flow-state-stores.md](durable-flow-state-stores.md) for package examples, lifetimes,
cleanup mechanics, and schema ownership.

## Channel options

Options are set through the channel registration callback (`.WithInMemoryChannel(options => …)`,
`.WithRedisChannel(options => …)`, `.WithNatsChannel(options => …)`,
`.WithPostgreSqlChannel(options => …)`, `.WithSqlServerChannel(options => …)`,
`.WithMongoDbChannel(options => …)`). Every channel has a complete registration in
[provider-examples.md](provider-examples.md#channel-examples).

| Option | Channels | Default | Purpose |
|---|---|---|---|
| `KeyPrefix` | Redis | `asyncresponse` | Isolates apps/environments sharing one Redis. **Persisted — treat as a deployment contract.** Response pub/sub channels are key-routed (`RedisChannel.WithKeyRouting`), so on Redis Cluster `SUBSCRIBE` and `PUBLISH` for one correlation id meet on the same node and the subscriber count — the lost-subscriber signal — stays meaningful. Do **not** hash-tag this prefix on a cluster: a tag (`{app}`) pins every response channel and recovery key to one slot and one node. (The Redis *transport*'s `KeyPrefix` is the opposite case — see [Transport options](#transport-options).) |
| `SubjectPrefix` | NATS | `asyncresponse` | Response subjects: `{prefix}.response.{cid}`. A dotted prefix (`my.app`) is fine; a value that would yield an empty subject token (leading/trailing `.`, or `..`) is rejected at startup. |
| `RecoveryBucket` | NATS | `asyncresponse-recovery` | JetStream KV bucket for recovery state. **Give every application or environment sharing one NATS system its own bucket as well as its own `SubjectPrefix`:** registrations are keyed by correlation id only, so deployments sharing a bucket see (and can consume) each other's registrations; a non-default `SubjectPrefix` with the default bucket logs a startup warning. An existing bucket is used as it is, never recreated; a warning reports a max age shorter than `RecoveryStateExpiry` or a replica count different from `RecoveryBucketReplicas`. The watchdog scan purges delete markers left by completed waiters once they are 30 minutes old; with the watchdog disabled they stay until the bucket's max age. |
| `RecoveryBucketReplicas` | NATS | `1` | Replica count the recovery bucket is created with (1–5, validated at startup); use 3 on a clustered JetStream so recovery state survives a node loss. Only applied when the bucket is created. Registrations are always read from the bucket stream's leader, never through Direct Get, which a follower that has not applied the latest write may answer. |
| `PresenceProbeTimeout` | NATS | 2 seconds | How long a presence ping waits for a waiter to answer — for the watchdog's liveness probe and for the re-check a publish makes before routing a response to recovery. Only NATS no-responders reads as "no live waiter"; a subscriber that does not answer in time (busy in a slow `Until` predicate) is reported as unprobeable and never has its registration consumed. |
| `SchemaName` | PostgreSQL, SQL Server | `public` / `dbo` | Schema that contains the channel tables. |
| `ConnectionString` | SQL Server, MongoDB | — | SQL Server: required; must point at an existing database (the package creates schema/tables, never the database). MongoDB: optional — the package prefers a host-registered `IMongoDatabase` (or `IMongoClient` + `DatabaseName`); against a single-node replica set include `directConnection=true`. PostgreSQL uses the registered `NpgsqlDataSource`. |
| `DatabaseName` | MongoDB | — | Database used when no `IMongoDatabase` is registered. |
| `RecoveryStateTable` / `RecoveryStateCollection` | PostgreSQL, SQL Server, MongoDB | `asyncresponse_recovery_state` | Durable lost-subscriber recovery registrations, one row/document per waiter. MongoDB expires them natively via a TTL index. |
| `MessageTable` / `MessageCollection` | PostgreSQL, SQL Server, MongoDB | `asyncresponse_channel_messages` | Stored response envelopes loaded after `LISTEN/NOTIFY` wake-ups (PostgreSQL), by the adaptive polling sweep (SQL Server), or after change-stream wake-ups (MongoDB). The sweep carries the envelope only for unacknowledged rows; acknowledged history comes back header-only and is fetched by id only when a live subscription still needs it. |
| `SubscriberTable` / `SubscriberCollection` | PostgreSQL, SQL Server, MongoDB | `asyncresponse_channel_subscribers` | Live waiter heartbeat rows/documents used for subscriber counts and delivery confirmation. |
| `NotificationChannel` | PostgreSQL | `asyncresponse_channel_notify` | PostgreSQL `LISTEN/NOTIFY` channel; must be a simple identifier. |
| `AutoCreateSchema` / `AutoCreateIndexes` | PostgreSQL, SQL Server, MongoDB | `true` | Create schema/tables/indexes (TTL + lookup indexes on MongoDB) on first use; set `false` when migrations/provisioning own DDL. With `AutoCreateIndexes = false`, MongoDB runs a one-time read-only check and **warns** (never throws) if the TTL or correlation-id index is missing — indexes affect retention/performance, not correctness. With it `true`, an equivalent index under another name is accepted, but not a hidden one. MongoDB also warns once about a channel collection created with a non-simple default collation. |
| `UseOwnershipLedger` | MongoDB | `true` | Claim the effective collections in the reserved `asyncresponse_ownership` collection so two components misconfigured onto one collection fail startup (see [the MongoDB notes](#mongodb-ownership-ledger)). |
| `UseChangeStreams` | MongoDB | `true` | Wake waiters with a change stream on the message collection (requires a replica set; single-node is sufficient). Disabled, or on a standalone server, waiters poll every `ListenerPollInterval` with a full sweep on **every** tick. Each (re)opened stream triggers one immediate full sweep. The channel needs MongoDB **4.4+**. |
| `MessageRetention` | PostgreSQL, SQL Server, MongoDB | 1 hour | How long response envelopes remain available for missed-notification / cross-process sweep recovery. MongoDB reaps them natively via a TTL index. |
| `PruneInterval` | PostgreSQL, SQL Server | 30 seconds | Minimum interval between opportunistic prunes of expired channel rows (reads filter on expiry, so pruning is housekeeping only). Zero prunes on every operation. |
| `PublishMaxAttempts` / `PublishRetryBaseDelay` / `PublishRetryMaxDelay` | PostgreSQL, SQL Server, MongoDB | 3 / 50 ms / 1 s | Retry policy for a failed response-envelope insert; `PublishMaxAttempts = 1` disables retries. |
| `DeliveryConfirmationTimeout` | PostgreSQL, SQL Server, MongoDB | 5 seconds | How long a publisher waits for live-waiter confirmation before routing the response to lost-subscriber recovery. Measured on a monotonic clock, so a stepped system clock neither shortens nor stretches it. |
| `DeliveryConfirmationTimeout` | NATS | 5 seconds | How long a publish waits for a subscribed waiter to acknowledge the response. **The opposite outcome from the database channels:** a subscriber that does not acknowledge in time is treated as *delivered* and recovery is not consulted. A waiter whose host died or was partitioned without closing its connection keeps its subscription until the server's ping timeout (about four minutes by default), and a response published in that window is lost — the flow notices only when its own wait times out. Only NATS no-responders routes to recovery. |
| `DeliveryConfirmationPollInterval` | PostgreSQL, SQL Server, MongoDB | 50 ms | Poll cadence for cross-process delivery confirmation. |
| `ListenerPollInterval` | PostgreSQL, MongoDB | 250 ms | Missed-notification safety scan interval (and the wake cadence when MongoDB change streams are unavailable). The deadline is absolute: targeted wake-ups (a notification, a local publish, a new waiter) never postpone it, so traffic cannot starve this tick or the full sweep behind it. |
| `FullSweepInterval` | PostgreSQL, SQL Server, MongoDB | 5 s (PostgreSQL, MongoDB) / `null` (SQL Server) | Throttle for the full missed-notification sweep while push (`LISTEN/NOTIFY`, change streams) carries delivery; `null` sweeps on every poll tick, which SQL Server keeps because polling *is* its delivery. MongoDB polling for good (`UseChangeStreams = false`, a standalone server) sweeps every tick. While push is only down — PostgreSQL before the first `LISTEN` and until a `LISTEN` whose self-sent `NOTIFY` probe came back; MongoDB until a stream (re)opens — the sweep runs every `min(FullSweepInterval, DeliveryConfirmationTimeout / 4)` (1.25 s on defaults). Sweeps dispatch up to 8 correlation ids concurrently, skip the rest of a pass once its first 8 all failed transiently, and record `asyncresponse.channel.sweep.duration` for each completed sweep that visited a waiter. |
| `ActivePollInterval` / `IdlePollInterval` | SQL Server | 250 ms / 2 s | Adaptive polling wake (SQL Server has no `LISTEN/NOTIFY`): sweep cadence while waiters are subscribed, and the backed-off cadence while idle. Same-process deliveries never wait for the sweep, and the absolute poll deadline keeps local traffic from delaying cross-process responses. |
| `PendingMessageBatchSize` | PostgreSQL, SQL Server, MongoDB | 64 | Keyset-page size. Each correlation gets up to 16 forward pages and one history page per pass, then continues after a poll interval. Forward cursors persist between scans. |
| `HistoryReconciliationInterval` | PostgreSQL, SQL Server, MongoDB | 5 s | Minimum delay after a completed historical pass before starting another. One retained-history page per pass discovers late commits behind the forward cursor, including already-acknowledged fan-out responses; large histories take more poll intervals — not a hard discovery deadline. New subscriptions and failed claims rewind immediately, and commits landing within a short lookback (half of `DeliveryConfirmationTimeout`, at most 2 s) behind a fresh cursor are found by the next forward pass. |
| `SubscriberHeartbeatInterval` / `SubscriberHeartbeatTimeout` | PostgreSQL, SQL Server, MongoDB | 10 s / 30 s | Heartbeat cadence and liveness window. One channel-level loop renews the process's active registrations per interval; abandoned rows are not renewed. A failed round retries on a short backoff (at most 1 s); the first failure logs a warning, later ones at most one per interval. |
| `RecoveryStateExpiry` | all | 7 days (in-memory: 30 minutes) | How long recovery state survives; set it above your longest flow duration. While `DefaultTimeout` is unset it is also the waiter-timeout fallback and must then be at most ~49.7 days (the .NET timer ceiling); with `DefaultTimeout` set it is a pure persistence TTL (hard cap 10 years, e.g. 90-day retention). |
| `DefaultTimeout` | all | `RecoveryStateExpiry` | Default per-waiter timeout when a flow doesn't call `WithTimeout`. Every resolved waiter timeout — explicit ones included — must be positive and at most ~49.7 days, checked before the waiter registers anything. |
| `DisposalDrainTimeout` | all | 30 seconds | How long ending a wait (disposal, or its own timeout) drains a delivery already in flight — an `Until` predicate mid-run — before abandoning it. A drained delivery settles the waiter as delivered; a lapsed budget faults it with `AsyncResponseIndeterminateDeliveryException`, never a cancellation or `TimeoutException`, which would invite re-attaching to a possibly-consumed correlation id. On NATS the registration delete and unsubscribe get one more budget, so during an outage disposal returns within about twice this value; the registration left behind expires with its TTL. At most ~49.7 days. |
| `IncludeRemoteStackTrace` | all | `true` | Whether a failed response carries the remote exception's stack trace (`Exception.Data["RemoteStackTrace"]`). The in-memory channel reproduces the wire failure shape. See [security.md](security.md). |
| `MaxRemoteStackTraceLength` | all | `16384` | Length cap (chars) applied to the remote stack trace on both publish and receive; must not be negative. |
| `MaxCorrelationIdLength` (const on `AsyncResponseChannelOptions`) | all | 400 | Portable correlation-id length in UTF-16 code units (the SQL Server channel's column width). Ids longer than this, empty, with leading/trailing spaces, with control characters, or with ill-formed UTF-16 are rejected where they enter the library. |

**Clock note for the database channels** (PostgreSQL, SQL Server, MongoDB): stored envelopes and
each waiter's delivery watermark are both stamped on the *database* clock (the publish path returns
the server-stamped `created_at`, so even the same-process fast path compares like clocks), which
keeps app-host clock skew out of the delivery decision. The 1 s watermark tolerance covers the
database clock's own granularity, not app↔database skew. App clocks only stamp non-delivery
metadata (e.g. recovery-registration age for the watchdog), where ordinary NTP sync is ample.

**NATS permissions and payload ceiling.** The NATS channel's user needs to publish and subscribe on
`{SubjectPrefix}.response.>` (responses, presence pings); publish and subscribe on the
connection's inbox prefix (`_INBOX.>` by default — replies to its own requests, and the
acknowledgement a waiter sends to the requester's inbox; `allow_responses` also grants that
publish); publish on `$KV.{RecoveryBucket}.>` (registration writes); and the JetStream API
(`$JS.API.…`) for the bucket's stream `KV_{RecoveryBucket}`: stream info, stream create (unless
the bucket is provisioned out of band), message get (`$JS.API.STREAM.MSG.GET.KV_{RecoveryBucket}` —
every registration read goes to the stream leader; Direct Get is not used), stream purge
(delete-marker maintenance), and the ephemeral consumer create/delete behind the watchdog's key
listing.

nats-server drops a request or subscription it does not permit without an error the client
surfaces, so missing permissions show up indirectly: without
`$JS.API.STREAM.INFO.KV_{RecoveryBucket}` (needed even when the bucket exists and `STREAM.CREATE`
is granted) the bucket lookup times out and **every waiter registration fails**; without
`$JS.API.STREAM.MSG.GET.KV_{RecoveryBucket}` every registration read times out, so registrations
fail and a lost response is retried until it dead-letters; without
`subscribe` on the response subjects every response takes the lost-subscriber path while its live
waiter runs to its timeout; without `STREAM.PURGE` the watchdog warns on every scan and delete
markers stay until the bucket's max age. A message larger than the server's `max_payload` (1 MiB
by default) cannot travel over the channel: it is reported as unprocessable at once (the ingress
fails the waiter or runs its failure callback instead of retrying), so size responses — or
`max_payload` — accordingly.

## Transport options

Transport options are set through the transport registration callback; each transport package owns
its own option type, and the common shapes are summarized here. See
[Install and run](../README.md#install-and-run) for the local minimum,
[transport examples](provider-examples.md#transport-examples) for every provider, and
[transport-semantics.md](transport-semantics.md) for the full ACK/redelivery/dead-letter matrix.

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
running job — a durable flow starting a child, or a child waking its parent — never waits: the
workers are the queue's only consumers, so a worker waiting on a full queue would wait on itself.
Such follow-ups go to an **in-job overflow** that a worker drains, one round at a time, after each
job and before it takes new work off the queue, so a chain of follow-ups cannot starve queued jobs
while external producers stay parked; the overflow counts toward the shutdown drain. It is bounded
by `InJobOverflowCapacity` (default 4096; `0` allows none; negative is rejected at startup): past
it a follow-up publish throws `InvalidOperationException`, failing the publishing job into the
retry ladder below — make in-job publishes idempotent. Depth and rejections are the
`asyncresponse.worker.inmemory_overflow_depth` gauge and
`asyncresponse.worker.inmemory_overflow_rejections` counter.

**Delayed jobs** — `EnqueueWorkerAsync(..., delay)` and the wake-ups behind suspended flow timers —
are bounded by `DelayedJobCapacity` (default 4096; must be positive), counting jobs waiting on
their due time or fired and waiting for queue room. At the bound a delayed publish from outside a
job waits (honoring its cancellation token); one from inside a running job (a flow parking on a
timer) throws `InvalidOperationException` instead, so the publishing job fails and is redelivered —
make it idempotent. Size it above the number of flows you expect to be sleeping at once. The count
is the `asyncresponse.worker.inmemory_delayed_jobs` gauge; in-job rejections count on
`asyncresponse.worker.inmemory_delayed_rejections`.

Failed jobs retry with backoff (`RetryBaseDelay` 100 ms, doubling to `RetryMaxDelay` 5 s) up to
`MaxDeliveryAttempts` (default 5; `0` = unlimited). During the shutdown drain retries continue,
each backoff capped at `RetryBaseDelay`, and a backoff the stop interrupts retries at once; a job
with unlimited attempts that fails during the drain, or whose backoff the stop interrupts, is
dropped (logged) so the jobs behind it still drain. A delayed job published from a job the drain
runs — a flow parking on a timer — is dropped with a warning like the other pending delayed jobs,
and the park commits: the flow sleeps until it is resumed explicitly after the restart. A job
interrupted *by* the stop (`DurableFlowInterruptedException`) is not a failure: it is abandoned
without a retry or a `failed`/`dropped` outcome, with one warning naming the `ResumeAsync` its flow
needs after the restart — this queue cannot redeliver it. Any other cancellation a job throws (its
own `HttpClient` timeout, say) is an ordinary failure and is retried.

Broker and database transport options:

| Option | Transports | Purpose |
|---|---|---|
| `KeyPrefix` / `SubjectPrefix` / `SchemaName` / `TopicPrefix` | Redis / NATS / PostgreSQL, SQL Server / Kafka | Namespace for worker and response streams/subjects/tables/topics (default `asyncresponse`; `public`/`dbo` for the schemas). On Redis Cluster give the Redis transport a hash-tagged prefix (`{app}`) so every derived key — the idempotent-publish marker included — shares one slot; a prefix or stream name whose braces do not form one well-formed tag is rejected at startup. NATS caps every resolved subject and JetStream stream/consumer name at 255 characters and rejects a prefix or explicit subject that would yield an empty token (a dotted `my.app` is fine), validated at startup. |
| `DefaultReplyTargetName` / `ReplyTargets` / `AddReplyTarget(...)` | all broker and database transports | Named reply destinations a waiter advertises with `.WithReplyTarget()` / `.WithReplyTarget(name)` (see [operations.md](operations.md)). With none configured, the transport's own response queue/topic/stream/subject is the `default` target. |
| `PublishMaxAttempts` / `PublishRetryBaseDelay` / `PublishRetryMaxDelay` | all except Google Pub/Sub and RabbitMQ | Retry policy for a failed publish (default 3 / 50 ms / 1 s); `1` disables retries. |
| `SubscriberRetryBaseDelay` / `SubscriberRetryMaxDelay` | all broker and database transports | Bounded backoff for restarting a failed hosted subscriber (consume-loop fault, transient auth/startup errors). Defaults 100 ms (250 ms on Azure Service Bus, RabbitMQ, SQS) → 5 s. On RabbitMQ these, not `NetworkRecoveryInterval`, pace subscriber restarts. |
| `WorkerSubscriber` / `ResponseSubscriber` | all broker and database transports | Per-role subscriber options: `AckMode` (default `AckAfterHandlerCompletes`), `MaxDeliveryAttempts`, the early-ACK settings below, and per-transport knobs (`BatchSize` 16 on Redis/NATS/PostgreSQL/SQL Server/MongoDB; `RedeliveryDelay` 5 s on NATS/PostgreSQL/SQL Server/MongoDB; `EmptyPollDelay` 250 ms on PostgreSQL/SQL Server/MongoDB, 50 ms on Redis). |
| `StreamReplicas` | NATS | Replica count (JetStream `num_replicas`, 1–5, validated at startup) for the worker, response and dead-letter streams the transport creates; use 3 or 5 on a clustered JetStream. Only applied when a stream is created. Default `1`. |
| `CreateStreams` / `StreamMaxMessages` / `AckWait` | NATS | Create the worker, response and dead-letter streams (and durable consumers) when missing (default `true`); an existing stream or consumer is never modified — one that does not fit (wrong subject, retention, push consumer, max deliver at or below `MaxDeliveryAttempts`) is refused with a retried, logged error, and drift is logged. `StreamMaxMessages` (default 100 000; `null` = unlimited) and `AckWait` (default 30 s; the in-progress heartbeat renews at a third of it) apply only when the library creates the stream/consumer. |
| `WorkerSubject` / `WorkerStream` / `WorkerConsumer` (and `Response*`) | NATS | Explicit subject/stream names (derived from `SubjectPrefix` when `null`) and durable consumer names (default `asyncresponse-workers` / `asyncresponse-responses`). |
| `ConnectionString` | Azure Service Bus | Service Bus namespace connection string. Omit when you register your own singleton `ServiceBusClient`. |
| `ConnectionString` or `HostName` / `Port` / `VirtualHost` / `UserName` / `Password` | RabbitMQ | Broker connection (defaults `localhost:5672`, vhost `/`, `guest`/`guest`); plus `ClientProvidedName`, `AutomaticRecoveryEnabled`, `TopologyRecoveryEnabled`, `NetworkRecoveryInterval` (5 s), `RequestedHeartbeat` (30 s). |
| `WorkerExchange` / `WorkerQueue` / `WorkerRoutingKey` (and `Response*`) | RabbitMQ | Direct exchange, durable queue and routing key per role (default `asyncresponse.worker` / `asyncresponse.response` for all three). |
| `WorkerSubscriber.PrefetchCount` | RabbitMQ | Unacknowledged deliveries buffered per subscriber (default 16; must be positive). Interacts with `BrokerConsumerTimeout` below. |
| `ServiceUrl` / `Region` / `AccessKey` / `SecretKey` | SQS | Endpoint and credentials. All optional: omit everything to use the AWS SDK default chain, set `ServiceUrl` for LocalStack or a proxy, or register your own singleton `Amazon.SQS.IAmazonSQS` (e.g. via `AWSSDK.Extensions.NETCore.Setup`) and the package reuses it. |
| `ConnectionString` | SQL Server | SQL Server connection string; must point at an existing database (the package creates schema/table/indexes, never the database). |
| `BootstrapServers` / `ClientId` | Kafka | Comma-separated broker list and optional client id. The package speaks the Kafka protocol via `Confluent.Kafka`, so Redpanda, Amazon MSK, WarpStream, Aiven, and Confluent Cloud all work. |
| `WorkerTopic` / `ResponseTopic` + `WorkerConsumerGroup` / `ResponseConsumerGroup` | Kafka | Topics for worker jobs and response ingress (default `{TopicPrefix}.transport.worker` / `.response`) and the consumer group per role. The groups default to the fixed `asyncresponse-workers` / `asyncresponse-responses` — **not** derived from `TopicPrefix` — so deployments sharing a cluster need their own group names too, or every member change in one deployment rebalances the consumers of all of them. |
| `CreateTopics` / `TopicNumPartitions` / `TopicReplicationFactor` | Kafka | Provision missing topics (worker, response, dead-letter) on subscriber startup (default `true`); existing topics are left untouched. Partitions (default 8) are the unit of consumer parallelism and ordering; replication factor defaults to `-1`, and `-1` in either uses the broker default. `OperationTimeout` (10 s) bounds these admin calls. |
| `OffsetCommitInterval` | Kafka | Auto-commit cadence (default 5 s) for offsets stored after each resolved message; a crash inside the window redelivers at-least-once. |
| `PollTimeout` / `BackpressurePollDelay` | Kafka | Longest one poll waits for a message (default 200 ms), and the short poll slice (default 50 ms) used while the poll thread waits on in-process work — capacity re-checks under a full `AckAfterEnqueue` queue, and completion checks for detached `AckAfterHandlerCompletes` handlers. |
| `MaxPollInterval` | Kafka | Maximum gap between polls before the broker evicts the consumer and rebalances (librdkafka `max.poll.interval.ms`); default 5 minutes. Handler time does not count: a handler that outlives `DetachHandlerAfter` runs detached while polling continues. Startup requires `DetachHandlerAfter + PollTimeout` to fit within half of the effective interval (this option, or a `MaxPollIntervalMs` set by `ConfigureConsumer`, which runs last) and, under the classic group protocol, the interval to be at least the consumer's `session.timeout.ms` (45 s by default — lower `SessionTimeoutMs` in `ConfigureConsumer` for a shorter interval). Each dead-letter produce is bounded to a quarter of it. |
| `DetachHandlerAfter` | Kafka | In `AckAfterHandlerCompletes`, how long the poll thread waits for a handler inline before detaching it (default 1 s; `0` detaches every handler at once). A detached message's partition is paused (its order holds), the handler and its retry ladder continue on the thread pool, and polling continues, so other partitions keep flowing, `MaxPollInterval` is honored, and rebalance callbacks fire; the offset is stored and the partition resumed once the handler settles. A stop waits for detached handlers whose partitions are still assigned; one whose partition was revoked is not waited for and its offset is never stored. This lets a durable-flow step await a response or sleep on a timer for minutes without eviction. |
| `FaultDrainTimeout` | Kafka | In `AckAfterHandlerCompletes`, how long a subscriber whose poll loop *failed* (consume error, dropped connection, failed burial) waits for its detached handlers before closing the consumer for the supervisor to rebuild (default 5 s; `0` abandons at once). Handlers settling within it get their offsets committed; the rest are abandoned — offsets unstored, messages redelivered on the rebuilt consumer (possibly while the abandoned handler still runs), outcomes logged. A graceful stop is bounded by the host's shutdown budget instead. |
| `HandlerRetryBaseDelay` / `HandlerRetryMaxDelay` | Kafka | Backoff of the in-process retry ladder (default 100 ms → 5 s); see `MaxDeliveryAttempts`. |
| `DeadLetterTopic` / `DeadLetterTopicSuffix` | Kafka | Explicit dead-letter topic, or the suffix appended per source topic (default `.deadletter` → `{topic}.deadletter`). |
| `ConfigureProducer` / `ConfigureConsumer` / `ConfigureAdminClient` | Kafka | Last-chance hooks over the Confluent client configs (security, compression, fetch tuning, …). The producer's `MessageMaxBytes` (default 1,000,000) also sizes dead-letter copies: while `DeadLetterEnabled`, a worker job whose record plus burial headers would exceed it is refused at publish (a durable-flow start refused this way surfaces as `DurableFlowNotDispatchedException` — raise the limit rather than retrying), and a consumed record whose copy cannot fit fails its burial naming the size. Raise it here (and the topics' `max.message.bytes`) or put large arguments behind a claim check. |
| `WorkerQueue` / `ResponseQueue` | Azure Service Bus | Queues for worker jobs and response ingress (default `asyncresponse-worker` / `asyncresponse-response`); they must be distinct, compared case-insensitively as Service Bus entity names are. |
| `WorkerQueue` / `ResponseQueue` | SQS | Queues for worker jobs and response ingress (default `asyncresponse-worker` / `asyncresponse-response`; distinct). Each accepts a queue name (resolved once via `GetQueueUrl`) or a full queue URL, not an ARN; a name must pass the SQS name rule (80 characters of letters, digits, `-`, `_`, `.fifo` counted) at startup. Two names or two (normalized) URLs are compared exactly; a name/URL pair sharing a queue name only warns (configure both as URLs to make it exact). A name ending in `.fifo` opts into FIFO: the correlation id becomes the `MessageGroupId` and every message gets a unique `MessageDeduplicationId`. Every job **without** a correlation id — durable-flow start, resume and wake-up jobs, unless the flow was started inside a request scope — shares the single `FifoMessageGroupIdFallback` group (default `asyncresponse`), which SQS delivers strictly one at a time, so one parked flow holds every other flow's jobs (the worker subscriber warns at startup). Prefer a standard worker queue for durable flows. |
| `CreateQueues` / `DeadLetterQueueSuffix` / `MaxReceiveCount` | SQS | Provision the queues on startup (default **off** — point at existing queues in production), each with a native dead-letter queue (`{queue}-dlq`) wired through a redrive policy that moves a message after `MaxReceiveCount` (default 5) receives. Converging an existing `.fifo` queue re-applies only its mutable attributes. Startup validates every name it would create (derived dead-letter names included), and provisioning retries only transient failures, so a deterministic rejection fails startup at once. |
| `WorkerSubscriber.VisibilityTimeout` / `RedeliveryDelay` | SQS | Per-receive visibility timeout (`null` uses the queue's setting) and an optional shortened invisibility applied via `ChangeMessageVisibility` when a handler fails (`null` lets the timeout lapse). With renewal off, set `VisibilityTimeout` to the queue's value when durable flows run on SQS: unset, the transport advertises the 12-hour SQS maximum to the engine as its in-flight ceiling (the worker subscriber warns at startup). |
| `WorkerSubscriber.VisibilityRenewalInterval` | SQS | Opt-in visibility heartbeat for the message whose handler is running (default `null` = off; requires `VisibilityTimeout` and a shorter interval). It renews past the host stop until the handler returns and clamps its last extension to the 12-hour SQS in-flight maximum, then logs one warning and stops. Off by default because extending visibility overrides queue-tuned redrive timing, and on FIFO queues a wedged consumer keeps its whole message group blocked. |
| `MessageTable` / `MessageCollection` | PostgreSQL, SQL Server, MongoDB | Single queue table/collection (default `asyncresponse_transport_messages`) holding worker, response-ingress, and dead-letter rows/documents. |
| `ConnectionString` / `DatabaseName` | MongoDB | Optional — the package prefers a host-registered `IMongoDatabase` (or `IMongoClient` + `DatabaseName`). Against a single-node replica set include `directConnection=true`. |
| `WorkerQueue` / `ResponseQueue` / `DeadLetterQueue` | PostgreSQL, SQL Server, MongoDB | Logical queue names stored in the queue table/collection (default `worker` / `response` / `deadletter`); must be distinct. |
| `NotificationChannel` | PostgreSQL | `LISTEN/NOTIFY` channel (default `asyncresponse_transport_notify`) that wakes subscribers after publishes; a NAK or delayed publish sends no wake (a zero-delay NAK is picked up at the next poll). The payload is the queue name and each subscriber wakes only for its own queue; a foreign producer should notify with the queue name, or an empty payload to wake every subscriber. SQL Server has no equivalent: same-process publishes wake subscribers in process, and cross-process rows are picked up within `EmptyPollDelay`. |
| `UseChangeStreamWake` | MongoDB | Wake idle subscribers with a change stream on the queue collection (default `true`; requires a replica set). Disabled, or on a standalone server, subscribers poll every `EmptyPollDelay`. |
| `UseOwnershipLedger` | MongoDB | See [the MongoDB notes](#mongodb-ownership-ledger). Default `true`. |
| `LockTimeout` | PostgreSQL, SQL Server, MongoDB | How long a claimed row/document stays leased before another subscriber may retry it (default 30 s). While a handler runs — including past the host stop — the subscriber renews the claim every third of this, fenced by `lock_id`, with bounded, backed-off retries inside the lease, so a slow handler is not redelivered mid-execution. |
| `WorkerSubscriber.LockRenewalInterval` | Azure Service Bus | Peek-lock renewal cadence for the message whose handler is running (default 10 s; `null` disables; must be under the 5-minute `LockDuration` maximum). Keeps renewing past the host stop until the handler returns. With `null` in `AckAfterHandlerCompletes` the worker subscriber warns at startup: the transport then advertises the 5-minute maximum to the durable-flow engine, while the entity's own `LockDuration` (60 s by default) decides redelivery. |
| `WorkerSubscriber.PrefetchCount` | Azure Service Bus | Messages the receiver buffers locally (default 0). Buffered messages are locked but never renewed, so keep `PrefetchCount × handler latency` (or, in `AckAfterEnqueue`, the time a saturated background queue parks the receive loop) well under the queue's `LockDuration`, or leave it at 0. Startup warns when it is positive. |
| `MaxMessagesPerReceive` / `ReceiveWaitTime` | Azure Service Bus, SQS | Receive batch size and long-poll wait (Service Bus 16 / 1 s; SQS 10 / 20 s, also SQS's maxima; an SQS `ReceiveWaitTime` of 0 backs off between empty receives). The `AckAfterHandlerCompletes` worker subscriber receives one message at a time instead, because the broker locks and counts every message a receive hands over. The response subscriber keeps the batch but runs a lost waiter's recovery callbacks inline, so where callbacks can be slow keep lock renewal on (Service Bus) or set `ResponseSubscriber.VisibilityTimeout` + `VisibilityRenewalInterval` (SQS), or batch-mates' locks lapse and a peer runs their callbacks too. |
| `WorkerSubscriber.UseAckAfterEnqueue(workerCount, queueCapacity, drainTimeout?)` | all broker and database transports | Opt-in early ACK for long-running workers: the message is ACKed once accepted into a bounded in-process queue, before the handler runs. Durable-flow wake-ups ride this queue and lose broker redelivery under early ACK, so startup throws unless `DurableFlowOptions.AllowEarlyAckWorkerSubscriber = true` accepts the risk (see [transport-semantics.md](transport-semantics.md)). |
| `WorkerSubscriber.BackgroundDrainTimeout` | all broker and database transports | Bounded wait (default 20 s; `UseAckAfterEnqueue`'s optional third argument) for queued and running background handlers when the subscriber stops — a wait, not a cancellation. Split three quarters for the handlers and one quarter for entries still queued when that lapses: PostgreSQL, SQL Server, MongoDB, NATS, RabbitMQ, Kafka and Redis dead-letter them (and report via `OnBackgroundFailure`); SQS, Azure Service Bus and Google Pub/Sub can write no copy of a settled message, so they report each through `OnBackgroundFailure` with an Error log. In `AckAfterHandlerCompletes`, RabbitMQ and Google Pub/Sub also use it to bound the wait for handlers still running at stop, shortened to what `HostShutdownTimeout` leaves after their `ShutdownTimeout` spend (a handler parked on an awaited flow response can cost the stop this whole bound). |
| `WorkerSubscriber.OnBackgroundFailure` | all broker and database transports | Hook for operator-visible metrics, alerting, or a durable dead-letter path when a background handler fails after early ACK. |
| `WorkerSubscriber.MaxDeliveryAttempts` | all except Google Pub/Sub and SQS | Delivery attempts before dead-lettering (default 5; **RabbitMQ default `0` = unlimited**; negative fails startup). Google Pub/Sub and SQS redeliver natively — bound them with the subscription's `DeadLetterPolicy` or the queue's redrive `maxReceiveCount`. RabbitMQ does not count plain `basic.nack` requeues, so values above 2 need a TTL-retry dead-letter cycle you provision (startup warns otherwise); a message reaching the cap after riding that cycle is parked in `ParkQueue`, else `DeadLetterQueue`, else ACKed and dropped, and a failed park is requeued and retried. A RabbitMQ worker cap of `1` in `AckAfterHandlerCompletes` warns at startup: a delivery the broker requeues on its own (a flow handed back at stop, a prefetched delivery, a closed channel) returns as attempt 2 and is rejected unrun. RabbitMQ 4.x quorum queues also apply their own `delivery-limit` (20 by default), so `0` is not "forever" there. Kafka counts in-process retries per process delivery (offsets cannot NACK one message); `0` is unlimited under `AckAfterHandlerCompletes` but a **single** attempt under `AckAfterEnqueue`. |
| `ShutdownTimeout` | Azure Service Bus, RabbitMQ, Google Pub/Sub, PostgreSQL, MongoDB, SQS | Bound on each close/join step of the stop path (default 5 s); counted against `HostShutdownTimeout` below. |
| `HostShutdownTimeout` | all broker and database transports | The shutdown budget startup validation checks the stop path against (default 30 s, the Generic Host default; `null` skips the check). It must fit `BackgroundDrainTimeout` under early ACK plus `ShutdownTimeout` where the transport has one — RabbitMQ counts it **twice** (consumer cancel, then channel/connection close). Azure Service Bus, SQS and Google Pub/Sub are validated in `AckAfterHandlerCompletes` too (Service Bus: `2 × ShutdownTimeout` with `LockRenewalInterval` set, `1 ×` without). On NATS, PostgreSQL, SQL Server and MongoDB, early ACK on **both** `WorkerSubscriber` and `ResponseSubscriber` sums both drains, because the host stops hosted services one after another (and a hosted service registered after them stops first and spends from the same budget, which validation cannot see) — raise `HostOptions.ShutdownTimeout` or shorten the drains. Mirror any custom `HostOptions.ShutdownTimeout` here. On SQS it also bounds how long a delivery handed back at host stop stays invisible when `VisibilityTimeout` is longer. |
| `DeclareTopology` | RabbitMQ | Declare the durable exchanges, queues and bindings — plus the configured dead-letter exchange, `DeadLetterQueue` and `ParkQueue` — (`true`, default) or leave topology to your infrastructure (`false`). A TTL-retry cycle is never declared by the library. |
| `DeadLetterExchange` / `DeadLetterQueue` / `DeadLetterRoutingKey` / `ParkQueue` | RabbitMQ | The dead-letter exchange the worker and response queues are declared with (`x-dead-letter-exchange`; default `null` → exhausted messages are dropped), the queue bound to it, and the routing key (blank: dead-lettered messages keep their original key). `ParkQueue` receives only what the delivery cap parks — declared unbound, so it never collects the hops of a TTL-retry cycle as `DeadLetterQueue` does — and wins over `DeadLetterQueue` as the park destination. With any of the exchange, `DeadLetterQueue` or `ParkQueue` set, the subscriber channel uses publisher confirms, so an unroutable copy or park fails loudly. In `AckAfterEnqueue` a failed job's copy goes to the dead-letter exchange, and a copy that comes back through it and fails again is parked (or dropped with an error when there is no park destination). A durable flow handed back at host stop in `AckAfterEnqueue` is copied to the exchange with an `AR-DeadLetter-Reason` starting `handed_back_after_commit:` (safe to replay) and reported through `OnBackgroundFailure`; without a written copy it is logged at Error and the wake-up is lost unless `OnBackgroundFailure` records it. |
| `BrokerConsumerTimeout` | RabbitMQ | Mirror of the broker's `consumer_timeout` (default 30 minutes, the broker default): past it RabbitMQ closes the channel and requeues every unacknowledged delivery. Keep it equal to or below the broker's setting; `null` only when the broker's timeout is disabled; must be positive. In `AckAfterHandlerCompletes` the worker transport advertises it divided by `WorkerSubscriber.PrefetchCount` as its in-flight ceiling (prefetched deliveries age while they wait), floored at one minute (or the timeout itself, if shorter), so durable-flow timers wait in process for at most half of that per delivery (see [timers-and-scheduling.md](timers-and-scheduling.md)). A share below the floor — `PrefetchCount` above 30 at the default — logs a startup warning. Set `PrefetchCount = 1` for workers whose timers should use the whole timeout. |
| `CorrelationIdAttribute` / `CorrelationIdProperty` / `CorrelationIdHeader` / `CorrelationIdField` | Pub/Sub, SQS / Azure Service Bus / RabbitMQ, Kafka, NATS, PostgreSQL, SQL Server, MongoDB / Redis | Broker metadata key used to resolve the correlation id before falling back to JSON body paths (default `correlationId`; `AR-Correlation-Id` on NATS, PostgreSQL, SQL Server and MongoDB). On Kafka the correlation id also becomes the message key, and on FIFO SQS the `MessageGroupId`, keeping jobs that share it ordered. Durable-flow jobs carry no correlation id unless the flow was started inside a request scope. SQS and Pub/Sub validate the name at startup against the broker's attribute-name rule (SQS: at most 256 ASCII letters, digits, `_`, `-`, `.`, no leading/trailing/consecutive periods, no `AWS.`/`Amazon.` prefix; Pub/Sub: at most 256 UTF-8 bytes, no `goog` prefix). Redis also names the payload field (`PayloadField`, default `payload`). |
| `CorrelationIdJsonPaths` | broker and database transports | JSON paths inspected when metadata does not carry the correlation id. Every transport also unwraps nested JSON strings at those paths. |
| `DeadLetterEnabled` / `DeadLetterRetention` | Redis / NATS / PostgreSQL / SQL Server / MongoDB / Kafka | Whether poison messages are preserved (default `true`). `DeadLetterRetention` exists only on PostgreSQL, SQL Server, and MongoDB (row/document retention, pruned after a publish at most once a minute per process, in 1,000-row batches for up to 2 s; a prune failure never fails the publish). Redis and NATS bound their dead-letter streams with `DeadLetterStreamMaxLength` / `DeadLetterStreamMaxMessages` (default 100 000), and Kafka's `.deadletter` topic uses broker retention. See [transport-semantics.md](transport-semantics.md) for the per-transport dead-letter matrix. |
| `StreamMaxLength` / `UseApproximateStreamTrimming` | Redis | Worker-stream cap applied on every worker publish as `XADD … MAXLEN ~ N` (default 100 000, approximate; `null` disables). Trimming is by length alone and **deletes unread and pending jobs** with no dead-letter copy — size it well above the deepest backlog an outage can build. Response producers trim the response stream themselves; the library only ACKs it. |
| `ConsumerName` / `CreateConsumerGroups` | Redis | Consumer name inside the groups (default generated per process, `{machine}-{pid}-{guid}` plus the role, within 64 characters; set it only when your orchestrator guarantees one unique value per running process — consumers sharing a name share one pending-entry list). `CreateConsumerGroups` (default `true`) creates the groups at the start of the stream, so a new group on a stream with history replays that backlog. |
| `WorkerSubscriber.PendingMessageMinIdleTime` / `PendingClaimInterval` / `PendingClaimBatchSize` | Redis | Reclaim loop: entries idle longer than this (default 30 s) are claimed for retry by a peer, scanned every 5 s, 16 at a time. |
| `WorkerSubscriber.MaxTotalAckExtension` | Google Pub/Sub | How long the client keeps extending one message's ack deadline (default 60 minutes; at least one minute). This is the transport's in-flight ceiling: past it Pub/Sub redelivers the same message while the first handler still runs. Raise it for handlers that legitimately run longer. |
| `WorkerSubscriber.ClientCount` / `MaxOutstandingMessages` / `MaxOutstandingBytes` | Google Pub/Sub | Streaming-pull connections (default 1; each applies flow control independently, so more connections lease messages that sit idle) and the flow-control ceilings (default 1000 messages / 100 MB; in `AckAfterHandlerCompletes` the message ceiling is also the concurrent-handler limit; ignored under `AckAfterEnqueue`, which is bounded to `BackgroundQueueCapacity`). |

A worker handler failure propagates out of the ingress to the transport dispatcher, which owns the
retry decision: in `AckAfterHandlerCompletes` the delivery is NACKed/abandoned and redelivered up
to `MaxDeliveryAttempts`, then dead-lettered; after an early ACK the failure is reported through
`OnBackgroundFailure` (and written to the transport's own dead-letter destination where one
exists). A failing worker never completes the waiter by itself — publish a failure response from
the worker's error handling when the flow should fail fast instead of waiting out its timeout.

### Per-transport notes

These are the configuration-relevant points; each links to the full semantics.

- **Azure Service Bus** — peek-lock: success completes, failure abandons until
  `MaxDeliveryAttempts`, then Service Bus dead-letters. `AckAfterEnqueue` completes on enqueue, so
  later failures go only to `OnBackgroundFailure`. The `AckAfterHandlerCompletes` worker receives
  one message at a time and renews its lock every `LockRenewalInterval`; with renewal disabled keep
  handler latency well under the queue's `LockDuration` (60 s default, 5 minutes max), and keep
  `PrefetchCount` at 0 unless handlers are fast.
  [Details](transport-semantics.md#azure-service-bus).
- **SQS** — visibility-timeout settlement with long polling: success deletes; failure lets the
  visibility timeout lapse (or shortens it with `RedeliveryDelay`), and the queue's redrive policy
  dead-letters after `maxReceiveCount` — there is no app-level `MaxDeliveryAttempts`.
  `AckAfterEnqueue` deletes on enqueue. Keep handler latency under the visibility timeout, or opt
  into `VisibilityRenewalInterval`. Prefer a standard (not FIFO) worker queue for durable flows.
  [Details](transport-semantics.md#sqs).
- **Kafka** — classic consumer groups with manual offset storage (`enable.auto.commit=true`,
  `enable.auto.offset.store=false`: an offset is stored only once its message is resolved).
  Parallelism equals the partition count, and a slow or retrying message blocks only its own
  partition; `DetachHandlerAfter` keeps long handlers from overrunning `MaxPollInterval`. In
  `AckAfterEnqueue` the offset is stored at enqueue, fetching pauses while the queue is full, and
  exhausted failures go to the dead-letter topic and `OnBackgroundFailure`. From
  `ApplicationStopping` on the worker subscriber takes nothing new.
  [Details](transport-semantics.md#kafka).
- **Redis Streams** — consumer groups with `XREADGROUP`/`XACK`; a reclaim loop takes over entries
  idle past `PendingMessageMinIdleTime`, and `MaxDeliveryAttempts` counts the stream delivery count,
  then writes to the dead-letter stream. `AckAfterHandlerCompletes` reads and claims one entry at a
  time (`BatchSize` and batch claims apply to `AckAfterEnqueue`). Mind the `StreamMaxLength`
  trimming caveat above. [Details](transport-semantics.md#redis).
- **RabbitMQ** — publisher confirms and mandatory routing on publish; per-message `basic.ack` /
  `basic.nack` on consume. Attempt caps above 2 need a TTL-retry dead-letter cycle you provision.
  The early-ACK queue belongs to the hosted subscriber, so a channel fault or broker restart does
  not drain it — only a host stop does. A correlation id longer than an AMQP short string (255
  UTF-8 bytes) travels only in the `CorrelationIdHeader` header, not the native `correlation-id`.
  [Details](transport-semantics.md#rabbitmq).
- **Google Pub/Sub** — streaming pull with ack-deadline extension; redelivery and dead-lettering
  are native (`DeadLetterPolicy`'s `maxDeliveryAttempts`). In `AckAfterEnqueue` flow control is
  bounded to `BackgroundQueueCapacity`, so a full queue parks delivery instead of NACKing. Deliveries
  held at stop are handed back and each costs one `DeadLetterPolicy` attempt, so size
  `maxDeliveryAttempts` (and `MaxOutstandingMessages` / `BackgroundQueueCapacity`) with rolling
  deploys in mind. [Details](transport-semantics.md#google-pubsub).
- **NATS JetStream** — explicit ACK; a failure NAKs with `RedeliveryDelay`, and a message reaching
  `MaxDeliveryAttempts` goes to the dead-letter stream. Consumers are durable. Under early ACK a
  full queue pauses the consume loop rather than NAKing. [Details](transport-semantics.md#nats).
- **PostgreSQL / SQL Server** — table-backed queues: an idempotent `INSERT` per publish, and
  competing subscribers claim rows one atomic claim at a time (`FOR UPDATE SKIP LOCKED` /
  `UPDLOCK, ROWLOCK, READPAST`). PostgreSQL wakes subscribers with `LISTEN/NOTIFY`; SQL Server wakes
  same-process subscribers in process and polls for cross-process rows. A row reaching
  `MaxDeliveryAttempts` moves to the dead-letter queue in the same table. See
  [postgresql.md](postgresql.md) and [sqlserver.md](sqlserver.md) for layout and tuning, and
  [transport-semantics.md](transport-semantics.md#postgresql-sql-server-mongodb).
- **MongoDB** — the document analogue: idempotent inserts (a retried publish collides on `_id` and
  counts as success), atomic `findOneAndUpdate` claims fenced by `lock_id` with a `locked_until`
  lease evaluated on the server clock (`$$NOW`), and dead-letter documents under an id derived from
  the source message, so a crash mid-burial cannot duplicate them. Claims pin the simple (binary)
  collation, so a collection with a folding default collation cannot cross queues — but an index
  built under that collation cannot serve the claim. Subscribers wake via a change stream on a
  replica set (single-node suffices) and poll on a standalone server. The channel's collections are
  TTL-indexed, so MongoDB reaps them itself.
  [Details](transport-semantics.md#postgresql-sql-server-mongodb).

<a id="mongodb-ownership-ledger"></a>**MongoDB ownership ledger.** Every MongoDB component (channel,
transport, durable flows) claims its effective collections — derived ones such as the channel's
`{MessageCollection}_counters` included — in a reserved `asyncresponse_ownership` collection at
first use, so two components misconfigured onto one collection (in one process or across hosts)
fail startup with an error naming both claimants. The claim runs whatever `AutoCreateIndexes` is
set to. A least-privilege deployment whose principal cannot write `asyncresponse_ownership` sets
`UseOwnershipLedger = false` on the channel, transport and durable-flow options and audits its
collection layout externally. Effective namespaces (`database.collection`, UTF-8 bytes) are
validated against MongoDB's sharded limit (235 bytes) at construction.

## Redis-compatible servers

The Redis channel (`AsyncResponse.Channels.Redis`) and Redis Streams transport
(`AsyncResponse.Transports.Redis`) talk RESP through `StackExchange.Redis` and use only widely
implemented commands, so they run unchanged on Redis-compatible servers:

| Component | Commands used | Portability |
|---|---|---|
| Channel | `SUBSCRIBE`/`PUBLISH` + `PUBSUB NUMSUB` (the lost-subscriber probe), `GET`/`SET EX`/`DEL`, `SCAN`, `WATCH`/`MULTI`/`EXEC` for the recovery-state CAS, and on Redis Cluster `CLUSTER NODES` (read by the recovery scan, and by the liveness probe only when a primary-flagged node has been disconnected past a 90 s failover grace) | pub/sub, strings, `SCAN`, transactions — universally supported |
| Transport | Streams: `XADD` (with `MAXLEN ~`, no Redis 8 trim-mode token), `XGROUP CREATE`, `XREADGROUP`, `XPENDING … IDLE`, `XCLAIM` (incl. `JUSTID` for the in-flight heartbeat), `XACK`; `EVAL`/`EVALSHA` for the idempotent worker publish and the stop-time consumer retirement (`XGROUP DELCONSUMER`) — the ACL must allow scripts | requires Redis Streams + consumer groups, Redis 6.2+ (`XPENDING … IDLE`) |

| Server | Channel | Transport | Notes |
|---|---|---|---|
| **Redis** 6.2+/8 | ✅ | ✅ | reference implementation |
| **Valkey** 7.2 / 8 | ✅ | ✅ | drop-in; a scheduled CI matrix reruns the full Redis-backed integration suite against it |
| **Dragonfly** | ✅ | ✅ | RESP-compatible; channel and transport validated against a live server (not through the Aspire harness, whose container contract it does not share) |
| **Garnet** 1.0 | ✅ | ❌ | pub/sub, strings, and `SCAN` work, so it is a valid **channel**; it has no stream commands, so pairing it with the Streams **transport** fails fast |
| **ElastiCache / MemoryDB / Azure Managed Redis** | ✅ | ✅ | managed Redis/Valkey — same command surface; cluster mode included (give the transport a hash-tagged `KeyPrefix`) |

No configuration change is needed — point `IConnectionMultiplexer` at the server.
