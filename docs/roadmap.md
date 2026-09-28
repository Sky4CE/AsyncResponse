# Roadmap

What ships today, what comes next, and what is deliberately not being built. The backend matrix
is essentially complete, so the next priorities are **capabilities** — what a flow can do while it
runs — rather than more brokers. Market and download figures are as of **July 2026** (sources at
the end).

**Status legend:** 🟢 shipped · 🟠 SHOULD (next) · 🟡 COULD (demand-driven) · ⚫ WON'T for now
(revisit on demand).

**On this page**

- [1. Shipped](#1-shipped)
- [2. The bar for a new package](#2-the-bar-for-a-new-package)
- [3. Next: capabilities](#3-next-capabilities)
- [4. Backends, demand-paced](#4-backends-demand-paced)
- [5. Platform feature uptake](#5-platform-feature-uptake)
- [6. Watch](#6-watch)
- [7. WON'T for now — and why](#7-wont-for-now--and-why)

---

## 1. Shipped

| Axis | Providers |
|---|---|
| **Channels (6)** | In-memory, Redis (+ Valkey / Dragonfly / Garnet), NATS, PostgreSQL, SQL Server, MongoDB |
| **Transports (11)** | In-memory, Redis Streams, RabbitMQ, Azure Service Bus, Google Pub/Sub, NATS JetStream, PostgreSQL, Kafka, SQL Server, AWS SQS, MongoDB |
| **Durable-flow stores (10)** | In-memory, SQL Server, PostgreSQL, MySQL, SQLite, Oracle, MongoDB, Cosmos DB, DynamoDB, EF Core |

Capabilities, each documented on its own page:

- 🟢 **Durable timers, delayed worker jobs, cron-scheduled flows** — `flow.DelayAsync` /
  `DelayUntilAsync`, delayed `EnqueueWorkerAsync`, `WithScheduledFlow<TFlow, TInput>`; native
  delayed delivery on 6 transports, in-process fallback on the rest.
  [timers-and-scheduling.md](timers-and-scheduling.md)
- 🟢 **`AsyncResponse.Testing`** — virtual clock, `FlowTestHarness` with scripted replies,
  crash injection at checkpoints, simulated restarts. [testing.md](testing.md)
- 🟢 **Conformance suites** — one behavioral contract per axis (channel, transport, flow store)
  plus the full 660-cell provider cross product.
  [README — How it's tested](../README.md#how-its-tested)
- 🟢 **Per-transport semantics matrix** — ACK modes, attempt counting, dead-lettering,
  early-ACK failure handling, shutdown drain. [transport-semantics.md](transport-semantics.md)
- 🟢 **Sequence-arbitrated delivery confirmation on the database channels** — `acked_seq` breaks
  the same-tick registration/claim tie. [postgresql.md](postgresql.md),
  [sqlserver.md](sqlserver.md)

The two-axes rule from [#14](https://github.com/Sky4CE/AsyncResponse/issues/14) and
[#17](https://github.com/Sky4CE/AsyncResponse/issues/17) governs every backend verdict below: a
**transport** needs competing consumers, acks, redelivery, and dead-lettering; a **channel** needs
targeted wake, a per-key-TTL recovery KV, and a delivery-confirmation protocol; the durable-flow
**store** is a separate third axis. A great transport is usually a poor channel, and vice versa.

---

## 2. The bar for a new package

A new backend or capability package ships only when it meets the bar the existing ones set:

- **Adapter seam** over the vendor SDK (internal `I<Backend>Client`-style interfaces) so unit
  tests run on fakes.
- **Unit tests** in `tests/AsyncResponse.Tests` for the dispatcher hot path, ack/redelivery,
  options validation, and failure paths.
- **Integration tests** against a real container or official emulator in the Aspire fixture:
  the axis's conformance suite, a cross-product cell (`MatrixCompletenessTests` fails without
  one), and — for transports — a dedicated early-ACK sample-app variant.
- **Both ACK modes** (transports): ack-after-handler by default; opt-in early ACK with explicit
  `BackgroundWorkerCount` / `BackgroundQueueCapacity`, a drain budget validated against host
  shutdown, and `OnBackgroundFailure`.
- **Bounded redelivery + dead-lettering**, or an explicit documented delegation (as Google Pub/Sub
  delegates to its subscription's `DeadLetterPolicy`).
- **Correlation id** in broker metadata, with the `CorrelationIdJsonPaths` fallback.
- **Observability**: publish *and* receive spans with OTel messaging attributes.
- **Wire contract untouched**: schema-versioned envelopes pass through opaquely.
- **Docs**: options in `configuration.md`, a row in the README matrix and
  `transport-semantics.md`, a stress-harness scenario and an NBomber profile, and every
  per-provider list the package touches — `recovery.md`, `observability.md`, `sample.md`, and
  `troubleshooting.md` for new gotchas.

For scale: a shipped broker transport is ~2–3.5k LOC plus tests (Google Pub/Sub ≈ 2.0k,
RabbitMQ ≈ 3.4k).

---

## 3. Next: capabilities

### 3.1 Claim-check payload seam — `IPayloadStore` 🟠

Large payloads do not belong in response envelopes or the flow ledger. An `IPayloadStore` seam
with S3 / Azure Blob / GridFS providers would move oversized payloads to blob storage and pass
references on the wire, transparently on publish and materialization. Today the store's
`MaxStateBytes` guard and `DurableFlowOptions.LedgerSizeWarningBytes` tell you when a ledger hits
or approaches the limit; claim-check is how you stop hitting it. Temporal productized the same
idea as "External Storage" in 2026.

**Incremental step persistence** 🟡 — appending completed steps instead of rewriting the ledger on
every checkpoint — waits for a workload that measurably needs it; the current quadratic write cost
is documented in [durable-flows.md](durable-flows.md#storage-where-flow-state-lives).

### 3.2 Flow operations API + observability pack 🟠

Operating flows in production today means querying the store by hand. `IDurableFlows` exposes
only `StartAsync`, `ResumeAsync`, and `GetStateAsync`.

- **Operations surface** — list/query runs by status, flow type, and age, and cancel a run. This
  is the API the sample's endpoints and any future dashboard would sit on.
- **Observability pack** — per-transport dead-letter and early-ACK background-failure counters
  (the `asyncresponse.worker.jobs` counter covers executions, failures, and rejections, not
  dead-letters), plus a Grafana dashboard pack over the existing OTel metrics.

---

## 4. Backends, demand-paced

New backends ship on demand signals, not on principle.

### 4.1 Hangfire transport 🟠

Hangfire is purely a worker-execution system, so it fits `IWorkerTransport` cleanly: **durable
workers with no broker at all**, on the SQL database a team already runs. Hangfire core is LGPLv3
(fine for an opt-in package) and actively maintained. Caveats: transport only (pair with any
channel); DB-polling dispatch latency. Effort is small — map `MaxDeliveryAttempts` onto
`AutomaticRetryAttribute` and route final failures to `OnBackgroundFailure`.

### 4.2 Azure Storage Queues transport 🟡

`Azure.Storage.Queues` sees roughly 90 % of Azure Service Bus's daily downloads (partly inflated by
Azure Functions bindings). Technically a near-clone of the SQS port: visibility-timeout settlement,
`DequeueCount` for attempts, library-managed dead-lettering as on Redis Streams. Positioned as
**the cheap-Azure option**; Azure Service Bus stays the default for its DLQ, scheduled messages,
and duplicate detection.

### 4.3 Store-mixing → MQTT 5 channel, Cosmos DB / DynamoDB recovery stores 🟠 / 🟡

Each durable channel bundles its own `IRecoveryStateStore`. Supporting **"channel = wake mechanism
+ recovery store"** as a composition (e.g. `.WithMqttChannel(...).WithCosmosRecoveryStore(...)`)
unlocks backends with great delivery but no KV, and KVs with no targeted wake. Curated
single-dependency pairings stay the documented default; mixing is an advanced opt-in guarded by a
startup validator. The watchdog, health check, and `RecoveryState` wire format already work over
any `IRecoveryStateStore`, so this is registration and documentation work (🟠). First consumers
(🟡):

- **MQTT 5 channel** — native request/response (`ResponseTopic` + `CorrelationData`),
  topic-per-cid wake, QoS 1; MQTTnet v5 is stable under the dotnet org. No broker KV, so it
  needs store-mixing.
- **Cosmos DB / DynamoDB recovery stores** — both already ship as durable-flow stores; as recovery
  stores they supply KV + TTL. Their change feeds are shard-polled, not targeted wakes, so they
  are never channels on their own.

### 4.4 Redis/Valkey and NATS KV durable-flow stores 🟡

"Everything on Redis" or "everything on NATS" works for the channel and transport but not the flow
ledger. Both meet the atomic store contract: NATS KV has per-revision compare-and-set (already used
by the NATS channel's recovery store), and Redis gets CAS via scripts, with its durability caveat
documented. Promote when all-Redis / all-NATS requests arrive with flow-store requirements.

### 4.5 MassTransit migration recipe (docs) 🟠 — bridge 🟡

MassTransit v8 community support ends at the end of 2026 and v9 is commercial. Publish a migration
recipe in H2 2026 — request/response and saga-shaped patterns mapped onto durable flows, side by
side. An `IWorkerTransport` bridge over `IPublishEndpoint` / `IConsumer` ships only on concrete
demand.

---

## 5. Platform feature uptake

Small items that keep shipped packages current:

| Item | Size | Status | What and why |
|---|---|---|---|
| **SQS fair queues** | XS | 🟠 | Per-`MessageGroupId` fairness on standard queues (July 2025) — document it as the answer to noisy-neighbor correlation ids; no code change. |
| **Redis 8.4 `XREADGROUP … CLAIM`** | S | 🟠 | Folds the separate pending-entry reclaim pass into the read itself. Feature-detected only — the transport must keep running unchanged on Valkey, Dragonfly, and older Redis. |
| **NATS batch publish** | XS | 🟡 | NATS 2.14 batched JetStream publishes — a throughput win on the transport's publish path when the server supports it. |
| **Valkey 9 in the CI matrix** | XS | 🟠 | The Redis-compatibility CI job still pins Valkey 8. |

---

## 6. Watch

Real triggers, deliberately not built yet:

- **Kafka share groups (KIP-932).** Removes Kafka's head-of-line caveat, but the .NET client has
  no share consumer yet (librdkafka ships only a preview). The Kafka adapter seam leaves room for
  a `ShareGroup` consumption mode without breaking options; build nothing until the client lands.
- **RabbitMQ Streams transport.** Feasible (`RabbitMQ.Stream.Client` is production-grade), but
  streams are Kafka-shaped — offset log, no per-message ack — so it would inherit the Kafka
  transport's caveats while serving an audience the classic-queue RabbitMQ transport already
  covers. 🟡, on concrete asks.
- **NATS native delayed delivery.** NATS 2.12 per-message schedules could make the NATS transport
  delay-capable once the .NET client supports them; until then NATS timers use the in-process path.

---

## 7. WON'T for now — and why

- **Kafka as a channel** — no targeted wake, no per-key-TTL KV; compacted topics require full
  materialization. Transport only, by design.
- **Azure Event Hubs / Amazon Kinesis** — partitioned logs with checkpointing: no per-message ack,
  no broker DLQ, no single-message redelivery. Ingestion streams, not work queues.
- **Apache Pulsar** — good primitives and a maintained client (`DotPulsar`), but .NET demand is
  ~1 % of `Confluent.Kafka`'s. Revisit when demand appears.
- **Garnet streams** — Garnet is a validated **channel-only** server; stream commands are not in a
  Garnet release.
- **ActiveMQ / Artemis, IBM MQ, Solace** — declining or commercial-niche brokers with weak .NET
  clients; sponsored demand only.
- **EventStoreDB** — an event log is the wrong primitive for a correlation-id rendezvous.
- **Hazelcast / Ignite** — meet the channel bar technically, but second-class .NET clients and a
  heavy operational footprint.
- **Consul / etcd / ZooKeeper** — coordination stores: tiny values, config-tuned watches.
- **Memcached** — no pub/sub, volatile only; strictly worse than the Redis channel.
- **ZeroMQ / gRPC / WebSockets** — point-to-point, no durability, no competing consumers.
  gRPC/webhook *responses* already enter through `IAsyncResponseIngress`.
- **NServiceBus adapter** — a commercial audience that already owns sagas and outbox; only on a
  paying customer's request.
- **A full flow-operations web dashboard** — a product, not a feature. The operations API and
  observability pack (3.2) capture the operational value first.

---

### Sources

- Backend investigation: [#14](https://github.com/Sky4CE/AsyncResponse/issues/14),
  [#17](https://github.com/Sky4CE/AsyncResponse/issues/17)
- Kafka share groups:
  [KIP-932](https://cwiki.apache.org/confluence/display/KAFKA/KIP-932%3A+Queues+for+Kafka),
  [Confluent Cloud — share consumers (Java clients only)](https://docs.confluent.io/cloud/current/client-apps/share-consumers.html),
  [librdkafka INTRODUCTION (share-consumer preview)](https://github.com/confluentinc/librdkafka/blob/master/INTRODUCTION.md),
  [confluent-kafka-dotnet releases](https://github.com/confluentinc/confluent-kafka-dotnet/releases)
- MassTransit: [Announcing MassTransit v9](https://masstransit.io/introduction/v9-announcement)
- Hangfire: [hangfire.io/licenses](https://www.hangfire.io/licenses.html),
  [nuget.org/packages/Hangfire.Core](https://www.nuget.org/packages/Hangfire.Core)
- NATS: server 2.12 and 2.14 release notes (docs.nats.io, nats.io/blog)
- Temporal External Storage: Replay 2026 announcements (temporal.io/blog)
- SQS fair queues: AWS What's New, July 2025
- Redis 8.4 `XREADGROUP … CLAIM`: Redis 8.4 what's-new (redis.io)
- Download figures:
  [Azure.Storage.Queues](https://www.nuget.org/packages/Azure.Storage.Queues),
  [MQTTnet](https://www.nuget.org/packages/MQTTnet),
  [DotPulsar](https://www.nuget.org/packages/DotPulsar)
