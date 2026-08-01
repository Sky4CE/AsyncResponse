# Roadmap

The backend investigation that produced the previous version of this document
([#14 — Support more transports](https://github.com/Sky4CE/AsyncResponse/issues/14) and
[#17 — Support more channels](https://github.com/Sky4CE/AsyncResponse/issues/17)) has done its
job: every backend item in its release trains 1–2 shipped — Kafka, SQS, the
Valkey/Dragonfly/Garnet validation, and the SQL Server pair — plus MongoDB from the COULD tier.
This rewrite re-baselines on what the library is missing *now*, and that is no longer backends.
Popularity figures are NuGet download counts **as of July 2026**; competitive and client-library
facts were re-verified July 2026 (sources at the end).

**Status legend:** 🟢 shipped · 🔴 MUST (next release train) · 🟠 SHOULD · 🟡 COULD
(demand-driven) · ⚫ WON'T for now (revisit on demand).

**On this page**

- [1. Where we are](#1-where-we-are)
- [2. The bar for admission (definition of done)](#2-the-bar-for-admission-definition-of-done)
- [3. Train 0 — hardening (now)](#3-train-0--hardening-now)
- [4. Train A — capabilities (the new headline)](#4-train-a--capabilities-the-new-headline)
- [5. Train B — backends, demand-paced](#5-train-b--backends-demand-paced)
- [6. Cheap 2025-26 feature uptake](#6-cheap-2025-26-feature-uptake)
- [7. Watch](#7-watch)
- [8. WON'T for now — and why](#8-wont-for-now--and-why)

---

## 1. Where we are

| Axis | Shipped |
|---|---|
| **Channels (6)** | In-memory, Redis (+ Valkey / Dragonfly / Garnet), NATS, PostgreSQL, SQL Server, MongoDB |
| **Transports (11)** | In-memory, Redis Streams, RabbitMQ, Azure Service Bus, Google Pub/Sub, NATS JetStream, PostgreSQL, Kafka, SQL Server, AWS SQS, MongoDB |
| **Durable-flow stores (10)** | In-memory, SQL Server, PostgreSQL, MySQL, SQLite, Oracle, MongoDB, Cosmos DB, DynamoDB, EF Core |

The backend matrix is essentially complete: every mainstream .NET messaging stack can run
AsyncResponse today without writing an adapter. The two-axes rule from #14/#17 still governs
every backend verdict below — a **transport** needs competing consumers, acks, redelivery, and
dead-lettering; a **channel** needs targeted wake, a per-key-TTL recovery KV, and a
delivery-confirmation/claim protocol; and the durable-flow **store** is a third, explicitly
separate axis. A great transport is usually a poor channel, and vice versa.

What shifted: with the matrix filled, **the frontier has moved from backends to capabilities**.
Temporal, the Azure Durable Task Scheduler, Dapr, and now AWS Lambda all ship durable timers;
none of them ships AsyncResponse's "plain `await`s, no replay rules" model. The competitive gap
is no longer "does it run on my broker" — it is what a flow can *do* while it runs. That is
Train A. The remaining one item of the old train 2 — the store-mixing enabler — moves to
Train B, where it gates the candidates that need it.

---

## 2. The bar for admission (definition of done)

Every shipped package has set a quality bar; a new backend that can't meet it shouldn't ship.
Capability packages (Train A) adopt the same gates wherever they apply — tests, observability,
wire discipline, docs. For each new package:

- **Adapter seam** over the vendor SDK (`I<Backend>Client`-style internal interfaces) so unit
  tests run on fakes/Moq — the pattern every existing transport uses.
- **Unit tests** in `tests/AsyncResponse.Tests` covering the dispatcher hot path, ack/redelivery
  semantics, options validation, and failure paths.
- **Integration tests** in the Aspire fixture against a real container or official emulator,
  including a dedicated **early-ACK app variant** (every existing transport has one).
- **Both ACK modes** with the standard contract: safe ack-after-handler default; opt-in early
  ACK requiring explicit `BackgroundWorkerCount`/`BackgroundQueueCapacity`, drain budget
  validated against host shutdown, `OnBackgroundFailure` surfaced.
- **Bounded redelivery + dead-lettering**, or an explicit, documented delegation (as Google
  Pub/Sub delegates to its subscription's `DeadLetterPolicy`).
- **Correlation id** carried in broker metadata with `CorrelationIdJsonPaths` fallback.
- **Observability**: publish *and receive* spans with OTel messaging attributes (every shipped
  transport now emits both).
- **Wire contract untouched**: schema-versioned envelopes pass through opaquely.
- **Docs**: options in `configuration.md`, row in the README matrix, a stress-harness scenario,
  an NBomber load-test profile — **and every per-provider list the package touches**:
  `recovery.md` (durable-channel lists), `observability.md` (span table), `sample.md` (accepted
  configuration values and recovery endpoints), and `troubleshooting.md` when the package brings
  a new gotcha. The July 2026 documentation pass fixed exactly this drift class after SQL Server
  and MongoDB shipped; the bar now names it so it cannot recur silently.

Effort estimates below are relative to shipped packages: RabbitMQ ≈ 1.6k LOC, ASB ≈ 1.4k LOC,
Google Pub/Sub ≈ 1.2k LOC, the PostgreSQL channel+transport pair ≈ 3.2k LOC (plus tests).

---

## 3. Train 0 — hardening (now)

The 2026-07-28 full-codebase review produced a wave of correctness fixes — wake retry for stuck
flows, registration-failure rethrow, SQS early-ACK gating, unified drain, heartbeat upserts,
server-clock lease math, and friends — tracked in [CHANGELOG.md](../CHANGELOG.md) under
*Unreleased*. Two structural follow-ups closed the review's systemic findings rather than its
individual bugs; both have landed:

- **Channel-contract conformance suite (unit + integration derivations).** 🟢 The in-memory
  channel is the de-facto behavioral spec, and the review found three database ports that had
  drifted from it in different ways. One shared test suite, run against every
  `IAsyncResponseChannel` implementation, turns "the in-memory channel happens to be the
  reference" into an enforced contract — the same move the flow stores already made with the
  atomic store contract tests.
- **Per-transport semantics matrix.** 🟢 Drain behavior on shutdown, what happens when a handler
  fails *after* an early ACK, and how delivery attempts are counted differ per transport for
  good reasons — those differences are now documented in one place,
  [transport-semantics.md](transport-semantics.md), derived from the transport source rather
  than prose scattered across `configuration.md`.

---

## 4. Train A — capabilities (the new headline)

### 4.1 Durable timers + delayed steps → cron-scheduled flows 🔴

The loudest competitive gap. Every neighboring system ships durable timers: Temporal always has;
the Azure Durable Task Scheduler's Consumption SKU went GA in March 2026 and is
Aspire-integrated; Dapr's Jobs API went stable in 1.18 (June 2026); AWS shipped Lambda durable
functions in December 2025 — notably with **no .NET runtime support at launch**, which is an
opportunity, not just a threat. "Sleep for 3 days inside a flow" is the first question every
workflow-shaped evaluation asks, and today the honest answer is "use your scheduler".

Design, two layers so every backend works:

| Layer | Mechanics |
|---|---|
| **`IDelayedPublish` transport capability (optional)** | Native delayed delivery where the broker has it: Azure Service Bus scheduled messages; SQS `DelaySeconds` (≤ 15 min — chunked re-delays for longer waits); NATS 2.12+ per-message delivery schedules (NATS 2.14 even runs server-side cron); a `visible_at` column/field on the PostgreSQL / SQL Server / MongoDB queue tables |
| **Store-driven sweep fallback** | Timers persisted in the flow store and woken by a sweep reusing the existing lease machinery — works on every backend, including Kafka, where the log has no per-message delay |

Delayed steps (`flow.DelayAsync(...)`, delayed `EnqueueWorkerAsync`) land first; cron-scheduled
flows (start a flow on a schedule) build on the same primitive. The transport capability is
opt-in per package, so shipping the fallback first unblocks every backend at once.

### 4.2 `AsyncResponse.Testing` package 🔴

Table stakes the moment timers exist — nobody will wait three real days in a test. Temporal's
time-skipping test server is the bar to meet:

- **Virtual clock / time-skipping** for waits, timers, leases, and watchdog schedules.
- **Flow-test harness** — run a flow against the in-memory stack with scripted responses,
  assert checkpoints and outcomes without brokers or sleeps.
- **Deterministic crash-at-checkpoint helpers** — kill the "process" at step N, restart,
  assert resume — the pattern our own integration tests use, packaged for applications.
- **CI adoption of the matured Azure Service Bus emulator** (already in the Aspire fixture) as
  the template for emulator-first test recipes in the docs.

### 4.3 Claim-check payload seam — `IPayloadStore` 🟠

Large payloads do not belong in response envelopes or the flow ledger. A small seam —
`IPayloadStore` with S3 / Azure Blob / GridFS providers — moves oversized payloads to blob
storage and passes references on the wire, transparently on publish and materialization. The
market moved here too: Temporal productized exactly this as "External Storage" at Replay 2026,
and SQS raised its maximum payload to 1 MB in January 2026 (which helps, and also signals where
payloads are heading). This is also the structural answer to flow-ledger size limits: the
`MaxStateBytes` guard (Train 0) tells you when you hit the wall; claim-check is how you stop
hitting it.

### 4.4 Flow operations API + observability pack 🟠

Operating flows in production today means querying the store by hand. Two deliverables:

- **`IDurableFlows` operations surface** — list/query runs by status, flow type, and age;
  cancel a run; the existing `GetStateAsync`/`ResumeAsync` complete the set. This is the API
  the sample's endpoints and any future dashboard both sit on.
- **Observability pack** — per-transport DLQ and background-failure counters to complement the
  existing recovery gauges, plus a shippable Grafana dashboard pack over the OTel metrics. A
  full web dashboard is a product in its own right — Wolverine spun theirs out as CritterWatch —
  and the OTel pack captures most of the operational value at a fraction of the effort.

---

## 5. Train B — backends, demand-paced

The matrix is complete enough that new backends ship on demand signals, not on principle.

### 5.1 Hangfire transport — `AsyncResponse.Transports.Hangfire` 🟠

Scope unchanged from the previous roadmap: Hangfire is purely the worker-execution half with no
correlation ambitions, so it slots into `IWorkerTransport` with zero conceptual conflict —
**durable workers with no broker at all**, on the SQL database mid-market shops already run.
Re-verified July 2026: Hangfire core is LGPLv3 (fine as an opt-in package where the user already
chose Hangfire), actively maintained (1.8.24, July 2026), 383 M downloads. Caveats to state
plainly: transport only (pair with any channel); dispatch latency is DB-polling-grade, not
broker-grade. Effort: small — retries and queues are Hangfire's semantics, not ours; map
`MaxDeliveryAttempts` onto `AutomaticRetryAttribute` and route final failures to
`OnBackgroundFailure` + a failed-jobs convention.

### 5.2 Azure Storage Queues transport 🟡

The demand picture changed: `Azure.Storage.Queues` runs ~292 K downloads/day — roughly 90 % of
Azure Service Bus's rate (a share of it inflated by Azure Functions bindings, so read it as a
floor-to-ceiling range, not a point). Technically it is a near-clone of the shipped SQS port:
visibility-timeout settlement, `DequeueCount` for attempts, no broker DLQ (library-managed,
as Redis Streams already does). Positioned honestly: **the cheap-Azure option** for
cost-sensitive workloads; Azure Service Bus stays the default recommendation for its DLQ,
scheduled messages, and duplicate detection.

### 5.3 Store-mixing enabler → MQTT 5 channel + Cosmos DB / DynamoDB recovery stores 🟠 / 🟡

`IRecoveryStateStore` is already a separately registered interface — each durable channel just
bundles its own. Formalizing **"channel = wake mechanism + recovery store"** as a supported
composition (e.g. `.WithMqttChannel(...).WithCosmosRecoveryStore(...)`) unlocks every candidate
with great delivery but no KV, and every great KV with no targeted wake. Guardrails, per the
project's DX-first stance: curated one-dependency pairings stay the documented defaults; mixing
is an *advanced* opt-in with a startup validator that refuses half-configured combinations. The
watchdog, health check, and schema-versioned `RecoveryState` wire format already work over any
`IRecoveryStateStore`, so this is registration-surface and documentation work (🟠).

First consumers, both 🟡 until their demand trigger fires:

- **MQTT 5 channel** — MQTT 5 has *native* request/response (`ResponseTopic` +
  `CorrelationData`), topic-per-cid wake, QoS 1. The client risk cited in the previous roadmap
  is gone: **MQTTnet v5 is stable and lives under the dotnet org**. No broker KV → needs
  store-mixing. "Await a device" stays a story nobody else in this space tells.
- **Cosmos DB / DynamoDB recovery stores** — durable KV+TTL behind `IRecoveryStateStore`; their
  change feeds/streams are shard-polled, **not** targeted wakes — they are stores, not channels,
  and ship only as the store half of a mixed channel.

### 5.4 Redis/Valkey and NATS KV durable-flow stores 🟡

The last asymmetry in the matrix: "run everything on Redis" or "run everything on NATS" works
for the channel and the transport but not the flow ledger, which today requires a database. Both
targets meet the atomic store contract: NATS KV has native per-revision compare-and-set (the
same primitive the NATS channel's recovery store already uses), and Redis gets CAS via the
existing script/transaction patterns — with the durability caveat documented honestly
(ElastiCache now runs Valkey 9 with synchronous-durability options, which materially improves
the story on the biggest managed deployment). Demand-paced: promote when "all-Redis / all-NATS
stack" asks arrive with flow-store requirements attached.

### 5.5 MassTransit migration recipe (docs) 🟠 — bridge 🟡

The exodus is now on a clock: MassTransit v8 community support ends **end-2026**, the OpenTransit
fork states it will not be production-ready before then, and Massient (commercial v9) prices at
$400–1,200/mo. **Publish the migration recipe in H2 2026** — request/response and saga-shaped
patterns mapped onto durable flows, side-by-side, while evaluations are actually happening. The
`IWorkerTransport` bridge over `IPublishEndpoint`/`IConsumer` remains possible but ships only on
concrete demand: building an adapter onto a sunsetting base is the weaker play; the recipe is
the durable asset either way.

---

### 5.4 Claim-sequence delivery watermark for the DB channels 🟡

The DB channels' delivery watermark is timestamp-based, and a same-tick tie between a waiter's
registration and another process's claim is deliberately resolved toward exclusion (at-most-once;
see the `IsWithinWatermark` XML doc for the full trade — the excluded waiter stalls out its
timeout). No timestamp can separate that tie; the durable fix is an identity: a monotonic
sequence shared by registrations and claims (PG/SQL Server sequence, MongoDB counter collection),
with the watermark comparing sequence numbers instead of clocks. Schema + query change across all
three stores; do it in one train with a store-schema version bump, only if the trade shows up in
practice.

## 6. Cheap 2025-26 feature uptake

Small items (XS–S each) that keep shipped packages current with their platforms:

| Item | Size | Status | What and why |
|---|---|---|---|
| **SQS fair queues** | XS | 🟠 | Per-`MessageGroupId` fairness on *standard* queues (AWS, July 2025) — document it as the answer to noisy-neighbor correlation ids, no code change required; also document the new 1 MB payload ceiling (January 2026). |
| **Redis 8.4 `XREADGROUP … CLAIM`** | S | 🟠 | Collapses the streams claim loop (`XPENDING` + `XCLAIM` reclaim pass) into the read itself. Adopt **feature-detected only** — the transport must keep running unchanged on Valkey, Dragonfly, and older Redis, protecting the compatibility matrix the CI job validates. |
| **NATS batch publish** | XS | 🟡 | NATS 2.14 adds batched JetStream publishes — an easy throughput win for the NATS transport's hot publish path when the connected server supports it. |
| **Valkey 9 in the CI matrix** | XS | 🟠 | Bump the weekly scheduled compatibility job from Valkey 8 to Valkey 9. |

---

## 7. Watch

Items with a real trigger, deliberately not being built yet:

- **Kafka share groups (KIP-932).** Still Java-only in practice: librdkafka ships only a
  *preview* of the share consumer, `confluent-kafka-dotnet` 2.15.0 (June 30, 2026) contains zero
  share-consumer mentions, and non-Java support remains targeted at H2 2026. The Kafka
  transport's adapter seam was designed so a `ShareGroup` consumption mode can be added without
  breaking options — keep that plan, build nothing until the .NET client actually lands.
  Share groups remove Kafka's head-of-line caveat when they arrive.
- **RabbitMQ Streams transport.** Now *feasible* — the official `RabbitMQ.Stream.Client` hit
  1.12 (June 2026) and is production-grade — but streams are Kafka-shaped (offset log, no
  per-message ack), so it would inherit the same in-process retry + head-of-line caveats as the
  Kafka transport while serving an audience the classic-queues RabbitMQ transport already
  covers. Demand-thin: 🟡 COULD, revisit on concrete asks.

---

## 8. WON'T for now — and why

Kept deliberately; it answers issues quickly. Refreshed July 2026.

- **Kafka as a channel** — no targeted wake, no per-key TTL KV; compacted topics require full
  materialization. Transport only, by design.
- **Azure Event Hubs / Amazon Kinesis** — partitioned logs with checkpointing: no per-message
  ack, no broker DLQ, no redelivery of a single message. Wrong shape for a job queue; ingestion
  streams, not work queues.
- **Apache Pulsar** — excellent primitives (per-message ack, NACK-with-delay, retry/DLQ
  topics), and `DotPulsar` is actively maintained (5.3.1) — but .NET demand remains a rounding
  error: 2.3 M total downloads vs `Confluent.Kafka`'s 252.7 M, about 1 %. The trigger
  ("the .NET client story improves *and* demand appears") has not fired.
- **Garnet streams** — Garnet remains a validated **channel-only** server: stream-command
  support upstream exists only as an unmerged draft PR. The transport claim stays off until
  streams actually land in a Garnet release.
- **ActiveMQ / Artemis, IBM MQ, Solace** — declining or commercial-niche enterprise brokers;
  weak/awkward .NET clients; build only against sponsored demand.
- **EventStoreDB** — event log + subscriptions is the wrong primitive for a cid rendezvous, and
  its DDD/CQRS audience already lives in that model.
- **Hazelcast / Ignite** — technically meet the channel bar, but second-class .NET clients and
  heavy operational footprint.
- **Consul / etcd / ZooKeeper** — coordination stores: tiny values, watch semantics tuned for
  config, not payload fan-out.
- **Memcached** — no pub/sub, volatile only; strictly worse than the Redis channel.
- **ZeroMQ / gRPC / WebSockets** — brokerless or point-to-point; no durability, no competing
  consumers. gRPC/webhook *responses* already enter through `IAsyncResponseIngress` — that's an
  ingress recipe, not a transport.
- **NServiceBus adapter** — fully commercial audience that already owns sagas/outbox; our
  differentiator lands softest exactly there. Only on a paying customer's request.
- **A full flow-operations web dashboard** — a product, not a feature (see Wolverine →
  CritterWatch). The Train A observability pack + `IDurableFlows` API capture the operational
  value; revisit only if the API's adoption proves out a UI audience.

---

### Sources

- Issues consolidated: [#14](https://github.com/Sky4CE/AsyncResponse/issues/14),
  [#17](https://github.com/Sky4CE/AsyncResponse/issues/17) (original NuGet download figures,
  June 2026; updated figures in this document are as of July 2026)
- Kafka share groups status:
  [Confluent — Kafka Queue Semantics Now GA](https://www.confluent.io/blog/kafka-queue-semantics-share-consumer-ga/),
  [Apache Kafka 4.2.0 release announcement](https://kafka.apache.org/blog/2026/02/17/apache-kafka-4.2.0-release-announcement/),
  [Confluent Cloud — share consumers (Java clients only)](https://docs.confluent.io/cloud/current/client-apps/share-consumers.html),
  [KIP-932](https://cwiki.apache.org/confluence/display/KAFKA/KIP-932%3A+Queues+for+Kafka),
  [librdkafka INTRODUCTION (share-consumer preview status)](https://github.com/confluentinc/librdkafka/blob/master/INTRODUCTION.md),
  [confluent-kafka-dotnet releases (2.15.0, 2026-06-30)](https://github.com/confluentinc/confluent-kafka-dotnet/releases),
  [librdkafka status via Karafka docs](https://karafka.io/docs/Development-KIP-932-Rdkafka/)
- MassTransit landscape:
  [Announcing MassTransit v9](https://masstransit.io/introduction/v9-announcement),
  [Massient (commercial v9) pricing](https://massient.com),
  OpenTransit fork announcement (dev.to)
- Hangfire: [hangfire.io/licenses](https://www.hangfire.io/licenses.html),
  [nuget.org/packages/Hangfire.Core](https://www.nuget.org/packages/Hangfire.Core)
- NATS: NATS Server 2.12 what's-new (per-message delivery schedules; docs.nats.io release
  notes), NATS 2.14 release blog (server-side cron, batch publish; nats.io/blog)
- Timers/competitive: Temporal Replay 2026 product announcements (External Storage,
  time-skipping test tooling; temporal.io/blog),
  Durable Task Scheduler Consumption SKU GA (Microsoft Tech Community, March 2026) and
  Aspire 13.3 what's-new (learn.microsoft.com),
  [Dapr v1.18 release blog](https://blog.dapr.io) (Jobs API stable),
  AWS What's New — Lambda durable functions (December 2025; aws.amazon.com/about-aws/whats-new)
- SQS: AWS What's New — SQS fair queues (July 2025) and 1 MB payloads (January 2026;
  aws.amazon.com/about-aws/whats-new)
- Redis/Valkey: Redis 8.4 what's-new (`XREADGROUP … CLAIM`; redis.io),
  AWS blog — ElastiCache for Valkey 9 (synchronous durability; aws.amazon.com/blogs)
- NuGet download figures (as of July 2026):
  [Azure.Storage.Queues](https://www.nuget.org/packages/Azure.Storage.Queues),
  [MQTTnet](https://www.nuget.org/packages/MQTTnet),
  [DotPulsar](https://www.nuget.org/packages/DotPulsar),
  [Temporalio](https://www.nuget.org/packages/Temporalio)
