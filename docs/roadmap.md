# Channels & transports roadmap

This document consolidates the investigation from
[#14 — Support more transports](https://github.com/Sky4CE/AsyncResponse/issues/14) and
[#17 — Support more channels](https://github.com/Sky4CE/AsyncResponse/issues/17) into one
prioritized plan: what to implement, why, in what order, and what "done" means for each item.
Popularity figures are NuGet direct-download counts as of June 2026 (from #14); Kafka client
facts were re-verified July 2026 (sources at the end).

**Status legend:** 🟢 shipped · 🔴 MUST (next release train) · 🟠 SHOULD · 🟡 COULD
(demand-driven) · ⚫ WON'T for now (revisit on demand).

---

## 1. Where we are

| Axis | Shipped | Coverage gap |
|---|---|---|
| **Channels** | In-memory, Redis, NATS, PostgreSQL | SQL Server shops; teams standardized on a Redis *fork* wanting an explicit compatibility statement |
| **Transports** | In-memory, Redis Streams, RabbitMQ, Azure Service Bus, Google Pub/Sub, NATS JetStream, PostgreSQL | **Kafka** (the single most-requested broker), **all of AWS**, broker-free durable execution (Hangfire-style) |

Three of the original #14/#17 top picks — Azure Service Bus, the NATS pair, and the PostgreSQL
pair — have shipped since that investigation was written. This roadmap re-baselines on what
remains.

The loudest signal in both issues, restated: **the two axes must not be conflated.**

- A **transport** moves `WorkerJobEnvelope`s and inbound responses: it needs competing
  consumers, explicit acks, bounded redelivery, dead-lettering, and a place to carry the
  correlation id.
- A **channel** is a response rendezvous: it needs **(1)** targeted, low-latency wake of the one
  waiter keyed by an arbitrary correlation id, **(2)** a durable point-lookup KV with per-key
  TTL for recovery state, and **(3)** either an "is anyone listening?" signal (NATS
  no-responders, Redis receiver count) *or* a delivery-confirmation/claim protocol (the
  PostgreSQL channel's `acked_at`/`recovery_claimed` design) so a live waiter and the recovery
  path can never both handle one response.

A great transport is usually a poor channel and vice-versa. Kafka and SQS are superb transports
and structurally wrong channels; that boundary drives most verdicts below.

---

## 2. The bar for admission (definition of done)

Every shipped package has set a quality bar; a new backend that can't meet it shouldn't ship.
For each new package:

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
- **Observability**: publish *and receive* spans with OTel messaging attributes (the NATS and
  PostgreSQL transports currently lack receive spans — new packages should not repeat that; fix
  those two while at it).
- **Wire contract untouched**: schema-versioned envelopes pass through opaquely.
- **Docs**: options in `configuration.md`, row in the README matrix, a stress-harness scenario,
  and an NBomber load-test profile.

Effort estimates below are relative to shipped packages: RabbitMQ ≈ 1.6k LOC, ASB ≈ 1.4k LOC,
Google Pub/Sub ≈ 1.2k LOC, the PostgreSQL channel+transport pair ≈ 3.2k LOC (plus tests).

---

## 3. Priority ladder

### 🔴 MUST — release train 1

#### 3.1 Kafka transport — `AsyncResponse.Transports.Kafka`

The #1 ask in #14 (242M `Confluent.Kafka` downloads) and the largest single audience gap.
Protocol compatibility means one package also unlocks **Redpanda, Amazon MSK, WarpStream, Aiven,
Confluent Cloud**.

**Design decision (verified July 2026):** *build on classic consumer groups now.* KIP-932
"Queues for Kafka" (share groups — true per-message ack queue semantics) is GA as of Kafka
4.2 / Confluent Cloud, **but only for Java clients**; librdkafka — and therefore
`Confluent.Kafka` — does not support it yet (targeted H2 2026). Design the adapter seam so a
`ShareGroup` consumption mode can be added later without breaking options.

Semantics mapping:

| AsyncResponse contract | Kafka mechanics |
|---|---|
| Worker/response queues | One topic per role (`WorkerTopic`, `ResponseTopic`), consumer group per role |
| `AckAfterHandlerCompletes` | Manual offset commit *after* the handler returns; process partition-serially per assignment |
| `AckAfterEnqueue` (early ACK) | Commit after bounded in-process enqueue; the standard early-ACK contract applies |
| Bounded redelivery | In-process retry with backoff (offsets can't NACK); after `MaxDeliveryAttempts`, produce to the DLQ topic and commit |
| Dead-letter | `<topic>.deadletter` topic, original headers + failure reason preserved |
| Backpressure | `Pause`/`Resume` on assigned partitions when the in-process queue is full |
| Correlation id | Kafka message header (`CorrelationIdHeader`), JSON-path fallback |

Honest caveats to document: ordering is per-partition, so parallelism = partition count, not
worker count; a slow message delays its partition (head-of-line) — this is inherent to classic
consumer groups and exactly what share groups will fix later. **Explicitly not a channel** —
the "compacted topic as recovery KV" idea from #17 requires materializing the whole topic,
provides no targeted wake and no per-key TTL; rejected.

Integration tests: single-broker KRaft container (and it doubles as a Redpanda compatibility
check). Effort: ~RabbitMQ + 30% (retry/DLQ machinery is on us, as it was for Redis Streams).

#### 3.2 AWS SQS transport — `AsyncResponse.Transports.SQS`

The only whole-cloud gap: Azure and GCP are covered, AWS is not (313M `AWSSDK.SQS` downloads).
SQS is the cleanest conceptual fit of any managed queue:

| AsyncResponse contract | SQS mechanics |
|---|---|
| Worker/response queues | Two SQS queues; long-poll `ReceiveMessage` (up to 10/batch) |
| `AckAfterHandlerCompletes` | `DeleteMessage` after the handler; failure → let visibility timeout expire (or `ChangeMessageVisibility` for `RedeliveryDelay`) |
| `AckAfterEnqueue` | Delete after bounded enqueue; standard early-ACK contract |
| Bounded redelivery | Native: `ApproximateReceiveCount` + queue **redrive policy** |
| Dead-letter | Native DLQ via redrive policy — document as the default; no library-managed DLQ needed |
| Correlation id | Message attribute, JSON-path fallback |
| FIFO queues | Supported via options (`MessageGroupId` = correlation id); documented as opt-in |

SNS is *not* needed for the core model (fan-out is not our shape) — defer any SNS ingress to
demand. Document the **AWS recipe** in the README alongside the package: SQS transport + Redis
channel on ElastiCache/MemoryDB *or* PostgreSQL channel on RDS/Aurora — full AWS-native stack
with zero new package work on the channel side.

Integration tests: LocalStack container. Effort: ~Google Pub/Sub (the SDK does the heavy
lifting).

#### 3.3 Valkey / Garnet / Dragonfly compatibility validation (no new package)

Near-free, strategically timely after the 2024 Redis relicensing. The existing Redis channel
and transport speak RESP via `StackExchange.Redis`, which these servers target.

Work item: run the existing Redis unit-relevant integration suites against **Valkey** and
**Garnet** containers (Dragonfly best-effort), fix or document any divergence (Garnet's
pub/sub and `SCAN`/keyspace behavior are the risk areas; the transport also needs stream
commands — verify `XADD`/`XREADGROUP`/`XAUTOCLAIM` parity per server), then:

- add a CI matrix job (Redis + Valkey at minimum),
- state supported servers in the README and `configuration.md`,
- mention ElastiCache/MemoryDB explicitly for the AWS story.

Effort: days, not weeks. Marketing value exceeds the code cost.

### 🟠 SHOULD — release train 2

#### 3.4 SQL Server channel + transport — `AsyncResponse.Channels.SqlServer` / `AsyncResponse.Transports.SqlServer`

The largest remaining enterprise-.NET audience (#17's #2). Mirrors the PostgreSQL pair, which
de-risks the design — most decisions are already made and battle-tested:

- **Transport**: queue table claimed with `UPDLOCK, ROWLOCK, READPAST` (SQL Server's
  `SKIP LOCKED` equivalent), fenced acks via `lock_id`, idempotent publish
  (`WHERE NOT EXISTS` upsert), single-transaction dead-lettering — all direct ports of the
  PostgreSQL store.
- **Channel wake**: SQL Server has no `LISTEN/NOTIFY`. **Start with adaptive polling** (the PG
  channel already has the sweep machinery to reuse; make the interval tighter when waiters are
  active, coarser when idle). **Defer Service Broker** — it's powerful but operationally
  unloved, frequently disabled by DBAs, and `SqlDependency` is effectively legacy; add it later
  as an opt-in wake mechanism if users ask.
- **Recovery**: row-per-registration table with DB-clock expiry (`SYSUTCDATETIME()`), same
  claim-pair (`acked_at`/`recovery_claimed`) delivery arbitration as PostgreSQL.

Integration tests: `mcr.microsoft.com/mssql/server` container. Effort: the largest single item
(~PG pair). Split across two releases if needed — transport first (it's the simpler half and
useful standalone with the Redis/NATS channel).

#### 3.5 Hangfire transport — `AsyncResponse.Transports.Hangfire`

The best adapter fit from #14's analysis: Hangfire is purely the worker-execution half with no
correlation ambitions, so it slots into `IWorkerTransport` with zero conceptual conflict.
`PublishAsync` enqueues a Hangfire job that calls the ingress; Hangfire brings durable storage,
automatic retries, and its dashboard — **durable workers with no broker at all**, on the SQL
database mid-market shops already run.

Caveats to state plainly: transport only (pair with any channel); dispatch latency is
DB-polling-grade, not broker-grade; Hangfire core is **LGPL** — fine as an opt-in package where
the user already chose Hangfire. Effort: small (the job-storage semantics — retries, queues —
are Hangfire's, not ours; map `MaxDeliveryAttempts` onto `AutomaticRetryAttribute` and route
final failures to `OnBackgroundFailure` + a failed-jobs convention).

### 🟡 COULD — demand-driven, keep on the radar

| Candidate | Shape | Why it waits | Trigger to promote |
|---|---|---|---|
| **MongoDB** channel (+transport) | Change streams (targeted `$match` on cid) for wake; collections for recovery; `findOneAndUpdate` claim loop as a queue | Change streams require a replica set; .NET+Mongo demand is real but a tier below SQL Server | Issue traffic / sponsor |
| **MassTransit v8 bridge** | `IWorkerTransport` over `IPublishEndpoint` + an `IConsumer` feeding ingress | v8 is Apache-2.0 but frozen (v9 went commercial, ~$400–1,200/mo); building on a sunsetting base. The positioning win ("durable awaits without sagas, no v9 license") may exceed the adapter's value — a docs/blog recipe might suffice | MassTransit-refugee demand |
| **Azure Storage Queues** transport | Visibility-timeout queue, `DequeueCount` for attempts | ASB already covers Azure; Storage Queues only win on cost/simplicity | User ask |
| **Cosmos DB / DynamoDB recovery stores** | Durable KV+TTL (native TTL on both) behind `IRecoveryStateStore` | Their change feeds/streams are shard-polled, **not** targeted wakes — they're stores, not channels. Blocked on the store-mixing enabler (§4) | Store-mixing ships |
| **MQTT 5 channel** (IoT) | MQTT 5 has *native* request/response: `ResponseTopic` + `CorrelationData`; topic-per-cid wake; QoS 1 | No KV in the broker → needs store-mixing; niche audience, but a genuinely unique "await a device" story | Store-mixing + IoT demand |

### ⚫ WON'T for now — and why (keep this list; it answers issues quickly)

- **Kafka as a channel** — no targeted wake, no per-key TTL KV; compacted topics require full
  materialization. Transport only, by design.
- **Azure Event Hubs / Amazon Kinesis** — partitioned logs with checkpointing: no per-message
  ack, no broker DLQ, no redelivery of a single message. Wrong shape for a job queue; ingestion
  streams, not work queues.
- **Apache Pulsar** — excellent primitives (per-message ack, NACK-with-delay, retry/DLQ
  topics), but `DotPulsar` trails the Java client and .NET demand is a rounding error next to
  Kafka. Revisit if the .NET client story improves.
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

---

## 4. Architectural enabler: first-class store-mixing

`IRecoveryStateStore` is already a separately registered interface — each durable channel just
happens to bundle its own. Formalizing **"channel = wake mechanism + recovery store"** as a
supported composition (e.g. `.WithMqttChannel(...).WithPostgreSqlRecoveryStore(...)`) unlocks
every candidate above that has great delivery but no KV (MQTT 5, ASB sessions-as-channel,
Web PubSub) and every candidate that is a great KV but has no targeted wake (Cosmos DB,
DynamoDB, any RDBMS).

Guardrails, per the project's DX-first stance: the curated one-dependency pairings stay the
documented defaults; mixing is an *advanced* opt-in with a startup validator that refuses
half-configured combinations (same fail-fast philosophy as `AddAsyncResponse()` today). The
watchdog, health check, and schema-versioned `RecoveryState` wire format already work over any
`IRecoveryStateStore`, so the engine needs no changes — this is registration-surface and
documentation work. Schedule alongside train 2.

---

## 5. Suggested sequence

| Train | Items | Rationale |
|---|---|---|
| **1** | Kafka transport · SQS transport · Valkey/Garnet validation (+ NATS/PG receive-span gap fix) | Closes the two loudest gaps (Kafka, AWS) plus a near-free compatibility claim; README matrix gets its biggest wins |
| **2** | SQL Server transport → SQL Server channel · store-mixing enabler | Largest build; transport half ships value early; store-mixing lands with a second consumer (SQL Server recovery store) to prove it |
| **3** | Hangfire transport · MassTransit-v8 recipe or bridge | Broker-free durable execution + opportunistic positioning during the v9 exodus |
| **Watch** | librdkafka share-group support (H2 2026) → add Kafka `ShareGroup` mode · Mongo/MQTT/Cosmos per demand | Share groups remove Kafka's head-of-line caveat; revisit COULD tier quarterly |

Every item ships against the §2 definition of done; nothing ships without an emulator/container
integration suite and an early-ACK variant.

---

### Sources

- Issues consolidated: [#14](https://github.com/Sky4CE/AsyncResponse/issues/14),
  [#17](https://github.com/Sky4CE/AsyncResponse/issues/17) (NuGet download figures, June 2026)
- Kafka share groups GA (Java-only) and client support:
  [Confluent — Kafka Queue Semantics Now GA](https://www.confluent.io/blog/kafka-queue-semantics-share-consumer-ga/),
  [Apache Kafka 4.2.0 release announcement](https://kafka.apache.org/blog/2026/02/17/apache-kafka-4.2.0-release-announcement/),
  [Confluent Cloud — share consumers (Java clients only)](https://docs.confluent.io/cloud/current/client-apps/share-consumers.html),
  [KIP-932](https://cwiki.apache.org/confluence/display/KAFKA/KIP-932%3A+Queues+for+Kafka),
  [librdkafka status via Karafka docs](https://karafka.io/docs/Development-KIP-932-Rdkafka/)
- MassTransit v9 commercial licensing:
  [Announcing MassTransit v9](https://masstransit.io/introduction/v9-announcement)
