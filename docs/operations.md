# Operations

[← Back to README](../README.md)

This page covers running AsyncResponse well in production and in development: the operational best
practices distilled from the rest of the docs, how to build and run the test suites, and how to
benchmark and load-test the library (micro-benchmarks, the stress harness, and the NBomber
end-to-end profiles).

- [Best practices](#best-practices)
- [Building and testing](#building-and-testing)
- [Benchmarking and load testing](#benchmarking-and-load-testing)

## Best practices

1. **Always make the send the trigger** (the `WaitAsync` argument). Sending before subscribing is a
   race: a fast first response finds nobody listening and, on first registration, no recovery state
   either.
2. **Use reply targets for generic response topics.** If the remote system needs reply-to metadata,
   call `.WithReplyTarget()` and pass the `AsyncResponseRequestContext` into the trigger. Transport
   packages own how native destinations become reply targets.
3. **Decide recovery routing honestly.** Override `OnRecovery()` on **every** payload type you
   register lost-subscriber recovery callbacks for — every channel, the in-memory one included,
   fails fast at waiter creation without it. That includes success-only payloads
   (`=> RecoveryAction.Resume`) and progress-only checkpoints (`=> RecoveryAction.KeepWaiting`):
   `Fail` for states that must not resume, `KeepWaiting` for non-terminal checkpoints so they don't
   consume the registration before the terminal response. It is independent of your `Until`
   predicate, which owns live completion — "what does this result do to the flow?" versus "is the
   operation done?". See [recovery.md](recovery.md).
4. **Register both recovery callbacks** for any flow that must survive redeploys. A failed payload
   with no failure callback is logged and dropped — never resumed — but dropped is still a stuck
   flow.
5. **Make resume callbacks re-entrant.** A resume may re-trigger a flow whose step is still running
   remotely; resume should *re-attach* (subscribe to the same correlation id) rather than re-execute
   side effects. Persist enough state to tell the difference.
6. **Treat callback method names and the `KeyPrefix` as deployment contracts.** They are persisted;
   rename with a migration window.
7. **Set timeouts per flow.** The default (`DefaultTimeout = null` falls back to
   `RecoveryStateExpiry`, 7 days on the durable channels) is a backstop, not a recommendation; a
   payment flow should fail in minutes.
8. **Run the watchdog in exactly one host per durable store** and alert on its warnings or the
   `Degraded` health status — stale recovery state is your earliest signal of stuck flows.
9. **Mind channel wakeup semantics.** Redis pub/sub delivery is at-most-once to live subscribers, and
   PostgreSQL `NOTIFY` is only a wakeup signal; the durable response/recovery state is what makes
   the system safe across gaps. Keep `RecoveryStateExpiry` above your longest flow duration.
10. **Reuse client singletons.** Reuse your application's existing Redis `IConnectionMultiplexer`,
    NATS connection, or PostgreSQL `NpgsqlDataSource`; don't create a second pool for AsyncResponse.
11. **Share correlation ids deliberately.** Live delivery and lost-subscriber recovery both fan out
    across multiple waiters on one correlation id; a normally completing waiter removes only its own
    recovery registration. The database channels' edge cases are in
    [recovery.md](recovery.md#shared-correlation-recovery).
12. **Measure hot paths in isolation before comparing profiles.** The sample's remote simulator
    deliberately waits before progress and terminal messages, so broad HTTP load-test latency mostly
    reflects sample workflow timing. Use the micro-benchmarks, stress harness, and NBomber
    `--scenario` filter to separate library overhead from demo behavior.
13. **Choose exactly one atomic flow store.** `AddAsyncResponse()` does not select one implicitly,
    and startup rejects a missing or duplicate choice. Every built-in `AsyncResponse.DurableFlows.*`
    store and every custom `IFlowStateStore` must provide atomic start, revision-checked
    checkpoints, and a renewable execution lease. Use `.WithInMemoryDurableFlows()` only when process-local state is
    intentional.
14. **Treat queue capacity as backpressure, not an error.** In-memory workers and internal
    per-correlation dispatch queues are bounded and publishers wait asynchronously when full. Size
    `InMemoryWorkerTransportOptions.QueueCapacity` for the burst you accept and raise `WorkerCount`
    only when jobs are safe to run concurrently. Jobs published from inside a running job never wait
    (the worker would be waiting on itself): they spill into a separate overflow bounded by
    `InJobOverflowCapacity` (4096), past which the publishing job fails and is redelivered — so
    make in-job publishes idempotent. Delayed jobs are bounded separately by `DelayedJobCapacity`.
15. **Keep durable-flow hosts time-synchronized.** Execution leases use absolute UTC expiry. Run
    NTP (or the platform equivalent), and keep `ExecutionLeaseDuration` comfortably above expected
    clock skew and renewal jitter so a fast replica cannot take over a healthy owner's lease early.

## Building and testing

```bash
dotnet build
dotnet test            # Microsoft.Testing.Platform (xUnit.net v3), selected by global.json
```

The unit suite runs on the shipped [`AsyncResponse.Testing`](testing.md) virtual clock, so
timer, cron, timeout, lease, and crash-recovery scenarios execute in milliseconds — no real
sleeps to tune, no timing flakiness to chase. The suites under `tests/AsyncResponse.Tests` whose
names start with `FlowTestHarness`, `DurableFlowTimer`, `ScheduledFlow`, and `TestingHarness`
double as reference examples for testing applications the same way.

Test projects are Microsoft.Testing.Platform applications, so you can also run one directly and
use MTP options — filtering, a TRX report, and code coverage:

```bash
dotnet run --project tests/AsyncResponse.Tests -f net10.0 -- \
    --filter-namespace AsyncResponse.Tests \
    --report-trx --coverage --results-directory ./TestResults
```

The integration tests in [`tests/AsyncResponse.IntegrationTests`](../tests/AsyncResponse.IntegrationTests)
exercise the library end-to-end, driving the **sample app itself** as the system under test (no
separate fixture app to keep in sync). They run at two levels:

- **In-process, no Docker** — `WebApplicationFactory` boots the sample on the fully in-memory channel
  and transport, covering the core request/response, attach, worker, and concurrency paths.
- **Aspire-orchestrated, Docker** — `Aspire.Hosting.Testing` boots an AppHost that starts the
  broker, emulator, and store containers a batch needs, plus sample-app SUTs for the default and
  early-ACK variants of each transport. The container inventory lives in the README's
  [How it's tested](../README.md#how-its-tested) section. They need a running Docker daemon (and
  pull images on first run); CI runs them in the `integration-tests` job.

```bash
dotnet run --project tests/AsyncResponse.IntegrationTests
```

The same suite can run against the **Native AOT-published** sample: every SUT whose whole driver
stack is AOT-capable (today the NATS and PostgreSQL pairs) switches from the JIT project to the
trimmed native binary, and the rest stay JIT (see [aot.md](aot.md#vendor-sdk-compatibility)). CI
runs this as `integration-tests-aot`.

```bash
dotnet publish samples/AsyncResponse.Sample/AsyncResponse.Sample.csproj -c Release -o ./artifacts/sut-aot
ASYNCRESPONSE_ITEST_SUT=aot \
ASYNCRESPONSE_ITEST_SUT_PATH=$PWD/artifacts/sut-aot/AsyncResponse.Sample \
dotnet test --project tests/AsyncResponse.IntegrationTests
```

### Batches

The Aspire-orchestrated tests are split into **batches**. A batch is a named subset of the fleet: the
AppHost declares only that batch's containers and sample apps (selected by `ASYNCRESPONSE_ITEST_BATCH`,
which the fixtures set), and each batch has its own xUnit collection and fixture. Collections run
sequentially (`[assembly: Parallelization(Mode = ParallelMode.None)]` in `Batches.cs`), so one
batch's containers are torn down before the next batch boots. Peak footprint is the largest batch,
not the whole fleet, and running a single test boots only its own batch.

The split follows the suite's one structural line: a test either drives a sample app over HTTP or
drives a driver directly. Driver-only batches start no processes; the app-driven ones split by
family. Batches are balanced on **measured memory, not container count** — a few database servers
cost more than a larger set of brokers:

| Batch | Collection | Containers | Apps | Tests | What's in it |
| --- | --- | --- | --- | --- | --- |
| `data` | `DataCollection` | 8 | 9 | 380 | Everything database-backed: channel conformance, store contracts, the "direct" driver tests, the database channel/transport SUTs, and the SIGKILL crash-recovery suite (`DurableFlowAbruptCrashRecoveryTests`, driving `tests/AsyncResponse.IntegrationTests.CrashWorker`) |
| `oracle-cosmos` | `OracleCosmosCollection` | 2 | 0 | 17 | Oracle and Cosmos store contracts, isolated — the two largest containers in the suite |
| `brokers` | `BrokersCollection` | 5 | 10 | 65 | Message brokers proper (Redis, Pub/Sub, RabbitMQ, NATS, Kafka) |
| `cloud` | `CloudCollection` | 4 | 4 | 18 | Azure Service Bus + SQS emulators. Service Bus brings its own SQL Server |
| `matrix-*` | nine collections | 6–10 | 0 | 2,121 | The provider cross product and the transport contract — see [The provider cross product](#the-provider-cross-product) — plus the Pub/Sub emulator's stop-drain test (`matrix-cloud-light`) |

Peak footprint across a full run is ~3.3 GiB. Among the app-driven and store batches every heavy
container starts exactly once per run; only the cheap ones (Redis, NATS, Pub/Sub, LocalStack) start
more than once. Each `matrix-*` shard is a separate CI leg with its own fleet: SQL Server,
PostgreSQL, and MongoDB start in every shard (the channel axis is complete within each one), Oracle
and Cosmos in three shards each.

Two containers are explicitly capped, because both size themselves from the host: Oracle via
`INIT_SGA_SIZE`/`INIT_PGA_SIZE` (2,180 → 518 MiB) and both SQL Servers via `MSSQL_MEMORY_LIMIT_MB`.
Override with `ASYNCRESPONSE_ITEST_ORACLE_SGA_MB`, `ASYNCRESPONSE_ITEST_ORACLE_PGA_MB`, and
`ASYNCRESPONSE_ITEST_SQLSERVER_MEMORY_MB`.

Tests that need no AppHost at all (the in-memory suite, the Native AOT publish gate, the batch
guards) are tagged `batch=none` and boot nothing. Every test class carries
`[Trait(Batches.Trait, Batches.<Name>)]`, so a batch can be run on its own:

```bash
dotnet test --project tests/AsyncResponse.IntegrationTests/AsyncResponse.IntegrationTests.csproj --filter-trait "batch=data"
```

CI does exactly that in a matrix — one leg per batch, `none` included, running concurrently, so
wall-clock is the slowest batch rather than the sum. Each runner pulls only its own batch's images
(which is what the job's disk-reclaim step exists for). Legs upload `coverage-integration-<batch>`;
the coverage job globs `coverage-*` and merges, so the published number covers the whole suite.

### CI retries

Two mechanisms retry hosted-runner flakes, and both consult the **same classifier**,
`scripts/ci-retryable-failure.sh`: the integration legs retry their own suite once in-job, and
`auto-retry.yml` re-runs the failed jobs of a failed CI or CodeQL run on a `main` push. A log
qualifies only when it matches a known infrastructure signature (a batch or matrix fixture that
failed to boot, the runner going away, and SQLite's "database is locked" on a slow runner disk —
that last one only inside a failed EF Core storm test, the one place its busy timeout is the test's
rather than the product's; from any other test, such as the SQLite flow store's own, it is a real
failure) **and**
carries no evidence that a test failed on its merits or that the build broke (an xunit assertion
message, `XunitException`, a `CS`/`MSB`/`NU` error code) — a fixture-boot flake next to an assertion
failure is a real failure and is never retried. Each attempt's console log is kept in the results
artifact (`itest-console.<batch>.attempt<N>.log`), and every automatic retry leaves a `::warning::`
naming its evidence, so a green re-run is never indistinguishable from a clean pass. The
classifier's fixture logs run as a self-test in `build-and-test`
(`scripts/tests/ci-retryable-failure.test.sh`).

### The provider cross product

Channels, transports, and durable-flow stores are chosen independently, so "it works" has to mean
every combination works — not every provider in isolation. The cross product is enumerated in full:
**6 channels × 11 transports × 10 stores = 660 combinations**, each running three scenarios (a durable
flow end to end, a terminal domain failure, and a worker job with its context restored).

A cell builds a DI provider inside the test process — `AddAsyncResponse().With…Channel()
.With…Transport().With…DurableFlows()`, exactly as an application would — against the containers its
shard booted. No sample app is involved, which is what makes 660 of them affordable.

The cells are partitioned into nine shards on two axes, because the whole fleet at once is roughly
9 GiB and the two heavyweight stores cannot share a runner:

| Shard axis | Values | What it decides |
| --- | --- | --- |
| Transport family | `database` (6 transports: in-memory, PostgreSQL, SQL Server, MongoDB, Redis, NATS), `broker` (Kafka, RabbitMQ), `cloud` (SQS, Service Bus, Pub/Sub) | Which brokers boot |
| Store family | `light` (8 stores), `oracle`, `cosmos` | Whether Oracle (2,180 MiB) or Cosmos (1,031 MiB) boots |

Every shard carries the five channel containers (Redis, NATS, PostgreSQL, SQL Server, MongoDB). The
largest shard, `matrix-database-light`, is 288 cells and runs in about 6½ minutes locally once its
fleet is up.

To reproduce a single combination without waiting out a whole shard, filter by cell name — the same
string the test id shows:

```bash
ASYNCRESPONSE_MATRIX_FILTER=PostgreSql+Kafka+MongoDb dotnet test --project tests/AsyncResponse.IntegrationTests/AsyncResponse.IntegrationTests.csproj --filter-trait "batch=matrix-broker-light"
```

`MatrixCompletenessTests` keeps the product honest: it reflects over the shipped `With…Channel`,
`With…Transport`, and `With…DurableFlows` registrations and fails when one has no matrix axis member,
asserts the shards partition every cell exactly once, and requires each shard to have a test class
carrying its trait whose theories run exactly that shard's cells. A new provider package therefore
fails the build the day it lands rather than shipping with no cross-product coverage.

Because these shards start **no sample app**, they own two things the app-driven batches get for
free. First, backend readiness: an app-driven batch waits for its sample apps to report healthy
(and they `WaitFor` their containers), but a shard must probe every backend itself — and probe the
right thing, since several servers accept a connection before they can serve one (the Cosmos
emulator answers its gateway while still replying `503 pgcosmos extension is still starting`; NATS
serves core requests before its JetStream API responds). Second, subscriber readiness: every
transport subscriber is a `BackgroundService`, so `StartAsync` returns before its consumer group,
JetStream consumer, or queue receiver exists; the harness publishes a probe job and waits until it
is consumed before handing the host to a test.

The transport contract runs in these shards too, because it is driver-level and needs only its own
broker — and delivery-dependent facts are timing-sensitive, so they stay out of the app-heavy `data`
batch.

### Behavioral contracts

Depth within a single axis belongs to that axis's contract suite, which runs **per provider** rather
than per combination — so adding a scenario costs N runs, not 660:

| Suite | Facts | Derivations |
| --- | --- | --- |
| `ChannelConformanceSuite` | 34 | 6 channels |
| `TransportConformanceSuite` | 14 | 11 transports |
| `FlowStoreContract` | one composed contract | 10 stores |

`TransportConformanceSuite` covers dead-lettering, redelivery after a transient failure,
poison-message bounds, shutdown drain (and a graceful stop that settles the job it drained, so a peer
never receives it again), large payloads, concurrency, ambient context restoration, and durability
across a consumer outage.

Transports differ in *where* a guarantee comes from, and `TransportCapabilities` records that rather
than letting it become a skipped test. Every transport bounds redelivery, but in a different place: a
`MaxDeliveryAttempts` subscriber knob on eight of them, the in-process retry budget on the in-memory
queue, the queue's redrive policy on SQS, and the subscription's `DeadLetterPolicy` on Google Pub/Sub.
Two transports constrain the bound itself: RabbitMQ cannot count past two without an
application-owned TTL-retry cycle, and a Pub/Sub `DeadLetterPolicy` rejects anything under five. The
payload fact is sized per transport, because ceilings differ by two orders of magnitude — Service Bus
standard tier rejects messages over 256 KB, SQS over 1 MiB (a queue's `MaximumMessageSize` may be set
lower; LocalStack and older queues still apply 256 KiB), and NATS anything above the server's
`max_payload` (1 MiB by default). Per-transport details are in
[transport-semantics.md](transport-semantics.md).

Where a capability is genuinely absent the contract asserts the absence rather than skipping: the
in-memory transport has no early-ACK mode and no life beyond its host, so a mode appearing later
fails the test and forces the capability table to be updated.

`BatchAssignmentTests` holds the arrangement together. It fails if a class — nested public classes
included, which xUnit runs like any other — asks for a fixture without declaring its batch, declares
one batch and takes another's fixture, carries no batch trait, or carries a trait that disagrees
with its collection. The trait check matters most: an untagged class is in no CI leg, so CI would
quietly stop running it and stay green.

`DurableFlowIntegrationTests` is the one class that spans families — it drives flows across
PostgreSQL, SQL Server, MongoDB, NATS, and SQS at once. It sits in `data` because adding NATS and
LocalStack there costs less than adding three database servers to another batch.

To add a batch: add a `case` to the AppHost's switch on `ASYNCRESPONSE_ITEST_BATCH` composing the
container and app-group functions it needs, derive a fixture overriding `Batch` and `WireAsync`, add
a `[CollectionDefinition]`, register it in `BatchAssignmentTests`, and add the batch's name to the
`batch:` matrix in `.github/workflows/ci.yml`. That last step has no guard: nothing asserts the
hand-listed legs, so a batch missing from the matrix runs locally but never in CI, and CI stays green.
A batch without sample apps inherits work they normally do — `DriverOnlyBatchFixture` waits for
PostgreSQL and creates the SQL Server database itself.

> **The AppHost must stay in the solution.** `tests/AsyncResponse.IntegrationTests.AppHost` is a
> solution member, not merely a `ProjectReference`. Left out, an IDE's "build solution and run all
> tests" rebuilds the test assembly but leaves the AppHost stale, so the suite orchestrates from an
> old build. The symptom is `Resource '<name>' not found`, an AppHost error that
> `ASYNCRESPONSE_ITEST_BATCH` is unknown, or container fleets that match no batch in this table. If
> you see that, rebuild the AppHost first.

In Rider, use the Unit Tests window or gutter icons to run/debug individual unit or integration
tests. Aspire is not a test explorer here; it is only the infrastructure harness that the integration
fixture starts for you.

## Benchmarking and load testing

[`benchmarks/AsyncResponse.Benchmarks`](../benchmarks/AsyncResponse.Benchmarks) is a console app with two
modes — micro-benchmarks (BenchmarkDotNet) and an in-process load/stress harness. Run both from a
**Release** build.

**Benchmarks** — per-operation latency, allocations, and GC (`[MemoryDiagnoser]`) for the hot paths:
in-memory request/response round-trip, raw broker ingress, shared-correlation and exception fanout,
recovery-state save/scan (plus the Redis recovery scan against a latency-injecting fake),
watchdog/health evaluation, context propagation, envelope (de)serialization, payload classification,
expression→callback conversion, reflection invoke, the durable-flow ledger's growth curve, the
SQLite flow store against the in-memory store, and every broker transport's subscriber ACK dispatch
modes (`<Transport>AckDispatchBenchmarks` for all ten):

```bash
dotnet run -c Release --project benchmarks/AsyncResponse.Benchmarks -- --filter '*'                   # all benchmarks
dotnet run -c Release --project benchmarks/AsyncResponse.Benchmarks -- --filter '*Channel*'
dotnet run -c Release --project benchmarks/AsyncResponse.Benchmarks -- --filter '*Ingress*'
dotnet run -c Release --project benchmarks/AsyncResponse.Benchmarks -- --filter '*RedisAckDispatch*'  # any <Transport>AckDispatch
```

Without `--filter`, BenchmarkDotNet prompts for a benchmark interactively.

**Load / stress** — 22 high-concurrency scenarios that *assert* correctness under contention (no
lost/crossed responses, no duplicate worker executions, no cleanup leaks, no context bleed, no hangs)
and report throughput, latency percentiles, allocations, GC counts, and working set. The process
exits non-zero if any correctness check fails, so it doubles as a soak gate:

```bash
dotnet run -c Release --project benchmarks/AsyncResponse.Benchmarks -- stress
dotnet run -c Release --project benchmarks/AsyncResponse.Benchmarks -- stress --concurrency 512 --count 200000 --progress 5
dotnet run -c Release --project benchmarks/AsyncResponse.Benchmarks -- stress --fanout 8 --timeout-count 5000 --timeout-ms 50
```

The scenarios:

- **waiter-storm** — N concurrent waiters, each must receive exactly its own response.
- **progress-storm** — a burst of progress messages, then a terminal, per flow.
- **worker-storm** — N fire-and-forget jobs, each executed exactly once.
- **Ten `<transport>-ack-after-…-dispatch-storm` scenarios** (Google Pub/Sub, RabbitMQ, Redis, NATS,
  PostgreSQL, SQL Server, MongoDB, Azure Service Bus, SQS, Kafka) — the bounded early-ACK
  dispatcher: every ACKed message is processed exactly once (for SQS, deleted exactly once and never
  released back via `ChangeMessageVisibility`). They drive the transport dispatchers in process, with
  no broker, and their timed region includes the background drain, so the throughput they publish
  measures dispatch, not enqueue (a drain that times out fails the correctness gate).
- **race-burst** — subscribe-before-send under contention.
- **raw-ingress-storm** — broker JSON into typed waiters.
- **shared-response-fanout** and **exception-fanout** — many waiters on one correlation id.
- **timeout-storm** and **dispose-cleanup-storm** — subscription/recovery cleanup.
- **context-isolation-storm** — captured `ExecutionContext` under foreign publishers.
- **watchdog-scan-storm** — scanner + active-subscriber probe + stale evaluation.
- **durable-flow-storm** — hundreds of concurrent 5-step checkpointed flows through the real worker
  transport and the in-memory flow store: every flow ends `Succeeded`, every step runs exactly once.

The core concurrency invariants are also gated on every CI run, at smaller scale, by
[`ConcurrencyTests`](../tests/AsyncResponse.Tests/ConcurrencyTests.cs) in the unit suite.

**End-to-end load (NBomber).** [`benchmarks/AsyncResponse.LoadTests`](../benchmarks/AsyncResponse.LoadTests)
drives the sample app's HTTP endpoints with [NBomber v4](https://nbomber.com) over the **real** stack —
durable channels + broker/table transports — reporting throughput, latency percentiles, and failures
per scenario. By default it boots the AppHost's `loadtest` fleet via Aspire (Docker required): Redis,
the Pub/Sub emulator, the Azure Service Bus emulator, LocalStack (SQS), RabbitMQ, Kafka, NATS,
PostgreSQL, SQL Server, and MongoDB, with a default and an early-ACK sample app per transport.

To load already-running instances instead, pass `--url` (the default Pub/Sub/Redis-channel app) and
`--early-ack-url`, or a transport's pair: `--<transport>-url` / `--<transport>-early-ack-url` for
`azure-servicebus`, `rabbitmq`, `redis`, `nats`, `postgresql`, `sqlserver`, `kafka`, `sqs`, and
`mongodb`. `--profile` picks the scenario set:

| Profile | Scenarios |
| --- | --- |
| `broad` (default) | Non-destructive request/response, attach, observed worker, multi-step, ambient and shared exception, reply target — plus a subset of every available transport target's scenarios (worker, response ingress, reply target, and early-ACK worker when that target is available) |
| `pubsub`, `azure-servicebus`, `rabbitmq`, `redis`, `nats`, `postgresql`, `sqlserver`, `kafka`, `sqs`, `mongodb` | One transport: worker dispatch, response ingress through native header/attribute and body correlation ids, and early-ACK worker dispatch when an early target is available; all but `pubsub` add a reply target, all but `pubsub`, `rabbitmq` and `redis` add request/response success and domain failure, and `sqs` and `mongodb` add a durable flow |
| `recovery` | Lost-subscriber resume/failure/exception and stale health — destructive by design, so run it separately |

```bash
dotnet run -c Release --project benchmarks/AsyncResponse.LoadTests -- --profile broad --rate 20 --duration 60
dotnet run -c Release --project benchmarks/AsyncResponse.LoadTests -- --profile kafka --rate 10 --duration 60
dotnet run -c Release --project benchmarks/AsyncResponse.LoadTests -- --profile recovery --rate 5 --duration 60
dotnet run -c Release --project benchmarks/AsyncResponse.LoadTests -- --profile broad --scenario request_response_success_redis --rate 20 --duration 60
dotnet run -c Release --project benchmarks/AsyncResponse.LoadTests -- --url http://localhost:5000 --early-ack-url http://localhost:5001 --profile pubsub
dotnet run -c Release --project benchmarks/AsyncResponse.LoadTests -- --rabbitmq-url http://localhost:5002 --rabbitmq-early-ack-url http://localhost:5003 --profile rabbitmq
```

Use `--scenario name` (or a comma-separated list) for a cleaner single-scenario baseline; the mixed
profiles are better at finding interference between flows. Defaults are `--rate 20` (per scenario)
and `--duration 30`. The run exits non-zero when the overall failure rate exceeds `--max-fail-rate`
(default 5%) or any single scenario exceeds `--max-scenario-fail-rate` (default 75%). The sample's
Pub/Sub emit endpoint reuses its publisher client, while the Azure Service Bus and RabbitMQ response
emits open short-lived broker clients per request to model an external producer. Reports
(HTML/CSV/Markdown) are written to `nbomber-report/`.

The [load-test workflow](../.github/workflows/loadtest.yml) runs it on every push to `main` that
touches code or build files (a `paths:` filter), and on demand, publishing per-scenario throughput and
latency to the **same dashboard** as the benchmarks and uploading the full report as an artifact.
Push runs execute the broad profile at `3` requests/sec per scenario so every scenario can run
together on one shared runner without overloading a backing service. Manual runs take `profile`,
`rate`, and `duration` inputs; put any other CLI switches in `extra_args` (for example
`--scenario azure_servicebus_worker_ack_after_receive_observed`) and Aspire SUT tuning in
`apphost_env` as newline- or space-separated `KEY=VALUE` entries (for example
`ASYNCRESPONSE_ITEST_POSTGRESQL_WORKER_QUEUE_CAPACITY=512
ASYNCRESPONSE_ITEST_POSTGRESQL_WORKER_BACKGROUND_WORKERS=8`), which keeps the workflow under GitHub's
`workflow_dispatch` input limit. The pushed JSON uses github-action-benchmark's `customBiggerIsBetter`
and `customSmallerIsBetter` formats, so new scenario series appear automatically under `dev/bench` on
`gh-pages`.

**Performance over time.** Every push to `main` that touches code or build files runs the
micro-benchmarks and the stress harness ([`benchmarks.yml`](../.github/workflows/benchmarks.yml)) and
publishes them with [github-action-benchmark](https://github.com/benchmark-action/github-action-benchmark)
as interactive, per-commit charts — micro-benchmark timings and allocations, the in-process stress
suites, and (from the load-test workflow) end-to-end throughput and latency over the real stack:

**📈 [Benchmark dashboard](https://sky4ce.github.io/AsyncResponse/dev/bench/)**

Alerting is off (both workflows set `comment-on-alert` and `fail-on-alert` to `false`), so a
regression raises no annotation, comment, or failure; every run prints a results table to its
[workflow summary](https://github.com/Sky4CE/AsyncResponse/actions/workflows/benchmarks.yml). The
numbers come from shared CI runners, so read them as **trends** rather than absolute hardware
figures — run the benchmarks locally (above) for stable measurements.

> The dashboard goes live after the workflow's first run on `main`, once GitHub Pages is enabled for
> the `gh-pages` branch (Settings → Pages → Branch: `gh-pages`).
