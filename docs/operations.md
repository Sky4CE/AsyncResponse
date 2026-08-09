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
   register lost-subscriber recovery callbacks for — durable channels fail fast at waiter creation
   without it. That includes success-only payloads (`=> RecoveryAction.Resume`) and progress-only
   checkpoints (`=> RecoveryAction.KeepWaiting`), not just payloads that can carry a domain
   failure: `Fail` for the states that must not resume, `KeepWaiting` for non-terminal checkpoints
   so they don't consume the registration out from under the terminal response. It's independent
   of your `Until` predicate (which owns live completion) — they answer different questions:
   "what does this result do to the flow?" versus "is the operation done?". See
   [recovery.md](recovery.md).
4. **Register both recovery callbacks** for any flow that must survive redeploys. A failed payload
   with no failure callback is logged and dropped — never resumed — but dropped is still a stuck
   flow.
5. **Make resume callbacks re-entrant.** A resume may re-trigger a flow whose step is still running
   remotely; resume should *re-attach* (subscribe to the same correlation id) rather than re-execute
   side effects. Persist enough state to tell the difference.
6. **Treat callback method names and the `KeyPrefix` as deployment contracts.** They are persisted;
   rename with a migration window.
7. **Set timeouts per flow.** The 7-day default is a backstop, not a recommendation; a payment flow
   should fail in minutes.
8. **Run the watchdog in exactly one host per durable store** and alert on its warnings or the
   `Degraded` health status — stale recovery state is your earliest signal of stuck flows.
9. **Mind channel wakeup semantics.** Redis pub/sub delivery is at-most-once to live subscribers, and
   PostgreSQL `NOTIFY` is only a wakeup signal; the durable response/recovery state
   is what makes the system safe across gaps. Don't disable it (`RecoveryStateExpiry`) below your
   longest flow duration.
10. **Reuse client singletons.** Reuse your application's existing Redis `IConnectionMultiplexer`,
    NATS connection, or PostgreSQL `NpgsqlDataSource`; don't create a second pool for AsyncResponse.
11. **Share correlation ids deliberately.** Live delivery and lost-subscriber recovery both fan out
    across multiple waiters on one correlation id; a normally completing waiter removes only its own
    recovery registration. On the database channels, a registration tying another process's
    delivery claim within one server-clock tick is arbitrated exactly by the monotonic ack
    sequence; only a claim whose sequence draw stalled across ticks resolves conservatively
    (the waiter times out and the idempotent step restarts — never a replayed response). See
    [recovery.md](recovery.md#shared-correlation-recovery).
12. **Measure hot paths in isolation before comparing profiles.** The sample's remote simulator
    deliberately waits before progress and terminal messages, so broad HTTP load-test latency mostly
    reflects sample workflow timing. Use the micro-benchmarks, stress harness, and NBomber
    `--scenario` filter to separate library overhead from demo behavior.
13. **Choose exactly one atomic flow store.** `AddAsyncResponse()` does not select one implicitly,
    and startup rejects a missing or duplicate choice. Every built-in `AsyncResponse.DurableFlows.*` store and
    every custom `IFlowStateStore` must provide atomic start, revision-checked checkpoints, and a
    renewable execution lease. Use `.WithInMemoryDurableFlows()` only when process-local state is
    intentional.
14. **Treat queue capacity as backpressure, not an error.** In-memory workers and internal
    per-correlation dispatch queues are bounded and publishers wait asynchronously when full. Size
    `InMemoryWorkerTransportOptions.QueueCapacity` for the burst you accept and raise `WorkerCount`
    only when jobs are safe to run concurrently.
15. **Keep durable-flow hosts time-synchronized.** Execution leases use absolute UTC expiry. Run
    NTP (or the platform equivalent), and keep `ExecutionLeaseDuration` comfortably above expected
    clock skew and renewal jitter so a fast replica cannot take over a healthy owner's lease early.

## Building and testing

```bash
dotnet build
dotnet test            # runs on Microsoft.Testing.Platform (xUnit.net v3)
```

The Docker-backed integration suite can also run against the **Native AOT-published** sample as
the system under test — the same tests, with every SUT resource switched from the JIT project to
the trimmed native binary (MongoDB SUTs stay JIT; see [aot.md](aot.md#vendor-sdk-compatibility)):

```bash
dotnet publish samples/AsyncResponse.Sample/AsyncResponse.Sample.csproj -c Release -o ./artifacts/sut-aot
ASYNCRESPONSE_ITEST_SUT=aot \
ASYNCRESPONSE_ITEST_SUT_PATH=$PWD/artifacts/sut-aot/AsyncResponse.Sample \
dotnet test --project tests/AsyncResponse.IntegrationTests
```

The test project is a Microsoft.Testing.Platform application, so you can also run it directly and use
MTP options — test filtering, a TRX report, and code coverage:

```bash
dotnet run --project tests/AsyncResponse.Tests -f net10.0 -- \
    --filter-namespace AsyncResponse.Tests \
    --report-trx --coverage --results-directory ./TestResults
```

The integration tests in [`tests/AsyncResponse.IntegrationTests`](../tests/AsyncResponse.IntegrationTests)
exercise the library end-to-end, driving the **sample app itself** as the system under test (one app —
no separate fixture app to keep in sync). They run at two levels:

- **In-process, no Docker** — `WebApplicationFactory` boots the sample on the fully in-memory channel
  and transport, covering the core request/response, attach, worker, and concurrency paths. They need
  no containers, so they stay fast and reliable even where Docker is unavailable.
- **Aspire-orchestrated, Docker** — `Aspire.Hosting.Testing` boots an AppHost that starts every
  broker, emulator, and store container the suite needs, plus a dedicated sample-app SUT per
  transport for the default and early-ACK variants. The complete container inventory lives in the
  README's [How it's tested](../README.md#how-its-tested) section (the single source of truth, so
  this page doesn't drift). Tests drive each provider's scenarios over HTTP. They need a running
  Docker daemon (and pull broker images on first run), so CI runs them in a separate Docker-backed
  `integration-tests` job:

```bash
dotnet run --project tests/AsyncResponse.IntegrationTests
```

In Rider, use the Unit Tests window or gutter icons to run/debug individual unit or integration
tests. Aspire is not a test explorer here; it is only the infrastructure harness that the integration
fixture starts for you.

## Benchmarking and load testing

[`benchmarks/AsyncResponse.Benchmarks`](../benchmarks/AsyncResponse.Benchmarks) is a console app with two
modes — micro-benchmarks (BenchmarkDotNet) and an in-process load/stress harness. Run both from a
**Release** build.

**Benchmarks** — per-operation latency, allocations, and GC for the hot paths (in-memory
request/response round-trip, raw broker ingress, shared-correlation fanout, exception fanout,
recovery-state save/scan, watchdog/health evaluation, context propagation, envelope
(de)serialization, payload classification, expression→callback conversion, reflection invoke, and
Google Pub/Sub/Azure Service Bus/RabbitMQ/Redis/NATS/PostgreSQL/SQL Server subscriber ACK dispatch modes).
`[MemoryDiagnoser]` reports allocated bytes and Gen0/1/2 collections per op alongside
mean/median/percentile timings:

```bash
dotnet run -c Release --project benchmarks/AsyncResponse.Benchmarks                 # all benchmarks
dotnet run -c Release --project benchmarks/AsyncResponse.Benchmarks -- --filter '*Channel*'
dotnet run -c Release --project benchmarks/AsyncResponse.Benchmarks -- --filter '*Ingress*'
dotnet run -c Release --project benchmarks/AsyncResponse.Benchmarks -- --filter '*RedisAckDispatch*'
dotnet run -c Release --project benchmarks/AsyncResponse.Benchmarks -- --filter '*AzureServiceBusAckDispatch*'
dotnet run -c Release --project benchmarks/AsyncResponse.Benchmarks -- --filter '*GooglePubSubAckDispatch*'
dotnet run -c Release --project benchmarks/AsyncResponse.Benchmarks -- --filter '*RabbitMqAckDispatch*'
dotnet run -c Release --project benchmarks/AsyncResponse.Benchmarks -- --filter '*NatsAckDispatch*'
dotnet run -c Release --project benchmarks/AsyncResponse.Benchmarks -- --filter '*PostgreSqlAckDispatch*'
dotnet run -c Release --project benchmarks/AsyncResponse.Benchmarks -- --filter '*SqlServerAckDispatch*'
dotnet run -c Release --project benchmarks/AsyncResponse.Benchmarks -- --filter '*SqsAckDispatch*'
```

**Load / stress** — high-concurrency scenarios that *assert* correctness under contention (no
lost/crossed responses, no duplicate worker executions, no cleanup leaks, no context bleed, no hangs)
and report throughput, latency percentiles, allocations, GC counts, and working set. The process exits
non-zero if any correctness check fails, so it doubles as a soak gate:

```bash
dotnet run -c Release --project benchmarks/AsyncResponse.Benchmarks -- stress
dotnet run -c Release --project benchmarks/AsyncResponse.Benchmarks -- stress --concurrency 512 --count 200000 --progress 5
dotnet run -c Release --project benchmarks/AsyncResponse.Benchmarks -- stress --fanout 8 --timeout-count 5000 --timeout-ms 50
```

The stress harness now checks the system from multiple angles: **waiter-storm** (N concurrent waiters,
each must receive exactly its own response — no cross-correlation leakage), **progress-storm** (a burst
of progress messages then a terminal per flow), **worker-storm** (N fire-and-forget jobs, each executed
exactly once), **google-pubsub-ack-after-enqueue-dispatch-storm** (bounded early-ACK dispatcher:
every ACKed message must be processed once), **rabbitmq-ack-after-enqueue-dispatch-storm** (the same
bounded early-ACK invariant for RabbitMQ deliveries), **redis-ack-after-enqueue-dispatch-storm** (the
same bounded early-ACK invariant for Redis stream entries), **nats-ack-after-receive-dispatch-storm**
(the same bounded early-ACK invariant for NATS JetStream deliveries),
**postgresql-ack-after-receive-dispatch-storm** (the same bounded early-ACK invariant for PostgreSQL
queue rows), **sqlserver-ack-after-enqueue-dispatch-storm** (the same bounded early-ACK invariant for
SQL Server queue rows), **azure-servicebus-ack-after-receive-dispatch-storm** (the same bounded early-ACK
invariant for Azure Service Bus messages), **sqs-ack-after-enqueue-dispatch-storm** (the same bounded
early-ACK invariant for AWS SQS deliveries: every message deleted exactly once, never released back via
`ChangeMessageVisibility`), **kafka-ack-after-enqueue-dispatch-storm** (the same bounded early-ACK
invariant for Kafka deliveries), **mongodb-ack-after-enqueue-dispatch-storm** (the same bounded
early-ACK invariant for MongoDB queue documents), **race-burst** (subscribe-before-send under contention),
**raw-ingress-storm** (broker JSON into typed waiters), **shared-response-fanout** and
**exception-fanout** (many waiters on one correlation id), **timeout-storm** and
**dispose-cleanup-storm** (subscription/recovery cleanup), **context-isolation-storm** (captured
`ExecutionContext` under foreign publishers),
**watchdog-scan-storm** (scanner + active-subscriber probe + stale evaluation), and
**durable-flow-storm** (hundreds of concurrent 5-step checkpointed flows through the real worker
transport and explicit atomic in-memory flow store: every flow must end `Succeeded`, every step
exactly once). A separate BenchmarkDotNet baseline compares the SQLite durable-flow package store
against the explicit in-memory store. The core concurrency
invariants are gated on every CI run, at smaller scale, by
[`ConcurrencyTests`](../tests/AsyncResponse.Tests/ConcurrencyTests.cs) in the unit suite. The broker
dispatch storms stay in-process too: they bypass external Pub/Sub/Azure Service Bus/SQS/RabbitMQ/Redis/NATS/PostgreSQL/SQL Server/Kafka/MongoDB
servers while exercising the transport callback/ACK dispatchers.

**End-to-end load (NBomber).** [`benchmarks/AsyncResponse.LoadTests`](../benchmarks/AsyncResponse.LoadTests)
drives the sample app's HTTP endpoints with [NBomber v4](https://nbomber.com) over the **real** stack —
durable channels + broker/table transports — reporting throughput, latency percentiles, and failures
per scenario. By default it boots Redis + a Pub/Sub emulator + the Azure Service Bus emulator +
LocalStack (SQS) + RabbitMQ + NATS + PostgreSQL + SQL Server + Kafka + the SUT fleet via Aspire
(Docker required): default/early-ACK Pub/Sub apps, Azure Service Bus apps, SQS apps, RabbitMQ apps,
Redis Streams apps, NATS apps, PostgreSQL apps, SQL Server apps, and Kafka apps. Pass
`--url` to load an already-running default instance, `--early-ack-url` for the Pub/Sub early-ACK
target, `--azure-servicebus-url` / `--azure-servicebus-early-ack-url` for Azure Service Bus targets, `--rabbitmq-url` /
`--rabbitmq-early-ack-url` for RabbitMQ targets, `--redis-url` / `--redis-early-ack-url` for Redis
transport targets, `--nats-url` / `--nats-early-ack-url` for NATS targets, `--postgresql-url` /
`--postgresql-early-ack-url` for PostgreSQL targets, `--sqlserver-url` /
`--sqlserver-early-ack-url` for SQL Server targets, or `--sqs-url` / `--sqs-early-ack-url` for SQS
targets. Profiles let you choose the scenario set:
`broad` (default, non-destructive request/response, attach, observed worker, multi-step, ambient
exception, shared exception, reply target, plus RabbitMQ worker/response/reply-target throughput when
a RabbitMQ target is available), `pubsub` (default worker dispatch, response-topic ingress
with attribute/body correlation ids, and early-ACK worker dispatch when an early target is available),
`azure-servicebus` (request/response, worker dispatch, response-queue ingress through property/body
correlation ids, reply target, and early-ACK worker dispatch when an early target is available),
`rabbitmq` (default worker dispatch, response-queue ingress with header/body correlation ids, reply
target, and early-ACK worker dispatch when an early target is available), `redis` (default worker
dispatch, response-stream ingress with field/body correlation ids, reply target, and early-ACK worker
dispatch when an early target is available), `nats` (request/response, worker dispatch,
response-subject ingress, reply target, and early-ACK worker dispatch), `postgresql`
(request/response, worker dispatch, response-table ingress through header/body correlation ids, reply
target, and early-ACK worker dispatch), `sqlserver` (the same request/response, worker dispatch,
response-table ingress, reply-target, and early-ACK worker scenarios over the SQL Server pair), `sqs`
(request/response, worker dispatch, response-queue ingress with attribute/body correlation ids, reply
target, a durable-flow-over-SQS scenario, and early-ACK worker dispatch), or `recovery`
(lost-subscriber resume/failure/exception and stale health). Run the
recovery profile separately because it intentionally simulates subscriber loss:

```bash
dotnet run -c Release --project benchmarks/AsyncResponse.LoadTests -- --profile broad --rate 20 --duration 60
dotnet run -c Release --project benchmarks/AsyncResponse.LoadTests -- --profile azure-servicebus --rate 10 --duration 60
dotnet run -c Release --project benchmarks/AsyncResponse.LoadTests -- --profile pubsub --rate 10 --duration 60
dotnet run -c Release --project benchmarks/AsyncResponse.LoadTests -- --profile rabbitmq --rate 10 --duration 60
dotnet run -c Release --project benchmarks/AsyncResponse.LoadTests -- --profile redis --rate 10 --duration 60
dotnet run -c Release --project benchmarks/AsyncResponse.LoadTests -- --profile nats --rate 10 --duration 60
dotnet run -c Release --project benchmarks/AsyncResponse.LoadTests -- --profile postgresql --rate 10 --duration 60
dotnet run -c Release --project benchmarks/AsyncResponse.LoadTests -- --profile sqlserver --rate 10 --duration 60
dotnet run -c Release --project benchmarks/AsyncResponse.LoadTests -- --profile sqs --rate 10 --duration 60
dotnet run -c Release --project benchmarks/AsyncResponse.LoadTests -- --profile recovery --rate 5 --duration 60
dotnet run -c Release --project benchmarks/AsyncResponse.LoadTests -- --profile broad --scenario request_response_success_redis --rate 20 --duration 60
dotnet run -c Release --project benchmarks/AsyncResponse.LoadTests -- --profile broad --scenario rabbitmq_worker_default_ack_observed,rabbitmq_worker_ack_after_enqueue_observed --rate 10 --duration 60
dotnet run -c Release --project benchmarks/AsyncResponse.LoadTests -- --url http://localhost:5000
dotnet run -c Release --project benchmarks/AsyncResponse.LoadTests -- --url http://localhost:5000 --early-ack-url http://localhost:5001 --profile pubsub
dotnet run -c Release --project benchmarks/AsyncResponse.LoadTests -- --azure-servicebus-url http://localhost:5010 --azure-servicebus-early-ack-url http://localhost:5011 --profile azure-servicebus
dotnet run -c Release --project benchmarks/AsyncResponse.LoadTests -- --rabbitmq-url http://localhost:5002 --rabbitmq-early-ack-url http://localhost:5003 --profile rabbitmq
dotnet run -c Release --project benchmarks/AsyncResponse.LoadTests -- --redis-url http://localhost:5004 --redis-early-ack-url http://localhost:5005 --profile redis
dotnet run -c Release --project benchmarks/AsyncResponse.LoadTests -- --nats-url http://localhost:5006 --nats-early-ack-url http://localhost:5007 --profile nats
dotnet run -c Release --project benchmarks/AsyncResponse.LoadTests -- --postgresql-url http://localhost:5008 --postgresql-early-ack-url http://localhost:5009 --profile postgresql
```

Use `--scenario name` (or a comma-separated list) when you want a cleaner single-scenario baseline;
the mixed profiles are better at finding interference between flows. The sample Pub/Sub emit endpoint
reuses its publisher client, while Azure Service Bus and RabbitMQ response emits open short-lived
broker clients per request to model an external producer. It writes an HTML/CSV/Markdown report to
`nbomber-report/`.
The [load-test workflow](../.github/workflows/loadtest.yml) runs it on every push to `main` (and on demand),
publishing per-scenario throughput and latency to the **same dashboard** as the benchmarks and
uploading the full report as an artifact. Push runs execute the broad profile at a conservative
`5` requests/sec per scenario so every defined provider scenario can run together on a shared GitHub
runner without overloading one backing service. Manual workflow runs keep `profile`, `rate`, and
`duration` as first-class inputs. Put any current or future `AsyncResponse.LoadTests` CLI switches in
`extra_args`, for example `--azure-servicebus-url http://host --azure-servicebus-early-ack-url
http://host-early --scenario azure_servicebus_worker_ack_after_receive_observed`. Put Aspire SUT
tuning in `apphost_env` as newline-separated or shell-style `KEY=VALUE` entries, for example
`ASYNCRESPONSE_ITEST_POSTGRESQL_WORKER_QUEUE_CAPACITY=512
ASYNCRESPONSE_ITEST_POSTGRESQL_WORKER_BACKGROUND_WORKERS=8`. This avoids GitHub's `workflow_dispatch`
input limit while still allowing new transports/channels and provider knobs to be tested without
changing the workflow. The pushed JSON still uses github-action-benchmark's `customBiggerIsBetter`
and `customSmallerIsBetter` formats, so new scenario series appear automatically under `dev/bench` on
`gh-pages`.

**Performance over time.** Every push to `main` runs the micro-benchmarks and the stress harness
([`benchmarks.yml`](../.github/workflows/benchmarks.yml)) and publishes them with
[github-action-benchmark](https://github.com/benchmark-action/github-action-benchmark) as
interactive, per-commit charts: micro-benchmark timings & allocations, the in-process stress suites,
and — from the load-test workflow — end-to-end throughput & latency over the real Redis/broker/table stack:

**📈 [Benchmark dashboard](https://sky4ce.github.io/AsyncResponse/dev/bench/)**

A change that moves a number stands out immediately; a regression beyond the alert threshold is posted
as a comment on the offending commit, and every run prints a results table to its
[workflow summary](https://github.com/Sky4CE/AsyncResponse/actions/workflows/benchmarks.yml). The
numbers come from shared CI runners, so read them as **trends** rather than absolute hardware figures —
run the benchmarks locally (above) for stable measurements.

> The dashboard goes live after the workflow's first run on `main`, once GitHub Pages is enabled for
> the `gh-pages` branch (Settings → Pages → Branch: `gh-pages`).
