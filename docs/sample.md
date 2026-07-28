# The sample app

[← Back to README](../README.md)

A complete testbed lives in [`samples/AsyncResponse.Sample`](../samples/AsyncResponse.Sample) —
one configuration-driven app that runs fully in-memory with zero dependencies, or against any
channel/transport pair, and exposes an HTTP endpoint per scenario (the same endpoints the
integration tests drive).

**On this page**

- [Run with Aspire](#run-with-aspire)
- [Run standalone](#run-standalone) — in-memory by default, or per-provider recipes
- [Walking the scenarios](#walking-the-scenarios)

## Run with Aspire

Run it as an Aspire playground when you want the dashboard, managed Redis, logs, traces, metrics,
resource environment, and health checks in one place:

```bash
dotnet run --project samples/AsyncResponse.AppHost
```

The sample AppHost starts Redis plus the sample API, then opens the Aspire dashboard.
Use the dashboard's `playground` resource to open the API endpoint and inspect `AsyncResponse`
logs/traces. The local playground pins the dashboard to `http://localhost:18888` and uses HTTP
resource/OTLP endpoints to avoid local HTTPS certificate issues. The sample also exposes Aspire
service-default endpoints at `/health` and `/alive`.

Prerequisites: .NET 10 SDK, `dotnet` available on `PATH`, and a supported container runtime
such as Docker or Podman for the Redis resource.

## Run standalone

The sample is **configuration-driven**: `AsyncResponse:Channel` (`InMemory` | `Redis` | `NATS` |
`PostgreSQL` | `SqlServer` | `MongoDB`) and `AsyncResponse:Transport` (`InMemory` | `AzureServiceBus` |
`GooglePubSub` | `SQS` | `Kafka` | `RabbitMQ` | `Redis` | `NATS` | `PostgreSQL` | `SqlServer` |
`MongoDB`) select the providers,
defaulting to fully in-memory — so it runs standalone with **no external dependencies**:

```bash
dotnet run --project samples/AsyncResponse.Sample      # in-memory channel + in-memory worker transport
```

The durable lost-subscriber recovery flow needs a real channel — point the sample at Redis for it:

```bash
docker compose up -d                                                  # local Redis
AsyncResponse__Channel=Redis dotnet run --project samples/AsyncResponse.Sample
```

To run the standalone sample on Redis for both the response channel and the worker/ingress
transport:

```bash
docker compose up -d                                                  # local Redis
AsyncResponse__Channel=Redis \
AsyncResponse__Transport=Redis \
Redis__KeyPrefix=sample \
dotnet run --project samples/AsyncResponse.Sample
```

To run the standalone sample against a local RabbitMQ broker, point the transport at an AMQP
connection string:

```bash
docker compose up -d                                                  # local Redis
docker run -d --rm --name asyncresponse-rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3.13-management
AsyncResponse__Channel=Redis \
AsyncResponse__Transport=RabbitMQ \
RabbitMQ__ConnectionString=amqp://guest:guest@localhost:5672/ \
dotnet run --project samples/AsyncResponse.Sample
```

To run the standalone sample against the Azure Service Bus emulator, use Redis for the durable
response channel and point the transport at the emulator connection string. The emulator queues must
exist in its `Config.json` before startup (see the integration AppHost's
`servicebus-emulator-config.json` for a working queue-only example):

```bash
docker compose up -d                                                  # local Redis

AsyncResponse__Channel=Redis \
AsyncResponse__Transport=AzureServiceBus \
ConnectionStrings__AzureServiceBus='Endpoint=sb://localhost:5672;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;' \
AzureServiceBus__WorkerQueue=asyncresponse-itest-asb-worker \
AzureServiceBus__ResponseQueue=asyncresponse-itest-asb-response \
dotnet run --project samples/AsyncResponse.Sample
```

To run the standalone sample on PostgreSQL for both the durable response channel and the worker/ingress
transport:

```bash
docker run -d --rm --name asyncresponse-postgres -p 5432:5432 \
  -e POSTGRES_DB=asyncresponse \
  -e POSTGRES_PASSWORD=postgres \
  postgres:16-alpine

AsyncResponse__Channel=PostgreSQL \
AsyncResponse__Transport=PostgreSQL \
ConnectionStrings__PostgreSQL='Host=localhost;Port=5432;Username=postgres;Password=postgres;Database=asyncresponse' \
dotnet run --project samples/AsyncResponse.Sample
```

To run the standalone sample on Microsoft SQL Server for both the durable response channel and the
worker/ingress transport (the sample creates the `asyncresponse` database on first start):

```bash
docker run -d --rm --name asyncresponse-sqlserver -p 1433:1433 \
  -e ACCEPT_EULA=Y \
  -e MSSQL_SA_PASSWORD='P@ssword12345' \
  mcr.microsoft.com/mssql/server:2022-latest

AsyncResponse__Channel=SqlServer \
AsyncResponse__Transport=SqlServer \
ConnectionStrings__SqlServer='Server=localhost,1433;User ID=sa;Password=P@ssword12345;Database=asyncresponse;TrustServerCertificate=True' \
dotnet run --project samples/AsyncResponse.Sample
```

## Walking the scenarios

Then walk the scenarios (the same HTTP endpoints the integration tests drive):

```bash
curl -X POST 'http://localhost:5000/request-response?behavior=Succeed'      # happy path with progress messages
curl -X POST 'http://localhost:5000/request-response?behavior=FailDomain'   # domain failure seen by the active waiter
curl -X POST 'http://localhost:5000/request-response?behavior=Fail'         # technical failure (SetException)
curl -X POST 'http://localhost:5000/request-response?behavior=Timeout'      # 2s timeout vs a slow remote
curl -X POST 'http://localhost:5000/attach'                                 # attach to an in-flight op by correlation id
curl -X POST 'http://localhost:5000/multi-step?first=Succeed&second=Succeed' # sequential two-step flow
curl -X POST 'http://localhost:5000/multi-step?first=Succeed&second=Fail'    # step 2 fails through SetException
curl -X POST 'http://localhost:5000/ambient-exception?message=boom'          # SetException from inside the trigger (explicit cid)
curl -X POST 'http://localhost:5000/shared-correlation-exception?message=boom' # one exception faults two waiters
curl -X POST 'http://localhost:5000/worker?token=order-42'                  # fire-and-forget background worker job

# Durable flows (the checkpointed multi-step orchestration API):
curl -X POST 'http://localhost:5000/durable-flow?name=acme'                  # start a 5-step durable flow → {"flowId":"flow-…"}
curl -X POST 'http://localhost:5000/durable-flow?failAtImport=true'          # a domain failure terminally fails the run
curl -X POST 'http://localhost:5000/durable-flow?flowId=run-42'              # caller-supplied id → idempotent start
curl      'http://localhost:5000/durable-flow/<flowId>'                     # observe state: status, step checkpoints, progress
curl -X POST 'http://localhost:5000/durable-flow/<flowId>/resume'            # kick a run (no-op when already terminal)
curl -X POST 'http://localhost:5000/emit-response?correlationId=<id>&status=Completed&useAttribute=true' # broker response ingress
curl      'http://localhost:5000/healthz'                                   # recovery watchdog findings
curl      'http://localhost:5000/alive'                                     # liveness check

# Recovery after a "redeploy" (needs a durable channel such as Redis, PostgreSQL, or SQL Server):
curl -X POST 'http://localhost:5000/arm'                                          # returns a correlationId
curl -X POST 'http://localhost:5000/crash'                                        # drops every subscription
curl -X POST 'http://localhost:5000/publish?correlationId=<id>&status=Completed'  # → resume callback
curl -X POST 'http://localhost:5000/publish?correlationId=<id>&status=Failed'     # → failure callback
curl -X POST 'http://localhost:5000/publish?correlationId=<id>&exception=boom'    # → failure callback via SetException

# Same recovery flow, composed into one endpoint:
curl -X POST 'http://localhost:5000/lost-subscriber-flow?outcome=Completed'        # arm + drop this channel + late success → resume
curl -X POST 'http://localhost:5000/lost-subscriber-flow?outcome=Failed'           # arm + drop this channel + late failed payload → fail
curl -X POST 'http://localhost:5000/lost-subscriber-flow?outcome=Exception'        # arm + drop this channel + late SetException → fail
```

For the lost-subscriber flow, copy the `correlationId` returned by `/arm` and replace `<id>` in a
`/publish` request. `Completed` exercises the resume callback; `Failed` exercises the failure
callback with an `AsyncResponseDomainFailureException`; `exception=...` exercises the technical
failure path through `IAsyncResponsePublisher.SetException`. (`/arm`, `/crash`, `/publish`, and
`/lost-subscriber-flow` require a durable channel — run with `AsyncResponse__Channel` set to
`Redis`, `NATS`, `PostgreSQL`, `SqlServer`, or `MongoDB`.)
`/crash` is intentionally a blunt manual demo that drops all local durable-channel subscriptions. For
Redis, `/lost-subscriber-flow` drops only the correlation id it just armed so load tests can run many
recovery flows concurrently without disturbing each other; for PostgreSQL and SQL Server, it uses the
channel's local subscription-drop hook to simulate the same lost-subscriber condition before
publishing the late response.

`/shared-correlation-exception` demonstrates fan-out: two waiters attach to the same correlation
id, then one `SetException` faults both. This works with the in-memory and Redis channels; Redis may
multiplex local handlers through one server-side subscription, so the sample waits for both waiter
registrations directly rather than relying on Redis subscriber counts.

> Shared-correlation fan-out applies to both live delivery and durable lost-subscriber recovery:
> every live waiter on the id is faulted, and if all waiters are lost, every stored recovery
> registration for that id is dispatched. See [recovery.md](recovery.md#shared-correlation-recovery).

The sample also wires two context propagators (`SampleTracePropagator`, `SampleTenantPropagator`) —
watch the `traceId`/`tenant` fields in the logs: `/request-response` shows them on `HANDLER:` lines
(flowing into the response handler via `ExecutionContext`), `/worker` shows them on the `WORKER:`
line (the in-memory worker, also `ExecutionContext`), and the `/arm`→`/crash`→`/publish` flow shows
them on the `RECOVERY:` line — there they survived the simulated crash as serialized baggage
persisted in the recovery state.
