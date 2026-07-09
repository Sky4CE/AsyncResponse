# Durable flows

AsyncResponse's headline use is awaiting one response. Its most powerful use is composing
**dozens** of them: a multi-step flow across remote services, written as plain sequential C#,
that survives crashes, redeploys, and redeliveries mid-step and resumes exactly where it left
off. Durable flows are a first-class API in `AsyncResponse.Core` — the library owns the
checkpointing, the crash-recovery bookkeeping, and the recovery callbacks, so your flow is just
the steps.

```csharp
public sealed record ProvisioningInput(long TenantId);

public sealed class TenantProvisioningFlow(
    IWorkspaceService _workspaces,      // a plain local dependency
    IMigrationService _migrations,      // remote systems that reply via broker/webhook
    IImportService _imports,
    INotifier _notifier) : IDurableFlow<ProvisioningInput>
{
    public async Task ExecuteAsync(IDurableFlowContext flow, ProvisioningInput input)
    {
        // A local step: runs once per flow run, its result is memoized in the flow state.
        var workspaceId = await flow.StepAsync("create-workspace",
            () => _workspaces.CreateAsync(input.TenantId));

        // An awaited step: triggers the remote job and durably awaits its terminal response.
        var migration = await flow.AwaitStepAsync<MigrationResult>("run-migration",
            trigger: cid => _migrations.StartAsync(input.TenantId, cid),
            until: r => r.Status != MigrationStatus.Running);

        if (migration.Status == MigrationStatus.Failed)
            throw new DurableFlowFailedException($"Migration failed: {migration.Message}");

        // Progress-aware awaited step: intermediate responses keep the wait open.
        await flow.AwaitStepAsync<ImportResult>("import-data",
            trigger: cid => _imports.StartAsync(input.TenantId, cid),
            until: async r =>
            {
                if (r.State == ImportState.InProgress)
                {
                    await flow.ReportProgressAsync(r.Message);   // → FlowState.LastMessage → your UI
                    return false;
                }
                return true;
            });

        await flow.StepAsync("notify", () => _notifier.SendProvisionedAsync(input.TenantId));
    }
}
```

Register the flow class and start it:

```csharp
builder.Services.AddScoped<TenantProvisioningFlow>();

// anywhere (e.g. a controller):
var flowId = await _flows.StartAsync<TenantProvisioningFlow, ProvisioningInput>(
    new ProvisioningInput(tenantId));

// later: observe or kick it
FlowState? state = await _flows.GetStateAsync(flowId);
await _flows.ResumeAsync(flowId);
```

`IDurableFlows` and `IDurableFlowContext` are registered by `AddAsyncResponse()`. The default
`IFlowStateStore` stores flow ledgers in the channel recovery store, which is handy for tests,
development, and migration. For production durable flows, keep state in application-owned storage:

```csharp
builder.Services
    .AddAsyncResponse()
    .WithRedisChannel()
    .WithRabbitMqTransport(...)
    .WithSqlServerDurableFlows(options =>
    {
        options.ConnectionString = builder.Configuration.GetConnectionString("SqlServer");
    });
```

Built-in store packages are available for SQL Server, PostgreSQL, MySQL/MariaDB, SQLite, Oracle,
MongoDB, Azure Cosmos DB, and DynamoDB. Use `WithCustomDurableFlows<TStore>()` only when those
packages do not match your storage model.

## The rules (there are only three)

1. **Step names are stable and unique** within the flow — they key the checkpoints. Renaming a
   step makes in-flight runs re-execute it under the new name.
2. **Steps are idempotent** (or harmless to repeat). Everything is at-least-once: a crash
   between a step completing and its checkpoint persisting re-executes that step on resume.
   Key remote side effects on the correlation id (`AwaitStepAsync` hands it to your trigger)
   or on `flow.FlowId`.
3. **The flow class resolves from DI by its persisted type name** — register the class itself
   and treat its name like a recovery-callback name: rename only with a forwarding type.

Everything else — what your `ExecuteAsync` does between steps, conditionals, loops, computed
values — is ordinary C#. Values that must stay stable across resumes (computed dates, generated
ids) belong inside a `StepAsync<TResult>` so they're memoized rather than recomputed.

## Injecting your own services (SignalR, audit, metrics, …)

A flow is a plain DI class: inject whatever it needs and call it from anywhere — step bodies,
between steps, or inside `until` predicates. Pushing live progress to a UI is just another
dependency:

```csharp
public sealed class ReportFlow(
    IHubContext<ProgressHub> _hub,          // SignalR — or any service you own
    IReportJobs _jobs) : IDurableFlow<ReportInput>
{
    public async Task ExecuteAsync(IDurableFlowContext flow, ReportInput input)
    {
        await flow.AwaitStepAsync<JobResult>("build-report",
            trigger: cid => _jobs.StartAsync(input.ReportId, cid),
            until: async r =>
            {
                if (r.State == JobState.Running)
                {
                    await _hub.Clients.Group(input.ReportId).SendAsync("progress", r.Message);
                    await flow.ReportProgressAsync(r.Message!);   // and persist it on the state
                    return false;
                }
                return true;
            });

        await _hub.Clients.Group(input.ReportId).SendAsync("done", flow.FlowId);
    }
}
```

One idempotency note: side effects in `until` predicates fire once per *received message*, and
side effects in step bodies fire once per *step execution* — which, under crash-resume, is
at-least-once. A duplicate "progress 50%" toast is usually fine; if a side effect must be truly
once, put it in its own checkpointed `StepAsync`.

## Child flows

Use child flows when a stage is large enough to deserve its own durable ledger, progress screen,
or retry boundary. The parent starts the child once and then parks: no worker is held while the
child (or grandchildren) run.

```csharp
public sealed class TenantProvisioningFlow : IDurableFlow<ProvisioningInput>
{
    public async Task ExecuteAsync(IDurableFlowContext flow, ProvisioningInput input)
    {
        var migration = await flow.AwaitChildFlowAsync<TenantMigrationFlow, MigrationInput>(
            "migrate-tenant",
            new MigrationInput(input.TenantId),
            flowId: $"{flow.FlowId}:migration");

        await flow.SetValueAsync("migration-flow-id", migration.FlowId);
        await flow.StepAsync("notify", () => _notifier.SendProvisionedAsync(input.TenantId));
    }
}
```

`AwaitChildFlowAsync` checkpoints the parent step with the child id, creates the child `FlowState`
with `ParentFlowId`/`ParentStepName`, enqueues the child, and suspends the parent run. When the
child reaches `Succeeded` or `Failed`, the child executor re-enqueues the parent. On resume the
parent reloads the child state: `Succeeded` completes the parent step; `Failed` completes the step
with the failed child snapshot and throws `DurableFlowFailedException` by default.

For best-effort child work, keep the failure as data:

```csharp
var audit = await flow.AwaitChildFlowAsync<AuditFlow, AuditInput>(
    "audit",
    new AuditInput(input.TenantId),
    failOnChildFailure: false);

if (audit.Status == FlowRunStatus.Failed)
    await flow.ReportProgressAsync($"Audit failed; continuing ({audit.LastMessage})");
```

Do not hand-roll child waits by combining `AwaitStepAsync` with `IDurableFlows.StartAsync`. That
keeps the parent worker busy while the child waits in the same worker queue; a single-worker
transport (including the in-memory transport used in tests) can starve the child. The child-flow
primitive parks the parent first, so parent → child → grandchild chains are deadlock-free under the
same worker.

## What happens when things die

The flow body always runs from the top; checkpoints make re-running cheap and safe. That single
property makes every failure mode collapse into "run it again":

| What dies | What the library does |
|---|---|
| Process crashes **before** a step | Worker redelivery re-runs the flow; completed steps skip; the step runs normally |
| Process crashes **while awaiting** a remote step | The re-run **re-attaches** to the in-flight wait via the persisted correlation-id breadcrumb — the request is *not* re-sent; progress keeps streaming |
| Process is **down** when a progress/success response arrives | The payload's `ShouldResumeOnRecovery() == true` routes to the auto-registered **resume** callback, which re-enqueues the run on the worker transport |
| Process is down when a **failed** response arrives | `ShouldResumeOnRecovery() == false` routes to the auto-registered **failure** callback: the run is marked `Failed` — a failure is never resumed as a success |
| The **terminal** response itself was the lost message | Recovery consumed it, so the re-attached wait has nothing to receive: the step times out and restarts fresh — rule 2 (idempotent steps) is what makes that safe |
| A child flow is running | The parent run is parked as `Running`; the child terminal state re-enqueues the parent, which reloads the child state and continues |
| A step keeps failing | The exception propagates; the worker transport redelivers the run with bounded attempts, then **dead-letters it — that's your "run is stuck" alarm** |
| The flow decides it's hopeless | Throw `DurableFlowFailedException`: the run is marked `Failed` terminally, with no redelivery |

Two exception semantics, deliberately: **any ordinary exception is retriable** (transport
redelivery will re-run the flow), **`DurableFlowFailedException` is terminal**. Domain failures
you detect in a response (`migration.Status == Failed`) are yours to classify — throw the
terminal exception, throw a retriable one, or run compensating steps first.

## Editing a flow

- **Insert a step**: add a `StepAsync`/`AwaitStepAsync` call. In-flight runs execute it on their
  next resume — no state migration, because checkpoints are keyed by name, not position.
- **Reorder steps**: move the calls. Order isn't persisted.
- **Run a subset / skip steps**: ordinary `if` statements around steps.
- **Hotfix an in-flight run**: deploy the fix, `ResumeAsync(flowId)` (or wait for redelivery) —
  runs continue into the *current* code. No replay history, no determinism constraints, no
  workflow-version patching.

## Compensation

The flow state tells you exactly which steps completed (`FlowState.Steps`), so compensation is
explicit and local: catch the failure in the flow, run compensating steps (guarded by their own
names, awaited through `AwaitStepAsync` if remote), then throw `DurableFlowFailedException` to
close the run. You author the undo logic next to the steps it undoes; what you don't get is an
engine deriving the compensation sequence for you.

## Cookbook: patterns from production flows

These are the shapes the API was distilled from — a production system running provisioning
pipelines of a dozen-plus steps across SQL jobs, orchestrators, and ticketing systems.

**Best-effort step (catch and continue).** A stage that should not sink the pipeline:

```csharp
try
{
    await flow.AwaitStepAsync<DagRunResult>("run-lineage",
        trigger: cid => _dags.TriggerLineageAsync(cid),
        until: r => r.State is not DagRunState.Queued and not DagRunState.Running,
        timeout: TimeSpan.FromMinutes(30));
}
catch (Exception ex)
{
    await flow.ReportProgressAsync($"lineage failed ({ex.Message}); continuing");
}
```

The step is recorded as faulted-not-completed and the flow moves on. If the run is later resumed,
a faulted awaited step restarts fresh — which is what you want for a best-effort stage.

**Subset runs.** "Only create the ticket this time" is an input flag and an early return — no
pre-seeded state:

```csharp
await flow.StepAsync("create-ticket", () => _tickets.CreateAsync(input.TenantId));
if (input.TicketOnly)
    return;
```

**A different payload type per awaited step.** Each `AwaitStepAsync<T>` declares its own response
type — a SQL-job status here, an Airflow DAG result there — with its own `until` and its own
`ShouldResumeOnRecovery()` semantics.

**Operator controls.** Expose your own endpoints over `IDurableFlows`: start with a
caller-supplied `flowId` for idempotent "run it" buttons, `GetStateAsync` for a progress screen
(status, per-step checkpoints, `LastMessage`), `ResumeAsync` as the "kick it" action. The sample
app ships exactly this (`/durable-flow`, see [sample.md](sample.md)).

## Testing your flows

Test the real thing: the in-memory channel + transport give you the full engine in-process, so a
flow test is *start → answer the triggers → assert the state* — no mocks of library internals.

```csharp
var services = new ServiceCollection();
services.AddSingleton<FakeNotifier>();          // your fakes, injected like production services
services.AddScoped<TenantProvisioningFlow>();
services.AddAsyncResponse().WithInMemoryChannel().WithInMemoryTransport();
await using var provider = services.BuildServiceProvider();

var flows = provider.GetRequiredService<IDurableFlows>();
var executor = provider.GetRequiredService<IDurableFlowExecutor>();
var publisher = provider.GetRequiredService<IAsyncResponsePublisher>();

var flowId = await flows.StartAsync<TenantProvisioningFlow, ProvisioningInput>(new(7));
var run = executor.ExecuteAsync(flowId);                    // drive directly in unit tests
await publisher.SetResponse(new MigrationResult { … }, capturedCorrelationId);
await run;

Assert.Equal(FlowRunStatus.Succeeded, (await flows.GetStateAsync(flowId))!.Status);
```

Crash-resume is testable deterministically: throw from a step (or seed a
`PendingCorrelationId` breadcrumb), run the executor again, and assert every step executed
exactly once. The library's own suites are the reference:
[`DurableFlowScenarioTests`](../tests/AsyncResponse.Tests/DurableFlowScenarioTests.cs) (a
production-shaped pipeline with a crash-at-every-checkpoint matrix, subset runs, catch-and-continue,
and injected notifications), [`DurableChildFlowTests`](../tests/AsyncResponse.Tests/DurableChildFlowTests.cs)
(single-worker parent → child → grandchild execution plus the old hand-rolled starvation
regression), integration tests running the same flow against **every durable channel** (Redis,
NATS, PostgreSQL, SQL Server) over real infrastructure, and a stress-harness storm asserting
exactly-once step execution across hundreds of concurrent flows. For pure unit tests of flow
logic, `IDurableFlowContext` is an interface you can fake outright.

## Observing runs

- `IDurableFlows.GetStateAsync(flowId)` → the full `FlowState` snapshot: status, per-step
  checkpoints, `LastMessage` progress, attempts, the value bag.
- Child flow relationships are visible in state: the parent step has `ChildFlowId`, and the child
  run has `ParentFlowId`/`ParentStepName`.
- `flow.ReportProgressAsync(...)` and `flow.SetValueAsync(key, value)` persist operator-facing
  progress and arbitrary values on the state.
- Executions emit an `asyncresponse.flow.execute` activity tagged with the flow id and type.

## Storage: where flow state lives

By default, flow state rides in the channel's `IRecoveryStateStore` (one entry per run under a
sentinel marker; the watchdog knows to skip them). This keeps tests, development, and migrations
simple, but recovery stores are often cache-shaped: Redis keys expire, NATS KV buckets may have
limits, and `AsyncResponseOptions.DurableFlows.StateExpiry` defaults to 7 days. That TTL refreshes
on every checkpoint, so it bounds the gap *between* checkpoints, not total run duration.

To keep flow state in durable app-owned storage (e.g. a table next to the domain entities the
flow operates on, where your dashboards already look), use a store package:

```csharp
builder.Services.AddAsyncResponse()
    .WithRedisChannel()
    .WithRabbitMqTransport(...)
    .WithPostgreSqlDurableFlows(options =>
    {
        options.ConnectionString = builder.Configuration.GetConnectionString("PostgreSQL");
        options.SchemaName = "public";
        options.TableName = "asyncresponse_flow_state";
    });
```

Supported packages:

| Store | Package registration |
|---|---|
| SQL Server | `WithSqlServerDurableFlows(...)` |
| PostgreSQL | `WithPostgreSqlDurableFlows(...)` |
| MySQL / MariaDB | `WithMySqlDurableFlows(...)` |
| SQLite | `WithSqliteDurableFlows(...)` |
| Oracle | `WithOracleDurableFlows(...)` |
| MongoDB | `WithMongoDbDurableFlows(...)` |
| Azure Cosmos DB | `WithCosmosDurableFlows(...)` |
| DynamoDB | `WithDynamoDbDurableFlows(...)` |

If your application already has a different persistence abstraction, register your own store. The
library calls exactly three members:

```csharp
public interface IFlowStateStore
{
    Task SaveAsync(string flowId, FlowState state, TimeSpan ttl, CancellationToken ct = default);
    Task<FlowState?> LoadAsync(string flowId, CancellationToken ct = default);
    Task<bool> TryDeleteAsync(string flowId, CancellationToken ct = default);
}

builder.Services
    .AddAsyncResponse()
    .WithRedisChannel()
    .WithRabbitMqTransport(...)
    .WithCustomDurableFlows<MyDatabaseFlowStateStore>();
```

Store packages and `WithCustomDurableFlows<TStore>()` register the store as scoped, so EF Core
`DbContext`, `NpgsqlDataSource`, `IMongoDatabase`, or similar dependencies can be used normally.
The default recovery-backed store logs a warning the first time it persists flow state, pointing
production apps at the package/custom-store path.

Implementation guide: [durable-flow-state-stores.md](durable-flow-state-stores.md).

## Under the hood (for the curious — you don't need this to use flows)

The API encodes the *checkpointed-flow pattern*, extracted from years of production use:

- Each run persists a **ledger** (`FlowState`): a checkpoint per step name plus the run's
  status — human-readable JSON you can query and, in an emergency, hand-edit.
- `AwaitStepAsync` creates the response subscription **first**, then persists the correlation-id
  **breadcrumb**, then runs your trigger. That ordering is the whole trick: "breadcrumb exists"
  implies "someone is listening", so a crash on either side of the send re-attaches or safely
  restarts — never a lost run, never a double-send.
- On durable channels, every awaited step auto-registers the flow executor's resume/failure
  methods as lost-subscriber callbacks — the same recovery machinery as
  [recovery.md](recovery.md), with its at-least-once, idempotency-required contract. (On the
  in-memory channel these callbacks don't exist; flows still checkpoint and re-attach, with
  process-lifetime durability.)
- Starting a flow enqueues a worker job carrying only the flow id; resume, redelivery, and
  operator kicks all re-enqueue that same job. `StartAsync` with a caller-supplied `flowId` is
  idempotent — an existing run is re-enqueued, never duplicated.

## Honest comparison with a dedicated workflow engine

| Concern | AsyncResponse durable flows | Workflow engine (Temporal, Durable Task) |
|---|---|---|
| Flow definition | Plain C# in your service; steps edited like any code | Workflow code under replay rules: deterministic-only, versioned patches for changes |
| Position after a crash | A human-readable per-run state you can query and hand-edit | Event-sourced history, reconstructed by replay |
| Redeploy mid-step | Re-attach to the in-flight wait; late responses classified by domain outcome | Replay reconstructs position |
| Progress from remote steps | First-class (`until` sees every message; `ReportProgressAsync`) | Signals/queries — more ceremony |
| Hotfixing in-flight runs | Resume into current code; no determinism constraints | Version/patch workflows so old histories still replay |
| Compensation | Explicit: you write compensating steps; the state tells you what completed | Saga frameworks track and run compensations automatically |
| Durable timers (weeks), cron, human tasks | Per-step timeouts up to `StateExpiry`; longer cadence belongs to your scheduler | First-class durable timers |
| Extra infrastructure | None — state rides in the channel you already run | An engine cluster/service to operate and upgrade |

Reach for an engine when you need engine-*owned* semantics: automatically derived compensation
graphs, months-long durable timers, human-approval tasks, or replayable audit histories. For
request/response orchestration — even at dozens of steps — durable flows are smaller,
transparent, and hotfix-friendly.
