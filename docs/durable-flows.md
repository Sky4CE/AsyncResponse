# Durable flows

AsyncResponse's headline use is awaiting one response. Its most powerful use is composing
**dozens** of them: a multi-step flow across remote services, written as plain sequential C#,
that survives crashes, redeploys, and redeliveries mid-step and resumes exactly where it left
off. Durable flows are a first-class API in `AsyncResponse.Core` — the library owns the
checkpointing, the crash-recovery bookkeeping, and the recovery callbacks, so your flow is just
the steps. Built-in flow stores also fence duplicate deliveries across application replicas with
atomic creation, optimistic revisions, and a renewable execution lease.

**On this page**

- [The rules (there are only three)](#the-rules-there-are-only-three)
- [Injecting your own services](#injecting-your-own-services-signalr-audit-metrics)
- [Child flows](#child-flows)
- [What happens when things die](#what-happens-when-things-die)
- [Editing a flow](#editing-a-flow) · [Compensation](#compensation)
- [Cookbook: patterns from production flows](#cookbook-patterns-from-production-flows)
- [Testing your flows](#testing-your-flows) · [Observing runs](#observing-runs)
- [Storage: where flow state lives](#storage-where-flow-state-lives)
- [Under the hood](#under-the-hood) · [Honest comparison with a dedicated workflow engine](#honest-comparison-with-a-dedicated-workflow-engine)

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

Every AsyncResponse registration explicitly chooses a state store. Applications that only await
individual responses use `.WithInMemoryDurableFlows()` as the zero-infrastructure choice; no ledger
is created until a flow starts. For restart-safe production flows, select a provider-backed atomic
store:

```csharp
var connectionString = builder.Configuration.GetConnectionString("SqlServer")
    ?? throw new InvalidOperationException("ConnectionStrings:SqlServer is required.");

builder.Services.AddAsyncResponse()
    .WithSqlServerChannel(options =>
        options.ConnectionString = connectionString)
    .WithSqlServerTransport(options =>
        options.ConnectionString = connectionString)
    .WithSqlServerDurableFlows(options =>
        options.ConnectionString = connectionString);
```

Built-in store packages are available for SQL Server, PostgreSQL, MySQL/MariaDB, SQLite, Oracle,
MongoDB, Azure Cosmos DB, DynamoDB, and EF Core. Use `.WithInMemoryDurableFlows()` for tests or one
process, and `.WithDurableFlows<TStore>()` only when those packages do not match your storage model.

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

## Injecting your own services (SignalR, audit, metrics)

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
with the failed child snapshot and throws `DurableFlowFailedException` by default. A failed child's
step keeps its `Faulted = true` marker even after memoization, so operators see the failure on the
step itself instead of digging through the memoized child snapshot.

**The child id contract.** A child flow id is **exclusive to one step of the parent that started
it**. The
notification that resumes a suspended parent follows the child's single `ParentFlowId`, so if a
step awaits an id that belongs to another parent — or to a top-level run started via
`IDurableFlows.StartAsync` — the parent would park forever. The library rejects that loudly
instead: awaiting a foreign id throws `DurableFlowFailedException`. The persisted parent step,
flow type, input type, and semantic JSON input value are validated both while waiting and when a
completed checkpoint is replayed, so changed arguments can never silently adopt a stale child.
The default id, `{parentFlowId}:{stepName}`, is always safe; pass a custom nonblank `flowId` only
when it is unique per parent step, and keep that id and input stable on every replay.

**No timeout on a child wait — deliberately.** A suspended parent holds no worker, so there is
nothing to time out cheaply; the child is bounded by its own step timeouts and by the worker
transport's dead-lettering. If a child gets stuck, that shows up as the child's alarm (its DLQ
entry or its stale ledger), not as a silent parent hang — see the failure table below.

**Ledger-size note.** The memoized child snapshot excludes the captured ambient `Context` (it is
propagation machinery the parent never needs), but it does embed the child's own step results —
so deeply nested parent → child → grandchild chains grow the parent's ledger with each completed
child. Keep very large payloads in your own storage and pass references through flow state.
Stores enforce a `MaxStateBytes` budget on every write (defaulted below each provider's hard
item/document cap — DynamoDB 400 KB, Cosmos 2 MB, MongoDB 16 MB); an oversized checkpoint fails
with an error naming the flow id, size, and limit instead of a raw provider error
(see [configuration.md](configuration.md#common-durable-flow-options)).

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
| Process crashes **before** a step | Worker redelivery re-runs the flow; completed steps skip; the step runs normally. This relies on the worker subscriber's default `AckAfterHandlerCompletes` — the wake job must stay unacknowledged until the handler finishes |
| The worker subscriber uses **early ACK** (`UseAckAfterEnqueue`) | Vetoed at startup: a crash after the ACK but before execution would strand the run as `Running` with no lease, no queued job, and no discovery API. `DurableFlowOptions.AllowEarlyAckWorkerSubscriber = true` accepts the risk — a stranded run then waits for an operator `ResumeAsync(flowId)` (a run already awaiting a step also self-heals when its response arrives and recovery re-enqueues it) |
| Process crashes **while awaiting** a remote step | The re-run **re-attaches** to the in-flight wait via the persisted correlation-id breadcrumb — the request is *not* re-sent; progress keeps streaming. (One unavoidable sliver: if the response had already been claimed for delivery at the instant of the crash — or its recovery registration was deleted by a cancelled execution's teardown just as it arrived — nothing can replay it; the re-attached wait then ends at the step timeout and the idempotent step restarts fresh. Disposal drains an in-flight delivery first — bounded by `DisposalDrainTimeout` — so a response mid-processing settles as delivered rather than falling into this sliver; if that drain budget lapses with the delivery still running, the wait faults as *indeterminate* instead of cancelling, and the step restarts fresh immediately rather than re-attaching to a possibly-consumed correlation id) |
| Process is **down** when a progress/success response arrives | The payload's `OnRecovery() == Resume` routes to the auto-registered recovery callback with the materialized payload. It finds the step by correlation id, checkpoints the actual payload, clears the pending wait, and re-enqueues the run. A payload that classifies a progress message as `KeepWaiting` skips all of that: nothing fires, the registration stays armed, and only the terminal response resumes — one recovery per step instead of one per checkpoint |
| Process is down when a **failed** response arrives | `OnRecovery() == Fail` routes to the auto-registered **failure** callback: the run is marked `Failed` — a failure is never resumed as a success |
| The **terminal** response itself was the lost message | Its payload is already the step result. The resumed run skips that completed await and continues; it does not wait for a consumed correlation id or re-send the remote request |
| The same flow job is delivered to two replicas | Atomic start preserves the first input, and the execution lease lets one worker run. The duplicate delivery returns without entering flow code; if the owner disappears, the lease expires and another worker resumes from the last compare-and-swap checkpoint |
| `StartAsync`'s **publish fails ambiguously** (the job may or may not have been accepted) | With a **caller-supplied `flowId`**, retrying `StartAsync` is safe: the atomic create dedupes and re-enqueues the same run. With a **generated id** (the `flowId: null` default), a retry mints a fresh id — a second independent run is created and, if the first publish had actually been accepted, **both execute**. Supply deterministic ids wherever the caller may retry. If the create succeeded but the publish threw outright, the run exists as `Running` with no job: retry with the same id, or call `ResumeAsync(flowId)` |
| A child flow is running | The parent run is parked as `Running`; the child terminal state re-enqueues the parent, which reloads the child state and continues |
| A **child run dead-letters** (a retriable failure exhausts the transport's delivery attempts) | The child stays `Running` and the parent stays suspended — **the child's DLQ entry is the alarm**. Replay the DLQ entry or call `ResumeAsync(childFlowId)`; re-enqueueing the parent (`ResumeAsync(parentFlowId)`) also works — it re-enqueues the child. The parent resumes automatically once the child reaches a terminal state |
| You want a dead-lettered run to **wait for you** | A `Running` run can be resurrected at any time by a late response or recovery — by design. To take manual control first, set the run's status to `FlowRunStatus.Suspended` in the flow store: wake-ups, recoveries, resumes, and failure signals are all ignored while suspended (a parent awaiting a suspended child keeps waiting). When ready, set it back to `Running` and call `ResumeAsync(flowId)` to replay from checkpoints. **Park only runs that are not mid-execution**: the store write bumps the ledger revision, so a worker actively executing that flow fails its next checkpoint (logged as a lost execution lease) and everything after its last checkpoint replays on un-park — the normal at-least-once replay, but with side effects that already ran once |
| The **child's ledger expired** while the parent was suspended | The parent step fails terminally with `DurableFlowFailedException` (`"has no state (expired or deleted)"`) instead of silently re-running the child's side effects — the child's outcome is unknowable. Size `DurableFlowOptions.StateExpiry` beyond the longest child idle time; the TTL refreshes on every checkpoint |
| The **parent's ledger expired** while suspended | Same sizing rule — `StateExpiry` bounds the idle time of a suspended parent too. An expired run cannot be resumed: the executor logs a warning and no-ops |
| A step keeps failing | The exception propagates; the worker transport redelivers the run with bounded attempts, then **dead-letters it — that's your "run is stuck" alarm** |
| The flow decides it's hopeless | Throw `DurableFlowFailedException`: the run is marked `Failed` terminally, with no redelivery |
| The **parent fails (or is failed) while a child still runs** | The child is deliberately independent: it keeps running to completion — its side effects happen — and its terminal notification to the already-terminal parent is a no-op. There is no cascade-cancel. If the child's work must not continue, act on the child explicitly: park it (`FlowRunStatus.Suspended`) or fail it (`IDurableFlowExecutor.FailAsync`). Policy modes (cascade cancel/park on parent failure) are on the roadmap |

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
`OnRecovery()` semantics.

**Operator controls.** Expose your own endpoints over `IDurableFlows`: start with a
caller-supplied `flowId` for idempotent "run it" buttons, `GetStateAsync` for a progress screen
(status, per-step checkpoints, `LastMessage`), `ResumeAsync` as the "kick it" action. The sample
app ships exactly this (`/durable-flow`, see [sample.md](sample.md)).

## Testing your flows

Test the real thing: the in-memory channel + transport give you the full engine in-process, so a
flow test is *start → answer the triggers → assert the state* — no mocks of library internals.

```csharp
var services = new ServiceCollection();
services.AddSingleton<IWorkspaceService, FakeWorkspaceService>();
services.AddSingleton<IMigrationService, FakeMigrationService>();
services.AddSingleton<IImportService, FakeImportService>();
services.AddSingleton<INotifier, FakeNotifier>(); // ordinary DI: fakes replace production services
services.AddScoped<TenantProvisioningFlow>();
services.AddAsyncResponse()
    .WithInMemoryChannel()
    .WithInMemoryTransport()
    .WithInMemoryDurableFlows();
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
NATS, PostgreSQL, SQL Server, MongoDB) over real infrastructure, and a stress-harness storm asserting
exactly-once step execution across hundreds of concurrent flows. For pure unit tests of flow
logic, `IDurableFlowContext` is an interface you can fake outright.

## Observing runs

- `IDurableFlows.GetStateAsync(flowId)` → the full `FlowState` snapshot: status, per-step
  checkpoints, `LastMessage` progress, attempts, the value bag.
- `FlowState.Attempts` counts **executions** of the run — every time the executor picks it up,
  including resumes after a suspension or a re-enqueue — not only failures. A parent that suspends
  for three children will legitimately show four-plus attempts on a fully successful run.
- Child flow relationships are visible in state: the parent step has `ChildFlowId`, and the child
  run has `ParentFlowId`/`ParentStepName`.
- `flow.SetValueAsync(key, value)` checkpoints arbitrary values immediately.
  `flow.ReportProgressAsync(...)` updates operator-facing progress; by default rapid reports within
  one second are coalesced into the next checkpoint or outcome to avoid rewriting the whole ledger
  for every tick. Set `ProgressPersistenceInterval = TimeSpan.Zero` in the selected
  `With*DurableFlows(...)` callback to write every report immediately.
- Executions emit an `asyncresponse.flow.execute` activity tagged with the flow id and type.

## Storage: where flow state lives

Flow state is explicit and separate from channel recovery metadata. Choose exactly one store in
every registration. A common production setup keeps the ledger beside the application's domain data:

```csharp
using Npgsql;

var connectionString = builder.Configuration.GetConnectionString("PostgreSQL")
    ?? throw new InvalidOperationException("ConnectionStrings:PostgreSQL is required.");

builder.Services.AddSingleton(_ => NpgsqlDataSource.Create(connectionString));

builder.Services.AddAsyncResponse()
    .WithPostgreSqlChannel(options => options.SchemaName = "public")
    .WithPostgreSqlTransport(options => options.SchemaName = "public")
    .WithPostgreSqlDurableFlows(options =>
    {
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
| Entity Framework Core (any relational provider) | `WithEFCoreDurableFlows<TDbContext>(...)` |

For tests, development, or a deliberately one-process application:

```csharp
builder.Services.AddAsyncResponse()
    .WithInMemoryChannel()
    .WithInMemoryTransport()
    .WithInMemoryDurableFlows();
```

For application-owned storage, implement the single atomic `IFlowStateStore` contract and register
it explicitly:

```csharp
public sealed class MyDatabaseFlowStateStore : IFlowStateStore
{
    // Atomic create, revision-checked update, lease acquire/renew/release,
    // TTL-filtered load, and delete are all required.
}

builder.Services
    .AddAsyncResponse()
    .WithInMemoryChannel()
    .WithInMemoryTransport()
    .WithDurableFlows<MyDatabaseFlowStateStore>();
```

There is no weaker three-method or local-lock compatibility mode. Every store must atomically
create a run, compare-and-swap revisions, and fence execution with renewable leases. Built-in
providers implement the full contract; the in-memory store implements it within one process.

The store packages register their stores as **singletons** — schema/index/container provisioning
runs once per process, and a host-registered client (`NpgsqlDataSource`, `IMongoDatabase`,
`CosmosClient`, `IAmazonDynamoDB`) is reused when present. `WithDurableFlows<TStore>()`
registers *your* store as **scoped**, so EF Core `DbContext`-style dependencies work normally.

`StateExpiry` defaults to 14 days and refreshes on every checkpoint, so it limits the maximum idle
gap between checkpoints rather than total run duration. The default is deliberately double the
7-day default step-timeout chain (`DefaultStepTimeout` → channel `DefaultTimeout` →
`RecoveryStateExpiry`): a step that silently waits out the full default timeout still faults and
checkpoints before its ledger can expire, instead of racing it. Expired, malformed,
identity-mismatched, revision-mismatched, or unsupported-schema ledgers load as absent instead of
entering execution.

Full contract, package lifetimes, schema requirements, and expired-state cleanup:
[durable-flow-state-stores.md](durable-flow-state-stores.md). `StateExpiry` and
`DefaultStepTimeout` live on the selected store options — see
[configuration.md](configuration.md#common-durable-flow-options).

## Under the hood

You don't need any of this to use flows — it's here for the curious.

The API encodes the *checkpointed-flow pattern*, extracted from years of production use:

- Each run persists a **ledger** (`FlowState`): a checkpoint per step name plus the run's
  status — human-readable JSON you can query and, in an emergency, hand-edit.
- `AwaitStepAsync` creates the response subscription **first**, then persists the correlation-id
  **breadcrumb**, then runs your trigger. That ordering is the whole trick: "breadcrumb exists"
  implies "someone is listening", so a crash on either side of the send re-attaches or safely
  restarts — never a lost run, never a double-send.
- On durable channels, every awaited step auto-registers the flow executor's payload-recovery and
  failure methods as lost-subscriber callbacks — the same recovery machinery as
  [recovery.md](recovery.md), with its at-least-once, idempotency-required contract. (On the
  in-memory channel these callbacks don't exist; flows still checkpoint and re-attach, with
  process-lifetime durability.)
- Starting a flow enqueues a worker job carrying only the flow id; resume, redelivery, and
  operator kicks all re-enqueue that same job. `StartAsync` with a caller-supplied `flowId` is
  atomically idempotent for the same flow type and semantically identical input. Conflicting reuse
  is rejected; an existing run is never replaced silently. A **generated** id (the default) cannot
  survive a retried ambiguous publish — the retry mints a fresh id and a second independent run —
  so supply deterministic ids wherever the caller may retry (see the failure table above).
- Built-in stores persist a monotonic `FlowState.Revision`. Every execution owns a renewable lease
  and every checkpoint requires both the expected revision and that lease, so a stale worker cannot
  overwrite recovery state written by a newer execution.

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
| Extra infrastructure | None new — the flow ledger lives in a database/store you already run | An engine cluster/service to operate and upgrade |

Reach for an engine when you need engine-*owned* semantics: automatically derived compensation
graphs, months-long durable timers, human-approval tasks, or replayable audit histories. For
request/response orchestration — even at dozens of steps — durable flows are smaller,
transparent, and hotfix-friendly.
