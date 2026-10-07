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
- [Durable timers and scheduling](#durable-timers-and-scheduling)
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

        // A durable timer: on a transport with native delayed delivery the run SUSPENDS for six
        // hours — no worker, lease, or memory held — and a crash mid-sleep resumes the remainder.
        await flow.DelayAsync("settle", TimeSpan.FromHours(6));

        await flow.StepAsync("notify", () => _notifier.SendProvisionedAsync(input.TenantId));
    }
}
```

Register the flow class and start it:

```csharp
builder.Services.AddScoped<TenantProvisioningFlow>();
// or, on the registration chain: .WithDurableFlow<TenantProvisioningFlow, ProvisioningInput>()
// — the same scoped registration plus a statically typed execution route (required under
// trimming / Native AOT, see aot.md).

// anywhere (e.g. a controller) that injects IDurableFlows:
var flowId = await _flows.StartAsync<TenantProvisioningFlow, ProvisioningInput>(
    new ProvisioningInput(tenantId));          // optional flowId: parameter for idempotent starts

// later: observe or kick it
FlowState? state = await _flows.GetStateAsync(flowId);
await _flows.ResumeAsync(flowId);
```

Every registration chooses exactly one flow state store. `.WithInMemoryDurableFlows()` is the
zero-infrastructure choice for applications that only await individual responses (no ledger exists
until a flow starts); restart-safe production flows use a provider-backed store:

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

Built-in store packages cover SQL Server, PostgreSQL, MySQL/MariaDB, SQLite, Oracle, MongoDB,
Azure Cosmos DB, DynamoDB, and EF Core; `.WithDurableFlows<TStore>()` plugs in your own. See
[Storage](#storage-where-flow-state-lives).

## The rules (there are only three)

1. **Step names are stable and unique** within the flow — they key the checkpoints. Renaming a
   step makes in-flight runs re-execute it under the new name.
2. **Steps are idempotent** (or harmless to repeat). Everything is at-least-once: a crash
   between a step completing and its checkpoint persisting re-executes that step on resume.
   Key remote side effects on the correlation id (`AwaitStepAsync` hands it to your trigger)
   or on `flow.FlowId`. A `CancellationToken` passed to a step cancels it only *before* its body
   runs; once the side effect has happened, the completion checkpoint is written uninterruptibly.
   A token that cancels a checkpoint mid-write (`SetValueAsync`, `ReportProgressAsync`, an awaited
   step's breadcrumb) surfaces as `OperationCanceledException`, and one read before the next step
   body settles whether the write committed — the execution continues from it, or, if another
   writer moved the ledger meanwhile, is abandoned before that body runs.
3. **The flow class resolves from DI by its persisted type name** — register the class itself
   and treat its name like a recovery-callback name: rename only with a forwarding type.

Everything else — what your `ExecuteAsync` does between steps, conditionals, loops, computed
values — is ordinary C#. Values that must stay stable across resumes (computed dates, generated
ids) belong inside a `StepAsync<TResult>` so they're memoized rather than recomputed. Steps run
**sequentially**: await each context call before making the next — starting a step while another
is still executing (e.g. `Task.WhenAll` over steps) throws `InvalidOperationException`. Run
parallel work inside one step body, without context calls.

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
public sealed class TenantProvisioningFlow(INotifier _notifier) : IDurableFlow<ProvisioningInput>
{
    public async Task ExecuteAsync(IDurableFlowContext flow, ProvisioningInput input)
    {
        FlowState migration = await flow.AwaitChildFlowAsync<TenantMigrationFlow, MigrationInput>(
            "migrate-tenant",
            new MigrationInput(input.TenantId));   // child id defaults to "{parentFlowId}:migrate-tenant"

        await flow.SetValueAsync("migration-flow-id", migration.FlowId);
        await flow.StepAsync("notify", () => _notifier.SendProvisionedAsync(input.TenantId));
    }
}
```

`AwaitChildFlowAsync` checkpoints the parent step with the child id, creates the child `FlowState`
with `ParentFlowId`/`ParentStepName`, enqueues the child, and suspends the parent run. When the
child reaches `Succeeded` or `Failed`, the child executor re-enqueues the parent, which reloads the
child: `Succeeded` completes the parent step; `Failed` completes it with the failed child snapshot
and, by default, throws `DurableFlowFailedException`. A failed child's step keeps `Faulted = true`
so operators see the failure on the step itself. The parent always reads the child through
`IFlowStateStore.LoadCurrentAsync` — the outcome is memoized into the parent's ledger, whose fences
say nothing about the child's, and under a reused child id an older copy would be the *previous*
run's outcome.

**The child id contract.** A child flow id is **exclusive to one step of the parent that started
it**: the notification that resumes a parent follows the child's single `ParentFlowId`, so awaiting
an id that belongs to another parent, or to a top-level run started with `IDurableFlows.StartAsync`,
would park forever. The library throws `DurableFlowFailedException` instead. Until the parent step
has recorded the child's outcome, the child's flow type, input type, and semantic JSON input are
validated against the call too (even when the child has already finished), so a child is never
awaited under input it did not receive; once recorded, the step answers from its memo (see
[editing a flow](#editing-a-flow)). The default id, `{parentFlowId}:{stepName}`, is always safe; pass
a custom nonblank `flowId` only when it is unique per parent step and stable on every replay.

**Flow ids are a portable contract**, enforced centrally at creation so an id cannot work on one
store and fail on another. Every final id — root, composed child, scheduled occurrence — must:

- fit `DurableFlowOptions.MaxFlowIdLength` (400 UTF-16 code units — the `flow_id` column length in
  the SQL Server, MySQL, Oracle, and EF Core stores);
- fit `DurableFlowOptions.MaxFlowIdBytes` (1023 UTF-8 bytes — the Cosmos DB id limit; 400
  non-ASCII characters can reach 1200 bytes);
- be non-empty, well-formed UTF-16 (no unpaired surrogates), and free of `/`, `\`, `?`, `#`
  (Cosmos rejects them) and control characters;
- carry no leading or trailing space — SQL Server pads the shorter operand of an equality
  comparison (binary collations included) and MySQL's `utf8mb4_bin` is PAD SPACE, so `flow` and
  `flow ` would be one key to those stores and two flows to the engine.

Ids are compared **ordinally** everywhere — `flow-a` and `FLOW-A` are two flows — and the
relational stores make the database agree (SQL Server and MySQL pin a binary collation; PostgreSQL,
Oracle, and SQLite verify the comparison is ordinal). Budget root ids for growth: each child level
appends `:{stepName}`, and `WithScheduledFlow` wraps its name as `sched:{name}:{timestamp}`
(validated at registration). A non-portable root id is rejected at `StartAsync`; a non-portable
composed child id fails the parent terminally with the budget in the message.

**No timeout on a child wait — deliberately.** A suspended parent holds no worker, so there is
nothing to time out cheaply; the child is bounded by its own step timeouts and by the worker
transport's dead-lettering. A stuck child shows up as the child's alarm (its DLQ entry or its stale
ledger), not as a silent parent hang — see the failure table below.

**What the parent memoizes.** The child snapshot the parent stores omits the child's captured
ambient `Context` and the `ResultJson` of the child's *own* child steps (their id, completion, and
fault marker are kept), so a memoized snapshot stays the same size at any nesting depth instead of
re-escaping every level below it. A grandchild's outcome lives in its own ledger, loadable through
`IDurableFlows.GetStateAsync(step.ChildFlowId)` while that ledger lives. The child's own local step
results *are* embedded, so keep large payloads in your own storage and pass references. The
`FlowState` that `AwaitChildFlowAsync` returns is this snapshot on **every** execution, including
the first, so parent logic sees one shape before and after a restart. Oversized checkpoints are
covered under [ledger size and cost](#ledger-size-and-cost).

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

## Durable timers and scheduling

Flows sleep durably — `await flow.DelayAsync("payment-window", TimeSpan.FromDays(3))` — and
flows start on cron schedules (`WithScheduledFlow<TFlow, TInput>("nightly", "0 6 * * *", …)`)
with one run per occurrence across replicas and no leader election. The due time is a
checkpoint (crashes resume the remainder, never restart the delay), and on transports with
native delayed delivery a sleeping run suspends entirely — no worker, no lease, no memory while
it sleeps. The full design, the per-transport delayed-delivery matrix, cron syntax, and DST
semantics live in [durable timers, delayed jobs, and cron-scheduled flows](timers-and-scheduling.md).

## What happens when things die

The flow body always runs from the top; checkpoints make re-running cheap and safe. That single
property makes every failure mode collapse into "run it again":

| What dies | What the library does |
|---|---|
| Process crashes **before** a step | Worker redelivery re-runs the flow; completed steps skip; the step runs normally. This relies on the worker subscriber's default `AckAfterHandlerCompletes`: the wake job stays unacknowledged until the handler finishes |
| The worker subscriber uses **early ACK** (`UseAckAfterEnqueue`) | Refused at startup: a crash after the ACK but before execution would strand the run as `Running` with no lease, no queued job, and no way to discover it. `DurableFlowOptions.AllowEarlyAckWorkerSubscriber = true` accepts that risk — a stranded run then waits for an operator `ResumeAsync(flowId)` (a run already awaiting a step also self-heals when its response arrives and recovery re-enqueues it) |
| Process crashes **while awaiting** a remote step | The re-run **re-attaches** to the in-flight wait via the persisted correlation-id breadcrumb — the request is *not* re-sent; progress keeps streaming. One unavoidable sliver: a response already claimed for delivery at the instant of the crash (or whose recovery registration a cancelled execution's teardown deleted as it arrived) cannot be replayed, so the re-attached wait ends at the step timeout and the idempotent step restarts fresh. Waiter disposal first drains an in-flight delivery (bounded by the channel's `DisposalDrainTimeout`); if that budget lapses, the wait faults as *indeterminate* and the step restarts fresh immediately instead of re-attaching to a possibly consumed correlation id |
| A **redelivery repeats faster than an awaited step's timeout** | The step's fault deadline is persisted when the wait is first armed, so each re-attach arms only the remainder of the original window. A deadline that already elapsed faults the step immediately (after checking whether recovery completed it in the gap) |
| Process is **down** when a progress/success response arrives | `OnRecovery() == Resume` routes the materialized payload to the auto-registered recovery callback: it finds the step by correlation id, checkpoints the payload, clears the pending wait, and re-enqueues the run. A payload that classifies a progress message as `KeepWaiting` fires nothing and keeps the registration armed, so only the terminal response resumes the run |
| Process is down when a **failed** response arrives | `OnRecovery() == Fail` routes to the auto-registered **failure** callback: the run is marked `Failed` — a failure is never resumed as a success |
| The **terminal** response itself was the lost message | Its payload is already the step result. The resumed run skips that completed await and continues; it neither waits on a consumed correlation id nor re-sends the request |
| The same flow job is delivered to two replicas | Atomic start preserves the first input, and the execution lease lets one worker run. The duplicate is acknowledged without entering flow code **once the store shows the lease being renewed or taken over** (proof of a live holder); a lease that never changes belongs to a dead owner, so the delivery waits for its *persisted* expiry — even when this deployment configures a shorter `ExecutionLeaseDuration` — and resumes from the last checkpoint. See [lease contention](durable-flow-state-stores.md#lease-contention-and-deployments-that-change-the-lease-duration) |
| The broker redelivers the job whose handler is **still running** | A broker in-flight ceiling lapsed under the handler (Pub/Sub `MaxTotalAckExtension`, RabbitMQ `consumer_timeout`, the SQS 12-hour cap, a Kafka rebalance), so the contending delivery *is* the holder's own job — the last copy of the wake-up. The lease records the job driving it (`WorkerJobEnvelope.JobId`), so such a delivery is never acknowledged as a duplicate: a transport that can delay re-publishes it as the same job just past the holder's lease; otherwise it is handed back with `DurableFlowLeaseContendedException`. Counted on `asyncresponse.flow.own_job_redeliveries` and logged as a warning — shorten the park (`DurableFlowOptions.MaxInProcessParkDuration`) or raise the ceiling |
| The process **dies mid-`StartAsync`** | Publishing the start job is the start's commit point, and the job carries the initial ledger: `IDurableFlowExecutor.CreateAndExecuteAsync` creates the run (insert-if-absent) before executing it. A crash **before** the publish leaves nothing behind; a crash **after** it leaves a job that creates and runs the flow — never a `Running` ledger that nothing will execute. The starter also writes the ledger after publishing; if that write fails transiently, `GetStateAsync` may return null until the worker creates it, and `StartAsync` still returns the id. Past the publish only a start the job would not run either throws: a `DurableFlowIdConflictException` found by that create, or a `FlowStateTooLargeException`/`ArgumentException` a store's preflight missed. A caller-chosen id already bound to different work is refused with `DurableFlowIdConflictException` before anything is published, whenever the existing ledger can be read |
| `StartAsync`'s **publish fails** | After its retry ladder, `StartAsync` throws **`DurableFlowNotDispatchedException`** and **nothing was persisted**. Its `FlowId` is the id the start would have used (including a generated one): if the publish did land, retrying with that id dedupes against the run it created, whereas a fresh generated id would start a second run — supply deterministic ids wherever the caller may retry. The caller's own token firing during the publish ends the ladder with an `OperationCanceledException` that also carries the id (message and `Exception.Data["FlowId"]`). A start job over the ingress envelope budget throws `WorkerJobTooLargeException` unretried. On Kafka, a job that cannot fit `message.max.bytes` minus the dead-letter burial headers surfaces as `DurableFlowNotDispatchedException` — do not retry it; raise the producer's `message.max.bytes` (see [Kafka](transport-semantics.md#kafka)) |
| A child flow is running | The parent run is parked as `Running`; the child's terminal state re-enqueues the parent, which reloads the child and continues |
| A **child run dead-letters** (a retriable failure exhausts the transport's delivery attempts) | The child stays `Running` and the parent stays suspended — **the child's DLQ entry is the alarm**. Replay it (on RabbitMQ strip its `x-death` header first — see [troubleshooting](troubleshooting.md#rabbitmq-startup-warns-about-maxdeliveryattempts-or-a-poison-message-loops-forever)), call `ResumeAsync(childFlowId)`, or `ResumeAsync(parentFlowId)`, which re-enqueues the child. The parent resumes once the child reaches a terminal state |
| You want a dead-lettered run to **wait for you** | A `Running` run can be resurrected at any time by a late response or recovery — by design. To take manual control, set its status to `FlowRunStatus.Suspended` in the flow store: wake-ups, resumes, and failure signals are ignored while suspended (a parent awaiting a suspended child keeps waiting). A recovered **terminal** response is checkpointed into the suspended ledger *without waking it*; non-terminal ones keep the recovery registration armed. When ready, set it back to `Running` and call `ResumeAsync(flowId)`. **Park only runs that are not mid-execution**: the store write bumps the revision, so an executing worker fails its next checkpoint (logged as a lost execution lease) and everything after its last checkpoint replays on un-park, side effects included |
| The **child's ledger expired** while the parent was suspended | The parent step fails terminally with `DurableFlowFailedException` (`"has no state (expired or deleted)"`) instead of re-running the child's side effects — the child's outcome is unknowable. Size `StateExpiry` beyond the longest child idle time; the TTL refreshes on every checkpoint |
| The **parent's ledger expired** while suspended | A descendant's long park (a timer sleep, or an awaited step whose wait window exceeds `StateExpiry`) stamps a **retention floor** (`FlowState.RetainUntilUtc`) on the run and every `Running` ancestor up to the root, and every later write of a non-terminal run keeps its TTL at or above that floor. The extension is **part of the park**: if an ancestor cannot be written (a store outage, or losing every revision race against it), the park fails with nothing published and the redelivery retries it. The walk reads ancestors through `LoadCurrentAsync` before stopping at one that is not `Running` or already covered, detects cycles, and fails the run terminally (`DurableFlowFailedException`) on a cycle or past 256 levels. Only parking propagates: a child that keeps checkpointing without parking longer than its own `StateExpiry` does not extend its parent, so size `StateExpiry` above that child's total duration. An expired run cannot be resumed: the executor logs a warning and no-ops |
| A step keeps failing | The exception propagates; the worker transport redelivers the run with bounded attempts, then **dead-letters it — that's your "run is stuck" alarm** |
| The flow decides it's hopeless | Throw `DurableFlowFailedException`: the run is marked `Failed` terminally, with no redelivery |
| The **parent fails (or is failed) while a child still runs** | The child is deliberately independent: it runs to completion (its side effects happen) and its notification to the terminal parent is a no-op — there is no cascade-cancel. To stop it, park it (`FlowRunStatus.Suspended`) or fail it (`IDurableFlowExecutor.FailAsync`) |

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
- **Change a child flow's input**: a child step that has already **completed** answers from its
  memo whatever the current arguments say — as a completed `StepAsync` never re-reads its lambda
  and a completed `DelayAsync` never re-reads its delay. The mismatch is logged as a warning, so a
  deploy that edits the input cannot fail every parent already past that step. A mismatch against
  a child that is still **running** is terminal: it would be awaited under input it never received.
- **Reuse a step name**: don't. Checkpoints are keyed by name, so a second step under a name that
  already returned in the same execution would be handed the first one's result (the classic
  "step inside a loop ran iteration one and skipped the rest"). The engine rejects it with an
  `InvalidOperationException` naming the step; put the iteration key in the name
  (`$"send-{item.Id}"`). A step that *threw* is not recorded, so retrying it under its own name
  within one execution still works.

## Compensation

The flow state tells you exactly which steps completed (`FlowState.Steps`), so compensation is
explicit and local: catch the failure in the flow, run compensating steps (guarded by their own
names, awaited through `AwaitStepAsync` if remote), then throw `DurableFlowFailedException` to
close the run. You author the undo logic next to the steps it undoes; what you don't get is an
engine deriving the compensation sequence for you.

**Do not compensate on an interruption.** A `catch (Exception)` around a step also catches this
*attempt* being interrupted rather than the work failing:

- the run **parking** — a durable timer or a child flow suspended it, and its wake-up is already
  published;
- the **host stopping** while the run waits in process on a timer. The wait is handed over like a
  hop (checkpoint, immediate wake-up, delivery acknowledged), so a long sleep does not burn a
  delivery attempt per deploy; if that wake-up cannot be published, or the timer is reached on an
  already-stopping host, the delivery is handed back for redelivery instead. This holds even when
  the timer's own token is `ApplicationStopping`.

Both are raised as `DurableFlowInterruptedException`, which derives from
`OperationCanceledException`, so the usual filter excludes it along with caller-token
cancellations. The checkpoints are intact and the work is about to be replayed; a hand-back is not
a failed attempt (nothing is checkpointed over the ledger, no span is marked as an error), though
observers still get `OnRunAttemptFailedAsync`.

The same goes for this attempt **losing the run**: its execution lease lapsed (a store outage, a
GC or VM pause longer than the lease) or another worker took it over, or a checkpoint was refused
because someone else wrote the ledger first (a lost-subscriber recovery, a failure signal, an
operator suspending the run). That is raised as `DurableFlowLeaseLostException`. It derives from
`InvalidOperationException` — what lease loss surfaced as before the type existed — so the
`OperationCanceledException` filter does **not** exclude it on its own: name it too. Compensating
on it runs the refund on a deposed worker, outside any step and fenced by nothing, while the
takeover replays the charge (idempotently, returning the existing charge) and carries on as paid:

```csharp
try
{
    await flow.StepAsync("charge", () => _payments.ChargeAsync(order));
}
catch (Exception ex) when (ex is not (OperationCanceledException or DurableFlowLeaseLostException))
{
    await flow.StepAsync("refund", () => _payments.RefundAsync(order));
    throw new DurableFlowFailedException("Charge failed; refunded.", ex);
}
```

A lost lease is sticky like an interruption — every later context call throws it again, so a
swallowed one cannot checkpoint anything — and the executor never fails the run over it.

The interruption is sticky: flow code that swallows it gets it again from its next context call,
a body that returns after swallowing it is not marked completed, and converting it into another
exception (`DurableFlowFailedException` included) is logged as a warning and overruled.

An awaited response is **not** interrupted by host stop: disposing the step's waiter would delete
its lost-subscriber recovery registration and drop a response arriving during the downtime. The
step keeps waiting until the channel shuts down, which cancels the wait but keeps the
registration, so a response that lands in the downtime is recovered into the checkpoint and the
redelivered execution re-attaches to the same correlation id.

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
catch (Exception ex) when (ex is not (OperationCanceledException or DurableFlowLeaseLostException))
{
    await flow.ReportProgressAsync($"lineage failed ({ex.Message}); continuing");
}
```

The filter matters: without it this also swallows the run parking and the host stopping
(`DurableFlowInterruptedException`), turning a redeploy into "lineage failed", and a takeover
(`DurableFlowLeaseLostException`). The step is recorded as faulted-not-completed and the flow
moves on. If the run is later resumed, a faulted awaited step restarts fresh — which is what you
want for a best-effort stage. The fault also retires the step's correlation id: a response or
failure the remote side sends for it **after** the timeout — even one that reaches a registration
left behind by a crashed or redeployed worker — is ignored, so a late failure cannot fail the run
that already moved past the step, and a late success cannot rewrite the branch it took.

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

Use [`AsyncResponse.Testing`](testing.md): a `FlowTestHarness` that runs the complete engine
in-process on a virtual clock, observes every step through the executor's observer seam, and
scripts the remote side — with **zero instrumentation in the flow class under test**:

```csharp
await using var harness = await FlowTestHarness.StartAsync(options =>
{
    options.ConfigureServices = services => services.AddSingleton<IMigrationService>(fake);
    options.ConfigureAsyncResponse = b => b.WithDurableFlow<TenantProvisioningFlow, ProvisioningInput>();
});

var run = await harness.StartFlowAsync<TenantProvisioningFlow, ProvisioningInput>(new(7));

await run.WaitForAwaitingStepAsync("run-migration");            // durably parked, cid observed
await run.ReplyAsync(new MigrationResult { Status = MigrationStatus.Completed });

await run.WaitForTimerStepAsync("settle");                      // a durable timer parks the run…
await harness.AdvanceAsync(TimeSpan.FromHours(6));              // …virtual time skips it

Assert.Equal(FlowRunStatus.Succeeded, await run.WaitForFinishedAsync());
Assert.Equal(1, run.StepExecutions("create-workspace"));        // exactly-once, asserted
```

Crash-resume is a first-class assertion: `harness.CrashBeforeStep("swap")` /
`CrashAfterStep("swap")` inject a one-shot `SimulatedCrashException` at the exact checkpoint
boundary, the transport redelivers (backoff on the virtual clock), and the run resumes — run a
`[Theory]` over every step for the crash-at-every-checkpoint matrix. Production-sized timeouts,
lost-subscriber recovery across `SimulateRestartAsync()`, timers, and cron schedules are all
deterministic; the guide is [Testing AsyncResponse applications](testing.md).

The library's own suites are the reference:
[`FlowTestHarnessShowcaseTests`](../tests/AsyncResponse.Tests/FlowTestHarnessShowcaseTests.cs)
(a production-shaped pipeline — scripted replies, progress-aware steps, a timer, a remote
failure, and the crash matrix),
[`DurableFlowTimerTests`](../tests/AsyncResponse.Tests/DurableFlowTimerTests.cs) (timer suspend /
in-process / remainder semantics),
[`DurableFlowScenarioTests`](../tests/AsyncResponse.Tests/DurableFlowScenarioTests.cs) (the same
pipeline hand-wired without the harness — also a guide to what the harness abstracts),
[`DurableChildFlowTests`](../tests/AsyncResponse.Tests/DurableChildFlowTests.cs), integration
tests running the same flow against **every durable channel** over real infrastructure, and a
stress-harness storm asserting exactly-once step execution across hundreds of concurrent flows.
For pure unit tests of flow logic, `IDurableFlowContext` is an interface you can fake outright.

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
- Executions emit an `asyncresponse.flow.execute` activity tagged with the flow id and type; the
  flow metrics are listed in [observability.md](observability.md#instruments).

## Storage: where flow state lives

Flow state is explicit and separate from channel recovery metadata; every registration chooses
exactly one store. A common production setup keeps the ledger beside the application's domain data:

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

The store matrix, a registration example per provider, client lifetimes, schema ownership, cleanup,
and the contract for an application-owned `IFlowStateStore` (`.WithDurableFlows<TStore>()`) are in
[durable-flow state stores](durable-flow-state-stores.md). The common flow options (`StateExpiry`,
`DefaultStepTimeout`, lease settings, budgets) are set in the same `With*DurableFlows(...)` callback —
see [configuration.md](configuration.md#common-durable-flow-options).

**`StateExpiry` is an idle TTL.** It defaults to 14 days and refreshes on every checkpoint, so it
bounds the gap between checkpoints, not total run duration. The default is double the 7-day default
step-timeout chain (`DefaultStepTimeout` → channel `DefaultTimeout` → `RecoveryStateExpiry`), so a
step that waits out the full default timeout still faults and checkpoints before its ledger can
expire. While `DefaultStepTimeout` is unset, startup enforces that margin: `StateExpiry` must exceed
the channel's effective default wait timeout (`DefaultTimeout ?? RecoveryStateExpiry`) or startup
throws with the fix; setting `DefaultStepTimeout` explicitly opts out of the check.

Expired, identity-mismatched, and revision-mismatched ledgers never enter execution: an expired one
loads as absent; malformed JSON, an unsupported schema version, or a mismatched identity/revision
throws `FlowStateUnreadableException`, because reporting a present-but-unreadable row as absent
would let a rolling deployment acknowledge a live flow's only wake-up.

### Ledger size and cost

**Every checkpoint rewrites the *whole* ledger** — input, every completed step's result, values,
context — so a run of N steps with similar result sizes serializes about N²/2 step results over its
lifetime (100 steps of 1 KiB: ~6 MB written for a 115 KB final ledger; 400 steps: ~92 MB for
458 KB). Three limits keep that in check:

- **`DurableFlowOptions.MaxRetainedSteps`** (default **256** distinct steps per run — local,
  awaited, timer, and child steps alike). Adding step 257 fails the run terminally *before* that
  step's side effects or trigger; replays of existing checkpoints stay valid, including ledgers
  already past the limit. Raise it (or set `null`) only after measuring; never delete checkpoints
  to make room — that replays their side effects.
- **`DurableFlowOptions.LedgerSizeWarningBytes`** (default 512 KiB — 256 KiB on DynamoDB — `null`
  disables) logs a warning naming the flow when its size first crosses the threshold and again at
  each doubling. The size is what the store measured for the write: the UTF-8 bytes of the
  serialized ledger, the same bytes `MaxStateBytes` judges. (It used to be a character count of the
  strings the ledger carries, which JSON escaping can leave far below the stored size.) A custom
  store that does not measure is judged by that lower-bound estimate.
  The warning comes from whichever write first records the crossing — the first execution for an
  initial ledger already past it, the recovery write for a response checkpointed without the lease,
  the first failing attempt for a retried failure's message.
- **The store's `MaxStateBytes`** is the hard cap: the DynamoDB, Cosmos DB, and MongoDB stores
  default it below the provider's item limit (350 000 bytes under DynamoDB's 400 KB, 1 900 000
  under Cosmos DB's 2 MB, 15 000 000 under MongoDB's 16 MB); the relational and EF Core stores
  leave it unset (`null`) unless you configure one. An oversized write fails with
  `FlowStateTooLargeException` (`FlowId`, `SerializedSizeBytes`, `MaxStateBytes`) instead of a raw
  provider error. A `LedgerSizeWarningBytes` at or above a store's `MaxStateBytes` could never fire
  before the cap refused a checkpoint: left at its default it is lowered to three quarters of the
  cap (and the DynamoDB store's default is 256 KiB, under its 350 KB cap), while a value you set
  there is refused when the store validates its options at startup.

Measured with the production serializer, retaining about 1 KiB per step and serializing once after
each step — four times the steps cost about sixteen times the bytes:

| Retained steps | Cumulative bytes serialized | Mean bytes per checkpoint |
|---:|---:|---:|
| 64 | 2,286,541 | 35,727 |
| 128 | 9,066,755 | 70,834 |
| 256 (the default `MaxRetainedSteps`) | 36,118,147 | 141,087 |

These exclude the engine's own checkpoints (attempt counters, breadcrumbs) and the provider's
document envelope, so a real run pays somewhat more. To see what *your* workload pays, read the
`asyncresponse.flow_state.checkpoint.size` histogram ([observability](observability.md#instruments)):
every store records the size of each ledger it serializes for a write, so its sum is the cumulative
cost and its largest bucket the ledger size to budget against.

`IDurableFlows.StartAsync` also checks the initial ledger against the store before publishing (see
[initial-state preflight](durable-flow-state-stores.md#initial-state-preflight)), so an oversized
input fails the start with nothing persisted.

### Supported ledger budgets

The full-ledger checkpoint is deliberate: one document, one revision check, one lease fence,
readable on any store and byte-identical across providers. These are the budgets the library is
built and tested for — outside them the persistence cost arrives well before the size cap does:

| Budget | Supported | What happens past it |
|---|---|---|
| Ledger size | ≤ `LedgerSizeWarningBytes` (512 KiB by default; 256 KiB on DynamoDB, whose cap is 350 KB) | The warning fires at the threshold and each doubling. A checkpoint over `MaxStateBytes` is refused with `FlowStateTooLargeException`: the attempt fails and is retried under the transport's redelivery bound, then dead-lettered where the transport has a dead-letter destination (the run stays `Running`, and a step body whose checkpoint was refused runs again on each redelivery) — the dead-letter queue is the alarm. RabbitMQ classic queues requeue without limit by default and the in-memory transport drops the job after `MaxDeliveryAttempts`, so bound the attempts and configure a dead-letter destination wherever this alarm matters. |
| Retained steps per run | 256 by default (`MaxRetainedSteps`) | A new step fails before side effects. Raising the budget grows the cumulative cost with the square of the step count (~36 MB serialized for 256 steps of 1 KiB), so measure `asyncresponse.flow_state.checkpoint.size` first. |
| Size of one step result | a few KiB | One large result is paid again on every later checkpoint of the run. |
| Flow input | Must fit both the worker-envelope budget and the store's `MaxStateBytes`, including provider document overhead | Built-in stores reject oversized initial state with `FlowStateTooLargeException` before publication; oversized envelopes throw `WorkerJobTooLargeException`. Nothing is persisted. |

Two patterns keep a long-running or data-heavy flow inside them. **Store large results by
reference**: the step persists its payload where it belongs (blob storage, a table, a cache) and
returns only the key, so the ledger retains a few dozen bytes per step (the claim-check seam on
the [roadmap](roadmap.md) will do this transparently):

```csharp
// The step's checkpoint holds the key, not the report.
var reportKey = await flow.StepAsync("render-report", async () =>
{
    var report = await renderer.RenderAsync(input, ct);
    var key = $"reports/{flow.FlowId}/{Guid.NewGuid():N}";
    await blobs.UploadAsync(key, report, ct);
    return key;
});

// A later step re-reads it by key; a replay after a restart re-reads the same key.
await flow.StepAsync("publish", () => publisher.PublishAsync(blobs.OpenRead(reportKey), ct));
```

**Partition a long history into child flows**: a parent that fans out or loops for hundreds of
steps starts a [child flow](#child-flows) per batch and memoizes only each child's compact
snapshot, so every ledger stays bounded. This bounds the workload, not the format: checkpoint cost
inside each ledger is still quadratic. Incremental (append-only) checkpoint persistence is on the
[roadmap](roadmap.md). The curve is measured, not inferred: `LedgerGrowthBenchmarks` in
`benchmarks/AsyncResponse.Benchmarks` runs raw store writes for N = 50, 200, 400 results of 1 KiB,
in one ledger and in bounded 8-step ledgers (bypassing the step budget to expose the unbounded
baseline), and `LedgerBudgetTests` verifies the engine's 256-step boundary, replay at the limit,
and approximately linear total checkpoint bytes for a bounded child tree.

## Under the hood

You don't need any of this to use flows — it's here for the curious.

The API encodes the *checkpointed-flow pattern*, extracted from years of production use:

- Each run persists a **ledger** (`FlowState`): a checkpoint per step name plus the run's
  status — human-readable JSON you can query and, in an emergency, hand-edit.
- `AwaitStepAsync` creates the response subscription **first**, then persists the correlation-id
  **breadcrumb**, then runs your trigger. That ordering is the whole trick: "breadcrumb exists"
  implies "someone is listening", so a crash on either side of the send re-attaches or safely
  restarts — never a lost run, never a double-send. A failure raised *after* the send but outside
  the wait (a throwing `IDurableFlowExecutionObserver.OnStepWaitingAsync`, a logger) fails the
  attempt but keeps the breadcrumb, so the redelivery re-attaches (or faults at the persisted
  deadline) instead of sending the request again; only a genuine step failure restarts a step
  fresh.
- Every awaited step auto-registers the executor's payload-recovery and failure methods as
  lost-subscriber callbacks — the same machinery as [recovery.md](recovery.md), with its
  at-least-once, idempotency-required contract. The failure callback is correlation-scoped:
  `IDurableFlowExecutor.FailAsync(flowId, exception, correlationId)` fails the run only while a
  step is still pending on that correlation id, so a late error for a superseded id is ignored;
  the two-argument `FailAsync(flowId, exception)` is the unscoped operator form. A step that
  **faulted** on the id (it timed out, or its wait failed) is no longer pending on it — the
  breadcrumb stays in the ledger for diagnosis, but the step restarts fresh under a new id — so
  neither a late failure nor a late response for it is applied: the run is not failed, and the
  payload is not checkpointed into a step whose fault flow code may already have handled (a late
  response still wakes a `Running` run, as any stale one does). The in-memory
  channel registers them too (covering waiter loss within one process and the simulated restarts
  of [AsyncResponse.Testing](testing.md)); durable channels extend the contract across real
  restarts.
- Starting a flow **publishes first**: the start job carries the initial ledger (flow and input
  type names, input JSON, captured ambient context), and `IDurableFlowExecutor.CreateAndExecuteAsync`
  creates the ledger if the starter's own write never happened — the publish is the start's single
  commit point. Resumes, redeliveries, and operator kicks re-enqueue the lighter
  `ExecuteAsync(flowId)` job. The input therefore travels twice (job and ledger), so mind the
  transport's payload ceiling for large inputs (Azure Service Bus standard tier: 256 KB; SQS: 1 MiB
  or the queue's lower `MaximumMessageSize`; NATS: the server's `max_payload`, 1 MiB by default).
  `StartAsync` with a caller-supplied `flowId` is idempotent for the same flow type and
  semantically identical input; a conflicting reuse throws `DurableFlowIdConflictException` at the
  starter, and the executor drops an already-published job on the same test, so an existing run is
  never replaced. A **generated** id cannot survive a retried ambiguous publish (the retry mints a
  second run), so supply deterministic ids wherever the caller may retry.
- Built-in stores persist a monotonic `FlowState.Revision`. Every execution owns a renewable lease
  and every checkpoint requires both the expected revision and that lease, so a stale worker cannot
  overwrite state written by a newer execution. One deliberate exception: a response won in the
  instant the lease lapses is still checkpointed through the lease-less compare-and-swap recovery
  uses, because the channel has already acked that payload and it exists nowhere else; the
  execution then stops as lease-lost and the redelivery replays from that checkpoint. That write
  applies only while the step is still pending on the **same** correlation id (and has not faulted
  on it) and the run is `Running` or `Suspended`; if a takeover already timed the step out,
  re-triggered it under a new id or failed the run, the stale response is discarded with a
  warning. Flow code sees a lost lease as `DurableFlowLeaseLostException` (see
  [Compensation](#compensation)).

## Honest comparison with a dedicated workflow engine

| Concern | AsyncResponse durable flows | Workflow engine (Temporal, Durable Task) |
|---|---|---|
| Flow definition | Plain C# in your service; steps edited like any code | Workflow code under replay rules: deterministic-only, versioned patches for changes |
| Position after a crash | A human-readable per-run state you can query and hand-edit | Event-sourced history, reconstructed by replay |
| Redeploy mid-step | Re-attach to the in-flight wait; late responses classified by domain outcome | Replay reconstructs position |
| Progress from remote steps | First-class (`until` sees every message; `ReportProgressAsync`) | Signals/queries — more ceremony |
| Hotfixing in-flight runs | Resume into current code; no determinism constraints | Version/patch workflows so old histories still replay |
| Compensation | Explicit: you write compensating steps; the state tells you what completed | Saga frameworks track and run compensations automatically |
| Durable timers, cron | First-class: `flow.DelayAsync` sleeps (a suspended run holds no worker or memory) and replica-safe `WithScheduledFlow` cron — see [timers-and-scheduling.md](timers-and-scheduling.md) | First-class durable timers |
| Human tasks | Model as an awaited step (the approval UI publishes the response); no dedicated task-queue/assignment primitives | First-class human-task activities in some engines |
| Time-skipping tests | First-class: `AsyncResponse.Testing` virtual clock, scripted replies, crash injection — see [testing.md](testing.md) | Temporal's time-skipping test server; varies by engine |
| Extra infrastructure | None new — the flow ledger lives in a database/store you already run | An engine cluster/service to operate and upgrade |

Reach for an engine when you need engine-*owned* semantics: automatically derived compensation
graphs, human-task assignment and escalation, or replayable audit histories. For
request/response orchestration — even at dozens of steps, sleeps included — durable flows are
smaller, transparent, and hotfix-friendly.
