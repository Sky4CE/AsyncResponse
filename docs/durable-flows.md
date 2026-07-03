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

`IDurableFlows`, `IDurableFlowContext`, and `IFlowStateStore` are registered by
`AddAsyncResponse()` — nothing extra to configure. Flow state is persisted through the
configured channel's recovery store, so durability follows your channel: durable with
Redis/NATS/PostgreSQL/SQL Server, process-local with the in-memory channel.

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

## Observing runs

- `IDurableFlows.GetStateAsync(flowId)` → the full `FlowState` snapshot: status, per-step
  checkpoints, `LastMessage` progress, attempts, the value bag.
- `flow.ReportProgressAsync(...)` and `flow.SetValueAsync(key, value)` persist operator-facing
  progress and arbitrary values on the state.
- Executions emit an `asyncresponse.flow.execute` activity tagged with the flow id and type.

## Storage: where flow state lives

By default, flow state rides in the channel's `IRecoveryStateStore` (one entry per run under a
sentinel marker; the watchdog knows to skip them). This means zero new infrastructure and
durability identical to your recovery state, with `AsyncResponseOptions.DurableFlows.StateExpiry`
(default 7 days) as the idle TTL — it refreshes on every checkpoint, so it bounds the gap
*between* checkpoints, not total run duration.

To keep flow state in your own storage instead (e.g. a table next to the domain entities the
flow operates on, where your dashboards already look), register your own store — the library
calls exactly three members:

```csharp
public interface IFlowStateStore
{
    Task SaveAsync(string flowId, FlowState state, TimeSpan ttl, CancellationToken ct = default);
    Task<FlowState?> LoadAsync(string flowId, CancellationToken ct = default);
    Task<bool> TryDeleteAsync(string flowId, CancellationToken ct = default);
}

builder.Services.AddSingleton<IFlowStateStore, MyDatabaseFlowStateStore>();
```

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
