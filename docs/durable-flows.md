# Durable multi-step flows — the checkpointed-flow pattern

AsyncResponse's headline use is awaiting one response. Its most powerful use is composing
**dozens** of them: a multi-step flow across remote services, written as plain sequential
`await`s, that survives redeploys mid-step and resumes exactly where it left off. This page
documents the pattern the library was extracted from — it has run production flows of a dozen+
steps (SQL jobs, Airflow DAGs, ETL runners, ticketing systems) for years.

The pattern needs three ingredients, all of which you already have:

1. **A run ledger** — one small persisted JSON object per flow run: a `bool` per completed step,
   plus the correlation id of the step currently in flight. Your database, your row. This is the
   flow's entire durable state.
2. **The flow as a worker job** — the flow method runs via `EnqueueWorkerAsync`, so starting and
   *re-starting* it is a durable, at-least-once operation on your transport.
3. **One awaited-step helper** — encapsulates the fresh-start / re-attach decision per step.

```
RunAsync(runId)                          ← entry point AND resume target; always safe to re-run
 ├─ if (!ledger.StepA) { do; mark; }     ← sync step: guard → do → persist
 ├─ if (!ledger.StepB) await Step(...)   ← awaited step: trigger remote, await terminal response
 ├─ if (!ledger.StepC) await Step(...)   ← insert/reorder/delete steps = ordinary code edit
 └─ ...                                  ← finish: mark the run succeeded
```

Because every step is guarded by its ledger flag, **re-running the whole flow is always safe** —
that single property makes redelivery, crash recovery, and manual resume all collapse into the
same operation: *run it again*.

## The ledger

```csharp
public sealed class ProvisioningLedger
{
    public bool WorkspaceCreated { get; set; }
    public bool MigrationRan { get; set; }
    public bool DataImported { get; set; }
    public bool NotificationsSent { get; set; }

    // The awaited step currently in flight, if any — the breadcrumb a restarted
    // process uses to re-attach instead of re-triggering the remote operation.
    public string? PendingCorrelationId { get; set; }
    public bool PendingStepFailed { get; set; }
    public string? LastMessage { get; set; }
}
```

Flags, not a step counter: inserting a step later is adding a property, and old in-flight runs
(which don't have the new flag set) simply execute the new step when resumed — no migration.

## The flow

```csharp
public sealed class TenantProvisioningFlow(
    IRecoverableAsyncResponseBuilder _asyncResponse,
    IProvisioningLedgerStore _ledgers,       // your persistence — one row per runId
    IWorkspaceService _workspaces,           // a plain synchronous dependency
    IMigrationService _migrations,           // remote systems that reply via broker/webhook
    IImportService _imports,
    INotifier _notifier) : ITenantProvisioningFlow
{
    private static readonly TimeSpan StepTimeout = TimeSpan.FromHours(2);

    /// <summary>Entry point and resume target. Idempotent: safe to run any number of times.</summary>
    public async Task RunAsync(Guid runId)
    {
        var ledger = await _ledgers.LoadAsync(runId);

        // --- Sync step: guard → do → persist ------------------------------------------
        if (!ledger.WorkspaceCreated)
        {
            await _workspaces.CreateAsync(runId);
            ledger.WorkspaceCreated = true;
            await _ledgers.SaveAsync(runId, ledger);
        }

        // --- Awaited step: trigger a remote job, await its terminal response ----------
        if (!ledger.MigrationRan)
        {
            var result = await AwaitedStepAsync<MigrationResult>(
                runId, ledger,
                trigger: cid => _migrations.StartAsync(runId, cid),
                until: r => Task.FromResult(r.Status != MigrationStatus.Running));

            if (result.Status == MigrationStatus.Failed)
                throw new ApplicationException($"Migration failed: {result.Message}");

            ledger.MigrationRan = true;
            await _ledgers.SaveAsync(runId, ledger);
        }

        // --- Awaited step with progress streaming -------------------------------------
        if (!ledger.DataImported)
        {
            var result = await AwaitedStepAsync<ImportResult>(
                runId, ledger,
                trigger: cid => _imports.StartAsync(runId, cid),
                until: async r =>
                {
                    if (r.State == ImportState.InProgress)
                    {
                        ledger.LastMessage = r.Message;          // progress → ledger → your UI
                        await _ledgers.SaveAsync(runId, ledger);
                        return false;                            // keep waiting
                    }
                    return true;                                 // terminal — complete the wait
                });

            if (result.State == ImportState.Failed)
                throw new ApplicationException($"Import failed: {result.Message}");

            ledger.DataImported = true;
            await _ledgers.SaveAsync(runId, ledger);
        }

        if (!ledger.NotificationsSent)
        {
            await _notifier.SendProvisionedAsync(runId);
            ledger.NotificationsSent = true;
            await _ledgers.SaveAsync(runId, ledger);
        }

        await _ledgers.MarkSucceededAsync(runId);
    }

    /// <summary>Lost-subscriber resume target: just run the flow again — the guards skip
    /// everything already done, and the pending step re-attaches.</summary>
    public Task ResumeAsync(Guid runId) =>
        _asyncResponse.EnqueueWorkerAsync<ITenantProvisioningFlow>(flow => flow.RunAsync(runId));

    /// <summary>Lost-subscriber failure target: the classified dead end for this run.</summary>
    public async Task FailAsync(Guid runId, Exception exception)
    {
        var ledger = await _ledgers.LoadAsync(runId);
        ledger.PendingStepFailed = true;
        ledger.LastMessage = exception is AsyncResponseDomainFailureException domain
            ? $"Remote step failed (payload: {domain.PayloadJson})"
            : exception.Message;
        await _ledgers.SaveAsync(runId, ledger);
        await _ledgers.MarkFailedAsync(runId, ledger.LastMessage);
    }

    // ----------------------------------------------------------------------------------
    // One helper owns the fresh-start / re-attach split for every awaited step.
    private async Task<T> AwaitedStepAsync<T>(
        Guid runId,
        ProvisioningLedger ledger,
        Func<string, Task> trigger,
        Func<T, Task<bool>> until) where T : IAsyncResponsePayload
    {
        // Re-attach when a previous process already triggered this step and died waiting;
        // start fresh when there is no breadcrumb, or the last attempt ended in failure.
        var reattach = ledger.PendingCorrelationId is not null && !ledger.PendingStepFailed;

        T response;
        try
        {
            response = reattach
                // The step is already running remotely — subscribe to its correlation id.
                // The attached builder takes no trigger BY TYPE: a double-send is
                // unrepresentable, which is exactly what makes resume safe.
                ? await _asyncResponse
                    .For<T>(ledger.PendingCorrelationId!)
                    .WithTimeout(StepTimeout)
                    .Until(until)
                    .OnLostSubscriberResume<ITenantProvisioningFlow>(f => f.ResumeAsync(runId))
                    .OnLostSubscriberFailure<ITenantProvisioningFlow>(f => f.FailAsync(runId, Placeholder.Exception()))
                    .WaitAsync()
                : await _asyncResponse
                    .For<T>()
                    .WithTimeout(StepTimeout)
                    .Until(until)
                    .OnLostSubscriberResume<ITenantProvisioningFlow>(f => f.ResumeAsync(runId))
                    .OnLostSubscriberFailure<ITenantProvisioningFlow>(f => f.FailAsync(runId, Placeholder.Exception()))
                    .WaitAsync(async context =>
                    {
                        // Persist the breadcrumb BEFORE sending. This trigger runs only once
                        // the subscription and recovery state exist, so: a crash after this
                        // line re-attaches instead of re-triggering, a fast first response
                        // can never beat the registration, and a failed send tears the
                        // registration down without leaving a breadcrumb behind.
                        ledger.PendingCorrelationId = context.CorrelationId;
                        ledger.PendingStepFailed = false;
                        await _ledgers.SaveAsync(runId, ledger);

                        await trigger(context.CorrelationId);
                    });
        }
        catch
        {
            // Timeout or fault: the next run restarts this step fresh (steps are idempotent).
            ledger.PendingStepFailed = true;
            await _ledgers.SaveAsync(runId, ledger);
            throw;
        }

        // Terminal response consumed — clear the breadcrumb before the caller marks the flag.
        ledger.PendingCorrelationId = null;
        await _ledgers.SaveAsync(runId, ledger);
        return response;
    }
}
```

Payload types decide their own recovery route — a failed result must never resume the happy
path, and with the override below it can't:

```csharp
public sealed class MigrationResult : IAsyncResponsePayload
{
    public MigrationStatus Status { get; set; }
    public string? Message { get; set; }

    public bool ShouldResumeOnRecovery() => Status != MigrationStatus.Failed;
}
```

## Why the ordering rules matter

- **Breadcrumb inside the trigger, before the send.** The trigger only runs once the
  subscription and recovery state exist, so persisting `PendingCorrelationId` there gives you a
  precise guarantee: *breadcrumb exists ⇒ registration existed*. A crash between persist and
  send re-attaches, times out, and restarts the step — never a lost run, never a double-send.
- **Clear the breadcrumb only after the terminal response.** A progress message keeps both the
  wait and the breadcrumb alive; a crash mid-progress re-attaches and keeps streaming.
- **Mark the step flag after the breadcrumb is cleared.** The flag is the caller's checkpoint;
  the breadcrumb is the helper's. Two separate writes keep each step's state machine trivial.

## What each failure mode does

| What dies | What happens |
|---|---|
| Process crashes **before** a step triggers | Redelivery/resume re-runs the flow; guards skip done steps; the step triggers normally |
| Process crashes **while awaiting** | Re-run re-attaches via `PendingCorrelationId`; progress keeps streaming; terminal completes the step |
| Process is **down when a progress/success response arrives** | `ShouldResumeOnRecovery() == true` → the durable **resume callback** re-enqueues `RunAsync`; the flow re-attaches and continues |
| Process is down when a **failed** response arrives | `ShouldResumeOnRecovery() == false` → the **failure callback** marks the run failed — the happy path is never resumed on a failure |
| The **terminal** response itself was the lost message | Recovery consumed it, so re-attach has nothing to receive: the step **times out and restarts fresh** — which is why steps must be idempotent. (Optional refinement: have the resume callback accept `Placeholder.Payload<T>()` and stash it on the ledger; the helper checks the stash before re-attaching. Most flows don't need it — timeout-and-restart is simpler and correct.) |
| A step keeps failing | The exception propagates out of `RunAsync`; your worker transport redelivers with bounded attempts, then dead-letters the run for operator attention |

## Editing the flow

This is the point of the pattern:

- **Insert a step**: add a flag and a guarded block. In-flight runs execute it on resume.
- **Reorder steps**: move the blocks. The ledger doesn't encode order.
- **Run a subset**: pre-mark the flags you want skipped when creating the run (e.g. a
  "retry only the import" operator action seeds every other flag as done).
- **Hotfix an in-flight run**: deploy the fix and resume — runs continue into the *current*
  code. There is no replay history to keep deterministic and no workflow-version patching.

## Compensation

Compensation here is explicit, and the ledger is what makes it tractable: at failure time it
tells you exactly which steps completed, so `FailAsync` (or an operator action) can run
compensating steps for precisely those — guarded by their own flags, awaited through the same
helper if they're remote. What you do *not* get is an engine that derives the compensation
sequence automatically; you write it, next to the steps it undoes.

## Honest comparison with a dedicated workflow engine

| Concern | AsyncResponse (checkpointed flow) | Workflow engine (Temporal, Durable Task) |
|---|---|---|
| Flow definition | Plain C# in your service; steps edited like any code | Workflow code under replay rules: deterministic-only, versioned patches for changes |
| Position after a crash | Your ledger — one human-readable row you can query and hand-edit | Event-sourced history, reconstructed by replay |
| Redeploy mid-step | Re-attach to the in-flight wait; late responses classified by domain outcome | Replay reconstructs position |
| Progress from remote steps | First-class (`Until` sees every message) | Signals/queries — more ceremony |
| Hotfixing in-flight runs | Resume into current code; no determinism constraints | Version/patch workflows so old histories still replay |
| Compensation | Explicit: you write compensating steps; ledger tells you what completed | Saga frameworks track and run compensations automatically |
| Durable timers (days-weeks), cron, human tasks | Per-wait timeouts up to `RecoveryStateExpiry`; longer cadence belongs to your scheduler | First-class durable timers |
| Extra infrastructure | None beyond the channel/transport you already run | An engine cluster/service to operate and upgrade |
| State kept per run | One ledger row (yours) | Full event history (engine-managed) |

Reach for an engine when you need engine-*owned* semantics: automatic compensation graphs,
months-long durable timers, human-approval tasks, or replayable audit histories of every
decision. For request/response orchestration — even at dozens of steps — the checkpointed flow
is smaller, transparent, and hotfix-friendly.

## Checklist

- [ ] Every step is idempotent or guarded by its ledger flag (restart-safe).
- [ ] Awaited steps persist `PendingCorrelationId` *inside the trigger*, before the send.
- [ ] The breadcrumb is cleared on terminal response; set `PendingStepFailed` on step failure.
- [ ] Every awaited step registers **both** `OnLostSubscriberResume` and `OnLostSubscriberFailure`.
- [ ] The resume callback re-enqueues the flow entry point — nothing else.
- [ ] Payload types override `ShouldResumeOnRecovery()` so failures route to the failure callback.
- [ ] Step timeouts are shorter than `RecoveryStateExpiry`, and the flow's worker queue has
      bounded redelivery + a dead-letter queue (that's your "run is stuck" alarm).
- [ ] Callback method names are stable across deploys (they're persisted by name — see the
      naming contract in [recovery.md](recovery.md)).
