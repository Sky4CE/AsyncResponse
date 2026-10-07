# Testing AsyncResponse applications

`AsyncResponse.Testing` runs the **complete engine** in process on a **virtual clock**: the
in-memory channel (with full lost-subscriber recovery), the in-memory worker transport (with
native delayed delivery), the in-memory flow store, and every background service. Tests script
the remote side, skip time instead of sleeping, inject crashes at exact checkpoints, and simulate
process restarts — against production-sized timeouts, leases, timers, and schedules.

```bash
dotnet add package AsyncResponse.Testing   # test projects only
```

**On this page**

- [The virtual clock](#the-virtual-clock)
- [FlowTestHarness — testing durable flows](#flowtestharness--testing-durable-flows)
- [Crash injection at checkpoints](#crash-injection-at-checkpoints)
- [Timers, schedules, and retries on virtual time](#timers-schedules-and-retries-on-virtual-time)
- [AsyncResponseTestHarness — testing direct awaits](#asyncresponsetestharness--testing-direct-awaits)
- [Simulated restarts and lost-subscriber recovery](#simulated-restarts-and-lost-subscriber-recovery)
- [Sizing and guard rails](#sizing-and-guard-rails)

## The virtual clock

`VirtualTimeProvider` is a deterministic `TimeProvider` the whole engine runs on (the engine
resolves its clock from DI; the harness registers the virtual one). Time starts at a fixed epoch
(2030-01-01Z) and moves only when the test advances it. Advancing walks armed timers **in due
order**, firing each at its own instant — so a lease renewal that precedes a lease expiry on the
timeline also precedes it under a big jump, and interleavings match real time.

Everything time-driven runs on it: waiter timeouts, execution leases, watchdog scans, in-process
retry backoff, durable timers, delayed jobs, cron schedules. A five-minute production timeout
elapses without anyone waiting five minutes, and no test code sleeps for real. Advancing is not
free, though: at every armed timer it walks, the harness spends a few real milliseconds letting
the worker pipeline settle, so the cost of an advance grows with the number of timers it crosses
(see [Sizing and guard rails](#sizing-and-guard-rails)).

## FlowTestHarness — testing durable flows

The flow class under test needs **zero instrumentation** — no probes, no captured correlation
ids, no crash hooks. The harness observes execution through the engine's
`IDurableFlowExecutionObserver` seam and answers awaited steps the way the remote systems would:

```csharp
await using var harness = await FlowTestHarness.StartAsync(options =>
{
    options.ConfigureServices = services =>
    {
        services.AddSingleton<IProvisioningClient>(fakeClient);   // your flow's dependencies
    };
    options.ConfigureAsyncResponse = builder =>
        builder.WithDurableFlow<TenantOnboardingFlow, OnboardingInput>();
});

var run = await harness.StartFlowAsync<TenantOnboardingFlow, OnboardingInput>(new(TenantId: 7));

// The flow triggered its migration and is durably parked — reply as the remote system.
await run.WaitForAwaitingStepAsync("run-migration");
await run.ReplyAsync(new OperationResult { Status = OperationStatus.Completed });

// Progress-aware steps take several replies; non-terminal payloads keep the wait open.
await run.WaitForAwaitingStepAsync("import-data");
await run.ReplyAsync(new OperationResult { Status = OperationStatus.Running, Message = "60%" });
await run.ReplyAsync(new OperationResult { Status = OperationStatus.Completed });

// A six-hour settle timer parks the run; skip it.
await run.WaitForTimerStepAsync("settle");
await harness.AdvanceAsync(TimeSpan.FromHours(6));

Assert.Equal(FlowRunStatus.Succeeded, await run.WaitForFinishedAsync());
Assert.Equal(1, run.StepExecutions("create-workspace"));   // exactly-once, from the probe
```

The handle also exposes `GetStateAsync()` (the persisted ledger), `Events` (the recorded step
timeline), `ReplyExceptionAsync` (a remote failure — the step faults and restarts fresh on
redelivery, new correlation id, idempotent-trigger contract), `ExecuteDirectAsync()` (drive the
executor inline, no worker queue — fully single-threaded for exhaustive matrices), and
`ResumeAsync()` (the operator wake-up).

`ReplyExceptionAsync` faults the step with the **wire** failure shape every durable channel
produces: a plain `Exception` carrying the original message, with the capped stack trace in
`Data["RemoteStackTrace"]` — the concrete exception type never crosses the wire, so a
`catch (MyDomainException)` in a flow can never match in production and will not match in the
harness either.

## Crash injection at checkpoints

`CrashBeforeStep` / `CrashAfterStep` arm a one-shot `SimulatedCrashException` thrown from the
observer seam at the exact boundary — before a step's first side effect, or right after its
checkpoint persisted. The execution attempt fails exactly like a process death at that point, the
transport redelivers (backoff on the virtual clock), and the run resumes from the last
checkpoint. One crash can be armed at a time — arming another while one is pending throws, so a
test can never believe it exercised a crash path that never ran:

```csharp
harness.CrashAfterStep("create-workspace");   // die between the checkpoint and the next step
var run = await harness.StartFlowAsync<TenantOnboardingFlow, OnboardingInput>(input);
await harness.AdvanceAsync(TimeSpan.FromSeconds(2));      // let the redelivery backoff elapse
// … script replies … then:
Assert.Equal(FlowRunStatus.Succeeded, await run.WaitForFinishedAsync());
Assert.Equal(1, run.StepExecutions("create-workspace")); // a crash costs a delivery, never a duplicate side effect
```

When more than one run can reach the armed step — concurrent flows, or a parent and a child flow
reusing step names — pin the crash to one run by the id you start it with
(`harness.CrashAfterStep("work", flowId: "run-b")`, then `StartFlowAsync<TFlow, TInput>(input, flowId: "run-b")`);
an unscoped crash fires on whichever run gets there first.

On an awaited step, `CrashAfterStep` stops the attempt right after the response is checkpointed:
assert that the next step has not run before the retry, that the step completed once, and that the
resumed run's `Attempts` increased.

Run it as a `[Theory]` over every step of your flow, before and after each — the
crash-at-every-checkpoint matrix in the library's own
[FlowTestHarnessShowcaseTests](../tests/AsyncResponse.Tests/FlowTestHarnessShowcaseTests.cs).

## Timers, schedules, and retries on virtual time

`harness.AdvanceAsync(delta)` advances stepwise and lets the worker pipeline settle between
steps, so chained work is honored inside one call: a durable timer wakes, the flow re-suspends
for the next chunk, a retry backoff elapses and redelivers, a cron loop fires and re-arms.
Before each step it waits for any job still running code to reach its next wait, so the clock
never moves under a step body: calling `AdvanceAsync` straight after `StartFlowAsync`, or across a
cron occurrence whose run sleeps, anchors every timer on the instant the flow actually reached it.
The engine's own hang guards (the execution lease's renewal and deadline timers, its bounded
disposal joins) are not waits of the job and never end that wait early; they still fire, in
order, as the clock moves.

```csharp
var run = await harness.StartFlowAsync<ReminderFlow, ReminderInput>(new("acme"));
var wakeAt = await run.WaitForTimerStepAsync("cool-down");   // flow sleeps 3 days, holds nothing
await harness.AdvanceAsync(TimeSpan.FromDays(3));            // …skipped
Assert.Equal(FlowRunStatus.Succeeded, await run.WaitForFinishedAsync());
```

Cron schedules fire with their deterministic run ids
(`harness.Attach("sched:nightly-report:20300101T060000Z")` observes one), and an outage is one
line: `harness.Engine.SimulateRestartAsync(whileDown: () => harness.Clock.Advance(TimeSpan.FromHours(4)))`
— occurrences that fell into the downtime are skipped, exactly as in production. See
[ScheduledFlowTests](../tests/AsyncResponse.Tests/ScheduledFlowTests.cs) and
[DurableFlowTimerTests](../tests/AsyncResponse.Tests/DurableFlowTimerTests.cs).

## AsyncResponseTestHarness — testing direct awaits

For code that uses the fluent builder without flows, `AsyncResponseTestHarness` (which
`FlowTestHarness` wraps — it's `harness.Engine`) hosts the same engine:

```csharp
await using var harness = await AsyncResponseTestHarness.StartAsync();

string correlationId = null!;
var wait = harness.Builder
    .For<OperationResult>()
    .WithTimeout(TimeSpan.FromMinutes(5))                  // the PRODUCTION value
    .Until(r => r.Status != OperationStatus.Running)
    .WaitAsync(ctx => { correlationId = ctx.CorrelationId; return Task.CompletedTask; });

await harness.PublishAsync(new OperationResult { Status = OperationStatus.Running }, correlationId);
await harness.AdvanceAsync(TimeSpan.FromMinutes(5));       // nothing terminal arrived…
await Assert.ThrowsAsync<TimeoutException>(() => wait);    // …so the production timeout fires
```

`PublishAsync` / `PublishExceptionAsync` play the remote side; `Builder` is the recoverable
builder — the in-memory channel implements the full `IRecoverableAsyncResponseSubscriber`
contract, including the `OnRecovery()`-override guard, so what passes here passes on Redis.

## Simulated restarts and lost-subscriber recovery

`SimulateRestartAsync()` (on `AsyncResponseTestHarness`; `harness.Engine` from a `FlowTestHarness`)
models a redeploy: the service provider — and with it every live waiter, subscription, and
in-flight execution — is discarded and rebuilt, while the durable state a real deployment would
retain survives: recovery registrations, flow ledgers, and scheduled (delayed) worker jobs,
re-published with their remaining virtual delay like broker-held scheduled messages (all of them,
even past `DelayedJobCapacity` — a broker keeps every scheduled message). The optional `whileDown`
callback runs while no engine is up; advance `harness.Clock` there to model an outage.

```csharp
var subscriber = harness.Services.GetRequiredService<IRecoverableAsyncResponseSubscriber>();
_ = await subscriber.CreateRecoverableResponseWaiter<OperationResult>(
    correlationId, resumeCallback: resume);                // …and the process "dies"

await harness.SimulateRestartAsync();

await harness.PublishAsync(new OperationResult { Status = OperationStatus.Completed }, correlationId);
// No live waiter → the persisted registration routes the payload through OnRecovery() →
// the resume callback runs against the NEW incarnation's services.
```

This is how to test the recovery tri-state (`Resume` / `Fail` / `KeepWaiting`, returned by
`OnRecovery()`) without a broker. The dead incarnation's waiters are abandoned, not disposed: their
`ResponseTask` is cancelled for anyone still holding it, and disposing one afterwards (a flow
disposes its waiter after the cancelled wait; an `await using` caller does the same) is a no-op
that leaves the recovery registration in place — exactly what a crashed process leaves behind — so
the late response routes through `OnRecovery()`. Assert through the recovery side effects, not the
dead incarnation's task.

**The restart is cooperative.** It discards everything a crash would lose and breaks the dead
incarnation's execution leases, but there is no process to kill. The graceful stop (bounded by
`options.RealTimeGuard`) ends as soon as only engine-owned waits remain — an awaited step, an
in-process timer, a crashed attempt asleep in a redelivery backoff taken during the drain, a
duplicate wake-up polling for another execution's lease — because only the test can end those, and
it cannot while it awaits the restart. Jobs still queued behind them never started and run no user
code. A backoff taken *before* the stop is ended by it and its job dropped: the crashed attempt
dies with the old incarnation whatever `MaxDeliveryAttempts` says (a production host stop would
retry it during the drain; the restart keeps crash semantics).

User code still executing after the stop lapsed — a step body that ignored its cancellation and is
blocked on something the test controls — would keep running beside the new incarnation and perform
its side effects after the restart returned, so `SimulateRestartAsync` refuses with
`InvalidOperationException`. Let the step observe its cancellation token or finish before
restarting; for crash-*at-a-checkpoint* semantics use `CrashBeforeStep` / `CrashAfterStep`, which
fail the attempt at the exact boundary with nothing left running. A test that deliberately wants
the overlap sets `options.AbandonLingeringExecutionsOnRestart = true` and owns it: the abandoned
execution's side effects land whenever it unblocks. Nothing in the harness is a subprocess kill; a
guarantee that must hold against abrupt termination needs a real process and a real broker (see
[the abrupt-crash suite](#the-abrupt-crash-suite-how-the-library-tests-what-the-harness-cannot)).

**Jobs that never started carry over; interrupted executions do not.** A job still queued at the
restart — a queued flow *start* included — runs in the new incarnation, as a broker would deliver a
message it still holds, under the ambient context (`AsyncLocal` state) it was published under;
scheduled jobs keep their publisher's ambient context too. Once the old incarnation has stopped,
none of its workers starts another job. Breaking a lease does not redeliver the execution that held
it: the wake-up of every execution the stop abandoned — parked on an awaited step or an in-process
timer, or a crashed attempt asleep in its backoff (after `CrashAfterStep`, say, when the restart
comes before the clock has moved) — dies with the old incarnation. Resume those runs explicitly
after the restart (`run.ResumeAsync()`, `IDurableFlows.ResumeAsync`), or, for an awaited step,
publish its response — lost-subscriber recovery routes it into the run. With `WorkerCount` ≥ 2, a
duplicate delivery that was polling for another execution's lease keeps polling on the shared
virtual clock: if the test advances the clock before the new incarnation retakes the flow, that
poll can take the lease once and write one extra attempt (and its failure message) to the ledger —
resume or reply before advancing when a test asserts `Attempts` or `LastMessage`.

**Step barriers after a restart.** Parks the dead incarnation held in process no longer count:
`WaitForAwaitingStepAsync` — and `WaitForTimerStepAsync` for a timer at or below
`TimerInProcessThreshold` — wait for the *new* incarnation to park the step (resume the run
first), since a re-executed step may have replaced the old correlation id. What is durable stays
visible: `WaitForStepCompletedAsync` returns for a checkpoint persisted before the restart (by the
old incarnation's graceful drain, too), and `WaitForTimerStepAsync` for a suspended timer, whose
wake-up the restart carried over. `ReplyAsync` answers the new incarnation's wait once the run has
recorded anything in it; until then it answers the wait that survived the restart — a response
arriving while the process is down. A wait a failed attempt released (an awaited step that timed
out, or received a failure the flow does not treat as terminal) is not live either: `ReplyAsync`
and `WaitForAwaitingStepAsync` wait for the retry to park the step again under a fresh correlation
id, so advance the clock past the redelivery backoff. `Events` and `StepExecutions` keep the whole
history across restarts.

Because the harness breaks the dead incarnation's leases *for* the new one, it cannot reach one
window a real crash opens: the dead process's lease stays persisted and unexpired, and the run's
redelivered wake-up must get past it alone — where a wake-up could be acknowledged as a "duplicate"
of an execution that no longer exists. The library's own suite for that window follows.

### The abrupt-crash suite (how the library tests what the harness cannot)

`DurableFlowAbruptCrashRecoveryTests` (integration batch `data`) is the one suite that kills a real
process. It launches `tests/AsyncResponse.IntegrationTests.CrashWorker` as a subprocess on the
PostgreSQL worker transport and PostgreSQL flow-state store (one container, a private schema per
scenario). The worker SIGKILLs itself at an armed crash point — after lease acquisition, after a
step checkpoint, after a publish but before its checkpoint, or after a child checkpoint but before
the child is published. No `finally` runs, the lease is not released, and the queue claim is not
settled. A successor process then starts against the unchanged database; one scenario gives it a
much shorter `ExecutionLeaseDuration` than the dead owner's (see
[lease contention](durable-flow-state-stores.md#lease-contention-and-deployments-that-change-the-lease-duration)).

The suite only ever *reads* the database. After the kill it asserts the ledger is `Running`
behind a persisted, unexpired lease; at the end, that the run and its child are `Succeeded`, the
queue has drained, and nothing was dead-lettered on the transport's default retry budget. A lease
journal written on the database clock proves the premise instead of assuming it: the successor was
refused while the owner's lease was unexpired, took over only after it expired, and never handed
the wake-up back to the queue. No lease deletion, no `ResumeAsync`, no extra wake-up. Step
re-execution counts pin checkpoint and at-least-once semantics at each boundary (a step behind a
persisted checkpoint never re-runs; a publish that died before its checkpoint runs twice).

```bash
dotnet run --project tests/AsyncResponse.IntegrationTests -f net10.0 -- --filter-class "*DurableFlowAbruptCrashRecoveryTests"
```

It takes about two minutes plus the data fleet's boot (each scenario waits out a 20-second owner
lease). Every worker's stdout and stderr is attached to the test output; crash points, environment
variables, and table names live in `CrashWorkerContract`. To model your own crash points against your own broker and store, copy the
shape: a store wrapped in a `DispatchProxy` (a hand-written decorator silently drops
default-interface members such as `ObserveLeaseAsync`), `IDurableFlowExecutionObserver` for step
boundaries, `Process.GetCurrentProcess().Kill()` rather than `Environment.Exit`, and a transport
claim timeout shorter than the execution lease so the redelivery arrives while the lease is held.
Launch the worker from **its own** build output, not from the copy a `ProjectReference` drops next
to the test assembly: a test project that references ASP.NET Core has the package assemblies the
shared framework also ships (`Microsoft.Extensions.Hosting.Abstractions`, …) removed from its
output whenever the SDK's framework is at least as new as the package, while the worker's
`runtimeconfig.json` lists `Microsoft.NETCore.App` only — so the copy runs on one SDK and dies at
startup with `FileNotFoundException` on the next. The integration test project stamps the worker's
exact `TargetPath` into its assembly metadata (`EmbedCrashWorkerPath` in its csproj) for this.

## Sizing and guard rails

- Every harness wait (`WaitFor*`, `WaitForWorkerIdleAsync`) is bounded by
  `options.RealTimeGuard` (default 10 s of *real* time) and fails with a diagnosis — a hung test
  tells you it hung and why, usually "advance the clock first".
- A publish (`PublishAsync`, `PublishExceptionAsync`, `ReplyAsync`) runs a lost subscriber's
  recovery callbacks inline, and a callback that throws is retried with backoff on the virtual
  clock: while the publish is pending, the harness advances the clock to the timers the publish
  itself armed and due within a few seconds (a ladder step is at most two), and nothing else —
  not the timers of work a callback enqueues (a flow run it starts or resumes, a delayed job it
  schedules), nor a longer wait the callback armed. A publish still pending after
  `RealTimeGuard` — a callback blocked on something the test controls, waiting on a longer
  virtual timer, or sleeping on the system clock — fails with a `TimeoutException` naming the
  cause.
- Advancing walks every armed timer, and every step costs a few real milliseconds of settle
  (three 1 ms delays at least — about 15 ms each on Windows' default timer resolution). Anything
  that holds its execution lease while the clock crosses days renews that lease every
  `ExecutionLeaseRenewInterval` (20 s by default): a long **in-process** sleep, and an **awaited
  step** with a long timeout (awaited steps are never hopped). `AdvanceAsync(TimeSpan.FromDays(3))`
  past either walks about 13,000 renewals — tens of seconds of real time on Linux, minutes on
  Windows. Widen the lease cadence (`ExecutionLeaseDuration` / `ExecutionLeaseRenewInterval`) in
  such tests so the walk is a few steps, not thousands. Suspend-path timers (the default for long
  sleeps) don't have this concern — a 3-day sleep is one timer.
- The in-memory flow store has no ledger-size budget by default. Set `options.MaxStateBytes` to
  your production store's `MaxStateBytes` (DynamoDB 350 000 bytes by default, Cosmos DB
  1 900 000, MongoDB 15 000 000) so a ledger that store would refuse fails the test that grows it:
  a start whose initial state is over it throws `FlowStateTooLargeException` before anything is
  published, and a checkpoint over it fails the attempt with that exception (retried, then
  dead-lettered, as in production). A `LedgerSizeWarningBytes` left at its default is fitted under
  the budget; one set at or above it fails `StartAsync`.
- The in-memory channel's default wait timeout is 30 minutes (its `RecoveryStateExpiry`, used
  while `DefaultTimeout` is unset); drive scripted conversations with advances smaller than that
  (or set `options.Channel = c => c.DefaultTimeout = …`) unless the timeout is what you're testing.
- Keep flow dependencies as ordinary DI fakes via `options.ConfigureServices` — the harness
  re-applies registrations on every simulated restart, so keep instances you assert on in test
  locals (registered as singletons), like the recorders in the library's suites.
- Don't register your own `TimeProvider` in `ConfigureServices` or `ConfigureAsyncResponse` (whose
  builder exposes the same `Services`): it would displace the virtual clock so that no timer,
  timeout, lease, or backoff ever elapses, so harness construction fails with
  `InvalidOperationException` naming the fix. Drive time through `harness.Clock` /
  `AdvanceAsync` instead.
- Call `services.AddLogging(...)` in `ConfigureServices` (or `builder.Services.AddLogging(...)` in
  `ConfigureAsyncResponse`) to see the engine's own diagnostics; without it the harness falls back
  to `NullLogger<>`.

### Concurrency and publication regressions (library contributors)

- Fakes whose call collections are read while subscribers run must support concurrent snapshots
  (the Redis acknowledgment fake uses `ConcurrentQueue`, pinned by a regression test).
- Coordinate races with completion signals, not sleeps — e.g. the SQS tests block an
  already-started renewal of a message before its failure schedules a retry.
- Publication outcomes the broker decides need a real server: the Redis integration tests run the
  actual publish, replace the first executed command's reply with a simulated timeout, and check
  both the failed-append and successful-append outcomes. They run in the Redis compatibility job,
  Valkey included; a mocked successful transaction cannot prove that contract.
