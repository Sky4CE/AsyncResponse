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
elapses in a microsecond of test time; nothing in a test ever calls a real sleep.

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

var run = await harness.StartFlowAsync<TenantOnboardingFlow, OnboardingInput>(new(tenantId: 7));

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
checkpoint. One crash can be armed at a time: arming another while one is still pending throws
instead of silently discarding the first, which would let a test believe it exercised a
crash/resume path that never ran — arm the next crash after the current one has fired:

```csharp
harness.CrashAfterStep("create-workspace");   // die between the checkpoint and the next step
var run = await harness.StartFlowAsync<TenantOnboardingFlow, OnboardingInput>(input);
await harness.AdvanceAsync(TimeSpan.FromSeconds(2));      // let the redelivery backoff elapse
// … script replies … then:
Assert.Equal(FlowRunStatus.Succeeded, await run.WaitForFinishedAsync());
Assert.Equal(1, recorder.Count("create-workspace"));      // a crash costs a delivery, never a duplicate side effect
```

When more than one run can reach the armed step — concurrent flows, or a parent and a child flow
reusing step names — pass the flow id to pin the one-shot crash to its run
(`harness.CrashAfterStep("create-workspace", flowId: run.FlowId)`); an unscoped crash fires on
whichever run gets there first.

For awaited steps, `CrashAfterStep("remote-step")` also stops the attempt after the response is
checkpointed. Completion observers run outside response-settlement recovery, so their exception
cannot be swallowed by checkpointing the same response a second time. Assert that the next step
has not run before retry, that the completion event occurred once, and that the resumed run's
`Attempts` increased. Include both before/after cases for awaited steps in a crash matrix.

Run it as a `[Theory]` over every step of your flow — the crash-at-every-checkpoint matrix from
the library's own suite ([FlowTestHarnessShowcaseTests](../tests/AsyncResponse.Tests/FlowTestHarnessShowcaseTests.cs)),
now three lines per row.

## Timers, schedules, and retries on virtual time

`harness.AdvanceAsync(delta)` advances stepwise and lets the worker pipeline settle between
steps, so chained work is honored inside one call: a durable timer wakes, the flow re-suspends
for the next chunk, a retry backoff elapses and redelivers, a cron loop fires and re-arms.

```csharp
var run = await harness.StartFlowAsync<ReminderFlow, ReminderInput>(new("acme"));
var wakeAt = await run.WaitForTimerStepAsync("cool-down");   // flow sleeps 3 days, holds nothing
await harness.AdvanceAsync(TimeSpan.FromDays(3));            // …skipped
Assert.Equal(FlowRunStatus.Succeeded, await run.WaitForFinishedAsync());
```

Cron schedules fire with their deterministic run ids
(`harness.Attach("sched:nightly-report:20300101T060000Z")` observes one), and an outage is one
line: `SimulateRestartAsync(whileDown: () => harness.Clock.Advance(TimeSpan.FromHours(4)))` —
occurrences that fell into the downtime are skipped, exactly as in production. See
[ScheduledFlowTests](../tests/AsyncResponse.Tests/ScheduledFlowTests.cs) and
[DurableFlowTimerTests](../tests/AsyncResponse.Tests/DurableFlowTimerTests.cs).

## AsyncResponseTestHarness — testing direct awaits

For code that uses the fluent builder without flows, `AsyncResponseTestHarness` (which
`FlowTestHarness` wraps — it's `harness.Engine`) hosts the same engine:

```csharp
await using var harness = await AsyncResponseTestHarness.StartAsync();

var wait = harness.Builder
    .For<OperationResult>()
    .WithTimeout(TimeSpan.FromMinutes(5))                  // the PRODUCTION value, finally testable
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

`SimulateRestartAsync()` models a redeploy: the service provider — and with it every live waiter,
subscription, and in-flight execution — is discarded and rebuilt, while the durable state a real
deployment would retain survives: recovery registrations, flow ledgers, and scheduled (delayed)
worker jobs, re-published with their remaining virtual delay like broker-held scheduled messages.

```csharp
_ = await subscriber.CreateRecoverableResponseWaiter<OperationResult>(
    correlationId, resumeCallback: resume);                // …and the process "dies"

await harness.SimulateRestartAsync();

await harness.PublishAsync(new OperationResult { Status = OperationStatus.Completed }, correlationId);
// No live waiter → the persisted registration routes the payload through OnRecovery() →
// the resume callback runs against the NEW incarnation's services.
```

The dead incarnation's waiters are abandoned, not disposed: their tasks are cancelled for
anyone still holding them, and disposing one afterwards (a flow disposes its waiter after the
cancelled wait; an `await using` caller does the same) is a no-op that leaves the recovery
registration in place — exactly what a crashed process leaves behind.

**The restart is cooperative.** It discards everything a crash would lose and breaks the dead
incarnation's execution leases, but there is no process to kill: a step body that outlives the
graceful stop (bounded by `options.RealTimeGuard`) — it ignored its cancellation and is blocked
on something the test controls — keeps running beside the new incarnation and performs its side
effects *after* the restart returned, which is less than a "restart" claims. `SimulateRestartAsync`
therefore refuses with `InvalidOperationException` when user code is still executing after the
stop lapsed (engine-owned parks — an awaited step or an in-process timer holding its worker slot
on the virtual clock — are expected and never trip this). Let the step observe its cancellation
token or finish before restarting; for crash-*at-a-checkpoint* semantics use
`FlowTestHarness.CrashBeforeStep` / `CrashAfterStep`, which fail the attempt at the exact
boundary with nothing left running. A test that deliberately wants the overlap sets
`options.AbandonLingeringExecutionsOnRestart = true` and then owns it: the abandoned execution's
side effects land whenever it unblocks. Nothing in the harness is a subprocess kill; a guarantee
that must hold against abrupt termination needs a real process and a real broker.

It also breaks the dead incarnation's leases *for* the new one. A process that really dies leaves
its execution lease persisted and unexpired, and whatever redelivers the run's wake-up has to get
past that lease by itself — the window in which a wake-up can be acknowledged as a "duplicate" of
an execution that no longer exists. The harness cannot reach that window; the library's own
suite for it is below.

### The abrupt-crash suite (how the library tests what the harness cannot)

`DurableFlowAbruptCrashRecoveryTests` (integration batch `data`) is the one suite that kills a real
process. It launches `tests/AsyncResponse.IntegrationTests.CrashWorker` as a subprocess on the
PostgreSQL worker transport and PostgreSQL flow-state store (one container, a private schema per
scenario). The worker SIGKILLs itself at an armed crash point — after lease acquisition, after a
step checkpoint, after a publish but before its checkpoint, or after a child checkpoint but before
the child is published. No `finally` runs, the lease is not released, and the queue claim is not
settled. A successor process then starts against the unchanged database; one scenario gives it a
much shorter `ExecutionLeaseDuration` than the dead owner's, which is the deployment change that
used to strand runs (see
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

About two minutes plus the data fleet's boot (each scenario waits out a 20-second owner lease). Every worker's stdout and stderr is attached to the
test output, and crash points, environment variables, and table names live in
`CrashWorkerContract`. To model your own crash points against your own broker and store, copy the
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

This is the recovery tri-state (`Resume` / `Fail` / `KeepWaiting`) — the part of the API teams
most need to test and previously could not without a broker. Waiter tasks obtained before the
restart never carry a response or a timeout: the restart abandons them exactly as a crash does —
their `ResponseTask` is cancelled and their recovery registration is deliberately left intact, so
the late response routes through `OnRecovery()`. Assert through the recovery side effects, not the
dead incarnation's task.

## Sizing and guard rails

- Every harness wait (`WaitFor*`, `WaitForWorkerIdleAsync`) is bounded by
  `options.RealTimeGuard` (default 10 s of *real* time) and fails with a diagnosis — a hung test
  tells you it hung and why, usually "advance the clock first".
- Advancing walks every armed timer: if a test forces a long **in-process** sleep, widen the
  lease cadence (`ExecutionLeaseDuration` / `ExecutionLeaseRenewInterval`) so the walk is a few
  steps, not thousands. Suspend-path timers (the default for long sleeps) don't have this
  concern — a 3-day sleep is one timer.
- The in-memory channel's default wait timeout is 30 minutes; drive scripted conversations with
  advances smaller than that (or set `options.Channel = c => c.DefaultTimeout = …`) unless the
  timeout is what you're testing.
- Keep flow dependencies as ordinary DI fakes via `options.ConfigureServices` — the harness
  re-applies registrations on every simulated restart, so keep instances you assert on in test
  locals (registered as singletons), like the recorders in the library's suites.
- Don't register your own `TimeProvider` in `ConfigureServices` — the harness runs the whole engine
  on its own virtual clock, and construction now fails fast naming the fix instead of letting a
  registered clock silently displace it (no timer, timeout, lease, or backoff would ever elapse).
  Drive time through `harness.Clock` / `AdvanceAsync` instead.
- Call `services.AddLogging(...)` in `ConfigureServices` to see the engine's own diagnostics — it
  now wins over the harness's `NullLogger<>` fallback, which previously always registered first and
  swallowed them regardless of what the test configured.

### Concurrency and publication regressions

Fake call collections observed while subscribers run must support concurrent snapshots; the
Redis acknowledgment fake uses `ConcurrentQueue` and pins snapshot behavior in a regression.
Use completion signals to coordinate races. SQS tests block an already-started renewal of the
same message before its failure schedules a retry. Redis integration tests execute the actual
publish operation, replace the first executed command's reply with a simulated timeout, and
check both failed-append and successful-append outcomes. These tests run in the existing Redis
compatibility job, including Valkey; a mocked successful transaction cannot prove that contract.
