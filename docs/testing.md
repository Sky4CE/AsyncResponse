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
