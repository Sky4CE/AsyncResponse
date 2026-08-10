# Durable timers, delayed jobs, and cron-scheduled flows

"Sleep for three days inside a flow" and "start this flow every night at 06:00" are first-class
operations. Both are durable: they survive crashes, redeploys, and redeliveries, on every
supported backend, and both run instantly in tests on the
[AsyncResponse.Testing virtual clock](testing.md).

**On this page**

- [Durable timers inside flows](#durable-timers-inside-flows)
- [How a sleeping flow costs nothing](#how-a-sleeping-flow-costs-nothing)
- [Delayed worker jobs](#delayed-worker-jobs)
- [Native delayed delivery by transport](#native-delayed-delivery-by-transport)
- [Cron-scheduled flows](#cron-scheduled-flows)
- [Cron syntax](#cron-syntax)
- [Semantics worth knowing](#semantics-worth-knowing)

## Durable timers inside flows

`DelayAsync` sleeps for a duration; `DelayUntilAsync` sleeps to an absolute UTC instant. Both are
checkpointed steps: the due time is persisted the first time the step is reached, replays wait out
the **remainder** (never restart the delay), and a completed timer is skipped like any memoized
step.

```csharp
public async Task ExecuteAsync(IDurableFlowContext flow, OrderInput input)
{
    await flow.StepAsync("reserve-stock", () => _inventory.ReserveAsync(input.OrderId));

    // Give the customer three days to pay. The run holds NO worker, lease, or memory meanwhile.
    await flow.DelayAsync("payment-window", TimeSpan.FromDays(3));

    var payment = await flow.StepAsync("check-payment", () => _payments.CheckAsync(input.OrderId));
    if (!payment.Received)
        await flow.StepAsync("cancel", () => _inventory.ReleaseAsync(input.OrderId));
}
```

A timer's cancellation token (like every step's) stops *this execution*, not the timer: the due
time stays checkpointed and the next delivery resumes the remainder.

## How a sleeping flow costs nothing

On a transport with native delayed delivery the run **suspends** — the same mechanism as awaiting
a child flow. The executor persists the due time, enqueues a *delayed wake-up job*, and ends the
current delivery. The broker holds the wake-up; at the due time it delivers, any replica
re-executes the flow, completed steps skip, and the timer step completes. While the flow sleeps
there is no worker occupied, no execution lease being renewed, and no in-process state — a
process crash during the sleep is a non-event, because the wake-up lives on the broker.

Two refinements:

- **Short remainders stay in process.** Below `DurableFlowOptions.TimerInProcessThreshold`
  (default 10 seconds) the executor just waits under its lease — a broker round-trip for a
  two-second sleep costs more than it frees.
- **Transports with capped per-hop delay chunk transparently.** SQS caps a single hop at
  15 minutes. Wake-ups carry their absolute due time (`WorkerJobEnvelope.NotBeforeUtc`), and any
  job delivered early is re-published for the remaining delay by the shared worker-job executor —
  a 3-day sleep on SQS is ~288 automatic 15-minute hops, none of which execute flow code.

On transports **without** native delayed delivery (Kafka, RabbitMQ, Google Pub/Sub, Redis
Streams, NATS), timers wait in process under the execution lease — the same footprint as an
awaited step, with the same crash story (broker redelivery of the executing job resumes the
remainder). The ledger's TTL is automatically extended to cover the sleep on both paths, so a
run can never out-sleep its own state.

## Delayed worker jobs

Outside flows, the fluent builder can schedule any worker job:

```csharp
await _asyncResponse.EnqueueWorkerAsync<INotificationService>(
    svc => svc.SendReminderAsync(customerId),
    delay: TimeSpan.FromHours(4));
```

This requires the registered transport to implement `IDelayedWorkerTransport` (see the matrix
below); on other transports it throws with guidance at the call site. Delays longer than the
transport's per-hop cap chunk automatically via the `NotBeforeUtc` re-publish chain. Inside a
flow, prefer `flow.DelayAsync(...)` followed by a normal enqueue — that works on every transport.

## Native delayed delivery by transport

| Transport | Native mechanism | Per-hop cap | Notes |
|---|---|---|---|
| In-memory | `TimeProvider` timer wheel | none | Delayed jobs share the process lifetime; dropped (loudly) at shutdown. Virtual-clock aware in tests. |
| Azure Service Bus | scheduled messages (`ScheduledEnqueueTime`) | none | The broker holds the message; survives restarts. |
| AWS SQS | `DelaySeconds` | 15 min (chunked) | Standard queues only — SQS rejects per-message delays on FIFO queues, so a FIFO worker queue advertises **no** delay capability (`MaxPublishDelay` = zero): flow timers fall back to the in-process path, and a bare delayed enqueue fails fast at the publish call site. |
| PostgreSQL | `available_at` gate on the claim query | none | Due time computed on the **database** clock (`now() + delay`); precision bounded by the subscriber's `EmptyPollDelay`. |
| SQL Server | `available_at` gate on the claim query | none | Due time on the database clock (`SYSUTCDATETIME`); precision as PostgreSQL. |
| MongoDB | `available_at` gate on the claim filter | none | Insert stamps a client-computed due time in one atomic write; early delivery from clock skew is corrected by the `NotBeforeUtc` guard, which detects a non-shrinking remainder (persistent skew) and executes rather than re-publishing forever. |
| Kafka, RabbitMQ, Google Pub/Sub, Redis Streams, NATS | — | — | No native delay; flow timers use the in-process path, bare delayed enqueue throws. |

## Cron-scheduled flows

Start a flow on a schedule, with **no leader election**:

```csharp
services.AddAsyncResponse()
    .WithRedisChannel(...)
    .WithRabbitMqTransport(...)
    .WithPostgreSqlDurableFlows(...)
    .WithScheduledFlow<NightlyReportFlow, ReportInput>(
        name: "nightly-report",
        cron: "0 6 * * *",
        input: occurrence => new ReportInput(occurrence),
        configure: s => s.TimeZone = TimeZoneInfo.FindSystemTimeZoneById("Europe/Berlin"));
```

Every replica runs the scheduler; every replica computes the same occurrence and the same
deterministic run id (`sched:nightly-report:20300101T060000Z`); the flow store's atomic create
accepts exactly one, and the losers' duplicate wake-ups are absorbed by the execution lease. The
registration also routes the flow statically (like `WithDurableFlow`), so scheduled flows are
trim/AOT-safe.

The `input` factory receives the occurrence's scheduled UTC instant and **must be deterministic
across replicas** (every replica must produce the same value for the same occurrence — don't put
`Guid.NewGuid()` in it).

## Cron syntax

Five fields — `minute hour day-of-month month day-of-week` — parsed by `CronSchedule` (public,
usable on its own):

- `*` (and `?` in the day fields), single values, lists `1,15`, ranges `1-5` (wrap-around
  `22-2` supported), steps `*/15`, `10-40/5`, `8/2`, names `JAN…DEC` / `SUN…SAT`.
- Day-of-month and day-of-week combine with classic Vixie-cron **OR** semantics when both are
  restricted; `0` and `7` are both Sunday.
- Expressions are validated at registration — a typo fails the `WithScheduledFlow` call, not
  silently at 3 a.m.

Time zones: occurrences are computed as wall-clock times in the schedule's `TimeZone` (default
UTC) and fired at the corresponding UTC instant. Across DST transitions: a wall time skipped by
spring-forward fires at the moment the clock jumps past it; a wall time repeated by fall-back
fires on the first (earlier-offset) pass only.

## Semantics worth knowing

- **Timers are anchored at first execution.** `DelayAsync("w", 3 days)` reached at T sleeps
  until T+3d — a crash at T+1d resumes a 2-day sleep. `DelayUntilAsync` checkpoints its instant,
  so editing the code mid-run cannot double- or under-sleep an in-flight run.
- **Schedules are at-most-once.** Occurrences that pass while *no* replica is up are skipped on
  restart, by design — the run history shows the gap. A late timer fire (seconds) still starts
  its own occurrence.
- **Renaming a schedule** changes the ids future occurrences dedup on; in-flight runs are
  unaffected.
- **Suspended-timer wake-ups are broker messages.** Their loss modes are the transport's loss
  modes; on the in-memory transport, delayed jobs deliberately die with the process (logged), and
  an operator `ResumeAsync(flowId)` revives a stranded run.
- **Everything here is testable in milliseconds** — production-sized sleeps, schedules, and
  retry backoffs run on the virtual clock. See [Testing AsyncResponse applications](testing.md).
