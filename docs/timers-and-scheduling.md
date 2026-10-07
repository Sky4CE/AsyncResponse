# Durable timers, delayed jobs, and cron-scheduled flows

"Sleep for three days inside a flow" and "start this flow every night at 06:00" are first-class
operations. Both are durable — they survive crashes, redeploys, and redeliveries on every
supported backend — and both run instantly in tests on the
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
step. A non-positive delay, or an instant in the past, completes immediately.

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

On a transport with native delayed delivery (`IDelayedWorkerTransport`) the run **suspends** — the
same mechanism as awaiting a child flow. The executor persists the due time, enqueues a *delayed
wake-up job*, and ends the current delivery. At the due time the broker delivers the wake-up, any
replica re-executes the flow, completed steps skip, and the timer step completes. While the flow
sleeps no worker is occupied, no lease is renewed, and nothing is held in process — a crash during
the sleep is a non-event, because the wake-up lives on the broker.

- **Short remainders stay in process.** A remainder at or under
  `DurableFlowOptions.TimerInProcessThreshold` (default 10 seconds; zero always suspends) waits
  under the execution lease — a broker round-trip for a two-second sleep costs more than it frees.
- **Capped per-hop delays chunk transparently.** Wake-ups carry their absolute due time
  (`WorkerJobEnvelope.NotBeforeUtc`), and the shared worker-job executor re-publishes any job
  delivered early for the remainder. A 3-day sleep on SQS (15-minute cap) is ~288 automatic hops,
  none of which run flow code.

### Timers that wait in process

On transports **without** native delayed delivery (Kafka, RabbitMQ, Google Pub/Sub, Redis Streams,
NATS, and an SQS FIFO queue), timers wait in process under the execution lease — the same footprint
and crash story as an awaited step: broker redelivery of the executing job resumes the remainder.

Such a wait holds its broker delivery unsettled, and some brokers cap how long one delivery may stay
in flight however alive its handler is. Past the ceiling the broker hands the **same job** to
another consumer while the first handler is still sleeping:

| Broker | In-flight ceiling |
|---|---|
| Google Pub/Sub | `MaxTotalAckExtension` (default 60 minutes). |
| RabbitMQ | `consumer_timeout` (default 30 minutes) — mirror your broker's value in [`BrokerConsumerTimeout`](configuration.md#transport-options). The clock starts when the broker *sends* a delivery, so prefetched deliveries age while they wait: the transport advertises `BrokerConsumerTimeout / WorkerSubscriber.PrefetchCount`, never less than one minute nor more than the timeout. In ack-after-handler mode a startup warning flags a share below that floor; set `PrefetchCount = 1` for longer in-process hops. |
| SQS | 12 hours (the visibility-timeout maximum). |
| Azure Service Bus | None with lock renewal (the default). With `LockRenewalInterval = null` the entity's lock duration applies, advertised as its 5-minute maximum. |

RabbitMQ and Service Bus advertise no ceiling under early ACK (`AckAfterEnqueue`), where nothing
stays unsettled at the broker. (Service Bus and standard SQS queues suspend long timers, so their
ceilings matter only for in-process waits: short remainders and awaited steps.)

A transport that knows its ceiling advertises it (`IWorkerTransportInFlightLimit`), and the engine
waits a long sleep in **hops**. Each hop parks for the shortest of: half the ceiling; what the
delivery has left of the ceiling after the steps that ran before the timer, less a tenth of the
ceiling as headroom (with nothing left it hands over at once); and
`DurableFlowOptions.MaxInProcessParkDuration`. Then it checkpoints, publishes an immediate wake-up
for the run, and ends the delivery; the replay resumes the same timer (its due time is
checkpointed) under a fresh delivery whose in-flight clock starts again. A wake-up that arrives
while its own handler is still running is recognised and never acknowledged as a duplicate (see
[durable-flows.md](durable-flows.md#what-happens-when-things-die)). A transport with neither
delayed delivery nor a ceiling waits a sleep longer than the ~49.7-day .NET timer ceiling in hops
of that length.

Awaited-response steps are **not** hopped: a step re-attaches to a correlation id, so handing its
delivery back would mean re-establishing the wait from the ledger on every hop. Instead, a step
whose timeout exceeds half the transport's ceiling logs a warning naming both, once per park
(`MaxInProcessParkDuration` shortens timer hops only).

**Host stop** ends an in-process timer wait the same way: the run checkpoints, publishes an
immediate wake-up, and acknowledges its delivery. A sleep that spans many deploys therefore never
accumulates delivery attempts — brokers count an unsettled redelivery as a failed one, and an
attempt cap would eventually dead-letter the run's only wake-up without running it. A timer reached
on a host that is already stopping, or whose hand-over cannot be published, hands the delivery back
to the transport instead (`DurableFlowInterruptedException`) for redelivery to a live replica or
after the restart. That redelivery does count: on RabbitMQ it arrives `redelivered`, so a worker
`MaxDeliveryAttempts` of 1 rejects it unrun (the worker subscriber warns at startup) — keep it at 2
or more. Either way host stop decides, even when the token passed to `DelayAsync` is one it also
cancels (`ApplicationStopping` injected into the flow), and the executor runs none of its failure
path (no failure checkpoint, no error span).

### How long one sleep can be

On both paths the ledger's TTL is extended to cover the sleep, so a run never out-sleeps its own
state. That bounds a single sleep at the 3650-day persistence ceiling **minus**
`DurableFlowOptions.StateExpiry` (default 14 days → 3636 days), so the extended TTL outlives the due
instant by the full idle margin. A longer delay fails the run terminally, naming the budget.

## Delayed worker jobs

Outside flows, the fluent builder can schedule any worker job:

```csharp
await _asyncResponse.EnqueueWorkerAsync<INotificationService>(
    svc => svc.SendReminderAsync(customerId),
    delay: TimeSpan.FromHours(4));
```

This requires a transport that implements `IDelayedWorkerTransport` in its current configuration
(see the matrix below); otherwise the call throws with guidance. A delay may be at most 3650 days;
one longer than the transport's per-hop cap chunks automatically through the `NotBeforeUtc`
re-publish chain. Inside a flow, prefer `flow.DelayAsync(...)` followed by a normal enqueue — that
works on every transport.

## Native delayed delivery by transport

| Transport | Native mechanism | Per-hop cap | Notes |
|---|---|---|---|
| In-memory | `TimeProvider` timer wheel | ~49.7 days (chunked) | Delayed jobs share the process lifetime and are dropped (logged) at shutdown. Bounded by `DelayedJobCapacity` (default 4096): an external publisher waits for a slot; a flow parking from inside a job is rejected and redelivered. Virtual-clock aware in tests. |
| Azure Service Bus | scheduled messages (`ScheduledEnqueueTime`) | none | The broker holds the message; survives restarts. |
| AWS SQS | `DelaySeconds` | 15 min (chunked) | Standard queues only. SQS rejects per-message delays on FIFO queues, so a FIFO worker queue advertises **no** delay capability: flow timers wait in process and a bare delayed enqueue fails at the call site. On FIFO, flow jobs without a correlation id also share one message group, so a flow parked in process holds every other flow's jobs — prefer a standard worker queue for durable flows. |
| PostgreSQL | `available_at` gate on the claim query | none | Due time on the **database** clock (`now() + delay`); precision bounded by the subscriber's `EmptyPollDelay`. |
| SQL Server | `available_at` gate on the claim query | none | Due time on the database clock (`SYSUTCDATETIME()`); precision as PostgreSQL. |
| MongoDB | `available_at` gate on the claim filter | none | Due time stamped on the **database** clock (`$$NOW + delay`) in one atomic write. Early delivery caused by skew between this host's clock (which stamps `NotBeforeUtc`) and the server's is corrected by the `NotBeforeUtc` guard, which executes rather than re-publishing forever once the remainder stops shrinking. |
| Kafka, RabbitMQ, Google Pub/Sub, Redis Streams, NATS | — | — | No native delay: flow timers wait in process; a bare delayed enqueue throws. |

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

Every replica runs the scheduler and computes the same occurrence and the same deterministic run id,
`sched:{name}:{occurrence as UTC yyyyMMddTHHmmssZ}` (`sched:nightly-report:20300101T060000Z`). The
flow store's atomic create accepts exactly one; the losers re-enqueue the same run. The execution
lease keeps those duplicate wake-ups from running the flow concurrently but does not deduplicate
them: one that arrives after the run has parked (on a child flow, or a durable timer on a
delayed-capable transport) replays it — completed steps skip — and re-parks it with its own wake-up.
A run that parks early can therefore carry one wake-up chain per replica for its lifetime: extra
executions, ledger writes, and observer events, never a repeated step.

`WithScheduledFlow` also registers the flow like `WithDurableFlow` (statically routed, so scheduled
flows are trim/AOT-safe) and validates at the call site: the name must be unique and produce
portable flow ids, and the expression must parse and have a future occurrence.

The `input` factory receives the occurrence's scheduled UTC instant and **must be deterministic
across replicas** — the idempotent start compares inputs, so don't put `Guid.NewGuid()` in it.

| `ScheduledFlowOptions` | Default | Meaning |
|---|---|---|
| `TimeZone` | UTC | Zone the expression is evaluated in. |
| `Enabled` | `true` | `false` keeps the registration (and its flow routing) but schedules nothing — e.g. per environment. |
| `RedriveInterval` | 30 seconds | Retry cadence for an occurrence whose start could not be published. |
| `StartupRedriveWindow` | 1 hour | How far back the startup probe looks for lost starts; zero disables it. |

**A due occurrence whose start could not be published is never abandoned while the process
lives.** Starting an occurrence publishes its start job first — the job carries the initial ledger
and creates the run when executed (see
[durable-flows.md](durable-flows.md#what-happens-when-things-die)) — so a publish that fails after
the start's own retry ladder (a broker outage; `DurableFlowNotDispatchedException`) persists
nothing. The scheduler keeps such an occurrence in an in-process re-drive queue and repeats the
idempotent start every `RedriveInterval` until the job is published or the run is seen to have
executed (another replica started it); an absent ledger means "not yet published", never
"expired". The queue holds at most 256 occurrences per schedule: beyond that it drops the oldest
with an error naming its id, which stays startable by hand. The queue dies with its process: an
occurrence whose publish was still failing at shutdown — or whose publish the host stop cancelled —
persisted nothing, so it is skipped like any occurrence missed while no replica was up.

**At startup, each schedule probes for lost starts.** It looks back `StartupRedriveWindow` (at most
the 64 most recent occurrences, so a long window on a frequent schedule costs startup nothing) and
re-drives any occurrence whose ledger *exists*, is Running, and has zero attempts — a start whose
job was published and then lost in transit (an early-ACK worker subscriber, a broker that dropped
it). A run merely queued behind a busy worker looks the same and is re-driven harmlessly: the lease
keeps the two wake-ups from running the flow at once and completed steps replay from their
checkpoints (a run that has parked by then gains a second wake-up chain, as above). Every re-drive
is logged. The scheduler's logging is best-effort: each outcome — queue for re-drive, start, skip —
is decided before its log line, so a logging provider that throws neither loses an occurrence nor
stops the schedule (or the host).

## Cron syntax

Exactly five fields — `minute hour day-of-month month day-of-week` — parsed by `CronSchedule`
(public, usable on its own via `CronSchedule.Parse(expression, timeZone)` and
`GetNextOccurrence`). There is no seconds field, no `@daily`-style macro, and no `L`/`W`/`#`.

- `*` (and `?` in the day fields), single values, lists `1,15`, ranges `1-5` (wrap-around
  `22-2` supported), steps `*/15`, `10-40/5`, `8/2`, names `JAN…DEC` / `SUN…SAT`
  (case-insensitive).
- Day-of-month and day-of-week combine with classic Vixie-cron semantics: **OR** when both are
  explicitly restricted, **AND** when either is star-shaped (`*`, `*/2`, `?`) — a star-step field
  stays out of the either/or rule while its step mask still applies, as Vixie's
  `DOM_STAR`/`DOW_STAR` flags do. `0` and `7` are both Sunday, and a stepped day-of-week range that
  wraps past Saturday strides on the real 7-day week — `SAT-MON/2` fires Saturday and Monday, not
  Saturday and the Sunday duplicate.
- Expressions are validated at registration — a typo fails the `WithScheduledFlow` call, not
  silently at 3 a.m.

**Time zones.** Occurrences are computed as wall-clock times in the schedule's `TimeZone` (default
UTC) and fired at the corresponding UTC instant. A wall time skipped by spring-forward fires at the
**gap's end** — the transition instant (a 02:30 schedule in a 02:00→03:00 jump fires when the clock
reads 03:00); several scheduled minutes inside one gap collapse onto that single fire. A wall time
repeated by fall-back fires on the first (earlier-offset) pass only. Zone rules are captured when
the loop starts, so an OS time-zone database update takes effect after a process restart.

Sparse-but-valid combinations resolve however far out the next occurrence is (`0 0 29 2 */7` — Feb
29 on a Sunday — waits decades between fires): satisfiability is proven over a full 400-year
Gregorian cycle, so only genuinely impossible dates ("Feb 30") are rejected.

## Semantics worth knowing

- **Timers are anchored at first execution.** `DelayAsync("w", 3 days)` reached at T sleeps
  until T+3d — a crash at T+1d resumes a 2-day sleep. `DelayUntilAsync` checkpoints its instant,
  so editing the code mid-run cannot double- or under-sleep an in-flight run.
- **Schedules are at-most-once.** Occurrences that pass while *no* replica is up are skipped on
  restart, by design — the run history shows the gap. A late timer fire (seconds) still starts its
  own occurrence; a loop that wakes so late that several occurrences are due (a paused VM, a clock
  jump) starts only the latest. An occurrence the loop reached but could not publish is re-driven in
  process (see above); a published start whose job was then lost in transit is found by the startup
  probe.
- **Renaming a schedule** changes the ids future occurrences dedup on; in-flight runs are
  unaffected.
- **Suspended-timer wake-ups are broker messages.** Their loss modes are the transport's loss
  modes; on the in-memory transport, delayed jobs deliberately die with the process (logged), and
  an operator `ResumeAsync(flowId)` revives a stranded run.
- **Everything here is testable in milliseconds** — production-sized sleeps, schedules, and
  retry backoffs run on the virtual clock. See [Testing AsyncResponse applications](testing.md).
