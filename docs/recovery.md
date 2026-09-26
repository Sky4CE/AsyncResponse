# Lost-subscriber recovery

[← Back to README](../README.md)

Every wait records *recovery state* for cleanup and watchdog visibility; durable channels (Redis,
NATS, PostgreSQL, SQL Server, MongoDB) persist it beyond the process. When a response arrives after the waiter died
(e.g. a redeploy), it is **classified by its domain outcome** and routed to the right callback:
resume the flow, fail it — never resume a failure — or, for a non-terminal checkpoint, keep the
registration armed and wait for the terminal response.

The `OnLostSubscriber*` methods live on `IRecoverableAsyncResponseBuilder` and its fluent builders,
which every bundled channel registers — `.WithInMemoryChannel()` included. The in-memory channel's
recovery is process-local: it spans waiter loss within one process lifetime (and the simulated
restarts of [AsyncResponse.Testing](testing.md)), not a real process exit. For recovery that
survives a redeploy, use a durable channel (`.WithRedisChannel()` / `.WithNatsChannel()` /
`.WithPostgreSqlChannel(...)` / `.WithSqlServerChannel(...)` / `.WithMongoDbChannel(...)`).

**On this page**

- [`OnRecovery()` — classifying recovered responses](#onrecovery--classifying-recovered-responses)
- [Non-terminal checkpoints: `KeepWaiting`](#non-terminal-checkpoints-keepwaiting)
- [Callbacks receive the materialized payload](#callbacks-receive-the-materialized-payload)
- [Registering recovery callbacks](#registering-recovery-callbacks)
- [Why recovery routing and `Until` stay separate](#why-recovery-routing-and-until-stay-separate)
- [The recovery watchdog + health check](#the-recovery-watchdog--health-check)
- [Recovery-state durability](#recovery-state-durability)
- [Wire/schema versioning](#wireschema-versioning)
- [Shared-correlation recovery](#shared-correlation-recovery)

## `OnRecovery()` — classifying recovered responses

Override `OnRecovery()` on **every payload type you use with lost-subscriber recovery
callbacks** — durable channels fail fast at waiter creation without it. That includes payloads that
can never fail (a success-only notification still needs `=> RecoveryAction.Resume`) and
progress-only checkpoints (`=> RecoveryAction.KeepWaiting`), not just payloads that can carry a
domain failure. It answers one question — *what should a late response of this type do to the flow:
resume it, fail it, or keep waiting for the terminal response?* — and is consulted **only** on the
recovery path, never for live completion (which your `Until` predicate owns):

```csharp
public sealed class OrderResult : IAsyncResponsePayload
{
    public OrderStatus Status { get; set; }
    public string? Message { get; set; }

    // Recovery routing only: resume on the terminal success, keep the registration armed on a
    // progress checkpoint, fail otherwise.
    public RecoveryAction OnRecovery() => Status switch
    {
        OrderStatus.Completed => RecoveryAction.Resume,
        OrderStatus.Processing => RecoveryAction.KeepWaiting,
        _ => RecoveryAction.Fail,
    };
}
```

A payload whose response stream is strictly terminal (every message ends the operation) simply never
returns `KeepWaiting` — e.g. `public RecoveryAction OnRecovery() => Status == OrderStatus.Failed ?
RecoveryAction.Fail : RecoveryAction.Resume;`.

The default (no override) is `Fail`, so a response can never resume the happy path by omission.
Every channel requires the override when you register recovery callbacks — waiter creation fails
fast if it is missing. That includes the in-memory channel: it implements the same recoverable
contract against its process-local store (recovery there spans waiter loss within one process
lifetime — and the simulated restarts of [AsyncResponse.Testing](testing.md) — rather than a real
process exit), so a payload that passes in tests passes unchanged on Redis.

Recovery classification is **independent of your `Until` predicate** (which owns live completion).
They answer different questions: "what does this result do to the flow?" versus "is the operation
done?". A failed payload is *still a valid response* for an active waiter — your `Until` and flow
code want to see it. This is the
[two-axes recovery model](#why-recovery-routing-and-until-stay-separate).

## Non-terminal checkpoints: `KeepWaiting`

Some remote systems report progress on the same correlation id before the terminal result — "still
running" heartbeats, staged sub-results. A live waiter simply lets its `Until` predicate observe and
skip them. On the lost-subscriber path the same message needs an explicit third route:
`RecoveryAction.KeepWaiting` invokes **no callback** and **retains the recovery registration**, so
the terminal response that follows still routes.

This lane exists because both alternatives corrupt the flow (this is the production incident that
motivated it — a deploy killed a waiter mid-step, and the remote side then published two
"in progress" messages followed by a terminal success two seconds later):

- Classifying a checkpoint as `Resume` spawns one resumed worker *per checkpoint*, consumes the
  registration, and the terminal response then finds nothing to route against and is dropped — the
  resumed workers re-attach to a correlation id nothing can answer and hang to their step timeout.
- Classifying it as `Fail` fails a flow that is still running remotely, and equally consumes the
  registration out from under the real result.

A retained registration stays bounded by `RecoveryStateExpiry` and visible to the
[watchdog](#the-recovery-watchdog--health-check); the lost-subscriber route metric/trace tag reports
`keep_waiting` for these dispatches.

## Callbacks receive the materialized payload

A response that arrives through a broker ingress is raw JSON. The recovery path materializes it as
the payload type recorded in the registration *before* classifying it, and the chosen callback
receives **that materialized instance** — regardless of the callback parameter's declared type.
Declaring the parameter as `object`, `IAsyncResponsePayload`, a base class, or the concrete type all
work; guards like `payload is OrderResult` behave identically to the live path.

This matters most for services that register **one callback for several payload types** (an
`object`-typed parameter): binding the raw JSON to the *declared* parameter type used to hand such a
callback a `JsonElement`, silently failing every type guard inside it. If the recorded payload type
cannot be resolved or materialized (e.g. renamed/removed across a deploy, or a name whose generic
argument lives in an assembly this process has not loaded — resolution never loads one), the
response is routed conservatively to the failure callback with the raw payload attached, and the
type-resolution failure is surfaced through diagnostics.

## Registering recovery callbacks

Register what should happen if the response arrives after your process died. Callbacks are
serializable method descriptors (persisted in the store, invoked through DI by the process that
receives the late response — which may be a *different deployment*):

```csharp
public sealed class OrderController(
    IRecoverableAsyncResponseBuilder _asyncResponse,
    IRemoteSystem _remoteSystem)
{
    public async Task<OrderResult> SubmitAsync(string orderId)
    {
        return await _asyncResponse
            .For<OrderResult>()
            .Until(r => r.Status != OrderStatus.Processing)
            .OnLostSubscriberResume<IOrderFlow>(flow =>
                flow.ResumeAsync(orderId, Placeholder.Payload<OrderResult>(), Placeholder.CorrelationId()))
            .OnLostSubscriberFailure<IOrderFlow>(flow =>
                flow.FailAsync(Placeholder.Exception(), Placeholder.CorrelationId()))
            .WaitAsync(context => _remoteSystem.SubmitAsync(orderId, context.CorrelationId));
    }
}
```

`Placeholder.Payload<T>()`, `Placeholder.Exception()`, and `Placeholder.CorrelationId()` are
compile-time markers substituted with the real values when the callback fires. Literal arguments
(`orderId`) are captured by value.

The failure callback receives an `AsyncResponseDomainFailureException` for domain failures (carrying
the payload JSON, outcome, and correlation id) and the original exception for technical ones —
pattern-match to tell them apart:

```csharp
public sealed class OrderFlow(ILogger<OrderFlow> _logger, IOrderStore _orders) : IOrderFlow
{
    public Task FailAsync(Exception ex, string correlationId)
    {
        if (ex is AsyncResponseDomainFailureException domain)
            _logger.LogError("Order flow {CorrelationId} failed remotely: {Payload}", correlationId, domain.PayloadJson);

        return _orders.MarkFailedRetriableAsync(correlationId, ex.Message);
    }
}
```

> The domain payload JSON is carried on `AsyncResponseDomainFailureException.PayloadJson`, **not** in
> the exception `Message`, so the payload (which may contain PII) does not leak into generic
> exception logs. Log `PayloadJson` deliberately, where you mean to.

> ⚠️ **Naming contract:** callback targets are persisted as interface/method *name strings* and live
> in the store for up to `RecoveryStateExpiry`. Renaming a registered callback method is a breaking
> change for in-flight recovery state — deploy renames with care (keep a forwarding method for one
> expiry window).

> ⚠️ **Binding contract:** the persisted descriptor is *name + parameter count*, so the target
> must be the only public method on the interface (base interfaces included) with that name and
> arity — an overload set such as `Run(int)` / `Run(string)` cannot be told apart on the wire.
> The expression overloads (`OnLostSubscriberResume<T>(...)`, `EnqueueWorkerAsync<T>(...)`)
> validate this **at registration**, in your stack, and throw for an ambiguous, by-ref, or
> open-generic target; the same check runs at dispatch, so the two never disagree. Targets must
> return `Task`, `ValueTask`, or `void` **synchronously** — an `async void` implementation is
> refused before it is invoked (its body would still be running when the job is acknowledged and
> its DI scope disposed, and any later exception would escape to the thread pool). The refusal
> applies to the *implementation* the interface resolves to, so it is checked on the first
> dispatch and cached per implementation type.

### Make resume callbacks re-entrant

A resume may re-trigger a flow whose step is still running remotely; resume should *re-attach*
(subscribe to the same correlation id) rather than re-execute side effects. Persist enough state to
tell the difference. And **register both callbacks** for any flow that must survive redeploys — a
failed payload with no failure callback is logged and dropped (never resumed), but dropped is still
a stuck flow. The inverse also routes conservatively: a resumable payload arriving for a
registration with only a failure callback takes the failure route (the flow cannot proceed without
a resume callback) instead of being discarded.

Recovery callbacks are **at-least-once**. Two publishers racing on the same orphaned correlation id
can each load the registration before either deletes it, and a crash between "callback invoked" and
"registration deleted" re-invokes the callback on the next publish. There is deliberately no
distributed claim step in front of the callback — resume must already be re-attach-safe, so the
extra store round-trip per recovery would buy nothing. Treat both callbacks as idempotent: key side
effects on the correlation id, not on the invocation.

**When the failure callback cannot be invoked.** The dispatcher makes up to four in-process
attempts at a failure callback for transient faults (jittered backoff of up to 250 ms, 500 ms, then
1 s between them — under 2 s in all) — on both failure routes, a response that declined to resume
and a `SetException` alike. If every attempt fails, the publish
throws `RecoveryCallbackFailedException` (carrying the correlation id and attempt count) instead
of returning normally: the registration stays armed, the broker ingress passes the exception
through untouched — no further retry, no `SetException` escalation (that would only invoke the
same failing callback again) — and the **transport redelivers the message** under its own
`MaxDeliveryAttempts`/dead-letter policy. A terminal signal is therefore never acknowledged into
a log line while the flow stays stuck: it waits in the broker until the callback's dependency
recovers or an operator replays it from the dead-letter destination. On RabbitMQ's default
`MaxDeliveryAttempts = 0` that is the same unlimited requeue any failing handler gets — configure
a cap and a `DeadLetterExchange` there as you would for worker jobs. Deterministic faults — an
unauthorized or unresolvable target, a malformed persisted descriptor (a null parameter list, a
null entry, a null or blank name), a method that no longer binds, or a persisted argument that no
longer converts to its parameter's type (for example a failure callback whose payload parameter is
the registered type, handed a response that could not be materialized as it) — are still logged
and acknowledged (redelivery cannot fix them); the kept registration is what the watchdog
surfaces. A resume callback is classified the same way: through the broker ingress, a
deterministic resume fault escalates to the failure callback at once, without the ingress's own
retry ladder.
A direct caller of `SetResponse`/`SetException` (an HTTP callback endpoint) sees the same
exception; answer the remote system with a retriable status.

**When the stored registrations cannot be read.** A recovery store holding registrations for the
correlation id that this build cannot interpret — none of them — throws
`RecoveryStateUnreadableException` rather than reporting "no registration". The broker ingress
propagates it without a `SetException` escalation (its dispatch reads the same rows first and fails
identically), so the transport redelivers or dead-letters the message for a build, or an
operator, that can resolve it. It still runs the ingress's retry ladder (4 attempts, about 1.75 s)
first: that is what paces each redelivery. A broker without a delivery cap — RabbitMQ with its
default `MaxDeliveryAttempts = 0` — keeps redelivering it at that pace until a build that can read
the registrations consumes it or an operator removes them; a capped broker dead-letters it once its
delivery count is spent.

The complete multi-step recipe built on these rules — a persisted step ledger, re-attach via the
pending correlation id, subset runs, and compensation — is documented in
[durable-flows.md](durable-flows.md).

## Why recovery routing and `Until` stay separate

Recovery classification is consulted only when nobody is listening — which is exactly when
somebody has to make the call. The two are deliberately two axes:

- **`Until` (live)** — owns completion for an *active* waiter. A failed payload is a valid response
  here: your predicate and flow code can persist details, decide to retry, or throw a rich domain
  error.
- **`OnRecovery()` (recovery)** — owns routing for a response that arrives with *no waiter
  alive*. It picks the resume callback, the failure callback, or the keep-waiting lane — the
  recovery-side mirror of `Until` skipping a non-terminal message.

They can't be merged because they answer different questions at different times with different
information available.

## The recovery watchdog + health check

The recovery watchdog is part of the engine: `AddAsyncResponse()` starts it by default, and it works
for whichever channel you registered (in-memory, Redis, NATS, PostgreSQL, SQL Server, or
MongoDB). It periodically scans the
persisted recovery state and warns about entries that are old and have no live waiter — flows that
are probably stuck. `AddAsyncResponseRecoveryCheck()` surfaces the cached findings on your health
endpoint with stats and the offending correlation ids.

```csharp
builder.Services.AddHealthChecks()
    .AddAsyncResponseRecoveryCheck();              // surface the watchdog on /readyz
```

Tune or disable it through `AsyncResponseOptions.Watchdog` — e.g. set `Watchdog.Enabled = false` in
all but one host when several hosts share one store, so its scan and warnings aren't duplicated.

The check reports at most **`Degraded`** — a stuck *business flow* must never pull a healthy
*process* out of rotation, so keep `Degraded → 200` on readiness endpoints (the ASP.NET Core
default). Alert on its warnings or the `Degraded` status: stale recovery state is your earliest
signal of stuck flows. The watchdog also feeds the `asyncresponse.recovery.*` gauges (see
[observability.md](observability.md)).

Before the first scan completes, the check reports one of three states: **idle** — this host's
watchdog deliberately does not scan (`Watchdog.Enabled = false`, or the channel registers no
`IRecoveryStateScanner`) — stays `Healthy` with `scanning: false` and a `reason` in its data, since
staleness is attested by whichever host does scan (with the in-memory channel the scanner is the
capability of whatever `IRecoveryStateStore` resolves: a custom store — or a decorator over the
built-in one — that does not implement or forward `IRecoveryStateScanner` idles the watchdog this
way); **armed** — the scan loop is running but still
inside its first-scan budget (`StartupDelay` plus twice `Interval`) — stays `Healthy` with
`scanning: true` and a `firstScanDueByUtc`; and **overdue** — armed but past that budget with
nothing published — reports `Degraded`, the same as a scan loop that stopped publishing after its
first scan. Once scanning, `Degraded` also fires when a scan could not probe waiter liveness for
some entries (a probe outage, or no `IActiveSubscriberProbe` registered): those entries' staleness
is unknown and never flagged stale by themselves, so without this the check would attest a clean
pass it never actually computed. It fires, too, when the scan found stored registrations this build
**cannot read** — malformed, an incomplete identity, or a schema version newer than this build
(expected briefly during a rolling upgrade): its callback cannot run. A response whose
registrations are all unreadable is refused and redelivered rather than acknowledged unread; one
beside readable siblings is dispatched to those, and the unreadable one is skipped with a warning.
Their count is in the `unreadable` stat and the `asyncresponse.recovery.unreadable` gauge, and the
store logs a warning for each (with its key or correlation id wherever the record still yields
one — a database row whose JSON will not parse does not). Deploy a build that can read them, or
remove them.

A scan that **fails** reports `Degraded` with the failure — and "could not read part of the store"
is a failure, not an empty result. That is a contract on `IRecoveryStateScanner.ScanAsync`: a
scanner that cannot inspect some of its storage must throw, never complete with the reachable
subset. The Redis scanner enforces it on topology: it scans connected primaries only and fails the
scan when **no primary is connected**, or when the deployment is a **cluster and any primary is
disconnected** (its slots hold registrations no other node has). Outside a cluster one connected
primary is the whole keyspace, so a failed-over deployment that still lists its old primary as
disconnected scans normally, and a disconnected replica never matters. Earlier versions skipped
disconnected servers silently: with Redis down the scan enumerated nothing, published "0
registrations, no error", and the health check went from `Degraded` to `Healthy` *because of* the
outage. Expect the opposite now — a Redis outage shows up here as
`Async-response watchdog scan failed: …` until the connection is back.

Registrations a scanner can find but cannot interpret are not absence either. Every built-in
scanner (Redis, NATS, PostgreSQL, SQL Server, MongoDB) yields everything it can read and then
throws `RecoveryStateScanUnreadableException` with the count of unexpired unreadable
registrations; the watchdog keeps the readable results — so one corrupt record never hides the
staleness of the rest — and reports the count, which degrades the check as described above. Earlier
versions skipped them: a store whose only registrations were unreadable scanned as empty and read
`Healthy`. A custom `IRecoveryStateScanner` should follow the same contract; any other caller of
`ScanAsync` sees such a scan fail.

Every cluster scan asks a connected primary for the cluster's own node table (`CLUSTER NODES` —
one small command next to a walk of the whole keyspace) and ignores any unreachable node the table
lists as a **replica** or as **owning no slots** — only a slot owner can make the scan partial. The
client's view of a node's role is only as good as its last handshake: a replica that was already
down when the process started has never been handshaken and reads as "not a replica", and used to
fail every scan of a cluster whose slot owners were all reachable. The same lookup lets a
failed-over cluster scan normally as soon as the failover has moved the old primary's slots,
instead of failing until the node rejoins. It runs on healthy scans too because it also checks the
other direction: every slot owner the table lists must be scanned — a promoted node still carrying
the client's stale "replica" flag is scanned as the slot owner it is, and a slot owner with no
connected server fails the scan. Every unknown stays a failure: a table that cannot be read while a
primary-flagged endpoint is unreachable (the ACL must allow `CLUSTER NODES` for the lookup to
help), a slot-owning primary with no connected server. An unreachable endpoint the table does not
list (a DNS name the cluster does not announce) is excused only once every slot owner the table
lists has been scanned.

The Redis scan streams: keys come from `SCAN` asynchronously, and registration blobs are read in
pipelined batches of 128 (individual `GET`s, so a cluster routes each to its shard) rather than
one awaited round trip per key — a 100,000-registration keyspace at 2 ms per round trip used to
spend over three minutes on latency alone before the first liveness probe. The NATS scan does the
same over its key-value bucket: values are read in concurrent batches of 128 while the key listing
streams (it used to await one read per key — 100,000 registrations at 5 ms took over eight
minutes), and a read that fails fails the scan. `ProbeConcurrency` still governs the probe phase
that follows either one.

## Recovery-state durability

Recovery state lives in the durable channel's store and survives a redeploy:

- **Redis** — recovery state is stored in Redis keys under `KeyPrefix`, with a per-registration
  expiry layered inside the shared per-correlation key: every registration carries its own
  `RecoveryStateExpiry` (7 days default) stamped at save time, and the key's own TTL tracks the
  longest-remaining registration — a fresh registration cannot keep a dead sibling recoverable, nor
  truncate a longer-lived one. Registration-list updates are optimistic (transaction-conditioned
  compare-and-set with retries), so concurrent registrations for one correlation id all survive.
  A committed transaction is not taken as proof that its write landed: Redis runs every queued
  command and reports each one's own error, so a save or delete whose write Redis rejects fails
  instead of reporting success (a save that said "persisted" while nothing was written, or a
  delete that said "removed" while the consumed registration stayed armed). The key's TTL is
  rounded **up** to a whole millisecond — Redis's expiry precision — so a sub-millisecond
  lifetime (a tiny `ttl`, or a surviving sibling about to lapse) is no longer sent as the invalid
  `PX 0`.
  Waiter-liveness probing asks every endpoint `PUBSUB NUMSUB` concurrently: a positive count
  anywhere is proof of a live waiter, and a zero is conclusive once the nodes that could hold the
  subscription have answered. Every endpoint flagged as a primary must have answered, except one
  that this process saw disconnect **at least 90 seconds** ago (counted from the multiplexer's
  `ConnectionFailed` for it and reset by its `ConnectionRestored` — or by finding the endpoint
  connected when that failure is handled or when a probe asks it, since StackExchange.Redis can
  deliver a blip's failure after its restore — so a blip that ended unprobed cannot shorten a
  later failover's grace; an endpoint this process never saw connected — a
  replica that never connected, a node already down when the channel was created — gets no grace
  at all): a node that went down moments ago may be the old owner of a failover whose waiters
  have not re-subscribed on the promoted node yet (StackExchange.Redis
  moves a subscription once it learns the new topology — at the latest on its `ConfigCheckSeconds`
  check, 60 s by default — and the failover itself takes the cluster node timeout or Sentinel's
  `down-after-milliseconds`), so the promoted node's zero proves nothing during that window. Past
  it, outside a cluster one answering primary is the whole answer (a failed-over deployment keeps
  listing the old primary, disconnected, until it rejoins); in a cluster the `CLUSTER NODES` table
  is read — only then, when a primary-flagged endpoint is disconnected — and the disconnected
  endpoints are excused only when every slot owner the table lists answered, so a replica that
  never connected or a seed endpoint the cluster no longer lists does not block the verdict.
  Without the table, an endpoint still flagged as a replica is not treated as a possible owner,
  even after a promotion this process has not seen yet; once the table is read, every slot owner
  it lists must answer. Anything less is **unprobeable**,
  and a lost-subscriber publish then throws for its caller to retry (the transport redelivers it)
  instead of consuming a live waiter's registration; if you raise `ConfigCheckSeconds` above the
  default, a waiter can take longer than the grace to follow a failover. Inside the grace a lost
  response survives only through the response transport's redelivery, so its redelivery budget
  should outlast the 90 s: the defaults of the NATS, PostgreSQL, SQL Server and MongoDB transports
  (5 attempts × 5 s) do not, nor do Kafka's in-process retries or Service Bus's immediate
  redeliveries, and a response still unrecovered when the budget runs out is dead-lettered.
- **NATS** — recovery state lives in a JetStream Key-Value bucket (`RecoveryBucket`), with a
  per-entry expiry layered over the bucket's `MaxAge`. Registration-list updates are
  revision-conditioned (KV compare-and-set with retries), so concurrent registrations for one
  correlation id all survive. Waiter-liveness probing over NATS Core reports presence, not
  counts: only a "no responders" answer is a definitive zero (the channel pins
  `ThrowIfNoResponders` on every request, so an application connection configured with
  `RequestReplyMode = Direct` cannot turn that answer into an ordinary reply). A request that was delivered but not
  answered inside `PresenceProbeTimeout` is reported as **unprobeable**, not dead — the consume
  loop acks a probe only when it reads it, serially, behind the previous message's `Until`
  predicate, so any predicate slower than the 2 s default would otherwise make a healthy waiter
  look gone (and let the lost-subscriber dispatcher consume its registration). The same
  asymmetry applies to a publish, the other way round: a response a subscriber received but did
  not acknowledge inside `DeliveryConfirmationTimeout` counts as delivered and never reaches
  recovery — including a response sent to a waiter whose host died without closing its connection,
  until the server drops that stale subscription at its ping timeout (see
  [configuration.md](configuration.md#channel-options)). Registrations are keyed by correlation id
  only, not by `SubjectPrefix`, so deployments sharing one NATS system need their own
  `RecoveryBucket` each. The bucket is created on first use only when it does not exist — an
  existing one is used as it is, with drift reported when it is first opened — and every completed waiter
  leaves a KV delete marker, which the watchdog scan purges once it is 30 minutes old (each purge
  bounded by the marker's own sequence, so it can never remove a registration written after it).
- **PostgreSQL** — recovery state lives in `RecoveryStateTable` (default
  `asyncresponse_recovery_state`) as one row per waiter registration. Rows expire by `expires_at`
  and are pruned opportunistically during channel operations.
- **SQL Server** — recovery state lives in `RecoveryStateTable` (default
  `asyncresponse_recovery_state`) as one row per waiter registration. Rows expire by DB clock
  (`SYSUTCDATETIME()`) and are pruned opportunistically during channel operations.
- **MongoDB** — recovery state lives in `RecoveryStateCollection` (default
  `asyncresponse_recovery_state`) as one document per waiter registration. Documents expire
  natively via a TTL index.

The carrier for propagated ambient context (trace id, principal, tenant) is persisted alongside the
recovery state as a `string`→`string` bag, so it survives the redeploy too. Don't set
`RecoveryStateExpiry` below your longest flow duration — once recovery state is gone, a late response
has nothing to route against.

On PostgreSQL, late-delivery routing is protected by two columns on the channel message table:
`acked_at` means a live waiter claimed the response, while `recovery_claimed` means the publisher
won the lost-subscriber path after the confirmation timeout. The channel updates those columns
atomically so a slow live waiter and a recovery callback cannot both process the same response.
The SQL Server and MongoDB channels implement the same claim protocol — the same pair of
columns/fields on the message table/collection, updated atomically — so the guarantee holds
across all three database channels.

Each delivery claim additionally stamps `acked_seq` from a store-side monotonic sequence
(PostgreSQL/SQL Server: a `SEQUENCE` next to the message table; MongoDB: a counter document in
`{messages}_counters`), and every waiter registration draws its own position from the same
sequence. That order separates "acked before this waiter registered" (history — a reused
correlation id must not replay its predecessor's response) from "acked to a fan-out group
including this waiter" (delivered) even when both events land on the same server-clock tick — a
tie timestamps cannot arbitrate. The arbitration is conservative-exact: exact whenever the
claim's sequence draw was not stalled across ticks, and on a stalled draw it resolves as history
(a missed delivery that recovers through the step timeout — never a replayed response; see the
shared-correlation section below). Rows acked by a build predating the column fall back to the
strict server-clock comparison.

## Wire/schema versioning

`RecoveryState`, `WorkerJobEnvelope`, and the response envelope each carry a **`SchemaVersion`**. A
reader accepts only versions explicitly supported by that build. Today that is the current version;
arbitrary lower numbers are not guessed to be compatible. The schema property is mandatory;
missing, null, and unsupported values are rejected rather than inferred. The response envelope's
`Payload` is held to the same standard: on a `Success: true` envelope an **absent** `Payload` is
rejected exactly like an explicit `"Payload": null` (`JsonException`, permanent — the delivery
faults instead of completing a waiter with `null`), and a duplicated `Payload` key binds
last-wins. The envelope's `Success` flag is mandatory as well: an envelope without it is rejected
("Success is required.") instead of reading as a message-less failure — a foreign producer must
always send it. Add historical versions only alongside a tested migration path.

Keep all hosts that share recovery or worker storage on the same wire schema during deployment.
An incompatible writer fails safe—the reader refuses the payload instead of invoking a callback or
worker with a shape it does not understand.

**Unreadable is not missing.** On the Redis and NATS channels several registrations for one
correlation id share a single stored blob, and updating one is a read-modify-write of the whole
thing. A registration a newer host wrote — one this build cannot interpret — is carried through
those rewrites untouched rather than pruned to the readable subset: dropping it would silently
delete a live sibling's recovery callback mid-rolling-upgrade. Reads still filter it out (this
build cannot dispatch it), but a *whole* stored value this build cannot parse fails the read
instead of reporting "no registration", so the terminal response is redelivered to a host that
can read it rather than acknowledged away. The durable-flow ledger applies the same rule
(`FlowStateUnreadableException`).

## Shared-correlation recovery

Multiple recoverable waiters may share one correlation id. Live delivery fans out to every active
waiter. On the database channels, a registration and another process's delivery claim can land in
the **same server-clock tick** — indistinguishable by timestamps from a finished predecessor
reusing the correlation id. The monotonic ack sequence breaks exactly that tie (see the delivery
protocol above): claims and registrations draw from one sequence, so tick-tied history and
fan-out separate by integer order. The one conservative residual: a claim whose sequence draw
stalled from an earlier tick into that exact tick resolves as history — the waiter misses that
delivery (a durable-flow step faults at its step timeout and restarts the idempotent step fresh;
a plain waiter surfaces a `TimeoutException`), which is the same verdict the previous
timestamp-only rule gave every tie and can never replay a consumed response. Rows acked by a
pre-1.0 build carry no sequence and keep that older at-most-once tie resolution. See the
`IsWithinWatermark` documentation in the DB channel source for the full reasoning. If all
waiters are lost, the recovery store keeps one registration per waiter and a
late response/exception dispatches to every stored callback for that correlation id. A waiter that
completes normally removes only its own registration, so a still-active sibling remains recoverable.

Each registration keeps its own delivery guarantee. A successful callback consumes only its own
registration. **Any transient callback failure** throws `RecoveryCallbackFailedException`,
including a single registration and a fan-out in which no callback succeeded. Ingress propagates
it without `SetException` escalation; broker redelivery and the configured dead-letter policy own
the retry. This matters when `RecoverAsync` has saved a successful response but cannot publish its
resume job: the registration survives, and redelivery publishes the missing wake-up without
repeating the completed remote step. Direct publishers must also retry this exception.

The classification examines every failed sibling. A transient failure preserves redelivery even
when a deterministic failure came first. Deterministic binding/authorization failures are not
wrapped: after partial success they are logged and retained for watchdog visibility; if all
callbacks fail deterministically, the original exception follows ingress's exception-routing
policy. A failure callback that exhausts its own retry ladder preserves its existing
`RecoveryCallbackFailedException` and attempt count.

The watchdog reports shared-correlation recovery state once per correlation id, not once per stored
waiter registration.
