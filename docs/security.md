# Security & hardening

[← Back to README](../README.md)

AsyncResponse invokes serializable method descriptors (recovery callbacks and worker jobs) that are
**persisted in your store and resolved through DI by whatever process reads them** — possibly a
different deployment. That makes the store a trust boundary. The features below are opt-in
defense-in-depth; defaults preserve existing behavior.

## Secure your store and transport first

The single most important control is the obvious one: **a persisted callback/worker descriptor is
only as trustworthy as the store and transport it travels through.** Recovery state and worker jobs
name a service interface and method that the receiving process will resolve from its DI container and
invoke. Anyone who can write to the recovery store or worker stream can therefore ask a consuming
process to invoke any registered (service, method) pair with attacker-influenced arguments.
Persisted type names are resolved only against assemblies already loaded into the process — a
name never makes the process load one — so that reach is bounded to what the consuming process
has itself loaded.

- Authenticate and authorize access to your channel store and transport broker — Redis (or Valkey /
  Dragonfly / Garnet), NATS, PostgreSQL, SQL Server, Azure Service Bus, AWS SQS, Google Pub/Sub,
  RabbitMQ, Kafka — and isolate it from untrusted networks. On the managed clouds prefer IAM/managed
  identity (SQS IAM roles, Azure Service Bus Azure AD, Pub/Sub service accounts) over static keys.
- Use a dedicated namespace per app/tenant/environment so they can't read or write each other's
  recovery state and jobs: a `KeyPrefix` (Redis), subject prefix (NATS), schema/table set (PostgreSQL /
  SQL Server), distinct queues (Azure Service Bus, SQS, RabbitMQ), or topic/consumer-group names (Kafka,
  Google Pub/Sub).
- Enable transport-level TLS and credentials end to end.
- Mind local conveniences too: the repository's `docker-compose.yml` binds its Redis to
  `127.0.0.1` on purpose — an unqualified `6379:6379` publishes on every host interface, and the
  official image runs with protected mode off, so a development Redis reachable from the network
  segment is an unauthenticated write path into recovery descriptors and response envelopes for
  any application pointed at it. Anything that must be reachable from another machine needs
  `requirepass`/ACLs and network isolation, never a wider bind.

The callback authorizer below is a second layer on top of this — not a replacement for it.

## Callback authorization (opt-in allowlist)

By default there is **no authorizer**: every registered callback/worker target is invokable, exactly
as before — zero boilerplate. When you register an authorizer, only the allowed (service, method)
pairs are invokable by persisted callbacks and worker jobs; everything else is refused. This is
**type-level** authorization — you allow a service type (and optionally narrow by method name), not
per-method attributes on your flow classes. A type-level allowance admits only the type's ordinary
methods: property and event accessors (`get_`/`set_`/`add_`/`remove_`) and `object`'s own members
are never callback candidates, so a descriptor aimed at `set_ApiKey` on an allowed DI singleton is
refused rather than executed.

```csharp
builder.Services.AddAsyncResponse()
    .AuthorizeCallbacks(a => a.Allow<IOrderFlow>())   // only IOrderFlow methods are invokable
    .WithRedisChannel()
    .WithRedisTransport(options => options.KeyPrefix = "orders")
    .WithInMemoryDurableFlows();
```

The builder offers several `Allow` shapes, plus a fully custom authorizer:

```csharp
.AuthorizeCallbacks(a =>
{
    a.Allow<IOrderFlow>();                                  // by generic type
    a.Allow(typeof(IPaymentFlow));                          // by Type
    a.Allow("MyApp.Flows.IShippingFlow");                   // by service full name
    a.Allow((serviceFullName, methodName) =>                // by predicate
        serviceFullName.StartsWith("MyApp.Flows.") && methodName.EndsWith("Async"));
})

// …or supply your own:
.AuthorizeCallbacks(new MyCustomAuthorizer());              // IAsyncResponseCallbackAuthorizer
```

When an incoming descriptor names a (service, method) pair the authorizer rejects, the invocation is
refused rather than executed. Use this as defense-in-depth: even if a malicious or corrupted entry
reaches the store, only an explicitly allowlisted surface can be driven.

### The durable-flow executor and the allowlist

Durable flows persist `IDurableFlowExecutor` methods (`CreateAndExecuteAsync`, `ExecuteAsync`,
`ResumeAsync`, `RecoverAsync`, `FailAsync`) as every flow's start/resume/recover/fail targets, so
the **allowlist builder admits the executor by default** — rejecting it would break flow starts and
recovery. This is a deliberate, visible trade-off: an attacker with write access to the recovery
store or worker transport can then drive those methods, which is bounded to waking/failing flows
by id, checkpointing a chosen payload into a flow's pending step (`RecoverAsync`), and starting a
run of a *registered* flow type with a chosen input (`CreateAndExecuteAsync` — the same thing a
worker-transport writer could already do by publishing any worker job) — not arbitrary service
invocation. If you do not use durable flows, or want to gate the executor yourself, opt out:

```csharp
.AuthorizeCallbacks(a =>
{
    a.AllowDurableFlowExecutor = false;   // executor targets now need explicit allowance
    a.Allow<IOrderFlow>();
})
```

A **custom** `IAsyncResponseCallbackAuthorizer` gets no implicit entries: when durable flows are
enabled it must allow `IDurableFlowExecutor` itself, or flow recovery callbacks will be refused.

> Default = no authorizer = allow all = unchanged behavior. The authorizer is type-level; it does
> **not** read per-method attributes.

## Remote stack-trace policy

When a remote side fails technically (`SetException`), the exception's stack trace can travel on the
wire and is surfaced on the receiving side via `Exception.Data["RemoteStackTrace"]`. Two channel
options (on the durable channels — Redis, NATS, PostgreSQL, SQL Server, MongoDB) bound this:

| Option | Default | Effect |
|---|---|---|
| `IncludeRemoteStackTrace` | `true` | When `false`, the remote stack trace is omitted from the wire entirely. |
| `MaxRemoteStackTraceLength` | `16384` | Length cap (chars) applied to the stack trace on **both** publish and receive, so an oversized or hostile trace can't bloat your payloads or logs. `0` disables the cap; a negative value is rejected at startup by every channel, the in-memory one included. |

```csharp
.WithRedisChannel(options =>
{
    options.IncludeRemoteStackTrace = false;        // omit remote stack traces on this channel
    // or keep them but cap harder:
    // options.MaxRemoteStackTraceLength = 4096;
})
```

Related: the domain payload JSON is **no longer embedded in
`AsyncResponseDomainFailureException.Message`** — it stays on the `PayloadJson` property — so a
payload (which may contain PII) does not leak into generic exception logs that print `ex.Message`.
Log `PayloadJson` deliberately, where you intend to.

### The library never logs a message body

At every log level, including `Debug`. This matters most at the ingress, where every inbound
response and every worker job passes through: a worker envelope carries the job's arguments and
whatever the context propagators captured (tenant, auth, trace baggage), so logging it whole would
put all of that in the application log the moment someone turned Debug on to diagnose something
else. What is logged instead is a size, plus routing metadata that is safe by construction: the
correlation id, the reply target, and the target service and method.

**Nor the JSON reader's own message.** A `System.Text.Json` parse failure looks like bounded
metadata and is not: it appends `Path: $.<name>` built from the *inbound* property names —
dictionary keys read straight off the wire, such as a worker envelope's propagated `Context` — and
for a malformed literal it quotes several raw body characters. Both the message and the chained
inner exception the ingress logs (and, on the response path, republishes to the waiter through
`SetException`) are rebuilt from position only: line, byte position, and size. The reader's own
message and path are dropped, not chained. The same scrubbing covers the **second** reader pass —
converting an already-parsed worker-job argument or recovery payload into the callback's
parameter type, which walks the payload's own property names and dictionary keys — because the
exception that escapes it is logged by the worker ingress too. The same contract covers every
reader that materializes a body the library did not write itself: the Redis, NATS, and database
(PostgreSQL, SQL Server, MongoDB) channels' response readers, whose parse failure is both logged
and handed to the waiter (as `InvalidDataException`), and the durable-flow ledger reader, whether
it reads a stored ledger or the initial state a start job carries — the
`FlowStateUnreadableException` it raises chains the rebuilt, position-only failure, never the
reader's own.

**But not our own diagnostics.** The distinction is who wrote the message. `System.Text.Json`'s
messages quote the body, so they are dropped; the envelope reader's own contract violations —
`SchemaVersion is required.`, `Payload is null or absent on a Success envelope`, `Success must be
a boolean.` — name only the wire contract's own property names and are preserved verbatim. They
are the primary operator diagnosis for the commonest malformed-envelope cause in production, a
foreign or mismatched producer writing to the response channel, and scrubbing them to "failed at
line 0, byte position 2" would cost the diagnosis while protecting nothing. Such a failure stays
a plain `JsonException` (the ingress classifies it as permanent, so it is never retried).

**Nor a hash of one.** A content digest reads like harmless metadata and is not: it is
deterministic, so two log entries showing the same prefix prove the two payloads were identical —
across messages, hosts, and days — and a payload drawn from a small set (a status enum, an account
id, a yes/no result) can be confirmed outright by hashing the candidates until one matches. The
correlation id and the trace id already tie an entry to its conversation, which is what the digest
was there for.

### The sample's test-only routes are gated

The sample application (the integration suite's system under test) exposes unauthenticated routes
that exist for tests and demos, in two groups: the **mutation** routes — `/seed-recovery`,
`DELETE /test/recovery/{correlationId}`, and `POST /test/reset`, which erases every recovery
registration the scanner can see — and the **simulation, injection, and observability** routes —
`/arm`, `/crash` (drops every local subscription on the shared channel; with Redis it calls
`UnsubscribeAll` on the shared multiplexer), `/publish` and `/emit-response` (inject a response or
exception for any correlation id), `/lost-subscriber-flow` (composes all three), `/calls` (recorded
call data), and `GET /durable-flow/{flowId}` / `POST /durable-flow/{flowId}/resume` (a run's full
ledger, input JSON included, and an operator kick). All of them are mapped only in the Development
environment or when `Sample:EnableTestEndpoints=true` is configured (the integration AppHost, the
in-process test factory, the load-test launcher, and the Native AOT gate set it); a Production
instance answers 404 for every one and logs that they are disabled, and an integration test pins
the exact Production route inventory. If you fork the sample into a service, keep them behind that
switch — and put flow reads/resumes behind real authorization and ownership checks — rather than on
a shared backend.

## Explicit correlation id

`IAsyncResponsePublisher.SetResponse`/`SetException` take the correlation id as a **required**
parameter — there is no ambient fallback, so a publish can never silently target whatever
`AsyncResponseContext.CorrelationId` happens to be set in a nested flow. Inside a wait trigger use the
`context.CorrelationId` you are handed; elsewhere pass the id you already hold (or
`AsyncResponseContext.CorrelationId` explicitly if that is genuinely what you want):

```csharp
await asyncResponse
    .For<OrderResult>()
    .WaitAsync(context => gateway.SubmitAsync(orderId, context.CorrelationId));

// Direct publish from an in-process producer:
await publisher.SetResponse(result, correlationId);
```

A blank/whitespace correlation id (e.g. from a malformed broker header) is a no-op: the publish is
logged and skipped rather than throwing, so bad input cannot crash ingress.

## Type resolution for plugins / AssemblyLoadContext

Recovery callbacks and worker payloads are persisted as **type name strings** and resolved on the
receiving side — against the assemblies **already loaded** into the process only, every component
of the name included: a generic argument naming an assembly the process has not loaded resolves
the whole name to unresolved rather than forcing that assembly to load. If your callback/payload
types live in assemblies loaded into a non-default `AssemblyLoadContext` (plugins, add-ins,
dynamically loaded modules), the default resolver may not find them. Register them explicitly
(opt-in):

```csharp
using AsyncResponse;

// register a whole assembly's types for resolution…
IDisposable assemblyRegistration =
    AsyncResponseTypeResolution.RegisterAssembly(typeof(MyPlugin.IPluginFlow).Assembly);

// …or supply a custom resolver function:
IDisposable resolverRegistration = AsyncResponseTypeResolution.RegisterResolver(name =>
    PluginCatalog.TryFind(name, out var t) ? t : null);
```

**Keep the returned handle and dispose it when the plugin goes away.** A registration lives in a
process-wide list, and `RegisterAssembly` holds the assembly strongly — so an undisposed
registration keeps the plugin's `AssemblyLoadContext` alive for the life of the process, and a
resolver you meant to replace keeps answering. Disposal removes the registration and drops the
resolved-type caches it fed, so a revoked alias stops resolving to its old type:

```csharp
// Own the registration for exactly as long as the plugin is loaded.
using (AsyncResponseTypeResolution.RegisterAssembly(pluginAssembly))
{
    await RunPluginWorkloadAsync();
}   // registration removed here — the context can now unload

context.Unload();
```

Type names that still can't be resolved are surfaced via the
`asyncresponse.type_resolution.unresolved` metric (tag `kind = service|payload`) — see
[observability.md](observability.md) — so an unresolved plugin type shows up as an observable signal
rather than a silent drop. Unresolvable names are negatively cached (bounded), and the cache is
invalidated automatically when a new assembly loads or a resolver registers — a plugin that
registers late is picked up immediately, while a poisoned/renamed type name stops costing a full
assembly scan per redelivery.

### Unloadable (collectible) plugin contexts

If your plugins load into a **collectible** `AssemblyLoadContext` and you expect `Unload()` to
actually reclaim them, keep the types AsyncResponse touches — payload types and callback service
**interfaces** — in a shared, non-collectible **contracts assembly**, and load only the plugin's
*implementations* into the collectible context. This is the standard .NET plugin architecture, and
under it unloading works: AsyncResponse's own resolution caches additionally skip any type from a
collectible assembly (resolving it per call instead), so the library never pins your context.
What the library cannot control is `System.Text.Json` itself: serializing or deserializing a type
that *lives in* a collectible assembly pins that context through runtime-internal caches
(regardless of `JsonSerializerOptions` instance, verified through .NET 10) — which is exactly why
payload contracts belong in the non-collectible contracts assembly.
