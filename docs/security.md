# Security & hardening

[← Back to README](../README.md)

AsyncResponse invokes serializable method descriptors (recovery callbacks and worker jobs) that are
**persisted in your store and resolved through DI by whatever process reads them** — possibly a
different deployment. That makes the store a trust boundary. The controls below are defense in
depth; the callback allowlist and custom type resolution are opt-in.

## Secure your store and transport first

The single most important control is the obvious one: **a persisted callback/worker descriptor is
only as trustworthy as the store and transport it travels through.** Recovery state and worker jobs
name a service interface and method that the receiving process resolves from its DI container and
invokes, so anyone who can write to the recovery store or worker stream can ask a consuming process
to invoke any registered (service, method) pair with attacker-influenced arguments.

What bounds that reach:

- **Loaded assemblies only.** A persisted type name resolves only against assemblies already loaded
  into the process; a name never makes the process load a file its author supplies. One exception
  is outside the library's control: the framework facades nearly every process has loaded
  (`netstandard`, `mscorlib`, `System.Runtime`) forward to most of the framework, and asking one for
  a forwarded name makes the runtime load the defining framework assembly. The resolution that
  caused the load refuses the result, but the assembly stays loaded, so a later resolution (a
  redelivery) finds it — the reach extends to the framework's own assemblies, never beyond. Such a
  type still has to pass the caller's own gate (the payload marker interface, a DI registration, the
  flow contract) before anything uses it.
- **Bounded name shape.** Before any cache, resolver, or callback authorization, a persisted name is
  held to at most 4,096 characters, 16 levels of `[` nesting and 64 `[` in total, with by-ref (`&`)
  and pointer (`*`) decorations refused. The runtime's type-name parser recurses per generic
  argument, so a few hundred kilobytes of `A\`1[[A\`1[[…` would overflow the parsing thread's stack
  — an uncatchable `StackOverflowException` that would take down every worker the message was
  redelivered to. The recovery path resolves the payload's type name before any callback is chosen,
  so no authorizer could stand in front of it. A name outside the limits is simply unresolvable,
  like a renamed type.
- **Payload names are judged by their definition first.** Resolving a generic instantiation or an
  array builds a new runtime type, which the runtime keeps for the life of the process (outside the
  managed heap). A recovery row's payload type name is therefore refused *before* anything is
  constructed when it names an array (never a payload) or an instantiation whose loaded generic
  definition does not implement `IAsyncResponsePayload` — so a store writer cannot grow the process
  by naming distinct instantiations of arbitrary loaded generics (``Dictionary`2`` over pairs of
  loaded types). Residual: the type *arguments* of a genuine generic payload (`Envelope<T>`) are
  not constrained — naming distinct arguments to it still constructs closed types, and its `T`
  members are materialized from the stored JSON before any callback authorization. Prefer
  non-generic payload types where the recovery store is shared with less-trusted writers.

What you must do:

- Authenticate and authorize access to your channel store and transport broker — Redis (or Valkey /
  Dragonfly / Garnet), NATS, PostgreSQL, SQL Server, MongoDB, Azure Service Bus, AWS SQS, Google
  Pub/Sub, RabbitMQ, Kafka — and isolate it from untrusted networks. On the managed clouds prefer IAM/managed
  identity (SQS IAM roles, Azure Service Bus Azure AD, Pub/Sub service accounts) over static keys.
- Use a dedicated namespace per app/tenant/environment so they can't read or write each other's
  recovery state and jobs: a `KeyPrefix` (Redis), subject prefix and recovery bucket (NATS),
  schema/table set (PostgreSQL / SQL Server), database/collection set (MongoDB), distinct queues
  (Azure Service Bus, SQS, RabbitMQ), or topic/consumer-group names (Kafka, Google Pub/Sub).
- Enable transport-level TLS and credentials end to end.
- Mind local conveniences too: the repository's `docker-compose.yml` binds its Redis to
  `127.0.0.1` on purpose — an unqualified `6379:6379` publishes on every host interface, and the
  official image runs with protected mode off, so a development Redis reachable from the network
  segment is an unauthenticated write path into recovery descriptors and response envelopes for
  any application pointed at it. Anything that must be reachable from another machine needs
  `requirepass`/ACLs and network isolation, never a wider bind.

The callback authorizer below is a second layer on top of this — not a replacement for it.

## Callback authorization (opt-in allowlist)

By default there is **no authorizer**: every DI-registered callback/worker target is invokable. When
you register one, only the allowed (service, method) pairs are invokable by persisted callbacks and
worker jobs; everything else is refused rather than executed. Authorization is **type-level** — you
allow a service type, or supply a predicate over service and method names; it does **not** read
per-method attributes. A type allowance admits only the type's ordinary methods: property and event
accessors (`get_`/`set_`/`add_`/`remove_`), `object`'s own members, and disposal (the members of
`IDisposable`/`IAsyncDisposable` and any parameterless `Dispose`/`DisposeAsync`) are never callback
candidates, so a descriptor aimed at `set_ApiKey` or `Dispose` on an allowed DI singleton is refused.
Every other public method is — including those an allowed interface inherits from its base
interfaces, framework ones too, and, for an allowed class, those it inherits from its base classes.
Allow narrow interfaces rather than implementation classes.

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

Configure authorization in **one** `AuthorizeCallbacks` call. Every consumer resolves a single
authorizer — the last one registered — so a second call (a module's allowlist followed by the
application's) would silently discard the first, and its targets would be refused; the host
therefore fails to start when more than one authorizer is registered. Combine the allowances
(`a => a.Allow<IModuleService>().Allow<IAppService>()`), or merge the rules in one custom
authorizer.

Even if a malicious or corrupted entry reaches the store, only the allowlisted surface can be
driven. A refused worker job is counted as `rejected` on `asyncresponse.worker.jobs` and thrown, so
the transport redelivers it (a replica configured to allow the target can run it) and then
dead-letters it; a refused recovery callback is a deterministic fault, logged and acknowledged with
its registration kept for the watchdog (see [recovery.md](recovery.md#when-the-failure-callback-cannot-be-invoked)).

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
Register it as a singleton, as `AuthorizeCallbacks` does: with a broker transport the ingress (a
singleton) takes the authorizer directly, so a scoped authorizer fails scope validation
(`ValidateScopes`/`ValidateOnBuild`).

## Remote stack-trace policy

When a remote side fails technically (`SetException`), the exception's stack trace can travel on the
wire and is surfaced on the receiving side via `Exception.Data["RemoteStackTrace"]`. Two options on
every bundled channel — in-memory, Redis, NATS, PostgreSQL, SQL Server, MongoDB — bound this:

| Option | Default | Effect |
|---|---|---|
| `IncludeRemoteStackTrace` | `true` | When `false`, the remote stack trace is omitted from the wire entirely. |
| `MaxRemoteStackTraceLength` | `16384` | Length cap (chars) applied on **both** publish and receive, so an oversized or hostile trace can't bloat your payloads or logs. `0` disables the cap; a negative value is rejected at startup. |

```csharp
.WithRedisChannel(options =>
{
    options.IncludeRemoteStackTrace = false;        // omit remote stack traces on this channel
    // or keep them but cap harder:
    // options.MaxRemoteStackTraceLength = 4096;
})
```

Related: a domain failure's payload JSON is carried on
`AsyncResponseDomainFailureException.PayloadJson`, **not** in its `Message`, so a payload (which may
contain PII) does not leak into generic exception logs that print `ex.Message`. Log `PayloadJson`
deliberately, where you intend to.

### The library never logs a message body

At any log level, `Debug` included. This matters most at the ingress, which every inbound response
and worker job passes through: a worker envelope carries the job's arguments and whatever the
context propagators captured (tenant, auth, trace baggage). What is logged instead is a size plus
routing metadata that is safe by construction — the correlation id, the reply target, and the target
service and method.

**Nor a hash of one.** A content digest is deterministic: equal digests prove two payloads identical
across messages, hosts and days, and a payload drawn from a small set (a status enum, an account id,
a yes/no result) can be confirmed by hashing the candidates. The correlation id and trace id already
tie a log entry to its conversation.

**Nor the JSON reader's own message.** A `System.Text.Json` failure is not bounded metadata: it
appends `Path: $.<name>` built from *inbound* property names — dictionary keys read straight off the
wire, such as a worker envelope's propagated `Context` — and for a malformed literal it quotes raw
body characters. The library rebuilds such failures from position only (line, byte position, size)
and drops the reader's message and path rather than chaining them. `NotSupportedException` from the
reader (e.g. a missing polymorphic discriminator, which also appends inbound keys) is scrubbed the
same way, keeping only size and a safe failure category. Metadata-resolution errors keep their
configuration guidance, including the library's own register-your-type message raised mid-read,
which names the type and never a byte of the body.

This applies to every reader that materializes a body the library did not write itself:

- the ingress, for both the log line and the exception it republishes to the waiter through
  `SetException`;
- the **second** pass that converts an already-parsed worker-job argument or recovery payload into
  the callback's parameter type (it walks the payload's own property names and keys);
- the Redis, NATS, and database channels' response readers, whose parse failure is logged and handed
  to the waiter as `InvalidDataException`;
- the in-memory channel's typed delivery: each waiter re-materializes the published payload from its
  wire bytes, and a payload that does not fit the waiter's type faults it with the same body-free
  `InvalidDataException`, so the offending key never reaches the waiter's exception or the wait
  span's error status;
- the durable-flow ledger reader, for a stored ledger and for the initial state a start job carries
  (`FlowStateUnreadableException` chains the position-only failure);
- the recovery-state readers (Redis, NATS, PostgreSQL, SQL Server, MongoDB), on delivery and during
  the watchdog scan — a registration's `Context` carries the same tenant and auth keys.

**But not our own diagnostics.** `System.Text.Json`'s messages can quote the body, so they are
dropped; the envelope reader's own contract violations — `SchemaVersion is required.`,
`Success is required.`, `Payload is null or absent on a Success envelope…`,
`Success must be a boolean.` — name only wire-contract property names and are kept verbatim. They
diagnose the commonest malformed-envelope cause, a foreign or mismatched producer writing to the
response channel. Such a failure stays a plain `JsonException`, which the ingress treats as permanent
(never retried).

### The sample's test-only routes are gated

The sample application (the integration suite's system under test) exposes unauthenticated
test/demo routes:

- **mutation** — `/seed-recovery`, `DELETE /test/recovery/{correlationId}`, and `POST /test/reset`
  (erases every recovery registration the scanner can see);
- **simulation and injection** — `/arm`, `/crash` (drops every local subscription on the shared
  channel; with Redis, `UnsubscribeAll` on the shared multiplexer), `/publish` and `/emit-response`
  (inject a response or exception for any correlation id), `/lost-subscriber-flow` (all three);
- **observability** — `/calls` (recorded call data), `GET /durable-flow/{flowId}` (a run's full
  ledger, input JSON included) and `POST /durable-flow/{flowId}/resume` (an operator kick).

They are mapped only in the Development environment or when `Sample:EnableTestEndpoints=true` (set
by the integration AppHost, the in-process test factory, the load-test launcher, and the Native AOT
gate). A Production instance answers 404 for every one and logs that they are disabled; an
integration test pins the exact Production route inventory. If you fork the sample into a service,
keep them behind that switch — and put flow reads/resumes behind real authorization and ownership
checks.

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

Correlation ids follow one **portable contract** on every channel: at most 400 UTF-16 code units
(`AsyncResponseChannelOptions.MaxCorrelationIdLength`), no leading or trailing space, well-formed
UTF-16, and no control characters. An id outside it would be truncated or rejected at its first
database write, or — space-padded — match its trimmed form in SQL Server and surface at another
waiter. How each case is handled:

| Id | Wait (`For<T>(id)`) | `SetResponse`/`SetException` | Broker ingress |
|---|---|---|---|
| blank / whitespace | throws `ArgumentNullException` | no-op: logged and skipped | acknowledged without routing |
| non-blank, outside the contract | throws `ArgumentException` | throws `ArgumentException` | acknowledged without routing |

The ingress acknowledges rather than throws because such a message can never route, so redelivery
would loop; each drop is logged at error level and counted on
`asyncresponse.ingress.unroutable_responses`.

## Type resolution for plugins / AssemblyLoadContext

Recovery callbacks and worker payloads are persisted as **type name strings** and resolved on the
receiving side — by default against the assemblies **already loaded** into the process, every
component of the name included: a generic argument naming an unloaded assembly makes the whole name
unresolved rather than loading it. "Loaded" spans every `AssemblyLoadContext`: a plugin's types
resolve once its context has loaded them, and a name defined in several loaded assemblies (the same
plugin in two contexts) resolves to the first one loaded. If your callback/payload types may not be
loaded yet when a persisted name arrives (plugins, add-ins, dynamically loaded modules), register
them explicitly. Registered resolvers are consulted only when the default scan finds nothing, so
they cannot pick between copies the scan already sees:

```csharp
using AsyncResponse;

// register a whole assembly's types for resolution…
IDisposable assemblyRegistration =
    AsyncResponseTypeResolution.RegisterAssembly(typeof(MyPlugin.IPluginFlow).Assembly);

// …or supply a custom resolver function:
IDisposable resolverRegistration = AsyncResponseTypeResolution.RegisterResolver(name =>
    PluginCatalog.TryFind(name, out var t) ? t : null);
```

**A registered assembly may load what the name asks for.** `RegisterAssembly` resolves with the
runtime's own parser (`Assembly.GetType`), which is not confined to loaded assemblies: a generic
argument naming an unloaded assembly makes the registered assembly's `AssemblyLoadContext` load it
(from the plugin's dependencies or the application's trusted platform assemblies) — and that name is
written by whoever can write the recovery store or worker stream. Only files the application or
plugin deploys can load this way, never one the name's author supplies, and the resolved type must
still pass the payload marker gate, the DI registration, and the callback authorizer. If even the
load is unacceptable, register a `RegisterResolver` delegate that answers only the names you expect
(as `PluginCatalog` above).

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

Names that still can't be resolved are counted on `asyncresponse.type_resolution.unresolved`
(`kind` = `service`|`payload`; see [observability.md](observability.md#instruments)), so an
unresolved plugin type is an observable signal rather than a silent drop. A registered resolver that
**throws** is skipped and counted under `kind = resolver`, once per throw, even when a later resolver
answers. Unresolvable names are negatively cached (bounded); the cache is invalidated when an
assembly loads or a resolver registers, so a late plugin is picked up immediately while a poisoned or
renamed name stops costing a full assembly scan per redelivery.

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
