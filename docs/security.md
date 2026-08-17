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

- Authenticate and authorize access to your channel store and transport broker — Redis (or Valkey /
  Dragonfly / Garnet), NATS, PostgreSQL, SQL Server, Azure Service Bus, AWS SQS, Google Pub/Sub,
  RabbitMQ, Kafka — and isolate it from untrusted networks. On the managed clouds prefer IAM/managed
  identity (SQS IAM roles, Azure Service Bus Azure AD, Pub/Sub service accounts) over static keys.
- Use a dedicated namespace per app/tenant/environment so they can't read or write each other's
  recovery state and jobs: a `KeyPrefix` (Redis), subject prefix (NATS), schema/table set (PostgreSQL /
  SQL Server), distinct queues (Azure Service Bus, SQS, RabbitMQ), or topic/consumer-group names (Kafka,
  Google Pub/Sub).
- Enable transport-level TLS and credentials end to end.

The callback authorizer below is a second layer on top of this — not a replacement for it.

## Callback authorization (opt-in allowlist)

By default there is **no authorizer**: every registered callback/worker target is invokable, exactly
as before — zero boilerplate. When you register an authorizer, only the allowed (service, method)
pairs are invokable by persisted callbacks and worker jobs; everything else is refused. This is
**type-level** authorization — you allow a service type (and optionally narrow by method name), not
per-method attributes on your flow classes.

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

Durable flows persist `IDurableFlowExecutor` methods (`ExecuteAsync`, `ResumeAsync`, `RecoverAsync`,
`FailAsync`) as every flow's resume/recover/fail targets, so the **allowlist builder admits the
executor by default** — rejecting it would break flow recovery. This is a deliberate, visible
trade-off: an attacker with write access to the recovery store or worker transport can then drive
those four methods, which is bounded to waking/failing flows by id and checkpointing a chosen
payload into a flow's pending step (`RecoverAsync`) — not arbitrary service invocation. If you do
not use durable flows, or want to gate the executor yourself, opt out:

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
| `MaxRemoteStackTraceLength` | `16384` | Length cap (chars) applied to the stack trace on **both** publish and receive, so an oversized or hostile trace can't bloat your payloads or logs. |

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

**Nor a hash of one.** A content digest reads like harmless metadata and is not: it is
deterministic, so two log entries showing the same prefix prove the two payloads were identical —
across messages, hosts, and days — and a payload drawn from a small set (a status enum, an account
id, a yes/no result) can be confirmed outright by hashing the candidates until one matches. The
correlation id and the trace id already tie an entry to its conversation, which is what the digest
was there for.

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
receiving side. If your callback/payload types live in assemblies loaded into a non-default
`AssemblyLoadContext` (plugins, add-ins, dynamically loaded modules), the default resolver may not
find them. Register them explicitly (opt-in):

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
