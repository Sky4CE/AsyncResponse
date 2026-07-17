# Trimming and Native AOT

Every AsyncResponse package is marked `IsAotCompatible=true` and builds with zero trim/AOT
analyzer warnings (IL2026/IL3050 and friends), enforced by CI's warnings-as-errors. Three layers
of proof run in CI:

1. **The analyzer gate** — all 26 packages compile warning-free with the trim/AOT analyzers on.
2. **A Native AOT smoke app** ([samples/AsyncResponse.AotSmoke](../samples/AsyncResponse.AotSmoke))
   publishes fully trimmed on every run and exercises the envelope round-trip, a durable flow,
   worker dispatch, and the JSON registration seam described below.
3. **The full integration suite against Native AOT SUTs** — the sample app publishes as Native
   AOT and the Aspire harness swaps SUT resources from the JIT project to the native binary,
   running the *same* integration tests as the JIT pass (`integration-tests-aot` in CI). SUTs go
   native wherever the whole driver stack is AOT-capable (see the vendor matrix below for
   exactly which, and why the rest stay JIT). Set `ASYNCRESPONSE_ITEST_SUT=aot` and
   `ASYNCRESPONSE_ITEST_SUT_PATH=<published binary>` to run it locally.

The first two layers also run in every **local** full test run:
`NativeAotPublishGateTests` (in the integration test project, no Docker needed) publishes the
sample with `-warnaserror` and then boots the native binary and drives a request/response round
trip plus a durable flow. It exists because two defect classes are invisible to build + unit
runs — ILC-only trim errors (e.g. anonymous-type LINQ projections lower to the trim-unsafe
`Expression.New` overload that Roslyn's analyzer never flags) and runtime-only AOT breaks — and
it makes them fail in the IDE instead of the pipeline. Set `ASYNCRESPONSE_SKIP_AOT_GATE=1` to
skip it while iterating.

## What you do in a trimmed / Native AOT app

Two startup lines, and one registration per flow:

```csharp
// 1) Register JSON metadata for the types AsyncResponse serializes on your behalf:
//    response payloads, flow inputs, step results, and values-bag entries.
AsyncResponseJsonSerialization.RegisterResolver(MyAppJsonContext.Default);

// 2) Register each durable flow so the executor never needs its persisted type name:
builder.Services.AddAsyncResponse()
    .WithRedisChannel(...)
    .WithRabbitMqTransport(...)
    .WithRedisDurableFlows(...)
    .WithDurableFlow<ProvisioningFlow, ProvisionRequest>();   // per flow
```

where `MyAppJsonContext` is an ordinary source-generated context listing your types:

```csharp
[JsonSerializable(typeof(ProvisionRequest))]
[JsonSerializable(typeof(ProvisioningResult))]
internal sealed partial class MyAppJsonContext : JsonSerializerContext;
```

Non-trimmed (JIT) apps need neither line: everything falls back to reflection-based
`System.Text.Json` exactly as before, and `WithDurableFlow` is optional (flows then resolve by
their persisted type name through DI, as they always have).

## How it works

- **Library wire types** (envelopes, `FlowState`, `RecoveryState`, `WorkerJobEnvelope`, callback
  descriptors) use source-generated metadata compiled into the packages. The wire format is
  byte-identical to previous releases; the schema-version stamps are unchanged.
- **Your payload types** resolve through a chain: the library's own metadata → resolvers you
  register via `AsyncResponseJsonSerialization.RegisterResolver(...)` (process-wide and additive,
  like `AsyncResponseTypeResolution`) → the runtime's reflection resolver when the app has it
  enabled (`JsonSerializer.IsReflectionEnabledByDefault`). In a trimmed app the reflection link is
  removed by the feature switch, and an unregistered type fails with an error naming the type and
  the registration call to make.
- **Registered flows** execute through a statically-typed route (no `MakeGenericType`, no
  `MethodInfo.Invoke`); unregistered flows fall back to the historical reflection path, which in a
  trimmed app fails closed with guidance to add `WithDurableFlow`.

## The annotated dynamic surface

Persisted callbacks are resolved by *name* when they fire — possibly in a different deployment —
so some APIs are inherently reflection-shaped. The expression-based registration APIs
(`EnqueueWorkerAsync<TService>(svc => ...)`, `OnLostSubscriberResume<TService>(svc => ...)`)
carry `DynamicallyAccessedMembers` annotations that root the target service's public methods in
any app that compiles the registration, so they work under trimming with no extra steps. The
raw-descriptor overloads (`EnqueueWorkerAsync(ReflectionCallDto)`,
`OnLostSubscriberResume(ReflectionCallDto)`) and `AsyncResponseTypeResolution.RegisterAssembly`
are annotated `RequiresUnreferencedCode`: the analyzer warns at your callsite because nothing
statically ties the string names to code, and it is on the app to keep those targets un-trimmed
(e.g. `[DynamicDependency]`) or to prefer the expression overloads.

Two operational caveats under Native AOT:

- A worker/recovery deployment that only *executes* callbacks enqueued by a different app must
  itself reference the callback target types statically (registering them in DI is the natural
  way); type names cannot resurrect trimmed code.
- Callback methods returning `ValueTask<T>` with a *value-type* `T` need that exact instantiation
  compiled into the app; `Task`/`Task<T>`/`ValueTask` returns have no such constraint. Failures
  are loud, not silent.

One behavioral note: the durable channels' fail-fast check that a recovery-enabled payload
actually overrides `ShouldResumeOnRecovery` relies on `Type.GetInterfaceMap`, which Native AOT
cannot compute for interfaces with default implementations. Implicit (public-method)
implementations are still detected; for the rare explicit-implementation shape the check fails
*open* under Native AOT — a payload that silently inherits the conservative default is caught at
development time on JIT rather than at runtime on AOT.

One serialization note: the recovery health check reports its `Data` payload through the named
types `AsyncResponseRecoveryStats` and `AsyncResponseStaleRecoveryEntry` (JSON property names are
pinned on the types). An AOT app that renders health reports as JSON registers those two types
(plus `List<AsyncResponseStaleRecoveryEntry>`) in its own context — the sample's
`SampleHttpJsonContext` shows the pattern.

## Vendor SDK compatibility

The packages themselves are trim/AOT-clean; what limits a *fully* native deployment is the broker
driver underneath. Current state, as exercised by the AOT integration run:

| Status | Provider (driver) | Detail |
| --- | --- | --- |
| Verified natively in CI | NATS (NATS.Net), PostgreSQL (Npgsql) | Channel + transport pairs run as Native AOT SUTs against the real servers. |
| JIT-only today (driver defect, observed empirically in this harness) | Redis (StackExchange.Redis 3.x), SQL Server (Microsoft.Data.SqlClient), MongoDB (MongoDB.Driver) | SE.Redis's net8+ `Delegates` helper reads CoreCLR's `MulticastDelegate._invocationList` via `UnsafeAccessor`; that private field does not exist in the Native AOT runtime, so pub/sub completion throws `MissingFieldException` (no upstream guard as of 3.0.17). SqlClient fails the TDS pre-login handshake in a native binary. MongoDB.Driver serializes BSON through reflection. All three run as JIT SUTs in the AOT pass, so their tests still execute. |
| Not yet verified natively (harness pairing) | RabbitMQ, Kafka, SQS, Google Pub/Sub, Azure Service Bus, Redis Streams transport | These transport SUTs pair with the Redis *channel*, so the SE.Redis defect keeps them JIT for now. The transports themselves carry no known AOT blockers (librdkafka is native code; AWS SDK v4, gRPC/protobuf, RabbitMQ.Client v7 and Azure.Messaging.ServiceBus are trim-friendly); a channel-remap mode (PostgreSQL channel under each broker transport) can verify them natively before the SE.Redis fix lands. |

The AOT smoke app additionally proves the in-memory channel/transport and Sqlite
(Microsoft.Data.Sqlite) durable-flow store natively on every CI run.

Vendor SDKs without trim annotations produce publish-time rollup warnings (IL2104/IL3053); the
sample suppresses exactly those two codes, while its own code stays gated by the Roslyn analyzers
on every build.

## Verifying your own app

`dotnet publish /p:PublishAot=true` and run it — or, for a fast signal without ILC, run your app
with the trimmed-JSON semantics enabled: `dotnet run -p:JsonSerializerIsReflectionEnabledByDefault=false`.
Any payload type you forgot to register fails immediately with the register-a-context error.
