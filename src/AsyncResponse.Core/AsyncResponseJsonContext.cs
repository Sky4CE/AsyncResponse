using System.Text.Json;
using System.Text.Json.Serialization;

namespace AsyncResponse;

/// <summary>
/// Source-generated JSON metadata for every library-owned wire type, so none of the internal
/// serialization requires reflection (trim- and AOT-safe). User payload types are deliberately not
/// here: they resolve through <see cref="AsyncResponseJsonSerialization"/>-registered resolvers,
/// falling back to the runtime reflection resolver in non-trimmed apps (see
/// <see cref="AsyncResponseJson"/>).
/// <para>
/// Metadata-only generation: serialization behavior (naming, null handling, case sensitivity) is
/// governed by the consuming <see cref="JsonSerializerOptions"/> at runtime, exactly like the
/// reflection-based serializer these types were previously written with — the wire format must not
/// change (<see cref="FlowStateSchema"/>, <see cref="RecoveryStateSchema"/>,
/// <see cref="WorkerJobEnvelopeSchema"/> stay at their current versions).
/// </para>
/// <para>
/// <see cref="object"/>, <see cref="JsonElement"/>, and the primitives are registered because
/// <see cref="CallbackParam.Value"/> and <see cref="ReflectionInvocationDto.Params"/> carry
/// heterogeneous literal values captured from callback expressions: they serialize by runtime type
/// and deserialize as <see cref="JsonElement"/>.
/// </para>
/// </summary>
[JsonSourceGenerationOptions(GenerationMode = JsonSourceGenerationMode.Metadata)]
[JsonSerializable(typeof(FlowState))]
[JsonSerializable(typeof(FlowStepState))]
[JsonSerializable(typeof(RecoveryState))]
[JsonSerializable(typeof(List<RecoveryState>))]
[JsonSerializable(typeof(WorkerJobEnvelope))]
[JsonSerializable(typeof(ReflectionCallDto))]
[JsonSerializable(typeof(ReflectionInvocationDto))]
[JsonSerializable(typeof(CallbackParam))]
[JsonSerializable(typeof(AsyncResponseReplyTarget))]
[JsonSerializable(typeof(Dictionary<string, string>))]
// The transport stores serialize message headers through the interface-typed parameter, and
// source-generated metadata is looked up by the STATIC type — the concrete Dictionary
// registration above does not satisfy it (reflection did; this must stay registered).
[JsonSerializable(typeof(IReadOnlyDictionary<string, string>))]
[JsonSerializable(typeof(JsonElement))]
[JsonSerializable(typeof(object))]
[JsonSerializable(typeof(string))]
[JsonSerializable(typeof(bool))]
[JsonSerializable(typeof(int))]
[JsonSerializable(typeof(long))]
[JsonSerializable(typeof(double))]
[JsonSerializable(typeof(decimal))]
[JsonSerializable(typeof(Guid))]
[JsonSerializable(typeof(DateTime))]
[JsonSerializable(typeof(DateTimeOffset))]
[JsonSerializable(typeof(TimeSpan))]
internal sealed partial class AsyncResponseJsonContext : JsonSerializerContext;
