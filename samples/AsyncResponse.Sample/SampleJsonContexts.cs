using System.Text.Json;
using System.Text.Json.Serialization;

namespace AsyncResponse.Sample;

/// <summary>
/// Source-generated metadata for everything the sample returns over HTTP. Chained into ASP.NET's
/// <c>ConfigureHttpJsonOptions</c>, so the web defaults (camelCase naming, etc.) keep applying at
/// runtime — the JSON is identical to the reflection-based output, and the app publishes as
/// Native AOT. <see cref="object"/> and the primitives cover the health report's data bag, whose
/// values serialize by runtime type.
/// </summary>
[JsonSourceGenerationOptions(GenerationMode = JsonSourceGenerationMode.Metadata)]
[JsonSerializable(typeof(StatusMessageResult))]
[JsonSerializable(typeof(FaultResult))]
[JsonSerializable(typeof(ReplyTargetResult))]
[JsonSerializable(typeof(CorrelationResult))]
[JsonSerializable(typeof(AttachOutcomeResult))]
[JsonSerializable(typeof(DeletedResult))]
[JsonSerializable(typeof(StartFlowResult))]
[JsonSerializable(typeof(MultiStepFlowResult))]
[JsonSerializable(typeof(SharedExceptionResult))]
[JsonSerializable(typeof(LostSubscriberFlowResult))]
[JsonSerializable(typeof(FlowCall))]
[JsonSerializable(typeof(FlowState))]
[JsonSerializable(typeof(ConfigResponse))]
[JsonSerializable(typeof(HealthReportResponse))]
[JsonSerializable(typeof(AsyncResponseRecoveryStats))]
[JsonSerializable(typeof(List<AsyncResponseStaleRecoveryEntry>))]
[JsonSerializable(typeof(Microsoft.AspNetCore.Mvc.ProblemDetails))]
[JsonSerializable(typeof(string))]
[JsonSerializable(typeof(bool))]
[JsonSerializable(typeof(int))]
[JsonSerializable(typeof(long))]
[JsonSerializable(typeof(double))]
[JsonSerializable(typeof(DateTime))]
[JsonSerializable(typeof(DateTimeOffset))]
[JsonSerializable(typeof(TimeSpan))]
[JsonSerializable(typeof(object))]
internal sealed partial class SampleHttpJsonContext : JsonSerializerContext;

/// <summary>
/// Source-generated metadata for the payloads that cross AsyncResponse itself (or are fed raw to
/// broker response queues by the /emit-response helpers): response payloads, durable-flow inputs,
/// and the raw-body shape. No naming policy — these serialize with serializer defaults
/// (PascalCase), exactly as before. Registered with
/// <see cref="AsyncResponseJsonSerialization.RegisterResolver"/> at startup, which is the one
/// line a trimmed/AOT app needs so the library can (de)serialize its payload types.
/// </summary>
[JsonSourceGenerationOptions(GenerationMode = JsonSourceGenerationMode.Metadata)]
[JsonSerializable(typeof(OperationResult))]
[JsonSerializable(typeof(ProvisioningFlowInput))]
[JsonSerializable(typeof(OnboardingFlowInput))]
[JsonSerializable(typeof(RawResponseBody))]
internal sealed partial class SampleWireJsonContext : JsonSerializerContext;
