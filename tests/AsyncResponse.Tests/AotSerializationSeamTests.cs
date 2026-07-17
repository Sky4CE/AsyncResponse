using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// The trim/AOT JSON seam is a wire-compat commitment: the library's source-generated metadata
/// (<see cref="AsyncResponseJsonContext"/>, reached through <see cref="AsyncResponseJson"/>) must
/// produce byte-identical JSON to the reflection-based serialization these wire types shipped
/// with, and user payload types must resolve through resolvers registered via
/// <see cref="AsyncResponseJsonSerialization"/>.
/// </summary>
public class AotSerializationSeamTests
{
    [Fact]
    public void FlowState_SourceGenMatchesReflection_ByteForByte()
    {
        var state = new FlowState
        {
            FlowId = "flow-1",
            FlowTypeName = "My.Flows.ProvisioningFlow",
            InputTypeName = "My.Flows.ProvisionRequest",
            InputJson = """{"name":"x"}""",
            Status = FlowRunStatus.Running,
            LastMessage = "Step 'prepare' completed.",
            CreatedAtUtc = new DateTime(2026, 7, 16, 8, 30, 0, DateTimeKind.Utc),
            UpdatedAtUtc = new DateTime(2026, 7, 16, 8, 31, 0, DateTimeKind.Utc),
            Attempts = 2,
            Steps = new Dictionary<string, FlowStepState>(StringComparer.Ordinal)
            {
                ["prepare"] = new()
                {
                    Completed = true,
                    ResultJson = """{"slug":"x-prep"}""",
                    CompletedAtUtc = new DateTime(2026, 7, 16, 8, 30, 30, DateTimeKind.Utc)
                },
                ["remote-work"] = new() { PendingCorrelationId = "cid-42", Message = null }
            },
            Values = new Dictionary<string, string>(StringComparer.Ordinal) { ["greeting"] = "\"hi\"" },
            Context = new Dictionary<string, string>(StringComparer.Ordinal) { ["tenant"] = "t-1" }
        };

        // The ledger's historical wire format: reflection-based serializer, nulls omitted on write.
        var reflectionBaseline = JsonSerializer.Serialize(
            state,
            new JsonSerializerOptions { DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull });

        Assert.Equal(reflectionBaseline, FlowStateJson.Serialize(state));

        var roundTripped = FlowStateJson.Deserialize(reflectionBaseline);
        Assert.NotNull(roundTripped);
        Assert.Equal(state.FlowTypeName, roundTripped!.FlowTypeName);
        Assert.Equal("cid-42", roundTripped.Steps!["remote-work"].PendingCorrelationId);
    }

    [Fact]
    public void RecoveryStateList_SourceGenMatchesReflection_ByteForByte()
    {
        var states = new List<RecoveryState>
        {
            new()
            {
                RegistrationId = new Guid("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"),
                CorrelationId = "cid-1",
                PayloadTypeFullName = "My.App.ProvisioningResult",
                RegisteredAtUtc = new DateTime(2026, 7, 16, 9, 0, 0, DateTimeKind.Utc),
                ResumeCallback = new ReflectionCallDto
                {
                    ServiceInterfaceFullName = "My.App.IProvisioningService",
                    MethodName = "ResumeAsync",
                    Params =
                    [
                        CallbackParam.ForValue("literal"),
                        CallbackParam.ForValue(42),
                        CallbackParam.ForPlaceholder(PlaceholderType.Payload),
                        CallbackParam.ForPlaceholder(PlaceholderType.CorrelationId)
                    ]
                },
                Context = new Dictionary<string, string>(StringComparer.Ordinal) { ["principal"] = "u-7" }
            },
            new() { RegistrationId = Guid.Empty, CorrelationId = "cid-1" }
        };

        // Recovery stores historically serialized with serializer defaults (bare Serialize call).
        var reflectionBaseline = JsonSerializer.Serialize(states);

        Assert.Equal(reflectionBaseline, AsyncResponseJson.Serialize(states));

        var roundTripped = AsyncResponseJson.Deserialize<List<RecoveryState>>(reflectionBaseline);
        Assert.NotNull(roundTripped);
        Assert.Equal(2, roundTripped!.Count);
        Assert.Equal("ResumeAsync", roundTripped[0].ResumeCallback!.MethodName);
        Assert.Equal(PlaceholderType.Payload, roundTripped[0].ResumeCallback!.Params[2].Placeholder);
    }

    [Fact]
    public void WorkerJobEnvelope_SourceGenMatchesReflection_ByteForByte()
    {
        var job = new WorkerJobEnvelope
        {
            Call = new ReflectionCallDto
            {
                ServiceInterfaceFullName = "My.App.IWorker",
                MethodName = "RunAsync",
                Params = [CallbackParam.ForValue("job-7"), CallbackParam.ForValue(true)]
            },
            CorrelationId = "cid-9",
            ReplyTarget = new AsyncResponseReplyTarget { Name = "default", Transport = "rabbitmq", Address = "amq.topic/replies" },
            Context = new Dictionary<string, string>(StringComparer.Ordinal) { ["trace"] = "00-ab" }
        };

        var reflectionBaseline = JsonSerializer.Serialize(job);

        Assert.Equal(reflectionBaseline, AsyncResponseJson.Serialize(job));

        var roundTripped = JsonSafety.SafeDeserialize<WorkerJobEnvelope>(reflectionBaseline);
        Assert.NotNull(roundTripped);
        Assert.Equal("RunAsync", roundTripped!.Call.MethodName);
        Assert.Equal("rabbitmq", roundTripped.ReplyTarget!.Transport);
    }

    [Fact]
    public void Envelope_GoldenWireFormat_AndSeamEquivalence()
    {
        var envelope = new AsyncResponseEnvelope<OperationResult>
        {
            Success = true,
            Payload = new OperationResult { Status = OperationStatus.Completed, Message = "done" }
        };

        var json = AsyncResponseEnvelopeJson.Serialize(envelope);

        // The exact shape the custom converter has always written, property order included.
        Assert.StartsWith("""{"SchemaVersion":1,"Success":true,"Payload":{""", json, StringComparison.Ordinal);
        Assert.EndsWith(""","ExceptionMessage":null,"ExceptionStackTrace":null}""", json, StringComparison.Ordinal);

        // The typed-metadata path and the options-based path used by tests/channels agree.
        Assert.Equal(
            JsonSerializer.Serialize(envelope, AsyncResponseEnvelopeOptions<OperationResult>.Instance),
            json);

        var restored = AsyncResponseEnvelopeJson.SafeDeserialize<OperationResult>(json);
        Assert.NotNull(restored);
        Assert.Equal(OperationStatus.Completed, restored!.Payload!.Status);
    }

    [Fact]
    public void LibraryContext_CoversEveryStaticallySerializedWireType()
    {
        // Metadata is resolved by the STATIC type at each callsite, so every type the packages
        // pass to AsyncResponseJson (including interface-typed parameters, which the reflection
        // serializer used to tolerate silently) must resolve from the built-in context alone —
        // no reflection fallback. Found the hard way: transport stores serialize headers as
        // IReadOnlyDictionary<string, string>, which crashed only under Native AOT.
        Type[] staticallySerialized =
        [
            typeof(FlowState),
            typeof(RecoveryState),
            typeof(List<RecoveryState>),
            typeof(WorkerJobEnvelope),
            typeof(ReflectionCallDto),
            typeof(ReflectionInvocationDto),
            typeof(Dictionary<string, string>),
            typeof(IReadOnlyDictionary<string, string>),
            typeof(System.Text.Json.JsonElement),
        ];

        var context = (IJsonTypeInfoResolver)AsyncResponseJsonContext.Default;
        var options = new JsonSerializerOptions();
        foreach (var type in staticallySerialized)
        {
            Assert.True(
                context.GetTypeInfo(type, options) is not null,
                $"AsyncResponseJsonContext must register {type} — a package serializes it by this static type.");
        }
    }

    [Fact]
    public void RegisteredResolvers_AreConsultedAfterLibraryWireTypes()
    {
        var tracker = new TrackingResolver();
        AsyncResponseJsonSerialization.RegisterResolver(tracker);
        try
        {
            // A type unknown to the library's context: the chain must consult the registered
            // resolver (which declines) and still succeed via the reflection fallback in tests.
            var json = AsyncResponseJson.Serialize(new SeamProbePayload { Name = "probe" });
            Assert.Contains("\"probe\"", json, StringComparison.Ordinal);
            Assert.Contains(typeof(SeamProbePayload), tracker.SeenTypes);

            // Library wire types resolve from the built-in context first: the registered resolver
            // must never be asked for them.
            _ = AsyncResponseJson.Serialize(new List<RecoveryState>());
            Assert.DoesNotContain(typeof(List<RecoveryState>), tracker.SeenTypes);
        }
        finally
        {
            AsyncResponseJsonSerialization.Reset();
        }
    }

    private sealed class TrackingResolver : IJsonTypeInfoResolver
    {
        public List<Type> SeenTypes { get; } = [];

        public JsonTypeInfo? GetTypeInfo(Type type, JsonSerializerOptions options)
        {
            lock (SeenTypes)
            {
                SeenTypes.Add(type);
            }

            return null;
        }
    }

    private sealed class SeamProbePayload
    {
        public string? Name { get; set; }
    }
}
