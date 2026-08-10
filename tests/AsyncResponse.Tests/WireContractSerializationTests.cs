using System.Text.Json;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// The persisted/transported DTOs are a wire contract: <see cref="RecoveryState"/> is serialized
/// into the recovery store and <see cref="WorkerJobEnvelope"/> onto the worker queue, both readable
/// across deployments. These guard that their PascalCase property names round-trip (additive-only
/// contract) and that placeholder callbacks survive serialization.
/// </summary>
public class WireContractSerializationTests
{
    [Fact]
    public void RecoveryState_RoundTrips_WithStablePropertyNames()
    {
        var state = new RecoveryState
        {
            RegistrationId = Guid.Parse("11111111-1111-1111-1111-111111111111"),
            CorrelationId = "corr-1",
            PayloadTypeFullName = "My.Payload",
            RegisteredAtUtc = new DateTime(2026, 1, 2, 3, 4, 5, DateTimeKind.Utc),
            Context = new Dictionary<string, string> { ["trace"] = "abc" },
            ResumeCallback = new ReflectionCallDto
            {
                ServiceInterfaceFullName = "My.IFlow",
                MethodName = "Resume",
                Params = [CallbackParam.ForPlaceholder(PlaceholderType.Payload)]
            },
            FailureCallback = new ReflectionCallDto
            {
                ServiceInterfaceFullName = "My.IFlow",
                MethodName = "Fail",
                Params = [CallbackParam.ForPlaceholder(PlaceholderType.Exception)]
            }
        };

        var json = JsonSerializer.Serialize(state);

        // Property names are the contract — assert the wire shape, not just round-trip equality.
        Assert.Contains("\"CorrelationId\"", json, StringComparison.Ordinal);
        Assert.Contains("\"RegistrationId\"", json, StringComparison.Ordinal);
        Assert.Contains("\"PayloadTypeFullName\"", json, StringComparison.Ordinal);
        Assert.Contains("\"RegisteredAtUtc\"", json, StringComparison.Ordinal);
        Assert.Contains("\"ResumeCallback\"", json, StringComparison.Ordinal);
        Assert.Contains("\"FailureCallback\"", json, StringComparison.Ordinal);
        Assert.Contains("\"Context\"", json, StringComparison.Ordinal);

        var restored = JsonSerializer.Deserialize<RecoveryState>(json);
        Assert.NotNull(restored);
        Assert.Equal("corr-1", restored!.CorrelationId);
        Assert.Equal(state.RegistrationId, restored.RegistrationId);
        Assert.Equal("My.Payload", restored.PayloadTypeFullName);
        Assert.Equal(state.RegisteredAtUtc, restored.RegisteredAtUtc);
        Assert.Equal("abc", restored.Context!["trace"]);
        Assert.Equal("Resume", restored.ResumeCallback!.MethodName);
        Assert.Equal(PlaceholderType.Payload, restored.ResumeCallback.Params[0].Placeholder);
        Assert.Equal("Fail", restored.FailureCallback!.MethodName);
        Assert.Equal(PlaceholderType.Exception, restored.FailureCallback.Params[0].Placeholder);
    }

    [Fact]
    public void WorkerJobEnvelope_RoundTrips()
    {
        var envelope = new WorkerJobEnvelope
        {
            Call = new ReflectionCallDto
            {
                ServiceInterfaceFullName = "My.IWorker",
                MethodName = "Process",
                Params = [CallbackParam.ForValue("order-42")]
            },
            CorrelationId = "corr-1",
            ReplyTarget = new AsyncResponseReplyTarget { Name = "default", Transport = "test", Address = "test://reply" },
            Context = new Dictionary<string, string> { ["tenant"] = "acme" },
            NotBeforeUtc = new DateTime(2026, 5, 1, 8, 0, 0, DateTimeKind.Utc),
            LastRedelayRemaining = TimeSpan.FromMinutes(90),
            RedelayStallCount = 1
        };

        var json = JsonSerializer.Serialize(envelope);

        Assert.Contains("\"Call\"", json, StringComparison.Ordinal);
        Assert.Contains("\"CorrelationId\"", json, StringComparison.Ordinal);
        Assert.Contains("\"ReplyTarget\"", json, StringComparison.Ordinal);
        Assert.Contains("\"Context\"", json, StringComparison.Ordinal);

        var restored = JsonSerializer.Deserialize<WorkerJobEnvelope>(json);
        Assert.NotNull(restored);
        Assert.Equal("corr-1", restored!.CorrelationId);
        Assert.Equal("Process", restored.Call.MethodName);
        Assert.Equal("default", restored.ReplyTarget!.Name);
        Assert.Equal("acme", restored.Context!["tenant"]);
        // The chunk chain's only durable carriers: the absolute due time and the progress baseline
        // must survive a serialize/deserialize hop or delayed delivery breaks across a broker.
        Assert.Equal(envelope.NotBeforeUtc, restored.NotBeforeUtc);
        Assert.Equal(TimeSpan.FromMinutes(90), restored.LastRedelayRemaining);
        Assert.Equal(1, restored.RedelayStallCount);
    }

    [Fact]
    public void WorkerJobEnvelope_PinnedLegacyPayload_WithoutDelayFields_Deserializes()
    {
        // Pinned raw v1 envelope JSON written BEFORE delayed delivery existed (no NotBeforeUtc /
        // LastRedelayRemaining). Additive wire properties: this literal must keep deserializing to
        // null forever — a producer on an older build must interop with a newer consumer.
        const string legacyJson =
            """
            {
              "SchemaVersion": 1,
              "Call": { "ServiceInterfaceFullName": "My.IWorker", "MethodName": "Process", "Params": [] },
              "CorrelationId": "legacy-corr"
            }
            """;

        var restored = JsonSerializer.Deserialize<WorkerJobEnvelope>(legacyJson);

        Assert.NotNull(restored);
        Assert.Equal("legacy-corr", restored!.CorrelationId);
        Assert.Null(restored.NotBeforeUtc);
        Assert.Null(restored.LastRedelayRemaining);
        Assert.Equal(0, restored.RedelayStallCount);
    }

    [Fact]
    public void FlowStateJson_OmitsAbsentChildFlowMetadata_ButKeepsPresentRelationships()
    {
        var state = new FlowState
        {
            FlowId = "root",
            FlowTypeName = "Flow",
            InputTypeName = "Input",
            InputJson = "{}",
            Status = FlowRunStatus.Running,
            Steps = new Dictionary<string, FlowStepState>(StringComparer.Ordinal)
            {
                ["local"] = new() { Completed = true }
            }
        };

        var json = FlowStateJson.Serialize(state);

        Assert.DoesNotContain("\"ParentFlowId\"", json, StringComparison.Ordinal);
        Assert.DoesNotContain("\"ParentStepName\"", json, StringComparison.Ordinal);
        Assert.DoesNotContain("\"ChildFlowId\"", json, StringComparison.Ordinal);

        state.ParentFlowId = "parent";
        state.ParentStepName = "child-step";
        state.Steps["local"].ChildFlowId = "child";

        json = FlowStateJson.Serialize(state);

        Assert.Contains("\"ParentFlowId\":\"parent\"", json, StringComparison.Ordinal);
        Assert.Contains("\"ParentStepName\":\"child-step\"", json, StringComparison.Ordinal);
        Assert.Contains("\"ChildFlowId\":\"child\"", json, StringComparison.Ordinal);

        var restored = JsonSerializer.Deserialize<FlowState>(json);
        Assert.Equal("parent", restored!.ParentFlowId);
        Assert.Equal("child-step", restored.ParentStepName);
        Assert.Equal("child", restored.Steps!["local"].ChildFlowId);
    }

    [Fact]
    public void FlowState_PinnedLegacyPayload_WithoutChildFlowFields_Deserializes()
    {
        // Pinned raw v1 ledger JSON written BEFORE child flows existed (no ParentFlowId /
        // ParentStepName / ChildFlowId). This literal must keep deserializing forever — it guards
        // against anyone making the child-flow fields required or giving them throwing setters.
        const string legacyJson =
            """
            {
              "SchemaVersion": 1,
              "FlowId": "legacy-1",
              "FlowTypeName": "Flow",
              "InputTypeName": "Input",
              "InputJson": "{}",
              "Status": 0,
              "Attempts": 2,
              "LastMessage": "Step 'remote-op' completed.",
              "CreatedAtUtc": "2026-05-01T08:00:00Z",
              "UpdatedAtUtc": "2026-05-01T08:00:05Z",
              "Steps": {
                "remote-op": {
                  "Completed": true,
                  "ResultJson": "{\"Status\":2}",
                  "CompletedAtUtc": "2026-05-01T08:00:05Z"
                }
              },
              "Values": { "tenant": "7" }
            }
            """;

        var state = JsonSerializer.Deserialize<FlowState>(legacyJson);

        Assert.NotNull(state);
        Assert.Equal("legacy-1", state!.FlowId);
        Assert.Equal(FlowRunStatus.Running, state.Status);
        Assert.Equal(2, state.Attempts);
        Assert.True(state.Steps!["remote-op"].Completed);
        Assert.Equal("7", state.Values!["tenant"]);
        Assert.Null(state.ParentFlowId);
        Assert.Null(state.ParentStepName);
        Assert.Null(state.Steps["remote-op"].ChildFlowId);
        // WakeAtUtc is the timers feature's additive wire property: absent on ledgers written
        // before timers existed, and it must stay optional on the read path forever.
        Assert.Null(state.Steps["remote-op"].WakeAtUtc);
    }
}
