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
            Context = new Dictionary<string, string> { ["tenant"] = "acme" }
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
    }
}
