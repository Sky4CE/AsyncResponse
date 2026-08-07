using System.Text.Json;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// The recovery-routing entry point of the lost-subscriber fallback: typed payloads answer for
/// themselves via <c>OnRecovery</c>; untyped (broker-delivered) JSON is materialized as the
/// registered payload type first — and the materialized instance is returned so callbacks receive
/// it instead of raw JSON; anything unclassifiable yields a <c>null</c> action so the caller fails
/// conservatively (it never resumes a payload it cannot understand).
/// </summary>
public class PayloadRecoveryClassifierTests
{
    [Theory]
    [InlineData(OperationStatus.Completed, RecoveryAction.Resume)]
    [InlineData(OperationStatus.Running, RecoveryAction.KeepWaiting)]
    [InlineData(OperationStatus.Failed, RecoveryAction.Fail)]
    [InlineData(OperationStatus.Unknown, RecoveryAction.Fail)]
    public void TypedPayload_UsesItsOwnOnRecoveryAction(OperationStatus status, RecoveryAction expected)
    {
        var payload = new OperationResult { Status = status };

        var classification = PayloadRecoveryClassifier.Classify(payload, payloadTypeFullName: null);

        Assert.Equal(expected, classification.Action);
        Assert.Same(payload, classification.MaterializedPayload);
    }

    [Theory]
    [InlineData(IncidentStepStatus.Succeeded, RecoveryAction.Resume)]
    [InlineData(IncidentStepStatus.InProgress, RecoveryAction.KeepWaiting)]
    [InlineData(IncidentStepStatus.Failed, RecoveryAction.Fail)]
    public void TypedPayload_IncidentShape_UsesItsOwnAction(IncidentStepStatus status, RecoveryAction expected)
    {
        var payload = new IncidentStepResult { Status = status };

        var classification = PayloadRecoveryClassifier.Classify(payload, payloadTypeFullName: null);

        Assert.Equal(expected, classification.Action);
        Assert.Same(payload, classification.MaterializedPayload);
    }

    [Fact]
    public void RawJsonElement_MaterializesAsRegisteredTypeAndDecides()
    {
        // The redeploy scenario: a broker ingress deserializes the message as object?
        // (a JsonElement) and the payload type is only known from the recovery state.
        var json = JsonSerializer.Deserialize<object?>("""{"Status":3,"Message":"remote step failed"}""");

        var classification = PayloadRecoveryClassifier.Classify(json, typeof(OperationResult).FullName);

        Assert.Equal(RecoveryAction.Fail, classification.Action);

        // 292332 regression: the materialized instance must be returned for the callback — the
        // raw JsonElement handed to an object-typed callback parameter is what deadlocked the flow.
        var materialized = Assert.IsType<OperationResult>(classification.MaterializedPayload);
        Assert.Equal(OperationStatus.Failed, materialized.Status);
        Assert.Equal("remote step failed", materialized.Message);
    }

    [Fact]
    public void RawJsonElement_NonTerminalCheckpoint_KeepsWaiting()
    {
        var json = JsonSerializer.Deserialize<object?>("""{"Status":1,"Message":"still running"}""");

        var classification = PayloadRecoveryClassifier.Classify(json, typeof(IncidentStepResult).FullName);

        Assert.Equal(RecoveryAction.KeepWaiting, classification.Action);
        var materialized = Assert.IsType<IncidentStepResult>(classification.MaterializedPayload);
        Assert.Equal(IncidentStepStatus.InProgress, materialized.Status);
    }

    [Fact]
    public void RawJsonElement_PropertyMatchingIsCaseInsensitive()
    {
        var json = JsonSerializer.Deserialize<object?>("""{"status":2,"message":"done"}""");

        var classification = PayloadRecoveryClassifier.Classify(json, typeof(OperationResult).FullName);

        Assert.Equal(RecoveryAction.Resume, classification.Action);
        var materialized = Assert.IsType<OperationResult>(classification.MaterializedPayload);
        Assert.Equal("done", materialized.Message);
    }

    [Fact]
    public void RawJsonElement_MissingStatusField_DoesNotResume()
    {
        // A message without the expected status field deserializes to the enum default
        // (Unknown = 0) and must be handled conservatively, not resumed as a success.
        var json = JsonSerializer.Deserialize<object?>("""{"Message":"no status here"}""");

        Assert.Equal(RecoveryAction.Fail, PayloadRecoveryClassifier.Classify(json, typeof(OperationResult).FullName).Action);
    }

    [Fact]
    public void NullPayload_IsUnclassifiable()
    {
        var classification = PayloadRecoveryClassifier.Classify(null, typeof(OperationResult).FullName);

        Assert.Null(classification.Action);
        Assert.Null(classification.MaterializedPayload);
    }

    [Fact]
    public void RawJsonWithoutRegisteredType_IsUnclassifiable()
    {
        var json = JsonSerializer.Deserialize<object?>("""{"Status":3}""");

        var classification = PayloadRecoveryClassifier.Classify(json, payloadTypeFullName: null);

        Assert.Null(classification.Action);
        Assert.Null(classification.MaterializedPayload);
    }

    [Fact]
    public void UnresolvableOrNonPayloadType_IsUnclassifiable()
    {
        var json = JsonSerializer.Deserialize<object?>("""{"Status":3}""");

        Assert.Null(PayloadRecoveryClassifier.Classify(json, "Does.Not.Exist.Type").Action);
        Assert.Null(PayloadRecoveryClassifier.Classify(json, typeof(string).FullName).Action);
    }

    [Fact]
    public void RawJsonThatCannotMaterializeAsRegisteredType_IsUnclassifiable()
    {
        var classification = PayloadRecoveryClassifier.Classify("not-json", typeof(OperationResult).FullName);

        Assert.Null(classification.Action);
        Assert.Null(classification.MaterializedPayload);
    }
}
