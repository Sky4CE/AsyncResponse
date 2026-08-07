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
    public void TypedPayload_AssignableToRegisteredType_ReusesTheInstance()
    {
        var payload = new OperationResult { Status = OperationStatus.Completed };

        var classification = PayloadRecoveryClassifier.Classify(payload, typeof(OperationResult).FullName);

        Assert.Equal(RecoveryAction.Resume, classification.Action);
        Assert.Same(payload, classification.MaterializedPayload);
    }

    [Fact]
    public void TypedPayload_DerivedInstanceForRegisteredBaseType_RematerializesAsTheBaseType()
    {
        // Only an EXACT runtime-type match reuses the instance. A derived instance must be
        // re-materialized as the registered base — exactly what the broker route would produce —
        // so the routing verdict cannot depend on whether the response crossed a serialization
        // boundary.
        var payload = new DerivedStepResult { Status = IncidentStepStatus.Succeeded, Extra = "dropped" };

        var classification = PayloadRecoveryClassifier.Classify(payload, typeof(BaseStepResult).FullName);

        Assert.Equal(RecoveryAction.Resume, classification.Action);
        var materialized = Assert.IsType<BaseStepResult>(classification.MaterializedPayload);
        Assert.NotSame(payload, materialized);
        Assert.Equal(IncidentStepStatus.Succeeded, materialized.Status);
    }

    [Fact]
    public void TypedPayload_DerivedOverridingOnRecovery_CannotDivergeFromTheWireRoute()
    {
        // The divergence this pins: base classifies Fail, derived overrides to Resume. The typed
        // in-process route used to ask the derived instance (Resume) while the broker route
        // materialized the registered base (Fail) — the same logical response resuming or failing
        // the flow depending on which process happened to publish it. Both routes must agree on
        // the registered type's verdict.
        var typed = PayloadRecoveryClassifier.Classify(
            new RoutingDerivedPayload(), typeof(RoutingBasePayload).FullName);
        var wire = PayloadRecoveryClassifier.Classify(
            JsonSerializer.Deserialize<object?>("""{}"""), typeof(RoutingBasePayload).FullName);

        Assert.Equal(RecoveryAction.Fail, wire.Action);
        Assert.Equal(wire.Action, typed.Action);
        Assert.IsType<RoutingBasePayload>(typed.MaterializedPayload);
    }

    [Fact]
    public void TypedPayload_MismatchedRegisteredType_MaterializesAsTheRegisteredType()
    {
        // Shared-correlation, mixed payload types: registration B registered AlwaysCheckpointProbe
        // while the publisher published an IncidentStepResult. B's registration must be classified
        // as B's type — via the same JSON round-trip a broker delivery would take — not by the
        // publisher's runtime type (which spuriously resumed and consumed B's registration).
        var payload = new IncidentStepResult { Status = IncidentStepStatus.Succeeded, Message = "done" };

        var classification = PayloadRecoveryClassifier.Classify(payload, typeof(AlwaysCheckpointProbe).FullName);

        Assert.Equal(RecoveryAction.KeepWaiting, classification.Action);
        var materialized = Assert.IsType<AlwaysCheckpointProbe>(classification.MaterializedPayload);
        Assert.Equal("done", materialized.Message);
    }

    [Fact]
    public void TypedPayload_UnresolvableRegisteredType_IsUnclassifiable()
    {
        // The registration named a type this process cannot load: the registered type governs, so
        // classification stays conservative rather than letting the instance answer for a
        // registration that asked for something else.
        var payload = new OperationResult { Status = OperationStatus.Completed };

        var classification = PayloadRecoveryClassifier.Classify(payload, "Does.Not.Exist.Type");

        Assert.Null(classification.Action);
        Assert.Null(classification.MaterializedPayload);
    }

    [Fact]
    public void TypedPayload_NonPayloadRegisteredType_IsUnclassifiable()
    {
        var payload = new OperationResult { Status = OperationStatus.Completed };

        var classification = PayloadRecoveryClassifier.Classify(payload, typeof(string).FullName);

        Assert.Null(classification.Action);
        Assert.Null(classification.MaterializedPayload);
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

/// <summary>Base payload whose recovery verdict is Fail; the derived override flips it to Resume.</summary>
public class RoutingBasePayload : IAsyncResponsePayload
{
    public virtual RecoveryAction OnRecovery() => RecoveryAction.Fail;
}

public sealed class RoutingDerivedPayload : RoutingBasePayload
{
    public override RecoveryAction OnRecovery() => RecoveryAction.Resume;
}
