using System.Text.Json;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// The recovery-routing entry point of the lost-subscriber fallback. Input is always a WIRE
/// representation — raw broker JSON, or a typed publish normalized by the dispatcher to its
/// declared-type serialization — which is materialized as the REGISTERED payload type and asked
/// <c>OnRecovery</c>; the materialized instance is returned so callbacks receive it instead of
/// raw JSON. Anything unclassifiable yields a <c>null</c> action so the caller fails
/// conservatively (it never resumes a payload it cannot understand).
/// </summary>
public class PayloadRecoveryClassifierTests
{
    [Theory]
    [InlineData(OperationStatus.Completed, RecoveryAction.Resume)]
    [InlineData(OperationStatus.Running, RecoveryAction.KeepWaiting)]
    [InlineData(OperationStatus.Failed, RecoveryAction.Fail)]
    [InlineData(OperationStatus.Unknown, RecoveryAction.Fail)]
    public void WireRepresentation_UsesTheRegisteredTypesOwnAction(OperationStatus status, RecoveryAction expected)
    {
        // What the dispatcher hands over for a typed publish: the declared-type wire JSON.
        var wireJson = AsyncResponseJson.Serialize(new OperationResult { Status = status, Message = "wire" });

        var classification = PayloadRecoveryClassifier.Classify(wireJson, typeof(OperationResult).FullName);

        Assert.Equal(expected, classification.Action);
        var materialized = Assert.IsType<OperationResult>(classification.MaterializedPayload);
        Assert.Equal(status, materialized.Status);
        Assert.Equal("wire", materialized.Message);
    }

    [Fact]
    public void WireRepresentation_OfDeclaredBasePublish_MaterializesTheBaseVerdict()
    {
        // A derived instance published under its declared (non-polymorphic) base serializes to the
        // BASE's wire shape — the derived OnRecovery override cannot leak into routing, which is
        // exactly how the broker route behaves for the same publish.
        var wireJson = AsyncResponseJson.Serialize<RoutingBasePayload>(new RoutingDerivedPayload());

        var classification = PayloadRecoveryClassifier.Classify(wireJson, typeof(RoutingBasePayload).FullName);

        Assert.Equal(RecoveryAction.Fail, classification.Action);
        Assert.IsType<RoutingBasePayload>(classification.MaterializedPayload);
    }

    [Fact]
    public void WireRepresentation_WithPolymorphicDiscriminator_MaterializesTheDerivedType()
    {
        // A [JsonPolymorphic] base publish carries the discriminator on the wire; classification
        // must honor it and take the DERIVED type's verdict.
        var wireJson = AsyncResponseJson.Serialize<PolyStepBase>(new PolyStepCompleted { Message = "poly" });

        var classification = PayloadRecoveryClassifier.Classify(wireJson, typeof(PolyStepBase).FullName);

        Assert.Equal(RecoveryAction.Resume, classification.Action);
        var materialized = Assert.IsType<PolyStepCompleted>(classification.MaterializedPayload);
        Assert.Equal("poly", materialized.Message);
    }

    [Fact]
    public void WireRepresentation_ForSiblingRegistration_MaterializesTheSiblingsType()
    {
        // Shared-correlation, mixed payload types: registration B registered AlwaysCheckpointProbe
        // while the publisher published an IncidentStepResult. B's registration is classified as
        // B's type from the same wire JSON — never by the publisher's runtime type (which
        // spuriously resumed and consumed B's registration).
        var wireJson = AsyncResponseJson.Serialize(new IncidentStepResult { Status = IncidentStepStatus.Succeeded, Message = "done" });

        var classification = PayloadRecoveryClassifier.Classify(wireJson, typeof(AlwaysCheckpointProbe).FullName);

        Assert.Equal(RecoveryAction.KeepWaiting, classification.Action);
        var materialized = Assert.IsType<AlwaysCheckpointProbe>(classification.MaterializedPayload);
        Assert.Equal("done", materialized.Message);
    }

    [Fact]
    public void WireRepresentation_WithoutRegisteredTypeName_IsUnclassifiable()
    {
        // No registered type, no classification: the wire payload alone cannot say what it is.
        var wireJson = AsyncResponseJson.Serialize(new OperationResult { Status = OperationStatus.Completed });

        var classification = PayloadRecoveryClassifier.Classify(wireJson, payloadTypeFullName: null);

        Assert.Null(classification.Action);
        Assert.Null(classification.MaterializedPayload);
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
