using System.Text.Json;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// The recovery-routing entry point of the lost-subscriber fallback: typed payloads answer for
/// themselves via <c>ShouldResumeOnRecovery</c>; untyped (broker-delivered) JSON is materialized as
/// the registered payload type first; anything unclassifiable yields <c>null</c> so the caller fails
/// conservatively (it never resumes a payload it cannot understand).
/// </summary>
public class PayloadRecoveryClassifierTests
{
    [Theory]
    [InlineData(OperationStatus.Completed, true)]
    [InlineData(OperationStatus.Running, true)]
    [InlineData(OperationStatus.Failed, false)]
    [InlineData(OperationStatus.Unknown, false)]
    public void TypedPayload_UsesItsOwnDecision(OperationStatus status, bool expected)
    {
        var payload = new OperationResult { Status = status };

        Assert.Equal(expected, PayloadRecoveryClassifier.ShouldResume(payload, payloadTypeFullName: null));
    }

    [Fact]
    public void RawJsonElement_MaterializesAsRegisteredTypeAndDecides()
    {
        // The redeploy scenario: a broker ingress deserializes the message as object?
        // (a JsonElement) and the payload type is only known from the recovery state.
        var json = JsonSerializer.Deserialize<object?>("""{"Status":3,"Message":"remote step failed"}""");

        Assert.False(PayloadRecoveryClassifier.ShouldResume(json, typeof(OperationResult).FullName));
    }

    [Fact]
    public void RawJsonElement_PropertyMatchingIsCaseInsensitive()
    {
        var json = JsonSerializer.Deserialize<object?>("""{"status":2,"message":"done"}""");

        Assert.True(PayloadRecoveryClassifier.ShouldResume(json, typeof(OperationResult).FullName));
    }

    [Fact]
    public void RawJsonElement_MissingStatusField_DoesNotResume()
    {
        // A message without the expected status field deserializes to the enum default
        // (Unknown = 0) and must be handled conservatively, not resumed as a success.
        var json = JsonSerializer.Deserialize<object?>("""{"Message":"no status here"}""");

        Assert.False(PayloadRecoveryClassifier.ShouldResume(json, typeof(OperationResult).FullName));
    }

    [Fact]
    public void NullPayload_ReturnsNull()
        => Assert.Null(PayloadRecoveryClassifier.ShouldResume(null, typeof(OperationResult).FullName));

    [Fact]
    public void RawJsonWithoutRegisteredType_ReturnsNull()
    {
        var json = JsonSerializer.Deserialize<object?>("""{"Status":3}""");

        Assert.Null(PayloadRecoveryClassifier.ShouldResume(json, payloadTypeFullName: null));
    }

    [Fact]
    public void UnresolvableOrNonPayloadType_ReturnsNull()
    {
        var json = JsonSerializer.Deserialize<object?>("""{"Status":3}""");

        Assert.Null(PayloadRecoveryClassifier.ShouldResume(json, "Does.Not.Exist.Type"));
        Assert.Null(PayloadRecoveryClassifier.ShouldResume(json, typeof(string).FullName));
    }

    [Fact]
    public void RawJsonThatCannotMaterializeAsRegisteredType_ReturnsNull()
        => Assert.Null(PayloadRecoveryClassifier.ShouldResume("not-json", typeof(OperationResult).FullName));
}
