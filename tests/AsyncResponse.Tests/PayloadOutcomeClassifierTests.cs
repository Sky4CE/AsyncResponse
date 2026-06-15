using System.Text.Json;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// The classification entry point of the lost-subscriber fallback: typed payloads classify
/// themselves; untyped (broker-delivered) JSON is materialized as the registered payload type
/// first; anything unclassifiable yields <c>null</c> so the caller keeps the resume routing.
/// </summary>
public class PayloadOutcomeClassifierTests
{
    [Theory]
    [InlineData(OperationStatus.Completed, AsyncResponseOutcome.Succeeded)]
    [InlineData(OperationStatus.Running, AsyncResponseOutcome.InProgress)]
    [InlineData(OperationStatus.Failed, AsyncResponseOutcome.Failed)]
    [InlineData(OperationStatus.Unknown, AsyncResponseOutcome.Unknown)]
    public void TypedPayload_UsesItsOwnClassifier(OperationStatus status, AsyncResponseOutcome expected)
    {
        var payload = new OperationResult { Status = status };

        Assert.Equal(expected, PayloadOutcomeClassifier.TryClassify(payload, payloadTypeFullName: null));
    }

    [Fact]
    public void RawJsonElement_MaterializesAsRegisteredTypeAndClassifies()
    {
        // The redeploy scenario: a broker ingress deserializes the message as object?
        // (a JsonElement) and the payload type is only known from the recovery state.
        var json = JsonSerializer.Deserialize<object?>("""{"Status":3,"Message":"remote step failed"}""");

        var outcome = PayloadOutcomeClassifier.TryClassify(json, typeof(OperationResult).FullName);

        Assert.Equal(AsyncResponseOutcome.Failed, outcome);
    }

    [Fact]
    public void RawJsonElement_PropertyMatchingIsCaseInsensitive()
    {
        var json = JsonSerializer.Deserialize<object?>("""{"status":2,"message":"done"}""");

        Assert.Equal(AsyncResponseOutcome.Succeeded, PayloadOutcomeClassifier.TryClassify(json, typeof(OperationResult).FullName));
    }

    [Fact]
    public void RawJsonElement_MissingStatusField_ClassifiesAsUnknown()
    {
        // A message without the expected status field deserializes to the enum default
        // (Unknown = 0) and must be handled conservatively, not resumed as a success.
        var json = JsonSerializer.Deserialize<object?>("""{"Message":"no status here"}""");

        Assert.Equal(AsyncResponseOutcome.Unknown, PayloadOutcomeClassifier.TryClassify(json, typeof(OperationResult).FullName));
    }

    [Fact]
    public void NullPayload_ReturnsNull()
        => Assert.Null(PayloadOutcomeClassifier.TryClassify(null, typeof(OperationResult).FullName));

    [Fact]
    public void RawJsonWithoutRegisteredType_ReturnsNull()
    {
        var json = JsonSerializer.Deserialize<object?>("""{"Status":3}""");

        Assert.Null(PayloadOutcomeClassifier.TryClassify(json, payloadTypeFullName: null));
    }

    [Fact]
    public void UnresolvableOrNonPayloadType_ReturnsNull()
    {
        var json = JsonSerializer.Deserialize<object?>("""{"Status":3}""");

        Assert.Null(PayloadOutcomeClassifier.TryClassify(json, "Does.Not.Exist.Type"));
        Assert.Null(PayloadOutcomeClassifier.TryClassify(json, typeof(string).FullName));
    }
}
