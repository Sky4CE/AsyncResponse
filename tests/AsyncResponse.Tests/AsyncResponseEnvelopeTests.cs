using System.Text.Json;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// The wire envelope every published response travels in. Its custom converter is a persisted
/// contract: it must round-trip payload and failure envelopes, tolerate a JSON <c>null</c> payload
/// even for a non-nullable value type (assigning <c>default(T)</c> rather than throwing), and skip
/// unknown properties so additive schema changes stay compatible.
/// </summary>
public class AsyncResponseEnvelopeTests
{
    [Fact]
    public void RoundTrips_SuccessPayload()
    {
        var envelope = new AsyncResponseEnvelope<OperationResult>
        {
            Success = true,
            Payload = new OperationResult { Status = OperationStatus.Completed, Message = "done" }
        };

        var json = JsonSerializer.Serialize(envelope, AsyncResponseEnvelopeOptions<OperationResult>.Instance);
        var restored = JsonSerializer.Deserialize<AsyncResponseEnvelope<OperationResult>>(
            json, AsyncResponseEnvelopeOptions<OperationResult>.Instance);

        Assert.NotNull(restored);
        Assert.True(restored!.Success);
        Assert.Equal(OperationStatus.Completed, restored.Payload!.Status);
        Assert.Equal("done", restored.Payload.Message);
    }

    [Fact]
    public void RoundTrips_FailureEnvelope()
    {
        var envelope = new AsyncResponseEnvelope<OperationResult>
        {
            Success = false,
            ExceptionMessage = "remote boom",
            ExceptionStackTrace = "at Remote.Do()"
        };

        var json = JsonSerializer.Serialize(envelope, AsyncResponseEnvelopeOptions<OperationResult>.Instance);
        var restored = JsonSerializer.Deserialize<AsyncResponseEnvelope<OperationResult>>(
            json, AsyncResponseEnvelopeOptions<OperationResult>.Instance);

        Assert.NotNull(restored);
        Assert.False(restored!.Success);
        Assert.Null(restored.Payload);
        Assert.Equal("remote boom", restored.ExceptionMessage);
        Assert.Equal("at Remote.Do()", restored.ExceptionStackTrace);
    }

    [Fact]
    public void NullPayload_ForNonNullableValueType_BecomesDefault()
    {
        // The converter assigns default(int) instead of throwing on a JSON null payload.
        var restored = JsonSerializer.Deserialize<AsyncResponseEnvelope<int>>(
            """{"Success":true,"Payload":null}""",
            AsyncResponseEnvelopeOptions<int>.Instance);

        Assert.NotNull(restored);
        Assert.True(restored!.Success);
        Assert.Equal(0, restored.Payload);
    }

    [Fact]
    public void UnknownProperties_AreSkipped()
    {
        var restored = JsonSerializer.Deserialize<AsyncResponseEnvelope<OperationResult>>(
            """{"Success":true,"Payload":{"Status":2},"Unknown":{"nested":true},"Extra":42}""",
            AsyncResponseEnvelopeOptions<OperationResult>.Instance);

        Assert.NotNull(restored);
        Assert.True(restored!.Success);
        Assert.Equal(OperationStatus.Completed, restored.Payload!.Status);
    }

    [Fact]
    public void NonObjectToken_Throws()
        => Assert.Throws<JsonException>(() => JsonSerializer.Deserialize<AsyncResponseEnvelope<int>>(
            "123", AsyncResponseEnvelopeOptions<int>.Instance));
}
