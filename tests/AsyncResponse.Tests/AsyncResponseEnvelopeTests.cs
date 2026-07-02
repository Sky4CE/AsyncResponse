using System.Buffers;
using System.Text;
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
    public void SegmentedJsonPropertyNames_AreReadCorrectly()
    {
        var sequence = Segmented(
            """{"SchemaVersion":1,"Success":true,"Payload":{"Status":2,"Message":"done"},"ExceptionMessage":null,"ExceptionStackTrace":null}""",
            chunkSize: 3);
        var reader = new Utf8JsonReader(sequence);

        var restored = JsonSerializer.Deserialize<AsyncResponseEnvelope<OperationResult>>(
            ref reader,
            AsyncResponseEnvelopeOptions<OperationResult>.Instance);

        Assert.NotNull(restored);
        Assert.Equal(AsyncResponseEnvelopeSchema.Current, restored!.SchemaVersion);
        Assert.True(restored.Success);
        Assert.Equal("done", restored.Payload!.Message);
        Assert.Null(restored.ExceptionMessage);
        Assert.Null(restored.ExceptionStackTrace);
    }

    [Fact]
    public void NonObjectToken_Throws()
        => Assert.Throws<JsonException>(() => JsonSerializer.Deserialize<AsyncResponseEnvelope<int>>(
            "123", AsyncResponseEnvelopeOptions<int>.Instance));

    private static ReadOnlySequence<byte> Segmented(string json, int chunkSize)
    {
        var bytes = Encoding.UTF8.GetBytes(json);
        BufferSegment? first = null;
        BufferSegment? last = null;
        var runningIndex = 0L;
        for (var offset = 0; offset < bytes.Length; offset += chunkSize)
        {
            var length = Math.Min(chunkSize, bytes.Length - offset);
            var segment = new BufferSegment(bytes.AsMemory(offset, length), runningIndex);
            if (first is null)
            {
                first = segment;
            }
            else
            {
                last!.SetNext(segment);
            }

            last = segment;
            runningIndex += length;
        }

        return new ReadOnlySequence<byte>(first!, 0, last!, last!.Memory.Length);
    }

    private sealed class BufferSegment : ReadOnlySequenceSegment<byte>
    {
        public BufferSegment(ReadOnlyMemory<byte> memory, long runningIndex)
        {
            Memory = memory;
            RunningIndex = runningIndex;
        }

        public void SetNext(BufferSegment next) => Next = next;
    }
}
