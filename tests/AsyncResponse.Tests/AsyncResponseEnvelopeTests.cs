using System.Buffers;
using System.Text;
using System.Text.Json;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// The wire envelope every published response travels in. Its custom converter is a persisted
/// contract: it must round-trip payload and failure envelopes, tolerate a JSON <c>null</c> payload
/// on failure envelopes even for a non-nullable value type (assigning <c>default(T)</c> rather
/// than throwing) while rejecting one on success envelopes, bind payload properties
/// case-insensitively like every other broker-ingress read, and skip unknown properties so
/// additive schema changes stay compatible.
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

    [Theory]
    [InlineData("""{"SchemaVersion":1,"Success":"true","Payload":{"Status":2}}""")]
    [InlineData("""{"SchemaVersion":1,"Success":1,"Payload":{"Status":2}}""")]
    [InlineData("""{"SchemaVersion":1,"Success":null,"Payload":{"Status":2}}""")]
    public void WrongTypedSuccess_ThrowsJsonException_NotInvalidOperation(string json)
    {
        // Regression (r24): GetBoolean on a non-boolean token threw InvalidOperationException,
        // which the ingress retry predicate (`ex is not (JsonException or InvalidDataException)`)
        // classified as TRANSIENT — a permanently malformed envelope burned the full re-parse
        // retry budget before terminally failing the flow. A malformed envelope must fail fast
        // as a JsonException, like every sibling property in this converter.
        Assert.Throws<JsonException>(() => JsonSerializer.Deserialize<AsyncResponseEnvelope<OperationResult>>(
            json, AsyncResponseEnvelopeOptions<OperationResult>.Instance));
    }

    [Theory]
    [InlineData("""{"SchemaVersion":1,"Success":true,"Payload":null,"ExceptionMessage":123}""")]
    [InlineData("""{"SchemaVersion":1,"Success":true,"Payload":null,"ExceptionStackTrace":false}""")]
    public void WrongTypedExceptionText_ThrowsJsonException_NotInvalidOperation(string json)
        => Assert.Throws<JsonException>(() => JsonSerializer.Deserialize<AsyncResponseEnvelope<OperationResult>>(
            json, AsyncResponseEnvelopeOptions<OperationResult>.Instance));

    [Fact]
    public void NullPayload_OnAFailureEnvelope_BecomesDefault()
    {
        // The failure envelope's routine shape: the converter assigns default(int) instead of
        // throwing on its JSON null payload, even for a non-nullable value type.
        var restored = JsonSerializer.Deserialize<AsyncResponseEnvelope<int>>(
            """{"SchemaVersion":1,"Success":false,"Payload":null,"ExceptionMessage":"boom"}""",
            AsyncResponseEnvelopeOptions<int>.Instance);

        Assert.NotNull(restored);
        Assert.False(restored!.Success);
        Assert.Equal(0, restored.Payload);
        Assert.Equal("boom", restored.ExceptionMessage);
    }

    [Theory]
    [InlineData("""{"SchemaVersion":1,"Success":true,"Payload":null}""")]
    [InlineData("""{"SchemaVersion":1,"Payload":null,"Success":true}""")]
    public void NullPayload_OnASuccessEnvelope_ThrowsJsonException(string json)
    {
        // No publisher ever writes Success=true with a null Payload — the shape only arises from
        // producer-side garbage (typically a raw ingress body of literal `null` wrapped verbatim).
        // Accepting it completed the waiter with a null payload that surfaced as an NRE at the
        // consumer, far from the message; it must fail fast instead, in either property order.
        Assert.Throws<JsonException>(() => JsonSerializer.Deserialize<AsyncResponseEnvelope<OperationResult>>(
            json, AsyncResponseEnvelopeOptions<OperationResult>.Instance));
        Assert.Throws<JsonException>(() => JsonSerializer.Deserialize<AsyncResponseEnvelope<int>>(
            json, AsyncResponseEnvelopeOptions<int>.Instance));
    }

    [Fact]
    public void PayloadProperties_BindCaseInsensitively()
    {
        // External producers publish payload JSON in their own casing; the raw ingress wraps those
        // bytes verbatim into the envelope. Binding them case-sensitively silently completed the
        // waiter with an all-default payload — the envelope's own fields matched, so nothing
        // signaled the loss. The payload must bind like every other broker-ingress read.
        var restored = JsonSerializer.Deserialize<AsyncResponseEnvelope<OperationResult>>(
            """{"SchemaVersion":1,"Success":true,"Payload":{"status":2,"message":"done"}}""",
            AsyncResponseEnvelopeOptions<OperationResult>.Instance);

        Assert.NotNull(restored);
        Assert.Equal(OperationStatus.Completed, restored!.Payload!.Status);
        Assert.Equal("done", restored.Payload.Message);
    }

    [Fact]
    public void EnvelopeFields_StayByteExact_DespiteCaseInsensitivePayloadBinding()
    {
        // The converter matches the envelope's OWN properties byte-exact regardless of the
        // options flag: a case-variant SchemaVersion is an unknown property, and its absence is
        // rejected — the envelope contract does not loosen with the payload binding.
        Assert.Throws<JsonException>(() => JsonSerializer.Deserialize<AsyncResponseEnvelope<OperationResult>>(
            """{"schemaVersion":1,"Success":true,"Payload":{"Status":2}}""",
            AsyncResponseEnvelopeOptions<OperationResult>.Instance));
    }

    [Fact]
    public void TypeInfo_ReturnsTheSameInstanceAcrossCalls()
    {
        // The per-T memo on the publish/dispatch hot path: same metadata instance every call.
        Assert.Same(
            AsyncResponseEnvelopeJson.TypeInfo<OperationResult>(),
            AsyncResponseEnvelopeJson.TypeInfo<OperationResult>());
    }

    [Fact]
    public void UnknownProperties_AreSkipped()
    {
        var restored = JsonSerializer.Deserialize<AsyncResponseEnvelope<OperationResult>>(
            """{"SchemaVersion":1,"Success":true,"Payload":{"Status":2},"Unknown":{"nested":true},"Extra":42}""",
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
