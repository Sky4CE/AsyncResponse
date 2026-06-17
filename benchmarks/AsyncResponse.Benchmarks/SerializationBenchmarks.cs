using System.Text.Json;
using BenchmarkDotNet.Attributes;

namespace AsyncResponse.Benchmarks;

/// <summary>
/// The wire/classification hot paths exercised on every delivered response: envelope (de)serialize
/// and payload recovery-routing classification (typed vs. broker-delivered raw JSON).
/// </summary>
[MemoryDiagnoser]
public class SerializationBenchmarks
{
    private static readonly BenchPayload Payload = new() { Status = BenchStatus.Completed, Message = "done" };

    private string _envelopeJson = null!;
    private JsonElement _rawJson;
    private string _payloadTypeName = null!;

    [GlobalSetup]
    public void Setup()
    {
        _envelopeJson = JsonSerializer.Serialize(
            new AsyncResponseEnvelope<BenchPayload> { Success = true, Payload = Payload },
            AsyncResponseEnvelopeOptions<BenchPayload>.Instance);
        _rawJson = JsonSerializer.SerializeToElement(Payload);
        _payloadTypeName = typeof(BenchPayload).FullName!;
    }

    [Benchmark]
    public string Envelope_Serialize()
        => JsonSerializer.Serialize(
            new AsyncResponseEnvelope<BenchPayload> { Success = true, Payload = Payload },
            AsyncResponseEnvelopeOptions<BenchPayload>.Instance);

    [Benchmark]
    public BenchStatus Envelope_Deserialize()
        => JsonSerializer.Deserialize<AsyncResponseEnvelope<BenchPayload>>(
            _envelopeJson, AsyncResponseEnvelopeOptions<BenchPayload>.Instance)!.Payload!.Status;

    [Benchmark]
    public bool? Classify_TypedPayload()
        => PayloadRecoveryClassifier.ShouldResume(Payload, _payloadTypeName);

    [Benchmark]
    public bool? Classify_RawJson()
        => PayloadRecoveryClassifier.ShouldResume(_rawJson, _payloadTypeName);
}
