using System.Text.Json;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// <see cref="WorkerJobEnvelope.JobId"/> on the wire: additive, carried by both serialization
/// paths byte for byte, and absent — not defaulted — on jobs written before it existed, which is
/// what keeps those on the previous evidence-based duplicate handling.
/// </summary>
public sealed class WorkerJobEnvelopeJobIdWireTests
{
    private static WorkerJobEnvelope Job(string? jobId) => new()
    {
        Call = new ReflectionCallDto
        {
            ServiceInterfaceFullName = "My.App.IWorker",
            MethodName = "RunAsync",
            Params = [CallbackParam.ForValue("job-7")]
        },
        CorrelationId = "cid-9",
        JobId = jobId
    };

    [Fact]
    public void JobId_RoundTrips_AndSourceGenMatchesReflection_ByteForByte()
    {
        var job = Job("3f2b8c1d9e4a4f0c8b7a6d5e4c3b2a19");

        var reflectionBaseline = JsonSerializer.Serialize(job);

        Assert.Contains("\"JobId\":\"3f2b8c1d9e4a4f0c8b7a6d5e4c3b2a19\"", reflectionBaseline, StringComparison.Ordinal);
        Assert.Equal(reflectionBaseline, AsyncResponseJson.Serialize(job));
        Assert.Equal("3f2b8c1d9e4a4f0c8b7a6d5e4c3b2a19", JsonSafety.SafeDeserialize<WorkerJobEnvelope>(reflectionBaseline)!.JobId);
        Assert.Equal("3f2b8c1d9e4a4f0c8b7a6d5e4c3b2a19", JsonSerializer.Deserialize<WorkerJobEnvelope>(reflectionBaseline)!.JobId);
    }

    [Fact]
    public void PinnedPayload_WrittenBeforeJobIdExisted_DeserializesWithoutOne()
    {
        // A producer on an older build must interop with a newer consumer: this literal keeps
        // deserializing to a null JobId forever, and a null JobId means "no identity" — never a
        // shared default that would make every legacy job look like the same job.
        const string legacyJson =
            """
            {
              "SchemaVersion": 1,
              "Call": { "ServiceInterfaceFullName": "AsyncResponse.IDurableFlowExecutor", "MethodName": "ExecuteAsync", "Params": [] },
              "CorrelationId": "legacy-corr",
              "NotBeforeUtc": "2026-05-01T08:00:00Z",
              "RedelayStallCount": 1
            }
            """;

        var restored = JsonSafety.SafeDeserialize<WorkerJobEnvelope>(legacyJson);

        Assert.NotNull(restored);
        Assert.Null(restored!.JobId);
        Assert.Equal("legacy-corr", restored.CorrelationId);
        Assert.Null(FlowLeaseContention.JobTag(restored.JobId));
    }

    [Fact]
    public void ProducerSideSizeEstimate_CountsTheJobId()
    {
        // The estimate gates the exact measurement against MaxInboundMessageChars and must never
        // undercount: every string at its fully-escaped size (six characters per code unit).
        var without = Job(null);
        var with = Job(new string('"', 1_000));

        Assert.True(AsyncResponseBuilderBase.TryEstimateUpperBound(without, out var baseline));
        Assert.True(AsyncResponseBuilderBase.TryEstimateUpperBound(with, out var grown));

        Assert.True(grown - baseline >= 5_990, $"The estimate grew by {grown - baseline} for a 1000-character JobId that serializes to 6000.");
        Assert.True(grown >= AsyncResponseJson.Serialize(with).Length);
    }
}
