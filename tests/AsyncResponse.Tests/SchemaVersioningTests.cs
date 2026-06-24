using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Wire/recovery-state schema versioning (P5): every persisted or queued shape carries a version,
/// and a reader rejects anything stamped newer than it understands rather than silently
/// misinterpreting it after a mixed-version deploy.
/// </summary>
public class SchemaVersioningTests
{
    [Fact]
    public void Schemas_RejectNewerVersions_AcceptCurrentAndOlder()
    {
        Assert.True(RecoveryStateSchema.IsReadable(RecoveryStateSchema.Current));
        Assert.True(RecoveryStateSchema.IsReadable(0));
        Assert.False(RecoveryStateSchema.IsReadable(RecoveryStateSchema.Current + 1));

        Assert.True(WorkerJobEnvelopeSchema.IsReadable(WorkerJobEnvelopeSchema.Current));
        Assert.False(WorkerJobEnvelopeSchema.IsReadable(WorkerJobEnvelopeSchema.Current + 1));

        Assert.True(AsyncResponseEnvelopeSchema.IsReadable(AsyncResponseEnvelopeSchema.Current));
        Assert.False(AsyncResponseEnvelopeSchema.IsReadable(AsyncResponseEnvelopeSchema.Current + 1));
    }

    [Fact]
    public async Task InMemoryStore_GetAsync_RejectsNewerSchemaVersion()
    {
        var store = new InMemoryRecoveryStateStore();
        await store.SaveAsync(
            "cid",
            new RecoveryState { CorrelationId = "cid", SchemaVersion = RecoveryStateSchema.Current + 1 },
            TimeSpan.FromMinutes(5));

        Assert.Null(await store.GetAsync("cid"));
    }

    [Fact]
    public async Task InMemoryStore_ScanAsync_SkipsNewerSchemaVersion()
    {
        var store = new InMemoryRecoveryStateStore();
        await store.SaveAsync(
            "cid-new",
            new RecoveryState { CorrelationId = "cid-new", SchemaVersion = RecoveryStateSchema.Current + 1 },
            TimeSpan.FromMinutes(5));
        await store.SaveAsync(
            "cid-ok",
            new RecoveryState { CorrelationId = "cid-ok", SchemaVersion = RecoveryStateSchema.Current },
            TimeSpan.FromMinutes(5));

        var scanned = new List<RecoveryState>();
        await foreach (var state in store.ScanAsync())
            scanned.Add(state);

        Assert.Single(scanned);
        Assert.Equal("cid-ok", scanned[0].CorrelationId);
    }

    [Fact]
    public async Task WorkerJobExecutor_RejectsNewerSchemaVersion()
    {
        await using var provider = new ServiceCollection().BuildServiceProvider();
        var executor = new WorkerJobExecutor(
            provider.GetRequiredService<IServiceScopeFactory>(),
            NullLogger<WorkerJobExecutor>.Instance);

        var job = new WorkerJobEnvelope
        {
            SchemaVersion = WorkerJobEnvelopeSchema.Current + 1,
            Call = new ReflectionCallDto { ServiceInterfaceFullName = "Svc", MethodName = "M", Params = [] },
            CorrelationId = "cid"
        };

        await Assert.ThrowsAsync<InvalidOperationException>(() => executor.ExecuteAsync(job));
    }

    [Fact]
    public void Envelope_RoundTrips_SchemaVersion()
    {
        var json = JsonSerializer.Serialize(
            new AsyncResponseEnvelope<OperationResult> { Success = true, Payload = new OperationResult { Status = OperationStatus.Completed } },
            AsyncResponseEnvelopeOptions<OperationResult>.Instance);

        Assert.Contains("\"SchemaVersion\":1", json);

        var envelope = JsonSerializer.Deserialize<AsyncResponseEnvelope<OperationResult>>(
            json, AsyncResponseEnvelopeOptions<OperationResult>.Instance);

        Assert.NotNull(envelope);
        Assert.Equal(AsyncResponseEnvelopeSchema.Current, envelope!.SchemaVersion);
    }

    [Fact]
    public void Envelope_MissingSchemaVersion_ReadsAsCurrent()
    {
        // A legacy envelope written before the field existed must still be accepted.
        const string legacyJson = """{"Success":true,"Payload":{"Status":2},"ExceptionMessage":null,"ExceptionStackTrace":null}""";

        var envelope = JsonSerializer.Deserialize<AsyncResponseEnvelope<OperationResult>>(
            legacyJson, AsyncResponseEnvelopeOptions<OperationResult>.Instance);

        Assert.NotNull(envelope);
        Assert.Equal(AsyncResponseEnvelopeSchema.Current, envelope!.SchemaVersion);
        Assert.True(AsyncResponseEnvelopeSchema.IsReadable(envelope.SchemaVersion));
    }

    [Fact]
    public void Envelope_NewerSchemaVersion_DeserializesButIsNotReadable()
    {
        const string newerJson = """{"SchemaVersion":999,"Success":true,"Payload":{"Status":2},"ExceptionMessage":null,"ExceptionStackTrace":null}""";

        var envelope = JsonSerializer.Deserialize<AsyncResponseEnvelope<OperationResult>>(
            newerJson, AsyncResponseEnvelopeOptions<OperationResult>.Instance);

        Assert.NotNull(envelope);
        Assert.Equal(999, envelope!.SchemaVersion);
        Assert.False(AsyncResponseEnvelopeSchema.IsReadable(envelope.SchemaVersion));
    }
}
