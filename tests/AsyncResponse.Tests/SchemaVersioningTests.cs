using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Wire/recovery-state schema versioning (P5): every persisted or queued shape carries a version,
/// and a reader rejects missing or unsupported stamps rather than silently misinterpreting a
/// payload after a mixed-version deploy.
/// </summary>
public class SchemaVersioningTests
{
    [Fact]
    public void Schemas_AcceptOnlyCurrentVersion()
    {
        Assert.True(RecoveryStateSchema.IsReadable(RecoveryStateSchema.Current));
        Assert.False(RecoveryStateSchema.IsReadable(0));
        Assert.False(RecoveryStateSchema.IsReadable(RecoveryStateSchema.Current + 1));

        Assert.True(WorkerJobEnvelopeSchema.IsReadable(WorkerJobEnvelopeSchema.Current));
        Assert.False(WorkerJobEnvelopeSchema.IsReadable(0));
        Assert.False(WorkerJobEnvelopeSchema.IsReadable(WorkerJobEnvelopeSchema.Current + 1));

        Assert.True(AsyncResponseEnvelopeSchema.IsReadable(AsyncResponseEnvelopeSchema.Current));
        Assert.False(AsyncResponseEnvelopeSchema.IsReadable(0));
        Assert.False(AsyncResponseEnvelopeSchema.IsReadable(AsyncResponseEnvelopeSchema.Current + 1));

        Assert.True(FlowStateSchema.IsReadable(FlowStateSchema.Current));
        Assert.False(FlowStateSchema.IsReadable(0));
        Assert.False(FlowStateSchema.IsReadable(FlowStateSchema.Current + 1));
    }

    [Fact]
    public async Task InMemoryStore_RejectsUnrecognizedSchemaVersionOnWrite()
    {
        var store = new InMemoryRecoveryStateStore();
        await Assert.ThrowsAsync<ArgumentException>(() => store.SaveAsync(
            "cid",
            new RecoveryState { CorrelationId = "cid", SchemaVersion = RecoveryStateSchema.Current + 1 },
            TimeSpan.FromMinutes(5)));
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
    public void PersistedContracts_MissingSchemaVersion_AreRejected()
    {
        Assert.Throws<JsonException>(() => JsonSerializer.Deserialize<RecoveryState>("{}"));
        Assert.Throws<JsonException>(() => JsonSerializer.Deserialize<FlowState>("{}"));
        Assert.Throws<JsonException>(() => JsonSerializer.Deserialize<WorkerJobEnvelope>("{}"));

        const string envelopeJson = """{"Success":true,"Payload":{"Status":2},"ExceptionMessage":null,"ExceptionStackTrace":null}""";
        Assert.Throws<JsonException>(() => JsonSerializer.Deserialize<AsyncResponseEnvelope<OperationResult>>(
            envelopeJson,
            AsyncResponseEnvelopeOptions<OperationResult>.Instance));
    }

    [Fact]
    public void Envelope_NullSchemaVersion_IsRejected()
    {
        const string json = """{"SchemaVersion":null,"Success":true,"Payload":{"Status":2}}""";

        Assert.Throws<JsonException>(() => JsonSerializer.Deserialize<AsyncResponseEnvelope<OperationResult>>(
            json,
            AsyncResponseEnvelopeOptions<OperationResult>.Instance));
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
