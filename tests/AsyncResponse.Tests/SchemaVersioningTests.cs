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

    [Theory]
    [InlineData("cid ")]
    [InlineData(" cid")]
    [InlineData("looooong")]
    // "surrogate" is a shape, not a literal: xUnit's theory-argument serialization would replace
    // an unpaired surrogate with U+FFFD before the test ever saw it.
    [InlineData("surrogate")]
    public async Task WorkerJobExecutor_RejectsANonPortableCorrelationId_BeforeRunningTheHandler(string correlationId)
    {
        // A correlation id arriving over a broker gets the same contract as one handed to a
        // publisher, and it has to be checked HERE — before the handler runs. Otherwise the job
        // executes, its implicit response publish throws on the id, the transport redelivers, and
        // the handler's side effects happen again on every attempt until the job dead-letters.
        // Rejecting first turns that into an ordinary poison message.
        correlationId = correlationId switch
        {
            "looooong" => new string('c', AsyncResponseChannelOptions.MaxCorrelationIdLength + 1),
            "surrogate" => "cid-\ud800",
            _ => correlationId
        };

        var probe = new HandlerProbe();
        var services = new ServiceCollection();
        services.AddSingleton(probe);
        services.AddSingleton<IProbeService>(probe);
        await using var provider = services.BuildServiceProvider();
        var executor = new WorkerJobExecutor(
            provider.GetRequiredService<IServiceScopeFactory>(),
            NullLogger<WorkerJobExecutor>.Instance);

        var job = new WorkerJobEnvelope
        {
            Call = new ReflectionCallDto
            {
                ServiceInterfaceFullName = typeof(IProbeService).FullName!,
                MethodName = nameof(IProbeService.RunAsync),
                Params = []
            },
            CorrelationId = correlationId
        };

        await Assert.ThrowsAsync<InvalidOperationException>(() => executor.ExecuteAsync(job));
        Assert.Equal(0, probe.Invocations);
    }

    [Theory]
    // null: a fire-and-forget job, which has no response to publish.
    [InlineData(null)]
    // ...and an ordinary, perfectly valid id, which is the false-positive guard: the new check must
    // reject only ids that break the contract, not every job that carries one.
    [InlineData("cid-ok")]
    public async Task WorkerJobExecutor_StillRunsAJobWhoseCorrelationIdIsFine(string? correlationId)
    {
        var probe = new HandlerProbe();
        var services = new ServiceCollection();
        services.AddSingleton(probe);
        services.AddSingleton<IProbeService>(probe);
        await using var provider = services.BuildServiceProvider();
        var executor = new WorkerJobExecutor(
            provider.GetRequiredService<IServiceScopeFactory>(),
            NullLogger<WorkerJobExecutor>.Instance);

        await executor.ExecuteAsync(new WorkerJobEnvelope
        {
            Call = new ReflectionCallDto
            {
                ServiceInterfaceFullName = typeof(IProbeService).FullName!,
                MethodName = nameof(IProbeService.RunAsync),
                Params = []
            },
            CorrelationId = correlationId
        });

        Assert.Equal(1, probe.Invocations);
    }

    public interface IProbeService
    {
        Task RunAsync();
    }

    private sealed class HandlerProbe : IProbeService
    {
        private int _invocations;

        public int Invocations => Volatile.Read(ref _invocations);

        public Task RunAsync()
        {
            Interlocked.Increment(ref _invocations);
            return Task.CompletedTask;
        }
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
