using AsyncResponse.Testing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;

namespace AsyncResponse.Tests;

// ---------------------------------------------------------------------------------------------
// Delayed worker delivery mechanics: the builder's capability check, the due-time stamp, the
// executor's NotBeforeUtc early-delivery guard (the chunk chain every capped transport rides),
// and the in-memory transport's timer wheel.
// ---------------------------------------------------------------------------------------------

public class DelayedWorkerTransportTests
{
    private sealed class NonDelayedTransport : IWorkerTransport
    {
        public List<WorkerJobEnvelope> Published { get; } = [];

        public Task PublishAsync(WorkerJobEnvelope job, CancellationToken cancellationToken = default)
        {
            Published.Add(job);
            return Task.CompletedTask;
        }
    }

    private static ReflectionCallDto Work()
        => CallbackExpressionConverter.ToReflectionCall<IDeferredWorkAudit>(target => target.RanAsync("job"));

    [Fact]
    public async Task DelayedEnqueue_OnANonDelayedTransport_FailsWithGuidance()
    {
        var transport = new NonDelayedTransport();
        var builder = new AsyncResponseBuilder(
            new InMemoryAsyncResponseChannel(
                new ServiceCollection().BuildServiceProvider().GetRequiredService<IServiceScopeFactory>(),
                new InMemoryRecoveryStateStore(),
                Options.Create(new InMemoryAsyncResponseOptions()),
                new AsyncResponseContextPropagation([]),
                NullLogger<InMemoryAsyncResponseChannel>.Instance),
            transport,
            propagation: new AsyncResponseContextPropagation([]));

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => builder.EnqueueWorkerAsync(Work(), TimeSpan.FromMinutes(5)));

        Assert.Contains(nameof(IDelayedWorkerTransport), ex.Message, StringComparison.Ordinal);
        Assert.Contains("DelayAsync", ex.Message, StringComparison.Ordinal);
        Assert.Empty(transport.Published);
    }

    [Fact]
    public async Task NonPositiveDelay_PublishesImmediately_EvenOnANonDelayedTransport()
    {
        var transport = new NonDelayedTransport();
        var builder = new AsyncResponseBuilder(
            new InMemoryAsyncResponseChannel(
                new ServiceCollection().BuildServiceProvider().GetRequiredService<IServiceScopeFactory>(),
                new InMemoryRecoveryStateStore(),
                Options.Create(new InMemoryAsyncResponseOptions()),
                new AsyncResponseContextPropagation([]),
                NullLogger<InMemoryAsyncResponseChannel>.Instance),
            transport,
            propagation: new AsyncResponseContextPropagation([]));

        await builder.EnqueueWorkerAsync(Work(), TimeSpan.Zero);
        await builder.EnqueueWorkerAsync(Work(), TimeSpan.FromSeconds(-5));

        Assert.Equal(2, transport.Published.Count);
        Assert.All(transport.Published, job => Assert.Null(job.NotBeforeUtc));
    }

    [Fact]
    public async Task DelayedEnqueue_StampsTheAbsoluteDueTime()
    {
        var audit = new RecordingDeferredWorkAudit();
        await using var harness = await AsyncResponseTestHarness.StartAsync(options =>
            options.ConfigureServices = services => services.AddSingleton<IDeferredWorkAudit>(audit));

        await harness.Builder.EnqueueWorkerAsync<IDeferredWorkAudit>(
            worker => worker.RanAsync("stamped"),
            TimeSpan.FromMinutes(30));

        var transport = (InMemoryWorkerTransport)harness.Services.GetRequiredService<IWorkerTransport>();
        var pending = Assert.Single(transport.SnapshotDelayedJobs());
        Assert.Equal(
            harness.Clock.GetUtcNow().UtcDateTime + TimeSpan.FromMinutes(30),
            pending.NotBeforeUtc);
    }

    [Fact]
    public async Task EarlyDeliveredJob_IsRedelayedByTheExecutor_NotExecuted()
    {
        var audit = new RecordingDeferredWorkAudit();
        await using var harness = await AsyncResponseTestHarness.StartAsync(options =>
            options.ConfigureServices = services => services.AddSingleton<IDeferredWorkAudit>(audit));

        // Simulate a chunk hop arriving early: an IMMEDIATE publish whose envelope says "due in
        // an hour" (what an SQS 15-minute hop looks like at minute 15 of a 75-minute delay).
        var transport = harness.Services.GetRequiredService<IWorkerTransport>();
        await transport.PublishAsync(new WorkerJobEnvelope
        {
            Call = Work(),
            NotBeforeUtc = harness.Clock.GetUtcNow().UtcDateTime.AddHours(1)
        });

        await harness.WaitForWorkerIdleAsync();
        Assert.Empty(audit.Ran);

        // The executor re-scheduled the remainder on the transport's timer wheel; crossing the
        // due time runs it exactly once.
        await harness.AdvanceAsync(TimeSpan.FromHours(1));
        await harness.WaitForWorkerIdleAsync();
        Assert.Equal(["job"], audit.Ran);
    }

    [Fact]
    public async Task DelayBeyondThePersistenceCeiling_IsRejected()
    {
        await using var harness = await AsyncResponseTestHarness.StartAsync();

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            harness.Builder.EnqueueWorkerAsync<IDeferredWorkAudit>(
                worker => worker.RanAsync("never"),
                TimeSpan.FromDays(5000)));
    }

    [Fact]
    public async Task DelayedTransports_AdvertiseTheirPerHopCaps()
    {
        await using var harness = await AsyncResponseTestHarness.StartAsync();
        var inMemory = Assert.IsAssignableFrom<IDelayedWorkerTransport>(harness.Services.GetRequiredService<IWorkerTransport>());
        Assert.True(inMemory.MaxPublishDelay > TimeSpan.FromDays(1));

        // The SQS transport chunks at the DelaySeconds ceiling.
        Assert.Equal(TimeSpan.FromSeconds(900), AsyncResponse.Transports.SQS.SqsWorkerTransport.SqsMaxDelay);
    }
}
