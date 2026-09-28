using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Telemetry cannot change an outcome (round 48): a waiter's <c>asyncresponse.wait</c> span ends
/// in its one-shot cleanup, and an ActivityListener whose stopped callback threw used to fault that
/// cleanup. The faulted task is cached, so every later dispose of the waiter rethrew the listener's
/// exception — and on the in-memory channel the publisher that completed the waiter saw it too.
/// </summary>
public sealed class WaitSpanStopRegressionTests
{
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(10);

    [Fact]
    public void StopActivity_WhoseStoppedListenerThrows_EndsTheSpan_AndRestoresTheAmbientOne()
    {
        using var telemetry = new ThrowingSpanStopListener("asyncresponse.test.stop_throws");
        var ambient = Activity.Current;
        var span = AsyncResponseDiagnostics.StartActivity("asyncresponse.test.stop_throws");
        Assert.NotNull(span);
        Assert.Same(span, Activity.Current);

        AsyncResponseDiagnostics.StopActivity(span);

        Assert.Equal(1, telemetry.Thrown);
        Assert.True(span.IsStopped);
        Assert.Same(ambient, Activity.Current);
        AsyncResponseDiagnostics.StopActivity(null); // nothing sampled: nothing to end
    }

    [Fact]
    public async Task InMemory_WaiterDisposal_WithAThrowingWaitSpanListener_DoesNotThrow_OnAnyDispose()
    {
        using var telemetry = new ThrowingSpanStopListener();
        var channel = CreateInMemoryChannel();
        var waiter = await channel.CreateResponseWaiter<OperationResult>("span-dispose");

        await waiter.DisposeAsync().AsTask().WaitAsync(Wait);
        await waiter.DisposeAsync().AsTask().WaitAsync(Wait);

        Assert.Equal(1, telemetry.Thrown);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => waiter.ResponseTask);
    }

    [Fact]
    public async Task InMemory_ADeliveredResponse_WithAThrowingWaitSpanListener_ReachesTheWaiter_AndThePublisherSucceeds()
    {
        using var telemetry = new ThrowingSpanStopListener();
        var channel = CreateInMemoryChannel();
        var waiter = await channel.CreateResponseWaiter<OperationResult>("span-deliver");

        await channel.SetResponse(new OperationResult { Status = OperationStatus.Completed, Message = "done" }, "span-deliver")
            .WaitAsync(Wait);
        var response = await waiter.ResponseTask.WaitAsync(Wait);
        await waiter.DisposeAsync().AsTask().WaitAsync(Wait);

        Assert.Equal("done", response.Message);
        Assert.Equal(1, telemetry.Thrown);
    }

    [Fact]
    public async Task InMemory_AbandonedWaiter_WithAThrowingWaitSpanListener_IsAbandonedWithoutThrowing()
    {
        using var telemetry = new ThrowingSpanStopListener();
        var channel = CreateInMemoryChannel();
        var waiter = await channel.CreateResponseWaiter<OperationResult>("span-abandon");

        await channel.AbandonAllAsync().AsTask().WaitAsync(Wait);

        Assert.Equal(1, telemetry.Thrown);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => waiter.ResponseTask);
        await waiter.DisposeAsync().AsTask().WaitAsync(Wait);
    }

    private static InMemoryAsyncResponseChannel CreateInMemoryChannel()
    {
        var store = new Mock<IRecoveryStateStore>();
        store.Setup(instance => instance.TryDeleteAsync(It.IsAny<string>(), It.IsAny<Guid>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);
        return new InMemoryAsyncResponseChannel(
            new ServiceCollection().BuildServiceProvider().GetRequiredService<IServiceScopeFactory>(),
            store.Object,
            Options.Create(new InMemoryAsyncResponseOptions
            {
                DefaultTimeout = TimeSpan.FromMinutes(1),
                RecoveryStateExpiry = TimeSpan.FromMinutes(1)
            }),
            new AsyncResponseContextPropagation([]),
            NullLogger<InMemoryAsyncResponseChannel>.Instance);
    }
}
