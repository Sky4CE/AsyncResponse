using Xunit;

namespace AsyncResponse.Tests;

public sealed partial class DbChannelSharedCoverageTests
{
    /// <summary>
    /// Telemetry cannot change an outcome (round 48): the DB channels end a waiter's
    /// <c>asyncresponse.wait</c> span in its one-shot cleanup, and an ActivityListener whose stopped
    /// callback threw faulted that cleanup — cached, so every later dispose rethrew it. The shared
    /// source is the same in every DB channel assembly; the Mongo harness drives it without a server.
    /// </summary>
    [Fact]
    public async Task WaiterDisposal_WithAThrowingWaitSpanListener_DoesNotThrow_OnAnyDispose()
    {
        using var telemetry = new ThrowingSpanStopListener();
        await using var harness = Harness.Create(Provider.MongoDb, failing: false, pollInterval: TimeSpan.FromSeconds(30));
        var waiter = await ((IAsyncResponseSubscriber)harness.Channel).CreateResponseWaiter<OperationResult>("span-db-dispose");

        await waiter.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10));
        await waiter.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Equal(1, telemetry.Thrown);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => waiter.ResponseTask);
    }
}
