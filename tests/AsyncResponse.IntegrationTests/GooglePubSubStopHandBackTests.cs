using AsyncResponse.Conformance;
using Google.Cloud.PubSub.V1;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace AsyncResponse.IntegrationTests;

/// <summary>
/// Fixpoint r1 (S8#7), against the Pub/Sub emulator: the SDK's <c>StopAsync</c> hands every
/// message still in leasing back at once for any stop timeout under its 30-second hard-stop
/// window — including one whose handler is still running — stops that message's ack-deadline
/// extension and drops its eventual Ack. The subscriber used to call it the moment the host
/// stopped, so on every rolling deploy each in-flight job was redelivered to a peer and ran twice.
/// It now drains the running handlers first. The unit pin
/// (GooglePubSubSubscriberTests.WorkerSubscriberService_Stop_DrainsTheRunningHandlerBeforeStoppingTheClient)
/// covers the ordering; only the real client shows the redelivery it prevents.
/// </summary>
[Collection(MatrixCloudLightCollection.Name)]
[Trait(Batches.Trait, Batches.MatrixCloudLight)]
public sealed class GooglePubSubStopHandBackTests(MatrixCloudLightFixture fixture)
{
    private static readonly MatrixCell Cell = new(MatrixChannel.InMemory, MatrixTransport.GooglePubSub, MatrixStore.InMemory);

    [Fact]
    public async Task AckAfterHandler_StopWhileAHandlerRuns_DoesNotHandTheJobToAPeer()
    {
        var names = new MatrixNames(Cell, Guid.NewGuid().ToString("N"));

        // The holder receives the job and holds it mid-handler. It must not tear the shared
        // subscription down when it stops: the peer below still consumes from it.
        var holder = await CreateHarnessAsync(names, teardownNamespaces: false);
        var holderProbe = holder.Provider.GetRequiredService<TransportProbe>();
        holderProbe.Gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Task? holderStop = null;
        try
        {
            await EnqueueAsync(holder, names.NewCorrelationId("drain"), token: 31);
            await holderProbe.GateReached.Task.WaitAsync(TimeSpan.FromSeconds(45));

            // A peer on the same subscription: whatever the holder hands back, it receives.
            await using var peer = await CreateHarnessAsync(names, teardownNamespaces: true);
            var peerProbe = peer.Provider.GetRequiredService<TransportProbe>();

            // The host stops while the handler is still running. On the old code the SDK nacked the
            // message at once and the emulator redelivered it to the peer within a second or two.
            holderStop = holder.DisposeAsync().AsTask();
            await Task.Delay(TimeSpan.FromSeconds(5));
            Assert.Equal(0, peerProbe.CallCount);

            holderProbe.Gate.TrySetResult();
            await holderStop.WaitAsync(TimeSpan.FromSeconds(60));
            Assert.Equal(1, holderProbe.GatedCompletions);

            // The Ack reached the client before its stop: nothing comes back to the peer, even past
            // the ack deadline a dropped Ack would redeliver after. The streaming client leases
            // with its own deadline (SubscriberClient.DefaultAckDeadline, 60 s — which the harness's
            // AckDeadlineSeconds = 60 subscriptions match), extended until the stop, so a message
            // whose Ack was lost without a Nack reappears at most that long after it.
            await Task.Delay(SubscriberClient.DefaultAckDeadline + TimeSpan.FromSeconds(10));
            Assert.Equal(0, peerProbe.CallCount);
        }
        finally
        {
            // Never leave a gated subscriber alive in the collection process, even when an
            // assertion above failed: release its handler, and stop it if the test did not.
            holderProbe.Gate.TrySetResult();
            try
            {
                await (holderStop ?? holder.DisposeAsync().AsTask()).WaitAsync(TimeSpan.FromSeconds(60));
            }
            catch (Exception) when (holderStop is null || !holderStop.IsCompletedSuccessfully)
            {
                // Best-effort cleanup after a failure; the original assertion is the one to report.
            }
        }
    }

    private Task<MatrixHarness> CreateHarnessAsync(MatrixNames names, bool teardownNamespaces)
        => MatrixHarness.CreateAsync(
            Cell,
            fixture.Backends,
            services =>
            {
                services.AddSingleton<TransportProbe>();
                services.AddSingleton<ITransportWorkerService, TransportWorkerService>();
            },
            tuning: new MatrixTransportTuning { HostShutdownTimeout = TimeSpan.FromSeconds(30) },
            sharedNames: names,
            teardownNamespaces: teardownNamespaces);

    private static async Task EnqueueAsync(MatrixHarness harness, string correlationId, int token)
    {
        AsyncResponseContext.SetCorrelationId(correlationId);
        try
        {
            await harness.Provider.GetRequiredService<IAsyncResponseBuilder>()
                .EnqueueWorkerAsync<ITransportWorkerService>(service => service.HandleAsync(token, false));
        }
        finally
        {
            AsyncResponseContext.ClearCorrelationId();
        }
    }
}
