using AsyncResponse.Transports.NATS;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;

namespace AsyncResponse.Tests;

public class NatsSubscriberServicesTests
{
    private readonly FakeNatsJetStreamTransport _jetStream = new();
    private readonly FakeAsyncResponseIngress _ingress = new();

    private static IOptions<NatsAsyncResponseTransportOptions> Options(Action<NatsAsyncResponseTransportOptions>? configure = null)
    {
        var options = new NatsAsyncResponseTransportOptions
        {
            SubjectPrefix = "itest-nats",
            SubscriberRetryBaseDelay = TimeSpan.FromMilliseconds(1),
            SubscriberRetryMaxDelay = TimeSpan.FromMilliseconds(5)
        };
        configure?.Invoke(options);
        return Microsoft.Extensions.Options.Options.Create(options);
    }

    [Fact]
    public async Task WorkerSubscriber_EnsuresTopology_RoutesToIngress_AndAcks()
    {
        var subscriber = new NatsWorkerSubscriber(Options(), _jetStream, _ingress, new TestLogger<NatsWorkerSubscriber>());
        await subscriber.StartAsync(CancellationToken.None);
        try
        {
            await Eventually(() => _jetStream.EnsuredConsumers.Count > 0);
            Assert.Contains(("itest-nats_transport_worker", "asyncresponse-workers"), _jetStream.EnsuredConsumers);
            Assert.Contains(("itest-nats_transport_worker", "itest-nats.transport.worker"), _jetStream.EnsuredStreams);
            // Dead-letter stream is provisioned because dead-lettering is enabled by default.
            Assert.Contains(("itest-nats_transport_deadletter", "itest-nats.transport.deadletter"), _jetStream.EnsuredStreams);

            var delivery = new RecordingDelivery();
            _jetStream.EnqueueDelivery(delivery.Create("worker-payload", numDelivered: 1));

            await Eventually(() => _ingress.WorkerCount == 1 && delivery.Acks == 1);
            Assert.Equal("worker-payload", _ingress.WorkerMessages[0]);
        }
        finally
        {
            await subscriber.StopAsync(CancellationToken.None);
            subscriber.Dispose();
        }
    }

    [Fact]
    public async Task ResponseIngressSubscriber_ExtractsCorrelationId_AndRoutesToIngress()
    {
        var subscriber = new NatsResponseIngressSubscriber(Options(), _jetStream, _ingress, new TestLogger<NatsResponseIngressSubscriber>());
        await subscriber.StartAsync(CancellationToken.None);
        try
        {
            var headers = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase) { ["AR-Correlation-Id"] = "corr-9" };
            var delivery = new RecordingDelivery();
            _jetStream.EnqueueDelivery(delivery.Create("""{"Status":2}""", numDelivered: 1, subject: "itest-nats.transport.response", headers: headers));

            await Eventually(() => _ingress.ResponseCount == 1 && delivery.Acks == 1);
            Assert.Equal("corr-9", _ingress.ResponseMessages[0].CorrelationId);
            Assert.Equal("""{"Status":2}""", _ingress.ResponseMessages[0].Json);
        }
        finally
        {
            await subscriber.StopAsync(CancellationToken.None);
            subscriber.Dispose();
        }
    }

    [Fact]
    public async Task Subscriber_RetriesAfterTransientConsumerSetupFailure()
    {
        // Fail the first EnsureConsumer attempt; the subscriber backs off and retries, succeeding next time.
        _jetStream.EnsureConsumerFailureForAttempt = attempt => attempt == 1 ? new TimeoutException("transient") : null;
        var subscriber = new NatsWorkerSubscriber(Options(), _jetStream, _ingress, new TestLogger<NatsWorkerSubscriber>());
        await subscriber.StartAsync(CancellationToken.None);
        try
        {
            var delivery = new RecordingDelivery();
            _jetStream.EnqueueDelivery(delivery.Create("after-retry", numDelivered: 1));

            await Eventually(() => _ingress.WorkerCount == 1);
            Assert.Single(_jetStream.EnsuredConsumers); // only the successful (second) attempt is recorded
        }
        finally
        {
            await subscriber.StopAsync(CancellationToken.None);
            subscriber.Dispose();
        }
    }

    [Fact]
    public async Task WorkerSubscriber_SlowHandler_SignalsInProgressForTheMessageInFlight_ThenStops()
    {
        // The AckWait heartbeat: a handler that outlasts AckWait had its own message redelivered
        // to a competing consumer while it was still running, and NumDelivered climbed toward the
        // Term cap on healthy work.
        var ingress = new GatedIngress();
        var first = new RecordingDelivery();
        _jetStream.EnqueueDelivery(first.Create("p1", numDelivered: 1));
        var subscriber = new NatsWorkerSubscriber(
            Options(o => o.AckWait = TimeSpan.FromMilliseconds(300)),
            _jetStream,
            ingress,
            new TestLogger<NatsWorkerSubscriber>());

        await subscriber.StartAsync(CancellationToken.None);
        try
        {
            await ingress.Started.Task.WaitAsync(TimeSpan.FromSeconds(5)); // p1 is wedged in the handler

            // While p1 sits in the handler the ~AckWait/3 heartbeat signals in-progress for it.
            await Eventually(() => first.Progresses >= 1);
            Assert.Equal(0, first.Acks);

            ingress.Release.TrySetResult();
            await Eventually(() => first.Acks == 1);

            // The heartbeat dies with the batch: no further renewals after it settled.
            var firstProgresses = first.Progresses;
            await Task.Delay(400);
            Assert.Equal(firstProgresses, first.Progresses);
        }
        finally
        {
            await subscriber.StopAsync(CancellationToken.None);
            subscriber.Dispose();
        }
    }

    [Fact]
    public async Task WorkerSubscriber_AckAfterHandler_FetchesOneMessageAtATime()
    {
        // A fetch consumes the delivery count of EVERY message it returns, whether or not a
        // handler ever ran: a message that kills the process took its prefetched batch-mates with
        // it, and they were dead-lettered as "max delivery attempts exceeded" without ever being
        // executed. Where the handler decides settlement, only the message actually handed to it
        // may burn an attempt — so the fetch asks for exactly one.
        var ingress = new GatedIngress();
        var first = new RecordingDelivery();
        var second = new RecordingDelivery();
        _jetStream.EnqueueDelivery(first.Create("p1", numDelivered: 1));
        _jetStream.EnqueueDelivery(second.Create("p2", numDelivered: 1));
        var subscriber = new NatsWorkerSubscriber(
            Options(o => o.WorkerSubscriber.BatchSize = 16),
            _jetStream,
            ingress,
            new TestLogger<NatsWorkerSubscriber>());

        await subscriber.StartAsync(CancellationToken.None);
        try
        {
            await ingress.Started.Task.WaitAsync(TimeSpan.FromSeconds(5));

            // p1 is in the handler; p2 has NOT been fetched, so it is still the server's to give
            // to an idle peer — and its delivery count is untouched by p1's fate.
            Assert.Equal(0, second.Progresses);
            Assert.Equal(0, second.Acks);
            Assert.All(_jetStream.FetchSizes, size => Assert.Equal(1, size));

            ingress.Release.TrySetResult();
            await Eventually(() => first.Acks == 1 && second.Acks == 1);
            Assert.All(_jetStream.FetchSizes, size => Assert.Equal(1, size));
        }
        finally
        {
            ingress.Release.TrySetResult();
            await subscriber.StopAsync(CancellationToken.None);
            subscriber.Dispose();
        }
    }

    [Fact]
    public async Task WorkerSubscriber_FastBatch_SignalsNoInProgress()
    {
        // Default AckWait (30s) puts the first heartbeat sweep at ~10s: a batch settled quickly
        // must never pay a renewal round trip.
        var subscriber = new NatsWorkerSubscriber(Options(), _jetStream, _ingress, new TestLogger<NatsWorkerSubscriber>());
        await subscriber.StartAsync(CancellationToken.None);
        try
        {
            var delivery = new RecordingDelivery();
            _jetStream.EnqueueDelivery(delivery.Create("fast", numDelivered: 1));

            await Eventually(() => delivery.Acks == 1);
            Assert.Equal(0, delivery.Progresses);
        }
        finally
        {
            await subscriber.StopAsync(CancellationToken.None);
            subscriber.Dispose();
        }
    }

    /// <summary>Wedges the FIRST worker message in the handler until released; later ones pass through.</summary>
    private sealed class GatedIngress : IAsyncResponseIngress
    {
        private int _calls;

        public TaskCompletionSource Started { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public async Task HandleWorkerMessageAsync(string messageJson)
        {
            if (Interlocked.Increment(ref _calls) == 1)
            {
                Started.TrySetResult();
                await Release.Task;
            }
        }

        public Task HandleResponseMessageAsync(string messageJson, string? correlationId = null)
            => Task.CompletedTask;
    }

    [Fact]
    public async Task WorkerSubscriber_InvalidOptions_FailHostStartupSynchronously()
    {
        // Red-on-old (Hosting 10.0.10+): validation used to sit at the top of ExecuteAsync, which
        // BackgroundService.StartAsync no longer runs inline — StartAsync returned without
        // throwing and the misconfiguration surfaced late or never. Validation now runs in
        // StartAsync so a misconfigured subscriber fails host startup synchronously.
        var subscriber = new NatsWorkerSubscriber(
            Options(o => o.WorkerSubscriber.AckMode = NatsAckMode.AckAfterEnqueue),
            _jetStream,
            _ingress,
            new TestLogger<NatsWorkerSubscriber>());

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => subscriber.StartAsync(CancellationToken.None));
        Assert.Contains("BackgroundWorkerCount", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// Round 39: the in-progress heartbeat ran with <c>CancellationToken.None</c> and the batch's
    /// cleanup joined the renewal loop without a bound, so ONE heartbeat wedged on a dead socket
    /// kept the batch pending after every message in it had settled — no further batch was
    /// fetched, a stop never completed, and the supervisor had nothing to restart. The token now
    /// reaches the heartbeat: a client-side stall aborts with it and the batch completes at once.
    /// Pre-fix: the second delivery is never fetched.
    /// </summary>
    [Fact]
    public async Task WorkerSubscriber_AHeartbeatThatHonorsCancellation_IsAbortedWhenTheBatchSettles()
    {
        var ingress = new GatedIngress();
        var first = new RecordingDelivery
        {
            // The heartbeat stalls until its token is cancelled.
            ProgressBehavior = async cancellationToken =>
            {
                var stalled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                using var registration = cancellationToken.Register(() => stalled.TrySetCanceled(cancellationToken));
                await stalled.Task;
            }
        };
        var second = new RecordingDelivery();
        _jetStream.EnqueueDelivery(first.Create("p1", numDelivered: 1));
        var subscriber = new NatsWorkerSubscriber(
            Options(o => o.AckWait = TimeSpan.FromMilliseconds(150)),
            _jetStream,
            ingress,
            new TestLogger<NatsWorkerSubscriber>());

        await subscriber.StartAsync(CancellationToken.None);
        try
        {
            await ingress.Started.Task.WaitAsync(TimeSpan.FromSeconds(5));
            await Eventually(() => first.Progresses >= 1); // the heartbeat is now wedged
            ingress.Release.TrySetResult();
            await Eventually(() => first.Acks == 1);

            // The batch settled; the wedged heartbeat must not hold the loop: the next batch is
            // fetched and settled.
            _jetStream.EnqueueDelivery(second.Create("p2", numDelivered: 1));
            await Eventually(() => second.Acks == 1);
        }
        finally
        {
            await subscriber.StopAsync(CancellationToken.None);
            subscriber.Dispose();
        }
    }

    /// <summary>
    /// The backstop for a heartbeat the client cannot abort at all (it ignores its token): the
    /// join is bounded by one heartbeat interval, after which the renewal loop is abandoned with
    /// a warning and the loop moves on — unsettled deliveries fall back to the server's AckWait.
    /// </summary>
    [Fact]
    public async Task WorkerSubscriber_AHeartbeatThatIgnoresCancellation_IsAbandonedAfterOneInterval()
    {
        var ingress = new GatedIngress();
        var never = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var first = new RecordingDelivery { ProgressBehavior = async _ => await never.Task };
        var second = new RecordingDelivery();
        _jetStream.EnqueueDelivery(first.Create("p1", numDelivered: 1));
        var logger = new RecordingThrowingLogger<NatsWorkerSubscriber>();
        var subscriber = new NatsWorkerSubscriber(
            Options(o => o.AckWait = TimeSpan.FromMilliseconds(150)),
            _jetStream,
            ingress,
            logger);

        await subscriber.StartAsync(CancellationToken.None);
        try
        {
            await ingress.Started.Task.WaitAsync(TimeSpan.FromSeconds(5));
            await Eventually(() => first.Progresses >= 1);
            ingress.Release.TrySetResult();
            await Eventually(() => first.Acks == 1);

            _jetStream.EnqueueDelivery(second.Create("p2", numDelivered: 1));
            await Eventually(() => second.Acks == 1);
            Assert.True(logger.HasEntry(Microsoft.Extensions.Logging.LogLevel.Warning, "did not stop within"), "the abandoned heartbeat must be logged");
        }
        finally
        {
            never.TrySetResult();
            await subscriber.StopAsync(CancellationToken.None);
            subscriber.Dispose();
        }
    }

    private static async Task Eventually(Func<bool> condition)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        while (!condition())
            await Task.Delay(10, timeout.Token);
    }
}
