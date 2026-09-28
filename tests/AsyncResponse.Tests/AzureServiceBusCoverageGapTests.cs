using AsyncResponse.Transports.AzureServiceBus;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Pins the Azure Service Bus subscriber and dispatcher paths the broader suites leave unexercised:
/// a lock renewal that outlives its join, a failed hand-back or renew, the sweep's exit between
/// messages, the early-ACK queue's capacity wait across dispose, the drain's surfacing cut-off,
/// the lapsed-worker surfacing and a drain fault kept inside <c>DisposeAsync</c>.
/// </summary>
public sealed class AzureServiceBusCoverageGapTests
{
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(10);

    [Fact]
    public async Task RenewalJoin_ARenewThatIgnoresCancellation_IsAbandonedAfterTheShutdownTimeout()
    {
        // The renewal task is joined for at most ShutdownTimeout after the batch ends: a renew stuck
        // in the SDK retry pipeline (ignoring its token) must not hold the receive loop hostage.
        var renewStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseRenew = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var hooks = new Hooks
        {
            Renew = async _ =>
            {
                renewStarted.TrySetResult();
                await releaseRenew.Task;
            }
        };
        var logger = new CollectingLogger();
        var ingress = new Mock<IAsyncResponseIngress>();
        ingress.Setup(i => i.HandleWorkerMessageAsync("first")).Returns(() => renewStarted.Task);
        var options = WorkerOptions();
        options.ShutdownTimeout = TimeSpan.FromMilliseconds(200);
        options.WorkerSubscriber.LockRenewalInterval = TimeSpan.FromMilliseconds(20);
        var subscriber = Worker(options, [Delivery("m1", "first", hooks)], ingress.Object, logger);

        try
        {
            await subscriber.StartAsync(CancellationToken.None);
            await logger.WaitForAsync("did not stop within the shutdown budget");

            Assert.Equal(1, Volatile.Read(ref hooks.CompleteCalls));
            Assert.Contains(logger.Messages, message => message.Contains("abandoning the renewal task", StringComparison.Ordinal));
        }
        finally
        {
            releaseRenew.TrySetResult();
            await subscriber.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task HandBack_AnAbandonThatThrows_IsLogged_AndTheSweepExitsBetweenMessagesOnceTheBatchEnds()
    {
        // m1's renew is in flight when the stop ends the batch: it returns only once the renewal is
        // cancelled, and the sweep must then leave without renewing m2 (whose handler never ran).
        // m2's hand-back fails — logged, never escaping the stop.
        var firstRenewStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var firstHooks = new Hooks
        {
            Renew = async cancellationToken =>
            {
                firstRenewStarted.TrySetResult();
                var cancelled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                using var registration = cancellationToken.Register(() => cancelled.TrySetResult());
                await cancelled.Task;
            }
        };
        var abandonFailure = new InvalidOperationException("namespace unavailable");
        var secondHooks = new Hooks { Abandon = () => throw abandonFailure };
        var logger = new CollectingLogger();
        var ingress = new Mock<IAsyncResponseIngress>();
        ingress.Setup(i => i.HandleWorkerMessageAsync("first")).Returns(() => releaseFirst.Task);
        var options = WorkerOptions();
        options.WorkerSubscriber.LockRenewalInterval = TimeSpan.FromMilliseconds(20);
        var subscriber = Worker(
            options,
            [Delivery("m1", "first", firstHooks), Delivery("m2", "second", secondHooks)],
            ingress.Object,
            logger);

        await subscriber.StartAsync(CancellationToken.None);
        await firstRenewStarted.Task.WaitAsync(Wait);

        var stopping = subscriber.StopAsync(CancellationToken.None);
        releaseFirst.TrySetResult();
        await stopping.WaitAsync(Wait);

        ingress.Verify(i => i.HandleWorkerMessageAsync("second"), Times.Never);
        Assert.Equal(1, Volatile.Read(ref firstHooks.CompleteCalls));
        Assert.Equal(1, Volatile.Read(ref firstHooks.RenewCalls));
        Assert.Equal(0, Volatile.Read(ref secondHooks.RenewCalls));
        Assert.Equal(1, Volatile.Read(ref secondHooks.AbandonCalls));
        Assert.Contains(
            logger.Entries,
            entry => entry.Message.Contains("Failed to hand unstarted Azure Service Bus message m2", StringComparison.Ordinal)
                && ReferenceEquals(entry.Exception, abandonFailure));
    }

    [Fact]
    public async Task RenewalSweep_ATransientRenewFailure_IsWarned_AndTheHandlerStillCompletes()
    {
        // Not a lost lock and not a race with the settlement: a plain renewal failure is warned
        // about (it may redeliver while still running) without ending the sweep or the batch.
        var failure = new ServiceBusException(isTransient: true, "service busy");
        var hooks = new Hooks { Renew = _ => throw failure };
        var logger = new CollectingLogger();
        var ingress = new Mock<IAsyncResponseIngress>();
        ingress.Setup(i => i.HandleWorkerMessageAsync("first"))
            .Returns(() => logger.WaitForAsync("Failed to renew the lock of Azure Service Bus message m1"));
        var options = WorkerOptions();
        options.WorkerSubscriber.LockRenewalInterval = TimeSpan.FromMilliseconds(20);
        var subscriber = Worker(options, [Delivery("m1", "first", hooks)], ingress.Object, logger);

        await subscriber.StartAsync(CancellationToken.None);
        await hooks.Completed.Task.WaitAsync(TimeSpan.FromSeconds(35));
        await subscriber.StopAsync(CancellationToken.None);

        Assert.Contains(
            logger.Entries,
            entry => entry.Message.Contains("Failed to renew the lock of Azure Service Bus message m1", StringComparison.Ordinal)
                && ReferenceEquals(entry.Exception, failure));
        Assert.DoesNotContain(logger.Messages, message => message.Contains("is lost and can no longer be renewed", StringComparison.Ordinal));
    }

    [Fact]
    public async Task AwaitingDispatcher_NeverGatesTheReceiveLoop()
    {
        await using var dispatcher = AzureServiceBusMessageDispatcher.Create(
            (_, _) => Task.CompletedTask,
            WorkerOptions(),
            new AzureServiceBusSubscriberOptions(),
            NullLogger.Instance,
            "workers",
            AzureServiceBusSubscriberRole.Worker);

        Assert.IsType<AwaitingAzureServiceBusMessageDispatcher>(dispatcher);
        Assert.True(dispatcher.CanAcceptMore);
        Assert.Equal(int.MaxValue, dispatcher.FreeCapacity);
        Assert.True(dispatcher.WaitForCapacityAsync(CancellationToken.None).IsCompletedSuccessfully);
    }

    [Fact]
    public async Task QueuedDispatcher_CapacityWait_EndsWhenTheDispatcherIsDisposedWhileSaturated()
    {
        // WaitToWriteAsync answers false once dispose completes the channel: the receive loop's
        // capacity wait must end there rather than park forever behind a saturated queue.
        var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handled = new System.Collections.Concurrent.ConcurrentQueue<string>();
        var dispatcher = (QueuedAzureServiceBusMessageDispatcher)AzureServiceBusMessageDispatcher.Create(
            async (delivery, _) =>
            {
                handled.Enqueue(delivery.MessageId);
                if (delivery.MessageId == "m1")
                    await releaseFirst.Task;
            },
            WorkerOptions(),
            EarlyAck(workers: 1, capacity: 1, drain: TimeSpan.FromSeconds(10)),
            NullLogger.Instance,
            "workers",
            AzureServiceBusSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery("m1", "first", new Hooks()), CancellationToken.None);
        await WaitUntilAsync(() => dispatcher.RunningCount == 1);
        await dispatcher.HandleAsync(Delivery("m2", "second", new Hooks()), CancellationToken.None);
        Assert.False(dispatcher.CanAcceptMore);

        var capacity = dispatcher.WaitForCapacityAsync(CancellationToken.None).AsTask();
        Assert.False(capacity.IsCompleted);

        var disposing = dispatcher.DisposeAsync().AsTask();
        await capacity.WaitAsync(Wait);
        Assert.False(disposing.IsCompleted);

        releaseFirst.TrySetResult();
        await disposing.WaitAsync(Wait);
        Assert.Equal(["m1", "m2"], handled.ToArray());
    }

    [Fact]
    public async Task QueuedDispatcher_DrainLapse_SlowCallbackCutsSurfacingShort_ThenAFreedWorkerSurfacesTheRest()
    {
        // m1 runs (ignoring the token) past the drain budget with m2 and m3 still queued. The dispose
        // surfaces m2, but its OnBackgroundFailure callback outlives the reserve, so surfacing stops
        // and m3 is counted as lost. Once m1 finally returns, the freed worker must NOT run m3 — the
        // budget has lapsed — and surfaces it instead.
        var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseCallback = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var thirdSurfaced = new TaskCompletionSource<AzureServiceBusBackgroundFailureContext>(TaskCreationOptions.RunContinuationsAsynchronously);
        var surfaced = new System.Collections.Concurrent.ConcurrentQueue<string>();
        var handled = new System.Collections.Concurrent.ConcurrentQueue<string>();
        var logger = new CollectingLogger();
        var subscriberOptions = EarlyAck(workers: 1, capacity: 3, drain: TimeSpan.FromMilliseconds(400));
        subscriberOptions.OnBackgroundFailure = async context =>
        {
            surfaced.Enqueue(context.MessageId);
            if (context.MessageId == "m2")
                await releaseCallback.Task;
            else
                thirdSurfaced.TrySetResult(context);
        };
        var dispatcher = (QueuedAzureServiceBusMessageDispatcher)AzureServiceBusMessageDispatcher.Create(
            async (delivery, _) =>
            {
                handled.Enqueue(delivery.MessageId);
                if (delivery.MessageId == "m1")
                    await releaseFirst.Task;
            },
            WorkerOptions(),
            subscriberOptions,
            logger,
            "workers",
            AzureServiceBusSubscriberRole.Worker);

        try
        {
            await dispatcher.HandleAsync(Delivery("m1", "first", new Hooks()), CancellationToken.None);
            await WaitUntilAsync(() => dispatcher.RunningCount == 1);
            await dispatcher.HandleAsync(Delivery("m2", "second", new Hooks()), CancellationToken.None);
            await dispatcher.HandleAsync(Delivery("m3", "third", new Hooks(), sequenceNumber: 33), CancellationToken.None);

            await dispatcher.DisposeAsync().AsTask().WaitAsync(Wait);

            Assert.Equal(["m2"], surfaced.ToArray());
            Assert.Contains(logger.Messages, message => message.Contains("Surfaced 1 via OnBackgroundFailure", StringComparison.Ordinal)
                && message.Contains("1 could not be surfaced", StringComparison.Ordinal));

            releaseFirst.TrySetResult();
            var third = await thirdSurfaced.Task.WaitAsync(Wait);

            Assert.Equal(33, third.SequenceNumber);
            Assert.Equal(AzureServiceBusSubscriberRole.Worker.ToString(), third.SubscriberRole);
            Assert.IsType<OperationCanceledException>(third.Exception);
            Assert.Equal(["m1"], handled.ToArray());
        }
        finally
        {
            releaseFirst.TrySetResult();
            releaseCallback.TrySetResult();
        }
    }

    [Fact]
    public async Task QueuedDispatcher_ADrainFault_StaysInsideDisposeAsync()
    {
        // The catch-all behind the drain: whatever faults the drain wait itself (here a budget past
        // the timer ceiling, which only direct construction can pass — the validator refuses it)
        // is logged at Debug and never escapes DisposeAsync to mask the real shutdown path.
        var logger = new CollectingLogger();
        var dispatcher = new QueuedAzureServiceBusMessageDispatcher(
            (_, _) => Task.CompletedTask,
            WorkerOptions(),
            EarlyAck(workers: 1, capacity: 1, drain: TimeSpan.MaxValue),
            logger,
            "workers",
            AzureServiceBusSubscriberRole.Worker);

        await dispatcher.DisposeAsync();

        Assert.Contains(
            logger.Entries,
            entry => entry.Message.Contains("drain for workers ended with an error", StringComparison.Ordinal)
                && entry.Exception is ArgumentOutOfRangeException);
    }

    [Fact]
    public void CorrelationPropertyConversion_NullAndNonFormattableValues()
    {
        Assert.Null(AzureServiceBusCorrelationIdExtractor.TryConvertProperty(null));
        // Neither text, bytes nor IFormattable: the value's own ToString() is the id.
        Assert.Equal("c-1", AzureServiceBusCorrelationIdExtractor.TryConvertProperty(new OpaqueProperty("c-1")));
    }

    private sealed class OpaqueProperty(string value)
    {
        public override string ToString() => value;
    }

    private static AzureServiceBusAsyncResponseOptions WorkerOptions()
        => new()
        {
            WorkerQueue = "workers",
            ResponseQueue = "responses",
            ReceiveWaitTime = TimeSpan.FromMilliseconds(10)
        };

    private static AzureServiceBusSubscriberOptions EarlyAck(int workers, int capacity, TimeSpan drain)
        => new()
        {
            AckMode = AzureServiceBusAckMode.AckAfterEnqueue,
            BackgroundWorkerCount = workers,
            BackgroundQueueCapacity = capacity,
            BackgroundDrainTimeout = drain
        };

    private static AzureServiceBusWorkerSubscriber Worker(
        AzureServiceBusAsyncResponseOptions options,
        IReadOnlyList<AzureServiceBusTransportDelivery> batch,
        IAsyncResponseIngress ingress,
        CollectingLogger logger)
        => new(Options.Create(options), new OneBatchClient(batch), ingress, logger.For<AzureServiceBusWorkerSubscriber>());

    private static AzureServiceBusTransportDelivery Delivery(string messageId, string body, Hooks hooks, long sequenceNumber = 7)
        => new(
            "workers",
            body,
            messageId,
            CorrelationId: null,
            sequenceNumber,
            DeliveryCount: 1,
            new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase),
            () =>
            {
                Interlocked.Increment(ref hooks.CompleteCalls);
                hooks.Completed.TrySetResult();
                return ValueTask.CompletedTask;
            },
            () =>
            {
                Interlocked.Increment(ref hooks.AbandonCalls);
                return hooks.Abandon();
            },
            (_, _) => ValueTask.CompletedTask,
            cancellationToken =>
            {
                Interlocked.Increment(ref hooks.RenewCalls);
                return hooks.Renew(cancellationToken);
            });

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        using var cts = new CancellationTokenSource(Wait);
        while (!condition())
            await Task.Delay(10, cts.Token);
    }

    private sealed class Hooks
    {
        public int CompleteCalls;
        public int AbandonCalls;
        public int RenewCalls;
        public Func<ValueTask> Abandon { get; init; } = () => ValueTask.CompletedTask;
        public Func<CancellationToken, ValueTask> Renew { get; init; } = _ => ValueTask.CompletedTask;
        public TaskCompletionSource Completed { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    /// <summary>Hands one batch to the first receive, then long-polls empty until the stop.</summary>
    private sealed class OneBatchClient(IReadOnlyList<AzureServiceBusTransportDelivery> batch) : IAzureServiceBusClient
    {
        public IAzureServiceBusSender CreateSender(string queue) => throw new NotSupportedException();

        public IAzureServiceBusReceiver CreateReceiver(string queue, AzureServiceBusSubscriberOptions subscriberOptions)
            => new Receiver(batch);

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;

        private sealed class Receiver(IReadOnlyList<AzureServiceBusTransportDelivery> batch) : IAzureServiceBusReceiver
        {
            private int _handedOver;

            public async Task<IReadOnlyList<AzureServiceBusTransportDelivery>> ReceiveMessagesAsync(
                int maxMessages,
                TimeSpan maxWaitTime,
                CancellationToken cancellationToken = default)
            {
                if (Interlocked.Exchange(ref _handedOver, 1) == 0)
                    return batch;

                await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
                return [];
            }

            public Task CloseAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;

            public ValueTask DisposeAsync() => ValueTask.CompletedTask;
        }
    }
}
