using AsyncResponse.Transports.RabbitMQ;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Direct unit tests for <see cref="RabbitMqMessageDispatcher"/> and its two ACK strategies
/// (<see cref="RabbitMqAckMode.AckAfterHandlerCompletes"/> and <see cref="RabbitMqAckMode.AckAfterEnqueue"/>),
/// plus the option validation that gates them.
/// </summary>
public class RabbitMqDispatcherTests
{
    // ---------- ValidateOptions ----------

    [Fact]
    public void ValidateOptions_AckAfterHandlerCompletes_DoesNotThrow()
    {
        RabbitMqMessageDispatcher.ValidateOptions(
            new RabbitMqAsyncResponseOptions(),
            new RabbitMqSubscriberOptions { AckMode = RabbitMqAckMode.AckAfterHandlerCompletes },
            RabbitMqSubscriberRole.Worker);
    }

    [Fact]
    public void ValidateOptions_SameWorkerAndResponseQueues_Throws()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            RabbitMqMessageDispatcher.ValidateOptions(
                new RabbitMqAsyncResponseOptions
                {
                    WorkerQueue = "shared-queue",
                    ResponseQueue = "shared-queue"
                },
                new RabbitMqSubscriberOptions(),
                RabbitMqSubscriberRole.Worker));

        Assert.Contains(nameof(RabbitMqAsyncResponseOptions.WorkerQueue), ex.Message, StringComparison.Ordinal);
        Assert.Contains(nameof(RabbitMqAsyncResponseOptions.ResponseQueue), ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateOptions_ZeroPrefetch_Throws()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            RabbitMqMessageDispatcher.ValidateOptions(
                new RabbitMqAsyncResponseOptions(),
                new RabbitMqSubscriberOptions { PrefetchCount = 0 },
                RabbitMqSubscriberRole.ResponseIngress));

        Assert.Contains(nameof(RabbitMqSubscriberOptions.PrefetchCount), ex.Message, StringComparison.Ordinal);
        Assert.Contains(nameof(RabbitMqAsyncResponseOptions.ResponseSubscriber), ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateOptions_AckAfterEnqueue_RequiresPositiveBackgroundWorkerCount()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            RabbitMqMessageDispatcher.ValidateOptions(
                new RabbitMqAsyncResponseOptions(),
                new RabbitMqSubscriberOptions { AckMode = RabbitMqAckMode.AckAfterEnqueue },
                RabbitMqSubscriberRole.Worker));

        Assert.Contains(nameof(RabbitMqSubscriberOptions.BackgroundWorkerCount), ex.Message, StringComparison.Ordinal);
        Assert.Contains(nameof(RabbitMqAsyncResponseOptions.WorkerSubscriber), ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateOptions_AckAfterEnqueue_RequiresPositiveBackgroundQueueCapacity()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            RabbitMqMessageDispatcher.ValidateOptions(
                new RabbitMqAsyncResponseOptions(),
                new RabbitMqSubscriberOptions
                {
                    AckMode = RabbitMqAckMode.AckAfterEnqueue,
                    BackgroundWorkerCount = 2
                },
                RabbitMqSubscriberRole.Worker));

        Assert.Contains(nameof(RabbitMqSubscriberOptions.BackgroundQueueCapacity), ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateOptions_AckAfterEnqueue_RequiresPositiveDrainTimeout()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            RabbitMqMessageDispatcher.ValidateOptions(
                new RabbitMqAsyncResponseOptions(),
                new RabbitMqSubscriberOptions
                {
                    AckMode = RabbitMqAckMode.AckAfterEnqueue,
                    BackgroundWorkerCount = 2,
                    BackgroundQueueCapacity = 8,
                    BackgroundDrainTimeout = TimeSpan.Zero
                },
                RabbitMqSubscriberRole.Worker));

        Assert.Contains(nameof(RabbitMqSubscriberOptions.BackgroundDrainTimeout), ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateOptions_AckAfterEnqueue_RequiresPositiveShutdownTimeout()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            RabbitMqMessageDispatcher.ValidateOptions(
                new RabbitMqAsyncResponseOptions { ShutdownTimeout = TimeSpan.Zero },
                EnqueueSubscriber(drain: TimeSpan.FromSeconds(5)),
                RabbitMqSubscriberRole.Worker));

        Assert.Contains(nameof(RabbitMqAsyncResponseOptions.ShutdownTimeout), ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateOptions_AckAfterEnqueue_RejectsNonPositiveHostShutdownTimeout()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            RabbitMqMessageDispatcher.ValidateOptions(
                new RabbitMqAsyncResponseOptions
                {
                    ShutdownTimeout = TimeSpan.FromSeconds(15),
                    HostShutdownTimeout = TimeSpan.Zero
                },
                EnqueueSubscriber(drain: TimeSpan.FromSeconds(5)),
                RabbitMqSubscriberRole.Worker));

        Assert.Contains(nameof(RabbitMqAsyncResponseOptions.HostShutdownTimeout), ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateOptions_AckAfterEnqueue_RejectsDrainPlusShutdownExceedingHostBudget()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            RabbitMqMessageDispatcher.ValidateOptions(
                new RabbitMqAsyncResponseOptions
                {
                    ShutdownTimeout = TimeSpan.FromSeconds(20),
                    HostShutdownTimeout = TimeSpan.FromSeconds(25)
                },
                EnqueueSubscriber(drain: TimeSpan.FromSeconds(10)),
                RabbitMqSubscriberRole.Worker));

        Assert.Contains(nameof(RabbitMqAsyncResponseOptions.HostShutdownTimeout), ex.Message, StringComparison.Ordinal);
        Assert.Contains(nameof(RabbitMqSubscriberOptions.BackgroundDrainTimeout), ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateOptions_AckAfterEnqueue_NullHostShutdownTimeout_Passes()
    {
        RabbitMqMessageDispatcher.ValidateOptions(
            new RabbitMqAsyncResponseOptions
            {
                ShutdownTimeout = TimeSpan.FromSeconds(15),
                HostShutdownTimeout = null
            },
            EnqueueSubscriber(drain: TimeSpan.FromSeconds(10)),
            RabbitMqSubscriberRole.Worker);
    }

    [Fact]
    public void ValidateOptions_DocumentedEarlyAckDefaults_Pass()
    {
        // Regression: the documented two-arg early-ACK opt-in with stock defaults
        // (5s ShutdownTimeout + 20s BackgroundDrainTimeout vs HostShutdownTimeout 30s)
        // must not fail startup.
        RabbitMqMessageDispatcher.ValidateOptions(
            new RabbitMqAsyncResponseOptions(),
            new RabbitMqSubscriberOptions().UseAckAfterEnqueue(4, 256),
            RabbitMqSubscriberRole.Worker);
    }

    /// <summary>
    /// Round 33 (B3): the early-ACK shutdown budget summed BackgroundDrainTimeout + ShutdownTimeout,
    /// but the stop path arms ShutdownTimeout TWICE — once for BasicCancel, then a fresh budget for
    /// the channel/connection closes after the drain. Pre-fix 5s + 25s (+ the uncounted 5s) passed a
    /// 30s host budget that the real stop path overran by a full ShutdownTimeout.
    /// </summary>
    [Fact]
    public void ValidateOptions_AckAfterEnqueue_CountsShutdownTimeoutTwice_ForTheCancelAndTheClose()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            RabbitMqMessageDispatcher.ValidateOptions(
                new RabbitMqAsyncResponseOptions
                {
                    ShutdownTimeout = TimeSpan.FromSeconds(5),
                    HostShutdownTimeout = TimeSpan.FromSeconds(30)
                },
                EnqueueSubscriber(drain: TimeSpan.FromSeconds(25)),
                RabbitMqSubscriberRole.Worker));

        Assert.Contains(nameof(RabbitMqAsyncResponseOptions.HostShutdownTimeout), ex.Message, StringComparison.Ordinal);
        Assert.Contains(nameof(RabbitMqSubscriberOptions.BackgroundDrainTimeout), ex.Message, StringComparison.Ordinal);
        Assert.Contains(nameof(RabbitMqAsyncResponseOptions.ShutdownTimeout), ex.Message, StringComparison.Ordinal);
        Assert.Contains("00:00:35", ex.Message, StringComparison.Ordinal); // 5s cancel + 25s drain + 5s close
    }

    /// <summary>
    /// Control for the three-term budget: 5s + 20s + 5s = 30s fits a 30s host budget exactly (the
    /// comparison is inclusive), so the documented defaults keep starting.
    /// </summary>
    [Fact]
    public void ValidateOptions_AckAfterEnqueue_TwoShutdownTimeoutsPlusDrainExactlyAtTheHostBudget_Passes()
    {
        RabbitMqMessageDispatcher.ValidateOptions(
            new RabbitMqAsyncResponseOptions
            {
                ShutdownTimeout = TimeSpan.FromSeconds(5),
                HostShutdownTimeout = TimeSpan.FromSeconds(30)
            },
            EnqueueSubscriber(drain: TimeSpan.FromSeconds(20)),
            RabbitMqSubscriberRole.Worker);
    }

    [Fact]
    public void ValidateOptions_UnsupportedAckMode_Throws()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            RabbitMqMessageDispatcher.ValidateOptions(
                new RabbitMqAsyncResponseOptions(),
                new RabbitMqSubscriberOptions { AckMode = (RabbitMqAckMode)99 },
                RabbitMqSubscriberRole.Worker));

        Assert.Contains("unsupported value", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    // ---------- Create ----------

    [Fact]
    public async Task Create_AckAfterHandlerCompletes_BuildsAwaitingDispatcher()
    {
        await using var dispatcher = RabbitMqMessageDispatcher.Create(
            (_, _) => Task.CompletedTask,
            new RabbitMqAsyncResponseOptions(),
            new RabbitMqSubscriberOptions { AckMode = RabbitMqAckMode.AckAfterHandlerCompletes },
            NullLogger.Instance,
            "worker.q",
            RabbitMqSubscriberRole.Worker);

        Assert.IsType<AwaitingRabbitMqMessageDispatcher>(dispatcher);
    }

    [Fact]
    public async Task Create_AckAfterEnqueue_BuildsQueuedDispatcher()
    {
        await using var dispatcher = RabbitMqMessageDispatcher.Create(
            (_, _) => Task.CompletedTask,
            new RabbitMqAsyncResponseOptions(),
            EnqueueSubscriber(),
            NullLogger.Instance,
            "worker.q",
            RabbitMqSubscriberRole.Worker);

        Assert.IsType<QueuedRabbitMqMessageDispatcher>(dispatcher);
    }

    // ---------- AwaitingRabbitMqMessageDispatcher ----------

    [Fact]
    public async Task Awaiting_HandlerSucceeds_AcksDelivery()
    {
        var channel = new FakeDispatcherChannel();
        var handled = new TaskCompletionSource<RabbitMqDelivery>(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var dispatcher = RabbitMqMessageDispatcher.Create(
            (delivery, _) =>
            {
                handled.TrySetResult(delivery);
                return Task.CompletedTask;
            },
            new RabbitMqAsyncResponseOptions(),
            new RabbitMqSubscriberOptions { AckMode = RabbitMqAckMode.AckAfterHandlerCompletes },
            NullLogger.Instance,
            "worker.q",
            RabbitMqSubscriberRole.Worker);

        await dispatcher.HandleAsync(
            Delivery("payload", new BasicProperties { CorrelationId = "cid-await" }, deliveryTag: 11),
            channel,
            CancellationToken.None);

        Assert.Equal(11UL, (await handled.Task.WaitAsync(TimeSpan.FromSeconds(2))).DeliveryTag);
        Assert.Equal(11UL, Assert.Single(channel.Acks));
        Assert.Empty(channel.Nacks);
    }

    [Fact]
    public async Task Awaiting_AckIgnoresCancellation_SoAShutdownRacingTheAckStillSettles()
    {
        // Regression (r24): both BasicAckAsync sites passed subscriberCancellationToken (the NACK
        // sites already used None), so a shutdown racing the ACK aborted the settle — the broker
        // requeued and redelivered work whose handler had already completed, running it twice.
        // Settlement now deliberately ignores cancellation, like every sibling transport.
        var channel = new FakeDispatcherChannel();
        using var cts = new CancellationTokenSource();
        await using var dispatcher = RabbitMqMessageDispatcher.Create(
            (_, _) =>
            {
                cts.Cancel(); // shutdown lands while the handler is finishing
                return Task.CompletedTask;
            },
            new RabbitMqAsyncResponseOptions(),
            new RabbitMqSubscriberOptions { AckMode = RabbitMqAckMode.AckAfterHandlerCompletes },
            NullLogger.Instance,
            "worker.q",
            RabbitMqSubscriberRole.Worker);

        await dispatcher.HandleAsync(
            Delivery("payload", new BasicProperties { CorrelationId = "cid-settle" }, deliveryTag: 61),
            channel,
            cts.Token);

        Assert.Equal(61UL, Assert.Single(channel.Acks));
        Assert.All(channel.AckTokens, token => Assert.Equal(CancellationToken.None, token));
        Assert.Empty(channel.Nacks);
    }

    [Fact]
    public async Task Queued_AckIgnoresCancellation_SoAShutdownRacingTheAckStillSettles()
    {
        // Same regression as the awaiting variant, for the early-ACK path: the delivery already
        // belongs to a background worker when the ACK runs, so an aborted settle redelivered a
        // job that was still executing in-process — two concurrent executions of one job.
        var channel = new FakeDispatcherChannel();
        var releaseHandler = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync(); // shutdown already in progress when the delivery lands
        var dispatcher = (QueuedRabbitMqMessageDispatcher)RabbitMqMessageDispatcher.Create(
            async (_, _) => await releaseHandler.Task.ConfigureAwait(false),
            new RabbitMqAsyncResponseOptions(),
            EnqueueSubscriber(workers: 1, capacity: 8),
            NullLogger.Instance,
            "worker.q",
            RabbitMqSubscriberRole.Worker);

        try
        {
            await dispatcher.HandleAsync(Delivery("payload", deliveryTag: 62), channel, cts.Token);

            Assert.Equal(62UL, Assert.Single(channel.Acks));
            Assert.All(channel.AckTokens, token => Assert.Equal(CancellationToken.None, token));
        }
        finally
        {
            releaseHandler.TrySetResult();
            await dispatcher.DisposeAsync();
        }
    }

    [Fact]
    public async Task Enqueue_BackgroundHandlerFails_PublishesTheDeliveryToTheDeadLetterExchange()
    {
        // Regression: a failed background handler was only logged and passed to
        // OnBackgroundFailure — the already-ACKed delivery never reached the configured
        // DeadLetterExchange (the early ACK forecloses the native reject-without-requeue DLX
        // route), so a permanently failing job vanished with one log line while Kafka and Redis
        // both write a dead-letter copy in the identical spot.
        var channel = new FakeDispatcherChannel();
        var dispatcher = RabbitMqMessageDispatcher.Create(
            (_, _) => throw new InvalidOperationException("background boom"),
            new RabbitMqAsyncResponseOptions { DeadLetterExchange = "dlx" },
            EnqueueSubscriber(workers: 1, capacity: 8),
            NullLogger.Instance,
            "worker.q",
            RabbitMqSubscriberRole.Worker);

        try
        {
            await dispatcher.HandleAsync(
                Delivery("poison-payload", new BasicProperties { CorrelationId = "cid-dlx" }, routingKey: "worker.route", deliveryTag: 91),
                channel,
                CancellationToken.None);

            var guard = TimeProvider.System.GetUtcNow() + TimeSpan.FromSeconds(10);
            while (channel.Publishes.Count == 0)
            {
                Assert.True(TimeProvider.System.GetUtcNow() < guard, "the failed background delivery was never dead-lettered");
                await Task.Delay(TimeSpan.FromMilliseconds(5));
            }

            var publish = Assert.Single(channel.Publishes);
            Assert.Equal("dlx", publish.Exchange);
            // Native dead-lettering keeps the original routing key unless DeadLetterRoutingKey
            // overrides it — the manual copy must route the same way.
            Assert.Equal("worker.route", publish.RoutingKey);
            Assert.Equal("poison-payload", Encoding.UTF8.GetString(publish.Body.ToArray()));
            Assert.Equal("background boom", Assert.IsType<string>(publish.Properties.Headers!["AR-DeadLetter-Reason"]));
        }
        finally
        {
            await dispatcher.DisposeAsync();
        }
    }

    [Fact]
    public async Task Enqueue_BackgroundHandlerFails_WithoutADeadLetterExchange_OnlyLogsAndNotifies()
    {
        // No DLX configured: the pre-fix behavior (log + OnBackgroundFailure) is still the whole
        // story — nothing may be published anywhere.
        var channel = new FakeDispatcherChannel();
        var notified = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var subscriber = EnqueueSubscriber(workers: 1, capacity: 8);
        subscriber.OnBackgroundFailure = _ =>
        {
            notified.TrySetResult();
            return ValueTask.CompletedTask;
        };
        var dispatcher = RabbitMqMessageDispatcher.Create(
            (_, _) => throw new InvalidOperationException("background boom"),
            new RabbitMqAsyncResponseOptions(),
            subscriber,
            NullLogger.Instance,
            "worker.q",
            RabbitMqSubscriberRole.Worker);

        try
        {
            await dispatcher.HandleAsync(Delivery("payload", deliveryTag: 92), channel, CancellationToken.None);
            await notified.Task.WaitAsync(TimeSpan.FromSeconds(10));

            Assert.Empty(channel.Publishes);
        }
        finally
        {
            await dispatcher.DisposeAsync();
        }
    }

    [Fact]
    public async Task Awaiting_HandlerThrows_NacksWithRequeue()
    {
        var channel = new FakeDispatcherChannel();
        await using var dispatcher = RabbitMqMessageDispatcher.Create(
            (_, _) => throw new InvalidOperationException("handler boom"),
            new RabbitMqAsyncResponseOptions(),
            new RabbitMqSubscriberOptions { AckMode = RabbitMqAckMode.AckAfterHandlerCompletes },
            NullLogger.Instance,
            "worker.q",
            RabbitMqSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery("payload", deliveryTag: 12), channel, CancellationToken.None);

        Assert.Empty(channel.Acks);
        var nack = Assert.Single(channel.Nacks);
        Assert.Equal(12UL, nack.DeliveryTag);
        Assert.True(nack.Requeue);
    }

    [Fact]
    public async Task Awaiting_HandlerThrows_ChannelAlreadyClosed_SkipsNackAndLogsWarning()
    {
        // Red-on-old: the failure-path NACK was the one unguarded settle. A closed channel has
        // already returned every un-ACKed delivery to the queue, so NACKing it throws into the
        // client's delivery callback — the requeue/reject decision silently lost, with no log.
        var logger = new ListLogger();
        var channel = new FakeDispatcherChannel { IsOpen = false };
        await using var dispatcher = RabbitMqMessageDispatcher.Create(
            (_, _) => throw new InvalidOperationException("handler boom"),
            new RabbitMqAsyncResponseOptions(),
            new RabbitMqSubscriberOptions { AckMode = RabbitMqAckMode.AckAfterHandlerCompletes },
            logger,
            "worker.q",
            RabbitMqSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery("payload", deliveryTag: 41), channel, CancellationToken.None);

        Assert.Empty(channel.Acks);
        Assert.Empty(channel.Nacks);
        Assert.Contains(
            logger.Entries,
            entry => entry.Level == LogLevel.Warning
                && entry.Message.Contains("41", StringComparison.Ordinal)
                && entry.Message.Contains("requeue", StringComparison.Ordinal));
    }

    [Fact]
    public async Task Awaiting_HandlerThrows_NackFailure_DoesNotEscapeTheDeliveryCallbackAndLogsWarning()
    {
        // Red-on-old: a throwing NACK (stale delivery tag after automatic recovery, channel torn
        // down between handler failure and settle) escaped HandleAsync — which runs inside the
        // client's delivery callback — leaving the delivery neither ACKed nor NACKed.
        var logger = new ListLogger();
        var channel = new FakeDispatcherChannel
        {
            ThrowOnNack = new global::RabbitMQ.Client.Exceptions.AlreadyClosedException(
                new ShutdownEventArgs(ShutdownInitiator.Peer, 406, "channel closed"))
        };
        await using var dispatcher = RabbitMqMessageDispatcher.Create(
            (_, _) => throw new InvalidOperationException("handler boom"),
            new RabbitMqAsyncResponseOptions(),
            new RabbitMqSubscriberOptions { AckMode = RabbitMqAckMode.AckAfterHandlerCompletes },
            logger,
            "worker.q",
            RabbitMqSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery("payload", deliveryTag: 42), channel, CancellationToken.None);

        Assert.Contains(
            logger.Entries,
            entry => entry.Level == LogLevel.Warning
                && entry.Exception is global::RabbitMQ.Client.Exceptions.AlreadyClosedException
                && entry.Message.Contains("42", StringComparison.Ordinal)
                && entry.Message.Contains("requeue", StringComparison.Ordinal));
    }

    [Fact]
    public async Task Awaiting_ShutdownCancellation_LeavesTheDeliveryUnsettled()
    {
        // A handler cancelled by host shutdown is not a handler failure: NACKing would count a
        // healthy delivery against the cap — and at the cap reject it without requeue, dropping
        // work whose side effects never ran (no dead-letter exchange configured = discarded
        // outright). Left un-ACKed, the broker redelivers it when the channel closes.
        var channel = new FakeDispatcherChannel();
        var stopping = new CancellationToken(canceled: true);
        await using var dispatcher = RabbitMqMessageDispatcher.Create(
            (_, token) => Task.FromException(new OperationCanceledException(token)),
            new RabbitMqAsyncResponseOptions(),
            new RabbitMqSubscriberOptions { MaxDeliveryAttempts = 1 },
            NullLogger.Instance,
            "worker.q",
            RabbitMqSubscriberRole.Worker);

        // Attempt 1 with MaxDeliveryAttempts = 1 is AT the cap: the old behavior was a
        // requeue:false NACK here, i.e. the broker discarded the cancelled-but-healthy message.
        await dispatcher.HandleAsync(Delivery("payload", deliveryTag: 7), channel, stopping);

        Assert.Empty(channel.Acks);
        Assert.Empty(channel.Nacks);
    }

    [Fact]
    public async Task Awaiting_BelowMaxDeliveryAttempts_RequeuesForRetry()
    {
        var channel = new FakeDispatcherChannel();
        await using var dispatcher = RabbitMqMessageDispatcher.Create(
            (_, _) => throw new InvalidOperationException("handler boom"),
            new RabbitMqAsyncResponseOptions(),
            new RabbitMqSubscriberOptions { MaxDeliveryAttempts = 3 },
            NullLogger.Instance,
            "worker.q",
            RabbitMqSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery("payload", deliveryTag: 1), channel, CancellationToken.None);

        Assert.True(Assert.Single(channel.Nacks).Requeue); // attempt 1 < 3
    }

    [Fact]
    public async Task Awaiting_AtMaxDeliveryAttempts_RejectsWithoutRequeue()
    {
        var channel = new FakeDispatcherChannel();
        await using var dispatcher = RabbitMqMessageDispatcher.Create(
            (_, _) => throw new InvalidOperationException("handler boom"),
            new RabbitMqAsyncResponseOptions(),
            new RabbitMqSubscriberOptions { MaxDeliveryAttempts = 1 },
            NullLogger.Instance,
            "worker.q",
            RabbitMqSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery("payload", deliveryTag: 1), channel, CancellationToken.None);

        Assert.False(Assert.Single(channel.Nacks).Requeue); // attempt 1 >= 1 -> dead-letter
    }

    [Fact]
    public async Task Awaiting_RedeliveredAtMaxDeliveryAttempts_RejectsWithoutRequeue()
    {
        var channel = new FakeDispatcherChannel();
        await using var dispatcher = RabbitMqMessageDispatcher.Create(
            (_, _) => throw new InvalidOperationException("handler boom"),
            new RabbitMqAsyncResponseOptions(),
            new RabbitMqSubscriberOptions { MaxDeliveryAttempts = 2 },
            NullLogger.Instance,
            "worker.q",
            RabbitMqSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery("payload", deliveryTag: 1, redelivered: true), channel, CancellationToken.None);

        Assert.False(Assert.Single(channel.Nacks).Requeue); // attempt 2 >= 2 -> dead-letter
    }

    [Fact]
    public async Task Awaiting_CapAboveTwo_RejectsAtTwo_InsteadOfRequeueingForever()
    {
        // Regression (round 29): a plain basic.nack requeue adds no x-death, so ResolveDeliveryAttempt
        // saturates at 2 and a cap ABOVE 2 was unreachable — every retry requeued, at broker rate,
        // forever. docs/transport-semantics.md promises such a cap "behaves like 2"; enforce that
        // rather than degrading into the unlimited cap.
        var channel = new FakeDispatcherChannel();
        await using var dispatcher = RabbitMqMessageDispatcher.Create(
            (_, _) => throw new InvalidOperationException("handler boom"),
            new RabbitMqAsyncResponseOptions(),
            new RabbitMqSubscriberOptions { MaxDeliveryAttempts = 5 },
            NullLogger.Instance,
            "worker.q",
            RabbitMqSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery("payload", deliveryTag: 1, redelivered: true), channel, CancellationToken.None);

        Assert.False(
            Assert.Single(channel.Nacks).Requeue,
            "a cap above 2 with no countable x-death must reject at 2, not requeue forever");
    }

    [Fact]
    public async Task Awaiting_CapAboveTwo_RidesTheDeadLetterCycle_OnceXDeathIsPresent()
    {
        // The counterpart: with a dead-letter cycle configured the broker counts attempts — but
        // ONLY dead-letter hops. Attempt 2 of 5 used to plain-requeue here, and a plain requeue
        // never advances x-death (and `redelivered` is already set), so the message resolved to
        // attempt 2 forever: an unbounded requeue loop at broker rate that never re-entered the
        // DLX. Once x-death is present every retry below the cap must reject WITHOUT requeue so
        // the dead-letter cycle is what counts it up to the operator's cap.
        var properties = new BasicProperties
        {
            Headers = new Dictionary<string, object?>
            {
                ["x-death"] = new List<object?> { new Dictionary<string, object?> { ["count"] = 1L } }
            }
        };

        var channel = new FakeDispatcherChannel();
        await using var dispatcher = RabbitMqMessageDispatcher.Create(
            (_, _) => throw new InvalidOperationException("handler boom"),
            new RabbitMqAsyncResponseOptions(),
            new RabbitMqSubscriberOptions { MaxDeliveryAttempts = 5 },
            NullLogger.Instance,
            "worker.q",
            RabbitMqSubscriberRole.Worker);

        Assert.Equal(2, RabbitMqMessageDispatcher.ResolveDeliveryAttempt(Delivery("payload", properties)));

        await dispatcher.HandleAsync(Delivery("payload", properties, deliveryTag: 1), channel, CancellationToken.None);

        Assert.False(
            Assert.Single(channel.Nacks).Requeue,
            "once x-death is present a plain requeue can never advance the attempt; the retry must ride the dead-letter cycle");
    }

    /// <summary>
    /// Review 2026-09-21 F8: the <c>AR-DeadLetter-Reason</c> header was cut with
    /// <c>message[..512]</c>. With a non-BMP character straddling the limit that kept a lone high
    /// surrogate, which the client's UTF-8 encoding replaces with U+FFFD — the parked copy's
    /// forensic text was corrupted at the cut. The cut is surrogate-aware now.
    /// </summary>
    [Fact]
    public async Task Awaiting_AtCap_NeverCutsTheDeadLetterReasonInsideASurrogatePair()
    {
        var properties = new BasicProperties
        {
            CorrelationId = "cid-park-surrogate",
            Headers = new Dictionary<string, object?>
            {
                ["x-death"] = new List<object?> { new Dictionary<string, object?> { ["count"] = 2L } }
            }
        };
        var longMessage = new string('x', 511) + "\U0001F600" + new string('y', 100);

        var channel = new FakeDispatcherChannel();
        await using var dispatcher = RabbitMqMessageDispatcher.Create(
            (_, _) => throw new InvalidOperationException(longMessage),
            new RabbitMqAsyncResponseOptions { DeadLetterExchange = "dlx", DeadLetterQueue = "parked" },
            new RabbitMqSubscriberOptions { MaxDeliveryAttempts = 3 },
            NullLogger.Instance,
            "worker.q",
            RabbitMqSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery("poison-payload", properties, routingKey: "worker.route", deliveryTag: 72), channel, CancellationToken.None);

        var reason = Assert.IsType<string>(Assert.Single(channel.Publishes).Properties.Headers!["AR-DeadLetter-Reason"]);
        Assert.Equal(-1, PortableText.IndexOfIllFormedUtf16(reason));
        Assert.Equal(longMessage[..511], reason);
    }

    /// <summary>
    /// Round 33 (B1): AT the cap with <c>x-death</c> present the catch still rejected without
    /// requeue — but x-death means the dead-letter exchange already returned this message once
    /// (DLX → retry queue → TTL → back), so that reject re-entered the same cycle and the poison
    /// message looped at the cycle's TTL rate forever; the cap never parked anything. Pre-fix:
    /// <c>BasicNack(requeue: false)</c>, no publish, no ACK. Now attempt 3 of 3 is copied to
    /// <see cref="RabbitMqAsyncResponseOptions.DeadLetterQueue"/> through the DEFAULT exchange
    /// (bypassing the cycling DLX) and ACKed so the loop ends.
    /// </summary>
    [Fact]
    public async Task Awaiting_AtCapWithXDeath_ParksInTheDeadLetterQueueAndAcks_InsteadOfRejectingBackIntoTheCycle()
    {
        var properties = new BasicProperties
        {
            CorrelationId = "cid-park",
            Headers = new Dictionary<string, object?>
            {
                ["x-death"] = new List<object?> { new Dictionary<string, object?> { ["count"] = 2L } }
            }
        };

        var channel = new FakeDispatcherChannel();
        await using var dispatcher = RabbitMqMessageDispatcher.Create(
            (_, _) => throw new InvalidOperationException("handler boom"),
            new RabbitMqAsyncResponseOptions { DeadLetterExchange = "dlx", DeadLetterQueue = "parked" },
            new RabbitMqSubscriberOptions { MaxDeliveryAttempts = 3 },
            NullLogger.Instance,
            "worker.q",
            RabbitMqSubscriberRole.Worker);

        var delivery = Delivery("poison-payload", properties, routingKey: "worker.route", deliveryTag: 71);
        Assert.Equal(3, RabbitMqMessageDispatcher.ResolveDeliveryAttempt(delivery)); // at the cap, and countable
        Assert.Equal(3, dispatcher.EffectiveDeliveryCap(delivery));

        await dispatcher.HandleAsync(delivery, channel, CancellationToken.None);

        var parked = Assert.Single(channel.Publishes);
        Assert.Equal(string.Empty, parked.Exchange); // the default exchange routes by queue name: never back through the DLX
        Assert.Equal("parked", parked.RoutingKey);
        Assert.Equal("poison-payload", Encoding.UTF8.GetString(parked.Body.ToArray()));
        Assert.Equal("cid-park", parked.Properties.CorrelationId);
        Assert.Equal("handler boom", Assert.IsType<string>(parked.Properties.Headers!["AR-DeadLetter-Reason"]));
        Assert.Equal("worker.q", Assert.IsType<string>(parked.Properties.Headers!["AR-DeadLetter-Source-Queue"]));
        Assert.Equal(71UL, Assert.Single(channel.Acks));
        Assert.Empty(channel.Nacks);
    }

    /// <summary>
    /// Round 33 (B1), no <see cref="RabbitMqAsyncResponseOptions.DeadLetterQueue"/>: there is nowhere
    /// to park the message, but another reject would still re-enter the dead-letter cycle forever, so
    /// the delivery is ACKed (the message is dropped) and the drop is logged as an error. Pre-fix:
    /// <c>BasicNack(requeue: false)</c>, no ACK.
    /// </summary>
    [Fact]
    public async Task Awaiting_AtCapWithXDeath_WithoutADeadLetterQueue_AcksTheDropAndLogsAnError()
    {
        var logger = new ListLogger();
        var properties = new BasicProperties
        {
            Headers = new Dictionary<string, object?>
            {
                ["x-death"] = new List<object?> { new Dictionary<string, object?> { ["count"] = 2L } }
            }
        };

        var channel = new FakeDispatcherChannel();
        await using var dispatcher = RabbitMqMessageDispatcher.Create(
            (_, _) => throw new InvalidOperationException("handler boom"),
            new RabbitMqAsyncResponseOptions { DeadLetterExchange = "dlx" },
            new RabbitMqSubscriberOptions { MaxDeliveryAttempts = 3 },
            logger,
            "worker.q",
            RabbitMqSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery("payload", properties, deliveryTag: 72), channel, CancellationToken.None);

        Assert.Equal(72UL, Assert.Single(channel.Acks));
        Assert.Empty(channel.Nacks);
        Assert.Empty(channel.Publishes);
        Assert.Contains(
            logger.Entries,
            entry => entry.Level == LogLevel.Error
                && entry.Message.Contains("72", StringComparison.Ordinal)
                && entry.Message.Contains(nameof(RabbitMqAsyncResponseOptions.DeadLetterQueue), StringComparison.Ordinal));
    }

    /// <summary>
    /// Round 33 (B2): the cap was consulted only in the catch, i.e. only when THIS attempt threw. A
    /// delivery whose previous attempt ended without a thrown exception — the process OOM-killed or
    /// FailFast mid-handler, a hang that tripped the broker's consumer_timeout — comes back requeued
    /// with <c>redelivered</c> set and ran again with no cap check at all, so with
    /// <c>MaxDeliveryAttempts = 1</c> one poison message crash-looped every replica in turn.
    /// Pre-fix: the handler ran (and here ACKed). Now attempt 2 of a 1-attempt cap is rejected
    /// without requeue BEFORE the handler runs.
    /// </summary>
    [Fact]
    public async Task Awaiting_RedeliveredPastTheCap_RejectsWithoutRunningTheHandler()
    {
        var channel = new FakeDispatcherChannel();
        var logger = new ListLogger();
        var handlerRuns = 0;
        await using var dispatcher = RabbitMqMessageDispatcher.Create(
            (_, _) =>
            {
                // The crash that ended the previous attempt was the process's, not the handler's:
                // nothing here throws, so the catch-side cap never sees this delivery.
                Interlocked.Increment(ref handlerRuns);
                return Task.CompletedTask;
            },
            new RabbitMqAsyncResponseOptions(),
            new RabbitMqSubscriberOptions { MaxDeliveryAttempts = 1 },
            logger,
            "worker.q",
            RabbitMqSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery("payload", deliveryTag: 81, redelivered: true), channel, CancellationToken.None);

        Assert.Equal(0, Volatile.Read(ref handlerRuns));
        var nack = Assert.Single(channel.Nacks);
        Assert.Equal(81UL, nack.DeliveryTag);
        Assert.False(nack.Requeue);
        Assert.Empty(channel.Acks);

        // Regression (r1 GS5#7): nothing threw, so nothing logged the reject either — and with no
        // dead-letter exchange the broker drops the message, i.e. a silent loss. Error when this
        // package knows of no dead-letter exchange.
        Assert.Contains(
            logger.Entries,
            entry => entry.Level == LogLevel.Error
                && entry.Message.Contains("81", StringComparison.Ordinal)
                && entry.Message.Contains(nameof(RabbitMqAsyncResponseOptions.DeadLetterExchange), StringComparison.Ordinal));
    }

    [Fact]
    public async Task Awaiting_RedeliveredPastTheCap_WithADeadLetterExchange_LogsTheRejectAsAWarning()
    {
        var channel = new FakeDispatcherChannel();
        var logger = new ListLogger();
        await using var dispatcher = RabbitMqMessageDispatcher.Create(
            (_, _) => Task.CompletedTask,
            new RabbitMqAsyncResponseOptions { DeadLetterExchange = "dlx" },
            new RabbitMqSubscriberOptions { MaxDeliveryAttempts = 1 },
            logger,
            "worker.q",
            RabbitMqSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery("payload", deliveryTag: 84, redelivered: true), channel, CancellationToken.None);

        Assert.False(Assert.Single(channel.Nacks).Requeue);
        Assert.Contains(
            logger.Entries,
            entry => entry.Level == LogLevel.Warning
                && entry.Message.Contains("84", StringComparison.Ordinal)
                && entry.Message.Contains("dlx", StringComparison.Ordinal));
        Assert.DoesNotContain(logger.Entries, entry => entry.Level >= LogLevel.Error);
    }

    /// <summary>
    /// Control for the pre-execution cap: attempt 1 of a 1-attempt cap is AT the cap, not past it,
    /// so the handler runs and its success ACKs exactly as before.
    /// </summary>
    [Fact]
    public async Task Awaiting_FirstDeliveryAtTheCap_StillRunsTheHandler()
    {
        var channel = new FakeDispatcherChannel();
        var handlerRuns = 0;
        await using var dispatcher = RabbitMqMessageDispatcher.Create(
            (_, _) =>
            {
                Interlocked.Increment(ref handlerRuns);
                return Task.CompletedTask;
            },
            new RabbitMqAsyncResponseOptions(),
            new RabbitMqSubscriberOptions { MaxDeliveryAttempts = 1 },
            NullLogger.Instance,
            "worker.q",
            RabbitMqSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery("payload", deliveryTag: 82, redelivered: false), channel, CancellationToken.None);

        Assert.Equal(1, Volatile.Read(ref handlerRuns));
        Assert.Equal(82UL, Assert.Single(channel.Acks));
        Assert.Empty(channel.Nacks);
    }

    /// <summary>
    /// Round 33 (B2), the guard's other arm: past the cap AND <c>x-death</c> present means the
    /// dead-letter cycle already returned this message, so a reject would only re-enter it (B1) —
    /// it is parked in <see cref="RabbitMqAsyncResponseOptions.DeadLetterQueue"/> and ACKed, and the
    /// handler never runs. Pre-fix: the handler ran again.
    /// </summary>
    [Fact]
    public async Task Awaiting_PastTheCapWithXDeath_ParksWithoutRunningTheHandler()
    {
        var properties = new BasicProperties
        {
            Headers = new Dictionary<string, object?>
            {
                ["x-death"] = new List<object?> { new Dictionary<string, object?> { ["count"] = 2L } }
            }
        };

        var channel = new FakeDispatcherChannel();
        var handlerRuns = 0;
        await using var dispatcher = RabbitMqMessageDispatcher.Create(
            (_, _) =>
            {
                Interlocked.Increment(ref handlerRuns);
                return Task.CompletedTask;
            },
            new RabbitMqAsyncResponseOptions { DeadLetterExchange = "dlx", DeadLetterQueue = "parked" },
            new RabbitMqSubscriberOptions { MaxDeliveryAttempts = 2 },
            NullLogger.Instance,
            "worker.q",
            RabbitMqSubscriberRole.Worker);

        var delivery = Delivery("payload", properties, deliveryTag: 83);
        Assert.Equal(3, RabbitMqMessageDispatcher.ResolveDeliveryAttempt(delivery)); // past a cap of 2

        await dispatcher.HandleAsync(delivery, channel, CancellationToken.None);

        Assert.Equal(0, Volatile.Read(ref handlerRuns));
        var parked = Assert.Single(channel.Publishes);
        Assert.Equal(string.Empty, parked.Exchange);
        Assert.Equal("parked", parked.RoutingKey);
        Assert.Equal(83UL, Assert.Single(channel.Acks));
        Assert.Empty(channel.Nacks);
    }

    [Fact]
    public void ResolveDeliveryAttempt_FreshDelivery_IsOne()
        => Assert.Equal(1, RabbitMqMessageDispatcher.ResolveDeliveryAttempt(Delivery("{}")));

    [Fact]
    public void ResolveDeliveryAttempt_Redelivered_IsTwo()
        => Assert.Equal(2, RabbitMqMessageDispatcher.ResolveDeliveryAttempt(Delivery("{}", redelivered: true)));

    [Fact]
    public void ResolveDeliveryAttempt_UsesXDeathCount()
    {
        var properties = new BasicProperties
        {
            Headers = new Dictionary<string, object?>
            {
                ["x-death"] = new List<object>
                {
                    new Dictionary<string, object?> { ["count"] = 4L }
                }
            }
        };

        Assert.Equal(5, RabbitMqMessageDispatcher.ResolveDeliveryAttempt(Delivery("{}", properties, redelivered: true)));
    }

    [Fact]
    public void ResolveDeliveryAttempt_IgnoresNonDictionaryXDeathEntries()
    {
        var properties = new BasicProperties
        {
            Headers = new Dictionary<string, object?> { ["x-death"] = new List<object> { "not-a-dictionary" } }
        };

        // No usable count: falls back to the redelivered flag (attempt 2).
        Assert.Equal(2, RabbitMqMessageDispatcher.ResolveDeliveryAttempt(Delivery("{}", properties, redelivered: true)));
    }

    [Fact]
    public void ResolveDeliveryAttempt_IgnoresMalformedXDeathCount()
    {
        var properties = new BasicProperties
        {
            Headers = new Dictionary<string, object?>
            {
                ["x-death"] = new List<object>
                {
                    new Dictionary<string, object?> { ["count"] = "not-a-number" }
                }
            }
        };

        // Unparseable count is ignored; a fresh, non-redelivered message stays at attempt 1.
        Assert.Equal(1, RabbitMqMessageDispatcher.ResolveDeliveryAttempt(Delivery("{}", properties)));
    }

    // ---------- QueuedRabbitMqMessageDispatcher (AckAfterEnqueue) ----------

    [Fact]
    public async Task Queued_EnqueuesAndAcksBeforeHandlerCompletes()
    {
        var channel = new FakeDispatcherChannel();
        var handlerStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseHandler = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var dispatcher = (QueuedRabbitMqMessageDispatcher)RabbitMqMessageDispatcher.Create(
            async (_, _) =>
            {
                handlerStarted.TrySetResult();
                await releaseHandler.Task.ConfigureAwait(false);
            },
            new RabbitMqAsyncResponseOptions(),
            EnqueueSubscriber(workers: 1, capacity: 8),
            NullLogger.Instance,
            "worker.q",
            RabbitMqSubscriberRole.Worker);

        try
        {
            await dispatcher.HandleAsync(Delivery("payload", deliveryTag: 21), channel, CancellationToken.None);

            Assert.Equal(21UL, Assert.Single(channel.Acks));
            await handlerStarted.Task.WaitAsync(TimeSpan.FromSeconds(2));
            Assert.Equal(0, dispatcher.PendingCount);
            Assert.Equal(1, dispatcher.RunningCount);

            releaseHandler.TrySetResult();
        }
        finally
        {
            releaseHandler.TrySetResult();
            await dispatcher.DisposeAsync();
        }

        Assert.Equal(0, dispatcher.RunningCount);
    }

    [Fact]
    public async Task Queued_WhenBackgroundQueueIsFull_PausesDeliveryLoopInsteadOfNacking()
    {
        var channel = new FakeDispatcherChannel();
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var dispatcher = RabbitMqMessageDispatcher.Create(
            async (_, _) => await gate.Task.ConfigureAwait(false),
            new RabbitMqAsyncResponseOptions(),
            EnqueueSubscriber(workers: 1, capacity: 1),
            NullLogger.Instance,
            "worker.q",
            RabbitMqSubscriberRole.Worker);

        // The gated worker holds m1; m2 fills the capacity-1 queue; m3 overflows.
        await dispatcher.HandleAsync(Delivery("m1", deliveryTag: 1), channel, CancellationToken.None);
        await dispatcher.HandleAsync(Delivery("m2", deliveryTag: 2), channel, CancellationToken.None);
        var overflow = dispatcher.HandleAsync(Delivery("m3", deliveryTag: 3), channel, CancellationToken.None);

        // The overflow delivery must park the handler — RabbitMQ.Client dispatches a channel's
        // deliveries sequentially, so this pauses the delivery loop — not NACK: the early ACKs
        // already returned the prefetch credit, so a NACK would redeliver and spin at network rate.
        // No ACK either until the delivery is actually enqueued.
        Assert.False(overflow.IsCompleted);
        Assert.Empty(channel.Nacks);
        Assert.Equal(new[] { 1UL, 2UL }, channel.Acks);

        gate.TrySetResult();
        await overflow.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Empty(channel.Nacks);
        Assert.Equal(new[] { 1UL, 2UL, 3UL }, channel.Acks); // every delivery ACKed exactly once
    }

    [Fact]
    public async Task Queued_DisposeWhileParkedOnFullQueue_RequeuesTheParkedDeliveryOnce()
    {
        var channel = new FakeDispatcherChannel();
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var dispatcher = (QueuedRabbitMqMessageDispatcher)RabbitMqMessageDispatcher.Create(
            async (_, _) => await gate.Task.ConfigureAwait(false),
            new RabbitMqAsyncResponseOptions(),
            EnqueueSubscriber(workers: 1, capacity: 1),
            NullLogger.Instance,
            "worker.q",
            RabbitMqSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery("m1", deliveryTag: 1), channel, CancellationToken.None);
        await dispatcher.HandleAsync(Delivery("m2", deliveryTag: 2), channel, CancellationToken.None);
        var overflow = dispatcher.HandleAsync(Delivery("m3", deliveryTag: 3), channel, CancellationToken.None);
        Assert.False(overflow.IsCompleted);

        // Draining completes the queue writer; the parked write must fall back to one NACK-requeue
        // (the delivery was never enqueued or ACKed) instead of throwing into the delivery callback.
        var disposing = dispatcher.DisposeAsync().AsTask();
        await overflow.WaitAsync(TimeSpan.FromSeconds(5));
        gate.TrySetResult();
        await disposing.WaitAsync(TimeSpan.FromSeconds(5));

        var nack = Assert.Single(channel.Nacks);
        Assert.Equal(3UL, nack.DeliveryTag);
        Assert.True(nack.Requeue);
        Assert.Equal(new[] { 1UL, 2UL }, channel.Acks);
        Assert.Equal(0, dispatcher.PendingCount); // the failed write refunded its pending slot
    }

    [Fact]
    public async Task Queued_DisposeWhileParkedOnFullQueue_ClosedChannel_DoesNotNack()
    {
        var channel = new FakeDispatcherChannel();
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var dispatcher = (QueuedRabbitMqMessageDispatcher)RabbitMqMessageDispatcher.Create(
            async (_, _) => await gate.Task.ConfigureAwait(false),
            new RabbitMqAsyncResponseOptions(),
            EnqueueSubscriber(workers: 1, capacity: 1),
            NullLogger.Instance,
            "worker.q",
            RabbitMqSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery("m1", deliveryTag: 1), channel, CancellationToken.None);
        await dispatcher.HandleAsync(Delivery("m2", deliveryTag: 2), channel, CancellationToken.None);
        var overflow = dispatcher.HandleAsync(Delivery("m3", deliveryTag: 3), channel, CancellationToken.None);
        Assert.False(overflow.IsCompleted);

        // A closed channel already returned the un-ACKed delivery to the broker; NACKing would throw.
        channel.IsOpen = false;
        var disposing = dispatcher.DisposeAsync().AsTask();
        await overflow.WaitAsync(TimeSpan.FromSeconds(5));
        gate.TrySetResult();
        await disposing.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Empty(channel.Nacks);
        Assert.Equal(0, dispatcher.PendingCount);
    }

    [Fact]
    public async Task Queued_WhenAckThrows_DoesNotNackEnqueuedDelivery()
    {
        var channel = new FakeDispatcherChannel { ThrowOnAck = new InvalidOperationException("ack failed") };
        var handled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var dispatcher = (QueuedRabbitMqMessageDispatcher)RabbitMqMessageDispatcher.Create(
            (_, _) =>
            {
                handled.TrySetResult();
                return Task.CompletedTask;
            },
            new RabbitMqAsyncResponseOptions(),
            EnqueueSubscriber(workers: 1, capacity: 8),
            NullLogger.Instance,
            "worker.q",
            RabbitMqSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery("payload", deliveryTag: 31), channel, CancellationToken.None);

        // The delivery was already handed to a background worker: a NACK here would race a duplicate
        // redelivery against the in-process execution, so the failed ACK is only logged.
        await handled.Task.WaitAsync(TimeSpan.FromSeconds(2));
        Assert.Empty(channel.Nacks);

        // Draining proves the pending counter was not double-decremented for the enqueued delivery.
        await dispatcher.DisposeAsync();
        Assert.Equal(0, dispatcher.PendingCount);
    }

    [Fact]
    public async Task Queued_BackgroundHandlerFailure_LogsOnceAndInvokesHook()
    {
        var channel = new FakeDispatcherChannel();
        var logger = new ListLogger();
        var failure = new TaskCompletionSource<RabbitMqBackgroundFailureContext>(TaskCreationOptions.RunContinuationsAsynchronously);
        var subscriber = EnqueueSubscriber(workers: 1, capacity: 8);
        subscriber.OnBackgroundFailure = context =>
        {
            failure.TrySetResult(context);
            return ValueTask.CompletedTask;
        };

        await using var dispatcher = RabbitMqMessageDispatcher.Create(
            (_, _) => Task.FromException(new InvalidOperationException("queued boom")),
            new RabbitMqAsyncResponseOptions(),
            subscriber,
            logger,
            "worker.q",
            RabbitMqSubscriberRole.ResponseIngress);

        await dispatcher.HandleAsync(
            Delivery("payload", exchange: "ex-1", routingKey: "rk-1", deliveryTag: 41),
            channel,
            CancellationToken.None);

        var context = await failure.Task.WaitAsync(TimeSpan.FromSeconds(2));
        Assert.Equal("worker.q", context.Queue);
        Assert.Equal("ResponseIngress", context.SubscriberRole);
        Assert.Equal("ex-1", context.Exchange);
        Assert.Equal("rk-1", context.RoutingKey);
        Assert.Equal(41UL, context.DeliveryTag);
        Assert.IsType<InvalidOperationException>(context.Exception);

        // The delivery was already ACKed, so the failure surfaces exactly once via the logger.
        Assert.Equal(41UL, Assert.Single(channel.Acks));
        var errors = logger.Entries
            .Where(entry => entry.Level == LogLevel.Error && entry.Exception is InvalidOperationException)
            .ToList();
        Assert.Single(errors);
        Assert.Contains("already-ACKed", errors[0].Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Queued_BackgroundHandlerFailure_WithoutHook_DoesNotThrow()
    {
        var channel = new FakeDispatcherChannel();
        var handlerRan = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var dispatcher = RabbitMqMessageDispatcher.Create(
            (_, _) =>
            {
                handlerRan.TrySetResult();
                return Task.FromException(new InvalidOperationException("queued boom"));
            },
            new RabbitMqAsyncResponseOptions(),
            EnqueueSubscriber(workers: 1, capacity: 8),
            NullLogger.Instance,
            "worker.q",
            RabbitMqSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery("payload", deliveryTag: 51), channel, CancellationToken.None);
        await handlerRan.Task.WaitAsync(TimeSpan.FromSeconds(2));

        // Draining must complete cleanly even though the background handler threw and no hook is set.
        await dispatcher.DisposeAsync();
        Assert.Equal(51UL, Assert.Single(channel.Acks));
    }

    [Fact]
    public async Task Queued_BackgroundFailureHookThrows_IsLogged()
    {
        var channel = new FakeDispatcherChannel();
        var logger = new ListLogger();
        var hookInvoked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var subscriber = EnqueueSubscriber(workers: 1, capacity: 8);
        subscriber.OnBackgroundFailure = _ =>
        {
            hookInvoked.TrySetResult();
            throw new InvalidOperationException("hook boom");
        };

        await using var dispatcher = RabbitMqMessageDispatcher.Create(
            (_, _) => Task.FromException(new InvalidOperationException("queued boom")),
            new RabbitMqAsyncResponseOptions(),
            subscriber,
            logger,
            "worker.q",
            RabbitMqSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery("payload", deliveryTag: 61), channel, CancellationToken.None);
        await hookInvoked.Task.WaitAsync(TimeSpan.FromSeconds(2));
        await dispatcher.DisposeAsync();

        Assert.Contains(
            logger.Entries,
            entry => entry.Level == LogLevel.Error
                && entry.Message.Contains("callback failed", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public async Task Queued_Dispose_DrainsQueuedWorkBeforeReturning()
    {
        var channel = new FakeDispatcherChannel();
        var handlerStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseHandler = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handlerCompleted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var dispatcher = RabbitMqMessageDispatcher.Create(
            async (_, _) =>
            {
                handlerStarted.TrySetResult();
                await releaseHandler.Task.ConfigureAwait(false);
                handlerCompleted.TrySetResult();
            },
            new RabbitMqAsyncResponseOptions(),
            EnqueueSubscriber(workers: 1, capacity: 8, drain: TimeSpan.FromSeconds(5)),
            NullLogger.Instance,
            "worker.q",
            RabbitMqSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery("payload", deliveryTag: 71), channel, CancellationToken.None);
        await handlerStarted.Task.WaitAsync(TimeSpan.FromSeconds(2));

        var disposeTask = dispatcher.DisposeAsync();
        await Task.Delay(50);
        Assert.False(disposeTask.IsCompleted);

        releaseHandler.TrySetResult();
        await disposeTask.AsTask().WaitAsync(TimeSpan.FromSeconds(2));
        Assert.True(handlerCompleted.Task.IsCompletedSuccessfully);
    }

    [Fact]
    public async Task Queued_Dispose_IsIdempotent()
    {
        var channel = new FakeDispatcherChannel();
        var dispatcher = RabbitMqMessageDispatcher.Create(
            (_, _) => Task.CompletedTask,
            new RabbitMqAsyncResponseOptions(),
            EnqueueSubscriber(workers: 2, capacity: 8),
            NullLogger.Instance,
            "worker.q",
            RabbitMqSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery("payload", deliveryTag: 81), channel, CancellationToken.None);

        await dispatcher.DisposeAsync();
        await dispatcher.DisposeAsync();
    }

    [Fact]
    public async Task Queued_Dispose_CancelsRunningHandlerAfterDrainTimeout()
    {
        var channel = new FakeDispatcherChannel();
        var handlerStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var cancellationObserved = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var logger = new ListLogger();
        var dispatcher = RabbitMqMessageDispatcher.Create(
            async (_, cancellationToken) =>
            {
                handlerStarted.TrySetResult();
                try
                {
                    await Task.Delay(TimeSpan.FromSeconds(30), cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    cancellationObserved.TrySetResult();
                    throw;
                }
            },
            new RabbitMqAsyncResponseOptions(),
            EnqueueSubscriber(workers: 1, capacity: 8, drain: TimeSpan.FromMilliseconds(50)),
            logger,
            "worker.q",
            RabbitMqSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery("payload", deliveryTag: 91), channel, CancellationToken.None);
        await handlerStarted.Task.WaitAsync(TimeSpan.FromSeconds(2));

        var elapsed = Stopwatch.StartNew();
        await dispatcher.DisposeAsync();
        elapsed.Stop();

        // The drain cancellation must reach the still-running handler; generous budget so the assertion is
        // not flaky under coverage-instrumented or CI parallel load (it only waits this long on real failure).
        await cancellationObserved.Task.WaitAsync(TimeSpan.FromSeconds(10));
        // Dispose returned via the drain timeout instead of waiting for the 30s handler to finish.
        Assert.True(elapsed.Elapsed < TimeSpan.FromSeconds(25));
        Assert.Contains(
            logger.Entries,
            entry => entry.Level == LogLevel.Warning && entry.Message.Contains("Timed out", StringComparison.Ordinal));
    }

    // ---------- RabbitMqSubscriberOptions.UseAckAfterEnqueue ----------

    [Fact]
    public void UseAckAfterEnqueue_RequiresPositiveSettings()
    {
        var options = new RabbitMqSubscriberOptions();

        Assert.Throws<ArgumentOutOfRangeException>(() => options.UseAckAfterEnqueue(0, 10));
        Assert.Throws<ArgumentOutOfRangeException>(() => options.UseAckAfterEnqueue(1, 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => options.UseAckAfterEnqueue(1, 10, TimeSpan.Zero));
    }

    [Fact]
    public void UseAckAfterEnqueue_AppliesSettings()
    {
        var options = new RabbitMqSubscriberOptions()
            .UseAckAfterEnqueue(3, 64, TimeSpan.FromSeconds(7));

        Assert.Equal(RabbitMqAckMode.AckAfterEnqueue, options.AckMode);
        Assert.Equal(3, options.BackgroundWorkerCount);
        Assert.Equal(64, options.BackgroundQueueCapacity);
        Assert.Equal(TimeSpan.FromSeconds(7), options.BackgroundDrainTimeout);
    }

    [Fact]
    public void UseAckAfterEnqueue_NullDrainTimeout_KeepsDefault()
    {
        var options = new RabbitMqSubscriberOptions();
        var defaultDrain = options.BackgroundDrainTimeout;

        options.UseAckAfterEnqueue(1, 8);

        Assert.Equal(defaultDrain, options.BackgroundDrainTimeout);
    }

    // ---------- helpers ----------

    [Fact]
    public async Task Queued_AfterTheDrainBudgetLapses_DoesNotStartFreshWork_DeadLettersAndSurfacesIt()
    {
        // Regression (round 31): the drain token cannot stop the REAL handler — it is
        // _ingress.HandleWorkerMessageAsync(payload), whose target takes no CancellationToken — so
        // the loop kept dequeuing and EXECUTING past the budget, and whatever was still queued at
        // process exit vanished with no record: those deliveries were ACKed at enqueue, so the
        // broker never redelivers them (DB/Redis/Pub-Sub parity).
        var channel = new FakeDispatcherChannel();
        var firstStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handlerRuns = 0;
        var failures = new List<RabbitMqBackgroundFailureContext>();
        var subscriber = EnqueueSubscriber(workers: 1, capacity: 8, drain: TimeSpan.FromMilliseconds(100));
        subscriber.OnBackgroundFailure = context =>
        {
            lock (failures)
            {
                failures.Add(context);
            }

            return ValueTask.CompletedTask;
        };

        var dispatcher = RabbitMqMessageDispatcher.Create(
            async (_, _) =>
            {
                // Deliberately ignores the token, exactly like the ingress handler in production.
                if (Interlocked.Increment(ref handlerRuns) == 1)
                {
                    firstStarted.TrySetResult();
                    await releaseFirst.Task.ConfigureAwait(false);
                }
            },
            new RabbitMqAsyncResponseOptions { DeadLetterExchange = "dlx" },
            subscriber,
            NullLogger.Instance,
            "worker.q",
            RabbitMqSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery("m1", deliveryTag: 1), channel, CancellationToken.None);
        await firstStarted.Task.WaitAsync(TimeSpan.FromSeconds(2));
        await dispatcher.HandleAsync(Delivery("m2", deliveryTag: 2), channel, CancellationToken.None); // ACKed, waiting in queue

        await dispatcher.DisposeAsync(); // the 100ms drain budget lapses while the first handler blocks
        releaseFirst.TrySetResult();      // ...and only now can the loop reach the queued entry

        var guard = TimeProvider.System.GetUtcNow() + TimeSpan.FromSeconds(10);
        while (true)
        {
            lock (failures)
            {
                if (failures.Count == 1)
                    break;
            }

            Assert.True(TimeProvider.System.GetUtcNow() < guard, "the undrained delivery was never surfaced");
            await Task.Delay(TimeSpan.FromMilliseconds(5));
        }

        lock (failures)
        {
            Assert.IsAssignableFrom<OperationCanceledException>(Assert.Single(failures).Exception);
        }

        // The queued entry was NOT executed after the budget lapsed — and, unlike a mid-handler
        // interruption, it was published to the DLX: it was ACKed at enqueue, so nothing else can
        // ever record it.
        Assert.Equal(1, Volatile.Read(ref handlerRuns));
        var buried = Assert.Single(channel.Publishes);
        Assert.Equal("dlx", buried.Exchange);
        Assert.Equal("m2", Encoding.UTF8.GetString(buried.Body.ToArray()));
    }

    private static RabbitMqSubscriberOptions EnqueueSubscriber(
        int workers = 1,
        int capacity = 8,
        TimeSpan? drain = null)
        => new RabbitMqSubscriberOptions()
            .UseAckAfterEnqueue(workers, capacity, drain ?? TimeSpan.FromSeconds(5));

    [Fact]
    public async Task QueuedDispose_SurvivesAWorkerFaultingOutsideItsHandlerGuard()
    {
        // Regression: the drain join caught only TimeoutException (the shared DB base and NATS
        // also carry a general arm). A worker faulting outside its handler guard — here the log
        // sink throwing from the "handler failed" entry inside the catch arm — rethrew from
        // Task.WhenAll, escaped DisposeAsync into the subscriber's `await using` and leaked the
        // drain token source.
        var logger = new ErrorThrowingLogger();
        var dispatcher = RabbitMqMessageDispatcher.Create(
            (_, _) => throw new InvalidOperationException("handler boom"),
            new RabbitMqAsyncResponseOptions(),
            new RabbitMqSubscriberOptions().UseAckAfterEnqueue(1, 8, TimeSpan.FromSeconds(5)),
            logger,
            "worker.q",
            RabbitMqSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery("payload", deliveryTag: 1), new FakeDispatcherChannel(), CancellationToken.None);
        await logger.ErrorThrown.Task.WaitAsync(TimeSpan.FromSeconds(5));

        await dispatcher.DisposeAsync();
    }

    // ---------- Round-1 fixpoint (G10) ----------

    [Fact]
    public async Task Awaiting_HostStopHandBack_WhileTheSubscriberTokenIsLive_LeavesTheDeliveryUnsettled()
    {
        // Regression (r1 S7#1): the host fires ApplicationStopping before it stops any hosted
        // service, so the flow engine hands a parked flow's delivery back
        // (DurableFlowInterruptedException) while this subscriber's own token is still live. The
        // shutdown filter keyed only on that token, so the hand-back ran the FAILURE branch — at a
        // cap of 1, a reject without requeue: a flow's only wake-up dropped (or dead-lettered) by
        // a routine deploy. Left un-ACKed, the channel close redelivers it after the restart.
        var channel = new FakeDispatcherChannel();
        var logger = new ListLogger();
        await using var dispatcher = RabbitMqMessageDispatcher.Create(
            (_, _) => Task.FromException(new DurableFlowInterruptedException("Host is stopping; the wake-up is handed back.")),
            new RabbitMqAsyncResponseOptions(),
            new RabbitMqSubscriberOptions { MaxDeliveryAttempts = 1 },
            logger,
            "worker.q",
            RabbitMqSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery("payload", deliveryTag: 5), channel, CancellationToken.None);

        Assert.Empty(channel.Acks);
        Assert.Empty(channel.Nacks);
        Assert.DoesNotContain(logger.Entries, entry => entry.Level >= LogLevel.Error);
    }

    [Fact]
    public async Task Awaiting_Dispose_WaitsForTheRunningHandler_SoItsAckLandsBeforeTheChannelCloses()
    {
        // Regression (r1 S7#8): the stop path cancels the consumer, disposes the dispatcher, then
        // closes the channel — and the awaiting dispatcher's dispose was a no-op, so the close ran
        // under a handler still in its delivery callback. The broker requeued that delivery for a
        // peer, and the finished handler's ACK then failed on the closed channel: every deploy
        // ran in-flight jobs twice. Dispose now waits (bounded) for the callback to leave.
        var channel = new FakeDispatcherChannel();
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var dispatcher = RabbitMqMessageDispatcher.Create(
            async (_, _) =>
            {
                started.TrySetResult();
                await release.Task.ConfigureAwait(false);
            },
            new RabbitMqAsyncResponseOptions(),
            new RabbitMqSubscriberOptions(),
            NullLogger.Instance,
            "worker.q",
            RabbitMqSubscriberRole.Worker);

        var handling = dispatcher.HandleAsync(Delivery("payload", deliveryTag: 9), channel, CancellationToken.None);
        await started.Task.WaitAsync(TimeSpan.FromSeconds(5));

        try
        {
            var disposing = dispatcher.DisposeAsync().AsTask();
            Assert.False(disposing.IsCompleted); // completes only once the callback has left
            Assert.Empty(channel.Acks);

            release.TrySetResult();
            await disposing.WaitAsync(TimeSpan.FromSeconds(10));
            Assert.Equal(9UL, Assert.Single(channel.Acks)); // settled BEFORE dispose returned, i.e. before the close
        }
        finally
        {
            release.TrySetResult();
            await handling.WaitAsync(TimeSpan.FromSeconds(10));
        }
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Awaiting_OnceTheSubscriberIsStopping_ABufferedDeliveryIsNotStarted(bool stopViaDispose)
    {
        // Regression (r1 S7#8): RabbitMQ.Client keeps dispatching the deliveries it had buffered
        // after the consumer cancel, and HandleAsync started each one — work the channel close
        // then cut off and the broker redelivered to a peer. Left un-ACKed and unstarted instead.
        var channel = new FakeDispatcherChannel();
        var handlerRuns = 0;
        var dispatcher = RabbitMqMessageDispatcher.Create(
            (_, _) =>
            {
                Interlocked.Increment(ref handlerRuns);
                return Task.CompletedTask;
            },
            new RabbitMqAsyncResponseOptions(),
            new RabbitMqSubscriberOptions(),
            NullLogger.Instance,
            "worker.q",
            RabbitMqSubscriberRole.Worker);

        if (stopViaDispose)
            await dispatcher.DisposeAsync();

        await dispatcher.HandleAsync(
            Delivery("payload", deliveryTag: 10),
            channel,
            stopViaDispose ? CancellationToken.None : new CancellationToken(canceled: true));

        Assert.Equal(0, Volatile.Read(ref handlerRuns));
        Assert.Empty(channel.Acks);
        Assert.Empty(channel.Nacks);
        await dispatcher.DisposeAsync();
    }

    [Fact]
    public async Task Awaiting_Dispose_GivesUpOnAHandlerStillRunningPastItsBound_AndSaysSo()
    {
        var logger = new ListLogger();
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var dispatcher = RabbitMqMessageDispatcher.Create(
            async (_, _) =>
            {
                started.TrySetResult();
                await release.Task.ConfigureAwait(false);
            },
            new RabbitMqAsyncResponseOptions(),
            new RabbitMqSubscriberOptions { BackgroundDrainTimeout = TimeSpan.FromMilliseconds(50) },
            logger,
            "worker.q",
            RabbitMqSubscriberRole.Worker);

        var handling = dispatcher.HandleAsync(Delivery("payload", deliveryTag: 11), new FakeDispatcherChannel(), CancellationToken.None);
        await started.Task.WaitAsync(TimeSpan.FromSeconds(5));

        await dispatcher.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Contains(
            logger.Entries,
            entry => entry.Level == LogLevel.Warning && entry.Message.Contains("still running", StringComparison.Ordinal));
        release.TrySetResult();
        await handling.WaitAsync(TimeSpan.FromSeconds(10));
    }

    [Fact]
    public async Task Awaiting_Dispose_WaitsOnlyForWhatTheHostBudgetLeavesAfterTheCancelAndTheClose()
    {
        // The in-flight wait is clamped, not validated: HostShutdownTimeout 10 s minus the stop
        // path's two ShutdownTimeout spends (5 s each) leaves nothing, so the stop does not wait at
        // all — a configuration that started before this wait existed must not overrun its host.
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var dispatcher = RabbitMqMessageDispatcher.Create(
            async (_, _) =>
            {
                started.TrySetResult();
                await release.Task.ConfigureAwait(false);
            },
            new RabbitMqAsyncResponseOptions { HostShutdownTimeout = TimeSpan.FromSeconds(10) },
            new RabbitMqSubscriberOptions(), // BackgroundDrainTimeout stays at its 20 s default
            NullLogger.Instance,
            "worker.q",
            RabbitMqSubscriberRole.Worker);

        var handling = dispatcher.HandleAsync(Delivery("payload", deliveryTag: 12), new FakeDispatcherChannel(), CancellationToken.None);
        await started.Task.WaitAsync(TimeSpan.FromSeconds(5));

        try
        {
            Assert.True(dispatcher.DisposeAsync().IsCompleted);
        }
        finally
        {
            release.TrySetResult();
            await handling.WaitAsync(TimeSpan.FromSeconds(10));
        }
    }

    [Fact]
    public void ValidateOptions_RejectsANegativeMaxDeliveryAttempts()
    {
        // Regression (r1 GS5#10): every cap check is `> 0` / `<= 0`, so -1 silently meant
        // "unlimited" (ASB, Kafka and NATS reject it).
        var ex = Assert.Throws<InvalidOperationException>(() => RabbitMqMessageDispatcher.ValidateOptions(
            new RabbitMqAsyncResponseOptions(),
            new RabbitMqSubscriberOptions { MaxDeliveryAttempts = -1 },
            RabbitMqSubscriberRole.Worker));

        Assert.Contains(nameof(RabbitMqSubscriberOptions.MaxDeliveryAttempts), ex.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("asyncresponse.worker")]   // default WorkerQueue
    [InlineData("asyncresponse.response")] // default ResponseQueue
    public void ValidateOptions_RejectsAParkQueueThatIsALiveQueue(string liveQueue)
    {
        // Round 42 shipped ParkQueue untested (r1 S7#11): parked into a live queue, a capped message
        // is redelivered straight back to the subscriber that parked it — past its cap — in a loop.
        var ex = Assert.Throws<InvalidOperationException>(() => RabbitMqMessageDispatcher.ValidateOptions(
            new RabbitMqAsyncResponseOptions { ParkQueue = liveQueue },
            new RabbitMqSubscriberOptions(),
            RabbitMqSubscriberRole.Worker));

        Assert.Contains(nameof(RabbitMqAsyncResponseOptions.ParkQueue), ex.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void ValidateOptions_RejectsANonPositiveBrokerConsumerTimeout(int minutes)
    {
        // r1 S7#11: ValidateConsumerTimeout had no test. Zero or negative leaves the flow engine no
        // room for any in-process wait; null (the broker's timeout disabled) is the way to say
        // "no ceiling".
        var ex = Assert.Throws<InvalidOperationException>(() => RabbitMqMessageDispatcher.ValidateOptions(
            new RabbitMqAsyncResponseOptions { BrokerConsumerTimeout = TimeSpan.FromMinutes(minutes) },
            new RabbitMqSubscriberOptions(),
            RabbitMqSubscriberRole.Worker));

        Assert.Contains(nameof(RabbitMqAsyncResponseOptions.BrokerConsumerTimeout), ex.Message, StringComparison.Ordinal);
        RabbitMqMessageDispatcher.ValidateOptions(
            new RabbitMqAsyncResponseOptions { BrokerConsumerTimeout = null },
            new RabbitMqSubscriberOptions(),
            RabbitMqSubscriberRole.Worker);
    }

    [Fact]
    public async Task Awaiting_AtCapWithXDeath_ParksInTheParkQueue_AheadOfTheDeadLetterQueue()
    {
        // r1 S7#11: ParkQueue precedence was untested. With both set, the park goes to ParkQueue
        // (unbound, so a TTL-retry cycle's hops never land there) — never to DeadLetterQueue.
        var properties = new BasicProperties
        {
            Headers = new Dictionary<string, object?>
            {
                ["x-death"] = new List<object?> { new Dictionary<string, object?> { ["count"] = 2L } }
            }
        };

        var channel = new FakeDispatcherChannel();
        await using var dispatcher = RabbitMqMessageDispatcher.Create(
            (_, _) => throw new InvalidOperationException("handler boom"),
            new RabbitMqAsyncResponseOptions { DeadLetterExchange = "dlx", DeadLetterQueue = "dead", ParkQueue = "parked" },
            new RabbitMqSubscriberOptions { MaxDeliveryAttempts = 3 },
            NullLogger.Instance,
            "worker.q",
            RabbitMqSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery("payload", properties, deliveryTag: 73), channel, CancellationToken.None);

        var parked = Assert.Single(channel.Publishes);
        Assert.Equal(string.Empty, parked.Exchange);
        Assert.Equal("parked", parked.RoutingKey);
        Assert.Equal(73UL, Assert.Single(channel.Acks));
    }

    [Fact]
    public async Task Awaiting_AFailedPark_IsRequeuedAfterABackoff_NotAckedOrLeftUnsettled()
    {
        // r1 S7#11: the round-42 failed-park path was untested. A park publish that fails (with
        // publisher confirms: the queue is missing, a reject-publish queue is full) must hand the
        // delivery back — NACK with requeue after the subscriber backoff — never ACK it (the
        // message would be gone) and never leave it unsettled (AMQP does not redeliver while the
        // channel stays open, so every failed park pinned one prefetch credit).
        var properties = new BasicProperties
        {
            Headers = new Dictionary<string, object?>
            {
                ["x-death"] = new List<object?> { new Dictionary<string, object?> { ["count"] = 2L } }
            }
        };

        var logger = new ListLogger();
        var channel = new FakeDispatcherChannel { ThrowOnPublish = new InvalidOperationException("NOT_FOUND - no queue 'parked'") };
        await using var dispatcher = RabbitMqMessageDispatcher.Create(
            (_, _) => throw new InvalidOperationException("handler boom"),
            new RabbitMqAsyncResponseOptions
            {
                ParkQueue = "parked",
                SubscriberRetryBaseDelay = TimeSpan.FromMilliseconds(1),
                SubscriberRetryMaxDelay = TimeSpan.FromMilliseconds(2)
            },
            new RabbitMqSubscriberOptions { MaxDeliveryAttempts = 3 },
            logger,
            "worker.q",
            RabbitMqSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery("payload", properties, deliveryTag: 74), channel, CancellationToken.None);

        Assert.Empty(channel.Acks);
        var nack = Assert.Single(channel.Nacks);
        Assert.Equal(74UL, nack.DeliveryTag);
        Assert.True(nack.Requeue);
        Assert.Contains(
            logger.Entries,
            entry => entry.Level == LogLevel.Error
                && entry.Message.Contains("74", StringComparison.Ordinal)
                && entry.Message.Contains("parked", StringComparison.Ordinal));
    }

    [Fact]
    public void ResolveDeliveryAttempt_SaturatesAForgedXDeathCount_InsteadOfWrappingBelowEveryCap()
    {
        // r1 S7#11: the round-42 saturation was untested. x-death is a publisher-writable header; a
        // count of long.MaxValue used to wrap on the +1 and cast to attempt 0 — below every cap.
        var delivery = Delivery("payload", new BasicProperties
        {
            Headers = new Dictionary<string, object?>
            {
                ["x-death"] = new List<object?> { new Dictionary<string, object?> { ["count"] = long.MaxValue } }
            }
        });

        Assert.Equal(int.MaxValue, RabbitMqMessageDispatcher.ResolveDeliveryAttempt(delivery));
    }

    [Fact]
    public async Task Queued_ACopyThatCameBackThroughTheDeadLetterExchange_IsParked_InsteadOfCopiedIntoTheCycleAgain()
    {
        // Regression (r1 S7#14): with early ACK plus an operator's TTL-retry queue bound to the
        // dead-letter exchange, the dead-letter copy of a permanently failing job rode the cycle
        // back into the worker queue, failed again, and was copied to the DLX again — re-executed,
        // side effects included, once per TTL for ever. A delivery that carries this queue's own
        // dead-letter marker is now parked through the default exchange instead.
        var channel = new FakeDispatcherChannel();
        var dispatcher = RabbitMqMessageDispatcher.Create(
            (_, _) => throw new InvalidOperationException("still broken"),
            new RabbitMqAsyncResponseOptions { DeadLetterExchange = "dlx", ParkQueue = "parked" },
            EnqueueSubscriber(workers: 1, capacity: 8),
            NullLogger.Instance,
            "worker.q",
            RabbitMqSubscriberRole.Worker);

        try
        {
            // As it comes back from the broker: string headers arrive as bytes.
            var cycled = new BasicProperties
            {
                Headers = new Dictionary<string, object?>
                {
                    ["AR-DeadLetter-Source-Queue"] = Encoding.UTF8.GetBytes("worker.q"),
                    ["x-death"] = new List<object?> { new Dictionary<string, object?> { ["count"] = 1L } }
                }
            };
            await dispatcher.HandleAsync(Delivery("poison", cycled, routingKey: "worker.route", deliveryTag: 95), channel, CancellationToken.None);
            await WaitForPublishesAsync(channel, 1);

            var parked = Assert.Single(channel.Publishes);
            Assert.Equal(string.Empty, parked.Exchange);
            Assert.Equal("parked", parked.RoutingKey);
        }
        finally
        {
            await dispatcher.DisposeAsync();
        }
    }

    [Fact]
    public async Task Queued_ACycledCopyWithNowhereToPark_IsDroppedWithAnError_NotCopiedIntoTheCycleAgain()
    {
        var channel = new FakeDispatcherChannel();
        var logger = new ListLogger();
        var dispatcher = RabbitMqMessageDispatcher.Create(
            (_, _) => throw new InvalidOperationException("still broken"),
            new RabbitMqAsyncResponseOptions { DeadLetterExchange = "dlx" },
            EnqueueSubscriber(workers: 1, capacity: 8),
            logger,
            "worker.q",
            RabbitMqSubscriberRole.Worker);

        try
        {
            var cycled = new BasicProperties
            {
                Headers = new Dictionary<string, object?> { ["AR-DeadLetter-Source-Queue"] = "worker.q" }
            };
            await dispatcher.HandleAsync(Delivery("poison", cycled, deliveryTag: 96), channel, CancellationToken.None);
        }
        finally
        {
            // The drain waits for the worker, so the settlement has run when this returns.
            await dispatcher.DisposeAsync();
        }

        Assert.Empty(channel.Publishes);
        Assert.Contains(
            logger.Entries,
            entry => entry.Level == LogLevel.Error
                && entry.Message.Contains("96", StringComparison.Ordinal)
                && entry.Message.Contains("dropped", StringComparison.Ordinal));
    }

    [Fact]
    public async Task Queued_AFailureCarryingAnotherQueuesDeathHistory_IsStillCopiedToTheDeadLetterExchange()
    {
        // Scope guard for the park rule above: x-death alone is not the marker — a delay queue that
        // dead-letters INTO the worker exchange stamps it on every message — so without this
        // queue's own dead-letter marker the copy goes to the DLX exactly as before.
        var channel = new FakeDispatcherChannel();
        var dispatcher = RabbitMqMessageDispatcher.Create(
            (_, _) => throw new InvalidOperationException("boom"),
            new RabbitMqAsyncResponseOptions { DeadLetterExchange = "dlx", ParkQueue = "parked" },
            EnqueueSubscriber(workers: 1, capacity: 8),
            NullLogger.Instance,
            "worker.q",
            RabbitMqSubscriberRole.Worker);

        try
        {
            var delayed = new BasicProperties
            {
                Headers = new Dictionary<string, object?>
                {
                    ["x-death"] = new List<object?> { new Dictionary<string, object?> { ["count"] = 1L, ["queue"] = "delay.q" } }
                }
            };
            await dispatcher.HandleAsync(Delivery("job", delayed, routingKey: "worker.route", deliveryTag: 97), channel, CancellationToken.None);
        }
        finally
        {
            await dispatcher.DisposeAsync();
        }

        var copy = Assert.Single(channel.Publishes);
        Assert.Equal("dlx", copy.Exchange);
        Assert.Equal("worker.route", copy.RoutingKey);
    }

    [Fact]
    public async Task Queued_DrainLapse_DisposeItselfDeadLettersAndSurfacesTheQueuedDeliveries_BeforeReturning()
    {
        // Regression (r1 GS5#4): once the drain budget lapsed, the entries still queued were routed
        // only by the worker loop's lapse branch — which runs only when a busy worker frees up.
        // With the one worker still inside a long handler, DisposeAsync returned with nothing
        // routed, the subscriber closed the channel, and both already-ACKed deliveries were lost
        // with no DLX copy and no OnBackgroundFailure. A reserved quarter of the budget now routes
        // them from DisposeAsync itself, on the still-open channel.
        var channel = new FakeDispatcherChannel();
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var failures = new ConcurrentQueue<RabbitMqBackgroundFailureContext>();
        var subscriber = EnqueueSubscriber(workers: 1, capacity: 8, drain: TimeSpan.FromSeconds(2)); // a 500 ms reserve: generous on a loaded runner
        subscriber.OnBackgroundFailure = context =>
        {
            failures.Enqueue(context);
            return ValueTask.CompletedTask;
        };
        var dispatcher = RabbitMqMessageDispatcher.Create(
            async (delivery, _) =>
            {
                if (delivery.DeliveryTag == 1)
                {
                    started.TrySetResult();
                    await release.Task.ConfigureAwait(false); // ignores the drain token, like the ingress
                }
            },
            new RabbitMqAsyncResponseOptions { DeadLetterExchange = "dlx" },
            subscriber,
            NullLogger.Instance,
            "worker.q",
            RabbitMqSubscriberRole.Worker);

        try
        {
            await dispatcher.HandleAsync(Delivery("m1", deliveryTag: 1), channel, CancellationToken.None);
            await started.Task.WaitAsync(TimeSpan.FromSeconds(5));
            await dispatcher.HandleAsync(Delivery("m2", deliveryTag: 2), channel, CancellationToken.None);
            await dispatcher.HandleAsync(Delivery("m3", deliveryTag: 3), channel, CancellationToken.None);

            await dispatcher.DisposeAsync();

            // Asserted at the moment DisposeAsync returned — before the channel would close.
            Assert.Equal([2UL, 3UL], failures.Select(failure => failure.DeliveryTag).Order().ToArray());
            Assert.Equal(["m2", "m3"], channel.Publishes.Select(publish => Encoding.UTF8.GetString(publish.Body.ToArray())).Order().ToArray());
            Assert.All(channel.Publishes, publish => Assert.Equal("dlx", publish.Exchange));
        }
        finally
        {
            release.TrySetResult();
        }
    }

    // ---------- Round-1 fixpoint, pre-commit (H) ----------

    [Fact]
    public async Task Queued_AHostStopHandBack_IsAWarning_SurfacedAndDeadLetteredWithItsOwnReason()
    {
        // Pre-commit r1 H6 — one early-ACK hand-back rule across the transports that can
        // dead-letter: a durable flow handed back at host stop is a stop, not a handler failure (an
        // Error per deploy was an alert per deploy), but the delivery was ACKed at enqueue and is
        // never redelivered, so it keeps its OnBackgroundFailure call and its dead-letter copy — the
        // wake-up's only record, safe to replay — under a reason that tells it apart.
        var channel = new FakeDispatcherChannel();
        var logger = new ListLogger();
        var notified = new TaskCompletionSource<RabbitMqBackgroundFailureContext>(TaskCreationOptions.RunContinuationsAsynchronously);
        var subscriber = EnqueueSubscriber(workers: 1, capacity: 8);
        subscriber.OnBackgroundFailure = context =>
        {
            notified.TrySetResult(context);
            return ValueTask.CompletedTask;
        };
        var dispatcher = RabbitMqMessageDispatcher.Create(
            (_, _) => Task.FromException(new DurableFlowInterruptedException("Host is stopping; the wake-up is handed back.")),
            new RabbitMqAsyncResponseOptions { DeadLetterExchange = "dlx" },
            subscriber,
            logger,
            "worker.q",
            RabbitMqSubscriberRole.Worker);

        try
        {
            await dispatcher.HandleAsync(Delivery("wake-up", routingKey: "worker.route", deliveryTag: 93), channel, CancellationToken.None);
            var context = await notified.Task.WaitAsync(TimeSpan.FromSeconds(10));
            await WaitForPublishesAsync(channel, 1);

            Assert.DoesNotContain(logger.Entries, entry => entry.Level >= LogLevel.Error);
            var warning = Assert.Single(logger.Entries, entry => entry.Level == LogLevel.Warning && entry.Exception is DurableFlowInterruptedException);
            Assert.Contains("Dead-lettering a copy (handed_back_after_commit)", warning.Message, StringComparison.Ordinal);
            Assert.IsType<DurableFlowInterruptedException>(context.Exception);
            var buried = Assert.Single(channel.Publishes);
            Assert.Equal("dlx", buried.Exchange);
            Assert.Equal("worker.route", buried.RoutingKey);
            Assert.Equal(
                "handed_back_after_commit: Host is stopping; the wake-up is handed back.",
                Assert.IsType<string>(buried.Properties.Headers!["AR-DeadLetter-Reason"]));
        }
        finally
        {
            await dispatcher.DisposeAsync();
        }
    }

    [Fact]
    public async Task Queued_AHostStopHandBack_WithoutADeadLetterExchange_IsAnError_ThatSaysTheWakeUpIsLost()
    {
        // Pre-commit r1 pass 2 (critic E 1): with no DeadLetterExchange — the default — the
        // hand-back logged a Warning claiming the delivery "is dead-lettered for replay", but no copy
        // is written and the broker never redelivers an ACKed delivery: a lost wake-up, reported as
        // a routine stop. It is an Error that says so; OnBackgroundFailure is still its one record.
        var channel = new FakeDispatcherChannel();
        var logger = new ListLogger();
        var notified = new TaskCompletionSource<RabbitMqBackgroundFailureContext>(TaskCreationOptions.RunContinuationsAsynchronously);
        var subscriber = EnqueueSubscriber(workers: 1, capacity: 8);
        subscriber.OnBackgroundFailure = context =>
        {
            notified.TrySetResult(context);
            return ValueTask.CompletedTask;
        };
        var dispatcher = RabbitMqMessageDispatcher.Create(
            (_, _) => Task.FromException(new DurableFlowInterruptedException("Host is stopping; the wake-up is handed back.")),
            new RabbitMqAsyncResponseOptions(),
            subscriber,
            logger,
            "worker.q",
            RabbitMqSubscriberRole.Worker);

        try
        {
            await dispatcher.HandleAsync(Delivery("wake-up", routingKey: "worker.route", deliveryTag: 94), channel, CancellationToken.None);
            var context = await notified.Task.WaitAsync(TimeSpan.FromSeconds(10));

            Assert.IsType<DurableFlowInterruptedException>(context.Exception);
            Assert.DoesNotContain(logger.Entries, entry => entry.Level == LogLevel.Warning && entry.Exception is DurableFlowInterruptedException);
            var error = Assert.Single(logger.Entries, entry => entry.Level >= LogLevel.Error);
            Assert.IsType<DurableFlowInterruptedException>(error.Exception);
            Assert.Contains("no dead-letter destination is configured, so the wake-up is lost", error.Message, StringComparison.Ordinal);
        }
        finally
        {
            await dispatcher.DisposeAsync();
        }

        Assert.Empty(channel.Publishes);
    }

    [Fact]
    public async Task Awaiting_AHostStopHandBack_DoesNotMarkTheReceiveSpanAsAnError()
    {
        // Pre-commit r1 H7: the hand-back was no longer logged as a failure, but the receive span
        // still got an Error status — every deploy painted a red span per parked flow.
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == AsyncResponseDiagnostics.ActivitySourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded
        };
        ActivitySource.AddActivityListener(listener);

        static async Task<Activity> ReceiveSpanOfAsync(Exception thrown)
        {
            Activity? observed = null;
            await using var dispatcher = RabbitMqMessageDispatcher.Create(
                (_, _) =>
                {
                    observed = Activity.Current;
                    return Task.FromException(thrown);
                },
                new RabbitMqAsyncResponseOptions(),
                new RabbitMqSubscriberOptions(),
                NullLogger.Instance,
                "worker.q",
                RabbitMqSubscriberRole.Worker);

            await dispatcher.HandleAsync(Delivery("payload", deliveryTag: 14), new FakeDispatcherChannel(), CancellationToken.None);
            Assert.NotNull(observed);
            Assert.Equal("asyncresponse.rabbitmq.receive", observed!.OperationName);
            return observed;
        }

        var handedBack = await ReceiveSpanOfAsync(new DurableFlowInterruptedException("Host is stopping."));
        Assert.NotEqual(ActivityStatusCode.Error, handedBack.Status);
        Assert.Null(handedBack.GetTagItem("error.type"));

        // A real failure still is one.
        Assert.Equal(ActivityStatusCode.Error, (await ReceiveSpanOfAsync(new InvalidOperationException("handler boom"))).Status);
    }

    [Fact]
    public async Task Awaiting_Dispose_WithNoInFlightWaitLeft_SaysSoOnceAtStartup_InsteadOfWarningOnEveryStop()
    {
        // Pre-commit r1 H7: with the wait clamped to zero (HostShutdownTimeout 10 s minus the two
        // 5 s ShutdownTimeout spends — the conformance suite's budget), every stop with a handler in
        // flight logged "Waiting up to 00:00:00" and then warned the handler was "still running
        // 00:00:00 after the subscriber began stopping" — a timeout that never happened. The
        // configuration is now reported once, when the dispatcher is created.
        var logger = new ListLogger();
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var dispatcher = RabbitMqMessageDispatcher.Create(
            async (_, _) =>
            {
                started.TrySetResult();
                await release.Task.ConfigureAwait(false);
            },
            new RabbitMqAsyncResponseOptions { HostShutdownTimeout = TimeSpan.FromSeconds(10) },
            new RabbitMqSubscriberOptions(), // BackgroundDrainTimeout stays at its 20 s default
            logger,
            "worker.q",
            RabbitMqSubscriberRole.Worker);
        var atStartup = logger.Entries.ToArray();

        var handling = dispatcher.HandleAsync(Delivery("payload", deliveryTag: 15), new FakeDispatcherChannel(), CancellationToken.None);
        await started.Task.WaitAsync(TimeSpan.FromSeconds(5));

        try
        {
            Assert.True(dispatcher.DisposeAsync().IsCompleted);
            Assert.DoesNotContain(logger.Entries, entry => entry.Level >= LogLevel.Warning);
            Assert.DoesNotContain(logger.Entries, entry => entry.Message.Contains("00:00:00", StringComparison.Ordinal));
        }
        finally
        {
            release.TrySetResult();
            await handling.WaitAsync(TimeSpan.FromSeconds(10));
        }

        var said = Assert.Single(atStartup, entry => entry.Message.Contains("will not wait for a running handler", StringComparison.Ordinal));
        Assert.Equal(LogLevel.Information, said.Level);
        Assert.Contains("HostShutdownTimeout", said.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Queued_ADeliveryArrivingOnceTheDrainHasBegun_IsLeftForTheCloseToRequeue_NotNackedBackIntoALoop()
    {
        // Pre-commit r1 H4 follow-on: a stop whose consumer cancel failed now still drains before
        // the close — with the consumer possibly still registered. Every delivery arriving meanwhile
        // hit the completed queue and was NACK-requeued, and the broker handed it straight back to
        // the same consumer: a requeue loop at network rate for the whole drain. Left un-ACKed
        // instead, it holds a prefetch credit until the close requeues it.
        var channel = new FakeDispatcherChannel();
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handlerRuns = 0;
        var dispatcher = RabbitMqMessageDispatcher.Create(
            async (_, _) =>
            {
                Interlocked.Increment(ref handlerRuns);
                started.TrySetResult();
                await gate.Task.ConfigureAwait(false);
            },
            new RabbitMqAsyncResponseOptions(),
            EnqueueSubscriber(workers: 1, capacity: 8),
            NullLogger.Instance,
            "worker.q",
            RabbitMqSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery("m1", deliveryTag: 1), channel, CancellationToken.None);
        await started.Task.WaitAsync(TimeSpan.FromSeconds(5));
        var disposing = dispatcher.DisposeAsync().AsTask(); // the drain waits behind the running m1

        try
        {
            for (ulong tag = 2; tag <= 4; tag++)
                await dispatcher.HandleAsync(Delivery("again", deliveryTag: tag, redelivered: true), channel, new CancellationToken(canceled: true));

            Assert.Empty(channel.Nacks);
            Assert.Equal([1UL], channel.Acks);
        }
        finally
        {
            gate.TrySetResult();
            await disposing.WaitAsync(TimeSpan.FromSeconds(10));
        }

        Assert.Equal(1, Volatile.Read(ref handlerRuns));
    }

    private static async Task WaitForPublishesAsync(FakeDispatcherChannel channel, int count)
    {
        var guard = TimeProvider.System.GetUtcNow() + TimeSpan.FromSeconds(10);
        while (true)
        {
            lock (channel.Publishes)
            {
                if (channel.Publishes.Count >= count)
                    return;
            }

            Assert.True(TimeProvider.System.GetUtcNow() < guard, "the expected publishes never happened");
            await Task.Delay(TimeSpan.FromMilliseconds(5));
        }
    }

    private static RabbitMqDelivery Delivery(
        string body,
        BasicProperties? properties = null,
        string exchange = "exchange",
        string routingKey = "route",
        ulong deliveryTag = 1,
        bool redelivered = false)
        => new(
            "consumer",
            deliveryTag,
            redelivered,
            exchange,
            routingKey,
            properties ?? new BasicProperties(),
            Encoding.UTF8.GetBytes(body),
            CancellationToken.None);

    private sealed class FakeDispatcherChannel : IRabbitMqChannel
    {
        public bool IsOpen { get; set; } = true;
        public List<ulong> Acks { get; } = [];
        public List<CancellationToken> AckTokens { get; } = [];
        public List<(ulong DeliveryTag, bool Requeue)> Nacks { get; } = [];
        public Exception? ThrowOnAck { get; init; }
        public Exception? ThrowOnNack { get; init; }

        public ValueTask BasicAckAsync(ulong deliveryTag, CancellationToken cancellationToken = default)
        {
            lock (AckTokens)
                AckTokens.Add(cancellationToken);

            // Mirrors the real adapter, which forwards the token verbatim to the SDK: a cancelled
            // token aborts the settle instead of acking.
            cancellationToken.ThrowIfCancellationRequested();

            if (ThrowOnAck is not null)
                throw ThrowOnAck;

            lock (Acks)
                Acks.Add(deliveryTag);
            return ValueTask.CompletedTask;
        }

        public ValueTask BasicNackAsync(ulong deliveryTag, bool requeue, CancellationToken cancellationToken = default)
        {
            if (ThrowOnNack is not null)
                throw ThrowOnNack;

            lock (Nacks)
                Nacks.Add((deliveryTag, requeue));
            return ValueTask.CompletedTask;
        }

        public Task ExchangeDeclareAsync(string exchange, string type, bool durable, bool autoDelete, CancellationToken cancellationToken = default)
            => Task.CompletedTask;

        public Task QueueDeclareAsync(string queue, bool durable, bool exclusive, bool autoDelete, IDictionary<string, object?>? arguments = null, CancellationToken cancellationToken = default)
            => Task.CompletedTask;

        public Task QueueBindAsync(string queue, string exchange, string routingKey, CancellationToken cancellationToken = default)
            => Task.CompletedTask;

        public Task BasicQosAsync(ushort prefetchCount, CancellationToken cancellationToken = default)
            => Task.CompletedTask;

        public List<(string Exchange, string RoutingKey, BasicProperties Properties, ReadOnlyMemory<byte> Body)> Publishes { get; } = [];

        public Exception? ThrowOnPublish { get; init; }

        public ValueTask BasicPublishAsync(string exchange, string routingKey, BasicProperties properties, ReadOnlyMemory<byte> body, CancellationToken cancellationToken = default)
        {
            if (ThrowOnPublish is not null)
                throw ThrowOnPublish;

            lock (Publishes)
                Publishes.Add((exchange, routingKey, properties, body));
            return ValueTask.CompletedTask;
        }

        public Task<RabbitMqConsumer> BasicConsumeAsync(string queue, Func<RabbitMqDelivery, Task> handler, CancellationToken cancellationToken = default)
            => Task.FromResult(new RabbitMqConsumer("consumer-tag", new TaskCompletionSource<string>().Task));

        public Task BasicCancelAsync(string consumerTag, CancellationToken cancellationToken = default)
            => Task.CompletedTask;

        public Task CloseAsync(CancellationToken cancellationToken = default)
            => Task.CompletedTask;

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class ListLogger : ILogger
    {
        public ConcurrentQueue<LogEntry> Entries { get; } = [];

        public IDisposable BeginScope<TState>(TState state)
            where TState : notnull
            => NullScope.Instance;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            Entries.Enqueue(new LogEntry(logLevel, formatter(state, exception), exception));
        }
    }

    private sealed record LogEntry(LogLevel Level, string Message, Exception? Exception);

    private sealed class NullScope : IDisposable
    {
        public static readonly NullScope Instance = new();

        public void Dispose()
        {
        }
    }
}
