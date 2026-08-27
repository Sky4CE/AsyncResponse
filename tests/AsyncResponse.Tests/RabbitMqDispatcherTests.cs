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
    public async Task Awaiting_CapAboveTwo_UsesTheOperatorsFullCap_OnceXDeathMakesAttemptsCountable()
    {
        // The counterpart: with a dead-letter cycle configured the broker DOES count attempts, so
        // the operator's cap applies in full and attempt 2 of 5 still requeues.
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

        Assert.True(Assert.Single(channel.Nacks).Requeue);
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

    private static RabbitMqSubscriberOptions EnqueueSubscriber(
        int workers = 1,
        int capacity = 8,
        TimeSpan? drain = null)
        => new RabbitMqSubscriberOptions()
            .UseAckAfterEnqueue(workers, capacity, drain ?? TimeSpan.FromSeconds(5));

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

        public ValueTask BasicPublishAsync(string exchange, string routingKey, BasicProperties properties, ReadOnlyMemory<byte> body, CancellationToken cancellationToken = default)
            => ValueTask.CompletedTask;

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
