using AsyncResponse.Transports.RabbitMQ;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using RabbitMQ.Client;
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
    public async Task Queued_WhenBackgroundQueueIsFull_NacksWithRequeue()
    {
        var channel = new FakeDispatcherChannel();
        var firstStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var calls = 0;
        await using var dispatcher = RabbitMqMessageDispatcher.Create(
            async (_, _) =>
            {
                if (Interlocked.Increment(ref calls) == 1)
                {
                    firstStarted.TrySetResult();
                    await releaseFirst.Task.ConfigureAwait(false);
                }
            },
            new RabbitMqAsyncResponseOptions(),
            EnqueueSubscriber(workers: 1, capacity: 1),
            NullLogger.Instance,
            "worker.q",
            RabbitMqSubscriberRole.Worker);

        // First delivery is dequeued by the single worker and blocks; it gets ACKed.
        await dispatcher.HandleAsync(Delivery("m1", deliveryTag: 1), channel, CancellationToken.None);
        await firstStarted.Task.WaitAsync(TimeSpan.FromSeconds(2));

        // Second fills the bounded queue (capacity 1) -> ACK.
        await dispatcher.HandleAsync(Delivery("m2", deliveryTag: 2), channel, CancellationToken.None);
        // Third cannot be written -> NACK/requeue.
        await dispatcher.HandleAsync(Delivery("m3", deliveryTag: 3), channel, CancellationToken.None);

        Assert.Equal(new[] { 1UL, 2UL }, channel.Acks);
        var nack = Assert.Single(channel.Nacks);
        Assert.Equal(3UL, nack.DeliveryTag);
        Assert.True(nack.Requeue);

        releaseFirst.TrySetResult();
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
        public List<ulong> Acks { get; } = [];
        public List<(ulong DeliveryTag, bool Requeue)> Nacks { get; } = [];
        public Exception? ThrowOnAck { get; init; }

        public ValueTask BasicAckAsync(ulong deliveryTag, CancellationToken cancellationToken = default)
        {
            if (ThrowOnAck is not null)
                throw ThrowOnAck;

            lock (Acks)
                Acks.Add(deliveryTag);
            return ValueTask.CompletedTask;
        }

        public ValueTask BasicNackAsync(ulong deliveryTag, bool requeue, CancellationToken cancellationToken = default)
        {
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

        public Task<string> BasicConsumeAsync(string queue, Func<RabbitMqDelivery, Task> handler, CancellationToken cancellationToken = default)
            => Task.FromResult("consumer-tag");

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
