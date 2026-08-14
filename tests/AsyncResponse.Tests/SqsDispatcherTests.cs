using AsyncResponse.Transports.SQS;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace AsyncResponse.Tests;

public sealed class SqsDispatcherTests
{
    [Fact]
    public void ValidateOptions_AckAfterHandlerCompletes_DoesNotThrow()
    {
        SqsMessageDispatcher.ValidateOptions(
            new SqsAsyncResponseOptions(),
            new SqsSubscriberOptions(),
            SqsSubscriberRole.Worker);
    }

    [Fact]
    public void ValidateOptions_InvalidVisibilityTimeout_Throws()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqsMessageDispatcher.ValidateOptions(
                new SqsAsyncResponseOptions(),
                new SqsSubscriberOptions { VisibilityTimeout = TimeSpan.Zero },
                SqsSubscriberRole.ResponseIngress));
        Assert.Contains(nameof(SqsSubscriberOptions.VisibilityTimeout), ex.Message, StringComparison.Ordinal);
        Assert.Contains(nameof(SqsAsyncResponseOptions.ResponseSubscriber), ex.Message, StringComparison.Ordinal);

        Assert.Throws<InvalidOperationException>(() =>
            SqsMessageDispatcher.ValidateOptions(
                new SqsAsyncResponseOptions(),
                new SqsSubscriberOptions { VisibilityTimeout = TimeSpan.FromHours(13) },
                SqsSubscriberRole.Worker));
    }

    [Fact]
    public void ValidateOptions_InvalidRedeliveryDelay_Throws()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqsMessageDispatcher.ValidateOptions(
                new SqsAsyncResponseOptions(),
                new SqsSubscriberOptions { RedeliveryDelay = TimeSpan.FromSeconds(-1) },
                SqsSubscriberRole.Worker));
        Assert.Contains(nameof(SqsSubscriberOptions.RedeliveryDelay), ex.Message, StringComparison.Ordinal);

        Assert.Throws<InvalidOperationException>(() =>
            SqsMessageDispatcher.ValidateOptions(
                new SqsAsyncResponseOptions(),
                new SqsSubscriberOptions { RedeliveryDelay = TimeSpan.FromHours(13) },
                SqsSubscriberRole.Worker));

        // Zero is valid: it releases the message for immediate redelivery.
        SqsMessageDispatcher.ValidateOptions(
            new SqsAsyncResponseOptions(),
            new SqsSubscriberOptions { RedeliveryDelay = TimeSpan.Zero },
            SqsSubscriberRole.Worker);
    }

    [Fact]
    public void ValidateOptions_AckAfterEnqueue_RequiresPositiveBackgroundWorkerCount()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqsMessageDispatcher.ValidateOptions(
                new SqsAsyncResponseOptions(),
                new SqsSubscriberOptions { AckMode = SqsAckMode.AckAfterEnqueue },
                SqsSubscriberRole.Worker));

        Assert.Contains(nameof(SqsSubscriberOptions.BackgroundWorkerCount), ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateOptions_AckAfterEnqueue_RequiresPositiveBackgroundQueueCapacity()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqsMessageDispatcher.ValidateOptions(
                new SqsAsyncResponseOptions(),
                new SqsSubscriberOptions
                {
                    AckMode = SqsAckMode.AckAfterEnqueue,
                    BackgroundWorkerCount = 2
                },
                SqsSubscriberRole.Worker));

        Assert.Contains(nameof(SqsSubscriberOptions.BackgroundQueueCapacity), ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateOptions_AckAfterEnqueue_RequiresPositiveDrainTimeout()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqsMessageDispatcher.ValidateOptions(
                new SqsAsyncResponseOptions(),
                new SqsSubscriberOptions
                {
                    AckMode = SqsAckMode.AckAfterEnqueue,
                    BackgroundWorkerCount = 1,
                    BackgroundQueueCapacity = 8,
                    BackgroundDrainTimeout = TimeSpan.Zero
                },
                SqsSubscriberRole.Worker));

        Assert.Contains(nameof(SqsSubscriberOptions.BackgroundDrainTimeout), ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateOptions_AckAfterEnqueue_RejectsDrainExceedingHostBudget()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqsMessageDispatcher.ValidateOptions(
                new SqsAsyncResponseOptions
                {
                    HostShutdownTimeout = TimeSpan.FromSeconds(25)
                },
                new SqsSubscriberOptions().UseAckAfterEnqueue(1, 8, TimeSpan.FromSeconds(26)),
                SqsSubscriberRole.Worker));

        Assert.Contains(nameof(SqsAsyncResponseOptions.HostShutdownTimeout), ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateOptions_DocumentedEarlyAckDefaults_Pass()
    {
        // Regression: the documented two-arg early-ACK opt-in with stock defaults
        // (BackgroundDrainTimeout 20s vs HostShutdownTimeout 30s) must not fail startup.
        SqsMessageDispatcher.ValidateOptions(
            new SqsAsyncResponseOptions(),
            new SqsSubscriberOptions().UseAckAfterEnqueue(4, 256),
            SqsSubscriberRole.Worker);
    }

    [Fact]
    public void ValidateOptions_AckAfterEnqueue_RequiresPositiveHostShutdownTimeoutWhenSet()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqsMessageDispatcher.ValidateOptions(
                new SqsAsyncResponseOptions { HostShutdownTimeout = TimeSpan.Zero },
                new SqsSubscriberOptions().UseAckAfterEnqueue(1, 8),
                SqsSubscriberRole.Worker));

        Assert.Contains(nameof(SqsAsyncResponseOptions.HostShutdownTimeout), ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void UseAckAfterEnqueue_WhenDrainTimeoutIsOmitted_KeepsDefaultDrainTimeout()
    {
        var options = new SqsSubscriberOptions();

        var returned = options.UseAckAfterEnqueue(2, 16);

        Assert.Same(options, returned);
        Assert.Equal(SqsAckMode.AckAfterEnqueue, options.AckMode);
        Assert.Equal(2, options.BackgroundWorkerCount);
        Assert.Equal(16, options.BackgroundQueueCapacity);
        Assert.Equal(TimeSpan.FromSeconds(20), options.BackgroundDrainTimeout);
    }

    [Fact]
    public void UseAckAfterEnqueue_RejectsInvalidArguments()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new SqsSubscriberOptions().UseAckAfterEnqueue(0, 1));
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new SqsSubscriberOptions().UseAckAfterEnqueue(1, 0));
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new SqsSubscriberOptions().UseAckAfterEnqueue(1, 1, TimeSpan.Zero));
    }

    [Fact]
    public void ValidateOptions_UnsupportedAckMode_Throws()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqsMessageDispatcher.ValidateOptions(
                new SqsAsyncResponseOptions(),
                new SqsSubscriberOptions { AckMode = (SqsAckMode)99 },
                SqsSubscriberRole.Worker));

        Assert.Contains("unsupported value", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task AckAfterHandlerCompletes_DeletesOnlyAfterSuccessfulHandler()
    {
        var calls = new SettlementCalls();
        await using var dispatcher = SqsMessageDispatcher.Create(
            (_, _) =>
            {
                calls.Handler++;
                Assert.Equal(0, calls.Delete);
                return Task.CompletedTask;
            },
            new SqsAsyncResponseOptions(),
            new SqsSubscriberOptions(),
            NullLogger.Instance,
            "workers",
            SqsSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery(calls), CancellationToken.None);

        Assert.Equal(1, calls.Handler);
        Assert.Equal(1, calls.Delete);
        Assert.Empty(calls.VisibilityChanges);
    }

    [Fact]
    public async Task AckAfterHandlerCompletes_FailureLeavesMessageForVisibilityTimeout()
    {
        var calls = new SettlementCalls();
        await using var dispatcher = SqsMessageDispatcher.Create(
            (_, _) => throw new InvalidOperationException("boom"),
            new SqsAsyncResponseOptions(),
            new SqsSubscriberOptions(),
            NullLogger.Instance,
            "workers",
            SqsSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery(calls, receiveCount: 3), CancellationToken.None);

        // No delete and no visibility change: SQS redelivery + the queue's redrive policy own retry.
        Assert.Equal(0, calls.Delete);
        Assert.Empty(calls.VisibilityChanges);
    }

    [Fact]
    public async Task AckAfterHandlerCompletes_FailureAppliesRedeliveryDelay()
    {
        var calls = new SettlementCalls();
        await using var dispatcher = SqsMessageDispatcher.Create(
            (_, _) => throw new InvalidOperationException("boom"),
            new SqsAsyncResponseOptions(),
            new SqsSubscriberOptions { RedeliveryDelay = TimeSpan.FromSeconds(7) },
            NullLogger.Instance,
            "workers",
            SqsSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery(calls), CancellationToken.None);

        Assert.Equal(0, calls.Delete);
        Assert.Equal(TimeSpan.FromSeconds(7), Assert.Single(calls.VisibilityChanges));
    }

    [Fact]
    public async Task AckAfterHandlerCompletes_DeleteFailureAfterSuccessfulHandler_DoesNotShortenVisibility()
    {
        // Regression (review fix): DeleteAsync used to sit inside the handler try, so a delete
        // failure after a successful handler took the failure branch — shortening visibility to
        // hasten a duplicate of already-completed work and burning receives toward the redrive
        // policy. The delete failure is now swallowed and logged; the visibility timeout owns
        // redelivery on its own schedule.
        var calls = new SettlementCalls { DeleteException = new InvalidOperationException("receipt expired") };
        await using var dispatcher = SqsMessageDispatcher.Create(
            (_, _) =>
            {
                calls.Handler++;
                return Task.CompletedTask;
            },
            new SqsAsyncResponseOptions(),
            new SqsSubscriberOptions { RedeliveryDelay = TimeSpan.FromSeconds(7) },
            NullLogger.Instance,
            "workers",
            SqsSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery(calls), CancellationToken.None);

        Assert.Equal(1, calls.Handler);
        Assert.Equal(1, calls.Delete);
        Assert.Empty(calls.VisibilityChanges);
    }

    [Fact]
    public async Task AckAfterHandlerCompletes_RedeliveryDelayVisibilityFailure_IsSwallowed()
    {
        var calls = new SettlementCalls { ChangeVisibilityException = new InvalidOperationException("receipt expired") };
        await using var dispatcher = SqsMessageDispatcher.Create(
            (_, _) => throw new InvalidOperationException("boom"),
            new SqsAsyncResponseOptions(),
            new SqsSubscriberOptions { RedeliveryDelay = TimeSpan.FromSeconds(1) },
            NullLogger.Instance,
            "workers",
            SqsSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery(calls), CancellationToken.None);

        Assert.Equal(0, calls.Delete);
    }

    [Fact]
    public async Task AckAfterEnqueue_DeletesBeforeHandlerFinishes()
    {
        var calls = new SettlementCalls();
        var handlerStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseHandler = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handlerCompleted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var dispatcher = SqsMessageDispatcher.Create(
            async (_, _) =>
            {
                calls.Handler++;
                handlerStarted.TrySetResult();
                await releaseHandler.Task.ConfigureAwait(false);
                handlerCompleted.TrySetResult();
            },
            new SqsAsyncResponseOptions(),
            new SqsSubscriberOptions().UseAckAfterEnqueue(1, 8, TimeSpan.FromSeconds(5)),
            NullLogger.Instance,
            "workers",
            SqsSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery(calls), CancellationToken.None);

        Assert.Equal(1, calls.Delete);
        await handlerStarted.Task.WaitAsync(TimeSpan.FromSeconds(2));
        Assert.False(handlerCompleted.Task.IsCompleted);

        releaseHandler.TrySetResult();
        await handlerCompleted.Task.WaitAsync(TimeSpan.FromSeconds(2));
    }

    [Fact]
    public async Task AckAfterEnqueue_BackgroundFailure_InvokesCallback()
    {
        var calls = new SettlementCalls();
        var handled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var failureReported = new TaskCompletionSource<SqsBackgroundFailureContext>(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var dispatcher = SqsMessageDispatcher.Create(
            (_, _) =>
            {
                handled.TrySetResult();
                throw new InvalidOperationException("background boom");
            },
            new SqsAsyncResponseOptions { CorrelationIdAttribute = "cid" },
            new SqsSubscriberOptions
            {
                OnBackgroundFailure = context =>
                {
                    failureReported.TrySetResult(context);
                    return ValueTask.CompletedTask;
                }
            }.UseAckAfterEnqueue(1, 8, TimeSpan.FromSeconds(5)),
            NullLogger.Instance,
            "workers",
            SqsSubscriberRole.Worker);

        await dispatcher.HandleAsync(
            Delivery(calls, receiveCount: 4, attributes: new Dictionary<string, string> { ["cid"] = "corr-background" }),
            CancellationToken.None);

        await handled.Task.WaitAsync(TimeSpan.FromSeconds(2));
        var callback = await failureReported.Task.WaitAsync(TimeSpan.FromSeconds(2));

        Assert.Equal(1, calls.Delete);
        Assert.Equal("workers", callback.Queue);
        Assert.Equal("Worker", callback.SubscriberRole);
        Assert.Equal("message-id", callback.MessageId);
        Assert.Equal(4, callback.ReceiveCount);
        Assert.Equal("corr-background", callback.CorrelationId);
        Assert.IsType<InvalidOperationException>(callback.Exception);
    }

    [Fact]
    public async Task AckAfterEnqueue_BackgroundFailureCallbackExceptionsAreSwallowed()
    {
        var calls = new SettlementCalls();
        var handled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var dispatcher = SqsMessageDispatcher.Create(
            (_, _) =>
            {
                handled.TrySetResult();
                throw new InvalidOperationException("background boom");
            },
            new SqsAsyncResponseOptions(),
            new SqsSubscriberOptions
            {
                OnBackgroundFailure = _ => throw new InvalidOperationException("callback boom")
            }.UseAckAfterEnqueue(1, 8, TimeSpan.FromSeconds(5)),
            NullLogger.Instance,
            "workers",
            SqsSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery(calls), CancellationToken.None);

        await handled.Task.WaitAsync(TimeSpan.FromSeconds(2));
        await Task.Delay(50);
        Assert.Equal(1, calls.Delete);
    }

    [Fact]
    public async Task AckAfterEnqueue_WhenDeleteFails_DoesNotReleaseEnqueuedDelivery()
    {
        var calls = new SettlementCalls { DeleteException = new InvalidOperationException("delete failed") };
        var handled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var dispatcher = (QueuedSqsMessageDispatcher)SqsMessageDispatcher.Create(
            (_, _) =>
            {
                handled.TrySetResult();
                return Task.CompletedTask;
            },
            new SqsAsyncResponseOptions(),
            new SqsSubscriberOptions().UseAckAfterEnqueue(1, 8, TimeSpan.FromSeconds(5)),
            NullLogger.Instance,
            "workers",
            SqsSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery(calls), CancellationToken.None);

        // The delivery was already handed to a background worker: releasing its visibility would race
        // a duplicate redelivery against the in-process execution, so the failed delete is only logged.
        await handled.Task.WaitAsync(TimeSpan.FromSeconds(2));
        Assert.Equal(1, calls.Delete);
        Assert.Empty(calls.VisibilityChanges);

        // Draining proves the pending counter was not double-decremented for the enqueued delivery.
        await dispatcher.DisposeAsync();
        Assert.Equal(0, dispatcher.PendingCount);
    }

    [Fact]
    public async Task AckAfterEnqueue_Overflow_LetsVisibilityTimeoutLapseInsteadOfZeroingIt()
    {
        var calls = new SettlementCalls();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var dispatcher = SqsMessageDispatcher.Create(
            async (_, _) =>
            {
                entered.TrySetResult();
                await release.Task.ConfigureAwait(false);
            },
            new SqsAsyncResponseOptions(),
            new SqsSubscriberOptions().UseAckAfterEnqueue(1, 1, TimeSpan.FromSeconds(5)),
            NullLogger.Instance,
            "workers",
            SqsSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery(calls), CancellationToken.None);
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(2));
        await dispatcher.HandleAsync(Delivery(calls), CancellationToken.None);
        await dispatcher.HandleAsync(Delivery(calls), CancellationToken.None);

        // The overflowing message is neither deleted nor made instantly re-receivable: a zero
        // visibility would burn a receive against the queue's redrive policy on every retry while
        // the queue stays full. It redelivers when its visibility timeout lapses.
        Assert.Empty(calls.VisibilityChanges);
        Assert.Equal(2, calls.Delete);
        release.TrySetResult();
    }

    [Fact]
    public async Task AckAfterEnqueue_OverflowWithRedeliveryDelay_ShortensVisibilityToThatDelay()
    {
        var calls = new SettlementCalls();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var dispatcher = SqsMessageDispatcher.Create(
            async (_, _) =>
            {
                entered.TrySetResult();
                await release.Task.ConfigureAwait(false);
            },
            new SqsAsyncResponseOptions(),
            new SqsSubscriberOptions { RedeliveryDelay = TimeSpan.FromSeconds(9) }
                .UseAckAfterEnqueue(1, 1, TimeSpan.FromSeconds(5)),
            NullLogger.Instance,
            "workers",
            SqsSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery(calls), CancellationToken.None);
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(2));
        await dispatcher.HandleAsync(Delivery(calls), CancellationToken.None);
        await dispatcher.HandleAsync(Delivery(calls), CancellationToken.None);

        Assert.Equal(TimeSpan.FromSeconds(9), Assert.Single(calls.VisibilityChanges));
        Assert.Equal(2, calls.Delete);
        release.TrySetResult();
    }

    [Fact]
    public async Task AckAfterEnqueue_CapacitySignals_TrackSaturationAndRelease()
    {
        var calls = new SettlementCalls();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var dispatcher = SqsMessageDispatcher.Create(
            async (_, _) =>
            {
                entered.TrySetResult();
                await release.Task.ConfigureAwait(false);
            },
            new SqsAsyncResponseOptions(),
            new SqsSubscriberOptions().UseAckAfterEnqueue(1, 1, TimeSpan.FromSeconds(5)),
            NullLogger.Instance,
            "workers",
            SqsSubscriberRole.Worker);

        Assert.True(dispatcher.CanAcceptMore);
        Assert.Equal(1, dispatcher.FreeCapacity);

        // First delivery goes straight to the (blocked) worker; the second fills the queue.
        await dispatcher.HandleAsync(Delivery(calls), CancellationToken.None);
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(2));
        await dispatcher.HandleAsync(Delivery(calls), CancellationToken.None);

        Assert.False(dispatcher.CanAcceptMore);
        Assert.Equal(0, dispatcher.FreeCapacity);

        var waitTask = dispatcher.WaitForCapacityAsync(CancellationToken.None).AsTask();
        Assert.False(waitTask.IsCompleted);

        release.TrySetResult();
        await waitTask.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.True(dispatcher.CanAcceptMore);
    }

    [Fact]
    public async Task AckAfterHandlerCompletes_CapacitySignals_AlwaysOpen()
    {
        await using var dispatcher = SqsMessageDispatcher.Create(
            (_, _) => Task.CompletedTask,
            new SqsAsyncResponseOptions(),
            new SqsSubscriberOptions(),
            NullLogger.Instance,
            "workers",
            SqsSubscriberRole.Worker);

        Assert.True(dispatcher.CanAcceptMore);
        Assert.Equal(int.MaxValue, dispatcher.FreeCapacity);
        await dispatcher.WaitForCapacityAsync(CancellationToken.None);
    }

    [Fact]
    public void ValidateOptions_VisibilityRenewalInterval_RequiresVisibilityTimeoutAndShorterInterval()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqsMessageDispatcher.ValidateOptions(
                new SqsAsyncResponseOptions(),
                new SqsSubscriberOptions { VisibilityRenewalInterval = TimeSpan.Zero },
                SqsSubscriberRole.Worker));
        Assert.Contains(nameof(SqsSubscriberOptions.VisibilityRenewalInterval), ex.Message, StringComparison.Ordinal);

        ex = Assert.Throws<InvalidOperationException>(() =>
            SqsMessageDispatcher.ValidateOptions(
                new SqsAsyncResponseOptions(),
                new SqsSubscriberOptions { VisibilityRenewalInterval = TimeSpan.FromSeconds(10) },
                SqsSubscriberRole.Worker));
        Assert.Contains(nameof(SqsSubscriberOptions.VisibilityTimeout), ex.Message, StringComparison.Ordinal);

        ex = Assert.Throws<InvalidOperationException>(() =>
            SqsMessageDispatcher.ValidateOptions(
                new SqsAsyncResponseOptions(),
                new SqsSubscriberOptions
                {
                    VisibilityTimeout = TimeSpan.FromSeconds(10),
                    VisibilityRenewalInterval = TimeSpan.FromSeconds(10)
                },
                SqsSubscriberRole.Worker));
        Assert.Contains("shorter", ex.Message, StringComparison.OrdinalIgnoreCase);

        // A renewal interval below the visibility timeout is valid.
        SqsMessageDispatcher.ValidateOptions(
            new SqsAsyncResponseOptions(),
            new SqsSubscriberOptions
            {
                VisibilityTimeout = TimeSpan.FromSeconds(30),
                VisibilityRenewalInterval = TimeSpan.FromSeconds(10)
            },
            SqsSubscriberRole.Worker);
    }

    [Fact]
    public async Task AckAfterEnqueue_DisposeTimesOutAndSecondDisposeIsNoOp()
    {
        var calls = new SettlementCalls();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var dispatcher = SqsMessageDispatcher.Create(
            async (_, _) =>
            {
                entered.TrySetResult();
                await release.Task.ConfigureAwait(false);
            },
            new SqsAsyncResponseOptions(),
            new SqsSubscriberOptions().UseAckAfterEnqueue(1, 8, TimeSpan.FromMilliseconds(10)),
            NullLogger.Instance,
            "workers",
            SqsSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery(calls), CancellationToken.None);
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(2));
        await dispatcher.DisposeAsync();
        await dispatcher.DisposeAsync();

        Assert.Equal(1, calls.Delete);
        release.TrySetResult();
        await Task.Delay(50);
    }

    [Fact]
    public async Task ShutdownCancellation_LeavesVisibilityUntouched()
    {
        // Regression (r23): a graceful drain cancelling the stoppingToken while user code was in
        // the handler used to land in the generic failure catch — shortening visibility to hasten
        // a redelivery of work that never ran, as if the handler had failed. Shutdown now rethrows
        // and leaves the message's visibility timeout to lapse on its own.
        var calls = new SettlementCalls();
        using var stopping = new CancellationTokenSource();
        stopping.Cancel();
        await using var dispatcher = SqsMessageDispatcher.Create(
            (_, _) => throw new OperationCanceledException(stopping.Token),
            new SqsAsyncResponseOptions(),
            new SqsSubscriberOptions { RedeliveryDelay = TimeSpan.FromSeconds(5) },
            NullLogger.Instance,
            "workers",
            SqsSubscriberRole.Worker);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => dispatcher.HandleAsync(Delivery(calls), stopping.Token));

        Assert.Equal(0, calls.Delete);
        Assert.Empty(calls.VisibilityChanges);
    }

    private static SqsTransportDelivery Delivery(
        SettlementCalls calls,
        string queueUrl = "https://sqs.us-east-1.amazonaws.com/000000000000/workers",
        string body = "{}",
        string messageId = "message-id",
        int receiveCount = 1,
        IReadOnlyDictionary<string, string>? attributes = null)
        => new(
            queueUrl,
            body,
            messageId,
            "receipt-handle",
            receiveCount,
            attributes ?? new Dictionary<string, string>(StringComparer.Ordinal),
            () =>
            {
                calls.Delete++;
                if (calls.DeleteException is not null)
                    throw calls.DeleteException;

                return ValueTask.CompletedTask;
            },
            delay =>
            {
                calls.VisibilityChanges.Add(delay);
                if (calls.ChangeVisibilityException is not null)
                    throw calls.ChangeVisibilityException;

                return ValueTask.CompletedTask;
            });

    private sealed class SettlementCalls
    {
        public int Handler;
        public int Delete;
        public Exception? DeleteException;
        public Exception? ChangeVisibilityException;
        public List<TimeSpan> VisibilityChanges { get; } = [];
    }
}
