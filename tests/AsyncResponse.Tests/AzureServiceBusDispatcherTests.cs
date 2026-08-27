using AsyncResponse.Transports.AzureServiceBus;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace AsyncResponse.Tests;

public sealed class AzureServiceBusDispatcherTests
{
    [Fact]
    public void ValidateOptions_AckAfterHandlerCompletes_DoesNotThrow()
    {
        AzureServiceBusMessageDispatcher.ValidateOptions(
            new AzureServiceBusAsyncResponseOptions(),
            new AzureServiceBusSubscriberOptions(),
            AzureServiceBusSubscriberRole.Worker);
    }

    [Fact]
    public void ValidateOptions_NegativeMaxDeliveryAttempts_Throws()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            AzureServiceBusMessageDispatcher.ValidateOptions(
                new AzureServiceBusAsyncResponseOptions(),
                new AzureServiceBusSubscriberOptions { MaxDeliveryAttempts = -1 },
                AzureServiceBusSubscriberRole.ResponseIngress));

        Assert.Contains(nameof(AzureServiceBusSubscriberOptions.MaxDeliveryAttempts), ex.Message, StringComparison.Ordinal);
        Assert.Contains(nameof(AzureServiceBusAsyncResponseOptions.ResponseSubscriber), ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateOptions_NegativePrefetchCount_Throws()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            AzureServiceBusMessageDispatcher.ValidateOptions(
                new AzureServiceBusAsyncResponseOptions(),
                new AzureServiceBusSubscriberOptions { PrefetchCount = -1 },
                AzureServiceBusSubscriberRole.Worker));

        Assert.Contains(nameof(AzureServiceBusSubscriberOptions.PrefetchCount), ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateOptions_AckAfterEnqueue_RequiresPositiveBackgroundWorkerCount()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            AzureServiceBusMessageDispatcher.ValidateOptions(
                new AzureServiceBusAsyncResponseOptions(),
                new AzureServiceBusSubscriberOptions { AckMode = AzureServiceBusAckMode.AckAfterEnqueue },
                AzureServiceBusSubscriberRole.Worker));

        Assert.Contains(nameof(AzureServiceBusSubscriberOptions.BackgroundWorkerCount), ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateOptions_AckAfterEnqueue_RequiresPositiveBackgroundQueueCapacity()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            AzureServiceBusMessageDispatcher.ValidateOptions(
                new AzureServiceBusAsyncResponseOptions(),
                new AzureServiceBusSubscriberOptions
                {
                    AckMode = AzureServiceBusAckMode.AckAfterEnqueue,
                    BackgroundWorkerCount = 2
                },
                AzureServiceBusSubscriberRole.Worker));

        Assert.Contains(nameof(AzureServiceBusSubscriberOptions.BackgroundQueueCapacity), ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateOptions_AckAfterEnqueue_RejectsDrainPlusShutdownExceedingHostBudget()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            AzureServiceBusMessageDispatcher.ValidateOptions(
                new AzureServiceBusAsyncResponseOptions
                {
                    ShutdownTimeout = TimeSpan.FromSeconds(20),
                    HostShutdownTimeout = TimeSpan.FromSeconds(25)
                },
                new AzureServiceBusSubscriberOptions().UseAckAfterEnqueue(1, 8, TimeSpan.FromSeconds(10)),
                AzureServiceBusSubscriberRole.Worker));

        Assert.Contains(nameof(AzureServiceBusAsyncResponseOptions.HostShutdownTimeout), ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateOptions_DocumentedEarlyAckDefaults_Pass()
    {
        // Regression: the documented two-arg early-ACK opt-in with stock defaults
        // (5s ShutdownTimeout + 20s BackgroundDrainTimeout vs HostShutdownTimeout 30s)
        // must not fail startup.
        AzureServiceBusMessageDispatcher.ValidateOptions(
            new AzureServiceBusAsyncResponseOptions(),
            new AzureServiceBusSubscriberOptions().UseAckAfterEnqueue(4, 256),
            AzureServiceBusSubscriberRole.Worker);
    }

    [Fact]
    public void ValidateOptions_AckAfterEnqueue_RequiresPositiveDrainTimeout()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            AzureServiceBusMessageDispatcher.ValidateOptions(
                new AzureServiceBusAsyncResponseOptions(),
                new AzureServiceBusSubscriberOptions
                {
                    AckMode = AzureServiceBusAckMode.AckAfterEnqueue,
                    BackgroundWorkerCount = 1,
                    BackgroundQueueCapacity = 8,
                    BackgroundDrainTimeout = TimeSpan.Zero
                },
                AzureServiceBusSubscriberRole.Worker));

        Assert.Contains(nameof(AzureServiceBusSubscriberOptions.BackgroundDrainTimeout), ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateOptions_AckAfterEnqueue_RequiresPositiveHostShutdownTimeoutWhenSet()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            AzureServiceBusMessageDispatcher.ValidateOptions(
                new AzureServiceBusAsyncResponseOptions { HostShutdownTimeout = TimeSpan.Zero },
                new AzureServiceBusSubscriberOptions().UseAckAfterEnqueue(1, 8),
                AzureServiceBusSubscriberRole.Worker));

        Assert.Contains(nameof(AzureServiceBusAsyncResponseOptions.HostShutdownTimeout), ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void UseAckAfterEnqueue_WhenDrainTimeoutIsOmitted_KeepsDefaultDrainTimeout()
    {
        var options = new AzureServiceBusSubscriberOptions();

        var returned = options.UseAckAfterEnqueue(2, 16);

        Assert.Same(options, returned);
        Assert.Equal(AzureServiceBusAckMode.AckAfterEnqueue, options.AckMode);
        Assert.Equal(2, options.BackgroundWorkerCount);
        Assert.Equal(16, options.BackgroundQueueCapacity);
        Assert.Equal(TimeSpan.FromSeconds(20), options.BackgroundDrainTimeout);
    }

    [Fact]
    public void UseAckAfterEnqueue_RejectsInvalidArguments()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new AzureServiceBusSubscriberOptions().UseAckAfterEnqueue(0, 1));
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new AzureServiceBusSubscriberOptions().UseAckAfterEnqueue(1, 0));
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new AzureServiceBusSubscriberOptions().UseAckAfterEnqueue(1, 1, TimeSpan.Zero));
    }

    [Fact]
    public void ValidateOptions_UnsupportedAckMode_Throws()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            AzureServiceBusMessageDispatcher.ValidateOptions(
                new AzureServiceBusAsyncResponseOptions(),
                new AzureServiceBusSubscriberOptions { AckMode = (AzureServiceBusAckMode)99 },
                AzureServiceBusSubscriberRole.Worker));

        Assert.Contains("unsupported value", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task AckAfterHandlerCompletes_CompletesOnlyAfterSuccessfulHandler()
    {
        var calls = new SettlementCalls();
        await using var dispatcher = AzureServiceBusMessageDispatcher.Create(
            (_, _) =>
            {
                calls.Handler++;
                Assert.Equal(0, calls.Complete);
                return Task.CompletedTask;
            },
            new AzureServiceBusAsyncResponseOptions(),
            new AzureServiceBusSubscriberOptions(),
            NullLogger.Instance,
            "workers",
            AzureServiceBusSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery(calls), CancellationToken.None);

        Assert.Equal(1, calls.Handler);
        Assert.Equal(1, calls.Complete);
        Assert.Equal(0, calls.Abandon);
        Assert.Equal(0, calls.DeadLetter);
    }

    [Fact]
    public async Task AckAfterHandlerCompletes_AbandonsBeforeMaxAttempts()
    {
        var calls = new SettlementCalls();
        await using var dispatcher = AzureServiceBusMessageDispatcher.Create(
            (_, _) => throw new InvalidOperationException("boom"),
            new AzureServiceBusAsyncResponseOptions(),
            new AzureServiceBusSubscriberOptions { MaxDeliveryAttempts = 2 },
            NullLogger.Instance,
            "workers",
            AzureServiceBusSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery(calls, deliveryCount: 1), CancellationToken.None);

        Assert.Equal(0, calls.Complete);
        Assert.Equal(1, calls.Abandon);
        Assert.Equal(0, calls.DeadLetter);
    }

    [Fact]
    public async Task AckAfterHandlerCompletes_DeadLettersAtMaxAttempts()
    {
        var calls = new SettlementCalls();
        await using var dispatcher = AzureServiceBusMessageDispatcher.Create(
            (_, _) => throw new InvalidOperationException("boom"),
            new AzureServiceBusAsyncResponseOptions(),
            new AzureServiceBusSubscriberOptions { MaxDeliveryAttempts = 2 },
            NullLogger.Instance,
            "workers",
            AzureServiceBusSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery(calls, deliveryCount: 2), CancellationToken.None);

        Assert.Equal(0, calls.Abandon);
        Assert.Equal(1, calls.DeadLetter);
        Assert.Equal("AsyncResponseHandlerFailed", calls.DeadLetterReason);
        Assert.Equal("boom", calls.DeadLetterDescription);
    }

    [Fact]
    public async Task AckAfterHandlerCompletes_TruncatesALongHandlerMessage_SoTheDeadLetterIsAccepted()
    {
        // Regression (round 29): Service Bus rejects a dead-letter description longer than 4096
        // characters with ArgumentOutOfRangeException, thrown client-side before any network call.
        // The surrounding catch could not tell that apart from a lost lock, so a handler whose
        // exception message ran long (a serializer dump, a wrapped SQL error, an HTTP body) could
        // never be dead-lettered at all: MaxDeliveryAttempts went silently inoperative and the
        // handler re-ran until the ENTITY's own MaxDeliveryCount.
        var calls = new SettlementCalls();
        var longMessage = new string('x', 10_000);
        await using var dispatcher = AzureServiceBusMessageDispatcher.Create(
            (_, _) => throw new InvalidOperationException(longMessage),
            new AzureServiceBusAsyncResponseOptions(),
            new AzureServiceBusSubscriberOptions { MaxDeliveryAttempts = 2 },
            NullLogger.Instance,
            "workers",
            AzureServiceBusSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery(calls, deliveryCount: 2), CancellationToken.None);

        Assert.Equal(1, calls.DeadLetter);
        Assert.Equal("AsyncResponseHandlerFailed", calls.DeadLetterReason);
        Assert.Equal(4096, calls.DeadLetterDescription!.Length);
        Assert.Equal(longMessage[..4096], calls.DeadLetterDescription);
    }

    [Fact]
    public async Task AckAfterHandlerCompletes_LeavesAShortHandlerMessageIntact()
    {
        // The truncation must not touch the common case: a message at or under the limit is passed
        // through byte for byte.
        var calls = new SettlementCalls();
        var exactLimit = new string('y', 4096);
        await using var dispatcher = AzureServiceBusMessageDispatcher.Create(
            (_, _) => throw new InvalidOperationException(exactLimit),
            new AzureServiceBusAsyncResponseOptions(),
            new AzureServiceBusSubscriberOptions { MaxDeliveryAttempts = 2 },
            NullLogger.Instance,
            "workers",
            AzureServiceBusSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery(calls, deliveryCount: 2), CancellationToken.None);

        Assert.Equal(exactLimit, calls.DeadLetterDescription);
    }

    [Fact]
    public async Task AckAfterHandlerCompletes_CompleteFailureAfterSuccessfulHandler_DoesNotDeadLetterOrAbandon()
    {
        // Regression (review fix): CompleteAsync used to sit inside the handler try, so a lost
        // peek-lock after a successful handler was misread as a handler failure — dead-lettering
        // succeeded work at max attempts, or abandoning it into an immediate duplicate below.
        // The settlement failure is now swallowed and logged; the lock lapse owns redelivery.
        var calls = new SettlementCalls { CompleteException = new InvalidOperationException("lock lost") };
        await using var dispatcher = AzureServiceBusMessageDispatcher.Create(
            (_, _) =>
            {
                calls.Handler++;
                return Task.CompletedTask;
            },
            new AzureServiceBusAsyncResponseOptions(),
            new AzureServiceBusSubscriberOptions { MaxDeliveryAttempts = 2 },
            NullLogger.Instance,
            "workers",
            AzureServiceBusSubscriberRole.Worker);

        // deliveryCount at max attempts: the old in-try Complete routed this to DeadLetterAsync.
        await dispatcher.HandleAsync(Delivery(calls, deliveryCount: 2), CancellationToken.None);

        Assert.Equal(1, calls.Handler);
        Assert.Equal(1, calls.Complete);
        Assert.Equal(0, calls.Abandon);
        Assert.Equal(0, calls.DeadLetter);
    }

    [Fact]
    public async Task AckAfterHandlerCompletes_DeadLetterFailureAfterFailedHandler_DoesNotEscape()
    {
        // Regression (r24): the failure-path settlements ran bare while the success-path Complete
        // was guarded — a slow handler that outlived its peek lock made DeadLetterAsync throw
        // MessageLockLost, and the escaping settlement tore down the whole receiver, dropping the
        // rest of the already-received batch un-settled. The failure-path settles are now
        // swallow-and-log; the lock lapse owns redelivery.
        var calls = new SettlementCalls { DeadLetterException = new InvalidOperationException("lock lost") };
        await using var dispatcher = AzureServiceBusMessageDispatcher.Create(
            (_, _) => throw new InvalidOperationException("boom"),
            new AzureServiceBusAsyncResponseOptions(),
            new AzureServiceBusSubscriberOptions { MaxDeliveryAttempts = 2 },
            NullLogger.Instance,
            "workers",
            AzureServiceBusSubscriberRole.Worker);

        // Must NOT throw: an escaping settle rebuilds the receiver mid-batch.
        await dispatcher.HandleAsync(Delivery(calls, deliveryCount: 2), CancellationToken.None);

        Assert.Equal(1, calls.DeadLetter);
        Assert.Equal(0, calls.Complete);
    }

    [Fact]
    public async Task AckAfterHandlerCompletes_AbandonFailureAfterFailedHandler_DoesNotEscape()
    {
        // Same regression as above, for the below-cap branch: a lock-lost Abandon must not escape.
        var calls = new SettlementCalls { AbandonException = new InvalidOperationException("lock lost") };
        await using var dispatcher = AzureServiceBusMessageDispatcher.Create(
            (_, _) => throw new InvalidOperationException("boom"),
            new AzureServiceBusAsyncResponseOptions(),
            new AzureServiceBusSubscriberOptions { MaxDeliveryAttempts = 5 },
            NullLogger.Instance,
            "workers",
            AzureServiceBusSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery(calls, deliveryCount: 1), CancellationToken.None);

        Assert.Equal(1, calls.Abandon);
        Assert.Equal(0, calls.DeadLetter);
    }

    [Fact]
    public void LockRenewalInterval_DefaultBeatsAzureDefaultLockDuration()
    {
        // Regression (r24): the default was 30 s — exactly Azure Service Bus's own default
        // LockDuration — and the renewal loop sleeps a FULL interval before its first renew, so
        // at defaults the heartbeat could never beat lock expiry: later batch messages were
        // redelivered to a competing consumer and handled twice. The default must stay
        // comfortably below the 30-second lock.
        var options = new AzureServiceBusSubscriberOptions();

        Assert.Equal(TimeSpan.FromSeconds(10), options.LockRenewalInterval);
        Assert.True(options.LockRenewalInterval < TimeSpan.FromSeconds(30));
    }

    [Fact]
    public async Task AckAfterHandlerCompletes_UnlimitedAttemptsAlwaysAbandons()
    {
        var calls = new SettlementCalls();
        await using var dispatcher = AzureServiceBusMessageDispatcher.Create(
            (_, _) => throw new InvalidOperationException("boom"),
            new AzureServiceBusAsyncResponseOptions(),
            new AzureServiceBusSubscriberOptions { MaxDeliveryAttempts = 0 },
            NullLogger.Instance,
            "responses",
            AzureServiceBusSubscriberRole.ResponseIngress);

        await dispatcher.HandleAsync(Delivery(calls, deliveryCount: 99), CancellationToken.None);

        Assert.Equal(1, calls.Abandon);
        Assert.Equal(0, calls.DeadLetter);
    }

    [Fact]
    public async Task AckAfterEnqueue_CompletesBeforeHandlerFinishes()
    {
        var calls = new SettlementCalls();
        var handlerStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseHandler = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handlerCompleted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var dispatcher = AzureServiceBusMessageDispatcher.Create(
            async (_, _) =>
            {
                calls.Handler++;
                handlerStarted.TrySetResult();
                await releaseHandler.Task.ConfigureAwait(false);
                handlerCompleted.TrySetResult();
            },
            new AzureServiceBusAsyncResponseOptions(),
            new AzureServiceBusSubscriberOptions().UseAckAfterEnqueue(1, 8, TimeSpan.FromSeconds(5)),
            NullLogger.Instance,
            "workers",
            AzureServiceBusSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery(calls), CancellationToken.None);

        Assert.Equal(1, calls.Complete);
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
        var failureReported = new TaskCompletionSource<AzureServiceBusBackgroundFailureContext>(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var dispatcher = AzureServiceBusMessageDispatcher.Create(
            (_, _) =>
            {
                handled.TrySetResult();
                throw new InvalidOperationException("background boom");
            },
            new AzureServiceBusAsyncResponseOptions { CorrelationIdProperty = "cid" },
            new AzureServiceBusSubscriberOptions
            {
                OnBackgroundFailure = context =>
                {
                    failureReported.TrySetResult(context);
                    return ValueTask.CompletedTask;
                }
            }.UseAckAfterEnqueue(1, 8, TimeSpan.FromSeconds(5)),
            NullLogger.Instance,
            "workers",
            AzureServiceBusSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery(calls, properties: new Dictionary<string, object?> { ["cid"] = "corr-background" }), CancellationToken.None);

        await handled.Task.WaitAsync(TimeSpan.FromSeconds(2));
        var callback = await failureReported.Task.WaitAsync(TimeSpan.FromSeconds(2));

        Assert.Equal(1, calls.Complete);
        Assert.Equal(0, calls.DeadLetter);
        Assert.Equal("workers", callback.Queue);
        Assert.Equal("Worker", callback.SubscriberRole);
        Assert.Equal("corr-background", callback.CorrelationId);
        Assert.IsType<InvalidOperationException>(callback.Exception);
    }

    [Fact]
    public async Task AckAfterEnqueue_BackgroundFailureCallbackExceptionsAreSwallowed()
    {
        var calls = new SettlementCalls();
        var handled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var dispatcher = AzureServiceBusMessageDispatcher.Create(
            (_, _) =>
            {
                handled.TrySetResult();
                throw new InvalidOperationException("background boom");
            },
            new AzureServiceBusAsyncResponseOptions(),
            new AzureServiceBusSubscriberOptions
            {
                OnBackgroundFailure = _ => throw new InvalidOperationException("callback boom")
            }.UseAckAfterEnqueue(1, 8, TimeSpan.FromSeconds(5)),
            NullLogger.Instance,
            "workers",
            AzureServiceBusSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery(calls), CancellationToken.None);

        await handled.Task.WaitAsync(TimeSpan.FromSeconds(2));
        await Task.Delay(50);
        Assert.Equal(1, calls.Complete);
    }

    [Fact]
    public async Task AckAfterEnqueue_WhenCompleteFails_DoesNotAbandonEnqueuedDelivery()
    {
        var calls = new SettlementCalls { CompleteException = new InvalidOperationException("complete failed") };
        var handled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var dispatcher = (QueuedAzureServiceBusMessageDispatcher)AzureServiceBusMessageDispatcher.Create(
            (_, _) =>
            {
                handled.TrySetResult();
                return Task.CompletedTask;
            },
            new AzureServiceBusAsyncResponseOptions(),
            new AzureServiceBusSubscriberOptions().UseAckAfterEnqueue(1, 8, TimeSpan.FromSeconds(5)),
            NullLogger.Instance,
            "workers",
            AzureServiceBusSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery(calls), CancellationToken.None);

        // The delivery was already handed to a background worker: abandoning it would race a duplicate
        // redelivery against the in-process execution, so the failed Complete is only logged.
        await handled.Task.WaitAsync(TimeSpan.FromSeconds(2));
        Assert.Equal(1, calls.Complete);
        Assert.Equal(0, calls.Abandon);

        // Draining proves the pending counter was not double-decremented for the enqueued delivery.
        await dispatcher.DisposeAsync();
        Assert.Equal(0, dispatcher.PendingCount);
    }

    [Fact]
    public async Task AckAfterEnqueue_Overflow_AbandonsForRetry()
    {
        var calls = new SettlementCalls();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var dispatcher = AzureServiceBusMessageDispatcher.Create(
            async (_, _) =>
            {
                entered.TrySetResult();
                await release.Task.ConfigureAwait(false);
            },
            new AzureServiceBusAsyncResponseOptions(),
            new AzureServiceBusSubscriberOptions().UseAckAfterEnqueue(1, 1, TimeSpan.FromSeconds(5)),
            NullLogger.Instance,
            "workers",
            AzureServiceBusSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery(calls), CancellationToken.None);
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(2));
        await dispatcher.HandleAsync(Delivery(calls), CancellationToken.None);
        await dispatcher.HandleAsync(Delivery(calls), CancellationToken.None);

        Assert.Equal(1, calls.Abandon);
        release.TrySetResult();
    }

    [Fact]
    public async Task AckAfterEnqueue_DisposeTimesOutAndSecondDisposeIsNoOp()
    {
        var calls = new SettlementCalls();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var dispatcher = AzureServiceBusMessageDispatcher.Create(
            async (_, _) =>
            {
                entered.TrySetResult();
                await release.Task.ConfigureAwait(false);
            },
            new AzureServiceBusAsyncResponseOptions(),
            new AzureServiceBusSubscriberOptions().UseAckAfterEnqueue(1, 8, TimeSpan.FromMilliseconds(10)),
            NullLogger.Instance,
            "workers",
            AzureServiceBusSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery(calls), CancellationToken.None);
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(2));
        await dispatcher.DisposeAsync();
        await dispatcher.DisposeAsync();

        Assert.Equal(1, calls.Complete);
        release.TrySetResult();
        await Task.Delay(50);
    }

    [Fact]
    public async Task ShutdownCancellation_AtMaxAttempts_LeavesMessageUnsettled()
    {
        // Regression (r23): a graceful drain cancelling the stoppingToken while user code was in
        // the handler used to land in the generic failure catch — dead-lettering healthy work at
        // the delivery-count cap, or abandoning it below (burning a delivery count on work that
        // never ran). Shutdown now rethrows and leaves the peek lock to lapse on its own.
        var calls = new SettlementCalls();
        using var stopping = new CancellationTokenSource();
        stopping.Cancel();
        await using var dispatcher = AzureServiceBusMessageDispatcher.Create(
            (_, _) => throw new OperationCanceledException(stopping.Token),
            new AzureServiceBusAsyncResponseOptions(),
            new AzureServiceBusSubscriberOptions { MaxDeliveryAttempts = 2 },
            NullLogger.Instance,
            "workers",
            AzureServiceBusSubscriberRole.Worker);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => dispatcher.HandleAsync(Delivery(calls, deliveryCount: 2), stopping.Token));

        Assert.Equal(0, calls.Complete);
        Assert.Equal(0, calls.Abandon);
        Assert.Equal(0, calls.DeadLetter);
    }

    private static AzureServiceBusTransportDelivery Delivery(
        SettlementCalls calls,
        string queue = "workers",
        string body = "{}",
        string messageId = "message-id",
        string? correlationId = null,
        long sequenceNumber = 42,
        int deliveryCount = 1,
        IReadOnlyDictionary<string, object?>? properties = null)
        => new(
            queue,
            body,
            messageId,
            correlationId,
            sequenceNumber,
            deliveryCount,
            properties ?? new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase),
            () =>
            {
                calls.Complete++;
                if (calls.CompleteException is not null)
                    throw calls.CompleteException;

                return ValueTask.CompletedTask;
            },
            () =>
            {
                calls.Abandon++;
                if (calls.AbandonException is not null)
                    throw calls.AbandonException;

                return ValueTask.CompletedTask;
            },
            (reason, description) =>
            {
                calls.DeadLetter++;
                calls.DeadLetterReason = reason;
                calls.DeadLetterDescription = description;
                if (calls.DeadLetterException is not null)
                    throw calls.DeadLetterException;

                return ValueTask.CompletedTask;
            },
            _ =>
            {
                calls.RenewLock++;
                return ValueTask.CompletedTask;
            });

    private sealed class SettlementCalls
    {
        public int Handler;
        public int Complete;
        public Exception? CompleteException;
        public int Abandon;
        public Exception? AbandonException;
        public int DeadLetter;
        public Exception? DeadLetterException;
        public int RenewLock;
        public string? DeadLetterReason;
        public string? DeadLetterDescription;
    }
}
