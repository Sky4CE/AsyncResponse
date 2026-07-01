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
    public void ValidateOptions_AckAfterReceive_RequiresPositiveBackgroundWorkerCount()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            AzureServiceBusMessageDispatcher.ValidateOptions(
                new AzureServiceBusAsyncResponseOptions(),
                new AzureServiceBusSubscriberOptions { AckMode = AzureServiceBusAckMode.AckAfterReceive },
                AzureServiceBusSubscriberRole.Worker));

        Assert.Contains(nameof(AzureServiceBusSubscriberOptions.BackgroundWorkerCount), ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateOptions_AckAfterReceive_RequiresPositiveBackgroundQueueCapacity()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            AzureServiceBusMessageDispatcher.ValidateOptions(
                new AzureServiceBusAsyncResponseOptions(),
                new AzureServiceBusSubscriberOptions
                {
                    AckMode = AzureServiceBusAckMode.AckAfterReceive,
                    BackgroundWorkerCount = 2
                },
                AzureServiceBusSubscriberRole.Worker));

        Assert.Contains(nameof(AzureServiceBusSubscriberOptions.BackgroundQueueCapacity), ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateOptions_AckAfterReceive_RejectsDrainPlusShutdownExceedingHostBudget()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            AzureServiceBusMessageDispatcher.ValidateOptions(
                new AzureServiceBusAsyncResponseOptions
                {
                    ShutdownTimeout = TimeSpan.FromSeconds(20),
                    HostShutdownTimeout = TimeSpan.FromSeconds(25)
                },
                new AzureServiceBusSubscriberOptions().UseAckAfterReceive(1, 8, TimeSpan.FromSeconds(10)),
                AzureServiceBusSubscriberRole.Worker));

        Assert.Contains(nameof(AzureServiceBusAsyncResponseOptions.HostShutdownTimeout), ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateOptions_AckAfterReceive_RequiresPositiveDrainTimeout()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            AzureServiceBusMessageDispatcher.ValidateOptions(
                new AzureServiceBusAsyncResponseOptions(),
                new AzureServiceBusSubscriberOptions
                {
                    AckMode = AzureServiceBusAckMode.AckAfterReceive,
                    BackgroundWorkerCount = 1,
                    BackgroundQueueCapacity = 8,
                    BackgroundDrainTimeout = TimeSpan.Zero
                },
                AzureServiceBusSubscriberRole.Worker));

        Assert.Contains(nameof(AzureServiceBusSubscriberOptions.BackgroundDrainTimeout), ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateOptions_AckAfterReceive_RequiresPositiveHostShutdownTimeoutWhenSet()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            AzureServiceBusMessageDispatcher.ValidateOptions(
                new AzureServiceBusAsyncResponseOptions { HostShutdownTimeout = TimeSpan.Zero },
                new AzureServiceBusSubscriberOptions().UseAckAfterReceive(1, 8),
                AzureServiceBusSubscriberRole.Worker));

        Assert.Contains(nameof(AzureServiceBusAsyncResponseOptions.HostShutdownTimeout), ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void UseAckAfterReceive_WhenDrainTimeoutIsOmitted_KeepsDefaultDrainTimeout()
    {
        var options = new AzureServiceBusSubscriberOptions();

        var returned = options.UseAckAfterReceive(2, 16);

        Assert.Same(options, returned);
        Assert.Equal(AzureServiceBusAckMode.AckAfterReceive, options.AckMode);
        Assert.Equal(2, options.BackgroundWorkerCount);
        Assert.Equal(16, options.BackgroundQueueCapacity);
        Assert.Equal(TimeSpan.FromSeconds(30), options.BackgroundDrainTimeout);
    }

    [Fact]
    public void UseAckAfterReceive_RejectsInvalidArguments()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new AzureServiceBusSubscriberOptions().UseAckAfterReceive(0, 1));
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new AzureServiceBusSubscriberOptions().UseAckAfterReceive(1, 0));
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new AzureServiceBusSubscriberOptions().UseAckAfterReceive(1, 1, TimeSpan.Zero));
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
    public async Task AckAfterReceive_CompletesBeforeHandlerFinishes()
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
            new AzureServiceBusSubscriberOptions().UseAckAfterReceive(1, 8, TimeSpan.FromSeconds(5)),
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
    public async Task AckAfterReceive_BackgroundFailure_InvokesCallback()
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
            }.UseAckAfterReceive(1, 8, TimeSpan.FromSeconds(5)),
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
    public async Task AckAfterReceive_BackgroundFailureCallbackExceptionsAreSwallowed()
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
            }.UseAckAfterReceive(1, 8, TimeSpan.FromSeconds(5)),
            NullLogger.Instance,
            "workers",
            AzureServiceBusSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery(calls), CancellationToken.None);

        await handled.Task.WaitAsync(TimeSpan.FromSeconds(2));
        await Task.Delay(50);
        Assert.Equal(1, calls.Complete);
    }

    [Fact]
    public async Task AckAfterReceive_WhenCompleteFails_AbandonsForRetry()
    {
        var calls = new SettlementCalls { CompleteException = new InvalidOperationException("complete failed") };
        await using var dispatcher = AzureServiceBusMessageDispatcher.Create(
            (_, _) => Task.CompletedTask,
            new AzureServiceBusAsyncResponseOptions(),
            new AzureServiceBusSubscriberOptions().UseAckAfterReceive(1, 8, TimeSpan.FromSeconds(5)),
            NullLogger.Instance,
            "workers",
            AzureServiceBusSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery(calls), CancellationToken.None);

        Assert.Equal(1, calls.Complete);
        Assert.Equal(1, calls.Abandon);
    }

    [Fact]
    public async Task AckAfterReceive_Overflow_AbandonsForRetry()
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
            new AzureServiceBusSubscriberOptions().UseAckAfterReceive(1, 1, TimeSpan.FromSeconds(5)),
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
    public async Task AckAfterReceive_DisposeTimesOutAndSecondDisposeIsNoOp()
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
            new AzureServiceBusSubscriberOptions().UseAckAfterReceive(1, 8, TimeSpan.FromMilliseconds(10)),
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
                return ValueTask.CompletedTask;
            },
            (reason, description) =>
            {
                calls.DeadLetter++;
                calls.DeadLetterReason = reason;
                calls.DeadLetterDescription = description;
                return ValueTask.CompletedTask;
            });

    private sealed class SettlementCalls
    {
        public int Handler;
        public int Complete;
        public Exception? CompleteException;
        public int Abandon;
        public int DeadLetter;
        public string? DeadLetterReason;
        public string? DeadLetterDescription;
    }
}
