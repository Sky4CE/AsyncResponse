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

    /// <summary>
    /// Round 33 (B5): <c>AckAfterHandlerCompletes</c> returned without any shutdown-budget check,
    /// although its stop path spends <c>ShutdownTimeout</c> TWICE, sequentially — the lock-renewal
    /// join on the final batch, then the receiver close — whenever <c>LockRenewalInterval</c> is set
    /// (default 10s). A 20s ShutdownTimeout therefore needs 40s and overran a 30s host budget,
    /// force-terminating mid-close and redelivering handled-but-uncompleted messages whose locks then
    /// lapsed. Pre-fix: the configuration started.
    /// </summary>
    [Fact]
    public void ValidateOptions_AckAfterHandlerCompletes_WithLockRenewal_RejectsTwoShutdownTimeoutsExceedingHostBudget()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            AzureServiceBusMessageDispatcher.ValidateOptions(
                new AzureServiceBusAsyncResponseOptions
                {
                    ShutdownTimeout = TimeSpan.FromSeconds(20),
                    HostShutdownTimeout = TimeSpan.FromSeconds(30)
                },
                new AzureServiceBusSubscriberOptions { AckMode = AzureServiceBusAckMode.AckAfterHandlerCompletes },
                AzureServiceBusSubscriberRole.Worker));

        Assert.Contains(nameof(AzureServiceBusAsyncResponseOptions.HostShutdownTimeout), ex.Message, StringComparison.Ordinal);
        Assert.Contains(nameof(AzureServiceBusAsyncResponseOptions.ShutdownTimeout), ex.Message, StringComparison.Ordinal);
        Assert.Contains("00:00:40", ex.Message, StringComparison.Ordinal); // 20s renewal join + 20s receiver close
    }

    /// <summary>
    /// Control for the two-term budget: 15s × 2 = 30s fits a 30s host budget exactly (the
    /// comparison is inclusive).
    /// </summary>
    [Fact]
    public void ValidateOptions_AckAfterHandlerCompletes_WithLockRenewal_TwoShutdownTimeoutsExactlyAtTheHostBudget_Pass()
    {
        AzureServiceBusMessageDispatcher.ValidateOptions(
            new AzureServiceBusAsyncResponseOptions
            {
                ShutdownTimeout = TimeSpan.FromSeconds(15),
                HostShutdownTimeout = TimeSpan.FromSeconds(30)
            },
            new AzureServiceBusSubscriberOptions { AckMode = AzureServiceBusAckMode.AckAfterHandlerCompletes },
            AzureServiceBusSubscriberRole.Worker);
    }

    /// <summary>
    /// With renewal off there is no renewal task to join: only the receiver close spends
    /// <c>ShutdownTimeout</c>, so 20s fits the 30s budget that 2 × 20s could not.
    /// </summary>
    [Fact]
    public void ValidateOptions_AckAfterHandlerCompletes_WithoutLockRenewal_CountsShutdownTimeoutOnce()
    {
        AzureServiceBusMessageDispatcher.ValidateOptions(
            new AzureServiceBusAsyncResponseOptions
            {
                ShutdownTimeout = TimeSpan.FromSeconds(20),
                HostShutdownTimeout = TimeSpan.FromSeconds(30)
            },
            new AzureServiceBusSubscriberOptions
            {
                AckMode = AzureServiceBusAckMode.AckAfterHandlerCompletes,
                LockRenewalInterval = null
            },
            AzureServiceBusSubscriberRole.Worker);
    }

    /// <summary>
    /// Round 33 (B5), renewal off: the single receiver-close term is still validated — a 40s
    /// ShutdownTimeout cannot fit a 30s host budget. Pre-fix: the configuration started.
    /// </summary>
    [Fact]
    public void ValidateOptions_AckAfterHandlerCompletes_WithoutLockRenewal_RejectsShutdownTimeoutExceedingHostBudget()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            AzureServiceBusMessageDispatcher.ValidateOptions(
                new AzureServiceBusAsyncResponseOptions
                {
                    ShutdownTimeout = TimeSpan.FromSeconds(40),
                    HostShutdownTimeout = TimeSpan.FromSeconds(30)
                },
                new AzureServiceBusSubscriberOptions
                {
                    AckMode = AzureServiceBusAckMode.AckAfterHandlerCompletes,
                    LockRenewalInterval = null
                },
                AzureServiceBusSubscriberRole.Worker));

        Assert.Contains(nameof(AzureServiceBusAsyncResponseOptions.HostShutdownTimeout), ex.Message, StringComparison.Ordinal);
        Assert.Contains(nameof(AzureServiceBusAsyncResponseOptions.ShutdownTimeout), ex.Message, StringComparison.Ordinal);
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
    public async Task AckAfterEnqueue_AfterTheDrainBudgetLapses_DoesNotStartFreshWork_AndSurfacesIt()
    {
        // Regression (round 31): the drain token cannot stop the REAL handler — it is
        // _ingress.HandleWorkerMessageAsync(payload), whose target takes no CancellationToken — so
        // the loop kept dequeuing and EXECUTING past the budget, and whatever was still queued at
        // process exit vanished with no record: those messages were completed at enqueue, so the
        // broker never redelivers them. The settled lock rules out a DLQ write;
        // OnBackgroundFailure is the record (DB/Redis/Pub-Sub parity).
        var first = new SettlementCalls();
        var second = new SettlementCalls();
        var firstStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handlerRuns = 0;
        var failures = new List<AzureServiceBusBackgroundFailureContext>();
        var dispatcher = AzureServiceBusMessageDispatcher.Create(
            async (_, _) =>
            {
                // Deliberately ignores the token, exactly like the ingress handler in production.
                if (Interlocked.Increment(ref handlerRuns) == 1)
                {
                    firstStarted.TrySetResult();
                    await releaseFirst.Task.ConfigureAwait(false);
                }
            },
            new AzureServiceBusAsyncResponseOptions(),
            new AzureServiceBusSubscriberOptions
            {
                OnBackgroundFailure = context =>
                {
                    lock (failures)
                    {
                        failures.Add(context);
                    }

                    return ValueTask.CompletedTask;
                }
            }.UseAckAfterEnqueue(1, 8, TimeSpan.FromMilliseconds(100)),
            NullLogger.Instance,
            "workers",
            AzureServiceBusSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery(first), CancellationToken.None);
        await firstStarted.Task.WaitAsync(TimeSpan.FromSeconds(2));
        await dispatcher.HandleAsync(Delivery(second), CancellationToken.None); // completed, waiting in queue

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

            Assert.True(TimeProvider.System.GetUtcNow() < guard, "the undrained message was never surfaced");
            await Task.Delay(TimeSpan.FromMilliseconds(5));
        }

        lock (failures)
        {
            Assert.IsAssignableFrom<OperationCanceledException>(Assert.Single(failures).Exception);
        }

        // The queued message was NOT executed after the budget lapsed.
        Assert.Equal(1, Volatile.Read(ref handlerRuns));
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
    public async Task AckAfterHandlerCompletes_NeverCutsALongHandlerMessageInsideASurrogatePair()
    {
        // Review 2026-09-21 F8: the cut was description[..4096]. An exception message is arbitrary
        // text; with a non-BMP character straddling the limit the slice kept the high surrogate
        // and dropped its low half, and the AMQP encoder then replaces the orphan with U+FFFD —
        // the forensic text was corrupted exactly where it was cut. The cut now steps back one
        // unit so the pair stays whole (and out).
        var calls = new SettlementCalls();
        var longMessage = new string('x', 4095) + "\U0001F600" + new string('y', 100);
        await using var dispatcher = AzureServiceBusMessageDispatcher.Create(
            (_, _) => throw new InvalidOperationException(longMessage),
            new AzureServiceBusAsyncResponseOptions(),
            new AzureServiceBusSubscriberOptions { MaxDeliveryAttempts = 2 },
            NullLogger.Instance,
            "workers",
            AzureServiceBusSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery(calls, deliveryCount: 2), CancellationToken.None);

        Assert.Equal(1, calls.DeadLetter);
        Assert.Equal(-1, PortableText.IndexOfIllFormedUtf16(calls.DeadLetterDescription!));
        Assert.Equal(longMessage[..4095], calls.DeadLetterDescription);
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
        // Regression (r24): the default was 30 s — a LockDuration entities are commonly configured
        // with (Azure's own default is 60 s) — and the renewal loop sleeps a FULL interval before
        // its first renew, so against such a lock the heartbeat could never beat lock expiry: later
        // batch messages were redelivered to a competing consumer and handled twice. The default
        // must stay comfortably below a 30-second lock.
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
    public async Task AckAfterEnqueue_FlowHandBack_WarnsAndSurfacesIt_WithoutAnErrorLog()
    {
        // Fixpoint r1 pre-commit (H6): an early-ACK job the flow engine hands back at host stop
        // cannot be redelivered (it was completed at enqueue) and a completed message cannot be
        // dead-lettered, so it is surfaced through OnBackgroundFailure — at Warning, not as a
        // handler failure at Error.
        var calls = new SettlementCalls();
        var failure = new TaskCompletionSource<AzureServiceBusBackgroundFailureContext>(TaskCreationOptions.RunContinuationsAsynchronously);
        var logger = new CapturingLogger<AzureServiceBusDispatcherTests>();
        await using var dispatcher = AzureServiceBusMessageDispatcher.Create(
            (_, _) => throw new DurableFlowInterruptedException("the host is stopping"),
            new AzureServiceBusAsyncResponseOptions(),
            new AzureServiceBusSubscriberOptions
            {
                OnBackgroundFailure = context =>
                {
                    failure.TrySetResult(context);
                    return ValueTask.CompletedTask;
                }
            }.UseAckAfterEnqueue(1, 8, TimeSpan.FromSeconds(5)),
            logger,
            "workers",
            AzureServiceBusSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery(calls), CancellationToken.None);

        var surfaced = await failure.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.IsType<DurableFlowInterruptedException>(surfaced.Exception);
        Assert.Equal(1, calls.Complete);
        Assert.Contains(logger.Entries, entry => entry.Level == Microsoft.Extensions.Logging.LogLevel.Warning
            && entry.Message.Contains("handed back by the flow engine", StringComparison.Ordinal));
        Assert.DoesNotContain(logger.Entries, entry => entry.Level >= Microsoft.Extensions.Logging.LogLevel.Error);
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

    [Theory]
    [InlineData(1)] // below the cap: the failure path abandoned it, and the live receiver pulled it straight back
    [InlineData(5)] // at the cap: the failure path dead-lettered the flow's only wake-up
    public async Task HostStopInterruption_WhileTheSubscriberTokenIsLive_LeavesTheMessageUnsettled(int deliveryCount)
    {
        // Red-on-old (fixpoint r1, GS5#1): the flow engine throws DurableFlowInterruptedException
        // on ApplicationStopping, which fires BEFORE the hosted subscriber's own token. Keyed on
        // that token alone, the dispatcher treated the interruption as a handler failure — an
        // Error log, then Abandon (re-received at once by the still-running loop) until the
        // delivery-count cap dead-lettered the wake-up and stranded the run.
        var calls = new SettlementCalls();
        var logger = new CollectingLogger();
        await using var dispatcher = AzureServiceBusMessageDispatcher.Create(
            (_, _) => throw new DurableFlowInterruptedException("the host is stopping"),
            new AzureServiceBusAsyncResponseOptions(),
            new AzureServiceBusSubscriberOptions { MaxDeliveryAttempts = 5 },
            logger,
            "workers",
            AzureServiceBusSubscriberRole.Worker);

        // Returns normally: the receive loop is live and must not enter the supervisor's failure path.
        await dispatcher.HandleAsync(Delivery(calls, deliveryCount: deliveryCount), CancellationToken.None);

        Assert.Equal(0, calls.Complete);
        Assert.Equal(0, calls.Abandon);
        Assert.Equal(0, calls.DeadLetter);
        Assert.DoesNotContain(logger.Messages, message => message.Contains("handling failed", StringComparison.Ordinal));
    }

    [Fact]
    public async Task AckAfterEnqueue_DrainLapse_SurfacesEveryStillQueuedEntryBeforeDisposeReturns()
    {
        // Red-on-old (fixpoint r1, GS5#4): past the drain budget only a worker that freed up
        // surfaced a queued entry — and with every worker still inside a handler that ignores the
        // token, none did before DisposeAsync returned and the process exited, so already-completed
        // work vanished with no OnBackgroundFailure call. The dispose now surfaces the rest itself
        // within the last quarter of the budget and logs the loss with its count.
        var failures = new List<string>();
        var logger = new CollectingLogger();
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var dispatcher = AzureServiceBusMessageDispatcher.Create(
            async (_, _) =>
            {
                started.TrySetResult();
                await release.Task.ConfigureAwait(false);
            },
            new AzureServiceBusAsyncResponseOptions(),
            new AzureServiceBusSubscriberOptions
            {
                OnBackgroundFailure = context =>
                {
                    lock (failures)
                        failures.Add(context.MessageId);
                    return ValueTask.CompletedTask;
                }
            }.UseAckAfterEnqueue(1, 8, TimeSpan.FromMilliseconds(400)),
            logger,
            "workers",
            AzureServiceBusSubscriberRole.Worker);

        try
        {
            await dispatcher.HandleAsync(Delivery(new SettlementCalls(), messageId: "m1"), CancellationToken.None);
            await started.Task.WaitAsync(TimeSpan.FromSeconds(2));
            await dispatcher.HandleAsync(Delivery(new SettlementCalls(), messageId: "m2"), CancellationToken.None);
            await dispatcher.HandleAsync(Delivery(new SettlementCalls(), messageId: "m3"), CancellationToken.None);

            await dispatcher.DisposeAsync(); // the only worker is still blocked in m1's handler

            lock (failures)
                Assert.Equal(["m2", "m3"], failures);
            Assert.Contains(logger.Messages, message => message.Contains("lapsed with 2 already-completed message(s)", StringComparison.Ordinal));
        }
        finally
        {
            release.TrySetResult();
        }
    }

    [Fact]
    public async Task QueuedDispose_SurvivesAWorkerFaultingOutsideItsHandlerGuard()
    {
        // Regression: the drain join caught only TimeoutException (the shared DB base and NATS
        // also carry a general arm). A worker faulting outside its handler guard — here the log
        // sink throwing from the "handler failed" entry inside the catch arm — rethrew from
        // Task.WhenAll, escaped DisposeAsync into the subscriber's `await using` and leaked the
        // drain token source.
        var logger = new ErrorThrowingLogger();
        var calls = new SettlementCalls();
        var dispatcher = AzureServiceBusMessageDispatcher.Create(
            (_, _) => throw new InvalidOperationException("handler boom"),
            new AzureServiceBusAsyncResponseOptions(),
            new AzureServiceBusSubscriberOptions().UseAckAfterEnqueue(1, 8),
            logger,
            "workers",
            AzureServiceBusSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery(calls), CancellationToken.None);
        await logger.ErrorThrown.Task.WaitAsync(TimeSpan.FromSeconds(5));

        await dispatcher.DisposeAsync();
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
