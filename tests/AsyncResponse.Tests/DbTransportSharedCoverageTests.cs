using AsyncResponse.Testing;
using AsyncResponse.Transports.MongoDB;
using AsyncResponse.Transports.PostgreSQL;
using AsyncResponse.Transports.SqlServer;
using Microsoft.Extensions.Logging.Abstractions;
using System.Diagnostics;
using System.Reflection;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Failure paths of <c>src/Transports/Shared/DbTransportShared.cs</c>, which is
/// <c>&lt;Compile Include&gt;</c>-linked into the MongoDB, PostgreSQL and SQL Server transport
/// packages. The dispatcher compiles separately into each, so every fact runs against all three.
/// </summary>
public sealed class DbTransportSharedCoverageTests
{
    /// <summary>
    /// A lease renewal that throws is a transient store blip, not a lost fence: it is logged and the
    /// heartbeat keeps trying rather than abandoning the in-flight handler.
    /// </summary>
    [Theory]
    [InlineData(Provider.SqlServer)]
    [InlineData(Provider.PostgreSql)]
    [InlineData(Provider.MongoDb)]
    public async Task LeaseRenewal_ThatThrows_IsLoggedAndRetried(Provider provider)
    {
        var calls = new Calls { RenewThrows = true };
        var logger = new CollectingLogger();

        // Hold the handler until the renewal has faulted twice — proof the loop kept beating
        // instead of dying on the first exception.
        await RunAsync(
            provider,
            logger,
            lockTimeout: TimeSpan.FromMilliseconds(100),
            calls: calls,
            handler: async () =>
            {
                while (Volatile.Read(ref calls.Renew) < 2)
                    await Task.Delay(10);
            });

        Assert.True(calls.Renew >= 2);
        Assert.Equal(1, calls.Ack);
        Assert.Contains(logger.Messages, message => message.StartsWith("Failed to renew the lease of", StringComparison.Ordinal));
    }

    /// <summary>
    /// Regression: the beat ran at LockTimeout/2 and a FAILED beat simply waited for the next one —
    /// which therefore landed at claim + LockTimeout, after <c>locked_until</c>, every time. ONE
    /// transient renew failure (a command timeout, a broken pooled connection, a SQL Server 1205
    /// deadlock victim) guaranteed the lease lapsed; a peer claimed the row within its
    /// EmptyPollDelay and a healthy long handler ran twice concurrently. The beat now runs at
    /// LockTimeout/3 and a failed one is retried on a short backoff, so the renewal lands with most
    /// of the lease still in hand. Virtual clock: nothing here waits on real time.
    /// </summary>
    [Theory]
    [InlineData(Provider.SqlServer)]
    [InlineData(Provider.PostgreSql)]
    [InlineData(Provider.MongoDb)]
    public async Task LeaseRenewal_OneFailedBeat_IsRetriedAndLandsBeforeTheLeaseLapses(Provider provider)
    {
        var lockTimeout = TimeSpan.FromSeconds(30);
        var clock = new VirtualTimeProvider();
        var calls = new Calls { RenewFailuresRemaining = 1 };
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var (dispatcher, handle) = CreateInlineDispatcher(provider, calls, () => release.Task, lockTimeout, clock, new CollectingLogger());

        await using (dispatcher)
        {
            var handling = handle(CancellationToken.None);
            try
            {
                // Walk to one second short of the locked_until the claim stamped. Nothing has
                // extended the lease yet, so whatever renewal is going to save it must have
                // SUCCEEDED by now.
                await WalkAsync(clock, lockTimeout - TimeSpan.FromSeconds(1));

                Assert.True(
                    Volatile.Read(ref calls.RenewSucceeded) >= 1,
                    $"one failed beat and the lease was never renewed before it lapsed ({Volatile.Read(ref calls.Renew)} renew call(s), none successful)");
            }
            finally
            {
                release.TrySetResult();
                await handling.WaitAsync(TimeSpan.FromSeconds(30));
            }
        }

        Assert.Equal(1, calls.Ack);
    }

    /// <summary>
    /// The retry is "until it succeeds or the fence is lost": a store that keeps failing is retried
    /// on the backoff that starts short — several attempts inside one lease — and the loop stops for
    /// good once the renew reports that the <c>lock_id</c> no longer matches.
    /// </summary>
    [Theory]
    [InlineData(Provider.SqlServer)]
    [InlineData(Provider.PostgreSql)]
    [InlineData(Provider.MongoDb)]
    public async Task LeaseRenewal_KeepsRetryingOnTheShortBackoff_UntilTheFenceIsLost(Provider provider)
    {
        var lockTimeout = TimeSpan.FromSeconds(30);
        var clock = new VirtualTimeProvider();
        var calls = new Calls { RenewFailuresRemaining = 4, RenewResult = false };
        var logger = new CollectingLogger();
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var (dispatcher, handle) = CreateInlineDispatcher(provider, calls, () => release.Task, lockTimeout, clock, logger);

        await using (dispatcher)
        {
            var handling = handle(CancellationToken.None);
            try
            {
                // Beat at 10 s, then four retries on the backoff (half-jittered 1, 2, 4 and 8 s
                // steps: the fourth lands by 25 s), then the fence-lost answer: five renew calls,
                // all inside the one lease (at the LockTimeout/2 cadence there were two).
                await WalkAsync(clock, lockTimeout - TimeSpan.FromSeconds(1), until: () => Volatile.Read(ref calls.Renew) >= 5);
                Assert.Equal(5, Volatile.Read(ref calls.Renew));
                await Eventually(() => logger.Messages.Any(message => message.Contains("was lost", StringComparison.Ordinal)));

                // Fence lost: the loop is over, so nothing is armed on the clock any more and a
                // further lease's worth of time renews nothing.
                Assert.Null(clock.NextTimerDueAt);
                clock.Advance(lockTimeout);
                Assert.Equal(5, Volatile.Read(ref calls.Renew));
            }
            finally
            {
                release.TrySetResult();
                await handling.WaitAsync(TimeSpan.FromSeconds(30));
            }
        }
    }

    /// <summary>
    /// Regression (fixpoint round 2, S1#6): a failed renew was retried at a FIXED second (or
    /// LockTimeout/10) for as long as the store kept failing — past <c>locked_until</c> too — so a
    /// database outage had every in-flight delivery renewing once a second: about twentyfold the
    /// healthy renewal load, exactly while the store was down. The retries now back off
    /// (half-jittered, doubling up to the beat interval). Virtual clock: three leases' worth of a
    /// store that never answers.
    /// </summary>
    [Theory]
    [InlineData(Provider.SqlServer)]
    [InlineData(Provider.PostgreSql)]
    [InlineData(Provider.MongoDb)]
    public async Task LeaseRenewal_ARunOfFailures_BacksOffInsteadOfRetryingEverySecond(Provider provider)
    {
        var lockTimeout = TimeSpan.FromSeconds(30);
        var clock = new VirtualTimeProvider();
        var calls = new Calls { RenewThrows = true };
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var (dispatcher, handle) = CreateInlineDispatcher(provider, calls, () => release.Task, lockTimeout, clock, new CollectingLogger());

        await using (dispatcher)
        {
            var handling = handle(CancellationToken.None);
            try
            {
                // 90 s: the fixed cadence renews ~80 times (every second from the first beat on);
                // the backoff at most ~20 (1, 2, 4, 8 s, one just before the lease ends, then every
                // 5-10 s).
                await WalkAsync(clock, TimeSpan.FromSeconds(90));
                var renews = Volatile.Read(ref calls.Renew);
                Assert.True(renews <= 25, $"{renews} renew attempts in 90 s of failures — the retry is not backing off");
                Assert.True(renews >= 6, $"only {renews} renew attempts in 90 s of failures — the retry stopped");
            }
            finally
            {
                release.TrySetResult();
                await handling.WaitAsync(TimeSpan.FromSeconds(30));
            }
        }
    }

    /// <summary>
    /// The backoff never skips the end of the lease: while the lease is in hand, one retry always
    /// lands a <c>retryInterval</c> short of it, so an outage that clears anywhere inside the lease
    /// still renews it in time — the doubling alone would jump from ~25 s straight past the 30 s
    /// <c>locked_until</c> and let a peer re-claim the row under a healthy handler.
    /// </summary>
    [Theory]
    [InlineData(Provider.SqlServer)]
    [InlineData(Provider.PostgreSql)]
    [InlineData(Provider.MongoDb)]
    public async Task LeaseRenewal_AnOutageThatClearsJustBeforeTheLeaseEnds_StillRenewsInsideIt(Provider provider)
    {
        var lockTimeout = TimeSpan.FromSeconds(30);
        var clock = new VirtualTimeProvider();
        var start = clock.GetTimestamp();
        var calls = new Calls { RenewFailsWhile = () => clock.GetElapsedTime(start) < TimeSpan.FromSeconds(28.5) };
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var (dispatcher, handle) = CreateInlineDispatcher(provider, calls, () => release.Task, lockTimeout, clock, new CollectingLogger());

        await using (dispatcher)
        {
            var handling = handle(CancellationToken.None);
            try
            {
                await WalkAsync(clock, lockTimeout - TimeSpan.FromSeconds(0.5), until: () => Volatile.Read(ref calls.RenewSucceeded) >= 1);
                Assert.True(
                    Volatile.Read(ref calls.RenewSucceeded) >= 1,
                    $"the outage cleared at 28.5 s but no renew landed before the 30 s lease ended ({Volatile.Read(ref calls.Renew)} attempts)");
            }
            finally
            {
                release.TrySetResult();
                await handling.WaitAsync(TimeSpan.FromSeconds(30));
            }
        }

        Assert.Equal(1, calls.Ack);
    }

    /// <summary>
    /// Regression: the flow engine signals "host is stopping, hand this delivery back" with
    /// <see cref="DurableFlowInterruptedException"/>, and it arrives BEFORE the subscriber's own
    /// stopping token is cancelled (ApplicationStopping fires ahead of every hosted service's
    /// StopAsync, and the worker subscriber is stopped last). The dispatcher's filter tested only its
    /// own token, so the hand-back was treated as a handler failure: a "failed on attempt N" warning
    /// and a NAK — and at the attempt cap, the flow's wake-up dead-lettered with a "Host is stopping"
    /// reason. It now leaves the claim unsettled, exactly like its own shutdown, and returns — and
    /// the receive span is no longer marked as an error on the way (a routine shutdown put a failed
    /// span on the trace of every delivery it handed back).
    /// </summary>
    [Theory]
    [InlineData(Provider.SqlServer)]
    [InlineData(Provider.PostgreSql)]
    [InlineData(Provider.MongoDb)]
    public async Task HostStopHandBack_WhileTheSubscriberTokenIsLive_LeavesTheClaimUnsettled(Provider provider)
    {
        foreach (var cap in new[] { 0, 1 })
        {
            var calls = new Calls();
            var logger = new CollectingLogger();
            using var activities = new AsyncResponseActivityCollector();

            await RunAsync(
                provider,
                logger,
                lockTimeout: TimeSpan.FromSeconds(30),
                calls: calls,
                handler: static () => Task.FromException(new DurableFlowInterruptedException("Host is stopping; the flow will resume after the restart.")),
                maxDeliveryAttempts: cap);

            Assert.Equal(0, calls.Nak);
            Assert.Equal(0, calls.DeadLetter);
            Assert.Equal(0, calls.Ack);
            Assert.DoesNotContain(logger.Messages, message => message.Contains("failed on attempt", StringComparison.Ordinal));
            Assert.DoesNotContain(logger.Messages, message => message.Contains("dead-lettering", StringComparison.Ordinal));
            var receive = Assert.Single(activities.All(), activity => activity.OperationName.EndsWith(".receive", StringComparison.Ordinal));
            Assert.NotEqual(ActivityStatusCode.Error, receive.Status);
        }

        // Contrast: a real handler failure still marks the same span as an error.
        using (var failing = new AsyncResponseActivityCollector())
        {
            await RunAsync(
                provider,
                new CollectingLogger(),
                lockTimeout: TimeSpan.FromSeconds(30),
                calls: new Calls(),
                handler: static () => Task.FromException(new InvalidOperationException("handler blew up")));

            var receive = Assert.Single(failing.All(), activity => activity.OperationName.EndsWith(".receive", StringComparison.Ordinal));
            Assert.Equal(ActivityStatusCode.Error, receive.Status);
        }
    }

    /// <summary>
    /// Regression: a claim parked on a full early-ACK background queue kept waiting after its
    /// heartbeat reported the lease LOST (a renew fenced on <c>lock_id</c> alone answers "no match"
    /// only once a peer re-claimed or finished the row) — and once capacity freed it enqueued and ran
    /// that row anyway: a job a peer already owned, executed a second time, with this ack's fence
    /// failing silently. The park now drops the delivery the moment the lease is lost: no enqueue, no
    /// ack, no NAK (the fence is gone).
    /// </summary>
    [Theory]
    [InlineData(Provider.SqlServer)]
    [InlineData(Provider.PostgreSql)]
    [InlineData(Provider.MongoDb)]
    public async Task EarlyAckPark_DropsTheDelivery_WhenItsLeaseIsLost(Provider provider)
    {
        // Only the parked claim ever renews (early-ACK deliveries have no inline heartbeat), and
        // its store answers "fence gone". Explicit gates and a virtual clock: the worker is proven
        // to hold the first delivery before the second is handed over (otherwise the second finds
        // the one-slot queue still full, parks, and is dropped too), and the park's beat runs only
        // when the clock is walked.
        var calls = new Calls { RenewResult = false };
        var clock = new VirtualTimeProvider();
        var runs = 0;
        var firstRunning = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var (dispatcher, handle) = CreateEarlyAckDispatcher(
            provider,
            calls,
            handler: async _ =>
            {
                Interlocked.Increment(ref runs);
                firstRunning.TrySetResult();
                await releaseFirst.Task;
            },
            onBackgroundFailure: static () => { },
            drain: TimeSpan.FromSeconds(10),
            queueCapacity: 1,
            clock: clock);

        try
        {
            // The first delivery occupies the single worker, the second fills the one-slot queue,
            // the third parks with its heartbeat armed — and its first renew reports the lease lost.
            await handle(CancellationToken.None);
            await firstRunning.Task.WaitAsync(TimeSpan.FromSeconds(30));
            await handle(CancellationToken.None);
            Assert.Equal(0, Volatile.Read(ref calls.Renew));

            var parked = handle(CancellationToken.None);
            await WalkAsync(clock, TimeSpan.FromSeconds(30), until: () => parked.IsCompleted);
            await parked.WaitAsync(TimeSpan.FromSeconds(30));
            Assert.True(Volatile.Read(ref calls.Renew) >= 1);
        }
        finally
        {
            releaseFirst.TrySetResult();
            await dispatcher.DisposeAsync();
        }

        Assert.Equal(2, Volatile.Read(ref runs));
        Assert.Equal(2, calls.Ack);
        Assert.Equal(0, calls.Nak);
    }

    /// <summary>
    /// Regression: a renew attempt was unbounded — the stores pinned CancellationToken.None and set no
    /// command timeout — so a renew hung on a black-holed pooled connection or a failover failed only
    /// at the provider's 30 s command timeout (never, on MongoDB), after the 20 s of lease left past
    /// the beat: the short-backoff retry never got to run inside the lease and a peer re-claimed the
    /// row under a healthy handler. Each attempt is now bounded by the beat interval, so a hung one is
    /// abandoned and retried well before <c>locked_until</c>. Virtual clock.
    /// </summary>
    [Theory]
    [InlineData(Provider.SqlServer)]
    [InlineData(Provider.PostgreSql)]
    [InlineData(Provider.MongoDb)]
    public async Task LeaseRenewal_AHungAttempt_IsAbandonedAndRetriedInsideTheLease(Provider provider)
    {
        var lockTimeout = TimeSpan.FromSeconds(30);
        var clock = new VirtualTimeProvider();
        var calls = new Calls { RenewHangsUntilCancelled = true };
        var logger = new CollectingLogger();
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var (dispatcher, handle) = CreateInlineDispatcher(provider, calls, () => release.Task, lockTimeout, clock, logger);

        await using (dispatcher)
        {
            var handling = handle(CancellationToken.None);
            try
            {
                // Beat at 10 s hangs; its bound fires at 20 s; the retry starts a second later —
                // all inside the lease the claim stamped (30 s).
                await WalkAsync(clock, lockTimeout - TimeSpan.FromSeconds(1), until: () => Volatile.Read(ref calls.Renew) >= 2);
                Assert.True(
                    Volatile.Read(ref calls.Renew) >= 2,
                    "a hung renew attempt was never abandoned, so no retry ran before the lease lapsed");
                Assert.Contains(logger.Messages, message => message.StartsWith("Failed to renew the lease of", StringComparison.Ordinal));
            }
            finally
            {
                release.TrySetResult();
                await handling.WaitAsync(TimeSpan.FromSeconds(30));
            }
        }

        Assert.Equal(1, calls.Ack);
    }

    /// <summary>
    /// Regression: the inline heartbeat's source was LINKED to the subscriber's stopping token, so
    /// the beat ended the moment the host began stopping — while the handler, which takes no token,
    /// kept running through the stop budget. <c>locked_until</c> then passed under a live handler, a
    /// peer (a new replica mid-deploy) claimed the row and ran it concurrently, and the original's
    /// fenced ack silently no-opped. The beat now ends only when the handler does. Virtual clock.
    /// </summary>
    [Theory]
    [InlineData(Provider.SqlServer)]
    [InlineData(Provider.PostgreSql)]
    [InlineData(Provider.MongoDb)]
    public async Task LeaseRenewal_KeepsBeatingAfterTheSubscriberStops_WhileTheHandlerStillRuns(Provider provider)
    {
        // A 1 s beat, so the walk (and the old code's settle waits) stay short.
        var lockTimeout = TimeSpan.FromSeconds(3);
        var clock = new VirtualTimeProvider();
        var calls = new Calls();
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var (dispatcher, handle) = CreateInlineDispatcher(provider, calls, () => release.Task, lockTimeout, clock, new CollectingLogger());
        using var stopping = new CancellationTokenSource();

        await using (dispatcher)
        {
            var handling = handle(stopping.Token);
            try
            {
                // The host starts stopping while the handler is still running.
                await stopping.CancelAsync();

                await WalkAsync(clock, TimeSpan.FromSeconds(2.5), until: () => Volatile.Read(ref calls.RenewSucceeded) >= 2);
                Assert.True(
                    Volatile.Read(ref calls.RenewSucceeded) >= 2,
                    $"the lease heartbeat stopped with the subscriber while the handler still ran ({Volatile.Read(ref calls.Renew)} renew call(s))");
            }
            finally
            {
                release.TrySetResult();
                await handling.WaitAsync(TimeSpan.FromSeconds(30));
            }
        }

        Assert.Equal(1, calls.Ack);
    }

    /// <summary>
    /// A handler that completes before the first beat produces no renewal activity — the beat is
    /// cancelled exception-free before it fires — and the grace wait proves nothing keeps beating
    /// after the ack (no leaked renewal loop). The heartbeat is still ARMED before the handler
    /// runs; see the blocking-handler fact for why that must never be lazy.
    /// </summary>
    [Theory]
    [InlineData(Provider.SqlServer)]
    [InlineData(Provider.PostgreSql)]
    [InlineData(Provider.MongoDb)]
    public async Task FastHandler_AcksWithoutRenewalActivity_AndLeaksNoHeartbeat(Provider provider)
    {
        var calls = new Calls();
        var logger = new CollectingLogger();

        await RunAsync(
            provider,
            logger,
            lockTimeout: TimeSpan.FromMilliseconds(20),
            calls: calls,
            handler: static () => Task.CompletedTask);

        // Several beat intervals of grace: a leaked heartbeat would renew here.
        await Task.Delay(100);
        Assert.Equal(1, calls.Ack);
        Assert.Equal(0, calls.Renew);
    }

    /// <summary>
    /// The heartbeat must be armed BEFORE any user code runs: a handler can burn its whole lease
    /// synchronously (CPU work or blocking I/O with no await), and only an already-armed beat —
    /// firing on a timer thread — can renew under the blocked handler thread. This handler never
    /// yields until it OBSERVES a renewal, so a lazily-armed heartbeat (armed only after the
    /// first incomplete await) fails this fact by timeout.
    /// </summary>
    [Theory]
    [InlineData(Provider.SqlServer)]
    [InlineData(Provider.PostgreSql)]
    [InlineData(Provider.MongoDb)]
    public async Task SynchronouslyBlockingHandler_IsRenewedUnderTheBlockedThread(Provider provider)
    {
        var calls = new Calls();
        var logger = new CollectingLogger();

        await RunAsync(
            provider,
            logger,
            lockTimeout: TimeSpan.FromMilliseconds(100),
            calls: calls,
            handler: () =>
            {
                var blockedUntil = DateTime.UtcNow + TimeSpan.FromSeconds(10);
                while (Volatile.Read(ref calls.Renew) < 1 && DateTime.UtcNow < blockedUntil)
                    Thread.Sleep(10);
                return Task.CompletedTask;
            });

        Assert.True(calls.Renew >= 1, "the lease was never renewed while the handler blocked its thread");
        Assert.Equal(1, calls.Ack);
    }

    /// <summary>
    /// When the handler fails after an early ACK, the message is already gone from the queue, so a
    /// dead-letter that also fails leaves the failure observable only through logs and the callback —
    /// which is exactly what it must say.
    /// </summary>
    [Theory]
    [InlineData(Provider.SqlServer)]
    [InlineData(Provider.PostgreSql)]
    [InlineData(Provider.MongoDb)]
    public async Task EarlyAck_HandlerFailure_ReportsAnUnrecoverableDeadLetter(Provider provider)
    {
        var calls = new Calls { DeadLetterResult = false };
        var logger = new CollectingLogger();
        var failures = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await RunAsync(
            provider,
            logger,
            lockTimeout: TimeSpan.FromSeconds(30),
            calls: calls,
            handler: () => throw new InvalidOperationException("handler blew up"),
            earlyAck: true,
            onBackgroundFailure: () => failures.TrySetResult());

        await failures.Task.WaitAsync(TimeSpan.FromSeconds(30));
        await Eventually(() => logger.Messages.Any(
            message => message.StartsWith("Failed to dead-letter already-ACKed", StringComparison.Ordinal)));
        Assert.Equal(1, calls.DeadLetter);
    }

    /// <summary>
    /// Regression (round 31): a burial that THROWS must be contained like one that returns false.
    /// The delivery contract says DeadLetterAsync never throws, but the stores' DeadLetterEnabled
    /// = false branch runs its compensating ack OUTSIDE their guarded region — so a transient DB
    /// failure there escaped HandleAsync, tore the subscriber down mid-flight, and (via the
    /// pre-execution cap) re-threw on every re-claim of the same row.
    /// </summary>
    [Theory]
    [InlineData(Provider.SqlServer)]
    [InlineData(Provider.PostgreSql)]
    [InlineData(Provider.MongoDb)]
    public async Task Burial_ThatThrows_IsContainedAndFallsBackToNak(Provider provider)
    {
        var calls = new Calls { DeadLetterThrows = true };
        var logger = new CollectingLogger();

        // Last-attempt failure: the burial throws, the throw must not escape HandleAsync, and the
        // row is released for redelivery instead.
        await RunAsync(
            provider,
            logger,
            lockTimeout: TimeSpan.FromSeconds(30),
            calls: calls,
            handler: () => throw new InvalidOperationException("handler blew up"),
            maxDeliveryAttempts: 1,
            attempt: 1);

        Assert.Equal(1, calls.DeadLetter);
        Assert.Equal(1, calls.Nak);
        Assert.Equal(0, calls.Ack);

        // Pre-execution over-cap path: same containment, and the handler never runs.
        var overCap = new Calls { DeadLetterThrows = true };
        var handled = false;
        await RunAsync(
            provider,
            logger,
            lockTimeout: TimeSpan.FromSeconds(30),
            calls: overCap,
            handler: () =>
            {
                handled = true;
                return Task.CompletedTask;
            },
            maxDeliveryAttempts: 1,
            attempt: 2);

        Assert.False(handled);
        Assert.Equal(1, overCap.DeadLetter);
        Assert.Equal(1, overCap.Nak);
    }

    /// <summary>
    /// Regression: the at-the-cap dead-letter passed the subscriber's stopping token, while every
    /// other settlement in the shared file passes <see cref="CancellationToken.None"/>. A handler
    /// failing on its LAST attempt during a stop had the burial aborted inside the store (whose
    /// connection/transaction calls throw on the cancelled token) and the poison row was NAKed
    /// back into the queue instead of dead-lettered.
    /// </summary>
    [Theory]
    [InlineData(Provider.SqlServer)]
    [InlineData(Provider.PostgreSql)]
    [InlineData(Provider.MongoDb)]
    public async Task LastAttemptFailure_DuringShutdown_StillBuriesWithAnUncancellableSettle(Provider provider)
    {
        var calls = new Calls();
        var logger = new CollectingLogger();
        using var stopping = new CancellationTokenSource();
        await stopping.CancelAsync();

        await RunAsync(
            provider,
            logger,
            lockTimeout: TimeSpan.FromSeconds(30),
            calls: calls,
            handler: static () => throw new InvalidOperationException("handler boom on the last attempt"),
            maxDeliveryAttempts: 3,
            attempt: 3,
            cancellationToken: stopping.Token);

        Assert.Equal(1, calls.DeadLetter);
        Assert.Equal(CancellationToken.None, calls.LastDeadLetterToken);
        Assert.Equal(0, calls.Nak);
    }

    /// <summary>
    /// Regression: once the drain budget lapsed, the early-ACK background loop kept STARTING
    /// queued work — the drain token cannot stop the real handler, which takes no token — so the
    /// stop ran past the validated shutdown budget, and anything still queued at process exit
    /// vanished (its queue row was deleted by the early ACK) with no dead-letter and no
    /// OnBackgroundFailure. Past the budget, queued-but-unstarted deliveries are now routed
    /// through the dead-letter/OnBackgroundFailure path instead of being executed or lost
    /// (Redis/Pub-Sub dispatcher parity).
    /// </summary>
    [Theory]
    [InlineData(Provider.SqlServer)]
    [InlineData(Provider.PostgreSql)]
    [InlineData(Provider.MongoDb)]
    public async Task EarlyAck_DrainBudgetLapse_DeadLettersQueuedWorkInsteadOfRunningIt(Provider provider)
    {
        var calls = new Calls();
        var executed = 0;
        var backgroundFailures = 0;
        var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var (dispatcher, handle) = CreateEarlyAckDispatcher(
            provider,
            calls,
            handler: async _ =>
            {
                Interlocked.Increment(ref executed);
                await releaseFirst.Task;
            },
            onBackgroundFailure: () => Interlocked.Increment(ref backgroundFailures),
            drain: TimeSpan.FromMilliseconds(50));

        try
        {
            // First delivery: ACKed at enqueue, its handler blocks the single background worker.
            await handle(CancellationToken.None);
            await Eventually(() => Volatile.Read(ref executed) == 1);

            // Second delivery: ACKed at enqueue, queued behind the blocked worker.
            await handle(CancellationToken.None);
            Assert.Equal(2, calls.Ack);

            // Dispose lapses the 50 ms drain budget and cancels; the blocked handler is released
            // only afterwards, so the second delivery is read from the queue past the budget.
            await dispatcher.DisposeAsync();
        }
        finally
        {
            releaseFirst.TrySetResult();
        }

        await Eventually(() => calls.DeadLetter == 1 && Volatile.Read(ref backgroundFailures) >= 1);

        // The queued delivery was accounted for, not run: one execution (the first), one
        // dead-letter and one background-failure notification (the second).
        Assert.Equal(1, Volatile.Read(ref executed));
    }

    /// <summary>
    /// Regression (round 33): once the drain budget lapsed, DisposeAsync cancelled the workers and
    /// RETURNED — the routing of every still-queued already-ACKed entry (dead-letter +
    /// OnBackgroundFailure, the path the lapse fact above pins) ran fire-and-forget with no budget
    /// at all, so the subscriber and then the host finished stopping while the workers were only
    /// starting to bury them, and the entries vanished with no record (their rows were deleted by
    /// the early ACK). That is why the fact above has to poll with Eventually AFTER DisposeAsync
    /// returns. BackgroundDrainTimeout is now SPLIT rather than exceeded: three quarters let the
    /// handlers drain, the last quarter is reserved for the post-lapse routing, which DisposeAsync
    /// awaits before returning — so with slow burials the counts hold synchronously here.
    /// </summary>
    [Theory]
    [InlineData(Provider.SqlServer)]
    [InlineData(Provider.PostgreSql)]
    [InlineData(Provider.MongoDb)]
    public async Task EarlyAck_DrainBudgetLapse_FinishesDeadLetteringQueuedWorkBeforeDisposeReturns(Provider provider)
    {
        // Each burial takes 10 ms to commit; the counter moves only once it has. With a 6 s budget
        // the reserved quarter (1.5 s) covers the three burials (30 ms of work) ~50x over; the old
        // fire-and-forget dispose returned with none of them committed.
        //
        // The headroom is deliberately that wide. At 25 ms per burial inside a 3 s budget the
        // reserve was 750 ms for 75 ms of work — 10x — and a starved Windows CI runner still
        // stalled long enough to commit only two of the three (the assertion below is synchronous
        // by design, so it cannot wait the stall out). Nothing here measures speed: the fact is
        // that DisposeAsync does not return until the queued entries are buried, so buying the
        // margin with a smaller unit of work and a larger reserve costs a few seconds and removes
        // a wall-clock race against the runner.
        var calls = new Calls { DeadLetterDelay = TimeSpan.FromMilliseconds(10) };
        var backgroundFailures = 0;
        var drain = TimeSpan.FromSeconds(6);
        var (dispatcher, handle) = CreateEarlyAckDispatcher(
            provider,
            calls,
            // Blocks the single worker until the drain budget lapses and the worker token fires,
            // then returns normally so the loop reads what is still queued past the budget.
            handler: async token => await Task.Delay(Timeout.InfiniteTimeSpan, token).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing),
            onBackgroundFailure: () => Interlocked.Increment(ref backgroundFailures),
            drain: drain);

        // First delivery occupies the worker; three more queue behind it, each ACKed at enqueue.
        for (var i = 0; i < 4; i++)
            await handle(CancellationToken.None);
        Assert.Equal(4, calls.Ack);

        var stopwatch = Stopwatch.StartNew();
        await dispatcher.DisposeAsync();
        stopwatch.Stop();

        // Synchronous, no Eventually: every queued entry was buried and surfaced before
        // DisposeAsync returned.
        Assert.Equal(3, calls.DeadLetter);
        Assert.Equal(3, Volatile.Read(ref backgroundFailures));

        // Split, not extended: the routing rode inside BackgroundDrainTimeout, the only term the
        // shutdown-budget validator sums for this dispatcher.
        Assert.True(
            stopwatch.Elapsed < drain + TimeSpan.FromSeconds(1),
            $"DisposeAsync took {stopwatch.Elapsed} against a {drain} budget");
    }

    /// <summary>
    /// Regression (round-43 pre-commit review): an early-ACK job the flow engine handed back at
    /// host stop (DurableFlowInterruptedException) was logged as a background handler FAILURE at
    /// Error and dead-lettered under the generic failure reason — an alert on every deploy, while
    /// Kafka, RabbitMQ, Redis and NATS log it as a warning and dead-letter it as
    /// handed_back_after_commit. The copy still matters: the early ACK deleted the row, so nothing
    /// else records the wake-up.
    /// </summary>
    [Theory]
    [InlineData(Provider.SqlServer)]
    [InlineData(Provider.PostgreSql)]
    [InlineData(Provider.MongoDb)]
    public async Task EarlyAck_HostStopHandBack_IsAWarning_DeadLetteredAsHandedBackAfterCommit(Provider provider)
    {
        var calls = new Calls();
        var log = new CollectingLogger();
        var backgroundFailures = 0;
        var (dispatcher, handle) = CreateEarlyAckDispatcher(
            provider,
            calls,
            handler: _ => throw new DurableFlowInterruptedException("The host is stopping; the delivery is handed back."),
            onBackgroundFailure: () => Interlocked.Increment(ref backgroundFailures),
            drain: TimeSpan.FromSeconds(5),
            log: log);

        await using (dispatcher)
        {
            await handle(CancellationToken.None);
            await Eventually(() => calls.DeadLetter == 1 && Volatile.Read(ref backgroundFailures) == 1);
        }

        Assert.StartsWith("handed_back_after_commit", calls.LastDeadLetterException?.Message, StringComparison.Ordinal);
        Assert.Contains(log.Messages, message => message.Contains("handed back by the flow engine", StringComparison.Ordinal));
        Assert.DoesNotContain(log.Messages, message => message.Contains("background handler failed", StringComparison.Ordinal));
    }

    /// <summary>
    /// Regression (fixpoint round 2): with <c>DeadLetterEnabled = false</c> every store reports a
    /// burial as done without writing anything, so an early-ACK hand-back logged "Dead-lettered a
    /// copy" at Warning — a copy that does not exist — and never the loss Error; the drain-lapse and
    /// attempt-cap lines claimed "dead-lettering" too. Each now says no copy was written.
    /// </summary>
    [Theory]
    [InlineData(Provider.SqlServer)]
    [InlineData(Provider.PostgreSql)]
    [InlineData(Provider.MongoDb)]
    public async Task DeadLetteringDisabled_EarlyAckAndCapLogs_ClaimNoCopy(Provider provider)
    {
        // Hand-back after the early ACK.
        var handBackLog = new CollectingLogger();
        var handBackCalls = new Calls();
        var handBackFailures = 0;
        var (handBack, handleHandBack) = CreateEarlyAckDispatcher(
            provider,
            handBackCalls,
            handler: _ => throw new DurableFlowInterruptedException("The host is stopping; the delivery is handed back."),
            onBackgroundFailure: () => Interlocked.Increment(ref handBackFailures),
            drain: TimeSpan.FromSeconds(5),
            log: handBackLog,
            deadLetterEnabled: false);
        await using (handBack)
        {
            await handleHandBack(CancellationToken.None);
            await Eventually(() => Volatile.Read(ref handBackFailures) == 1);
        }

        Assert.DoesNotContain(handBackLog.Messages, message => message.Contains("Dead-lettered a copy", StringComparison.Ordinal));
        Assert.Contains(handBackLog.Messages, message => message.Contains("no dead-letter copy was written (DeadLetterEnabled is false)", StringComparison.Ordinal)
            && message.Contains("resume the flow explicitly", StringComparison.Ordinal));

        // Drain-budget lapse: the queued entry is routed, not started — and not claimed as buried.
        var lapseLog = new CollectingLogger();
        var lapseCalls = new Calls();
        var lapseFailures = 0;
        var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var (lapse, handleLapse) = CreateEarlyAckDispatcher(
            provider,
            lapseCalls,
            handler: _ => releaseFirst.Task,
            onBackgroundFailure: () => Interlocked.Increment(ref lapseFailures),
            drain: TimeSpan.FromMilliseconds(50),
            log: lapseLog,
            deadLetterEnabled: false);
        try
        {
            await handleLapse(CancellationToken.None);
            await handleLapse(CancellationToken.None);
            await lapse.DisposeAsync();
        }
        finally
        {
            releaseFirst.TrySetResult();
        }

        await Eventually(() => Volatile.Read(ref lapseFailures) >= 1);
        Assert.DoesNotContain(lapseLog.Messages, message => message.Contains("Dead-letter", StringComparison.Ordinal));
        Assert.Contains(lapseLog.Messages, message => message.Contains("no dead-letter copy is written (DeadLetterEnabled is false)", StringComparison.Ordinal));

        // Attempt cap on the inline path: the store drops the row, and the line says so.
        var capLog = new CollectingLogger();
        await RunAsync(
            provider,
            capLog,
            lockTimeout: TimeSpan.FromSeconds(30),
            calls: new Calls(),
            handler: static () => throw new InvalidOperationException("handler blew up"),
            maxDeliveryAttempts: 1,
            deadLetterEnabled: false);

        Assert.DoesNotContain(capLog.Messages, message => message.Contains("dead-lettering", StringComparison.Ordinal));
        Assert.Contains(capLog.Messages, message => message.Contains("no dead-letter copy is written", StringComparison.Ordinal));
    }

    /// <summary>
    /// Fixpoint r2 precommit E4: the dispose drain's own lines still claimed dead-lettering with
    /// <c>DeadLetterEnabled = false</c> — "dead-lettering the entries still queued", "did not finish
    /// dead-lettering the undrained entries", "were not all dead-lettered" — telling operators copies
    /// were being written when none were. Each now branches on the option. Pre-fix: all three lines
    /// said "dead-letter…".
    /// </summary>
    [Theory]
    [InlineData(Provider.SqlServer)]
    [InlineData(Provider.PostgreSql)]
    [InlineData(Provider.MongoDb)]
    public async Task DeadLetteringDisabled_DisposeDrainLines_ClaimNoCopy(Provider provider)
    {
        // Drain lapse, then the routing reserve lapses too: the worker is still inside the first
        // handler, so the queued entry cannot be routed within the reserve.
        var lapseLog = new CollectingLogger();
        var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var firstRunning = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var (lapse, handleLapse) = CreateEarlyAckDispatcher(
            provider,
            new Calls(),
            handler: _ =>
            {
                firstRunning.TrySetResult();
                return releaseFirst.Task;
            },
            onBackgroundFailure: static () => { },
            drain: TimeSpan.FromMilliseconds(50),
            log: lapseLog,
            deadLetterEnabled: false);
        try
        {
            await handleLapse(CancellationToken.None);
            await firstRunning.Task.WaitAsync(TimeSpan.FromSeconds(30));
            await handleLapse(CancellationToken.None);
            await lapse.DisposeAsync();
        }
        finally
        {
            releaseFirst.TrySetResult();
        }

        Assert.Contains(lapseLog.Messages, message => message.Contains("did not drain within", StringComparison.Ordinal)
            && message.Contains("without a dead-letter copy (DeadLetterEnabled is false)", StringComparison.Ordinal));
        Assert.Contains(lapseLog.Messages, message => message.Contains("did not finish reporting the undrained entries", StringComparison.Ordinal));
        Assert.DoesNotContain(lapseLog.Messages, message => message.Contains("dead-lettering", StringComparison.Ordinal));

        // Every worker faulted, and routing what they left queued outruns the reserve.
        var strandedLog = new CollectingLogger();
        var strandedCalls = new Calls { DeadLetterDelay = TimeSpan.FromSeconds(5) };
        var strandedRunning = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseStranded = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var (stranded, handleStranded) = CreateEarlyAckDispatcher(
            provider,
            strandedCalls,
            handler: async _ =>
            {
                strandedRunning.TrySetResult();
                await releaseStranded.Task;
            },
            onBackgroundFailure: static () => { },
            drain: TimeSpan.FromMilliseconds(200),
            log: strandedLog,
            deadLetterEnabled: false);
        try
        {
            await handleStranded(CancellationToken.None);
            await strandedRunning.Task.WaitAsync(TimeSpan.FromSeconds(30));
            await handleStranded(CancellationToken.None);
            var workers = (Task[])stranded.GetType().BaseType!
                .GetField("_backgroundWorkers", BindingFlags.Instance | BindingFlags.NonPublic)!
                .GetValue(stranded)!;
            workers[0] = Task.FromException(new InvalidOperationException("worker died"));
            await stranded.DisposeAsync();
        }
        finally
        {
            releaseStranded.TrySetResult();
        }

        Assert.Contains(strandedLog.Messages, message => message.Contains("were not all reported within", StringComparison.Ordinal));
        Assert.DoesNotContain(strandedLog.Messages, message => message.Contains("dead-lettered within", StringComparison.Ordinal));
    }

    /// <summary>
    /// Regression (fixpoint round 2, S9#1): the database worker subscribers kept claiming after host
    /// stop began, so a claim that landed as <c>ApplicationStopping</c> fired was started (and run up
    /// to its first flow wait, then handed back with its lock held until the lease lapsed) or, under
    /// early ACK, settled at enqueue. Once the host is stopping the dispatcher now hands such a
    /// delivery straight back — an immediate NAK, never started, never acknowledged.
    /// </summary>
    [Theory]
    [InlineData(Provider.SqlServer, false)]
    [InlineData(Provider.PostgreSql, false)]
    [InlineData(Provider.MongoDb, false)]
    [InlineData(Provider.SqlServer, true)]
    [InlineData(Provider.PostgreSql, true)]
    [InlineData(Provider.MongoDb, true)]
    public async Task HostStop_AClaimThatLandsAfterIt_IsHandedBackUnstarted(Provider provider, bool earlyAck)
    {
        using var host = new DurableFlowContextTestSupport.StoppingHost();
        host.StopApplication();
        var calls = new Calls();
        var runs = 0;

        await RunAsync(
            provider,
            new CollectingLogger(),
            lockTimeout: TimeSpan.FromSeconds(30),
            calls: calls,
            handler: () => { Interlocked.Increment(ref runs); return Task.CompletedTask; },
            earlyAck: earlyAck,
            hostStopping: host.ApplicationStopping);

        Assert.Equal(0, Volatile.Read(ref runs));
        Assert.Equal(0, calls.Ack);
        Assert.Equal(1, calls.Nak);
        Assert.Equal(TimeSpan.Zero, calls.LastNakDelay);
    }

    /// <summary>
    /// Regression (fixpoint round 2, S9#1): a claim parked on a full early-ACK queue kept waiting
    /// through host stop — until the subscriber's own stop, which comes last — and was then either
    /// enqueued and settled (a slot freed) or released with the full <c>RedeliveryDelay</c>. Host
    /// stop now ends the park at once and releases the claim with no delay, for a live replica.
    /// </summary>
    [Theory]
    [InlineData(Provider.SqlServer)]
    [InlineData(Provider.PostgreSql)]
    [InlineData(Provider.MongoDb)]
    public async Task HostStop_EndsTheEarlyAckPark_AndReleasesTheClaimWithoutDelay(Provider provider)
    {
        using var host = new DurableFlowContextTestSupport.StoppingHost();
        var calls = new Calls();
        var firstRunning = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var (dispatcher, handle) = CreateEarlyAckDispatcher(
            provider,
            calls,
            handler: async _ =>
            {
                firstRunning.TrySetResult();
                await releaseFirst.Task;
            },
            onBackgroundFailure: static () => { },
            drain: TimeSpan.FromSeconds(10),
            queueCapacity: 1,
            clock: new VirtualTimeProvider(),
            hostStopping: host.ApplicationStopping);

        try
        {
            // The first delivery occupies the single worker, the second fills the one-slot queue,
            // the third parks.
            await handle(CancellationToken.None);
            await firstRunning.Task.WaitAsync(TimeSpan.FromSeconds(30));
            await handle(CancellationToken.None);
            var parked = handle(CancellationToken.None);
            Assert.False(parked.IsCompleted);

            host.StopApplication();
            var released = await Task.WhenAny(parked, Task.Delay(TimeSpan.FromSeconds(10))) == parked;
            Assert.True(released, "the parked claim kept waiting for a queue slot after host stop began");
            await parked;

            Assert.Equal(2, calls.Ack);
            Assert.Equal(1, calls.Nak);
            Assert.Equal(TimeSpan.Zero, calls.LastNakDelay);
        }
        finally
        {
            releaseFirst.TrySetResult();
            await dispatcher.DisposeAsync();
        }
    }

    /// <summary>
    /// Fixpoint r2 precommit E3: a queue slot can free in the same instant ApplicationStopping
    /// fires, and the park's wait then completes TRUE rather than cancelled — the loop judged only
    /// the lease before writing, so the delivery was enqueued and early-ACKed on a stopping host
    /// (and then handed back into a dead-letter copy at its first flow wait). The clock hook stops
    /// the host exactly there: the park's lease check (between the slot freeing and the write) is the
    /// first clock read once the hook is armed. Pre-fix: three ACKs, no NAK.
    /// </summary>
    [Theory]
    [InlineData(Provider.SqlServer)]
    [InlineData(Provider.PostgreSql)]
    [InlineData(Provider.MongoDb)]
    public async Task HostStop_LandingAsTheParkedSlotFrees_ReleasesTheClaimInsteadOfEnqueueingIt(Provider provider)
    {
        using var host = new DurableFlowContextTestSupport.StoppingHost();
        var clock = new ClockReadHook(new VirtualTimeProvider());
        var calls = new Calls();
        var firstRunning = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var runs = 0;
        var (dispatcher, handle) = CreateEarlyAckDispatcher(
            provider,
            calls,
            handler: async _ =>
            {
                if (Interlocked.Increment(ref runs) == 1)
                {
                    firstRunning.TrySetResult();
                    await releaseFirst.Task;
                }
            },
            onBackgroundFailure: static () => { },
            drain: TimeSpan.FromSeconds(10),
            queueCapacity: 1,
            clock: clock,
            hostStopping: host.ApplicationStopping);

        try
        {
            await handle(CancellationToken.None);
            await firstRunning.Task.WaitAsync(TimeSpan.FromSeconds(30));
            await handle(CancellationToken.None);
            var parked = handle(CancellationToken.None);
            Assert.False(parked.IsCompleted);

            clock.OnNextRead = host.StopApplication;
            releaseFirst.TrySetResult();
            await parked.WaitAsync(TimeSpan.FromSeconds(30));

            Assert.True(host.ApplicationStopping.IsCancellationRequested);
            Assert.Equal(2, calls.Ack);
            Assert.Equal(1, calls.Nak);
            Assert.Equal(TimeSpan.Zero, calls.LastNakDelay);
        }
        finally
        {
            releaseFirst.TrySetResult();
            await dispatcher.DisposeAsync();
        }
    }

    /// <summary>A clock that runs a one-shot hook on its next timestamp read, then delegates.</summary>
    private sealed class ClockReadHook(VirtualTimeProvider inner) : TimeProvider
    {
        public Action? OnNextRead;

        public override long GetTimestamp()
        {
            Interlocked.Exchange(ref OnNextRead, null)?.Invoke();
            return inner.GetTimestamp();
        }

        public override long TimestampFrequency => inner.TimestampFrequency;

        public override DateTimeOffset GetUtcNow() => inner.GetUtcNow();

        public override TimeZoneInfo LocalTimeZone => inner.LocalTimeZone;

        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
            => inner.CreateTimer(callback, state, dueTime, period);
    }

    /// <summary>
    /// Regression (fixpoint round 2, S9#7): the park dropped a claim only on a POSITIVE "fence gone"
    /// renew. A host cut off from the database while its peers were not saw every renew THROW —
    /// never answer — so past LockTimeout a peer re-claimed and ran the row, and once a slot freed
    /// this host enqueued and ran it too. Renewals still failing once LockTimeout has passed since
    /// the last one that landed now count as a lost lease.
    /// </summary>
    [Theory]
    [InlineData(Provider.SqlServer)]
    [InlineData(Provider.PostgreSql)]
    [InlineData(Provider.MongoDb)]
    public async Task EarlyAckPark_DropsTheDelivery_WhenRenewalsKeepFailingPastLockTimeout(Provider provider)
    {
        var calls = new Calls { RenewThrows = true };
        var clock = new VirtualTimeProvider();
        var runs = 0;
        var firstRunning = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var (dispatcher, handle) = CreateEarlyAckDispatcher(
            provider,
            calls,
            handler: async _ =>
            {
                Interlocked.Increment(ref runs);
                firstRunning.TrySetResult();
                await releaseFirst.Task;
            },
            onBackgroundFailure: static () => { },
            drain: TimeSpan.FromSeconds(10),
            queueCapacity: 1,
            clock: clock);

        try
        {
            await handle(CancellationToken.None);
            await firstRunning.Task.WaitAsync(TimeSpan.FromSeconds(30));
            await handle(CancellationToken.None);

            var parked = handle(CancellationToken.None);
            await WalkAsync(clock, TimeSpan.FromSeconds(45), until: () => parked.IsCompleted);
            Assert.True(parked.IsCompleted, $"the parked claim was still waiting after {Volatile.Read(ref calls.Renew)} failed renewals spanning more than LockTimeout");
            await parked;
            Assert.True(Volatile.Read(ref calls.Renew) >= 2);
        }
        finally
        {
            releaseFirst.TrySetResult();
            await dispatcher.DisposeAsync();
        }

        Assert.Equal(2, Volatile.Read(ref runs));
        Assert.Equal(2, calls.Ack);
        Assert.Equal(0, calls.Nak);
    }

    /// <summary>
    /// The same loss judged by the park itself: after a GC or VM pause the in-memory write can
    /// complete before the timer-driven heartbeat has run again, so a slot that frees once the lease
    /// is past LockTimeout (the heartbeat here wedged inside a renew that never answers) drops the
    /// claim instead of enqueueing it. Before round 2 the park's WriteAsync enqueued and ran it.
    /// </summary>
    [Theory]
    [InlineData(Provider.SqlServer)]
    [InlineData(Provider.PostgreSql)]
    [InlineData(Provider.MongoDb)]
    public async Task EarlyAckPark_DropsTheDelivery_WhenASlotFreesAfterTheLeaseLapsed(Provider provider)
    {
        var calls = new Calls { RenewGate = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously) };
        var clock = new VirtualTimeProvider();
        var runs = 0;
        var firstRunning = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var (dispatcher, handle) = CreateEarlyAckDispatcher(
            provider,
            calls,
            handler: async _ =>
            {
                Interlocked.Increment(ref runs);
                firstRunning.TrySetResult();
                await releaseFirst.Task;
            },
            onBackgroundFailure: static () => { },
            drain: TimeSpan.FromSeconds(10),
            queueCapacity: 1,
            clock: clock);

        try
        {
            await handle(CancellationToken.None);
            await firstRunning.Task.WaitAsync(TimeSpan.FromSeconds(30));
            await handle(CancellationToken.None);

            // The park's first beat (10 s) wedges in the store; then the lease runs out from under it.
            var parked = handle(CancellationToken.None);
            clock.Advance(TimeSpan.FromSeconds(10));
            await Eventually(() => Volatile.Read(ref calls.Renew) == 1);
            clock.Advance(TimeSpan.FromSeconds(21));
            Assert.False(parked.IsCompleted);

            // A slot frees: the park judges the lease's age before writing.
            releaseFirst.TrySetResult();
            await parked.WaitAsync(TimeSpan.FromSeconds(30));
        }
        finally
        {
            releaseFirst.TrySetResult();
            calls.RenewGate.TrySetResult(true);
            await dispatcher.DisposeAsync();
        }

        Assert.Equal(2, Volatile.Read(ref runs));
        Assert.Equal(2, calls.Ack);
        Assert.Equal(0, calls.Nak);
    }

    /// <summary>
    /// Regression (fixpoint round 2, GS3#3): the early-ACK worker logged a background failure BEFORE
    /// dead-lettering it, and Microsoft.Extensions.Logging rethrows a provider's failure — so a
    /// throwing logger cost the already-ACKed job its dead-letter copy and its OnBackgroundFailure
    /// report, and ended the worker (nothing ran the jobs queued behind it). The copy and the report
    /// now come first and the log line cannot end the worker.
    /// </summary>
    [Theory]
    [InlineData(Provider.SqlServer)]
    [InlineData(Provider.PostgreSql)]
    [InlineData(Provider.MongoDb)]
    public async Task EarlyAck_ThrowingLogger_NeitherLosesTheFailureRecordNorEndsTheWorker(Provider provider)
    {
        var calls = new Calls();
        var log = new CollectingLogger { ThrowOnMessageContaining = "background handler failed" };
        var backgroundFailures = 0;
        var runs = 0;
        var secondRan = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var (dispatcher, handle) = CreateEarlyAckDispatcher(
            provider,
            calls,
            handler: _ =>
            {
                if (Interlocked.Increment(ref runs) == 1)
                    throw new InvalidOperationException("handler blew up");
                secondRan.TrySetResult();
                return Task.CompletedTask;
            },
            onBackgroundFailure: () => Interlocked.Increment(ref backgroundFailures),
            drain: TimeSpan.FromSeconds(5),
            log: log);

        await using (dispatcher)
        {
            await handle(CancellationToken.None);
            await handle(CancellationToken.None);
            var survived = await Task.WhenAny(secondRan.Task, Task.Delay(TimeSpan.FromSeconds(10))) == secondRan.Task;
            Assert.True(survived, "the background worker died on the throwing log line, so the job queued behind it never ran");
        }

        Assert.Equal(1, calls.DeadLetter);
        Assert.Equal(1, Volatile.Read(ref backgroundFailures));
    }

    /// <summary>
    /// The drain-lapse arm under the same throwing logger: it logged "was not started" before the
    /// burial, so the first lapsed entry's log line ended the worker with that entry and every one
    /// queued behind it neither dead-lettered nor reported.
    /// </summary>
    [Theory]
    [InlineData(Provider.SqlServer)]
    [InlineData(Provider.PostgreSql)]
    [InlineData(Provider.MongoDb)]
    public async Task EarlyAck_DrainLapse_UnderAThrowingLogger_StillRoutesEveryQueuedEntry(Provider provider)
    {
        var calls = new Calls();
        var log = new CollectingLogger { ThrowOnMessageContaining = "drain budget had lapsed" };
        var backgroundFailures = 0;
        var firstRunning = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var (dispatcher, handle) = CreateEarlyAckDispatcher(
            provider,
            calls,
            handler: _ =>
            {
                firstRunning.TrySetResult();
                return releaseFirst.Task;
            },
            onBackgroundFailure: () => Interlocked.Increment(ref backgroundFailures),
            drain: TimeSpan.FromMilliseconds(50),
            log: log);

        try
        {
            // One running, three queued behind it; the dispose lapses the budget before the first
            // is released, so all three are routed past it.
            await handle(CancellationToken.None);
            await firstRunning.Task.WaitAsync(TimeSpan.FromSeconds(30));
            for (var i = 0; i < 3; i++)
                await handle(CancellationToken.None);
            await dispatcher.DisposeAsync();
        }
        finally
        {
            releaseFirst.TrySetResult();
        }

        await Eventually(() => calls.DeadLetter == 3 && Volatile.Read(ref backgroundFailures) == 3);
    }

    /// <summary>
    /// Regression (fixpoint round 2, GS3#3): when every background worker had already ended with a
    /// fault, the drain's join completed faulted at once and DisposeAsync only logged it at Debug —
    /// the already-ACKed entries still queued (their rows deleted by the early ACK) were lost with no
    /// dead-letter copy and no report. DisposeAsync now routes them itself, inside the reserve. The
    /// fault is injected by swapping the worker task: no real fault can end a worker any more.
    /// </summary>
    [Theory]
    [InlineData(Provider.SqlServer)]
    [InlineData(Provider.PostgreSql)]
    [InlineData(Provider.MongoDb)]
    public async Task EarlyAck_DisposeAfterEveryWorkerFaulted_StillRoutesTheQueuedEntries(Provider provider)
    {
        var calls = new Calls();
        var backgroundFailures = 0;
        var firstRunning = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var (dispatcher, handle) = CreateEarlyAckDispatcher(
            provider,
            calls,
            handler: async _ =>
            {
                firstRunning.TrySetResult();
                await releaseFirst.Task;
            },
            onBackgroundFailure: () => Interlocked.Increment(ref backgroundFailures),
            drain: TimeSpan.FromSeconds(8));

        try
        {
            await handle(CancellationToken.None);
            await firstRunning.Task.WaitAsync(TimeSpan.FromSeconds(30));
            await handle(CancellationToken.None);
            await handle(CancellationToken.None);

            // The one worker is busy with the first entry; make the drain see it as having faulted.
            var workers = (Task[])dispatcher.GetType().BaseType!
                .GetField("_backgroundWorkers", BindingFlags.Instance | BindingFlags.NonPublic)!
                .GetValue(dispatcher)!;
            workers[0] = Task.FromException(new InvalidOperationException("worker died"));

            await dispatcher.DisposeAsync();

            // Synchronous: both queued entries were routed before DisposeAsync returned.
            Assert.Equal(2, calls.DeadLetter);
            Assert.Equal(2, Volatile.Read(ref backgroundFailures));
        }
        finally
        {
            releaseFirst.TrySetResult();
        }
    }

    private static (IAsyncDisposable Dispatcher, Func<CancellationToken, Task> Handle) CreateEarlyAckDispatcher(
        Provider provider,
        Calls calls,
        Func<CancellationToken, Task> handler,
        Action onBackgroundFailure,
        TimeSpan drain,
        int queueCapacity = 8,
        TimeSpan? lockTimeout = null,
        TimeProvider? clock = null,
        CollectingLogger? log = null,
        CancellationToken hostStopping = default,
        bool deadLetterEnabled = true)
    {
        var logger = log ?? new CollectingLogger();
        var lease = lockTimeout ?? TimeSpan.FromSeconds(30);
        switch (provider)
        {
            case Provider.SqlServer:
            {
                var options = new SqlServerAsyncResponseTransportOptions
                {
                    ConnectionString = "Server=localhost;Database=unused;User ID=sa;Password=unused;TrustServerCertificate=True",
                    LockTimeout = lease,
                    DeadLetterEnabled = deadLetterEnabled
                };
                var subscriber = new SqlServerSubscriberOptions();
                subscriber.UseAckAfterEnqueue(1, queueCapacity, drain);
                subscriber.OnBackgroundFailure = _ => { onBackgroundFailure(); return ValueTask.CompletedTask; };
                var dispatcher = new SqlServerMessageDispatcher((_, token) => handler(token), options, subscriber, logger, SqlServerSubscriberRole.Worker, clock, hostStopping);
                return (dispatcher, token => dispatcher.HandleAsync(
                    new SqlServerTransportDelivery(
                        Guid.NewGuid(), "worker", "{}", Headers, 1,
                        calls.AckAsync, calls.NakAsync, calls.DeadLetterAsync, calls.RenewAsync),
                    token));
            }

            case Provider.PostgreSql:
            {
                var options = new PostgreSqlAsyncResponseTransportOptions { LockTimeout = lease, DeadLetterEnabled = deadLetterEnabled };
                var subscriber = new PostgreSqlSubscriberOptions();
                subscriber.UseAckAfterEnqueue(1, queueCapacity, drain);
                subscriber.OnBackgroundFailure = _ => { onBackgroundFailure(); return ValueTask.CompletedTask; };
                var dispatcher = new PostgreSqlMessageDispatcher((_, token) => handler(token), options, subscriber, logger, PostgreSqlSubscriberRole.Worker, clock, hostStopping);
                return (dispatcher, token => dispatcher.HandleAsync(
                    new PostgreSqlTransportDelivery(
                        Guid.NewGuid(), "worker", "{}", Headers, 1,
                        calls.AckAsync, calls.NakAsync, calls.DeadLetterAsync, calls.RenewAsync),
                    token));
            }

            default:
            {
                var options = new MongoDbAsyncResponseTransportOptions { LockTimeout = lease, DeadLetterEnabled = deadLetterEnabled };
                var subscriber = new MongoDbSubscriberOptions();
                subscriber.UseAckAfterEnqueue(1, queueCapacity, drain);
                subscriber.OnBackgroundFailure = _ => { onBackgroundFailure(); return ValueTask.CompletedTask; };
                var dispatcher = new MongoDbMessageDispatcher((_, token) => handler(token), options, subscriber, logger, MongoDbSubscriberRole.Worker, clock, hostStopping);
                return (dispatcher, token => dispatcher.HandleAsync(
                    new MongoDbTransportDelivery(
                        Guid.NewGuid(), "worker", "{}", Headers, 1,
                        calls.AckAsync, calls.NakAsync, calls.DeadLetterAsync, calls.RenewAsync),
                    token));
            }
        }
    }

    /// <summary>
    /// A failed handler is released for redelivery with a NAK; a NAK that itself fails (connection
    /// blip during the release) must be swallowed like the guarded ACK — an escape would tear the
    /// whole subscriber down over one poison message, and the drain on the way out dead-letters
    /// unrelated already-ACKed in-flight work. The lease lapses on its own either way.
    /// </summary>
    [Theory]
    [InlineData(Provider.SqlServer)]
    [InlineData(Provider.PostgreSql)]
    [InlineData(Provider.MongoDb)]
    public async Task FailedHandler_WhoseReleaseNakThrows_DoesNotTearDownTheSubscriber(Provider provider)
    {
        var calls = new Calls { NakThrows = true };
        var logger = new CollectingLogger();

        await RunAsync(
            provider,
            logger,
            lockTimeout: TimeSpan.FromSeconds(30),
            calls: calls,
            handler: static () => throw new InvalidOperationException("handler blew up"));

        Assert.Equal(1, calls.Nak);
        Assert.Equal(0, calls.Ack);
        Assert.Contains(logger.Messages, message => message.StartsWith("Failed to NAK", StringComparison.Ordinal)
            && message.Contains("after a failed handler", StringComparison.Ordinal));
    }

    /// <summary>
    /// Same guard on the second bare release: when the attempt cap is reached but the dead-letter
    /// publish fails, the fallback NAK's own failure must not escape either.
    /// </summary>
    [Theory]
    [InlineData(Provider.SqlServer)]
    [InlineData(Provider.PostgreSql)]
    [InlineData(Provider.MongoDb)]
    public async Task FailedDeadLetter_WhoseFallbackNakThrows_DoesNotTearDownTheSubscriber(Provider provider)
    {
        var calls = new Calls { NakThrows = true, DeadLetterResult = false };
        var logger = new CollectingLogger();

        await RunAsync(
            provider,
            logger,
            lockTimeout: TimeSpan.FromSeconds(30),
            calls: calls,
            handler: static () => throw new InvalidOperationException("handler blew up"),
            maxDeliveryAttempts: 1);

        Assert.Equal(1, calls.DeadLetter);
        Assert.Equal(1, calls.Nak);
        Assert.Contains(logger.Messages, message => message.StartsWith("Failed to NAK", StringComparison.Ordinal));
    }

    /// <summary>
    /// Regression (round 29): the cap was consulted ONLY in HandleFailureAsync, which runs only
    /// when the handler threw. A delivery that ended any other way — the process died mid-handler,
    /// the lease lapsed while the store was unreachable at settlement — came back at attempts
    /// cap+1, cap+2, … and was EXECUTED again every time: redelivered forever, killing each replica
    /// in turn, never dead-lettered. The cap is now a pre-execution guard.
    /// </summary>
    [Theory]
    [InlineData(Provider.SqlServer, false)]
    [InlineData(Provider.PostgreSql, false)]
    [InlineData(Provider.MongoDb, false)]
    // The guard sits BEFORE the ack-mode branch, so early-ACK cannot enqueue it either.
    [InlineData(Provider.SqlServer, true)]
    [InlineData(Provider.PostgreSql, true)]
    [InlineData(Provider.MongoDb, true)]
    public async Task OverCapDelivery_IsDeadLetteredWithoutExecutingTheHandler(Provider provider, bool earlyAck)
    {
        var calls = new Calls();
        var logger = new CollectingLogger();
        var handlerRuns = 0;

        await RunAsync(
            provider,
            logger,
            lockTimeout: TimeSpan.FromSeconds(30),
            calls: calls,
            handler: () => { Interlocked.Increment(ref handlerRuns); return Task.CompletedTask; },
            earlyAck: earlyAck,
            maxDeliveryAttempts: 2,
            attempt: 3);

        Assert.Equal(0, Volatile.Read(ref handlerRuns));
        Assert.Equal(1, calls.DeadLetter);
        Assert.Equal(0, calls.Ack);
        Assert.Equal(0, calls.Nak);
        Assert.Contains(logger.Messages, message => message.Contains("dead-lettering without executing it", StringComparison.Ordinal));
    }

    /// <summary>
    /// The over-cap guard's own failure path: a dead-letter publish that reports false releases the
    /// row instead of dropping it, exactly as the post-handler cap does.
    /// </summary>
    [Theory]
    [InlineData(Provider.SqlServer)]
    [InlineData(Provider.PostgreSql)]
    [InlineData(Provider.MongoDb)]
    public async Task OverCapDelivery_WhoseDeadLetterFails_IsReleasedForRetry(Provider provider)
    {
        var calls = new Calls { DeadLetterResult = false };
        var logger = new CollectingLogger();

        await RunAsync(
            provider,
            logger,
            lockTimeout: TimeSpan.FromSeconds(30),
            calls: calls,
            handler: static () => Task.CompletedTask,
            maxDeliveryAttempts: 2,
            attempt: 5);

        Assert.Equal(1, calls.DeadLetter);
        Assert.Equal(1, calls.Nak);
        Assert.Equal(0, calls.Ack);
    }

    /// <summary>
    /// The unlimited cap (0, the default) still means unlimited: an attempt far past any bound is
    /// executed normally rather than swept into the dead-letter queue by the new guard.
    /// </summary>
    [Theory]
    [InlineData(Provider.SqlServer)]
    [InlineData(Provider.PostgreSql)]
    [InlineData(Provider.MongoDb)]
    public async Task UnlimitedCap_StillExecutesAHighAttemptDelivery(Provider provider)
    {
        var calls = new Calls();
        var handlerRuns = 0;

        await RunAsync(
            provider,
            new CollectingLogger(),
            lockTimeout: TimeSpan.FromSeconds(30),
            calls: calls,
            handler: () => { Interlocked.Increment(ref handlerRuns); return Task.CompletedTask; },
            maxDeliveryAttempts: 0,
            attempt: 99);

        Assert.Equal(1, Volatile.Read(ref handlerRuns));
        Assert.Equal(1, calls.Ack);
        Assert.Equal(0, calls.DeadLetter);
    }

    /// <summary>
    /// Regression: <c>DbTransportHeaders.Materialize</c> guarded only <c>JsonException</c>, but an
    /// ESCAPED lone surrogate (<c>"\ud800"</c>) is well-formed JSON — Parse accepts it — and only
    /// transcoding it throws, with <c>InvalidOperationException</c>. That is the very throw this
    /// helper exists to prevent: after the claim committed attempts+1/lock_id and before any
    /// delivery object exists, so the row can never reach the failure handler or the dead-letter
    /// path. The unusable header is skipped and the rest survive.
    /// </summary>
    [Theory]
    [InlineData(typeof(SqlServerAsyncResponseTransportOptions))]
    [InlineData(typeof(PostgreSqlAsyncResponseTransportOptions))]
    [InlineData(typeof(MongoDbAsyncResponseTransportOptions))]
    public void HeaderMaterialization_SkipsAHeaderThatCannotBeTranscoded_InsteadOfThrowing(Type marker)
    {
        var materialize = marker.Assembly
            .GetType("AsyncResponse.Transports.DbTransportHeaders", throwOnError: true)!
            .GetMethod("Materialize", BindingFlags.Public | BindingFlags.Static)!;

        IReadOnlyDictionary<string, string> Materialize(string json)
        {
            try
            {
                return (IReadOnlyDictionary<string, string>)materialize.Invoke(null, [json])!;
            }
            catch (TargetInvocationException ex) when (ex.InnerException is not null)
            {
                System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(ex.InnerException).Throw();
                throw;
            }
        }

        var badValue = Materialize("""{"AR-CorrelationId":"abc","noise":"\ud800","n":1}""");
        Assert.Equal("abc", badValue["AR-CorrelationId"]);
        Assert.Equal("1", badValue["n"]);
        Assert.False(badValue.ContainsKey("noise"));

        var badName = Materialize("""{"\udc00":"noise","AR-CorrelationId":"abc"}""");
        Assert.Equal("abc", Assert.Single(badName).Value);
    }

    /// <summary>
    /// Regression: a RAW lone surrogate (not the escape) in <c>headers_json</c> — SQL Server's
    /// <c>nvarchar(max)</c> stores UTF-16 code units unvalidated and SqlClient returns them verbatim
    /// — cannot even be transcoded to the UTF-8 <c>JsonDocument.Parse(string)</c> reads, and that
    /// throws <see cref="ArgumentException"/>, which the <see cref="System.Text.Json.JsonException"/>
    /// guard let through: inside <c>TryClaimAsync</c>, after the claim committed, before any delivery
    /// existed — an unkillable poison row that tore the subscriber down on every re-claim. The
    /// unusable column now degrades to no headers. Built in the body: theory data mangles lone
    /// surrogates.
    /// </summary>
    [Theory]
    [InlineData(typeof(SqlServerAsyncResponseTransportOptions))]
    [InlineData(typeof(PostgreSqlAsyncResponseTransportOptions))]
    [InlineData(typeof(MongoDbAsyncResponseTransportOptions))]
    public void HeaderMaterialization_DegradesARawLoneSurrogateToNoHeaders_InsteadOfThrowing(Type marker)
    {
        var materialize = marker.Assembly
            .GetType("AsyncResponse.Transports.DbTransportHeaders", throwOnError: true)!
            .GetMethod("Materialize", BindingFlags.Public | BindingFlags.Static)!;
        var json = "{\"AR-CorrelationId\":\"abc\",\"noise\":\"" + '\ud800' + "\"}";

        IReadOnlyDictionary<string, string> headers;
        try
        {
            headers = (IReadOnlyDictionary<string, string>)materialize.Invoke(null, [json])!;
        }
        catch (TargetInvocationException ex) when (ex.InnerException is not null)
        {
            System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(ex.InnerException).Throw();
            throw;
        }

        Assert.Empty(headers);
    }

    public enum Provider
    {
        SqlServer,
        PostgreSql,
        MongoDb
    }

    /// <summary>
    /// Builds the provider's dispatcher over a delivery wired to <paramref name="calls"/>, handles
    /// one message, and disposes — draining any background worker the early-ACK mode started.
    /// </summary>
    /// <summary>
    /// Regression: settlement JOINED the cancelled heartbeat first, for up to LockTimeout. The
    /// in-flight renew pins CancellationToken.None for its connect and command, so against a slow
    /// database a handler that had already SUCCEEDED sat waiting on the renew until the very lease
    /// it was extending had lapsed — and the row was re-claimed and run again before its ack went
    /// out. Every settlement is fenced by lock_id, so a beat still in flight is a no-op against it
    /// and nothing needs to wait: the ack goes out while the renew is still wedged. When that beat
    /// finally lands on a row the ack deleted, its "no match" is the settlement's doing and is NOT
    /// reported as a lost lease.
    /// </summary>
    [Theory]
    [InlineData(Provider.SqlServer)]
    [InlineData(Provider.PostgreSql)]
    [InlineData(Provider.MongoDb)]
    public async Task Settlement_DoesNotWaitOnAnInFlightRenewal(Provider provider)
    {
        // A lease whose REAL-time length the old join would have had to wait out in full.
        var lockTimeout = TimeSpan.FromMinutes(2);
        var clock = new VirtualTimeProvider();
        var calls = new Calls { RenewGate = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously) };
        var logger = new CollectingLogger();
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var (dispatcher, handle) = CreateInlineDispatcher(provider, calls, () => release.Task, lockTimeout, clock, logger);

        await using (dispatcher)
        {
            var handling = handle(CancellationToken.None);

            // The first beat fires and wedges inside the store call; then the handler succeeds.
            await WalkAsync(clock, lockTimeout, until: () => Volatile.Read(ref calls.Renew) >= 1);
            Assert.Equal(1, Volatile.Read(ref calls.Renew));
            release.TrySetResult();

            var settled = await Task.WhenAny(handling, Task.Delay(TimeSpan.FromSeconds(20))) == handling;
            Assert.True(settled, "the ack waited on a lease renewal that was still in flight");
            await handling;
            Assert.Equal(1, calls.Ack);

            // The abandoned beat now answers "no match" — the ack deleted the row.
            calls.RenewGate.TrySetResult(false);
            await Task.Delay(100);
            Assert.DoesNotContain(logger.Messages, message => message.Contains("was lost", StringComparison.Ordinal));
        }
    }

    /// <summary>
    /// Regression (round 33): the renewal join ran even while the subscriber was STOPPING. RenewAsync
    /// pins CancellationToken.None, so against a stalled database the stop path spent the full
    /// LockTimeout (30 s by default) — a term no shutdown validator sums — BEFORE the background
    /// drain even began, and already-ACKed entries were lost when the host expired mid-drain.
    /// Settlement is fenced by lock_id, so a beat still in flight is a no-op against it: the join
    /// was first skipped once the stopping token is cancelled, and has since been dropped
    /// altogether (see the settlement fact above). Inline (ack-after-handler) path.
    /// </summary>
    [Theory]
    [InlineData(Provider.SqlServer)]
    [InlineData(Provider.PostgreSql)]
    [InlineData(Provider.MongoDb)]
    public async Task RenewalJoin_IsSkippedWhileStopping_SoSettlementDoesNotSpendLockTimeout(Provider provider)
    {
        var calls = new Calls { RenewGate = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously) };
        var logger = new CollectingLogger();
        using var stopping = new CancellationTokenSource();
        var afterHandler = new Stopwatch();

        await RunAsync(
            provider,
            logger,
            lockTimeout: TimeSpan.FromSeconds(2),
            calls: calls,
            handler: async () =>
            {
                // Once a beat is in flight (and, in this fake, wedged for good) the subscriber
                // starts stopping; the handler itself completes normally.
                while (Volatile.Read(ref calls.Renew) < 1)
                    await Task.Delay(10);
                await stopping.CancelAsync();
                afterHandler.Start();
            },
            cancellationToken: stopping.Token).WaitAsync(TimeSpan.FromSeconds(10));
        afterHandler.Stop();

        Assert.Equal(1, calls.Ack);
        Assert.True(
            afterHandler.Elapsed < TimeSpan.FromSeconds(1),
            $"settlement waited {afterHandler.Elapsed} on the wedged renewal while stopping (LockTimeout is 2 s)");
        Assert.DoesNotContain(logger.Messages, message => message.Contains("did not stop within LockTimeout", StringComparison.Ordinal));
        calls.RenewGate.TrySetResult(true);
    }

    /// <summary>
    /// The same regression on the early-ACK park: a claim parked on a full background queue arms
    /// the heartbeat for the park's duration, and a stop that lands while its beat is wedged used
    /// to spend LockTimeout in the join before the NAK'ed delivery's HandleAsync returned.
    /// </summary>
    [Theory]
    [InlineData(Provider.SqlServer)]
    [InlineData(Provider.PostgreSql)]
    [InlineData(Provider.MongoDb)]
    public async Task RenewalJoin_IsSkippedWhileStopping_OnTheEarlyAckPark(Provider provider)
    {
        var calls = new Calls { RenewGate = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously) };
        var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var stopping = new CancellationTokenSource();
        var (dispatcher, handle) = CreateEarlyAckDispatcher(
            provider,
            calls,
            handler: _ => releaseFirst.Task,
            onBackgroundFailure: static () => { },
            drain: TimeSpan.FromSeconds(5),
            queueCapacity: 1,
            lockTimeout: TimeSpan.FromSeconds(2));

        try
        {
            // First delivery occupies the single worker, the second fills the one-slot queue, so
            // the third parks — with the fenced heartbeat armed for the park.
            await handle(CancellationToken.None);
            await handle(CancellationToken.None);
            var parked = handle(stopping.Token);
            await Eventually(() => Volatile.Read(ref calls.Renew) >= 1);

            var stopwatch = Stopwatch.StartNew();
            await stopping.CancelAsync();
            await parked.WaitAsync(TimeSpan.FromSeconds(10));
            stopwatch.Stop();

            // Released promptly (never enqueued), without a LockTimeout join on the wedged beat.
            Assert.Equal(1, calls.Nak);
            Assert.Equal(2, calls.Ack);
            Assert.True(
                stopwatch.Elapsed < TimeSpan.FromSeconds(1),
                $"the parked claim's HandleAsync took {stopwatch.Elapsed} to return after the stop (LockTimeout is 2 s)");
        }
        finally
        {
            releaseFirst.TrySetResult();
            calls.RenewGate.TrySetResult(true);
            await dispatcher.DisposeAsync();
        }
    }

    /// <summary>
    /// An ack-after-handler dispatcher whose lease-renewal beat runs on <paramref name="clock"/>,
    /// plus a delegate that handles one fresh delivery wired to <paramref name="calls"/>.
    /// </summary>
    private static (IAsyncDisposable Dispatcher, Func<CancellationToken, Task> Handle) CreateInlineDispatcher(
        Provider provider,
        Calls calls,
        Func<Task> handler,
        TimeSpan lockTimeout,
        TimeProvider clock,
        CollectingLogger logger)
    {
        switch (provider)
        {
            case Provider.SqlServer:
            {
                var options = new SqlServerAsyncResponseTransportOptions
                {
                    ConnectionString = "Server=localhost;Database=unused;User ID=sa;Password=unused;TrustServerCertificate=True",
                    LockTimeout = lockTimeout
                };
                var dispatcher = new SqlServerMessageDispatcher(
                    (_, _) => handler(), options, new SqlServerSubscriberOptions(), logger, SqlServerSubscriberRole.Worker, clock);
                return (dispatcher, token => dispatcher.HandleAsync(
                    new SqlServerTransportDelivery(
                        Guid.NewGuid(), "worker", "{}", Headers, 1,
                        calls.AckAsync, calls.NakAsync, calls.DeadLetterAsync, calls.RenewAsync),
                    token));
            }

            case Provider.PostgreSql:
            {
                var options = new PostgreSqlAsyncResponseTransportOptions { LockTimeout = lockTimeout };
                var dispatcher = new PostgreSqlMessageDispatcher(
                    (_, _) => handler(), options, new PostgreSqlSubscriberOptions(), logger, PostgreSqlSubscriberRole.Worker, clock);
                return (dispatcher, token => dispatcher.HandleAsync(
                    new PostgreSqlTransportDelivery(
                        Guid.NewGuid(), "worker", "{}", Headers, 1,
                        calls.AckAsync, calls.NakAsync, calls.DeadLetterAsync, calls.RenewAsync),
                    token));
            }

            default:
            {
                var options = new MongoDbAsyncResponseTransportOptions { LockTimeout = lockTimeout };
                var dispatcher = new MongoDbMessageDispatcher(
                    (_, _) => handler(), options, new MongoDbSubscriberOptions(), logger, MongoDbSubscriberRole.Worker, clock);
                return (dispatcher, token => dispatcher.HandleAsync(
                    new MongoDbTransportDelivery(
                        Guid.NewGuid(), "worker", "{}", Headers, 1,
                        calls.AckAsync, calls.NakAsync, calls.DeadLetterAsync, calls.RenewAsync),
                    token));
            }
        }
    }

    /// <summary>
    /// Walks the virtual clock forward by <paramref name="span"/> in half-second steps. The beat
    /// re-arms itself from a continuation that may run off the advancing thread, so each step first
    /// waits (briefly, in real time) for the loop to have a timer armed again — otherwise a step
    /// could pass over a beat that was not armed yet. A loop that has ended never re-arms; the
    /// settle wait then just lapses.
    /// </summary>
    private static async Task WalkAsync(VirtualTimeProvider clock, TimeSpan span, Func<bool>? until = null)
    {
        var step = TimeSpan.FromMilliseconds(500);
        for (var walked = TimeSpan.Zero; walked < span; walked += step)
        {
            await SettleAsync(clock, until);
            if (until?.Invoke() == true)
                return;

            clock.Advance(step);
        }

        await SettleAsync(clock, until);
    }

    private static async Task SettleAsync(VirtualTimeProvider clock, Func<bool>? until)
    {
        for (var i = 0; i < 200 && clock.NextTimerDueAt is null && until?.Invoke() != true; i++)
            await Task.Delay(5);
    }

    private static async Task RunAsync(
        Provider provider,
        CollectingLogger logger,
        TimeSpan lockTimeout,
        Calls calls,
        Func<Task> handler,
        bool earlyAck = false,
        Action? onBackgroundFailure = null,
        int? maxDeliveryAttempts = null,
        int attempt = 1,
        CancellationToken cancellationToken = default,
        bool deadLetterEnabled = true,
        CancellationToken hostStopping = default)
    {
        switch (provider)
        {
            case Provider.SqlServer:
            {
                var options = new SqlServerAsyncResponseTransportOptions
                {
                    ConnectionString = "Server=localhost;Database=unused;User ID=sa;Password=unused;TrustServerCertificate=True",
                    LockTimeout = lockTimeout,
                    DeadLetterEnabled = deadLetterEnabled
                };
                var subscriber = new SqlServerSubscriberOptions();
                if (maxDeliveryAttempts is { } sqlCap)
                    subscriber.MaxDeliveryAttempts = sqlCap;
                if (onBackgroundFailure is not null)
                    subscriber.OnBackgroundFailure = _ => { onBackgroundFailure(); return ValueTask.CompletedTask; };
                if (earlyAck)
                    subscriber.UseAckAfterEnqueue(1, 8, TimeSpan.FromMilliseconds(50));

                await using var dispatcher = new SqlServerMessageDispatcher(
                    (_, _) => handler(), options, subscriber, logger, SqlServerSubscriberRole.Worker, hostStopping: hostStopping);
                await dispatcher.HandleAsync(
                    new SqlServerTransportDelivery(
                        Guid.NewGuid(), "worker", "{}", Headers, attempt,
                        calls.AckAsync, calls.NakAsync, calls.DeadLetterAsync, calls.RenewAsync),
                    cancellationToken);
                return;
            }

            case Provider.PostgreSql:
            {
                var options = new PostgreSqlAsyncResponseTransportOptions { LockTimeout = lockTimeout, DeadLetterEnabled = deadLetterEnabled };
                var subscriber = new PostgreSqlSubscriberOptions();
                if (maxDeliveryAttempts is { } pgCap)
                    subscriber.MaxDeliveryAttempts = pgCap;
                if (onBackgroundFailure is not null)
                    subscriber.OnBackgroundFailure = _ => { onBackgroundFailure(); return ValueTask.CompletedTask; };
                if (earlyAck)
                    subscriber.UseAckAfterEnqueue(1, 8, TimeSpan.FromMilliseconds(50));

                await using var dispatcher = new PostgreSqlMessageDispatcher(
                    (_, _) => handler(), options, subscriber, logger, PostgreSqlSubscriberRole.Worker, hostStopping: hostStopping);
                await dispatcher.HandleAsync(
                    new PostgreSqlTransportDelivery(
                        Guid.NewGuid(), "worker", "{}", Headers, attempt,
                        calls.AckAsync, calls.NakAsync, calls.DeadLetterAsync, calls.RenewAsync),
                    cancellationToken);
                return;
            }

            default:
            {
                var options = new MongoDbAsyncResponseTransportOptions { LockTimeout = lockTimeout, DeadLetterEnabled = deadLetterEnabled };
                var subscriber = new MongoDbSubscriberOptions();
                if (maxDeliveryAttempts is { } mongoCap)
                    subscriber.MaxDeliveryAttempts = mongoCap;
                if (onBackgroundFailure is not null)
                    subscriber.OnBackgroundFailure = _ => { onBackgroundFailure(); return ValueTask.CompletedTask; };
                if (earlyAck)
                    subscriber.UseAckAfterEnqueue(1, 8, TimeSpan.FromMilliseconds(50));

                await using var dispatcher = new MongoDbMessageDispatcher(
                    (_, _) => handler(), options, subscriber, logger, MongoDbSubscriberRole.Worker, hostStopping: hostStopping);
                await dispatcher.HandleAsync(
                    new MongoDbTransportDelivery(
                        Guid.NewGuid(), "worker", "{}", Headers, attempt,
                        calls.AckAsync, calls.NakAsync, calls.DeadLetterAsync, calls.RenewAsync),
                    cancellationToken);
                return;
            }
        }
    }

    private static readonly Dictionary<string, string> Headers = new(StringComparer.OrdinalIgnoreCase);

    private static async Task Eventually(Func<bool> condition)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        while (!condition())
            await Task.Delay(15, timeout.Token);
    }

    /// <summary>Settlement callbacks shared by all three provider delivery records.</summary>
    private sealed class Calls
    {
        public int Ack;
        public int Nak;
        public int Renew;
        public int DeadLetter;
        public bool DeadLetterResult = true;
        public bool RenewThrows;
        public bool NakThrows;

        public ValueTask AckAsync()
        {
            Interlocked.Increment(ref Ack);
            return ValueTask.CompletedTask;
        }

        /// <summary>The delay the last NAK asked for.</summary>
        public TimeSpan? LastNakDelay;

        public ValueTask NakAsync(TimeSpan delay)
        {
            LastNakDelay = delay;
            Interlocked.Increment(ref Nak);
            if (NakThrows)
                throw new InvalidOperationException("release store unavailable");

            return ValueTask.CompletedTask;
        }

        public CancellationToken? LastDeadLetterToken;

        public bool DeadLetterThrows;

        /// <summary>When set, every burial takes this long to commit and is counted only once it has.</summary>
        public TimeSpan DeadLetterDelay;

        public Exception? LastDeadLetterException;

        public ValueTask<bool> DeadLetterAsync(Exception exception, bool deleteOriginal, CancellationToken cancellationToken)
        {
            LastDeadLetterToken = cancellationToken;
            LastDeadLetterException = exception;
            if (DeadLetterDelay > TimeSpan.Zero)
                return new ValueTask<bool>(SlowDeadLetterAsync());

            Interlocked.Increment(ref DeadLetter);
            if (DeadLetterThrows)
                throw new InvalidOperationException("dead-letter store unavailable");

            return ValueTask.FromResult(DeadLetterResult);
        }

        private async Task<bool> SlowDeadLetterAsync()
        {
            await Task.Delay(DeadLetterDelay);
            Interlocked.Increment(ref DeadLetter);
            return DeadLetterResult;
        }

        /// <summary>When set, every renew parks on this gate — a store call that never returns.</summary>
        public TaskCompletionSource<bool>? RenewGate;

        /// <summary>The next this-many renews throw (a transient store blip); later ones answer.</summary>
        public int RenewFailuresRemaining;

        /// <summary>What a renew that reaches the store answers: <c>false</c> = the lock_id fence no longer matches.</summary>
        public bool RenewResult = true;

        public int RenewSucceeded;

        /// <summary>When set, every renew hangs until the token it was handed fires (a black-holed connection).</summary>
        public bool RenewHangsUntilCancelled;

        /// <summary>When set, a renew throws while this answers true (an outage that clears on cue).</summary>
        public Func<bool>? RenewFailsWhile;

        public ValueTask<bool> RenewAsync(CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref Renew);
            if (RenewThrows || RenewFailsWhile?.Invoke() == true || Interlocked.Decrement(ref RenewFailuresRemaining) >= 0)
                throw new InvalidOperationException("lease store unavailable");
            if (RenewHangsUntilCancelled)
                return new ValueTask<bool>(HangAsync(cancellationToken));
            if (RenewGate is not null)
                return new ValueTask<bool>(RenewGate.Task);

            if (RenewResult)
                Interlocked.Increment(ref RenewSucceeded);
            return ValueTask.FromResult(RenewResult);
        }

        private static async Task<bool> HangAsync(CancellationToken cancellationToken)
        {
            await Task.Delay(Timeout.Infinite, cancellationToken);
            return true;
        }
    }

}
