using AsyncResponse.Transports.NATS;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Moq;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using System.Runtime.CompilerServices;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Coverage-gap tests for the NATS transport: the dispatcher's over-cap settlement failures, the
/// early-ACK park that meets a disposing dispatcher, the stop-time reserve and a worker freed after
/// it, the subscriber's hand-back and heartbeat edges, the JetStream adapter's verification of an
/// existing stream and its lost creation races, the validator bounds, and the interface defaults.
/// </summary>
public sealed class NatsTransportCoverageGapTests
{
    private static readonly TimeSpan HangGuard = TimeSpan.FromSeconds(10);
    private const string DeadLetterSubject = "asyncresponse.transport.deadletter";

    // ---------------------------------------------------------------- validator

    [Theory]
    [InlineData(0)]
    [InlineData(6)]
    public void ValidateCommon_StreamReplicasOutsideJetStreamsLimit_Throws(int replicas)
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            NatsTransportOptionsValidator.ValidateCommon(new NatsAsyncResponseTransportOptions { StreamReplicas = replicas }));

        Assert.Contains(nameof(NatsAsyncResponseTransportOptions.StreamReplicas), ex.Message, StringComparison.Ordinal);
        Assert.Contains($"it is {replicas}", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateSubscriber_AnUnknownRole_IsBudgetedAlone_WithoutTheOtherRolesDrain()
    {
        // Worker and response drains are summed because the host stops them one after the other;
        // a role that is neither has no sibling, so only its own drain counts against the budget.
        var options = new NatsAsyncResponseTransportOptions { HostShutdownTimeout = TimeSpan.FromSeconds(30) };
        options.WorkerSubscriber.UseAckAfterEnqueue(1, 8, TimeSpan.FromSeconds(20));
        options.ResponseSubscriber.UseAckAfterEnqueue(1, 8, TimeSpan.FromSeconds(20));

        // Summed with the worker's 20 s, the response role's 20 s exceeds 30 s ...
        Assert.Throws<InvalidOperationException>(() =>
            NatsTransportOptionsValidator.ValidateSubscriber(options, options.ResponseSubscriber, nameof(NatsSubscriberRole.ResponseIngress)));

        // ... but an unrelated role is validated on its own drain.
        NatsTransportOptionsValidator.ValidateSubscriber(options, options.ResponseSubscriber, "Custom");
    }

    // ---------------------------------------------------------------- INatsJetStreamTransport defaults

    [Fact]
    public void JetStreamTransportDefaults_FetchMembers_ThrowNotSupported()
    {
        INatsJetStreamTransport transport = new MinimalJetStream();

        var noWait = Assert.Throws<NotSupportedException>(() => transport.FetchNoWaitAsync("s", "d", 1, CancellationToken.None));
        Assert.Contains(nameof(INatsJetStreamTransport.FetchNoWaitAsync), noWait.Message, StringComparison.Ordinal);

        var longPoll = Assert.Throws<NotSupportedException>(() => transport.FetchAsync("s", "d", 1, TimeSpan.FromSeconds(1), CancellationToken.None));
        Assert.Contains(nameof(INatsJetStreamTransport.FetchAsync), longPoll.Message, StringComparison.Ordinal);
    }

    // ---------------------------------------------------------------- dispatcher

    [Fact]
    public async Task OverCapDelivery_WhoseTermFails_IsLoggedAndNotRetried()
    {
        var jetStream = new FakeNatsJetStreamTransport();
        var logger = new RecordingThrowingLogger<NatsTransportCoverageGapTests>();
        var rec = new RecordingDelivery { TermException = new InvalidOperationException("TERM refused") };
        await using var dispatcher = CreateDispatcher(jetStream, (_, _) => Task.CompletedTask, new NatsSubscriberOptions { MaxDeliveryAttempts = 3 }, logger: logger);

        await dispatcher.HandleAsync(rec.Create("payload", numDelivered: 4), CancellationToken.None);

        Assert.Equal(DeadLetterSubject, Assert.Single(jetStream.Published).Subject);
        Assert.Equal(1, rec.Terms);
        Assert.Empty(rec.Naks); // buried: a failed TERM is not turned into a redelivery request
        Assert.True(logger.HasEntry(LogLevel.Warning, "Failed to TERM NATS message"));
    }

    [Fact]
    public async Task OverCapDelivery_WhoseDeadLetterPublishFails_IsNakedForARetryInsteadOfTermed()
    {
        var jetStream = new FakeNatsJetStreamTransport { PublishFailureForAttempt = _ => new InvalidOperationException("DLQ down") };
        var rec = new RecordingDelivery();
        var handled = false;
        var subscriber = new NatsSubscriberOptions { MaxDeliveryAttempts = 3 };
        await using var dispatcher = CreateDispatcher(
            jetStream,
            (_, _) =>
            {
                handled = true;
                return Task.CompletedTask;
            },
            subscriber);

        await dispatcher.HandleAsync(rec.Create("payload", numDelivered: 4), CancellationToken.None);

        Assert.False(handled);
        Assert.Equal(0, rec.Terms);
        Assert.Equal([subscriber.RedeliveryDelay], rec.Naks);
        Assert.Empty(jetStream.Published);
    }

    /// <summary>
    /// A delivery parked on a full early-ACK queue when the dispatcher starts disposing is never
    /// enqueued: the completed queue ends the park and the delivery is NAKed, not ACKed.
    /// </summary>
    [Fact]
    public async Task EarlyAck_ParkedOnAFullQueue_WhenTheDispatcherDisposes_IsNakedNotAcked()
    {
        var jetStream = new FakeNatsJetStreamTransport();
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handled = new List<string>();
        var subscriber = new NatsSubscriberOptions().UseAckAfterEnqueue(backgroundWorkerCount: 1, backgroundQueueCapacity: 1, TimeSpan.FromSeconds(5));
        var dispatcher = CreateDispatcher(
            jetStream,
            async (delivery, _) =>
            {
                lock (handled)
                    handled.Add(delivery.Payload);
                if (delivery.Payload == "p1")
                {
                    started.TrySetResult();
                    await release.Task.ConfigureAwait(false);
                }
            },
            subscriber);

        var p1 = new RecordingDelivery();
        var p2 = new RecordingDelivery();
        var p3 = new RecordingDelivery();
        Task? dispose = null;
        try
        {
            await dispatcher.HandleAsync(p1.Create("p1", numDelivered: 1), CancellationToken.None);
            await started.Task.WaitAsync(HangGuard);                                            // p1 holds the only worker
            await dispatcher.HandleAsync(p2.Create("p2", numDelivered: 1), CancellationToken.None); // p2 fills the queue
            var parked = dispatcher.HandleAsync(p3.Create("p3", numDelivered: 1), CancellationToken.None);
            Assert.False(parked.IsCompleted);

            dispose = dispatcher.DisposeAsync().AsTask();
            await parked.WaitAsync(HangGuard);

            Assert.Equal(0, p3.Acks);
            Assert.Equal([subscriber.RedeliveryDelay], p3.Naks);
        }
        finally
        {
            release.TrySetResult();
            await (dispose ?? dispatcher.DisposeAsync().AsTask()).WaitAsync(HangGuard);
        }

        lock (handled)
            Assert.DoesNotContain("p3", handled);
        Assert.Equal(1, p2.Acks);
    }

    /// <summary>
    /// The reserve's dead-letter publish hangs until the reserve lapses, so its entry has no copy
    /// (Error) and the entry behind it is left queued. A worker that comes free afterwards does not
    /// run that entry: it buries and reports it (with the delivery's identity).
    /// </summary>
    [Fact]
    public async Task EarlyAck_ReserveLapsingWithEntriesStillQueued_AFreedWorkerRoutesThemWithoutRunningThem()
    {
        var jetStream = new MinimalJetStream
        {
            // The reserve passes its own token: hang until it lapses. A worker's routing passes
            // CancellationToken.None and is served at once.
            PublishGate = static token => token.CanBeCanceled ? Task.Delay(Timeout.InfiniteTimeSpan, token) : Task.CompletedTask
        };
        var logger = new RecordingThrowingLogger<NatsTransportCoverageGapTests>();
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handlerRuns = 0;
        var reports = new List<NatsBackgroundFailureContext>();
        var subscriber = new NatsSubscriberOptions().UseAckAfterEnqueue(backgroundWorkerCount: 1, backgroundQueueCapacity: 8, TimeSpan.FromMilliseconds(400));
        subscriber.OnBackgroundFailure = context =>
        {
            lock (reports)
                reports.Add(context);
            return ValueTask.CompletedTask;
        };
        var options = new NatsAsyncResponseTransportOptions();
        var dispatcher = CreateDispatcher(
            jetStream,
            async (_, _) =>
            {
                Interlocked.Increment(ref handlerRuns);
                started.TrySetResult();
                await release.Task.ConfigureAwait(false); // ignores its token, like the ingress
            },
            subscriber,
            options,
            logger);

        var headers = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase) { [options.CorrelationIdHeader] = "corr-3" };
        try
        {
            await dispatcher.HandleAsync(new RecordingDelivery().Create("p1", numDelivered: 1), CancellationToken.None);
            await started.Task.WaitAsync(HangGuard);
            await dispatcher.HandleAsync(new RecordingDelivery().Create("p2", numDelivered: 1), CancellationToken.None);
            await dispatcher.HandleAsync(new RecordingDelivery().Create("p3", numDelivered: 2, headers: headers), CancellationToken.None);

            await dispatcher.DisposeAsync().AsTask().WaitAsync(HangGuard);

            Assert.True(logger.HasEntry(LogLevel.Error, "its dead-letter copy could not be written"));
            Assert.Empty(jetStream.Published);
        }
        finally
        {
            release.TrySetResult();
        }

        await WaitUntilAsync(() =>
        {
            lock (reports)
                return reports.Any(report => report.CorrelationId == "corr-3");
        });

        Assert.Equal(1, Volatile.Read(ref handlerRuns));
        var copy = Assert.Single(jetStream.Published, published => published.Payload == "p3");
        Assert.Equal(DeadLetterSubject, copy.Subject);
        lock (reports)
        {
            var report = Assert.Single(reports, report => report.CorrelationId == "corr-3");
            Assert.Equal("asyncresponse.transport.worker", report.Subject);
            Assert.Equal(nameof(NatsSubscriberRole.Worker), report.SubscriberRole);
            Assert.Equal(2, report.NumDelivered);
            Assert.Equal("test-consumer", report.Consumer);
            Assert.IsAssignableFrom<OperationCanceledException>(report.Exception);
        }

        Assert.True(logger.HasEntry(LogLevel.Warning, "the drain budget had lapsed. Dead-lettered it"));
    }

    /// <summary>
    /// A logging provider that throws from the OnBackgroundFailure failure log while the stop-time
    /// reserve waits for the worker: the log is guarded, so the worker does not fault, the
    /// handler failure is still dead-lettered and DisposeAsync returns without a drain error.
    /// (Before the guard this throw faulted the worker outside its handler guard.)
    /// </summary>
    [Fact]
    public async Task EarlyAck_AThrowingLoggerDuringTheReserve_DoesNotFaultTheWorkerOrEscapeDisposeAsync()
    {
        var jetStream = new FakeNatsJetStreamTransport();
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var logger = new HookLogger
        {
            OnLog = message =>
            {
                if (message.Contains("did not drain within", StringComparison.Ordinal))
                    release.TrySetResult();
                else if (message.Contains("OnBackgroundFailure callback threw", StringComparison.Ordinal))
                    throw new InvalidOperationException("logging provider failed");
            }
        };
        var subscriber = new NatsSubscriberOptions().UseAckAfterEnqueue(backgroundWorkerCount: 1, backgroundQueueCapacity: 8, TimeSpan.FromSeconds(1));
        subscriber.OnBackgroundFailure = _ => throw new InvalidOperationException("alert sink down");
        var dispatcher = CreateDispatcher(
            jetStream,
            async (_, _) =>
            {
                started.TrySetResult();
                await release.Task.ConfigureAwait(false);
                throw new InvalidOperationException("handler boom");
            },
            subscriber,
            logger: logger);

        await dispatcher.HandleAsync(new RecordingDelivery().Create("p1", numDelivered: 1), CancellationToken.None);
        await started.Task.WaitAsync(HangGuard);
        await dispatcher.DisposeAsync().AsTask().WaitAsync(HangGuard);

        // The worker finishes on its own even when the reserve lapsed first; wait for it, not for time.
        var workers = (Task[])dispatcher.GetType()
            .GetField("_backgroundWorkers", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!
            .GetValue(dispatcher)!;
        await Task.WhenAll(workers).WaitAsync(HangGuard);

        Assert.True(logger.Has(LogLevel.Error, "OnBackgroundFailure callback threw"));
        Assert.False(logger.Has(LogLevel.Debug, "Background worker drain for Worker ended with an error."));
        Assert.Equal(DeadLetterSubject, Assert.Single(jetStream.Published).Subject);
    }

    // ---------------------------------------------------------------- subscriber

    /// <summary>
    /// Host stop closing the worker's intake in the middle of a fetch hands back what the fetch had
    /// already delivered — unstarted, not settled first — and the loop then parks.
    /// </summary>
    [Fact]
    public async Task WorkerSubscriber_HostStopMidFetch_HandsBackWhatTheFetchHadDelivered()
    {
        var p1 = new RecordingDelivery();
        var jetStream = new ScriptedFetchJetStream(p1.Create("p1", numDelivered: 1));
        var ingress = new FakeAsyncResponseIngress();
        using var host = new StoppingHostLifetime();
        var subscriber = new NatsWorkerSubscriber(
            SubscriberOptions(o => o.WorkerSubscriber.UseAckAfterEnqueue(1, 8, TimeSpan.FromSeconds(5))),
            jetStream,
            ingress,
            new TestLogger<NatsWorkerSubscriber>(),
            hostLifetime: host);

        await subscriber.StartAsync(CancellationToken.None);
        try
        {
            await jetStream.FirstDelivered.Task.WaitAsync(HangGuard);
            host.StopApplication();
            await WaitUntilAsync(() => p1.Naks.Count == 1);
        }
        finally
        {
            await subscriber.StopAsync(CancellationToken.None).WaitAsync(HangGuard);
            subscriber.Dispose();
        }

        Assert.Equal([TimeSpan.Zero], p1.Naks);
        Assert.Equal(0, p1.Acks);
        Assert.Equal(0, ingress.WorkerCount);
        Assert.Equal(1, jetStream.NoWaitFetches); // parked after the hand-back: nothing more fetched
    }

    /// <summary>
    /// A stop that cuts an early-ACK batch short hands back its unstarted tail; a hand-back whose
    /// NAK fails is only logged (AckWait redelivers it anyway), and the heartbeat that was still
    /// renewing the parked message exits between messages once the batch lets go.
    /// </summary>
    [Fact]
    public async Task WorkerSubscriber_StopMidBatch_AFailedHandBackIsLogged_AndTheHeartbeatExitsBetweenMessages()
    {
        var jetStream = new FakeNatsJetStreamTransport();
        var ingress = new FirstGatedIngress();
        var logger = new RecordingThrowingLogger<NatsWorkerSubscriber>();
        var p3ProgressEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var deliveries = new[]
        {
            new RecordingDelivery(),
            new RecordingDelivery(),
            new RecordingDelivery
            {
                // Holds the renewal sweep on the parked p3 until the batch lets go, then returns
                // normally, so the sweep's next step is the between-messages exit.
                ProgressBehavior = async token =>
                {
                    p3ProgressEntered.TrySetResult();
                    var cancelled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                    using (token.Register(() => cancelled.TrySetResult()))
                        await cancelled.Task.ConfigureAwait(false);
                }
            },
            new RecordingDelivery { NakException = new InvalidOperationException("NAK refused") }
        };
        for (var i = 0; i < deliveries.Length; i++)
            jetStream.EnqueueDelivery(deliveries[i].Create($"p{i + 1}", numDelivered: 1));
        var subscriber = new NatsWorkerSubscriber(
            SubscriberOptions(o =>
            {
                o.AckWait = TimeSpan.FromMilliseconds(150); // heartbeat every 50 ms
                o.WorkerSubscriber.UseAckAfterEnqueue(backgroundWorkerCount: 1, backgroundQueueCapacity: 1, TimeSpan.FromSeconds(5));
                o.WorkerSubscriber.BatchSize = 4;
            }),
            jetStream,
            ingress,
            logger);

        await subscriber.StartAsync(CancellationToken.None);
        Task? stop = null;
        try
        {
            await ingress.Started.Task.WaitAsync(HangGuard);     // p1 holds the only worker
            await WaitUntilAsync(() => deliveries[1].Acks == 1); // p2 fills the queue; p3 parks the loop
            await p3ProgressEntered.Task.WaitAsync(HangGuard);   // the heartbeat is renewing p3

            stop = subscriber.StopAsync(CancellationToken.None);
            await WaitUntilAsync(() => deliveries[3].Naks.Count == 1);
        }
        finally
        {
            ingress.Release.TrySetResult();
            await (stop ?? subscriber.StopAsync(CancellationToken.None)).WaitAsync(HangGuard);
            subscriber.Dispose();
        }

        Assert.Equal([TimeSpan.Zero], deliveries[3].Naks);
        Assert.Equal(0, deliveries[3].Acks);
        Assert.Equal(0, deliveries[3].Progresses); // the sweep exited before reaching p4
        Assert.True(logger.HasEntry(LogLevel.Debug, "Failed to hand back an unstarted NATS message"));
        Assert.False(logger.HasEntry(LogLevel.Warning, "did not stop within")); // the sweep ended promptly
        Assert.DoesNotContain("p4", ingress.Received);
    }

    /// <summary>
    /// A throwing logger provider must not fault the in-progress heartbeat: one failed renewal
    /// whose warning throws used to end the loop, so the rest of the batch stopped renewing and
    /// its AckWait lapsed under the live handler. Deterministic: the heartbeat runs on the
    /// injected clock, and each tick is advanced only once its timer is armed.
    /// </summary>
    [Fact]
    public async Task WorkerSubscriber_AThrowingLoggerOnAFailedRenewal_DoesNotStopTheHeartbeat()
    {
        var clock = new AsyncResponse.Testing.VirtualTimeProvider();
        var interval = TimeSpan.FromSeconds(1); // AckWait 3 s / 3
        var jetStream = new FakeNatsJetStreamTransport();
        var ingress = new FirstGatedIngress();
        var first = new RecordingDelivery { ProgressException = new InvalidOperationException("in-progress refused") };
        jetStream.EnqueueDelivery(first.Create("p1", numDelivered: 1));
        var logger = new RecordingThrowingLogger<NatsWorkerSubscriber> { ThrowOnMessageContaining = "Failed to signal in-progress" };
        var subscriber = new NatsWorkerSubscriber(
            SubscriberOptions(o => o.AckWait = TimeSpan.FromSeconds(3)),
            jetStream,
            ingress,
            logger,
            clock);

        await subscriber.StartAsync(CancellationToken.None);
        try
        {
            await ingress.Started.Task.WaitAsync(HangGuard); // p1 is wedged in the handler

            await WaitUntilAsync(() => clock.NextTimerDueAt == clock.GetUtcNow() + interval);
            clock.Advance(interval);
            await WaitUntilAsync(() => first.Progresses == 1); // refused, and its warning threw

            // The loop survived the unloggable failure: it re-arms and renews on the next tick.
            await WaitUntilAsync(() => clock.NextTimerDueAt == clock.GetUtcNow() + interval);
            clock.Advance(interval);
            await WaitUntilAsync(() => first.Progresses == 2);
            Assert.Equal(0, first.Acks);
        }
        finally
        {
            ingress.Release.TrySetResult();
            await subscriber.StopAsync(CancellationToken.None).WaitAsync(HangGuard);
            subscriber.Dispose();
        }
    }

    /// <summary>
    /// A heartbeat wedged past its join bound (the client ignores cancellation) is abandoned with a
    /// warning and the subscriber moves on to the next batch; when the wedged renewal later fails
    /// — and its warning throws — the abandoned loop absorbs it instead of faulting, so the
    /// "Abandoned NATS in-progress heartbeat faulted." backstop has nothing to report.
    /// Deterministic: the heartbeat and the join bound run on the injected clock.
    /// </summary>
    [Fact]
    public async Task WorkerSubscriber_AnAbandonedHeartbeatThatFailsLater_IsAbsorbed_AndTheSubscriberMovesOn()
    {
        var clock = new AsyncResponse.Testing.VirtualTimeProvider();
        var interval = TimeSpan.FromSeconds(1); // AckWait 3 s / 3
        var jetStream = new FakeNatsJetStreamTransport();
        var ingress = new FirstGatedIngress();
        var wedge = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var lateFailureThrown = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var first = new RecordingDelivery
        {
            // Ignores its token, then fails.
            ProgressBehavior = async _ =>
            {
                await wedge.Task.ConfigureAwait(false);
                lateFailureThrown.TrySetResult();
                throw new InvalidOperationException("in-progress refused");
            }
        };
        var second = new RecordingDelivery();
        jetStream.EnqueueDelivery(first.Create("p1", numDelivered: 1));
        var logger = new RecordingThrowingLogger<NatsWorkerSubscriber>();
        var subscriber = new NatsWorkerSubscriber(
            SubscriberOptions(o => o.AckWait = TimeSpan.FromSeconds(3)),
            jetStream,
            ingress,
            logger,
            clock);

        await subscriber.StartAsync(CancellationToken.None);
        try
        {
            await ingress.Started.Task.WaitAsync(HangGuard);
            await WaitUntilAsync(() => clock.NextTimerDueAt == clock.GetUtcNow() + interval);
            clock.Advance(interval);
            await WaitUntilAsync(() => first.Progresses == 1); // the heartbeat is now wedged

            ingress.Release.TrySetResult();
            await WaitUntilAsync(() => first.Acks == 1);
            // The batch's join waits one interval for the wedged heartbeat, then abandons it.
            await WaitUntilAsync(() => clock.NextTimerDueAt == clock.GetUtcNow() + interval);
            clock.Advance(interval);
            await WaitUntilAsync(() => logger.HasEntry(LogLevel.Warning, "did not stop within"));

            jetStream.EnqueueDelivery(second.Create("p2", numDelivered: 1));
            await WaitUntilAsync(() => second.Acks == 1); // the subscriber moved on

            logger.ThrowOnMessageContaining = "Failed to signal in-progress";
            wedge.TrySetResult();
            await lateFailureThrown.Task.WaitAsync(HangGuard);
        }
        finally
        {
            wedge.TrySetResult();
            ingress.Release.TrySetResult();
            await subscriber.StopAsync(CancellationToken.None).WaitAsync(HangGuard);
            subscriber.Dispose();
        }

        Assert.False(logger.HasEntry(LogLevel.Warning, "Abandoned NATS in-progress heartbeat faulted."));
        Assert.Equal(1, first.Acks);
    }

    // ---------------------------------------------------------------- JetStream adapter

    [Fact]
    public async Task EnsureStream_CreationRaceLostToAPeer_VerifiesThePeersStream()
    {
        var jetStream = new Mock<INatsJSContext>();
        var peer = new Mock<INatsJSStream>();
        peer.SetupGet(s => s.Info).Returns(new StreamInfo
        {
            Config = new StreamConfig("stream", ["subj"])
            {
                Retention = StreamConfigRetention.Workqueue,
                Discard = StreamConfigDiscard.New,
                MaxMsgs = -1
            }
        });
        jetStream.SetupSequence(c => c.GetStreamAsync("stream", It.IsAny<StreamInfoRequest?>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(StreamNotFound())
            .ReturnsAsync(peer.Object);
        jetStream.Setup(c => c.CreateStreamAsync(It.IsAny<StreamConfig>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(StreamNameInUse());
        var adapter = new NatsJetStreamTransportAdapter(jetStream.Object);

        await adapter.EnsureStreamAsync("stream", "subj", null, CancellationToken.None);

        jetStream.Verify(c => c.GetStreamAsync("stream", It.IsAny<StreamInfoRequest?>(), It.IsAny<CancellationToken>()), Times.Exactly(2));
        jetStream.Verify(c => c.CreateOrUpdateStreamAsync(It.IsAny<StreamConfig>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task EnsureStream_NameInUseButStillNotFound_RethrowsTheCreationFailure()
    {
        var jetStream = new Mock<INatsJSContext>();
        jetStream.Setup(c => c.GetStreamAsync("stream", It.IsAny<StreamInfoRequest?>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(StreamNotFound());
        var rejection = StreamNameInUse();
        jetStream.Setup(c => c.CreateStreamAsync(It.IsAny<StreamConfig>(), It.IsAny<CancellationToken>())).ThrowsAsync(rejection);
        var adapter = new NatsJetStreamTransportAdapter(jetStream.Object);

        var thrown = await Assert.ThrowsAsync<NatsJSApiException>(() => adapter.EnsureStreamAsync("stream", "subj", null, CancellationToken.None));

        Assert.Same(rejection, thrown);
    }

    public static TheoryData<List<string>?, string> StreamsNotCapturingTheSubject => new()
    {
        { null, "(it captures: none)" },
        { new List<string>(), "(it captures: none)" },
        { new List<string> { "other.a", "other.>" }, "(it captures: other.a, other.>)" }
    };

    [Theory]
    [MemberData(nameof(StreamsNotCapturingTheSubject))]
    public async Task EnsureStream_ExistingStreamNotCapturingTheSubject_Throws(List<string>? subjects, string expected)
    {
        var adapter = new NatsJetStreamTransportAdapter(ExistingStream(new StreamConfig { Name = "stream", Subjects = subjects, Retention = StreamConfigRetention.Workqueue }).Object);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => adapter.EnsureStreamAsync("stream", "work.subj", null, CancellationToken.None));

        Assert.Contains("does not capture subject 'work.subj'", ex.Message, StringComparison.Ordinal);
        Assert.Contains(expected, ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task EnsureStream_ExistingWorkStreamWithLimitsRetention_Throws()
    {
        var adapter = new NatsJetStreamTransportAdapter(ExistingStream(new StreamConfig("stream", ["subj"])
        {
            Retention = StreamConfigRetention.Limits,
            Discard = StreamConfigDiscard.New
        }).Object);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => adapter.EnsureStreamAsync("stream", "subj", null, CancellationToken.None));

        Assert.Contains("retention", ex.Message, StringComparison.Ordinal);
        Assert.Contains("does not allow changing the retention policy", ex.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("work.>", "work", false)]          // '>' needs at least one more token
    [InlineData("work.>", "work.a.b", true)]
    [InlineData("work.a.b", "work.a", false)]      // the subject runs out before the pattern
    [InlineData("work.*", "work.a", true)]
    [InlineData("work.*", "work.a.b", false)]
    public void SubjectCaptures_FollowsNatsWildcardRules(string captured, string subject, bool expected)
        => Assert.Equal(expected, NatsJetStreamTransportAdapter.SubjectCaptures(captured, subject));

    [Fact]
    public async Task EnsureConsumer_CreationFailsAndTheConsumerIsStillMissing_Rethrows()
    {
        var jetStream = new Mock<INatsJSContext>();
        jetStream.Setup(c => c.GetConsumerAsync("stream", "durable", It.IsAny<CancellationToken>()))
            .ThrowsAsync(new NatsJSApiException(new ApiError { Code = 404, ErrCode = 10014, Description = "consumer not found" }));
        var rejection = new NatsJSApiException(new ApiError { Code = 400, ErrCode = 10012, Description = "insufficient resources" });
        jetStream.Setup(c => c.CreateConsumerAsync("stream", It.IsAny<ConsumerConfig>(), It.IsAny<CancellationToken>())).ThrowsAsync(rejection);
        var adapter = new NatsJetStreamTransportAdapter(jetStream.Object);

        var thrown = await Assert.ThrowsAsync<NatsJSApiException>(() =>
            adapter.EnsureConsumerAsync("stream", "subj", "durable", TimeSpan.FromSeconds(30), 5, CancellationToken.None));

        Assert.Same(rejection, thrown);
        jetStream.Verify(c => c.GetConsumerAsync("stream", "durable", It.IsAny<CancellationToken>()), Times.Exactly(2));
    }

    // ---------------------------------------------------------------- helpers

    private static NatsJSApiException StreamNotFound()
        => new(new ApiError { Code = 404, ErrCode = 10059, Description = "stream not found" });

    private static NatsJSApiException StreamNameInUse()
        => new(new ApiError { Code = 400, ErrCode = 10058, Description = "stream name already in use with a different configuration" });

    private static Mock<INatsJSContext> ExistingStream(StreamConfig config)
    {
        var jetStream = new Mock<INatsJSContext>();
        var stream = new Mock<INatsJSStream>();
        stream.SetupGet(s => s.Info).Returns(new StreamInfo { Config = config });
        jetStream.Setup(c => c.GetStreamAsync(It.IsAny<string>(), It.IsAny<StreamInfoRequest?>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(stream.Object);
        return jetStream;
    }

    private static NatsMessageDispatcher CreateDispatcher(
        INatsJetStreamTransport jetStream,
        Func<NatsJobDelivery, CancellationToken, Task> handler,
        NatsSubscriberOptions subscriber,
        NatsAsyncResponseTransportOptions? options = null,
        ILogger? logger = null)
    {
        options ??= new NatsAsyncResponseTransportOptions();
        return new NatsMessageDispatcher(
            handler,
            jetStream,
            options,
            subscriber,
            new NatsTransportSubjectSchema(options),
            logger ?? new TestLogger(),
            NatsSubscriberRole.Worker,
            "test-consumer");
    }

    private static Microsoft.Extensions.Options.IOptions<NatsAsyncResponseTransportOptions> SubscriberOptions(Action<NatsAsyncResponseTransportOptions> configure)
    {
        var options = new NatsAsyncResponseTransportOptions
        {
            SubscriberRetryBaseDelay = TimeSpan.FromMilliseconds(1),
            SubscriberRetryMaxDelay = TimeSpan.FromMilliseconds(5)
        };
        configure(options);
        return Microsoft.Extensions.Options.Options.Create(options);
    }

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow + HangGuard;
        while (!condition())
        {
            if (DateTime.UtcNow >= deadline)
                throw new TimeoutException("Condition was not satisfied before the timeout.");

            await Task.Delay(10);
        }
    }

    /// <summary>A logger that runs a hook on every message (the hook may throw, as a provider can).</summary>
    private sealed class HookLogger : ILogger
    {
        private readonly List<(LogLevel Level, string Message)> _entries = [];

        public Action<string>? OnLog { get; init; }

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            var message = formatter(state, exception);
            lock (_entries)
                _entries.Add((logLevel, message));
            OnLog?.Invoke(message);
        }

        public bool Has(LogLevel level, string fragment)
        {
            lock (_entries)
                return _entries.Exists(entry => entry.Level == level && entry.Message.Contains(fragment, StringComparison.Ordinal));
        }
    }

    /// <summary>A host lifetime whose ApplicationStopping the test fires.</summary>
    private sealed class StoppingHostLifetime : IHostApplicationLifetime, IDisposable
    {
        private readonly CancellationTokenSource _stopping = new();

        public CancellationToken ApplicationStarted => CancellationToken.None;
        public CancellationToken ApplicationStopping => _stopping.Token;
        public CancellationToken ApplicationStopped => CancellationToken.None;
        public void StopApplication() => _stopping.Cancel();
        public void Dispose() => _stopping.Dispose();
    }

    /// <summary>Wedges the FIRST worker message in the handler until released; later ones pass through.</summary>
    private sealed class FirstGatedIngress : IAsyncResponseIngress
    {
        private int _calls;

        public TaskCompletionSource Started { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public System.Collections.Concurrent.ConcurrentQueue<string> Received { get; } = new();

        public async Task HandleWorkerMessageAsync(string messageJson)
        {
            Received.Enqueue(messageJson);
            if (Interlocked.Increment(ref _calls) == 1)
            {
                Started.TrySetResult();
                await Release.Task;
            }
        }

        public Task HandleResponseMessageAsync(string messageJson, string? correlationId = null) => Task.CompletedTask;
    }

    /// <summary>
    /// Implements only the required members (so the fetch defaults run), recording publishes and
    /// awaiting <see cref="PublishGate"/> with each publish's token first.
    /// </summary>
    private sealed class MinimalJetStream : INatsJetStreamTransport
    {
        private readonly List<(string Subject, string Payload)> _published = [];

        public Func<CancellationToken, Task>? PublishGate { get; init; }

        public IReadOnlyList<(string Subject, string Payload)> Published
        {
            get
            {
                lock (_published)
                    return _published.ToArray();
            }
        }

        public Task EnsureStreamAsync(string stream, string subject, long? maxMessages, CancellationToken cancellationToken) => Task.CompletedTask;

        public Task EnsureDeadLetterStreamAsync(string stream, string subject, long? maxMessages, CancellationToken cancellationToken) => Task.CompletedTask;

        public Task<TimeSpan> EnsureConsumerAsync(string stream, string subject, string durable, TimeSpan ackWait, int maxDeliveryAttempts, CancellationToken cancellationToken)
            => Task.FromResult(ackWait);

        public async Task<string> PublishAsync(string subject, string payload, IReadOnlyDictionary<string, string>? headers, CancellationToken cancellationToken)
        {
            if (PublishGate is { } gate)
                await gate(cancellationToken).ConfigureAwait(false);

            lock (_published)
            {
                _published.Add((subject, payload));
                return _published.Count.ToString(System.Globalization.CultureInfo.InvariantCulture);
            }
        }
    }

    /// <summary>
    /// The first no-wait fetch yields <paramref name="first"/> and then waits for its token (the
    /// worker's intake gate, at host stop) mid-fetch; later fetches are empty.
    /// </summary>
    private sealed class ScriptedFetchJetStream(NatsJobDelivery first) : INatsJetStreamTransport
    {
        private int _noWaitFetches;

        public TaskCompletionSource FirstDelivered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public int NoWaitFetches => Volatile.Read(ref _noWaitFetches);

        public Task EnsureStreamAsync(string stream, string subject, long? maxMessages, CancellationToken cancellationToken) => Task.CompletedTask;

        public Task EnsureDeadLetterStreamAsync(string stream, string subject, long? maxMessages, CancellationToken cancellationToken) => Task.CompletedTask;

        public Task<TimeSpan> EnsureConsumerAsync(string stream, string subject, string durable, TimeSpan ackWait, int maxDeliveryAttempts, CancellationToken cancellationToken)
            => Task.FromResult(ackWait);

        public Task<string> PublishAsync(string subject, string payload, IReadOnlyDictionary<string, string>? headers, CancellationToken cancellationToken)
            => Task.FromResult("1");

        public async IAsyncEnumerable<NatsJobDelivery> FetchNoWaitAsync(string stream, string durable, int maxMessages, [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            if (Interlocked.Increment(ref _noWaitFetches) != 1)
                yield break;

            yield return first;
            FirstDelivered.TrySetResult();
            await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken).ConfigureAwait(false);
        }

        public async IAsyncEnumerable<NatsJobDelivery> FetchAsync(string stream, string durable, int maxMessages, TimeSpan expires, [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken).ConfigureAwait(false);
            yield break;
        }
    }
}
