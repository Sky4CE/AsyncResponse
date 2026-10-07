using AsyncResponse.Transports.Redis;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using StackExchange.Redis;
using System.Reflection;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Coverage-gap tests for the Redis transport: the early-ACK dispatcher's stop-time reserve and
/// post-ACK burial arms, the subscriber's best-effort failure arms (consumer retirement, tombstone
/// ACK, claim heartbeat), the validator's distinct-stream rule and the stream-adapter interface's
/// default members.
/// </summary>
public sealed class RedisTransportCoverageGapTests
{
    private static readonly TimeSpan HangGuard = TimeSpan.FromSeconds(10);

    // ---------------------------------------------------------------- validator

    [Fact]
    public void ValidateCommon_WorkerAndResponseStreamsResolvingToTheSameName_Throws()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            RedisTransportOptionsValidator.ValidateCommon(new RedisAsyncResponseTransportOptions
            {
                WorkerStream = "shared",
                ResponseStream = "shared"
            }));

        Assert.Contains("must resolve to distinct streams", ex.Message, StringComparison.Ordinal);
        Assert.Contains(nameof(RedisAsyncResponseTransportOptions.WorkerStream), ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateCommon_AnExplicitWorkerStreamCollidingWithTheDerivedResponseDefault_Throws()
    {
        // The comparison is on RESOLVED names: an explicit value equal to the other role's
        // derived default collides just the same.
        var options = new RedisAsyncResponseTransportOptions();
        options.WorkerStream = $"{options.KeyPrefix}:transport:response";

        var ex = Assert.Throws<InvalidOperationException>(() => RedisTransportOptionsValidator.ValidateCommon(options));

        Assert.Contains("must resolve to distinct streams", ex.Message, StringComparison.Ordinal);
    }

    // ---------------------------------------------------------------- IRedisStreamDatabase defaults

    [Fact]
    public async Task StreamDatabaseDefaults_ClaimNothing_AppendNonIdempotently_AndDeleteNoConsumer()
    {
        var database = new DefaultsOnlyStreamDatabase();
        IRedisStreamDatabase seam = database;

        var claimed = await seam.StreamClaimIdsOnlyAsync("s", "g", "c", 0, ["1-0", "2-0"], CancellationToken.None);
        Assert.Empty(claimed);

        // The default idempotent append is a plain pass-through: it ignores the dedup key and the
        // capacity (a capacity is a refusal, never a length trim) and forwards the append.
        var id = await seam.StreamAddOnceAsync(
            "stream",
            "dedup-key",
            TimeSpan.FromMinutes(1),
            [new NameValueEntry("payload", "p")],
            maxLength: 42,
            settlingGroup: "workers",
            CancellationToken.None);
        Assert.Equal("1-0", id.ToString());
        var add = Assert.Single(database.Adds);
        Assert.Equal("stream", add.Stream);
        Assert.Null(add.MaxLength);
        Assert.False(add.Approximate);

        Assert.False(await seam.TryDeleteIdleConsumerAsync("s", "g", "c", CancellationToken.None));
    }

    // ---------------------------------------------------------------- queued dispatcher

    /// <summary>
    /// A background failure's dead-letter copy is written, then the entry is XACKed again; when
    /// that re-ACK fails the burial still counts (the copy exists), the failure is a Warning, and
    /// the OnBackgroundFailure report carries the delivery's identity.
    /// </summary>
    [Fact]
    public async Task Queued_BackgroundFailure_WhenTheReAckAfterDeadLetteringFails_WarnsAndStillReports()
    {
        var database = new RedisTransportTests.FakeRedisStreamDatabase
        {
            // Fails the enqueue-time ACK (swallowed there) and the post-burial re-ACK alike.
            AckException = new InvalidOperationException("XACK refused")
        };
        var logger = new RecordingThrowingLogger<RedisTransportCoverageGapTests>();
        var reported = new TaskCompletionSource<RedisBackgroundFailureContext>(TaskCreationOptions.RunContinuationsAsynchronously);
        var options = new RedisSubscriberOptions().UseAckAfterEnqueue(1, 8, TimeSpan.FromSeconds(5));
        options.OnBackgroundFailure = context =>
        {
            reported.TrySetResult(context);
            return ValueTask.CompletedTask;
        };
        await using var dispatcher = RedisMessageDispatcher.Create(
            (_, _) => throw new InvalidOperationException("handler boom"),
            database,
            new RedisAsyncResponseTransportOptions { DeadLetterStream = "dead" },
            options,
            logger,
            "worker-stream",
            "worker-group",
            RedisSubscriberRole.Worker);

        Assert.Equal(RedisDispatchOutcome.Processed, await dispatcher.HandleAsync(Delivery("1-0"), CancellationToken.None));

        var context = await reported.Task.WaitAsync(HangGuard);
        Assert.Equal("worker-stream", context.Stream);
        Assert.Equal("worker-group", context.ConsumerGroup);
        Assert.Equal(nameof(RedisSubscriberRole.Worker), context.SubscriberRole);
        Assert.Equal("1-0", context.MessageId);
        Assert.Equal("corr", context.CorrelationId);
        Assert.Equal("handler boom", context.Exception.Message);

        var dead = Assert.Single(database.Adds);
        Assert.Equal("dead", dead.Stream);
        Assert.Equal("background_handler_failed_after_ack", RedisTransportTests.Field(dead.Values, "reason"));
        Assert.True(logger.HasEntry(LogLevel.Warning, "Failed to re-ACK already-ACKed Redis message 1-0"));
        // The copy exists, so no "no dead-letter copy" loss is claimed.
        Assert.False(logger.HasEntry(LogLevel.Error, "No dead-letter copy"));
    }

    /// <summary>
    /// The stop-time reserve waits for an asynchronous OnBackgroundFailure report within itself; a
    /// report that finishes inside the reserve is simply awaited to completion.
    /// </summary>
    [Fact]
    public async Task Queued_DrainLapse_AnAsyncReportThatFinishesInsideTheReserve_IsAwaited()
    {
        var database = new RedisTransportTests.FakeRedisStreamDatabase();
        var firstStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var completedReports = new List<string>();
        var logger = new RecordingThrowingLogger<RedisTransportCoverageGapTests>();
        // 2 s drain: 1.5 s drain budget, 0.5 s reserve for one synchronous burial and one
        // yield-then-complete report.
        var options = new RedisSubscriberOptions().UseAckAfterEnqueue(1, 8, TimeSpan.FromSeconds(2));
        options.OnBackgroundFailure = async context =>
        {
            await Task.Yield(); // never completes synchronously: the reserve must wait for it
            lock (completedReports)
                completedReports.Add(context.MessageId);
        };
        var dispatcher = RedisMessageDispatcher.Create(
            async (_, _) =>
            {
                firstStarted.TrySetResult();
                await releaseFirst.Task.ConfigureAwait(false);
            },
            database,
            new RedisAsyncResponseTransportOptions { DeadLetterStream = "dead" },
            options,
            logger,
            "worker-stream",
            "worker-group",
            RedisSubscriberRole.Worker);

        try
        {
            await dispatcher.HandleAsync(Delivery("1-0"), CancellationToken.None);
            await firstStarted.Task.WaitAsync(HangGuard);
            await dispatcher.HandleAsync(Delivery("2-0"), CancellationToken.None);

            await dispatcher.DisposeAsync().AsTask().WaitAsync(HangGuard);

            var dead = Assert.Single(database.Adds);
            Assert.Equal("2-0", RedisTransportTests.Field(dead.Values, "messageId"));
            lock (completedReports)
                Assert.Equal(["2-0"], completedReports);
            Assert.False(logger.HasEntry(LogLevel.Warning, "did not finish within the stop-time reserve"));
        }
        finally
        {
            releaseFirst.TrySetResult();
        }
    }

    /// <summary>
    /// A report that outlives the stop-time reserve is abandoned (Warning), and when it faults
    /// later, the fault is still logged by the continuation left behind.
    /// </summary>
    [Fact]
    public async Task Queued_DrainLapse_AReportThatFaultsAfterTheReserve_IsStillLogged()
    {
        var database = new RedisTransportTests.FakeRedisStreamDatabase();
        var firstStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var callback = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var logger = new RecordingThrowingLogger<RedisTransportCoverageGapTests>();
        var options = new RedisSubscriberOptions().UseAckAfterEnqueue(1, 8, TimeSpan.FromMilliseconds(200));
        options.OnBackgroundFailure = _ => new ValueTask(callback.Task);
        var dispatcher = RedisMessageDispatcher.Create(
            async (_, _) =>
            {
                firstStarted.TrySetResult();
                await releaseFirst.Task.ConfigureAwait(false);
            },
            database,
            new RedisAsyncResponseTransportOptions { DeadLetterStream = "dead" },
            options,
            logger,
            "worker-stream",
            "worker-group",
            RedisSubscriberRole.Worker);

        try
        {
            await dispatcher.HandleAsync(Delivery("1-0"), CancellationToken.None);
            await firstStarted.Task.WaitAsync(HangGuard);
            await dispatcher.HandleAsync(Delivery("2-0"), CancellationToken.None);

            // The report never finishes inside the reserve: DisposeAsync returns regardless.
            await dispatcher.DisposeAsync().AsTask().WaitAsync(HangGuard);
            Assert.True(logger.HasEntry(LogLevel.Warning, "did not finish within the stop-time reserve"));
            Assert.False(logger.HasEntry(LogLevel.Error, "Redis background failure callback failed"));

            callback.TrySetException(new InvalidOperationException("alert endpoint down"));

            await WaitUntilAsync(() => logger.HasEntry(LogLevel.Error, "Redis background failure callback failed for already-ACKed message 2-0"));
        }
        finally
        {
            releaseFirst.TrySetResult();
        }
    }

    /// <summary>
    /// A dead-letter XADD that hangs past the reserve leaves entries still queued: the reserve
    /// reports how many were left (Error). A worker that comes free afterwards does not run them —
    /// it routes each through the same dead-letter + OnBackgroundFailure path.
    /// </summary>
    [Fact]
    public async Task Queued_ReserveLapsingWithEntriesStillQueued_ReportsTheLeftover_AndAFreedWorkerRoutesThem()
    {
        var database = new ScriptedStreamDatabase
        {
            // The reserve's burial passes the reserve token: hang until it lapses. A worker's
            // routing passes CancellationToken.None and is served at once.
            AddGate = static token => token.CanBeCanceled ? Task.Delay(Timeout.InfiniteTimeSpan, token) : Task.CompletedTask
        };
        var firstStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var reports = new List<RedisBackgroundFailureContext>();
        var handlerRuns = 0;
        var logger = new RecordingThrowingLogger<RedisTransportCoverageGapTests>();
        var options = new RedisSubscriberOptions().UseAckAfterEnqueue(1, 8, TimeSpan.FromMilliseconds(400));
        options.OnBackgroundFailure = context =>
        {
            lock (reports)
                reports.Add(context);
            return ValueTask.CompletedTask;
        };
        var dispatcher = RedisMessageDispatcher.Create(
            async (_, _) =>
            {
                Interlocked.Increment(ref handlerRuns);
                firstStarted.TrySetResult();
                await releaseFirst.Task.ConfigureAwait(false);
            },
            database,
            new RedisAsyncResponseTransportOptions { DeadLetterStream = "dead" },
            options,
            logger,
            "worker-stream",
            "worker-group",
            RedisSubscriberRole.Worker);

        try
        {
            await dispatcher.HandleAsync(Delivery("1-0"), CancellationToken.None);
            await firstStarted.Task.WaitAsync(HangGuard);
            await dispatcher.HandleAsync(Delivery("2-0"), CancellationToken.None);
            await dispatcher.HandleAsync(Delivery("3-0"), CancellationToken.None);

            await dispatcher.DisposeAsync().AsTask().WaitAsync(HangGuard);

            Assert.True(logger.HasEntry(LogLevel.Error, "already-ACKed Redis message(s) on worker-stream were still queued when the reserved"));
            Assert.Empty(database.Adds); // the reserve's only XADD hung until it lapsed
        }
        finally
        {
            releaseFirst.TrySetResult();
        }

        // The freed worker routes 3-0 (never executing it): copy first, then the report.
        await WaitUntilAsync(() =>
        {
            lock (reports)
                return reports.Any(report => report.MessageId == "3-0");
        });

        Assert.Equal(1, Volatile.Read(ref handlerRuns));
        var copy = Assert.Single(database.Adds, add => RedisTransportTests.Field(add, "messageId") == "3-0");
        Assert.Equal("drain_budget_lapsed_after_ack", RedisTransportTests.Field(copy, "reason"));
        lock (reports)
        {
            var report = Assert.Single(reports, report => report.MessageId == "3-0");
            Assert.IsAssignableFrom<OperationCanceledException>(report.Exception);
        }
    }

    // ---------------------------------------------------------------- subscriber

    [Fact]
    public async Task Subscriber_WhenRetiringItsGeneratedConsumerFails_LogsAndStillStops()
    {
        var database = new ScriptedStreamDatabase
        {
            DeleteConsumerException = new InvalidOperationException("DELCONSUMER refused")
        };
        var logger = new RecordingThrowingLogger<RedisWorkerSubscriber>();
        var subscriber = WorkerSubscriber(database, new BlockingIngress(), _ => { }, logger);

        await subscriber.StartAsync(CancellationToken.None);
        await WaitUntilAsync(() => database.ReadCount >= 1);
        await subscriber.StopAsync(CancellationToken.None).WaitAsync(HangGuard);

        Assert.Equal(1, database.DeleteConsumerCalls);
        Assert.True(logger.HasEntry(LogLevel.Debug, "Could not delete Redis consumer"));
        Assert.False(logger.HasEntry(LogLevel.Debug, "Deleted Redis consumer"));
    }

    /// <summary>
    /// A trimmed-while-pending tombstone whose ACK fails is left for the next claim cycle (a
    /// Warning), and the live entries of the same claim are still dispatched.
    /// </summary>
    [Fact]
    public async Task Subscriber_TombstoneWhoseAckFails_IsLoggedAndTheLiveEntryIsStillDispatched()
    {
        var database = new ScriptedStreamDatabase
        {
            AckFault = id => id == "1-0" ? new InvalidOperationException("XACK refused") : null
        };
        database.AddPending("1-0", StreamEntry.Null);
        database.AddPending("2-0", RedisTransportTests.Entry("2-0", ("payload", "p2"), ("correlationId", "c2")));
        var ingress = new BlockingIngress();
        var logger = new RecordingThrowingLogger<RedisWorkerSubscriber>();
        // Early ACK claims the whole candidate list in ONE positional XCLAIM.
        var subscriber = WorkerSubscriber(database, ingress, options => options.UseAckAfterEnqueue(1, 8, TimeSpan.FromSeconds(5)), logger);

        await subscriber.StartAsync(CancellationToken.None);
        try
        {
            await WaitUntilAsync(() => ingress.Handled.Contains("p2"));
        }
        finally
        {
            await subscriber.StopAsync(CancellationToken.None).WaitAsync(HangGuard);
        }

        Assert.True(logger.HasEntry(LogLevel.Warning, "Failed to ACK trimmed pending entry 1-0"));
        Assert.False(logger.HasEntry(LogLevel.Warning, "ACKed the tombstone so it drains"));
        Assert.DoesNotContain("1-0", database.Acks);
        Assert.Contains("2-0", database.Acks);
        Assert.Empty(database.Adds); // neither entry was dead-lettered
    }

    /// <summary>
    /// The in-flight claim heartbeat survives a failing XCLAIM JUSTID: it logs a Warning and keeps
    /// the batch going; the entry in the handler still completes and is ACKed.
    /// </summary>
    [Fact]
    public async Task Subscriber_HeartbeatClaimFailure_IsLoggedAndTheBatchCompletes()
    {
        var database = new RedisTransportTests.FakeRedisStreamDatabase
        {
            ClaimIdsOnlyException = new InvalidOperationException("XCLAIM JUSTID refused")
        };
        database.ReadBatches.Enqueue([RedisTransportTests.Entry("1-0", ("payload", "p1"), ("correlationId", "c1"))]);
        var ingress = new BlockingIngress("p1");
        var logger = new RecordingThrowingLogger<RedisWorkerSubscriber>();
        var subscriber = WorkerSubscriber(
            database,
            ingress,
            options =>
            {
                // Heartbeat cadence = PendingMessageMinIdleTime / 3 = 10 ms.
                options.PendingMessageMinIdleTime = TimeSpan.FromMilliseconds(30);
                options.PendingClaimInterval = TimeSpan.FromSeconds(30);
            },
            logger);

        await subscriber.StartAsync(CancellationToken.None);
        try
        {
            await ingress.Started("p1").WaitAsync(HangGuard);
            await WaitUntilAsync(() => logger.HasEntry(LogLevel.Warning, "Failed to refresh the pending idle time of 1 Redis entries on workers"));
            ingress.Release("p1");
            await WaitUntilAsync(() => database.Acks.Any(ack => ack.MessageId == "1-0"));
        }
        finally
        {
            ingress.Release("p1");
            await subscriber.StopAsync(CancellationToken.None).WaitAsync(HangGuard);
        }

        Assert.Contains("p1", ingress.Handled);
    }

    // ------------------------------------------- subscriber under a throwing logging provider

    /// <summary>
    /// Red-on-old (R6-04): the "subscriber started" line ran unguarded before the first read, so a
    /// throwing logging provider failed every supervised attempt and nothing was ever consumed.
    /// </summary>
    [Fact]
    public async Task Subscriber_WithAThrowingStartedLog_StillConsumes()
    {
        var database = new RedisTransportTests.FakeRedisStreamDatabase();
        database.ReadBatches.Enqueue([RedisTransportTests.Entry("1-0", ("payload", "p1"), ("correlationId", "c1"))]);
        var ingress = new BlockingIngress();
        var logger = new CollectingLogger { ThrowOnMessageContaining = "Redis subscriber started" };
        var subscriber = WorkerSubscriber(database, ingress, _ => { }, logger.For<RedisWorkerSubscriber>());

        await subscriber.StartAsync(CancellationToken.None);
        try
        {
            await WaitUntilAsync(() => database.Acks.Any(ack => ack.MessageId == "1-0"));
        }
        finally
        {
            await subscriber.StopAsync(CancellationToken.None).WaitAsync(HangGuard);
        }

        Assert.Contains("p1", ingress.Handled);
        Assert.DoesNotContain(logger.Messages, message => message.Contains("Redis subscriber failed", StringComparison.Ordinal));
    }

    /// <summary>
    /// Red-on-old (R6-11): the heartbeat's failure warning ran unguarded, so a throwing provider
    /// ended the heartbeat and its join failed the attempt after the batch had been handled.
    /// The second read's entry is handled only after the first batch's teardown, so by then any
    /// attempt failure has been logged.
    /// </summary>
    [Fact]
    public async Task Subscriber_HeartbeatClaimFailure_UnderAThrowingLogger_DoesNotFailTheAttempt()
    {
        var database = new RedisTransportTests.FakeRedisStreamDatabase
        {
            ClaimIdsOnlyException = new InvalidOperationException("XCLAIM JUSTID refused")
        };
        database.ReadBatches.Enqueue([RedisTransportTests.Entry("1-0", ("payload", "p1"), ("correlationId", "c1"))]);
        database.ReadBatches.Enqueue([RedisTransportTests.Entry("2-0", ("payload", "p2"), ("correlationId", "c2"))]);
        var ingress = new BlockingIngress("p1");
        var logger = new CollectingLogger { ThrowOnMessageContaining = "Failed to refresh the pending idle time" };
        var subscriber = WorkerSubscriber(
            database,
            ingress,
            options =>
            {
                // Heartbeat cadence = PendingMessageMinIdleTime / 3 = 10 ms.
                options.PendingMessageMinIdleTime = TimeSpan.FromMilliseconds(30);
                options.PendingClaimInterval = TimeSpan.FromSeconds(30);
            },
            logger.For<RedisWorkerSubscriber>());

        await subscriber.StartAsync(CancellationToken.None);
        try
        {
            await ingress.Started("p1").WaitAsync(HangGuard);
            await logger.WaitForAsync("Failed to refresh the pending idle time");
            ingress.Release("p1");
            await WaitUntilAsync(() => database.Acks.Any(ack => ack.MessageId == "2-0"));
        }
        finally
        {
            ingress.Release("p1");
            await subscriber.StopAsync(CancellationToken.None).WaitAsync(HangGuard);
        }

        Assert.Contains(database.Acks, ack => ack.MessageId == "1-0");
        Assert.DoesNotContain(logger.Messages, message => message.Contains("Redis subscriber failed", StringComparison.Ordinal));
    }

    /// <summary>
    /// Red-on-old: the tombstone warnings ran unguarded (the success line's throw landed in the
    /// catch, whose own line threw again), so the claim escaped before its live entry was
    /// dispatched and the attempt failed.
    /// </summary>
    [Fact]
    public async Task Subscriber_TombstoneAck_UnderAThrowingLogger_StillDispatchesTheLiveEntry()
    {
        var database = new ScriptedStreamDatabase();
        database.AddPending("1-0", StreamEntry.Null);
        database.AddPending("2-0", RedisTransportTests.Entry("2-0", ("payload", "p2"), ("correlationId", "c2")));
        var ingress = new BlockingIngress();
        var logger = new CollectingLogger { ThrowOnMessageContaining = "trimmed" };
        var subscriber = WorkerSubscriber(database, ingress, options => options.UseAckAfterEnqueue(1, 8, TimeSpan.FromSeconds(5)), logger.For<RedisWorkerSubscriber>());

        await subscriber.StartAsync(CancellationToken.None);
        try
        {
            await WaitUntilAsync(() => ingress.Handled.Contains("p2"));
        }
        finally
        {
            await subscriber.StopAsync(CancellationToken.None).WaitAsync(HangGuard);
        }

        Assert.Contains("1-0", database.Acks);
        Assert.Contains("2-0", database.Acks);
        Assert.DoesNotContain(logger.Messages, message => message.Contains("Redis subscriber failed", StringComparison.Ordinal));
    }

    /// <summary>
    /// Red-on-old: consumer retirement "never throws", yet its debug lines ran unguarded, so a
    /// throwing provider faulted the hosted service's execute task at stop.
    /// </summary>
    [Fact]
    public async Task Subscriber_RetiringItsConsumer_UnderAThrowingLogger_StopsCleanly()
    {
        var database = new ScriptedStreamDatabase
        {
            DeleteConsumerException = new InvalidOperationException("DELCONSUMER refused")
        };
        var logger = new CollectingLogger { ThrowOnMessageContaining = "Redis consumer" };
        var subscriber = WorkerSubscriber(database, new BlockingIngress(), _ => { }, logger.For<RedisWorkerSubscriber>());

        await subscriber.StartAsync(CancellationToken.None);
        await WaitUntilAsync(() => database.ReadCount >= 1);
        await subscriber.StopAsync(CancellationToken.None).WaitAsync(HangGuard);

        Assert.Equal(1, database.DeleteConsumerCalls);
        Assert.True(subscriber.ExecuteTask!.IsCompletedSuccessfully);
    }

    /// <summary>
    /// A response-ingress subscriber that does not override the inbound budget check treats every
    /// payload as within budget, so the correlation id is still extracted for it.
    /// </summary>
    [Fact]
    public async Task ResponseRoleSubscriber_WithoutABudgetOverride_ExtractsTheCorrelationId()
    {
        var database = new RedisTransportTests.FakeRedisStreamDatabase();
        database.ReadBatches.Enqueue([RedisTransportTests.Entry("1-0", ("payload", "response-json"), ("correlationId", "corr-7"))]);
        var delivered = new TaskCompletionSource<RedisStreamDelivery>(TaskCreationOptions.RunContinuationsAsynchronously);
        var options = new RedisAsyncResponseTransportOptions
        {
            SubscriberRetryBaseDelay = TimeSpan.FromMilliseconds(1),
            SubscriberRetryMaxDelay = TimeSpan.FromMilliseconds(1),
            ResponseSubscriber =
            {
                EmptyPollDelay = TimeSpan.FromMilliseconds(1),
                PendingClaimInterval = TimeSpan.FromSeconds(30)
            }
        };
        var subscriber = new ResponseRoleProbeSubscriber(Options.Create(options), database, delivery =>
        {
            delivered.TrySetResult(delivery);
            return Task.CompletedTask;
        });

        await subscriber.StartAsync(CancellationToken.None);
        try
        {
            var delivery = await delivered.Task.WaitAsync(HangGuard);
            Assert.Equal("corr-7", delivery.CorrelationId);
            Assert.Equal("response-json", delivery.Payload);
        }
        finally
        {
            await subscriber.StopAsync(CancellationToken.None).WaitAsync(HangGuard);
        }
    }

    // ---------------------------------------------------------------- helpers

    private static RedisStreamDelivery Delivery(string id)
        => new(
            "worker-stream",
            "worker-group",
            id,
            "payload-json",
            "corr",
            Attempt: 1,
            RedisTransportTests.Entry(id, ("payload", "payload-json"), ("correlationId", "corr")));

    private static RedisWorkerSubscriber WorkerSubscriber(
        IRedisStreamDatabase database,
        IAsyncResponseIngress ingress,
        Action<RedisSubscriberOptions> configure,
        ILogger<RedisWorkerSubscriber> logger)
    {
        var options = new RedisAsyncResponseTransportOptions
        {
            WorkerStream = "workers",
            WorkerConsumerGroup = "workers-group",
            SubscriberRetryBaseDelay = TimeSpan.FromMilliseconds(1),
            SubscriberRetryMaxDelay = TimeSpan.FromMilliseconds(1),
            WorkerSubscriber =
            {
                EmptyPollDelay = TimeSpan.FromMilliseconds(1),
                PendingClaimInterval = TimeSpan.FromSeconds(30),
                PendingMessageMinIdleTime = TimeSpan.FromSeconds(30)
            }
        };
        configure(options.WorkerSubscriber);
        return new RedisWorkerSubscriber(Options.Create(options), database, ingress, logger);
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

    /// <summary>A response-role subscriber that keeps the base (always-within) inbound budget.</summary>
    private sealed class ResponseRoleProbeSubscriber(
        IOptions<RedisAsyncResponseTransportOptions> options,
        IRedisStreamDatabase database,
        Func<RedisStreamDelivery, Task> handler)
        : RedisSubscriberService(options, database, NullLogger.Instance)
    {
        protected override RedisKey Stream => "responses";
        protected override RedisValue ConsumerGroup => "responses-group";
        protected override RedisSubscriberOptions SubscriberOptions => Options.ResponseSubscriber;
        protected override RedisSubscriberRole SubscriberRole => RedisSubscriberRole.ResponseIngress;

        protected override Task HandleMessageAsync(RedisStreamDelivery delivery, CancellationToken cancellationToken)
            => handler(delivery);
    }

    /// <summary>Records every worker payload it handles; the gated ones wait until released.</summary>
    private sealed class BlockingIngress(params string[] gated) : IAsyncResponseIngress
    {
        private readonly object _gate = new();
        private readonly HashSet<string> _gated = new(gated, StringComparer.Ordinal);
        private readonly Dictionary<string, TaskCompletionSource> _started = new(StringComparer.Ordinal);
        private readonly Dictionary<string, TaskCompletionSource> _release = new(StringComparer.Ordinal);
        private readonly List<string> _handled = [];

        public IReadOnlyList<string> Handled
        {
            get
            {
                lock (_gate)
                    return _handled.ToArray();
            }
        }

        public Task Started(string payload) => Source(_started, payload).Task;

        public void Release(string payload) => Source(_release, payload).TrySetResult();

        public async Task HandleWorkerMessageAsync(string messageJson)
        {
            Source(_started, messageJson).TrySetResult();
            if (_gated.Contains(messageJson))
                await Source(_release, messageJson).Task;

            lock (_gate)
                _handled.Add(messageJson);
        }

        public Task HandleResponseMessageAsync(string messageJson, string? correlationId) => Task.CompletedTask;

        private TaskCompletionSource Source(Dictionary<string, TaskCompletionSource> sources, string payload)
        {
            lock (_gate)
            {
                if (!sources.TryGetValue(payload, out var source))
                {
                    source = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                    sources[payload] = source;
                }

                return source;
            }
        }
    }

    /// <summary>Implements only the required members, so every default interface member runs.</summary>
    private sealed class DefaultsOnlyStreamDatabase : IRedisStreamDatabase
    {
        public List<(string Stream, long? MaxLength, bool Approximate)> Adds { get; } = [];

        public Task<RedisValue> StreamAddAsync(RedisKey stream, NameValueEntry[] values, long? maxLength, bool useApproximateMaxLength, CancellationToken cancellationToken)
        {
            Adds.Add((stream.ToString(), maxLength, useApproximateMaxLength));
            return Task.FromResult<RedisValue>($"{Adds.Count}-0");
        }

        public Task<bool> StreamCreateConsumerGroupAsync(RedisKey stream, RedisValue groupName, RedisValue position, bool createStream, CancellationToken cancellationToken)
            => Task.FromResult(true);

        public Task<StreamEntry[]> StreamReadGroupAsync(RedisKey stream, RedisValue groupName, RedisValue consumerName, int count, CancellationToken cancellationToken)
            => Task.FromResult(Array.Empty<StreamEntry>());

        public Task<long> StreamAcknowledgeAsync(RedisKey stream, RedisValue groupName, RedisValue messageId, CancellationToken cancellationToken)
            => Task.FromResult(1L);

        public Task<StreamPendingMessageInfo[]> StreamPendingMessagesAsync(RedisKey stream, RedisValue groupName, int count, RedisValue consumerName, RedisValue? minId, RedisValue? maxId, long minIdleTimeInMilliseconds, CancellationToken cancellationToken)
            => Task.FromResult(Array.Empty<StreamPendingMessageInfo>());

        public Task<StreamEntry[]> StreamClaimAsync(RedisKey stream, RedisValue groupName, RedisValue consumerName, long minIdleTimeInMilliseconds, RedisValue[] messageIds, CancellationToken cancellationToken)
            => Task.FromResult(Array.Empty<StreamEntry>());
    }

    /// <summary>
    /// A small stream model with failure hooks: XPENDING lists the seeded pending entries, XCLAIM
    /// answers positionally (nil for a trimmed entry), XREADGROUP serves nothing, and every call is
    /// recorded under a lock (the subscriber and the dispatcher workers run on background threads).
    /// </summary>
    private sealed class ScriptedStreamDatabase : IRedisStreamDatabase
    {
        private readonly object _gate = new();
        private readonly List<(RedisValue Id, StreamEntry Entry)> _pending = [];
        private readonly List<NameValueEntry[]> _adds = [];
        private readonly List<string> _acks = [];
        private int _reads;
        private int _deleteConsumerCalls;

        /// <summary>Awaited before every XADD with the call's token.</summary>
        public Func<CancellationToken, Task>? AddGate { get; set; }

        /// <summary>Given an id being ACKed, the exception that XACK fails with (or null).</summary>
        public Func<string, Exception?>? AckFault { get; set; }

        public Exception? DeleteConsumerException { get; set; }

        public IReadOnlyList<NameValueEntry[]> Adds
        {
            get
            {
                lock (_gate)
                    return _adds.ToArray();
            }
        }

        public IReadOnlyList<string> Acks
        {
            get
            {
                lock (_gate)
                    return _acks.ToArray();
            }
        }

        public int ReadCount => Volatile.Read(ref _reads);

        public int DeleteConsumerCalls => Volatile.Read(ref _deleteConsumerCalls);

        public void AddPending(RedisValue id, StreamEntry entry)
        {
            lock (_gate)
                _pending.Add((id, entry));
        }

        public async Task<RedisValue> StreamAddAsync(RedisKey stream, NameValueEntry[] values, long? maxLength, bool useApproximateMaxLength, CancellationToken cancellationToken)
        {
            if (AddGate is { } gate)
                await gate(cancellationToken);

            lock (_gate)
            {
                _adds.Add(values);
                return $"{_adds.Count}-0";
            }
        }

        public Task<bool> StreamCreateConsumerGroupAsync(RedisKey stream, RedisValue groupName, RedisValue position, bool createStream, CancellationToken cancellationToken)
            => Task.FromResult(true);

        public Task<StreamEntry[]> StreamReadGroupAsync(RedisKey stream, RedisValue groupName, RedisValue consumerName, int count, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _reads);
            return Task.FromResult(Array.Empty<StreamEntry>());
        }

        public Task<long> StreamAcknowledgeAsync(RedisKey stream, RedisValue groupName, RedisValue messageId, CancellationToken cancellationToken)
        {
            if (AckFault?.Invoke(messageId.ToString()) is { } fault)
                return Task.FromException<long>(fault);

            lock (_gate)
            {
                _acks.Add(messageId.ToString());
                return Task.FromResult((long)_pending.RemoveAll(item => item.Id == messageId));
            }
        }

        public Task<StreamPendingMessageInfo[]> StreamPendingMessagesAsync(RedisKey stream, RedisValue groupName, int count, RedisValue consumerName, RedisValue? minId, RedisValue? maxId, long minIdleTimeInMilliseconds, CancellationToken cancellationToken)
        {
            lock (_gate)
            {
                return Task.FromResult(_pending
                    .Where(item => minId is not { } only || maxId is not { } last || only != last || item.Id == only)
                    .Take(count)
                    .Select(item => PendingInfo(item.Id))
                    .ToArray());
            }
        }

        public Task<StreamEntry[]> StreamClaimAsync(RedisKey stream, RedisValue groupName, RedisValue consumerName, long minIdleTimeInMilliseconds, RedisValue[] messageIds, CancellationToken cancellationToken)
        {
            lock (_gate)
            {
                var reply = new List<StreamEntry>();
                foreach (var id in messageIds)
                {
                    var index = _pending.FindIndex(item => item.Id == id);
                    if (index >= 0)
                        reply.Add(_pending[index].Entry);
                }

                return Task.FromResult(reply.ToArray());
            }
        }

        public Task<RedisValue[]> StreamClaimIdsOnlyAsync(RedisKey stream, RedisValue groupName, RedisValue consumerName, long minIdleTimeInMilliseconds, RedisValue[] messageIds, CancellationToken cancellationToken)
            => Task.FromResult(messageIds);

        public Task<bool> TryDeleteIdleConsumerAsync(RedisKey stream, RedisValue groupName, RedisValue consumerName, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _deleteConsumerCalls);
            return DeleteConsumerException is { } fault ? Task.FromException<bool>(fault) : Task.FromResult(true);
        }

        private static StreamPendingMessageInfo PendingInfo(RedisValue messageId)
            => (StreamPendingMessageInfo)PendingConstructor.Invoke([messageId, (RedisValue)"old-consumer", 500L, 1]);

        private static readonly ConstructorInfo PendingConstructor =
            typeof(StreamPendingMessageInfo).GetConstructor(
                BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic,
                binder: null,
                [typeof(RedisValue), typeof(RedisValue), typeof(long), typeof(int)],
                modifiers: null)
            ?? throw new InvalidOperationException("StreamPendingMessageInfo constructor was not found.");
    }
}
