using AsyncResponse.Channels.MongoDB;
using AsyncResponse.Channels.PostgreSQL;
using AsyncResponse.Channels.SqlServer;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using MongoDB.Driver;
using Moq;
using Npgsql;
using System.Reflection;
using Xunit;
using PgServer = AsyncResponse.Tests.DbChannelCoverageGapPostgresServer;
using SqlServer = AsyncResponse.Tests.DbChannelCoverageGapSqlServer;

namespace AsyncResponse.Tests;

public sealed partial class DbChannelSharedCoverageTests
{
    /// <summary>
    /// The shared database-channel base's paths the MongoDB harness reached through its mocked
    /// collections, run in the PostgreSQL and SQL Server assemblies too — each of which compiles
    /// its own copy of <c>DbChannelShared.cs</c>. The relational stores run their real statements
    /// against scripted wire servers (<see cref="ScriptedRelationalStore"/>), so a store call can
    /// succeed with chosen rows or fail with a chosen fault without a container.
    /// </summary>
    public sealed class CoverageGaps
    {
        private static readonly TimeSpan LongPoll = TimeSpan.FromSeconds(30);

        // -----------------------------------------------------------------------------------
        // The scripted wire servers themselves
        // -----------------------------------------------------------------------------------

        /// <summary>The PostgreSQL wire server answers the channel store's real statements with the scripted rows.</summary>
        [Fact]
        public async Task ScriptedPostgres_TheChannelStoreReadsTheScriptedRows()
        {
            await using var server = new PgServer();
            var now = ScriptedRelationalStore.Now();
            var id = Guid.NewGuid();
            server.Respond = statement => Task.FromResult(statement switch
            {
                _ when statement.Contains("SELECT now(), nextval") => PgServer.Reply.Rows([("now", PgServer.TimestampTz), ("nextval", PgServer.Int8)], [now, 42L]),
                _ when statement.Contains("SELECT id, correlation_id") => PgServer.Reply.Rows(PgMessageColumns, [id, statement.Text(0), "{}", now, null, null]),
                _ when statement.Contains("RETURNING id") => PgServer.Reply.Rows([("id", PgServer.Uuid)], [id]),
                _ when statement.Contains("count(*)") => PgServer.Reply.Rows([("count", PgServer.Int8)], [3L]),
                _ => PgServer.Reply.Command(statement.Sql)
            });
            await using var dataSource = NpgsqlDataSource.Create(server.ConnectionString());
            var sql = new PostgreSqlChannelSql(dataSource, Options.Create(new PostgreSqlAsyncResponseChannelOptions { AutoCreateSchema = false }));
            SetField(sql, "_created", true);

            var (serverTime, seq) = await sql.GetSubscriptionStartAsync(CancellationToken.None);
            var messages = await sql.LoadMessagesAsync("corr", now.AddMinutes(-1), 10, null, null, CancellationToken.None);

            Assert.Equal(now, serverTime);
            Assert.Equal(42L, seq);
            var message = Assert.Single(messages);
            Assert.Equal(id, message.Id);
            Assert.Equal("corr", message.CorrelationId);
            Assert.True(await sql.TryClaimForDeliveryAsync(id, CancellationToken.None));
            Assert.Equal(3L, await sql.CountActiveSubscribersAsync("corr", CancellationToken.None));
        }

        /// <summary>The TDS server answers the SQL Server channel store's real statements with the scripted rows.</summary>
        [Fact]
        public async Task ScriptedSqlServer_TheChannelStoreReadsTheScriptedRows()
        {
            await using var server = new SqlServer();
            var now = ScriptedRelationalStore.Now().UtcDateTime;
            var id = Guid.NewGuid();
            server.Respond = statement => Task.FromResult(statement switch
            {
                _ when statement.Contains("SELECT SYSUTCDATETIME(), NEXT VALUE") => SqlServer.Reply.Rows([("now", SqlServer.Type.DateTime2), ("seq", SqlServer.Type.BigInt)], [now, 42L]),
                _ when statement.Contains("SELECT id, correlation_id") => SqlServer.Reply.Rows(SqlMessageColumns, [id, statement["correlation_id"], "{}", now, null, null]),
                _ when statement.Contains("OUTPUT inserted.id") => SqlServer.Reply.Rows([("id", SqlServer.Type.Guid)], [id]),
                _ when statement.Contains("COUNT_BIG") => SqlServer.Reply.Rows([("count", SqlServer.Type.BigInt)], [3L]),
                _ => SqlServer.Reply.Affected(1)
            });
            var sql = new SqlServerChannelSql(Options.Create(new SqlServerAsyncResponseChannelOptions { ConnectionString = server.ConnectionString(), AutoCreateSchema = false }));
            SetField(sql, "_created", true);

            var (serverTime, seq) = await sql.GetSubscriptionStartAsync(CancellationToken.None);
            var messages = await sql.LoadMessagesAsync("corr", DateTimeOffset.UtcNow.AddMinutes(-1), 10, null, null, CancellationToken.None);

            Assert.Equal(now, serverTime.UtcDateTime);
            Assert.Equal(42L, seq);
            var message = Assert.Single(messages);
            Assert.Equal(id, message.Id);
            Assert.Equal("corr", message.CorrelationId);
            Assert.True(await sql.TryClaimForDeliveryAsync(id, CancellationToken.None));
            Assert.Equal(3L, await sql.CountActiveSubscribersAsync("corr", CancellationToken.None));
        }

        // -----------------------------------------------------------------------------------
        // Waiter registration
        // -----------------------------------------------------------------------------------

        /// <summary>
        /// A registration that fails after the subscriber row was written (here the recovery-state
        /// save) is cleaned up — the row deleted, nothing left in the local map — and the original
        /// failure reaches the caller even when the logging provider throws on the error line.
        /// </summary>
        [Theory]
        [InlineData(Provider.PostgreSql)]
        [InlineData(Provider.SqlServer)]
        public async Task CreateWaiter_AFailedRegistration_IsCleanedUpAndRethrown(Provider provider)
        {
            await using var fixture = GapFixture.Create(provider, LongPoll);
            var harness = fixture.Harness;
            harness.Logger.ThrowOnMessageContaining = "Failed to create";
            harness.RecoveryState
                .Setup(store => store.SaveAsync(It.IsAny<string>(), It.IsAny<RecoveryState>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
                .ThrowsAsync(new TimeoutException("recovery store unreachable"));

            var failure = await Assert.ThrowsAsync<TimeoutException>(
                () => ((IAsyncResponseSubscriber)harness.Channel).CreateResponseWaiter<OperationResult>("corr"));

            Assert.Equal("recovery store unreachable", failure.Message);
            Assert.Equal(1, fixture.Store!.Ran("upsert-subscriber"));
            Assert.Equal(1, fixture.Store.Ran("delete-subscriber"));
            Assert.Empty(harness.Subscriptions);
            Assert.Contains(harness.Logger.Messages, message => message.Contains("Failed to create", StringComparison.Ordinal));
        }

        /// <summary>
        /// A disposal that lands while a registration is still writing its recovery state (after
        /// disposal took its snapshot of the waiters) is caught once the subscription is published:
        /// the new waiter cleans itself up and the caller gets <see cref="ObjectDisposedException"/>.
        /// </summary>
        [Theory]
        [InlineData(Provider.PostgreSql)]
        [InlineData(Provider.SqlServer)]
        public async Task CreateWaiter_ADisposalDuringRegistration_CleansTheWaiterUpAndThrowsObjectDisposed(Provider provider)
        {
            await using var fixture = GapFixture.Create(provider, LongPoll);
            var harness = fixture.Harness;
            var saving = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            harness.RecoveryState
                .Setup(store => store.SaveAsync(It.IsAny<string>(), It.IsAny<RecoveryState>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
                .Returns(async () =>
                {
                    saving.TrySetResult();
                    await release.Task;
                });

            var create = ((IAsyncResponseSubscriber)harness.Channel).CreateResponseWaiter<OperationResult>("corr");
            await saving.Task.WaitAsync(TimeSpan.FromSeconds(30));
            await harness.Channel.DisposeAsync();
            release.TrySetResult();

            await Assert.ThrowsAsync<ObjectDisposedException>(() => create.WaitAsync(TimeSpan.FromSeconds(30)));
            Assert.Empty(harness.Subscriptions);
            Assert.Equal(1, fixture.Store!.Ran("delete-subscriber"));
            harness.RecoveryState.Verify(store => store.TryDeleteAsync("corr", It.IsAny<Guid>(), It.IsAny<CancellationToken>()), Times.Once);
        }

        /// <summary>Loops never restart on a disposed channel: a late <c>EnsureListenerStarted</c> refuses.</summary>
        [Theory]
        [InlineData(Provider.MongoDb)]
        [InlineData(Provider.PostgreSql)]
        [InlineData(Provider.SqlServer)]
        public async Task EnsureListenerStarted_OnADisposedChannel_Throws(Provider provider)
        {
            await using var harness = Harness.Create(provider, failing: true, LongPoll);
            await harness.Channel.DisposeAsync();

            var failure = Assert.Throws<TargetInvocationException>(() => harness.Invoke("EnsureListenerStarted"));

            Assert.IsType<ObjectDisposedException>(failure.InnerException);
            Assert.Null(harness.ListenerCancellation);
        }

        // -----------------------------------------------------------------------------------
        // Publish liveness
        // -----------------------------------------------------------------------------------

        /// <summary>
        /// A publish whose subscriber count is zero still goes live when this process holds a
        /// live subscription for the id (its store row lapsed during an outage).
        /// </summary>
        [Theory]
        [InlineData(Provider.PostgreSql)]
        [InlineData(Provider.SqlServer)]
        public async Task Publish_ALiveLocalWaiter_IsDeliveredEvenWhenItsSubscriberRowHasLapsed(Provider provider)
        {
            await using var fixture = GapFixture.Create(provider, LongPoll);
            var harness = fixture.Harness;
            harness.ConfigureDeliveryConfirmation(timeout: TimeSpan.FromSeconds(5), pollInterval: TimeSpan.FromMilliseconds(50));
            var (waiter, completion) = harness.Subscription("corr", ScriptedRelationalStore.Now());
            harness.AddSubscription("corr", waiter);

            await ((IAsyncResponsePublisher)harness.Channel).SetResponse(
                new OperationResult { Status = OperationStatus.Completed, Message = "live" }, "corr");

            Assert.Equal("live", (await completion.Task.WaitAsync(TimeSpan.FromSeconds(10))).Message);
            Assert.Equal(1, fixture.Store!.Ran("count"));
            Assert.Equal(1, fixture.Store.Ran("insert"));
            harness.RecoveryState.Verify(store => store.GetAllAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
        }

        /// <summary>
        /// The local-liveness rule counts only subscriptions that are not dropped: an id whose only
        /// local subscription is mid-cleanup, with no subscriber row, goes to lost-subscriber
        /// recovery and is never inserted for live delivery.
        /// </summary>
        [Theory]
        [InlineData(Provider.MongoDb)]
        [InlineData(Provider.PostgreSql)]
        [InlineData(Provider.SqlServer)]
        public async Task Publish_OnlyDroppedLocalSubscriptions_DoNotCountAsALiveWaiter(Provider provider)
        {
            await using var fixture = GapFixture.Create(provider, LongPoll);
            var harness = fixture.Harness;
            var (dropped, completion) = harness.Subscription("corr");
            SetField(dropped, "_dropped", true);
            harness.AddSubscription("corr", dropped);

            await ((IAsyncResponsePublisher)harness.Channel).SetResponse(
                new OperationResult { Status = OperationStatus.Completed, Message = "lost" }, "corr");

            harness.RecoveryState.Verify(store => store.GetAllAsync("corr", It.IsAny<CancellationToken>()), Times.Once);
            Assert.False(completion.Task.IsCompleted);
            if (fixture.Store is { } store)
                Assert.Equal(0, store.Ran("insert"));
            else
                harness.MongoMessages!.Verify(
                    collection => collection.FindOneAndUpdateAsync(
                        It.IsAny<FilterDefinition<MongoChannelMessageDocument>>(),
                        It.IsAny<UpdateDefinition<MongoChannelMessageDocument>>(),
                        It.IsAny<FindOneAndUpdateOptions<MongoChannelMessageDocument, MongoChannelMessageDocument>>(),
                        It.IsAny<CancellationToken>()),
                    Times.Never);
        }

        // -----------------------------------------------------------------------------------
        // The dispatch pass's outage breaker
        // -----------------------------------------------------------------------------------

        /// <summary>
        /// A full sweep whose first wave of ids all failed transiently trips the breaker: the rest
        /// of the pass is skipped and the exception carries the first three failures and counts
        /// the others.
        /// </summary>
        [Theory]
        [InlineData(Provider.PostgreSql)]
        [InlineData(Provider.SqlServer)]
        public async Task FullSweep_AStoreOutage_TripsTheBreakerAfterTheFirstWave(Provider provider)
        {
            await using var fixture = GapFixture.Create(provider, LongPoll);
            var harness = fixture.Harness;
            harness.AddWaiters(Enumerable.Range(0, 40).Select(i => $"corr-{i}").ToArray());
            var loaded = new System.Collections.Concurrent.ConcurrentDictionary<string, byte>();
            fixture.Store!.Load = request =>
            {
                loaded.TryAdd(request.CorrelationId, 0);
                throw new ScriptedRelationalStore.Fault(transient: true);
            };

            var failure = await Assert.ThrowsAsync<AggregateException>(
                () => harness.InvokeAsync("DispatchPendingMessagesAsync", null, CancellationToken.None));

            Assert.InRange(loaded.Count, 8, 15);
            Assert.Equal(3, failure.InnerExceptions.Count);
            Assert.Contains($"failed for {loaded.Count} correlation ids (the first 3 attached, and {loaded.Count - 3} more)", failure.Message, StringComparison.Ordinal);
            Assert.Contains($"the store looks unavailable and the remaining {40 - loaded.Count} were left to a later pass", failure.Message, StringComparison.Ordinal);
            // A timer sweep that trips leaves its unvisited ids to the next scheduled sweep.
            Assert.False(harness.RequestedSweepRetryArmed);
        }

        /// <summary>
        /// A REQUESTED full sweep the breaker cut short (here one a full signal channel asked for)
        /// is armed for its retry at the wake-down floor instead of waiting for the throttle.
        /// </summary>
        [Theory]
        [InlineData(Provider.PostgreSql)]
        [InlineData(Provider.SqlServer)]
        public async Task FullSweep_ARequestedSweepTheBreakerCutShort_IsArmedForItsRetry(Provider provider)
        {
            await using var fixture = GapFixture.Create(provider, TimeSpan.FromMilliseconds(20), fullSweepInterval: TimeSpan.FromMinutes(10));
            var harness = fixture.Harness;
            harness.AddWaiters(Enumerable.Range(0, 20).Select(i => $"corr-{i}").ToArray());
            fixture.Store!.Load = _ => throw new ScriptedRelationalStore.Fault(transient: true);
            harness.SetChannelField("_fullSweepRequested", 1);
            Assert.Null(await harness.CollectDispatchScopeAsync().WaitAsync(TimeSpan.FromSeconds(10)));

            var tripped = await Assert.ThrowsAsync<AggregateException>(() => harness.InvokeAsync("DispatchPendingMessagesAsync", null, CancellationToken.None));

            Assert.Contains("the store looks unavailable", tripped.Message, StringComparison.Ordinal);
            Assert.True(harness.RequestedSweepRetryArmed);
        }

        /// <summary>
        /// The targeted pass walks its ids in turn, so an outage stops it after exactly the first
        /// wave; the skipped ids' signals are consumed, so the next pass is a full sweep.
        /// </summary>
        [Theory]
        [InlineData(Provider.PostgreSql)]
        [InlineData(Provider.SqlServer)]
        public async Task TargetedPass_AStoreOutage_StopsAfterTheFirstWave_AndRequestsAFullSweep(Provider provider)
        {
            await using var fixture = GapFixture.Create(provider, LongPoll);
            var harness = fixture.Harness;
            var ids = Enumerable.Range(0, 20).Select(i => $"corr-{i}").ToArray();
            harness.AddWaiters(ids);
            fixture.Store!.Load = _ => throw new ScriptedRelationalStore.Fault(transient: true);

            var failure = await Assert.ThrowsAsync<AggregateException>(
                () => harness.InvokeAsync("DispatchPendingMessagesAsync", new HashSet<string>(ids, StringComparer.Ordinal), CancellationToken.None));

            Assert.Equal(8, fixture.Store.Ran("load"));
            Assert.Contains("the remaining 12 were left to a later pass", failure.Message, StringComparison.Ordinal);
            harness.Invoke("SignalDispatcher", "corr-0");
            Assert.Null(await harness.CollectDispatchScopeAsync());
        }

        /// <summary>
        /// A transient load failure below the breaker (one waiter) schedules a rescan of just that
        /// id at the wake-down floor, while the configured full-sweep throttle is longer — SQL
        /// Server always, PostgreSQL once its LISTEN carries delivery.
        /// </summary>
        [Theory]
        [InlineData(Provider.PostgreSql)]
        [InlineData(Provider.SqlServer)]
        public async Task DispatchPass_ATransientLoadFailureBelowTheBreaker_IsRescannedAfterTheWakeDownFloor(Provider provider)
        {
            var floor = TimeSpan.FromMilliseconds(50);
            await using var fixture = GapFixture.Create(provider, LongPoll, fullSweepInterval: TimeSpan.FromMinutes(10));
            var harness = fixture.Harness;
            harness.ConfigureDeliveryConfirmation(timeout: floor * 4, pollInterval: TimeSpan.FromMilliseconds(5));
            if (provider == Provider.PostgreSql)
            {
                harness.MarkWakeListenerEstablished();
                Assert.Null(await harness.CollectDispatchScopeAsync().WaitAsync(TimeSpan.FromSeconds(10)));
            }

            Assert.Equal(TimeSpan.FromMinutes(10), harness.CurrentFullSweepInterval());
            harness.AddWaiters("corr");
            var loads = 0;
            fixture.Store!.Load = _ => Interlocked.Increment(ref loads) == 1
                ? throw new ScriptedRelationalStore.Fault(transient: true, "a pooled connection died in the failover")
                : Task.FromResult<IReadOnlyList<ScriptedRelationalStore.Row>>([]);

            var failedAt = System.Diagnostics.Stopwatch.GetTimestamp();
            var failure = await Assert.ThrowsAnyAsync<Exception>(() => harness.InvokeAsync(
                "DispatchPendingMessagesAsync", new HashSet<string>(StringComparer.Ordinal) { "corr" }, CancellationToken.None));
            Assert.Contains("a pooled connection died in the failover", failure.Message, StringComparison.Ordinal);

            var scope = Assert.IsType<HashSet<string>>(await harness.CollectDispatchScopeAsync().WaitAsync(TimeSpan.FromSeconds(10)));
            Assert.Single(scope, "corr");
            Assert.True(System.Diagnostics.Stopwatch.GetElapsedTime(failedAt) >= floor - TimeSpan.FromMilliseconds(15));
        }

        // -----------------------------------------------------------------------------------
        // The sweep's own reporting
        // -----------------------------------------------------------------------------------

        /// <summary>
        /// A full sweep that took longer than half the delivery-confirmation budget (the harness's
        /// 2 ms budget; each load here takes 20 ms) is reported once.
        /// </summary>
        [Theory]
        [InlineData(Provider.PostgreSql)]
        [InlineData(Provider.SqlServer)]
        public async Task FullSweep_ASlowSweep_WarnsThatItNearsTheConfirmationBudget(Provider provider)
        {
            await using var fixture = GapFixture.Create(provider, LongPoll);
            var harness = fixture.Harness;
            harness.AddWaiters("corr");
            fixture.Store!.Load = async _ =>
            {
                await Task.Delay(TimeSpan.FromMilliseconds(20));
                return [];
            };

            await harness.InvokeAsync("DispatchPendingMessagesAsync", null, CancellationToken.None);
            await harness.InvokeAsync("DispatchPendingMessagesAsync", null, CancellationToken.None);

            Assert.Single(harness.Logger.Messages, message => message.Contains("full dispatch sweep over 1 correlation ids took", StringComparison.Ordinal));
        }

        /// <summary>
        /// A store whose correlation-id column folds case answers a query for "corr" with the rows
        /// of "CORR": the ordinal re-check refuses the row and logs the collation problem instead
        /// of handing one waiter another waiter's response.
        /// </summary>
        [Theory]
        [InlineData(Provider.MongoDb)]
        [InlineData(Provider.PostgreSql)]
        [InlineData(Provider.SqlServer)]
        public async Task DispatchSweep_ARowForAnotherCaseOfTheId_IsRefusedAndLogged(Provider provider)
        {
            await using var fixture = GapFixture.Create(provider, LongPoll);
            var harness = fixture.Harness;
            var startedAt = ScriptedRelationalStore.Now();
            var delivered = 0;
            var (subscription, _) = harness.Subscription("corr", startedAt);
            harness.SetProcessHook(subscription, () => { Interlocked.Increment(ref delivered); return Task.CompletedTask; });
            harness.AddSubscription("corr", subscription);
            if (fixture.Store is { } store)
                store.Load = _ => Task.FromResult<IReadOnlyList<ScriptedRelationalStore.Row>>([new(Guid.NewGuid(), "CORR", LiveEnvelope, startedAt)]);
            else
                harness.MongoMessages!
                    .Setup(collection => collection.FindAsync(
                        It.IsAny<FilterDefinition<MongoChannelMessageDocument>>(),
                        It.IsAny<FindOptions<MongoChannelMessageDocument, MongoChannelMessageDocument>>(),
                        It.IsAny<CancellationToken>()))
                    .ReturnsAsync(() => Cursor(new List<MongoChannelMessageDocument> { Stored(Guid.NewGuid(), "CORR", startedAt) }));

            await SweepAndDrainAsync(harness);

            Assert.Equal(0, delivered);
            Assert.Contains(harness.Logger.Messages, message => message.Contains("returned a message for correlationId 'CORR' when asked for 'corr'", StringComparison.Ordinal));
        }

        // -----------------------------------------------------------------------------------
        // Delivery
        // -----------------------------------------------------------------------------------

        /// <summary>
        /// One waiter whose delivery throws does not strand its siblings: the fan-out still reaches
        /// the other waiter, the failure is logged per waiter, and the work item's failure rewinds
        /// the scan and schedules a rescan of the id.
        /// </summary>
        [Theory]
        [InlineData(Provider.PostgreSql)]
        [InlineData(Provider.SqlServer)]
        public async Task Dispatch_AWaiterWhoseDeliveryThrows_DoesNotStrandItsSibling(Provider provider)
        {
            await using var fixture = GapFixture.Create(provider, LongPoll);
            var harness = fixture.Harness;
            var startedAt = ScriptedRelationalStore.Now();
            var (failing, _) = harness.Subscription("corr", startedAt);
            harness.SetProcessHook(failing, () => throw new InvalidOperationException("waiter blew up"));
            var (healthy, _) = harness.Subscription("corr", startedAt);
            var delivered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            harness.SetProcessHook(healthy, () => { delivered.TrySetResult(); return Task.CompletedTask; });
            harness.AddSubscription("corr", failing);
            harness.AddSubscription("corr", healthy);
            fixture.Store!.Rows.Add(new(Guid.NewGuid(), "corr", LiveEnvelope, startedAt));

            await SweepAndDrainAsync(harness);

            await delivered.Task.WaitAsync(TimeSpan.FromSeconds(10));
            Assert.Contains(harness.Logger.Messages, message => message.Contains("to one waiter failed; the remaining waiters for this correlation id still receive it", StringComparison.Ordinal));
            Assert.Contains(harness.Logger.Messages, message => message.Contains("response dispatch failed for correlationId corr", StringComparison.Ordinal));
            Assert.Contains("corr", harness.BackpressureRescans.Keys.Cast<string>());
            Assert.Equal(1, fixture.Store.Ran("claim"));
        }

        // -----------------------------------------------------------------------------------
        // Forward paging, reconciliation and backpressure
        // -----------------------------------------------------------------------------------

        /// <summary>
        /// A pass reads at most 16 full pages forward; a backlog longer than that schedules a
        /// rescan of the id for the rest instead of monopolising the sweep.
        /// </summary>
        [Theory]
        [InlineData(Provider.PostgreSql)]
        [InlineData(Provider.SqlServer)]
        public async Task DispatchSweep_ABacklogPastThePerPassPageBudget_SchedulesARescan(Provider provider)
        {
            await using var fixture = GapFixture.Create(provider, LongPoll, pendingMessageBatchSize: 1);
            var harness = fixture.Harness;
            harness.SetOption("HistoryReconciliationInterval", TimeSpan.FromHours(1));
            var startedAt = ScriptedRelationalStore.Now();
            var delivered = 0;
            var (subscription, _) = harness.Subscription("corr", startedAt);
            harness.SetProcessHook(subscription, () => { Interlocked.Increment(ref delivered); return Task.CompletedTask; });
            harness.AddSubscription("corr", subscription);
            for (var i = 0; i < 20; i++)
                fixture.Store!.Rows.Add(new(Guid.NewGuid(), "corr", LiveEnvelope, startedAt.AddMilliseconds(i)));

            await SweepAndDrainAsync(harness);

            Assert.Equal(16, fixture.Store!.Ran("load"));
            Assert.Equal(16, delivered);
            Assert.Contains("corr", harness.BackpressureRescans.Keys.Cast<string>());
            var rescans = harness.Logger.Messages.Count(message => message.Contains("is at executor capacity", StringComparison.Ordinal));

            // The next pass walks another full budget of the backlog; the one rescan already
            // pending for the id is not scheduled (nor logged) twice.
            for (var i = 20; i < 40; i++)
                fixture.Store.Rows.Add(new(Guid.NewGuid(), "corr", LiveEnvelope, startedAt.AddMilliseconds(i)));
            await SweepAndDrainAsync(harness);

            Assert.Equal(32, delivered);
            Assert.Single(harness.BackpressureRescans);
            Assert.Equal(rescans, harness.Logger.Messages.Count(message => message.Contains("is at executor capacity", StringComparison.Ordinal)));
        }

        /// <summary>
        /// History reconciliation reads one page per pass; a page that neither exhausts the history
        /// nor reaches the forward cursor leaves reconciliation in progress and schedules a rescan.
        /// </summary>
        [Theory]
        [InlineData(Provider.PostgreSql)]
        [InlineData(Provider.SqlServer)]
        public async Task DispatchSweep_AnUnfinishedReconciliationPage_SchedulesARescan(Provider provider)
        {
            await using var fixture = GapFixture.Create(provider, LongPoll, pendingMessageBatchSize: 1);
            var harness = fixture.Harness;
            harness.SetOption("HistoryReconciliationInterval", TimeSpan.FromTicks(1));
            var startedAt = ScriptedRelationalStore.Now();
            var (subscription, _) = harness.Subscription("corr", startedAt);
            harness.SetProcessHook(subscription, () => Task.CompletedTask);
            harness.AddSubscription("corr", subscription);
            for (var i = 0; i < 3; i++)
                fixture.Store!.Rows.Add(new(Guid.NewGuid(), "corr", LiveEnvelope, startedAt.AddMilliseconds(i)));

            await SweepAndDrainAsync(harness);

            // Three full forward pages and the empty one that ends the walk, then one
            // reconciliation page from the start of the history.
            Assert.Equal(5, fixture.Store!.Ran("load"));
            Assert.Contains("corr", harness.BackpressureRescans.Keys.Cast<string>());
        }

        /// <summary>
        /// A late commit behind the cursor, re-read by the lookback revisit while the executor is
        /// full: the refused page puts the cursor back where the pass found it (caught up), keeps
        /// the window open, schedules a backpressure rescan, and the next pass with room delivers
        /// the late row.
        /// </summary>
        [Theory]
        [InlineData(Provider.PostgreSql)]
        [InlineData(Provider.SqlServer)]
        public async Task DispatchSweep_ARefusedLookbackPage_PutsTheCursorBackWhereThePassFoundIt(Provider provider)
        {
            await using var fixture = GapFixture.Create(provider, LongPoll);
            var harness = fixture.Harness;
            harness.ConfigureDeliveryConfirmation(timeout: TimeSpan.FromSeconds(5), pollInterval: TimeSpan.FromMilliseconds(50));
            harness.SetOption("HistoryReconciliationInterval", TimeSpan.FromHours(1));
            var started = ScriptedRelationalStore.Now();
            var store = fixture.Store!;
            // Acknowledged after the waiter started (fan-out history): they travel header-only and
            // are hydrated by id before delivery.
            for (var i = 1; i <= 5; i++)
                store.Rows.Add(new(Guid.NewGuid(), "corr", StaleEnvelope, started.AddMilliseconds(i), started.AddSeconds(1), i));
            var delivered = 0;
            var subscription = harness.Subscription("corr", started).Instance;
            harness.SetProcessHook(subscription, () => { Interlocked.Increment(ref delivered); return Task.CompletedTask; });
            harness.AddSubscription("corr", subscription);
            await SweepAndDrainAsync(harness);
            Assert.Equal(5, delivered);
            Assert.True(store.Ran("load-by-id") >= 1);
            var caughtUp = harness.ForwardCursor("corr");
            Assert.True(caughtUp.CaughtUp);

            // A late commit stamped with the oldest row's tick, and no room for it.
            store.Rows.Add(new(Guid.Empty, "corr", StaleEnvelope, started.AddMilliseconds(1)));
            var release = await OccupyExecutorAsync(harness, "corr");
            try
            {
                for (var i = 0; i < ChannelSerialExecutor.DefaultCapacity; i++)
                    Assert.Equal(SerialExecutorRegistry.TryEnqueueOutcome.Accepted, harness.Executors.TryEnqueue(harness.ChannelName("corr"), () => Task.CompletedTask));
                harness.HoldLookbackWindowOpen("corr");
                var before = System.Diagnostics.Stopwatch.GetTimestamp();

                await harness.InvokeAsync("DispatchPendingMessagesAsync", new HashSet<string> { "corr" }, CancellationToken.None);

                Assert.Equal(caughtUp, harness.ForwardCursor("corr"));
                Assert.InRange(harness.ForwardAdvancedAt("corr")!.Value, before, System.Diagnostics.Stopwatch.GetTimestamp());
                Assert.Contains("corr", harness.BackpressureRescans.Keys.Cast<string>());
                Assert.Contains(harness.Logger.Messages, message => message.Contains("is at executor capacity", StringComparison.Ordinal));
            }
            finally
            {
                release.TrySetResult();
            }

            await DrainAsync(harness, "corr");
            harness.HoldLookbackWindowOpen("corr");
            await SweepAndDrainAsync(harness);
            Assert.Equal(6, delivered);
        }

        // -----------------------------------------------------------------------------------
        // Teardown
        // -----------------------------------------------------------------------------------

        /// <summary>
        /// Disposal reports — never rethrows — a background loop that died of anything but the
        /// cancellation, and still cleans up every waiter.
        /// </summary>
        [Theory]
        [InlineData(Provider.PostgreSql)]
        [InlineData(Provider.SqlServer)]
        public async Task Dispose_ABackgroundLoopThatFaulted_StillCleansUpEveryWaiter(Provider provider)
        {
            await using var harness = Harness.Create(provider, failing: true, pollInterval: LongPoll);
            var (waiter, completion) = harness.Subscription("corr", cleanupStarted: false);
            harness.AddSubscription("corr", waiter);
            harness.Invoke("EnsureListenerStarted");
            harness.SetChannelField("_heartbeatTask", Task.FromException(new InvalidOperationException("the heartbeat loop died")));

            await harness.Channel.DisposeAsync();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => completion.Task.WaitAsync(TimeSpan.FromSeconds(10)));
            Assert.Empty(harness.Subscriptions);
            Assert.Contains(harness.Logger.Messages, message => message.Contains("background loop had failed before disposal", StringComparison.Ordinal));
        }

        /// <summary>
        /// Unlinking is by reference: an emptied group that is no longer the one mapped for its id
        /// (a successor was registered in its place) leaves the live group alone.
        /// </summary>
        [Theory]
        [InlineData(Provider.MongoDb)]
        [InlineData(Provider.PostgreSql)]
        [InlineData(Provider.SqlServer)]
        public async Task UnlinkIfEmpty_AStaleEmptyGroup_LeavesTheMappedGroupAlone(Provider provider)
        {
            await using var harness = Harness.Create(provider, failing: true, LongPoll);
            harness.AddWaiters("corr");
            var live = harness.Subscriptions["corr"]!;
            var stale = Activator.CreateInstance(live.GetType())!;

            harness.Invoke("UnlinkIfEmpty", "corr", stale);

            Assert.Same(live, harness.Subscriptions["corr"]);
            Assert.Single((System.Collections.IDictionary)live);
        }

        /// <summary>
        /// A disposal drain that fails with anything but its own budget lapsing (here the budget
        /// itself cannot be armed) still cannot prove settlement: the waiter is faulted as
        /// indeterminate and the drain failure logged, before the cleanup runs.
        /// </summary>
        [Theory]
        [InlineData(Provider.MongoDb)]
        [InlineData(Provider.PostgreSql)]
        [InlineData(Provider.SqlServer)]
        public async Task DisposalDrain_AnUnforeseenDrainFailure_FaultsTheWaiterAsIndeterminate(Provider provider)
        {
            await using var harness = Harness.Create(provider, failing: true, LongPoll);
            var (waiter, completion) = harness.Subscription("corr", cleanupStarted: false);
            harness.AddSubscription("corr", waiter);
            // Past what a CancellationTokenSource can arm: the drain's budget throws.
            harness.SetOption("DisposalDrainTimeout", TimeSpan.FromMilliseconds(-5));

            await ((ValueTask)waiter.GetType().GetMethod("DrainThenCleanupAsync")!.Invoke(waiter, [false, null])!);

            await Assert.ThrowsAsync<AsyncResponseIndeterminateDeliveryException>(() => completion.Task.WaitAsync(TimeSpan.FromSeconds(10)));
            Assert.Contains(harness.Logger.Entries, entry => entry.Message.Contains("Dispatch drain failed for correlationId corr", StringComparison.Ordinal) && entry.Exception is ArgumentOutOfRangeException);
        }

        /// <summary>
        /// A per-waiter executor retirement that throws is logged and swallowed: disposal awaits
        /// the tracked retirement and must not fault on it.
        /// </summary>
        [Theory]
        [InlineData(Provider.MongoDb)]
        [InlineData(Provider.PostgreSql)]
        [InlineData(Provider.SqlServer)]
        public async Task Cleanup_AnExecutorRetirementThatThrows_IsLoggedAndSwallowed(Provider provider)
        {
            await using var harness = Harness.Create(provider, failing: true, LongPoll);
            // A registry whose dispose budget cannot be armed: retiring a live executor throws.
            harness.SetChannelField("_executors", new SerialExecutorRegistry(NullLogger.Instance, disposeDrainLimit: TimeSpan.FromMilliseconds(-5)));
            var (waiter, completion) = harness.Subscription("corr", cleanupStarted: false);
            harness.AddSubscription("corr", waiter);
            Assert.True(await harness.Executors.EnqueueAsync(harness.ChannelName("corr"), () => Task.CompletedTask));

            await ((ValueTask)waiter.GetType().GetMethod("CleanupOnceAsync")!.Invoke(waiter, [false])!);
            await harness.Logger.WaitForAsync("Failed to retire the executor for channel");

            await harness.Channel.DisposeAsync();
            Assert.True(completion.Task.IsCanceled);
        }

        // -----------------------------------------------------------------------------------
        // The subscriber heartbeat
        // -----------------------------------------------------------------------------------

        /// <summary>
        /// A round whose own bookkeeping breaks (the snapshot, here) is reported distinctly from a
        /// failed store upsert, and the loop keeps running.
        /// </summary>
        [Theory]
        [InlineData(Provider.MongoDb)]
        [InlineData(Provider.PostgreSql)]
        [InlineData(Provider.SqlServer)]
        public async Task Heartbeat_ARoundWhoseSnapshotFails_IsReportedAndTheLoopContinues(Provider provider)
        {
            await using var harness = Harness.Create(provider, failing: true, LongPoll);
            var broken = DispatchProxy.Create(SubscriptionInterface(harness), typeof(BrokenSubscription));
            harness.AddSubscription("corr-broken", broken);

            harness.Invoke("EnsureListenerStarted");

            await harness.Logger.WaitForAsync("heartbeat round failed outside the store upsert", occurrences: 2);
        }

        /// <summary>
        /// Past the first failure of a run, a failed heartbeat retry inside the warning interval
        /// is logged at Debug, not as another warning.
        /// </summary>
        [Theory]
        [InlineData(Provider.PostgreSql)]
        [InlineData(Provider.SqlServer)]
        public async Task Heartbeat_ARunOfFailedRetries_WarnsOnceNotOncePerRetry(Provider provider)
        {
            await using var fixture = GapFixture.Create(provider, LongPoll);
            var harness = fixture.Harness;
            harness.AddWaiters("corr");
            var rounds = 0;
            var thirdRound = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            fixture.Store!.SubscriberWrite = operation =>
            {
                if (operation != "heartbeat")
                    return Task.CompletedTask;
                var round = Interlocked.Increment(ref rounds);
                if (round == 1)
                    harness.SetOption("SubscriberHeartbeatInterval", TimeSpan.FromHours(1));
                if (round == 3)
                    thirdRound.TrySetResult();
                throw new ScriptedRelationalStore.Fault(transient: true, "database down");
            };

            harness.Invoke("EnsureListenerStarted");
            await thirdRound.Task.WaitAsync(TimeSpan.FromSeconds(30));
            await harness.Logger.WaitForAsync("subscriber heartbeat retry");

            Assert.Single(harness.Logger.Messages, message => message.Contains("subscriber heartbeat failed", StringComparison.Ordinal));
        }

        /// <summary>A heartbeat round still in flight when the channel stops ends the loop quietly.</summary>
        [Fact]
        public async Task Heartbeat_ARoundInFlightAtDisposal_EndsTheLoopQuietly()
        {
            await using var harness = Harness.Create(Provider.MongoDb, failing: false, LongPoll);
            harness.AddWaiters("corr");
            var inFlight = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            harness.MongoSubscribers!
                .Setup(collection => collection.BulkWriteAsync(
                    It.IsAny<IEnumerable<WriteModel<MongoChannelSubscriberDocument>>>(),
                    It.IsAny<BulkWriteOptions>(),
                    It.IsAny<CancellationToken>()))
                .Returns(async (IEnumerable<WriteModel<MongoChannelSubscriberDocument>> _, BulkWriteOptions _, CancellationToken token) =>
                {
                    inFlight.TrySetResult();
                    await Task.Delay(Timeout.InfiniteTimeSpan, token);
                    return null!;
                });

            harness.Invoke("EnsureListenerStarted");
            await inFlight.Task.WaitAsync(TimeSpan.FromSeconds(10));
            await harness.Channel.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10));

            Assert.DoesNotContain(harness.Logger.Messages, message => message.Contains("heartbeat", StringComparison.OrdinalIgnoreCase));
        }

        /// <summary>
        /// A compensating delete (for a registration dropped while the round was in flight) that is
        /// still running when the channel stops is cancelled with it and ends the loop quietly.
        /// </summary>
        [Fact]
        public async Task Heartbeat_ACompensatingDeleteInFlightAtDisposal_EndsTheLoopQuietly()
        {
            await using var harness = Harness.Create(Provider.MongoDb, failing: false, LongPoll);
            var subscription = harness.Subscription("corr-mid-round").Instance;
            harness.AddSubscription("corr-mid-round", subscription);
            var deleting = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            harness.MongoSubscribers!
                .Setup(collection => collection.BulkWriteAsync(
                    It.IsAny<IEnumerable<WriteModel<MongoChannelSubscriberDocument>>>(),
                    It.IsAny<BulkWriteOptions>(),
                    It.IsAny<CancellationToken>()))
                .Callback(() => SetField(subscription, "_dropped", true))
                .ReturnsAsync((BulkWriteResult<MongoChannelSubscriberDocument>)null!);
            harness.MongoSubscribers
                .Setup(collection => collection.DeleteOneAsync(It.IsAny<FilterDefinition<MongoChannelSubscriberDocument>>(), It.IsAny<CancellationToken>()))
                .Returns(async (FilterDefinition<MongoChannelSubscriberDocument> _, CancellationToken token) =>
                {
                    deleting.TrySetResult();
                    await Task.Delay(Timeout.InfiniteTimeSpan, token);
                    return null!;
                });

            harness.Invoke("EnsureListenerStarted");
            await deleting.Task.WaitAsync(TimeSpan.FromSeconds(10));
            await harness.Channel.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10));

            Assert.DoesNotContain(harness.Logger.Messages, message => message.Contains("Failed to delete", StringComparison.Ordinal));
            Assert.DoesNotContain(harness.Logger.Messages, message => message.Contains("heartbeat round failed", StringComparison.Ordinal));
        }

        // -----------------------------------------------------------------------------------
        // The recovery-state scanner
        // -----------------------------------------------------------------------------------

        /// <summary>
        /// A scan yields every readable registration and then reports the unreadable ones it
        /// skipped, instead of completing over the readable subset.
        /// </summary>
        [Theory]
        [InlineData(Provider.PostgreSql)]
        [InlineData(Provider.SqlServer)]
        public async Task RecoveryScan_UnreadableRows_AreReportedAfterTheReadableOnes(Provider provider)
        {
            await using var store = new ScriptedRelationalStore(provider);
            var readable = new RecoveryState
            {
                RegistrationId = Guid.NewGuid(),
                CorrelationId = "corr",
                PayloadTypeFullName = typeof(OperationResult).FullName,
                RegisteredAtUtc = DateTime.UtcNow
            };
            store.RecoveryStates = () => Task.FromResult<IReadOnlyList<string>>([AsyncResponseJson.Serialize(readable), "{not json", """{"SchemaVersion":999}"""]);
            IRecoveryStateScanner scanner;
            NpgsqlDataSource? dataSource = null;
            if (provider == Provider.PostgreSql)
            {
                dataSource = NpgsqlDataSource.Create(store.ConnectionString);
                var sql = new PostgreSqlChannelSql(dataSource, Options.Create(new PostgreSqlAsyncResponseChannelOptions { AutoCreateSchema = false }));
                SetField(sql, "_created", true);
                scanner = new PostgreSqlRecoveryStateStore(sql, NullLogger<PostgreSqlRecoveryStateStore>.Instance);
            }
            else
            {
                var sql = new SqlServerChannelSql(Options.Create(new SqlServerAsyncResponseChannelOptions { ConnectionString = store.ConnectionString, AutoCreateSchema = false }));
                SetField(sql, "_created", true);
                scanner = new SqlServerRecoveryStateStore(sql, NullLogger<SqlServerRecoveryStateStore>.Instance);
            }

            try
            {
                var yielded = new List<RecoveryState>();
                var failure = await Assert.ThrowsAsync<RecoveryStateScanUnreadableException>(async () =>
                {
                    await foreach (var state in scanner.ScanAsync())
                        yielded.Add(state);
                });

                Assert.Equal(readable.RegistrationId, Assert.Single(yielded).RegistrationId);
                Assert.Equal(2, failure.UnreadableCount);
            }
            finally
            {
                if (dataSource is not null)
                    await dataSource.DisposeAsync();
            }
        }

        // -----------------------------------------------------------------------------------
        // Wake listeners
        // -----------------------------------------------------------------------------------

        /// <summary>
        /// A LISTEN whose channel stops right as it is established (the established LISTEN's
        /// full-sweep request is the last thing it does before its liveness wait) returns from the
        /// listen call normally rather than by cancellation: the loop ends its failure run and
        /// exits without logging a failure, and the connection is released with its UNLISTEN.
        /// </summary>
        [Fact]
        public async Task PostgreSqlListenLoop_StoppedAsTheListenIsEstablished_EndsCleanly()
        {
            await using var server = new FakePostgresWireServer();
            server.Respond = (_, sql) => Task.FromResult(FakePostgresWireServer.Reply.CompleteEchoingNotify(sql));
            await using var harness = Harness.Create(
                Provider.PostgreSql, failing: false, LongPoll, fullSweepInterval: TimeSpan.FromMinutes(10), postgreSqlConnectionString: server.ConnectionString());
            harness.MarkStoreCreated();
            harness.SetChannelField("_signals", new HookedSignals(() => harness.ListenerCancellation?.Cancel()));

            harness.Invoke("EnsureListenerStarted");
            var listen = (Task)harness.ChannelField("_listenTask")!;
            await listen.WaitAsync(TimeSpan.FromSeconds(30));

            Assert.True(listen.IsCompletedSuccessfully);
            Assert.DoesNotContain(harness.Logger.Messages, message => message.Contains("LISTEN loop failed", StringComparison.Ordinal));
            Assert.Contains(server.Statements, statement => statement.Sql.StartsWith("UNLISTEN", StringComparison.Ordinal));
        }

        /// <summary>
        /// A change stream that ends (an invalidate event) leaves the channel without its push
        /// wake: the full-sweep throttle is lifted again until the stream re-opens, and the end is
        /// not a failure.
        /// </summary>
        [Fact]
        public async Task MongoChangeStream_AStreamThatEnds_LiftsTheThrottleUntilItReopens()
        {
            await using var harness = Harness.Create(
                Provider.MongoDb, failing: false, LongPoll, fullSweepInterval: TimeSpan.FromMinutes(10), useChangeStreams: true);
            var ended = new Mock<IChangeStreamCursor<ChangeStreamDocument<MongoChannelMessageDocument>>>();
            ended.Setup(cursor => cursor.MoveNextAsync(It.IsAny<CancellationToken>())).ReturnsAsync(false);
            var watches = 0;
            var reopening = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            harness.MongoMessages!
                .Setup(collection => collection.WatchAsync(
                    It.IsAny<PipelineDefinition<ChangeStreamDocument<MongoChannelMessageDocument>, ChangeStreamDocument<MongoChannelMessageDocument>>>(),
                    It.IsAny<ChangeStreamOptions>(),
                    It.IsAny<CancellationToken>()))
                .Returns(async (PipelineDefinition<ChangeStreamDocument<MongoChannelMessageDocument>, ChangeStreamDocument<MongoChannelMessageDocument>> _, ChangeStreamOptions _, CancellationToken token) =>
                {
                    if (Interlocked.Increment(ref watches) == 1)
                        return ended.Object;
                    reopening.TrySetResult();
                    await Task.Delay(Timeout.InfiniteTimeSpan, token);
                    return ended.Object;
                });

            harness.Invoke("EnsureListenerStarted");
            await reopening.Task.WaitAsync(TimeSpan.FromSeconds(30));

            Assert.Equal(HarnessWakeDownInterval, harness.CurrentFullSweepInterval());
            Assert.DoesNotContain(harness.Logger.Messages, message => message.Contains("change-stream loop failed", StringComparison.Ordinal));
        }

        // -----------------------------------------------------------------------------------
        // Helpers
        // -----------------------------------------------------------------------------------

        /// <summary>A signal channel that runs <paramref name="onFullSweepRequest"/> after every null (full-sweep) write.</summary>
        private sealed class HookedSignals : System.Threading.Channels.Channel<string?>
        {
            public HookedSignals(Action onFullSweepRequest)
            {
                var inner = System.Threading.Channels.Channel.CreateUnbounded<string?>();
                Reader = inner.Reader;
                Writer = new HookedWriter(inner.Writer, onFullSweepRequest);
            }

            private sealed class HookedWriter(System.Threading.Channels.ChannelWriter<string?> inner, Action onFullSweepRequest)
                : System.Threading.Channels.ChannelWriter<string?>
            {
                public override bool TryWrite(string? item)
                {
                    var written = inner.TryWrite(item);
                    if (item is null)
                        onFullSweepRequest();
                    return written;
                }

                public override ValueTask<bool> WaitToWriteAsync(CancellationToken cancellationToken = default) => inner.WaitToWriteAsync(cancellationToken);

                public override bool TryComplete(Exception? error = null) => inner.TryComplete(error);
            }
        }

        private static Type SubscriptionInterface(Harness harness)
            => harness.Channel.GetType().BaseType!.GetNestedType("IDbSubscription", BindingFlags.NonPublic)!;

        private static readonly (string, int)[] PgMessageColumns =
        [
            ("id", PgServer.Uuid), ("correlation_id", PgServer.Text), ("envelope_json", PgServer.Text),
            ("created_at", PgServer.TimestampTz), ("acked_at", PgServer.TimestampTz), ("acked_seq", PgServer.Int8)
        ];

        private static readonly (string, SqlServer.Type)[] SqlMessageColumns =
        [
            ("id", SqlServer.Type.Guid), ("correlation_id", SqlServer.Type.NVarChar), ("envelope_json", SqlServer.Type.NVarCharMax),
            ("created_at", SqlServer.Type.DateTime2), ("acked_at", SqlServer.Type.DateTime2), ("acked_seq", SqlServer.Type.BigInt)
        ];
    }

    /// <summary>
    /// A subscription (proxying the provider assembly's private <c>IDbSubscription</c>) whose
    /// <c>Dropped</c> read throws: the heartbeat's snapshot breaks on it. Teardown members succeed.
    /// </summary>
    public class BrokenSubscription : DispatchProxy
    {
        private readonly Guid _id = Guid.NewGuid();

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            switch (targetMethod!.Name)
            {
                case "get_Id":
                    return _id;
                case "get_Dropped":
                    throw new InvalidOperationException("the snapshot broke");
                case "get_StartedAtUtc":
                    return DateTimeOffset.UtcNow;
                default:
                    var returnType = targetMethod.ReturnType;
                    if (returnType == typeof(ValueTask))
                        return ValueTask.CompletedTask;
                    if (returnType == typeof(Task))
                        return Task.CompletedTask;
                    return returnType.IsValueType && returnType != typeof(void) ? Activator.CreateInstance(returnType) : null;
            }
        }
    }

    /// <summary>A harness plus, for the relational providers, the scripted store it runs on.</summary>
    private sealed class GapFixture : IAsyncDisposable
    {
        private GapFixture(Harness harness, ScriptedRelationalStore? store)
        {
            Harness = harness;
            Store = store;
        }

        public Harness Harness { get; }

        public ScriptedRelationalStore? Store { get; }

        public static GapFixture Create(Provider provider, TimeSpan pollInterval, TimeSpan? fullSweepInterval = null, int? pendingMessageBatchSize = null)
        {
            if (provider == Provider.MongoDb)
                return new GapFixture(Harness.Create(provider, failing: false, pollInterval, fullSweepInterval, pendingMessageBatchSize: pendingMessageBatchSize), null);

            var store = new ScriptedRelationalStore(provider);
            return new GapFixture(store.CreateHarness(pollInterval, fullSweepInterval, pendingMessageBatchSize), store);
        }

        public async ValueTask DisposeAsync()
        {
            await Harness.DisposeAsync();
            if (Store is not null)
                await Store.DisposeAsync();
        }
    }
}
