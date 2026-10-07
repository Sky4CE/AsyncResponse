using AsyncResponse.Channels.PostgreSQL;
using AsyncResponse.Channels.SqlServer;
using AsyncResponse.Transports.MongoDB;
using AsyncResponse.Transports.PostgreSQL;
using AsyncResponse.Transports.SqlServer;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using Npgsql;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Reflection;
using System.Transactions;
using Xunit;

namespace AsyncResponse.Tests;

public sealed partial class DbChannelSharedCoverageTests
{
    /// <summary>
    /// Round 65 (dbproviders) regressions on the shared database-channel base, nested only to reach
    /// its harness (<see cref="Harness"/>, <see cref="ScriptedRelationalStore"/>): background loops
    /// that ran in the first caller's ExecutionContext, store calls that enlisted in an ambient
    /// System.Transactions transaction, a waiter that outlived its own recovery registration, and
    /// the recovery-state reader's unguarded warnings.
    /// </summary>
    public sealed class Round65DbProviderChannels
    {
        private static readonly TimeSpan LongPoll = TimeSpan.FromSeconds(30);

        /// <summary>
        /// F-05: the dispatch and heartbeat loops are started lazily from inside the first
        /// CreateResponseWaiter, and used to inherit that caller's ExecutionContext for good — its
        /// Activity (every poll parented to the first request's span) and its async-flow
        /// TransactionScope (every loop connection enlisted in the caller's transaction while it
        /// was open). They now start with flow suppressed. Red on 6d7e1aeb: the loops' log lines
        /// observed the request Activity and the caller's transaction.
        /// </summary>
        [Theory]
        [InlineData(Provider.SqlServer)]
        [InlineData(Provider.PostgreSql)]
        [InlineData(Provider.MongoDb)]
        public async Task BackgroundLoops_StartedInsideARequestAndATransactionScope_RunInACleanContext(Provider provider)
        {
            await using var harness = Harness.Create(provider, failing: true, pollInterval: TimeSpan.FromMilliseconds(20));
            var observed = new ConcurrentQueue<(string Message, string? ActivityId, string? TransactionId)>();
            harness.Logger.OnMessage = message => observed.Enqueue((
                message,
                Activity.Current?.Id,
                Transaction.Current?.TransactionInformation.LocalIdentifier));
            harness.AddSubscription("corr", harness.Subscription("corr").Instance);

            using var source = new ActivitySource("AsyncResponse.Tests.Round65.Request");
            using var listener = new ActivityListener
            {
                ShouldListenTo = candidate => ReferenceEquals(candidate, source),
                Sample = static (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded
            };
            ActivitySource.AddActivityListener(listener);

            using (var request = source.StartActivity("request"))
            using (var scope = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                Assert.NotNull(request);
                Assert.NotNull(Transaction.Current);

                harness.Invoke("EnsureListenerStarted");

                // Observed while the caller's scope and span are both still live: that is when an
                // inherited context would make the loops' connections enlist and their spans nest.
                await harness.Logger.WaitForAsync("subscriber heartbeat failed");
                await harness.Logger.WaitForAsync("response dispatch loop failed");
            }

            var loopLines = observed
                .Where(entry => entry.Message.Contains("subscriber heartbeat failed", StringComparison.Ordinal)
                    || entry.Message.Contains("response dispatch loop failed", StringComparison.Ordinal))
                .ToList();
            Assert.NotEmpty(loopLines);
            Assert.All(loopLines, entry =>
            {
                Assert.Null(entry.ActivityId);
                Assert.Null(entry.TransactionId);
            });
        }

        /// <summary>
        /// F-05: the channel's own statements never enlist in an ambient transaction — the waiter's
        /// registration (subscription start, subscriber row) runs on the caller's flow, and used to
        /// enlist there. An ABORTED ambient transaction makes any enlistment attempt throw on the
        /// client before a byte is sent, so it pins "no enlistment" without a real transaction
        /// manager. Red on 6d7e1aeb: CreateResponseWaiter threw TransactionException ("The
        /// operation is not valid for the state of the transaction").
        /// </summary>
        [Theory]
        [InlineData(Provider.SqlServer)]
        [InlineData(Provider.PostgreSql)]
        public async Task StoreCalls_UnderAnAmbientTransaction_DoNotEnlist(Provider provider)
        {
            await using var store = new ScriptedRelationalStore(provider);
            await using var harness = store.CreateHarness(LongPoll);

            IAsyncResponseWaiter<OperationResult> waiter;
            using (var transaction = new CommittableTransaction())
            using (var scope = new TransactionScope(transaction, TransactionScopeAsyncFlowOption.Enabled))
            {
                transaction.Rollback();
                Assert.Equal(TransactionStatus.Aborted, Transaction.Current!.TransactionInformation.Status);

                waiter = await ((IAsyncResponseSubscriber)harness.Channel).CreateResponseWaiter<OperationResult>("corr");
            }

            Assert.Equal(1, store.Ran("subscription-start"));
            Assert.Equal(1, store.Ran("upsert-subscriber"));
            await waiter.DisposeAsync();
        }

        /// <summary>
        /// F-05: the SQL Server channel owns its connection string, so it opts out of enlistment
        /// at the source — whatever the configured string says. Red on 6d7e1aeb: SqlClient's
        /// default (Enlist=true) was used as configured.
        /// </summary>
        [Theory]
        [InlineData("Server=localhost,1;Database=unused;User Id=sa;Password=unused;Encrypt=False")]
        [InlineData("Server=localhost,1;Database=unused;User Id=sa;Password=unused;Encrypt=False;Enlist=true")]
        public void SqlServerChannel_ConnectionString_NeverEnlists(string configured)
        {
            var sql = new SqlServerChannelSql(Options.Create(new SqlServerAsyncResponseChannelOptions { ConnectionString = configured }));

            var effective = (string)typeof(SqlServerChannelSql)
                .GetField("_connectionString", BindingFlags.Instance | BindingFlags.NonPublic)!
                .GetValue(sql)!;

            var builder = new SqlConnectionStringBuilder(effective);
            Assert.False(builder.Enlist);
            Assert.Equal("localhost,1", builder.DataSource);
        }

        /// <summary>
        /// L-09: a waiter's registration is saved once and never refreshed, so a wait longer than
        /// <c>RecoveryStateExpiry</c> outlived it and the tail of the wait had no recovery. The
        /// registration now lasts as long as the wait (every row carries its own expiry — Redis
        /// parity); a wait inside the expiry keeps the expiry. Red on 6d7e1aeb: the registration
        /// was saved with the expiry whatever the timeout.
        /// </summary>
        [Theory]
        [InlineData(Provider.SqlServer)]
        [InlineData(Provider.PostgreSql)]
        public async Task Waiter_WithATimeoutPastRecoveryStateExpiry_SavesARegistrationThatOutlivesTheWait(Provider provider)
        {
            await using var store = new ScriptedRelationalStore(provider);
            await using var harness = store.CreateHarness(LongPoll);
            var expiry = new AsyncResponseChannelOptionsProbe(harness).RecoveryStateExpiry;
            var saved = new ConcurrentQueue<(string CorrelationId, TimeSpan Ttl)>();
            harness.RecoveryState
                .Setup(recovery => recovery.SaveAsync(It.IsAny<string>(), It.IsAny<RecoveryState>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
                .Callback<string, RecoveryState, TimeSpan, CancellationToken>((correlationId, _, ttl, _) => saved.Enqueue((correlationId, ttl)))
                .Returns(Task.CompletedTask);
            var subscriber = (IAsyncResponseSubscriber)harness.Channel;
            var longWait = expiry + TimeSpan.FromDays(1);

            var longWaiter = await subscriber.CreateResponseWaiter<OperationResult>("corr-long", timeout: longWait);
            var shortWaiter = await subscriber.CreateResponseWaiter<OperationResult>("corr-short", timeout: TimeSpan.FromMinutes(5));
            await longWaiter.DisposeAsync();
            await shortWaiter.DisposeAsync();

            Assert.Equal(longWait, Assert.Single(saved, entry => entry.CorrelationId == "corr-long").Ttl);
            Assert.Equal(expiry, Assert.Single(saved, entry => entry.CorrelationId == "corr-short").Ttl);
        }

        /// <summary>
        /// L-01: the recovery-state reader's four warnings go through SafeLog. A throwing logging
        /// provider used to replace the documented outcomes — an ordinally rejected row (a legacy
        /// collation's cross-match) reads as absence, an unreadable row as
        /// <see cref="RecoveryStateUnreadableException"/> — with the logger's own exception. Red
        /// on 6d7e1aeb: every call threw "logger exploded".
        /// </summary>
        [Theory]
        [InlineData(Provider.SqlServer)]
        [InlineData(Provider.PostgreSql)]
        public async Task RecoveryStateReader_WithAThrowingLogger_KeepsItsDocumentedOutcomes(Provider provider)
        {
            await using var store = new ScriptedRelationalStore(provider);
            var logger = new CollectingLogger { ThrowOnMessageContaining = "recovery state" };
            var (recoveryStore, dataSource) = RecoveryStore(provider, store, logger);
            try
            {
                // A readable row for ANOTHER id (what a case-insensitive legacy collation returns).
                store.RecoveryStates = () => Task.FromResult<IReadOnlyList<string>>([AsyncResponseJson.Serialize(State("CORR"))]);
                Assert.Empty(await recoveryStore.GetAllAsync("corr"));

                // Unreadable: malformed JSON, an incomplete identity, an unsupported schema version.
                foreach (var unreadable in new[]
                         {
                             "{not json",
                             AsyncResponseJson.Serialize(State("corr", registrationId: Guid.Empty)),
                             AsyncResponseJson.Serialize(State("corr", schemaVersion: 999))
                         })
                {
                    store.RecoveryStates = () => Task.FromResult<IReadOnlyList<string>>([unreadable]);
                    var failure = await Assert.ThrowsAsync<RecoveryStateUnreadableException>(() => recoveryStore.GetAllAsync("corr"));
                    Assert.Equal(1, failure.UnreadableCount);
                }

                // The watchdog scan: counted and reported after the readable rows, not cut short.
                store.RecoveryStates = () => Task.FromResult<IReadOnlyList<string>>([AsyncResponseJson.Serialize(State("corr")), "{not json"]);
                var yielded = 0;
                var scanFailure = await Assert.ThrowsAsync<RecoveryStateScanUnreadableException>(async () =>
                {
                    await foreach (var _ in ((IRecoveryStateScanner)recoveryStore).ScanAsync())
                        yielded++;
                });
                Assert.Equal(1, yielded);
                Assert.Equal(1, scanFailure.UnreadableCount);

                // Every warning was attempted (the provider threw on each), none escaped.
                Assert.True(logger.Messages.Count(message => message.Contains("recovery state", StringComparison.OrdinalIgnoreCase)) >= 5);
            }
            finally
            {
                if (dataSource is not null)
                    await dataSource.DisposeAsync();
            }
        }

        private static RecoveryState State(string correlationId, Guid? registrationId = null, int? schemaVersion = null)
        {
            var state = new RecoveryState
            {
                RegistrationId = registrationId ?? Guid.NewGuid(),
                CorrelationId = correlationId,
                PayloadTypeFullName = typeof(OperationResult).FullName,
                RegisteredAtUtc = DateTime.UtcNow
            };
            if (schemaVersion is { } version)
                state.SchemaVersion = version;
            return state;
        }

        private static (IRecoveryStateStore Store, NpgsqlDataSource? DataSource) RecoveryStore(Provider provider, ScriptedRelationalStore store, CollectingLogger logger)
        {
            if (provider == Provider.PostgreSql)
            {
                var dataSource = NpgsqlDataSource.Create(store.ConnectionString);
                var sql = new PostgreSqlChannelSql(dataSource, Options.Create(new PostgreSqlAsyncResponseChannelOptions { AutoCreateSchema = false }));
                SetField(sql, "_created", true);
                return (new PostgreSqlRecoveryStateStore(sql, logger.For<PostgreSqlRecoveryStateStore>()), dataSource);
            }

            var sqlServer = new SqlServerChannelSql(Options.Create(new SqlServerAsyncResponseChannelOptions { ConnectionString = store.ConnectionString, AutoCreateSchema = false }));
            SetField(sqlServer, "_created", true);
            return (new SqlServerRecoveryStateStore(sqlServer, logger.For<SqlServerRecoveryStateStore>()), null);
        }

        /// <summary>Reads the harness channel's options without knowing its provider type.</summary>
        private readonly struct AsyncResponseChannelOptionsProbe(Harness harness)
        {
            public TimeSpan RecoveryStateExpiry
                => ((AsyncResponseChannelOptions)harness.ChannelField("_options")!).RecoveryStateExpiry;
        }
    }
}

/// <summary>
/// Round 65 (dbproviders) regressions outside the channel harness: the serial executor's drain
/// loop context, the DB channels' wake-cadence warning, and the DB transports' settlement retry.
/// </summary>
public sealed class Round65DbProvidersTests
{
    public enum Provider
    {
        SqlServer,
        PostgreSql,
        MongoDb
    }

    // -----------------------------------------------------------------------------------------
    // F-05: ChannelSerialExecutor
    // -----------------------------------------------------------------------------------------

    /// <summary>
    /// F-05: the executor's drain loop serves every later item of its key, so it must not run in
    /// the context of whoever created it. Red on 6d7e1aeb: the item saw the creator's Activity and
    /// its async-flow transaction (a delivery claim run there enlisted in the creator's
    /// transaction).
    /// </summary>
    [Fact]
    public async Task SerialExecutor_CreatedInsideARequestAndATransactionScope_RunsItemsInACleanContext()
    {
        using var source = new ActivitySource("AsyncResponse.Tests.Round65.Executor");
        using var listener = new ActivityListener
        {
            ShouldListenTo = candidate => ReferenceEquals(candidate, source),
            Sample = static (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded
        };
        ActivitySource.AddActivityListener(listener);

        using (var request = source.StartActivity("request"))
        using (var scope = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
        {
            Assert.NotNull(request);
            await using var executor = new ChannelSerialExecutor(NullLogger.Instance, "corr");
            var seen = new TaskCompletionSource<(string? ActivityId, Transaction? Transaction)>(TaskCreationOptions.RunContinuationsAsynchronously);
            Assert.True(await executor.Enqueue(() =>
            {
                seen.TrySetResult((Activity.Current?.Id, Transaction.Current));
                return Task.CompletedTask;
            }));

            var (activityId, transaction) = await seen.Task.WaitAsync(TimeSpan.FromSeconds(30));
            Assert.Null(activityId);
            Assert.Null(transaction);
        }
    }

    // -----------------------------------------------------------------------------------------
    // L-09: wake cadence vs DeliveryConfirmationTimeout
    // -----------------------------------------------------------------------------------------

    /// <summary>
    /// L-09: a sweep cadence that carries cross-process delivery but is more than half the
    /// publisher's confirmation budget is reported at construction, instead of silently routing
    /// live waiters' responses to recovery. Defaults stay quiet. Red on 6d7e1aeb: nothing logged.
    /// </summary>
    [Theory]
    [InlineData("SqlServer:ActivePollInterval", true)]
    [InlineData("SqlServer:FullSweepInterval", true)]
    [InlineData("SqlServer:defaults", false)]
    [InlineData("PostgreSql:ListenerPollInterval", true)]
    [InlineData("PostgreSql:defaults", false)]
    [InlineData("MongoDb:ListenerPollInterval", true)]
    [InlineData("MongoDb:defaults", false)]
    public async Task WakeCadence_PastHalfTheConfirmationBudget_IsWarnedAtConstruction(string configuration, bool warns)
    {
        var logger = new CollectingLogger();
        var scopeFactory = new ServiceCollection().BuildServiceProvider().GetRequiredService<IServiceScopeFactory>();
        var recovery = new Mock<IRecoveryStateStore>().Object;
        var (provider, knob) = (configuration.Split(':')[0], configuration.Split(':')[1]);
        IAsyncDisposable channel;
        NpgsqlDataSource? dataSource = null;
        if (provider == "SqlServer")
        {
            var options = new SqlServerAsyncResponseChannelOptions
            {
                ConnectionString = "Server=localhost,1;Database=unused;User Id=sa;Password=unused;Encrypt=False",
                DeliveryConfirmationTimeout = TimeSpan.FromSeconds(4)
            };
            if (knob == "ActivePollInterval")
            {
                options.ActivePollInterval = TimeSpan.FromSeconds(2.5);
                options.IdlePollInterval = TimeSpan.FromSeconds(2.5);
            }
            if (knob == "FullSweepInterval")
                options.FullSweepInterval = TimeSpan.FromSeconds(4);
            var wrapped = Options.Create(options);
            channel = new SqlServerAsyncResponseChannel(scopeFactory, new SqlServerChannelSql(wrapped), recovery, wrapped, new AsyncResponseContextPropagation([]), logger.For<SqlServerAsyncResponseChannel>());
        }
        else if (provider == "MongoDb")
        {
            var options = new AsyncResponse.Channels.MongoDB.MongoDbAsyncResponseChannelOptions
            {
                AutoCreateIndexes = false,
                UseOwnershipLedger = false,
                DeliveryConfirmationTimeout = TimeSpan.FromSeconds(4)
            };
            if (knob == "ListenerPollInterval")
                options.ListenerPollInterval = TimeSpan.FromSeconds(2.5);
            var wrapped = Options.Create(options);
            var database = new Mock<MongoDB.Driver.IMongoDatabase>(MockBehavior.Loose);
            database.SetupGet(d => d.DatabaseNamespace).Returns(new MongoDB.Driver.DatabaseNamespace("tests"));
            var recoveryDocs = new Mock<MongoDB.Driver.IMongoCollection<AsyncResponse.Channels.MongoDB.MongoRecoveryStateDocument>>();
            var messageDocs = new Mock<MongoDB.Driver.IMongoCollection<AsyncResponse.Channels.MongoDB.MongoChannelMessageDocument>>();
            var subscriberDocs = new Mock<MongoDB.Driver.IMongoCollection<AsyncResponse.Channels.MongoDB.MongoChannelSubscriberDocument>>();
            recoveryDocs.SelfPinning();
            messageDocs.SelfPinning();
            subscriberDocs.SelfPinning();
            database.WithLooseCollection<MongoDB.Bson.BsonDocument>();
            database.Setup(d => d.GetCollection<AsyncResponse.Channels.MongoDB.MongoRecoveryStateDocument>(It.IsAny<string>(), It.IsAny<MongoDB.Driver.MongoCollectionSettings>())).Returns(recoveryDocs.Object);
            database.Setup(d => d.GetCollection<AsyncResponse.Channels.MongoDB.MongoChannelMessageDocument>(It.IsAny<string>(), It.IsAny<MongoDB.Driver.MongoCollectionSettings>())).Returns(messageDocs.Object);
            database.Setup(d => d.GetCollection<AsyncResponse.Channels.MongoDB.MongoChannelSubscriberDocument>(It.IsAny<string>(), It.IsAny<MongoDB.Driver.MongoCollectionSettings>())).Returns(subscriberDocs.Object);
            channel = new AsyncResponse.Channels.MongoDB.MongoDbAsyncResponseChannel(
                scopeFactory,
                new AsyncResponse.Channels.MongoDB.MongoDbChannelStore(database.Object, wrapped),
                recovery,
                wrapped,
                new AsyncResponseContextPropagation([]),
                logger.For<AsyncResponse.Channels.MongoDB.MongoDbAsyncResponseChannel>());
        }
        else
        {
            var options = new PostgreSqlAsyncResponseChannelOptions { DeliveryConfirmationTimeout = TimeSpan.FromSeconds(4) };
            if (knob == "ListenerPollInterval")
                options.ListenerPollInterval = TimeSpan.FromSeconds(2.5);
            var wrapped = Options.Create(options);
            dataSource = NpgsqlDataSource.Create("Host=localhost;Port=1;Database=unused;Username=unused;Password=unused;Timeout=1;Pooling=false");
            channel = new PostgreSqlAsyncResponseChannel(scopeFactory, new PostgreSqlChannelSql(dataSource, wrapped), recovery, wrapped, new AsyncResponseContextPropagation([]), logger.For<PostgreSqlAsyncResponseChannel>());
        }

        try
        {
            var warnings = logger.Messages.Where(message => message.Contains("is more than half of DeliveryConfirmationTimeout", StringComparison.Ordinal)).ToList();
            if (warns)
            {
                var warning = Assert.Single(warnings);
                Assert.Contains(knob, warning, StringComparison.Ordinal);
            }
            else
            {
                Assert.Empty(warnings);
            }
        }
        finally
        {
            await channel.DisposeAsync();
            if (dataSource is not null)
                await dataSource.DisposeAsync();
        }
    }

    // -----------------------------------------------------------------------------------------
    // L-03: DB transport settlement retry
    // -----------------------------------------------------------------------------------------

    /// <summary>
    /// L-03: one transient fault on the post-handler ACK no longer leaves a completed job leased
    /// for a peer to run again — the fenced, idempotent ACK is retried on a fresh call. Red on
    /// 6d7e1aeb: one ACK attempt, then "Failed to ACK".
    /// </summary>
    [Theory]
    [InlineData(Provider.SqlServer)]
    [InlineData(Provider.PostgreSql)]
    [InlineData(Provider.MongoDb)]
    public async Task AckAfterHandler_OneTransientAckFault_IsRetried(Provider provider)
    {
        var logger = new CollectingLogger();
        var settle = new Settle { AckFaults = 1 };
        var handlerRuns = 0;
        await using var rig = Rig.Build(provider, _ => { Interlocked.Increment(ref handlerRuns); return Task.CompletedTask; }, logger, earlyAck: false);

        await rig.HandleAsync(settle);

        Assert.Equal(1, Volatile.Read(ref handlerRuns));
        Assert.Equal(2, settle.Ack);
        Assert.Equal(0, settle.Nak);
        Assert.Equal(0, settle.DeadLetter);
        Assert.DoesNotContain(logger.Messages, message => message.StartsWith("Failed to ACK", StringComparison.Ordinal));
    }

    /// <summary>
    /// L-03, early ACK: the job is already running on a background worker and nothing renews its
    /// claim, so a lost ACK let a peer run it concurrently once the lease lapsed. Red on 6d7e1aeb.
    /// </summary>
    [Theory]
    [InlineData(Provider.SqlServer)]
    [InlineData(Provider.PostgreSql)]
    [InlineData(Provider.MongoDb)]
    public async Task EarlyAck_OneTransientAckFault_IsRetried(Provider provider)
    {
        var logger = new CollectingLogger();
        var settle = new Settle { AckFaults = 1 };
        var ran = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using (var rig = Rig.Build(provider, _ => { ran.TrySetResult(); return Task.CompletedTask; }, logger, earlyAck: true))
        {
            await rig.HandleAsync(settle);
            await ran.Task.WaitAsync(TimeSpan.FromSeconds(30));
        }

        Assert.Equal(2, settle.Ack);
        Assert.Equal(0, settle.Nak);
        Assert.Equal(0, settle.DeadLetter);
        Assert.DoesNotContain(logger.Messages, message => message.StartsWith("Failed to ACK", StringComparison.Ordinal));
    }

    /// <summary>
    /// L-03: the NAK after a failed handler is retried the same way, so the row comes back after
    /// RedeliveryDelay instead of after the whole lease. Red on 6d7e1aeb: one NAK attempt, then
    /// "Failed to NAK".
    /// </summary>
    [Theory]
    [InlineData(Provider.SqlServer)]
    [InlineData(Provider.PostgreSql)]
    [InlineData(Provider.MongoDb)]
    public async Task NakAfterFailedHandler_OneTransientNakFault_IsRetried(Provider provider)
    {
        var logger = new CollectingLogger();
        var settle = new Settle { NakFaults = 1 };
        await using var rig = Rig.Build(provider, static _ => throw new InvalidOperationException("handler failed"), logger, earlyAck: false);

        await rig.HandleAsync(settle);

        Assert.Equal(2, settle.Nak);
        Assert.Equal(0, settle.Ack);
        Assert.Equal(0, settle.DeadLetter);
        Assert.DoesNotContain(logger.Messages, message => message.StartsWith("Failed to NAK", StringComparison.Ordinal));
    }

    /// <summary>
    /// L-03 bounds: a fault that stays transient gets a bounded number of attempts and is then
    /// swallowed and logged exactly as before; a provider-specific transient fault (Npgsql's
    /// connection break) is classified by the provider's own classifier.
    /// </summary>
    [Theory]
    [InlineData(Provider.SqlServer, false)]
    [InlineData(Provider.PostgreSql, false)]
    [InlineData(Provider.MongoDb, false)]
    [InlineData(Provider.PostgreSql, true)]
    public async Task AckAfterHandler_PersistentTransientFault_IsRetriedABoundedNumberOfTimes(Provider provider, bool npgsqlFault)
    {
        var logger = new CollectingLogger();
        var settle = new Settle
        {
            AckFaults = int.MaxValue,
            Fault = npgsqlFault ? () => new NpgsqlException("connection broken", new IOException("reset")) : null
        };
        await using var rig = Rig.Build(provider, static _ => Task.CompletedTask, logger, earlyAck: false);

        await rig.HandleAsync(settle);

        Assert.Equal(4, settle.Ack);
        Assert.Equal(0, settle.Nak);
        Assert.Single(logger.Messages, message => message.StartsWith("Failed to ACK", StringComparison.Ordinal));
    }

    /// <summary>
    /// L-03 (MongoDB): the MongoDB dispatcher classifies settlement faults with the driver's own
    /// transient classifier, so a connection break on the fenced ACK is retried like Npgsql's —
    /// not only a client-side <see cref="TimeoutException"/>. Red before the override: one attempt.
    /// </summary>
    [Fact]
    public async Task MongoDb_AckAfterHandler_ADriverConnectionFault_IsRetried()
    {
        var logger = new CollectingLogger();
        var connectionId = new MongoDB.Driver.Core.Connections.ConnectionId(
            new MongoDB.Driver.Core.Servers.ServerId(new MongoDB.Driver.Core.Clusters.ClusterId(), new System.Net.DnsEndPoint("localhost", 27017)));
        var settle = new Settle
        {
            AckFaults = 1,
            Fault = () => new MongoDB.Driver.MongoConnectionException(connectionId, "connection reset")
        };
        await using var rig = Rig.Build(Provider.MongoDb, static _ => Task.CompletedTask, logger, earlyAck: false);

        await rig.HandleAsync(settle);

        Assert.Equal(2, settle.Ack);
        Assert.Equal(0, settle.Nak);
        Assert.DoesNotContain(logger.Messages, message => message.StartsWith("Failed to ACK", StringComparison.Ordinal));
    }

    /// <summary>One provider's dispatcher plus a way to hand it a delivery wired to a <see cref="Settle"/>.</summary>
    private sealed class Rig : IAsyncDisposable
    {
        private readonly IAsyncDisposable _dispatcher;
        private readonly Func<Settle, Task> _handle;

        private Rig(IAsyncDisposable dispatcher, Func<Settle, Task> handle)
        {
            _dispatcher = dispatcher;
            _handle = handle;
        }

        public Task HandleAsync(Settle settle) => _handle(settle);

        public ValueTask DisposeAsync() => _dispatcher.DisposeAsync();

        public static Rig Build(Provider provider, Func<CancellationToken, Task> handler, ILogger logger, bool earlyAck)
        {
            var headers = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            switch (provider)
            {
                case Provider.SqlServer:
                {
                    var options = new SqlServerAsyncResponseTransportOptions { ConnectionString = "Server=localhost;Database=unused;User ID=sa;Password=unused;TrustServerCertificate=True" };
                    var subscriber = new SqlServerSubscriberOptions();
                    if (earlyAck)
                        subscriber.UseAckAfterEnqueue(1, 8, TimeSpan.FromSeconds(5));
                    var dispatcher = new SqlServerMessageDispatcher((_, token) => handler(token), options, subscriber, logger, SqlServerSubscriberRole.Worker);
                    return new Rig(dispatcher, s => dispatcher.HandleAsync(
                        new SqlServerTransportDelivery(Guid.NewGuid(), "worker", "{}", headers, 1, s.AckAsync, s.NakAsync, s.DeadLetterAsync, s.RenewAsync),
                        CancellationToken.None));
                }

                case Provider.PostgreSql:
                {
                    var options = new PostgreSqlAsyncResponseTransportOptions();
                    var subscriber = new PostgreSqlSubscriberOptions();
                    if (earlyAck)
                        subscriber.UseAckAfterEnqueue(1, 8, TimeSpan.FromSeconds(5));
                    var dispatcher = new PostgreSqlMessageDispatcher((_, token) => handler(token), options, subscriber, logger, PostgreSqlSubscriberRole.Worker);
                    return new Rig(dispatcher, s => dispatcher.HandleAsync(
                        new PostgreSqlTransportDelivery(Guid.NewGuid(), "worker", "{}", headers, 1, s.AckAsync, s.NakAsync, s.DeadLetterAsync, s.RenewAsync),
                        CancellationToken.None));
                }

                default:
                {
                    var options = new MongoDbAsyncResponseTransportOptions();
                    var subscriber = new MongoDbSubscriberOptions();
                    if (earlyAck)
                        subscriber.UseAckAfterEnqueue(1, 8, TimeSpan.FromSeconds(5));
                    var dispatcher = new MongoDbMessageDispatcher((_, token) => handler(token), options, subscriber, logger, MongoDbSubscriberRole.Worker);
                    return new Rig(dispatcher, s => dispatcher.HandleAsync(
                        new MongoDbTransportDelivery(Guid.NewGuid(), "worker", "{}", headers, 1, s.AckAsync, s.NakAsync, s.DeadLetterAsync, s.RenewAsync),
                        CancellationToken.None));
                }
            }
        }
    }

    /// <summary>Settlement callbacks whose first <see cref="AckFaults"/> / <see cref="NakFaults"/> calls fail transiently.</summary>
    private sealed class Settle
    {
        public int Ack;
        public int Nak;
        public int DeadLetter;
        public int AckFaults;
        public int NakFaults;

        /// <summary>The transient fault to throw; a client-side <see cref="TimeoutException"/> (transient for every provider) by default.</summary>
        public Func<Exception>? Fault;

        public ValueTask AckAsync()
            => Interlocked.Increment(ref Ack) <= AckFaults ? throw (Fault?.Invoke() ?? new TimeoutException("ack round trip lost")) : ValueTask.CompletedTask;

        public ValueTask NakAsync(TimeSpan delay)
            => Interlocked.Increment(ref Nak) <= NakFaults ? throw (Fault?.Invoke() ?? new TimeoutException("release round trip lost")) : ValueTask.CompletedTask;

        public ValueTask<bool> DeadLetterAsync(Exception exception, bool deleteOriginal, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref DeadLetter);
            return ValueTask.FromResult(true);
        }

        public ValueTask<bool> RenewAsync(CancellationToken cancellationToken) => ValueTask.FromResult(true);
    }
}
