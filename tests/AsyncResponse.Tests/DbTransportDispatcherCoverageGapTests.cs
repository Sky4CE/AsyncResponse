using AsyncResponse.Transports.MongoDB;
using AsyncResponse.Transports.PostgreSQL;
using AsyncResponse.Transports.SqlServer;
using Microsoft.Extensions.Logging;
using System.Reflection;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Settlement and drain paths of the shared <c>DbMessageDispatcherBase</c>
/// (<c>src/Transports/Shared/DbTransportShared.cs</c>) that the per-provider suites reach for one
/// provider only, or not at all. The dispatcher compiles separately into the MongoDB, PostgreSQL and
/// SQL Server transport assemblies, so every fact runs against all three copies.
/// </summary>
public sealed class DbTransportDispatcherCoverageGapTests
{
    public enum Provider
    {
        SqlServer,
        PostgreSql,
        MongoDb
    }

    /// <summary>
    /// The pre-execution attempt cap with <c>DeadLetterEnabled = false</c>: the store drops the row
    /// instead of burying it, and the line says so rather than claim a dead-letter copy — the handler
    /// is never run.
    /// </summary>
    [Theory]
    [InlineData(Provider.SqlServer)]
    [InlineData(Provider.PostgreSql)]
    [InlineData(Provider.MongoDb)]
    public async Task OverCapDelivery_WithDeadLetteringDisabled_IsDroppedAndSaysNoCopyIsWritten(Provider provider)
    {
        var logger = new HookLogger();
        var settle = new Settle();
        var handlerRuns = 0;
        await using var rig = Rig.Build(
            provider,
            _ => { Interlocked.Increment(ref handlerRuns); return Task.CompletedTask; },
            logger,
            new RigOptions { MaxDeliveryAttempts = 2, DeadLetterEnabled = false });

        await rig.HandleAsync(settle, attempt: 3);

        Assert.Equal(0, Volatile.Read(ref handlerRuns));
        Assert.Equal(1, settle.DeadLetter);
        Assert.Equal(0, settle.Ack);
        Assert.Equal(0, settle.Nak);
        Assert.Contains(logger.Messages, message => message.Contains("dropping it without executing it", StringComparison.Ordinal)
            && message.Contains("DeadLetterEnabled is false", StringComparison.Ordinal));
        Assert.DoesNotContain(logger.Messages, message => message.Contains("dead-lettering without executing it", StringComparison.Ordinal));
    }

    /// <summary>
    /// A post-handler ACK that fails is logged and swallowed: the handler's side effects already
    /// happened, so neither a NAK nor a dead-letter may follow, and the claim loop must not see a
    /// throw — the lease lapses on its own.
    /// </summary>
    [Theory]
    [InlineData(Provider.SqlServer)]
    [InlineData(Provider.PostgreSql)]
    [InlineData(Provider.MongoDb)]
    public async Task AckFailure_AfterASuccessfulHandler_IsLoggedAndNotTurnedIntoAFailure(Provider provider)
    {
        var logger = new HookLogger();
        var settle = new Settle { AckThrows = true };
        var handlerRuns = 0;
        await using var rig = Rig.Build(
            provider,
            _ => { Interlocked.Increment(ref handlerRuns); return Task.CompletedTask; },
            logger,
            new RigOptions());

        await rig.HandleAsync(settle);

        Assert.Equal(1, Volatile.Read(ref handlerRuns));
        Assert.Equal(1, settle.Ack);
        Assert.Equal(0, settle.Nak);
        Assert.Equal(0, settle.DeadLetter);
        var entry = Assert.Single(logger.Entries, entry => entry.Message.StartsWith("Failed to ACK", StringComparison.Ordinal));
        Assert.Contains("after a successful handler", entry.Message, StringComparison.Ordinal);
        Assert.IsType<InvalidOperationException>(entry.Exception);
    }

    /// <summary>
    /// A claim that lands after host stop is handed straight back with a NAK; when that NAK itself
    /// fails it is logged and swallowed (the lease lapses to the same effect) — never started, and
    /// never thrown out of the claim loop.
    /// </summary>
    [Theory]
    [InlineData(Provider.SqlServer)]
    [InlineData(Provider.PostgreSql)]
    [InlineData(Provider.MongoDb)]
    public async Task HostStopHandBack_WhoseNakThrows_IsLoggedAndSwallowed(Provider provider)
    {
        using var hostStopping = new CancellationTokenSource();
        await hostStopping.CancelAsync();
        var logger = new HookLogger();
        var settle = new Settle { NakThrows = true };
        var handlerRuns = 0;
        await using var rig = Rig.Build(
            provider,
            _ => { Interlocked.Increment(ref handlerRuns); return Task.CompletedTask; },
            logger,
            new RigOptions { HostStopping = hostStopping.Token });

        await rig.HandleAsync(settle);

        Assert.Equal(0, Volatile.Read(ref handlerRuns));
        Assert.Equal(1, settle.Nak);
        Assert.Equal(TimeSpan.Zero, settle.LastNakDelay);
        Assert.Equal(0, settle.Ack);
        Assert.Contains(logger.Messages, message => message.StartsWith("Failed to NAK", StringComparison.Ordinal)
            && message.Contains("while stopping", StringComparison.Ordinal));
    }

    /// <summary>
    /// Early ACK with <c>DeadLetterEnabled = false</c>: a background handler failure cannot leave a
    /// dead-letter copy, so the dispatcher says the failure is only observable via logs and
    /// OnBackgroundFailure — and the callback is handed the delivery's real attempt number.
    /// </summary>
    [Theory]
    [InlineData(Provider.SqlServer)]
    [InlineData(Provider.PostgreSql)]
    [InlineData(Provider.MongoDb)]
    public async Task EarlyAck_BackgroundFailure_WithDeadLetteringDisabled_ReportsNoCopyAndTheAttempt(Provider provider)
    {
        var logger = new HookLogger();
        var settle = new Settle();
        var reported = new TaskCompletionSource<(int Attempt, Exception Error)>(TaskCreationOptions.RunContinuationsAsynchronously);
        await using (var rig = Rig.Build(
            provider,
            static _ => throw new InvalidOperationException("background handler failed"),
            logger,
            new RigOptions
            {
                EarlyAck = true,
                DeadLetterEnabled = false,
                OnBackgroundFailure = (attempt, error) => reported.TrySetResult((attempt, error))
            }))
        {
            await rig.HandleAsync(settle, attempt: 2);
            var (attempt, error) = await reported.Task.WaitAsync(TimeSpan.FromSeconds(30));
            Assert.Equal(2, attempt);
            Assert.Equal("background handler failed", error.Message);
        }

        Assert.Equal(1, settle.Ack);
        Assert.Equal(1, settle.DeadLetter);
        Assert.Contains(logger.Messages, message => message.StartsWith("No dead-letter copy of already-ACKed", StringComparison.Ordinal)
            && message.Contains("DeadLetterEnabled is false", StringComparison.Ordinal));
    }

    /// <summary>
    /// Drain budget lapsed with an entry still queued: the worker routes it instead of starting it,
    /// and when that dead-letter write fails the loss is logged as an Error and still surfaced
    /// through OnBackgroundFailure.
    /// </summary>
    [Theory]
    [InlineData(Provider.SqlServer)]
    [InlineData(Provider.PostgreSql)]
    [InlineData(Provider.MongoDb)]
    public async Task EarlyAck_DrainLapse_WhoseDeadLetterFails_LogsTheLossAndStillReports(Provider provider)
    {
        var logger = new HookLogger();
        var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var firstRunning = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var reports = 0;
        var first = new Settle();
        var queued = new Settle { DeadLetterResult = false };
        var rig = Rig.Build(
            provider,
            _ =>
            {
                firstRunning.TrySetResult();
                return releaseFirst.Task;
            },
            logger,
            new RigOptions
            {
                EarlyAck = true,
                Drain = TimeSpan.FromMilliseconds(80),
                OnBackgroundFailure = (_, _) => Interlocked.Increment(ref reports)
            });
        try
        {
            await rig.HandleAsync(first);
            await firstRunning.Task.WaitAsync(TimeSpan.FromSeconds(30));
            await rig.HandleAsync(queued);
            await rig.DisposeAsync();
        }
        finally
        {
            releaseFirst.TrySetResult();
        }

        await Eventually(() => Volatile.Read(ref reports) >= 1);
        Assert.Equal(1, Volatile.Read(ref reports));
        Assert.Equal(1, queued.DeadLetter);
        Assert.Contains("drain budget lapsed", queued.LastDeadLetterError!.Message, StringComparison.Ordinal);
        Assert.Contains(logger.Messages, message => message.StartsWith("Failed to dead-letter undrained", StringComparison.Ordinal)
            && message.Contains("the drain budget had lapsed", StringComparison.Ordinal));
        Assert.Equal(0, first.DeadLetter);
    }

    /// <summary>
    /// Every background worker finishes — one by faulting — only AFTER the drain budget lapsed, inside
    /// the routing reserve: dispose then routes what the faulted worker left queued (dead-letter +
    /// OnBackgroundFailure) with what is left of the reserve, instead of losing those already-ACKed
    /// entries.
    /// </summary>
    [Theory]
    [InlineData(Provider.SqlServer)]
    [InlineData(Provider.PostgreSql)]
    [InlineData(Provider.MongoDb)]
    public async Task EarlyAck_WorkerFaultingInsideTheRoutingReserve_StillRoutesTheQueuedEntry(Provider provider)
    {
        var workerFault = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var logger = new HookLogger
        {
            // The drain budget has just lapsed (logged after the cancel): the worker dies now,
            // while dispose is about to wait out its routing reserve.
            OnMessage = message =>
            {
                if (message.Contains("did not drain within", StringComparison.Ordinal))
                    workerFault.TrySetException(new InvalidOperationException("worker died"));
            }
        };
        var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var firstRunning = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var reports = 0;
        var queued = new Settle();
        var rig = Rig.Build(
            provider,
            _ =>
            {
                firstRunning.TrySetResult();
                return releaseFirst.Task;
            },
            logger,
            new RigOptions
            {
                EarlyAck = true,
                Drain = TimeSpan.FromMilliseconds(400),
                OnBackgroundFailure = (_, _) => Interlocked.Increment(ref reports)
            });
        try
        {
            await rig.HandleAsync(new Settle());
            await firstRunning.Task.WaitAsync(TimeSpan.FromSeconds(30));
            await rig.HandleAsync(queued);

            // The dispatcher's only worker, as dispose sees it, is the one that will fault.
            rig.BackgroundWorkers[0] = workerFault.Task;
            await rig.DisposeAsync();

            // Dispose returned with the stranded entry already routed.
            Assert.Equal(1, queued.DeadLetter);
            Assert.Contains("No background worker was left", queued.LastDeadLetterError!.Message, StringComparison.Ordinal);
            Assert.Equal(1, Volatile.Read(ref reports));
        }
        finally
        {
            releaseFirst.TrySetResult();
        }

        Assert.Contains(logger.Entries, entry => entry.Message.Contains("drain for", StringComparison.Ordinal)
            && entry.Message.Contains("ended with an error", StringComparison.Ordinal)
            && entry.Exception?.Message == "worker died");
        Assert.Contains(logger.Messages, message => message.Contains("no background worker was left to start it", StringComparison.Ordinal));
        Assert.DoesNotContain(logger.Messages, message => message.Contains("did not finish dead-lettering", StringComparison.Ordinal));
    }

    /// <summary>
    /// Every worker faulted and routing what they left queued outruns the reserve: with dead-lettering
    /// ON the Error names the unfinished dead-letter copies (its DeadLetterEnabled = false twin is
    /// pinned in <see cref="DbTransportSharedCoverageTests"/>).
    /// </summary>
    [Theory]
    [InlineData(Provider.SqlServer)]
    [InlineData(Provider.PostgreSql)]
    [InlineData(Provider.MongoDb)]
    public async Task EarlyAck_StrandedRoutingThatOutrunsTheReserve_SaysTheCopiesWereNotAllWritten(Provider provider)
    {
        var logger = new HookLogger();
        var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var firstRunning = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var queued = new Settle { DeadLetterGate = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously) };
        var rig = Rig.Build(
            provider,
            _ =>
            {
                firstRunning.TrySetResult();
                return releaseFirst.Task;
            },
            logger,
            new RigOptions { EarlyAck = true, Drain = TimeSpan.FromMilliseconds(200) });
        try
        {
            await rig.HandleAsync(new Settle());
            await firstRunning.Task.WaitAsync(TimeSpan.FromSeconds(30));
            await rig.HandleAsync(queued);
            rig.BackgroundWorkers[0] = Task.FromException(new InvalidOperationException("worker died"));

            await rig.DisposeAsync();

            Assert.Equal(1, queued.DeadLetter); // started, never finished within the reserve
            Assert.Contains(logger.Entries, entry => entry.Message.Contains("faulted, and the entries they left queued were not all dead-lettered within", StringComparison.Ordinal)
                && entry.Exception is TimeoutException);
        }
        finally
        {
            queued.DeadLetterGate.TrySetResult(true);
            releaseFirst.TrySetResult();
        }
    }

    /// <summary>
    /// The lease heartbeat's outer guard: a cancellation that surfaces out of the beat itself (here,
    /// arming the beat's timer) ends the loop quietly, and a fault there is observed rather than
    /// left unobserved — either way the handler runs and is ACKed exactly as without a heartbeat.
    /// </summary>
    [Theory]
    [InlineData(Provider.SqlServer, true)]
    [InlineData(Provider.PostgreSql, true)]
    [InlineData(Provider.MongoDb, true)]
    [InlineData(Provider.SqlServer, false)]
    [InlineData(Provider.PostgreSql, false)]
    [InlineData(Provider.MongoDb, false)]
    public async Task LeaseHeartbeat_ThatCannotArmItsTimer_DoesNotDisturbTheDelivery(Provider provider, bool cancellation)
    {
        var clock = new TimerFaultingClock(cancellation
            ? new OperationCanceledException("beat cancelled")
            : new InvalidOperationException("timer unavailable"));
        var settle = new Settle();
        var handlerRuns = 0;
        await using var rig = Rig.Build(
            provider,
            _ => { Interlocked.Increment(ref handlerRuns); return Task.CompletedTask; },
            new HookLogger(),
            new RigOptions { Clock = clock });

        await rig.HandleAsync(settle);

        Assert.True(clock.TimerRequests >= 1, "the heartbeat never tried to arm its beat");
        Assert.Equal(1, Volatile.Read(ref handlerRuns));
        Assert.Equal(1, settle.Ack);
        Assert.Equal(0, settle.Nak);
        Assert.Equal(0, settle.Renew);
    }

    /// <summary>
    /// A claim parked on the full early-ACK queue keeps its lease renewed for the whole park (each
    /// renew that lands dates the lease), and when the dispatcher drains while it is still parked
    /// the write fails closed and the claim is released once — never enqueued, never ACKed.
    /// </summary>
    [Theory]
    [InlineData(Provider.SqlServer)]
    [InlineData(Provider.PostgreSql)]
    [InlineData(Provider.MongoDb)]
    public async Task EarlyAckPark_RenewsWhileParked_AndIsReleasedWhenTheDispatcherDrains(Provider provider)
    {
        var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var firstRunning = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var first = new Settle();
        var queued = new Settle();
        var parked = new Settle();
        var rig = Rig.Build(
            provider,
            _ =>
            {
                firstRunning.TrySetResult();
                return releaseFirst.Task;
            },
            new HookLogger(),
            new RigOptions { EarlyAck = true, Capacity = 1, Drain = TimeSpan.FromSeconds(10), LockTimeout = TimeSpan.FromMilliseconds(300) });
        Task disposing = Task.CompletedTask;
        try
        {
            await rig.HandleAsync(first);
            await firstRunning.Task.WaitAsync(TimeSpan.FromSeconds(30));
            await rig.HandleAsync(queued);
            var park = rig.HandleAsync(parked);

            await Eventually(() => Volatile.Read(ref parked.RenewSucceeded) >= 2);
            Assert.False(park.IsCompleted);

            disposing = rig.DisposeAsync().AsTask();
            await park.WaitAsync(TimeSpan.FromSeconds(30));
        }
        finally
        {
            releaseFirst.TrySetResult();
            await disposing.WaitAsync(TimeSpan.FromSeconds(30));
        }

        Assert.Equal(1, parked.Nak);
        Assert.Equal(0, parked.Ack);
        Assert.Equal(1, first.Ack);
        Assert.Equal(1, queued.Ack);
    }

    /// <summary>
    /// A parked claim's "renewals kept failing past LockTimeout" verdict can land after the park has
    /// already ended (the subscriber stopped meanwhile) and disposed its lease signal: marking a
    /// disposed lease lost is a no-op, and the heartbeat still finishes with its warning instead of
    /// faulting.
    /// </summary>
    [Theory]
    [InlineData(Provider.SqlServer)]
    [InlineData(Provider.PostgreSql)]
    [InlineData(Provider.MongoDb)]
    public async Task EarlyAckPark_LostVerdictLandingAfterTheParkEnded_IsHarmless(Provider provider)
    {
        var clock = new VerdictGateClock();
        var logger = new HookLogger();
        var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var firstRunning = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var parked = new Settle { RenewThrows = true, OnRenewThrow = clock.ArmOnThisThread };
        using var stopping = new CancellationTokenSource();
        var rig = Rig.Build(
            provider,
            _ =>
            {
                firstRunning.TrySetResult();
                return releaseFirst.Task;
            },
            logger,
            new RigOptions { EarlyAck = true, Capacity = 1, Drain = TimeSpan.FromSeconds(10), LockTimeout = TimeSpan.FromMilliseconds(300), Clock = clock });
        try
        {
            await rig.HandleAsync(new Settle());
            await firstRunning.Task.WaitAsync(TimeSpan.FromSeconds(30));
            await rig.HandleAsync(new Settle());
            var park = rig.HandleAsync(parked, cancellationToken: stopping.Token);

            // The heartbeat's renew failed and it is about to judge the lease's age — held there.
            Assert.True(clock.VerdictReached.Wait(TimeSpan.FromSeconds(30)), "the parked claim's renew never failed");

            // The subscriber stops: the park releases the claim and disposes its lease.
            await stopping.CancelAsync();
            await park.WaitAsync(TimeSpan.FromSeconds(30));
            Assert.Equal(1, parked.Nak);

            // Now the verdict lands: past LockTimeout, so the lease is marked lost — on a park that
            // is already gone.
            clock.ReleaseVerdict.Set();
            await Eventually(() => logger.Messages.Any(message => message.Contains("could not be renewed within LockTimeout", StringComparison.Ordinal)));
        }
        finally
        {
            clock.ReleaseVerdict.Set();
            releaseFirst.TrySetResult();
            await rig.DisposeAsync();
        }

        Assert.Equal(0, parked.Ack);
        Assert.Equal(1, parked.Nak);
    }

    /// <summary>
    /// <c>DbTransportHeaders.Materialize</c> on the other lenient shapes a foreign producer can leave in
    /// the headers column: a non-object root degrades to no headers, and a JSON <c>null</c> value is
    /// skipped rather than materialized as a header.
    /// </summary>
    [Theory]
    [InlineData(typeof(SqlServerAsyncResponseTransportOptions))]
    [InlineData(typeof(PostgreSqlAsyncResponseTransportOptions))]
    [InlineData(typeof(MongoDbAsyncResponseTransportOptions))]
    public void HeaderMaterialization_NonObjectRootAndNullValues_DegradeInsteadOfThrowing(Type marker)
    {
        var materialize = marker.Assembly
            .GetType("AsyncResponse.Transports.DbTransportHeaders", throwOnError: true)!
            .GetMethod("Materialize", BindingFlags.Public | BindingFlags.Static)!;
        IReadOnlyDictionary<string, string> Materialize(string json)
            => (IReadOnlyDictionary<string, string>)materialize.Invoke(null, [json])!;

        Assert.Empty(Materialize("""["AR-CorrelationId","abc"]"""));
        Assert.Empty(Materialize("\"just a string\""));

        var headers = Materialize("""{"AR-CorrelationId":"abc","gone":null,"flag":true}""");
        Assert.Equal("abc", headers["AR-CorrelationId"]);
        Assert.Equal("true", headers["flag"]);
        Assert.False(headers.ContainsKey("gone"));
        Assert.Equal(2, headers.Count);
    }

    private static async Task Eventually(Func<bool> condition)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        while (!condition())
            await Task.Delay(10, timeout.Token);
    }

    private sealed record RigOptions
    {
        public bool EarlyAck { get; init; }
        public int Capacity { get; init; } = 8;
        public TimeSpan Drain { get; init; } = TimeSpan.FromSeconds(5);
        public int? MaxDeliveryAttempts { get; init; }
        public bool DeadLetterEnabled { get; init; } = true;
        public TimeSpan LockTimeout { get; init; } = TimeSpan.FromSeconds(30);
        public TimeProvider? Clock { get; init; }
        public CancellationToken HostStopping { get; init; }
        public Action<int, Exception>? OnBackgroundFailure { get; init; }
    }

    /// <summary>One provider's dispatcher plus a way to hand it a delivery wired to a <see cref="Settle"/>.</summary>
    private sealed class Rig : IAsyncDisposable
    {
        private readonly IAsyncDisposable _dispatcher;
        private readonly Func<Settle, int, CancellationToken, Task> _handle;
        private int _disposed;

        private Rig(IAsyncDisposable dispatcher, Func<Settle, int, CancellationToken, Task> handle)
        {
            _dispatcher = dispatcher;
            _handle = handle;
        }

        public Task HandleAsync(Settle settle, int attempt = 1, CancellationToken cancellationToken = default)
            => _handle(settle, attempt, cancellationToken);

        /// <summary>The dispatcher's live early-ACK worker array (the drain waits on what it holds).</summary>
        public Task[] BackgroundWorkers => (Task[])_dispatcher.GetType().BaseType!
            .GetField("_backgroundWorkers", BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(_dispatcher)!;

        public ValueTask DisposeAsync()
            => Interlocked.Exchange(ref _disposed, 1) == 0 ? _dispatcher.DisposeAsync() : ValueTask.CompletedTask;

        public static Rig Build(Provider provider, Func<CancellationToken, Task> handler, ILogger logger, RigOptions o)
        {
            var headers = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            switch (provider)
            {
                case Provider.SqlServer:
                {
                    var options = new SqlServerAsyncResponseTransportOptions
                    {
                        ConnectionString = "Server=localhost;Database=unused;User ID=sa;Password=unused;TrustServerCertificate=True",
                        LockTimeout = o.LockTimeout,
                        DeadLetterEnabled = o.DeadLetterEnabled
                    };
                    var subscriber = new SqlServerSubscriberOptions();
                    if (o.MaxDeliveryAttempts is { } cap)
                        subscriber.MaxDeliveryAttempts = cap;
                    if (o.EarlyAck)
                        subscriber.UseAckAfterEnqueue(1, o.Capacity, o.Drain);
                    if (o.OnBackgroundFailure is { } report)
                        subscriber.OnBackgroundFailure = context => { report(context.Attempt, context.Exception); return ValueTask.CompletedTask; };
                    var dispatcher = new SqlServerMessageDispatcher((_, token) => handler(token), options, subscriber, logger, SqlServerSubscriberRole.Worker, o.Clock, o.HostStopping);
                    return new Rig(dispatcher, (s, attempt, token) => dispatcher.HandleAsync(
                        new SqlServerTransportDelivery(Guid.NewGuid(), "worker", "{}", headers, attempt, s.AckAsync, s.NakAsync, s.DeadLetterAsync, s.RenewAsync),
                        token));
                }

                case Provider.PostgreSql:
                {
                    var options = new PostgreSqlAsyncResponseTransportOptions { LockTimeout = o.LockTimeout, DeadLetterEnabled = o.DeadLetterEnabled };
                    var subscriber = new PostgreSqlSubscriberOptions();
                    if (o.MaxDeliveryAttempts is { } cap)
                        subscriber.MaxDeliveryAttempts = cap;
                    if (o.EarlyAck)
                        subscriber.UseAckAfterEnqueue(1, o.Capacity, o.Drain);
                    if (o.OnBackgroundFailure is { } report)
                        subscriber.OnBackgroundFailure = context => { report(context.Attempt, context.Exception); return ValueTask.CompletedTask; };
                    var dispatcher = new PostgreSqlMessageDispatcher((_, token) => handler(token), options, subscriber, logger, PostgreSqlSubscriberRole.Worker, o.Clock, o.HostStopping);
                    return new Rig(dispatcher, (s, attempt, token) => dispatcher.HandleAsync(
                        new PostgreSqlTransportDelivery(Guid.NewGuid(), "worker", "{}", headers, attempt, s.AckAsync, s.NakAsync, s.DeadLetterAsync, s.RenewAsync),
                        token));
                }

                default:
                {
                    var options = new MongoDbAsyncResponseTransportOptions { LockTimeout = o.LockTimeout, DeadLetterEnabled = o.DeadLetterEnabled };
                    var subscriber = new MongoDbSubscriberOptions();
                    if (o.MaxDeliveryAttempts is { } cap)
                        subscriber.MaxDeliveryAttempts = cap;
                    if (o.EarlyAck)
                        subscriber.UseAckAfterEnqueue(1, o.Capacity, o.Drain);
                    if (o.OnBackgroundFailure is { } report)
                        subscriber.OnBackgroundFailure = context => { report(context.Attempt, context.Exception); return ValueTask.CompletedTask; };
                    var dispatcher = new MongoDbMessageDispatcher((_, token) => handler(token), options, subscriber, logger, MongoDbSubscriberRole.Worker, o.Clock, o.HostStopping);
                    return new Rig(dispatcher, (s, attempt, token) => dispatcher.HandleAsync(
                        new MongoDbTransportDelivery(Guid.NewGuid(), "worker", "{}", headers, attempt, s.AckAsync, s.NakAsync, s.DeadLetterAsync, s.RenewAsync),
                        token));
                }
            }
        }
    }

    /// <summary>Settlement callbacks shared by the three provider delivery records.</summary>
    private sealed class Settle
    {
        public int Ack;
        public int Nak;
        public int DeadLetter;
        public int Renew;
        public int RenewSucceeded;
        public bool AckThrows;
        public bool NakThrows;
        public bool RenewThrows;
        public bool DeadLetterResult = true;
        public TaskCompletionSource<bool>? DeadLetterGate;
        public Action? OnRenewThrow;
        public TimeSpan? LastNakDelay;
        public Exception? LastDeadLetterError;

        public ValueTask AckAsync()
        {
            Interlocked.Increment(ref Ack);
            if (AckThrows)
                throw new InvalidOperationException("ack store unavailable");

            return ValueTask.CompletedTask;
        }

        public ValueTask NakAsync(TimeSpan delay)
        {
            LastNakDelay = delay;
            Interlocked.Increment(ref Nak);
            if (NakThrows)
                throw new InvalidOperationException("release store unavailable");

            return ValueTask.CompletedTask;
        }

        public ValueTask<bool> DeadLetterAsync(Exception exception, bool deleteOriginal, CancellationToken cancellationToken)
        {
            LastDeadLetterError = exception;
            Interlocked.Increment(ref DeadLetter);
            return DeadLetterGate is { } gate ? new ValueTask<bool>(gate.Task) : ValueTask.FromResult(DeadLetterResult);
        }

        public ValueTask<bool> RenewAsync(CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref Renew);
            if (RenewThrows)
            {
                OnRenewThrow?.Invoke();
                throw new InvalidOperationException("lease store unavailable");
            }

            Interlocked.Increment(ref RenewSucceeded);
            return ValueTask.FromResult(true);
        }
    }

    /// <summary>A clock whose timers cannot be armed: every <c>CreateTimer</c> throws the given exception.</summary>
    private sealed class TimerFaultingClock(Exception failure) : TimeProvider
    {
        private int _timerRequests;

        public int TimerRequests => Volatile.Read(ref _timerRequests);

        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
        {
            Interlocked.Increment(ref _timerRequests);
            throw failure;
        }
    }

    /// <summary>
    /// The system clock, except that the first clock read on a thread armed by
    /// <see cref="ArmOnThisThread"/> — the heartbeat judging a parked lease's age right after its
    /// renew failed — waits for <see cref="ReleaseVerdict"/> and then reads far past LockTimeout.
    /// </summary>
    private sealed class VerdictGateClock : TimeProvider
    {
        [ThreadStatic]
        private static VerdictGateClock? t_armedFor;

        public ManualResetEventSlim VerdictReached { get; } = new();

        public ManualResetEventSlim ReleaseVerdict { get; } = new();

        public void ArmOnThisThread() => t_armedFor = this;

        public override long GetTimestamp()
        {
            var now = System.GetTimestamp();
            if (!ReferenceEquals(t_armedFor, this))
                return now;

            t_armedFor = null;
            VerdictReached.Set();
            ReleaseVerdict.Wait(TimeSpan.FromSeconds(30));
            return now + (TimestampFrequency * 3600);
        }
    }

    /// <summary>Collects every rendered line (with its exception) and lets a test react to one as it is logged.</summary>
    private sealed class HookLogger : ILogger
    {
        private readonly List<(string Message, Exception? Exception)> _entries = [];
        private readonly object _gate = new();

        public Action<string>? OnMessage { get; init; }

        public IReadOnlyList<string> Messages
        {
            get { lock (_gate) return _entries.Select(entry => entry.Message).ToArray(); }
        }

        public IReadOnlyList<(string Message, Exception? Exception)> Entries
        {
            get { lock (_gate) return _entries.ToArray(); }
        }

        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull
            => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            var message = formatter(state, exception);
            lock (_gate)
                _entries.Add((message, exception));
            OnMessage?.Invoke(message);
        }
    }
}
