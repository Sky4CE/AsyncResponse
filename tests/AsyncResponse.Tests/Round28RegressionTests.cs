using AsyncResponse.Testing;
using AsyncResponse.Transports.NATS;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Regressions for round 28 (two independent external reviews). Each test drives the defect's real
/// path and fails on the pre-fix build.
/// </summary>
public sealed class Round28RegressionTests
{
    // -----------------------------------------------------------------------------------------
    // Finding — the lease deadline was stamped AFTER the store round trip returned, so response
    // latency was handed back to the client as lease time it did not own.
    // -----------------------------------------------------------------------------------------

    /// <summary>A store whose lease calls take a controllable amount of (virtual) time to answer.</summary>
    private sealed class SlowLeaseStore(VirtualTimeProvider _clock, TimeSpan _latency) : IFlowStateStore
    {
        private readonly InMemoryFlowStateStore _inner = new();

        public Task<bool> TryAcquireLeaseAsync(string flowId, string leaseId, TimeSpan leaseDuration, CancellationToken cancellationToken = default)
        {
            // The server started the lease when it ran the command; the answer only reaches us now.
            _clock.Advance(_latency);
            return Task.FromResult(true);
        }

        public Task<bool> TryRenewLeaseAsync(string flowId, string leaseId, TimeSpan leaseDuration, CancellationToken cancellationToken = default)
        {
            _clock.Advance(_latency);
            return Task.FromResult(true);
        }

        public Task<FlowState?> LoadAsync(string flowId, CancellationToken cancellationToken = default)
            => _inner.LoadAsync(flowId, cancellationToken);
        public Task<bool> TryCreateAsync(string flowId, FlowState state, TimeSpan ttl, CancellationToken cancellationToken = default)
            => _inner.TryCreateAsync(flowId, state, ttl, cancellationToken);
        public Task<bool> TryUpdateAsync(string flowId, FlowState state, long expectedRevision, TimeSpan ttl, string? leaseId = null, CancellationToken cancellationToken = default)
            => _inner.TryUpdateAsync(flowId, state, expectedRevision, ttl, leaseId, cancellationToken);
        public Task ReleaseLeaseAsync(string flowId, string leaseId, CancellationToken cancellationToken = default)
            => _inner.ReleaseLeaseAsync(flowId, leaseId, cancellationToken);
        public Task<bool> TryDeleteAsync(string flowId, CancellationToken cancellationToken = default)
            => _inner.TryDeleteAsync(flowId, cancellationToken);
    }

    [Fact]
    public async Task LeaseAcquiredSlowly_ExpiresOnTheServersClock_NotTheResponsesArrivalTime()
    {
        // 30s lease, 20s to hear back. The lease is already 20s old the moment it lands here, so it
        // has 10s left — not 30. Stamping "now + duration" after the round trip claimed ownership
        // for 20s past the point another replica was free to take the flow over.
        var clock = new VirtualTimeProvider();
        var options = new DurableFlowOptions
        {
            ExecutionLeaseDuration = TimeSpan.FromSeconds(30),
            ExecutionLeaseRenewInterval = TimeSpan.FromSeconds(25),
        };
        var store = new SlowLeaseStore(clock, TimeSpan.FromSeconds(20));

        await using var lease = await FlowStateConcurrency.TryAcquireExecutionLeaseAsync(
            store, "flow-slow-acquire", options, NullLogger.Instance, clock);

        Assert.NotNull(lease);

        // 25s of the server's 30 gone: still owned.
        clock.Advance(TimeSpan.FromSeconds(5));
        lease!.ThrowIfLost();

        // Past the server's deadline. The old code thought it had until T+50.
        clock.Advance(TimeSpan.FromSeconds(6));
        var lost = Assert.Throws<InvalidOperationException>(() => lease.ThrowIfLost());
        Assert.Contains("lost its execution lease", lost.Message, StringComparison.Ordinal);
    }

    // -----------------------------------------------------------------------------------------
    // Finding — the start-publish retry excluded every OperationCanceledException, so a transport
    // timeout (TaskCanceledException, caller's token untouched) got zero retries.
    // -----------------------------------------------------------------------------------------

    public sealed record R28Input(string Name);

    public sealed class R28Flow : IDurableFlow<R28Input>
    {
        public Task ExecuteAsync(IDurableFlowContext flow, R28Input input) => Task.CompletedTask;
    }

    private sealed class TimingOutWorkerTransport(int _failures) : IWorkerTransport
    {
        public int PublishAttempts;

        public Task PublishAsync(WorkerJobEnvelope job, CancellationToken cancellationToken = default)
        {
            if (Interlocked.Increment(ref PublishAttempts) <= _failures)
            {
                // How brokers and cloud SDKs report a timeout: a cancelled task whose token is the
                // SDK's own, not the caller's.
                throw new TaskCanceledException("The operation was canceled.", null, new CancellationTokenSource(0).Token);
            }

            return Task.CompletedTask;
        }
    }

    private static ServiceProvider BuildStartProvider(IWorkerTransport transport, VirtualTimeProvider clock)
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddSingleton<TimeProvider>(clock);
        services
            .AddAsyncResponse()
            .WithInMemoryChannel()
            .WithInMemoryDurableFlows()
            .WithDurableFlow<R28Flow, R28Input>();
        services.AddSingleton(transport);
        return services.BuildServiceProvider();
    }

    [Fact]
    public async Task TransportTimeoutOnStart_IsRetried_NotMistakenForCallerCancellation()
    {
        var clock = new VirtualTimeProvider();
        var transport = new TimingOutWorkerTransport(2);
        await using var provider = BuildStartProvider(transport, clock);
        var flows = provider.GetRequiredService<IDurableFlows>();

        var start = flows.StartAsync<R28Flow, R28Input>(new R28Input("acme"));
        for (var i = 0; i < 8 && !start.IsCompleted; i++)
        {
            clock.Advance(TimeSpan.FromSeconds(5));
            await Task.Yield();
        }

        // The third attempt lands, so the start succeeds instead of orphaning a Running ledger.
        var flowId = await start;
        Assert.False(string.IsNullOrWhiteSpace(flowId));
        Assert.Equal(3, Volatile.Read(ref transport.PublishAttempts));
    }

    [Fact]
    public async Task CallerCancellationOnStart_StillEndsTheLadderImmediately()
    {
        // The other half: the caller's own cancellation must not burn the backoff ladder.
        var clock = new VirtualTimeProvider();
        var transport = new TimingOutWorkerTransport(int.MaxValue);
        await using var provider = BuildStartProvider(transport, clock);
        var flows = provider.GetRequiredService<IDurableFlows>();

        using var canceled = new CancellationTokenSource();
        await canceled.CancelAsync();

        await Assert.ThrowsAnyAsync<Exception>(
            () => flows.StartAsync<R28Flow, R28Input>(new R28Input("acme"), flowId: null, canceled.Token));

        // Cancelled before the ladder could spend a single retry on it.
        Assert.True(Volatile.Read(ref transport.PublishAttempts) <= 1);
    }

    // -----------------------------------------------------------------------------------------
    // Finding — MaxInboundMessageChars was unvalidated, so 0 or a negative value silently
    // acknowledged every inbound message without dispatching it.
    // -----------------------------------------------------------------------------------------

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public async Task NonPositiveInboundBudget_IsRejectedAtStartup(int limit)
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddAsyncResponse(o => o.MaxInboundMessageChars = limit).WithInMemoryChannel().WithInMemoryTransport().WithInMemoryDurableFlows();
        await using var provider = services.BuildServiceProvider();
        var validator = provider.GetServices<IHostedService>().OfType<AsyncResponseStartupValidator>().Single();

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => validator.StartAsync(CancellationToken.None));

        Assert.Contains(nameof(AsyncResponseOptions.MaxInboundMessageChars), ex.Message, StringComparison.Ordinal);
        Assert.Contains("must be positive", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task NullInboundBudget_MeansUnbounded_AndStillStarts()
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddAsyncResponse(o => o.MaxInboundMessageChars = null).WithInMemoryChannel().WithInMemoryTransport().WithInMemoryDurableFlows();
        await using var provider = services.BuildServiceProvider();
        var validator = provider.GetServices<IHostedService>().OfType<AsyncResponseStartupValidator>().Single();

        await validator.StartAsync(CancellationToken.None);
    }

    [Fact]
    public async Task OverBudgetIsAnsweredBeforeTheBodyIsWorthParsing()
    {
        // The seam every response subscriber consults before extracting a correlation id from the
        // body — which parses the whole payload. Asking the ingress first is what moves the budget
        // ahead of that allocation instead of behind it.
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddAsyncResponse(o => o.MaxInboundMessageChars = 64).WithInMemoryChannel();
        await using var provider = services.BuildServiceProvider();
        var ingress = provider.GetRequiredService<IAsyncResponseIngress>();

        Assert.False(ingress.IsOverInboundBudget(new string('x', 64)));
        Assert.True(ingress.IsOverInboundBudget(new string('x', 65)));
    }

    [Fact]
    public async Task NoBudgetConfigured_NeverReportsOverBudget()
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddAsyncResponse(o => o.MaxInboundMessageChars = null).WithInMemoryChannel().WithInMemoryTransport().WithInMemoryDurableFlows();
        await using var provider = services.BuildServiceProvider();
        var ingress = provider.GetRequiredService<IAsyncResponseIngress>();

        Assert.False(ingress.IsOverInboundBudget(new string('x', 10_000_000)));
    }

    // -----------------------------------------------------------------------------------------
    // Finding — an inbound JSON property name reached logs through the duplicate-key error.
    // -----------------------------------------------------------------------------------------

    [Fact]
    public void DuplicatePropertyError_DoesNotEchoTheInboundPropertyName()
    {
        const string secret = "x-tenant-acme-bearer-9f3c1d2e";
        var body = $$"""{"{{secret}}":1,"{{secret}}":2,"correlationId":"cid"}""";

        // Reached through one transport's copy of the shared walker (the type is compiled into every
        // transport assembly, so it has to be named through exactly one of them).
        //
        // The walker no longer THROWS for a duplicate key — that made an unroutable message a
        // handler failure, which on RabbitMQ's default cap of 0 requeued forever. It reports the id
        // as unresolvable instead, and the ingress acknowledges the message. The property name is
        // still never surfaced anywhere, which is what this regression is about.
        var extracted = NatsCorrelationIdExtractor.Extract(
            headers: null,
            body,
            new AsyncResponse.Transports.NATS.NatsAsyncResponseTransportOptions { CorrelationIdJsonPaths = ["correlationId"] });

        Assert.Null(extracted);
    }

    // -----------------------------------------------------------------------------------------
    // Finding — watchdog jitter was validated separately from the delay it is added to, so two
    // legal values could arm an out-of-range timer; and jitter could exceed Interval, scheduling a
    // healthy scan past the freshness budget the health check derives from Interval.
    // -----------------------------------------------------------------------------------------

    [Fact]
    public void WatchdogJitter_IsValidatedAgainstTheDelayItIsAddedTo()
    {
        // Each part is legal on its own; their sum is not, and NextWait arms the sum.
        var options = new AsyncResponseWatchdogOptions
        {
            Interval = TimeSpan.FromDays(40),
            StartupDelay = TimeSpan.FromDays(40),
            IntervalJitter = TimeSpan.FromDays(40),
        };

        AsyncResponseChannelOptions.EnsureTimerBacked(options.Interval, "probe", "Interval");
        AsyncResponseChannelOptions.EnsureTimerBackedAllowZero(options.StartupDelay, "probe", "StartupDelay");
        AsyncResponseChannelOptions.EnsureTimerBackedAllowZero(options.ResolvedJitter, "probe", "Jitter");

        Assert.Throws<InvalidOperationException>(options.Validate);
    }

    [Fact]
    public void WatchdogJitter_CannotExceedTheInterval()
    {
        var options = new AsyncResponseWatchdogOptions
        {
            Interval = TimeSpan.FromMinutes(30),
            IntervalJitter = TimeSpan.FromHours(4),
        };

        var ex = Assert.Throws<InvalidOperationException>(options.Validate);
        Assert.Contains("cannot exceed", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void WatchdogDefaults_StayValid()
    {
        new AsyncResponseWatchdogOptions().Validate();
        new AsyncResponseWatchdogOptions { Interval = TimeSpan.FromHours(6), IntervalJitter = TimeSpan.Zero }.Validate();
    }

    // -----------------------------------------------------------------------------------------
    // Finding — a disposed resolver handle still held its delegate, and unregistering left already
    // RESOLVED entries in the positive caches, so a revoked alias kept serving its old type.
    // -----------------------------------------------------------------------------------------

    public interface IR28Probe { Task PingAsync(int value); }

    public sealed class R28Probe : IR28Probe { public Task PingAsync(int value) => Task.CompletedTask; }

    [Fact]
    public void DisposingAResolver_RevokesAliasesItAlreadyResolved()
    {
        var alias = $"R28.Alias.{Guid.NewGuid():N}";
        var registration = AsyncResponseTypeResolution.RegisterResolver(
            name => name == alias ? typeof(IR28Probe) : null);
        try
        {
            // Resolve once so the POSITIVE cache is populated — that is the entry the old
            // unregister left behind.
            Assert.Same(typeof(IR28Probe), ReflectionExtensions.ResolveServiceType(alias));

            registration.Dispose();

            // With the resolver gone the alias must stop resolving. It kept resolving from cache.
            Assert.Null(ReflectionExtensions.ResolveServiceType(alias));
        }
        finally
        {
            registration.Dispose();
            AsyncResponseTypeResolution.Reset();
        }
    }

    [Fact]
    public void DisposingAResolver_ReleasesItEvenWhileTheHandleIsRetained()
    {
        var (weakTarget, handle) = RegisterResolverCapturingAThrowawayTarget();

        handle.Dispose();

        for (var i = 0; i < 10 && weakTarget.IsAlive; i++)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
        }

        // The handle is still rooted by this stack frame — deliberately, because that is exactly
        // how a plugin host keeps one. It must not be a path back to the closure.
        GC.KeepAlive(handle);
        Assert.False(weakTarget.IsAlive, "The disposed handle still held its resolver delegate.");
    }

    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.NoInlining)]
    private static (WeakReference Target, IDisposable Handle) RegisterResolverCapturingAThrowawayTarget()
    {
        var captured = new object();
        var weak = new WeakReference(captured);
        var handle = AsyncResponseTypeResolution.RegisterResolver(name =>
        {
            GC.KeepAlive(captured);
            return null;
        });
        return (weak, handle);
    }

    // -----------------------------------------------------------------------------------------
    // Follow-up — the all-unreadable rule must not fire for a row that is perfectly readable and
    // simply belongs to a DIFFERENT correlation id. A legacy case-insensitive collation returns
    // "LEGACY-CI"'s row for a "legacy-ci" query; the ordinal re-check refuses it, and for the id
    // actually asked about that is absence, not corruption.
    // -----------------------------------------------------------------------------------------

    [Fact]
    public async Task RegistrationBelongingToAnotherCorrelationId_ReadsAsAbsent_NotAsUnreadable()
    {
        var kv = new FakeNatsKvStore();
        var clock = new TestTimeProvider();
        var store = new AsyncResponse.Channels.NATS.NatsRecoveryStateStore(
            kv,
            Options.Create(new AsyncResponse.Channels.NATS.NatsAsyncResponseChannelOptions()),
            NullLogger<AsyncResponse.Channels.NATS.NatsRecoveryStateStore>.Instance,
            clock);

        // One stored registration, fully readable, but registered under a different (upper-case) id.
        kv.Entries[AsyncResponse.Channels.NATS.NatsSubjectSchema.RecoveryKey("legacy-ci")] =
            System.Text.Json.JsonSerializer.Serialize(
                new AsyncResponse.Channels.NATS.NatsRecoveryStateStore.StoredRecoveryState
                {
                    States = [new RecoveryState { CorrelationId = "LEGACY-CI", RegistrationId = Guid.NewGuid() }],
                    ExpiresAtUtc = clock.Now + TimeSpan.FromMinutes(5),
                });

        // Empty, not a throw: nothing is armed for "legacy-ci", and failing the delivery would
        // redeliver a response that has no callback to reach.
        Assert.Empty(await store.GetAllAsync("legacy-ci"));
    }

    [Fact]
    public async Task MixOfForeignAndUnreadable_StillFails_BecauseTheUnreadableOneIsOurs()
    {
        var kv = new FakeNatsKvStore();
        var clock = new TestTimeProvider();
        var store = new AsyncResponse.Channels.NATS.NatsRecoveryStateStore(
            kv,
            Options.Create(new AsyncResponse.Channels.NATS.NatsAsyncResponseChannelOptions()),
            NullLogger<AsyncResponse.Channels.NATS.NatsRecoveryStateStore>.Instance,
            clock);

        kv.Entries[AsyncResponse.Channels.NATS.NatsSubjectSchema.RecoveryKey("mixed-ci")] =
            System.Text.Json.JsonSerializer.Serialize(
                new AsyncResponse.Channels.NATS.NatsRecoveryStateStore.StoredRecoveryState
                {
                    States =
                    [
                        new RecoveryState { CorrelationId = "MIXED-CI", RegistrationId = Guid.NewGuid() },
                        new RecoveryState { CorrelationId = "mixed-ci", RegistrationId = Guid.NewGuid(), SchemaVersion = RecoveryStateSchema.Current + 1 },
                    ],
                    ExpiresAtUtc = clock.Now + TimeSpan.FromMinutes(5),
                });

        // The second row IS this id's, and it is unreadable — so the response still must not be
        // acknowledged. Only the foreign row is discounted.
        var ex = await Assert.ThrowsAsync<RecoveryStateUnreadableException>(() => store.GetAllAsync("mixed-ci"));
        Assert.Equal(1, ex.UnreadableCount);
    }
}
