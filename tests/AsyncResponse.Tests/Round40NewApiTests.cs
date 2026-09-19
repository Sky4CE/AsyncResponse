using System.Diagnostics;
using Xunit;
using static AsyncResponse.Tests.Round40FlowLeaseTestSupport;

namespace AsyncResponse.Tests;

/// <summary>
/// Round 40 (2026-09-19) pins for the API the lease fix added:
/// <see cref="IFlowStateStore.ObserveLeaseAsync"/>, <see cref="FlowLeaseObservation"/> and
/// <see cref="DurableFlowLeaseContendedException"/>. The behavioural proof against the old code
/// is <see cref="Round40RegressionTests"/>; these pin the rules the new contract runs on.
/// </summary>
public sealed class Round40NewApiTests
{
    private static readonly DurableFlowOptions ShortLease = new()
    {
        ExecutionLeaseDuration = TimeSpan.FromMilliseconds(120),
        ExecutionLeaseRenewInterval = TimeSpan.FromMilliseconds(30)
    };

    [Fact]
    public async Task InMemoryStore_ReportsThePersistedLeaseRaw()
    {
        var store = new InMemoryFlowStateStore();
        var state = RunnableState("r40-observe");
        await store.TryCreateAsync(state.FlowId!, state, TimeSpan.FromMinutes(5));

        var unheld = await store.ObserveLeaseAsync(state.FlowId!);
        Assert.NotNull(unheld);
        Assert.Null(unheld.LeaseId);
        Assert.Null(unheld.ExpiresAtUtc);

        Assert.True(await store.TryAcquireLeaseAsync(state.FlowId!, "owner", TimeSpan.FromMilliseconds(150)));
        var acquired = await store.ObserveLeaseAsync(state.FlowId!);
        Assert.Equal("owner", acquired!.LeaseId);
        Assert.NotNull(acquired.ExpiresAtUtc);

        Assert.True(await store.TryRenewLeaseAsync(state.FlowId!, "owner", TimeSpan.FromMilliseconds(400)));
        var renewed = await store.ObserveLeaseAsync(state.FlowId!);
        Assert.Equal("owner", renewed!.LeaseId);
        Assert.True(renewed.ExpiresAtUtc > acquired.ExpiresAtUtc, "A renewal must move the persisted expiry forward: that is the liveness evidence.");

        // An expired lease stays on the row until someone acquires or releases it, and is
        // reported as stored — judging expiry is TryAcquireLeaseAsync's job, not the observer's.
        await Task.Delay(TimeSpan.FromMilliseconds(500));
        var lapsed = await store.ObserveLeaseAsync(state.FlowId!);
        Assert.Equal("owner", lapsed!.LeaseId);
        Assert.Equal(renewed.ExpiresAtUtc, lapsed.ExpiresAtUtc);

        await store.ReleaseLeaseAsync(state.FlowId!, "owner");
        Assert.Null((await store.ObserveLeaseAsync(state.FlowId!))!.LeaseId);

        // Absent is "unheld", never null: null means the store cannot report leases at all.
        Assert.Same(FlowLeaseObservation.Unheld, await store.ObserveLeaseAsync("r40-observe-missing"));
        await Assert.ThrowsAnyAsync<ArgumentException>(() => store.ObserveLeaseAsync(" "));
    }

    [Fact]
    public async Task ApplicationOwnedStore_DefaultsToNotReportingLeases()
    {
        IFlowStateStore store = new NonReportingStore(new InMemoryFlowStateStore());
        Assert.Null(await store.ObserveLeaseAsync("any"));
    }

    [Fact]
    public async Task RenewedLease_IsProofOfALiveHolder_AndAcksLongBeforeTheLocalWindow()
    {
        var store = new InMemoryFlowStateStore();
        var state = RunnableState("r40-live-holder");
        await store.TryCreateAsync(state.FlowId!, state, TimeSpan.FromMinutes(5));
        Assert.True(await store.TryAcquireLeaseAsync(state.FlowId!, "live-holder", TimeSpan.FromSeconds(10)));

        using var renewals = new CancellationTokenSource();
        var renewLoop = Task.Run(async () =>
        {
            while (!renewals.IsCancellationRequested)
            {
                await store.TryRenewLeaseAsync(state.FlowId!, "live-holder", TimeSpan.FromSeconds(10));
                await Task.Delay(25);
            }
        });

        // A 20 s local window: acking on the first observed renewal is what returns in a few
        // polls instead of 20 s, which frees the worker slot a duplicate would otherwise pin.
        await using var harness = CreateHarness(store, new DurableFlowOptions
        {
            ExecutionLeaseDuration = TimeSpan.FromSeconds(20),
            ExecutionLeaseRenewInterval = TimeSpan.FromMilliseconds(50)
        });

        var waited = Stopwatch.StartNew();
        try
        {
            await harness.Executor.ExecuteAsync(state.FlowId!).WaitAsync(TimeSpan.FromSeconds(15));
        }
        finally
        {
            renewals.Cancel();
            await renewLoop;
        }

        Assert.True(waited.Elapsed < TimeSpan.FromSeconds(10), $"Acked after {waited.ElapsedMilliseconds} ms; a renewal is observable within a poll or two.");
        var final = await store.LoadAsync(state.FlowId!);
        Assert.Equal(FlowRunStatus.Running, final!.Status);
        Assert.Equal(0, final.Attempts);
        Assert.Equal(0, harness.Flow.Executions);
    }

    [Fact]
    public async Task LeaseTakenOverByAnotherWorker_IsProofOfALiveHolder()
    {
        var expiry = DateTime.UtcNow.AddMinutes(1);
        var store = await ScriptedStore.CreateAsync(
            "r40-takeover",
            poll => poll == 0
                ? new FlowLeaseObservation("dead-owner", expiry)
                // Same expiry on purpose: the changed OWNER alone is the evidence.
                : new FlowLeaseObservation("another-delivery", expiry));
        await using var harness = CreateHarness(store, ShortLease);

        await harness.Executor.ExecuteAsync("r40-takeover").WaitAsync(TimeSpan.FromSeconds(15));

        Assert.Equal(0, harness.Flow.Executions);
        Assert.Equal(2, store.Observations);
    }

    [Fact]
    public async Task UnchangedLease_IsNeverAcknowledged_AndIsWaitedOutToItsPersistedExpiry()
    {
        // The lease in the way expires 600 ms from now — five local windows (150 ms) away.
        var persistedExpiry = DateTime.UtcNow.AddMilliseconds(600);
        var store = await ScriptedStore.CreateAsync(
            "r40-unchanged",
            _ => new FlowLeaseObservation("dead-owner", persistedExpiry));
        await using var harness = CreateHarness(store, ShortLease);

        var waited = Stopwatch.StartNew();
        var contended = await Assert.ThrowsAsync<DurableFlowLeaseContendedException>(
            () => harness.Executor.ExecuteAsync("r40-unchanged").WaitAsync(TimeSpan.FromSeconds(30)));
        waited.Stop();

        Assert.Equal("r40-unchanged", contended.FlowId);
        Assert.Contains("dead-owner", contended.Reason, StringComparison.Ordinal);
        Assert.True(
            waited.Elapsed >= TimeSpan.FromMilliseconds(550),
            $"Gave up after {waited.ElapsedMilliseconds} ms — before the persisted expiry the store reported.");
        Assert.Equal(0, harness.Flow.Executions);
    }

    [Fact]
    public async Task StoreThatCannotReportLeases_NeverAcksContention_ItHandsTheWakeUpBackToTheTransport()
    {
        var inner = new InMemoryFlowStateStore();
        var state = RunnableState("r40-non-reporting");
        await inner.TryCreateAsync(state.FlowId!, state, TimeSpan.FromMinutes(5));
        Assert.True(await inner.TryAcquireLeaseAsync(state.FlowId!, "unknown-holder", TimeSpan.FromMinutes(1)));

        await using var harness = CreateHarness(new NonReportingStore(inner), ShortLease);

        var contended = await Assert.ThrowsAsync<DurableFlowLeaseContendedException>(
            () => harness.Executor.ExecuteAsync(state.FlowId!).WaitAsync(TimeSpan.FromSeconds(30)));

        Assert.Contains(nameof(IFlowStateStore.ObserveLeaseAsync), contended.Reason, StringComparison.Ordinal);
        Assert.Equal(0, harness.Flow.Executions);
        Assert.Equal(FlowRunStatus.Running, (await inner.LoadAsync(state.FlowId!))!.Status);
    }

    [Fact]
    public async Task StoreThatCannotReportLeases_StillTakesOverADeadHoldersLeaseInsideTheLocalWindow()
    {
        var inner = new InMemoryFlowStateStore();
        var state = RunnableState("r40-non-reporting-takeover");
        await inner.TryCreateAsync(state.FlowId!, state, TimeSpan.FromMinutes(5));
        Assert.True(await inner.TryAcquireLeaseAsync(state.FlowId!, "dead-holder", TimeSpan.FromMilliseconds(60)));

        await using var harness = CreateHarness(new NonReportingStore(inner), ShortLease);
        await harness.Executor.ExecuteAsync(state.FlowId!).WaitAsync(TimeSpan.FromSeconds(15));

        Assert.Equal(FlowRunStatus.Succeeded, (await inner.LoadAsync(state.FlowId!))!.Status);
    }

    [Fact]
    public async Task TerminalRunBehindAHeldLease_StillAcksWithoutProof()
    {
        var store = await ScriptedStore.CreateAsync(
            "r40-terminal",
            _ => new FlowLeaseObservation("dead-owner", DateTime.UtcNow.AddMinutes(10)),
            FlowRunStatus.Succeeded);
        await using var harness = CreateHarness(store, ShortLease);

        await harness.Executor.ExecuteAsync("r40-terminal").WaitAsync(TimeSpan.FromSeconds(15));

        Assert.Equal(0, harness.Flow.Executions);
    }

    /// <summary>An application-owned store written before <c>ObserveLeaseAsync</c> existed.</summary>
    private sealed class NonReportingStore(IFlowStateStore inner) : IFlowStateStore
    {
        public Task<bool> TryCreateAsync(string flowId, FlowState state, TimeSpan ttl, CancellationToken cancellationToken = default)
            => inner.TryCreateAsync(flowId, state, ttl, cancellationToken);

        public Task<FlowState?> LoadAsync(string flowId, CancellationToken cancellationToken = default)
            => inner.LoadAsync(flowId, cancellationToken);

        public Task<bool> TryUpdateAsync(string flowId, FlowState state, long expectedRevision, TimeSpan ttl, string? leaseId = null, CancellationToken cancellationToken = default)
            => inner.TryUpdateAsync(flowId, state, expectedRevision, ttl, leaseId, cancellationToken);

        public Task<bool> TryAcquireLeaseAsync(string flowId, string leaseId, TimeSpan leaseDuration, CancellationToken cancellationToken = default)
            => inner.TryAcquireLeaseAsync(flowId, leaseId, leaseDuration, cancellationToken);

        public Task<bool> TryRenewLeaseAsync(string flowId, string leaseId, TimeSpan leaseDuration, CancellationToken cancellationToken = default)
            => inner.TryRenewLeaseAsync(flowId, leaseId, leaseDuration, cancellationToken);

        public Task ReleaseLeaseAsync(string flowId, string leaseId, CancellationToken cancellationToken = default)
            => inner.ReleaseLeaseAsync(flowId, leaseId, cancellationToken);

        public Task<bool> TryDeleteAsync(string flowId, CancellationToken cancellationToken = default)
            => inner.TryDeleteAsync(flowId, cancellationToken);
    }

    /// <summary>
    /// A ledger whose lease this delivery can never win, with the observations scripted per poll
    /// — the executor's verdict then depends on the evidence alone, not on a race with a holder.
    /// </summary>
    private sealed class ScriptedStore(InMemoryFlowStateStore inner, Func<int, FlowLeaseObservation?> script) : IFlowStateStore
    {
        private int _observations;

        public int Observations => Volatile.Read(ref _observations);

        public static async Task<ScriptedStore> CreateAsync(
            string flowId,
            Func<int, FlowLeaseObservation?> script,
            FlowRunStatus status = FlowRunStatus.Running)
        {
            var inner = new InMemoryFlowStateStore();
            var state = RunnableState(flowId);
            state.Status = status;
            await inner.TryCreateAsync(flowId, state, TimeSpan.FromMinutes(5));
            return new ScriptedStore(inner, script);
        }

        public Task<FlowLeaseObservation?> ObserveLeaseAsync(string flowId, CancellationToken cancellationToken = default)
            => Task.FromResult(script(Interlocked.Increment(ref _observations) - 1));

        public Task<bool> TryAcquireLeaseAsync(string flowId, string leaseId, TimeSpan leaseDuration, CancellationToken cancellationToken = default)
            => Task.FromResult(false);

        public Task<bool> TryCreateAsync(string flowId, FlowState state, TimeSpan ttl, CancellationToken cancellationToken = default)
            => inner.TryCreateAsync(flowId, state, ttl, cancellationToken);

        public Task<FlowState?> LoadAsync(string flowId, CancellationToken cancellationToken = default)
            => inner.LoadAsync(flowId, cancellationToken);

        public Task<bool> TryUpdateAsync(string flowId, FlowState state, long expectedRevision, TimeSpan ttl, string? leaseId = null, CancellationToken cancellationToken = default)
            => inner.TryUpdateAsync(flowId, state, expectedRevision, ttl, leaseId, cancellationToken);

        public Task<bool> TryRenewLeaseAsync(string flowId, string leaseId, TimeSpan leaseDuration, CancellationToken cancellationToken = default)
            => inner.TryRenewLeaseAsync(flowId, leaseId, leaseDuration, cancellationToken);

        public Task ReleaseLeaseAsync(string flowId, string leaseId, CancellationToken cancellationToken = default)
            => inner.ReleaseLeaseAsync(flowId, leaseId, cancellationToken);

        public Task<bool> TryDeleteAsync(string flowId, CancellationToken cancellationToken = default)
            => inner.TryDeleteAsync(flowId, cancellationToken);
    }
}
