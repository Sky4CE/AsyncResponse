using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using System.Linq.Expressions;
using System.Reflection;
using Xunit;

namespace AsyncResponse.Tests;

public sealed class CoreCoverageTests
{
    [Fact]
    public async Task DurableFlowService_ResumeValidatesLoadsAndEnqueuesOnlyRunningFlows()
    {
        var store = new Mock<IFlowStateStore>();
        store
            .SetupSequence(s => s.LoadAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((FlowState?)null)
            .ReturnsAsync(new FlowState { FlowId = "finished", Status = FlowRunStatus.Succeeded })
            .ReturnsAsync(new FlowState { FlowId = "running", Status = FlowRunStatus.Running });
        var services = new ServiceCollection();
        services.AddSingleton(store.Object);
        using var provider = services.BuildServiceProvider();
        var builder = new Mock<IAsyncResponseBuilder>();
        builder
            .Setup(b => b.EnqueueWorkerAsync(
                It.IsAny<Expression<Func<IDurableFlowExecutor, Task>>>(),
                It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        var service = new DurableFlowService(
            provider.GetRequiredService<IServiceScopeFactory>(),
            builder.Object,
            new AsyncResponseContextPropagation([]),
            new DurableFlowOptions(),
            NullLogger<DurableFlowService>.Instance);

        await Assert.ThrowsAsync<ArgumentException>(() => service.ResumeAsync(" "));
        await Assert.ThrowsAsync<InvalidOperationException>(() => service.ResumeAsync("missing"));
        await service.ResumeAsync("finished");
        await service.ResumeAsync("running");

        builder.Verify(b => b.EnqueueWorkerAsync(
            It.IsAny<Expression<Func<IDurableFlowExecutor, Task>>>(),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task FlowStateConcurrency_MutateCoversMissingNoOpContentionAndSuccess()
    {
        var missing = new Mock<IFlowStateStore>();
        missing.Setup(s => s.LoadAsync("missing", It.IsAny<CancellationToken>())).ReturnsAsync((FlowState?)null);
        Assert.False(await FlowStateConcurrency.MutateAsync(
            missing.Object,
            "missing",
            TimeSpan.FromMinutes(1),
            timeProvider: null,
            _ => true));

        var noOp = new Mock<IFlowStateStore>();
        noOp.Setup(s => s.LoadAsync("no-op", It.IsAny<CancellationToken>()))
            .ReturnsAsync(new FlowState { FlowId = "no-op", Revision = 4 });
        Assert.True(await FlowStateConcurrency.MutateAsync(
            noOp.Object,
            "no-op",
            TimeSpan.FromMinutes(1),
            timeProvider: null,
            _ => false));
        noOp.Verify(s => s.TryUpdateAsync(
            It.IsAny<string>(),
            It.IsAny<FlowState>(),
            It.IsAny<long>(),
            It.IsAny<TimeSpan>(),
            It.IsAny<string?>(),
            It.IsAny<CancellationToken>()), Times.Never);

        var success = new Mock<IFlowStateStore>();
        success.Setup(s => s.LoadAsync("success", It.IsAny<CancellationToken>()))
            .ReturnsAsync(new FlowState { FlowId = "success", Revision = 2 });
        success.Setup(s => s.TryUpdateAsync(
                "success",
                It.IsAny<FlowState>(),
                2,
                It.IsAny<TimeSpan>(),
                null,
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);
        Assert.True(await FlowStateConcurrency.MutateAsync(
            success.Object,
            "success",
            TimeSpan.FromMinutes(1),
            timeProvider: null,
            state =>
            {
                state.LastMessage = "updated";
                return true;
            }));

        var contention = new Mock<IFlowStateStore>();
        contention.Setup(s => s.LoadAsync("contended", It.IsAny<CancellationToken>()))
            .ReturnsAsync(() => new FlowState { FlowId = "contended", Revision = 0 });
        contention.Setup(s => s.TryUpdateAsync(
                "contended",
                It.IsAny<FlowState>(),
                0,
                It.IsAny<TimeSpan>(),
                null,
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(false);
        await Assert.ThrowsAsync<InvalidOperationException>(() => FlowStateConcurrency.MutateAsync(
            contention.Object,
            "contended",
            TimeSpan.FromMinutes(1),
            timeProvider: null,
            _ => true));
    }

    [Fact]
    public async Task FlowExecutionLease_RenewsLosesAndContainsReleaseFailure()
    {
        var renewals = 0;
        var store = new Mock<IFlowStateStore>();
        store.Setup(s => s.TryRenewLeaseAsync(
                "flow",
                "lease",
                It.IsAny<TimeSpan>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(() => Interlocked.Increment(ref renewals) < 2);
        store.Setup(s => s.ReleaseLeaseAsync("flow", "lease", It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("release failed"));
        var options = new DurableFlowOptions
        {
            ExecutionLeaseDuration = TimeSpan.FromMilliseconds(100),
            ExecutionLeaseRenewInterval = TimeSpan.FromMilliseconds(5)
        };
        var lease = new FlowExecutionLease(store.Object, "flow", "lease", options, NullLogger.Instance);

        await WaitUntilAsync(() => lease.LostToken.IsCancellationRequested);
        Assert.Throws<InvalidOperationException>(() => lease.ThrowIfLost());
        await lease.DisposeAsync();
        await lease.DisposeAsync();
        InvokePrivate(lease, "MarkLost");

        Assert.True(renewals >= 2);
    }

    [Fact]
    public async Task FlowExecutionLease_MarksLostWhenRenewalErrorsPastExpiry()
    {
        var store = new Mock<IFlowStateStore>();
        store.Setup(s => s.TryRenewLeaseAsync(
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<TimeSpan>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(new TimeoutException("store unavailable"));
        store.Setup(s => s.ReleaseLeaseAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        var lease = new FlowExecutionLease(
            store.Object,
            "flow-expiry",
            "lease-expiry",
            new DurableFlowOptions
            {
                ExecutionLeaseDuration = TimeSpan.FromMilliseconds(20),
                ExecutionLeaseRenewInterval = TimeSpan.FromMilliseconds(5)
            },
            NullLogger.Instance);

        await WaitUntilAsync(() => lease.LostToken.IsCancellationRequested);
        await lease.DisposeAsync();
    }

    [Fact]
    public async Task FlowExecutionLease_SaveRestoresRevisionAndMarksLostOnConflictOrError()
    {
        var conflictStore = new Mock<IFlowStateStore>();
        conflictStore.Setup(s => s.TryUpdateAsync(
                It.IsAny<string>(),
                It.IsAny<FlowState>(),
                It.IsAny<long>(),
                It.IsAny<TimeSpan>(),
                It.IsAny<string?>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(false);
        conflictStore.Setup(s => s.ReleaseLeaseAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        var conflictLease = LongLease(conflictStore.Object, "conflict");
        var conflicted = new FlowState { FlowId = "conflict", Revision = 7 };

        await Assert.ThrowsAsync<InvalidOperationException>(() => conflictLease.SaveAsync(conflicted, TimeSpan.FromMinutes(1)));
        Assert.Equal(7, conflicted.Revision);
        await conflictLease.DisposeAsync();

        var errorStore = new Mock<IFlowStateStore>();
        errorStore.Setup(s => s.TryUpdateAsync(
                It.IsAny<string>(),
                It.IsAny<FlowState>(),
                It.IsAny<long>(),
                It.IsAny<TimeSpan>(),
                It.IsAny<string?>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(new TimeoutException("update failed"));
        errorStore.Setup(s => s.ReleaseLeaseAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        var errorLease = LongLease(errorStore.Object, "error");
        var errored = new FlowState { FlowId = "error", Revision = 3 };

        await Assert.ThrowsAsync<TimeoutException>(() => errorLease.SaveAsync(errored, TimeSpan.FromMinutes(1)));
        Assert.Equal(3, errored.Revision);
        Assert.True(errorLease.LostToken.IsCancellationRequested);
        await errorLease.DisposeAsync();
    }

    [Fact]
    public async Task InMemoryFlowStateStore_CoversExpiryLeaseAndValidationBoundaries()
    {
        var store = new InMemoryFlowStateStore();
        var invalidRevision = State("invalid", revision: 1);
        await Assert.ThrowsAsync<ArgumentException>(() => store.TryCreateAsync("invalid", invalidRevision, TimeSpan.FromMinutes(1)));
        await Assert.ThrowsAsync<ArgumentException>(() => store.TryCreateAsync("other", State("mismatch"), TimeSpan.FromMinutes(1)));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => store.TryCreateAsync("ttl", State("ttl"), TimeSpan.Zero));

        Assert.True(await store.TryCreateAsync("existing", State("existing"), TimeSpan.FromMinutes(1)));
        Assert.False(await store.TryCreateAsync("existing", State("existing"), TimeSpan.FromMinutes(1)));

        // Callers can pass raw ttls (not just validated options); every external store saturates
        // the "now + ttl" stamp, and the in-memory one used to overflow instead.
        Assert.True(await store.TryCreateAsync("huge-ttl", State("huge-ttl"), TimeSpan.MaxValue));
        Assert.NotNull(await store.LoadAsync("huge-ttl"));

        var expiring = State("expiring");
        Assert.True(await store.TryCreateAsync("expiring", expiring, TimeSpan.FromMilliseconds(5)));
        await Task.Delay(20);
        Assert.Null(await store.LoadAsync("expiring"));
        Assert.True(await store.TryCreateAsync("expiring", State("expiring"), TimeSpan.FromMinutes(1)));

        var leased = State("leased");
        Assert.True(await store.TryCreateAsync("leased", leased, TimeSpan.FromMinutes(1)));
        Assert.True(await store.TryAcquireLeaseAsync("leased", "lease-a", TimeSpan.FromMinutes(1)));
        Assert.False(await store.TryAcquireLeaseAsync("leased", "lease-b", TimeSpan.FromMinutes(1)));
        Assert.False(await store.TryRenewLeaseAsync("leased", "lease-b", TimeSpan.FromMinutes(1)));
        Assert.True(await store.TryRenewLeaseAsync("leased", "lease-a", TimeSpan.FromMinutes(1)));

        var update = State("leased", revision: 1);
        Assert.False(await store.TryUpdateAsync("leased", update, 0, TimeSpan.FromMinutes(1), "lease-b"));
        Assert.True(await store.TryUpdateAsync("leased", update, 0, TimeSpan.FromMinutes(1), "lease-a"));
        Assert.False(await store.TryUpdateAsync("leased", State("leased", revision: 1), 0, TimeSpan.FromMinutes(1), "lease-a"));
        Assert.False(await store.TryUpdateAsync("missing", State("missing", revision: 1), 0, TimeSpan.FromMinutes(1)));

        await store.ReleaseLeaseAsync("leased", "not-owner");
        await store.ReleaseLeaseAsync("leased", "lease-a");
        Assert.True(await store.TryAcquireLeaseAsync("leased", "lease-b", TimeSpan.FromMinutes(1)));
        Assert.False(await store.TryRenewLeaseAsync("missing", "lease", TimeSpan.FromMinutes(1)));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            () => store.TryAcquireLeaseAsync("leased", "lease", TimeSpan.Zero));

        var expiredLease = State("expired-lease");
        Assert.True(await store.TryCreateAsync("expired-lease", expiredLease, TimeSpan.FromMilliseconds(5)));
        await Task.Delay(20);
        Assert.False(await store.TryAcquireLeaseAsync("expired-lease", "lease", TimeSpan.FromMinutes(1)));
    }

    [Fact]
    public async Task InMemoryFlowStateStore_CoversMalformedReadsExpiredLeaseTakeoverAndRemainingValidation()
    {
        var store = new InMemoryFlowStateStore();

        AddRawFlowEntry(store, "malformed", "{not-json", revision: 0);
        AddRawFlowEntry(store, "null-json", "null", revision: 0);
        AddRawFlowEntry(store, "revision-mismatch", FlowStateJson.Serialize(State("revision-mismatch")), revision: 1);
        AddRawFlowEntry(store, "flow-mismatch", FlowStateJson.Serialize(State("different-id")), revision: 0);

        Assert.Null(await store.LoadAsync("malformed"));
        Assert.Null(await store.LoadAsync("null-json"));
        Assert.Null(await store.LoadAsync("revision-mismatch"));
        Assert.Null(await store.LoadAsync("flow-mismatch"));
        Assert.Null(await store.LoadAsync("missing"));
        await Assert.ThrowsAsync<ArgumentException>(() => store.LoadAsync(" "));

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            store.TryUpdateAsync("missing", State("missing", revision: 0), -1, TimeSpan.FromMinutes(1)));
        await Assert.ThrowsAsync<ArgumentException>(() =>
            store.TryUpdateAsync("missing", State("missing", revision: 4), 0, TimeSpan.FromMinutes(1)));
        await Assert.ThrowsAsync<ArgumentException>(() =>
            store.TryCreateAsync("schema", new FlowState
            {
                FlowId = "schema",
                SchemaVersion = FlowStateSchema.Current + 1
            }, TimeSpan.FromMinutes(1)));

        Assert.True(await store.TryCreateAsync("lease-expiry", State("lease-expiry"), TimeSpan.FromMinutes(1)));
        Assert.True(await store.TryAcquireLeaseAsync("lease-expiry", "owner-a", TimeSpan.FromMilliseconds(5)));
        Assert.True(await store.TryAcquireLeaseAsync("lease-expiry", "owner-a", TimeSpan.FromMinutes(1)));
        Assert.True(await store.TryAcquireLeaseAsync("lease-expiry", "owner-a", TimeSpan.FromMilliseconds(5)));
        await Task.Delay(20);
        Assert.False(await store.TryRenewLeaseAsync("lease-expiry", "owner-a", TimeSpan.FromMinutes(1)));
        Assert.True(await store.TryAcquireLeaseAsync("lease-expiry", "owner-b", TimeSpan.FromMinutes(1)));

        await store.ReleaseLeaseAsync("missing", "owner");
        Assert.False(await store.TryDeleteAsync("missing"));
        Assert.True(await store.TryDeleteAsync("lease-expiry"));
        Assert.False(await store.TryDeleteAsync("lease-expiry"));

        using var canceled = new CancellationTokenSource();
        await canceled.CancelAsync();
        await Assert.ThrowsAsync<OperationCanceledException>(() =>
            store.TryCreateAsync("canceled", State("canceled"), TimeSpan.FromMinutes(1), canceled.Token));
        await Assert.ThrowsAsync<OperationCanceledException>(() =>
            store.TryAcquireLeaseAsync("missing", "owner", TimeSpan.FromMinutes(1), canceled.Token));
        await Assert.ThrowsAsync<OperationCanceledException>(() =>
            store.ReleaseLeaseAsync("missing", "owner", canceled.Token));
        await Assert.ThrowsAsync<OperationCanceledException>(() =>
            store.TryDeleteAsync("missing", canceled.Token));
    }

    [Fact]
    public void FlowStateJson_HandlesMalformedNullAndEquivalentDocuments()
    {
        Assert.Null(FlowStateJson.Deserialize("{not-json"));
        Assert.False(FlowStateJson.JsonEquivalent(null, "{}"));
        Assert.True(FlowStateJson.JsonEquivalent("{\"a\":1,\"b\":2}", "{\"b\":2,\"a\":1}"));
        Assert.False(FlowStateJson.JsonEquivalent("{not-json", "{}"));
    }

    private static FlowExecutionLease LongLease(IFlowStateStore store, string flowId)
        => new(
            store,
            flowId,
            $"lease-{flowId}",
            new DurableFlowOptions
            {
                ExecutionLeaseDuration = TimeSpan.FromHours(2),
                ExecutionLeaseRenewInterval = TimeSpan.FromHours(1)
            },
            NullLogger.Instance);

    private static FlowState State(string flowId, long revision = 0) => new()
    {
        FlowId = flowId,
        Revision = revision,
        Status = FlowRunStatus.Running
    };

    private static void AddRawFlowEntry(
        InMemoryFlowStateStore store,
        string flowId,
        string json,
        long revision)
    {
        var storeType = typeof(InMemoryFlowStateStore);
        var entryType = storeType.GetNestedType("Entry", BindingFlags.NonPublic)!;
        var entry = Activator.CreateInstance(
            entryType,
            BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic,
            binder: null,
            [json, revision, DateTime.UtcNow.AddMinutes(1), null, null],
            culture: null)!;
        var entries = storeType.GetField("_entries", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(store)!;
        var tryAdd = entries.GetType().GetMethod("TryAdd", [typeof(string), entryType])!;

        Assert.True((bool)tryAdd.Invoke(entries, [flowId, entry])!);
    }

    private static void InvokePrivate(object target, string name)
        => target.GetType().GetMethod(name, BindingFlags.Instance | BindingFlags.NonPublic)!.Invoke(target, null);

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow.AddSeconds(2);
        while (!condition() && DateTime.UtcNow < deadline)
            await Task.Delay(5);
        Assert.True(condition());
    }
}
