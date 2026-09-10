using System.Net;
using System.Reflection;
using System.Text.Json;
using AsyncResponse.DurableFlows.Cosmos;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Options;
using Moq;
using Xunit;

namespace AsyncResponse.Tests;

public sealed class CosmosDurableFlowStateStoreTests
{
    [Fact]
    public async Task Store_HandlesCreateAndUpdateConflicts()
    {
        using var harness = new CosmosHarness();
        var state = CreateState("flow");
        var expired = Document(state, DateTime.UtcNow.AddMinutes(-1));
        harness.Container
            .Setup(container => container.CreateItemAsync(
                It.IsAny<CosmosFlowStateDocument>(),
                It.IsAny<PartitionKey?>(),
                It.IsAny<ItemRequestOptions>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(CosmosError(HttpStatusCode.Conflict));
        harness.Reads(expired);
        harness.ReplacesSuccessfully();

        Assert.True(await harness.Store.TryCreateAsync("flow", state, TimeSpan.FromMinutes(1)));

        // An ETag moving under an expired-slot reclaim no longer concedes outright — the TTL purge
        // itself can bump it. A purge race heals on the next attempt's create; only exhaustion (or
        // reading a live document) reports the slot as taken.
        harness.Container
            .SetupSequence(container => container.CreateItemAsync(
                It.IsAny<CosmosFlowStateDocument>(),
                It.IsAny<PartitionKey?>(),
                It.IsAny<ItemRequestOptions>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(CosmosError(HttpStatusCode.Conflict))
            .ReturnsAsync((ItemResponse<CosmosFlowStateDocument>)null!);
        harness.Container
            .Setup(container => container.ReplaceItemAsync(
                It.IsAny<CosmosFlowStateDocument>(),
                It.IsAny<string>(),
                It.IsAny<PartitionKey?>(),
                It.IsAny<ItemRequestOptions>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(CosmosError(HttpStatusCode.PreconditionFailed));
        Assert.True(await harness.Store.TryCreateAsync("flow", state, TimeSpan.FromMinutes(1)));

        // Every attempt conflicting while every replace 412s = the bounded loop exhausts.
        harness.Container
            .Setup(container => container.CreateItemAsync(
                It.IsAny<CosmosFlowStateDocument>(),
                It.IsAny<PartitionKey?>(),
                It.IsAny<ItemRequestOptions>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(CosmosError(HttpStatusCode.Conflict));
        Assert.False(await harness.Store.TryCreateAsync("flow", state, TimeSpan.FromMinutes(1)));

        state.Revision = 1;
        var current = Document(CreateState("flow"), DateTime.UtcNow.AddMinutes(5));
        harness.Reads(current);
        Assert.False(await harness.Store.TryUpdateAsync(
            "flow", state, 0, TimeSpan.FromMinutes(1), leaseId: "other"));

        current.LeaseId = "owner";
        current.LeaseExpiresAtUtc = DateTime.UtcNow.AddMinutes(1);
        harness.ReplacesSuccessfully();
        Assert.True(await harness.Store.TryUpdateAsync(
            "flow", state, 0, TimeSpan.FromMinutes(1), leaseId: "owner"));

        harness.ReadsException(HttpStatusCode.NotFound);
        Assert.False(await harness.Store.TryUpdateAsync("flow", state, 0, TimeSpan.FromMinutes(1)));

        harness.Reads(current);
        harness.Container
            .Setup(container => container.ReplaceItemAsync(
                It.IsAny<CosmosFlowStateDocument>(),
                It.IsAny<string>(),
                It.IsAny<PartitionKey?>(),
                It.IsAny<ItemRequestOptions>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(CosmosError(HttpStatusCode.PreconditionFailed));
        Assert.False(await harness.Store.TryUpdateAsync("flow", state, 0, TimeSpan.FromMinutes(1)));
    }

    [Fact]
    public async Task Store_HandlesLeaseOutcomesAndReleaseRaces()
    {
        using var harness = new CosmosHarness();
        var state = CreateState("flow");
        var document = Document(state, DateTime.UtcNow.AddMinutes(5));
        harness.QueriesLease(document);
        harness.PatchesSuccessfully();

        Assert.True(await harness.Store.TryAcquireLeaseAsync("flow", "owner", TimeSpan.FromMinutes(1)));

        document.LeaseId = "other";
        document.LeaseExpiresAtUtc = DateTime.UtcNow.AddMinutes(1);
        Assert.False(await harness.Store.TryAcquireLeaseAsync("flow", "owner", TimeSpan.FromMinutes(1)));
        Assert.False(await harness.Store.TryRenewLeaseAsync("flow", "owner", TimeSpan.FromMinutes(1)));

        await harness.Store.ReleaseLeaseAsync("flow", "owner");
        document.LeaseId = "owner";
        await harness.Store.ReleaseLeaseAsync("flow", "owner");

        harness.QueriesNothing();
        Assert.False(await harness.Store.TryAcquireLeaseAsync("flow", "owner", TimeSpan.FromMinutes(1)));
        await harness.Store.ReleaseLeaseAsync("flow", "owner");

        // The patch's own 404/0: purged between the projection read and the write.
        harness.QueriesLease(document);
        harness.PatchesThrowing(CosmosError(HttpStatusCode.NotFound));
        Assert.False(await harness.Store.TryRenewLeaseAsync("flow", "owner", TimeSpan.FromMinutes(1)));
        await harness.Store.ReleaseLeaseAsync("flow", "owner");

        harness.PatchesThrowing(CosmosError(HttpStatusCode.PreconditionFailed));
        Assert.False(await harness.Store.TryRenewLeaseAsync("flow", "owner", TimeSpan.FromMinutes(1)));
        await harness.Store.ReleaseLeaseAsync("flow", "owner");

        // The lease paths never touch the document body: no point read, no replace.
        harness.Container.Verify(
            container => container.ReadItemAsync<CosmosFlowStateDocument>(
                It.IsAny<string>(), It.IsAny<PartitionKey>(), It.IsAny<ItemRequestOptions>(), It.IsAny<CancellationToken>()),
            Times.Never);
        harness.Container.Verify(
            container => container.ReplaceItemAsync(
                It.IsAny<CosmosFlowStateDocument>(), It.IsAny<string>(), It.IsAny<PartitionKey?>(), It.IsAny<ItemRequestOptions>(), It.IsAny<CancellationToken>()),
            Times.Never);

        await Assert.ThrowsAsync<ArgumentException>(() =>
            harness.Store.TryAcquireLeaseAsync(" ", "owner", TimeSpan.FromMinutes(1)));
        await Assert.ThrowsAsync<ArgumentException>(() =>
            harness.Store.TryAcquireLeaseAsync("flow", " ", TimeSpan.FromMinutes(1)));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            harness.Store.TryAcquireLeaseAsync("flow", "owner", TimeSpan.Zero));
    }

    [Fact]
    public async Task Store_ValidatesExistingAndAutoCreatedContainers()
    {
        using var wrongPartition = new CosmosHarness(new ContainerProperties("states", "/wrong")
        {
            DefaultTimeToLive = -1
        });
        await Assert.ThrowsAsync<InvalidOperationException>(() => wrongPartition.Store.LoadAsync("flow"));

        using var missingTtl = new CosmosHarness(new ContainerProperties("states", "/flowId"));
        await Assert.ThrowsAsync<InvalidOperationException>(() => missingTtl.Store.LoadAsync("flow"));

        var client = new Mock<CosmosClient>();
        var database = new Mock<Database>();
        var databaseResponse = new Mock<DatabaseResponse>();
        var container = new Mock<Container>();
        var response = ContainerResult(new ContainerProperties("states", "/flowId")
        {
            DefaultTimeToLive = -1
        });
        databaseResponse.SetupGet(item => item.Database).Returns(database.Object);
        client
            .Setup(item => item.CreateDatabaseIfNotExistsAsync(
                "flows",
                It.IsAny<int?>(),
                It.IsAny<RequestOptions>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(databaseResponse.Object);
        database
            .Setup(item => item.CreateContainerIfNotExistsAsync(
                It.IsAny<ContainerProperties>(),
                It.IsAny<int?>(),
                It.IsAny<ContainerRequestOptions>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(response.Object);
        client.Setup(item => item.GetContainer("flows", "states")).Returns(container.Object);
        container
            .Setup(item => item.ReadItemAsync<CosmosFlowStateDocument>(
                It.IsAny<string>(),
                It.IsAny<PartitionKey>(),
                It.IsAny<ItemRequestOptions>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(CosmosError(HttpStatusCode.NotFound));

        using var autoCreated = new CosmosFlowStateStore(client.Object, Options.Create(new CosmosDurableFlowOptions
        {
            DatabaseName = "flows",
            ContainerName = "states",
            AutoCreateContainer = true,
            Throughput = 400
        }));
        Assert.Null(await autoCreated.LoadAsync("flow"));
    }

    [Fact]
    public async Task Store_LoadsReadableStateAndHandlesDeleteOutcomes()
    {
        using var harness = new CosmosHarness();
        var state = CreateState("flow");

        harness.Reads(Document(state, DateTime.UtcNow.AddSeconds(-1)));
        Assert.Null(await harness.Store.LoadAsync("flow"));

        var unreadable = Document(state, DateTime.UtcNow.AddMinutes(1));
        unreadable.Revision = state.Revision + 1;
        harness.Reads(unreadable);
        Assert.Null(await harness.Store.LoadAsync("flow"));

        harness.Reads(Document(state, DateTime.UtcNow.AddMinutes(1)));
        Assert.Equal("flow", (await harness.Store.LoadAsync("flow"))?.FlowId);

        harness.Container
            .Setup(container => container.DeleteItemAsync<CosmosFlowStateDocument>(
                It.IsAny<string>(),
                It.IsAny<PartitionKey>(),
                It.IsAny<ItemRequestOptions>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(Mock.Of<ItemResponse<CosmosFlowStateDocument>>());
        Assert.True(await harness.Store.TryDeleteAsync("flow"));

        harness.Container
            .Setup(container => container.DeleteItemAsync<CosmosFlowStateDocument>(
                It.IsAny<string>(),
                It.IsAny<PartitionKey>(),
                It.IsAny<ItemRequestOptions>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(CosmosError(HttpStatusCode.NotFound));
        Assert.False(await harness.Store.TryDeleteAsync("flow"));
        await Assert.ThrowsAsync<ArgumentException>(() => harness.Store.TryDeleteAsync(" "));
    }

    [Fact]
    public async Task Store_ExhaustsOptimisticConcurrencyRetries()
    {
        using var harness = new CosmosHarness();
        var state = CreateState("flow");
        state.Revision = 1;
        harness.ReadsFactory(() => Document(CreateState("flow"), DateTime.UtcNow.AddMinutes(5)));
        harness.Container
            .Setup(container => container.ReplaceItemAsync(
                It.IsAny<CosmosFlowStateDocument>(),
                It.IsAny<string>(),
                It.IsAny<PartitionKey?>(),
                It.IsAny<ItemRequestOptions>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(CosmosError(HttpStatusCode.PreconditionFailed));

        Assert.False(await harness.Store.TryUpdateAsync(
            "flow", state, expectedRevision: 0, TimeSpan.FromMinutes(1)));

        // The lease paths read a projection and patch; every patch losing its ETag race exhausts
        // the same bounded loop.
        harness.QueriesLease(() => new CosmosLeaseProjection
        {
            Id = "flow",
            ETag = "etag",
            ExpiresAtUtc = DateTime.UtcNow.AddMinutes(5),
            Revision = 0,
            LeaseId = "owner",
            LeaseExpiresAtUtc = DateTime.UtcNow.AddMinutes(1)
        });
        harness.PatchesThrowing(CosmosError(HttpStatusCode.PreconditionFailed));
        Assert.False(await harness.Store.TryRenewLeaseAsync(
            "flow", "owner", TimeSpan.FromMinutes(1)));
        await harness.Store.ReleaseLeaseAsync("flow", "owner");
    }

    [Fact]
    public async Task Store_CreatesNewItemAndRejectsLiveConflict()
    {
        using var harness = new CosmosHarness();
        var state = CreateState("flow");
        harness.Container
            .Setup(container => container.CreateItemAsync(
                It.IsAny<CosmosFlowStateDocument>(),
                It.IsAny<PartitionKey?>(),
                It.IsAny<ItemRequestOptions>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(Mock.Of<ItemResponse<CosmosFlowStateDocument>>());
        Assert.True(await harness.Store.TryCreateAsync("flow", state, TimeSpan.FromMilliseconds(1100)));

        harness.Container
            .Setup(container => container.CreateItemAsync(
                It.IsAny<CosmosFlowStateDocument>(),
                It.IsAny<PartitionKey?>(),
                It.IsAny<ItemRequestOptions>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(CosmosError(HttpStatusCode.Conflict));
        harness.Reads(Document(state, DateTime.UtcNow.AddMinutes(1)));
        Assert.False(await harness.Store.TryCreateAsync("flow", state, TimeSpan.FromMinutes(1)));
    }

    [Fact]
    public async Task Store_CoversRevisionAndLeaseEligibilityBranches()
    {
        using var harness = new CosmosHarness();
        var state = CreateState("flow");
        var document = Document(state, DateTime.UtcNow.AddMinutes(5));

        document.Revision = null;
        harness.Reads(document);
        harness.QueriesLease(document);
        // Present-but-uninterpretable: the document is in the container, so reporting absence here
        // would ack the only wake-up of a run that still exists.
        await Assert.ThrowsAsync<FlowStateUnreadableException>(() => harness.Store.LoadAsync("flow"));
        Assert.False(await harness.Store.TryAcquireLeaseAsync("flow", "owner", TimeSpan.FromMinutes(1)));

        document = Document(state, DateTime.UtcNow.AddMinutes(-1));
        harness.Reads(document);
        state.Revision = 1;
        Assert.False(await harness.Store.TryUpdateAsync("flow", state, expectedRevision: 0, TimeSpan.FromMinutes(1)));

        document = Document(CreateState("flow"), DateTime.UtcNow.AddMinutes(5));
        document.Revision = 7;
        harness.Reads(document);
        Assert.False(await harness.Store.TryUpdateAsync("flow", state, expectedRevision: 0, TimeSpan.FromMinutes(1)));

        document = Document(CreateState("flow"), DateTime.UtcNow.AddMinutes(5));
        document.LeaseId = "owner";
        document.LeaseExpiresAtUtc = DateTime.UtcNow.AddMinutes(-1);
        harness.Reads(document);
        harness.QueriesLease(document);
        Assert.False(await harness.Store.TryUpdateAsync(
            "flow", state, expectedRevision: 0, TimeSpan.FromMinutes(1), leaseId: "owner"));
        Assert.False(await harness.Store.TryRenewLeaseAsync("flow", "owner", TimeSpan.FromMinutes(1)));

        document.LeaseId = "other";
        harness.PatchesSuccessfully();
        Assert.True(await harness.Store.TryAcquireLeaseAsync("flow", "owner", TimeSpan.FromMinutes(1)));
    }

    [Fact]
    public async Task Store_LeaseWritesRewriteTtlToRemainingLogicalWindow()
    {
        using var harness = new CosmosHarness();
        var state = CreateState("flow");
        var document = Document(state, DateTime.UtcNow.AddMinutes(10));
        // The stored ttl still carries the full window stamped at creation. Every replace
        // refreshes _ts (the server TTL anchor), so a lease write persisting that value unchanged
        // would restart the whole physical-retention countdown on each heartbeat.
        document.Ttl = (int)TimeSpan.FromHours(2).TotalSeconds;
        harness.QueriesLease(document);
        IReadOnlyList<PatchOperation>? patched = null;
        PatchItemRequestOptions? requestOptions = null;
        harness.PatchesSuccessfully((operations, options) => (patched, requestOptions) = (operations, options));

        Assert.True(await harness.Store.TryAcquireLeaseAsync("flow", "owner", TimeSpan.FromMinutes(1)));
        Assert.NotNull(patched);
        Assert.InRange((int)PatchValue(PatchFor(patched!, "/ttl"))!, 540, 601); // the ~10 minutes left, never the stored 7200
        Assert.Equal("owner", PatchValue(PatchFor(patched!, "/leaseId")));
        Assert.NotNull(PatchValue(PatchFor(patched!, "/leaseExpiresAtUtc")));
        // Round 36: conditional on the projection's ETag, and no document body comes back.
        Assert.Equal("etag", requestOptions!.IfMatchEtag);
        Assert.False(requestOptions.EnableContentResponseOnWrite);

        // Release patches too and must realign the same way. (The projection is re-read from
        // `document` on every call, so the acquired state is modeled explicitly.)
        document.LeaseId = "owner";
        document.LeaseExpiresAtUtc = DateTime.UtcNow.AddMinutes(1);
        document.ExpiresAtUtc = DateTime.UtcNow.AddMinutes(10);
        document.Ttl = (int)TimeSpan.FromHours(2).TotalSeconds;
        patched = null;
        await harness.Store.ReleaseLeaseAsync("flow", "owner");
        Assert.NotNull(patched);
        Assert.Null(PatchValue(PatchFor(patched!, "/leaseId")));
        Assert.Null(PatchValue(PatchFor(patched!, "/leaseExpiresAtUtc")));
        Assert.InRange((int)PatchValue(PatchFor(patched!, "/ttl"))!, 540, 601);

        // An already-due ledger collapses to the 1-second floor (Cosmos rejects 0) instead of the
        // release granting it a fresh retention window.
        document.LeaseId = "owner";
        document.ExpiresAtUtc = DateTime.UtcNow.AddMinutes(-5);
        document.Ttl = (int)TimeSpan.FromHours(2).TotalSeconds;
        patched = null;
        await harness.Store.ReleaseLeaseAsync("flow", "owner");
        Assert.Equal(1, PatchValue(PatchFor(patched!, "/ttl")));
    }

    [Fact]
    public async Task Store_ProvisioningGateIsSharedAcrossConcurrentCallers()
    {
        var client = new Mock<CosmosClient>();
        var container = new Mock<Container>();
        var provisioningStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var completeProvisioning = new TaskCompletionSource<ContainerResponse>(TaskCreationOptions.RunContinuationsAsynchronously);
        client.Setup(item => item.GetContainer("flows", "states")).Returns(container.Object);
        container
            .Setup(item => item.ReadContainerAsync(
                It.IsAny<ContainerRequestOptions>(), It.IsAny<CancellationToken>()))
            .Callback(() => provisioningStarted.TrySetResult())
            .Returns(completeProvisioning.Task);
        container
            .Setup(item => item.ReadItemAsync<CosmosFlowStateDocument>(
                It.IsAny<string>(), It.IsAny<PartitionKey>(), It.IsAny<ItemRequestOptions>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(CosmosError(HttpStatusCode.NotFound));
        using var store = new CosmosFlowStateStore(client.Object, Options.Create(new CosmosDurableFlowOptions
        {
            DatabaseName = "flows",
            ContainerName = "states",
            AutoCreateContainer = false
        }));

        var first = store.LoadAsync("first");
        await provisioningStarted.Task;
        var second = store.LoadAsync("second");
        await Task.Yield();
        completeProvisioning.SetResult(ContainerResult(new ContainerProperties("states", "/flowId")
        {
            DefaultTimeToLive = -1
        }).Object);

        Assert.Null(await first);
        Assert.Null(await second);
        var ensure = (Task)typeof(CosmosFlowStateStore)
            .GetMethod("EnsureCreatedAsync", BindingFlags.NonPublic | BindingFlags.Instance)!
            .Invoke(store, [CancellationToken.None])!;
        await ensure;
    }

    [Fact]
    public async Task LeaseFence_TreatsAMissingLeaseDeadlineAsNotHeld()
    {
        // Regression: the fence was written in NEGATED form over a DateTime? —
        // `LeaseId != leaseId || LeaseExpiresAtUtc <= now` — and a lifted comparison against null
        // is false, so a document carrying a lease id with no deadline PASSED both the checkpoint
        // fence and the renewal, while the acquire branch treated the same document as free to
        // steal: two holders at once. The siblings' positive SQL form is false for NULL; so is
        // this one now.
        using var harness = new CosmosHarness();
        var state = CreateState("flow");
        state.Revision = 1;
        var current = Document(CreateState("flow"), DateTime.UtcNow.AddMinutes(5));
        current.LeaseId = "owner";
        current.LeaseExpiresAtUtc = null;
        harness.Reads(current);
        harness.QueriesLease(current);
        harness.ReplacesSuccessfully();
        harness.PatchesSuccessfully();

        Assert.False(await harness.Store.TryUpdateAsync("flow", state, 0, TimeSpan.FromMinutes(1), leaseId: "owner"));
        Assert.False(await harness.Store.TryRenewLeaseAsync("flow", "owner", TimeSpan.FromMinutes(1)));
    }

    private static FlowState CreateState(string flowId) => new()
    {
        FlowId = flowId,
        FlowTypeName = typeof(TestOnboardingFlow).FullName,
        InputTypeName = typeof(TestFlowInput).FullName,
        Status = FlowRunStatus.Running,
        CreatedAtUtc = DateTime.UtcNow,
        UpdatedAtUtc = DateTime.UtcNow
    };

    private static CosmosFlowStateDocument Document(FlowState state, DateTime expiresAtUtc) => new()
    {
        Id = state.FlowId!,
        FlowId = state.FlowId!,
        StateJson = JsonSerializer.Serialize(state),
        ExpiresAtUtc = expiresAtUtc,
        UpdatedAtUtc = DateTime.UtcNow,
        Revision = state.Revision
    };

    [Fact]
    public async Task Load_NotFoundWithANonZeroSubStatus_ThrowsInsteadOfReportingAbsence()
    {
        // Regression (round 31): every Cosmos 404 was mapped to "the flow is gone", but Cosmos
        // answers 404 for conditions where the ledger still exists — sub-status 1002
        // (ReadSessionNotAvailable: routine Session-consistency lag when a DIFFERENT process reads
        // right after this one's write, surfaced once the SDK's session retries exhaust) and
        // 1003/1004 (container/database recreated). Callers acknowledge the wake-up on null, so a
        // live run's only wake-up was silently dropped. Only sub-status 0 means "no such item";
        // everything else must surface so the delivery is retried or dead-lettered instead.
        using var harness = new CosmosHarness();
        harness.Container
            .Setup(item => item.ReadItemAsync<CosmosFlowStateDocument>(
                It.IsAny<string>(),
                It.IsAny<PartitionKey>(),
                It.IsAny<ItemRequestOptions>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(new CosmosException("read session not available", HttpStatusCode.NotFound, 1002, "activity", 0));

        var ex = await Assert.ThrowsAsync<CosmosException>(() => harness.Store.LoadAsync("flow"));
        Assert.Equal(1002, ex.SubStatusCode);

        // Sub-status 0 stays a genuine absence.
        harness.ReadsException(HttpStatusCode.NotFound);
        Assert.Null(await harness.Store.LoadAsync("flow"));
    }

    /// <summary>
    /// Regression (round 33): TryUpdateAsync, the lease paths and TryDeleteAsync still caught every
    /// NotFound regardless of sub-status and reported the ledger gone (false, or a silent return) —
    /// only LoadAsync had learned to discriminate. A 404 with sub-status 1002
    /// (ReadSessionNotAvailable: a lagging replica) names a ledger that still exists, so the
    /// checkpoint returned false, the lease marked itself lost, the delivery redelivered, and the
    /// step's already-performed side effect ran a second time. Sub-status 0 alone is absence; every
    /// other sub-status now surfaces so the delivery is retried instead.
    /// </summary>
    [Fact]
    public async Task Writes_NotFoundWithANonZeroSubStatus_ThrowInsteadOfReportingAbsence()
    {
        using var harness = new CosmosHarness();
        var state = CreateState("flow");
        state.Revision = 1;
        harness.ReadsFactory(() =>
        {
            var document = Document(CreateState("flow"), DateTime.UtcNow.AddMinutes(5));
            document.LeaseId = "owner";
            document.LeaseExpiresAtUtc = DateTime.UtcNow.AddMinutes(1);
            return document;
        });

        harness.QueriesLease(() =>
        {
            var document = Document(CreateState("flow"), DateTime.UtcNow.AddMinutes(5));
            return new CosmosLeaseProjection
            {
                Id = "flow",
                ETag = "etag",
                ExpiresAtUtc = document.ExpiresAtUtc,
                Revision = document.Revision,
                LeaseId = "owner",
                LeaseExpiresAtUtc = DateTime.UtcNow.AddMinutes(1)
            };
        });

        // The write after a successful read answers 404/1002.
        harness.ReplacesThrowing(ReadSessionNotAvailable());
        harness.PatchesThrowing(ReadSessionNotAvailable());
        var checkpoint = await Assert.ThrowsAsync<CosmosException>(
            () => harness.Store.TryUpdateAsync("flow", state, 0, TimeSpan.FromMinutes(1), leaseId: "owner"));
        Assert.Equal(1002, checkpoint.SubStatusCode);
        await Assert.ThrowsAsync<CosmosException>(() => harness.Store.TryRenewLeaseAsync("flow", "owner", TimeSpan.FromMinutes(1)));
        await Assert.ThrowsAsync<CosmosException>(() => harness.Store.TryAcquireLeaseAsync("flow", "owner", TimeSpan.FromMinutes(1)));
        await Assert.ThrowsAsync<CosmosException>(() => harness.Store.ReleaseLeaseAsync("flow", "owner"));

        // The read itself answers 404/1002 (one filter guards both calls of each path).
        harness.ReadsThrowing(ReadSessionNotAvailable());
        harness.QueriesThrowing(ReadSessionNotAvailable());
        await Assert.ThrowsAsync<CosmosException>(() => harness.Store.TryUpdateAsync("flow", state, 0, TimeSpan.FromMinutes(1)));
        await Assert.ThrowsAsync<CosmosException>(() => harness.Store.TryAcquireLeaseAsync("flow", "owner", TimeSpan.FromMinutes(1)));
        await Assert.ThrowsAsync<CosmosException>(() => harness.Store.ReleaseLeaseAsync("flow", "owner"));

        harness.DeletesThrowing(ReadSessionNotAvailable());
        await Assert.ThrowsAsync<CosmosException>(() => harness.Store.TryDeleteAsync("flow"));

        // Sub-status 0 stays a genuine absence on every path.
        harness.ReadsException(HttpStatusCode.NotFound);
        harness.QueriesNothing();
        Assert.False(await harness.Store.TryUpdateAsync("flow", state, 0, TimeSpan.FromMinutes(1)));
        Assert.False(await harness.Store.TryAcquireLeaseAsync("flow", "owner", TimeSpan.FromMinutes(1)));
        await harness.Store.ReleaseLeaseAsync("flow", "owner");
        harness.DeletesThrowing(CosmosError(HttpStatusCode.NotFound));
        Assert.False(await harness.Store.TryDeleteAsync("flow"));
    }

    /// <summary>
    /// Round 36: the lease paths read a projection and patch. A projection without <c>_etag</c>
    /// (a serializer that hides system properties) cannot fence a write; silently treating it as
    /// "not held" would let the executor ack a wake-up as a duplicate against a run nobody holds.
    /// </summary>
    [Fact]
    public async Task LeaseQuery_WithoutAnEtag_ThrowsInsteadOfReportingTheLeaseFree()
    {
        using var harness = new CosmosHarness();
        var document = Document(CreateState("flow"), DateTime.UtcNow.AddMinutes(5));
        harness.QueriesLease(() => new CosmosLeaseProjection
        {
            Id = "flow",
            ETag = "",
            ExpiresAtUtc = document.ExpiresAtUtc,
            Revision = document.Revision
        });
        harness.PatchesSuccessfully();

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => harness.Store.TryAcquireLeaseAsync("flow", "owner", TimeSpan.FromMinutes(1)));
        Assert.Contains("_etag", ex.Message, StringComparison.Ordinal);
    }

    private static CosmosException ReadSessionNotAvailable()
        => new("read session not available", HttpStatusCode.NotFound, 1002, "activity", 0);

    private static CosmosException CosmosError(HttpStatusCode statusCode)
        => new("test", statusCode, 0, "activity", 0);

    private static Mock<ContainerResponse> ContainerResult(ContainerProperties properties)
    {
        var response = new Mock<ContainerResponse>();
        response.SetupGet(item => item.Resource).Returns(properties);
        return response;
    }

    private sealed class CosmosHarness : IDisposable
    {
        private readonly Mock<ContainerResponse> _containerResponse;

        public CosmosHarness(ContainerProperties? properties = null)
        {
            Client = new Mock<CosmosClient>();
            Container = new Mock<Container>();
            _containerResponse = ContainerResult(properties ?? new ContainerProperties("states", "/flowId")
            {
                DefaultTimeToLive = -1
            });
            Client.Setup(item => item.GetContainer("flows", "states")).Returns(Container.Object);
            Container
                .Setup(item => item.ReadContainerAsync(
                    It.IsAny<ContainerRequestOptions>(),
                    It.IsAny<CancellationToken>()))
                .ReturnsAsync(_containerResponse.Object);
            Store = new CosmosFlowStateStore(Client.Object, Options.Create(new CosmosDurableFlowOptions
            {
                DatabaseName = "flows",
                ContainerName = "states",
                AutoCreateContainer = false
            }));
        }

        public Mock<CosmosClient> Client { get; }
        public Mock<Container> Container { get; }
        public CosmosFlowStateStore Store { get; }

        public void Reads(CosmosFlowStateDocument document)
        {
            var response = new Mock<ItemResponse<CosmosFlowStateDocument>>();
            response.SetupGet(item => item.Resource).Returns(document);
            response.SetupGet(item => item.ETag).Returns("etag");
            Container
                .Setup(item => item.ReadItemAsync<CosmosFlowStateDocument>(
                    It.IsAny<string>(),
                    It.IsAny<PartitionKey>(),
                    It.IsAny<ItemRequestOptions>(),
                    It.IsAny<CancellationToken>()))
                .ReturnsAsync(response.Object);
        }

        public void ReadsFactory(Func<CosmosFlowStateDocument> createDocument)
            => Container
                .Setup(item => item.ReadItemAsync<CosmosFlowStateDocument>(
                    It.IsAny<string>(),
                    It.IsAny<PartitionKey>(),
                    It.IsAny<ItemRequestOptions>(),
                    It.IsAny<CancellationToken>()))
                .ReturnsAsync(() =>
                {
                    var response = new Mock<ItemResponse<CosmosFlowStateDocument>>();
                    response.SetupGet(item => item.Resource).Returns(createDocument());
                    response.SetupGet(item => item.ETag).Returns("etag");
                    return response.Object;
                });

        public void ReadsException(HttpStatusCode statusCode)
            => Container
                .Setup(item => item.ReadItemAsync<CosmosFlowStateDocument>(
                    It.IsAny<string>(),
                    It.IsAny<PartitionKey>(),
                    It.IsAny<ItemRequestOptions>(),
                    It.IsAny<CancellationToken>()))
                .ThrowsAsync(CosmosError(statusCode));

        public void ReadsThrowing(CosmosException exception)
            => Container
                .Setup(item => item.ReadItemAsync<CosmosFlowStateDocument>(
                    It.IsAny<string>(),
                    It.IsAny<PartitionKey>(),
                    It.IsAny<ItemRequestOptions>(),
                    It.IsAny<CancellationToken>()))
                .ThrowsAsync(exception);

        public void ReplacesThrowing(CosmosException exception)
            => Container
                .Setup(item => item.ReplaceItemAsync(
                    It.IsAny<CosmosFlowStateDocument>(),
                    It.IsAny<string>(),
                    It.IsAny<PartitionKey?>(),
                    It.IsAny<ItemRequestOptions>(),
                    It.IsAny<CancellationToken>()))
                .ThrowsAsync(exception);

        public void DeletesThrowing(CosmosException exception)
            => Container
                .Setup(item => item.DeleteItemAsync<CosmosFlowStateDocument>(
                    It.IsAny<string>(),
                    It.IsAny<PartitionKey>(),
                    It.IsAny<ItemRequestOptions>(),
                    It.IsAny<CancellationToken>()))
                .ThrowsAsync(exception);

        public void ReplacesSuccessfully(Action<CosmosFlowStateDocument>? onReplace = null)
            => Container
                .Setup(item => item.ReplaceItemAsync(
                    It.IsAny<CosmosFlowStateDocument>(),
                    It.IsAny<string>(),
                    It.IsAny<PartitionKey?>(),
                    It.IsAny<ItemRequestOptions>(),
                    It.IsAny<CancellationToken>()))
                .Callback<CosmosFlowStateDocument, string, PartitionKey?, ItemRequestOptions, CancellationToken>(
                    (document, _, _, _, _) => onReplace?.Invoke(document))
                .ReturnsAsync(Mock.Of<ItemResponse<CosmosFlowStateDocument>>());

        // ---- Round 36: the lease paths read a projection (no stateJson) and patch the lease fields. ----

        /// <summary>The lease query answers with the lease slice of <paramref name="document"/>, re-read on every call.</summary>
        public void QueriesLease(CosmosFlowStateDocument document)
            => QueriesLease(() => new CosmosLeaseProjection
            {
                Id = document.Id,
                ETag = "etag",
                ExpiresAtUtc = document.ExpiresAtUtc,
                Revision = document.Revision,
                LeaseId = document.LeaseId,
                LeaseExpiresAtUtc = document.LeaseExpiresAtUtc
            });

        /// <summary>The lease query answers with no rows: the flow does not exist.</summary>
        public void QueriesNothing() => QueriesLease(() => null);

        public void QueriesLease(Func<CosmosLeaseProjection?> projection)
            => Container
                .Setup(item => item.GetItemQueryIterator<CosmosLeaseProjection>(
                    It.IsAny<QueryDefinition>(),
                    It.IsAny<string?>(),
                    It.IsAny<QueryRequestOptions>()))
                .Returns(() => LeaseIterator(projection()));

        public void QueriesThrowing(CosmosException exception)
            => Container
                .Setup(item => item.GetItemQueryIterator<CosmosLeaseProjection>(
                    It.IsAny<QueryDefinition>(),
                    It.IsAny<string?>(),
                    It.IsAny<QueryRequestOptions>()))
                .Returns(() =>
                {
                    var iterator = new Mock<FeedIterator<CosmosLeaseProjection>>();
                    iterator.SetupGet(item => item.HasMoreResults).Returns(true);
                    iterator.Setup(item => item.ReadNextAsync(It.IsAny<CancellationToken>())).ThrowsAsync(exception);
                    return iterator.Object;
                });

        private static FeedIterator<CosmosLeaseProjection> LeaseIterator(CosmosLeaseProjection? projection)
        {
            var page = new Mock<FeedResponse<CosmosLeaseProjection>>();
            var rows = projection is null ? Array.Empty<CosmosLeaseProjection>() : [projection];
            page.Setup(item => item.GetEnumerator()).Returns(() => ((IEnumerable<CosmosLeaseProjection>)rows).GetEnumerator());
            page.SetupGet(item => item.Resource).Returns(rows);

            var iterator = new Mock<FeedIterator<CosmosLeaseProjection>>();
            var more = true;
            iterator.SetupGet(item => item.HasMoreResults).Returns(() => more);
            iterator
                .Setup(item => item.ReadNextAsync(It.IsAny<CancellationToken>()))
                .ReturnsAsync(() =>
                {
                    more = false;
                    return page.Object;
                });
            return iterator.Object;
        }

        /// <summary>Every lease patch succeeds; <paramref name="onPatch"/> sees the operations and request options.</summary>
        public void PatchesSuccessfully(Action<IReadOnlyList<PatchOperation>, PatchItemRequestOptions>? onPatch = null)
            => Container
                .Setup(item => item.PatchItemAsync<CosmosFlowStateDocument>(
                    It.IsAny<string>(),
                    It.IsAny<PartitionKey>(),
                    It.IsAny<IReadOnlyList<PatchOperation>>(),
                    It.IsAny<PatchItemRequestOptions>(),
                    It.IsAny<CancellationToken>()))
                .Callback<string, PartitionKey, IReadOnlyList<PatchOperation>, PatchItemRequestOptions, CancellationToken>(
                    (_, _, operations, requestOptions, _) => onPatch?.Invoke(operations, requestOptions))
                .ReturnsAsync(Mock.Of<ItemResponse<CosmosFlowStateDocument>>());

        public void PatchesThrowing(CosmosException exception)
            => Container
                .Setup(item => item.PatchItemAsync<CosmosFlowStateDocument>(
                    It.IsAny<string>(),
                    It.IsAny<PartitionKey>(),
                    It.IsAny<IReadOnlyList<PatchOperation>>(),
                    It.IsAny<PatchItemRequestOptions>(),
                    It.IsAny<CancellationToken>()))
                .ThrowsAsync(exception);

        public void Dispose() => Store.Dispose();
    }

    /// <summary>The value a patch operation carries, read through the SDK's public surface (the value itself is not exposed).</summary>
    private static object? PatchValue(PatchOperation operation)
    {
        var property = operation.GetType().GetProperty("Value", BindingFlags.Instance | BindingFlags.Public);
        return property?.GetValue(operation);
    }

    private static PatchOperation PatchFor(IReadOnlyList<PatchOperation> operations, string path)
        => Assert.Single(operations, operation => operation.Path == path);
}
