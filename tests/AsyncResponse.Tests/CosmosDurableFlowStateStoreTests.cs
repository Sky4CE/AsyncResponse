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
        harness.Reads(document);
        harness.ReplacesSuccessfully();

        Assert.True(await harness.Store.TryAcquireLeaseAsync("flow", "owner", TimeSpan.FromMinutes(1)));

        document.LeaseId = "other";
        document.LeaseExpiresAtUtc = DateTime.UtcNow.AddMinutes(1);
        Assert.False(await harness.Store.TryAcquireLeaseAsync("flow", "owner", TimeSpan.FromMinutes(1)));
        Assert.False(await harness.Store.TryRenewLeaseAsync("flow", "owner", TimeSpan.FromMinutes(1)));

        await harness.Store.ReleaseLeaseAsync("flow", "owner");
        document.LeaseId = "owner";
        await harness.Store.ReleaseLeaseAsync("flow", "owner");

        harness.ReadsException(HttpStatusCode.NotFound);
        Assert.False(await harness.Store.TryAcquireLeaseAsync("flow", "owner", TimeSpan.FromMinutes(1)));
        await harness.Store.ReleaseLeaseAsync("flow", "owner");

        harness.Reads(document);
        harness.Container
            .Setup(container => container.ReplaceItemAsync(
                It.IsAny<CosmosFlowStateDocument>(),
                It.IsAny<string>(),
                It.IsAny<PartitionKey?>(),
                It.IsAny<ItemRequestOptions>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(CosmosError(HttpStatusCode.PreconditionFailed));
        Assert.False(await harness.Store.TryRenewLeaseAsync("flow", "owner", TimeSpan.FromMinutes(1)));
        await harness.Store.ReleaseLeaseAsync("flow", "owner");

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

        harness.ReadsFactory(() =>
        {
            var document = Document(CreateState("flow"), DateTime.UtcNow.AddMinutes(5));
            document.LeaseId = "owner";
            document.LeaseExpiresAtUtc = DateTime.UtcNow.AddMinutes(1);
            return document;
        });
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
        Assert.False(await harness.Store.TryUpdateAsync(
            "flow", state, expectedRevision: 0, TimeSpan.FromMinutes(1), leaseId: "owner"));
        Assert.False(await harness.Store.TryRenewLeaseAsync("flow", "owner", TimeSpan.FromMinutes(1)));

        document.LeaseId = "other";
        harness.ReplacesSuccessfully();
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
        harness.Reads(document);
        CosmosFlowStateDocument? replaced = null;
        harness.ReplacesSuccessfully(written => replaced = written);

        Assert.True(await harness.Store.TryAcquireLeaseAsync("flow", "owner", TimeSpan.FromMinutes(1)));
        Assert.NotNull(replaced?.Ttl);
        Assert.InRange(replaced!.Ttl!.Value, 540, 601); // the ~10 minutes left, never the stored 7200

        // Release replaces too and must realign the same way.
        document.ExpiresAtUtc = DateTime.UtcNow.AddMinutes(10);
        document.Ttl = (int)TimeSpan.FromHours(2).TotalSeconds;
        replaced = null;
        await harness.Store.ReleaseLeaseAsync("flow", "owner");
        Assert.NotNull(replaced);
        Assert.Null(replaced!.LeaseId);
        Assert.InRange(replaced.Ttl!.Value, 540, 601);

        // An already-due ledger collapses to the 1-second floor (Cosmos rejects 0) instead of the
        // release granting it a fresh retention window.
        document.LeaseId = "owner";
        document.ExpiresAtUtc = DateTime.UtcNow.AddMinutes(-5);
        document.Ttl = (int)TimeSpan.FromHours(2).TotalSeconds;
        replaced = null;
        await harness.Store.ReleaseLeaseAsync("flow", "owner");
        Assert.Equal(1, replaced!.Ttl);
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
        harness.ReplacesSuccessfully();

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

        // The replace after a successful read answers 404/1002.
        harness.ReplacesThrowing(ReadSessionNotAvailable());
        var checkpoint = await Assert.ThrowsAsync<CosmosException>(
            () => harness.Store.TryUpdateAsync("flow", state, 0, TimeSpan.FromMinutes(1), leaseId: "owner"));
        Assert.Equal(1002, checkpoint.SubStatusCode);
        await Assert.ThrowsAsync<CosmosException>(() => harness.Store.TryRenewLeaseAsync("flow", "owner", TimeSpan.FromMinutes(1)));
        await Assert.ThrowsAsync<CosmosException>(() => harness.Store.TryAcquireLeaseAsync("flow", "owner", TimeSpan.FromMinutes(1)));
        await Assert.ThrowsAsync<CosmosException>(() => harness.Store.ReleaseLeaseAsync("flow", "owner"));

        // The read itself answers 404/1002 (one filter guards both calls of each path).
        harness.ReadsThrowing(ReadSessionNotAvailable());
        await Assert.ThrowsAsync<CosmosException>(() => harness.Store.TryUpdateAsync("flow", state, 0, TimeSpan.FromMinutes(1)));
        await Assert.ThrowsAsync<CosmosException>(() => harness.Store.TryAcquireLeaseAsync("flow", "owner", TimeSpan.FromMinutes(1)));
        await Assert.ThrowsAsync<CosmosException>(() => harness.Store.ReleaseLeaseAsync("flow", "owner"));

        harness.DeletesThrowing(ReadSessionNotAvailable());
        await Assert.ThrowsAsync<CosmosException>(() => harness.Store.TryDeleteAsync("flow"));

        // Sub-status 0 stays a genuine absence on every path.
        harness.ReadsException(HttpStatusCode.NotFound);
        Assert.False(await harness.Store.TryUpdateAsync("flow", state, 0, TimeSpan.FromMinutes(1)));
        Assert.False(await harness.Store.TryAcquireLeaseAsync("flow", "owner", TimeSpan.FromMinutes(1)));
        await harness.Store.ReleaseLeaseAsync("flow", "owner");
        harness.DeletesThrowing(CosmosError(HttpStatusCode.NotFound));
        Assert.False(await harness.Store.TryDeleteAsync("flow"));
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

        public void Dispose() => Store.Dispose();
    }
}
