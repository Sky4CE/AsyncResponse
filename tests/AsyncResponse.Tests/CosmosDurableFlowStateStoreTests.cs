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
        AbsentOnTheWritePath(container);

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

        // A document whose physical revision disagrees with the one inside its JSON is present
        // and inconsistent: unreadable, never absent (round 38 — "absent" acked its wake-up).
        var unreadable = Document(state, DateTime.UtcNow.AddMinutes(1));
        unreadable.Revision = state.Revision + 1;
        harness.Reads(unreadable);
        var inconsistent = await Assert.ThrowsAsync<FlowStateUnreadableException>(() => harness.Store.LoadAsync("flow"));
        Assert.Contains("stored revision is 1", inconsistent.Reason, StringComparison.Ordinal);

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
        AbsentOnTheWritePath(container);
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

    // ---- Round 39: the size budget is enforced on the DOCUMENT Cosmos receives, not the ledger inside it. ----

    /// <summary>
    /// A ledger of escaped backslashes: its own JSON is 1.2 MB (under the 1.9 MB default), but
    /// embedded as the document's <c>stateJson</c> string every <c>\\</c> escapes again to
    /// <c>\\\\</c> — a 2.4 MB document Cosmos's 2 MB item cap refuses on every retry. Pre-fix the
    /// guard measured the inner JSON only and the create went to the container. (Backslashes
    /// rather than quotes so the arithmetic does not depend on either serializer's encoder.)
    /// </summary>
    private static FlowState EscapeHeavyState(string flowId)
    {
        var state = CreateState(flowId);
        state.Steps = new Dictionary<string, FlowStepState>
        {
            ["blob"] = new FlowStepState
            {
                Completed = true,
                // A JSON string literal of 300k escaped backslashes: ResultJson is JSON text.
                ResultJson = "\"" + new string('\\', 600_000) + "\""
            }
        };
        return state;
    }

    [Fact]
    public async Task TryCreate_RejectsALedgerWhoseEscapedDocumentExceedsTheBudget()
    {
        using var harness = new CosmosHarness();
        var state = EscapeHeavyState("flow");
        var innerBytes = System.Text.Encoding.UTF8.GetByteCount(JsonSerializer.Serialize(state));
        Assert.InRange(innerBytes, 1_000_000, 1_900_000);

        // Preflight must measure the escaped document, not just the inner ledger, without I/O.
        Assert.Throws<FlowStateTooLargeException>(() => harness.Store.ValidateCreate("flow", state, TimeSpan.FromMinutes(1)));
        var ex = await Assert.ThrowsAnyAsync<Exception>(
            () => harness.Store.TryCreateAsync("flow", state, TimeSpan.FromMinutes(1)));

        Assert.Equal("FlowStateTooLargeException", ex.GetType().Name);
        Assert.Equal("flow", ex.GetType().GetProperty("FlowId")!.GetValue(ex));
        var reported = (long)ex.GetType().GetProperty("SerializedSizeBytes")!.GetValue(ex)!;
        Assert.True(reported > 1_900_000, $"reported size {reported} should be the escaped document's");
        harness.Container.Verify(container => container.CreateItemAsync(
            It.IsAny<CosmosFlowStateDocument>(),
            It.IsAny<PartitionKey?>(),
            It.IsAny<ItemRequestOptions>(),
            It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task TryUpdate_RejectsALedgerWhoseEscapedDocumentExceedsTheBudget()
    {
        using var harness = new CosmosHarness();
        var state = EscapeHeavyState("flow");
        state.Revision = 1;
        harness.Reads(Document(CreateState("flow"), DateTime.UtcNow.AddMinutes(5)));
        harness.ReplacesSuccessfully();

        var ex = await Assert.ThrowsAnyAsync<Exception>(
            () => harness.Store.TryUpdateAsync("flow", state, 0, TimeSpan.FromMinutes(1)));
        Assert.Equal("FlowStateTooLargeException", ex.GetType().Name);

        harness.Container.Verify(container => container.ReplaceItemAsync(
            It.IsAny<CosmosFlowStateDocument>(),
            It.IsAny<string>(),
            It.IsAny<PartitionKey?>(),
            It.IsAny<ItemRequestOptions>(),
            It.IsAny<CancellationToken>()), Times.Never);
    }

    /// <summary>A ledger that fits as a document still writes (the guard is not just "smaller").</summary>
    [Fact]
    public async Task TryCreate_AcceptsALedgerWhoseDocumentFitsTheBudget()
    {
        using var harness = new CosmosHarness();
        var state = CreateState("flow");
        state.Steps = new Dictionary<string, FlowStepState>
        {
            ["blob"] = new FlowStepState { Completed = true, ResultJson = "\"" + new string('x', 500_000) + "\"" }
        };
        harness.Container
            .Setup(container => container.CreateItemAsync(
                It.IsAny<CosmosFlowStateDocument>(),
                It.IsAny<PartitionKey?>(),
                It.IsAny<ItemRequestOptions>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(Mock.Of<ItemResponse<CosmosFlowStateDocument>>());

        Assert.True(await harness.Store.TryCreateAsync("flow", state, TimeSpan.FromMinutes(1)));
    }

    [Theory]
    [InlineData(false, false, false)]
    [InlineData(false, true, false)]
    [InlineData(true, false, true)]
    [InlineData(true, true, true)]
    public void DefaultDocumentMeasurement_MatchesSdkJsonWithoutReflection(bool indented, bool ignoreNull, bool leased)
    {
        var document = Document(CreateState("unicode-雪"), DateTime.UtcNow.AddMinutes(5));
        document.StateJson = "\"\\\n雪\t\u2028<>";
        document.Revision = leased ? 3 : null;
        document.LeaseId = leased ? "lease-\"雪" : null;
        document.LeaseExpiresAtUtc = leased ? DateTime.UtcNow : null;
        document.Ttl = leased ? 300 : null;
        var expected = Newtonsoft.Json.JsonConvert.SerializeObject(document, new Newtonsoft.Json.JsonSerializerSettings
        {
            Formatting = indented ? Newtonsoft.Json.Formatting.Indented : Newtonsoft.Json.Formatting.None,
            NullValueHandling = ignoreNull ? Newtonsoft.Json.NullValueHandling.Ignore : Newtonsoft.Json.NullValueHandling.Include
        });
        Assert.Equal(System.Text.Encoding.UTF8.GetByteCount(expected), CosmosFlowStateStore.MeasureDefaultDocumentBytes(
            document, new CosmosSerializationOptions { Indented = indented, IgnoreNullValues = ignoreNull }));
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

    // ---- Round 40: ObserveLeaseAsync (see Round40LeaseObservationTests for the other stores). ----

    [Fact]
    public async Task Round40_ObserveLease_ReportsTheRawPersistedLease_WithoutJudgingExpiry()
    {
        using var harness = new CosmosHarness();
        var document = Document(CreateState("flow"), DateTime.UtcNow.AddMinutes(5));
        harness.QueriesLease(document);
        harness.WritePathSeesTheLedger();

        // Present, never leased (or released: the release patches both fields to null).
        Assert.Same(FlowLeaseObservation.Unheld, await harness.Store.ObserveLeaseAsync("flow"));

        // Held: owner and expiry exactly as stored, in UTC.
        var live = new DateTime(2031, 3, 14, 9, 26, 53, DateTimeKind.Utc).AddTicks(1_234_567);
        document.LeaseId = "owner-a";
        document.LeaseExpiresAtUtc = live;
        var held = await harness.Store.ObserveLeaseAsync("flow");
        Assert.Equal("owner-a", held!.LeaseId);
        Assert.Equal(live, held.ExpiresAtUtc);
        Assert.Equal(DateTimeKind.Utc, held.ExpiresAtUtc!.Value.Kind);

        // Long lapsed — and the LEDGER itself past its logical expiry too: still reported as
        // stored. The lease writes compare both with the app clock; the observation compares
        // neither, because the executor's proof of a live holder is that two observations differ.
        document.ExpiresAtUtc = DateTime.UtcNow.AddMinutes(-5);
        document.LeaseId = "dead-worker";
        document.LeaseExpiresAtUtc = new DateTime(2001, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var lapsed = await harness.Store.ObserveLeaseAsync("flow");
        Assert.Equal("dead-worker", lapsed!.LeaseId);
        Assert.Equal(new DateTime(2001, 1, 1, 0, 0, 0, DateTimeKind.Utc), lapsed.ExpiresAtUtc);

        // A serializer that drops the zone designator hands back Unspecified; the digits are UTC
        // (every lease write stamps DateTime.UtcNow), so they are stamped, not shifted.
        document.LeaseExpiresAtUtc = DateTime.SpecifyKind(live, DateTimeKind.Unspecified);
        var unspecified = await harness.Store.ObserveLeaseAsync("flow");
        Assert.Equal(live.Ticks, unspecified!.ExpiresAtUtc!.Value.Ticks);
        Assert.Equal(DateTimeKind.Utc, unspecified.ExpiresAtUtc.Value.Kind);

        // A holder with no deadline is still a holder.
        document.LeaseExpiresAtUtc = null;
        var noExpiry = await harness.Store.ObserveLeaseAsync("flow");
        Assert.Equal("dead-worker", noExpiry!.LeaseId);
        Assert.Null(noExpiry.ExpiresAtUtc);

        // Read through the lease projection only: no point read (which would transfer stateJson)
        // and no lease write. (The write-path confirmation before each query can never apply.)
        harness.Container.Verify(
            item => item.ReadItemAsync<CosmosFlowStateDocument>(
                It.IsAny<string>(),
                It.IsAny<PartitionKey>(),
                It.IsAny<ItemRequestOptions>(),
                It.IsAny<CancellationToken>()),
            Times.Never);
        harness.Container.Verify(
            item => item.PatchItemAsync<CosmosFlowStateDocument>(
                It.IsAny<string>(),
                It.IsAny<PartitionKey>(),
                It.IsAny<IReadOnlyList<PatchOperation>>(),
                IsLeasePatch(),
                It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task Round40_ObserveLease_QueriesTheFlowsOwnPartitionById()
    {
        using var harness = new CosmosHarness();
        harness.WritePathSeesTheLedger();
        QueryDefinition? sentQuery = null;
        QueryRequestOptions? sentOptions = null;
        harness.Container
            .Setup(item => item.GetItemQueryIterator<CosmosLeaseProjection>(
                It.IsAny<QueryDefinition>(),
                It.IsAny<string?>(),
                It.IsAny<QueryRequestOptions>()))
            .Returns((QueryDefinition query, string? _, QueryRequestOptions options) =>
            {
                sentQuery = query;
                sentOptions = options;
                var iterator = new Mock<FeedIterator<CosmosLeaseProjection>>();
                iterator.SetupGet(item => item.HasMoreResults).Returns(false);
                return iterator.Object;
            });

        // No rows: the flow does not exist. Unheld, never null (null = "cannot report leases").
        Assert.Same(FlowLeaseObservation.Unheld, await harness.Store.ObserveLeaseAsync("flow-1"));

        Assert.NotNull(sentQuery);
        Assert.DoesNotContain("stateJson", sentQuery!.QueryText, StringComparison.Ordinal);
        Assert.Contains("c.leaseId", sentQuery.QueryText, StringComparison.Ordinal);
        Assert.Contains("c.leaseExpiresAtUtc", sentQuery.QueryText, StringComparison.Ordinal);
        Assert.Equal("flow-1", Assert.Single(sentQuery.GetQueryParameters()).Value);
        Assert.Equal("[\"flow-1\"]", sentOptions!.PartitionKey!.Value.ToString());
    }

    [Fact]
    public async Task Round40_ObserveLease_OnlyAGenuineNotFoundReadsAsUnheld()
    {
        using var harness = new CosmosHarness();
        harness.WritePathSeesTheLedger();

        // Sub-status 0 is the only 404 that means "nothing there" — the store-wide rule.
        harness.QueriesThrowing(CosmosError(HttpStatusCode.NotFound));
        Assert.Same(FlowLeaseObservation.Unheld, await harness.Store.ObserveLeaseAsync("flow"));

        // 1002 ReadSessionNotAvailable: the ledger may well exist and be held. Reporting Unheld
        // would hide a live holder from the waiting delivery, so it propagates.
        harness.QueriesThrowing(ReadSessionNotAvailable());
        var sessionLag = await Assert.ThrowsAsync<CosmosException>(() => harness.Store.ObserveLeaseAsync("flow"));
        Assert.Equal(1002, sessionLag.SubStatusCode);

        harness.QueriesThrowing(CosmosError(HttpStatusCode.ServiceUnavailable));
        await Assert.ThrowsAsync<CosmosException>(() => harness.Store.ObserveLeaseAsync("flow"));

        await Assert.ThrowsAsync<ArgumentException>(() => harness.Store.ObserveLeaseAsync(" "));
    }

    [Fact]
    public async Task Round40_ObserveLease_SeesWhatTheLeaseWritesPatched()
    {
        // Acquire, renew and release through the store's own patches, replayed onto the document
        // the lease query reads: the observation inverts exactly what UpdateLeaseAsync wrote.
        using var harness = new CosmosHarness();
        var document = Document(CreateState("flow"), DateTime.UtcNow.AddMinutes(5));
        harness.QueriesLease(document);
        harness.WritePathSeesTheLedger();
        harness.PatchesSuccessfully((operations, _) =>
        {
            document.LeaseId = (string?)PatchValue(PatchFor(operations, "/leaseId"));
            document.LeaseExpiresAtUtc = (DateTime?)PatchValue(PatchFor(operations, "/leaseExpiresAtUtc"));
        });

        var before = DateTime.UtcNow;
        Assert.True(await harness.Store.TryAcquireLeaseAsync("flow", "owner-a", TimeSpan.FromSeconds(30)));
        var after = DateTime.UtcNow;
        var acquired = await harness.Store.ObserveLeaseAsync("flow");
        Assert.Equal("owner-a", acquired!.LeaseId);
        Assert.InRange(acquired.ExpiresAtUtc!.Value, before.AddSeconds(30), after.AddSeconds(30));
        Assert.Equal(DateTimeKind.Utc, acquired.ExpiresAtUtc.Value.Kind);

        Assert.True(await harness.Store.TryRenewLeaseAsync("flow", "owner-a", TimeSpan.FromMinutes(5)));
        var renewed = await harness.Store.ObserveLeaseAsync("flow");
        Assert.Equal("owner-a", renewed!.LeaseId);
        Assert.True(renewed.ExpiresAtUtc > acquired.ExpiresAtUtc);

        await harness.Store.ReleaseLeaseAsync("flow", "owner-a");
        Assert.Same(FlowLeaseObservation.Unheld, await harness.Store.ObserveLeaseAsync("flow"));
    }

    // ---- Reads that let a wake-up be acknowledged are confirmed on the write path. ----
    //
    // Session consistency is read-your-writes for the client that wrote. A DIFFERENT process never
    // received the writer's session token, so a lagging replica answers it with a plain 404/0 or
    // an older document — not 1002. The mocks below model exactly that: reads lag until the client
    // has made a write-path round trip (whose 412 carries the write region's session token), and
    // are current afterwards.

    [Fact]
    public async Task Load_AReplicaThatHasNotSeenTheCreate_IsNotReportedAsNoState()
    {
        using var harness = new CosmosHarness();
        var sessionIsCurrent = false;
        harness.WritePath = () =>
        {
            sessionIsCurrent = true;
            return CosmosError(HttpStatusCode.PreconditionFailed);
        };
        var current = Document(CreateState("flow"), DateTime.UtcNow.AddMinutes(5));
        harness.ReadsLagging(() => sessionIsCurrent, stale: null, current);

        // Pre-fix: the lagging 404 was returned as null, and the executor acknowledged the only
        // wake-up of a run that exists.
        var loaded = await harness.Store.LoadAsync("flow");

        Assert.Equal("flow", loaded?.FlowId);
        Assert.Equal(1, harness.WritePathCalls);
    }

    [Fact]
    public async Task Load_AnOlderVersionThatHasSinceBeenExtended_IsNotReportedAsExpired()
    {
        using var harness = new CosmosHarness();
        var sessionIsCurrent = false;
        harness.WritePath = () =>
        {
            sessionIsCurrent = true;
            return CosmosError(HttpStatusCode.PreconditionFailed);
        };
        // The lagging replica still holds the version whose idle TTL has lapsed; a checkpoint it
        // has not applied yet extended it.
        harness.ReadsLagging(
            () => sessionIsCurrent,
            stale: Document(CreateState("flow"), DateTime.UtcNow.AddSeconds(-1)),
            current: Document(CreateState("flow"), DateTime.UtcNow.AddMinutes(5)));

        Assert.Equal("flow", (await harness.Store.LoadAsync("flow"))?.FlowId);

        // Genuinely expired and not yet purged: present on the write path, still expired on the
        // current read — one confirmation, then "no state".
        using var expired = new CosmosHarness();
        expired.WritePathSeesTheLedger();
        expired.Reads(Document(CreateState("flow"), DateTime.UtcNow.AddSeconds(-1)));
        Assert.Null(await expired.Store.LoadAsync("flow"));
        Assert.Equal(1, expired.WritePathCalls);
    }

    [Fact]
    public async Task Load_ConfirmsAbsenceOnce_AndNeverTouchesTheWritePathForALedgerItFound()
    {
        using var harness = new CosmosHarness();

        // Absent on the read AND on the write path: the authoritative "no state".
        harness.ReadsException(HttpStatusCode.NotFound);
        Assert.Null(await harness.Store.LoadAsync("flow"));
        Assert.Equal(1, harness.WritePathCalls);

        // The confirmation can never apply (an If-Match no document carries, never the wildcard),
        // asks for no body back, and names none of the ledger's own fields.
        var (operations, options) = harness.LastWritePathRequest!.Value;
        Assert.Equal(CosmosFlowStateStore.NeverMatchingEtag, options.IfMatchEtag);
        Assert.NotEqual("*", options.IfMatchEtag);
        Assert.False(options.EnableContentResponseOnWrite);
        Assert.DoesNotContain(
            Assert.Single(operations).Path,
            new[] { "/id", "/flowId", "/stateJson", "/expiresAtUtc", "/updatedAtUtc", "/revision", "/leaseId", "/leaseExpiresAtUtc", "/ttl" });

        // The RU bound: a load that finds its document costs exactly the point read.
        harness.Reads(Document(CreateState("flow"), DateTime.UtcNow.AddMinutes(5)));
        Assert.NotNull(await harness.Store.LoadAsync("flow"));
        Assert.Equal(1, harness.WritePathCalls);
    }

    [Fact]
    public async Task Load_PresentForWritesButNeverForReads_ThrowsInsteadOfReportingNoState()
    {
        // An Eventual / Consistent Prefix client sends no session token, so the 412 cannot make
        // its reads current. A delete can win the race between the two calls once — not every
        // time — so repeated disagreement is refused rather than acknowledged.
        using var harness = new CosmosHarness();
        harness.WritePathSeesTheLedger();
        harness.ReadsException(HttpStatusCode.NotFound);

        var ex = await Assert.ThrowsAsync<FlowStateUnreadableException>(() => harness.Store.LoadAsync("flow"));

        Assert.Contains("session-consistent", ex.Reason, StringComparison.Ordinal);
        Assert.Equal(3, harness.WritePathCalls);

        // A delete that really did land between the two calls: present, then gone on both paths.
        using var deleted = new CosmosHarness();
        var confirmations = 0;
        deleted.WritePath = () => CosmosError(++confirmations == 1 ? HttpStatusCode.PreconditionFailed : HttpStatusCode.NotFound);
        deleted.ReadsException(HttpStatusCode.NotFound);
        Assert.Null(await deleted.Store.LoadAsync("flow"));
    }

    [Fact]
    public async Task WritePathConfirmation_FaultsPropagate_InsteadOfReadingAsAbsentOrPresent()
    {
        using var harness = new CosmosHarness();
        harness.ReadsException(HttpStatusCode.NotFound);

        // 404/1002 on the write path names a ledger that may exist; 503 proves nothing either way.
        harness.WritePath = ReadSessionNotAvailable;
        Assert.Equal(1002, (await Assert.ThrowsAsync<CosmosException>(() => harness.Store.LoadAsync("flow"))).SubStatusCode);
        await Assert.ThrowsAsync<CosmosException>(() => harness.Store.ObserveLeaseAsync("flow"));

        harness.WritePath = () => CosmosError(HttpStatusCode.ServiceUnavailable);
        await Assert.ThrowsAsync<CosmosException>(() => harness.Store.LoadAsync("flow"));
        await Assert.ThrowsAsync<CosmosException>(() => harness.Store.ObserveLeaseAsync("flow"));
    }

    [Fact]
    public async Task ObserveLease_IsReadBehindAWritePathRoundTrip_SoALaggingReplicaCannotSupplyTheBaseline()
    {
        // The executor acknowledges a waiting delivery as a duplicate when two observations
        // differ. A baseline served by a lagging replica makes a renewal written BEFORE the
        // delivery arrived look like one written while it waited — and that holder may be dead.
        using var harness = new CosmosHarness();
        var sessionIsCurrent = false;
        harness.WritePath = () =>
        {
            sessionIsCurrent = true;
            return CosmosError(HttpStatusCode.PreconditionFailed);
        };
        var staleExpiry = new DateTime(2031, 3, 14, 9, 0, 0, DateTimeKind.Utc);
        var currentExpiry = staleExpiry.AddSeconds(20);
        var queriedBeforeTheRoundTrip = false;
        harness.QueriesLease(() =>
        {
            queriedBeforeTheRoundTrip |= !sessionIsCurrent;
            return new CosmosLeaseProjection
            {
                Id = "flow",
                ETag = "etag",
                ExpiresAtUtc = DateTime.UtcNow.AddMinutes(5),
                Revision = 0,
                LeaseId = "holder",
                LeaseExpiresAtUtc = sessionIsCurrent ? currentExpiry : staleExpiry
            };
        });

        var observed = await harness.Store.ObserveLeaseAsync("flow");

        Assert.Equal(currentExpiry, observed!.ExpiresAtUtc);
        Assert.False(queriedBeforeTheRoundTrip);

        // Every observation, not just the first: the store cannot know which one is the baseline.
        await harness.Store.ObserveLeaseAsync("flow");
        Assert.Equal(2, harness.WritePathCalls);
    }

    [Fact]
    public async Task ObserveLease_AbsentOnTheWritePath_IsUnheldWithoutAQuery()
    {
        using var harness = new CosmosHarness();

        Assert.Same(FlowLeaseObservation.Unheld, await harness.Store.ObserveLeaseAsync("flow"));

        harness.Container.Verify(
            item => item.GetItemQueryIterator<CosmosLeaseProjection>(
                It.IsAny<QueryDefinition>(),
                It.IsAny<string?>(),
                It.IsAny<QueryRequestOptions>()),
            Times.Never);
    }

    private static void AbsentOnTheWritePath(Mock<Container> container)
        => container
            .Setup(item => item.PatchItemAsync<CosmosFlowStateDocument>(
                It.IsAny<string>(),
                It.IsAny<PartitionKey>(),
                It.IsAny<IReadOnlyList<PatchOperation>>(),
                It.IsAny<PatchItemRequestOptions>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(CosmosError(HttpStatusCode.NotFound));

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

    [Fact]
    public async Task Round37_ConcurrentLeaseOperations_NeverExecuteWithEachOthersId()
    {
        // Regression (round 37, F1): the lease projection query was ONE static QueryDefinition
        // parameterized per call. WithParameter replaces the named parameter in place and returns
        // the same instance, so two flows' lease operations interleaving on one store instance
        // raced on that one parameter bag: flow A built its query with @id = A, flow B then set
        // @id = B on the same object, and A's query EXECUTED under A's partition key asking for
        // B — no such document in A's partition, "no rows", and a healthy renewal reported false
        // (the executor abandons the run and it replays). The mocks that hand back a fixed document
        // for any query could never see it. This one answers what the query ASKS FOR, as the
        // service does, observes the @id each query carries when it executes, and forces the
        // interleaving: A's query is built first, B's is built before A's executes.
        using var harness = new CosmosHarness();
        var documents = new Dictionary<string, CosmosFlowStateDocument>(StringComparer.Ordinal)
        {
            ["flow-a"] = Document(CreateState("flow-a"), DateTime.UtcNow.AddMinutes(5)),
            ["flow-b"] = Document(CreateState("flow-b"), DateTime.UtcNow.AddMinutes(5))
        };
        var aQueried = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var bQueried = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var executedWith = new System.Collections.Concurrent.ConcurrentDictionary<string, string?>(StringComparer.Ordinal);

        harness.Container
            .Setup(item => item.GetItemQueryIterator<CosmosLeaseProjection>(
                It.IsAny<QueryDefinition>(),
                It.IsAny<string?>(),
                It.IsAny<QueryRequestOptions>()))
            .Returns((QueryDefinition query, string? _, QueryRequestOptions options) =>
            {
                // PartitionKey renders as a JSON array of its components: ["flow-a"].
                var partition = JsonSerializer.Deserialize<string[]>(options.PartitionKey!.Value.ToString())![0];
                if (partition == "flow-a")
                    aQueried.TrySetResult();
                else
                    bQueried.TrySetResult();

                var more = true;
                var iterator = new Mock<FeedIterator<CosmosLeaseProjection>>();
                iterator.SetupGet(item => item.HasMoreResults).Returns(() => more);
                iterator
                    .Setup(item => item.ReadNextAsync(It.IsAny<CancellationToken>()))
                    .Returns(async () =>
                    {
                        // A executes only once B's query has been built.
                        if (partition == "flow-a")
                            await bQueried.Task.WaitAsync(TimeSpan.FromSeconds(5));

                        more = false;
                        var requestedId = (string?)query.GetQueryParameters().Single(parameter => parameter.Name == "@id").Value;
                        executedWith[partition] = requestedId;

                        // WHERE c.id = @id under this partition key: a row only when the id asked
                        // for lives in the partition queried.
                        var rows = requestedId == partition && documents.TryGetValue(requestedId, out var document)
                            ? new[]
                            {
                                new CosmosLeaseProjection
                                {
                                    Id = document.Id,
                                    ETag = "etag",
                                    ExpiresAtUtc = document.ExpiresAtUtc,
                                    Revision = document.Revision,
                                    LeaseId = document.LeaseId,
                                    LeaseExpiresAtUtc = document.LeaseExpiresAtUtc
                                }
                            }
                            : [];
                        var page = new Mock<FeedResponse<CosmosLeaseProjection>>();
                        page.Setup(item => item.GetEnumerator()).Returns(() => ((IEnumerable<CosmosLeaseProjection>)rows).GetEnumerator());
                        return page.Object;
                    });
                return iterator.Object;
            });
        harness.PatchesSuccessfully();

        var acquireA = harness.Store.TryAcquireLeaseAsync("flow-a", "owner-a", TimeSpan.FromMinutes(1));
        await aQueried.Task.WaitAsync(TimeSpan.FromSeconds(5));
        var acquireB = harness.Store.TryAcquireLeaseAsync("flow-b", "owner-b", TimeSpan.FromMinutes(1));

        Assert.True(await acquireB.WaitAsync(TimeSpan.FromSeconds(5)));
        Assert.True(await acquireA.WaitAsync(TimeSpan.FromSeconds(5)));
        Assert.Equal("flow-a", executedWith["flow-a"]);
        Assert.Equal("flow-b", executedWith["flow-b"]);
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
            AnswersTheWritePath();
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

        /// <summary>
        /// What the container's WRITE path answers the store's never-matching conditional patch:
        /// 404/0 (no such ledger — the default, an empty container), 412 (it exists), or a fault.
        /// </summary>
        public Func<CosmosException> WritePath { get; set; } = () => CosmosError(HttpStatusCode.NotFound);

        /// <summary>Write-path confirmations issued so far.</summary>
        public int WritePathCalls;

        /// <summary>The last confirmation's operations and request options.</summary>
        public (IReadOnlyList<PatchOperation> Operations, PatchItemRequestOptions Options)? LastWritePathRequest { get; private set; }

        public void WritePathSeesTheLedger() => WritePath = () => CosmosError(HttpStatusCode.PreconditionFailed);

        private void AnswersTheWritePath()
            => Container
                .Setup(item => item.PatchItemAsync<CosmosFlowStateDocument>(
                    It.IsAny<string>(),
                    It.IsAny<PartitionKey>(),
                    It.IsAny<IReadOnlyList<PatchOperation>>(),
                    It.Is<PatchItemRequestOptions>(options => options.IfMatchEtag == CosmosFlowStateStore.NeverMatchingEtag),
                    It.IsAny<CancellationToken>()))
                .Returns((string _, PartitionKey _, IReadOnlyList<PatchOperation> operations, PatchItemRequestOptions options, CancellationToken _) =>
                {
                    Interlocked.Increment(ref WritePathCalls);
                    LastWritePathRequest = (operations, options);
                    return Task.FromException<ItemResponse<CosmosFlowStateDocument>>(WritePath());
                });

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

        /// <summary>
        /// A replica this client reaches WITHOUT the writer's session token: it answers with
        /// <paramref name="stale"/> (null = a plain 404/0, the create not applied yet) until
        /// <paramref name="sessionIsCurrent"/>, and with <paramref name="current"/> afterwards.
        /// </summary>
        public void ReadsLagging(Func<bool> sessionIsCurrent, CosmosFlowStateDocument? stale, CosmosFlowStateDocument current)
            => Container
                .Setup(item => item.ReadItemAsync<CosmosFlowStateDocument>(
                    It.IsAny<string>(),
                    It.IsAny<PartitionKey>(),
                    It.IsAny<ItemRequestOptions>(),
                    It.IsAny<CancellationToken>()))
                .Returns(() =>
                {
                    var document = sessionIsCurrent() ? current : stale;
                    if (document is null)
                        return Task.FromException<ItemResponse<CosmosFlowStateDocument>>(CosmosError(HttpStatusCode.NotFound));

                    var response = new Mock<ItemResponse<CosmosFlowStateDocument>>();
                    response.SetupGet(item => item.Resource).Returns(document);
                    response.SetupGet(item => item.ETag).Returns("etag");
                    return Task.FromResult(response.Object);
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
                    IsLeasePatch(),
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
                    IsLeasePatch(),
                    It.IsAny<CancellationToken>()))
                .ThrowsAsync(exception);

        public void Dispose() => Store.Dispose();
    }

    /// <summary>
    /// A lease patch (fenced by a projection's ETag), as opposed to the write-path confirmation the
    /// harness answers on its own — the two share one <c>PatchItemAsync</c> overload.
    /// </summary>
    private static PatchItemRequestOptions IsLeasePatch()
        => It.Is<PatchItemRequestOptions>(options => options.IfMatchEtag != CosmosFlowStateStore.NeverMatchingEtag);

    /// <summary>The value a patch operation carries, read through the SDK's public surface (the value itself is not exposed).</summary>
    private static object? PatchValue(PatchOperation operation)
    {
        var property = operation.GetType().GetProperty("Value", BindingFlags.Instance | BindingFlags.Public);
        return property?.GetValue(operation);
    }

    private static PatchOperation PatchFor(IReadOnlyList<PatchOperation> operations, string path)
        => Assert.Single(operations, operation => operation.Path == path);
}
