using AsyncResponse.DurableFlows;
using System.Text.Json;
using Xunit;

namespace AsyncResponse.IntegrationTests;

/// <summary>
/// The durable-flow store contract, shared by every store's test class. Each store package gets the
/// same assertions run against a real server, so the contract lives here rather than being restated
/// per store — and so store tests can be split across batches without duplicating it.
/// </summary>
internal static class FlowStoreContract
{
    internal static async Task EventuallyAsync(Func<Task> action, TimeSpan? budget = null)
    {
        var timeout = budget ?? TimeSpan.FromSeconds(30);
        var deadline = DateTime.UtcNow + timeout;
        Exception? last = null;
        while (DateTime.UtcNow < deadline)
        {
            try
            {
                await action();
                return;
            }
            catch (Exception ex)
            {
                last = ex;
                await Task.Delay(500);
            }
        }

        throw new TimeoutException($"The backing store did not become ready within {timeout}.", last);
    }

    internal static async Task AssertStoreContractAsync(
        IFlowStateStore store,
        TimeSpan? expiryTtl = null,
        TimeSpan? expiryDelay = null,
        Func<string, string, Task>? seedRawStateAsync = null)
    {
        var state = CreateState("flow-itest");

        Assert.True(await store.TryCreateAsync(state.FlowId!, state, TimeSpan.FromMinutes(5)));
        var loaded = await store.LoadAsync(state.FlowId!);
        Assert.NotNull(loaded);
        Assert.Equal(FlowRunStatus.Running, loaded!.Status);
        Assert.True(loaded.Steps!["step-a"].Completed);

        state.Status = FlowRunStatus.Succeeded;
        state.LastMessage = "done";
        state.Revision = 1;
        Assert.True(await store.TryUpdateAsync(state.FlowId!, state, 0, TimeSpan.FromMinutes(5)));
        Assert.Equal(FlowRunStatus.Succeeded, (await store.LoadAsync(state.FlowId!))!.Status);

        Assert.True(await store.TryCreateAsync("expired-flow", CreateState("expired-flow"), expiryTtl ?? TimeSpan.FromMilliseconds(1)));
        await Task.Delay(expiryDelay ?? TimeSpan.FromMilliseconds(30));
        Assert.Null(await store.LoadAsync("expired-flow"));

        var concurrent = store;
        var replacement = CreateState("expired-flow");
        Assert.True(await concurrent.TryCreateAsync("expired-flow", replacement, TimeSpan.FromMinutes(5)));
        Assert.NotNull(await store.LoadAsync("expired-flow"));
        Assert.True(await store.TryDeleteAsync("expired-flow"));

        var concurrentFlowId = $"flow-concurrency-{Guid.NewGuid():N}";
        var createResults = await Task.WhenAll(
            Enumerable.Range(0, 16)
                .Select(_ => concurrent.TryCreateAsync(
                    concurrentFlowId,
                    CreateState(concurrentFlowId),
                    TimeSpan.FromMinutes(5))));
        Assert.Single(createResults, static created => created);

        var concurrentState = await store.LoadAsync(concurrentFlowId);
        Assert.NotNull(concurrentState);
        Assert.Equal(0, concurrentState!.Revision);

        Assert.True(await concurrent.TryAcquireLeaseAsync(
            concurrentFlowId,
            "owner-a",
            TimeSpan.FromMinutes(1)));
        Assert.False(await concurrent.TryAcquireLeaseAsync(
            concurrentFlowId,
            "owner-b",
            TimeSpan.FromMinutes(1)));

        concurrentState.Status = FlowRunStatus.Succeeded;
        concurrentState.Revision = 1;
        Assert.False(await concurrent.TryUpdateAsync(
            concurrentFlowId,
            concurrentState,
            expectedRevision: 0,
            TimeSpan.FromMinutes(5),
            leaseId: "owner-b"));
        Assert.True(await concurrent.TryUpdateAsync(
            concurrentFlowId,
            concurrentState,
            expectedRevision: 0,
            TimeSpan.FromMinutes(5),
            leaseId: "owner-a"));
        Assert.False(await concurrent.TryUpdateAsync(
            concurrentFlowId,
            concurrentState,
            expectedRevision: 0,
            TimeSpan.FromMinutes(5),
            leaseId: "owner-a"));
        Assert.Equal(1, (await store.LoadAsync(concurrentFlowId))!.Revision);

        Assert.False(await concurrent.TryRenewLeaseAsync(
            concurrentFlowId,
            "owner-b",
            TimeSpan.FromMinutes(1)));
        Assert.True(await concurrent.TryRenewLeaseAsync(
            concurrentFlowId,
            "owner-a",
            TimeSpan.FromMinutes(1)));
        await concurrent.ReleaseLeaseAsync(concurrentFlowId, "owner-b");
        Assert.False(await concurrent.TryAcquireLeaseAsync(
            concurrentFlowId,
            "owner-b",
            TimeSpan.FromMinutes(1)));
        await concurrent.ReleaseLeaseAsync(concurrentFlowId, "owner-a");
        Assert.True(await concurrent.TryAcquireLeaseAsync(
            concurrentFlowId,
            "owner-b",
            TimeSpan.FromMinutes(1)));
        await concurrent.ReleaseLeaseAsync(concurrentFlowId, "owner-b");
        Assert.True(await store.TryDeleteAsync(concurrentFlowId));

        Assert.True(await store.TryDeleteAsync(state.FlowId!));
        Assert.Null(await store.LoadAsync(state.FlowId!));
        Assert.False(await store.TryDeleteAsync(state.FlowId!));

        await AssertLeaseExpiryContractAsync(store);
        await AssertMissingFlowContractAsync(store);
        await AssertLargeStateContractAsync(store);
        await AssertCaseSensitiveFlowIdContractAsync(store);
        await AssertSchemaVersionContractAsync(store, seedRawStateAsync);
    }

    /// <summary>
    /// Flow ids are compared ORDINALLY by the engine, so two ids differing only in case are two
    /// different flows — and the store has to agree. It is the store that can disagree: SQL Server
    /// and MySQL columns inherit the database collation, and the common default is
    /// case-insensitive, which makes the second create collide on the primary key and a load
    /// return the OTHER run's state. Both stores now pin a binary collation on the column; this
    /// contract is what proves it, on every store the library ships.
    /// </summary>
    private static async Task AssertCaseSensitiveFlowIdContractAsync(IFlowStateStore store)
    {
        var suffix = Guid.NewGuid().ToString("N");
        var lower = $"flow-case-{suffix}";
        var upper = lower.ToUpperInvariant();

        Assert.True(await store.TryCreateAsync(lower, CreateState(lower), TimeSpan.FromMinutes(5)));
        Assert.True(
            await store.TryCreateAsync(upper, CreateState(upper), TimeSpan.FromMinutes(5)),
            "a flow id differing only in case is a DIFFERENT flow and must be creatable");

        Assert.Equal(lower, (await store.LoadAsync(lower))!.FlowId);
        Assert.Equal(upper, (await store.LoadAsync(upper))!.FlowId);

        // Leases are keyed by the same id, so a case-folding store would also let one run steal
        // the other's execution lease.
        Assert.True(await store.TryAcquireLeaseAsync(lower, "owner-lower", TimeSpan.FromMinutes(1)));
        Assert.True(await store.TryAcquireLeaseAsync(upper, "owner-upper", TimeSpan.FromMinutes(1)));
        await store.ReleaseLeaseAsync(lower, "owner-lower");
        await store.ReleaseLeaseAsync(upper, "owner-upper");

        Assert.True(await store.TryDeleteAsync(lower));
        Assert.NotNull(await store.LoadAsync(upper));
        Assert.True(await store.TryDeleteAsync(upper));
    }

    /// <summary>
    /// A lease has to expire on its own. The executor's crash-recovery path depends on it: a worker
    /// that dies mid-flow never releases its lease, and if the store honoured that lease forever the
    /// run would be unrecoverable — stuck Running with an owner that no longer exists.
    /// </summary>
    private static async Task AssertLeaseExpiryContractAsync(IFlowStateStore store)
    {
        var flowId = $"flow-lease-expiry-{Guid.NewGuid():N}";
        Assert.True(await store.TryCreateAsync(flowId, CreateState(flowId), TimeSpan.FromMinutes(5)));

        // A lease short enough to expire during the test, then long enough to hold afterwards.
        Assert.True(await store.TryAcquireLeaseAsync(flowId, "dead-worker", TimeSpan.FromMilliseconds(250)));
        Assert.False(await store.TryAcquireLeaseAsync(flowId, "live-worker", TimeSpan.FromMinutes(1)));

        // Poll rather than sleep once: stores differ in clock granularity (DynamoDB's TTL epoch is
        // whole seconds), so the steal becomes possible at slightly different moments.
        await EventuallyAsync(async () =>
            Assert.True(
                await store.TryAcquireLeaseAsync(flowId, "live-worker", TimeSpan.FromMinutes(1)),
                "an expired lease must become stealable"),
            TimeSpan.FromSeconds(15));

        // Renewing a lease you no longer hold must fail, or two workers would both believe they own
        // the run and both write checkpoints.
        Assert.False(await store.TryRenewLeaseAsync(flowId, "dead-worker", TimeSpan.FromMinutes(1)));
        Assert.True(await store.TryRenewLeaseAsync(flowId, "live-worker", TimeSpan.FromMinutes(1)));

        // Releasing a lease you do not hold is a no-op, not a theft.
        await store.ReleaseLeaseAsync(flowId, "dead-worker");
        Assert.False(await store.TryAcquireLeaseAsync(flowId, "third-worker", TimeSpan.FromMinutes(1)));

        await store.ReleaseLeaseAsync(flowId, "live-worker");
        Assert.True(await store.TryDeleteAsync(flowId));
    }

    /// <summary>
    /// Every operation on an unknown flow id answers "no" rather than throwing or inventing a row.
    /// The executor probes state it may never have written — a resume kick for an expired run, a
    /// callback arriving after the TTL swept the state.
    /// </summary>
    private static async Task AssertMissingFlowContractAsync(IFlowStateStore store)
    {
        var missing = $"flow-missing-{Guid.NewGuid():N}";

        Assert.Null(await store.LoadAsync(missing));
        Assert.False(await store.TryDeleteAsync(missing));
        Assert.False(await store.TryAcquireLeaseAsync(missing, "owner", TimeSpan.FromMinutes(1)));
        Assert.False(await store.TryRenewLeaseAsync(missing, "owner", TimeSpan.FromMinutes(1)));
        // Revision 1 against an expected 0: the store requires the new revision to increment the
        // expected one, so a well-formed update is what proves the "no such flow" answer rather than
        // an argument check firing first.
        var update = CreateState(missing);
        update.Revision = 1;
        Assert.False(await store.TryUpdateAsync(missing, update, expectedRevision: 0, TimeSpan.FromMinutes(5)));

        // Releasing a lease on a flow that does not exist must not throw either.
        await store.ReleaseLeaseAsync(missing, "owner");
    }

    /// <summary>
    /// A real flow accumulates state: a step per remote call, a memoized result per step, a values
    /// bag. This writes a state far larger than the tidy fixtures above to catch a store whose column
    /// or document silently truncates — the failure mode where a flow resumes with its later steps
    /// missing and re-executes work it already did.
    /// </summary>
    private static async Task AssertLargeStateContractAsync(IFlowStateStore store)
    {
        var flowId = $"flow-large-{Guid.NewGuid():N}";
        var state = CreateState(flowId);
        state.Steps = new Dictionary<string, FlowStepState>(StringComparer.Ordinal);
        state.Values = new Dictionary<string, string>(StringComparer.Ordinal);

        // ~64 KiB of state: past every "small text" column default, well under Cosmos's 2 MB item
        // ceiling and Mongo's 16 MB document ceiling, so it is a portable "large" for all ten stores.
        var chunk = new string('y', 512);
        for (var i = 0; i < 64; i++)
        {
            state.Steps[$"step-{i:00}"] = new FlowStepState
            {
                Completed = true,
                ResultJson = JsonSerializer.Serialize(new { index = i, blob = chunk }),
                CompletedAtUtc = DateTime.UtcNow
            };
            state.Values[$"value-{i:00}"] = JsonSerializer.Serialize(chunk);
        }

        Assert.True(await store.TryCreateAsync(flowId, state, TimeSpan.FromMinutes(5)));

        var loaded = await store.LoadAsync(flowId);
        Assert.NotNull(loaded);
        Assert.Equal(64, loaded!.Steps!.Count);
        Assert.Equal(64, loaded.Values!.Count);
        Assert.Contains(chunk, loaded.Steps["step-63"].ResultJson);
        Assert.True(await store.TryDeleteAsync(flowId));
    }

    /// <summary>
    /// A state written by a newer build must be refused, not executed against. Silently loading a
    /// schema this build does not understand is how a rolling deploy runs a flow with half its
    /// checkpoints invisible.
    /// <para>
    /// "Refused" means <see cref="FlowStateUnreadableException"/>, not <c>null</c>. Returning null
    /// says the run does not exist, and every caller answers that by acknowledging the wake-up and
    /// stopping — which is precisely the failure this contract exists to prevent: the ledger stays
    /// <c>Running</c> and the message that would have resumed it is gone. Throwing routes the
    /// delivery to the transport's retry/dead-letter path instead, where a replica that can read
    /// the ledger gets its turn.
    /// </para>
    /// </summary>
    private static async Task AssertSchemaVersionContractAsync(IFlowStateStore store, Func<string, string, Task>? seedRawStateAsync)
    {
        var flowId = $"flow-schema-{Guid.NewGuid():N}";
        var future = CreateState(flowId);
        future.SchemaVersion = FlowStateSchema.Current + 1;

        await Assert.ThrowsAnyAsync<ArgumentException>(
            () => store.TryCreateAsync(flowId, future, TimeSpan.FromMinutes(5)));

        // The write was rejected, so nothing was ever stored — this id really is absent, and null
        // is the right answer for it.
        Assert.Null(await store.LoadAsync(flowId));

        // Write rejection alone cannot catch a backend-specific READ regression: state written by
        // a NEWER build reaches this build through the database, never through this build's write
        // API. Callers that can write a raw record seed one and the read path must refuse it.
        if (seedRawStateAsync is not null)
        {
            var rawFlowId = $"flow-schema-raw-{Guid.NewGuid():N}";
            var rawFuture = CreateState(rawFlowId);
            rawFuture.SchemaVersion = FlowStateSchema.Current + 1;
            await seedRawStateAsync(rawFlowId, JsonSerializer.Serialize(rawFuture));

            var unreadable = await Assert.ThrowsAsync<FlowStateUnreadableException>(
                () => store.LoadAsync(rawFlowId));
            Assert.Equal(rawFlowId, unreadable.FlowId);
            Assert.Contains(
                $"schema version is {FlowStateSchema.Current + 1}",
                unreadable.Reason,
                StringComparison.Ordinal);
        }
    }

    /// <summary>Retries an assertion until it holds or the budget expires, then lets it through.</summary>
    private static async Task EventuallyAsync(Func<Task> assertion, TimeSpan budget)
    {
        var deadline = DateTime.UtcNow + budget;
        while (true)
        {
            try
            {
                await assertion();
                return;
            }
            catch when (DateTime.UtcNow < deadline)
            {
                await Task.Delay(250);
            }
        }
    }

    internal static FlowState CreateState(string flowId)
        => new()
        {
            FlowId = flowId,
            FlowTypeName = typeof(FlowStoreContract).FullName,
            InputTypeName = typeof(int).FullName,
            InputJson = JsonSerializer.Serialize(7),
            Status = FlowRunStatus.Running,
            LastMessage = "started",
            CreatedAtUtc = DateTime.UtcNow,
            UpdatedAtUtc = DateTime.UtcNow,
            Steps = new Dictionary<string, FlowStepState>(StringComparer.Ordinal)
            {
                ["step-a"] = new() { Completed = true, ResultJson = "123", CompletedAtUtc = DateTime.UtcNow }
            }
        };

    internal static string NewIdentifier(string prefix, int maxLength)
    {
        var identifier = $"{prefix}_{Guid.NewGuid():N}";
        return identifier.Length <= maxLength ? identifier : identifier[..maxLength];
    }
}
