using AsyncResponse.Testing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Regressions for round 27 (external full-codebase review). Each test drives the defect's real
/// path and fails on the pre-fix build.
/// </summary>
public sealed class Round27RegressionTests
{
    // -----------------------------------------------------------------------------------------
    // Finding — an unreadable ledger was indistinguishable from a missing one, so the executor
    // reported success and the transport acknowledged a live flow's only wake-up.
    // -----------------------------------------------------------------------------------------

    public sealed record R27Input(string Name);

    /// <summary>A flow that records having run, so a test can prove it never did.</summary>
    public sealed class MarkerFlow : IDurableFlow<R27Input>
    {
        public static int Executions;

        public Task ExecuteAsync(IDurableFlowContext flow, R27Input input)
        {
            Interlocked.Increment(ref Executions);
            return Task.CompletedTask;
        }
    }

    /// <summary>
    /// A store whose ledger bytes a test controls, so it can present the row a NEWER build wrote.
    /// This build refuses to write such a row (<c>ValidateWrite</c> rejects a foreign schema
    /// version), which is exactly why the defect only ever appeared on the read side — during a
    /// rolling deployment, against bytes the running build did not produce. Everything but the
    /// materialization delegates to a real store, so leases and revisions behave normally, and
    /// <c>LoadAsync</c> goes through the same <see cref="FlowStateJson.Deserialize"/> that every
    /// shipped store reaches via <c>DurableFlowStoreShared.ReadState</c>.
    /// </summary>
    private sealed class ForeignBytesFlowStateStore(InMemoryFlowStateStore _inner) : IFlowStateStore
    {
        public string? RawLedgerOverride { get; set; }

        public Task<FlowState?> LoadAsync(string flowId, CancellationToken cancellationToken = default)
            => RawLedgerOverride is { } raw
                ? Task.FromResult<FlowState?>(FlowStateJson.Deserialize(raw, flowId))
                : _inner.LoadAsync(flowId, cancellationToken);

        public Task<bool> TryCreateAsync(string flowId, FlowState state, TimeSpan ttl, CancellationToken cancellationToken = default)
            => _inner.TryCreateAsync(flowId, state, ttl, cancellationToken);

        public Task<bool> TryUpdateAsync(string flowId, FlowState state, long expectedRevision, TimeSpan ttl, string? leaseId = null, CancellationToken cancellationToken = default)
            => _inner.TryUpdateAsync(flowId, state, expectedRevision, ttl, leaseId, cancellationToken);

        public Task<bool> TryAcquireLeaseAsync(string flowId, string leaseId, TimeSpan leaseDuration, CancellationToken cancellationToken = default)
            => _inner.TryAcquireLeaseAsync(flowId, leaseId, leaseDuration, cancellationToken);

        public Task<bool> TryRenewLeaseAsync(string flowId, string leaseId, TimeSpan leaseDuration, CancellationToken cancellationToken = default)
            => _inner.TryRenewLeaseAsync(flowId, leaseId, leaseDuration, cancellationToken);

        public Task ReleaseLeaseAsync(string flowId, string leaseId, CancellationToken cancellationToken = default)
            => _inner.ReleaseLeaseAsync(flowId, leaseId, cancellationToken);

        public Task<bool> TryDeleteAsync(string flowId, CancellationToken cancellationToken = default)
            => _inner.TryDeleteAsync(flowId, cancellationToken);
    }

    private static ServiceProvider BuildFlowProvider(out ForeignBytesFlowStateStore store)
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services
            .AddAsyncResponse()
            .WithInMemoryChannel()
            .WithInMemoryTransport()
            .WithInMemoryDurableFlows()
            .WithDurableFlow<MarkerFlow, R27Input>();

        var backing = new ForeignBytesFlowStateStore(new InMemoryFlowStateStore());
        store = backing;
        services.AddSingleton<IFlowStateStore>(backing);
        return services.BuildServiceProvider();
    }

    private static FlowState RunningState(string flowId, int? schemaVersion = null) => new()
    {
        FlowId = flowId,
        FlowTypeName = typeof(MarkerFlow).FullName,
        InputTypeName = typeof(R27Input).FullName,
        InputJson = """{"Name":"acme"}""",
        Status = FlowRunStatus.Running,
        SchemaVersion = schemaVersion ?? FlowStateSchema.Current,
    };

    [Fact]
    public async Task LedgerWrittenByANewerSchema_FailsTheDelivery_InsteadOfAckingIt()
    {
        // The rolling-deployment case the schema gate exists for: this replica draws a job whose
        // ledger a newer replica already rewrote. Reading that as "nothing to execute" returned
        // successfully, the transport acked, and a Running flow was left with no wake-up left in
        // the system. It has to fail so redelivery reaches a replica that CAN read the ledger.
        await using var provider = BuildFlowProvider(out var store);
        var executor = provider.GetRequiredService<IDurableFlowExecutor>();

        const string flowId = "flow-newer-schema";
        var executionsBefore = Volatile.Read(ref MarkerFlow.Executions);

        // Started by a build that shared this schema...
        Assert.True(await store.TryCreateAsync(flowId, RunningState(flowId), TimeSpan.FromDays(1)));
        // ...then rewritten by a newer one, which is what this replica now finds on disk.
        store.RawLedgerOverride = FlowStateJson.Serialize(RunningState(flowId, FlowStateSchema.Current + 1));

        var ex = await Assert.ThrowsAsync<FlowStateUnreadableException>(() => executor.ExecuteAsync(flowId));

        Assert.Equal(flowId, ex.FlowId);
        Assert.Contains($"schema version is {FlowStateSchema.Current + 1}", ex.Reason, StringComparison.Ordinal);

        // The flow body never ran, and — the point of the fix — the throw is what reaches the
        // transport, so the job is retried or dead-lettered instead of acked as completed work.
        Assert.Equal(executionsBefore, Volatile.Read(ref MarkerFlow.Executions));
    }

    [Fact]
    public async Task GenuinelyMissingLedger_StillCompletesQuietly_SoTheWakeUpIsStillAcked()
    {
        // The other half of the contract. Absence is the one case where acknowledging is right:
        // there is no run to strand, and failing here would hot-loop a job for a flow that was
        // legitimately pruned or expired. The narrowing must not swallow that distinction going
        // the other way.
        await using var provider = BuildFlowProvider(out var store);
        var executor = provider.GetRequiredService<IDurableFlowExecutor>();

        Assert.Null(await store.LoadAsync("flow-that-never-existed"));
        await executor.ExecuteAsync("flow-that-never-existed");
    }

    // -----------------------------------------------------------------------------------------
    // Finding — StartAsync commits the ledger and then publishes. A publish failure left a
    // Running run with no wake-up, and with a generated id the caller never learned which run to
    // re-drive. IFlowStateStore has no enumeration, so nothing could find it afterwards either.
    // -----------------------------------------------------------------------------------------

    /// <summary>A worker transport that refuses to publish, for as long as a test wants.</summary>
    private sealed class FailingWorkerTransport : IWorkerTransport
    {
        public int PublishAttempts;
        public bool Fail = true;
        public readonly List<WorkerJobEnvelope> Published = [];

        public Task PublishAsync(WorkerJobEnvelope job, CancellationToken cancellationToken = default)
        {
            Interlocked.Increment(ref PublishAttempts);
            if (Fail)
                throw new InvalidOperationException("broker unavailable");

            lock (Published)
                Published.Add(job);
            return Task.CompletedTask;
        }
    }

    private static ServiceProvider BuildStartProvider(FailingWorkerTransport transport, VirtualTimeProvider clock)
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddSingleton<TimeProvider>(clock);
        services
            .AddAsyncResponse()
            .WithInMemoryChannel()
            .WithInMemoryDurableFlows()
            .WithDurableFlow<MarkerFlow, R27Input>();
        services.AddSingleton<IWorkerTransport>(transport);
        return services.BuildServiceProvider();
    }

    [Fact]
    public async Task StartWithAGeneratedId_WhosePublishFails_SurfacesTheIdSoTheRunCanBeReDriven()
    {
        var clock = new VirtualTimeProvider();
        var transport = new FailingWorkerTransport();
        await using var provider = BuildStartProvider(transport, clock);
        var flows = provider.GetRequiredService<IDurableFlows>();
        var store = provider.GetRequiredService<InMemoryFlowStateStore>();

        var start = flows.StartAsync<MarkerFlow, R27Input>(new R27Input("acme"));
        // Walk the retry ladder's backoff on virtual time.
        for (var i = 0; i < 8 && !start.IsCompleted; i++)
        {
            clock.Advance(TimeSpan.FromSeconds(5));
            await Task.Yield();
        }

        var ex = await Assert.ThrowsAsync<DurableFlowNotDispatchedException>(() => start);

        // The publish was actually retried, not given up on after one throw.
        Assert.True(transport.PublishAttempts > 1, $"expected the publish to be retried; saw {transport.PublishAttempts} attempt(s)");

        // The generated id survives the failure — this is what made the orphan unrecoverable.
        Assert.False(string.IsNullOrWhiteSpace(ex.FlowId));
        var orphan = await store.LoadAsync(ex.FlowId);
        Assert.NotNull(orphan);
        Assert.Equal(FlowRunStatus.Running, orphan!.Status);

        // And with the id, the documented recovery genuinely works: the same start re-enqueues the
        // existing run rather than creating a second one.
        transport.Fail = false;
        var reDriven = await flows.StartAsync<MarkerFlow, R27Input>(new R27Input("acme"), ex.FlowId);

        Assert.Equal(ex.FlowId, reDriven);
        lock (transport.Published)
            Assert.Single(transport.Published);
    }

    // -----------------------------------------------------------------------------------------
    // Finding — LostToken was cancelled only by a renewal call that RETURNED. A renewal that
    // hangs left it live past the server-side lease deadline, so anything watching the token saw
    // an owned lease while another replica was already free to take the flow over.
    // -----------------------------------------------------------------------------------------

    /// <summary>A store whose lease renewal never answers, like a wedged connection.</summary>
    private sealed class WedgedRenewalStore : IFlowStateStore
    {
        private readonly InMemoryFlowStateStore _inner = new();
        private readonly TaskCompletionSource _renewEntered = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task RenewEntered => _renewEntered.Task;

        public async Task<bool> TryRenewLeaseAsync(string flowId, string leaseId, TimeSpan leaseDuration, CancellationToken cancellationToken = default)
        {
            _renewEntered.TrySetResult();
            // Never answers. Real time, not virtual: no amount of advancing the clock can shake a
            // call loose that the server has simply stopped responding to — which is the whole
            // point. It unblocks only when disposal cancels.
            await Task.Delay(Timeout.Infinite, cancellationToken).ConfigureAwait(false);
            return true;
        }

        public Task<FlowState?> LoadAsync(string flowId, CancellationToken cancellationToken = default)
            => _inner.LoadAsync(flowId, cancellationToken);

        public Task<bool> TryCreateAsync(string flowId, FlowState state, TimeSpan ttl, CancellationToken cancellationToken = default)
            => _inner.TryCreateAsync(flowId, state, ttl, cancellationToken);

        public Task<bool> TryUpdateAsync(string flowId, FlowState state, long expectedRevision, TimeSpan ttl, string? leaseId = null, CancellationToken cancellationToken = default)
            => _inner.TryUpdateAsync(flowId, state, expectedRevision, ttl, leaseId, cancellationToken);

        public Task<bool> TryAcquireLeaseAsync(string flowId, string leaseId, TimeSpan leaseDuration, CancellationToken cancellationToken = default)
            => _inner.TryAcquireLeaseAsync(flowId, leaseId, leaseDuration, cancellationToken);

        public Task ReleaseLeaseAsync(string flowId, string leaseId, CancellationToken cancellationToken = default)
            => _inner.ReleaseLeaseAsync(flowId, leaseId, cancellationToken);

        public Task<bool> TryDeleteAsync(string flowId, CancellationToken cancellationToken = default)
            => _inner.TryDeleteAsync(flowId, cancellationToken);
    }

    [Fact]
    public async Task LeaseDeadlinePasses_CancelsLostToken_EvenWhileRenewalIsWedged()
    {
        var clock = new VirtualTimeProvider();
        var store = new WedgedRenewalStore();
        var options = new DurableFlowOptions
        {
            ExecutionLeaseDuration = TimeSpan.FromSeconds(30),
            ExecutionLeaseRenewInterval = TimeSpan.FromSeconds(10),
        };

        await using var lease = new FlowExecutionLease(store, "flow-wedged", "lease-1", options, NullLogger.Instance, clock);

        Assert.False(lease.LostToken.IsCancellationRequested);

        // Wake the renewal loop and let it wedge inside the store call.
        clock.Advance(TimeSpan.FromSeconds(11));
        await store.RenewEntered.WaitAsync(TimeSpan.FromSeconds(10));

        // Now walk past the lease deadline. The renewal call is still hanging and will never
        // report the loss, so only a deadline armed independently of it can.
        clock.Advance(TimeSpan.FromSeconds(30));

        Assert.True(
            lease.LostToken.WaitHandle.WaitOne(TimeSpan.FromSeconds(10)),
            "LostToken stayed live past the lease deadline because the wedged renewal never returned to report it.");
    }

    [Fact]
    public async Task SuccessfulRenewal_PushesTheDeadlineOut_SoALiveLeaseIsNotCancelled()
    {
        // The complement: the watcher re-reads the deadline every pass, so a lease that keeps
        // renewing must never be torn down by it.
        var clock = new VirtualTimeProvider();
        var store = new InMemoryFlowStateStore(clock);
        var options = new DurableFlowOptions
        {
            ExecutionLeaseDuration = TimeSpan.FromSeconds(30),
            ExecutionLeaseRenewInterval = TimeSpan.FromSeconds(10),
        };

        Assert.True(await store.TryCreateAsync("flow-live", RunningState("flow-live"), TimeSpan.FromDays(1)));
        Assert.True(await store.TryAcquireLeaseAsync("flow-live", "lease-1", options.ExecutionLeaseDuration));

        await using var lease = new FlowExecutionLease(store, "flow-live", "lease-1", options, NullLogger.Instance, clock);

        // Well past the original deadline, but renewals keep landing.
        clock.Advance(TimeSpan.FromMinutes(5));

        Assert.False(lease.LostToken.IsCancellationRequested);
        lease.ThrowIfLost();
    }

    // -----------------------------------------------------------------------------------------
    // Finding — a resolver registration was process-static with no way to remove it, so
    // RegisterAssembly pinned a collectible AssemblyLoadContext for the life of the process,
    // contradicting this type's own "never pins a collectible AssemblyLoadContext".
    // -----------------------------------------------------------------------------------------

    [Fact]
    public void DisposingAResolverRegistration_LetsItsCollectibleContextUnload()
    {
        var weakContext = RegisterPluginAssemblyAndDisposeTheHandle();

        for (var i = 0; i < 10 && weakContext.IsAlive; i++)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
        }

        Assert.False(
            weakContext.IsAlive,
            "The registration held the plugin's AssemblyLoadContext after its handle was disposed.");
    }

    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.NoInlining)]
    private static WeakReference RegisterPluginAssemblyAndDisposeTheHandle()
    {
        var context = new System.Runtime.Loader.AssemblyLoadContext($"r27-plugin-{Guid.NewGuid():N}", isCollectible: true);
        var assembly = context.LoadFromAssemblyPath(typeof(Round27RegressionTests).Assembly.Location);

#pragma warning disable IL2026 // Plugin scenario; the test asserts lifetime, not trimmability.
        var registration = AsyncResponseTypeResolution.RegisterAssembly(assembly);
#pragma warning restore IL2026

        // The registration is live and answering...
        Assert.NotNull(AsyncResponseTypeResolution.Resolve(typeof(PluginProbeMarker).FullName!));

        // ...and disposing it must both stop that and let go of the assembly.
        registration.Dispose();
        Assert.Null(AsyncResponseTypeResolution.Resolve(typeof(PluginProbeMarker).FullName!));
        registration.Dispose(); // idempotent

        var weak = new WeakReference(context);
        context.Unload();
        return weak;
    }

    /// <summary>A type loaded into the collectible twin, so the resolver has something to answer.</summary>
    public sealed class PluginProbeMarker;

    // -----------------------------------------------------------------------------------------
    // Finding — no inbound size budget existed anywhere: transport adapters materialized whatever
    // the broker handed them as a string and parsed it immediately.
    // -----------------------------------------------------------------------------------------

    [Fact]
    public async Task OversizedWorkerMessage_IsDroppedBeforeItIsEverParsed()
    {
        var target = new SizeProbeTarget();
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddSingleton<ISizeProbeTarget>(target);
        services.AddAsyncResponse(o => o.MaxInboundMessageChars = 256).WithInMemoryChannel();
        await using var provider = services.BuildServiceProvider();
        var ingress = provider.GetRequiredService<IAsyncResponseIngress>();

        // Valid, well-formed, authorized — and too big. Nothing about it should run.
        var json = System.Text.Json.JsonSerializer.Serialize(new WorkerJobEnvelope
        {
            Call = new ReflectionCallDto
            {
                ServiceInterfaceFullName = typeof(ISizeProbeTarget).FullName!,
                MethodName = nameof(ISizeProbeTarget.RunAsync),
                Params = [CallbackParam.ForValue(new string('x', 4096))],
            },
            CorrelationId = "cid",
        });
        Assert.True(json.Length > 256);

        await ingress.HandleWorkerMessageAsync(json);

        Assert.Equal(0, target.Calls);
    }

    [Fact]
    public async Task MessageInsideTheBudget_StillRuns()
    {
        var target = new SizeProbeTarget();
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddSingleton<ISizeProbeTarget>(target);
        services.AddAsyncResponse(o => o.MaxInboundMessageChars = 8192).WithInMemoryChannel();
        await using var provider = services.BuildServiceProvider();
        var ingress = provider.GetRequiredService<IAsyncResponseIngress>();

        var json = System.Text.Json.JsonSerializer.Serialize(new WorkerJobEnvelope
        {
            Call = new ReflectionCallDto
            {
                ServiceInterfaceFullName = typeof(ISizeProbeTarget).FullName!,
                MethodName = nameof(ISizeProbeTarget.RunAsync),
                Params = [CallbackParam.ForValue("small")],
            },
            CorrelationId = "cid",
        });
        Assert.True(json.Length < 8192);

        await ingress.HandleWorkerMessageAsync(json);

        Assert.Equal(1, target.Calls);
    }

    public interface ISizeProbeTarget { Task RunAsync(string value); }

    public sealed class SizeProbeTarget : ISizeProbeTarget
    {
        public int Calls;
        public Task RunAsync(string value) { Interlocked.Increment(ref Calls); return Task.CompletedTask; }
    }

    // -----------------------------------------------------------------------------------------
    // Finding — the watchdog's interval was exactly periodic, so replicas deployed together
    // scanned in lockstep forever, each firing one liveness probe per correlation id at once.
    // -----------------------------------------------------------------------------------------

    [Fact]
    public void WatchdogInterval_JittersByDefault_AndCanBeMadeExact()
    {
        var options = new AsyncResponseWatchdogOptions { Interval = TimeSpan.FromHours(6) };

        // Default: a bounded offset, so co-deployed replicas drift apart instead of colliding.
        Assert.Equal(TimeSpan.FromMinutes(36), options.ResolvedJitter);

        // Opt out for a single-host deployment, or a test asserting on scan timing.
        options.IntervalJitter = TimeSpan.Zero;
        Assert.Equal(TimeSpan.Zero, options.ResolvedJitter);

        // An explicit bound wins over the derived default.
        options.IntervalJitter = TimeSpan.FromSeconds(30);
        Assert.Equal(TimeSpan.FromSeconds(30), options.ResolvedJitter);
    }

    [Fact]
    public void WatchdogJitter_IsValidatedAgainstTheTimerCeiling()
    {
        // It is added to a timer-armed wait, so it shares that ceiling — caught at startup rather
        // than at the first arming inside the scan loop, where it would fault the host.
        var options = new AsyncResponseWatchdogOptions
        {
            Interval = TimeSpan.FromHours(1),
            IntervalJitter = TimeSpan.FromDays(400),
        };

        Assert.Throws<InvalidOperationException>(options.Validate);
    }

    // -----------------------------------------------------------------------------------------
    // Finding — three raw NUL bytes inside string literals made git classify a C# source file as
    // binary and made ripgrep skip it in directory searches, hiding a regression suite from
    // ordinary diffs and greps.
    // -----------------------------------------------------------------------------------------

    [Fact]
    public void NoCheckedInSourceFileContainsRawControlBytes()
    {
        // Control characters belong in source as escapes ('\0', '\u001f'), never as raw bytes.
        // A raw NUL is the one that bites hardest — git's binary heuristic stops at the first one,
        // so the file drops out of `git diff` AND out of `rg`, and code nobody can see or search
        // is code nobody reviews. Tab, CR and LF are the legitimate whitespace exceptions.
        var repoRoot = FindRepoRoot();
        var offenders = new List<string>();

        foreach (var path in Directory.EnumerateFiles(repoRoot, "*.cs", SearchOption.AllDirectories))
        {
            var relative = Path.GetRelativePath(repoRoot, path);
            if (relative.Contains($"{Path.DirectorySeparatorChar}obj{Path.DirectorySeparatorChar}", StringComparison.Ordinal)
                || relative.Contains($"{Path.DirectorySeparatorChar}bin{Path.DirectorySeparatorChar}", StringComparison.Ordinal))
                continue;

            var bytes = File.ReadAllBytes(path);
            var index = Array.FindIndex(bytes, b => b < 0x20 && b != (byte)'\t' && b != (byte)'\r' && b != (byte)'\n');
            if (index >= 0)
                offenders.Add($"{relative} (byte 0x{bytes[index]:x2} at offset {index})");
        }

        Assert.True(
            offenders.Count == 0,
            "Raw control bytes in checked-in C# source hide it from git diff and ripgrep; use escapes instead:"
            + Environment.NewLine + string.Join(Environment.NewLine, offenders));
    }

    private static string FindRepoRoot()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        while (directory is not null)
        {
            if (File.Exists(Path.Combine(directory.FullName, "AsyncResponse.slnx")))
                return directory.FullName;
            directory = directory.Parent!;
        }

        throw new InvalidOperationException("Repository root (AsyncResponse.slnx) not found above the test base directory.");
    }

    [Fact]
    public async Task ExpiredLedger_ReadsAsAbsent_NotAsUnreadable()
    {
        // TTL expiry is ordinary absence: the run really is gone, so it must keep acking rather
        // than joining the retry class.
        var clock = new TestTimeProvider();
        var store = new InMemoryFlowStateStore(clock);

        Assert.True(await store.TryCreateAsync("flow-expiring", RunningState("flow-expiring"), TimeSpan.FromMinutes(1)));
        clock.Advance(TimeSpan.FromMinutes(2));

        Assert.Null(await store.LoadAsync("flow-expiring"));
    }
}
