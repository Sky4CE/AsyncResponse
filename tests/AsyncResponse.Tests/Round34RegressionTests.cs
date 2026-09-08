using System.Collections.Concurrent;
using System.Text.Json;
using AsyncResponse.DurableFlows.Sqlite;
using AsyncResponse.Testing;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Regression pins for the round-34 review (2026-09-08). Every fact here compiles against the
/// pre-fix build and fails there: the file deliberately uses no new API (the new-API pins live in
/// <see cref="Round34NewApiTests"/>).
/// </summary>
public sealed class Round34RegressionTests
{
    // ---------------------------------------------------------------------------------------------
    // F1 — the lease-less "won response" checkpoint must be fenced to the attempt that won it.

    /// <summary>
    /// Round 34, finding 1: <c>CheckpointReceivedWithoutLeaseAsync</c> located its target by step
    /// name and "not completed" only. After a lease loss a takeover may already have timed the
    /// breadcrumb out and RE-TRIGGERED the step under a new correlation id — the reloaded step is
    /// then pending on that new id, and the stale executor's response completed it with a payload
    /// that answers the OLD request. The takeover skipped the step and consumed the wrong result.
    /// Pre-fix failure: the persisted step is completed with the stale response; post-fix it stays
    /// pending on the takeover's id and the stale response is discarded.
    /// </summary>
    [Fact]
    public async Task AwaitStep_LeaseLostAfterATakeoverReTriggered_DoesNotCompleteTheNewerAttemptWithTheStaleResponse()
    {
        var store = new TakeoverStore("remote", takeover: state =>
        {
            var step = state.Steps!["remote"];
            step.PendingCorrelationId = "takeover-cid";
            step.PendingPayloadTypeFullName = typeof(OperationResult).FullName;
            step.Completed = false;
            step.ResultJson = null;
            state.LastMessage = "re-triggered by the takeover";
        });
        var state = new FlowState { FlowId = "r34-stale-response-vs-takeover" };
        Assert.True(await store.TryCreateAsync(state.FlowId!, state, TimeSpan.FromMinutes(5)));

        var stale = new OperationResult { Status = OperationStatus.Completed, Message = "answer-to-the-OLD-request" };
        await using (var lease = await AcquireLeaseAsync(store, state.FlowId!))
        {
            var context = CreateContext(state, store, SubscriberReturning(Task.FromResult(stale)), lease);
            var surfaced = await Assert.ThrowsAsync<InvalidOperationException>(() => context.AwaitStepAsync<OperationResult>(
                "remote",
                _ => Task.CompletedTask));
            Assert.Contains("lost its execution lease", surfaced.Message, StringComparison.Ordinal);
        }

        Assert.Equal(1, store.Takeovers);
        var persisted = await store.LoadAsync(state.FlowId!);
        Assert.NotNull(persisted);
        var step = persisted!.Steps!["remote"];
        Assert.False(step.Completed);
        Assert.Equal("takeover-cid", step.PendingCorrelationId);
        Assert.Null(step.ResultJson);
        Assert.Equal("re-triggered by the takeover", persisted.LastMessage);
    }

    /// <summary>
    /// Round 34, finding 1 (second shape): the same unfenced write also mutated a run the takeover
    /// had already FAILED — a terminal ledger gained a completed step and a new LastMessage.
    /// Pre-fix failure: the Failed run's step is completed and its message rewritten.
    /// </summary>
    [Fact]
    public async Task AwaitStep_LeaseLostAfterTheRunWasFailed_LeavesTheTerminalLedgerUntouched()
    {
        var store = new TakeoverStore("remote", takeover: state =>
        {
            state.Status = FlowRunStatus.Failed;
            state.LastMessage = "failed by the takeover";
        });
        var state = new FlowState { FlowId = "r34-stale-response-vs-failed-run" };
        Assert.True(await store.TryCreateAsync(state.FlowId!, state, TimeSpan.FromMinutes(5)));

        var stale = new OperationResult { Status = OperationStatus.Completed, Message = "late" };
        await using (var lease = await AcquireLeaseAsync(store, state.FlowId!))
        {
            var context = CreateContext(state, store, SubscriberReturning(Task.FromResult(stale)), lease);
            await Assert.ThrowsAsync<InvalidOperationException>(() => context.AwaitStepAsync<OperationResult>("remote", _ => Task.CompletedTask));
        }

        var persisted = await store.LoadAsync(state.FlowId!);
        Assert.NotNull(persisted);
        Assert.Equal(FlowRunStatus.Failed, persisted!.Status);
        Assert.Equal("failed by the takeover", persisted.LastMessage);
        Assert.False(persisted.Steps!["remote"].Completed);
        Assert.Null(persisted.Steps["remote"].ResultJson);
    }

    /// <summary>
    /// Rejects the FIRST lease-fenced completion save for <c>stepName</c> exactly as a store answers
    /// a lost lease — and, before answering, applies <paramref name="takeover"/> to the persisted
    /// ledger through a lease-less write, so the rescue's reload sees what a real takeover wrote.
    /// </summary>
    private sealed class TakeoverStore(string stepName, Action<FlowState> takeover) : IFlowStateStore
    {
        private readonly InMemoryFlowStateStore _inner = new();
        private int _takeovers;

        public int Takeovers => Volatile.Read(ref _takeovers);

        public Task<bool> TryCreateAsync(string flowId, FlowState state, TimeSpan ttl, CancellationToken cancellationToken = default)
            => _inner.TryCreateAsync(flowId, state, ttl, cancellationToken);

        public Task<FlowState?> LoadAsync(string flowId, CancellationToken cancellationToken = default)
            => _inner.LoadAsync(flowId, cancellationToken);

        public async Task<bool> TryUpdateAsync(
            string flowId,
            FlowState state,
            long expectedRevision,
            TimeSpan ttl,
            string? leaseId = null,
            CancellationToken cancellationToken = default)
        {
            if (leaseId is not null
                && Volatile.Read(ref _takeovers) == 0
                && state.Steps is { } steps
                && steps.TryGetValue(stepName, out var step)
                && step.Completed)
            {
                Interlocked.Increment(ref _takeovers);
                var current = await _inner.LoadAsync(flowId, cancellationToken)
                    ?? throw new InvalidOperationException("The ledger under test vanished.");
                takeover(current);
                var revision = current.Revision;
                current.Revision = revision + 1;
                Assert.True(await _inner.TryUpdateAsync(flowId, current, revision, ttl, leaseId: null, cancellationToken));
                return false;
            }

            return await _inner.TryUpdateAsync(flowId, state, expectedRevision, ttl, leaseId, cancellationToken);
        }

        public Task<bool> TryAcquireLeaseAsync(string flowId, string leaseId, TimeSpan leaseDuration, CancellationToken cancellationToken = default)
            => _inner.TryAcquireLeaseAsync(flowId, leaseId, leaseDuration, cancellationToken);

        public Task<bool> TryRenewLeaseAsync(string flowId, string leaseId, TimeSpan leaseDuration, CancellationToken cancellationToken = default)
            => _inner.TryRenewLeaseAsync(flowId, leaseId, leaseDuration, cancellationToken);

        public Task ReleaseLeaseAsync(string flowId, string leaseId, CancellationToken cancellationToken = default)
            => _inner.ReleaseLeaseAsync(flowId, leaseId, cancellationToken);

        public Task<bool> TryDeleteAsync(string flowId, CancellationToken cancellationToken = default)
            => _inner.TryDeleteAsync(flowId, cancellationToken);
    }

    // ---------------------------------------------------------------------------------------------
    // F2 — worker-argument conversion must not quote the payload in the exception it throws.

    private const string PrivateMarker = "SYNTHETIC_PRIVATE_MARKER";

    /// <summary>
    /// Round 34, finding 2: the envelope parse was sanitized, but converting a JsonElement argument
    /// into the callback's parameter type used the raw serializer, whose JsonException carries
    /// <c>Path: $.&lt;key&gt;</c> — dictionary keys read straight off the wire. Pre-fix failure:
    /// the marker (a payload key) is in the exception chain.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void As_ConversionFailure_NeverQuotesThePayload(bool asElement)
    {
        var json = $$"""{"{{PrivateMarker}}":"not-an-int"}""";
        object value = asElement ? JsonSerializer.Deserialize<object>(json)! : json;

        var thrown = Assert.ThrowsAny<Exception>(() => value.As<Dictionary<string, int>>());

        for (var ex = thrown; ex is not null; ex = ex.InnerException)
        {
            Assert.DoesNotContain(PrivateMarker, ex.Message, StringComparison.Ordinal);
            Assert.DoesNotContain("not-an-int", ex.Message, StringComparison.Ordinal);
        }

        // Still diagnosable: size and reader position survive.
        Assert.Contains("code units", thrown.Message, StringComparison.Ordinal);
        Assert.Contains("byte position", thrown.Message, StringComparison.Ordinal);
    }

    public interface IDictionaryWorker
    {
        Task Take(Dictionary<string, int> values);
    }

    private sealed class DictionaryWorker : IDictionaryWorker
    {
        public Task Take(Dictionary<string, int> values) => Task.CompletedTask;
    }

    /// <summary>
    /// The path the review reproduced end to end: a worker envelope whose argument cannot convert
    /// to the parameter type, executed through the ingress, which logs the failure with its
    /// exception. Pre-fix failure: the logged exception chain names the payload's key.
    /// </summary>
    [Fact]
    public async Task HandleWorkerMessageAsync_ArgumentConversionFailure_NeverLogsThePayloadKeys()
    {
        var logger = new CollectingLogger();
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddSingleton<ILogger<AsyncResponseIngress>>(logger.For<AsyncResponseIngress>());
        services.AddSingleton<IDictionaryWorker>(new DictionaryWorker());
        services.AddAsyncResponse().WithInMemoryChannel();
        await using var provider = services.BuildServiceProvider();
        var ingress = provider.GetRequiredService<IAsyncResponseIngress>();

        var envelope = AsyncResponseJson.Serialize(new WorkerJobEnvelope
        {
            CorrelationId = "corr-r34-argument",
            Call = new ReflectionCallDto
            {
                ServiceInterfaceFullName = typeof(IDictionaryWorker).FullName!,
                MethodName = nameof(IDictionaryWorker.Take),
                Params = [CallbackParam.ForValue(new Dictionary<string, string> { [PrivateMarker] = "not-an-int" })]
            }
        });

        // Deterministic, so it propagates for the transport's bounded redelivery — unchanged.
        await Assert.ThrowsAnyAsync<Exception>(() => ingress.HandleWorkerMessageAsync(envelope));

        Assert.NotEmpty(logger.Entries);
        foreach (var (message, exception) in logger.Entries)
        {
            Assert.DoesNotContain(PrivateMarker, message, StringComparison.Ordinal);
            for (var ex = exception; ex is not null; ex = ex.InnerException)
                Assert.DoesNotContain(PrivateMarker, ex.Message, StringComparison.Ordinal);
        }
    }

    // ---------------------------------------------------------------------------------------------
    // F4 — expired in-memory ledgers must be swept without anyone loading their ids again.

    /// <summary>
    /// Round 34, finding 4: the in-memory store dropped an expired entry only when THAT id was
    /// loaded or replaced, which a completed run's id never is again — a long-lived process with
    /// unique flow ids retained every expired ledger for its lifetime. Pre-fix failure: 1001
    /// entries after a create that follows 1000 expired ledgers by a month.
    /// </summary>
    [Fact]
    public async Task InMemoryFlowStateStore_SweepsExpiredLedgersOnCreate_WithoutLoadingThem()
    {
        var clock = new VirtualTimeProvider();
        var store = new InMemoryFlowStateStore(clock);
        for (var i = 0; i < 1000; i++)
            Assert.True(await store.TryCreateAsync($"expired-{i}", new FlowState { FlowId = $"expired-{i}" }, TimeSpan.FromMinutes(1)));
        Assert.Equal(1000, EntryCount(store));

        clock.Advance(TimeSpan.FromDays(30));
        Assert.True(await store.TryCreateAsync("fresh", new FlowState { FlowId = "fresh" }, TimeSpan.FromMinutes(1)));

        Assert.Equal(1, EntryCount(store));
        Assert.NotNull(await store.LoadAsync("fresh"));
    }

    /// <summary>Retained entries, read off the private dictionary so this fact compiles against the pre-fix build.</summary>
    private static int EntryCount(InMemoryFlowStateStore store)
    {
        var entries = typeof(InMemoryFlowStateStore)
            .GetField("_entries", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!
            .GetValue(store);
        return ((System.Collections.ICollection)entries!).Count;
    }

    // ---------------------------------------------------------------------------------------------
    // F6 — memoized child snapshots must not compound with nesting depth.

    /// <summary>
    /// Round 34, finding 6: a child snapshot is stored as a JSON STRING inside the parent's ledger,
    /// so every ancestor level re-escapes the level below; carrying the child's own memoized
    /// grandchild snapshots along made the ledger grow exponentially with depth (a 72-byte leaf
    /// reached ~600 KB at depth 15 with no business payload). Descendant snapshots are now elided
    /// from the memo (the grandchild's own ledger keeps them). Pre-fix failure: the depth-15
    /// snapshot is hundreds of kilobytes.
    /// </summary>
    [Fact]
    public void SerializeSnapshot_ElidesDescendantSnapshots_SoNestingDepthDoesNotCompoundSize()
    {
        var snapshot = FlowStateJson.SerializeSnapshot(new FlowState { FlowId = "leaf", Status = FlowRunStatus.Succeeded });
        var depthOne = 0;
        for (var depth = 1; depth <= 15; depth++)
        {
            var parent = new FlowState
            {
                FlowId = $"depth-{depth}",
                Status = FlowRunStatus.Succeeded,
                Steps = new Dictionary<string, FlowStepState>(StringComparer.Ordinal)
                {
                    ["child"] = new() { Completed = true, ChildFlowId = $"depth-{depth - 1}", ResultJson = snapshot }
                }
            };
            snapshot = FlowStateJson.SerializeSnapshot(parent);
            if (depth == 1)
                depthOne = snapshot.Length;
        }

        // Depth-independent up to the id digits (pre-fix: ~600 KB at depth 15 against ~150 bytes).
        Assert.True(snapshot.Length < 2 * depthOne, $"depth 15 snapshot is {snapshot.Length} bytes; depth 1 was {depthOne}");
    }

    /// <summary>
    /// What the memo must still carry: the child's outcome, its local step results, and the
    /// elided step's identity (id, completion, fault marker) — and the instance handed in is
    /// restored, because the caller returns it to flow code.
    /// </summary>
    [Fact]
    public void SerializeSnapshot_KeepsLocalResultsAndChildIdentity_AndRestoresTheInstance()
    {
        var child = new FlowState
        {
            FlowId = "child",
            Status = FlowRunStatus.Failed,
            LastMessage = "child failed",
            Context = new Dictionary<string, string> { ["tenant"] = "acme" },
            Steps = new Dictionary<string, FlowStepState>(StringComparer.Ordinal)
            {
                ["local"] = new() { Completed = true, ResultJson = "\"local-result\"" },
                ["grandchild"] = new() { Completed = true, Faulted = true, ChildFlowId = "child:grandchild", ResultJson = "{\"FlowId\":\"child:grandchild\"}" }
            }
        };

        var memo = FlowStateJson.Deserialize(FlowStateJson.SerializeSnapshot(child), "child");

        Assert.Equal(FlowRunStatus.Failed, memo.Status);
        Assert.Equal("child failed", memo.LastMessage);
        Assert.Null(memo.Context);
        Assert.Equal("\"local-result\"", memo.Steps!["local"].ResultJson);
        var elided = memo.Steps["grandchild"];
        Assert.True(elided.Completed);
        Assert.True(elided.Faulted);
        Assert.Equal("child:grandchild", elided.ChildFlowId);
        Assert.Null(elided.ResultJson);

        // Restored: the flow code that receives this instance still sees everything.
        Assert.Equal("{\"FlowId\":\"child:grandchild\"}", child.Steps["grandchild"].ResultJson);
        Assert.Equal("acme", child.Context["tenant"]);
    }

    // ---------------------------------------------------------------------------------------------
    // F8 — a failure callback that keeps failing transiently must NOT acknowledge the response.

    private const string ExhaustedCorrelationId = "r34-failure-callback-exhausted";

    /// <summary>
    /// Round 34, finding 8: after its four in-process attempts the failure callback's fault was
    /// swallowed and the publish returned normally — the transport acknowledged a TERMINAL signal
    /// that then existed nowhere (the registration keeps the callback, not the payload; the
    /// watchdog only reports). The exhausted transient fault now propagates as a dedicated type so
    /// the transport redelivers under its own bounded policy. Pre-fix failure: SetResponse
    /// completes normally.
    /// </summary>
    [Fact]
    public async Task SetResponse_FailureCallbackExhaustsTransientRetries_ThrowsInsteadOfAcknowledging()
    {
        var time = new VirtualTimeProvider();
        var spy = new AlwaysThrowingFailureSpy();
        await using var provider = BuildProvider(time, spy);
        var recoveryStateStore = provider.GetRequiredService<IRecoveryStateStore>();
        await RegisterFailureCallbackAsync(recoveryStateStore, ExhaustedCorrelationId);

        var publisher = provider.GetRequiredService<IAsyncResponsePublisher>();
        var dispatching = publisher.SetResponse(
            new OperationResult { Status = OperationStatus.Failed, Message = "remote step failed" },
            ExhaustedCorrelationId);

        var thrown = await Assert.ThrowsAnyAsync<Exception>(() => DriveBackoffAsync(time, dispatching));

        Assert.Equal("RecoveryCallbackFailedException", thrown.GetType().Name);
        Assert.Contains(ExhaustedCorrelationId, thrown.Message, StringComparison.Ordinal);
        Assert.Equal(4, spy.Calls);
        // The registration stays armed for the redelivery.
        Assert.Single(await recoveryStateStore.GetAllAsync(ExhaustedCorrelationId));
    }

    /// <summary>
    /// The ingress half of finding 8: the broker ingress must neither re-run its own retry ladder
    /// over the dispatcher's (four attempts, not sixteen) nor escalate through SetException (which
    /// would only invoke the same failing callback again) — it passes the fault to the transport.
    /// Pre-fix failure: HandleResponseMessageAsync returns normally (the message would be ACKed).
    /// </summary>
    [Fact]
    public async Task HandleResponseMessageAsync_FailureCallbackExhausted_PropagatesForRedelivery_WithoutSetException()
    {
        var time = new VirtualTimeProvider();
        var spy = new AlwaysThrowingFailureSpy();
        await using var provider = BuildProvider(time, spy);
        var recoveryStateStore = provider.GetRequiredService<IRecoveryStateStore>();
        const string correlationId = ExhaustedCorrelationId + "-ingress";
        await RegisterFailureCallbackAsync(recoveryStateStore, correlationId);

        var ingress = provider.GetRequiredService<IAsyncResponseIngress>();
        var handling = ingress.HandleResponseMessageAsync(
            AsyncResponseJson.Serialize(new OperationResult { Status = OperationStatus.Failed, Message = "remote step failed" }),
            correlationId);

        var thrown = await Assert.ThrowsAnyAsync<Exception>(() => DriveBackoffAsync(time, handling));

        Assert.Equal("RecoveryCallbackFailedException", thrown.GetType().Name);
        Assert.Equal(4, spy.Calls);
        Assert.Single(await recoveryStateStore.GetAllAsync(correlationId));
    }

    /// <summary>
    /// The deliberately unchanged half: a DETERMINISTIC callback fault (the target interface is not
    /// registered) is still swallowed and acknowledged — redelivery cannot fix it, and RabbitMQ's
    /// unbounded default would hot-loop it — with the registration kept for the watchdog.
    /// </summary>
    [Fact]
    public async Task SetResponse_FailureCallbackWithUnresolvableTarget_StillAcknowledges()
    {
        var time = new VirtualTimeProvider();
        await using var provider = BuildProvider(time, spy: null);
        var recoveryStateStore = provider.GetRequiredService<IRecoveryStateStore>();
        const string correlationId = ExhaustedCorrelationId + "-unresolvable";
        await RegisterFailureCallbackAsync(recoveryStateStore, correlationId);

        var publisher = provider.GetRequiredService<IAsyncResponsePublisher>();
        await publisher.SetResponse(new OperationResult { Status = OperationStatus.Failed }, correlationId)
            .WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Single(await recoveryStateStore.GetAllAsync(correlationId));
    }

    private static ServiceProvider BuildProvider(VirtualTimeProvider time, IAlwaysThrowingFailureSpy? spy)
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddSingleton<TimeProvider>(time);
        if (spy is not null)
            services.AddSingleton<IAlwaysThrowingFailureSpy>(spy);
        services.AddAsyncResponse().WithInMemoryChannel();
        return services.BuildServiceProvider();
    }

    private static Task RegisterFailureCallbackAsync(IRecoveryStateStore recoveryStateStore, string correlationId)
        => recoveryStateStore.SaveAsync(
            correlationId,
            new RecoveryState
            {
                RegistrationId = Guid.NewGuid(),
                CorrelationId = correlationId,
                PayloadTypeFullName = typeof(OperationResult).FullName,
                RegisteredAtUtc = DateTime.UtcNow,
                FailureCallback = new ReflectionCallDto
                {
                    ServiceInterfaceFullName = typeof(IAlwaysThrowingFailureSpy).FullName!,
                    MethodName = nameof(IAlwaysThrowingFailureSpy.OnFailure),
                    Params = [CallbackParam.ForPlaceholder(PlaceholderType.Exception)]
                }
            },
            TimeSpan.FromMinutes(5));

    /// <summary>
    /// Fires every backoff timer the retry ladder arms on the virtual clock until
    /// <paramref name="operation"/> settles (bounded, so a ladder that never ends fails the test
    /// instead of hanging it), then awaits the operation's outcome.
    /// </summary>
    private static async Task DriveBackoffAsync(VirtualTimeProvider time, Task operation)
    {
        for (var round = 0; round < 12 && !operation.IsCompleted; round++)
        {
            var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(5);
            while (!operation.IsCompleted && time.NextTimerDueAt is null)
            {
                if (DateTime.UtcNow > deadline)
                    throw new TimeoutException("the retry ladder armed no backoff timer on the virtual clock");
                await Task.Delay(10);
            }

            if (!operation.IsCompleted)
                time.Advance(TimeSpan.FromSeconds(2));
        }

        await operation.WaitAsync(TimeSpan.FromSeconds(10));
    }

    public interface IAlwaysThrowingFailureSpy
    {
        Task OnFailure(Exception exception);
    }

    private sealed class AlwaysThrowingFailureSpy : IAlwaysThrowingFailureSpy
    {
        private int _calls;

        public int Calls => Volatile.Read(ref _calls);

        public Task OnFailure(Exception exception)
        {
            Interlocked.Increment(ref _calls);
            return Task.FromException(new InvalidOperationException("dependency still down"));
        }
    }

    // ---------------------------------------------------------------------------------------------
    // F9 — a scheduled occurrence whose ledger is committed but whose job was not published must
    //      be re-driven, in-process and across a restart.

    /// <summary>
    /// Round 34, finding 9: when the worker-job publish failed after the ledger commit, the
    /// scheduler logged the occurrence and advanced — the Running run had no wake-up and nothing
    /// ever retried it. It is now re-driven every RedriveInterval until published. Pre-fix
    /// failure: exactly one start attempt, and the second never comes.
    /// </summary>
    [Fact]
    public async Task ScheduledFlow_UndispatchedOccurrence_IsRedrivenUntilItsJobIsPublished()
    {
        var time = new VirtualTimeProvider(new DateTimeOffset(2030, 1, 1, 0, 0, 30, TimeSpan.Zero));
        var flows = new FakeFlows();
        const string occurrenceId = "sched:hourly:20300101T010000Z";
        var failuresLeft = 1;
        flows.OnStart = flowId =>
        {
            // The ledger is committed by the time the publish fails — the shape of the finding.
            flows.States[flowId] = new FlowState { FlowId = flowId, Status = FlowRunStatus.Running, Attempts = 0 };
            return Interlocked.Decrement(ref failuresLeft) >= 0
                ? new DurableFlowNotDispatchedException(flowId, new TimeoutException("broker down"))
                : null;
        };

        using var scheduler = new ScheduledFlowService(flows, [HourlyRegistration()], NullLogger<ScheduledFlowService>.Instance, time);
        await scheduler.StartAsync(CancellationToken.None);
        try
        {
            // Each advance waits for the loop to arm its sleep on the virtual clock first: an
            // advance that lands between the loop reading "now" and arming would leave a timer
            // aimed an hour past the occurrence.
            // Land a few seconds past the 01:00 occurrence — short of the 30-second re-drive.
            await WaitForArmedTimerAsync(time);
            time.Advance(TimeSpan.FromMinutes(59) + TimeSpan.FromSeconds(35));
            await flows.WaitForStartsAsync(1);
            Assert.Equal([occurrenceId], flows.Starts.ToArray());

            // Well before the NEXT occurrence (02:00): the re-drive fires on RedriveInterval.
            await WaitForArmedTimerAsync(time);
            time.Advance(TimeSpan.FromSeconds(30));
            await flows.WaitForStartsAsync(2);
            Assert.Equal([occurrenceId, occurrenceId], flows.Starts.ToArray());

            // Settled: no third attempt after the successful re-publish.
            await WaitForArmedTimerAsync(time);
            time.Advance(TimeSpan.FromMinutes(5));
            await Task.Delay(100);
            Assert.Equal(2, flows.Starts.Count);
        }
        finally
        {
            await scheduler.StopAsync(CancellationToken.None);
        }
    }

    /// <summary>
    /// The cross-restart half of finding 9: an in-process re-drive queue dies with its process,
    /// and a crash between the ledger commit and the publish never reached it. At startup the loop
    /// probes recent occurrences and re-drives any whose ledger is Running with zero attempts.
    /// Pre-fix failure: the committed, never-executed occurrence is never started again.
    /// </summary>
    [Fact]
    public async Task ScheduledFlow_StartupProbe_RedrivesACommittedNeverExecutedOccurrence()
    {
        var time = new VirtualTimeProvider(new DateTimeOffset(2030, 1, 1, 0, 0, 30, TimeSpan.Zero));
        var flows = new FakeFlows();
        // Both inside the default one-hour window of a half-hourly schedule started at 00:00:30.
        const string undispatched = "sched:half-hourly:20300101T000000Z";
        const string executed = "sched:half-hourly:20291231T233000Z";
        flows.States[undispatched] = new FlowState { FlowId = undispatched, Status = FlowRunStatus.Running, Attempts = 0 };
        flows.States[executed] = new FlowState { FlowId = executed, Status = FlowRunStatus.Running, Attempts = 1 };

        using var scheduler = new ScheduledFlowService(flows, [Registration("half-hourly", "*/30 * * * *")], NullLogger<ScheduledFlowService>.Instance, time);
        await scheduler.StartAsync(CancellationToken.None);
        try
        {
            await flows.WaitForStartsAsync(1);
            Assert.Equal([undispatched], flows.Starts.ToArray());

            // The executed one (attempts > 0) and the ledger-less ones are left alone.
            await Task.Delay(100);
            Assert.Single(flows.Starts);
        }
        finally
        {
            await scheduler.StopAsync(CancellationToken.None);
        }
    }

    private static async Task WaitForArmedTimerAsync(VirtualTimeProvider time)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
        while (time.NextTimerDueAt is null)
        {
            if (DateTime.UtcNow > deadline)
                throw new TimeoutException("the scheduler loop armed no sleep on the virtual clock");
            await Task.Delay(10);
        }
    }

    private static ScheduledFlowRegistration HourlyRegistration() => Registration("hourly", "0 * * * *");

    private static ScheduledFlowRegistration Registration(string name, string cron) => new()
    {
        Name = name,
        CronExpression = cron,
        Options = new ScheduledFlowOptions(),
        StartOccurrenceAsync = static (flows, flowId, occurrence, cancellationToken) =>
            flows.StartAsync<NightlyReportFlow, ReportInput>(new ReportInput(occurrence), flowId, cancellationToken)
    };

    private sealed class FakeFlows : IDurableFlows
    {
        public ConcurrentDictionary<string, FlowState> States { get; } = new(StringComparer.Ordinal);

        public ConcurrentQueue<string> Starts { get; } = new();

        /// <summary>Returns the exception a start should throw, or null to succeed.</summary>
        public Func<string, Exception?> OnStart { get; set; } = _ => null;

        public Task<string> StartAsync<TFlow, TInput>(TInput input, string? flowId = null, CancellationToken cancellationToken = default)
            where TFlow : class, IDurableFlow<TInput>
        {
            Starts.Enqueue(flowId!);
            if (OnStart(flowId!) is { } failure)
                throw failure;
            return Task.FromResult(flowId!);
        }

        public Task ResumeAsync(string flowId, CancellationToken cancellationToken = default) => Task.CompletedTask;

        public Task<FlowState?> GetStateAsync(string flowId, CancellationToken cancellationToken = default)
            => Task.FromResult(States.TryGetValue(flowId, out var state) ? state : null);

        public async Task WaitForStartsAsync(int count)
        {
            var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
            while (Starts.Count < count)
            {
                if (DateTime.UtcNow > deadline)
                    throw new TimeoutException($"Expected {count} start(s); saw [{string.Join(", ", Starts)}].");
                await Task.Delay(10);
            }
        }
    }

    // ---------------------------------------------------------------------------------------------
    // F5 — the relational prune must drain a backlog, not one batch per interval.

    /// <summary>
    /// Round 34, finding 5, proven against the real in-process SQLite store: one 1000-row batch per
    /// prune interval capped cleanup at ~3 rows/second, which any busier instance outgrew forever.
    /// The prune now drains batches until one comes back short or the budget lapses. Pre-fix
    /// failure: 1200 of 2200 expired rows survive the create that pruned.
    /// </summary>
    [Fact]
    public async Task SqliteTryCreate_DrainsTheWholeExpiredBacklog_NotOneBatch()
    {
        await using var database = new TempSqlite();
        var options = new SqliteDurableFlowOptions
        {
            ConnectionString = database.ConnectionString,
            // Seed without pruning: the first create consumes the interval, the rest ride it.
            PruneInterval = TimeSpan.FromHours(1)
        };
        var store = new SqliteFlowStateStore(Options.Create(options));

        const int backlog = 2200;
        for (var i = 0; i < backlog; i++)
            Assert.True(await store.TryCreateAsync($"expired-{i}", NewState($"expired-{i}"), TimeSpan.FromMilliseconds(1)));
        await Task.Delay(50);
        Assert.Equal(backlog, await database.CountExpiredAsync());

        // The same options instance the store holds: the next create prunes.
        options.PruneInterval = TimeSpan.Zero;
        Assert.True(await store.TryCreateAsync("fresh", NewState("fresh"), TimeSpan.FromMinutes(5)));

        Assert.Equal(0, await database.CountExpiredAsync());
        Assert.Equal("fresh", (await store.LoadAsync("fresh"))?.FlowId);
    }

    private static FlowState NewState(string flowId) => new()
    {
        FlowId = flowId,
        FlowTypeName = "PruneBacklogFlow",
        Status = FlowRunStatus.Running,
        Steps = []
    };

    private sealed class TempSqlite : IAsyncDisposable
    {
        private readonly string _path = Path.Combine(Path.GetTempPath(), $"ar-prune-backlog-{Guid.NewGuid():N}.db");

        public string ConnectionString => $"Data Source={_path}";

        public async Task<long> CountExpiredAsync()
        {
            await using var connection = new SqliteConnection(ConnectionString);
            await connection.OpenAsync();
            await using var command = connection.CreateCommand();
            command.CommandText = """SELECT COUNT(*) FROM "asyncresponse_flow_state" WHERE flow_id LIKE 'expired-%';""";
            return (long)(await command.ExecuteScalarAsync())!;
        }

        public ValueTask DisposeAsync()
        {
            SqliteConnection.ClearPool(new SqliteConnection(ConnectionString));
            foreach (var suffix in new[] { "", "-wal", "-shm" })
            {
                try
                {
                    File.Delete(_path + suffix);
                }
                catch (IOException)
                {
                }
            }

            return ValueTask.CompletedTask;
        }
    }

    // ---------------------------------------------------------------------------------------------
    // Shared helpers (mirroring Round33RegressionTests).

    private static DurableFlowContext CreateContext(
        FlowState state,
        IFlowStateStore store,
        IAsyncResponseSubscriber subscriber,
        FlowExecutionLease lease)
        => new(
            state,
            store,
            Mock.Of<IAsyncResponseBuilder>(),
            new AsyncResponseContextPropagation([]),
            new DurableFlowOptions(),
            subscriber,
            null,
            NullLogger.Instance,
            lease);

    private static async Task<FlowExecutionLease> AcquireLeaseAsync(IFlowStateStore store, string flowId)
    {
        var lease = await FlowStateConcurrency.TryAcquireExecutionLeaseAsync(
            store,
            flowId,
            new DurableFlowOptions(),
            NullLogger.Instance);
        return Assert.IsType<FlowExecutionLease>(lease);
    }

    private static IAsyncResponseSubscriber SubscriberReturning(Task<OperationResult> responseTask)
    {
        var waiter = new Mock<IAsyncResponseWaiter<OperationResult>>();
        waiter.SetupGet(instance => instance.ResponseTask).Returns(responseTask);
        waiter.Setup(instance => instance.DisposeAsync()).Returns(ValueTask.CompletedTask);

        var subscriber = new Mock<IAsyncResponseSubscriber>();
        subscriber.Setup(instance => instance.CreateResponseWaiter<OperationResult>(
                It.IsAny<string>(),
                It.IsAny<Func<OperationResult, ValueTask<bool>>?>(),
                It.IsAny<TimeSpan?>()))
            .ReturnsAsync(waiter.Object);
        return subscriber.Object;
    }
}
