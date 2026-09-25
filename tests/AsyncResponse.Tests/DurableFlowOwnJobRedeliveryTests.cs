using AsyncResponse.Testing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text.Json;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>A worker-job target that records the ambient job it runs under.</summary>
public interface IWorkerJobScopeProbe
{
    Task RunAsync();
}

/// <summary>
/// The broker redelivers a worker job whose handler is STILL RUNNING once an in-flight ceiling
/// lapses (Pub/Sub <c>MaxTotalAckExtension</c>, RabbitMQ <c>consumer_timeout</c>, the SQS 12-hour
/// visibility cap, a Kafka rebalance). For a durable flow that redelivery finds the execution lease
/// held — by its own first delivery — and sees it renewed. Acknowledging it as "a duplicate of a
/// live execution" discards the only copy of the wake-up the broker still has: when the holder's
/// process later ends, nothing redelivers and the run stays <see cref="FlowRunStatus.Running"/>
/// forever, with no dead-letter entry.
/// <para>
/// Every delivery here goes through the real <c>WorkerJobExecutor</c>, from the envelope's wire
/// form, the way a broker hands it over — the job identity has to survive the wire, reach the flow
/// executor through the ambient job scope, and be recorded with the lease for any of this to work.
/// The decision rule itself is pinned in isolation in <see cref="FlowLeaseContentionTests"/>.
/// </para>
/// </summary>
public sealed class DurableFlowOwnJobRedeliveryTests
{
    // The holder's lease is generous on purpose: a renewal delayed by a loaded test host must
    // never lose it, or the contender would simply (and correctly) take the run over.
    private static DurableFlowOptions HolderLease => new()
    {
        ExecutionLeaseDuration = TimeSpan.FromSeconds(10),
        ExecutionLeaseRenewInterval = TimeSpan.FromMilliseconds(40)
    };

    // A short window, and a contention budget that caps the store-driven deadline extension, so
    // a delivery that must NOT be acknowledged is handed back in about a second.
    private static DurableFlowOptions ContenderLease => new()
    {
        ExecutionLeaseDuration = TimeSpan.FromMilliseconds(200),
        ExecutionLeaseRenewInterval = TimeSpan.FromMilliseconds(40),
        MaxLeaseContentionWait = TimeSpan.FromSeconds(1)
    };

    // ---------------------------------------------------------------------------------------
    // The defect: the holder's own job, redelivered under a live handler.
    // ---------------------------------------------------------------------------------------

    [Fact]
    public async Task HoldersOwnJob_RedeliveredWhileItsHandlerStillRuns_IsNeverAckedAsADuplicate()
    {
        var store = new InMemoryFlowStateStore();
        var flow = new ParkedFlow();
        var transport = new RecordingTransport();
        await using var holder = new Deployment(store, flow, HolderLease, transport);
        await using var contender = new Deployment(store, flow, ContenderLease, transport);
        await store.TryCreateAsync("own-job", RunningState("own-job"), TimeSpan.FromMinutes(5));

        // One job, minted by the real builder; the broker delivers it, then — its in-flight
        // ceiling lapsed under the parked handler — delivers the SAME message again.
        var wireJob = await holder.EnqueueExecuteAsync("own-job");
        var firstDelivery = holder.DeliverAsync(wireJob);
        await flow.Entered.Task.WaitAsync(TimeSpan.FromSeconds(15));

        var contended = await Assert.ThrowsAsync<DurableFlowLeaseContendedException>(
            () => contender.DeliverAsync(wireJob).WaitAsync(TimeSpan.FromSeconds(30)));

        Assert.Equal("own-job", contended.FlowId);
        Assert.Contains("in-flight ceiling", contended.Reason, StringComparison.Ordinal);
        Assert.Contains("consumer_timeout", contended.Reason, StringComparison.Ordinal);
        Assert.Contains("MaxTotalAckExtension", contended.Reason, StringComparison.Ordinal);
        Assert.Equal(1, flow.Executions);
        Assert.False(firstDelivery.IsCompleted, "The holder must still be running: this is a redelivery UNDER a live handler.");
        Assert.Contains(
            contender.Log.Entries,
            entry => entry.Level == LogLevel.Warning && entry.Message.Contains("in-flight ceiling", StringComparison.Ordinal));

        flow.Release.SetResult();
        await firstDelivery.WaitAsync(TimeSpan.FromSeconds(15));
        Assert.Equal(FlowRunStatus.Succeeded, (await store.LoadAsync("own-job"))!.Status);
    }

    [Fact]
    public async Task HoldersOwnJob_OnATransportThatCanDelay_IsRepublishedAsTheSameJob_PastTheLease_AndOnlyThenAcked()
    {
        var store = new InMemoryFlowStateStore();
        var flow = new ParkedFlow();
        var transport = new RecordingDelayedTransport();
        await using var holder = new Deployment(store, flow, HolderLease, transport);
        await using var contender = new Deployment(
            store,
            flow,
            new DurableFlowOptions { ExecutionLeaseDuration = TimeSpan.FromMilliseconds(200), ExecutionLeaseRenewInterval = TimeSpan.FromMilliseconds(40) },
            transport);
        await store.TryCreateAsync("own-job-delayed", RunningState("own-job-delayed"), TimeSpan.FromMinutes(5));

        var wireJob = await holder.EnqueueExecuteAsync("own-job-delayed");
        var jobId = Read(wireJob).JobId;
        var firstDelivery = holder.DeliverAsync(wireJob);
        await flow.Entered.Task.WaitAsync(TimeSpan.FromSeconds(15));

        var before = DateTime.UtcNow;
        await contender.DeliverAsync(wireJob).WaitAsync(TimeSpan.FromSeconds(30));
        var after = DateTime.UtcNow;

        // Acknowledged — but only after the wake-up went back to the broker, as the SAME job: a
        // fresh id would read as a different, redundant job at its next contention and be
        // acknowledged on the holder's renewal, recreating the loss one hop later.
        var (hopWire, delay) = Assert.Single(transport.Delayed);
        var hop = Read(hopWire);
        Assert.NotNull(jobId);
        Assert.Equal(jobId, hop.JobId);
        Assert.Equal(Read(wireJob).Call.MethodName, hop.Call.MethodName);
        Assert.Equal(JsonSerializer.Serialize(Read(wireJob).Call), JsonSerializer.Serialize(hop.Call));
        // Due about when the holder's lease would lapse if it died now (10 s lease, renewed every 40 ms).
        Assert.InRange(hop.NotBeforeUtc!.Value, before.AddSeconds(8), after.AddSeconds(11));
        Assert.InRange(delay, TimeSpan.FromSeconds(8), TimeSpan.FromSeconds(11));
        Assert.Null(hop.LastRedelayRemaining);
        Assert.Equal(1, flow.Executions);
        Assert.False(firstDelivery.IsCompleted);

        // While the holder lives, the hop lands in the same contention and goes round again.
        await contender.DeliverAsync(hopWire, at: hop.NotBeforeUtc.Value.AddSeconds(1)).WaitAsync(TimeSpan.FromSeconds(30));
        Assert.Equal(2, transport.Delayed.Count);
        Assert.Equal(jobId, Read(transport.Delayed.Last().WireJob).JobId);

        // Once the holder finishes, the hop finds a terminal ledger and is simply acknowledged.
        flow.Release.SetResult();
        await firstDelivery.WaitAsync(TimeSpan.FromSeconds(15));
        await contender.DeliverAsync(hopWire, at: hop.NotBeforeUtc.Value.AddSeconds(1)).WaitAsync(TimeSpan.FromSeconds(30));
        Assert.Equal(2, transport.Delayed.Count);
        Assert.Equal(1, flow.Executions);
        Assert.Equal(FlowRunStatus.Succeeded, (await store.LoadAsync("own-job-delayed"))!.Status);
    }

    [Fact]
    public async Task HoldersOwnJob_WhoseRepublishFails_StaysWithTheTransport()
    {
        var store = new InMemoryFlowStateStore();
        var flow = new ParkedFlow();
        var transport = new RecordingDelayedTransport();
        await using var holder = new Deployment(store, flow, HolderLease, transport);
        await using var contender = new Deployment(store, flow, ContenderLease, transport);
        await store.TryCreateAsync("own-job-publish-fails", RunningState("own-job-publish-fails"), TimeSpan.FromMinutes(5));

        var wireJob = await holder.EnqueueExecuteAsync("own-job-publish-fails");
        var firstDelivery = holder.DeliverAsync(wireJob);
        await flow.Entered.Task.WaitAsync(TimeSpan.FromSeconds(15));

        // The ack is only safe AFTER the copy is back at the broker.
        transport.FailDelayedPublishWith = new IOException("broker unreachable");
        var failure = await Assert.ThrowsAsync<IOException>(
            () => contender.DeliverAsync(wireJob).WaitAsync(TimeSpan.FromSeconds(30)));

        Assert.Equal("broker unreachable", failure.Message);
        Assert.Empty(transport.Delayed);

        flow.Release.SetResult();
        await firstDelivery.WaitAsync(TimeSpan.FromSeconds(15));
    }

    [Fact]
    public async Task ALiveHoldersRenewals_NeverExtendTheOwnJobDeliverysDeadline()
    {
        // A holder that renews forever, on the virtual clock: every observation reports the same
        // own-job lease with an expiry one full lease ahead of "now". The delivery must come back
        // to the transport on the deadline fixed by the FIRST observation (its expiry plus one
        // window: 60 s + 80 s with the defaults) — not chase the renewals.
        var time = new VirtualTimeProvider();
        var options = new DurableFlowOptions();
        var job = ExecuteJob("own-job-deadline", "job-deadline");
        var leaseId = FlowLeaseContention.NewLeaseId(FlowLeaseContention.JobTag(job.JobId));
        var store = await RenewingHolderStore.CreateAsync(
            "own-job-deadline",
            () => new FlowLeaseObservation(leaseId, time.GetUtcNow().UtcDateTime + options.ExecutionLeaseDuration));
        await using var contender = new Deployment(store, new ParkedFlow(), options, new RecordingTransport(), time);

        var started = time.GetUtcNow();
        var delivery = contender.DeliverAsync(Wire(job));
        for (var step = 0; step < 600 && !delivery.IsCompleted; step++)
        {
            await WaitForArmedTimerOrCompletionAsync(time, delivery);
            if (!delivery.IsCompleted)
                time.Advance(TimeSpan.FromSeconds(1));
        }

        Assert.True(delivery.IsCompleted, "Ten virtual minutes in, the delivery is still polling a lease its own job's holder keeps renewing.");
        var contended = await Assert.ThrowsAsync<DurableFlowLeaseContendedException>(() => delivery);
        Assert.Contains("in-flight ceiling", contended.Reason, StringComparison.Ordinal);
        Assert.InRange(time.GetUtcNow() - started, TimeSpan.FromSeconds(139), TimeSpan.FromSeconds(145));
    }

    // ---------------------------------------------------------------------------------------
    // A park's own follow-up wake-up.
    // ---------------------------------------------------------------------------------------

    [Fact]
    public async Task AParksOwnFollowUpWakeUp_IsNotAckedAsADuplicate_WhileTheParkedBodyIsStillUnwinding()
    {
        // The holder parks on a child and then lingers in a finally block: user cleanup, an
        // `await using` resource, a catch-and-rethrow — a park is a cancellation, so all of them
        // run on every park. The child finishes at once and wakes the parent. That wake-up is the
        // park's OWN continuation, not a duplicate of the holder's job: the holder's job is
        // acknowledged the moment the body finishes unwinding. Judged against a holder that was
        // still renewing its lease, it used to be acknowledged as a duplicate — and the run stayed
        // Running with nothing queued and nothing dead-lettered.
        var store = new InMemoryFlowStateStore();
        var parent = new ParkThenLingerFlow();
        var transport = new RecordingTransport();
        await using var holder = new Deployment(store, new ParkedFlow(), HolderLease, transport, extraFlows: [parent, new ChildNoopFlow()]);
        await using var contender = new Deployment(store, new ParkedFlow(), ContenderLease, transport, extraFlows: [parent, new ChildNoopFlow()]);
        await store.TryCreateAsync("park-successor", RunningState("park-successor", typeof(ParkThenLingerFlow)), TimeSpan.FromMinutes(5));

        var firstDelivery = holder.DeliverAsync(Wire(ExecuteJob("park-successor", "job-first")));
        await parent.Lingering.Task.WaitAsync(TimeSpan.FromSeconds(15));

        // The park published the child's start; the child runs, finishes, and wakes the parent.
        var childJob = Assert.Single(transport.Immediate);
        await holder.DeliverAsync(childJob).WaitAsync(TimeSpan.FromSeconds(15));
        Assert.Equal(2, transport.Immediate.Count);
        var wakeUp = transport.Immediate.Last();

        await contender.DeliverAsync(wakeUp).WaitAsync(TimeSpan.FromSeconds(30));

        Assert.False(firstDelivery.IsCompleted, "The holder must still be unwinding its parked body.");
        Assert.Equal(2, parent.Executions);
        Assert.Equal(FlowRunStatus.Succeeded, (await store.LoadAsync("park-successor"))!.Status);

        // The unwound holder writes nothing over the run its successor already finished.
        parent.Linger.SetResult();
        await firstDelivery.WaitAsync(TimeSpan.FromSeconds(15));
        Assert.Equal(FlowRunStatus.Succeeded, (await store.LoadAsync("park-successor"))!.Status);
    }

    [Fact]
    public async Task HostStop_WhileAWakeUpWaitsOutADeadHoldersLease_HandsItBackAsAnInterruption_NotAPlainCancellation()
    {
        // ApplicationStopping fires before any hosted service stops, so the worker subscriber's own
        // token is usually still live when the poll gives up: only the exception's TYPE tells the
        // dispatcher "hand this back, it is not a failure". A plain OperationCanceledException read
        // as a handler failure — a NAK, a retry ladder, a dead-lettered wake-up.
        var store = new InMemoryFlowStateStore();
        var flow = new ParkedFlow();
        flow.Release.SetResult();
        using var host = new DurableFlowContextTestSupport.StoppingHost();
        await using var contender = new Deployment(
            store,
            flow,
            new DurableFlowOptions { ExecutionLeaseDuration = TimeSpan.FromSeconds(10), ExecutionLeaseRenewInterval = TimeSpan.FromMilliseconds(40) },
            new RecordingTransport(),
            hostLifetime: host);
        await store.TryCreateAsync("host-stop-poll", RunningState("host-stop-poll"), TimeSpan.FromMinutes(5));
        Assert.True(await store.TryAcquireLeaseAsync("host-stop-poll", FlowLeaseContention.NewLeaseId(null), TimeSpan.FromMinutes(5)));

        var delivery = contender.DeliverAsync(Wire(ExecuteJob("host-stop-poll", "job-poll")));
        host.StopApplication();

        var interrupted = await Assert.ThrowsAsync<DurableFlowInterruptedException>(() => delivery.WaitAsync(TimeSpan.FromSeconds(30)));
        Assert.Contains("Host is stopping", interrupted.Message, StringComparison.Ordinal);
        Assert.Equal(0, flow.Executions);
    }

    // ---------------------------------------------------------------------------------------
    // The cases that must NOT change.
    // ---------------------------------------------------------------------------------------

    [Theory]
    [InlineData("job-a", "job-b")]   // a second, independently enqueued job: the holder's own job is still at the broker
    [InlineData(null, null)]         // both written before JobId existed
    [InlineData("job-a", null)]      // rolling upgrade: an old producer's job contends with a new holder
    [InlineData(null, "job-b")]      // ...and the other way round
    public async Task ADifferentOrUnidentifiedJob_ContendingWithALiveHolder_IsStillAckedAsADuplicate(string? holderJobId, string? contenderJobId)
    {
        var flowId = $"other-job-{holderJobId ?? "legacy"}-{contenderJobId ?? "legacy"}";
        var store = new InMemoryFlowStateStore();
        var flow = new ParkedFlow();
        var transport = new RecordingDelayedTransport();
        await using var holder = new Deployment(store, flow, HolderLease, transport);
        await using var contender = new Deployment(store, flow, ContenderLease, transport);
        await store.TryCreateAsync(flowId, RunningState(flowId), TimeSpan.FromMinutes(5));

        var firstDelivery = holder.DeliverAsync(Wire(ExecuteJob(flowId, holderJobId)));
        await flow.Entered.Task.WaitAsync(TimeSpan.FromSeconds(15));

        var waited = Stopwatch.StartNew();
        await contender.DeliverAsync(Wire(ExecuteJob(flowId, contenderJobId))).WaitAsync(TimeSpan.FromSeconds(30));

        Assert.True(waited.Elapsed < TimeSpan.FromSeconds(10), $"Acked after {waited.ElapsedMilliseconds} ms; a renewal is observable within a poll or two.");
        Assert.Empty(transport.Delayed);
        Assert.Equal(1, flow.Executions);

        flow.Release.SetResult();
        await firstDelivery.WaitAsync(TimeSpan.FromSeconds(15));
    }

    [Fact]
    public async Task ADeadHolderOfTheSameJob_IsWaitedOut_AndTheRedeliveryTakesTheRunOver()
    {
        // The ordinary crash: the holder died, the broker redelivers its job inside the unexpired
        // lease window. The lease never changes, so nothing is re-published or acknowledged — the
        // redelivery waits to the persisted expiry and executes.
        var store = new InMemoryFlowStateStore();
        var flow = new ParkedFlow();
        flow.Release.SetResult();
        var transport = new RecordingDelayedTransport();
        await using var contender = new Deployment(store, flow, ContenderLease, transport);
        await store.TryCreateAsync("own-job-dead-holder", RunningState("own-job-dead-holder"), TimeSpan.FromMinutes(5));

        var job = ExecuteJob("own-job-dead-holder", "job-crashed");
        Assert.True(await store.TryAcquireLeaseAsync(
            "own-job-dead-holder",
            FlowLeaseContention.NewLeaseId(FlowLeaseContention.JobTag(job.JobId)),
            TimeSpan.FromMilliseconds(400)));

        await contender.DeliverAsync(Wire(job)).WaitAsync(TimeSpan.FromSeconds(30));

        Assert.Empty(transport.Delayed);
        Assert.Equal(1, flow.Executions);
        Assert.Equal(FlowRunStatus.Succeeded, (await store.LoadAsync("own-job-dead-holder"))!.Status);
    }

    [Fact]
    public async Task WaitingOutADeadHoldersLease_RereadsTheLedgerOccasionally_NotOnEveryPoll()
    {
        // A crashed holder's lease is waited out to its persisted expiry, polling every 2 s. Each
        // poll used to load the WHOLE ledger to read its status — about thirty loads for this
        // one-minute lease, seventy with the default window, per parked wake-up of every run the
        // dead holder had. The ledger is now re-read when the lease changes and otherwise every
        // ~30 s, so a run turned terminal by a lease-less writer is still noticed.
        var time = new VirtualTimeProvider();
        var inner = new InMemoryFlowStateStore(time);
        var store = new LoadCountingStore(inner);
        var flow = new ParkedFlow();
        flow.Release.SetResult();
        await using var contender = new Deployment(store, flow, new DurableFlowOptions(), new RecordingTransport(), time);
        await inner.TryCreateAsync("dead-holder-loads", RunningState("dead-holder-loads"), TimeSpan.FromDays(1));
        Assert.True(await inner.TryAcquireLeaseAsync("dead-holder-loads", FlowLeaseContention.NewLeaseId(null), TimeSpan.FromMinutes(1)));

        var delivery = contender.DeliverAsync(Wire(ExecuteJob("dead-holder-loads", "job-after-crash")));
        for (var step = 0; step < 600 && !delivery.IsCompleted; step++)
        {
            await WaitForArmedTimerOrCompletionAsync(time, delivery);
            if (!delivery.IsCompleted)
                time.Advance(TimeSpan.FromSeconds(1));
        }

        await delivery.WaitAsync(TimeSpan.FromSeconds(30));
        Assert.Equal(1, flow.Executions);
        Assert.Equal(FlowRunStatus.Succeeded, (await inner.LoadAsync("dead-holder-loads"))!.Status);
        // The minute's wait, plus the execution's own load and checkpoint diagnosis headroom.
        Assert.InRange(store.Loads, 2, 8);
    }

    // ---------------------------------------------------------------------------------------
    // The plumbing the rule stands on: mint, wire, ambient scope, lease tag.
    // ---------------------------------------------------------------------------------------

    [Fact]
    public async Task EveryEnqueue_MintsItsOwnJobId_ImmediateAndDelayedAlike()
    {
        var transport = new RecordingDelayedTransport();
        var builder = new AsyncResponseBuilder(
            Mock.Of<IAsyncResponseSubscriber>(),
            transport,
            propagation: new AsyncResponseContextPropagation([]));

        await builder.EnqueueWorkerAsync<IDurableFlowExecutor>(executor => executor.ExecuteAsync("flow"));
        await builder.EnqueueWorkerAsync<IDurableFlowExecutor>(executor => executor.ExecuteAsync("flow"));
        await builder.EnqueueWorkerAsync<IDurableFlowExecutor>(executor => executor.ExecuteAsync("flow"), TimeSpan.FromMinutes(5));

        var ids = transport.Immediate.Select(wire => Read(wire).JobId)
            .Concat(transport.Delayed.Select(entry => Read(entry.WireJob).JobId))
            .ToArray();
        Assert.Equal(3, ids.Length);
        Assert.All(ids, id => Assert.Matches("^[0-9a-f]{32}$", id!));
        // Two enqueues of the very same call are two jobs.
        Assert.Equal(3, ids.Distinct(StringComparer.Ordinal).Count());
    }

    [Fact]
    public async Task TheDueTimeRedelayHop_RepublishesTheSameJob()
    {
        var transport = new RecordingDelayedTransport();
        await using var deployment = new Deployment(new InMemoryFlowStateStore(), new ParkedFlow(), ContenderLease, transport);

        // Delivered an hour early (a capped hop, broker imprecision): re-published, not executed.
        var early = ExecuteJob("redelay", "job-redelay");
        early.NotBeforeUtc = DateTime.UtcNow.AddHours(1);
        await deployment.DeliverAsync(Wire(early));

        var (hopWire, _) = Assert.Single(transport.Delayed);
        Assert.Equal("job-redelay", Read(hopWire).JobId);
    }

    [Fact]
    public async Task TheExecutionLease_RecordsTheJobThatDrivesIt_ThroughTheRealWorkerPath()
    {
        var store = new InMemoryFlowStateStore();
        var flow = new ParkedFlow();
        await using var deployment = new Deployment(store, flow, HolderLease, new RecordingTransport());
        await store.TryCreateAsync("lease-tag", RunningState("lease-tag"), TimeSpan.FromMinutes(5));

        var wireJob = await deployment.EnqueueExecuteAsync("lease-tag");
        var delivery = deployment.DeliverAsync(wireJob);
        await flow.Entered.Task.WaitAsync(TimeSpan.FromSeconds(15));

        var held = await store.ObserveLeaseAsync("lease-tag");
        Assert.Equal(FlowLeaseContention.JobTag(Read(wireJob).JobId), FlowLeaseContention.JobTagOf(held!.LeaseId));
        // Every relational store keeps the lease id in a 64-character column.
        Assert.Equal(55, held.LeaseId!.Length);

        flow.Release.SetResult();
        await delivery.WaitAsync(TimeSpan.FromSeconds(15));
    }

    [Fact]
    public async Task AnExecutionNotDrivenByAWorkerJob_KeepsThePlainLeaseId()
    {
        var store = new InMemoryFlowStateStore();
        var flow = new ParkedFlow();
        await using var deployment = new Deployment(store, flow, HolderLease, new RecordingTransport());
        await store.TryCreateAsync("lease-untagged", RunningState("lease-untagged"), TimeSpan.FromMinutes(5));

        var direct = deployment.FlowExecutor.ExecuteAsync("lease-untagged");
        await flow.Entered.Task.WaitAsync(TimeSpan.FromSeconds(15));

        var held = await store.ObserveLeaseAsync("lease-untagged");
        Assert.Matches("^[0-9a-f]{32}$", held!.LeaseId!);
        Assert.Null(FlowLeaseContention.JobTagOf(held.LeaseId));

        flow.Release.SetResult();
        await direct.WaitAsync(TimeSpan.FromSeconds(15));
    }

    [Fact]
    public async Task TheAmbientJob_IsTheOneBeingExecuted_NeverTheOneWhoseContextItInherited()
    {
        // The in-memory transport runs a job under its ENQUEUER's captured execution context, so
        // a follow-up job starts out with the ambient job of the handler that published it. The
        // worker-job executor has to replace that for every job — including one without an id,
        // which must read as "no identity", not as its publisher's.
        var probe = new WorkerJobScopeProbe();
        await using var provider = new ServiceCollection().AddSingleton<IWorkerJobScopeProbe>(probe).BuildServiceProvider();
        var worker = new WorkerJobExecutor(provider.GetRequiredService<IServiceScopeFactory>(), NullLogger<WorkerJobExecutor>.Instance);
        var publisher = ExecuteJob("publisher", "job-publisher");
        var followUp = new WorkerJobEnvelope { Call = CallbackExpressionConverter.ToReflectionCall<IWorkerJobScopeProbe>(target => target.RunAsync()) };

        Assert.Null(WorkerJobScope.Current);
        using (WorkerJobScope.Enter(publisher))
        {
            await worker.ExecuteAsync(followUp);

            Assert.Same(followUp, probe.Seen);
            Assert.Null(probe.Seen!.JobId);
            // ...and the executing job's scope ended with it.
            Assert.Same(publisher, WorkerJobScope.Current);
        }

        Assert.Null(WorkerJobScope.Current);
    }

    // ---------------------------------------------------------------------------------------
    // Harness.
    // ---------------------------------------------------------------------------------------

    private static FlowState RunningState(string flowId, Type? flowType = null) => new()
    {
        FlowId = flowId,
        FlowTypeName = (flowType ?? typeof(ParkedFlow)).FullName,
        InputTypeName = typeof(TestFlowInput).FullName,
        InputJson = JsonSerializer.Serialize(new TestFlowInput(1)),
        Status = FlowRunStatus.Running,
        CreatedAtUtc = DateTime.UtcNow,
        UpdatedAtUtc = DateTime.UtcNow
    };

    private static WorkerJobEnvelope ExecuteJob(string flowId, string? jobId) => new()
    {
        Call = CallbackExpressionConverter.ToReflectionCall<IDurableFlowExecutor>(executor => executor.ExecuteAsync(flowId)),
        JobId = jobId
    };

    /// <summary>The envelope as a transport puts it on the wire.</summary>
    private static string Wire(WorkerJobEnvelope job) => AsyncResponseJson.Serialize(job);

    /// <summary>The envelope as the ingress reads it off the wire: a NEW instance per delivery.</summary>
    private static WorkerJobEnvelope Read(string wireJob) => JsonSafety.SafeDeserialize<WorkerJobEnvelope>(wireJob)!;

    private static async Task WaitForArmedTimerOrCompletionAsync(VirtualTimeProvider time, Task delivery)
    {
        var deadline = Stopwatch.StartNew();
        while (time.NextTimerDueAt is null && !delivery.IsCompleted)
        {
            if (deadline.Elapsed > TimeSpan.FromSeconds(10))
                throw new TimeoutException("The contention poll armed no delay on the virtual clock.");
            await Task.Delay(1);
        }
    }

    /// <summary>
    /// One process: a flow executor behind the real worker-job executor, sharing the store, the
    /// flow instance and the broker with the other deployments of the test.
    /// </summary>
    private sealed class Deployment : IAsyncDisposable
    {
        private readonly ServiceProvider _provider;
        private readonly IWorkerTransport _transport;
        private readonly AsyncResponseBuilder _builder;

        public Deployment(
            IFlowStateStore store,
            ParkedFlow flow,
            DurableFlowOptions options,
            IWorkerTransport transport,
            TimeProvider? clock = null,
            object[]? extraFlows = null,
            Microsoft.Extensions.Hosting.IHostApplicationLifetime? hostLifetime = null)
        {
            _transport = transport;
            _builder = new AsyncResponseBuilder(
                Mock.Of<IAsyncResponseSubscriber>(),
                transport,
                propagation: new AsyncResponseContextPropagation([]));

            var services = new ServiceCollection();
            services.AddSingleton(store);
            services.AddSingleton(flow);
            foreach (var extra in extraFlows ?? [])
                services.AddSingleton(extra.GetType(), extra);
            services.AddSingleton<IDurableFlowExecutor>(provider => new DurableFlowExecutor(
                provider.GetRequiredService<IServiceScopeFactory>(),
                _builder,
                Mock.Of<IAsyncResponseSubscriber>(),
                recoverableSubscriber: null,
                new AsyncResponseContextPropagation([]),
                options,
                Log,
                hostLifetime: hostLifetime,
                timeProvider: clock,
                workerTransport: transport));
            _provider = services.BuildServiceProvider();
        }

        public CapturingLogger<DurableFlowExecutor> Log { get; } = new();

        public DurableFlowExecutor FlowExecutor => (DurableFlowExecutor)_provider.GetRequiredService<IDurableFlowExecutor>();

        /// <summary>Publishes an <c>ExecuteAsync</c> wake-up through the real builder and returns its wire form.</summary>
        public async Task<string> EnqueueExecuteAsync(string flowId)
        {
            var recorded = _transport switch
            {
                RecordingTransport plain => plain.Immediate,
                RecordingDelayedTransport delayed => delayed.Immediate,
                _ => throw new InvalidOperationException("Unknown test transport.")
            };
            var before = recorded.Count;
            await _builder.EnqueueWorkerAsync<IDurableFlowExecutor>(executor => executor.ExecuteAsync(flowId));
            Assert.Equal(before + 1, recorded.Count);
            return recorded.Last();
        }

        /// <summary>
        /// Hands <paramref name="wireJob"/> to the worker-job executor as a fresh delivery.
        /// <paramref name="at"/> pins the due-time guard's clock (a hop delivered when due); the
        /// flow executor keeps its own.
        /// </summary>
        public Task DeliverAsync(string wireJob, DateTime? at = null)
            => new WorkerJobExecutor(
                    _provider.GetRequiredService<IServiceScopeFactory>(),
                    NullLogger<WorkerJobExecutor>.Instance,
                    _transport,
                    at is { } instant ? new VirtualTimeProvider(new DateTimeOffset(instant, TimeSpan.Zero)) : null)
                .ExecuteAsync(Read(wireJob));

        public ValueTask DisposeAsync() => _provider.DisposeAsync();
    }

    public sealed class ParkedFlow : IDurableFlow<TestFlowInput>
    {
        private int _executions;

        public int Executions => Volatile.Read(ref _executions);

        public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public async Task ExecuteAsync(IDurableFlowContext context, TestFlowInput input)
        {
            Interlocked.Increment(ref _executions);
            Entered.TrySetResult();
            await Release.Task;
        }
    }

    /// <summary>Parks on a child, then — on its FIRST execution only — lingers in a finally block until released.</summary>
    public sealed class ParkThenLingerFlow : IDurableFlow<TestFlowInput>
    {
        private int _executions;

        public int Executions => Volatile.Read(ref _executions);

        public TaskCompletionSource Lingering { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource Linger { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public async Task ExecuteAsync(IDurableFlowContext context, TestFlowInput input)
        {
            var execution = Interlocked.Increment(ref _executions);
            try
            {
                await context.AwaitChildFlowAsync<ChildNoopFlow, TestFlowInput>("child", input);
            }
            finally
            {
                if (execution == 1)
                {
                    Lingering.TrySetResult();
                    await Linger.Task;
                }
            }
        }
    }

    public sealed class ChildNoopFlow : IDurableFlow<TestFlowInput>
    {
        public Task ExecuteAsync(IDurableFlowContext context, TestFlowInput input) => Task.CompletedTask;
    }

    private sealed class WorkerJobScopeProbe : IWorkerJobScopeProbe
    {
        public WorkerJobEnvelope? Seen { get; private set; }

        public Task RunAsync()
        {
            Seen = WorkerJobScope.Current;
            return Task.CompletedTask;
        }
    }

    /// <summary>A broker without delayed delivery (Pub/Sub, RabbitMQ, Kafka): it keeps what was published, as published.</summary>
    private sealed class RecordingTransport : IWorkerTransport
    {
        public ConcurrentQueue<string> Immediate { get; } = new();

        public Task PublishAsync(WorkerJobEnvelope job, CancellationToken cancellationToken = default)
        {
            Immediate.Enqueue(Wire(job));
            return Task.CompletedTask;
        }
    }

    /// <summary>A broker with native delayed delivery (SQS, Service Bus, the database queues), with publish-failure injection.</summary>
    private sealed class RecordingDelayedTransport : IDelayedWorkerTransport
    {
        public ConcurrentQueue<string> Immediate { get; } = new();

        public ConcurrentQueue<(string WireJob, TimeSpan Delay)> Delayed { get; } = new();

        public Exception? FailDelayedPublishWith { get; set; }

        public TimeSpan MaxPublishDelay => TimeSpan.FromMinutes(15);

        public Task PublishAsync(WorkerJobEnvelope job, CancellationToken cancellationToken = default)
        {
            Immediate.Enqueue(Wire(job));
            return Task.CompletedTask;
        }

        public Task PublishAsync(WorkerJobEnvelope job, TimeSpan delay, CancellationToken cancellationToken = default)
        {
            if (FailDelayedPublishWith is { } failure)
                return Task.FromException(failure);

            // What the real brokers enforce.
            if (delay <= TimeSpan.Zero || delay > MaxPublishDelay)
                return Task.FromException(new ArgumentOutOfRangeException(nameof(delay), delay, "Outside what the broker can delay."));

            Delayed.Enqueue((Wire(job), delay));
            return Task.CompletedTask;
        }
    }

    /// <summary>The in-memory store, counting ledger loads.</summary>
    private sealed class LoadCountingStore(InMemoryFlowStateStore inner) : IFlowStateStore
    {
        private int _loads;

        public int Loads => Volatile.Read(ref _loads);

        public Task<FlowState?> LoadAsync(string flowId, CancellationToken cancellationToken = default)
        {
            Interlocked.Increment(ref _loads);
            return inner.LoadAsync(flowId, cancellationToken);
        }

        public Task<FlowLeaseObservation?> ObserveLeaseAsync(string flowId, CancellationToken cancellationToken = default)
            => inner.ObserveLeaseAsync(flowId, cancellationToken);

        public Task<bool> TryAcquireLeaseAsync(string flowId, string leaseId, TimeSpan leaseDuration, CancellationToken cancellationToken = default)
            => inner.TryAcquireLeaseAsync(flowId, leaseId, leaseDuration, cancellationToken);

        public Task<bool> TryCreateAsync(string flowId, FlowState state, TimeSpan ttl, CancellationToken cancellationToken = default)
            => inner.TryCreateAsync(flowId, state, ttl, cancellationToken);

        public Task<bool> TryUpdateAsync(string flowId, FlowState state, long expectedRevision, TimeSpan ttl, string? leaseId = null, CancellationToken cancellationToken = default)
            => inner.TryUpdateAsync(flowId, state, expectedRevision, ttl, leaseId, cancellationToken);

        public Task<bool> TryRenewLeaseAsync(string flowId, string leaseId, TimeSpan leaseDuration, CancellationToken cancellationToken = default)
            => inner.TryRenewLeaseAsync(flowId, leaseId, leaseDuration, cancellationToken);

        public Task ReleaseLeaseAsync(string flowId, string leaseId, CancellationToken cancellationToken = default)
            => inner.ReleaseLeaseAsync(flowId, leaseId, cancellationToken);

        public Task<bool> TryDeleteAsync(string flowId, CancellationToken cancellationToken = default)
            => inner.TryDeleteAsync(flowId, cancellationToken);
    }

    /// <summary>A Running ledger whose lease this delivery can never win, held by a holder that renews on every look.</summary>
    private sealed class RenewingHolderStore(InMemoryFlowStateStore inner, Func<FlowLeaseObservation> observe) : IFlowStateStore
    {
        public static async Task<RenewingHolderStore> CreateAsync(string flowId, Func<FlowLeaseObservation> observe)
        {
            var inner = new InMemoryFlowStateStore();
            await inner.TryCreateAsync(flowId, RunningState(flowId), TimeSpan.FromMinutes(5));
            return new RenewingHolderStore(inner, observe);
        }

        public Task<FlowLeaseObservation?> ObserveLeaseAsync(string flowId, CancellationToken cancellationToken = default)
            => Task.FromResult<FlowLeaseObservation?>(observe());

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
