using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using System.Threading.Channels;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Serialized against the rest of the suite: the drain-race regression below drives a scheduling
/// race in a tight loop, and its thread-pool burst starves the virtual-clock harness tests'
/// real-time settle budgets when run alongside them.
/// </summary>
[CollectionDefinition(nameof(InMemoryWorkerTransportDrainTests), DisableParallelization = true)]
public sealed class InMemoryWorkerTransportDrainCollection;

/// <summary>
/// The in-process transport promises accepted jobs in-process execution, so host shutdown must
/// drain the bounded queue to completion (stop accepting, then finish what was accepted) rather
/// than abandoning queued jobs mid-queue.
/// </summary>
[Collection(nameof(InMemoryWorkerTransportDrainTests))]
public sealed class InMemoryWorkerTransportDrainTests
{
    public interface IDrainProbe
    {
        Task RunAsync();
    }

    private sealed class DrainProbe : IDrainProbe
    {
        private int _executed;

        public int Executed => Volatile.Read(ref _executed);
        public TaskCompletionSource FirstStarted { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource ReleaseFirst { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public async Task RunAsync()
        {
            if (Interlocked.Increment(ref _executed) == 1)
            {
                FirstStarted.TrySetResult();
                await ReleaseFirst.Task;
            }
        }
    }

    [Fact]
    public async Task StopAsync_DrainsEveryAcceptedJobBeforeExiting()
    {
        const int jobCount = 8;
        var probe = new DrainProbe();
        var provider = new ServiceCollection()
            .AddSingleton<IDrainProbe>(probe)
            .BuildServiceProvider();
        var transport = new InMemoryWorkerTransport();
        var executor = new WorkerJobExecutor(
            provider.GetRequiredService<IServiceScopeFactory>(),
            NullLogger<WorkerJobExecutor>.Instance);
        var host = new InMemoryWorkerHost(transport, executor, NullLogger<InMemoryWorkerHost>.Instance);

        await host.StartAsync(CancellationToken.None);

        for (var index = 0; index < jobCount; index++)
        {
            await transport.PublishAsync(new WorkerJobEnvelope
            {
                Call = new ReflectionCallDto
                {
                    ServiceInterfaceFullName = typeof(IDrainProbe).FullName!,
                    MethodName = nameof(IDrainProbe.RunAsync),
                    Params = []
                },
                CorrelationId = $"drain-{index}"
            });
        }

        // Stop while the first job is still executing and the rest sit in the queue: the old
        // cancellation-driven read dropped every queued job here.
        await probe.FirstStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));
        var stop = host.StopAsync(CancellationToken.None);
        probe.ReleaseFirst.TrySetResult();
        await stop.WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Equal(jobCount, probe.Executed);
        await provider.DisposeAsync();
    }

    public interface IOverflowProbe
    {
        Task RunAsync(int id);
    }

    private sealed class OverflowProbe : IOverflowProbe
    {
        private readonly List<int> _executed = [];

        public InMemoryWorkerTransport? Transport { get; set; }
        public TaskCompletionSource FirstStarted { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource ReleaseFirst { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource FollowUpRan { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public IReadOnlyList<int> Executed
        {
            get { lock (_executed) return _executed.ToArray(); }
        }

        public async Task RunAsync(int id)
        {
            lock (_executed)
            {
                _executed.Add(id);
            }

            if (id == 3)
            {
                FollowUpRan.TrySetResult();
                return;
            }

            if (id != 1)
                return;

            FirstStarted.TrySetResult();
            await ReleaseFirst.Task;

            // The in-job publish: a durable-flow child start, or a child waking its parent. The
            // queue is FULL at this moment, and this worker is the only consumer.
            await Transport!.PublishAsync(Job(3));
        }

        public static WorkerJobEnvelope Job(int id) => new()
        {
            Call = new ReflectionCallDto
            {
                ServiceInterfaceFullName = typeof(IOverflowProbe).FullName!,
                MethodName = nameof(IOverflowProbe.RunAsync),
                Params = [CallbackParam.ForValue(id)]
            },
            CorrelationId = $"overflow-{id}"
        };
    }

    [Fact]
    public async Task PublishFromInsideAJob_AgainstAFullQueue_DoesNotWedgeTheTransport()
    {
        // Regression (round 29): a publish from inside a running job used the same capacity-waiting
        // WriteAsync as an external producer. The workers are the only consumers, so a worker that
        // parks against a full queue is waiting on itself: with the default WorkerCount = 1 one
        // in-job publish past capacity wedged the transport permanently — no job ever completed,
        // _outstanding never reached zero, the shutdown drain never completed the writer, and the
        // whole backlog was lost at process exit. Durable flows publish from inside jobs routinely.
        var probe = new OverflowProbe();
        var provider = new ServiceCollection()
            .AddSingleton<IOverflowProbe>(probe)
            .BuildServiceProvider();
        var transport = new InMemoryWorkerTransport(
            Options.Create(new InMemoryWorkerTransportOptions { QueueCapacity = 1, WorkerCount = 1 }));
        probe.Transport = transport;
        var executor = new WorkerJobExecutor(
            provider.GetRequiredService<IServiceScopeFactory>(),
            NullLogger<WorkerJobExecutor>.Instance);
        var host = new InMemoryWorkerHost(transport, executor, NullLogger<InMemoryWorkerHost>.Instance);

        await host.StartAsync(CancellationToken.None);

        await transport.PublishAsync(OverflowProbe.Job(1));
        await probe.FirstStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));

        // Fill the single queue slot behind the running job, so the in-job publish has nowhere to go.
        await transport.PublishAsync(OverflowProbe.Job(2));

        probe.ReleaseFirst.TrySetResult();

        // Old behavior: this never completes — the only worker is blocked publishing to itself.
        await probe.FollowUpRan.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await host.StopAsync(CancellationToken.None).WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Equal([1, 2, 3], probe.Executed);
        await provider.DisposeAsync();
    }

    public interface IChainProbe
    {
        Task RunAsync();
    }

    private sealed class ChainProbe : IChainProbe
    {
        private int _executed;

        public InMemoryWorkerTransport? Transport { get; set; }
        public int Executed => Volatile.Read(ref _executed);
        public TaskCompletionSource FirstStarted { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource ReleaseFirst { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public async Task RunAsync()
        {
            if (Interlocked.Increment(ref _executed) > 1)
                return;

            FirstStarted.TrySetResult();
            await ReleaseFirst.Task;

            // Published while the host is already stopping: exactly what a durable-flow child does
            // when it wakes its parent, or a recovery path does when it re-enqueues a run.
            await Transport!.PublishAsync(new WorkerJobEnvelope
            {
                Call = new ReflectionCallDto
                {
                    ServiceInterfaceFullName = typeof(IChainProbe).FullName!,
                    MethodName = nameof(IChainProbe.RunAsync),
                    Params = []
                },
                CorrelationId = "chain-follow-up"
            });
        }
    }

    [Fact]
    public async Task StopAsync_JobEnqueuedDuringDrain_StillExecutes()
    {
        var probe = new ChainProbe();
        var provider = new ServiceCollection()
            .AddSingleton<IChainProbe>(probe)
            .BuildServiceProvider();
        var transport = new InMemoryWorkerTransport();
        probe.Transport = transport;
        var executor = new WorkerJobExecutor(
            provider.GetRequiredService<IServiceScopeFactory>(),
            NullLogger<WorkerJobExecutor>.Instance);
        var host = new InMemoryWorkerHost(transport, executor, NullLogger<InMemoryWorkerHost>.Instance);

        await host.StartAsync(CancellationToken.None);

        await transport.PublishAsync(new WorkerJobEnvelope
        {
            Call = new ReflectionCallDto
            {
                ServiceInterfaceFullName = typeof(IChainProbe).FullName!,
                MethodName = nameof(IChainProbe.RunAsync),
                Params = []
            },
            CorrelationId = "chain-first"
        });

        // Begin stopping while the first job runs, then let it publish its follow-up mid-drain.
        // Completing the writer at stop-begin made this publish throw ChannelClosedException and
        // silently lose the follow-up (a stuck parent flow, in durable-flow terms).
        await probe.FirstStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));
        var stop = host.StopAsync(CancellationToken.None);
        probe.ReleaseFirst.TrySetResult();
        await stop.WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Equal(2, probe.Executed);
        await provider.DisposeAsync();
    }

    [Fact]
    public async Task BeginShutdownDrain_RacingDelayedPublishes_RetainsEveryWakeUp()
    {
        // The drain snapshots pending delayed jobs and appends them to the retention list while a
        // flow suspending mid-drain appends its own wake-up to the SAME list from PublishAsync.
        // Both appends must run under _delayedGate: two unsynchronized List<T>.Add calls can lose
        // an entry (both writing one slot) or throw mid-grow, so SimulateRestartAsync would drop
        // a wake-up it promised to retain. The race is probabilistic by nature — the loop gives
        // the interleaving many chances; on correctly locked code the count is deterministic.
        const int iterations = 60;
        const int pendingJobs = 48;
        const int racingJobs = 48;
        for (var i = 0; i < iterations; i++)
        {
            var transport = new InMemoryWorkerTransport();
            for (var j = 0; j < pendingJobs; j++)
                await transport.PublishAsync(DelayedJob(), TimeSpan.FromHours(1));

            var retention = transport.BeginRetainingDelayedJobs();
            using var start = new ManualResetEventSlim();
            var publisher = Task.Run(() =>
            {
                start.Wait(TimeSpan.FromSeconds(30));
                for (var j = 0; j < racingJobs; j++)
                    _ = transport.PublishAsync(DelayedJob(), TimeSpan.FromHours(1));
            });
            var drainer = Task.Run(() =>
            {
                start.Wait(TimeSpan.FromSeconds(30));
                transport.BeginShutdownDrain();
            });

            start.Set();
            await Task.WhenAll(publisher, drainer).WaitAsync(TimeSpan.FromSeconds(30));

            // Every pending job and every mid-drain wake-up retained, none lost to the race.
            Assert.Equal(pendingJobs + racingJobs, retention.Count);
        }
    }

    [Fact]
    public async Task StartThenImmediateStop_UnderThreadPoolPressure_StillDrainsAndRetains()
    {
        // Regression (Hosting.Abstractions 10.0.10): BackgroundService.StartAsync stopped invoking
        // ExecuteAsync inline — it queues it to the thread pool and DISCARDS the queued work when
        // the stopping token fires before the pool runs it. The shutdown-drain hook was installed
        // inside ExecuteAsync, so a fast start→stop under pool pressure never installed it:
        // pending delayed jobs were neither retained nor dropped loudly (SimulateRestartAsync
        // intermittently lost retained wake-ups exactly this way — the job's leaked timer later
        // fired into a worker-less channel), and accepted queued jobs sat unread forever. The
        // host now owns its lifecycle and brings the loops and the drain hook up synchronously
        // inside StartAsync, so the drain below is deterministic however starved the pool is.
        var provider = new ServiceCollection().BuildServiceProvider();
        var transport = new InMemoryWorkerTransport();
        var executor = new WorkerJobExecutor(
            provider.GetRequiredService<IServiceScopeFactory>(),
            NullLogger<WorkerJobExecutor>.Instance);
        var host = new InMemoryWorkerHost(transport, executor, NullLogger<InMemoryWorkerHost>.Instance);

        await transport.PublishAsync(DelayedJob(), TimeSpan.FromHours(2));
        var retention = transport.BeginRetainingDelayedJobs();

        // Saturate the thread pool so anything the host QUEUES (instead of running inline) cannot
        // execute during the start→stop window: once PendingWorkItemCount is positive, every pool
        // thread is occupied and later work items wait behind the blockers.
        using var gate = new ManualResetEventSlim(initialState: false);
        var blockers = Enumerable.Range(0, Environment.ProcessorCount * 2)
            .Select(_ => Task.Run(() => gate.Wait()))
            .ToArray();
        try
        {
            var saturated = DateTime.UtcNow.AddSeconds(5);
            while (ThreadPool.PendingWorkItemCount == 0 && DateTime.UtcNow < saturated)
                Thread.Yield();
            Assert.True(ThreadPool.PendingWorkItemCount > 0, "could not saturate the thread pool");

            await host.StartAsync(CancellationToken.None);
            var stopping = host.StopAsync(CancellationToken.None);

            // The drain runs synchronously inside StopAsync's cancel, before its first await —
            // the retained job must already be there, however starved the pool is. On the old
            // BackgroundService-based host the deferred ExecuteAsync was discarded un-run, no
            // drain ever happened, and this list stayed empty.
            var retained = Assert.Single(retention);
            Assert.Equal(typeof(IDrainProbe).FullName, retained.Call.ServiceInterfaceFullName);

            gate.Set();
            await stopping.WaitAsync(TimeSpan.FromSeconds(10));
        }
        finally
        {
            gate.Set();
            await Task.WhenAll(blockers);
            await provider.DisposeAsync();
        }
    }

    [Fact]
    public async Task PublishFailureDuringDrain_NeverStrandsTheDrainWithAnUncompletedWriter()
    {
        // Regression (r23): PublishAsync's catch decremented _outstanding without the drain-aware
        // "complete the writer at zero" step OnJobFinished performs. When a cancelled publisher's
        // decrement was the LAST one after the drain began, the channel stayed open-but-empty and
        // the worker loop parked forever — shutdown stalled to the host budget. The interleaving
        // is timing-dependent, so drive it repeatedly; every iteration must end completed.
        for (var i = 0; i < 200; i++)
        {
            var transport = new InMemoryWorkerTransport(Options.Create(new InMemoryWorkerTransportOptions { QueueCapacity = 1 }));
            await transport.PublishAsync(DelayedJob());
            using var cts = new CancellationTokenSource();
            var parked = transport.PublishAsync(DelayedJob(), cts.Token);
            transport.BeginShutdownDrain();

            // Race the cancellation of the parked writer against a worker draining the queued job.
            var cancel = Task.Run(cts.Cancel);
            var consume = Task.Run(async () =>
            {
                try
                {
                    using var readCts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
                    await transport.Reader.ReadAsync(readCts.Token);
                    transport.OnJobFinished();
                }
                catch (ChannelClosedException)
                {
                    // The writer completed with nothing left to read: the race already settled.
                }
            });

            var parkedWon = false;
            try
            {
                await parked;
                parkedWon = true;
            }
            catch (OperationCanceledException)
            {
            }

            await cancel;
            await consume;
            if (parkedWon)
            {
                // The parked write slid into the freed slot: finish it like a worker would.
                Assert.True(transport.Reader.TryRead(out _));
                transport.OnJobFinished();
            }

            await transport.Reader.Completion.WaitAsync(TimeSpan.FromSeconds(2));
        }
    }

    [Fact]
    public async Task DelayedFire_RacingShutdownDrain_IsNeverLostBetweenSnapshotAndCount()
    {
        // Regression (r23): FireDelayed removed the job from the timer map inside the gate but
        // incremented _outstanding only after releasing it. A drain running in that window saw
        // neither the map entry nor the count — it completed the writer underneath the fired
        // job, which was then dropped. The count now moves inside the gate, atomic with the
        // removal, so the drain either retains the job or waits for its write.
        var time = new CapturedTimerTimeProvider();
        var transport = new InMemoryWorkerTransport(Options.Create(new InMemoryWorkerTransportOptions()), time);

        await transport.PublishAsync(DelayedJob(), TimeSpan.FromMinutes(5));
        Assert.NotNull(time.Callback);

        // Fire the timer on a background thread: its timer.Dispose parks on the gate, freezing
        // the fire between "removed from the map" and "written to the queue" — exactly the
        // window a shutdown drain races.
        var firing = Task.Run(() => time.Callback!(time.State));
        Assert.True(time.DisposeReached.Wait(TimeSpan.FromSeconds(5)));

        transport.BeginShutdownDrain();
        time.DisposeGate.Set();
        await firing;

        // The fired wake-up must survive: on the old code the drain observed zero outstanding,
        // completed the writer, and the write landed in a closed channel and was dropped.
        using var readCts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var fired = await transport.Reader.ReadAsync(readCts.Token);
        Assert.Equal(typeof(IDrainProbe).FullName, fired.Job.Call.ServiceInterfaceFullName);
        transport.OnJobFinished();
    }

    private sealed class CapturedTimerTimeProvider : TimeProvider
    {
        public TimerCallback? Callback;
        public object? State;
        public ManualResetEventSlim DisposeGate { get; } = new(initialState: false);
        public ManualResetEventSlim DisposeReached { get; } = new(initialState: false);

        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
        {
            Callback = callback;
            State = state;
            return new GateTimer(this);
        }

        private sealed class GateTimer(CapturedTimerTimeProvider owner) : ITimer
        {
            public bool Change(TimeSpan dueTime, TimeSpan period) => true;

            public void Dispose()
            {
                owner.DisposeReached.Set();
                owner.DisposeGate.Wait();
            }

            public ValueTask DisposeAsync()
            {
                Dispose();
                return ValueTask.CompletedTask;
            }
        }
    }

    private static WorkerJobEnvelope DelayedJob()
        => new()
        {
            Call = new ReflectionCallDto
            {
                ServiceInterfaceFullName = typeof(IDrainProbe).FullName!,
                MethodName = nameof(IDrainProbe.RunAsync),
                Params = []
            }
        };
}
