using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// The in-process transport promises accepted jobs in-process execution, so host shutdown must
/// drain the bounded queue to completion (stop accepting, then finish what was accepted) rather
/// than abandoning queued jobs mid-queue.
/// </summary>
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
}
