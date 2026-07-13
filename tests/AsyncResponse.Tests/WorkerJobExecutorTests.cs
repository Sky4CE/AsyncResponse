using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>A worker job target that records the ambient context observed while it runs.</summary>
public interface IWorkerProbe
{
    Task RunAsync();
}

public sealed class WorkerProbe : IWorkerProbe
{
    public string? SeenCorrelationId { get; private set; }
    public AsyncResponseReplyTarget? SeenReplyTarget { get; private set; }

    public Task RunAsync()
    {
        SeenCorrelationId = AsyncResponseContext.CorrelationId;
        SeenReplyTarget = AsyncResponseContext.ReplyTarget;
        return Task.CompletedTask;
    }
}

/// <summary>
/// The shared worker-job executor restores the captured correlation id and reply target into the
/// ambient context for the duration of the job (so downstream publishes correlate), then unwinds
/// that scope so one job never leaks its context to the next.
/// </summary>
public class WorkerJobExecutorTests
{
    [Fact]
    public async Task ExecutesJob_RestoringCorrelationAndReplyTargetContext()
    {
        var probe = new WorkerProbe();
        var provider = new ServiceCollection().AddSingleton<IWorkerProbe>(probe).BuildServiceProvider();
        var executor = new WorkerJobExecutor(
            provider.GetRequiredService<IServiceScopeFactory>(),
            NullLogger<WorkerJobExecutor>.Instance);

        await executor.ExecuteAsync(new WorkerJobEnvelope
        {
            Call = new ReflectionCallDto
            {
                ServiceInterfaceFullName = typeof(IWorkerProbe).FullName!,
                MethodName = nameof(IWorkerProbe.RunAsync),
                Params = []
            },
            CorrelationId = "cid-1",
            ReplyTarget = new AsyncResponseReplyTarget { Name = "default", Transport = "test", Address = "test://reply" }
        });

        Assert.Equal("cid-1", probe.SeenCorrelationId);
        Assert.Equal("default", probe.SeenReplyTarget?.Name);

        // The restored context is scoped to the job and must not leak back to the caller.
        Assert.Null(AsyncResponseContext.CorrelationId);
        Assert.Null(AsyncResponseContext.ReplyTarget);
    }

    [Fact]
    public async Task NullJob_Throws()
    {
        var provider = new ServiceCollection().BuildServiceProvider();
        var executor = new WorkerJobExecutor(
            provider.GetRequiredService<IServiceScopeFactory>(),
            NullLogger<WorkerJobExecutor>.Instance);

        await Assert.ThrowsAsync<ArgumentNullException>(() => executor.ExecuteAsync(null!));
    }

    [Fact]
    public async Task MissingService_FaultsThroughExecutorErrorPath()
    {
        var provider = new ServiceCollection().BuildServiceProvider();
        var executor = new WorkerJobExecutor(
            provider.GetRequiredService<IServiceScopeFactory>(),
            NullLogger<WorkerJobExecutor>.Instance);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => executor.ExecuteAsync(new WorkerJobEnvelope
        {
            Call = new ReflectionCallDto
            {
                ServiceInterfaceFullName = typeof(IWorkerProbe).FullName!,
                MethodName = nameof(IWorkerProbe.RunAsync),
                Params = []
            },
            CorrelationId = "cid-missing"
        }));

        Assert.Contains(nameof(IWorkerProbe), ex.Message);
    }

    [Fact]
    public async Task InMemoryWorkerTransport_CanceledPublish_FaultsThroughProducerErrorPath()
    {
        var transport = new InMemoryWorkerTransport();
        using var canceled = new CancellationTokenSource();
        await canceled.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => transport.PublishAsync(new WorkerJobEnvelope
        {
            CorrelationId = "cid-canceled",
            Call = new ReflectionCallDto
            {
                ServiceInterfaceFullName = typeof(IWorkerProbe).FullName!,
                MethodName = nameof(IWorkerProbe.RunAsync),
                Params = []
            }
        }, canceled.Token));
    }

    [Fact]
    public async Task InMemoryWorkerTransport_BoundedQueueBackpressuresPublishers()
    {
        var transport = new InMemoryWorkerTransport(Options.Create(new InMemoryWorkerTransportOptions
        {
            QueueCapacity = 1,
            WorkerCount = 1
        }));
        var first = CreateJob("first");
        var second = CreateJob("second");

        await transport.PublishAsync(first);
        var blocked = transport.PublishAsync(second);
        await Task.Delay(30);
        Assert.False(blocked.IsCompleted);

        Assert.Equal("first", (await transport.Reader.ReadAsync()).Job.CorrelationId);
        await blocked.WaitAsync(TimeSpan.FromSeconds(2));
        Assert.Equal("second", (await transport.Reader.ReadAsync()).Job.CorrelationId);
    }

    [Theory]
    [InlineData(nameof(InMemoryWorkerTransportOptions.QueueCapacity))]
    [InlineData(nameof(InMemoryWorkerTransportOptions.WorkerCount))]
    public void InMemoryWorkerTransport_InvalidOptionsAreRejected(string propertyName)
    {
        var options = new InMemoryWorkerTransportOptions();
        if (propertyName == nameof(InMemoryWorkerTransportOptions.QueueCapacity))
            options.QueueCapacity = 0;
        else
            options.WorkerCount = 0;

        var exception = Assert.Throws<InvalidOperationException>(
            () => new InMemoryWorkerTransport(Options.Create(options)));

        Assert.Contains(propertyName, exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task InMemoryWorkerHost_ContinuesAfterFailedJobAndRunsWithoutCapturedExecutionContext()
    {
        var probe = new WorkerProbe();
        var provider = new ServiceCollection().AddSingleton<IWorkerProbe>(probe).BuildServiceProvider();
        var transport = new InMemoryWorkerTransport();
        var executor = new WorkerJobExecutor(
            provider.GetRequiredService<IServiceScopeFactory>(),
            NullLogger<WorkerJobExecutor>.Instance);
        var host = new InMemoryWorkerHost(
            transport,
            executor,
            NullLogger<InMemoryWorkerHost>.Instance);

        var flow = ExecutionContext.SuppressFlow();
        try
        {
            await transport.PublishAsync(new WorkerJobEnvelope
            {
                CorrelationId = "cid-missing-service",
                Call = new ReflectionCallDto
                {
                    ServiceInterfaceFullName = "AsyncResponse.Tests.IMissingWorker",
                    MethodName = nameof(IWorkerProbe.RunAsync),
                    Params = []
                }
            });

            await transport.PublishAsync(new WorkerJobEnvelope
            {
                CorrelationId = "cid-direct",
                Call = new ReflectionCallDto
                {
                    ServiceInterfaceFullName = typeof(IWorkerProbe).FullName!,
                    MethodName = nameof(IWorkerProbe.RunAsync),
                    Params = []
                }
            });
        }
        finally
        {
            flow.Undo();
        }

        await host.StartAsync(CancellationToken.None);
        try
        {
            await WaitUntilAsync(() => probe.SeenCorrelationId == "cid-direct");
        }
        finally
        {
            await host.StopAsync(CancellationToken.None);
        }

        Assert.Equal("cid-direct", probe.SeenCorrelationId);
    }

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(2);
        while (DateTime.UtcNow < deadline)
        {
            if (condition())
                return;

            await Task.Delay(20);
        }

        throw new TimeoutException("Condition was not met in time.");
    }

    private static WorkerJobEnvelope CreateJob(string correlationId)
        => new()
        {
            CorrelationId = correlationId,
            Call = new ReflectionCallDto
            {
                ServiceInterfaceFullName = typeof(IWorkerProbe).FullName!,
                MethodName = nameof(IWorkerProbe.RunAsync),
                Params = []
            }
        };
}
