using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
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
}
