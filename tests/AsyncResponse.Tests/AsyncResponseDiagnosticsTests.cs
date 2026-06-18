using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using System.Diagnostics;
using System.Text.Json;
using Xunit;

namespace AsyncResponse.Tests;

public class AsyncResponseDiagnosticsTests
{
    [Fact]
    public async Task InMemoryWaitAndPublish_EmitActivitiesFromPublicSource()
    {
        using var collector = new ActivityCollector();
        await using var provider = CreateProvider();
        var asyncResponse = provider.GetRequiredService<IAsyncResponseBuilder>();
        var publisher = provider.GetRequiredService<IAsyncResponsePublisher>();

        string? correlationId = null;
        var result = await asyncResponse
            .For<OperationResult>()
            .WithTimeout(TimeSpan.FromSeconds(5))
            .WaitAsync(context =>
            {
                correlationId = context.CorrelationId;
                return publisher.SetResponse(
                    new OperationResult { Status = OperationStatus.Completed, Message = "done" },
                    context.CorrelationId);
            });

        Assert.Equal("done", result.Message);

        var wait = collector.Single("asyncresponse.wait", "asyncresponse.correlation_id", correlationId);
        Assert.Equal(correlationId, Tag(wait, "asyncresponse.correlation_id"));
        Assert.Equal("inmemory", Tag(wait, "asyncresponse.channel"));
        Assert.Equal(typeof(OperationResult).FullName, Tag(wait, "asyncresponse.payload_type"));

        var publish = collector.Single("asyncresponse.set_response", "asyncresponse.correlation_id", correlationId);
        Assert.Equal(ActivityKind.Producer, publish.Kind);
        Assert.Equal("inmemory", Tag(publish, "asyncresponse.channel"));
        Assert.Equal(1, Convert.ToInt32(Tag(publish, "asyncresponse.subscribers")));
    }

    [Fact]
    public async Task ResponseIngress_EmitsIngressPublishAndLostSubscriberActivities()
    {
        using var collector = new ActivityCollector();
        await using var provider = CreateProvider();
        var ingress = provider.GetRequiredService<IAsyncResponseIngress>();

        await ingress.HandleResponseMessageAsync(
            JsonSerializer.Serialize(new OperationResult { Status = OperationStatus.Completed }),
            "corr-ingress");

        var ingressActivity = collector.Single("asyncresponse.ingress.response", "asyncresponse.correlation_id", "corr-ingress");
        Assert.Equal(ActivityKind.Consumer, ingressActivity.Kind);
        Assert.Equal("corr-ingress", Tag(ingressActivity, "asyncresponse.correlation_id"));

        var publish = collector.Single("asyncresponse.ingress.raw_response", "asyncresponse.correlation_id", "corr-ingress");
        Assert.Equal("corr-ingress", Tag(publish, "asyncresponse.correlation_id"));
        Assert.Equal("inmemory", Tag(publish, "asyncresponse.channel"));
        Assert.Equal(0, Convert.ToInt32(Tag(publish, "asyncresponse.subscribers")));

        var dispatch = collector.Single("asyncresponse.lost_subscriber.dispatch", "asyncresponse.channel_name", "inmemory:response:corr-ingress");
        Assert.Equal("response", Tag(dispatch, "asyncresponse.lost_subscriber.kind"));
        Assert.Equal("unclassified", Tag(dispatch, "asyncresponse.lost_subscriber_route"));
        Assert.Equal(false, Tag(dispatch, "asyncresponse.recovery.callback_invoked"));
    }

    [Fact]
    public async Task WorkerEnqueuePublishAndExecute_EmitActivities()
    {
        using var collector = new ActivityCollector();
        var transport = new Mock<IWorkerTransport>();
        transport
            .Setup(t => t.PublishAsync(It.IsAny<WorkerJobEnvelope>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        AsyncResponseContext.SetCorrelationId("corr-worker");
        try
        {
            await new AsyncResponseBuilder(Mock.Of<IAsyncResponseSubscriber>(), transport.Object)
                .EnqueueWorkerAsync<IRecoverySpy>(spy => spy.OnWorkerJob(7));
        }
        finally
        {
            AsyncResponseContext.ClearCorrelationId();
        }

        var enqueue = collector.Single("asyncresponse.enqueue_worker", "asyncresponse.correlation_id", "corr-worker");
        Assert.Equal(ActivityKind.Producer, enqueue.Kind);
        Assert.Equal("corr-worker", Tag(enqueue, "asyncresponse.correlation_id"));
        Assert.Equal(nameof(IRecoverySpy.OnWorkerJob), Tag(enqueue, "asyncresponse.worker.method"));

        var inMemoryTransport = new InMemoryWorkerTransport();
        await inMemoryTransport.PublishAsync(WorkerJob("corr-queued", 8));

        var publish = collector.Single("asyncresponse.worker.publish", "asyncresponse.correlation_id", "corr-queued");
        Assert.Equal(ActivityKind.Producer, publish.Kind);
        Assert.Equal("inmemory", Tag(publish, "asyncresponse.transport"));
        Assert.Equal("corr-queued", Tag(publish, "asyncresponse.correlation_id"));

        var spy = new RecoverySpy();
        var services = new ServiceCollection();
        services.AddSingleton<IRecoverySpy>(spy);
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddSingleton<WorkerJobExecutor>();
        await using var provider = services.BuildServiceProvider();

        await provider.GetRequiredService<WorkerJobExecutor>()
            .ExecuteAsync(WorkerJob("corr-execute", 9));

        var execute = collector.Single("asyncresponse.worker.execute", "asyncresponse.correlation_id", "corr-execute");
        Assert.Equal(ActivityKind.Consumer, execute.Kind);
        Assert.Equal("corr-execute", Tag(execute, "asyncresponse.correlation_id"));
        Assert.Equal(nameof(IRecoverySpy.OnWorkerJob), Tag(execute, "asyncresponse.worker.method"));
        Assert.Equal((9, "corr-execute", null), Assert.Single(spy.WorkerJobs));
    }

    private static ServiceProvider CreateProvider()
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddAsyncResponse()
            .WithInMemoryChannel(options =>
            {
                options.DefaultTimeout = TimeSpan.FromSeconds(5);
                options.RecoveryStateExpiry = TimeSpan.FromMinutes(5);
            });
        return services.BuildServiceProvider();
    }

    private static WorkerJobEnvelope WorkerJob(string correlationId, int orderId)
        => new()
        {
            CorrelationId = correlationId,
            Call = new ReflectionCallDto
            {
                ServiceInterfaceFullName = typeof(IRecoverySpy).FullName!,
                MethodName = nameof(IRecoverySpy.OnWorkerJob),
                Params = [CallbackParam.ForValue(orderId)]
            }
        };

    private static object? Tag(Activity activity, string key)
        => activity.TagObjects.FirstOrDefault(tag => tag.Key == key).Value;

    private sealed class ActivityCollector : IDisposable
    {
        private readonly object _gate = new();
        private readonly List<Activity> _activities = [];
        private readonly ActivityListener _listener;

        public ActivityCollector()
        {
            _listener = new ActivityListener
            {
                ShouldListenTo = source => source.Name == AsyncResponseDiagnostics.ActivitySourceName,
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
                SampleUsingParentId = (ref ActivityCreationOptions<string> _) => ActivitySamplingResult.AllDataAndRecorded,
                ActivityStopped = activity =>
                {
                    lock (_gate)
                    {
                        _activities.Add(activity);
                    }
                }
            };
            ActivitySource.AddActivityListener(_listener);
        }

        public Activity Single(string name, string tagKey, object? tagValue)
        {
            lock (_gate)
            {
                return Assert.Single(
                    _activities,
                    activity => activity.OperationName == name && Equals(Tag(activity, tagKey), tagValue));
            }
        }

        public void Dispose()
            => _listener.Dispose();
    }
}
