using System.Collections.Concurrent;
using AsyncResponse.Transports.RabbitMQ;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Xunit;

namespace AsyncResponse.IntegrationTests;

/// <summary>
/// Round 1 (G10) real-broker pins for RabbitMQ behaviour the unit fakes cannot vouch for: the client's
/// own ordering on a graceful stop (S7#8) and its AMQP shortstr encoding of the native
/// correlation-id (GS5#3).
/// </summary>
[Collection(BrokersCollection.Name)]
[Trait(Batches.Trait, Batches.Brokers)]
public sealed class RabbitMqShutdownIntegrationTests(BrokersBatchFixture fixture) : IntegrationTestBase(fixture)
{
    public interface ISlowJob
    {
        Task RunAsync(string token);
    }

    public sealed class SlowJob : ISlowJob
    {
        public static readonly ConcurrentDictionary<string, int> Runs = new(StringComparer.Ordinal);
        public static readonly ConcurrentDictionary<string, TaskCompletionSource> Started = new(StringComparer.Ordinal);
        public static readonly TimeSpan Duration = TimeSpan.FromSeconds(3);

        public async Task RunAsync(string token)
        {
            Runs.AddOrUpdate(token, 1, (_, count) => count + 1);
            Started.GetOrAdd(token, _ => new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)).TrySetResult();
            await Task.Delay(Duration);
        }
    }

    [Fact]
    public async Task AckAfterHandler_AGracefulStopDuringARunningJob_AcksItInsteadOfHandingItToTheNextConsumer()
    {
        // S7#8: the stop used to cancel the consumer and close the channel under the running
        // handler; the broker requeued the delivery and the next consumer ran the job again, while
        // the first handler's ACK failed on the closed channel. The stop now waits (bounded by
        // BackgroundDrainTimeout) for the handler, so the ACK lands first and nothing is redelivered.
        var prefix = NewId("r1-rabbit-stop");
        var token = NewId("slow");

        await using (var first = await StartHostAsync(prefix))
        {
            await first.Provider.GetRequiredService<IAsyncResponseBuilder>().EnqueueWorkerAsync<ISlowJob>(job => job.RunAsync(token));
            await SlowJob.Started.GetOrAdd(token, _ => new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)).Task
                .WaitAsync(TimeSpan.FromSeconds(30));
        } // graceful stop while the 3-second job is still running

        // A second consumer on the same queue: a requeued delivery would reach it within moments.
        await using (await StartHostAsync(prefix))
        {
            await Task.Delay(SlowJob.Duration + TimeSpan.FromSeconds(5));
            Assert.Equal(1, SlowJob.Runs.GetValueOrDefault(token));
        }
    }

    [Fact]
    public async Task WorkerPublish_ACorrelationIdLongerThanAnAmqpShortString_Succeeds()
    {
        // GS5#3: the native correlation-id is an AMQP shortstr (at most 255 UTF-8 bytes) and the
        // client throws while serializing a longer one, so every publish of a 300-character id —
        // within the library's portable 400-unit bound — failed.
        var prefix = NewId("r1-rabbit-cid");
        var options = new RabbitMqAsyncResponseOptions
        {
            ConnectionString = Fixture.RabbitMqConnectionString!,
            WorkerExchange = $"{prefix}.worker",
            WorkerQueue = $"{prefix}.worker",
            WorkerRoutingKey = $"{prefix}.worker",
            ResponseExchange = $"{prefix}.response",
            ResponseQueue = $"{prefix}.response",
            ResponseRoutingKey = $"{prefix}.response"
        };

        await using var transport = new RabbitMqWorkerTransport(Options.Create(options));
        await transport.PublishAsync(new WorkerJobEnvelope
        {
            CorrelationId = new string('c', 300),
            Call = new ReflectionCallDto
            {
                ServiceInterfaceFullName = typeof(ISlowJob).FullName!,
                MethodName = nameof(ISlowJob.RunAsync),
                Params = [CallbackParam.ForValue("unused")]
            }
        });
    }

    private async Task<HostRun> StartHostAsync(string prefix)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<ISlowJob, SlowJob>();
        services.AddAsyncResponse()
            .WithInMemoryChannel()
            .WithInMemoryDurableFlows()
            .WithRabbitMqTransport(options =>
            {
                options.ConnectionString = Fixture.RabbitMqConnectionString!;
                options.WorkerExchange = $"{prefix}.worker";
                options.WorkerQueue = $"{prefix}.worker";
                options.WorkerRoutingKey = $"{prefix}.worker";
                options.ResponseExchange = $"{prefix}.response";
                options.ResponseQueue = $"{prefix}.response";
                options.ResponseRoutingKey = $"{prefix}.response";
            });

        var provider = services.BuildServiceProvider();
        var hosted = provider.GetServices<IHostedService>().ToList();
        foreach (var service in hosted)
            await service.StartAsync(CancellationToken.None);

        return new HostRun(provider, hosted);
    }

    private sealed class HostRun(ServiceProvider provider, List<IHostedService> hosted) : IAsyncDisposable
    {
        public ServiceProvider Provider { get; } = provider;

        public async ValueTask DisposeAsync()
        {
            for (var i = hosted.Count - 1; i >= 0; i--)
                await hosted[i].StopAsync(CancellationToken.None);

            await Provider.DisposeAsync();
        }
    }
}
