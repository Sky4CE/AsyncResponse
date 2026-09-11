using System.Collections.Concurrent;
using AsyncResponse.Transports.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace AsyncResponse.IntegrationTests;

/// <summary>
/// Round 37 (F7): the real broker's answer to a handler that outlives <c>max.poll.interval.ms</c>.
/// The poll thread used to await the whole handler, so past the interval librdkafka left the
/// group (MAXPOLL), the next poll rejoined, the partition was re-fetched from the last committed
/// offset — the message's offset was never stored — and the same job ran a second time. With the
/// handler detached and polling continuing, the consumer stays in its group and the job runs once.
/// </summary>
[Collection(BrokersCollection.Name)]
[Trait(Batches.Trait, Batches.Brokers)]
public sealed class KafkaLongHandlerIntegrationTests(BrokersBatchFixture fixture) : IntegrationTestBase(fixture)
{
    public interface ISlowProbe
    {
        Task RunAsync(string token);
    }

    public sealed class SlowProbe : ISlowProbe
    {
        public static readonly ConcurrentDictionary<string, int> Runs = new(StringComparer.Ordinal);
        public static readonly TimeSpan Duration = TimeSpan.FromSeconds(12);

        public async Task RunAsync(string token)
        {
            Runs.AddOrUpdate(token, 1, (_, count) => count + 1);
            await Task.Delay(Duration);
        }
    }

    [Fact]
    public async Task AckAfterHandler_AHandlerOutlivingMaxPollInterval_RunsExactlyOnce()
    {
        var prefix = NewId("r37-long");
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<ISlowProbe, SlowProbe>();
        services.AddAsyncResponse()
            .WithInMemoryChannel()
            .WithInMemoryDurableFlows()
            .WithKafkaTransport(options =>
            {
                options.BootstrapServers = Fixture.KafkaBootstrapServers!;
                options.TopicPrefix = prefix;
                options.WorkerConsumerGroup = $"{prefix}-workers";
                options.ResponseConsumerGroup = $"{prefix}-responses";
                options.CreateTopics = true;
                // A poll interval the 12-second handler comfortably outlives, with the session
                // settings the broker's default minimums allow.
                options.WorkerSubscriber.MaxPollInterval = TimeSpan.FromSeconds(8);
                options.WorkerSubscriber.PollTimeout = TimeSpan.FromMilliseconds(100);
                options.ConfigureConsumer = config =>
                {
                    config.SessionTimeoutMs = 6000;
                    config.HeartbeatIntervalMs = 2000;
                };
            });

        await using var provider = services.BuildServiceProvider();
        var hosted = provider.GetServices<IHostedService>().ToList();
        foreach (var service in hosted)
            await service.StartAsync(CancellationToken.None);

        try
        {
            var token = NewId("slow");
            await provider.GetRequiredService<IAsyncResponseBuilder>().EnqueueWorkerAsync<ISlowProbe>(probe => probe.RunAsync(token));

            var firstRun = await PollAsync(
                () => Task.FromResult(SlowProbe.Runs.GetValueOrDefault(token)),
                runs => runs >= 1,
                TimeSpan.FromSeconds(90));
            Assert.Equal(1, firstRun);

            // Past the handler's own duration and the poll interval with margin: a consumer evicted
            // for exceeding max.poll.interval.ms would have rejoined and re-run the job by now.
            await Task.Delay(SlowProbe.Duration + TimeSpan.FromSeconds(10));
            Assert.Equal(1, SlowProbe.Runs.GetValueOrDefault(token));
        }
        finally
        {
            hosted.Reverse();
            foreach (var service in hosted)
                await service.StopAsync(CancellationToken.None);
        }
    }
}
