using AsyncResponse.Testing;
using AsyncResponse.Transports.AzureServiceBus;
using AsyncResponse.Transports.GooglePubSub;
using AsyncResponse.Transports.Kafka;
using AsyncResponse.Transports.MongoDB;
using AsyncResponse.Transports.NATS;
using AsyncResponse.Transports.PostgreSQL;
using AsyncResponse.Transports.RabbitMQ;
using AsyncResponse.Transports.Redis;
using AsyncResponse.Transports.SqlServer;
using AsyncResponse.Transports.SQS;
using Microsoft.Extensions.Hosting;
using System.Reflection;
using System.Text;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// The small shared-source helpers under <c>src/Transports/Shared</c> —
/// <c>SubscriberSupervisor</c>, <c>CorrelationIdJsonPaths</c> and <c>WorkerIntakeGate</c> — compile
/// into every transport assembly under the same type name, so each fact runs by reflection against
/// all ten compiled copies.
/// </summary>
public sealed class TransportSharedCoverageGapTests
{
    public static TheoryData<Type> TransportAssemblies =>
    [
        typeof(AzureServiceBusAsyncResponseOptions),
        typeof(GooglePubSubAsyncResponseOptions),
        typeof(KafkaAsyncResponseTransportOptions),
        typeof(MongoDbAsyncResponseTransportOptions),
        typeof(NatsAsyncResponseTransportOptions),
        typeof(PostgreSqlAsyncResponseTransportOptions),
        typeof(RabbitMqAsyncResponseOptions),
        typeof(RedisAsyncResponseTransportOptions),
        typeof(SqlServerAsyncResponseTransportOptions),
        typeof(SqsAsyncResponseOptions)
    ];

    /// <summary>
    /// The supervisor's failure count is CONSECUTIVE: failures escalate the delay the policy is asked
    /// for, a run that outlived the healthy-run threshold starts the count over, and every retry is
    /// reported with the delay it waits — in every transport's compiled copy.
    /// </summary>
    [Theory]
    [MemberData(nameof(TransportAssemblies))]
    public async Task Supervisor_EscalatesThenStartsOverAfterAHealthyRun(Type marker)
    {
        var run = marker.Assembly
            .GetType("AsyncResponse.Transports.SubscriberSupervisor", throwOnError: true)!
            .GetMethod("RunAsync", BindingFlags.Public | BindingFlags.Static)!;
        var clock = new VirtualTimeProvider();
        var threshold = TimeSpan.FromSeconds(30);
        var attempt = 0;
        var asked = new List<int>();
        var logged = new List<(string Error, TimeSpan Delay)>();

        Func<CancellationToken, Task> body = _ =>
        {
            attempt++;
            switch (attempt)
            {
                case 1:
                case 2:
                    throw new InvalidOperationException($"fail {attempt}");
                case 3:
                    // Up and consuming for longer than the threshold before it died: healthy.
                    clock.Advance(threshold + TimeSpan.FromSeconds(1));
                    throw new InvalidOperationException("fail 3");
                default:
                    return Task.CompletedTask;
            }
        };

        await (Task)run.Invoke(
            null,
            [
                body,
                CancellationToken.None,
                (Func<int, TimeSpan>)(failures =>
                {
                    asked.Add(failures);
                    return TimeSpan.Zero;
                }),
                (Action<Exception, TimeSpan>)((error, delay) => logged.Add((error.Message, delay))),
                threshold,
                clock
            ])!;

        Assert.Equal(4, attempt);
        Assert.Equal([1, 2, 1], asked);
        Assert.Equal([("fail 1", TimeSpan.Zero), ("fail 2", TimeSpan.Zero), ("fail 3", TimeSpan.Zero)], logged);
    }

    /// <summary>
    /// The duplicate-key scan's per-thread name set is reused across walks, but a set grown by an
    /// unusually wide object (past 256 names) is dropped rather than pinned on the thread — and the
    /// walk over that wide object still resolves the id.
    /// </summary>
    [Theory]
    [MemberData(nameof(TransportAssemblies))]
    public void CorrelationIdWalk_OverAWideObject_ResolvesAndDoesNotPinItsNameSet(Type marker)
    {
        var type = marker.Assembly.GetType("AsyncResponse.Transports.CorrelationIdJsonPaths", throwOnError: true)!;
        var extract = type.GetMethod("Extract", BindingFlags.Public | BindingFlags.Static)!;
        var retained = type.GetField("t_seenNames", BindingFlags.NonPublic | BindingFlags.Static)!;
        string? Extract(string json) => (string?)extract.Invoke(null, [json, new[] { "CorrelationId" }]);

        // A narrow walk leaves its (cleared) set on the thread for the next one.
        Assert.Equal("narrow", Extract("""{"a":1,"CorrelationId":"narrow"}"""));
        var pooled = Assert.IsType<HashSet<string>>(retained.GetValue(null));
        Assert.Empty(pooled);

        var wide = new StringBuilder("{");
        for (var i = 0; i < 300; i++)
            wide.Append("\"p").Append(i).Append("\":").Append(i).Append(',');
        wide.Append("\"CorrelationId\":\"wide\"}");

        Assert.Equal("wide", Extract(wide.ToString()));
        Assert.Null(retained.GetValue(null));

        // The next walk simply starts a fresh set.
        Assert.Equal("again", Extract("""{"CorrelationId":"again"}"""));
        Assert.NotNull(retained.GetValue(null));
    }

    /// <summary>
    /// The worker intake gate exposes the host's ApplicationStopping token itself (loops park on it)
    /// and closes with it; without a registered lifetime it never closes.
    /// </summary>
    [Theory]
    [MemberData(nameof(TransportAssemblies))]
    public void WorkerIntakeGate_FollowsTheHostsApplicationStopping(Type marker)
    {
        var type = marker.Assembly.GetType("AsyncResponse.Transports.WorkerIntakeGate", throwOnError: true)!;
        var isClosed = type.GetProperty("IsClosed")!;
        var hostStopping = type.GetProperty("HostStopping")!;
        using var host = new DurableFlowContextTestSupport.StoppingHost();

        var gate = Activator.CreateInstance(type, [(IHostApplicationLifetime)host])!;
        Assert.Equal(host.ApplicationStopping, (CancellationToken)hostStopping.GetValue(gate)!);
        Assert.False((bool)isClosed.GetValue(gate)!);

        host.StopApplication();
        Assert.True((bool)isClosed.GetValue(gate)!);
        Assert.True(((CancellationToken)hostStopping.GetValue(gate)!).IsCancellationRequested);

        var unhosted = Activator.CreateInstance(type, new object?[] { null })!;
        Assert.Equal(CancellationToken.None, (CancellationToken)hostStopping.GetValue(unhosted)!);
        Assert.False((bool)isClosed.GetValue(unhosted)!);
    }
}
