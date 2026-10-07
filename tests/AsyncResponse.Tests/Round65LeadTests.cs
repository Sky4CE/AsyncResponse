using System.Collections.Concurrent;
using System.Reflection;
using AsyncResponse.DurableFlows.EFCore;
using AsyncResponse.Transports.Kafka;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using AsyncResponse.Transports.RabbitMQ;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Round 65 sibling fixes applied by the lead: the Kafka and RabbitMQ siblings of the Redis
/// summed-drain finding (L-04). The worker and response subscribers are two hosted services the
/// host stops one after the other inside one budget, so with both roles in early ACK their stop
/// paths must fit the budget together.
/// </summary>
public sealed class Round65LeadTests
{
    /// <summary>
    /// Critic follow-up on L-06: a refused generic payload name (its loaded definition is not a
    /// payload) is remembered, so a poisoned recovery row redelivered again costs a dictionary hit
    /// instead of re-resolving the definition (an unqualified name walks every loaded assembly).
    /// The refusal stays out of the shared negative cache the callback service resolver reads.
    /// </summary>
    [Fact]
    public void RefusedGenericPayloadName_IsRememberedOutsideTheSharedNegativeCache()
    {
        var name = typeof(Dictionary<Round65LeadKey, Round65LeadKey>).FullName!;
        var refused = (ConcurrentDictionary<string, byte>)typeof(PayloadRecoveryClassifier)
            .GetField("RefusedPayloadNames", BindingFlags.NonPublic | BindingFlags.Static)!
            .GetValue(null)!;

        Assert.Null(PayloadRecoveryClassifier.ResolvePayloadType(name));
        Assert.True(refused.ContainsKey(name));
        Assert.False(UnresolvableTypeNames.IsKnownMiss(name));
        Assert.Null(PayloadRecoveryClassifier.ResolvePayloadType(name));
    }

    private sealed class Round65LeadKey;

    /// <summary>
    /// Critic follow-up on L-18: reading the model at host start runs the application's own
    /// OnModelCreating/OnConfiguring, which may fail for a reason the first operation does not
    /// share. Only the mapping refusal may fail the start; anything else is logged and left.
    /// </summary>
    [Fact]
    public async Task EFCore_AModelThatCannotBeBuiltAtStart_IsLeftToTheFirstOperation()
    {
        var logger = new CollectingLogger();
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddSingleton(logger.For<EFCoreFlowStateStore<ModelNotReadyFlowDbContext>>());
        services.AddDbContextFactory<ModelNotReadyFlowDbContext>(options => options.UseSqlite("Data Source=unused-r65-lead.db"));
        services.AddAsyncResponse().WithInMemoryChannel().WithInMemoryTransport().WithEFCoreDurableFlows<ModelNotReadyFlowDbContext>();
        await using var provider = services.BuildServiceProvider();
        var validator = provider.GetServices<IHostedService>().OfType<AsyncResponseStartupValidator>().Single();

        await validator.StartAsync(CancellationToken.None);

        Assert.Single(logger.Messages, message => message.Contains("could not read the 'ModelNotReadyFlowDbContext' model while the host started", StringComparison.Ordinal));
        Assert.False(File.Exists("unused-r65-lead.db"), "the start check must not open the database");
    }

    private sealed class ModelNotReadyFlowDbContext(DbContextOptions<ModelNotReadyFlowDbContext> options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
            => throw new InvalidOperationException("the tenant model is not available yet");
    }

    /// <summary>Red on 6d7e1aeb: each role was validated alone, so two 20 s drains passed against 30 s.</summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void Kafka_BothRolesEarlyAck_TheirDrainsAreSummedAgainstTheHostBudget(bool validateWorker)
    {
        var options = KafkaTestData.NewOptions();
        options.HostShutdownTimeout = TimeSpan.FromSeconds(30);
        options.WorkerSubscriber.UseAckAfterEnqueue(4, 100, TimeSpan.FromSeconds(20));
        options.ResponseSubscriber.UseAckAfterEnqueue(4, 100, TimeSpan.FromSeconds(20));

        var ex = Assert.Throws<InvalidOperationException>(() => KafkaMessageDispatcher.ValidateOptions(
            options,
            validateWorker ? options.WorkerSubscriber : options.ResponseSubscriber,
            validateWorker ? KafkaSubscriberRole.Worker : KafkaSubscriberRole.ResponseIngress));

        Assert.Contains("00:00:40", ex.Message, StringComparison.Ordinal);
        Assert.Contains("WorkerSubscriber.BackgroundDrainTimeout", ex.Message, StringComparison.Ordinal);
        Assert.Contains("ResponseSubscriber.BackgroundDrainTimeout", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Kafka_BothRolesEarlyAck_WithinTheBudgetTogether_IsAccepted_AndOneRoleAloneIsNotSummed()
    {
        var both = KafkaTestData.NewOptions();
        both.HostShutdownTimeout = TimeSpan.FromSeconds(30);
        both.WorkerSubscriber.UseAckAfterEnqueue(4, 100, TimeSpan.FromSeconds(15));
        both.ResponseSubscriber.UseAckAfterEnqueue(4, 100, TimeSpan.FromSeconds(15));
        KafkaMessageDispatcher.ValidateOptions(both, both.WorkerSubscriber, KafkaSubscriberRole.Worker);
        KafkaMessageDispatcher.ValidateOptions(both, both.ResponseSubscriber, KafkaSubscriberRole.ResponseIngress);

        var one = KafkaTestData.NewOptions();
        one.HostShutdownTimeout = TimeSpan.FromSeconds(30);
        one.WorkerSubscriber.UseAckAfterEnqueue(4, 100, TimeSpan.FromSeconds(25));
        KafkaMessageDispatcher.ValidateOptions(one, one.WorkerSubscriber, KafkaSubscriberRole.Worker);
    }

    /// <summary>
    /// Red on 6d7e1aeb: one subscriber's 5 s cancel + 15 s drain + 5 s close fit 30 s alone, so
    /// both passed although the second stop needs another 25 s.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void RabbitMq_BothRolesEarlyAck_TheirStopPathsAreSummedAgainstTheHostBudget(bool validateWorker)
    {
        var options = new RabbitMqAsyncResponseOptions { HostShutdownTimeout = TimeSpan.FromSeconds(30) };
        options.WorkerSubscriber.UseAckAfterEnqueue(4, 100, TimeSpan.FromSeconds(15));
        options.ResponseSubscriber.UseAckAfterEnqueue(4, 100, TimeSpan.FromSeconds(15));

        var ex = Assert.Throws<InvalidOperationException>(() => RabbitMqMessageDispatcher.ValidateOptions(
            options,
            validateWorker ? options.WorkerSubscriber : options.ResponseSubscriber,
            validateWorker ? RabbitMqSubscriberRole.Worker : RabbitMqSubscriberRole.ResponseIngress));

        Assert.Contains("00:00:50", ex.Message, StringComparison.Ordinal);
        Assert.Contains("WorkerSubscriber.BackgroundDrainTimeout", ex.Message, StringComparison.Ordinal);
        Assert.Contains("ResponseSubscriber.BackgroundDrainTimeout", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void RabbitMq_BothRolesEarlyAck_WithinTheBudgetTogether_IsAccepted_AndOneRoleAloneIsNotSummed()
    {
        var both = new RabbitMqAsyncResponseOptions { HostShutdownTimeout = TimeSpan.FromSeconds(30), ShutdownTimeout = TimeSpan.FromSeconds(2) };
        both.WorkerSubscriber.UseAckAfterEnqueue(4, 100, TimeSpan.FromSeconds(10));
        both.ResponseSubscriber.UseAckAfterEnqueue(4, 100, TimeSpan.FromSeconds(10));
        RabbitMqMessageDispatcher.ValidateOptions(both, both.WorkerSubscriber, RabbitMqSubscriberRole.Worker);
        RabbitMqMessageDispatcher.ValidateOptions(both, both.ResponseSubscriber, RabbitMqSubscriberRole.ResponseIngress);

        var one = new RabbitMqAsyncResponseOptions { HostShutdownTimeout = TimeSpan.FromSeconds(30) };
        one.WorkerSubscriber.UseAckAfterEnqueue(4, 100, TimeSpan.FromSeconds(20));
        RabbitMqMessageDispatcher.ValidateOptions(one, one.WorkerSubscriber, RabbitMqSubscriberRole.Worker);
    }
}
