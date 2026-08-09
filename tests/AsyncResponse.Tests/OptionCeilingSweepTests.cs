using AsyncResponse.Channels.NATS;
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
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// The "passes validation, throws (or hangs) mid-operation" family, closed as a sweep: every
/// TimeSpan option is bounded at its ACTUAL sink. Timer-armed knobs (Task.Delay, WaitAsync, CTS
/// budgets, client-library timers) carry the ~49.7-day .NET timer ceiling; values that become
/// persisted or server-side "now + value" stamps carry the larger 3650-day persistence bound and
/// deliberately ACCEPT beyond-timer-ceiling values — the distinction is the point, over-tight
/// bounds were themselves a shipped bug (a valid 60-day flow lease failed startup).
/// Client-specific domains are tighter still: librdkafka takes 32-bit milliseconds and caps
/// auto.commit.interval.ms at one day; AMQP heartbeats are 16-bit seconds.
/// </summary>
public sealed class OptionCeilingSweepTests
{
    private static readonly TimeSpan BeyondTimerCeiling = TimeSpan.FromDays(60);

    [Fact]
    public void NatsChannel_ConfirmationAndProbeTimeouts_AreTimerBounded()
    {
        // Both feed NatsSubOpts.Timeout, where the NATS client arms a timer at subscribe time.
        var confirmation = Assert.Throws<InvalidOperationException>(
            () => new NatsAsyncResponseChannelOptions { DeliveryConfirmationTimeout = BeyondTimerCeiling }.Validate());
        Assert.Contains(nameof(NatsAsyncResponseChannelOptions.DeliveryConfirmationTimeout), confirmation.Message);

        var probe = Assert.Throws<InvalidOperationException>(
            () => new NatsAsyncResponseChannelOptions { PresenceProbeTimeout = BeyondTimerCeiling }.Validate());
        Assert.Contains(nameof(NatsAsyncResponseChannelOptions.PresenceProbeTimeout), probe.Message);
    }

    [Fact]
    public void NatsTransport_TimerKnobsAreCeilinged_ServerSideDeadlinesAreNot()
    {
        var retry = Assert.Throws<InvalidOperationException>(() => NatsTransportOptionsValidator.ValidateCommon(
            new NatsAsyncResponseTransportOptions { SubscriberRetryBaseDelay = BeyondTimerCeiling, SubscriberRetryMaxDelay = BeyondTimerCeiling }));
        Assert.Contains(nameof(NatsAsyncResponseTransportOptions.SubscriberRetryBaseDelay), retry.Message);

        // AckWait and the NAK redelivery delay ride the wire as nanoseconds and are honored by
        // the SERVER — a 60-day ack deadline is a legitimate JetStream configuration.
        NatsTransportOptionsValidator.ValidateCommon(new NatsAsyncResponseTransportOptions { AckWait = BeyondTimerCeiling });
        NatsTransportOptionsValidator.ValidateSubscriber(new NatsSubscriberOptions { RedeliveryDelay = BeyondTimerCeiling }, "worker");

        var drain = Assert.Throws<InvalidOperationException>(() => NatsTransportOptionsValidator.ValidateSubscriber(
            new NatsSubscriberOptions
            {
                AckMode = NatsAckMode.AckAfterEnqueue,
                BackgroundWorkerCount = 1,
                BackgroundQueueCapacity = 1,
                BackgroundDrainTimeout = BeyondTimerCeiling
            },
            "worker"));
        Assert.Contains(nameof(NatsSubscriberOptions.BackgroundDrainTimeout), drain.Message);
    }

    [Fact]
    public void KafkaTransport_HonorsTheClientsIntMillisecondAndCommitIntervalDomains()
    {
        static KafkaAsyncResponseTransportOptions Options() => new() { BootstrapServers = "localhost:9092" };

        // librdkafka's auto.commit.interval.ms range tops out at one day; a larger value fails
        // CONSUMER CONSTRUCTION inside the subscriber loop, not validation.
        var commit = Assert.Throws<InvalidOperationException>(() => KafkaTransportOptionsValidator.ValidateCommon(
            new KafkaAsyncResponseTransportOptions { BootstrapServers = "localhost:9092", OffsetCommitInterval = TimeSpan.FromDays(2) }));
        Assert.Contains(nameof(KafkaAsyncResponseTransportOptions.OffsetCommitInterval), commit.Message);

        // Flush/admin/Consume timeouts become 32-bit milliseconds inside the Kafka client.
        var operation = Assert.Throws<InvalidOperationException>(() => KafkaTransportOptionsValidator.ValidateCommon(
            new KafkaAsyncResponseTransportOptions { BootstrapServers = "localhost:9092", OperationTimeout = TimeSpan.FromDays(30) }));
        Assert.Contains(nameof(KafkaAsyncResponseTransportOptions.OperationTimeout), operation.Message);

        var poll = Assert.Throws<InvalidOperationException>(() => KafkaMessageDispatcher.ValidateOptions(
            Options(),
            new KafkaSubscriberOptions { PollTimeout = TimeSpan.FromDays(30) },
            KafkaSubscriberRole.Worker));
        Assert.Contains(nameof(KafkaSubscriberOptions.PollTimeout), poll.Message);

        var handlerRetry = Assert.Throws<InvalidOperationException>(() => KafkaMessageDispatcher.ValidateOptions(
            Options(),
            new KafkaSubscriberOptions { HandlerRetryBaseDelay = BeyondTimerCeiling, HandlerRetryMaxDelay = BeyondTimerCeiling },
            KafkaSubscriberRole.Worker));
        Assert.Contains(nameof(KafkaSubscriberOptions.HandlerRetryBaseDelay), handlerRetry.Message);
    }

    [Fact]
    public void RabbitMq_ConnectionFactoryKnobs_AreBoundedBeforeTheFirstConnect()
    {
        // AMQP heartbeats are 16-bit seconds; the client would throw copying a larger value into
        // ConnectionFactory at the FIRST connect. Zero (disabled) stays valid.
        var heartbeat = Assert.Throws<InvalidOperationException>(() => RabbitMqOptionsValidator.ValidateConnection(
            new RabbitMqAsyncResponseOptions { RequestedHeartbeat = TimeSpan.FromSeconds(ushort.MaxValue + 1) }));
        Assert.Contains(nameof(RabbitMqAsyncResponseOptions.RequestedHeartbeat), heartbeat.Message);
        RabbitMqOptionsValidator.ValidateConnection(new RabbitMqAsyncResponseOptions { RequestedHeartbeat = TimeSpan.Zero });

        var recovery = Assert.Throws<InvalidOperationException>(() => RabbitMqOptionsValidator.ValidateConnection(
            new RabbitMqAsyncResponseOptions { NetworkRecoveryInterval = BeyondTimerCeiling }));
        Assert.Contains(nameof(RabbitMqAsyncResponseOptions.NetworkRecoveryInterval), recovery.Message);

        // The RabbitMQ client uses the interval DIRECTLY in its recovery loop's Task.Delay: a
        // negative value faults (and terminates) that loop, zero spins it — both are rejected.
        Assert.Throws<InvalidOperationException>(() => RabbitMqOptionsValidator.ValidateConnection(
            new RabbitMqAsyncResponseOptions { NetworkRecoveryInterval = TimeSpan.Zero }));
        Assert.Throws<InvalidOperationException>(() => RabbitMqOptionsValidator.ValidateConnection(
            new RabbitMqAsyncResponseOptions { NetworkRecoveryInterval = TimeSpan.FromSeconds(-1) }));
    }

    [Fact]
    public void GooglePubSub_TimeoutsArePinnedAtLast()
    {
        // These previously had NO validation: a negative retry delay surfaced as a raw
        // ArgumentOutOfRangeException inside the subscriber retry loop's Task.Delay.
        var negative = Assert.Throws<InvalidOperationException>(() => GooglePubSubOptionsValidator.ValidateTimeouts(
            new GooglePubSubAsyncResponseOptions { SubscriberRetryBaseDelay = TimeSpan.FromSeconds(-1) }));
        Assert.Contains(nameof(GooglePubSubAsyncResponseOptions.SubscriberRetryBaseDelay), negative.Message);

        var ceiling = Assert.Throws<InvalidOperationException>(() => GooglePubSubOptionsValidator.ValidateTimeouts(
            new GooglePubSubAsyncResponseOptions { SubscriberRetryBaseDelay = BeyondTimerCeiling, SubscriberRetryMaxDelay = BeyondTimerCeiling }));
        Assert.Contains(nameof(GooglePubSubAsyncResponseOptions.SubscriberRetryBaseDelay), ceiling.Message);

        var misordered = Assert.Throws<InvalidOperationException>(() => GooglePubSubOptionsValidator.ValidateTimeouts(
            new GooglePubSubAsyncResponseOptions { SubscriberRetryBaseDelay = TimeSpan.FromSeconds(10), SubscriberRetryMaxDelay = TimeSpan.FromSeconds(1) }));
        Assert.Contains(nameof(GooglePubSubAsyncResponseOptions.SubscriberRetryMaxDelay), misordered.Message);

        var shutdown = Assert.Throws<InvalidOperationException>(() => GooglePubSubOptionsValidator.ValidateTimeouts(
            new GooglePubSubAsyncResponseOptions { ShutdownTimeout = BeyondTimerCeiling }));
        Assert.Contains(nameof(GooglePubSubAsyncResponseOptions.ShutdownTimeout), shutdown.Message);

        GooglePubSubOptionsValidator.ValidateTimeouts(new GooglePubSubAsyncResponseOptions());
    }

    [Fact]
    public void RedisTransport_OperationTimeoutIsTimerBounded_ServerSideIdleIsNot()
    {
        var operation = Assert.Throws<InvalidOperationException>(() => RedisTransportOptionsValidator.ValidateCommon(
            new RedisAsyncResponseTransportOptions { OperationTimeout = BeyondTimerCeiling }));
        Assert.Contains(nameof(RedisAsyncResponseTransportOptions.OperationTimeout), operation.Message);

        // PendingMessageMinIdleTime is the server-side XAUTOCLAIM min-idle: beyond the timer
        // ceiling is legitimate. EmptyPollDelay arms the idle Task.Delay and is not.
        RedisMessageDispatcher.ValidateOptions(
            new RedisAsyncResponseTransportOptions(),
            new RedisSubscriberOptions { PendingMessageMinIdleTime = BeyondTimerCeiling },
            RedisSubscriberRole.Worker);

        var poll = Assert.Throws<InvalidOperationException>(() => RedisMessageDispatcher.ValidateOptions(
            new RedisAsyncResponseTransportOptions(),
            new RedisSubscriberOptions { EmptyPollDelay = BeyondTimerCeiling },
            RedisSubscriberRole.Worker));
        Assert.Contains(nameof(RedisSubscriberOptions.EmptyPollDelay), poll.Message);
    }

    [Fact]
    public void DbTransports_LockTimeoutIsTimerBounded_RedeliveryStampsAreNot()
    {
        // LockTimeout also drives the in-process lease-renewal Task.Delay (at half its value);
        // RedeliveryDelay is a database-side visibility stamp and accepts 60 days.
        var pgLock = Assert.Throws<InvalidOperationException>(() => PostgreSqlTransportOptionsValidator.ValidateCommon(
            new PostgreSqlAsyncResponseTransportOptions { LockTimeout = BeyondTimerCeiling }));
        Assert.Contains(nameof(PostgreSqlAsyncResponseTransportOptions.LockTimeout), pgLock.Message);
        PostgreSqlTransportOptionsValidator.ValidateSubscriber(
            new PostgreSqlSubscriberOptions { RedeliveryDelay = BeyondTimerCeiling }, "worker");

        var sqlLock = Assert.Throws<InvalidOperationException>(() => SqlServerTransportOptionsValidator.ValidateCommon(
            new SqlServerAsyncResponseTransportOptions
            {
                ConnectionString = "Server=localhost;Database=x;User ID=sa;Password=unused;TrustServerCertificate=True",
                LockTimeout = BeyondTimerCeiling
            }));
        Assert.Contains(nameof(SqlServerAsyncResponseTransportOptions.LockTimeout), sqlLock.Message);
        SqlServerTransportOptionsValidator.ValidateSubscriber(
            new SqlServerSubscriberOptions { RedeliveryDelay = BeyondTimerCeiling }, "worker");

        var mongoLock = Assert.Throws<InvalidOperationException>(() => MongoDbTransportOptionsValidator.ValidateCommon(
            new MongoDbAsyncResponseTransportOptions { LockTimeout = BeyondTimerCeiling }));
        Assert.Contains(nameof(MongoDbAsyncResponseTransportOptions.LockTimeout), mongoLock.Message);
        MongoDbTransportOptionsValidator.ValidateSubscriber(
            new MongoDbSubscriberOptions { RedeliveryDelay = BeyondTimerCeiling }, "worker");

        // DeadLetterRetention is a prune cutoff and carries the persistence bound.
        var retention = Assert.Throws<InvalidOperationException>(() => PostgreSqlTransportOptionsValidator.ValidateCommon(
            new PostgreSqlAsyncResponseTransportOptions { DeadLetterRetention = TimeSpan.FromDays(4000) }));
        Assert.Contains(nameof(PostgreSqlAsyncResponseTransportOptions.DeadLetterRetention), retention.Message);
    }

    [Fact]
    public void SqsAndServiceBus_TimerKnobsAreCeilinged()
    {
        var sqs = Assert.Throws<InvalidOperationException>(() => SqsOptionsValidator.ValidateCommon(
            new SqsAsyncResponseOptions { SubscriberRetryBaseDelay = BeyondTimerCeiling, SubscriberRetryMaxDelay = BeyondTimerCeiling }));
        Assert.Contains(nameof(SqsAsyncResponseOptions.SubscriberRetryBaseDelay), sqs.Message);

        var receive = Assert.Throws<InvalidOperationException>(() => AzureServiceBusOptionsValidator.ValidateCommon(
            new AzureServiceBusAsyncResponseOptions { ReceiveWaitTime = BeyondTimerCeiling }));
        Assert.Contains(nameof(AzureServiceBusAsyncResponseOptions.ReceiveWaitTime), receive.Message);

        var renewal = Assert.Throws<InvalidOperationException>(() => AzureServiceBusOptionsValidator.ValidateSubscriber(
            new AzureServiceBusAsyncResponseOptions(),
            new AzureServiceBusSubscriberOptions { LockRenewalInterval = BeyondTimerCeiling },
            AzureServiceBusSubscriberRole.Worker));
        Assert.Contains(nameof(AzureServiceBusSubscriberOptions.LockRenewalInterval), renewal.Message);
    }
}
