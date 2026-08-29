using AsyncResponse.Transports.AzureServiceBus;
using AsyncResponse.Transports.GooglePubSub;
using AsyncResponse.Transports.Kafka;
using AsyncResponse.Transports.NATS;
using AsyncResponse.Transports.RabbitMQ;
using AsyncResponse.Transports.Redis;
using AsyncResponse.Transports.SQS;
using Microsoft.Extensions.Options;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Regression (round 31): the r24 named-reply-target distinctness rule (see
/// <see cref="DbTransportReplyTargetCollisionTests"/>) landed only in the three database
/// transports. On the seven broker transports a NAMED target was still only checked for
/// non-emptiness, so <c>AddReplyTarget("billing", &lt;the worker destination&gt;)</c> passed every
/// check and stamped the WORKER destination as the reply address — every response addressed to the
/// target was consumed as a worker job (NAK-cycled to the cap and dead-lettered) while the waiter
/// timed out. Each provider now rejects a named target that collides with its worker or
/// dead-letter destination.
/// </summary>
public sealed class BrokerReplyTargetCollisionTests
{
    [Theory]
    [InlineData("asyncresponse.transport.worker")]
    [InlineData("asyncresponse.transport.deadletter")]
    public void Nats_NamedTargetCollidingWithWorkerOrDeadLetterSubject_IsRejected(string subject)
    {
        var options = new NatsAsyncResponseTransportOptions().AddReplyTarget("billing", subject);
        var provider = new NatsReplyTargetProvider(Options.Create(options));

        var exception = Assert.Throws<InvalidOperationException>(() => provider.GetReplyTarget("billing"));
        Assert.Contains("billing", exception.Message);
        Assert.Contains(subject, exception.Message);
    }

    [Fact]
    public void Nats_NamedTargetMatchingTheResponseSubject_IsAllowed()
    {
        // The transport-wide response subject IS the default target's destination, so a named
        // target pointing at it is legitimate.
        var options = new NatsAsyncResponseTransportOptions().AddReplyTarget("billing", "asyncresponse.transport.response");
        var provider = new NatsReplyTargetProvider(Options.Create(options));

        Assert.Equal("asyncresponse.transport.response", provider.GetReplyTarget("billing").Address);
    }

    [Theory]
    [InlineData("asyncresponse:transport:worker")]
    [InlineData("asyncresponse:transport:deadletter")]
    public void Redis_NamedTargetCollidingWithWorkerOrDeadLetterStream_IsRejected(string stream)
    {
        var options = new RedisAsyncResponseTransportOptions().AddReplyTarget("billing", stream);
        var provider = new RedisReplyTargetProvider(Options.Create(options));

        var exception = Assert.Throws<InvalidOperationException>(() => provider.GetReplyTarget("billing"));
        Assert.Contains("billing", exception.Message);
        Assert.Contains(stream, exception.Message);
    }

    [Theory]
    [InlineData("asyncresponse.transport.worker")]
    [InlineData("asyncresponse.transport.worker.deadletter")]
    [InlineData("asyncresponse.transport.response.deadletter")]
    public void Kafka_NamedTargetCollidingWithWorkerOrDerivedDeadLetterTopic_IsRejected(string topic)
    {
        var options = new KafkaAsyncResponseTransportOptions { BootstrapServers = "localhost:9092" }
            .AddReplyTarget("billing", topic);
        var provider = new KafkaReplyTargetProvider(Options.Create(options));

        var exception = Assert.Throws<InvalidOperationException>(() => provider.GetReplyTarget("billing"));
        Assert.Contains("billing", exception.Message);
        Assert.Contains(topic, exception.Message);
    }

    [Fact]
    public void RabbitMq_NamedTargetRoutingToTheWorkerPublishPair_IsRejected()
    {
        var options = new RabbitMqAsyncResponseOptions()
            .AddReplyTarget("billing", "asyncresponse.worker", "asyncresponse.worker");
        var provider = new RabbitMqReplyTargetProvider(Options.Create(options));

        var exception = Assert.Throws<InvalidOperationException>(() => provider.GetReplyTarget("billing"));
        Assert.Contains("billing", exception.Message);
    }

    [Fact]
    public void RabbitMq_NamedTargetAimedAtTheDeadLetterExchange_IsRejected()
    {
        // DeadLetterRoutingKey defaults to the delivery's own routing key, so ANY routing key on
        // the dead-letter exchange lands in buried traffic.
        var options = new RabbitMqAsyncResponseOptions { DeadLetterExchange = "dlx" }
            .AddReplyTarget("billing", "dlx", "some.route");
        var provider = new RabbitMqReplyTargetProvider(Options.Create(options));

        var exception = Assert.Throws<InvalidOperationException>(() => provider.GetReplyTarget("billing"));
        Assert.Contains("billing", exception.Message);
    }

    [Fact]
    public void AzureServiceBus_NamedTargetCollidingWithTheWorkerQueue_IsRejected()
    {
        var options = new AzureServiceBusAsyncResponseOptions().AddReplyTarget("billing", "asyncresponse-worker");
        var provider = new AzureServiceBusReplyTargetProvider(Options.Create(options));

        var exception = Assert.Throws<InvalidOperationException>(() => provider.GetReplyTarget("billing"));
        Assert.Contains("billing", exception.Message);
        Assert.Contains("asyncresponse-worker", exception.Message);
    }

    [Theory]
    [InlineData("asyncresponse-worker")]
    [InlineData("asyncresponse-worker-dlq")]
    [InlineData("asyncresponse-response-dlq")]
    public void Sqs_NamedTargetCollidingWithWorkerOrDerivedDeadLetterQueue_IsRejected(string queue)
    {
        var options = new SqsAsyncResponseOptions().AddReplyTarget("billing", queue);
        var provider = new SqsReplyTargetProvider(Options.Create(options));

        var exception = Assert.Throws<InvalidOperationException>(() => provider.GetReplyTarget("billing"));
        Assert.Contains("billing", exception.Message);
        Assert.Contains(queue, exception.Message);
    }

    [Fact]
    public void GooglePubSub_NamedTargetCollidingWithTheWorkerTopicInTheSameProject_IsRejected()
    {
        var options = new GooglePubSubAsyncResponseOptions
        {
            ProjectId = "proj",
            WorkerTopicId = "worker-topic",
            ResponseTopicId = "response-topic"
        }.AddReplyTarget("billing", "proj", "worker-topic");
        var provider = new GooglePubSubReplyTargetProvider(Options.Create(options));

        var exception = Assert.Throws<InvalidOperationException>(() => provider.GetReplyTarget("billing"));
        Assert.Contains("billing", exception.Message);
        Assert.Contains("worker-topic", exception.Message);
    }

    [Fact]
    public void GooglePubSub_SameTopicIdInAnotherProject_IsAllowed()
    {
        // A different project cannot collide with the transport's own worker topic.
        var options = new GooglePubSubAsyncResponseOptions
        {
            ProjectId = "proj",
            WorkerTopicId = "worker-topic",
            ResponseTopicId = "response-topic"
        }.AddReplyTarget("billing", "other-proj", "worker-topic");
        var provider = new GooglePubSubReplyTargetProvider(Options.Create(options));

        Assert.Contains("other-proj", provider.GetReplyTarget("billing").Address);
    }
}
