using AsyncResponse.Transports.AzureServiceBus;
using Azure.Core.Amqp;
using Azure.Messaging.ServiceBus;
using Moq;
using Xunit;

namespace AsyncResponse.Tests;

public sealed class AzureServiceBusClientAdapterTests
{
    [Fact]
    public async Task ReceiverAdapter_AcceptsApplicationPropertiesDifferingOnlyInCase()
    {
        // AMQP application-property names are case-sensitive, so a producer can legally stamp
        // both 'TenantId' and 'tenantId' on one message. Copying them through the Dictionary
        // constructor with a case-insensitive comparer used Add() internally and threw
        // ArgumentException out of ReceiveMessagesAsync — before any delivery in the batch was
        // settled, so the whole batch stalled and redelivered until the lock-expiry DLQ path.
        // The copy must go through the indexer (last-seen wins), like the SQS and Kafka adapters.
        var message = ServiceBusModelFactory.ServiceBusReceivedMessage(
            body: BinaryData.FromString("{}"),
            messageId: "m1",
            correlationId: "c1",
            properties: new Dictionary<string, object>
            {
                ["TenantId"] = "a",
                ["tenantId"] = "b"
            });
        var receiver = new Mock<ServiceBusReceiver>();
        receiver
            .Setup(inner => inner.ReceiveMessagesAsync(It.IsAny<int>(), It.IsAny<TimeSpan?>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync([message]);
        var adapter = new AzureServiceBusReceiverAdapter(receiver.Object, queueOverride: "worker-queue");

        var deliveries = await adapter.ReceiveMessagesAsync(10, TimeSpan.FromMilliseconds(50));

        var delivery = Assert.Single(deliveries);
        // The two casings collapse to one entry under the case-insensitive lookup comparer, and
        // the lookup itself stays case-insensitive.
        Assert.Single(delivery.ApplicationProperties);
        Assert.True(delivery.ApplicationProperties.ContainsKey("TENANTID"));
    }

    [Fact]
    public async Task ReceiverAdapter_BuriesAnUnprojectableMessage_AndKeepsTheRestOfTheBatch()
    {
        // Regression (round 29): ServiceBusReceivedMessage.Body throws NotSupportedException for an
        // AMQP Value/Sequence body — what a JMS or raw-AMQP producer sends — and that throw escaped
        // the WHOLE batch. Nothing was settled, all N locks lapsed, all N DeliveryCounts advanced,
        // and the poison message crashed the receive loop again next cycle while its innocent
        // batch-mates burned attempts toward the entity's MaxDeliveryCount without ever running.
        var poison = ServiceBusModelFactory.ServiceBusReceivedMessage(
            body: BinaryData.FromString("{}"), messageId: "poison");
        poison.GetRawAmqpMessage().Body = AmqpMessageBody.FromValue("an AMQP Value body");
        Assert.Throws<NotSupportedException>(() => poison.Body);

        var healthy = ServiceBusModelFactory.ServiceBusReceivedMessage(
            body: BinaryData.FromString("""{"ok":true}"""), messageId: "healthy");

        var receiver = new Mock<ServiceBusReceiver>();
        receiver
            .Setup(inner => inner.ReceiveMessagesAsync(It.IsAny<int>(), It.IsAny<TimeSpan?>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync([poison, healthy]);
        receiver
            .Setup(inner => inner.DeadLetterMessageAsync(
                It.IsAny<ServiceBusReceivedMessage>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        var adapter = new AzureServiceBusReceiverAdapter(receiver.Object, queueOverride: "worker-queue");

        var deliveries = await adapter.ReceiveMessagesAsync(10, TimeSpan.FromMilliseconds(50));

        // The batch survives, minus the one message that could never be projected...
        Assert.Equal("healthy", Assert.Single(deliveries).MessageId);

        // ...which is buried rather than left to lapse and poison the loop forever.
        receiver.Verify(
            inner => inner.DeadLetterMessageAsync(
                poison,
                "AsyncResponseUnsupportedBody",
                nameof(NotSupportedException),
                It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task ReceiverAdapter_WhenBuryingTheUnprojectableMessageAlsoFails_StillReturnsTheRest()
    {
        // Best-effort burial: if the dead-letter itself fails the lock simply lapses and the broker
        // redelivers, which is still strictly better than losing the deliveries already projected.
        var poison = ServiceBusModelFactory.ServiceBusReceivedMessage(
            body: BinaryData.FromString("{}"), messageId: "poison");
        poison.GetRawAmqpMessage().Body = AmqpMessageBody.FromValue("an AMQP Value body");

        var healthy = ServiceBusModelFactory.ServiceBusReceivedMessage(
            body: BinaryData.FromString("""{"ok":true}"""), messageId: "healthy");

        var receiver = new Mock<ServiceBusReceiver>();
        receiver
            .Setup(inner => inner.ReceiveMessagesAsync(It.IsAny<int>(), It.IsAny<TimeSpan?>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync([poison, healthy]);
        receiver
            .Setup(inner => inner.DeadLetterMessageAsync(
                It.IsAny<ServiceBusReceivedMessage>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new ServiceBusException("lock lost", ServiceBusFailureReason.MessageLockLost));
        var adapter = new AzureServiceBusReceiverAdapter(receiver.Object, queueOverride: "worker-queue");

        var deliveries = await adapter.ReceiveMessagesAsync(10, TimeSpan.FromMilliseconds(50));

        Assert.Equal("healthy", Assert.Single(deliveries).MessageId);
    }
}
