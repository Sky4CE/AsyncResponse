using AsyncResponse.Transports.AzureServiceBus;
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
}
