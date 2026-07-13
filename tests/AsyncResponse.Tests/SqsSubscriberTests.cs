using Amazon.SQS;
using Amazon.SQS.Model;
using AsyncResponse.Transports.SQS;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using Xunit;

namespace AsyncResponse.Tests;

public sealed class SqsSubscriberTests
{
    [Fact]
    public async Task WorkerSubscriber_ForwardsBodyAndDeletesMessage()
    {
        var client = new FakeSqsClient();
        var ingress = new Mock<IAsyncResponseIngress>();
        ingress.Setup(i => i.HandleWorkerMessageAsync("worker-json")).Returns(Task.CompletedTask);
        var calls = new SettlementCalls();
        var subscriber = new SqsWorkerSubscriber(
            Options.Create(new SqsAsyncResponseOptions
            {
                WorkerQueue = "workers",
                ResponseQueue = "responses",
                ReceiveWaitTime = TimeSpan.FromMilliseconds(10)
            }),
            client,
            ingress.Object,
            NullLogger<SqsWorkerSubscriber>.Instance);

        await subscriber.StartAsync(CancellationToken.None);
        client.Enqueue(Delivery(calls, body: "worker-json"));

        await calls.Deleted.Task.WaitAsync(TimeSpan.FromSeconds(2));
        await subscriber.StopAsync(CancellationToken.None);

        ingress.Verify(i => i.HandleWorkerMessageAsync("worker-json"), Times.Once);
        Assert.Contains("workers", client.ResolvedQueueNames);
        Assert.Equal(FakeSqsClient.UrlFor("workers"), client.LastReceiveRequest!.QueueUrl);
        Assert.Equal(1, calls.Delete);
    }

    [Fact]
    public async Task ResponseSubscriber_ExtractsCorrelationAndForwardsBody()
    {
        var client = new FakeSqsClient();
        var ingress = new Mock<IAsyncResponseIngress>();
        ingress.Setup(i => i.HandleResponseMessageAsync("response-json", "corr-response")).Returns(Task.CompletedTask);
        var calls = new SettlementCalls();
        var subscriber = new SqsResponseIngressSubscriber(
            Options.Create(new SqsAsyncResponseOptions
            {
                WorkerQueue = "workers",
                ResponseQueue = "responses",
                ReceiveWaitTime = TimeSpan.FromMilliseconds(10)
            }),
            client,
            ingress.Object,
            NullLogger<SqsResponseIngressSubscriber>.Instance);

        await subscriber.StartAsync(CancellationToken.None);
        client.Enqueue(Delivery(
            calls,
            body: "response-json",
            attributes: new Dictionary<string, string> { ["correlationId"] = "corr-response" }));

        await calls.Deleted.Task.WaitAsync(TimeSpan.FromSeconds(2));
        await subscriber.StopAsync(CancellationToken.None);

        ingress.Verify(i => i.HandleResponseMessageAsync("response-json", "corr-response"), Times.Once);
        Assert.Contains("responses", client.ResolvedQueueNames);
        Assert.Equal(1, calls.Delete);
    }

    [Fact]
    public async Task WorkerSubscriber_RetriesAfterReceiveFailure()
    {
        var client = new FakeSqsClient { FailuresBeforeReceive = 1 };
        var ingress = new Mock<IAsyncResponseIngress>();
        ingress.Setup(i => i.HandleWorkerMessageAsync("worker-json")).Returns(Task.CompletedTask);
        var calls = new SettlementCalls();
        var subscriber = new SqsWorkerSubscriber(
            Options.Create(new SqsAsyncResponseOptions
            {
                WorkerQueue = "workers",
                ResponseQueue = "responses",
                ReceiveWaitTime = TimeSpan.FromMilliseconds(10),
                SubscriberRetryBaseDelay = TimeSpan.FromMilliseconds(1),
                SubscriberRetryMaxDelay = TimeSpan.FromMilliseconds(1)
            }),
            client,
            ingress.Object,
            NullLogger<SqsWorkerSubscriber>.Instance);

        await subscriber.StartAsync(CancellationToken.None);
        client.Enqueue(Delivery(calls, body: "worker-json"));

        await calls.Deleted.Task.WaitAsync(TimeSpan.FromSeconds(2));
        await subscriber.StopAsync(CancellationToken.None);

        Assert.True(client.ReceiveAttempts >= 2);
        ingress.Verify(i => i.HandleWorkerMessageAsync("worker-json"), Times.Once);
        Assert.Equal(1, calls.Delete);
    }

    [Fact]
    public async Task WorkerSubscriber_PassesReceiveTuningToClient()
    {
        var client = new FakeSqsClient();
        var options = new SqsAsyncResponseOptions
        {
            WorkerQueue = "workers",
            ResponseQueue = "responses",
            MaxMessagesPerReceive = 7,
            ReceiveWaitTime = TimeSpan.FromMilliseconds(10)
        };
        options.WorkerSubscriber.VisibilityTimeout = TimeSpan.FromSeconds(45);
        var calls = new SettlementCalls();
        var subscriber = new SqsWorkerSubscriber(
            Options.Create(options),
            client,
            Mock.Of<IAsyncResponseIngress>(),
            NullLogger<SqsWorkerSubscriber>.Instance);

        await subscriber.StartAsync(CancellationToken.None);
        client.Enqueue(Delivery(calls));
        await calls.Deleted.Task.WaitAsync(TimeSpan.FromSeconds(2));
        await subscriber.StopAsync(CancellationToken.None);

        Assert.Equal(7, client.LastReceiveRequest!.MaxMessages);
        Assert.Equal(TimeSpan.FromMilliseconds(10), client.LastReceiveRequest.WaitTime);
        Assert.Equal(TimeSpan.FromSeconds(45), client.LastReceiveRequest.VisibilityTimeout);
    }

    [Fact]
    public async Task WorkerSubscriber_QueueUrlConfigured_SkipsResolution()
    {
        var client = new FakeSqsClient();
        var queueUrl = "https://sqs.us-east-1.amazonaws.com/000000000000/workers";
        var calls = new SettlementCalls();
        var subscriber = new SqsWorkerSubscriber(
            Options.Create(new SqsAsyncResponseOptions
            {
                WorkerQueue = queueUrl,
                ResponseQueue = "responses",
                ReceiveWaitTime = TimeSpan.FromMilliseconds(10)
            }),
            client,
            Mock.Of<IAsyncResponseIngress>(),
            NullLogger<SqsWorkerSubscriber>.Instance);

        await subscriber.StartAsync(CancellationToken.None);
        client.Enqueue(Delivery(calls));
        await calls.Deleted.Task.WaitAsync(TimeSpan.FromSeconds(2));
        await subscriber.StopAsync(CancellationToken.None);

        Assert.Equal(0, client.GetQueueUrlCalls);
        Assert.Equal(queueUrl, client.LastReceiveRequest!.QueueUrl);
    }

    [Fact]
    public async Task ProvisioningService_Disabled_DoesNothing()
    {
        var client = new FakeSqsClient();
        var service = new SqsQueueProvisioningService(
            Options.Create(new SqsAsyncResponseOptions()),
            client,
            NullLogger<SqsQueueProvisioningService>.Instance);

        await service.StartAsync(CancellationToken.None);
        await service.StopAsync(CancellationToken.None);

        Assert.Empty(client.CreatedQueues);
    }

    [Fact]
    public async Task ProvisioningService_CreatesQueuesAndDeadLettersWithRedrivePolicy()
    {
        var client = new FakeSqsClient();
        var service = new SqsQueueProvisioningService(
            Options.Create(new SqsAsyncResponseOptions
            {
                WorkerQueue = "workers",
                ResponseQueue = "responses",
                CreateQueues = true,
                MaxReceiveCount = 3
            }),
            client,
            NullLogger<SqsQueueProvisioningService>.Instance);

        await service.StartAsync(CancellationToken.None);

        Assert.Equal(
            ["workers-dlq", "workers", "responses-dlq", "responses"],
            client.CreatedQueues.Select(created => created.QueueName));

        var workerQueue = client.CreatedQueues.Single(created => created.QueueName == "workers");
        var redrivePolicy = workerQueue.Attributes[QueueAttributeName.RedrivePolicy];
        Assert.Contains(FakeSqsClient.ArnFor("workers-dlq"), redrivePolicy, StringComparison.Ordinal);
        Assert.Contains("\"maxReceiveCount\":\"3\"", redrivePolicy, StringComparison.Ordinal);
        Assert.False(workerQueue.Attributes.ContainsKey(QueueAttributeName.FifoQueue));
    }

    [Fact]
    public async Task ProvisioningService_FifoQueues_CreateFifoDeadLetterPairs()
    {
        var client = new FakeSqsClient();
        var service = new SqsQueueProvisioningService(
            Options.Create(new SqsAsyncResponseOptions
            {
                WorkerQueue = "workers.fifo",
                ResponseQueue = "responses",
                CreateQueues = true
            }),
            client,
            NullLogger<SqsQueueProvisioningService>.Instance);

        await service.StartAsync(CancellationToken.None);

        var workerDlq = client.CreatedQueues.Single(created => created.QueueName == "workers-dlq.fifo");
        Assert.Equal("true", workerDlq.Attributes[QueueAttributeName.FifoQueue]);

        var workerQueue = client.CreatedQueues.Single(created => created.QueueName == "workers.fifo");
        Assert.Equal("true", workerQueue.Attributes[QueueAttributeName.FifoQueue]);
        Assert.Contains(FakeSqsClient.ArnFor("workers-dlq.fifo"), workerQueue.Attributes[QueueAttributeName.RedrivePolicy], StringComparison.Ordinal);

        // The standard response queue stays standard.
        var responseDlq = client.CreatedQueues.Single(created => created.QueueName == "responses-dlq");
        Assert.False(responseDlq.Attributes.ContainsKey(QueueAttributeName.FifoQueue));
    }

    [Fact]
    public async Task ProvisioningService_SkipsUrlConfiguredQueues()
    {
        var client = new FakeSqsClient();
        var service = new SqsQueueProvisioningService(
            Options.Create(new SqsAsyncResponseOptions
            {
                WorkerQueue = "https://sqs.us-east-1.amazonaws.com/000000000000/workers",
                ResponseQueue = "responses",
                CreateQueues = true
            }),
            client,
            NullLogger<SqsQueueProvisioningService>.Instance);

        await service.StartAsync(CancellationToken.None);

        Assert.Equal(
            ["responses-dlq", "responses"],
            client.CreatedQueues.Select(created => created.QueueName));
    }

    [Fact]
    public async Task ProvisioningService_ExistingQueue_ConvergesAttributes()
    {
        var client = new FakeSqsClient();
        client.ExistingQueues.Add("workers");
        var service = new SqsQueueProvisioningService(
            Options.Create(new SqsAsyncResponseOptions
            {
                WorkerQueue = "workers",
                ResponseQueue = "responses",
                CreateQueues = true
            }),
            client,
            NullLogger<SqsQueueProvisioningService>.Instance);

        await service.StartAsync(CancellationToken.None);

        Assert.DoesNotContain(client.CreatedQueues, created => created.QueueName == "workers");
        var update = Assert.Single(client.AttributeUpdates);
        Assert.Equal(FakeSqsClient.UrlFor("workers"), update.QueueUrl);
        Assert.Contains(QueueAttributeName.RedrivePolicy.Value, update.Attributes.Keys);
    }

    [Fact]
    public async Task ProvisioningService_RetriesTransientFailures()
    {
        var client = new FakeSqsClient { CreateQueueFailuresBeforeSuccess = 2 };
        var service = new SqsQueueProvisioningService(
            Options.Create(new SqsAsyncResponseOptions
            {
                WorkerQueue = "workers",
                ResponseQueue = "responses",
                CreateQueues = true,
                SubscriberRetryBaseDelay = TimeSpan.FromMilliseconds(1),
                SubscriberRetryMaxDelay = TimeSpan.FromMilliseconds(1)
            }),
            client,
            NullLogger<SqsQueueProvisioningService>.Instance);

        await service.StartAsync(CancellationToken.None);

        Assert.Equal(4, client.CreatedQueues.Count);
    }

    [Fact]
    public async Task ClientAdapter_SendMapsAttributesAndFifoFields()
    {
        var sdkClient = new Mock<IAmazonSQS>();
        SendMessageRequest? captured = null;
        sdkClient
            .Setup(c => c.SendMessageAsync(It.IsAny<SendMessageRequest>(), It.IsAny<CancellationToken>()))
            .Callback<SendMessageRequest, CancellationToken>((request, _) => captured = request)
            .ReturnsAsync(new SendMessageResponse { MessageId = "sent-1" });
        var adapter = new SqsClientAdapter(sdkClient.Object, ownsClient: false);

        var messageId = await adapter.SendMessageAsync(new SqsOutboundMessage(
            "https://queue-url",
            """{"ok":true}""",
            "corr-1",
            "group-1",
            "dedup-1",
            new Dictionary<string, string> { ["cid"] = "corr-1", ["tenant"] = "acme" }));

        Assert.Equal("sent-1", messageId);
        Assert.NotNull(captured);
        Assert.Equal("https://queue-url", captured.QueueUrl);
        Assert.Equal("""{"ok":true}""", captured.MessageBody);
        Assert.Equal("group-1", captured.MessageGroupId);
        Assert.Equal("dedup-1", captured.MessageDeduplicationId);
        Assert.Equal("String", captured.MessageAttributes!["cid"].DataType);
        Assert.Equal("corr-1", captured.MessageAttributes["cid"].StringValue);
        Assert.Equal("acme", captured.MessageAttributes["tenant"].StringValue);
    }

    [Fact]
    public async Task ClientAdapter_ReceiveMapsDeliveriesAndDelegatesSettlement()
    {
        var sdkClient = new Mock<IAmazonSQS>();
        ReceiveMessageRequest? captured = null;
        sdkClient
            .Setup(c => c.ReceiveMessageAsync(It.IsAny<ReceiveMessageRequest>(), It.IsAny<CancellationToken>()))
            .Callback<ReceiveMessageRequest, CancellationToken>((request, _) => captured = request)
            .ReturnsAsync(new ReceiveMessageResponse
            {
                Messages =
                [
                    new Message
                    {
                        MessageId = "message-1",
                        Body = """{"Status":2}""",
                        ReceiptHandle = "receipt-1",
                        Attributes = new Dictionary<string, string> { ["ApproximateReceiveCount"] = "3" },
                        MessageAttributes = new Dictionary<string, MessageAttributeValue>
                        {
                            ["cid"] = new() { DataType = "String", StringValue = "corr-1" }
                        }
                    }
                ]
            });
        sdkClient
            .Setup(c => c.DeleteMessageAsync("https://queue-url", "receipt-1", It.IsAny<CancellationToken>()))
            .ReturnsAsync(new DeleteMessageResponse());
        sdkClient
            .Setup(c => c.ChangeMessageVisibilityAsync("https://queue-url", "receipt-1", 30, It.IsAny<CancellationToken>()))
            .ReturnsAsync(new ChangeMessageVisibilityResponse());
        var adapter = new SqsClientAdapter(sdkClient.Object, ownsClient: false);

        var deliveries = await adapter.ReceiveMessagesAsync(new SqsReceiveRequest(
            "https://queue-url",
            MaxMessages: 5,
            WaitTime: TimeSpan.FromSeconds(20),
            VisibilityTimeout: TimeSpan.FromSeconds(90)));
        var delivery = Assert.Single(deliveries);
        await delivery.DeleteAsync();
        await delivery.ChangeVisibilityAsync(TimeSpan.FromSeconds(30));

        Assert.NotNull(captured);
        Assert.Equal(5, captured.MaxNumberOfMessages);
        Assert.Equal(20, captured.WaitTimeSeconds);
        Assert.Equal(90, captured.VisibilityTimeout);
        Assert.Contains("All", (IEnumerable<string>)captured.MessageAttributeNames!);

        Assert.Equal("https://queue-url", delivery.QueueUrl);
        Assert.Equal("""{"Status":2}""", delivery.Body);
        Assert.Equal("message-1", delivery.MessageId);
        Assert.Equal("receipt-1", delivery.ReceiptHandle);
        Assert.Equal(3, delivery.ReceiveCount);
        Assert.Equal("corr-1", delivery.MessageAttributes["cid"]);
        sdkClient.Verify(c => c.DeleteMessageAsync("https://queue-url", "receipt-1", It.IsAny<CancellationToken>()), Times.Once);
        sdkClient.Verify(c => c.ChangeMessageVisibilityAsync("https://queue-url", "receipt-1", 30, It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task ClientAdapter_ReceiveWithoutMessages_ReturnsEmpty()
    {
        var sdkClient = new Mock<IAmazonSQS>();
        sdkClient
            .Setup(c => c.ReceiveMessageAsync(It.IsAny<ReceiveMessageRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new ReceiveMessageResponse { Messages = null });
        var adapter = new SqsClientAdapter(sdkClient.Object, ownsClient: false);

        var deliveries = await adapter.ReceiveMessagesAsync(new SqsReceiveRequest(
            "https://queue-url",
            MaxMessages: 1,
            WaitTime: TimeSpan.FromMilliseconds(1),
            VisibilityTimeout: null));

        Assert.Empty(deliveries);
    }

    [Fact]
    public async Task ClientAdapter_ReceiveDefaultsMalformedOptionalMessageMetadata()
    {
        var sdkClient = new Mock<IAmazonSQS>();
        sdkClient
            .Setup(c => c.ReceiveMessageAsync(It.IsAny<ReceiveMessageRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new ReceiveMessageResponse
            {
                Messages =
                [
                    new Message
                    {
                        Attributes = new Dictionary<string, string>
                        {
                            [MessageSystemAttributeName.ApproximateReceiveCount] = "not-a-number"
                        },
                        MessageAttributes = new Dictionary<string, MessageAttributeValue>
                        {
                            ["missing"] = null!,
                            ["binary"] = new() { DataType = "Binary" }
                        }
                    }
                ]
            });
        var adapter = new SqsClientAdapter(sdkClient.Object, ownsClient: false);

        var delivery = Assert.Single(await adapter.ReceiveMessagesAsync(new SqsReceiveRequest(
            "queue-url", 1, TimeSpan.Zero, null)));

        Assert.Equal(1, delivery.ReceiveCount);
        Assert.Equal(string.Empty, delivery.Body);
        Assert.Equal(string.Empty, delivery.MessageId);
        Assert.Empty(delivery.MessageAttributes);
    }

    [Fact]
    public async Task ClientAdapter_ProvisioningCallsMapToSdkRequests()
    {
        var sdkClient = new Mock<IAmazonSQS>();
        CreateQueueRequest? createRequest = null;
        SetQueueAttributesRequest? setRequest = null;
        sdkClient
            .Setup(c => c.CreateQueueAsync(It.IsAny<CreateQueueRequest>(), It.IsAny<CancellationToken>()))
            .Callback<CreateQueueRequest, CancellationToken>((request, _) => createRequest = request)
            .ReturnsAsync(new CreateQueueResponse { QueueUrl = "https://queue-url" });
        sdkClient
            .Setup(c => c.GetQueueUrlAsync("workers", It.IsAny<CancellationToken>()))
            .ReturnsAsync(new GetQueueUrlResponse { QueueUrl = "https://queue-url" });
        sdkClient
            .Setup(c => c.GetQueueAttributesAsync(It.IsAny<GetQueueAttributesRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new GetQueueAttributesResponse
            {
                Attributes = new Dictionary<string, string> { ["QueueArn"] = "arn:aws:sqs:us-east-1:1:workers" }
            });
        sdkClient
            .Setup(c => c.SetQueueAttributesAsync(It.IsAny<SetQueueAttributesRequest>(), It.IsAny<CancellationToken>()))
            .Callback<SetQueueAttributesRequest, CancellationToken>((request, _) => setRequest = request)
            .ReturnsAsync(new SetQueueAttributesResponse());
        var adapter = new SqsClientAdapter(sdkClient.Object, ownsClient: false);

        var queueUrl = await adapter.CreateQueueAsync(
            "workers",
            new Dictionary<string, string> { ["FifoQueue"] = "true" });
        var resolvedUrl = await adapter.GetQueueUrlAsync("workers");
        var arn = await adapter.GetQueueArnAsync("https://queue-url");
        await adapter.SetQueueAttributesAsync(
            "https://queue-url",
            new Dictionary<string, string> { ["RedrivePolicy"] = "{}" });

        Assert.Equal("https://queue-url", queueUrl);
        Assert.Equal("https://queue-url", resolvedUrl);
        Assert.Equal("arn:aws:sqs:us-east-1:1:workers", arn);
        Assert.Equal("workers", createRequest!.QueueName);
        Assert.Equal("true", createRequest.Attributes!["FifoQueue"]);
        Assert.Equal("{}", setRequest!.Attributes!["RedrivePolicy"]);
    }

    [Fact]
    public async Task ClientAdapter_DisposesOwnedSdkClientOnly()
    {
        var ownedClient = new Mock<IAmazonSQS>();
        await new SqsClientAdapter(ownedClient.Object, ownsClient: true).DisposeAsync();
        ownedClient.Verify(c => c.Dispose(), Times.Once);

        var externalClient = new Mock<IAmazonSQS>();
        await new SqsClientAdapter(externalClient.Object, ownsClient: false).DisposeAsync();
        externalClient.Verify(c => c.Dispose(), Times.Never);
    }

    private static SqsTransportDelivery Delivery(
        SettlementCalls calls,
        string body = "{}",
        string messageId = "message-id",
        int receiveCount = 1,
        IReadOnlyDictionary<string, string>? attributes = null)
        => new(
            "https://sqs.test.local/000000000000/queue",
            body,
            messageId,
            "receipt-handle",
            receiveCount,
            attributes ?? new Dictionary<string, string>(StringComparer.Ordinal),
            () =>
            {
                calls.Delete++;
                calls.Deleted.TrySetResult();
                return ValueTask.CompletedTask;
            },
            delay =>
            {
                calls.VisibilityChanges.Add(delay);
                return ValueTask.CompletedTask;
            });

    private sealed class SettlementCalls
    {
        public int Delete;
        public List<TimeSpan> VisibilityChanges { get; } = [];
        public TaskCompletionSource Deleted { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }
}
