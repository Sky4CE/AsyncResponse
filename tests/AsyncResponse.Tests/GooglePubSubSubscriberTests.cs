using AsyncResponse.Transports.GooglePubSub;
using Google.Cloud.PubSub.V1;
using Google.Protobuf;
using Google.Api.Gax.ResourceNames;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using System.Reflection;
using Xunit;

namespace AsyncResponse.Tests;

public class GooglePubSubSubscriberTests
{
    [Fact]
    public async Task WorkerSubscriberService_AcksHandledMessagesAndStopsSubscriber()
    {
        var client = new FakeSubscriberClient();
        var subscriptionNames = new List<SubscriptionName>();
        var ingress = new Mock<IAsyncResponseIngress>();
        var options = new GooglePubSubAsyncResponseOptions
        {
            ProjectId = "project-a",
            WorkerSubscriptionId = "workers",
            ShutdownTimeout = TimeSpan.FromMilliseconds(250)
        };
        var subscriber = new GooglePubSubWorkerSubscriber(
            Options.Create(options),
            ingress.Object,
            NullLogger<GooglePubSubWorkerSubscriber>.Instance,
            subscriptionName =>
            {
                subscriptionNames.Add(subscriptionName);
                return Task.FromResult<IGooglePubSubSubscriberClient>(client);
            });

        await subscriber.StartAsync(CancellationToken.None);
        var handler = await WaitForHandlerAsync(client);
        var message = new PubsubMessage
        {
            MessageId = "message-1",
            Data = ByteString.CopyFromUtf8("""{"Call":{"MethodName":"DoWork"}}""")
        };

        var reply = await handler(message, CancellationToken.None);
        await subscriber.StopAsync(CancellationToken.None);

        Assert.Equal(SubscriberClient.Reply.Ack, reply);
        ingress.Verify(i => i.HandleWorkerMessageAsync(message.Data.ToStringUtf8()), Times.Once);
        Assert.Equal("projects/project-a/subscriptions/workers", Assert.Single(subscriptionNames).ToString());
        Assert.Equal(1, client.StartCalls);
        Assert.Equal(1, client.StopCalls);
        Assert.Equal(options.ShutdownTimeout, client.LastShutdownOptions?.Timeout);
    }

    [Fact]
    public async Task WorkerSubscriberService_WhenHandlerFails_NacksMessage()
    {
        var client = new FakeSubscriberClient();
        var ingress = new Mock<IAsyncResponseIngress>();
        ingress
            .Setup(i => i.HandleWorkerMessageAsync(It.IsAny<string>()))
            .ThrowsAsync(new InvalidOperationException("cannot handle"));
        var subscriber = new GooglePubSubWorkerSubscriber(
            Options.Create(new GooglePubSubAsyncResponseOptions
            {
                ProjectId = "project-a",
                WorkerSubscriptionId = "workers"
            }),
            ingress.Object,
            NullLogger<GooglePubSubWorkerSubscriber>.Instance,
            _ => Task.FromResult<IGooglePubSubSubscriberClient>(client));

        await subscriber.StartAsync(CancellationToken.None);
        var handler = await WaitForHandlerAsync(client);

        var reply = await handler(new PubsubMessage
        {
            MessageId = "message-2",
            Data = ByteString.CopyFromUtf8("{}")
        }, CancellationToken.None);
        await subscriber.StopAsync(CancellationToken.None);

        Assert.Equal(SubscriberClient.Reply.Nack, reply);
    }

    [Fact]
    public async Task ResponseSubscriberService_ForwardsExtractedCorrelation()
    {
        var client = new FakeSubscriberClient();
        var ingress = new Mock<IAsyncResponseIngress>();
        var subscriber = new GooglePubSubResponseIngressSubscriber(
            Options.Create(new GooglePubSubAsyncResponseOptions
            {
                ProjectId = "project-a",
                ResponseSubscriptionId = "responses"
            }),
            ingress.Object,
            NullLogger<GooglePubSubResponseIngressSubscriber>.Instance,
            _ => Task.FromResult<IGooglePubSubSubscriberClient>(client));
        var message = new PubsubMessage
        {
            MessageId = "message-3",
            Data = ByteString.CopyFromUtf8("""{"CorrelationId":"from-json","Status":2}""")
        };

        await subscriber.StartAsync(CancellationToken.None);
        var handler = await WaitForHandlerAsync(client);
        var reply = await handler(message, CancellationToken.None);
        await subscriber.StopAsync(CancellationToken.None);

        Assert.Equal(SubscriberClient.Reply.Ack, reply);
        ingress.Verify(i => i.HandleResponseMessageAsync(message.Data.ToStringUtf8(), "from-json"), Times.Once);
    }

    [Fact]
    public async Task SubscriberClientAdapter_DelegatesStartAndStop()
    {
        Func<PubsubMessage, CancellationToken, Task<SubscriberClient.Reply>> handler =
            (_, _) => Task.FromResult(SubscriberClient.Reply.Ack);
        var shutdownOptions = new SubscriberClient.ShutdownOptions
        {
            Timeout = TimeSpan.FromSeconds(1)
        };
        using var cts = new CancellationTokenSource();
        var subscriber = new Mock<SubscriberClient>();
        subscriber.Setup(s => s.StartAsync(handler)).Returns(Task.CompletedTask);
        subscriber.Setup(s => s.StopAsync(shutdownOptions, cts.Token)).Returns(Task.CompletedTask);
        var adapter = new GooglePubSubSubscriberClientAdapter(subscriber.Object);

        await adapter.StartAsync(handler);
        await adapter.StopAsync(shutdownOptions, cts.Token);

        subscriber.Verify(s => s.StartAsync(handler), Times.Once);
        subscriber.Verify(s => s.StopAsync(shutdownOptions, cts.Token), Times.Once);
    }

    [Fact]
    public async Task SubscriberService_WhenProjectIdMissing_FailsBeforeCreatingClient()
    {
        var factoryCalled = false;
        var subscriber = new GooglePubSubWorkerSubscriber(
            Options.Create(new GooglePubSubAsyncResponseOptions { WorkerSubscriptionId = "workers" }),
            Mock.Of<IAsyncResponseIngress>(),
            NullLogger<GooglePubSubWorkerSubscriber>.Instance,
            _ =>
            {
                factoryCalled = true;
                return Task.FromResult<IGooglePubSubSubscriberClient>(new FakeSubscriberClient());
            });

        await Assert.ThrowsAsync<InvalidOperationException>(() => InvokeExecuteAsync(subscriber, CancellationToken.None));

        Assert.False(factoryCalled);
    }

    [Fact]
    public async Task WorkerSubscriber_ForwardsMessageBodyToWorkerIngress()
    {
        var ingress = new Mock<IAsyncResponseIngress>();
        var subscriber = new GooglePubSubWorkerSubscriber(
            Options.Create(new GooglePubSubAsyncResponseOptions
            {
                ProjectId = "project-a",
                WorkerSubscriptionId = "workers"
            }),
            ingress.Object,
            NullLogger<GooglePubSubWorkerSubscriber>.Instance);
        var message = new PubsubMessage
        {
            Data = ByteString.CopyFromUtf8("""{"Call":{"MethodName":"DoWork"}}""")
        };

        await InvokeHandleMessageAsync(subscriber, message);

        ingress.Verify(i => i.HandleWorkerMessageAsync(message.Data.ToStringUtf8()), Times.Once);
        Assert.Equal("workers", GetSubscriptionId(subscriber));
    }

    [Fact]
    public async Task ResponseSubscriber_ExtractsCorrelationAndForwardsMessageBody()
    {
        var ingress = new Mock<IAsyncResponseIngress>();
        var subscriber = new GooglePubSubResponseIngressSubscriber(
            Options.Create(new GooglePubSubAsyncResponseOptions
            {
                ProjectId = "project-a",
                ResponseSubscriptionId = "responses"
            }),
            ingress.Object,
            NullLogger<GooglePubSubResponseIngressSubscriber>.Instance);
        var message = new PubsubMessage
        {
            Data = ByteString.CopyFromUtf8("""{"CorrelationId":"from-json","Status":2}""")
        };
        message.Attributes["correlationId"] = "from-attribute";

        await InvokeHandleMessageAsync(subscriber, message);

        ingress.Verify(i => i.HandleResponseMessageAsync(message.Data.ToStringUtf8(), "from-attribute"), Times.Once);
        Assert.Equal("responses", GetSubscriptionId(subscriber));
    }

    [Fact]
    public void SubscriberSubscriptionId_RequiresConfiguredValue()
    {
        var ingress = Mock.Of<IAsyncResponseIngress>();
        var subscriber = new GooglePubSubWorkerSubscriber(
            Options.Create(new GooglePubSubAsyncResponseOptions { ProjectId = "project-a" }),
            ingress,
            NullLogger<GooglePubSubWorkerSubscriber>.Instance);

        var ex = Assert.Throws<TargetInvocationException>(() => GetSubscriptionId(subscriber));
        Assert.IsType<InvalidOperationException>(ex.InnerException);
    }

    private static Task InvokeHandleMessageAsync(object subscriber, PubsubMessage message)
    {
        var method = subscriber.GetType().GetMethod(
            "HandleMessageAsync",
            BindingFlags.Instance | BindingFlags.NonPublic)!;
        return (Task)method.Invoke(subscriber, [message, CancellationToken.None])!;
    }

    private static Task InvokeExecuteAsync(object subscriber, CancellationToken cancellationToken)
    {
        var method = typeof(GooglePubSubSubscriberService).GetMethod(
            "ExecuteAsync",
            BindingFlags.Instance | BindingFlags.NonPublic)!;
        return (Task)method.Invoke(subscriber, [cancellationToken])!;
    }

    private static string GetSubscriptionId(object subscriber)
    {
        var property = subscriber.GetType().GetProperty(
            "SubscriptionId",
            BindingFlags.Instance | BindingFlags.NonPublic)!;
        return (string)property.GetValue(subscriber)!;
    }

    private static async Task<Func<PubsubMessage, CancellationToken, Task<SubscriberClient.Reply>>> WaitForHandlerAsync(
        FakeSubscriberClient client)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));

        while (client.Handler is null)
            await Task.Delay(10, cts.Token);

        return client.Handler;
    }

    private sealed class FakeSubscriberClient : IGooglePubSubSubscriberClient
    {
        private readonly TaskCompletionSource _run = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Func<PubsubMessage, CancellationToken, Task<SubscriberClient.Reply>>? Handler { get; private set; }
        public int StartCalls { get; private set; }
        public int StopCalls { get; private set; }
        public SubscriberClient.ShutdownOptions? LastShutdownOptions { get; private set; }

        public Task StartAsync(Func<PubsubMessage, CancellationToken, Task<SubscriberClient.Reply>> handler)
        {
            StartCalls++;
            Handler = handler;
            return _run.Task;
        }

        public Task StopAsync(SubscriberClient.ShutdownOptions options, CancellationToken cancellationToken)
        {
            StopCalls++;
            LastShutdownOptions = options;
            _run.TrySetResult();
            return Task.CompletedTask;
        }
    }
}
