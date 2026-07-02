using AsyncResponse.Transports.GooglePubSub;
using Google.Cloud.PubSub.V1;
using Google.Protobuf;
using Google.Api.Gax.ResourceNames;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using System.Diagnostics;
using System.Reflection;
using Xunit;

namespace AsyncResponse.Tests;

public class GooglePubSubSubscriberTests
{
    [Fact]
    public async Task WorkerSubscriberService_DefaultAckMode_WaitsForHandlerBeforeAck()
    {
        var client = new FakeSubscriberClient();
        var handlerStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseHandler = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var ingress = new Mock<IAsyncResponseIngress>();
        ingress
            .Setup(i => i.HandleWorkerMessageAsync(It.IsAny<string>()))
            .Returns(async () =>
            {
                handlerStarted.TrySetResult();
                await releaseHandler.Task.ConfigureAwait(false);
            });
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
        var replyTask = handler(new PubsubMessage
        {
            MessageId = "message-await",
            Data = ByteString.CopyFromUtf8("{}")
        }, CancellationToken.None);

        await handlerStarted.Task.WaitAsync(TimeSpan.FromSeconds(2));
        Assert.False(replyTask.IsCompleted);

        releaseHandler.TrySetResult();
        Assert.Equal(SubscriberClient.Reply.Ack, await replyTask.WaitAsync(TimeSpan.FromSeconds(2)));
        await subscriber.StopAsync(CancellationToken.None);
    }

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
    public async Task WorkerSubscriberService_AckAfterEnqueue_AcksBeforeHandlerCompletes()
    {
        var client = new FakeSubscriberClient();
        var handlerStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handlerCompleted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseHandler = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var ingress = new Mock<IAsyncResponseIngress>();
        ingress
            .Setup(i => i.HandleWorkerMessageAsync(It.IsAny<string>()))
            .Returns(async () =>
            {
                handlerStarted.TrySetResult();
                await releaseHandler.Task.ConfigureAwait(false);
                handlerCompleted.TrySetResult();
            });
        var options = new GooglePubSubAsyncResponseOptions
        {
            ProjectId = "project-a",
            WorkerSubscriptionId = "workers",
            ShutdownTimeout = TimeSpan.FromMilliseconds(250)
        };
        options.WorkerSubscriber.UseAckAfterEnqueue(
            backgroundWorkerCount: 1,
            backgroundQueueCapacity: 8,
            backgroundDrainTimeout: TimeSpan.FromSeconds(5));
        var subscriber = new GooglePubSubWorkerSubscriber(
            Options.Create(options),
            ingress.Object,
            NullLogger<GooglePubSubWorkerSubscriber>.Instance,
            _ => Task.FromResult<IGooglePubSubSubscriberClient>(client));

        await subscriber.StartAsync(CancellationToken.None);
        var handler = await WaitForHandlerAsync(client);

        var reply = await handler(new PubsubMessage
        {
            MessageId = "message-early-ack",
            Data = ByteString.CopyFromUtf8("{}")
        }, CancellationToken.None);

        Assert.Equal(SubscriberClient.Reply.Ack, reply);
        await handlerStarted.Task.WaitAsync(TimeSpan.FromSeconds(2));
        Assert.False(handlerCompleted.Task.IsCompleted);

        releaseHandler.TrySetResult();
        await handlerCompleted.Task.WaitAsync(TimeSpan.FromSeconds(2));
        await subscriber.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task WorkerSubscriberService_AckAfterEnqueue_BackgroundFailureKeepsAck()
    {
        var client = new FakeSubscriberClient();
        var handlerStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var failureReported = new TaskCompletionSource<GooglePubSubBackgroundFailureContext>(TaskCreationOptions.RunContinuationsAsynchronously);
        var ingress = new Mock<IAsyncResponseIngress>();
        ingress
            .Setup(i => i.HandleWorkerMessageAsync(It.IsAny<string>()))
            .Returns(() =>
            {
                handlerStarted.TrySetResult();
                return Task.FromException(new InvalidOperationException("background-failure"));
            });
        var options = new GooglePubSubAsyncResponseOptions
        {
            ProjectId = "project-a",
            WorkerSubscriptionId = "workers"
        };
        options.WorkerSubscriber.UseAckAfterEnqueue(
            backgroundWorkerCount: 1,
            backgroundQueueCapacity: 8,
            backgroundDrainTimeout: TimeSpan.FromSeconds(5));
        options.WorkerSubscriber.OnBackgroundFailure = context =>
        {
            failureReported.TrySetResult(context);
            return ValueTask.CompletedTask;
        };
        var subscriber = new GooglePubSubWorkerSubscriber(
            Options.Create(options),
            ingress.Object,
            NullLogger<GooglePubSubWorkerSubscriber>.Instance,
            _ => Task.FromResult<IGooglePubSubSubscriberClient>(client));

        await subscriber.StartAsync(CancellationToken.None);
        var handler = await WaitForHandlerAsync(client);

        var reply = await handler(new PubsubMessage
        {
            MessageId = "message-background-failure",
            Data = ByteString.CopyFromUtf8("{}")
        }, CancellationToken.None);

        Assert.Equal(SubscriberClient.Reply.Ack, reply);
        await handlerStarted.Task.WaitAsync(TimeSpan.FromSeconds(2));
        var failure = await failureReported.Task.WaitAsync(TimeSpan.FromSeconds(2));
        Assert.Equal("workers", failure.SubscriptionId);
        Assert.Equal("Worker", failure.SubscriberRole);
        Assert.Equal("message-background-failure", failure.MessageId);
        Assert.IsType<InvalidOperationException>(failure.Exception);
        await subscriber.StopAsync(CancellationToken.None);
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
    public async Task ResponseSubscriberService_AckAfterEnqueue_AcksBeforeIngressCompletes()
    {
        var client = new FakeSubscriberClient();
        var handlerStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseHandler = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var ingress = new Mock<IAsyncResponseIngress>();
        ingress
            .Setup(i => i.HandleResponseMessageAsync(It.IsAny<string>(), It.IsAny<string?>()))
            .Returns(async () =>
            {
                handlerStarted.TrySetResult();
                await releaseHandler.Task.ConfigureAwait(false);
            });
        var options = new GooglePubSubAsyncResponseOptions
        {
            ProjectId = "project-a",
            ResponseSubscriptionId = "responses"
        };
        options.ResponseSubscriber.UseAckAfterEnqueue(
            backgroundWorkerCount: 1,
            backgroundQueueCapacity: 8,
            backgroundDrainTimeout: TimeSpan.FromSeconds(5));
        var subscriber = new GooglePubSubResponseIngressSubscriber(
            Options.Create(options),
            ingress.Object,
            NullLogger<GooglePubSubResponseIngressSubscriber>.Instance,
            _ => Task.FromResult<IGooglePubSubSubscriberClient>(client));
        var message = new PubsubMessage
        {
            MessageId = "message-response-early-ack",
            Data = ByteString.CopyFromUtf8("""{"CorrelationId":"from-json","Status":2}""")
        };

        await subscriber.StartAsync(CancellationToken.None);
        var handler = await WaitForHandlerAsync(client);

        var reply = await handler(message, CancellationToken.None);

        Assert.Equal(SubscriberClient.Reply.Ack, reply);
        await handlerStarted.Task.WaitAsync(TimeSpan.FromSeconds(2));
        ingress.Verify(i => i.HandleResponseMessageAsync(message.Data.ToStringUtf8(), "from-json"), Times.Once);

        releaseHandler.TrySetResult();
        await subscriber.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task WorkerSubscriberService_AckAfterEnqueue_StopWaitsForQueuedWorkToDrain()
    {
        var client = new FakeSubscriberClient();
        var handlerStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseHandler = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var ingress = new Mock<IAsyncResponseIngress>();
        ingress
            .Setup(i => i.HandleWorkerMessageAsync(It.IsAny<string>()))
            .Returns(async () =>
            {
                handlerStarted.TrySetResult();
                await releaseHandler.Task.ConfigureAwait(false);
            });
        var options = new GooglePubSubAsyncResponseOptions
        {
            ProjectId = "project-a",
            WorkerSubscriptionId = "workers",
            ShutdownTimeout = TimeSpan.FromMilliseconds(250)
        };
        options.WorkerSubscriber.UseAckAfterEnqueue(
            backgroundWorkerCount: 1,
            backgroundQueueCapacity: 8,
            backgroundDrainTimeout: TimeSpan.FromSeconds(5));
        var subscriber = new GooglePubSubWorkerSubscriber(
            Options.Create(options),
            ingress.Object,
            NullLogger<GooglePubSubWorkerSubscriber>.Instance,
            _ => Task.FromResult<IGooglePubSubSubscriberClient>(client));

        await subscriber.StartAsync(CancellationToken.None);
        var handler = await WaitForHandlerAsync(client);
        var reply = await handler(new PubsubMessage
        {
            MessageId = "message-drain",
            Data = ByteString.CopyFromUtf8("{}")
        }, CancellationToken.None);
        await handlerStarted.Task.WaitAsync(TimeSpan.FromSeconds(2));

        var stopTask = subscriber.StopAsync(CancellationToken.None);
        await Task.Delay(50);

        Assert.Equal(SubscriberClient.Reply.Ack, reply);
        Assert.False(stopTask.IsCompleted);

        releaseHandler.TrySetResult();
        await stopTask.WaitAsync(TimeSpan.FromSeconds(2));
    }

    [Fact]
    public async Task WorkerSubscriberService_AckAfterEnqueue_NacksWhenBackgroundQueueIsFull()
    {
        var firstStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var calls = 0;
        await using var dispatcher = GooglePubSubMessageDispatcher.Create(
            async (_, _) =>
            {
                var call = Interlocked.Increment(ref calls);
                if (call == 1)
                {
                    firstStarted.TrySetResult();
                    await releaseFirst.Task.ConfigureAwait(false);
                }
                else if (call == 2)
                {
                    secondStarted.TrySetResult();
                }
            },
            new GooglePubSubAsyncResponseOptions(),
            new GooglePubSubSubscriberOptions().UseAckAfterEnqueue(
                backgroundWorkerCount: 1,
                backgroundQueueCapacity: 1,
                backgroundDrainTimeout: TimeSpan.FromSeconds(5)),
            NullLogger.Instance,
            "workers",
            GooglePubSubSubscriberRole.Worker);

        Assert.Equal(SubscriberClient.Reply.Ack, await dispatcher.HandleAsync(Message("message-1"), CancellationToken.None));
        await firstStarted.Task.WaitAsync(TimeSpan.FromSeconds(2));

        Assert.Equal(SubscriberClient.Reply.Ack, await dispatcher.HandleAsync(Message("message-2"), CancellationToken.None));
        Assert.Equal(SubscriberClient.Reply.Nack, await dispatcher.HandleAsync(Message("message-3"), CancellationToken.None));
        Assert.Equal(1, Volatile.Read(ref calls));

        releaseFirst.TrySetResult();
        await secondStarted.Task.WaitAsync(TimeSpan.FromSeconds(2));
        Assert.Equal(2, Volatile.Read(ref calls));
    }

    [Fact]
    public async Task Dispatcher_AckAfterEnqueue_BackgroundFailureLogsOnceAndInvokesHook()
    {
        var logger = new ListLogger();
        var failureReported = new TaskCompletionSource<GooglePubSubBackgroundFailureContext>(TaskCreationOptions.RunContinuationsAsynchronously);
        var subscriberOptions = new GooglePubSubSubscriberOptions().UseAckAfterEnqueue(
            backgroundWorkerCount: 1,
            backgroundQueueCapacity: 8,
            backgroundDrainTimeout: TimeSpan.FromSeconds(5));
        subscriberOptions.OnBackgroundFailure = context =>
        {
            failureReported.TrySetResult(context);
            return ValueTask.CompletedTask;
        };

        await using var dispatcher = GooglePubSubMessageDispatcher.Create(
            (_, _) => Task.FromException(new InvalidOperationException("queued-handler-failure")),
            new GooglePubSubAsyncResponseOptions(),
            subscriberOptions,
            logger,
            "workers",
            GooglePubSubSubscriberRole.Worker);

        var reply = await dispatcher.HandleAsync(Message("message-logged-once"), CancellationToken.None);

        Assert.Equal(SubscriberClient.Reply.Ack, reply);
        await failureReported.Task.WaitAsync(TimeSpan.FromSeconds(2));
        var handlerFailureLogs = logger.Entries
            .Where(entry => entry.Level == LogLevel.Error)
            .Where(entry => entry.Exception is InvalidOperationException)
            .ToList();
        Assert.Single(handlerFailureLogs);
        Assert.Contains("already-ACKed message", handlerFailureLogs[0].Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Dispatcher_AckAfterEnqueue_CancelsRunningHandlerAfterDrainTimeout()
    {
        var handlerStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var cancellationObserved = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var dispatcher = GooglePubSubMessageDispatcher.Create(
            async (_, cancellationToken) =>
            {
                handlerStarted.TrySetResult();
                try
                {
                    await Task.Delay(TimeSpan.FromSeconds(30), cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    cancellationObserved.TrySetResult();
                    throw;
                }
            },
            new GooglePubSubAsyncResponseOptions(),
            new GooglePubSubSubscriberOptions().UseAckAfterEnqueue(
                backgroundWorkerCount: 1,
                backgroundQueueCapacity: 8,
                backgroundDrainTimeout: TimeSpan.FromMilliseconds(50)),
            NullLogger.Instance,
            "workers",
            GooglePubSubSubscriberRole.Worker);

        try
        {
            Assert.Equal(SubscriberClient.Reply.Ack, await dispatcher.HandleAsync(Message("message-cancelled"), CancellationToken.None));
            await handlerStarted.Task.WaitAsync(TimeSpan.FromSeconds(2));

            var elapsed = Stopwatch.StartNew();
            await dispatcher.DisposeAsync();
            elapsed.Stop();

            await cancellationObserved.Task.WaitAsync(TimeSpan.FromSeconds(2));
            Assert.True(elapsed.Elapsed >= TimeSpan.FromMilliseconds(40));
            Assert.True(elapsed.Elapsed < TimeSpan.FromSeconds(2));
        }
        finally
        {
            await dispatcher.DisposeAsync();
        }
    }

    [Fact]
    public async Task SubscriberService_WhenSubscriberFactoryFails_RetriesWithBackoff()
    {
        var client = new FakeSubscriberClient();
        var attempts = 0;
        var subscriber = new GooglePubSubWorkerSubscriber(
            Options.Create(new GooglePubSubAsyncResponseOptions
            {
                ProjectId = "project-a",
                WorkerSubscriptionId = "workers",
                SubscriberRetryBaseDelay = TimeSpan.FromMilliseconds(1),
                SubscriberRetryMaxDelay = TimeSpan.FromMilliseconds(1)
            }),
            Mock.Of<IAsyncResponseIngress>(),
            NullLogger<GooglePubSubWorkerSubscriber>.Instance,
            _ => Interlocked.Increment(ref attempts) == 1
                ? Task.FromException<IGooglePubSubSubscriberClient>(new InvalidOperationException("pubsub unreachable"))
                : Task.FromResult<IGooglePubSubSubscriberClient>(client));

        await subscriber.StartAsync(CancellationToken.None);
        // Reaching a live handler proves the loop retried past the failed factory attempt instead of
        // letting the fault stop the hosted service.
        var handler = await WaitForHandlerAsync(client);
        var reply = await handler(new PubsubMessage
        {
            MessageId = "message-after-retry",
            Data = ByteString.CopyFromUtf8("{}")
        }, CancellationToken.None);
        await subscriber.StopAsync(CancellationToken.None);

        Assert.True(Volatile.Read(ref attempts) >= 2);
        Assert.Equal(SubscriberClient.Reply.Ack, reply);
    }

    [Fact]
    public async Task SubscriberService_WhenStreamingPullFaults_RestartsSubscriber()
    {
        var client = new FakeSubscriberClient();
        var clients = 0;
        var subscriber = new GooglePubSubWorkerSubscriber(
            Options.Create(new GooglePubSubAsyncResponseOptions
            {
                ProjectId = "project-a",
                WorkerSubscriptionId = "workers",
                SubscriberRetryBaseDelay = TimeSpan.FromMilliseconds(1),
                SubscriberRetryMaxDelay = TimeSpan.FromMilliseconds(1)
            }),
            Mock.Of<IAsyncResponseIngress>(),
            NullLogger<GooglePubSubWorkerSubscriber>.Instance,
            _ => Interlocked.Increment(ref clients) == 1
                ? Task.FromResult<IGooglePubSubSubscriberClient>(new FaultingSubscriberClient())
                : Task.FromResult<IGooglePubSubSubscriberClient>(client));

        await subscriber.StartAsync(CancellationToken.None);
        // A faulted streaming pull must restart the subscriber, not stop the hosted service.
        var handler = await WaitForHandlerAsync(client);
        await subscriber.StopAsync(CancellationToken.None);

        Assert.NotNull(handler);
        Assert.Equal(2, Volatile.Read(ref clients));
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
    public async Task SubscriberService_AckAfterEnqueueWithoutExplicitBackgroundSettings_FailsBeforeCreatingClient()
    {
        var factoryCalled = false;
        var options = new GooglePubSubAsyncResponseOptions
        {
            ProjectId = "project-a",
            WorkerSubscriptionId = "workers"
        };
        options.WorkerSubscriber.AckMode = GooglePubSubAckMode.AckAfterEnqueue;
        var subscriber = new GooglePubSubWorkerSubscriber(
            Options.Create(options),
            Mock.Of<IAsyncResponseIngress>(),
            NullLogger<GooglePubSubWorkerSubscriber>.Instance,
            _ =>
            {
                factoryCalled = true;
                return Task.FromResult<IGooglePubSubSubscriberClient>(new FakeSubscriberClient());
            });

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => InvokeExecuteAsync(subscriber, CancellationToken.None));

        Assert.Contains(nameof(GooglePubSubSubscriberOptions.BackgroundWorkerCount), ex.Message, StringComparison.Ordinal);
        Assert.False(factoryCalled);
    }

    [Fact]
    public async Task SubscriberService_AckAfterEnqueueDrainBudgetExceedsHostShutdownBudget_FailsBeforeCreatingClient()
    {
        var factoryCalled = false;
        var options = new GooglePubSubAsyncResponseOptions
        {
            ProjectId = "project-a",
            WorkerSubscriptionId = "workers",
            ShutdownTimeout = TimeSpan.FromSeconds(20),
            HostShutdownTimeout = TimeSpan.FromSeconds(25)
        };
        options.WorkerSubscriber.UseAckAfterEnqueue(
            backgroundWorkerCount: 1,
            backgroundQueueCapacity: 8,
            backgroundDrainTimeout: TimeSpan.FromSeconds(10));
        var subscriber = new GooglePubSubWorkerSubscriber(
            Options.Create(options),
            Mock.Of<IAsyncResponseIngress>(),
            NullLogger<GooglePubSubWorkerSubscriber>.Instance,
            _ =>
            {
                factoryCalled = true;
                return Task.FromResult<IGooglePubSubSubscriberClient>(new FakeSubscriberClient());
            });

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => InvokeExecuteAsync(subscriber, CancellationToken.None));

        Assert.Contains(nameof(GooglePubSubAsyncResponseOptions.HostShutdownTimeout), ex.Message, StringComparison.Ordinal);
        Assert.Contains(nameof(GooglePubSubSubscriberOptions.BackgroundDrainTimeout), ex.Message, StringComparison.Ordinal);
        Assert.False(factoryCalled);
    }

    [Fact]
    public void SubscriberOptions_UseAckAfterEnqueue_RequiresExplicitPositiveSettings()
    {
        var options = new GooglePubSubSubscriberOptions();

        Assert.Throws<ArgumentOutOfRangeException>(() => options.UseAckAfterEnqueue(0, 10));
        Assert.Throws<ArgumentOutOfRangeException>(() => options.UseAckAfterEnqueue(1, 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => options.UseAckAfterEnqueue(1, 10, TimeSpan.Zero));

        options.UseAckAfterEnqueue(2, 32, TimeSpan.FromSeconds(3));
        Assert.Equal(GooglePubSubAckMode.AckAfterEnqueue, options.AckMode);
        Assert.Equal(2, options.BackgroundWorkerCount);
        Assert.Equal(32, options.BackgroundQueueCapacity);
        Assert.Equal(TimeSpan.FromSeconds(3), options.BackgroundDrainTimeout);
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

    private static PubsubMessage Message(string messageId)
        => new()
        {
            MessageId = messageId,
            Data = ByteString.CopyFromUtf8("{}")
        };

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

    private sealed class FaultingSubscriberClient : IGooglePubSubSubscriberClient
    {
        public Task StartAsync(Func<PubsubMessage, CancellationToken, Task<SubscriberClient.Reply>> handler)
            => Task.FromException(new InvalidOperationException("streaming pull faulted"));

        public Task StopAsync(SubscriberClient.ShutdownOptions options, CancellationToken cancellationToken)
            => Task.CompletedTask;
    }

    private sealed class ListLogger : ILogger
    {
        public List<LogEntry> Entries { get; } = [];

        public IDisposable BeginScope<TState>(TState state)
            where TState : notnull
            => NullScope.Instance;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
            => Entries.Add(new LogEntry(logLevel, formatter(state, exception), exception));
    }

    private sealed record LogEntry(LogLevel Level, string Message, Exception? Exception);

    private sealed class NullScope : IDisposable
    {
        public static readonly NullScope Instance = new();

        public void Dispose()
        {
        }
    }
}
