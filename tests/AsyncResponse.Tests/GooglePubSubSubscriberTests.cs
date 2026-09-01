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
            (_, _) => Task.FromResult<IGooglePubSubSubscriberClient>(client));

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
            (subscriptionName, _) =>
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
    public async Task WorkerSubscriberService_StopsAFaultedClient_BeforeTheRetryBuildsAReplacement()
    {
        // Regression (r24): a NON-cancellation streaming-pull failure escaped past the
        // cancellation-only catch (where StopAsync lived), so the retry loop built a replacement
        // client while the failed one — with its gRPC channels, pull connection and ack-extension
        // timers — was never released: one leaked client per rebuild, and the seam has no other
        // disposal member. The failure path now stops the client best-effort before rethrowing.
        var stops = 0;
        var clientsBuilt = 0;
        var secondBuild = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var subscriber = new GooglePubSubWorkerSubscriber(
            Options.Create(new GooglePubSubAsyncResponseOptions
            {
                ProjectId = "project-a",
                WorkerSubscriptionId = "workers",
                SubscriberRetryBaseDelay = TimeSpan.FromMilliseconds(1),
                SubscriberRetryMaxDelay = TimeSpan.FromMilliseconds(5)
            }),
            new Mock<IAsyncResponseIngress>().Object,
            NullLogger<GooglePubSubWorkerSubscriber>.Instance,
            (_, _) =>
            {
                if (Interlocked.Increment(ref clientsBuilt) >= 2)
                {
                    secondBuild.TrySetResult();
                    return Task.FromResult<IGooglePubSubSubscriberClient>(new FakeSubscriberClient());
                }

                return Task.FromResult<IGooglePubSubSubscriberClient>(
                    new CountingFaultingClient(() => Interlocked.Increment(ref stops)));
            });

        await subscriber.StartAsync(CancellationToken.None);
        await secondBuild.Task.WaitAsync(TimeSpan.FromSeconds(5));

        // The faulted first client was stopped before its replacement went live.
        Assert.True(Volatile.Read(ref stops) >= 1, "the faulted subscriber client was never stopped");
        await subscriber.StopAsync(CancellationToken.None);
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
            (_, _) => Task.FromResult<IGooglePubSubSubscriberClient>(client));

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
            (_, _) => Task.FromResult<IGooglePubSubSubscriberClient>(client));

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
            (_, _) => Task.FromResult<IGooglePubSubSubscriberClient>(client));

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
            (_, _) => Task.FromResult<IGooglePubSubSubscriberClient>(client));
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
            (_, _) => Task.FromResult<IGooglePubSubSubscriberClient>(client));
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
            (_, _) => Task.FromResult<IGooglePubSubSubscriberClient>(client));

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
    public async Task WorkerSubscriberService_AckAfterEnqueue_QueueFullAppliesBackpressureInsteadOfNacking()
    {
        var firstStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
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

        // Queue full: the callback parks until a worker frees a slot instead of NACKing — a NACK
        // would burn a delivery attempt of a DeadLetterPolicy configured on the subscription.
        var thirdHandle = dispatcher.HandleAsync(Message("message-3"), CancellationToken.None);
        await Task.Delay(100);
        Assert.False(thirdHandle.IsCompleted);
        Assert.Equal(1, Volatile.Read(ref calls));

        releaseFirst.TrySetResult();
        Assert.Equal(SubscriberClient.Reply.Ack, await thirdHandle.WaitAsync(TimeSpan.FromSeconds(5)));
        await WaitForCallsAsync(() => Volatile.Read(ref calls) == 3);
    }

    [Fact]
    public async Task WorkerSubscriberService_AckAfterEnqueue_QueueFullDuringShutdown_Nacks()
    {
        var firstStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var dispatcher = GooglePubSubMessageDispatcher.Create(
            async (_, _) =>
            {
                firstStarted.TrySetResult();
                await releaseFirst.Task.ConfigureAwait(false);
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

        using var stopping = new CancellationTokenSource();
        var thirdHandle = dispatcher.HandleAsync(Message("message-3"), stopping.Token);
        await Task.Delay(50);
        stopping.Cancel();

        // A message caught waiting when the subscriber stops is NACKed so Pub/Sub redelivers it.
        Assert.Equal(SubscriberClient.Reply.Nack, await thirdHandle.WaitAsync(TimeSpan.FromSeconds(5)));

        releaseFirst.TrySetResult();
    }

    private static async Task WaitForCallsAsync(Func<bool> condition)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        while (!condition())
        {
            cts.Token.ThrowIfCancellationRequested();
            await Task.Delay(20, cts.Token);
        }
    }

    [Fact]
    public async Task Dispatcher_AckAfterEnqueue_ReturnsNackWhenQueueAccessFails()
    {
        var workerStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var dispatcher = Assert.IsType<QueuedGooglePubSubMessageDispatcher>(
            GooglePubSubMessageDispatcher.Create(
                (_, _) =>
                {
                    workerStarted.TrySetResult();
                    return Task.CompletedTask;
                },
                new GooglePubSubAsyncResponseOptions(),
                new GooglePubSubSubscriberOptions().UseAckAfterEnqueue(1, 1, TimeSpan.FromSeconds(1)),
                NullLogger.Instance,
                "subscription",
                GooglePubSubSubscriberRole.Worker));
        var queueField = typeof(QueuedGooglePubSubMessageDispatcher)
            .GetField("_queue", BindingFlags.NonPublic | BindingFlags.Instance)!;
        var queue = queueField.GetValue(dispatcher);

        Assert.Equal(SubscriberClient.Reply.Ack, await dispatcher.HandleAsync(Message("prime-worker"), CancellationToken.None));
        await workerStarted.Task.WaitAsync(TimeSpan.FromSeconds(2));

        try
        {
            queueField.SetValue(dispatcher, null);
            var reply = await dispatcher.HandleAsync(Message("queue-failure"), CancellationToken.None);

            Assert.Equal(SubscriberClient.Reply.Nack, reply);
            Assert.Equal(0, dispatcher.PendingCount);
        }
        finally
        {
            queueField.SetValue(dispatcher, queue);
            await dispatcher.DisposeAsync();
        }
    }

    [Theory]
    [MemberData(nameof(InvalidDispatcherOptions))]
    public void DispatcherValidateOptions_RejectsInvalidOptions(
        GooglePubSubAsyncResponseOptions transportOptions,
        GooglePubSubSubscriberOptions subscriberOptions,
        string expectedMessageFragment)
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            GooglePubSubMessageDispatcher.ValidateOptions(
                transportOptions,
                subscriberOptions,
                GooglePubSubSubscriberRole.ResponseIngress));

        Assert.Contains(expectedMessageFragment, ex.Message, StringComparison.Ordinal);
    }

    public static TheoryData<GooglePubSubAsyncResponseOptions, GooglePubSubSubscriberOptions, string> InvalidDispatcherOptions()
        => new()
        {
            {
                new GooglePubSubAsyncResponseOptions(),
                new GooglePubSubSubscriberOptions
                {
                    AckMode = GooglePubSubAckMode.AckAfterEnqueue,
                    BackgroundWorkerCount = 1
                },
                nameof(GooglePubSubSubscriberOptions.BackgroundQueueCapacity)
            },
            {
                new GooglePubSubAsyncResponseOptions(),
                new GooglePubSubSubscriberOptions
                {
                    AckMode = GooglePubSubAckMode.AckAfterEnqueue,
                    BackgroundWorkerCount = 1,
                    BackgroundQueueCapacity = 8,
                    BackgroundDrainTimeout = TimeSpan.Zero
                },
                nameof(GooglePubSubSubscriberOptions.BackgroundDrainTimeout)
            },
            {
                new GooglePubSubAsyncResponseOptions { ShutdownTimeout = TimeSpan.Zero },
                new GooglePubSubSubscriberOptions().UseAckAfterEnqueue(1, 8),
                nameof(GooglePubSubAsyncResponseOptions.ShutdownTimeout)
            },
            {
                new GooglePubSubAsyncResponseOptions { HostShutdownTimeout = TimeSpan.Zero },
                new GooglePubSubSubscriberOptions().UseAckAfterEnqueue(1, 8),
                nameof(GooglePubSubAsyncResponseOptions.HostShutdownTimeout)
            },
            {
                new GooglePubSubAsyncResponseOptions
                {
                    ShutdownTimeout = TimeSpan.FromSeconds(10),
                    HostShutdownTimeout = TimeSpan.FromSeconds(20)
                },
                new GooglePubSubSubscriberOptions().UseAckAfterEnqueue(1, 8, TimeSpan.FromSeconds(15)),
                nameof(GooglePubSubAsyncResponseOptions.HostShutdownTimeout)
            },
            {
                new GooglePubSubAsyncResponseOptions(),
                new GooglePubSubSubscriberOptions { AckMode = (GooglePubSubAckMode)999 },
                nameof(GooglePubSubSubscriberOptions.AckMode)
            }
        };

    [Fact]
    public void DispatcherValidateOptions_SameWorkerAndResponseSubscriptionIds_Throws()
    {
        var ex = Assert.Throws<InvalidOperationException>(() => GooglePubSubMessageDispatcher.ValidateOptions(
            new GooglePubSubAsyncResponseOptions
            {
                WorkerSubscriptionId = "shared-subscription",
                ResponseSubscriptionId = "shared-subscription"
            },
            new GooglePubSubSubscriberOptions(),
            GooglePubSubSubscriberRole.Worker));

        Assert.Contains(nameof(GooglePubSubAsyncResponseOptions.WorkerSubscriptionId), ex.Message, StringComparison.Ordinal);
        Assert.Contains(nameof(GooglePubSubAsyncResponseOptions.ResponseSubscriptionId), ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void DispatcherValidateOptions_SameWorkerAndResponseTopicIds_Throws()
    {
        var ex = Assert.Throws<InvalidOperationException>(() => GooglePubSubMessageDispatcher.ValidateOptions(
            new GooglePubSubAsyncResponseOptions
            {
                WorkerTopicId = "shared-topic",
                ResponseTopicId = "shared-topic"
            },
            new GooglePubSubSubscriberOptions(),
            GooglePubSubSubscriberRole.Worker));

        Assert.Contains(nameof(GooglePubSubAsyncResponseOptions.WorkerTopicId), ex.Message, StringComparison.Ordinal);
        Assert.Contains(nameof(GooglePubSubAsyncResponseOptions.ResponseTopicId), ex.Message, StringComparison.Ordinal);

        // Distinct (or unset) destinations pass.
        GooglePubSubMessageDispatcher.ValidateOptions(
            new GooglePubSubAsyncResponseOptions
            {
                WorkerTopicId = "worker-topic",
                ResponseTopicId = "response-topic",
                WorkerSubscriptionId = "worker-sub",
                ResponseSubscriptionId = "response-sub"
            },
            new GooglePubSubSubscriberOptions(),
            GooglePubSubSubscriberRole.Worker);
        GooglePubSubMessageDispatcher.ValidateOptions(
            new GooglePubSubAsyncResponseOptions(),
            new GooglePubSubSubscriberOptions(),
            GooglePubSubSubscriberRole.Worker);
    }

    [Fact]
    public async Task SubscriberService_Startup_WarnsThatRedeliveryIsUnboundedWithoutDeadLetterPolicy()
    {
        var client = new FakeSubscriberClient();
        var logger = new CapturingLogger<GooglePubSubWorkerSubscriber>();
        var subscriber = new GooglePubSubWorkerSubscriber(
            Options.Create(new GooglePubSubAsyncResponseOptions
            {
                ProjectId = "project-a",
                WorkerSubscriptionId = "workers"
            }),
            Mock.Of<IAsyncResponseIngress>(),
            logger,
            (_, _) => Task.FromResult<IGooglePubSubSubscriberClient>(client));

        await subscriber.StartAsync(CancellationToken.None);
        await WaitForHandlerAsync(client);
        await subscriber.StopAsync(CancellationToken.None);

        // The transport has no delivery-attempt cap and no library DLQ for Pub/Sub; the operator is
        // told (once, at startup) that only a subscription DeadLetterPolicy bounds redelivery.
        Assert.Contains(logger.Entries, entry =>
            entry.Level == LogLevel.Warning
            && entry.Message.Contains("unbounded", StringComparison.OrdinalIgnoreCase)
            && entry.Message.Contains("DeadLetterPolicy", StringComparison.Ordinal));
    }

    [Fact]
    public async Task QueuedDispose_SurvivesAWorkerFaultingOutsideItsHandlerGuard()
    {
        // Regression: the drain join caught only TimeoutException (the shared DB base and NATS
        // also carry a general arm). A worker faulting outside its handler guard — here the log
        // sink throwing from the "handler failed" entry inside the catch arm — rethrew from
        // Task.WhenAll, escaped DisposeAsync into the subscriber's `await using` and leaked the
        // drain token source.
        var logger = new ErrorThrowingLogger();
        var dispatcher = GooglePubSubMessageDispatcher.Create(
            (_, _) => throw new InvalidOperationException("handler boom"),
            new GooglePubSubAsyncResponseOptions(),
            new GooglePubSubSubscriberOptions().UseAckAfterEnqueue(
                backgroundWorkerCount: 1,
                backgroundQueueCapacity: 8,
                backgroundDrainTimeout: TimeSpan.FromSeconds(5)),
            logger,
            "workers",
            GooglePubSubSubscriberRole.Worker);

        Assert.Equal(SubscriberClient.Reply.Ack, await dispatcher.HandleAsync(Message("message-fault"), CancellationToken.None));
        await logger.ErrorThrown.Task.WaitAsync(TimeSpan.FromSeconds(5));

        await dispatcher.DisposeAsync();
    }

    private sealed class CapturingLogger<T> : ILogger<T>
    {
        private readonly object _gate = new();
        private readonly List<(LogLevel Level, string Message)> _entries = [];

        public IReadOnlyList<(LogLevel Level, string Message)> Entries
        {
            get
            {
                lock (_gate)
                {
                    return _entries.ToArray();
                }
            }
        }

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            lock (_gate)
            {
                _entries.Add((logLevel, formatter(state, exception)));
            }
        }
    }

    [Fact]
    public void DispatcherValidateOptions_AllowsEarlyAckWithinHostShutdownBudget()
    {
        GooglePubSubMessageDispatcher.ValidateOptions(
            new GooglePubSubAsyncResponseOptions
            {
                ShutdownTimeout = TimeSpan.FromSeconds(10),
                HostShutdownTimeout = TimeSpan.FromSeconds(45)
            },
            new GooglePubSubSubscriberOptions().UseAckAfterEnqueue(1, 8, TimeSpan.FromSeconds(15)),
            GooglePubSubSubscriberRole.Worker);
    }

    [Fact]
    public void DispatcherValidateOptions_DocumentedEarlyAckDefaults_Pass()
    {
        // Regression: the documented two-arg early-ACK opt-in with stock defaults
        // (5s ShutdownTimeout + 20s BackgroundDrainTimeout vs HostShutdownTimeout 30s)
        // must not fail startup.
        GooglePubSubMessageDispatcher.ValidateOptions(
            new GooglePubSubAsyncResponseOptions(),
            new GooglePubSubSubscriberOptions().UseAckAfterEnqueue(4, 256),
            GooglePubSubSubscriberRole.Worker);
    }

    [Fact]
    public async Task DispatcherCreate_ReturnsConfiguredDispatcherTypes()
    {
        await using var awaiting = GooglePubSubMessageDispatcher.Create(
            (_, _) => Task.CompletedTask,
            new GooglePubSubAsyncResponseOptions(),
            new GooglePubSubSubscriberOptions(),
            NullLogger.Instance,
            "workers",
            GooglePubSubSubscriberRole.Worker);
        await using var queued = GooglePubSubMessageDispatcher.Create(
            (_, _) => Task.CompletedTask,
            new GooglePubSubAsyncResponseOptions(),
            new GooglePubSubSubscriberOptions().UseAckAfterEnqueue(1, 8, TimeSpan.FromSeconds(5)),
            NullLogger.Instance,
            "workers",
            GooglePubSubSubscriberRole.Worker);

        Assert.IsType<AwaitingGooglePubSubMessageDispatcher>(awaiting);
        Assert.IsType<QueuedGooglePubSubMessageDispatcher>(queued);
    }

    [Fact]
    public async Task Dispatcher_AwaitingHandlerSucceeds_EmitsReceiveActivityTags()
    {
        using var collector = new AsyncResponseActivityCollector();
        await using var dispatcher = GooglePubSubMessageDispatcher.Create(
            (_, _) => Task.CompletedTask,
            new GooglePubSubAsyncResponseOptions(),
            new GooglePubSubSubscriberOptions(),
            NullLogger.Instance,
            "workers",
            GooglePubSubSubscriberRole.Worker);
        var message = Message("message-activity");
        message.Attributes["correlationId"] = "corr-activity";

        var reply = await dispatcher.HandleAsync(message, CancellationToken.None);

        Assert.Equal(SubscriberClient.Reply.Ack, reply);
        var activity = collector.Single("asyncresponse.pubsub.receive", "asyncresponse.transport", "google_pubsub");
        Assert.Equal("Worker", AsyncResponseActivityCollector.Tag(activity, "asyncresponse.pubsub.role"));
        Assert.Equal(nameof(GooglePubSubAckMode.AckAfterHandlerCompletes), AsyncResponseActivityCollector.Tag(activity, "asyncresponse.pubsub.ack_mode"));
        Assert.Equal("gcp_pubsub", AsyncResponseActivityCollector.Tag(activity, "messaging.system"));
        Assert.Equal("workers", AsyncResponseActivityCollector.Tag(activity, "messaging.destination.name"));
        Assert.Equal("message-activity", AsyncResponseActivityCollector.Tag(activity, "messaging.message.id"));
        Assert.Equal("corr-activity", AsyncResponseActivityCollector.Tag(activity, "asyncresponse.correlation_id"));
    }

    [Fact]
    public async Task Dispatcher_AckAfterEnqueue_CallbackFailureIsSwallowed()
    {
        var logger = new ListLogger();
        var failureObserved = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var subscriberOptions = new GooglePubSubSubscriberOptions().UseAckAfterEnqueue(
            backgroundWorkerCount: 1,
            backgroundQueueCapacity: 8,
            backgroundDrainTimeout: TimeSpan.FromSeconds(5));
        subscriberOptions.OnBackgroundFailure = _ =>
        {
            failureObserved.TrySetResult();
            throw new InvalidOperationException("callback-failed");
        };

        await using var dispatcher = GooglePubSubMessageDispatcher.Create(
            (_, _) => Task.FromException(new InvalidOperationException("handler-failed")),
            new GooglePubSubAsyncResponseOptions(),
            subscriberOptions,
            logger,
            "workers",
            GooglePubSubSubscriberRole.Worker);

        var reply = await dispatcher.HandleAsync(Message("message-callback-failure"), CancellationToken.None);

        Assert.Equal(SubscriberClient.Reply.Ack, reply);
        await failureObserved.Task.WaitAsync(TimeSpan.FromSeconds(2));
        await Eventually(() => logger.Entries.Any(entry =>
            entry.Level == LogLevel.Error &&
            entry.Exception is InvalidOperationException { Message: "callback-failed" }));
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
    public async Task Dispatcher_AckAfterEnqueue_AfterTheDrainBudgetLapses_DoesNotStartFreshWork_AndSurfacesIt()
    {
        // Regression (round 29): the drain token cannot stop the REAL handler — it is the ingress,
        // whose target takes no CancellationToken — so the sibling fact's token-honoring handler
        // hid the defect. With a handler that ignores the token the loop kept dequeuing and
        // EXECUTING past the budget, and whatever was still queued at process exit vanished with no
        // record: those messages were ACKed at enqueue, so Pub/Sub never redelivers them.
        var firstStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handlerRuns = 0;
        var failures = new List<GooglePubSubBackgroundFailureContext>();

        var subscriberOptions = new GooglePubSubSubscriberOptions().UseAckAfterEnqueue(
            backgroundWorkerCount: 1,
            backgroundQueueCapacity: 8,
            backgroundDrainTimeout: TimeSpan.FromMilliseconds(100));
        subscriberOptions.OnBackgroundFailure = context =>
        {
            lock (failures)
            {
                failures.Add(context);
            }

            return ValueTask.CompletedTask;
        };

        var dispatcher = GooglePubSubMessageDispatcher.Create(
            async (_, _) =>
            {
                // Deliberately ignores the token, exactly like the ingress handler in production.
                if (Interlocked.Increment(ref handlerRuns) == 1)
                {
                    firstStarted.TrySetResult();
                    await releaseFirst.Task.ConfigureAwait(false);
                }
            },
            new GooglePubSubAsyncResponseOptions(),
            subscriberOptions,
            NullLogger.Instance,
            "workers",
            GooglePubSubSubscriberRole.Worker);

        Assert.Equal(SubscriberClient.Reply.Ack, await dispatcher.HandleAsync(Message("message-running"), CancellationToken.None));
        await firstStarted.Task.WaitAsync(TimeSpan.FromSeconds(2));
        Assert.Equal(SubscriberClient.Reply.Ack, await dispatcher.HandleAsync(Message("message-queued"), CancellationToken.None));

        await dispatcher.DisposeAsync(); // the 100ms drain budget lapses while the first handler blocks
        releaseFirst.TrySetResult();      // ...and only now can the loop reach the queued message

        var deadline = Stopwatch.StartNew();
        while (deadline.Elapsed < TimeSpan.FromSeconds(5))
        {
            lock (failures)
            {
                if (failures.Count == 1)
                    break;
            }

            await Task.Delay(15);
        }

        lock (failures)
        {
            var dropped = Assert.Single(failures);
            Assert.Equal("message-queued", dropped.MessageId);
            Assert.IsAssignableFrom<OperationCanceledException>(dropped.Exception);
        }

        // The queued message was NOT executed after the budget lapsed.
        Assert.Equal(1, Volatile.Read(ref handlerRuns));
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
            (_, _) => Interlocked.Increment(ref attempts) == 1
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
            (_, _) => Interlocked.Increment(ref clients) == 1
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
            (_, _) =>
            {
                factoryCalled = true;
                return Task.FromResult<IGooglePubSubSubscriberClient>(new FakeSubscriberClient());
            });

        await Assert.ThrowsAsync<InvalidOperationException>(() => subscriber.StartAsync(CancellationToken.None));

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
            (_, _) =>
            {
                factoryCalled = true;
                return Task.FromResult<IGooglePubSubSubscriberClient>(new FakeSubscriberClient());
            });

        // Red-on-old (Hosting 10.0.10+): validation used to sit at the top of ExecuteAsync, which
        // BackgroundService.StartAsync no longer runs inline — StartAsync returned without
        // throwing and the misconfiguration surfaced late or never. Validation now runs in
        // StartAsync, so it fails host startup synchronously — before the client factory runs.
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => subscriber.StartAsync(CancellationToken.None));

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
            (_, _) =>
            {
                factoryCalled = true;
                return Task.FromResult<IGooglePubSubSubscriberClient>(new FakeSubscriberClient());
            });

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => subscriber.StartAsync(CancellationToken.None));

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
    public void SubscriberOptions_UseAckAfterEnqueue_LeavesDefaultDrainTimeoutWhenOmitted()
    {
        var options = new GooglePubSubSubscriberOptions();

        options.UseAckAfterEnqueue(2, 32);

        Assert.Equal(TimeSpan.FromSeconds(20), options.BackgroundDrainTimeout);
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

    private static async Task Eventually(Func<bool> condition)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        while (!condition())
            await Task.Delay(10, cts.Token);
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

    private sealed class CountingFaultingClient(Action _onStop) : IGooglePubSubSubscriberClient
    {
        public Task StartAsync(Func<PubsubMessage, CancellationToken, Task<SubscriberClient.Reply>> handler)
            => Task.FromException(new InvalidOperationException("streaming pull faulted"));

        public Task StopAsync(SubscriberClient.ShutdownOptions options, CancellationToken cancellationToken)
        {
            _onStop();
            return Task.CompletedTask;
        }
    }

    private sealed class ListLogger : ILogger
    {
        private readonly object _gate = new();
        private readonly List<LogEntry> _entries = [];

        public IReadOnlyList<LogEntry> Entries
        {
            get
            {
                lock (_gate)
                    return _entries.ToList();
            }
        }

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
        {
            lock (_gate)
                _entries.Add(new LogEntry(logLevel, formatter(state, exception), exception));
        }
    }

    private sealed record LogEntry(LogLevel Level, string Message, Exception? Exception);

    private sealed class NullScope : IDisposable
    {
        public static readonly NullScope Instance = new();

        public void Dispose()
        {
        }
    }

    [Fact]
    public async Task WorkerSubscriber_InvalidOptions_FailHostStartupSynchronously()
    {
        // Red-on-old (Hosting 10.0.10+): validation used to sit at the top of ExecuteAsync, which
        // BackgroundService.StartAsync no longer runs inline — StartAsync returned without
        // throwing and the misconfiguration surfaced late or never. Validation now runs in
        // StartAsync so a misconfigured subscriber fails host startup synchronously.
        var subscriber = new GooglePubSubWorkerSubscriber(
            Options.Create(new GooglePubSubAsyncResponseOptions
            {
                ProjectId = "project-a",
                WorkerSubscriptionId = "workers",
                WorkerSubscriber = { AckMode = GooglePubSubAckMode.AckAfterEnqueue }
            }),
            Mock.Of<IAsyncResponseIngress>(),
            NullLogger<GooglePubSubWorkerSubscriber>.Instance,
            (_, _) => Task.FromResult<IGooglePubSubSubscriberClient>(null!));

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => subscriber.StartAsync(CancellationToken.None));
        Assert.Contains("BackgroundWorkerCount", ex.Message, StringComparison.Ordinal);
    }
}
