using Amazon.SQS;
using Amazon.SQS.Model;
using AsyncResponse.Transports.SQS;
using Microsoft.Extensions.Logging;
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
    public async Task WorkerSubscriber_EarlyAckSaturated_PausesReceivingAndBoundsRequestSize()
    {
        var client = new FakeSqsClient();
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var ingress = new Mock<IAsyncResponseIngress>();
        ingress.Setup(i => i.HandleWorkerMessageAsync(It.IsAny<string>()))
            .Returns(async () =>
            {
                started.TrySetResult();
                await release.Task;
            });
        var options = new SqsAsyncResponseOptions
        {
            WorkerQueue = "workers",
            ResponseQueue = "responses",
            ReceiveWaitTime = TimeSpan.FromMilliseconds(10)
        };
        options.WorkerSubscriber.UseAckAfterEnqueue(backgroundWorkerCount: 1, backgroundQueueCapacity: 1, backgroundDrainTimeout: TimeSpan.FromSeconds(5));
        var firstCalls = new SettlementCalls();
        var secondCalls = new SettlementCalls();
        var thirdCalls = new SettlementCalls();
        var subscriber = new SqsWorkerSubscriber(
            Options.Create(options),
            client,
            ingress.Object,
            NullLogger<SqsWorkerSubscriber>.Instance);

        await subscriber.StartAsync(CancellationToken.None);
        client.Enqueue(Delivery(firstCalls, messageId: "m1"));
        client.Enqueue(Delivery(secondCalls, messageId: "m2"));
        client.Enqueue(Delivery(thirdCalls, messageId: "m3"));

        // m1 goes to the (blocked) worker; m2 fills the queue of capacity 1; the receive loop must
        // now pause instead of receiving m3 — every receive counts toward the redrive policy.
        await started.Task.WaitAsync(TimeSpan.FromSeconds(2));
        await secondCalls.Deleted.Task.WaitAsync(TimeSpan.FromSeconds(2));
        var receiveAttemptsWhileSaturated = client.ReceiveAttempts;
        await Task.Delay(200);
        Assert.Equal(receiveAttemptsWhileSaturated, client.ReceiveAttempts);
        Assert.Equal(0, thirdCalls.Delete);
        // Requests are bounded by the dispatcher's free capacity, never the full MaxMessagesPerReceive.
        Assert.Equal(1, client.LastReceiveRequest!.MaxMessages);

        release.TrySetResult();
        await thirdCalls.Deleted.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await subscriber.StopAsync(CancellationToken.None);

        // No message was released with zero visibility or abandoned; all three were processed.
        Assert.Empty(firstCalls.VisibilityChanges);
        Assert.Empty(secondCalls.VisibilityChanges);
        Assert.Empty(thirdCalls.VisibilityChanges);
    }

    [Fact]
    public async Task WorkerSubscriber_SlowHandler_RenewsVisibilityOfUnprocessedBatchMessages()
    {
        var client = new FakeSqsClient();
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var firstStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var first = true;
        var ingress = new Mock<IAsyncResponseIngress>();
        ingress.Setup(i => i.HandleWorkerMessageAsync(It.IsAny<string>()))
            .Returns(async () =>
            {
                if (first)
                {
                    first = false;
                    firstStarted.TrySetResult();
                    await release.Task;
                }
            });
        var options = new SqsAsyncResponseOptions
        {
            WorkerQueue = "workers",
            ResponseQueue = "responses",
            ReceiveWaitTime = TimeSpan.FromMilliseconds(10)
        };
        options.WorkerSubscriber.VisibilityTimeout = TimeSpan.FromSeconds(45);
        options.WorkerSubscriber.VisibilityRenewalInterval = TimeSpan.FromMilliseconds(50);
        var firstCalls = new SettlementCalls();
        var secondCalls = new SettlementCalls();
        var subscriber = new SqsWorkerSubscriber(
            Options.Create(options),
            client,
            ingress.Object,
            NullLogger<SqsWorkerSubscriber>.Instance);

        // Both messages arrive in one batch; the first blocks in the handler while the second waits
        // its turn. The heartbeat must extend both (the in-handler one and the queued one).
        client.Enqueue(Delivery(firstCalls, messageId: "m1"));
        client.Enqueue(Delivery(secondCalls, messageId: "m2"));
        await subscriber.StartAsync(CancellationToken.None);

        await firstStarted.Task.WaitAsync(TimeSpan.FromSeconds(2));
        await WaitUntilAsync(() => firstCalls.VisibilityChanges.Count >= 2 && secondCalls.VisibilityChanges.Count >= 2);
        Assert.All(firstCalls.VisibilityChanges, delay => Assert.Equal(TimeSpan.FromSeconds(45), delay));
        Assert.All(secondCalls.VisibilityChanges, delay => Assert.Equal(TimeSpan.FromSeconds(45), delay));

        release.TrySetResult();
        await secondCalls.Deleted.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await subscriber.StopAsync(CancellationToken.None);

        Assert.Equal(1, firstCalls.Delete);
        Assert.Equal(1, secondCalls.Delete);
    }

    [Fact]
    public async Task RenewalSweep_AbortsBetweenMessages_WhenTheSubscriberStops()
    {
        // Regression (r24): the renewal heartbeat hardcoded CancellationToken.None in its seam
        // closure and never re-checked its token between messages, and the shutdown path awaited
        // it unbounded — a degraded SQS endpoint held StopAsync hostage for up to a full SDK retry
        // budget PER remaining batch message. The seam now threads the shutdown-linked token and
        // the sweep exits between messages once it fires.
        var client = new FakeSqsClient();
        var releaseFirstHandler = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var sweepBlocked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseSweep = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var sweepToken = CancellationToken.None;
        var ingress = new Mock<IAsyncResponseIngress>();
        ingress.Setup(i => i.HandleWorkerMessageAsync("m1-body")).Returns(async () => await releaseFirstHandler.Task);
        var options = new SqsAsyncResponseOptions
        {
            WorkerQueue = "workers",
            ResponseQueue = "responses",
            ReceiveWaitTime = TimeSpan.FromMilliseconds(10)
        };
        options.WorkerSubscriber.VisibilityTimeout = TimeSpan.FromSeconds(45);
        options.WorkerSubscriber.VisibilityRenewalInterval = TimeSpan.FromMilliseconds(50);
        var firstCalls = new SettlementCalls();
        var secondCalls = new SettlementCalls();
        var subscriber = new SqsWorkerSubscriber(
            Options.Create(options),
            client,
            ingress.Object,
            NullLogger<SqsWorkerSubscriber>.Instance);

        // One batch of two: m1's handler blocks, and the sweep parks inside m1's renewal while
        // recording the token the seam handed it.
        client.Enqueue(new SqsTransportDelivery(
            FakeSqsClient.UrlFor("workers"),
            "m1-body",
            "m1",
            "m1-receipt",
            1,
            new Dictionary<string, string>(StringComparer.Ordinal),
            () =>
            {
                firstCalls.Delete++;
                firstCalls.Deleted.TrySetResult();
                return ValueTask.CompletedTask;
            },
            async (delay, token) =>
            {
                firstCalls.RecordVisibilityChange(delay);
                sweepToken = token;
                sweepBlocked.TrySetResult();
                await releaseSweep.Task;
            }));
        client.Enqueue(Delivery(secondCalls, body: "m2-body", messageId: "m2"));
        await subscriber.StartAsync(CancellationToken.None);
        await sweepBlocked.Task.WaitAsync(TimeSpan.FromSeconds(5));

        // Shutdown fires while the sweep is parked in m1's renew: the linked token must reach the
        // in-flight SDK call (so the SDK can abort its retries)...
        var stopping = subscriber.StopAsync(CancellationToken.None);
        await WaitUntilAsync(() => sweepToken.IsCancellationRequested);

        // ...and once the parked renew returns, the sweep must exit BETWEEN messages instead of
        // spending another SDK call on m2.
        releaseSweep.TrySetResult();
        releaseFirstHandler.TrySetResult();
        await stopping.WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Empty(secondCalls.VisibilityChanges);
    }

    [Fact]
    public async Task RenewalSweep_SurvivesAnSdkClientTimeout_AndKeepsRenewingTheRestOfTheBatch()
    {
        // Regression (round 29): the sweep's catch filter excluded EVERY OperationCanceledException,
        // but the AWS SDK surfaces its own client-side HTTP timeout as TaskCanceledException with
        // the caller's token untouched. That escaped to the outer catch — whose body is just a
        // comment — silently ending renewal for the whole remaining batch. Messages 3..N then went
        // visible mid-processing and a peer re-ran them: systematic duplicate execution, no log.
        var client = new FakeSqsClient();
        var logger = new CollectingLogger<SqsWorkerSubscriber>();
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var ingress = new Mock<IAsyncResponseIngress>();
        ingress.Setup(i => i.HandleWorkerMessageAsync("m1-body")).Returns(async () => await release.Task);
        ingress.Setup(i => i.HandleWorkerMessageAsync("m2-body")).Returns(Task.CompletedTask);
        var options = new SqsAsyncResponseOptions
        {
            WorkerQueue = "workers",
            ResponseQueue = "responses",
            ReceiveWaitTime = TimeSpan.FromMilliseconds(10)
        };
        options.WorkerSubscriber.VisibilityTimeout = TimeSpan.FromSeconds(45);
        options.WorkerSubscriber.VisibilityRenewalInterval = TimeSpan.FromMilliseconds(50);
        var firstCalls = new SettlementCalls();
        var secondCalls = new SettlementCalls();
        var subscriber = new SqsWorkerSubscriber(
            Options.Create(options),
            client,
            ingress.Object,
            logger);

        // m1 is renewed FIRST in every sweep pass and always fails the way the SDK reports its own
        // HTTP timeout: TaskCanceledException, with the sweep's token never signalled.
        client.Enqueue(new SqsTransportDelivery(
            FakeSqsClient.UrlFor("workers"),
            "m1-body",
            "m1",
            "m1-receipt",
            1,
            new Dictionary<string, string>(StringComparer.Ordinal),
            () =>
            {
                firstCalls.Delete++;
                firstCalls.Deleted.TrySetResult();
                return ValueTask.CompletedTask;
            },
            (delay, token) =>
            {
                firstCalls.RecordVisibilityChange(delay);
                Assert.False(token.IsCancellationRequested);
                throw new TaskCanceledException("The request was canceled due to the configured HttpClient.Timeout.");
            }));
        client.Enqueue(Delivery(secondCalls, body: "m2-body", messageId: "m2"));
        await subscriber.StartAsync(CancellationToken.None);

        // m2 keeps being renewed across several passes even though m1 fails on every one.
        await WaitUntilAsync(() => secondCalls.VisibilityChanges.Count >= 2);

        release.TrySetResult();
        await secondCalls.Deleted.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await subscriber.StopAsync(CancellationToken.None);

        Assert.Contains(
            logger.Snapshot(),
            entry => entry.Level == LogLevel.Warning
                && entry.Message.Contains("Failed to renew visibility of SQS message", StringComparison.Ordinal));
    }

    [Fact]
    public async Task RenewalJoin_IsBounded_WhenAnInFlightRenewIgnoresCancellation()
    {
        // Red-on-old: the batch finally awaited the renewal task unbounded. The sweep is well
        // token-plumbed, but one in-flight SDK call that fails to honor the token promptly parked
        // that await forever — the receive loop never asked for another batch and, at shutdown,
        // held the host budget hostage. The join is now bounded by ShutdownTimeout (ASB parity):
        // the wedged renewal task is abandoned with a warning and the loop keeps receiving.
        var client = new FakeSqsClient();
        var logger = new CollectingLogger<SqsWorkerSubscriber>();
        var releaseFirstHandler = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var sweepBlocked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var ingress = new Mock<IAsyncResponseIngress>();
        ingress.Setup(i => i.HandleWorkerMessageAsync("m1-body")).Returns(async () => await releaseFirstHandler.Task);
        ingress.Setup(i => i.HandleWorkerMessageAsync("m2-body")).Returns(Task.CompletedTask);
        var options = new SqsAsyncResponseOptions
        {
            WorkerQueue = "workers",
            ResponseQueue = "responses",
            ReceiveWaitTime = TimeSpan.FromMilliseconds(10),
            ShutdownTimeout = TimeSpan.FromMilliseconds(250)
        };
        options.WorkerSubscriber.VisibilityTimeout = TimeSpan.FromSeconds(45);
        options.WorkerSubscriber.VisibilityRenewalInterval = TimeSpan.FromMilliseconds(50);
        var firstCalls = new SettlementCalls();
        var secondCalls = new SettlementCalls();
        var subscriber = new SqsWorkerSubscriber(
            Options.Create(options),
            client,
            ingress.Object,
            logger);

        // m1's handler blocks while the sweep parks inside m1's renew — which never returns and
        // never observes its token, like an SDK call wedged past cancellation.
        client.Enqueue(new SqsTransportDelivery(
            FakeSqsClient.UrlFor("workers"),
            "m1-body",
            "m1",
            "m1-receipt",
            1,
            new Dictionary<string, string>(StringComparer.Ordinal),
            () =>
            {
                firstCalls.Delete++;
                firstCalls.Deleted.TrySetResult();
                return ValueTask.CompletedTask;
            },
            async (delay, _) =>
            {
                firstCalls.RecordVisibilityChange(delay);
                sweepBlocked.TrySetResult();
                await new TaskCompletionSource().Task;
            }));
        await subscriber.StartAsync(CancellationToken.None);
        await sweepBlocked.Task.WaitAsync(TimeSpan.FromSeconds(5));

        // The batch finishes; the finally must give up on the wedged renewal task within the
        // bound and keep receiving — a follow-up batch's message still gets processed.
        releaseFirstHandler.TrySetResult();
        await firstCalls.Deleted.Task.WaitAsync(TimeSpan.FromSeconds(5));
        client.Enqueue(Delivery(secondCalls, body: "m2-body", messageId: "m2"));
        await secondCalls.Deleted.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await subscriber.StopAsync(CancellationToken.None);

        Assert.Contains(
            logger.Snapshot(),
            entry => entry.Level == LogLevel.Warning
                && entry.Message.Contains("did not stop within the shutdown budget", StringComparison.Ordinal));
    }

    [Fact]
    public async Task WorkerSubscriber_FailedMessageRedeliveryDelay_IsNotOverwrittenByRenewalHeartbeat()
    {
        var client = new FakeSqsClient();
        var releaseFirstHandler = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var sweepBlocked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseSweep = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var ingress = new Mock<IAsyncResponseIngress>();
        ingress.Setup(i => i.HandleWorkerMessageAsync("m1-body")).Returns(async () => await releaseFirstHandler.Task);
        ingress.Setup(i => i.HandleWorkerMessageAsync("m2-body")).ThrowsAsync(new InvalidOperationException("handler failed"));
        var options = new SqsAsyncResponseOptions
        {
            WorkerQueue = "workers",
            ResponseQueue = "responses",
            ReceiveWaitTime = TimeSpan.FromMilliseconds(10)
        };
        options.WorkerSubscriber.VisibilityTimeout = TimeSpan.FromSeconds(45);
        options.WorkerSubscriber.VisibilityRenewalInterval = TimeSpan.FromMilliseconds(50);
        options.WorkerSubscriber.RedeliveryDelay = TimeSpan.FromSeconds(3);
        var firstCalls = new SettlementCalls();
        var secondCalls = new SettlementCalls();
        var subscriber = new SqsWorkerSubscriber(
            Options.Create(options),
            client,
            ingress.Object,
            NullLogger<SqsWorkerSubscriber>.Instance);

        // One batch of two. The heartbeat's sweep blocks inside m1's renewal while both messages
        // are still unsettled — a sweep pass that started from a stale settled prefix. m1 then
        // completes and m2's handler throws, so the failure path shortens m2's visibility to the
        // 3s RedeliveryDelay (its receipt handle stays live: the message was never deleted). Only
        // afterwards does the blocked sweep resume and reach m2 — pre-fix it would overwrite the
        // fast retry with the full 45s timeout.
        client.Enqueue(new SqsTransportDelivery(
            FakeSqsClient.UrlFor("workers"),
            "m1-body",
            "m1",
            "m1-receipt",
            1,
            new Dictionary<string, string>(StringComparer.Ordinal),
            () =>
            {
                firstCalls.Delete++;
                firstCalls.Deleted.TrySetResult();
                return ValueTask.CompletedTask;
            },
            async (delay, _) =>
            {
                firstCalls.RecordVisibilityChange(delay);
                sweepBlocked.TrySetResult();
                await releaseSweep.Task;
            }));
        client.Enqueue(Delivery(secondCalls, body: "m2-body", messageId: "m2"));
        await subscriber.StartAsync(CancellationToken.None);

        await sweepBlocked.Task.WaitAsync(TimeSpan.FromSeconds(5));
        releaseFirstHandler.TrySetResult();
        // The failure path must have applied the redelivery delay before the sweep gets to move on
        // toward m2.
        await WaitUntilAsync(() => secondCalls.VisibilityChanges.Count >= 1);
        releaseSweep.TrySetResult();

        // The receive loop only issues its next receive after the batch and its renewal loop fully
        // completed, so another receive attempt proves the sweep drained past m2.
        await WaitUntilAsync(() => client.ReceiveAttempts >= 2);
        await subscriber.StopAsync(CancellationToken.None);

        var visibilityChange = Assert.Single(secondCalls.VisibilityChanges);
        Assert.Equal(TimeSpan.FromSeconds(3), visibilityChange);
        Assert.Equal(0, secondCalls.Delete);
        Assert.Equal(1, firstCalls.Delete);
        Assert.All(firstCalls.VisibilityChanges, delay => Assert.Equal(TimeSpan.FromSeconds(45), delay));
    }

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        while (!condition())
        {
            cts.Token.ThrowIfCancellationRequested();
            await Task.Delay(20, cts.Token);
        }
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

    /// <summary>
    /// Round 33 (B6): on <see cref="QueueNameExistsException"/> the WHOLE create dictionary was
    /// re-applied through SetQueueAttributes — including <c>FifoQueue</c>, which SQS accepts only at
    /// creation (InvalidAttributeName on update) — so re-provisioning an existing ".fifo" pair could
    /// never converge: RetryAsync burned its 40 attempts on that deterministic rejection (the FIFO
    /// dead-letter queue first, created with that attribute alone) and aborted host startup.
    /// Pre-fix: both updates carry <c>FifoQueue</c>. Now it is dropped from the update and the
    /// redrive policy still converges.
    /// </summary>
    [Fact]
    public async Task ProvisioningService_ExistingFifoQueues_ConvergeWithoutTheCreateOnlyFifoAttribute()
    {
        var client = new FakeSqsClient();
        client.ExistingQueues.Add("workers-dlq.fifo");
        client.ExistingQueues.Add("workers.fifo");
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

        Assert.DoesNotContain(client.CreatedQueues, created => created.QueueName.EndsWith(".fifo", StringComparison.Ordinal));
        Assert.All(
            client.AttributeUpdates,
            update => Assert.False(
                update.Attributes.ContainsKey(QueueAttributeName.FifoQueue.Value),
                $"{update.QueueUrl} re-applied the create-only FifoQueue attribute"));

        // The dead-letter queue was created with FifoQueue alone, so nothing is left to converge.
        Assert.DoesNotContain(client.AttributeUpdates, update => update.QueueUrl == FakeSqsClient.UrlFor("workers-dlq.fifo"));

        // The worker queue's redrive policy still converges onto the FIFO dead-letter queue.
        var workerUpdate = Assert.Single(client.AttributeUpdates, update => update.QueueUrl == FakeSqsClient.UrlFor("workers.fifo"));
        Assert.Contains(
            FakeSqsClient.ArnFor("workers-dlq.fifo"),
            workerUpdate.Attributes[QueueAttributeName.RedrivePolicy.Value],
            StringComparison.Ordinal);

        // The standard response pair is unaffected.
        Assert.Equal(["responses-dlq", "responses"], client.CreatedQueues.Select(created => created.QueueName));
    }

    /// <summary>
    /// Round 33 (B6), the production shape: SQS answers an update that carries <c>FifoQueue</c> with
    /// <see cref="InvalidAttributeNameException"/>, deterministically, and the provisioning retry loop
    /// treated it as transient — 40 attempts, then the exception escaped <c>StartAsync</c> and the
    /// host never came up. Pre-fix: StartAsync throws. With the attribute dropped the existing FIFO
    /// pair converges on the first try.
    /// </summary>
    [Fact]
    public async Task ProvisioningService_ExistingFifoQueues_StartupNoLongerBurnsEveryRetryOnTheRejectedFifoAttribute()
    {
        var inner = new FakeSqsClient();
        inner.ExistingQueues.Add("workers-dlq.fifo");
        inner.ExistingQueues.Add("workers.fifo");
        var client = new FifoAttributeRejectingSqsClient(inner);
        var service = new SqsQueueProvisioningService(
            Options.Create(new SqsAsyncResponseOptions
            {
                WorkerQueue = "workers.fifo",
                ResponseQueue = "responses",
                CreateQueues = true,
                SubscriberRetryBaseDelay = TimeSpan.FromMilliseconds(1),
                SubscriberRetryMaxDelay = TimeSpan.FromMilliseconds(1)
            }),
            client,
            NullLogger<SqsQueueProvisioningService>.Instance);

        await service.StartAsync(CancellationToken.None);

        Assert.Equal(0, client.RejectedUpdates);
        var workerUpdate = Assert.Single(inner.AttributeUpdates);
        Assert.Equal(FakeSqsClient.UrlFor("workers.fifo"), workerUpdate.QueueUrl);
        Assert.Contains(QueueAttributeName.RedrivePolicy.Value, workerUpdate.Attributes.Keys);
    }

    /// <summary>
    /// Wraps <see cref="FakeSqsClient"/> with real SQS's rule that <c>FifoQueue</c> is create-only:
    /// an update carrying it is rejected with <see cref="InvalidAttributeNameException"/>.
    /// </summary>
    private sealed class FifoAttributeRejectingSqsClient(FakeSqsClient inner) : ISqsClient
    {
        public int RejectedUpdates { get; private set; }

        public Task<string> GetQueueUrlAsync(string queueName, CancellationToken cancellationToken = default)
            => inner.GetQueueUrlAsync(queueName, cancellationToken);

        public Task<string> CreateQueueAsync(string queueName, IReadOnlyDictionary<string, string> attributes, CancellationToken cancellationToken = default)
            => inner.CreateQueueAsync(queueName, attributes, cancellationToken);

        public Task<string> GetQueueArnAsync(string queueUrl, CancellationToken cancellationToken = default)
            => inner.GetQueueArnAsync(queueUrl, cancellationToken);

        public Task SetQueueAttributesAsync(string queueUrl, IReadOnlyDictionary<string, string> attributes, CancellationToken cancellationToken = default)
        {
            if (attributes.ContainsKey(QueueAttributeName.FifoQueue.Value))
            {
                RejectedUpdates++;
                throw new InvalidAttributeNameException("Unknown Attribute FifoQueue.");
            }

            return inner.SetQueueAttributesAsync(queueUrl, attributes, cancellationToken);
        }

        public Task<string> SendMessageAsync(SqsOutboundMessage message, CancellationToken cancellationToken = default)
            => inner.SendMessageAsync(message, cancellationToken);

        public Task<IReadOnlyList<SqsTransportDelivery>> ReceiveMessagesAsync(SqsReceiveRequest request, CancellationToken cancellationToken = default)
            => inner.ReceiveMessagesAsync(request, cancellationToken);

        public ValueTask DisposeAsync() => inner.DisposeAsync();
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
        await delivery.ChangeVisibilityAsync(TimeSpan.FromSeconds(30), CancellationToken.None);

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
    public async Task ClientAdapter_RoundsSubSecondDurationsUp_NeverDownToZero()
    {
        // SQS speaks whole seconds, so every duration crosses an int conversion — and truncating
        // is not a rounding nicety. A 500 ms visibility timeout floored to 0 makes the message
        // visible again the instant it is received, so a second consumer picks it up while the
        // first is still handling it: concurrent duplicate handling, which is precisely what the
        // visibility timeout exists to prevent. A redelivery delay floored to 0 is a hot retry
        // loop for the same reason. All three of these values are accepted by options validation.
        var sdkClient = new Mock<IAmazonSQS>();
        ReceiveMessageRequest? captured = null;
        var visibilityChanges = new List<int>();
        var visibilityTokens = new List<CancellationToken>();
        sdkClient
            .Setup(c => c.ReceiveMessageAsync(It.IsAny<ReceiveMessageRequest>(), It.IsAny<CancellationToken>()))
            .Callback<ReceiveMessageRequest, CancellationToken>((request, _) => captured = request)
            .ReturnsAsync(new ReceiveMessageResponse
            {
                Messages = [new Message { MessageId = "m-1", Body = "{}", ReceiptHandle = "receipt-1" }]
            });
        sdkClient
            .Setup(c => c.ChangeMessageVisibilityAsync("https://queue-url", "receipt-1", It.IsAny<int>(), It.IsAny<CancellationToken>()))
            .Callback<string, string, int?, CancellationToken>((_, _, seconds, token) =>
            {
                visibilityChanges.Add(seconds ?? -1);
                visibilityTokens.Add(token);
            })
            .ReturnsAsync(new ChangeMessageVisibilityResponse());
        var adapter = new SqsClientAdapter(sdkClient.Object, ownsClient: false);

        var deliveries = await adapter.ReceiveMessagesAsync(new SqsReceiveRequest(
            "https://queue-url",
            MaxMessages: 1,
            WaitTime: TimeSpan.FromMilliseconds(500),
            VisibilityTimeout: TimeSpan.FromMilliseconds(500)));
        var delivery = Assert.Single(deliveries);
        await delivery.ChangeVisibilityAsync(TimeSpan.FromMilliseconds(1), CancellationToken.None);
        await delivery.ChangeVisibilityAsync(TimeSpan.FromSeconds(1.2), CancellationToken.None);
        // Zero survives as zero: "make it visible now" is a legitimate request, not a rounding case.
        await delivery.ChangeVisibilityAsync(TimeSpan.Zero, CancellationToken.None);

        // Regression (r24): the seam closure hardcoded CancellationToken.None into the SDK call,
        // so a renewal against a degraded endpoint could not be aborted at shutdown and burned
        // the full SDK retry budget. The closure now forwards the CALLER's token — settlement
        // passes None, the renewal heartbeat its shutdown-linked token.
        using var renewalCts = new CancellationTokenSource();
        await delivery.ChangeVisibilityAsync(TimeSpan.FromSeconds(5), renewalCts.Token);
        Assert.True(visibilityTokens[^1].CanBeCanceled);
        Assert.Equal(renewalCts.Token, visibilityTokens[^1]);
        Assert.All(visibilityTokens[..^1], token => Assert.False(token.CanBeCanceled));

        Assert.NotNull(captured);
        Assert.Equal(1, captured.WaitTimeSeconds);
        Assert.Equal(1, captured.VisibilityTimeout);
        Assert.Equal([1, 2, 0, 5], visibilityChanges);
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
            (delay, _) =>
            {
                calls.RecordVisibilityChange(delay);
                return ValueTask.CompletedTask;
            });

    private sealed class SettlementCalls
    {
        private readonly List<TimeSpan> _visibilityChanges = [];

        public int Delete;
        public TaskCompletionSource Deleted { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        // The visibility-renewal heartbeat mutates this from a background loop while tests poll it.
        public IReadOnlyList<TimeSpan> VisibilityChanges
        {
            get
            {
                lock (_visibilityChanges)
                {
                    return _visibilityChanges.ToArray();
                }
            }
        }

        public void RecordVisibilityChange(TimeSpan delay)
        {
            lock (_visibilityChanges)
            {
                _visibilityChanges.Add(delay);
            }
        }
    }

    private sealed class CollectingLogger<T> : ILogger<T>
    {
        private readonly object _gate = new();
        private readonly List<(LogLevel Level, string Message, Exception? Exception)> _entries = [];

        public IDisposable BeginScope<TState>(TState state)
            where TState : notnull
            => NoopScope.Instance;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            lock (_gate)
                _entries.Add((logLevel, formatter(state, exception), exception));
        }

        public IReadOnlyList<(LogLevel Level, string Message, Exception? Exception)> Snapshot()
        {
            lock (_gate)
                return _entries.ToArray();
        }

        private sealed class NoopScope : IDisposable
        {
            public static readonly NoopScope Instance = new();

            public void Dispose()
            {
            }
        }
    }

    [Fact]
    public async Task WorkerSubscriber_InvalidOptions_FailHostStartupSynchronously()
    {
        // Red-on-old (Hosting 10.0.10+): validation used to sit at the top of ExecuteAsync, which
        // BackgroundService.StartAsync no longer runs inline — StartAsync returned without
        // throwing and the misconfiguration surfaced late or never. Validation now runs in
        // StartAsync so a misconfigured subscriber fails host startup synchronously.
        var subscriber = new SqsWorkerSubscriber(
            Options.Create(new SqsAsyncResponseOptions
            {
                WorkerQueue = "workers",
                ResponseQueue = "responses",
                WorkerSubscriber = { AckMode = SqsAckMode.AckAfterEnqueue }
            }),
            new FakeSqsClient(),
            Mock.Of<IAsyncResponseIngress>(),
            NullLogger<SqsWorkerSubscriber>.Instance);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => subscriber.StartAsync(CancellationToken.None));
        Assert.Contains("BackgroundWorkerCount", ex.Message, StringComparison.Ordinal);
    }
}
