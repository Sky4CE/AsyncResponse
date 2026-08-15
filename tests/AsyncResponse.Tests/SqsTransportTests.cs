using Amazon.SQS;
using AsyncResponse.Transports.SQS;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using System.Net;
using System.Text.Json;
using Xunit;

namespace AsyncResponse.Tests;

public sealed class SqsTransportTests
{
    [Fact]
    public void WithSqsTransport_ReplacesWorkerTransportAndReplyTargetProvider()
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddSingleton<ISqsClient>(new FakeSqsClient());

        var provider = services
            .AddAsyncResponse()
            .WithInMemoryChannel()
            .WithSqsTransport(options =>
            {
                options.ServiceUrl = "http://localhost:4566";
                options.WorkerQueue = "workers";
                options.ResponseQueue = "responses";
            })
            .Services
            .BuildServiceProvider();

        Assert.IsType<SqsWorkerTransport>(provider.GetRequiredService<IWorkerTransport>());
        Assert.IsType<SqsReplyTargetProvider>(provider.GetRequiredService<IAsyncResponseReplyTargetProvider>());
        Assert.Equal(SqsAsyncResponseOptions.TransportName, provider.GetRequiredService<AsyncResponseTransportMarker>().Name);
        Assert.Contains(provider.GetServices<IHostedService>(), service => service is SqsQueueProvisioningService);
        Assert.Contains(provider.GetServices<IHostedService>(), service => service is SqsWorkerSubscriber);
        Assert.Contains(provider.GetServices<IHostedService>(), service => service is SqsResponseIngressSubscriber);
    }

    [Fact]
    public void WorkerTransport_InvalidCommonOptionsThrow()
    {
        AssertInvalidCommon(
            options => options.ResponseQueue = options.WorkerQueue,
            nameof(SqsAsyncResponseOptions.ResponseQueue));
        AssertInvalidCommon(
            options => options.MaxMessagesPerReceive = 0,
            nameof(SqsAsyncResponseOptions.MaxMessagesPerReceive));
        AssertInvalidCommon(
            options => options.MaxMessagesPerReceive = 11,
            nameof(SqsAsyncResponseOptions.MaxMessagesPerReceive));
        AssertInvalidCommon(
            options => options.ReceiveWaitTime = TimeSpan.FromSeconds(-1),
            nameof(SqsAsyncResponseOptions.ReceiveWaitTime));
        AssertInvalidCommon(
            options => options.ReceiveWaitTime = TimeSpan.FromSeconds(21),
            nameof(SqsAsyncResponseOptions.ReceiveWaitTime));
        AssertInvalidCommon(
            options => options.PublishMaxAttempts = 0,
            nameof(SqsAsyncResponseOptions.PublishMaxAttempts));
        AssertInvalidCommon(
            options => options.PublishRetryBaseDelay = TimeSpan.FromSeconds(2),
            nameof(SqsAsyncResponseOptions.PublishRetryBaseDelay));
        AssertInvalidCommon(
            options => options.SubscriberRetryBaseDelay = TimeSpan.FromSeconds(2),
            nameof(SqsAsyncResponseOptions.SubscriberRetryBaseDelay));
        AssertInvalidCommon(
            options => options.ShutdownTimeout = TimeSpan.Zero,
            nameof(SqsAsyncResponseOptions.ShutdownTimeout));
        AssertInvalidCommon(
            options =>
            {
                options.CreateQueues = true;
                options.MaxReceiveCount = 0;
            },
            nameof(SqsAsyncResponseOptions.MaxReceiveCount));
        AssertInvalidCommon(
            options =>
            {
                options.CreateQueues = true;
                options.DeadLetterQueueSuffix = " ";
            },
            nameof(SqsAsyncResponseOptions.DeadLetterQueueSuffix));
        AssertInvalidCommon(
            options =>
            {
                options.WorkerQueue = "workers.fifo";
                options.FifoMessageGroupIdFallback = " ";
            },
            nameof(SqsAsyncResponseOptions.FifoMessageGroupIdFallback));

        // Regression (r24): the DERIVED dead-letter names were never checked against the live
        // queues — "jobs" + "-dlq" equals a response queue named "jobs-dlq", so provisioning
        // aimed the worker redrive policy at the LIVE response queue: every poison worker job was
        // moved into the ingress, where any parseable JSON completes a real waiter.
        AssertInvalidCommon(
            options =>
            {
                options.CreateQueues = true;
                options.WorkerQueue = "jobs";
                options.ResponseQueue = "jobs-dlq";
            },
            nameof(SqsAsyncResponseOptions.DeadLetterQueueSuffix));
        AssertInvalidCommon(
            options =>
            {
                options.CreateQueues = true;
                options.WorkerQueue = "jobs.fifo";
                options.ResponseQueue = "jobs-dlq.fifo";
            },
            nameof(SqsAsyncResponseOptions.DeadLetterQueueSuffix));
    }

    [Fact]
    public void DeriveDeadLetterQueueName_MatchesProvisioningForStandardAndFifo()
    {
        // The validator rejects collisions using the SAME derivation provisioning creates queues
        // with — pin both shapes so the two can never drift apart.
        Assert.Equal("jobs-dlq", SqsQueueAddress.DeriveDeadLetterQueueName("jobs", "-dlq"));
        Assert.Equal("jobs-dlq.fifo", SqsQueueAddress.DeriveDeadLetterQueueName("jobs.fifo", "-dlq"));
    }

    [Fact]
    public async Task WorkerTransport_PublishesSerializedJobWithCorrelationAttribute()
    {
        var client = new FakeSqsClient();
        var transport = new SqsWorkerTransport(
            Options.Create(new SqsAsyncResponseOptions
            {
                WorkerQueue = "worker-q",
                ResponseQueue = "response-q",
                CorrelationIdAttribute = "cid"
            }),
            client);

        await transport.PublishAsync(WorkerJob("corr-sqs"));

        var message = Assert.Single(client.SentMessages);
        Assert.Equal(FakeSqsClient.UrlFor("worker-q"), message.QueueUrl);
        Assert.Equal("corr-sqs", message.CorrelationId);
        Assert.Equal("corr-sqs", message.MessageAttributes["cid"]);
        Assert.Null(message.MessageGroupId);
        Assert.Null(message.MessageDeduplicationId);
        var roundTripped = JsonSerializer.Deserialize<WorkerJobEnvelope>(message.Body);
        Assert.NotNull(roundTripped);
        Assert.Equal("corr-sqs", roundTripped.CorrelationId);
        Assert.Equal("DoWork", roundTripped.Call.MethodName);
    }

    [Fact]
    public async Task WorkerTransport_ResolvesQueueUrlOnceAndCaches()
    {
        var client = new FakeSqsClient();
        var transport = new SqsWorkerTransport(
            Options.Create(new SqsAsyncResponseOptions { WorkerQueue = "worker-q" }),
            client);

        await transport.PublishAsync(WorkerJob("corr-1"));
        await transport.PublishAsync(WorkerJob("corr-2"));

        Assert.Equal(1, client.GetQueueUrlCalls);
        Assert.Equal(2, client.SentMessages.Count);
    }

    [Fact]
    public async Task WorkerTransport_QueueUrlConfigured_SkipsGetQueueUrl()
    {
        var client = new FakeSqsClient();
        var transport = new SqsWorkerTransport(
            Options.Create(new SqsAsyncResponseOptions
            {
                WorkerQueue = "https://sqs.us-east-1.amazonaws.com/000000000000/worker-q"
            }),
            client);

        await transport.PublishAsync(WorkerJob("corr-url"));

        Assert.Equal(0, client.GetQueueUrlCalls);
        Assert.Equal(
            "https://sqs.us-east-1.amazonaws.com/000000000000/worker-q",
            Assert.Single(client.SentMessages).QueueUrl);
    }

    [Fact]
    public async Task WorkerTransport_FailedQueueUrlResolutionIsNotCached()
    {
        var client = new FakeSqsClient { GetQueueUrlFailuresBeforeSuccess = 1 };
        var transport = new SqsWorkerTransport(
            Options.Create(new SqsAsyncResponseOptions { WorkerQueue = "worker-q" }),
            client);

        await Assert.ThrowsAsync<InvalidOperationException>(() => transport.PublishAsync(WorkerJob("corr-first")));
        await transport.PublishAsync(WorkerJob("corr-second"));

        Assert.Equal(2, client.GetQueueUrlCalls);
        Assert.Single(client.SentMessages);
    }

    [Fact]
    public async Task WorkerTransport_FifoQueue_SetsMessageGroupIdToCorrelationId()
    {
        var client = new FakeSqsClient();
        var transport = new SqsWorkerTransport(
            Options.Create(new SqsAsyncResponseOptions { WorkerQueue = "worker-q.fifo" }),
            client);

        await transport.PublishAsync(WorkerJob("corr-fifo"));
        await transport.PublishAsync(WorkerJob("corr-fifo"));

        Assert.Equal(2, client.SentMessages.Count);
        Assert.All(client.SentMessages, message => Assert.Equal("corr-fifo", message.MessageGroupId));
        // Distinct jobs of the same flow must never be deduplicated away.
        Assert.NotNull(client.SentMessages[0].MessageDeduplicationId);
        Assert.NotEqual(client.SentMessages[0].MessageDeduplicationId, client.SentMessages[1].MessageDeduplicationId);
    }

    [Fact]
    public async Task WorkerTransport_FifoQueue_BlankCorrelation_UsesFallbackGroupId()
    {
        var client = new FakeSqsClient();
        var transport = new SqsWorkerTransport(
            Options.Create(new SqsAsyncResponseOptions
            {
                WorkerQueue = "worker-q.fifo",
                FifoMessageGroupIdFallback = "fallback-group"
            }),
            client);

        await transport.PublishAsync(WorkerJob(" "));

        var message = Assert.Single(client.SentMessages);
        Assert.Equal("fallback-group", message.MessageGroupId);
        Assert.Null(message.CorrelationId);
        Assert.False(message.MessageAttributes.ContainsKey("correlationId"));
    }

    [Fact]
    public async Task WorkerTransport_RetriesTransientPublishFailures()
    {
        var client = new FakeSqsClient { SendFailuresBeforeSuccess = 2 };
        var transport = new SqsWorkerTransport(
            Options.Create(new SqsAsyncResponseOptions
            {
                PublishMaxAttempts = 3,
                PublishRetryBaseDelay = TimeSpan.FromMilliseconds(1),
                PublishRetryMaxDelay = TimeSpan.FromMilliseconds(1)
            }),
            client);

        await transport.PublishAsync(WorkerJob("corr-retry"));

        Assert.Equal(3, client.SendAttempts);
        Assert.Single(client.SentMessages);
    }

    [Fact]
    public async Task WorkerTransport_WhenTransientPublishKeepsFailing_Propagates()
    {
        var client = new FakeSqsClient { SendException = TransientSqsException("send failed") };
        var transport = new SqsWorkerTransport(
            Options.Create(new SqsAsyncResponseOptions
            {
                PublishMaxAttempts = 2,
                PublishRetryBaseDelay = TimeSpan.FromMilliseconds(1),
                PublishRetryMaxDelay = TimeSpan.FromMilliseconds(1)
            }),
            client);

        var ex = await Assert.ThrowsAsync<AmazonSQSException>(() => transport.PublishAsync(WorkerJob("corr-fail")));

        Assert.StartsWith("send failed", ex.Message, StringComparison.Ordinal);
        Assert.Equal(2, client.SendAttempts);
    }

    [Fact]
    public async Task WorkerTransport_NonTransientSqsFailure_ThrowsWithoutRetry()
    {
        var client = new FakeSqsClient
        {
            SendException = new AmazonSQSException(
                "message too large",
                Amazon.Runtime.ErrorType.Sender,
                "InvalidParameterValue",
                "request-id",
                HttpStatusCode.BadRequest)
        };
        var transport = new SqsWorkerTransport(
            Options.Create(new SqsAsyncResponseOptions
            {
                PublishMaxAttempts = 3,
                PublishRetryBaseDelay = TimeSpan.FromMilliseconds(1),
                PublishRetryMaxDelay = TimeSpan.FromMilliseconds(1)
            }),
            client);

        await Assert.ThrowsAsync<AmazonSQSException>(() => transport.PublishAsync(WorkerJob("corr-too-large")));

        Assert.Equal(1, client.SendAttempts);
    }

    [Fact]
    public async Task WorkerTransport_NonSqsFailure_ThrowsWithoutRetry()
    {
        var client = new FakeSqsClient { SendException = new InvalidOperationException("serialization failed") };
        var transport = new SqsWorkerTransport(
            Options.Create(new SqsAsyncResponseOptions
            {
                PublishMaxAttempts = 3,
                PublishRetryBaseDelay = TimeSpan.FromMilliseconds(1),
                PublishRetryMaxDelay = TimeSpan.FromMilliseconds(1)
            }),
            client);

        await Assert.ThrowsAsync<InvalidOperationException>(() => transport.PublishAsync(WorkerJob("corr-fatal")));

        Assert.Equal(1, client.SendAttempts);
    }

    [Fact]
    public void IsTransient_ClassifiesSqsFailures()
    {
        Assert.True(SqsWorkerTransport.IsTransient(TransientSqsException("5xx")));
        Assert.True(SqsWorkerTransport.IsTransient(new AmazonSQSException(
            "slow down",
            Amazon.Runtime.ErrorType.Sender,
            "RequestThrottled",
            "request-id",
            HttpStatusCode.Forbidden)));
        Assert.True(SqsWorkerTransport.IsTransient(new AmazonSQSException(
            "slow down",
            Amazon.Runtime.ErrorType.Sender,
            "ThrottlingException",
            "request-id",
            HttpStatusCode.BadRequest)));
        Assert.False(SqsWorkerTransport.IsTransient(new AmazonSQSException(
            "bad request",
            Amazon.Runtime.ErrorType.Sender,
            "InvalidParameterValue",
            "request-id",
            HttpStatusCode.BadRequest)));
        Assert.False(SqsWorkerTransport.IsTransient(new InvalidOperationException("not aws")));
    }

    [Fact]
    public async Task WorkerTransport_NullJob_Throws()
    {
        var transport = new SqsWorkerTransport(
            Options.Create(new SqsAsyncResponseOptions()),
            new FakeSqsClient());

        await Assert.ThrowsAsync<ArgumentNullException>(() => transport.PublishAsync(null!));
    }

    [Fact]
    public async Task WorkerTransport_DoubleDisposeIsNoOpAndKeepsSharedClientAlive()
    {
        var client = new FakeSqsClient();
        var transport = new SqsWorkerTransport(
            Options.Create(new SqsAsyncResponseOptions()),
            client);

        await transport.PublishAsync(WorkerJob("corr-dispose"));
        await transport.DisposeAsync();
        await transport.DisposeAsync();

        // The internal constructor shares the DI-owned client, so dispose must not touch it.
        Assert.Equal(0, client.DisposeCalls);

        // A transport disposed before its queue URL was cached rejects new publishes.
        var freshTransport = new SqsWorkerTransport(
            Options.Create(new SqsAsyncResponseOptions { WorkerQueue = "worker-q" }),
            new FakeSqsClient());
        await freshTransport.DisposeAsync();
        await Assert.ThrowsAsync<ObjectDisposedException>(() => freshTransport.PublishAsync(WorkerJob("corr-after-dispose")));
    }

    [Fact]
    public void ReplyTargetProvider_UsesResponseQueueAsDefaultTarget()
    {
        var provider = new SqsReplyTargetProvider(Options.Create(new SqsAsyncResponseOptions
        {
            ResponseQueue = "responses",
            CorrelationIdAttribute = "cid"
        }));

        var target = provider.GetReplyTarget();

        Assert.Equal("default", target.Name);
        Assert.Equal(SqsAsyncResponseOptions.TransportName, target.Transport);
        Assert.Equal("responses", target.Address);
        Assert.Equal("responses", target.Properties["queue"]);
        Assert.Equal("cid", target.Properties["correlationIdAttribute"]);
    }

    [Fact]
    public void ReplyTargetProvider_ResolvesNamedTargets()
    {
        var options = new SqsAsyncResponseOptions();
        options.AddReplyTarget("regional", "regional-responses");
        options.ReplyTargets["regional"].Properties["region"] = "us-east-1";

        var target = new SqsReplyTargetProvider(Options.Create(options)).GetReplyTarget("regional");

        Assert.Equal("regional", target.Name);
        Assert.Equal("regional-responses", target.Address);
        Assert.Equal("us-east-1", target.Properties["region"]);
    }

    [Fact]
    public void ReplyTargetProvider_MissingNamedTarget_Throws()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            new SqsReplyTargetProvider(Options.Create(new SqsAsyncResponseOptions()))
                .GetReplyTarget("missing"));

        Assert.Contains("missing", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void CorrelationIdExtractor_ReadsAttributeThenJson()
    {
        var options = new SqsAsyncResponseOptions { CorrelationIdAttribute = "cid" };

        Assert.Equal("from-attribute", SqsCorrelationIdExtractor.Extract(
            Delivery(attributes: new Dictionary<string, string> { ["cid"] = "from-attribute" }),
            "{}",
            options));

        Assert.Equal("from-json", SqsCorrelationIdExtractor.Extract(
            Delivery(body: """{"CustomParameters":{"CorrelationId":"from-json"}}"""),
            """{"CustomParameters":{"CorrelationId":"from-json"}}""",
            options));
    }

    [Fact]
    public void CorrelationIdExtractor_HandlesNestedJsonStringsAndInvalidJson()
    {
        var nested = """
            {
              "PubSubParams": {
                "CustomParameters": "{\"CorrelationId\":\"corr-nested\"}"
              }
            }
            """;

        Assert.Equal("corr-nested", SqsCorrelationIdExtractor.Extract(
            Delivery(body: nested),
            nested,
            new SqsAsyncResponseOptions()));

        Assert.Null(SqsCorrelationIdExtractor.Extract(
            Delivery(body: "{not-json"),
            "{not-json",
            new SqsAsyncResponseOptions()));
    }

    [Fact]
    public void CorrelationIdExtractor_ReturnsNullForMissingOrUnreadableSources()
    {
        Assert.Null(SqsCorrelationIdExtractor.Extract(
            Delivery(),
            "{}",
            new SqsAsyncResponseOptions { CorrelationIdJsonPaths = [] }));
        Assert.Null(SqsCorrelationIdExtractor.Extract(
            Delivery(),
            "   ",
            new SqsAsyncResponseOptions()));
        Assert.Null(SqsCorrelationIdExtractor.Extract(
            Delivery(),
            "null",
            new SqsAsyncResponseOptions()));
        Assert.Null(SqsCorrelationIdExtractor.Extract(
            Delivery(),
            """{"CorrelationId":"corr"}""",
            new SqsAsyncResponseOptions { CorrelationIdJsonPaths = [" "] }));
        Assert.Null(SqsCorrelationIdExtractor.Extract(
            Delivery(),
            """{"CorrelationId":"corr"}""",
            new SqsAsyncResponseOptions { CorrelationIdJsonPaths = ["CorrelationId.Value"] }));
        Assert.Null(SqsCorrelationIdExtractor.Extract(
            Delivery(),
            """{"CustomParameters":"{bad"}""",
            new SqsAsyncResponseOptions { CorrelationIdJsonPaths = ["CustomParameters.CorrelationId"] }));
    }

    [Fact]
    public void CorrelationIdExtractor_ReadsCaseInsensitiveAndNumericJsonValues()
    {
        Assert.Equal("corr-case", SqsCorrelationIdExtractor.Extract(
            Delivery(),
            """{"CustomParameters":{"CorrelationId":"corr-case"}}""",
            new SqsAsyncResponseOptions { CorrelationIdJsonPaths = ["customparameters.correlationid"] }));
        Assert.Equal("42", SqsCorrelationIdExtractor.Extract(
            Delivery(),
            """{"CorrelationId":42}""",
            new SqsAsyncResponseOptions { CorrelationIdJsonPaths = ["CorrelationId"] }));
    }

    [Fact]
    public void CorrelationIdExtractor_Throws_WhenTouchedObjectHasExactDuplicateKey()
        // The shared JSON-path walker materializes nothing, but still reproduces this runtime's
        // JsonObject-throws-on-exact-duplicate-key behavior rather than silently resolving to one
        // of the duplicates.
        => Assert.Throws<ArgumentException>(() => SqsCorrelationIdExtractor.Extract(
            Delivery(),
            """{"CorrelationId":"1","CorrelationId":"2"}""",
            new SqsAsyncResponseOptions { CorrelationIdJsonPaths = ["CorrelationId"] }));

    [Fact]
    public void QueueAddress_ClassifiesUrlsFifoQueuesAndNames()
    {
        Assert.True(SqsQueueAddress.IsUrl("https://sqs.us-east-1.amazonaws.com/000000000000/workers"));
        Assert.True(SqsQueueAddress.IsUrl("http://localhost:4566/000000000000/workers"));
        Assert.False(SqsQueueAddress.IsUrl("workers"));

        Assert.True(SqsQueueAddress.IsFifo("workers.fifo"));
        Assert.True(SqsQueueAddress.IsFifo("https://sqs.us-east-1.amazonaws.com/000000000000/workers.fifo"));
        Assert.False(SqsQueueAddress.IsFifo("workers"));

        Assert.Equal("workers", SqsQueueAddress.QueueName("workers"));
        Assert.Equal("workers.fifo", SqsQueueAddress.QueueName("https://sqs.us-east-1.amazonaws.com/000000000000/workers.fifo"));
        Assert.Equal("workers", SqsQueueAddress.QueueName("http://localhost:4566/000000000000/workers/"));
    }

    [Fact]
    public void ClientResolver_UsesRegisteredAmazonSqsClientOrBuildsFromOptions()
    {
        var registeredClient = new Mock<IAmazonSQS>().Object;
        var providerWithClient = new ServiceCollection()
            .AddSingleton(registeredClient)
            .Configure<SqsAsyncResponseOptions>(_ => { })
            .BuildServiceProvider();

        Assert.IsType<SqsClientAdapter>(SqsClientResolver.Create(providerWithClient));

        var providerWithoutClient = new ServiceCollection()
            .Configure<SqsAsyncResponseOptions>(options =>
            {
                options.ServiceUrl = "http://localhost:4566";
                options.AccessKey = "test";
                options.SecretKey = "test";
            })
            .BuildServiceProvider();

        Assert.IsType<SqsClientAdapter>(SqsClientResolver.Create(providerWithoutClient));
    }

    [Fact]
    public async Task ClientResolver_DoesNotDisposeRegisteredClient()
    {
        var registeredClient = new Mock<IAmazonSQS>();
        var provider = new ServiceCollection()
            .AddSingleton(registeredClient.Object)
            .Configure<SqsAsyncResponseOptions>(_ => { })
            .BuildServiceProvider();

        await SqsClientResolver.Create(provider).DisposeAsync();

        registeredClient.Verify(c => c.Dispose(), Times.Never);
    }

    [Fact]
    public async Task ClientFactory_MapsRegionWithoutExplicitCredentials()
    {
        var client = SqsClientFactory.Create(new SqsAsyncResponseOptions { Region = "us-east-1" });

        Assert.IsType<SqsClientAdapter>(client);
        await client.DisposeAsync();
    }

    [Fact]
    public async Task PublicWorkerTransportConstructor_OwnsItsConfiguredClient()
    {
        var transport = new SqsWorkerTransport(Options.Create(new SqsAsyncResponseOptions
        {
            ServiceUrl = "http://localhost:4566",
            AccessKey = "test",
            SecretKey = "test"
        }));

        await transport.DisposeAsync();
        await transport.DisposeAsync();
    }

    private static void AssertInvalidCommon(
        Action<SqsAsyncResponseOptions> configure,
        string expectedOptionName)
    {
        var options = new SqsAsyncResponseOptions
        {
            WorkerQueue = "workers",
            ResponseQueue = "responses",
            PublishRetryBaseDelay = TimeSpan.FromMilliseconds(1),
            PublishRetryMaxDelay = TimeSpan.FromMilliseconds(10),
            SubscriberRetryBaseDelay = TimeSpan.FromMilliseconds(1),
            SubscriberRetryMaxDelay = TimeSpan.FromMilliseconds(10)
        };
        configure(options);

        var ex = Assert.Throws<InvalidOperationException>(() =>
            new SqsWorkerTransport(Options.Create(options), new FakeSqsClient()));

        Assert.Contains(expectedOptionName, ex.Message, StringComparison.Ordinal);
    }

    internal static AmazonSQSException TransientSqsException(string message)
        => new(
            message,
            Amazon.Runtime.ErrorType.Receiver,
            "InternalError",
            "request-id",
            HttpStatusCode.InternalServerError);

    [Fact]
    public async Task WorkerTransport_PublishAfterDispose_ThrowsTransportNamedDisposedException()
    {
        // Regression (r23): DisposeAsync used to Release then Dispose the queue-url gate.
        // SemaphoreSlim.Dispose does not complete pending WaitAsync waiters, so publishers parked
        // on the gate during dispose hung forever. The gate must stay usable: every post-dispose
        // publish wakes in turn and gets the transport-named ObjectDisposedException.
        var transport = new SqsWorkerTransport(
            Options.Create(new SqsAsyncResponseOptions { WorkerQueue = "worker-q" }),
            new FakeSqsClient());

        await transport.DisposeAsync();

        var ex = await Assert.ThrowsAsync<ObjectDisposedException>(() => transport.PublishAsync(WorkerJob("c-disposed")));
        Assert.Contains(nameof(SqsWorkerTransport), ex.ObjectName, StringComparison.Ordinal);
    }

    private static WorkerJobEnvelope WorkerJob(string correlationId)
        => new()
        {
            CorrelationId = correlationId,
            Call = new ReflectionCallDto
            {
                ServiceInterfaceFullName = typeof(IRecoverySpy).AssemblyQualifiedName!,
                MethodName = "DoWork",
                Params = [CallbackParam.ForValue(42)]
            }
        };

    private static SqsTransportDelivery Delivery(
        string body = "{}",
        IReadOnlyDictionary<string, string>? attributes = null)
        => new(
            "https://sqs.us-east-1.amazonaws.com/000000000000/responses",
            body,
            "message-id",
            "receipt-handle",
            1,
            attributes ?? new Dictionary<string, string>(StringComparer.Ordinal),
            () => ValueTask.CompletedTask,
            (_, _) => ValueTask.CompletedTask);
}
