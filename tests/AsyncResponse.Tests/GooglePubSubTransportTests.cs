using AsyncResponse.Transports.GooglePubSub;
using Google.Cloud.PubSub.V1;
using Google.Protobuf;
using Microsoft.Extensions.Options;
using Moq;
using System.Text.Json;
using Xunit;

namespace AsyncResponse.Tests;

public class GooglePubSubTransportTests
{
    [Fact]
    public void WorkerTransport_RequiresProjectAndTopic()
    {
        Assert.Throws<InvalidOperationException>(() => new GooglePubSubWorkerTransport(
            Options.Create(new GooglePubSubAsyncResponseOptions { WorkerTopicId = "jobs" })));
        Assert.Throws<InvalidOperationException>(() => new GooglePubSubWorkerTransport(
            Options.Create(new GooglePubSubAsyncResponseOptions { ProjectId = "project-a" })));
    }

    [Fact]
    public async Task WorkerTransport_DisposeBeforePublish_DoesNotBuildPublisher()
    {
        var transport = new GooglePubSubWorkerTransport(Options.Create(new GooglePubSubAsyncResponseOptions
        {
            ProjectId = "project-a",
            WorkerTopicId = "jobs"
        }));

        await transport.DisposeAsync();
        await Assert.ThrowsAsync<ArgumentNullException>(() => transport.PublishAsync(null!));
    }

    [Fact]
    public async Task WorkerTransport_PublishesSerializedJobAndShutsDownPublisher()
    {
        var publisher = new FakePublisherClient { MessageId = "message-1" };
        var options = new GooglePubSubAsyncResponseOptions
        {
            ProjectId = "project-a",
            WorkerTopicId = "jobs",
            CorrelationIdAttribute = "async-correlation",
            ShutdownTimeout = TimeSpan.FromSeconds(3)
        };
        var factoryCalls = 0;
        var transport = new GooglePubSubWorkerTransport(
            Options.Create(options),
            () =>
            {
                factoryCalls++;
                return Task.FromResult<IGooglePubSubPublisherClient>(publisher);
            });
        var job = WorkerJob("corr-123");

        await transport.PublishAsync(job);
        await transport.DisposeAsync();

        Assert.Equal(1, factoryCalls);
        var message = Assert.Single(publisher.Messages);
        Assert.Equal("corr-123", message.Attributes["async-correlation"]);
        var roundTripped = JsonSerializer.Deserialize<WorkerJobEnvelope>(message.Data.ToStringUtf8());
        Assert.NotNull(roundTripped);
        Assert.Equal("corr-123", roundTripped.CorrelationId);
        Assert.Equal("DoWork", roundTripped.Call.MethodName);
        Assert.Equal(1, publisher.ShutdownCalls);
        Assert.Equal(options.ShutdownTimeout, publisher.LastShutdownTimeout);
    }

    [Fact]
    public async Task WorkerTransport_Publish_EmitsActivityTags()
    {
        using var collector = new AsyncResponseActivityCollector();
        var publisher = new FakePublisherClient { MessageId = "message-activity" };
        var transport = new GooglePubSubWorkerTransport(
            Options.Create(new GooglePubSubAsyncResponseOptions
            {
                ProjectId = "project-a",
                WorkerTopicId = "jobs"
            }),
            () => Task.FromResult<IGooglePubSubPublisherClient>(publisher));

        await transport.PublishAsync(WorkerJob("corr-activity"));

        var activity = collector.Single("asyncresponse.worker.publish", "asyncresponse.transport", "google_pubsub");
        Assert.Equal("gcp_pubsub", AsyncResponseActivityCollector.Tag(activity, "messaging.system"));
        Assert.Equal("jobs", AsyncResponseActivityCollector.Tag(activity, "messaging.destination.name"));
        Assert.Equal("message-activity", AsyncResponseActivityCollector.Tag(activity, "messaging.message.id"));
        Assert.Equal("reply", AsyncResponseActivityCollector.Tag(activity, "asyncresponse.reply_target.name"));
        Assert.Equal(GooglePubSubAsyncResponseOptions.TransportName, AsyncResponseActivityCollector.Tag(activity, "asyncresponse.reply_target.transport"));
        Assert.Equal("DoWork", AsyncResponseActivityCollector.Tag(activity, "asyncresponse.worker.method"));
    }

    [Fact]
    public async Task WorkerTransport_SequentialPublishes_ReusePublisherAndDisposeOnce()
    {
        var publisher = new FakePublisherClient();
        var factoryCalls = 0;
        var transport = new GooglePubSubWorkerTransport(
            Options.Create(new GooglePubSubAsyncResponseOptions
            {
                ProjectId = "project-a",
                WorkerTopicId = "jobs"
            }),
            () =>
            {
                factoryCalls++;
                return Task.FromResult<IGooglePubSubPublisherClient>(publisher);
            });

        await transport.PublishAsync(WorkerJob("corr-1"));
        await transport.PublishAsync(WorkerJob("corr-2"));
        await transport.DisposeAsync();
        await transport.DisposeAsync();

        Assert.Equal(1, factoryCalls);
        Assert.Equal(2, publisher.Messages.Count);
        Assert.Equal(1, publisher.ShutdownCalls);
    }

    [Fact]
    public async Task WorkerTransport_WhenCorrelationIdIsBlank_DoesNotAddAttribute()
    {
        var publisher = new FakePublisherClient();
        var transport = new GooglePubSubWorkerTransport(
            Options.Create(new GooglePubSubAsyncResponseOptions
            {
                ProjectId = "project-a",
                WorkerTopicId = "jobs"
            }),
            () => Task.FromResult<IGooglePubSubPublisherClient>(publisher));

        await transport.PublishAsync(WorkerJob("  "));

        var message = Assert.Single(publisher.Messages);
        Assert.False(message.Attributes.ContainsKey("correlationId"));
    }

    [Fact]
    public async Task WorkerTransport_WhenPublisherFails_PropagatesException()
    {
        var failure = new InvalidOperationException("publish failed");
        var publisher = new FakePublisherClient { PublishException = failure };
        var transport = new GooglePubSubWorkerTransport(
            Options.Create(new GooglePubSubAsyncResponseOptions
            {
                ProjectId = "project-a",
                WorkerTopicId = "jobs"
            }),
            () => Task.FromResult<IGooglePubSubPublisherClient>(publisher));

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => transport.PublishAsync(WorkerJob("corr-1")));

        Assert.Same(failure, ex);
    }

    [Fact]
    public async Task WorkerTransport_WhenPublisherBuildFails_RetriesOnNextPublish()
    {
        var publisher = new FakePublisherClient();
        var factoryCalls = 0;
        var transport = new GooglePubSubWorkerTransport(
            Options.Create(new GooglePubSubAsyncResponseOptions
            {
                ProjectId = "project-a",
                WorkerTopicId = "jobs"
            }),
            () => Interlocked.Increment(ref factoryCalls) == 1
                ? Task.FromException<IGooglePubSubPublisherClient>(new InvalidOperationException("build failed"))
                : Task.FromResult<IGooglePubSubPublisherClient>(publisher));

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => transport.PublishAsync(WorkerJob("corr-1")));
        Assert.Equal("build failed", ex.Message);

        // The faulted build attempt must not be cached: the next publish rebuilds and succeeds.
        await transport.PublishAsync(WorkerJob("corr-2"));
        await transport.DisposeAsync();

        Assert.Equal(2, factoryCalls);
        var message = Assert.Single(publisher.Messages);
        Assert.Equal("corr-2", message.Attributes["correlationId"]);
        Assert.Equal(1, publisher.ShutdownCalls);
    }

    [Fact]
    public async Task WorkerTransport_ConcurrentPublishes_BuildPublisherOnce()
    {
        var publisher = new FakePublisherClient();
        var releaseFactory = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var factoryCalls = 0;
        var transport = new GooglePubSubWorkerTransport(
            Options.Create(new GooglePubSubAsyncResponseOptions
            {
                ProjectId = "project-a",
                WorkerTopicId = "jobs"
            }),
            async () =>
            {
                Interlocked.Increment(ref factoryCalls);
                await releaseFactory.Task.ConfigureAwait(false);
                return publisher;
            });

        var first = transport.PublishAsync(WorkerJob("corr-1"));
        var second = transport.PublishAsync(WorkerJob("corr-2"));
        releaseFactory.TrySetResult();
        await Task.WhenAll(first, second);

        Assert.Equal(1, factoryCalls);
        Assert.Equal(2, publisher.Messages.Count);
    }

    [Fact]
    public async Task WorkerTransport_PublishAfterDispose_Throws()
    {
        var transport = new GooglePubSubWorkerTransport(
            Options.Create(new GooglePubSubAsyncResponseOptions
            {
                ProjectId = "project-a",
                WorkerTopicId = "jobs"
            }),
            () => Task.FromResult<IGooglePubSubPublisherClient>(new FakePublisherClient()));

        await transport.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(() => transport.PublishAsync(WorkerJob("corr-late")));
    }

    [Fact]
    public async Task WorkerTransport_WhenPublishIsCanceled_PropagatesCancellation()
    {
        var publisher = new FakePublisherClient
        {
            PublishCompletion = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously)
        };
        var transport = new GooglePubSubWorkerTransport(
            Options.Create(new GooglePubSubAsyncResponseOptions
            {
                ProjectId = "project-a",
                WorkerTopicId = "jobs"
            }),
            () => Task.FromResult<IGooglePubSubPublisherClient>(publisher));
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => transport.PublishAsync(WorkerJob("corr-1"), cts.Token));
    }

    [Fact]
    public async Task PublisherClientAdapter_DelegatesPublishAndShutdown()
    {
        var message = new PubsubMessage { Data = ByteString.CopyFromUtf8("{}") };
        var timeout = TimeSpan.FromSeconds(2);
        var publisher = new Mock<PublisherClient>();
        publisher.Setup(p => p.PublishAsync(message)).ReturnsAsync("message-id");
        publisher.Setup(p => p.ShutdownAsync(timeout)).Returns(Task.CompletedTask);
        var adapter = new GooglePubSubPublisherClientAdapter(publisher.Object);

        var messageId = await adapter.PublishAsync(message, CancellationToken.None);
        await adapter.ShutdownAsync(timeout);

        Assert.Equal("message-id", messageId);
        publisher.Verify(p => p.PublishAsync(message), Times.Once);
        publisher.Verify(p => p.ShutdownAsync(timeout), Times.Once);
    }

    [Fact]
    public void ReplyTargetProvider_UsesResponseTopicAsDefaultTarget()
    {
        var provider = new GooglePubSubReplyTargetProvider(Options.Create(new GooglePubSubAsyncResponseOptions
        {
            ProjectId = "project-a",
            ResponseTopicId = "responses"
        }));

        var target = provider.GetReplyTarget();

        Assert.Equal("default", target.Name);
        Assert.Equal(GooglePubSubAsyncResponseOptions.TransportName, target.Transport);
        Assert.Equal(TopicName.FromProjectTopic("project-a", "responses").ToString(), target.Address);
        Assert.Equal("project-a", target.Properties["projectId"]);
        Assert.Equal("responses", target.Properties["topicId"]);
    }

    [Fact]
    public void ReplyTargetProvider_ResolvesNamedTargets()
    {
        var options = new GooglePubSubAsyncResponseOptions { ProjectId = "project-a" }
            .AddReplyTarget("regional-us", "project-us", "responses-us");
        options.ReplyTargets["regional-us"].Properties["region"] = "us";
        var provider = new GooglePubSubReplyTargetProvider(Options.Create(options));

        var target = provider.GetReplyTarget("regional-us");

        Assert.Equal("regional-us", target.Name);
        Assert.Equal(TopicName.FromProjectTopic("project-us", "responses-us").ToString(), target.Address);
        Assert.Equal("us", target.Properties["region"]);
    }

    [Fact]
    public void CorrelationIdExtractor_ReadsAttributeFirst()
    {
        var message = new PubsubMessage
        {
            Data = ByteString.CopyFromUtf8("""{"CorrelationId":"from-json"}""")
        };
        message.Attributes["correlationId"] = "from-attribute";

        var correlationId = GooglePubSubCorrelationIdExtractor.Extract(
            message,
            message.Data.ToStringUtf8(),
            new GooglePubSubAsyncResponseOptions());

        Assert.Equal("from-attribute", correlationId);
    }

    [Fact]
    public void CorrelationIdExtractor_ReadsOptimaticStyleNestedJson()
    {
        var message = new PubsubMessage
        {
            Data = ByteString.CopyFromUtf8(
                """
                {
                  "PubSubParams": {
                    "CustomParameters": "{\"CorrelationId\":\"corr-nested\"}"
                  }
                }
                """)
        };

        var correlationId = GooglePubSubCorrelationIdExtractor.Extract(
            message,
            message.Data.ToStringUtf8(),
            new GooglePubSubAsyncResponseOptions());

        Assert.Equal("corr-nested", correlationId);
    }

    [Fact]
    public void CorrelationIdExtractor_ReadsDirectCustomParametersValue()
    {
        var message = new PubsubMessage
        {
            Data = ByteString.CopyFromUtf8("""{"CustomParameters":"corr-direct"}""")
        };

        var correlationId = GooglePubSubCorrelationIdExtractor.Extract(
            message,
            message.Data.ToStringUtf8(),
            new GooglePubSubAsyncResponseOptions());

        Assert.Equal("corr-direct", correlationId);
    }

    [Fact]
    public void CorrelationIdExtractor_WhenJsonPathsAreNull_ReturnsNull()
    {
        var message = new PubsubMessage
        {
            Data = ByteString.CopyFromUtf8("""{"CorrelationId":"from-json"}""")
        };

        var correlationId = GooglePubSubCorrelationIdExtractor.Extract(
            message,
            message.Data.ToStringUtf8(),
            new GooglePubSubAsyncResponseOptions { CorrelationIdJsonPaths = null! });

        Assert.Null(correlationId);
    }

    [Fact]
    public void CorrelationIdExtractor_WhenJsonIsInvalid_ReturnsNull()
    {
        var correlationId = GooglePubSubCorrelationIdExtractor.Extract(
            new PubsubMessage(),
            """{"CorrelationId": }""",
            new GooglePubSubAsyncResponseOptions());

        Assert.Null(correlationId);
    }

    [Fact]
    public void CorrelationIdExtractor_WhenJsonRootIsNull_ReturnsNull()
    {
        var correlationId = GooglePubSubCorrelationIdExtractor.Extract(
            new PubsubMessage(),
            "null",
            new GooglePubSubAsyncResponseOptions());

        Assert.Null(correlationId);
    }

    [Fact]
    public void CorrelationIdExtractor_IgnoresBlankPathsAndReadsCaseInsensitiveProperty()
    {
        var correlationId = GooglePubSubCorrelationIdExtractor.Extract(
            new PubsubMessage(),
            """{"correlationid":"case-insensitive"}""",
            new GooglePubSubAsyncResponseOptions
            {
                CorrelationIdJsonPaths = [" ", "CorrelationId"]
            });

        Assert.Equal("case-insensitive", correlationId);
    }

    [Fact]
    public void CorrelationIdExtractor_ReturnsNumericJsonValueAsString()
    {
        var correlationId = GooglePubSubCorrelationIdExtractor.Extract(
            new PubsubMessage(),
            """{"CorrelationId":12345}""",
            new GooglePubSubAsyncResponseOptions());

        Assert.Equal("12345", correlationId);
    }

    [Fact]
    public void CorrelationIdExtractor_WhenPathCannotTraverseObject_ReturnsNull()
    {
        var correlationId = GooglePubSubCorrelationIdExtractor.Extract(
            new PubsubMessage(),
            """{"CorrelationId":"corr-1"}""",
            new GooglePubSubAsyncResponseOptions
            {
                CorrelationIdJsonPaths = ["CorrelationId.Value"]
            });

        Assert.Null(correlationId);
    }

    [Fact]
    public void CorrelationIdExtractor_WhenNestedJsonStringIsInvalid_FallsBackToNull()
    {
        var correlationId = GooglePubSubCorrelationIdExtractor.Extract(
            new PubsubMessage(),
            """{"CustomParameters":"{\"CorrelationId\":"}""",
            new GooglePubSubAsyncResponseOptions
            {
                CorrelationIdJsonPaths = ["CustomParameters.CorrelationId"]
            });

        Assert.Null(correlationId);
    }

    [Fact]
    public void CorrelationIdExtractor_WhenNestedJsonStringIsArray_ReturnsNull()
    {
        var correlationId = GooglePubSubCorrelationIdExtractor.Extract(
            new PubsubMessage(),
            """{"CustomParameters":"[\"corr-array\"]"}""",
            new GooglePubSubAsyncResponseOptions
            {
                CorrelationIdJsonPaths = ["CustomParameters.CorrelationId"]
            });

        Assert.Null(correlationId);
    }

    [Fact]
    public void CorrelationIdExtractor_ReturnsNull_WhenTouchedObjectHasExactDuplicateKey()
    {
        // An object with a duplicate key cannot resolve a property, so the id is simply not in this
        // body: extraction reports "not found" and the ingress acknowledges the message as
        // unroutable. Throwing made it a handler failure, which on RabbitMQ's default cap of 0
        // requeued forever.
        var message = new PubsubMessage
        {
            Data = ByteString.CopyFromUtf8("""{"CorrelationId":"1","CorrelationId":"2"}""")
        };

        Assert.Null(GooglePubSubCorrelationIdExtractor.Extract(
            message,
            message.Data.ToStringUtf8(),
            new GooglePubSubAsyncResponseOptions()));
    }

    [Fact]
    public async Task WorkerTransport_PublishAfterDispose_ThrowsTransportNamedDisposedException()
    {
        // Regression (r23): DisposeAsync used to Release then Dispose the publisher gate.
        // SemaphoreSlim.Dispose does not complete pending WaitAsync waiters, so publishers parked
        // on the gate during dispose hung forever. The gate must stay usable: every post-dispose
        // publish wakes in turn and gets the transport-named ObjectDisposedException.
        var transport = new GooglePubSubWorkerTransport(
            Options.Create(new GooglePubSubAsyncResponseOptions
            {
                ProjectId = "project-a",
                WorkerTopicId = "jobs"
            }),
            () => Task.FromResult<IGooglePubSubPublisherClient>(new FakePublisherClient()));

        await transport.DisposeAsync();

        var ex = await Assert.ThrowsAsync<ObjectDisposedException>(() => transport.PublishAsync(WorkerJob("c-disposed")));
        Assert.Contains(nameof(GooglePubSubWorkerTransport), ex.ObjectName, StringComparison.Ordinal);
    }

    [Fact]
    public async Task PublishAsync_AlreadyCancelledToken_DoesNotHandTheMessageToTheClient()
    {
        // Regression (r23): the adapter seam had no CancellationToken, so a publish cancelled at
        // shutdown was still handed to PublisherClient's local batch queue — and DisposeAsync's
        // ShutdownAsync then actively flushed a message whose caller was told nothing was sent.
        // The token now travels through the seam and is checked before the hand-off.
        var publisher = new FakePublisherClient();
        var transport = new GooglePubSubWorkerTransport(
            Options.Create(new GooglePubSubAsyncResponseOptions
            {
                ProjectId = "project-a",
                WorkerTopicId = "jobs"
            }),
            () => Task.FromResult<IGooglePubSubPublisherClient>(publisher));
        using var cancelled = new CancellationTokenSource();
        cancelled.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => transport.PublishAsync(WorkerJob("c-cancelled"), cancelled.Token));

        Assert.Empty(publisher.Messages);
        await transport.DisposeAsync();
    }

    private static WorkerJobEnvelope WorkerJob(string? correlationId)
        => new()
        {
            CorrelationId = correlationId,
            ReplyTarget = new AsyncResponseReplyTarget
            {
                Name = "reply",
                Transport = GooglePubSubAsyncResponseOptions.TransportName,
                Address = "projects/project-a/topics/responses",
                Properties = new Dictionary<string, string> { ["topicId"] = "responses" }
            },
            Call = new ReflectionCallDto
            {
                ServiceInterfaceFullName = "AsyncResponse.Tests.IWorker",
                MethodName = "DoWork",
                Params = [CallbackParam.ForValue(42)]
            }
        };

    private sealed class FakePublisherClient : IGooglePubSubPublisherClient
    {
        private readonly object _gate = new();
        private readonly List<PubsubMessage> _messages = [];

        public IReadOnlyList<PubsubMessage> Messages
        {
            get
            {
                lock (_gate)
                    return [.. _messages];
            }
        }

        public string MessageId { get; init; } = "message-id";
        public Exception? PublishException { get; init; }
        public TaskCompletionSource<string>? PublishCompletion { get; init; }
        public int ShutdownCalls { get; private set; }
        public TimeSpan? LastShutdownTimeout { get; private set; }

        public Task<string> PublishAsync(PubsubMessage message, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            lock (_gate)
                _messages.Add(message);

            if (PublishException is not null)
                return Task.FromException<string>(PublishException);

            return PublishCompletion?.Task ?? Task.FromResult(MessageId);
        }

        public Task ShutdownAsync(TimeSpan timeout)
        {
            ShutdownCalls++;
            LastShutdownTimeout = timeout;
            return Task.CompletedTask;
        }
    }
}
