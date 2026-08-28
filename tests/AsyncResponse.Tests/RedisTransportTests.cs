using AsyncResponse.Transports.Redis;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using StackExchange.Redis;
using System.Diagnostics;
using System.Text.Json;
using Xunit;

namespace AsyncResponse.Tests;

public class RedisTransportTests
{
    [Fact]
    public void WithRedisTransport_ReplacesWorkerTransportAndReplyTargetProvider()
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddSingleton(Mock.Of<IConnectionMultiplexer>(m =>
            m.GetDatabase(It.IsAny<int>(), It.IsAny<object?>()) == Mock.Of<IDatabase>()));

        var provider = services
            .AddAsyncResponse()
            .WithInMemoryChannel()
            .WithRedisTransport(options => options.KeyPrefix = "orders")
            .Services
            .BuildServiceProvider();

        Assert.IsType<RedisWorkerTransport>(provider.GetRequiredService<IWorkerTransport>());
        Assert.IsType<RedisReplyTargetProvider>(provider.GetRequiredService<IAsyncResponseReplyTargetProvider>());
        Assert.Equal("Redis", provider.GetRequiredService<AsyncResponseTransportMarker>().Name);
        Assert.Contains(provider.GetServices<IHostedService>(), service => service is RedisWorkerSubscriber);
        Assert.Contains(provider.GetServices<IHostedService>(), service => service is RedisResponseIngressSubscriber);
    }

    [Fact]
    public void WithRedisTransport_NullConfigure_Throws()
        => Assert.Throws<ArgumentNullException>(
            () => new ServiceCollection().AddAsyncResponse().WithRedisTransport(null!));

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    public void WorkerTransport_RequiresKeyPrefix(string value)
    {
        var options = Options.Create(new RedisAsyncResponseTransportOptions { KeyPrefix = value });

        Assert.Throws<InvalidOperationException>(() => new RedisWorkerTransport(options, new FakeRedisStreamDatabase()));
    }

    [Fact]
    public async Task WorkerTransport_PublishesSerializedJobWithCorrelationField()
    {
        var database = new FakeRedisStreamDatabase();
        var options = new RedisAsyncResponseTransportOptions
        {
            KeyPrefix = "ar",
            WorkerStream = "ar-workers",
            CorrelationIdField = "cid",
            PayloadField = "body",
            StreamMaxLength = 123
        };
        var transport = new RedisWorkerTransport(Options.Create(options), database);

        await transport.PublishAsync(WorkerJob("corr-redis", 42));

        var add = Assert.Single(database.Adds);
        Assert.Equal("ar-workers", add.Stream);
        Assert.Equal(123, add.MaxLength);
        Assert.True(add.Approximate);
        Assert.Equal("corr-redis", Field(add.Values, "cid"));
        var job = JsonSerializer.Deserialize<WorkerJobEnvelope>(Field(add.Values, "body"));
        Assert.Equal("corr-redis", job!.CorrelationId);
        Assert.Equal(nameof(IRedisWorkerSpy.OnWorkerJob), job.Call.MethodName);
    }

    [Fact]
    public async Task WorkerTransport_WithActivityListener_TagsRedisMessageId()
    {
        Activity? stopped = null;
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == AsyncResponseDiagnostics.ActivitySourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = activity =>
            {
                if (activity.OperationName == "asyncresponse.worker.publish"
                    && activity.Tags.Any(tag => tag.Key == "asyncresponse.transport" && tag.Value == "redis"))
                {
                    stopped = activity;
                }
            }
        };
        ActivitySource.AddActivityListener(listener);
        var database = new FakeRedisStreamDatabase();
        var transport = new RedisWorkerTransport(
            Options.Create(new RedisAsyncResponseTransportOptions()),
            database);

        await transport.PublishAsync(WorkerJob("corr-trace"));

        Assert.NotNull(stopped);
        Assert.Contains(stopped!.Tags, tag => tag.Key == "asyncresponse.transport" && tag.Value == "redis");
        Assert.Contains(stopped.Tags, tag => tag.Key == "messaging.message.id" && tag.Value == "1-0");
    }

    [Fact]
    public async Task WorkerTransport_RetriesTransientPublishFailure()
    {
        var database = new FakeRedisStreamDatabase { TransientAddFailuresBeforeSuccess = 1 };
        var transport = new RedisWorkerTransport(
            Options.Create(new RedisAsyncResponseTransportOptions
            {
                PublishMaxAttempts = 2,
                PublishRetryBaseDelay = TimeSpan.FromMilliseconds(1),
                PublishRetryMaxDelay = TimeSpan.FromMilliseconds(1)
            }),
            database);

        await transport.PublishAsync(WorkerJob("corr", 1));

        Assert.Equal(2, database.AddAttempts);
        Assert.Single(database.Adds);
    }

    [Fact]
    public async Task WorkerTransport_RetryAfterAmbiguousTimeout_DoesNotAppendTheJobTwice()
    {
        // Regression: XADD was retried with a server-generated entry id and no dedup identity — a
        // slow but SUCCESSFUL append surfaced as a TimeoutException (the adapter abandons the
        // in-flight command best-effort while the multiplexer keeps running it), the retry
        // appended a second entry, and the worker job executed twice. The identity is now pinned
        // outside the retry loop (MongoDB/ASB/SQS parity) and the retried append is a no-op.
        var database = new FakeRedisStreamDatabase { AmbiguousTimeoutsBeforeSuccess = 1 };
        var transport = new RedisWorkerTransport(
            Options.Create(new RedisAsyncResponseTransportOptions
            {
                PublishMaxAttempts = 3,
                PublishRetryBaseDelay = TimeSpan.FromMilliseconds(1),
                PublishRetryMaxDelay = TimeSpan.FromMilliseconds(1)
            }),
            database);

        await transport.PublishAsync(WorkerJob("corr-ambiguous", 1));

        Assert.Single(database.Adds);
        Assert.Equal(2, database.AddOnceDedupKeys.Count);
        Assert.Single(database.AddOnceDedupKeys.Distinct());
    }

    [Fact]
    public async Task WorkerTransport_WhenPublishFailsAfterRetries_Rethrows()
    {
        var database = new FakeRedisStreamDatabase { TransientAddFailuresBeforeSuccess = 5 };
        var transport = new RedisWorkerTransport(
            Options.Create(new RedisAsyncResponseTransportOptions
            {
                PublishMaxAttempts = 2,
                PublishRetryBaseDelay = TimeSpan.FromMilliseconds(1),
                PublishRetryMaxDelay = TimeSpan.FromMilliseconds(1)
            }),
            database);

        await Assert.ThrowsAsync<RedisConnectionException>(() => transport.PublishAsync(WorkerJob("corr", 1)));
        Assert.Equal(2, database.AddAttempts);
    }

    [Fact]
    public async Task WorkerTransport_PublishNullJob_Throws()
    {
        var transport = new RedisWorkerTransport(
            Options.Create(new RedisAsyncResponseTransportOptions()),
            new FakeRedisStreamDatabase());

        await Assert.ThrowsAsync<ArgumentNullException>(() => transport.PublishAsync(null!));
    }

    [Fact]
    public async Task WorkerTransport_WhenCorrelationIdBlank_OmitsCorrelationField()
    {
        var database = new FakeRedisStreamDatabase();
        var transport = new RedisWorkerTransport(
            Options.Create(new RedisAsyncResponseTransportOptions
            {
                CorrelationIdField = "cid",
                PayloadField = "body"
            }),
            database);

        await transport.PublishAsync(WorkerJob(" "));

        var add = Assert.Single(database.Adds);
        Assert.Equal("body", Assert.Single(add.Values).Name);
    }

    [Theory]
    [MemberData(nameof(InvalidCommonOptions))]
    public void RedisOptionsValidator_RejectsInvalidCommonOptions(
        Action<RedisAsyncResponseTransportOptions> configure,
        string expectedMessageFragment)
    {
        var options = new RedisAsyncResponseTransportOptions();
        configure(options);

        var ex = Assert.Throws<InvalidOperationException>(() => RedisTransportOptionsValidator.ValidateCommon(options));

        Assert.Contains(expectedMessageFragment, ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void RedisOptionsValidator_AllowsNullMaxLengths()
    {
        var options = new RedisAsyncResponseTransportOptions
        {
            StreamMaxLength = null,
            DeadLetterStreamMaxLength = null
        };

        RedisTransportOptionsValidator.ValidateCommon(options);
    }

    [Fact]
    public void RedisTransportRetry_ClassifiesTransientExceptions()
    {
        Assert.True(RedisTransportRetry.IsTransient(new RedisConnectionException(
            ConnectionFailureType.UnableToConnect, CommandFlags.None, "down", null, CommandStatus.Unknown)));
        Assert.True(RedisTransportRetry.IsTransient(new RedisTimeoutException(
            CommandFlags.None, "timeout", CommandStatus.Unknown)));
        Assert.True(RedisTransportRetry.IsTransient(new TimeoutException()));
        // Cancellation is intentional, not transient: a cancelled command must propagate, not be retried.
        Assert.False(RedisTransportRetry.IsTransient(new OperationCanceledException()));
        Assert.False(RedisTransportRetry.IsTransient(new InvalidOperationException()));
    }

    [Fact]
    public void ReplyTargetProvider_UsesResolvedResponseStreamAsDefaultTarget()
    {
        var provider = new RedisReplyTargetProvider(Options.Create(new RedisAsyncResponseTransportOptions
        {
            KeyPrefix = "sample",
            ResponseConsumerGroup = "responses",
            PayloadField = "body",
            CorrelationIdField = "cid"
        }));

        var target = provider.GetReplyTarget();

        Assert.Equal("default", target.Name);
        Assert.Equal(RedisAsyncResponseTransportOptions.TransportName, target.Transport);
        Assert.Equal("sample:transport:response", target.Address);
        Assert.Equal("sample:transport:response", target.Properties["stream"]);
        Assert.Equal("responses", target.Properties["consumerGroup"]);
        Assert.Equal("body", target.Properties["payloadField"]);
        Assert.Equal("cid", target.Properties["correlationIdField"]);
    }

    [Fact]
    public void ReplyTargetProvider_ResolvesNamedTargets()
    {
        var options = new RedisAsyncResponseTransportOptions();
        options.AddReplyTarget("regional", "regional:responses");
        options.ReplyTargets["regional"].ConsumerGroup = "regional-group";
        options.ReplyTargets["regional"].Properties["region"] = "us";

        var target = new RedisReplyTargetProvider(Options.Create(options)).GetReplyTarget("regional");

        Assert.Equal("regional", target.Name);
        Assert.Equal("regional:responses", target.Address);
        Assert.Equal("regional-group", target.Properties["consumerGroup"]);
        Assert.Equal("us", target.Properties["region"]);
    }

    [Fact]
    public void ReplyTargetProvider_WhitespaceNameUsesDefaultTarget()
    {
        var target = new RedisReplyTargetProvider(Options.Create(new RedisAsyncResponseTransportOptions
        {
            KeyPrefix = "sample"
        })).GetReplyTarget(" ");

        Assert.Equal("default", target.Name);
        Assert.Equal("sample:transport:response", target.Address);
    }

    [Fact]
    public void ReplyTargetProvider_NamedTargetFallsBackToDefaultConsumerGroup()
    {
        var options = new RedisAsyncResponseTransportOptions
        {
            ResponseConsumerGroup = "fallback-group"
        };
        options.AddReplyTarget("regional", "regional:responses");

        var target = new RedisReplyTargetProvider(Options.Create(options)).GetReplyTarget("regional");

        Assert.Equal("fallback-group", target.Properties["consumerGroup"]);
    }

    [Fact]
    public void ReplyTargetProvider_WhenNamedTargetMissing_Throws()
    {
        var provider = new RedisReplyTargetProvider(Options.Create(new RedisAsyncResponseTransportOptions()));

        var ex = Assert.Throws<InvalidOperationException>(() => provider.GetReplyTarget("missing"));

        Assert.Contains("missing", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void CorrelationExtractor_ReadsFieldBeforeJson()
    {
        var entry = Entry("1-0",
            ("payload", """{"CorrelationId":"from-json"}"""),
            ("correlationId", "from-field"));

        var correlationId = RedisCorrelationIdExtractor.Extract(
            entry,
            """{"CorrelationId":"from-json"}""",
            new RedisAsyncResponseTransportOptions());

        Assert.Equal("from-field", correlationId);
    }

    [Fact]
    public void CorrelationExtractor_ReadsNestedJsonString()
    {
        var json = """
        {
          "PubSubParams": {
            "CustomParameters": "{\"CorrelationId\":\"corr-nested\"}"
          }
        }
        """;

        var correlationId = RedisCorrelationIdExtractor.Extract(
            Entry("1-0", ("payload", json)),
            json,
            new RedisAsyncResponseTransportOptions());

        Assert.Equal("corr-nested", correlationId);
    }

    [Fact]
    public void CorrelationExtractor_InvalidJson_ReturnsNull()
    {
        var correlationId = RedisCorrelationIdExtractor.Extract(
            Entry("1-0", ("payload", """{"CorrelationId": }""")),
            """{"CorrelationId": }""",
            new RedisAsyncResponseTransportOptions());

        Assert.Null(correlationId);
    }

    [Fact]
    public void CorrelationExtractor_NoJsonPaths_ReturnsNull()
    {
        var correlationId = RedisCorrelationIdExtractor.Extract(
            Entry("1-0", ("payload", """{"CorrelationId":"corr"}""")),
            """{"CorrelationId":"corr"}""",
            new RedisAsyncResponseTransportOptions { CorrelationIdJsonPaths = [] });

        Assert.Null(correlationId);
    }

    [Fact]
    public void CorrelationExtractor_WhitespaceBody_ReturnsNull()
    {
        var correlationId = RedisCorrelationIdExtractor.Extract(
            Entry("1-0", ("payload", " ")),
            " ",
            new RedisAsyncResponseTransportOptions());

        Assert.Null(correlationId);
    }

    [Fact]
    public void CorrelationExtractor_NullJsonRoot_ReturnsNull()
    {
        var correlationId = RedisCorrelationIdExtractor.Extract(
            Entry("1-0", ("payload", "null")),
            "null",
            new RedisAsyncResponseTransportOptions());

        Assert.Null(correlationId);
    }

    [Fact]
    public void CorrelationExtractor_NoMatchingPath_ReturnsNull()
    {
        var correlationId = RedisCorrelationIdExtractor.Extract(
            Entry("1-0", ("payload", """{"Other":"corr"}""")),
            """{"Other":"corr"}""",
            new RedisAsyncResponseTransportOptions { CorrelationIdJsonPaths = ["Missing"] });

        Assert.Null(correlationId);
    }

    [Fact]
    public void CorrelationExtractor_CaseInsensitivePropertyMatch()
    {
        var correlationId = RedisCorrelationIdExtractor.Extract(
            Entry("1-0", ("payload", """{"CorrelationId":"corr-case"}""")),
            """{"CorrelationId":"corr-case"}""",
            new RedisAsyncResponseTransportOptions { CorrelationIdJsonPaths = ["correlationid"] });

        Assert.Equal("corr-case", correlationId);
    }

    [Fact]
    public void CorrelationExtractor_NonObjectMidPath_ReturnsNull()
    {
        var correlationId = RedisCorrelationIdExtractor.Extract(
            Entry("1-0", ("payload", """{"CorrelationId":"corr"}""")),
            """{"CorrelationId":"corr"}""",
            new RedisAsyncResponseTransportOptions { CorrelationIdJsonPaths = ["CorrelationId.Value"] });

        Assert.Null(correlationId);
    }

    [Fact]
    public void CorrelationExtractor_NonStringJsonValue_ReturnsStringForm()
    {
        var correlationId = RedisCorrelationIdExtractor.Extract(
            Entry("1-0", ("payload", """{"CorrelationId":123}""")),
            """{"CorrelationId":123}""",
            new RedisAsyncResponseTransportOptions());

        Assert.Equal("123", correlationId);
    }

    [Fact]
    public void CorrelationExtractor_InvalidNestedJsonStringMidPath_ReturnsNull()
    {
        var correlationId = RedisCorrelationIdExtractor.Extract(
            Entry("1-0", ("payload", """{"CustomParameters":"{not-json"}""")),
            """{"CustomParameters":"{not-json"}""",
            new RedisAsyncResponseTransportOptions { CorrelationIdJsonPaths = ["CustomParameters.CorrelationId"] });

        Assert.Null(correlationId);
    }

    [Fact]
    public void CorrelationExtractor_NestedJsonStringArrayMidPath_ReturnsNull()
    {
        var correlationId = RedisCorrelationIdExtractor.Extract(
            Entry("1-0", ("payload", """{"CustomParameters":"[]"}""")),
            """{"CustomParameters":"[]"}""",
            new RedisAsyncResponseTransportOptions { CorrelationIdJsonPaths = ["CustomParameters.CorrelationId"] });

        Assert.Null(correlationId);
    }

    [Fact]
    public void CorrelationExtractor_NestedJsonStringWhitespaceMidPath_ReturnsNull()
    {
        var correlationId = RedisCorrelationIdExtractor.Extract(
            Entry("1-0", ("payload", """{"CustomParameters":"   "}""")),
            """{"CustomParameters":"   "}""",
            new RedisAsyncResponseTransportOptions { CorrelationIdJsonPaths = ["CustomParameters.CorrelationId"] });

        Assert.Null(correlationId);
    }

    [Fact]
    public void CorrelationExtractor_WhitespacePath_ReturnsNull()
    {
        var correlationId = RedisCorrelationIdExtractor.Extract(
            Entry("1-0", ("payload", """{"CorrelationId":"corr"}""")),
            """{"CorrelationId":"corr"}""",
            new RedisAsyncResponseTransportOptions { CorrelationIdJsonPaths = [" "] });

        Assert.Null(correlationId);
    }

    [Fact]
    public void CorrelationExtractor_ReturnsNull_WhenTouchedObjectHasExactDuplicateKey()
    {
        // An object with a duplicate key cannot resolve a property, so the id is simply not in this
        // body: extraction reports "not found" and the ingress acknowledges the message as
        // unroutable. Throwing made it a handler failure, which on RabbitMQ's default cap of 0
        // requeued forever.
        const string json = """{"CorrelationId":"1","CorrelationId":"2"}""";

        Assert.Null(RedisCorrelationIdExtractor.Extract(
            Entry("1-0", ("payload", json)),
            json,
            new RedisAsyncResponseTransportOptions()));
    }

    public static TheoryData<Action<RedisAsyncResponseTransportOptions>, string> InvalidCommonOptions()
        => new()
        {
            { options => options.KeyPrefix = "", nameof(RedisAsyncResponseTransportOptions.KeyPrefix) },
            // The dead-letter stream must never resolve to a live one: streams fan out to every
            // consumer group, so a dead-letter XADD into the worker stream comes back as a fresh
            // Attempt=1 entry (an unbounded fail/dead-letter/re-read loop), and into the response
            // stream it completes live waiters with worker envelopes. Both the explicit and the
            // derived-default collision must be caught.
            {
                options =>
                {
                    options.WorkerStream = "streams:live";
                    options.DeadLetterStream = "streams:live";
                },
                nameof(RedisAsyncResponseTransportOptions.DeadLetterStream)
            },
            {
                // Explicit worker stream colliding with the DERIVED dead-letter default
                // ({KeyPrefix}:transport:deadletter): only resolved-name comparison sees it.
                options => options.WorkerStream = "asyncresponse:transport:deadletter",
                nameof(RedisAsyncResponseTransportOptions.DeadLetterStream)
            },
            {
                options =>
                {
                    options.ResponseStream = "streams:replies";
                    options.DeadLetterStream = "streams:replies";
                },
                nameof(RedisAsyncResponseTransportOptions.DeadLetterStream)
            },
            { options => options.WorkerConsumerGroup = "", nameof(RedisAsyncResponseTransportOptions.WorkerConsumerGroup) },
            { options => options.ResponseConsumerGroup = "", nameof(RedisAsyncResponseTransportOptions.ResponseConsumerGroup) },
            { options => options.CorrelationIdField = "", nameof(RedisAsyncResponseTransportOptions.CorrelationIdField) },
            { options => options.PayloadField = "", nameof(RedisAsyncResponseTransportOptions.PayloadField) },
            { options => options.DefaultReplyTargetName = "", nameof(RedisAsyncResponseTransportOptions.DefaultReplyTargetName) },
            { options => options.OperationTimeout = TimeSpan.Zero, nameof(RedisAsyncResponseTransportOptions.OperationTimeout) },
            { options => options.StreamMaxLength = 0, nameof(RedisAsyncResponseTransportOptions.StreamMaxLength) },
            { options => options.DeadLetterStreamMaxLength = -1, nameof(RedisAsyncResponseTransportOptions.DeadLetterStreamMaxLength) },
            { options => options.PublishMaxAttempts = 0, nameof(RedisAsyncResponseTransportOptions.PublishMaxAttempts) },
            {
                options =>
                {
                    options.PublishRetryBaseDelay = TimeSpan.FromSeconds(2);
                    options.PublishRetryMaxDelay = TimeSpan.FromSeconds(1);
                },
                nameof(RedisAsyncResponseTransportOptions.PublishRetryBaseDelay)
            },
            {
                options =>
                {
                    options.SubscriberRetryBaseDelay = TimeSpan.FromSeconds(2);
                    options.SubscriberRetryMaxDelay = TimeSpan.FromSeconds(1);
                },
                nameof(RedisAsyncResponseTransportOptions.SubscriberRetryBaseDelay)
            },
            { options => options.HostShutdownTimeout = TimeSpan.Zero, nameof(RedisAsyncResponseTransportOptions.HostShutdownTimeout) }
        };

    internal static WorkerJobEnvelope WorkerJob(string correlationId, int id = 1)
        => new()
        {
            CorrelationId = correlationId,
            Call = new ReflectionCallDto
            {
                ServiceInterfaceFullName = typeof(IRedisWorkerSpy).FullName!,
                MethodName = nameof(IRedisWorkerSpy.OnWorkerJob),
                Params = [CallbackParam.ForValue(id)]
            }
        };

    internal static StreamEntry Entry(string id, params (string Name, string Value)[] values)
        => new(id, values.Select(value => new NameValueEntry(value.Name, value.Value)).ToArray());

    internal static string Field(NameValueEntry[] values, string name)
        => values.Single(value => value.Name == name).Value.ToString();

    public interface IRedisWorkerSpy
    {
        Task OnWorkerJob(int id);
    }

    internal sealed class FakeRedisStreamDatabase : IRedisStreamDatabase
    {
        public List<AddCall> Adds { get; } = [];
        public List<CancellationToken> AddTokens { get; } = [];
        public List<AckCall> Acks { get; } = [];
        public List<CancellationToken> AckTokens { get; } = [];
        public Queue<StreamEntry[]> ReadBatches { get; } = new();
        public List<CreateGroupCall> CreateGroupCalls { get; } = [];
        public List<ReadGroupCall> ReadGroupCalls { get; } = [];
        public List<PendingCall> PendingCalls { get; } = [];
        public List<ClaimCall> ClaimCalls { get; } = [];
        public StreamPendingMessageInfo[] PendingMessages { get; set; } = [];
        public StreamEntry[] ClaimedMessages { get; set; } = [];
        public int AddAttempts { get; private set; }
        public int TransientAddFailuresBeforeSuccess { get; set; }

        /// <summary>Dedup keys passed to StreamAddOnceAsync, in call order (retries repeat the key).</summary>
        public List<string> AddOnceDedupKeys { get; } = [];

        /// <summary>Markers committed atomically with an applied append (StreamAddOnceAsync semantics).</summary>
        public HashSet<string> CommittedDedupKeys { get; } = new(StringComparer.Ordinal);

        /// <summary>
        /// Simulates the ambiguous timeout: the append IS applied server-side (and its marker
        /// committed), then the call surfaces a TimeoutException as if the adapter abandoned it.
        /// </summary>
        public int AmbiguousTimeoutsBeforeSuccess { get; set; }
        public TaskCompletionSource? FirstReadStarted { get; set; }
        public Exception? AddException { get; set; }
        public Exception? AckException { get; set; }
        public Exception? CreateConsumerGroupException { get; set; }
        public int ReadFailuresBeforeSuccess { get; set; }

        public Task<RedisValue> StreamAddAsync(
            RedisKey stream,
            NameValueEntry[] values,
            long? maxLength,
            bool useApproximateMaxLength,
            CancellationToken cancellationToken)
        {
            AddAttempts++;
            AddTokens.Add(cancellationToken);
            if (AddAttempts <= TransientAddFailuresBeforeSuccess)
                throw new RedisConnectionException(
                    ConnectionFailureType.UnableToConnect, CommandFlags.None, "transient", null, CommandStatus.Unknown);
            if (AddException is not null)
                throw AddException;

            Adds.Add(new AddCall(stream.ToString(), values, maxLength, useApproximateMaxLength));
            return Task.FromResult<RedisValue>($"{AddAttempts}-0");
        }

        public async Task<RedisValue> StreamAddOnceAsync(
            RedisKey stream,
            RedisKey dedupKey,
            TimeSpan dedupTtl,
            NameValueEntry[] values,
            long? maxLength,
            bool useApproximateMaxLength,
            CancellationToken cancellationToken)
        {
            var key = dedupKey.ToString();
            AddOnceDedupKeys.Add(key);
            if (CommittedDedupKeys.Contains(key))
            {
                AddAttempts++;
                AddTokens.Add(cancellationToken);
                return RedisValue.Null;
            }

            var id = await StreamAddAsync(stream, values, maxLength, useApproximateMaxLength, cancellationToken);
            CommittedDedupKeys.Add(key);

            if (AmbiguousTimeoutsBeforeSuccess > 0)
            {
                AmbiguousTimeoutsBeforeSuccess--;
                throw new TimeoutException("The Redis command did not complete within the operation timeout.");
            }

            return id;
        }

        public Task<bool> StreamCreateConsumerGroupAsync(
            RedisKey stream,
            RedisValue groupName,
            RedisValue position,
            bool createStream,
            CancellationToken cancellationToken)
        {
            CreateGroupCalls.Add(new CreateGroupCall(stream.ToString(), groupName.ToString(), position.ToString(), createStream));
            if (CreateConsumerGroupException is not null)
                throw CreateConsumerGroupException;

            return Task.FromResult(true);
        }

        public Task<StreamEntry[]> StreamReadGroupAsync(
            RedisKey stream,
            RedisValue groupName,
            RedisValue consumerName,
            int count,
            CancellationToken cancellationToken)
        {
            FirstReadStarted?.TrySetResult();
            ReadGroupCalls.Add(new ReadGroupCall(stream.ToString(), groupName.ToString(), consumerName.ToString(), count));
            if (ReadFailuresBeforeSuccess > 0)
            {
                ReadFailuresBeforeSuccess--;
                throw new InvalidOperationException("read failed");
            }

            return Task.FromResult(ReadBatches.Count > 0 ? ReadBatches.Dequeue() : []);
        }

        public Task<long> StreamAcknowledgeAsync(
            RedisKey stream,
            RedisValue groupName,
            RedisValue messageId,
            CancellationToken cancellationToken)
        {
            if (AckException is not null)
                throw AckException;

            Acks.Add(new AckCall(stream.ToString(), groupName.ToString(), messageId.ToString()));
            AckTokens.Add(cancellationToken);
            return Task.FromResult(1L);
        }

        public Task<StreamPendingMessageInfo[]> StreamPendingMessagesAsync(
            RedisKey stream,
            RedisValue groupName,
            int count,
            RedisValue consumerName,
            RedisValue? minId,
            RedisValue? maxId,
            long minIdleTimeInMilliseconds,
            CancellationToken cancellationToken)
        {
            PendingCalls.Add(new PendingCall(
                stream.ToString(),
                groupName.ToString(),
                count,
                consumerName.ToString(),
                minId?.ToString(),
                maxId?.ToString(),
                minIdleTimeInMilliseconds));
            return Task.FromResult(PendingMessages);
        }

        public Task<StreamEntry[]> StreamClaimAsync(
            RedisKey stream,
            RedisValue groupName,
            RedisValue consumerName,
            long minIdleTimeInMilliseconds,
            RedisValue[] messageIds,
            CancellationToken cancellationToken)
        {
            ClaimCalls.Add(new ClaimCall(
                stream.ToString(),
                groupName.ToString(),
                consumerName.ToString(),
                minIdleTimeInMilliseconds,
                messageIds.Select(id => id.ToString()).ToArray()));
            return Task.FromResult(ClaimedMessages);
        }

        public Task<RedisValue[]> StreamClaimIdsOnlyAsync(
            RedisKey stream,
            RedisValue groupName,
            RedisValue consumerName,
            long minIdleTimeInMilliseconds,
            RedisValue[] messageIds,
            CancellationToken cancellationToken)
        {
            if (ClaimIdsOnlyException is not null)
                throw ClaimIdsOnlyException;

            lock (ClaimIdsOnlyCalls)
            {
                ClaimIdsOnlyCalls.Add(new ClaimCall(
                    stream.ToString(),
                    groupName.ToString(),
                    consumerName.ToString(),
                    minIdleTimeInMilliseconds,
                    messageIds.Select(id => id.ToString()).ToArray()));
            }

            return Task.FromResult(messageIds);
        }

        /// <summary>JUSTID heartbeat claims. Locked: the renewal loop runs on a background task.</summary>
        public List<ClaimCall> ClaimIdsOnlyCalls { get; } = [];

        /// <summary>When set, every JUSTID heartbeat claim throws this exception.</summary>
        public Exception? ClaimIdsOnlyException { get; set; }

        internal sealed record AddCall(string Stream, NameValueEntry[] Values, long? MaxLength, bool Approximate);
        internal sealed record AckCall(string Stream, string Group, string MessageId);
        internal sealed record CreateGroupCall(string Stream, string Group, string Position, bool CreateStream);
        internal sealed record ReadGroupCall(string Stream, string Group, string Consumer, int Count);
        internal sealed record PendingCall(
            string Stream,
            string Group,
            int Count,
            string Consumer,
            string? MinId,
            string? MaxId,
            long MinIdleTimeInMilliseconds);
        internal sealed record ClaimCall(string Stream, string Group, string Consumer, long MinIdleTimeInMilliseconds, string[] MessageIds);
    }
}
