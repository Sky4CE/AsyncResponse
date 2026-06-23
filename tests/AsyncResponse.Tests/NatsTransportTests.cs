using AsyncResponse.Transports.NATS;
using Microsoft.Extensions.Options;
using System.Text.Json;
using Xunit;

namespace AsyncResponse.Tests;

public class NatsTransportOptionsAndSchemaTests
{
    [Fact]
    public void ValidateCommon_Passes_ForDefaults()
        => NatsTransportOptionsValidator.ValidateCommon(new NatsAsyncResponseTransportOptions());

    [Theory]
    [InlineData("ap p")]
    [InlineData("ap*p")]
    [InlineData("app>")]
    public void ValidateCommon_Throws_ForWildcardPrefix(string prefix)
        => Assert.Throws<InvalidOperationException>(() =>
            NatsTransportOptionsValidator.ValidateCommon(new NatsAsyncResponseTransportOptions { SubjectPrefix = prefix }));

    [Fact]
    public void ValidateCommon_Throws_WhenRetryBaseExceedsMax()
        => Assert.Throws<InvalidOperationException>(() => NatsTransportOptionsValidator.ValidateCommon(
            new NatsAsyncResponseTransportOptions
            {
                PublishRetryBaseDelay = TimeSpan.FromSeconds(5),
                PublishRetryMaxDelay = TimeSpan.FromSeconds(1)
            }));

    [Fact]
    public void ValidateCommon_Throws_ForNonPositiveAckWait()
        => Assert.Throws<InvalidOperationException>(() => NatsTransportOptionsValidator.ValidateCommon(
            new NatsAsyncResponseTransportOptions { AckWait = TimeSpan.Zero }));

    [Fact]
    public void ValidateSubscriber_RequiresBackgroundSettingsForEarlyAck()
    {
        var subscriber = new NatsSubscriberOptions { AckMode = NatsAckMode.AckAfterReceive };
        Assert.Throws<InvalidOperationException>(() => NatsTransportOptionsValidator.ValidateSubscriber(subscriber, "Worker"));

        subscriber.UseAckAfterReceive(backgroundWorkerCount: 2, backgroundQueueCapacity: 8);
        NatsTransportOptionsValidator.ValidateSubscriber(subscriber, "Worker"); // now valid
    }

    [Fact]
    public void ValidateSubscriber_Throws_ForNonPositiveBatchSize()
        => Assert.Throws<InvalidOperationException>(() =>
            NatsTransportOptionsValidator.ValidateSubscriber(new NatsSubscriberOptions { BatchSize = 0 }, "Worker"));

    [Fact]
    public void SubjectSchema_DerivesDefaultsFromPrefix()
    {
        var schema = new NatsTransportSubjectSchema(new NatsAsyncResponseTransportOptions { SubjectPrefix = "myapp" });

        Assert.Equal("myapp.transport.worker", schema.WorkerSubject);
        Assert.Equal("myapp.transport.response", schema.ResponseSubject);
        Assert.Equal("myapp.transport.deadletter", schema.DeadLetterSubject);

        // Stream names cannot contain dots; the subject's dots become underscores.
        Assert.Equal("myapp_transport_worker", schema.WorkerStream);
        Assert.Equal("myapp_transport_response", schema.ResponseStream);
        Assert.Equal("myapp_transport_deadletter", schema.DeadLetterStream);
    }

    [Fact]
    public void SubjectSchema_HonorsExplicitOverrides()
    {
        var schema = new NatsTransportSubjectSchema(new NatsAsyncResponseTransportOptions
        {
            WorkerSubject = "jobs.work",
            WorkerStream = "JOBS_WORK"
        });

        Assert.Equal("jobs.work", schema.WorkerSubject);
        Assert.Equal("JOBS_WORK", schema.WorkerStream);
    }

    [Fact]
    public void SanitizeStreamName_ReplacesIllegalCharacters()
        => Assert.Equal("a_b_c_d", NatsTransportSubjectSchema.SanitizeStreamName("a.b*c>d"));

    [Fact]
    public void ValidateCommon_Throws_ForNonPositivePublishAttempts()
        => Assert.Throws<InvalidOperationException>(() => NatsTransportOptionsValidator.ValidateCommon(
            new NatsAsyncResponseTransportOptions { PublishMaxAttempts = 0 }));

    [Fact]
    public void ValidateCommon_Throws_ForNonPositiveShutdownTimeout()
        => Assert.Throws<InvalidOperationException>(() => NatsTransportOptionsValidator.ValidateCommon(
            new NatsAsyncResponseTransportOptions { ShutdownTimeout = TimeSpan.Zero }));

    [Fact]
    public void ValidateCommon_Throws_ForNegativeStreamMaxMessages()
        => Assert.Throws<InvalidOperationException>(() => NatsTransportOptionsValidator.ValidateCommon(
            new NatsAsyncResponseTransportOptions { StreamMaxMessages = -1 }));

    [Fact]
    public void ValidateCommon_Throws_WhenSubscriberRetryBaseExceedsMax()
        => Assert.Throws<InvalidOperationException>(() => NatsTransportOptionsValidator.ValidateCommon(
            new NatsAsyncResponseTransportOptions
            {
                SubscriberRetryBaseDelay = TimeSpan.FromSeconds(5),
                SubscriberRetryMaxDelay = TimeSpan.FromSeconds(1)
            }));

    [Fact]
    public void ValidateCommon_Throws_ForMissingWorkerConsumer()
        => Assert.Throws<InvalidOperationException>(() => NatsTransportOptionsValidator.ValidateCommon(
            new NatsAsyncResponseTransportOptions { WorkerConsumer = " " }));

    [Fact]
    public void ValidateSubscriber_Throws_ForNegativeMaxDeliveryAttempts()
        => Assert.Throws<InvalidOperationException>(() => NatsTransportOptionsValidator.ValidateSubscriber(
            new NatsSubscriberOptions { MaxDeliveryAttempts = -1 }, "Worker"));

    [Fact]
    public void ValidateSubscriber_Throws_ForNonPositiveRedeliveryDelay()
        => Assert.Throws<InvalidOperationException>(() => NatsTransportOptionsValidator.ValidateSubscriber(
            new NatsSubscriberOptions { RedeliveryDelay = TimeSpan.Zero }, "Worker"));

    [Fact]
    public void ValidateSubscriber_Throws_ForUnsupportedAckMode()
        => Assert.Throws<InvalidOperationException>(() => NatsTransportOptionsValidator.ValidateSubscriber(
            new NatsSubscriberOptions { AckMode = (NatsAckMode)999 }, "Worker"));
}

public class NatsReplyTargetProviderTests
{
    [Fact]
    public void GetReplyTarget_Default_UsesResponseSubject()
    {
        var provider = new NatsReplyTargetProvider(Options.Create(new NatsAsyncResponseTransportOptions { SubjectPrefix = "app" }));

        var target = provider.GetReplyTarget();

        Assert.Equal("default", target.Name);
        Assert.Equal(NatsAsyncResponseTransportOptions.TransportName, target.Transport);
        Assert.Equal("app.transport.response", target.Address);
        Assert.Equal("app.transport.response", target.Properties["subject"]);
        Assert.Equal("asyncresponse-responses", target.Properties["consumer"]);
        Assert.Equal("AR-Correlation-Id", target.Properties["correlationIdHeader"]);
    }

    [Fact]
    public void GetReplyTarget_Named_ReturnsConfiguredTarget()
    {
        var options = new NatsAsyncResponseTransportOptions();
        options.AddReplyTarget("regional", "app.regional.response");
        var provider = new NatsReplyTargetProvider(Options.Create(options));

        var target = provider.GetReplyTarget("regional");

        Assert.Equal("regional", target.Name);
        Assert.Equal("app.regional.response", target.Address);
    }

    [Fact]
    public void GetReplyTarget_UnknownName_Throws()
    {
        var provider = new NatsReplyTargetProvider(Options.Create(new NatsAsyncResponseTransportOptions()));
        Assert.Throws<InvalidOperationException>(() => provider.GetReplyTarget("nope"));
    }
}

public class NatsCorrelationIdExtractorTests
{
    private readonly NatsAsyncResponseTransportOptions _options = new();

    [Fact]
    public void Extract_PrefersHeader()
    {
        var headers = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase) { ["AR-Correlation-Id"] = "from-header" };
        Assert.Equal("from-header", NatsCorrelationIdExtractor.Extract(headers, """{"CorrelationId":"from-body"}""", _options));
    }

    [Fact]
    public void Extract_FallsBackToTopLevelJsonPath()
        => Assert.Equal("from-body", NatsCorrelationIdExtractor.Extract(null, """{"CorrelationId":"from-body"}""", _options));

    [Fact]
    public void Extract_ResolvesNestedJsonPath()
        => Assert.Equal("nested", NatsCorrelationIdExtractor.Extract(null, """{"CustomParameters":{"CorrelationId":"nested"}}""", _options));

    [Fact]
    public void Extract_ReturnsNull_WhenNotFound()
        => Assert.Null(NatsCorrelationIdExtractor.Extract(null, """{"unrelated":true}""", _options));

    [Fact]
    public void Extract_ReturnsNull_ForNonJsonBodyWithoutHeader()
        => Assert.Null(NatsCorrelationIdExtractor.Extract(null, "not json", _options));

    [Fact]
    public void Extract_MatchesPropertyCaseInsensitively()
        => Assert.Equal("x", NatsCorrelationIdExtractor.Extract(null, """{"correlationid":"x"}""", _options));

    [Fact]
    public void Extract_ConvertsNonStringScalarToString()
        => Assert.Equal("42", NatsCorrelationIdExtractor.Extract(null, """{"CorrelationId":42}""", _options));

    [Fact]
    public void Extract_UnwrapsJsonEncodedStringSegment()
        => Assert.Equal("nested", NatsCorrelationIdExtractor.Extract(null, """{"CustomParameters":"{\"CorrelationId\":\"nested\"}"}""", _options));

    [Fact]
    public void Extract_IgnoresHeaderWhitespace_AndFallsBackToBody()
    {
        var headers = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase) { ["AR-Correlation-Id"] = "  " };
        Assert.Equal("from-body", NatsCorrelationIdExtractor.Extract(headers, """{"CorrelationId":"from-body"}""", _options));
    }

    [Fact]
    public void Extract_ReturnsNull_ForJsonNullBody()
        => Assert.Null(NatsCorrelationIdExtractor.Extract(null, "null", _options));

    [Fact]
    public void Extract_SkipsEmptyConfiguredPath()
    {
        var options = new NatsAsyncResponseTransportOptions { CorrelationIdJsonPaths = ["", "CorrelationId"] };
        Assert.Equal("x", NatsCorrelationIdExtractor.Extract(null, """{"CorrelationId":"x"}""", options));
    }

    [Fact]
    public void Extract_ReturnsNull_WhenNestedSegmentMissing()
        => Assert.Null(NatsCorrelationIdExtractor.Extract(null, """{"CustomParameters":{}}""", _options));

    [Fact]
    public void Extract_ReturnsRawString_WhenSegmentValueIsInvalidJson()
        => Assert.Equal("{not valid", NatsCorrelationIdExtractor.Extract(null, """{"CorrelationId":"{not valid"}""", _options));
}

public class NatsWorkerTransportTests
{
    private readonly FakeNatsJetStreamTransport _jetStream = new();

    private NatsWorkerTransport CreateTransport(NatsAsyncResponseTransportOptions? options = null)
        => new(Options.Create(options ?? new NatsAsyncResponseTransportOptions()), _jetStream);

    private static WorkerJobEnvelope CreateJob(string? correlationId = "corr-a") => new()
    {
        Call = new ReflectionCallDto { ServiceInterfaceFullName = "ISvc", MethodName = "Do", Params = [] },
        CorrelationId = correlationId
    };

    [Fact]
    public async Task PublishAsync_PublishesEnvelopeWithCorrelationHeader_AndEnsuresStreamOnce()
    {
        var transport = CreateTransport();

        await transport.PublishAsync(CreateJob());
        await transport.PublishAsync(CreateJob());

        Assert.Equal(2, _jetStream.Published.Count);
        var (subject, payload, headers) = _jetStream.Published[0];
        Assert.Equal("asyncresponse.transport.worker", subject);
        Assert.Equal("corr-a", headers!["AR-Correlation-Id"]);
        var envelope = JsonSerializer.Deserialize<WorkerJobEnvelope>(payload);
        Assert.Equal("corr-a", envelope!.CorrelationId);

        // The stream is ensured lazily and only once across many publishes.
        Assert.Single(_jetStream.EnsuredStreams);
        Assert.Equal(("asyncresponse_transport_worker", "asyncresponse.transport.worker"), _jetStream.EnsuredStreams[0]);
    }

    [Fact]
    public async Task PublishAsync_OmitsHeader_WhenNoCorrelationId()
    {
        var transport = CreateTransport();
        await transport.PublishAsync(CreateJob(correlationId: null));
        Assert.Null(_jetStream.Published[0].Headers);
    }

    [Fact]
    public async Task PublishAsync_RetriesTransientFailures()
    {
        var attempts = 0;
        _jetStream.PublishFailureForAttempt = attempt =>
        {
            attempts = attempt;
            return attempt == 1 ? new TimeoutException("transient") : null;
        };
        var transport = CreateTransport(new NatsAsyncResponseTransportOptions
        {
            PublishMaxAttempts = 3,
            PublishRetryBaseDelay = TimeSpan.FromMilliseconds(1),
            PublishRetryMaxDelay = TimeSpan.FromMilliseconds(2)
        });

        await transport.PublishAsync(CreateJob());

        Assert.Equal(2, attempts);
        Assert.Single(_jetStream.Published);
    }

    [Fact]
    public async Task PublishAsync_DoesNotRetryNonTransientFailures()
    {
        _jetStream.PublishFailureForAttempt = _ => new InvalidOperationException("fatal");
        var transport = CreateTransport();

        await Assert.ThrowsAsync<InvalidOperationException>(() => transport.PublishAsync(CreateJob()));
        Assert.Empty(_jetStream.Published);
    }

    [Fact]
    public async Task PublishAsync_Throws_OnNullJob()
        => await Assert.ThrowsAsync<ArgumentNullException>(() => CreateTransport().PublishAsync(null!));
}

public class NatsTransportRetryTests
{
    [Fact]
    public void Backoff_GrowsExponentially_AndIsCapped()
    {
        var baseDelay = TimeSpan.FromMilliseconds(100);
        var maxDelay = TimeSpan.FromSeconds(1);

        Assert.Equal(TimeSpan.FromMilliseconds(100), NatsTransportRetry.Backoff(1, baseDelay, maxDelay));
        Assert.Equal(TimeSpan.FromMilliseconds(200), NatsTransportRetry.Backoff(2, baseDelay, maxDelay));
        Assert.Equal(maxDelay, NatsTransportRetry.Backoff(10, baseDelay, maxDelay));
    }

    [Fact]
    public void IsTransient_ClassifiesTimeoutAsTransient_AndCancellationAsNot()
    {
        Assert.True(NatsTransportRetry.IsTransient(new TimeoutException()));
        Assert.False(NatsTransportRetry.IsTransient(new OperationCanceledException()));
        Assert.False(NatsTransportRetry.IsTransient(new InvalidOperationException()));
    }
}
