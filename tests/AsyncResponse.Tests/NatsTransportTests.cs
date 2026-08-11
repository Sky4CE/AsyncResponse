using AsyncResponse.Transports.NATS;
using Microsoft.Extensions.Options;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
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
        var subscriber = new NatsSubscriberOptions { AckMode = NatsAckMode.AckAfterEnqueue };
        Assert.Throws<InvalidOperationException>(() => NatsTransportOptionsValidator.ValidateSubscriber(subscriber, "Worker"));

        subscriber.UseAckAfterEnqueue(backgroundWorkerCount: 2, backgroundQueueCapacity: 8);
        NatsTransportOptionsValidator.ValidateSubscriber(subscriber, "Worker"); // now valid
    }

    [Fact]
    public void EarlyAckAndOptionalLimits_CoverValidAndInvalidBoundaries()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new NatsSubscriberOptions().UseAckAfterEnqueue(1, 1, TimeSpan.Zero));
        Assert.Throws<InvalidOperationException>(() => NatsTransportOptionsValidator.ValidateSubscriber(
            new NatsSubscriberOptions
            {
                AckMode = NatsAckMode.AckAfterEnqueue,
                BackgroundWorkerCount = 1,
                BackgroundQueueCapacity = 0
            },
            "Worker"));

        NatsTransportOptionsValidator.ValidateCommon(new NatsAsyncResponseTransportOptions
        {
            StreamMaxMessages = 1,
            DeadLetterStreamMaxMessages = 1
        });
    }

    [Fact]
    public void ValidateSubscriber_Throws_ForNonPositiveBatchSize()
        => Assert.Throws<InvalidOperationException>(() =>
            NatsTransportOptionsValidator.ValidateSubscriber(new NatsSubscriberOptions { BatchSize = 0 }, "Worker"));

    [Fact]
    public void ValidateSubscriber_DrainBudgetExceedingHostShutdownBudget_Throws()
    {
        var options = new NatsAsyncResponseTransportOptions
        {
            HostShutdownTimeout = TimeSpan.FromSeconds(25)
        };
        var subscriber = new NatsSubscriberOptions().UseAckAfterEnqueue(1, 8, TimeSpan.FromSeconds(26));

        var ex = Assert.Throws<InvalidOperationException>(() =>
            NatsTransportOptionsValidator.ValidateSubscriber(options, subscriber, "Worker"));

        Assert.Contains(nameof(NatsAsyncResponseTransportOptions.HostShutdownTimeout), ex.Message);

        // A null host budget or an awaiting-mode subscriber skips the check.
        options.HostShutdownTimeout = null;
        NatsTransportOptionsValidator.ValidateSubscriber(options, subscriber, "Worker");
        NatsTransportOptionsValidator.ValidateSubscriber(
            new NatsAsyncResponseTransportOptions { HostShutdownTimeout = TimeSpan.FromSeconds(1) },
            new NatsSubscriberOptions(),
            "Worker");
    }

    [Fact]
    public void ValidateSubscriber_NonPositiveHostShutdownTimeout_Throws()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            NatsTransportOptionsValidator.ValidateSubscriber(
                new NatsAsyncResponseTransportOptions { HostShutdownTimeout = TimeSpan.Zero },
                new NatsSubscriberOptions().UseAckAfterEnqueue(1, 8),
                "Worker"));

        Assert.Contains(nameof(NatsAsyncResponseTransportOptions.HostShutdownTimeout), ex.Message);
    }

    [Fact]
    public void ValidateSubscriber_DocumentedEarlyAckDefaults_Pass()
    {
        // Regression: the documented two-arg early-ACK opt-in with stock defaults
        // (BackgroundDrainTimeout 20s vs HostShutdownTimeout 30s) must not fail startup.
        NatsTransportOptionsValidator.ValidateSubscriber(
            new NatsAsyncResponseTransportOptions(),
            new NatsSubscriberOptions().UseAckAfterEnqueue(4, 256),
            "Worker");
    }

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
    public void Extract_BlankHeaderFallsBackAndNonObjectPathReturnsNull()
    {
        var headers = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["AR-Correlation-Id"] = " "
        };
        Assert.Equal("from-body", NatsCorrelationIdExtractor.Extract(
            headers, """{"CorrelationId":"from-body"}""", _options));
        Assert.Null(NatsCorrelationIdExtractor.Extract(
            null,
            """{"CustomParameters":42}""",
            new NatsAsyncResponseTransportOptions
            {
                CorrelationIdJsonPaths = ["CustomParameters.CorrelationId"]
            }));
        Assert.Null(NatsCorrelationIdExtractor.Extract(
            null,
            "{}",
            new NatsAsyncResponseTransportOptions { CorrelationIdJsonPaths = null! }));
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
    public async Task PublishAsync_OmitsCorrelationHeader_ButAlwaysCarriesMsgId_WhenNoCorrelationId()
    {
        var transport = CreateTransport();
        await transport.PublishAsync(CreateJob(correlationId: null));

        // Every publish carries a Nats-Msg-Id so a retried publish is deduplicated by JetStream
        // instead of enqueuing the same worker job twice; the correlation header alone is omitted.
        var headers = _jetStream.Published[0].Headers;
        Assert.NotNull(headers);
        var messageId = Assert.Contains("Nats-Msg-Id", headers);
        Assert.False(string.IsNullOrWhiteSpace(messageId));
        Assert.DoesNotContain("AR-Correlation-Id", headers);
    }

    [Fact]
    public async Task PublishAsync_CarriesDedupMsgId_DistinctPerLogicalPublish()
    {
        var transport = CreateTransport(new NatsAsyncResponseTransportOptions
        {
            PublishMaxAttempts = 3,
            PublishRetryBaseDelay = TimeSpan.FromMilliseconds(1),
            PublishRetryMaxDelay = TimeSpan.FromMilliseconds(2)
        });
        _jetStream.PublishFailureForAttempt = attempt => attempt == 1 ? new TimeoutException("transient") : null;

        // The dedup id is generated once outside the retry loop, so the broker sees the same
        // Nats-Msg-Id on the retried attempt (JetStream drops the duplicate when the first
        // attempt actually landed); a subsequent logical publish must carry a fresh id.
        await transport.PublishAsync(CreateJob());
        await transport.PublishAsync(CreateJob());

        Assert.Equal(2, _jetStream.Published.Count);
        var firstId = Assert.Contains("Nats-Msg-Id", _jetStream.Published[0].Headers!);
        var secondId = Assert.Contains("Nats-Msg-Id", _jetStream.Published[1].Headers!);
        Assert.False(string.IsNullOrWhiteSpace(firstId));
        Assert.NotEqual(firstId, secondId);
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
    public async Task PublishAsync_RetriesTransientStreamProvisioningFailures()
    {
        // Stream provisioning runs on the first publish — precisely when the JetStream API is
        // likeliest to time out ("No API response received from the server" while the server
        // settles). It was the one call in this type NOT retried, so that transient condition
        // failed the caller's publish outright even though the identical condition on the publish
        // itself was absorbed. Reproduced in CI as random matrix-cell failures at exactly the
        // 5-second JetStream API timeout.
        var attempts = 0;
        _jetStream.EnsureStreamFailureForAttempt = attempt =>
        {
            attempts = attempt;
            return attempt == 1 ? new NatsJSApiNoResponseException() : null;
        };
        var transport = CreateTransport(new NatsAsyncResponseTransportOptions
        {
            PublishMaxAttempts = 3,
            PublishRetryBaseDelay = TimeSpan.FromMilliseconds(1),
            PublishRetryMaxDelay = TimeSpan.FromMilliseconds(2)
        });

        await transport.PublishAsync(CreateJob());

        Assert.Equal(2, attempts);
        Assert.Single(_jetStream.EnsuredStreams);
        Assert.Single(_jetStream.Published);
    }

    [Fact]
    public async Task PublishAsync_DoesNotRetryRejectedStreamProvisioning()
    {
        // The REAL deterministic failure type: JetStream ANSWERED, rejecting the request
        // (10058 = "stream name already in use" — a genuine name collision). It arrives as
        // NatsJSApiException, which inherits NatsException, so a bare "any NatsException is
        // transient" rule would burn the whole retry budget re-asking a question the server has
        // already answered. Only a 5xx (the server saying it is temporarily unable) retries.
        var attempts = 0;
        _jetStream.EnsureStreamFailureForAttempt = attempt =>
        {
            attempts = attempt;
            return new NatsJSApiException(new ApiError { Code = 400, ErrCode = 10058, Description = "stream name already in use" });
        };
        var transport = CreateTransport();

        var ex = await Assert.ThrowsAsync<NatsJSApiException>(() => transport.PublishAsync(CreateJob()));
        Assert.Equal(10058, ex.Error.ErrCode);
        Assert.Equal(1, attempts);
        Assert.Empty(_jetStream.Published);
    }

    [Fact]
    public async Task PublishAsync_RetriesServerUnavailableStreamProvisioning()
    {
        // The other side of the same coin: a 5xx API answer (503 while a meta-leader election
        // settles) is the server reporting a temporary condition, so it retries like any blip.
        _jetStream.EnsureStreamFailureForAttempt = attempt => attempt == 1
            ? new NatsJSApiException(new ApiError { Code = 503, ErrCode = 10008, Description = "JetStream system temporarily unavailable" })
            : null;
        var transport = CreateTransport(new NatsAsyncResponseTransportOptions
        {
            PublishMaxAttempts = 3,
            PublishRetryBaseDelay = TimeSpan.FromMilliseconds(1),
            PublishRetryMaxDelay = TimeSpan.FromMilliseconds(2)
        });

        await transport.PublishAsync(CreateJob());

        Assert.Single(_jetStream.EnsuredStreams);
        Assert.Single(_jetStream.Published);
    }

    [Fact]
    public async Task PublishAsync_AfterATransientProvisioningFailure_DoesNotLatchTheStreamAsEnsured()
    {
        // The "once" flag must latch on SUCCESS only: a failed ensure that marked itself done
        // would leave later publishes writing to a stream that does not exist.
        _jetStream.EnsureStreamFailureForAttempt = attempt => attempt <= 3 ? new NatsJSApiNoResponseException() : null;
        var transport = CreateTransport(new NatsAsyncResponseTransportOptions
        {
            PublishMaxAttempts = 2,
            PublishRetryBaseDelay = TimeSpan.FromMilliseconds(1),
            PublishRetryMaxDelay = TimeSpan.FromMilliseconds(2)
        });

        // Attempts 1-2 exhaust the budget and throw; attempts 3-4 are the next publish, whose
        // second try succeeds — proving the transport re-provisions instead of assuming success.
        await Assert.ThrowsAsync<NatsJSApiNoResponseException>(() => transport.PublishAsync(CreateJob()));
        await transport.PublishAsync(CreateJob());

        Assert.Single(_jetStream.EnsuredStreams);
        Assert.Single(_jetStream.Published);
    }

    [Fact]
    public async Task PublishAsync_Throws_OnNullJob()
        => await Assert.ThrowsAsync<ArgumentNullException>(() => CreateTransport().PublishAsync(null!));
}

public class NatsTransportRetryTests
{
    [Fact]
    public void Backoff_GrowsExponentially_WithHalfJitter_AndIsCapped()
    {
        var baseDelay = TimeSpan.FromMilliseconds(100);
        var maxDelay = TimeSpan.FromSeconds(1);

        // Half-jitter keeps each delay within [step/2, step] of the exponential step, so the
        // assertions pin the envelope rather than exact values.
        for (var i = 0; i < 20; i++)
        {
            Assert.InRange(AsyncResponseRetry.Backoff(1, baseDelay, maxDelay).TotalMilliseconds, 50, 100);
            Assert.InRange(AsyncResponseRetry.Backoff(2, baseDelay, maxDelay).TotalMilliseconds, 100, 200);
            Assert.InRange(AsyncResponseRetry.Backoff(10, baseDelay, maxDelay).TotalMilliseconds, 500, 1000);
        }
    }

    [Fact]
    public void Backoff_ToleratesNonPositiveAttempts()
        => Assert.InRange(
            AsyncResponseRetry.Backoff(0, TimeSpan.FromMilliseconds(100), TimeSpan.FromSeconds(1)).TotalMilliseconds,
            1, 100);

    [Fact]
    public void IsTransient_ClassifiesTimeoutAsTransient_AndCancellationAsNot()
    {
        Assert.True(NatsTransportRetry.IsTransient(new TimeoutException()));
        // The JetStream API's own "the server did not answer" — the condition that shows up under
        // load — must classify as transient; it reaches the classifier as a NatsException subtype.
        Assert.True(NatsTransportRetry.IsTransient(new NatsJSApiNoResponseException()));
        // An ANSWERED API rejection is a decision, not a blip: retrying re-asks a settled
        // question. Both types inherit NatsException, so they must be told apart by the answer.
        Assert.False(NatsTransportRetry.IsTransient(
            new NatsJSApiException(new ApiError { Code = 400, ErrCode = 10058, Description = "stream name already in use" })));
        Assert.True(NatsTransportRetry.IsTransient(
            new NatsJSApiException(new ApiError { Code = 503, ErrCode = 10008, Description = "JetStream system temporarily unavailable" })));
        Assert.False(NatsTransportRetry.IsTransient(new OperationCanceledException()));
        Assert.False(NatsTransportRetry.IsTransient(new InvalidOperationException()));
    }
}
