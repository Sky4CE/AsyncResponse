using AsyncResponse.Transports.GooglePubSub;
using Microsoft.Extensions.Options;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Google Pub/Sub option handling: the required-value validator, fluent reply-target registration,
/// reply-target resolution edges (unconfigured throws, named target inherits the transport project),
/// and the worker transport's fail-fast constructor validation.
/// </summary>
public class GooglePubSubOptionsTests
{
    [Fact]
    public void Validator_ReturnsValue_WhenPresent()
        => Assert.Equal("value", GooglePubSubOptionsValidator.Required("value", "Name"));

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void Validator_Throws_WhenMissing(string? value)
    {
        var ex = Assert.Throws<InvalidOperationException>(() => GooglePubSubOptionsValidator.Required(value, "ProjectId"));
        Assert.Contains("ProjectId", ex.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("", "proj", "topic")]
    [InlineData("name", "", "topic")]
    [InlineData("name", "proj", "")]
    public void AddReplyTarget_ValidatesArguments(string name, string projectId, string topicId)
        => Assert.Throws<ArgumentException>(() => new GooglePubSubAsyncResponseOptions().AddReplyTarget(name, projectId, topicId));

    [Fact]
    public void AddReplyTarget_RegistersTheTarget()
    {
        var options = new GooglePubSubAsyncResponseOptions().AddReplyTarget("regional-us", "proj-us", "topic-us");

        Assert.True(options.ReplyTargets.ContainsKey("regional-us"));
        Assert.Equal("proj-us", options.ReplyTargets["regional-us"].ProjectId);
        Assert.Equal("topic-us", options.ReplyTargets["regional-us"].TopicId);
    }

    [Fact]
    public void ReplyTargetProvider_Unconfigured_ThrowsForDefaultAndNamed()
    {
        var provider = new GooglePubSubReplyTargetProvider(Options.Create(new GooglePubSubAsyncResponseOptions()));

        Assert.Throws<InvalidOperationException>(() => provider.GetReplyTarget());        // no ResponseTopicId
        Assert.Throws<InvalidOperationException>(() => provider.GetReplyTarget("missing")); // unknown named target
    }

    [Fact]
    public void ReplyTargetProvider_NamedTarget_InheritsTransportProjectId()
    {
        var options = new GooglePubSubAsyncResponseOptions { ProjectId = "transport-project" };
        options.ReplyTargets["custom"] = new GooglePubSubReplyTargetOptions { TopicId = "custom-topic" }; // ProjectId omitted
        var provider = new GooglePubSubReplyTargetProvider(Options.Create(options));

        var target = provider.GetReplyTarget("custom");

        Assert.Equal("transport-project", target.Properties["projectId"]);
        Assert.Equal("custom-topic", target.Properties["topicId"]);
    }

    [Fact]
    public void WorkerTransport_Ctor_RequiresProjectId()
        => Assert.Throws<InvalidOperationException>(
            () => new GooglePubSubWorkerTransport(Options.Create(new GooglePubSubAsyncResponseOptions())));

    [Fact]
    public void DispatcherValidateOptions_AckAfterHandler_ShutdownTimeoutPastTheHostBudget_Throws()
    {
        // Red-on-old (fixpoint r1, S8#11): ShutdownTimeout bounds the subscriber-client stop in
        // BOTH ack modes, but ACK-after-handler returned without checking it against the host
        // budget — a raised value passed startup and the host killed the process mid-stop
        // (SQS / Azure Service Bus parity).
        var ex = Assert.Throws<InvalidOperationException>(() => GooglePubSubMessageDispatcher.ValidateOptions(
            new GooglePubSubAsyncResponseOptions { ShutdownTimeout = TimeSpan.FromSeconds(60) },
            new GooglePubSubSubscriberOptions(),
            GooglePubSubSubscriberRole.Worker));
        Assert.Contains(nameof(GooglePubSubAsyncResponseOptions.ShutdownTimeout), ex.Message, StringComparison.Ordinal);

        // At the budget it still starts (and the in-flight drain gets nothing).
        GooglePubSubMessageDispatcher.ValidateOptions(
            new GooglePubSubAsyncResponseOptions { ShutdownTimeout = TimeSpan.FromSeconds(30) },
            new GooglePubSubSubscriberOptions(),
            GooglePubSubSubscriberRole.Worker);
    }

    [Theory]
    [InlineData("googCorrelation")] // the reserved prefix
    [InlineData("goog")]
    [InlineData(null)]              // 257 bytes, built below
    [InlineData("é")]               // 129 × 'é' = 258 bytes in UTF-8, although only 129 characters
    public void CorrelationIdAttributePubSubWouldReject_FailsAtStartup(string? attribute)
    {
        // Red-on-old (fixpoint r2, GS5#5): only blankness was checked, so a key Pub/Sub rejects
        // passed startup and then every correlated publish failed.
        attribute = attribute switch
        {
            null => new string('a', 257),
            "é" => new string('é', 129),
            _ => attribute
        };
        var ex = Assert.Throws<InvalidOperationException>(() => GooglePubSubOptionsValidator.ValidateTimeouts(
            new GooglePubSubAsyncResponseOptions { CorrelationIdAttribute = attribute }));
        Assert.Contains(nameof(GooglePubSubAsyncResponseOptions.CorrelationIdAttribute), ex.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("correlationId")] // the default
    [InlineData("x-correlation")]
    [InlineData(null)]                 // 256 bytes, built below
    public void CorrelationIdAttributePubSubAccepts_Starts(string? attribute)
        => GooglePubSubOptionsValidator.ValidateTimeouts(
            new GooglePubSubAsyncResponseOptions { CorrelationIdAttribute = attribute ?? new string('a', 256) });

    [Fact]
    public void WorkerTransport_Ctor_RequiresWorkerTopicId()
        => Assert.Throws<InvalidOperationException>(
            () => new GooglePubSubWorkerTransport(Options.Create(new GooglePubSubAsyncResponseOptions { ProjectId = "p" })));
}
