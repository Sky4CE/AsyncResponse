using AsyncResponse.Transports.GooglePubSub;
using Google.Cloud.PubSub.V1;
using Google.Protobuf;
using Microsoft.Extensions.Options;
using Xunit;

namespace AsyncResponse.Tests;

public class GooglePubSubTransportTests
{
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
}
