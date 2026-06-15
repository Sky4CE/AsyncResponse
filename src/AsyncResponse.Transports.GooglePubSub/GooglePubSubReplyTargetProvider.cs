using Google.Cloud.PubSub.V1;
using Microsoft.Extensions.Options;

namespace AsyncResponse.Transports.GooglePubSub;

internal sealed class GooglePubSubReplyTargetProvider(
    IOptions<GooglePubSubAsyncResponseOptions> _options) : IAsyncResponseReplyTargetProvider
{
    public AsyncResponseReplyTarget GetReplyTarget(string? name = null)
    {
        var options = _options.Value;
        var targetName = string.IsNullOrWhiteSpace(name)
            ? options.DefaultReplyTargetName
            : name;

        var target = ResolveTarget(options, targetName);
        var projectId = GooglePubSubOptionsValidator.Required(
            target.ProjectId ?? options.ProjectId,
            $"{nameof(GooglePubSubReplyTargetOptions)}.{nameof(GooglePubSubReplyTargetOptions.ProjectId)}");
        var topicId = GooglePubSubOptionsValidator.Required(
            target.TopicId,
            $"{nameof(GooglePubSubReplyTargetOptions)}.{nameof(GooglePubSubReplyTargetOptions.TopicId)}");

        var properties = new Dictionary<string, string>(target.Properties, StringComparer.Ordinal)
        {
            ["projectId"] = projectId,
            ["topicId"] = topicId
        };

        return new AsyncResponseReplyTarget
        {
            Name = targetName,
            Transport = GooglePubSubAsyncResponseOptions.TransportName,
            Address = TopicName.FromProjectTopic(projectId, topicId).ToString(),
            Properties = properties
        };
    }

    private static GooglePubSubReplyTargetOptions ResolveTarget(
        GooglePubSubAsyncResponseOptions options,
        string targetName)
    {
        if (options.ReplyTargets.TryGetValue(targetName, out var configured))
            return configured;

        if (StringComparer.Ordinal.Equals(targetName, options.DefaultReplyTargetName)
            && !string.IsNullOrWhiteSpace(options.ResponseTopicId))
        {
            return new GooglePubSubReplyTargetOptions
            {
                ProjectId = options.ProjectId,
                TopicId = options.ResponseTopicId
            };
        }

        throw new InvalidOperationException(
            $"Google Pub/Sub async-response reply target '{targetName}' is not configured. " +
            $"Configure {nameof(GooglePubSubAsyncResponseOptions.ResponseTopicId)} for the default target " +
            $"or add a named target with {nameof(GooglePubSubAsyncResponseOptions.AddReplyTarget)}.");
    }
}
