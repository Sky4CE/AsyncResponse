using Google.Cloud.PubSub.V1;
using Microsoft.Extensions.Options;

namespace AsyncResponse.Transports.GooglePubSub;

internal sealed class GooglePubSubReplyTargetProvider(
    IOptions<GooglePubSubAsyncResponseOptions> _options) : IAsyncResponseReplyTargetProvider
{
    /// <summary>Gets the configured reply target.</summary>
    public AsyncResponseReplyTarget GetReplyTarget(string? name = null)
    {
        var options = _options.Value;
        // Options-validator parity with the other transports' providers: a hand-off address must
        // come from a configuration that passes the transport's own checks.
        GooglePubSubOptionsValidator.ValidateTimeouts(options);
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

        // A NAMED target must not be the worker topic in the transport's own project
        // (DB-transport parity): its responses would be consumed as worker jobs while the waiter
        // times out.
        if (StringComparer.Ordinal.Equals(projectId, options.ProjectId)
            && StringComparer.Ordinal.Equals(topicId, options.WorkerTopicId))
        {
            throw new InvalidOperationException(
                $"Google Pub/Sub async-response reply target '{targetName}' uses topic '{topicId}' in project '{projectId}', which collides with " +
                $"{nameof(GooglePubSubAsyncResponseOptions.WorkerTopicId)}; its responses would be consumed as worker jobs.");
        }

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
