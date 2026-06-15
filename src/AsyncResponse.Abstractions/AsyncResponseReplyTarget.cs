namespace AsyncResponse;

/// <summary>
/// Transport-neutral description of where a remote system should publish an async response.
/// Transport packages translate their native reply destinations (for example Pub/Sub topics or
/// broker exchanges) into this shape so application flows can pass reply metadata without taking
/// a dependency on a specific transport package.
/// </summary>
public sealed class AsyncResponseReplyTarget
{
    /// <summary>A stable logical name for this target, such as <c>default</c> or <c>regional-us</c>.</summary>
    public required string Name { get; init; }

    /// <summary>The transport that owns the target, such as <c>google-pubsub</c>.</summary>
    public required string Transport { get; init; }

    /// <summary>
    /// The transport-native address, such as a canonical topic, queue, exchange, or URL.
    /// Consumers that need structured values should read <see cref="Properties"/>.
    /// </summary>
    public required string Address { get; init; }

    /// <summary>Additional transport-specific values, for example <c>projectId</c> and <c>topicId</c>.</summary>
    public Dictionary<string, string> Properties { get; init; } = new(StringComparer.Ordinal);
}

/// <summary>
/// Context passed to triggers that need both the generated correlation id and the selected reply
/// target. The same values are also available ambiently through <see cref="AsyncResponseContext"/>
/// while the trigger executes.
/// </summary>
public sealed record AsyncResponseRequestContext(
    string CorrelationId,
    AsyncResponseReplyTarget? ReplyTarget);

/// <summary>
/// Optional capability registered by transport packages that can provide async-response reply
/// targets. Core uses this only when application code explicitly calls
/// <c>WithReplyTarget(...)</c> on a waiter builder.
/// </summary>
public interface IAsyncResponseReplyTargetProvider
{
    /// <summary>
    /// Resolves a reply target by logical name. Passing <c>null</c> asks for the provider's
    /// default target.
    /// </summary>
    AsyncResponseReplyTarget GetReplyTarget(string? name = null);
}
