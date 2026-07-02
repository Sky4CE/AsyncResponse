namespace AsyncResponse.Transports.Kafka;

/// <summary>
/// Resolves Kafka topic names from transport options. These names are deployment contracts:
/// changing them while messages are in flight strands unprocessed worker or response messages in
/// the old topic.
/// </summary>
internal sealed class KafkaTransportTopicSchema(KafkaAsyncResponseTransportOptions _options)
{
    public string WorkerTopic => Resolve(_options.WorkerTopic, "worker");
    public string ResponseTopic => Resolve(_options.ResponseTopic, "response");

    /// <summary>
    /// The dead-letter topic for one source topic: the explicit <see cref="KafkaAsyncResponseTransportOptions.DeadLetterTopic"/>
    /// when configured, otherwise <c>{sourceTopic}{DeadLetterTopicSuffix}</c>.
    /// </summary>
    public string DeadLetterTopicFor(string sourceTopic)
        => !string.IsNullOrWhiteSpace(_options.DeadLetterTopic)
            ? _options.DeadLetterTopic!
            : sourceTopic + _options.DeadLetterTopicSuffix;

    private string Resolve(string? configured, string role)
        => !string.IsNullOrWhiteSpace(configured)
            ? configured!
            : $"{_options.TopicPrefix}.transport.{role}";
}
