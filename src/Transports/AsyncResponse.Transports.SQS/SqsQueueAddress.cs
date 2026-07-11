namespace AsyncResponse.Transports.SQS;

/// <summary>
/// Interprets the queue strings accepted by <see cref="SqsAsyncResponseOptions"/>: either a plain
/// queue name (resolved through <c>GetQueueUrl</c>) or a full queue URL.
/// </summary>
internal static class SqsQueueAddress
{
    /// <summary>Returns <c>true</c> when the configured queue string is a full queue URL.</summary>
    public static bool IsUrl(string queue)
        => queue.StartsWith("http://", StringComparison.OrdinalIgnoreCase)
            || queue.StartsWith("https://", StringComparison.OrdinalIgnoreCase);

    /// <summary>Returns <c>true</c> when the queue (name or URL) addresses an SQS FIFO queue.</summary>
    public static bool IsFifo(string queue)
        => QueueName(queue).EndsWith(".fifo", StringComparison.OrdinalIgnoreCase);

    /// <summary>Extracts the queue name from a queue string (the last URL segment, or the string itself).</summary>
    public static string QueueName(string queue)
    {
        var trimmed = queue.TrimEnd('/');
        if (!IsUrl(trimmed))
            return trimmed;

        var lastSegment = trimmed.LastIndexOf('/');
        return lastSegment >= 0 ? trimmed[(lastSegment + 1)..] : trimmed;
    }
}
