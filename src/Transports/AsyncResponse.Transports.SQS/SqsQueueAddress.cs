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

    /// <summary>
    /// Whether two configured queue strings certainly address the same queue — the comparison the
    /// collision guards fail on. Two URLs compare as normalized URLs (case-insensitive scheme and
    /// host, default port and trailing '/' dropped; the path, which carries the account and the
    /// case-sensitive name, stays ordinal); two names compare ordinally (SQS names are
    /// case-sensitive). A name and a URL are never certain — see <see cref="MayBeSameQueue"/>.
    /// </summary>
    public static bool SameQueue(string first, string second)
    {
        var firstIsUrl = IsUrl(first);
        if (firstIsUrl != IsUrl(second))
            return false;

        return firstIsUrl
            ? StringComparer.Ordinal.Equals(NormalizeUrl(first), NormalizeUrl(second))
            : StringComparer.Ordinal.Equals(first, second);
    }

    /// <summary>
    /// Whether a queue name and a queue URL MAY address the same queue: the URL's queue name is the
    /// name. The name resolves in the client's own account and region, so it is one queue when the
    /// URL is there too — and a legitimately different queue when the URL is another account's,
    /// which the transport cannot tell without a <c>GetQueueUrl</c> call. Guards warn on it
    /// instead of failing; writing both as URLs makes the comparison exact.
    /// </summary>
    public static bool MayBeSameQueue(string first, string second)
        => IsUrl(first) != IsUrl(second)
            && StringComparer.Ordinal.Equals(QueueName(first), QueueName(second));

    private static string NormalizeUrl(string url)
    {
        var trimmed = url.TrimEnd('/');
        return Uri.TryCreate(trimmed, UriKind.Absolute, out var uri)
            ? uri.GetLeftPart(UriPartial.Path).TrimEnd('/')
            : trimmed;
    }

    /// <summary>The SQS queue-name length limit, the <c>.fifo</c> suffix included.</summary>
    private const int MaxQueueNameLength = 80;

    /// <summary>
    /// Whether SQS accepts <paramref name="queueName"/> as a queue name: at most 80 characters of
    /// ASCII letters, digits, <c>-</c> and <c>_</c>, plus the <c>.fifo</c> suffix of a FIFO queue
    /// (counted in the 80).
    /// </summary>
    public static bool IsValidQueueName(string queueName)
    {
        var body = IsFifo(queueName) ? queueName[..^".fifo".Length] : queueName;
        if (body.Length == 0 || queueName.Length > MaxQueueNameLength)
            return false;

        foreach (var character in body)
        {
            if (!char.IsAsciiLetterOrDigit(character) && character is not ('-' or '_'))
                return false;
        }

        return true;
    }

    /// <summary>
    /// Derives the dead-letter queue name provisioning creates for <paramref name="queueName"/>:
    /// the suffix is appended, and for FIFO queues re-inserted before the mandatory
    /// <c>.fifo</c> tail (a FIFO queue's dead-letter queue must itself be FIFO). The validator
    /// uses the SAME derivation to reject collisions with a live queue, so the two must not drift.
    /// </summary>
    public static string DeriveDeadLetterQueueName(string queueName, string suffix)
        => IsFifo(queueName)
            ? $"{queueName[..^".fifo".Length]}{suffix}.fifo"
            : $"{queueName}{suffix}";
}
