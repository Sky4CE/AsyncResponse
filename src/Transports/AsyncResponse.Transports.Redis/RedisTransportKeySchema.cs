using StackExchange.Redis;

namespace AsyncResponse.Transports.Redis;

/// <summary>
/// Resolves Redis stream names from transport options. These names are deployment contracts: changing
/// them while entries are in flight strands unprocessed worker or response messages in the old stream.
/// </summary>
internal sealed class RedisTransportKeySchema(RedisAsyncResponseTransportOptions _options)
{
    public RedisKey WorkerStream => Resolve(_options.WorkerStream, "worker");
    public RedisKey ResponseStream => Resolve(_options.ResponseStream, "response");
    public RedisKey DeadLetterStream => Resolve(_options.DeadLetterStream, "deadletter");

    /// <summary>
    /// Per-publish dedup marker for the idempotent worker XADD. Hash-tagged with the stream's own
    /// slot key so the marker and the stream share a cluster slot — the MULTI/EXEC that couples
    /// them would otherwise fail with CROSSSLOT on Redis Cluster. Wrapping the whole stream name
    /// in braces nested them when the name already carried a hash tag (the idiomatic
    /// <c>KeyPrefix = "{app}"</c> co-location), and Redis then read <c>{app</c> as the marker's
    /// tag — a different slot, and CROSSSLOT on every publish.
    /// </summary>
    public RedisKey WorkerPublishDedupKey(string publishId)
        => $"{{{HashTagOf(((string?)WorkerStream)!)}}}:publish:{publishId}";

    /// <summary>
    /// The key text Redis Cluster hashes for <paramref name="key"/>: the substring between the
    /// first <c>{</c> and the first <c>}</c> after it when that is non-empty, otherwise the whole
    /// key. A name whose braces do not form one such tag returns unchanged — see the validator,
    /// which rejects those, because no marker key could then share the stream's slot.
    /// </summary>
    internal static string HashTagOf(string key)
    {
        var open = key.IndexOf('{');
        if (open < 0)
            return key;
        var close = key.IndexOf('}', open + 1);
        return close > open + 1 ? key.Substring(open + 1, close - open - 1) : key;
    }

    private RedisKey Resolve(string? configured, string role)
        => !string.IsNullOrWhiteSpace(configured)
            ? configured
            : $"{_options.KeyPrefix}:transport:{role}";
}
