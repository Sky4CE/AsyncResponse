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
    /// Per-publish dedup marker for the idempotent worker XADD. Hash-tagged with the stream name
    /// so the marker and the stream share a cluster slot — the MULTI/EXEC that couples them
    /// would otherwise fail with CROSSSLOT on Redis Cluster.
    /// </summary>
    public RedisKey WorkerPublishDedupKey(string publishId)
        => $"{{{(string?)WorkerStream}}}:publish:{publishId}";

    private RedisKey Resolve(string? configured, string role)
        => !string.IsNullOrWhiteSpace(configured)
            ? configured
            : $"{_options.KeyPrefix}:transport:{role}";
}
