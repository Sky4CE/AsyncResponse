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

    private RedisKey Resolve(string? configured, string role)
        => !string.IsNullOrWhiteSpace(configured)
            ? configured
            : $"{_options.KeyPrefix}:transport:{role}";
}
