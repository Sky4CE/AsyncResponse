using Microsoft.Extensions.Hosting;

namespace AsyncResponse;

/// <summary>
/// Internal marker registered by each response-channel registration
/// (<c>.WithInMemoryChannel()</c> / <c>.WithRedisChannel()</c>). The
/// <see cref="AsyncResponseStartupValidator"/> asserts exactly one is present.
/// </summary>
internal sealed class AsyncResponseChannelMarker(string name)
{
    public string Name { get; } = name;
}

/// <summary>
/// Validates at host startup that <c>AddAsyncResponse()</c> was paired with exactly one response
/// channel. A channel is mandatory — without one, waiters can never receive a response — so this
/// turns a silently-broken configuration into a fast, explicit failure on boot.
/// </summary>
internal sealed class AsyncResponseStartupValidator(IEnumerable<AsyncResponseChannelMarker> _channels) : IHostedService
{
    public Task StartAsync(CancellationToken cancellationToken)
    {
        var names = _channels.Select(c => c.Name).Distinct(StringComparer.Ordinal).ToArray();

        if (names.Length == 0)
            throw new InvalidOperationException(
                "AsyncResponse has no response channel registered. After AddAsyncResponse(), call " +
                ".WithInMemoryChannel() (AsyncResponse.Core) or .WithRedisChannel() (AsyncResponse.Channels.Redis). " +
                "Without a channel, waiters can never receive a response.");

        if (names.Length > 1)
            throw new InvalidOperationException(
                $"AsyncResponse has multiple response channels registered ({string.Join(", ", names)}). " +
                "Register exactly one channel.");

        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
