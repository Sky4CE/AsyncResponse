using Microsoft.Extensions.Hosting;

namespace AsyncResponse;

/// <summary>
/// Internal marker registered by each response-channel registration
/// (<c>.WithInMemoryChannel()</c> / <c>.WithRedisChannel()</c>). The
/// <see cref="AsyncResponseStartupValidator"/> asserts exactly one channel is present.
/// </summary>
internal sealed class AsyncResponseChannelMarker(string name)
{
    public string Name { get; } = name;
}

/// <summary>
/// Internal marker registered by each worker-transport registration
/// (<c>.WithInMemoryTransport()</c> / <c>.WithGooglePubSubTransport(...)</c>). The
/// <see cref="AsyncResponseStartupValidator"/> asserts exactly one transport is present.
/// </summary>
internal sealed class AsyncResponseTransportMarker(string name)
{
    public string Name { get; } = name;
}

/// <summary>
/// Validates at host startup that <c>AddAsyncResponse()</c> was paired with exactly one response
/// channel and exactly one worker transport. Both are mandatory core concepts: without a channel,
/// waiters can never receive a response; without a transport, worker dispatch cannot run. This
/// turns silently-broken configuration into a fast, explicit failure on boot.
/// </summary>
internal sealed class AsyncResponseStartupValidator(
    IEnumerable<AsyncResponseChannelMarker> _channels,
    IEnumerable<AsyncResponseTransportMarker> _transports) : IHostedService
{
    /// <summary>Starts this service.</summary>
    public Task StartAsync(CancellationToken cancellationToken)
    {
        var channelNames = _channels.Select(c => c.Name).Distinct(StringComparer.Ordinal).ToArray();

        if (channelNames.Length == 0)
            throw new InvalidOperationException(
                "AsyncResponse has no response channel registered. After AddAsyncResponse(), call " +
                ".WithInMemoryChannel() (AsyncResponse.Core) or .WithRedisChannel() (AsyncResponse.Channels.Redis). " +
                "Without a channel, waiters can never receive a response.");

        if (channelNames.Length > 1)
            throw new InvalidOperationException(
                $"AsyncResponse has multiple response channels registered ({string.Join(", ", channelNames)}). " +
                "Register exactly one channel.");

        var transportNames = _transports.Select(t => t.Name).Distinct(StringComparer.Ordinal).ToArray();

        if (transportNames.Length == 0)
            throw new InvalidOperationException(
                "AsyncResponse has no worker transport registered. After AddAsyncResponse(), call " +
                ".WithInMemoryTransport() (AsyncResponse.Core), .WithGooglePubSubTransport(...) " +
                "(AsyncResponse.Transports.GooglePubSub), or another full AsyncResponse transport package. " +
                "Without a transport, EnqueueWorkerAsync cannot dispatch worker jobs.");

        if (transportNames.Length > 1)
            throw new InvalidOperationException(
                $"AsyncResponse has multiple worker transports registered ({string.Join(", ", transportNames)}). " +
                "Register exactly one transport.");

        return Task.CompletedTask;
    }

    /// <summary>Stops this service.</summary>
    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
