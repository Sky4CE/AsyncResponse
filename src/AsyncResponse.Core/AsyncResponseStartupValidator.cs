using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

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

/// <summary>Internal marker registered by each durable-flow state-store registration.</summary>
internal sealed class AsyncResponseDurableFlowStoreMarker(Type storeType)
{
    public Type StoreType { get; } = storeType;
    public string Name { get; } = storeType.FullName ?? storeType.Name;
}

/// <summary>
/// Validates at host startup that <c>AddAsyncResponse()</c> was paired with exactly one response
/// channel, one worker transport, and one durable-flow state store. These are mandatory core
/// choices; making each explicit keeps the fluent registration complete and prevents silently
/// unusable services from reaching production.
/// </summary>
internal sealed class AsyncResponseStartupValidator(
    IEnumerable<AsyncResponseChannelMarker> _channels,
    IEnumerable<AsyncResponseTransportMarker> _transports,
    IEnumerable<AsyncResponseDurableFlowStoreMarker> _flowStores,
    IOptions<AsyncResponseOptions> _options) : IHostedService
{
    /// <summary>Starts this service.</summary>
    public Task StartAsync(CancellationToken cancellationToken)
    {
        ValidateWatchdogOptions(_options.Value.Watchdog);

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

        var flowStores = _flowStores.DistinctBy(store => store.StoreType).ToArray();
        if (flowStores.Length == 0)
        {
            throw new InvalidOperationException(
                "AsyncResponse has no durable-flow state store registered. After AddAsyncResponse(), call " +
                ".WithInMemoryDurableFlows() (AsyncResponse.Core), a provider registration such as " +
                ".WithPostgreSqlDurableFlows(...), or .WithDurableFlows<TStore>() for an application-owned store.");
        }

        if (flowStores.Length > 1)
        {
            throw new InvalidOperationException(
                $"AsyncResponse has multiple durable-flow state stores registered ({string.Join(", ", flowStores.Select(store => store.Name))}). " +
                "Register exactly one durable-flow store.");
        }

        return Task.CompletedTask;
    }

    /// <summary>
    /// Fails fast on watchdog misconfiguration: a non-positive interval spins the scan loop, a
    /// non-positive stale threshold flags every entry, and a negative startup delay throws deep
    /// inside <see cref="Task.Delay(TimeSpan)"/> long after registration.
    /// </summary>
    private static void ValidateWatchdogOptions(AsyncResponseWatchdogOptions watchdog)
    {
        if (watchdog.Interval <= TimeSpan.Zero)
            throw new InvalidOperationException(
                $"{nameof(AsyncResponseOptions)}.{nameof(AsyncResponseOptions.Watchdog)}.{nameof(AsyncResponseWatchdogOptions.Interval)} must be positive.");
        if (watchdog.StaleAfter <= TimeSpan.Zero)
            throw new InvalidOperationException(
                $"{nameof(AsyncResponseOptions)}.{nameof(AsyncResponseOptions.Watchdog)}.{nameof(AsyncResponseWatchdogOptions.StaleAfter)} must be positive.");
        if (watchdog.StartupDelay < TimeSpan.Zero)
            throw new InvalidOperationException(
                $"{nameof(AsyncResponseOptions)}.{nameof(AsyncResponseOptions.Watchdog)}.{nameof(AsyncResponseWatchdogOptions.StartupDelay)} cannot be negative.");
    }

    /// <summary>Stops this service.</summary>
    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
