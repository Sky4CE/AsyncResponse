using AsyncResponse;
using AsyncResponse.Redis;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>
/// DI registrations for the Redis async-response transport.
/// </summary>
public static class RedisAsyncResponseServiceCollectionExtensions
{
    /// <summary>
    /// Registers the Redis transport behind <see cref="IAsyncResponsePublisher"/>,
    /// <see cref="IAsyncResponseSubscriber"/>, and <see cref="IAsyncResponseIngress"/>, plus the
    /// fluent <see cref="IAsyncResponseBuilder"/>.
    /// <para>
    /// Requires a <c>StackExchange.Redis.IConnectionMultiplexer</c> singleton to be registered by
    /// the host. All three transport interfaces resolve to one shared instance — per-interface
    /// instances would split internal per-channel state.
    /// </para>
    /// </summary>
    public static IServiceCollection AddRedisAsyncResponse(
        this IServiceCollection services,
        Action<RedisAsyncResponseOptions>? configure = null)
    {
        services.AddOptions();
        if (configure is not null)
        {
            services.Configure(configure);
        }

        services.TryAddSingleton<RedisAsyncResponseTransport>();
        services.TryAddSingleton<IAsyncResponsePublisher>(provider => provider.GetRequiredService<RedisAsyncResponseTransport>());
        services.TryAddSingleton<IAsyncResponseSubscriber>(provider => provider.GetRequiredService<RedisAsyncResponseTransport>());
        services.TryAddSingleton<IAsyncResponseIngress>(provider => provider.GetRequiredService<RedisAsyncResponseTransport>());
        services.AddAsyncResponseBuilder();

        return services;
    }

    /// <summary>
    /// Registers the report-only watchdog that periodically scans the persisted recovery state
    /// and warns about stale entries (no live waiter, no response for too long). Register it in
    /// a single host per Redis — running it everywhere only duplicates the reports. Each scan
    /// result is also published to <see cref="AsyncResponseWatchdogState"/> for the
    /// <see cref="AsyncResponseRecoveryHealthCheck"/>.
    /// </summary>
    public static IServiceCollection AddAsyncResponseWatchdog(
        this IServiceCollection services,
        Action<AsyncResponseWatchdogOptions>? configure = null)
    {
        services.AddOptions();
        if (configure is not null)
        {
            services.Configure(configure);
        }

        services.TryAddSingleton<AsyncResponseWatchdogState>();
        services.AddHostedService<AsyncResponseWatchdog>();

        return services;
    }

    /// <summary>
    /// Adds the <see cref="AsyncResponseRecoveryHealthCheck"/> to the health-check pipeline. It
    /// reads the watchdog's cached snapshot (probes never touch Redis) and reports at most
    /// <c>Degraded</c> — stale flows are an operator signal, not process ill-health, so keep
    /// <c>Degraded</c> mapped to HTTP 200 on readiness endpoints (the ASP.NET Core default).
    /// </summary>
    public static IHealthChecksBuilder AddAsyncResponseRecoveryCheck(
        this IHealthChecksBuilder builder,
        string name = "async-response-recovery",
        IEnumerable<string>? tags = null)
    {
        builder.Services.TryAddSingleton<AsyncResponseWatchdogState>();
        builder.Add(new HealthCheckRegistration(
            name,
            provider => new AsyncResponseRecoveryHealthCheck(provider.GetRequiredService<AsyncResponseWatchdogState>()),
            failureStatus: HealthStatus.Degraded,
            tags));

        return builder;
    }
}
