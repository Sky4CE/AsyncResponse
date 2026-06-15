using AsyncResponse;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>
/// Health-check registration for AsyncResponse. This is the one feature deliberately added on the
/// health-checks pipeline rather than the fluent <c>AddAsyncResponse()</c> builder, because it
/// belongs to <c>AddHealthChecks()</c>.
/// </summary>
public static class AsyncResponseHealthCheckExtensions
{
    /// <summary>
    /// Adds the <see cref="AsyncResponseRecoveryHealthCheck"/> to the health-check pipeline. It
    /// reads the watchdog's cached snapshot (probes never touch the recovery store) and reports at
    /// most <c>Degraded</c> — stale flows are an operator signal, not process ill-health, so keep
    /// <c>Degraded</c> mapped to HTTP 200 on readiness endpoints (the ASP.NET Core default). The
    /// watchdog itself runs by default once <c>AddAsyncResponse()</c> is registered.
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
