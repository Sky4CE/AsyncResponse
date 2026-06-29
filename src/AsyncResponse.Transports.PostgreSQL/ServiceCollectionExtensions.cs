using AsyncResponse;
using AsyncResponse.Transports.PostgreSQL;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>DI registration for the PostgreSQL AsyncResponse transport package.</summary>
public static class PostgreSqlAsyncResponseTransportServiceCollectionExtensions
{
    /// <summary>
    /// Registers PostgreSQL as the worker transport and response ingress in one call:
    /// worker jobs are inserted into the configured worker queue, hosted subscribers claim rows with
    /// <c>FOR UPDATE SKIP LOCKED</c>, and response rows are fed into the transport-neutral
    /// <see cref="IAsyncResponseIngress"/>. The host must register a shared
    /// <see cref="Npgsql.NpgsqlDataSource"/> singleton.
    /// </summary>
    public static AsyncResponseRegistrationBuilder WithPostgreSqlTransport(
        this AsyncResponseRegistrationBuilder builder,
        Action<PostgreSqlAsyncResponseTransportOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);

        var services = builder.Services;
        services.AddOptions();
        services.Configure(configure);

        services.TryAddSingleton<PostgreSqlTransportStore>();
        services.TryAddSingleton<PostgreSqlWorkerTransport>();
        services.Replace(ServiceDescriptor.Singleton<IWorkerTransport>(provider => provider.GetRequiredService<PostgreSqlWorkerTransport>()));
        services.Replace(ServiceDescriptor.Singleton<IAsyncResponseReplyTargetProvider, PostgreSqlReplyTargetProvider>());
        services.AddSingleton(new AsyncResponseTransportMarker(PostgreSqlAsyncResponseTransportOptions.TransportName));

        services.AddHostedService<PostgreSqlWorkerSubscriber>();
        services.AddHostedService<PostgreSqlResponseIngressSubscriber>();

        return builder;
    }
}
