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
        // Factory registration so the transport shares the singleton store: DI would otherwise
        // pick the public constructor (internal ctors are invisible to it), giving the publisher
        // a private store whose in-process MessagePublished event the subscribers never observe —
        // silently downgrading every same-process publish from instant wake to poll-interval
        // latency. Mirrors the MongoDB transport's factory registration.
        services.TryAddSingleton(provider => new PostgreSqlWorkerTransport(
            provider.GetRequiredService<Microsoft.Extensions.Options.IOptions<PostgreSqlAsyncResponseTransportOptions>>(),
            provider.GetRequiredService<PostgreSqlTransportStore>()));
        services.Replace(ServiceDescriptor.Singleton<IWorkerTransport>(provider => provider.GetRequiredService<PostgreSqlWorkerTransport>()));
        services.Replace(ServiceDescriptor.Singleton<IAsyncResponseReplyTargetProvider, PostgreSqlReplyTargetProvider>());
        services.AddSingleton(provider =>
        {
            // Resolved ack modes declared to the Core startup validator, which vetoes early ACK on
            // the worker queue durable-flow wake-ups ride (see AsyncResponseStartupValidator).
            var options = provider.GetRequiredService<Microsoft.Extensions.Options.IOptions<PostgreSqlAsyncResponseTransportOptions>>().Value;
            return new AsyncResponseTransportMarker(PostgreSqlAsyncResponseTransportOptions.TransportName)
            {
                WorkerSubscriberUsesEarlyAck = options.WorkerSubscriber.AckMode == PostgreSqlAckMode.AckAfterEnqueue,
                WorkerAckModePath = $"{nameof(PostgreSqlAsyncResponseTransportOptions)}.{nameof(options.WorkerSubscriber)}.{nameof(options.WorkerSubscriber.AckMode)}",
                ResponseSubscriberUsesEarlyAck = options.ResponseSubscriber.AckMode == PostgreSqlAckMode.AckAfterEnqueue,
                ResponseAckModePath = $"{nameof(PostgreSqlAsyncResponseTransportOptions)}.{nameof(options.ResponseSubscriber)}.{nameof(options.ResponseSubscriber.AckMode)}"
            };
        });

        services.AddHostedService<PostgreSqlWorkerSubscriber>();
        services.AddHostedService<PostgreSqlResponseIngressSubscriber>();

        return builder;
    }
}
