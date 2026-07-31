using AsyncResponse;
using AsyncResponse.Transports.SqlServer;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>DI registration for the Microsoft SQL Server AsyncResponse transport package.</summary>
public static class SqlServerAsyncResponseTransportServiceCollectionExtensions
{
    /// <summary>
    /// Registers SQL Server as the worker transport and response ingress in one call:
    /// worker jobs are inserted into the configured worker queue, hosted subscribers claim rows with
    /// <c>UPDLOCK, ROWLOCK, READPAST</c> (SQL Server's <c>SKIP LOCKED</c> equivalent), and response
    /// rows are fed into the transport-neutral <see cref="IAsyncResponseIngress"/>. Configure
    /// <see cref="SqlServerAsyncResponseTransportOptions.ConnectionString"/> with the application's
    /// SQL Server database.
    /// </summary>
    public static AsyncResponseRegistrationBuilder WithSqlServerTransport(
        this AsyncResponseRegistrationBuilder builder,
        Action<SqlServerAsyncResponseTransportOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);

        var services = builder.Services;
        services.AddOptions();
        services.Configure(configure);

        services.TryAddSingleton<SqlServerTransportStore>();
        services.TryAddSingleton<SqlServerWorkerTransport>();
        services.Replace(ServiceDescriptor.Singleton<IWorkerTransport>(provider => provider.GetRequiredService<SqlServerWorkerTransport>()));
        services.Replace(ServiceDescriptor.Singleton<IAsyncResponseReplyTargetProvider, SqlServerReplyTargetProvider>());
        services.AddSingleton(provider =>
        {
            // Resolved ack modes declared to the Core startup validator, which vetoes early ACK on
            // the worker queue durable-flow wake-ups ride (see AsyncResponseStartupValidator).
            var options = provider.GetRequiredService<Microsoft.Extensions.Options.IOptions<SqlServerAsyncResponseTransportOptions>>().Value;
            return new AsyncResponseTransportMarker(SqlServerAsyncResponseTransportOptions.TransportName)
            {
                WorkerSubscriberUsesEarlyAck = options.WorkerSubscriber.AckMode == SqlServerAckMode.AckAfterEnqueue,
                WorkerAckModePath = $"{nameof(SqlServerAsyncResponseTransportOptions)}.{nameof(options.WorkerSubscriber)}.{nameof(options.WorkerSubscriber.AckMode)}",
                ResponseSubscriberUsesEarlyAck = options.ResponseSubscriber.AckMode == SqlServerAckMode.AckAfterEnqueue,
                ResponseAckModePath = $"{nameof(SqlServerAsyncResponseTransportOptions)}.{nameof(options.ResponseSubscriber)}.{nameof(options.ResponseSubscriber.AckMode)}"
            };
        });

        services.AddHostedService<SqlServerWorkerSubscriber>();
        services.AddHostedService<SqlServerResponseIngressSubscriber>();

        return builder;
    }
}
