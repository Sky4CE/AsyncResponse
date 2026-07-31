global using DbAckMode = AsyncResponse.Transports.SqlServer.SqlServerAckMode;
global using DbBackgroundFailureContext = AsyncResponse.Transports.SqlServer.SqlServerBackgroundFailureContext;
global using DbSubscriberOptions = AsyncResponse.Transports.SqlServer.SqlServerSubscriberOptions;
global using DbSubscriberRole = AsyncResponse.Transports.SqlServer.SqlServerSubscriberRole;
global using DbTransportDelivery = AsyncResponse.Transports.SqlServer.SqlServerTransportDelivery;
global using DbTransportOptions = AsyncResponse.Transports.SqlServer.SqlServerAsyncResponseTransportOptions;
global using DbTransportOptionsValidator = AsyncResponse.Transports.SqlServer.SqlServerTransportOptionsValidator;
using Microsoft.Extensions.Logging;

namespace AsyncResponse.Transports.SqlServer;

/// <summary>
/// Applies acknowledgement, redelivery, and dead-letter policy to SQL Server transport deliveries.
/// All machinery lives in <see cref="DbMessageDispatcherBase"/> (shared source, compiled into this
/// assembly); the global using aliases above bind the provider seam types, and this derivation
/// supplies only the display strings rendered into logs and telemetry.
/// </summary>
internal sealed class SqlServerMessageDispatcher(
    Func<SqlServerTransportDelivery, CancellationToken, Task> handler,
    SqlServerAsyncResponseTransportOptions options,
    SqlServerSubscriberOptions subscriberOptions,
    ILogger logger,
    SqlServerSubscriberRole role)
    : DbMessageDispatcherBase(
        handler,
        options,
        subscriberOptions,
        logger,
        role,
        providerName: "SQL Server",
        unitNoun: "row",
        telemetryName: "sqlserver");
