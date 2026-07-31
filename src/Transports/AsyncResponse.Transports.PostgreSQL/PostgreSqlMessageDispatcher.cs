global using DbAckMode = AsyncResponse.Transports.PostgreSQL.PostgreSqlAckMode;
global using DbBackgroundFailureContext = AsyncResponse.Transports.PostgreSQL.PostgreSqlBackgroundFailureContext;
global using DbSubscriberOptions = AsyncResponse.Transports.PostgreSQL.PostgreSqlSubscriberOptions;
global using DbSubscriberRole = AsyncResponse.Transports.PostgreSQL.PostgreSqlSubscriberRole;
global using DbTransportDelivery = AsyncResponse.Transports.PostgreSQL.PostgreSqlTransportDelivery;
global using DbTransportOptions = AsyncResponse.Transports.PostgreSQL.PostgreSqlAsyncResponseTransportOptions;
global using DbTransportOptionsValidator = AsyncResponse.Transports.PostgreSQL.PostgreSqlTransportOptionsValidator;
using Microsoft.Extensions.Logging;

namespace AsyncResponse.Transports.PostgreSQL;

/// <summary>
/// Applies acknowledgement, redelivery, and dead-letter policy to PostgreSQL transport deliveries.
/// All machinery lives in <see cref="DbMessageDispatcherBase"/> (shared source, compiled into this
/// assembly); the global using aliases above bind the provider seam types, and this derivation
/// supplies only the display strings rendered into logs and telemetry.
/// </summary>
internal sealed class PostgreSqlMessageDispatcher(
    Func<PostgreSqlTransportDelivery, CancellationToken, Task> handler,
    PostgreSqlAsyncResponseTransportOptions options,
    PostgreSqlSubscriberOptions subscriberOptions,
    ILogger logger,
    PostgreSqlSubscriberRole role)
    : DbMessageDispatcherBase(
        handler,
        options,
        subscriberOptions,
        logger,
        role,
        providerName: "PostgreSQL",
        unitNoun: "row",
        telemetryName: "postgresql");
