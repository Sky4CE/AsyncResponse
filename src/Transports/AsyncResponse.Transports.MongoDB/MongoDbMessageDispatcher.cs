global using DbAckMode = AsyncResponse.Transports.MongoDB.MongoDbAckMode;
global using DbBackgroundFailureContext = AsyncResponse.Transports.MongoDB.MongoDbBackgroundFailureContext;
global using DbSubscriberOptions = AsyncResponse.Transports.MongoDB.MongoDbSubscriberOptions;
global using DbSubscriberRole = AsyncResponse.Transports.MongoDB.MongoDbSubscriberRole;
global using DbTransportDelivery = AsyncResponse.Transports.MongoDB.MongoDbTransportDelivery;
global using DbTransportOptions = AsyncResponse.Transports.MongoDB.MongoDbAsyncResponseTransportOptions;
global using DbTransportOptionsValidator = AsyncResponse.Transports.MongoDB.MongoDbTransportOptionsValidator;
using Microsoft.Extensions.Logging;

namespace AsyncResponse.Transports.MongoDB;

/// <summary>
/// Applies acknowledgement, redelivery, and dead-letter policy to MongoDB transport deliveries.
/// All machinery lives in <see cref="DbMessageDispatcherBase"/> (shared source, compiled into this
/// assembly); the global using aliases above bind the provider seam types, and this derivation
/// supplies only the display strings rendered into logs and telemetry.
/// </summary>
internal sealed class MongoDbMessageDispatcher(
    Func<MongoDbTransportDelivery, CancellationToken, Task> handler,
    MongoDbAsyncResponseTransportOptions options,
    MongoDbSubscriberOptions subscriberOptions,
    ILogger logger,
    MongoDbSubscriberRole role)
    : DbMessageDispatcherBase(
        handler,
        options,
        subscriberOptions,
        logger,
        role,
        providerName: "MongoDB",
        unitNoun: "document",
        telemetryName: "mongodb");
