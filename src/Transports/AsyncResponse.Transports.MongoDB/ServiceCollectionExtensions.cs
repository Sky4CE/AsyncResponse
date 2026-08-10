using AsyncResponse;
using AsyncResponse.Transports.MongoDB;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MongoDB.Driver;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>DI registration for the MongoDB AsyncResponse transport package.</summary>
public static class MongoDbAsyncResponseTransportServiceCollectionExtensions
{
    /// <summary>
    /// Registers MongoDB as the worker transport and response ingress in one call:
    /// worker jobs are inserted into the configured worker queue, hosted subscribers claim documents
    /// atomically with <c>findOneAndUpdate</c>, and response documents are fed into the
    /// transport-neutral <see cref="IAsyncResponseIngress"/>. Hosts may register an
    /// <see cref="IMongoDatabase"/> (or an <see cref="IMongoClient"/> plus
    /// <see cref="MongoDbAsyncResponseTransportOptions.DatabaseName"/>) singleton, or set
    /// <see cref="MongoDbAsyncResponseTransportOptions.ConnectionString"/> here.
    /// </summary>
    public static AsyncResponseRegistrationBuilder WithMongoDbTransport(
        this AsyncResponseRegistrationBuilder builder,
        Action<MongoDbAsyncResponseTransportOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);

        var services = builder.Services;
        services.AddOptions();
        services.Configure(configure);

        // The store reuses a host-registered IMongoDatabase / IMongoClient when present; otherwise it
        // creates and owns a client from the options. Nothing is registered as a bare
        // IMongoClient/IMongoDatabase service, so unrelated resolutions of those types are never
        // answered — or broken — by this package.
        services.TryAddSingleton<MongoNamespaceRegistry>();
        services.TryAddSingleton(provider =>
        {
            var options = provider.GetRequiredService<IOptions<MongoDbAsyncResponseTransportOptions>>();
            var logger = provider.GetService<ILogger<MongoDbTransportStore>>();
            var registry = provider.GetRequiredService<MongoNamespaceRegistry>();

            var database = provider.GetService<IMongoDatabase>();
            if (database is not null)
                return new MongoDbTransportStore(database, options, logger: logger, namespaceRegistry: registry);

            if (string.IsNullOrWhiteSpace(options.Value.DatabaseName))
                throw new InvalidOperationException($"{nameof(MongoDbAsyncResponseTransportOptions)}.{nameof(MongoDbAsyncResponseTransportOptions.DatabaseName)} must be configured when no IMongoDatabase is registered.");

            var sharedClient = provider.GetService<IMongoClient>();
            if (sharedClient is not null)
                return new MongoDbTransportStore(sharedClient.GetDatabase(options.Value.DatabaseName), options, logger: logger, namespaceRegistry: registry);

            if (string.IsNullOrWhiteSpace(options.Value.ConnectionString))
                throw new InvalidOperationException($"{nameof(MongoDbAsyncResponseTransportOptions)}.{nameof(MongoDbAsyncResponseTransportOptions.ConnectionString)} must be configured when no IMongoDatabase or IMongoClient is registered.");

            var ownedClient = new MongoClient(options.Value.ConnectionString);
            return new MongoDbTransportStore(ownedClient.GetDatabase(options.Value.DatabaseName), options, ownedClient, logger, namespaceRegistry: registry);
        });

        services.TryAddSingleton(provider => new MongoDbWorkerTransport(
            provider.GetRequiredService<IOptions<MongoDbAsyncResponseTransportOptions>>(),
            provider.GetRequiredService<MongoDbTransportStore>()));
        services.Replace(ServiceDescriptor.Singleton<IWorkerTransport>(provider => provider.GetRequiredService<MongoDbWorkerTransport>()));
        services.Replace(ServiceDescriptor.Singleton<IAsyncResponseReplyTargetProvider, MongoDbReplyTargetProvider>());
        services.AddSingleton(provider =>
        {
            // Resolved ack modes declared to the Core startup validator, which vetoes early ACK on
            // the worker queue durable-flow wake-ups ride (see AsyncResponseStartupValidator).
            var options = provider.GetRequiredService<Microsoft.Extensions.Options.IOptions<MongoDbAsyncResponseTransportOptions>>().Value;
            return new AsyncResponseTransportMarker(MongoDbAsyncResponseTransportOptions.TransportName)
            {
                WorkerSubscriberUsesEarlyAck = options.WorkerSubscriber.AckMode == MongoDbAckMode.AckAfterEnqueue,
                WorkerAckModePath = $"{nameof(MongoDbAsyncResponseTransportOptions)}.{nameof(options.WorkerSubscriber)}.{nameof(options.WorkerSubscriber.AckMode)}",
                ResponseSubscriberUsesEarlyAck = options.ResponseSubscriber.AckMode == MongoDbAckMode.AckAfterEnqueue,
                ResponseAckModePath = $"{nameof(MongoDbAsyncResponseTransportOptions)}.{nameof(options.ResponseSubscriber)}.{nameof(options.ResponseSubscriber.AckMode)}"
            };
        });

        services.AddHostedService<MongoDbWorkerSubscriber>();
        services.AddHostedService<MongoDbResponseIngressSubscriber>();

        return builder;
    }
}
