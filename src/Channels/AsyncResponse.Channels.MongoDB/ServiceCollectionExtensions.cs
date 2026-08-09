using AsyncResponse;
using AsyncResponse.Channels.MongoDB;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using MongoDB.Driver;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>DI registration for the MongoDB-backed AsyncResponse channel package.</summary>
public static class MongoDbAsyncResponseChannelServiceCollectionExtensions
{
    /// <summary>
    /// Registers MongoDB change streams as the response channel — behind
    /// <see cref="IAsyncResponsePublisher"/>, <see cref="IAsyncResponseSubscriber"/>, and
    /// <see cref="IRecoverableAsyncResponseSubscriber"/> — together with the durable TTL-indexed
    /// MongoDB recovery-state store. Hosts may register an <see cref="IMongoDatabase"/> (or an
    /// <see cref="IMongoClient"/> plus <see cref="MongoDbAsyncResponseChannelOptions.DatabaseName"/>)
    /// singleton, or set <see cref="MongoDbAsyncResponseChannelOptions.ConnectionString"/> here.
    /// Change streams require the server to run as a replica set (single-node is sufficient).
    /// </summary>
    public static AsyncResponseRegistrationBuilder WithMongoDbChannel(
        this AsyncResponseRegistrationBuilder builder,
        Action<MongoDbAsyncResponseChannelOptions>? configure = null)
    {
        var services = builder.Services;
        services.AddOptions();
        if (configure is not null)
            services.Configure(configure);

        // The store reuses a host-registered IMongoDatabase / IMongoClient when present; otherwise it
        // creates and owns a client from the options. Nothing is registered as a bare
        // IMongoClient/IMongoDatabase service, so unrelated resolutions of those types are never
        // answered — or broken — by this package.
        // One registry per container: DI-hosted Mongo components claim their effective
        // collections so a cross-component collision (e.g. a durable-flow store on the channel's
        // derived counters collection) fails startup in either construction order.
        services.TryAddSingleton<MongoNamespaceRegistry>();
        services.TryAddSingleton(provider =>
        {
            var options = provider.GetRequiredService<IOptions<MongoDbAsyncResponseChannelOptions>>();
            var registry = provider.GetRequiredService<MongoNamespaceRegistry>();

            var database = provider.GetService<IMongoDatabase>();
            if (database is not null)
                return new MongoDbChannelStore(database, options, namespaceRegistry: registry);

            if (string.IsNullOrWhiteSpace(options.Value.DatabaseName))
                throw new InvalidOperationException($"{nameof(MongoDbAsyncResponseChannelOptions)}.{nameof(MongoDbAsyncResponseChannelOptions.DatabaseName)} must be configured when no IMongoDatabase is registered.");

            var sharedClient = provider.GetService<IMongoClient>();
            if (sharedClient is not null)
                return new MongoDbChannelStore(sharedClient.GetDatabase(options.Value.DatabaseName), options, namespaceRegistry: registry);

            if (string.IsNullOrWhiteSpace(options.Value.ConnectionString))
                throw new InvalidOperationException($"{nameof(MongoDbAsyncResponseChannelOptions)}.{nameof(MongoDbAsyncResponseChannelOptions.ConnectionString)} must be configured when no IMongoDatabase or IMongoClient is registered.");

            var ownedClient = new MongoClient(options.Value.ConnectionString);
            return new MongoDbChannelStore(ownedClient.GetDatabase(options.Value.DatabaseName), options, ownedClient, namespaceRegistry: registry);
        });

        services.TryAddSingleton<MongoDbRecoveryStateStore>();
        services.Replace(ServiceDescriptor.Singleton<IRecoveryStateStore>(provider => provider.GetRequiredService<MongoDbRecoveryStateStore>()));
        services.Replace(ServiceDescriptor.Singleton<IRecoveryStateScanner>(provider => provider.GetRequiredService<MongoDbRecoveryStateStore>()));

        services.TryAddSingleton<MongoDbAsyncResponseChannel>();
        services.Replace(ServiceDescriptor.Singleton<IAsyncResponsePublisher>(provider => provider.GetRequiredService<MongoDbAsyncResponseChannel>()));
        services.Replace(ServiceDescriptor.Singleton<IRawAsyncResponsePublisher>(provider => provider.GetRequiredService<MongoDbAsyncResponseChannel>()));
        services.Replace(ServiceDescriptor.Singleton<IAsyncResponseSubscriber>(provider => provider.GetRequiredService<MongoDbAsyncResponseChannel>()));
        services.Replace(ServiceDescriptor.Singleton<IRecoverableAsyncResponseSubscriber>(provider => provider.GetRequiredService<MongoDbAsyncResponseChannel>()));
        services.Replace(ServiceDescriptor.Singleton<IActiveSubscriberProbe>(provider => provider.GetRequiredService<MongoDbAsyncResponseChannel>()));

        services.Replace(ServiceDescriptor.Singleton<IRecoverableAsyncResponseBuilder>(provider => new RecoverableAsyncResponseBuilder(
            provider.GetRequiredService<IRecoverableAsyncResponseSubscriber>(),
            provider.GetService<IWorkerTransport>(),
            provider.GetService<IAsyncResponseReplyTargetProvider>(),
            provider.GetRequiredService<AsyncResponseContextPropagation>())));
        services.Replace(ServiceDescriptor.Singleton<IAsyncResponseBuilder>(provider => provider.GetRequiredService<IRecoverableAsyncResponseBuilder>()));

        services.AddSingleton(new AsyncResponseChannelMarker(MongoDbAsyncResponseChannelOptions.ChannelName));
        return builder;
    }
}
