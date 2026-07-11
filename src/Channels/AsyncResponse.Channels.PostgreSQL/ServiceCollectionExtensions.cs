using AsyncResponse;
using AsyncResponse.Channels.PostgreSQL;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>DI registration for the PostgreSQL-backed AsyncResponse channel package.</summary>
public static class PostgreSqlAsyncResponseChannelServiceCollectionExtensions
{
    /// <summary>
    /// Registers PostgreSQL <c>LISTEN/NOTIFY</c> as the response channel — behind
    /// <see cref="IAsyncResponsePublisher"/>, <see cref="IAsyncResponseSubscriber"/>, and
    /// <see cref="IRecoverableAsyncResponseSubscriber"/> — together with the durable PostgreSQL
    /// recovery-state store. The host must register a shared <see cref="Npgsql.NpgsqlDataSource"/>
    /// singleton, typically from the application's existing PostgreSQL connection string.
    /// </summary>
    public static AsyncResponseRegistrationBuilder WithPostgreSqlChannel(
        this AsyncResponseRegistrationBuilder builder,
        Action<PostgreSqlAsyncResponseChannelOptions>? configure = null)
    {
        var services = builder.Services;
        services.AddOptions();
        if (configure is not null)
            services.Configure(configure);

        services.TryAddSingleton<PostgreSqlChannelSql>();

        services.TryAddSingleton<PostgreSqlRecoveryStateStore>();
        services.Replace(ServiceDescriptor.Singleton<IRecoveryStateStore>(provider => provider.GetRequiredService<PostgreSqlRecoveryStateStore>()));
        services.Replace(ServiceDescriptor.Singleton<IRecoveryStateScanner>(provider => provider.GetRequiredService<PostgreSqlRecoveryStateStore>()));

        services.TryAddSingleton<PostgreSqlAsyncResponseChannel>();
        services.Replace(ServiceDescriptor.Singleton<IAsyncResponsePublisher>(provider => provider.GetRequiredService<PostgreSqlAsyncResponseChannel>()));
        services.Replace(ServiceDescriptor.Singleton<IRawAsyncResponsePublisher>(provider => provider.GetRequiredService<PostgreSqlAsyncResponseChannel>()));
        services.Replace(ServiceDescriptor.Singleton<IAsyncResponseSubscriber>(provider => provider.GetRequiredService<PostgreSqlAsyncResponseChannel>()));
        services.Replace(ServiceDescriptor.Singleton<IRecoverableAsyncResponseSubscriber>(provider => provider.GetRequiredService<PostgreSqlAsyncResponseChannel>()));
        services.Replace(ServiceDescriptor.Singleton<IActiveSubscriberProbe>(provider => provider.GetRequiredService<PostgreSqlAsyncResponseChannel>()));

        services.Replace(ServiceDescriptor.Singleton<IRecoverableAsyncResponseBuilder>(provider => new RecoverableAsyncResponseBuilder(
            provider.GetRequiredService<IRecoverableAsyncResponseSubscriber>(),
            provider.GetService<IWorkerTransport>(),
            provider.GetService<IAsyncResponseReplyTargetProvider>(),
            provider.GetRequiredService<AsyncResponseContextPropagation>())));
        services.Replace(ServiceDescriptor.Singleton<IAsyncResponseBuilder>(provider => provider.GetRequiredService<IRecoverableAsyncResponseBuilder>()));

        services.AddSingleton(new AsyncResponseChannelMarker(PostgreSqlAsyncResponseChannelOptions.ChannelName));
        return builder;
    }
}
