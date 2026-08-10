using AsyncResponse;
using AsyncResponse.Channels.SqlServer;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>DI registration for the Microsoft SQL Server-backed AsyncResponse channel package.</summary>
public static class SqlServerAsyncResponseChannelServiceCollectionExtensions
{
    /// <summary>
    /// Registers SQL Server as the response channel — behind
    /// <see cref="IAsyncResponsePublisher"/>, <see cref="IAsyncResponseSubscriber"/>, and
    /// <see cref="IRecoverableAsyncResponseSubscriber"/> — together with the durable SQL Server
    /// recovery-state store. Active waiters are woken by an adaptive polling sweep (tight while
    /// waiters are subscribed, backed off while idle) because SQL Server has no
    /// <c>LISTEN/NOTIFY</c>; same-process deliveries bypass the sweep entirely. Configure
    /// <see cref="SqlServerAsyncResponseChannelOptions.ConnectionString"/> with the application's
    /// SQL Server database.
    /// </summary>
    public static AsyncResponseRegistrationBuilder WithSqlServerChannel(
        this AsyncResponseRegistrationBuilder builder,
        Action<SqlServerAsyncResponseChannelOptions>? configure = null)
    {
        var services = builder.Services;
        services.AddOptions();
        if (configure is not null)
            services.Configure(configure);

        services.TryAddSingleton<SqlServerChannelSql>();

        services.TryAddSingleton<SqlServerRecoveryStateStore>();
        services.Replace(ServiceDescriptor.Singleton<IRecoveryStateStore>(provider => provider.GetRequiredService<SqlServerRecoveryStateStore>()));
        services.Replace(ServiceDescriptor.Singleton<IRecoveryStateScanner>(provider => provider.GetRequiredService<SqlServerRecoveryStateStore>()));

        services.TryAddSingleton<SqlServerAsyncResponseChannel>();
        services.Replace(ServiceDescriptor.Singleton<IAsyncResponsePublisher>(provider => provider.GetRequiredService<SqlServerAsyncResponseChannel>()));
        services.Replace(ServiceDescriptor.Singleton<IRawAsyncResponsePublisher>(provider => provider.GetRequiredService<SqlServerAsyncResponseChannel>()));
        services.Replace(ServiceDescriptor.Singleton<IAsyncResponseSubscriber>(provider => provider.GetRequiredService<SqlServerAsyncResponseChannel>()));
        services.Replace(ServiceDescriptor.Singleton<IRecoverableAsyncResponseSubscriber>(provider => provider.GetRequiredService<SqlServerAsyncResponseChannel>()));
        services.Replace(ServiceDescriptor.Singleton<IActiveSubscriberProbe>(provider => provider.GetRequiredService<SqlServerAsyncResponseChannel>()));

        services.Replace(ServiceDescriptor.Singleton<IRecoverableAsyncResponseBuilder>(provider => new RecoverableAsyncResponseBuilder(
            provider.GetRequiredService<IRecoverableAsyncResponseSubscriber>(),
            provider.GetService<IWorkerTransport>(),
            provider.GetService<IAsyncResponseReplyTargetProvider>(),
            provider.GetRequiredService<AsyncResponseContextPropagation>(),
            provider.GetService<TimeProvider>())));
        services.Replace(ServiceDescriptor.Singleton<IAsyncResponseBuilder>(provider => provider.GetRequiredService<IRecoverableAsyncResponseBuilder>()));

        services.AddSingleton(new AsyncResponseChannelMarker(SqlServerAsyncResponseChannelOptions.ChannelName));
        return builder;
    }
}
