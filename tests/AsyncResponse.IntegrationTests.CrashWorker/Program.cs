using AsyncResponse.DurableFlows.PostgreSQL;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace AsyncResponse.IntegrationTests.CrashWorker;

/// <summary>
/// Entry point of the crash-suite subprocess. An explicit class rather than top-level statements:
/// the integration tests reference this assembly for <see cref="CrashWorkerContract"/> and already
/// use the sample app's global <c>Program</c> type, which a second one would make ambiguous.
/// </summary>
internal static class CrashWorkerProgram
{
    private static async Task<int> Main(string[] args)
    {
        var settings = CrashWorkerSettings.FromEnvironment();
        using var host = BuildHost(args, settings);

        await host.Services.GetRequiredService<CrashSuiteJournal>().EnsureCreatedAsync(CancellationToken.None);

        switch (settings.Role)
        {
            case CrashWorkerContract.Roles.Start:
            {
                // The host is built but never started: no subscriber runs here, so the start job
                // can only be picked up by a worker process — the one the scenario is about to kill.
                var flows = host.Services.GetRequiredService<IDurableFlows>();
                var flowId = await flows.StartAsync<CrashSuiteFlow, CrashSuiteInput>(
                    new CrashSuiteInput(Environment.GetEnvironmentVariable(CrashWorkerContract.Env.Scenario) ?? "unnamed"),
                    settings.FlowId);
                Console.Out.WriteLine($"{CrashWorkerContract.StartedMarker} {flowId}");
                return 0;
            }

            case CrashWorkerContract.Roles.Worker:
            {
                await host.StartAsync();
                Console.Out.WriteLine(
                    $"{CrashWorkerContract.ReadyMarker} worker={settings.WorkerLabel} crashPoint={settings.CrashPoint} " +
                    $"lease={settings.LeaseDuration} renew={settings.LeaseRenewInterval} lockTimeout={settings.LockTimeout}");

                using var lifetime = new CancellationTokenSource(settings.MaxLifetime);
                await host.WaitForShutdownAsync(lifetime.Token);
                return 0;
            }

            default:
                throw new InvalidOperationException($"{CrashWorkerContract.Env.Role} has unknown value '{settings.Role}'.");
        }
    }

    private static IHost BuildHost(string[] args, CrashWorkerSettings settings)
    {
        // Empty on purpose: the suite runs this assembly from the integration tests' output
        // directory, where the sample app's appsettings.json also lands — the default builder would
        // load it (content root = working directory) and let it reconfigure this process.
        // Everything here comes from CrashWorkerSettings and nothing else.
        var builder = Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings { Args = args });

        // stdout is the suite's diagnostic transcript. Debug for the engine: the line that matters
        // most when a takeover goes wrong ("skipping duplicate delivery") is logged at Debug.
        builder.Logging.AddSimpleConsole(console =>
        {
            console.SingleLine = true;
            console.TimestampFormat = "HH:mm:ss.fff ";
        });
        builder.Logging.SetMinimumLevel(LogLevel.Information);
        builder.Logging.AddFilter("AsyncResponse", LogLevel.Debug);

        builder.Services.AddSingleton(settings);
        builder.Services.AddSingleton(_ => NpgsqlDataSource.Create(settings.ConnectionString));
        builder.Services.AddSingleton<CrashSuiteJournal>();
        builder.Services.AddSingleton<ICrashSuiteJobs, CrashSuiteJobs>();
        builder.Services.AddSingleton<IDurableFlowExecutionObserver, CrashPointObserver>();

        var asyncResponse = builder.Services.AddAsyncResponse();

        // The flow has no awaited step, so the channel is never used; one must be registered.
        asyncResponse.WithInMemoryChannel();

        asyncResponse.WithPostgreSqlTransport(options =>
        {
            options.SchemaName = settings.Schema;
            options.MessageTable = CrashWorkerContract.Tables.TransportMessages;
            options.NotificationChannel = $"{settings.Schema}_notify";
            options.WorkerQueue = CrashWorkerContract.WorkerQueue;

            // The claim a dead worker leaves behind is what redelivers its job: LockTimeout is how
            // long the queue waits before handing the row to the next subscriber.
            options.LockTimeout = settings.LockTimeout;
            options.WorkerSubscriber.MaxDeliveryAttempts = settings.MaxDeliveryAttempts;
            options.WorkerSubscriber.RedeliveryDelay = settings.RedeliveryDelay;
            options.WorkerSubscriber.EmptyPollDelay = TimeSpan.FromMilliseconds(100);
        });

        asyncResponse.WithPostgreSqlDurableFlows(options =>
        {
            options.SchemaName = settings.Schema;
            options.TableName = CrashWorkerContract.Tables.FlowState;
            options.ExecutionLeaseDuration = settings.LeaseDuration;
            options.ExecutionLeaseRenewInterval = settings.LeaseRenewInterval;
        });

        asyncResponse.WithDurableFlow<CrashSuiteFlow, CrashSuiteInput>();
        asyncResponse.WithDurableFlow<CrashSuiteChildFlow, CrashSuiteInput>();

        // Last, so it replaces the forward WithPostgreSqlDurableFlows registered. Same lifetime as
        // the concrete store (singleton), which the startup validator insists on.
        builder.Services.Replace(ServiceDescriptor.Singleton(provider => CrashingFlowStateStore.Wrap(
            provider.GetRequiredService<PostgreSqlFlowStateStore>(),
            provider.GetRequiredService<CrashSuiteJournal>(),
            settings)));

        return builder.Build();
    }
}
