using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using System.Text.Json;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Verifies the default no-Redis setup: process-local response channel plus process-local
/// recovery store.
/// </summary>
public class InMemoryAsyncResponseTests
{
    private const string CorrelationId = "in-memory-correlation-id";

    [Fact]
    public async Task AddAsyncResponse_CompletesWaiterThroughTransportNeutralIngress()
    {
        var provider = CreateProvider();
        var asyncResponse = provider.GetRequiredService<IAsyncResponseBuilder>();
        var ingress = provider.GetRequiredService<IAsyncResponseIngress>();

        var result = await asyncResponse
            .For<OperationResult>()
            .WithTimeout(TimeSpan.FromSeconds(5))
            .Until(response => response.Status != OperationStatus.Running)
            .WaitAsync(async correlationId =>
            {
                await ingress.HandleResponseMessageAsync(
                    JsonSerializer.Serialize(new OperationResult { Status = OperationStatus.Running }),
                    correlationId);

                await ingress.HandleResponseMessageAsync(
                    JsonSerializer.Serialize(new OperationResult { Status = OperationStatus.Completed, Message = "done" }),
                    correlationId);
            });

        Assert.Equal(OperationStatus.Completed, result.Status);
        Assert.Equal("done", result.Message);
    }

    [Fact]
    public async Task AddAsyncResponse_NoSubscriber_UsesRecoveryStoreCallbacks()
    {
        var spy = new RecoverySpy();
        var provider = CreateProvider(services => services.AddSingleton<IRecoverySpy>(spy));
        var store = provider.GetRequiredService<IRecoveryStateStore>();
        var publisher = provider.GetRequiredService<IAsyncResponsePublisher>();

        await store.SaveAsync(
            CorrelationId,
            new RecoveryState
            {
                CorrelationId = CorrelationId,
                PayloadTypeFullName = typeof(OperationResult).FullName,
                RegisteredAtUtc = DateTime.UtcNow,
                ResumeCallback = new ReflectionCallDto
                {
                    ServiceInterfaceFullName = typeof(IRecoverySpy).FullName!,
                    MethodName = nameof(IRecoverySpy.OnResume),
                    Params = [CallbackParam.ForPlaceholder(PlaceholderType.Payload)]
                },
                FailureCallback = new ReflectionCallDto
                {
                    ServiceInterfaceFullName = typeof(IRecoverySpy).FullName!,
                    MethodName = nameof(IRecoverySpy.OnFailure),
                    Params = [CallbackParam.ForPlaceholder(PlaceholderType.Exception)]
                }
            },
            TimeSpan.FromMinutes(5));

        await publisher.SetResponse(
            new OperationResult { Status = OperationStatus.Completed, Message = "recovered" },
            CorrelationId);

        var resumed = Assert.IsType<OperationResult>(Assert.Single(spy.ResumedPayloads));
        Assert.Equal("recovered", resumed.Message);
        Assert.Empty(spy.Failures);
        Assert.Null(await store.GetAsync(CorrelationId));
    }

    private static ServiceProvider CreateProvider(Action<IServiceCollection>? configure = null)
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddAsyncResponse(options =>
        {
            options.DefaultTimeout = TimeSpan.FromSeconds(5);
            options.RecoveryStateExpiry = TimeSpan.FromMinutes(5);
        });
        configure?.Invoke(services);
        return services.BuildServiceProvider();
    }
}
