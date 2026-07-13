using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Focused coverage for <see cref="IAsyncResponsePublisher.SetException"/> on the process-local
/// channel: live subscribers, ambient correlation fallback, fan-out, and lost-subscriber recovery.
/// </summary>
public sealed class InMemorySetExceptionTests
{
    [Fact]
    public async Task SetException_WithExplicitCorrelation_FaultsActiveWaiterWithSameException()
    {
        await using var provider = CreateProvider();
        var asyncResponse = provider.GetRequiredService<IAsyncResponseBuilder>();
        var publisher = provider.GetRequiredService<IAsyncResponsePublisher>();
        var expected = new InvalidOperationException("remote technical error");

        var thrown = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            asyncResponse
                .For<OperationResult>()
                .WithTimeout(TimeSpan.FromSeconds(5))
                .WaitAsync(ctx => publisher.SetException(expected, ctx.CorrelationId)));

        Assert.Same(expected, thrown);
    }

    [Fact]
    public async Task SetException_PublishesToExplicitCorrelationId()
    {
        await using var provider = CreateProvider();
        var asyncResponse = provider.GetRequiredService<IAsyncResponseBuilder>();
        var publisher = provider.GetRequiredService<IAsyncResponsePublisher>();
        var probe = provider.GetRequiredService<IActiveSubscriberProbe>();
        var correlationId = $"ambient-{Guid.NewGuid():N}";
        var expected = new InvalidOperationException("ambient technical error");

        var waitTask = asyncResponse
            .For<OperationResult>(correlationId)
            .WithTimeout(TimeSpan.FromSeconds(5))
            .WaitAsync();

        Assert.True(await WaitForSubscriberCountAsync(probe, correlationId, expected: 1));

        await publisher.SetException(expected, correlationId);

        var thrown = await Assert.ThrowsAsync<InvalidOperationException>(async () => await waitTask);
        Assert.Same(expected, thrown);
    }

    [Fact]
    public async Task SetException_WithMultipleWaitersForSameCorrelation_FaultsAllWaiters()
    {
        await using var provider = CreateProvider();
        var asyncResponse = provider.GetRequiredService<IAsyncResponseBuilder>();
        var publisher = provider.GetRequiredService<IAsyncResponsePublisher>();
        var probe = provider.GetRequiredService<IActiveSubscriberProbe>();
        var correlationId = $"shared-{Guid.NewGuid():N}";
        var expected = new ApplicationException("shared failure");

        var first = asyncResponse.For<OperationResult>(correlationId).WithTimeout(TimeSpan.FromSeconds(5)).WaitAsync();
        var second = asyncResponse.For<OperationResult>(correlationId).WithTimeout(TimeSpan.FromSeconds(5)).WaitAsync();

        Assert.True(await WaitForSubscriberCountAsync(probe, correlationId, expected: 2));

        await publisher.SetException(expected, correlationId);

        var firstException = await Assert.ThrowsAsync<ApplicationException>(async () => await first);
        var secondException = await Assert.ThrowsAsync<ApplicationException>(async () => await second);
        Assert.Same(expected, firstException);
        Assert.Same(expected, secondException);
        Assert.True(await WaitForSubscriberCountAsync(probe, correlationId, expected: 0));
    }

    [Fact]
    public async Task SetException_NoSubscriber_InvokesFailureCallbackAndDeletesRecoveryState()
    {
        var spy = new RecoverySpy();
        await using var provider = CreateProvider(services => services.AddSingleton<IRecoverySpy>(spy));
        var store = provider.GetRequiredService<IRecoveryStateStore>();
        var publisher = provider.GetRequiredService<IAsyncResponsePublisher>();
        var correlationId = $"lost-{Guid.NewGuid():N}";
        var expected = new InvalidOperationException("late technical error");

        await store.SaveAsync(
            correlationId,
            new RecoveryState
            {
                CorrelationId = correlationId,
                RegisteredAtUtc = DateTime.UtcNow,
                FailureCallback = FailureCallback()
            },
            TimeSpan.FromMinutes(5));

        await publisher.SetException(expected, correlationId);

        var failure = Assert.Single(spy.Failures);
        Assert.Same(expected, failure);
        Assert.Empty(await store.GetAllAsync(correlationId));
    }

    [Fact]
    public async Task SetException_NoSubscriberWithoutFailureCallback_KeepsRecoveryState()
    {
        await using var provider = CreateProvider();
        var store = provider.GetRequiredService<IRecoveryStateStore>();
        var publisher = provider.GetRequiredService<IAsyncResponsePublisher>();
        var correlationId = $"lost-no-callback-{Guid.NewGuid():N}";

        await store.SaveAsync(
            correlationId,
            new RecoveryState
            {
                CorrelationId = correlationId,
                RegisteredAtUtc = DateTime.UtcNow
            },
            TimeSpan.FromMinutes(5));

        await publisher.SetException(new InvalidOperationException("late technical error"), correlationId);

        Assert.NotEmpty(await store.GetAllAsync(correlationId));
    }

    [Fact]
    public async Task SetException_NullException_ThrowsBeforePublishing()
    {
        await using var provider = CreateProvider();
        var publisher = provider.GetRequiredService<IAsyncResponsePublisher>();

        await Assert.ThrowsAsync<ArgumentNullException>(() => publisher.SetException(null!, "cid"));
    }

    private static ServiceProvider CreateProvider(Action<IServiceCollection>? configure = null)
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddAsyncResponse(options => options.Watchdog.Enabled = false)
            .WithInMemoryChannel(options =>
            {
                options.DefaultTimeout = TimeSpan.FromSeconds(5);
                options.RecoveryStateExpiry = TimeSpan.FromMinutes(5);
            });
        configure?.Invoke(services);
        return services.BuildServiceProvider();
    }

    private static ReflectionCallDto FailureCallback() => new()
    {
        ServiceInterfaceFullName = typeof(IRecoverySpy).FullName!,
        MethodName = nameof(IRecoverySpy.OnFailure),
        Params = [CallbackParam.ForPlaceholder(PlaceholderType.Exception)]
    };

    private static async Task<bool> WaitForSubscriberCountAsync(
        IActiveSubscriberProbe probe,
        string correlationId,
        long expected)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(5);
        while (DateTime.UtcNow < deadline)
        {
            var count = await probe.CountActiveSubscribersAsync(correlationId);
            if (expected == 0 ? count == 0 : count >= expected)
                return true;

            await Task.Delay(25);
        }

        return false;
    }
}
