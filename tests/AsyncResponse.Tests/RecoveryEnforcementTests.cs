using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using StackExchange.Redis;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// The recovery-classification override and its durable-channel enforcement: the reflection helper
/// that detects whether a payload overrides <c>OnRecovery</c>, and the Redis channel's
/// fail-fast when a recovery-enabled flow's payload would otherwise rely on the conservative
    /// default. The in-memory channel (which cannot survive a redeploy) does not expose the
    /// recoverable subscriber/builder capability. Also pins that the live path is untouched:
    /// omitting <c>Until</c> still returns the first response.
/// </summary>
public class RecoveryEnforcementTests
{
    private const string CorrelationId = "enforcement-cid";

    private static ReflectionCallDto ResumeCallback() => new()
    {
        ServiceInterfaceFullName = typeof(IRecoverySpy).FullName!,
        MethodName = nameof(IRecoverySpy.OnResume),
        Params = [CallbackParam.ForPlaceholder(PlaceholderType.Payload)]
    };

    [Fact]
    public void OverrideDetection_DistinguishesOverriddenFromDefault()
    {
        Assert.True(AsyncResponsePayloadReflection.OverridesOnRecovery(typeof(OperationResult)));
        Assert.True(AsyncResponsePayloadReflection.OverridesOnRecovery(typeof(SuccessOnlyPayload)));
        Assert.False(AsyncResponsePayloadReflection.OverridesOnRecovery(typeof(DefaultRecoveryPayload)));
    }

    [Fact]
    public async Task RedisChannel_RecoveryCallbackWithoutOverride_FailsFast()
    {
        var subscriber = RedisSubscriber();

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => subscriber.CreateRecoverableResponseWaiter<DefaultRecoveryPayload>(CorrelationId, resumeCallback: ResumeCallback()));

        Assert.Contains(nameof(IAsyncResponsePayload.OnRecovery), ex.Message, StringComparison.Ordinal);
        Assert.Contains(typeof(DefaultRecoveryPayload).ToString(), ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task RedisChannel_FailureCallbackWithoutOverride_AlsoFailsFast()
    {
        var subscriber = RedisSubscriber();

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => subscriber.CreateRecoverableResponseWaiter<DefaultRecoveryPayload>(CorrelationId, failureCallback: ResumeCallback()));
    }

    [Fact]
    public void InMemoryChannel_ExposesTheFullRecoverableSurface()
    {
        // The in-memory channel implements the same recoverable contract as the durable channels
        // (callbacks stored in the process-local recovery store), so tests and dev hosts exercise
        // the real recovery routing. IAsyncResponseBuilder resolves to the same recoverable
        // implementation, mirroring the durable channel registrations.
        var provider = InMemoryProvider();

        Assert.NotNull(provider.GetService<IRecoverableAsyncResponseBuilder>());
        Assert.NotNull(provider.GetService<IRecoverableAsyncResponseSubscriber>());
        Assert.Same(provider.GetService<IRecoverableAsyncResponseBuilder>(), provider.GetService<IAsyncResponseBuilder>());
    }

    [Fact]
    public async Task InMemoryChannel_RecoveryCallbackWithoutOverride_FailsFastLikeDurableChannels()
    {
        var provider = InMemoryProvider();
        var subscriber = provider.GetRequiredService<IRecoverableAsyncResponseSubscriber>();

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => subscriber.CreateRecoverableResponseWaiter<DefaultRecoveryPayload>(CorrelationId, resumeCallback: ResumeCallback()));

        Assert.Contains(nameof(IAsyncResponsePayload.OnRecovery), ex.Message, StringComparison.Ordinal);
        Assert.Contains(typeof(DefaultRecoveryPayload).ToString(), ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task NoUntil_ReturnsFirstResponse_LivePathUnchanged()
    {
        var provider = InMemoryProvider();
        var asyncResponse = provider.GetRequiredService<IAsyncResponseBuilder>();
        var publisher = provider.GetRequiredService<IAsyncResponsePublisher>();

        var result = await asyncResponse
            .For<OperationResult>()
            .WithTimeout(TimeSpan.FromSeconds(5))
            .WaitAsync(ctx => publisher.SetResponse(
                new OperationResult { Status = OperationStatus.Completed, Message = "first" }, ctx.CorrelationId));

        Assert.Equal(OperationStatus.Completed, result.Status);
        Assert.Equal("first", result.Message);
    }

    private static IRecoverableAsyncResponseSubscriber RedisSubscriber()
    {
        var multiplexer = new Mock<IConnectionMultiplexer>();
        multiplexer.Setup(m => m.GetSubscriber(It.IsAny<object?>())).Returns(Mock.Of<ISubscriber>());
        multiplexer.Setup(m => m.GetDatabase(It.IsAny<int>(), It.IsAny<object?>())).Returns(Mock.Of<IDatabase>());

        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddSingleton(multiplexer.Object);
        services.AddAsyncResponse().WithRedisChannel();
        return services.BuildServiceProvider().GetRequiredService<IRecoverableAsyncResponseSubscriber>();
    }

    private static ServiceProvider InMemoryProvider()
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddAsyncResponse().WithInMemoryChannel();
        return services.BuildServiceProvider();
    }
}

/// <summary>A payload that does NOT override <c>OnRecovery</c> (uses the conservative default).</summary>
public sealed class DefaultRecoveryPayload : IAsyncResponsePayload
{
    public string? Message { get; set; }
}
