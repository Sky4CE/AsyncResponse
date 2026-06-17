using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using StackExchange.Redis;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// The recovery-classification override and its durable-channel enforcement: the reflection helper
/// that detects whether a payload overrides <c>ShouldResumeOnRecovery</c>, and the Redis channel's
/// fail-fast when a recovery-enabled flow's payload would otherwise rely on the conservative
/// default. The in-memory channel (which cannot survive a redeploy) is deliberately exempt. Also
/// pins that the live path is untouched: omitting <c>Until</c> still returns the first response.
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
        Assert.True(AsyncResponsePayloadReflection.OverridesShouldResumeOnRecovery(typeof(OperationResult)));
        Assert.True(AsyncResponsePayloadReflection.OverridesShouldResumeOnRecovery(typeof(SuccessOnlyPayload)));
        Assert.False(AsyncResponsePayloadReflection.OverridesShouldResumeOnRecovery(typeof(DefaultRecoveryPayload)));
    }

    [Fact]
    public async Task RedisChannel_RecoveryCallbackWithoutOverride_FailsFast()
    {
        var subscriber = RedisSubscriber();

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => subscriber.CreateResponseWaiter<DefaultRecoveryPayload>(CorrelationId, resumeCallback: ResumeCallback()));

        Assert.Contains(nameof(IAsyncResponsePayload.ShouldResumeOnRecovery), ex.Message, StringComparison.Ordinal);
        Assert.Contains(typeof(DefaultRecoveryPayload).ToString(), ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task RedisChannel_FailureCallbackWithoutOverride_AlsoFailsFast()
    {
        var subscriber = RedisSubscriber();

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => subscriber.CreateResponseWaiter<DefaultRecoveryPayload>(CorrelationId, failureCallback: ResumeCallback()));
    }

    [Fact]
    public async Task InMemoryChannel_RecoveryCallbackWithoutOverride_IsExempt()
    {
        var subscriber = InMemorySubscriber();

        // Must NOT throw: the in-memory channel cannot recover across a redeploy anyway, so the
        // override is not required; the conservative default applies to its in-process fallback.
        await using var waiter = await subscriber.CreateResponseWaiter<DefaultRecoveryPayload>(
            CorrelationId, resumeCallback: ResumeCallback(), timeout: TimeSpan.FromSeconds(5));

        Assert.NotNull(waiter);
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

    private static IAsyncResponseSubscriber RedisSubscriber()
    {
        var multiplexer = new Mock<IConnectionMultiplexer>();
        multiplexer.Setup(m => m.GetSubscriber(It.IsAny<object?>())).Returns(Mock.Of<ISubscriber>());
        multiplexer.Setup(m => m.GetDatabase(It.IsAny<int>(), It.IsAny<object?>())).Returns(Mock.Of<IDatabase>());

        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddSingleton(multiplexer.Object);
        services.AddAsyncResponse().WithRedisChannel();
        return services.BuildServiceProvider().GetRequiredService<IAsyncResponseSubscriber>();
    }

    private static IAsyncResponseSubscriber InMemorySubscriber()
        => InMemoryProvider().GetRequiredService<IAsyncResponseSubscriber>();

    private static ServiceProvider InMemoryProvider()
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddAsyncResponse().WithInMemoryChannel();
        return services.BuildServiceProvider();
    }
}

/// <summary>A payload that does NOT override <c>ShouldResumeOnRecovery</c> (uses the conservative default).</summary>
public sealed class DefaultRecoveryPayload : IAsyncResponsePayload
{
    public string? Message { get; set; }
}
