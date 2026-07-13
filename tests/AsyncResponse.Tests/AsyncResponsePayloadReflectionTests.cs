using Xunit;

namespace AsyncResponse.Tests;

public class AsyncResponsePayloadReflectionTests
{
    [Fact]
    public void OverridesShouldResumeOnRecovery_RejectsNullType()
        => Assert.Throws<ArgumentNullException>(() => AsyncResponsePayloadReflection.OverridesShouldResumeOnRecovery(null!));

    [Fact]
    public void OverridesShouldResumeOnRecovery_ReturnsFalseForInterfaceAndNonPayloadTypes()
    {
        Assert.False(AsyncResponsePayloadReflection.OverridesShouldResumeOnRecovery(typeof(IAsyncResponsePayload)));
        Assert.False(AsyncResponsePayloadReflection.OverridesShouldResumeOnRecovery(typeof(string)));
    }

    [Fact]
    public void OverridesShouldResumeOnRecovery_DetectsDefaultInterfaceImplementation()
        => Assert.False(AsyncResponsePayloadReflection.OverridesShouldResumeOnRecovery(typeof(DefaultRecoveryPayload)));

    [Fact]
    public void DefaultShouldResumeOnRecovery_ReturnsFalse()
    {
        IAsyncResponsePayload payload = new DefaultRecoveryPayload();

        Assert.False(payload.ShouldResumeOnRecovery());
    }

    [Fact]
    public void OverridesShouldResumeOnRecovery_DetectsConcreteOverride()
        => Assert.True(AsyncResponsePayloadReflection.OverridesShouldResumeOnRecovery(typeof(OperationResult)));

    [Fact]
    public void DurableFlowFailedException_PreservesInnerException()
    {
        var inner = new InvalidOperationException("cause");
        var exception = new DurableFlowFailedException("terminal", inner);

        Assert.Equal("terminal", exception.Message);
        Assert.Same(inner, exception.InnerException);
    }

    private sealed class DefaultRecoveryPayload : IAsyncResponsePayload;
}
