using Xunit;

namespace AsyncResponse.Tests;

public class AsyncResponsePayloadReflectionTests
{
    [Fact]
    public void OverridesOnRecovery_RejectsNullType()
        => Assert.Throws<ArgumentNullException>(() => AsyncResponsePayloadReflection.OverridesOnRecovery(null!));

    [Fact]
    public void OverridesOnRecovery_ReturnsFalseForInterfaceAndNonPayloadTypes()
    {
        Assert.False(AsyncResponsePayloadReflection.OverridesOnRecovery(typeof(IAsyncResponsePayload)));
        Assert.False(AsyncResponsePayloadReflection.OverridesOnRecovery(typeof(string)));
    }

    [Fact]
    public void OverridesOnRecovery_DetectsDefaultInterfaceImplementation()
        => Assert.False(AsyncResponsePayloadReflection.OverridesOnRecovery(typeof(DefaultRecoveryPayload)));

    [Fact]
    public void DefaultOnRecovery_ReturnsFail()
    {
        // The conservative default: a payload never resumes a flow by omission.
        IAsyncResponsePayload payload = new DefaultRecoveryPayload();

        Assert.Equal(RecoveryAction.Fail, payload.OnRecovery());
    }

    [Fact]
    public void OverridesOnRecovery_DetectsConcreteOverride()
    {
        Assert.True(AsyncResponsePayloadReflection.OverridesOnRecovery(typeof(OperationResult)));
        Assert.True(AsyncResponsePayloadReflection.OverridesOnRecovery(typeof(IncidentStepResult)));
        Assert.True(AsyncResponsePayloadReflection.OverridesOnRecovery(typeof(SuccessOnlyPayload)));
    }

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
