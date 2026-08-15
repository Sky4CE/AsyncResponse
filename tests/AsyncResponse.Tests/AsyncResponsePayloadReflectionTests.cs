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

    [Fact]
    public void OverridesOnRecovery_IgnoresSameNameMethodsWithTheWrongReturnType()
    {
        // GetMethod matches name and parameters only. A `void`/`Task` OnRecovery cannot
        // implicitly implement `RecoveryAction OnRecovery()` — the interface default still
        // applies at dispatch — so reporting true waved exactly that payload through the
        // channels' fail-fast guard, and its flow silently failed instead of resuming.
        Assert.False(AsyncResponsePayloadReflection.OverridesOnRecovery(typeof(VoidOnRecoveryPayload)));
        Assert.False(AsyncResponsePayloadReflection.OverridesOnRecovery(typeof(TaskOnRecoveryPayload)));
        Assert.False(AsyncResponsePayloadReflection.OverridesOnRecovery(typeof(VoidOnRecoveryStructPayload)));
    }

    [Fact]
    public void OverridesOnRecovery_DetectsExplicitImplementationBesideAWrongTypedMethod()
    {
        // A wrong-typed public method must not mask a genuine explicit implementation: the fast
        // path declines and the interface map answers.
        Assert.True(AsyncResponsePayloadReflection.OverridesOnRecovery(typeof(ExplicitBesideVoidPayload)));
    }

    private sealed class DefaultRecoveryPayload : IAsyncResponsePayload;

    private sealed class VoidOnRecoveryPayload : IAsyncResponsePayload
    {
        public void OnRecovery()
        {
        }
    }

    private sealed class TaskOnRecoveryPayload : IAsyncResponsePayload
    {
        public Task OnRecovery() => Task.CompletedTask;
    }

    private struct VoidOnRecoveryStructPayload : IAsyncResponsePayload
    {
        public readonly void OnRecovery()
        {
        }
    }

    private sealed class ExplicitBesideVoidPayload : IAsyncResponsePayload
    {
        public void OnRecovery()
        {
        }

        RecoveryAction IAsyncResponsePayload.OnRecovery() => RecoveryAction.Resume;
    }
}
