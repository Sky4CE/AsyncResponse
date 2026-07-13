using Xunit;

namespace AsyncResponse.Tests;

public class CallbackExpressionConverterTests
{
    [Fact]
    public void LiteralArguments_AreCapturedByValue()
    {
        var orderId = 42;

        var call = CallbackExpressionConverter.ToReflectionCall<IRecoverySpy>(spy => spy.OnWorkerJob(orderId));

        Assert.Equal(typeof(IRecoverySpy).FullName, call.ServiceInterfaceFullName);
        Assert.Equal(nameof(IRecoverySpy.OnWorkerJob), call.MethodName);
        var param = Assert.Single(call.Params);
        Assert.Null(param.Placeholder);
        Assert.Equal(42, param.Value);
    }

    [Fact]
    public void PlaceholderMarkers_BecomeRuntimePlaceholders()
    {
        var call = CallbackExpressionConverter.ToReflectionCall<IRecoverySpy>(
            spy => spy.OnResume(Placeholder.Payload<OperationResult>()));

        Assert.Equal(PlaceholderType.Payload, Assert.Single(call.Params).Placeholder);
    }

    [Fact]
    public void MixedLiteralAndPlaceholderArguments_AreConvertedPositionally()
    {
        var flowName = "order-flow";

        var call = CallbackExpressionConverter.ToReflectionCall<IFlowCallbacks>(
            flow => flow.ResumeAsync(flowName, Placeholder.Payload<OperationResult>(), Placeholder.CorrelationId()));

        Assert.Equal(3, call.Params.Length);
        Assert.Equal("order-flow", call.Params[0].Value);
        Assert.Equal(PlaceholderType.Payload, call.Params[1].Placeholder);
        Assert.Equal(PlaceholderType.CorrelationId, call.Params[2].Placeholder);
    }

    [Fact]
    public void ValueChangingConversion_IsPreserved()
    {
        var value = 42;

        var call = CallbackExpressionConverter.ToReflectionCall<IFlowCallbacks>(
            flow => flow.AcceptLongAsync((long)value));

        Assert.Equal(42L, Assert.Single(call.Params).Value);
    }

    [Fact]
    public void MethodCallArguments_AreRejected()
        => Assert.Throws<NotSupportedException>(() =>
            CallbackExpressionConverter.ToReflectionCall<IRecoverySpy>(spy => spy.OnWorkerJob(int.Parse("42"))));

    [Fact]
    public void ArgumentsReferencingTheServiceParameter_AreRejected()
        => Assert.Throws<NotSupportedException>(() =>
            CallbackExpressionConverter.ToReflectionCall<IFlowCallbacks>(flow => flow.ResumeAsync(
                flow.FlowName, Placeholder.Payload<OperationResult>(), Placeholder.CorrelationId())));

    [Fact]
    public void StaticMethodCallBodies_AreRejected()
        => Assert.Throws<NotSupportedException>(() =>
            CallbackExpressionConverter.ToReflectionCall<IFlowCallbacks>(_ => StaticResumeAsync()));

    [Fact]
    public void NonMethodCallBodies_AreRejected()
        => Assert.Throws<NotSupportedException>(() =>
            CallbackExpressionConverter.ToReflectionCall<IRecoverySpy>(spy => Task.CompletedTask));

    [Fact]
    public void InvokingAPlaceholderMarkerDirectly_Throws()
        => Assert.Throws<InvalidOperationException>(() => Placeholder.CorrelationId());

    public interface IFlowCallbacks
    {
        string FlowName { get; }
        Task ResumeAsync(string flowName, OperationResult payload, string correlationId);
        Task AcceptLongAsync(long value);
    }

    private static Task StaticResumeAsync() => Task.CompletedTask;
}
