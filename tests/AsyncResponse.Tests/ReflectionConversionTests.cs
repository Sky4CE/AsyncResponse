using System.Text.Json;
using Xunit;

namespace AsyncResponse.Tests;

public class ReflectionConversionTests
{
    [Fact]
    public void As_JsonElement_DeserializesToTargetType()
    {
        var element = JsonSerializer.Deserialize<object>("""{"Status":2,"Message":"done"}""");

        var result = element.As<OperationResult>();

        Assert.Equal(OperationStatus.Completed, result.Status);
        Assert.Equal("done", result.Message);
    }

    [Fact]
    public void As_JsonString_DeserializesToTargetType()
    {
        var result = """{"Status":1}""".As<OperationResult>();

        Assert.Equal(OperationStatus.Running, result.Status);
    }

    [Fact]
    public void As_InstanceOfTargetType_PassesThrough()
    {
        var payload = new OperationResult { Status = OperationStatus.Completed };

        Assert.Same(payload, payload.As<OperationResult>());
    }

    [Fact]
    public void As_NullToReferenceType_ReturnsNull()
        => Assert.Null(((object?)null).As<OperationResult>());

    [Fact]
    public void As_NullToNonNullableValueType_Throws()
        => Assert.Throws<InvalidCastException>(() => ((object?)null).As<int>());

    [Fact]
    public void As_BoxedValueToNullable_PassesThrough()
        => Assert.Equal(42, ((object)42).As<int?>());

    [Fact]
    public void ResolveCallback_SubstitutesPlaceholdersPositionally()
    {
        var template = new ReflectionCallDto
        {
            ServiceInterfaceFullName = "X",
            MethodName = "Y",
            Params =
            [
                CallbackParam.ForValue("literal"),
                CallbackParam.ForPlaceholder(PlaceholderType.Payload),
                CallbackParam.ForPlaceholder(PlaceholderType.Exception),
                CallbackParam.ForPlaceholder(PlaceholderType.CorrelationId)
            ]
        };
        var payload = new OperationResult();
        var exception = new InvalidOperationException("boom");

        var invocation = ReflectionExtensions.ResolveCallback(template, payload, exception, "corr-1");

        Assert.Equal("literal", invocation.Params[0]);
        Assert.Same(payload, invocation.Params[1]);
        Assert.Same(exception, invocation.Params[2]);
        Assert.Equal("corr-1", invocation.Params[3]);
    }
}
