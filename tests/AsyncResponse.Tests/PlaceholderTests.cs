using Xunit;

namespace AsyncResponse.Tests;

public class PlaceholderTests
{
    [Fact]
    public void MarkerMethods_ThrowWhenInvokedOutsideExpressionTrees()
    {
        Assert.Contains("expression-tree markers", Assert.Throws<InvalidOperationException>(Placeholder.Payload<OperationResult>).Message);
        Assert.Contains("expression-tree markers", Assert.Throws<InvalidOperationException>(Placeholder.Exception).Message);
        Assert.Contains("expression-tree markers", Assert.Throws<InvalidOperationException>(Placeholder.CorrelationId).Message);
    }
}
