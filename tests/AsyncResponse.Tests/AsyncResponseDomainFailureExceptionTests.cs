using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// The exception that carries a non-resuming domain response down the lost-subscriber failure path.
/// Handlers pattern-match on it, so its properties and message must surface the correlation id,
/// payload type, and payload JSON.
/// </summary>
public class AsyncResponseDomainFailureExceptionTests
{
    [Fact]
    public void ExposesAllPropertiesAndMessage()
    {
        var ex = new AsyncResponseDomainFailureException(
            correlationId: "corr-1",
            payloadTypeFullName: "My.Payload",
            payloadJson: """{"Status":3}""");

        Assert.Equal("corr-1", ex.CorrelationId);
        Assert.Equal("My.Payload", ex.PayloadTypeFullName);
        Assert.Equal("""{"Status":3}""", ex.PayloadJson);

        Assert.Contains("corr-1", ex.Message, StringComparison.Ordinal);
        Assert.Contains("My.Payload", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void IsAnException_SoItCanBeThrownAndCaughtGenerically()
    {
        var ex = new AsyncResponseDomainFailureException("c", null, null);

        Assert.IsAssignableFrom<Exception>(ex);
    }
}
