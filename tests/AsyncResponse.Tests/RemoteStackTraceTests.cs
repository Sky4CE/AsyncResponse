using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// The remote-stack-trace wire policy: opt-out of carrying traces at all, and a hard length cap so a
/// buggy or hostile remote cannot push a multi-megabyte trace into the envelope and the logs.
/// </summary>
public class RemoteStackTraceTests
{
    [Fact]
    public void Cap_ReturnsInputUnchanged_WhenUnderLimit()
        => Assert.Equal("short trace", RemoteStackTrace.Cap("short trace", 100));

    [Fact]
    public void Cap_Truncates_WhenOverLimit()
    {
        var input = new string('x', 1000);

        var result = RemoteStackTrace.Cap(input, 100);

        Assert.NotNull(result);
        Assert.StartsWith(new string('x', 100), result);
        Assert.Contains("truncated", result);
        // Capped to the limit plus the (small, fixed) marker — never the original multi-KB length.
        Assert.True(result!.Length < 200);
    }

    [Fact]
    public void Cap_ReturnsNull_ForNull() => Assert.Null(RemoteStackTrace.Cap(null, 100));

    [Fact]
    public void Cap_ReturnsInput_ForNonPositiveMax() => Assert.Equal("abc", RemoteStackTrace.Cap("abc", 0));

    [Fact]
    public void ForWire_ReturnsNull_WhenIncludeFalse()
        => Assert.Null(RemoteStackTrace.ForWire(new string('x', 50), include: false, maxLength: 1000));

    [Fact]
    public void ForWire_CapsTrace_WhenIncludeTrue()
    {
        var input = new string('y', 1000);

        var result = RemoteStackTrace.ForWire(input, include: true, maxLength: 50);

        Assert.NotNull(result);
        Assert.Contains("truncated", result);
        Assert.True(result!.Length < 150);
    }
}
