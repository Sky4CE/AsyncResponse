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

    [Fact]
    public void Cap_NeverSplitsASurrogatePair_AtTheBoundary()
    {
        // Regression (r24): the cap sliced at a UTF-16 code-unit boundary, so a cap landing inside
        // a surrogate pair kept a lone high surrogate — ill-formed UTF-16 that every framework
        // UTF-8 encoder silently replaces with U+FFFD on the wire, so the delivered trace no
        // longer round-tripped. The cap now backs off to the last whole code point.
        var input = new string('x', 99) + "\U0001F600" + new string('y', 50); // emoji spans units 99-100

        var result = RemoteStackTrace.Cap(input, 100);

        Assert.NotNull(result);
        Assert.StartsWith(new string('x', 99), result);
        // The kept prefix ends on a whole code point (the emoji was dropped, not split)...
        Assert.False(char.IsHighSurrogate(result![98]));
        Assert.Contains("truncated", result);
        // ...and the whole capped string survives a UTF-8 round trip byte-identically — a lone
        // surrogate would come back as U+FFFD and fail this equality.
        Assert.Equal(result, System.Text.Encoding.UTF8.GetString(System.Text.Encoding.UTF8.GetBytes(result)));
    }
}
