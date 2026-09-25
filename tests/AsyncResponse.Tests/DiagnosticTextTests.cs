using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// <see cref="DiagnosticText.EscapedExcerpt"/> is how every rejected id and persisted name is
/// quoted into a log line, an exception message, or an activity status: what the line shows must
/// say unambiguously what was received, and the quoted text must not be able to reshape the line.
/// </summary>
public sealed class DiagnosticTextTests
{
    [Fact]
    public void ControlCharactersSeparatorsAndUnpairedSurrogates_AreEscaped()
    {
        Assert.Equal(@"a\u000d\u000ab", DiagnosticText.EscapedExcerpt("a\r\nb"));
        Assert.Equal(@"x\ud800y", DiagnosticText.EscapedExcerpt("x\ud800y"));
        Assert.Equal(@"p\u2028q", DiagnosticText.EscapedExcerpt("p\u2028q"));
    }

    [Fact]
    public void ALiteralEscapeSequence_DoesNotReadLikeTheCharacterItNames()
    {
        // Regression: the backslash itself was not escaped, so a received six-character literal
        // backslash-u-000a rendered exactly like an escaped line feed — the log no longer said
        // what was received. Escaping the backslash makes every backslash-u in the output ours.
        var literal = DiagnosticText.EscapedExcerpt(@"a\u000ab");
        var lineFeed = DiagnosticText.EscapedExcerpt("a\nb");

        Assert.NotEqual(lineFeed, literal);
        Assert.Equal(@"a\u005cu000ab", literal);
    }

    [Theory]
    [InlineData(0x061C)] // ARABIC LETTER MARK
    [InlineData(0x200E)] // LEFT-TO-RIGHT MARK
    [InlineData(0x200F)] // RIGHT-TO-LEFT MARK
    [InlineData(0x202A)] // LEFT-TO-RIGHT EMBEDDING
    [InlineData(0x202E)] // RIGHT-TO-LEFT OVERRIDE
    [InlineData(0x2066)] // LEFT-TO-RIGHT ISOLATE
    [InlineData(0x2069)] // POP DIRECTIONAL ISOLATE
    public void BidirectionalFormattingControls_AreEscaped(int codePoint)
    {
        // Regression: a right-to-left override passed through raw and visually reordered the rest
        // of the log line in bidi-aware viewers.
        var control = (char)codePoint;
        var excerpt = DiagnosticText.EscapedExcerpt($"id{control}gpj.exe");

        Assert.DoesNotContain(control.ToString(), excerpt, StringComparison.Ordinal);
        Assert.Contains($"\\u{codePoint:x4}", excerpt, StringComparison.Ordinal);
    }

    [Fact]
    public void ZeroWidthJoiner_AndWellFormedPairs_StayReadable()
    {
        // The bidi list is explicit, not all of Unicode's format category: that category also
        // holds the zero-width joiner composed emoji are made of — a valid id shape.
        const string family = "tenant-\U0001F468\u200D\U0001F469\u200D\U0001F467";

        Assert.Equal(family, DiagnosticText.EscapedExcerpt(family));
    }

    [Fact]
    public void TheBudgetAppliesToTheInput_AndACutIsMarked()
    {
        var excerpt = DiagnosticText.EscapedExcerpt(new string('\\', 50), maxLength: 10);

        Assert.Equal(string.Concat(Enumerable.Repeat(@"\u005c", 10)) + "…", excerpt);
    }
}
