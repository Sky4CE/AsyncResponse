using System.Globalization;
using System.Text;

namespace AsyncResponse;

/// <summary>
/// Quotes UNTRUSTED text — an inbound correlation id, a persisted type or method name — into a log
/// line, an exception message, or an activity status without handing its author the line.
/// <para>
/// The ids and names quoted here are rejected precisely because they are malformed, and the
/// malformation is frequently the payload: a CR/LF pair inside the quoted excerpt ends the real
/// log entry and starts a forged one in every line-oriented sink, and an unpaired surrogate is
/// replaced with U+FFFD by the sink's UTF-8 encoder, so the persisted line no longer says what
/// was received. The excerpt therefore carries such code units as visible backslash-u escapes,
/// never raw.
/// </para>
/// </summary>
internal static class DiagnosticText
{
    private const char LineSeparator = (char)0x2028;
    private const char ParagraphSeparator = (char)0x2029;

    /// <summary>
    /// The first <paramref name="maxLength"/> UTF-16 code units of <paramref name="value"/> (cut
    /// through <see cref="PortableText.TruncateWellFormed"/>, so a whole surrogate pair is never
    /// split), with every control character, line/paragraph separator, and unpaired surrogate
    /// written as a backslash-u escape, and an ellipsis appended when anything was cut. The budget
    /// applies to the INPUT: an escape widens one unit to six, so the result is at most six times
    /// the budget.
    /// </summary>
    internal static string EscapedExcerpt(string value, int maxLength = 40)
    {
        var truncated = value.Length > maxLength;
        var text = truncated ? PortableText.TruncateWellFormed(value, maxLength) : value;

        var firstUnsafe = IndexOfUnsafe(text);
        if (firstUnsafe < 0)
            return truncated ? string.Concat(text, "…") : text;

        var builder = new StringBuilder(text.Length + 16);
        builder.Append(text, 0, firstUnsafe);
        for (var index = firstUnsafe; index < text.Length; index++)
        {
            var unit = text[index];
            if (IsPairStart(text, index))
            {
                // A well-formed pair is ordinary text: keep both halves.
                builder.Append(unit).Append(text[++index]);
                continue;
            }

            if (IsUnsafe(unit))
                builder.Append('\\').Append('u').Append(((int)unit).ToString("x4", CultureInfo.InvariantCulture));
            else
                builder.Append(unit);
        }

        if (truncated)
            builder.Append('…');

        return builder.ToString();
    }

    private static int IndexOfUnsafe(string text)
    {
        for (var index = 0; index < text.Length; index++)
        {
            if (IsPairStart(text, index))
            {
                index++;
                continue;
            }

            if (IsUnsafe(text[index]))
                return index;
        }

        return -1;
    }

    private static bool IsPairStart(string text, int index)
        => char.IsHighSurrogate(text[index]) && index + 1 < text.Length && char.IsLowSurrogate(text[index + 1]);

    // Reached only for a unit that is NOT part of a well-formed pair, so any surrogate seen here
    // is unpaired. The line and paragraph separators are not control characters to char.IsControl,
    // but they are line breaks to the viewers and parsers that matter for log forging.
    private static bool IsUnsafe(char unit)
        => char.IsControl(unit) || char.IsSurrogate(unit) || unit is LineSeparator or ParagraphSeparator;
}
