using System.Text;

namespace AsyncResponse;

/// <summary>
/// Compares persisted CLR type names the way type resolution does: by type identity, not by the
/// assembly versions baked into the string.
/// <para>
/// A persisted name is <c>typeof(T).FullName</c>, and for a closed generic that string carries every
/// argument's assembly-qualified name — <c>List`1[[System.Int32, System.Private.CoreLib,
/// Version=8.0.0.0, Culture=neutral, PublicKeyToken=…]]</c>. An ordinal comparison therefore
/// changed its verdict whenever an argument's assembly version did (an application release, a .NET
/// major upgrade), although resolution (<see cref="AsyncResponseTypeResolution"/>) matches loaded
/// assemblies by simple name and still finds the same type: every in-flight run of a flow with a
/// generic input stranded on its next wake-up, and an allowlisted generic callback target was
/// refused. <see cref="Normalize"/> drops the <c>Version</c>, <c>Culture</c> and
/// <c>PublicKeyToken</c> parts of the assembly names inside generic-argument brackets, at any
/// depth. Persisted names are never rewritten — only compared through this — so the ledger format
/// and older replicas are unaffected.
/// </para>
/// <para>
/// All three, because resolution ignores all three: it binds an assembly-qualified argument to the
/// loaded assembly with the same SIMPLE name (<c>AssemblyName.ReferenceMatchesDefinition</c>
/// compares nothing else), so every spelling of the key resolves to the same type. Keeping the key
/// in the identity only refused what the reflection path resolves and runs: after a deploy that
/// strong-named or re-signed an argument's assembly, a registered flow's run, a child replay, an
/// idempotent restart and an allowlisted callback were all refused. The simple name stays, so a
/// type moved to another assembly behind a <c>[TypeForwardedTo]</c> is still a different type here
/// although resolution follows the forwarder — telling the two apart needs resolving the name,
/// which the statically-typed flow path deliberately never does.
/// </para>
/// <para>
/// Only well-formed values are dropped: a <c>Version=</c> that is not dotted decimal, a
/// <c>PublicKeyToken=</c> that is neither sixteen hex digits nor <c>null</c>, or a
/// <c>Culture=</c> that is not a culture name leaves the whole name as it is, compared ordinally:
/// store data that is not what a runtime writes is not rewritten into something that matches.
/// </para>
/// </summary>
internal static class TypeNameIdentity
{
    private const string VersionPart = "Version=";
    private const string CulturePart = "Culture=";
    private const string PublicKeyTokenPart = "PublicKeyToken=";

    // System.Version's grammar as an assembly name prints it: major.minor[.build[.revision]], each
    // component a 16-bit value.
    private const int MinVersionComponents = 2;
    private const int MaxVersionComponents = 4;
    private const int MaxVersionComponentDigits = 5;

    // An eight-byte token printed as hex, or "null" for an assembly that is not strong-named.
    private const int PublicKeyTokenHexDigits = 16;
    private const string NoPublicKeyToken = "null";

    // "neutral" or a culture name (language-region-…); LOCALE_NAME_MAX_LENGTH bounds it.
    private const int MaxCultureNameLength = 85;

    private enum Part
    {
        Version,
        Culture,
        PublicKeyToken
    }

    /// <summary>
    /// <paramref name="typeName"/> without the <c>Version</c>, <c>Culture</c> and
    /// <c>PublicKeyToken</c> parts of its generic arguments' assembly names. A name outside the
    /// persisted type-name limits is store data no resolver would accept, and is returned
    /// unchanged; so is one that carries none of those parts, and one with a malformed value.
    /// </summary>
    public static string? Normalize(string? typeName)
    {
        if (typeName is null
            || typeName.IndexOf('[') < 0
            || typeName.IndexOf('=') < 0
            || !AsyncResponseTypeResolution.IsWithinResolutionLimits(typeName))
        {
            return typeName;
        }

        // One forward pass (the limits above bound the length): copy everything except a
        // ", Version=…" / ", Culture=…" / ", PublicKeyToken=…" segment inside brackets, which runs
        // to the next ',' or ']'. No dropped value holds a bracket (each grammar below refuses
        // one), so the depth count stays exact and the result is its own normal form.
        var builder = new StringBuilder(typeName.Length);
        var dropped = false;
        var depth = 0;
        var index = 0;
        while (index < typeName.Length)
        {
            var unit = typeName[index];
            if (unit == '[')
            {
                depth++;
            }
            else if (unit == ']')
            {
                depth--;
            }
            else if (unit == ',' && depth > 0 && PartValueAt(typeName, index + 1) is var (part, start, end))
            {
                if (!IsWellFormed(part, typeName, start, end))
                    return typeName;

                dropped = true;
                index = end;
                continue;
            }

            builder.Append(unit);
            index++;
        }

        return dropped ? builder.ToString() : typeName;
    }

    /// <summary>Whether two persisted type names denote the same type, versions, cultures and keys of generic arguments aside.</summary>
    public static bool Same(string? left, string? right)
        => string.Equals(left, right, StringComparison.Ordinal)
            || string.Equals(Normalize(left), Normalize(right), StringComparison.Ordinal);

    /// <summary>
    /// When a <c>Version=</c>, <c>Culture=</c> or <c>PublicKeyToken=</c> part starts at
    /// <paramref name="index"/> (after optional spaces), which one and the bounds of its value,
    /// which runs to the next <c>,</c> or <c>]</c>; otherwise <c>null</c>.
    /// </summary>
    private static (Part Part, int Start, int End)? PartValueAt(string typeName, int index)
    {
        while (index < typeName.Length && typeName[index] == ' ')
            index++;

        Part part;
        int start;
        if (StartsWithAt(typeName, index, VersionPart))
        {
            part = Part.Version;
            start = index + VersionPart.Length;
        }
        else if (StartsWithAt(typeName, index, CulturePart))
        {
            part = Part.Culture;
            start = index + CulturePart.Length;
        }
        else if (StartsWithAt(typeName, index, PublicKeyTokenPart))
        {
            part = Part.PublicKeyToken;
            start = index + PublicKeyTokenPart.Length;
        }
        else
        {
            return null;
        }

        var end = start;
        while (end < typeName.Length && typeName[end] is not (',' or ']'))
            end++;
        return (part, start, end);
    }

    private static bool StartsWithAt(string typeName, int index, string prefix)
        => string.CompareOrdinal(typeName, index, prefix, 0, prefix.Length) == 0;

    private static bool IsWellFormed(Part part, string typeName, int start, int end)
        => part switch
        {
            Part.Version => IsVersion(typeName, start, end),
            Part.Culture => IsCultureName(typeName, start, end),
            _ => IsPublicKeyToken(typeName, start, end)
        };

    /// <summary>Whether <c>typeName[start..end)</c> is a dotted-decimal assembly version.</summary>
    private static bool IsVersion(string typeName, int start, int end)
    {
        var components = 0;
        var digits = 0;
        for (var index = start; index < end; index++)
        {
            var unit = typeName[index];
            if (unit is >= '0' and <= '9')
            {
                if (++digits > MaxVersionComponentDigits)
                    return false;
            }
            else if (unit == '.' && digits > 0)
            {
                components++;
                digits = 0;
            }
            else
            {
                return false;
            }
        }

        if (digits == 0)
            return false;

        components++;
        return components is >= MinVersionComponents and <= MaxVersionComponents;
    }

    /// <summary>Whether <c>typeName[start..end)</c> is sixteen hex digits or <c>null</c>.</summary>
    private static bool IsPublicKeyToken(string typeName, int start, int end)
    {
        if (end - start == NoPublicKeyToken.Length && StartsWithAt(typeName, start, NoPublicKeyToken))
            return true;

        if (end - start != PublicKeyTokenHexDigits)
            return false;

        for (var index = start; index < end; index++)
        {
            if (!char.IsAsciiHexDigit(typeName[index]))
                return false;
        }

        return true;
    }

    /// <summary>
    /// Whether <c>typeName[start..end)</c> is <c>neutral</c> or a culture name: ASCII letters and
    /// digits in hyphen-separated subtags.
    /// </summary>
    private static bool IsCultureName(string typeName, int start, int end)
    {
        if (end - start is 0 or > MaxCultureNameLength
            || typeName[start] == '-'
            || typeName[end - 1] == '-')
        {
            return false;
        }

        for (var index = start; index < end; index++)
        {
            var unit = typeName[index];
            if (!char.IsAsciiLetterOrDigit(unit) && !(unit == '-' && typeName[index - 1] != '-'))
                return false;
        }

        return true;
    }
}
