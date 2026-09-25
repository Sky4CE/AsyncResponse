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
/// refused. <see cref="Normalize"/> drops the <c>Version</c> part of the assembly names inside
/// generic-argument brackets, at any depth. Persisted names are never rewritten — only compared
/// through this — so the ledger format and older replicas are unaffected.
/// </para>
/// <para>
/// Only the version, and only a well-formed one. <c>Culture</c> and <c>PublicKeyToken</c> stay:
/// a version bump changes neither (CoreLib's key never changes), and the callback allowlist
/// matches normalized names, so dropping the key made an allowlisted
/// <c>IHandler&lt;Contoso.Order&gt;</c> also admit a descriptor naming an argument from a
/// same-named assembly signed with another key. A <c>Version=</c> value that is not dotted
/// decimal leaves the whole name as it is, compared ordinally: store data that is not what a
/// runtime writes is not rewritten into something that matches.
/// </para>
/// </summary>
internal static class TypeNameIdentity
{
    private const string VersionPart = "Version=";

    // System.Version's grammar as an assembly name prints it: major.minor[.build[.revision]], each
    // component a 16-bit value.
    private const int MinVersionComponents = 2;
    private const int MaxVersionComponents = 4;
    private const int MaxVersionComponentDigits = 5;

    /// <summary>
    /// <paramref name="typeName"/> without the <c>Version</c> parts of its generic arguments'
    /// assembly names. A name outside the persisted type-name limits is store data no resolver
    /// would accept, and is returned unchanged; so is one that carries no version part, and one
    /// whose version value is malformed.
    /// </summary>
    public static string? Normalize(string? typeName)
    {
        if (typeName is null
            || typeName.IndexOf('[') < 0
            || !AsyncResponseTypeResolution.IsWithinResolutionLimits(typeName)
            || !typeName.Contains(VersionPart, StringComparison.Ordinal))
        {
            return typeName;
        }

        // One forward pass (the limits above bound the length): copy everything except a
        // ", Version=…" segment inside brackets, which runs to the next ',' or ']'.
        var builder = new StringBuilder(typeName.Length);
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
            else if (unit == ',' && depth > 0 && VersionValueAt(typeName, index + 1) is var (start, end))
            {
                if (!IsVersion(typeName, start, end))
                    return typeName;

                index = end;
                continue;
            }

            builder.Append(unit);
            index++;
        }

        return builder.ToString();
    }

    /// <summary>Whether two persisted type names denote the same type, versions of generic arguments aside.</summary>
    public static bool Same(string? left, string? right)
        => string.Equals(left, right, StringComparison.Ordinal)
            || string.Equals(Normalize(left), Normalize(right), StringComparison.Ordinal);

    /// <summary>
    /// When a <c>Version=</c> part starts at <paramref name="index"/> (after optional spaces), the
    /// bounds of its value, which runs to the next <c>,</c> or <c>]</c>; otherwise <c>null</c>.
    /// </summary>
    private static (int Start, int End)? VersionValueAt(string typeName, int index)
    {
        while (index < typeName.Length && typeName[index] == ' ')
            index++;

        if (string.CompareOrdinal(typeName, index, VersionPart, 0, VersionPart.Length) != 0)
            return null;

        var start = index + VersionPart.Length;
        var end = start;
        while (end < typeName.Length && typeName[end] is not (',' or ']'))
            end++;
        return (start, end);
    }

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
}
