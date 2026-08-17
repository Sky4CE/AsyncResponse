namespace AsyncResponse.Internal;

/// <summary>
/// Pairwise distinctness for a store's effective object-name plan: the configured names plus every
/// index and sequence name derived from them. Derived-name helpers reserve suffix space by
/// truncating the stem, so two long configured names can still derive one name — and a configured
/// name can occupy a derived one outright — after which <c>CREATE ... IF NOT EXISTS</c> (or SQL
/// Server's <c>IF OBJECT_ID(...) IS NULL</c>) silently skips the object instead of failing.
/// Source-linked into the packages that own such a plan (separate packages cannot share compiled
/// code); the caller supplies the guidance tail so each options type keeps naming its own objects.
/// </summary>
internal static class RelationalNamePlan
{
    /// <summary>
    /// Throws when two entries resolve to the same name. Comparison is case-insensitive even where
    /// the DDL quotes identifiers (case-sensitive to PostgreSQL): a plan distinct only by letter
    /// case is a misconfiguration magnet, and SQL Server's catalogs fold case by default anyway.
    /// </summary>
    public static void RequireDistinct((string Role, string Name)[] plan, string optionsName, string guidance)
    {
        for (var i = 0; i < plan.Length; i++)
        {
            for (var j = i + 1; j < plan.Length; j++)
            {
                if (string.Equals(plan[i].Name, plan[j].Name, StringComparison.OrdinalIgnoreCase))
                {
                    // BOTH spellings, not just one: the comparison folds case, so a collision can
                    // exist between names the operator's configuration shows as different
                    // ("Messages" vs "messages"). Reporting a single spelling made that error read
                    // as a false positive, with nothing pointing at the case-fold rule above.
                    throw new InvalidOperationException(
                        $"{optionsName}: the {plan[i].Role} ('{plan[i].Name}') and the {plan[j].Role} ('{plan[j].Name}') resolve to " +
                        $"the same name — object names are compared case-insensitively" + guidance);
                }
            }
        }
    }

    /// <summary>
    /// A derived object name with the suffix space RESERVED before the provider's identifier cap:
    /// the stem is truncated, never the composed name. Truncating "{stem}{suffix}" as a whole lets
    /// a maximum-length table derive its OWN name, after which <c>CREATE ... IF NOT EXISTS</c> (or
    /// SQL Server's <c>IF OBJECT_ID(...) IS NULL</c> / <c>sys.indexes</c> guard) matches the
    /// existing object and silently creates nothing — and two derived names can collapse onto one
    /// another the same way.
    /// <para>
    /// The single implementation of a rule that was previously copy-pasted across the channel and
    /// transport stores, each with its own cap constant. Callers pass their provider's cap.
    /// </para>
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="suffix"/> leaves no room for a stem inside <paramref name="identifierCap"/>.
    /// Guarded explicitly: the slice below would otherwise take a negative length and fail schema
    /// creation with a bare index-out-of-range that names neither the suffix nor the cap.
    /// </exception>
    public static string DerivedName(string stem, string suffix, int identifierCap)
    {
        if (identifierCap <= 0 || stem.Length + suffix.Length <= identifierCap)
            return stem + suffix;

        if (suffix.Length >= identifierCap)
        {
            throw new ArgumentOutOfRangeException(
                nameof(suffix),
                $"The derived-name suffix '{suffix}' is {suffix.Length} characters, which leaves no room for a stem inside the " +
                $"{identifierCap}-character identifier limit. Shorten the suffix.");
        }

        return stem[..(identifierCap - suffix.Length)] + suffix;
    }
}
