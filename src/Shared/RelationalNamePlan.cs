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
                    throw new InvalidOperationException(
                        $"{optionsName}: the {plan[i].Role} and the {plan[j].Role} both resolve to '{plan[j].Name}'" + guidance);
                }
            }
        }
    }
}
