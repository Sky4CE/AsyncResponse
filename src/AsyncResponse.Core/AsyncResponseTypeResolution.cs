using System.Diagnostics.CodeAnalysis;
using System.Reflection;

namespace AsyncResponse;

/// <summary>
/// Opt-in extensibility for resolving the service and payload types named in persisted callbacks and
/// recovery state. By default AsyncResponse resolves a type name against the assemblies loaded into
/// the default <see cref="System.Runtime.Loader.AssemblyLoadContext"/>
/// (<c>AppDomain.CurrentDomain.GetAssemblies()</c>). Apps that load callback targets or payload types
/// into a <em>separate</em> <c>AssemblyLoadContext</c> — plugin hosts, dynamic-load scenarios — can
/// register an extra resolver (or assembly) here so those types resolve too, instead of the recovery
/// callback silently failing because the type was invisible to the default context.
/// <para>
/// This is process-wide and additive: registered resolvers are consulted only when the default scan
/// does not find the type. Registering nothing preserves the default behavior exactly.
/// </para>
/// </summary>
public static class AsyncResponseTypeResolution
{
    private static volatile Func<string, Type?>[] _resolvers = [];
    private static readonly object _gate = new();

    /// <summary>
    /// Registers a custom resolver consulted (after the default assembly scan) when resolving a
    /// persisted type name. The resolver returns the resolved <see cref="Type"/> or <c>null</c>.
    /// </summary>
    public static void RegisterResolver(Func<string, Type?> resolver)
    {
        ArgumentNullException.ThrowIfNull(resolver);
        lock (_gate)
        {
            _resolvers = [.. _resolvers, resolver];
        }
    }

    /// <summary>
    /// Registers an assembly (typically one loaded into a non-default <c>AssemblyLoadContext</c>) to
    /// be searched for persisted type names.
    /// </summary>
    [RequiresUnreferencedCode("Resolves persisted type names against the assembly by string; a trimmed app may have removed " +
                              "those types. Plugin/dynamic-load scenarios are inherently incompatible with trimming the plugin's types.")]
    public static void RegisterAssembly(Assembly assembly)
    {
        ArgumentNullException.ThrowIfNull(assembly);
        RegisterResolver(name => assembly.GetType(name, throwOnError: false));
    }

    /// <summary>Consults the registered resolvers in order; returns the first non-null match, or <c>null</c>.</summary>
    internal static Type? Resolve(string fullName)
    {
        foreach (var resolver in _resolvers)
        {
            try
            {
                if (resolver(fullName) is { } type)
                    return type;
            }
            catch
            {
                // A misbehaving custom resolver must never break recovery resolution; skip to the next.
            }
        }

        return null;
    }

    /// <summary>Clears all registered resolvers. Test seam only.</summary>
    internal static void Reset()
    {
        lock (_gate)
        {
            _resolvers = [];
        }
    }
}
