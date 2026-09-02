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
/// <para>
/// <b>Unloadable (collectible) contexts:</b> the library's resolution caches skip types from
/// collectible assemblies (resolving them per call), so AsyncResponse never pins a collectible
/// <c>AssemblyLoadContext</c> on its own. A registration made HERE is the exception, and it is
/// yours to manage: the resolver delegate lives in a process-wide list and, for
/// <see cref="RegisterAssembly"/>, strongly holds the assembly — which keeps its context alive for
/// the life of the process. Both registration methods therefore return an
/// <see cref="IDisposable"/>; dispose it before unloading the plugin, or the context never
/// collects. <c>System.Text.Json</c> pins any collectible type it serializes through
/// runtime-internal caches regardless — so for unloadable plugins, keep payload types and callback
/// service interfaces in a shared non-collectible contracts assembly and load only implementations
/// into the collectible context (see <c>docs/security.md</c>).
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
    /// <returns>
    /// A handle that removes this registration when disposed. Disposal is idempotent and safe from
    /// any thread. Ignoring it keeps the resolver — and everything its closure captures — for the
    /// life of the process; a host loading plugins into a collectible <c>AssemblyLoadContext</c>
    /// must dispose it before unloading, or the context is pinned.
    /// </returns>
    public static IDisposable RegisterResolver(Func<string, Type?> resolver)
    {
        ArgumentNullException.ThrowIfNull(resolver);
        lock (_gate)
        {
            _resolvers = [.. _resolvers, resolver];
        }

        // A new resolver can turn previously-unresolvable names into hits; drop the negative cache.
        ReflectionExtensions.InvalidateUnresolvableServiceTypes();
        return new ResolverRegistration(resolver);
    }

    /// <summary>
    /// Registers an assembly (typically one loaded into a non-default <c>AssemblyLoadContext</c>) to
    /// be searched for persisted type names.
    /// </summary>
    /// <returns>
    /// A handle that removes the registration when disposed. <b>Required</b> for a collectible
    /// assembly: the registration holds a strong reference to it, so until this is disposed the
    /// assembly's <c>AssemblyLoadContext</c> cannot unload.
    /// </returns>
    [RequiresUnreferencedCode("Resolves persisted type names against the assembly by string; a trimmed app may have removed " +
                              "those types. Plugin/dynamic-load scenarios are inherently incompatible with trimming the plugin's types.")]
    public static IDisposable RegisterAssembly(Assembly assembly)
    {
        ArgumentNullException.ThrowIfNull(assembly);
        return RegisterResolver(name => assembly.GetType(name, throwOnError: false));
    }

    /// <summary>Removes one registered resolver; a no-op when it is already gone.</summary>
    private static void Unregister(Func<string, Type?> resolver)
    {
        lock (_gate)
        {
            var current = _resolvers;
            var index = Array.IndexOf(current, resolver);
            if (index < 0)
                return;

            var remaining = new Func<string, Type?>[current.Length - 1];
            Array.Copy(current, remaining, index);
            Array.Copy(current, index + 1, remaining, index, current.Length - index - 1);
            _resolvers = remaining;
        }

        // Names this resolver was answering must stop resolving to its types — including the ones
        // already ANSWERED. The positive caches key a resolved Type by name, so leaving them
        // populated meant a revoked alias kept serving the old type (and kept its assembly alive)
        // for the life of the process, which is most of what disposing the handle is for.
        ReflectionExtensions.InvalidateResolvedServiceTypes();
        PayloadRecoveryClassifier.InvalidateResolvedPayloadTypes();
    }

    private sealed class ResolverRegistration : IDisposable
    {
        // Cleared on dispose, not just unregistered: a caller that keeps the handle around (a
        // field on a plugin host, a using-scoped variable still in scope) would otherwise keep the
        // delegate — and everything its closure captured, including the plugin assembly — reachable
        // through the handle itself, which is exactly the pinning the handle exists to end.
        private Func<string, Type?>? _resolver;

        public ResolverRegistration(Func<string, Type?> resolver) => _resolver = resolver;

        /// <summary>Removes the registration and drops this handle's reference. Idempotent.</summary>
        public void Dispose()
        {
            if (Interlocked.Exchange(ref _resolver, null) is { } resolver)
                Unregister(resolver);
        }
    }

    /// <summary>
    /// The default scan: resolves a persisted type name against the assemblies ALREADY loaded into
    /// the process, and only those. The name is parsed with the full CLR type-name grammar, so a
    /// generic instantiation whose argument is assembly-qualified — chosen by whoever can write
    /// the recovery store or the worker stream — made <c>Assembly.GetType</c> LOAD that assembly
    /// (and everything it references) on the way to a verdict, and then ran the argument type's
    /// constructor and setters through deserialization before the marker-interface gate ever
    /// looked at it. Supplying the resolvers confines every component of the name to what is
    /// loaded, which is the contract this class documents; the registered resolvers above are
    /// consulted only when this returns <c>null</c>.
    /// </summary>
    [RequiresUnreferencedCode("Resolves a persisted type name by string; a trimmed app may have removed the type.")]
    internal static Type? ResolveLoaded(string fullName)
    {
        var loaded = AppDomain.CurrentDomain.GetAssemblies();
        return Type.GetType(
            fullName,
            assemblyResolver: name => Array.Find(loaded, assembly => AssemblyName.ReferenceMatchesDefinition(name, assembly.GetName())),
            typeResolver: (assembly, typeName, ignoreCase) =>
            {
                if (assembly is not null)
                    return assembly.GetType(typeName, throwOnError: false, ignoreCase);

                foreach (var candidate in loaded)
                {
                    if (candidate.GetType(typeName, throwOnError: false, ignoreCase) is { } type)
                        return type;
                }

                return null;
            },
            throwOnError: false);
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
                // A misbehaving custom resolver must never break recovery resolution; skip to the
                // next. Counted, though: swallowed without a trace, a resolver that throws on every
                // call looked identical to one that simply had no answer, and the recovery
                // callbacks it should have resolved just kept failing to route with nothing to
                // explain why.
                AsyncResponseDiagnostics.RecordTypeResolutionFailure("resolver");
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

        ReflectionExtensions.InvalidateUnresolvableServiceTypes();

        // Same as Unregister: names the removed resolvers already ANSWERED must stop resolving,
        // so the positive caches are dropped too — otherwise a reset leaks resolved types (and
        // their assemblies) into whatever runs next in the process.
        ReflectionExtensions.InvalidateResolvedServiceTypes();
        PayloadRecoveryClassifier.InvalidateResolvedPayloadTypes();
    }
}
