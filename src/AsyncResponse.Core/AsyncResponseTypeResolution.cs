using System.Diagnostics.CodeAnalysis;
using System.Reflection;

namespace AsyncResponse;

/// <summary>
/// Opt-in extensibility for resolving the service and payload types named in persisted callbacks and
/// recovery state. By default AsyncResponse resolves a type name against every assembly already
/// loaded into the process (<c>AppDomain.CurrentDomain.GetAssemblies()</c>), in ANY
/// <see cref="System.Runtime.Loader.AssemblyLoadContext"/> — a plugin's types are found once its
/// context has loaded them. A name defined in several loaded assemblies (the same plugin loaded
/// into two contexts) resolves to the first match in load order. Apps whose callback targets or
/// payload types are not loaded yet when a persisted name arrives — plugin hosts, dynamic-load
/// scenarios — can register an extra resolver (or assembly) here so those types resolve too,
/// instead of the recovery callback silently failing to route.
/// <para>
/// This is process-wide and additive: registered resolvers are consulted only when the default scan
/// does not find the type, so a resolver cannot choose between copies the scan already sees.
/// Registering nothing preserves the default behavior exactly.
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
    /// <para>
    /// Unlike the default scan, this is not confined to assemblies already loaded: a name is
    /// resolved with the runtime's own parser (<see cref="Assembly.GetType(string, bool)"/>), so
    /// an assembly named in a generic argument that is not loaded yet is loaded through
    /// <paramref name="assembly"/>'s <c>AssemblyLoadContext</c> — from its dependencies or the
    /// application's trusted platform assemblies — on the way to a verdict, whoever wrote the
    /// name. Only files the application or plugin deploys can load that way, and the resolved type
    /// still meets the caller's own gate (payload marker, DI registration, callback authorizer);
    /// when even that load is unwanted, register a <see cref="RegisterResolver"/> delegate that
    /// answers only the names you expect.
    /// </para>
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
    /// Longest persisted type name that is resolved, in UTF-16 code units. The names this library
    /// writes are <see cref="Type.FullName"/> values, which spell every generic argument
    /// assembly-qualified — roughly 150–250 units per component (namespace-qualified name plus
    /// <c>, Assembly, Version=…, Culture=…, PublicKeyToken=…</c>). 4096 leaves room for about
    /// twenty such components (an eight-element <c>ValueTuple</c> of generic payloads fits), the
    /// node budget <c>System.Reflection.Metadata.TypeNameParseOptions.MaxNodes</c> defaults to,
    /// while keeping a hostile name three orders of magnitude under the inbound message budget —
    /// and keeping the name-keyed resolution caches from holding megabyte keys.
    /// </summary>
    internal const int MaxTypeNameLength = 4096;

    /// <summary>
    /// Deepest <c>[</c> nesting that is resolved: eight levels of generic nesting in the
    /// assembly-qualified form (<c>Outer`1[[Inner`1[[…]], asm]]</c> costs two brackets a level).
    /// </summary>
    internal const int MaxTypeNameNesting = 16;

    /// <summary>
    /// Most <c>[</c> a resolved name may hold in total — generic argument lists, the
    /// assembly-qualified wrapper around each argument, and array ranks together. This is the
    /// bound on a CHAIN of decorations (<c>T[][][]…</c>), which nests no deeper than one bracket
    /// however long it grows.
    /// </summary>
    internal const int MaxTypeNameBrackets = 64;

    /// <summary>
    /// Whether <paramref name="fullName"/> may be handed to the CLR type-name parser. Every
    /// resolution path — the default scan, the registered resolvers, and both name-keyed caches in
    /// front of them — asks this FIRST.
    /// <para>
    /// <c>Type.GetType</c> and <c>Assembly.GetType</c> parse generic arguments by recursion and
    /// build array/pointer/by-ref decorations as a chain that is then resolved by recursion, with
    /// no depth limit of their own inside the runtime. A persisted name is written by whoever can
    /// write the recovery store or the worker stream, and a few hundred kilobytes of
    /// <c>A`1[[A`1[[…</c> — far inside the inbound message budget — overflows the stack of the
    /// thread that parses it. A <see cref="StackOverflowException"/> cannot be caught: the process
    /// exits, the message is still unacknowledged, and every worker the broker redelivers it to
    /// exits the same way. The payload type name is resolved before any callback is chosen, so no
    /// callback authorizer stands in front of it. The scan below is a single iterative pass, so
    /// the guard cannot itself be driven deep.
    /// </para>
    /// <para>
    /// Deliberately conservative rather than a second parser: a backslash-escaped bracket is
    /// counted as a bracket, and <c>&amp;</c> / <c>*</c> are refused wherever they appear. No
    /// callback service, payload, flow, or flow-input type is a pointer, a by-ref, or an
    /// unknown-bound array, and a name refused here is simply unresolvable to the caller — the
    /// outcome it already has for a renamed type.
    /// </para>
    /// </summary>
    internal static bool IsWithinResolutionLimits(string fullName)
    {
        if (fullName.Length > MaxTypeNameLength)
            return false;

        var depth = 0;
        var brackets = 0;
        foreach (var unit in fullName)
        {
            switch (unit)
            {
                case '[':
                    if (++depth > MaxTypeNameNesting || ++brackets > MaxTypeNameBrackets)
                        return false;
                    break;
                case ']':
                    if (depth > 0)
                        depth--;
                    break;
                case '&' or '*':
                    return false;
            }
        }

        return true;
    }

    /// <summary>
    /// A persisted type name as it may appear in a log line or an exception message. Within the
    /// resolution limits it is the whole name — so an ordinary name reads exactly as before — with
    /// control characters and unpaired surrogates escaped; past them it is a short escaped excerpt
    /// plus the real length. Never megabytes of store-written text, and never its raw line breaks,
    /// copied into an Error log on every delivery.
    /// </summary>
    internal static string DescribeForDiagnostics(string? fullName)
    {
        if (fullName is null)
            return string.Empty;

        return IsWithinResolutionLimits(fullName)
            ? DiagnosticText.EscapedExcerpt(fullName, MaxTypeNameLength)
            : $"{DiagnosticText.EscapedExcerpt(fullName, 80)} ({fullName.Length} UTF-16 code units; outside the persisted type-name limits)";
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
    /// <para>
    /// "Loaded" means the snapshot taken here, and every resolved component is checked against
    /// it. <c>Assembly.GetType</c> follows type forwarders, and the facades nearly every process
    /// has loaded (<c>netstandard</c>, <c>mscorlib</c>, <c>System.Runtime</c>) forward to most of
    /// the framework: asking <c>netstandard</c> for <c>System.Net.Mail.SmtpClient</c> makes the
    /// runtime load <c>System.Net.Mail</c> and answer with a type from it. That load cannot be
    /// prevented from here — it is limited to the framework's own assemblies, never a file the
    /// name's author supplies — but its result is refused: an assembly-qualified component must
    /// come from a snapshotted assembly, and an unqualified one only ever from the candidate that
    /// defines it (never through another candidate's forwarder). The refusal holds for the
    /// resolution that caused the load, not beyond it: the loaded assembly is part of the process
    /// from then on, so the next snapshot contains it and a later resolution of the same name (a
    /// redelivery) answers with its type. Such a type still has to pass the caller's own gate —
    /// the payload marker interface, a DI registration, the flow contract — before anything uses it.
    /// </para>
    /// <para>
    /// <c>null</c> for every name that does not resolve — also one the runtime refuses to BUILD:
    /// <c>throwOnError: false</c> covers lookups, not the instantiation of a resolved generic
    /// definition, so a constraint or arity mismatch
    /// (<c>Nullable`1[[System.String, …]]</c>), a non-generic definition given arguments, or a
    /// malformed assembly name inside the brackets (<c>Version=x</c>) threw out of here instead. The
    /// callers then skipped recording the miss, and the lost-subscriber dispatcher read the
    /// escaped exception as a transient callback fault — the full retry ladder and a transport
    /// redelivery for a name that can never resolve.
    /// </para>
    /// </summary>
    [RequiresUnreferencedCode("Resolves a persisted type name by string; a trimmed app may have removed the type.")]
    internal static Type? ResolveLoaded(string fullName)
    {
        // Backstop: the caching resolvers in front of this refuse such a name before they touch
        // their caches, but nothing may reach the recursive parser below without the check.
        if (!IsWithinResolutionLimits(fullName))
            return null;

        var loaded = AppDomain.CurrentDomain.GetAssemblies();
        try
        {
            return Type.GetType(
                fullName,
                assemblyResolver: name => Array.Find(loaded, assembly => AssemblyName.ReferenceMatchesDefinition(name, assembly.GetName())),
                typeResolver: (assembly, typeName, ignoreCase) =>
                {
                    if (assembly is not null)
                    {
                        return assembly.GetType(typeName, throwOnError: false, ignoreCase) is { } named && IsDefinedIn(loaded, named)
                            ? named
                            : null;
                    }

                    foreach (var candidate in loaded)
                    {
                        // Only a candidate's OWN type: a forwarded hit is skipped, not final. The
                        // defining assembly, when it is loaded, is itself a candidate and answers for
                        // itself — so requiring the definer loses nothing, and a facade can never
                        // answer ahead of it in load order.
                        if (candidate.GetType(typeName, throwOnError: false, ignoreCase) is { } type && type.Assembly == candidate)
                            return type;
                    }

                    return null;
                },
                throwOnError: false);
        }
        catch (Exception ex) when (ex is ArgumentException
                                       or InvalidOperationException
                                       or FileLoadException
                                       or BadImageFormatException
                                       or TypeLoadException)
        {
            // Unresolvable, like any other name this cannot build (see the summary); the caller
            // records the miss and reports it through its own unresolved-type path.
            return null;
        }
    }

    /// <summary>Whether <paramref name="type"/> comes from one of the snapshotted assemblies rather than from one a type forwarder just loaded.</summary>
    private static bool IsDefinedIn(Assembly[] loaded, Type type)
        => Array.IndexOf(loaded, type.Assembly) >= 0;

    /// <summary>Consults the registered resolvers in order; returns the first non-null match, or <c>null</c>.</summary>
    internal static Type? Resolve(string fullName)
    {
        // RegisterAssembly's resolver is Assembly.GetType — the same recursive parser as the
        // default scan — and an application resolver is as likely to call Type.GetType itself.
        if (!IsWithinResolutionLimits(fullName))
            return null;

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
                // explain why. Counted per THROW, under its own kind — a later resolver may still
                // answer, and a name that stays unresolved is counted again by its caller under
                // "service" or "payload".
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
