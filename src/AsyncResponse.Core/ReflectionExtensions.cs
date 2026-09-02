using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Reflection;
using System.Text.Json;

namespace AsyncResponse;

/// <summary>
/// Conversion and invocation helpers for the reflection-based callback machinery:
/// materializing untyped JSON payloads as CLR types, resolving placeholder parameters, and
/// invoking <see cref="ReflectionInvocationDto"/>s against the DI container.
/// </summary>
/// <summary>
/// A callback could not be WIRED UP — it is unauthorized, its persisted service type no longer
/// resolves on this build, or that service is not registered in DI. Distinct from a failure thrown
/// by the callback BODY, which is ordinary application code and may well be transient.
/// <para>
/// Derives from <see cref="InvalidOperationException"/> so every existing catch and message
/// assertion keeps working; it exists purely so retry policies can tell "this call can never
/// succeed, no matter how many times it runs" from "the dependency behind this call blipped".
/// Without the distinction, a permanently mis-wired callback burned the full retry ladder on the
/// publish path for every lost response.
/// </para>
/// </summary>
internal sealed class CallbackTargetUnresolvableException(string message) : InvalidOperationException(message);

internal static class ReflectionExtensions
{
    private delegate ValueTask AsyncMethodInvoker(object service, object?[] args);

    // Entries carry the resolver-registry generation observed before the scan that produced them,
    // mirroring the negative cache's stamp: a plain clear-on-unregister has a race — an in-flight
    // resolution that got its answer from the departing resolver can insert AFTER the clear,
    // permanently re-poisoning the name with the revoked type. A stale stamp makes the entry a
    // non-hit, so the next lookup rescans against the current resolver set.
    private static readonly ConcurrentDictionary<string, (Type Type, int Generation)> ServiceTypes = new(StringComparer.Ordinal);
    private static int _resolvedServiceTypeGeneration;

    /// <summary>
    /// Invalidates the shared negative type-resolution cache (a new resolver or assembly may
    /// resolve cached misses). Kept as a named seam for <see cref="AsyncResponseTypeResolution"/>;
    /// the machinery lives in <see cref="UnresolvableTypeNames"/>, shared with the payload
    /// classifier's resolution path.
    /// </summary>
    internal static void InvalidateUnresolvableServiceTypes() => UnresolvableTypeNames.Invalidate();

    /// <summary>
    /// Drops resolved service types as well as the negative cache. Used when a resolver is
    /// UNREGISTERED: the negative cache holds names that failed, but the harm after a revoke is in
    /// the positive entries — a name the departing resolver already answered keeps resolving to its
    /// type, so a revoked alias still dispatches to the old service and its assembly stays
    /// reachable through the cache. Both directions of the registry change therefore clear both.
    /// </summary>
    internal static void InvalidateResolvedServiceTypes()
    {
        // Bump BEFORE clearing: the bump is what fences in-flight scans (their pre-scan stamp goes
        // stale); the clear just reclaims memory.
        Interlocked.Increment(ref _resolvedServiceTypeGeneration);
        ServiceTypes.Clear();
        UnresolvableTypeNames.Invalidate();
    }
    private static readonly ConcurrentDictionary<Type, ConversionPlan> ConversionPlans = new();
    private static readonly ConcurrentDictionary<InvocationPlanKey, InvocationPlan> InvocationPlans = new();
    private static readonly MethodInfo ToValueTaskMethod = typeof(ReflectionExtensions)
        .GetMethod(nameof(ToValueTask), BindingFlags.NonPublic | BindingFlags.Static)!;
    private static readonly MethodInfo AwaitGenericValueTaskMethod = typeof(ReflectionExtensions)
        .GetMethod(nameof(AwaitGenericValueTask), BindingFlags.NonPublic | BindingFlags.Static)!;

    /// <summary>
    /// If <paramref name="o"/> is a <see cref="JsonElement"/> (or a JSON string), deserializes it
    /// into <typeparamref name="T"/>; if it already is a <typeparamref name="T"/>, casts it;
    /// otherwise falls back to <see cref="Convert.ChangeType(object, Type)"/>.
    /// Throws if <paramref name="o"/> is null and <typeparamref name="T"/> is a non-nullable value type.
    /// </summary>
    public static T As<T>(this object? o) => (T)o.ConvertTo(typeof(T))!;

    /// <summary>
    /// Non-generic counterpart of <see cref="As{T}"/> for callers that only know the target type
    /// at runtime (e.g. classifying a payload against the type stored in the recovery state).
    /// </summary>
    public static object? ConvertTo(this object? o, Type targetType)
        => GetConversionPlan(targetType).Convert(o);

    /// <summary>
    /// Resolves the requested service from the provider and invokes the described method,
    /// converting each parameter to the method's parameter type via <see cref="ConvertTo"/>.
    /// </summary>
    public static Task InvokeAsync(this IServiceProvider provider, ReflectionInvocationDto dto)
    {
        try
        {
            // 1) Opt-in authorization — deliberately BEFORE any type resolution: the check is
            // string-based, so an unauthorized name is rejected without paying the assembly scan
            // (otherwise attacker-reachable work under the very threat model the authorizer
            // exists for). When an IAsyncResponseCallbackAuthorizer is registered, only allowed
            // (service, method) pairs may be invoked — defense-in-depth even if the recovery
            // store or worker transport is compromised. No authorizer registered ⇒ allow all. The
            // built-in flow executor gets no exemption: RecoverAsync carries an attacker-choosable
            // payload that is checkpointed into the flow ledger, so under the stated threat model it
            // must be gated like every other target. The AuthorizeCallbacks allowlist includes it by
            // default (see AsyncResponseCallbackAllowList.AllowDurableFlowExecutor); custom
            // authorizers must allow it explicitly when durable flows are enabled.
            //
            // Callers that restore propagated context around the invocation (the worker ingress,
            // the lost-subscriber dispatcher) authorize BEFORE they restore it and reach here
            // already cleared; this stays as the backstop for every other path.
            ThrowIfNotAuthorized(
                provider.GetService(typeof(IAsyncResponseCallbackAuthorizer)) as IAsyncResponseCallbackAuthorizer,
                dto.ServiceInterfaceFullName,
                dto.MethodName);

            // 2) Load the service type by full name
            var serviceType = ResolveServiceType(dto.ServiceInterfaceFullName);

            if (serviceType == null)
                throw new CallbackTargetUnresolvableException(
                    $"Type '{dto.ServiceInterfaceFullName}' not found in loaded assemblies.");

            // 3) Resolve the service instance
            var service = provider.GetService(serviceType)
                       ?? throw new CallbackTargetUnresolvableException(
                            $"Service '{dto.ServiceInterfaceFullName}' is not registered.");

            // 4) Resolve and cache method metadata + compiled invocation delegate. Collectible
            // (plugin) service types are planned per call: a strong Type-keyed cache entry would
            // pin the plugin's AssemblyLoadContext after unload.
            var planKey = new InvocationPlanKey(serviceType, dto.MethodName, dto.Params.Length);
            var plan = serviceType.Assembly.IsCollectible
                ? CreateInvocationPlan(planKey)
                : InvocationPlans.GetOrAdd(planKey, static key => CreateInvocationPlan(key));

            // 5) Convert only the arguments that need conversion, keeping already-typed arrays hot.
            var invocationArgs = plan.ConvertArguments(dto.Params);

            // 6) Invoke through the compiled delegate and await Task/ValueTask results.
            var pending = plan.Invoke(service, invocationArgs);
            return pending.IsCompletedSuccessfully ? Task.CompletedTask : AwaitSlow(pending);
        }
        catch (Exception ex)
        {
            // Match async-method exception behavior without paying for a state machine on the hot path.
            return Task.FromException(ex);
        }
    }

    /// <summary>
    /// Applies the registered callback authorizer to a persisted <c>(service, method)</c> pair.
    /// A <c>null</c> authorizer means none is registered, which allows everything — the opt-in
    /// default (see <c>AsyncResponseCallbackAuthorizationExtensions</c>).
    /// <para>
    /// Exposed separately from <see cref="InvokeAsync"/> so callers can run it at the point the
    /// descriptor is still just untrusted strings, before anything the descriptor's own message
    /// carries has been given effect. The worker envelope and the recovery row both ship a
    /// propagated-context carrier that gets pushed onto ambient state (principal, tenant, logging
    /// scope) for the dispatch; authorizing after that restore let a message choose the very
    /// context an authorizer would consult to judge it. The check is string-only, so running it
    /// first also costs nothing and keeps an unauthorized name away from the assembly scan.
    /// </para>
    /// </summary>
    internal static void ThrowIfNotAuthorized(
        IAsyncResponseCallbackAuthorizer? authorizer,
        string serviceInterfaceFullName,
        string methodName)
    {
        if (authorizer is not null && !authorizer.IsAllowed(serviceInterfaceFullName, methodName))
        {
            throw new CallbackTargetUnresolvableException(
                $"Callback target '{serviceInterfaceFullName}.{methodName}' is not authorized by the registered " +
                $"{nameof(IAsyncResponseCallbackAuthorizer)}; add it to the allowlist (AuthorizeCallbacks) to permit it.");
        }
    }

    private static async Task AwaitSlow(ValueTask pending)
        => await pending.ConfigureAwait(false);

    // Internal: the durable-flow executor resolves persisted flow/input type names through the
    // same default-ALC scan + custom-resolver chain as persisted callback targets.
    [UnconditionalSuppressMessage("Trimming", "IL2026",
        Justification = "Persisted callback/flow targets are registered through APIs that root them: the expression-based " +
                        "registration APIs annotate TService with DynamicallyAccessedMembers, WithDurableFlow<TFlow, TInput> roots " +
                        "flows, and the DTO-based registration APIs carry RequiresUnreferencedCode. A name that still cannot be " +
                        "resolved fails closed with an actionable error and a type-resolution-failure diagnostic instead of misbehaving.")]
    internal static Type? ResolveServiceType(string serviceInterfaceFullName)
    {
        // Must precede any cache consult/populate: a miss cached without the invalidation hook
        // active could outlive a later assembly load that makes the name resolvable.
        UnresolvableTypeNames.EnsureAssemblyLoadInvalidation();

        if (ServiceTypes.TryGetValue(serviceInterfaceFullName, out var cached)
            && cached.Generation == Volatile.Read(ref _resolvedServiceTypeGeneration))
        {
            return cached.Type;
        }

        // Fail fast on a name that already failed a full scan: without this, every delivery naming
        // an unresolvable type (a poisoned recovery row, a renamed class) re-walks every loaded
        // assembly on every attempt. Only a CURRENT-generation entry counts — a stale stamp means
        // the miss may have raced a resolver registration or assembly load, so it rescans.
        if (UnresolvableTypeNames.IsKnownMiss(serviceInterfaceFullName))
        {
            AsyncResponseDiagnostics.RecordTypeResolutionFailure("service");
            return null;
        }

        var generationBeforeScan = UnresolvableTypeNames.GenerationBeforeScan();
        var resolvedGenerationBeforeScan = Volatile.Read(ref _resolvedServiceTypeGeneration);

        // Loaded assemblies only — every component of the name, generic arguments included (see
        // ResolveLoaded): the wire-supplied name must never make the process load an assembly.
        var resolved = AsyncResponseTypeResolution.ResolveLoaded(serviceInterfaceFullName);
        if (resolved is not null)
        {
            CacheServiceType(serviceInterfaceFullName, resolved, resolvedGenerationBeforeScan);
            return resolved;
        }

        // Opt-in fallback for callback targets loaded into a non-default AssemblyLoadContext (plugins).
        var custom = AsyncResponseTypeResolution.Resolve(serviceInterfaceFullName);
        if (custom is not null)
        {
            CacheServiceType(serviceInterfaceFullName, custom, resolvedGenerationBeforeScan);
            return custom;
        }

        UnresolvableTypeNames.RecordMiss(serviceInterfaceFullName, generationBeforeScan);

        AsyncResponseDiagnostics.RecordTypeResolutionFailure("service");
        return null;
    }

    /// <summary>
    /// Caches a resolved service type — unless its assembly is collectible: a strong process-wide
    /// cache entry would pin the plugin's AssemblyLoadContext and keep an unloaded plugin's
    /// assemblies alive until process exit. Collectible-context types stay resolve-per-call (a
    /// cold path only plugin hosts hit); the negative cache is name-keyed and unaffected.
    /// </summary>
    private static void CacheServiceType(string serviceInterfaceFullName, Type resolved, int generationBeforeScan)
    {
        // Indexer, not TryAdd: a stale-stamped survivor of a raced unregister must be replaced by
        // the fresh scan's answer, not shadow it.
        if (!resolved.Assembly.IsCollectible)
            ServiceTypes[serviceInterfaceFullName] = (resolved, generationBeforeScan);
    }

    [UnconditionalSuppressMessage("Trimming", "IL2075",
        Justification = "The service type reaching this plan was rooted at registration: expression-based registration APIs " +
                        "annotate TService with DynamicallyAccessedMembers(PublicMethods), and DTO-based registration carries " +
                        "RequiresUnreferencedCode. A method removed regardless (e.g. a job enqueued by a different, non-trimmed " +
                        "deployment) fails closed with an actionable 'no method' error.")]
    private static InvocationPlan CreateInvocationPlan(InvocationPlanKey key)
    {
        // Pick the overload by name + parameter count once, then reuse the compiled plan.
        //
        // Interfaces do NOT inherit members in reflection: Type.GetMethods on an interface returns
        // only that interface's own declarations, and FlattenHierarchy does not change it. Searching
        // the service type alone therefore failed every callback and worker job whose method is
        // declared on a BASE interface — code that compiles cleanly, registers cleanly, and then
        // threw "no method" on every dispatch attempt forever. The base interfaces are searched too,
        // deduped by declaring type so a re-declaration does not read as an ambiguous overload.
        var candidates = CandidateMethods(key.ServiceType)
            .Where(m => m.Name == key.MethodName
                     && m.GetParameters().Length == key.ParameterCount)
            .DistinctBy(m => (m.DeclaringType, m.Name, string.Join(',', m.GetParameters().Select(p => p.ParameterType.FullName))))
            .ToArray();

        if (candidates.Length == 0)
            throw new CallbackTargetUnresolvableException(
                $"No method '{key.MethodName}' with {key.ParameterCount} parameter(s) on '{key.ServiceType.Name}'.");

        if (candidates.Length > 1)
            throw new CallbackTargetUnresolvableException(
                $"Method '{key.MethodName}' on '{key.ServiceType.Name}' has {candidates.Length} overloads with " +
                $"{key.ParameterCount} parameter(s); persisted callbacks cannot disambiguate overloads. " +
                "Give the callback target a unique name/arity.");

        var method = candidates[0];
        var parameters = method.GetParameters();
        var converters = new ConversionPlan[parameters.Length];
        for (var i = 0; i < parameters.Length; i++)
        {
            var parameterType = parameters[i].ParameterType;
            if (parameterType.IsByRef)
            {
                throw new CallbackTargetUnresolvableException(
                    $"Callback method '{method.Name}' on '{key.ServiceType.Name}' uses by-ref parameter '{parameters[i].Name}', which is not supported.");
            }

            converters[i] = GetConversionPlan(parameterType);
        }

        if (method.ContainsGenericParameters)
        {
            throw new CallbackTargetUnresolvableException(
                $"Callback method '{method.Name}' on '{key.ServiceType.Name}' has unbound generic parameters, which are not supported.");
        }

        return new InvocationPlan(converters, CreateInvoker(method, parameters));
    }

    /// <summary>
    /// The service type's own public instance methods plus, for an interface, those of every base
    /// interface — the members a caller can legally invoke through it, which is what a persisted
    /// callback names.
    /// </summary>
    /// <summary>
    /// Shared justification: the caller already carries the trimming contract — expression-based
    /// registration annotates TService with DynamicallyAccessedMembers(PublicMethods) and
    /// DTO-based registration carries RequiresUnreferencedCode. A base-interface method trimmed
    /// away fails closed with the same actionable "no method" error a missing method gives.
    /// Deliberately NOT an iterator: a suppression cannot reach a compiler-generated MoveNext.
    /// </summary>
    private const string TrimmingJustification =
        "The caller carries the trimming contract (DynamicallyAccessedMembers on TService, or RequiresUnreferencedCode " +
        "for DTO-based registration); a trimmed-away base-interface method fails closed with the same 'no method' error.";

    [UnconditionalSuppressMessage(
        "Trimming",
        "IL2070:UnrecognizedReflectionPattern",
        Justification = TrimmingJustification)]
    [UnconditionalSuppressMessage("Trimming", "IL2075:UnrecognizedReflectionPattern", Justification = TrimmingJustification)]
    private static List<MethodInfo> CandidateMethods(Type serviceType)
    {
        var candidates = new List<MethodInfo>();
        foreach (var method in serviceType.GetMethods(BindingFlags.Instance | BindingFlags.Public))
        {
            if (IsCallbackCandidate(method))
                candidates.Add(method);
        }

        if (!serviceType.IsInterface)
            return candidates;

        foreach (var baseInterface in serviceType.GetInterfaces())
        {
            foreach (var method in baseInterface.GetMethods(BindingFlags.Instance | BindingFlags.Public))
            {
                if (IsCallbackCandidate(method))
                    candidates.Add(method);
            }
        }

        return candidates;
    }

    /// <summary>
    /// Callback targets are ordinary methods. Property and event accessors and operators are
    /// <see cref="MethodBase.IsSpecialName"/>, and <see cref="object"/>'s members are never a
    /// callback — without this filter a type-level <c>Allow&lt;T&gt;()</c> on the callback
    /// allowlist also authorized every property SETTER (and <c>GetHashCode</c>/<c>ToString</c>),
    /// so a worker-transport writer could aim <c>set_ApiKey</c> at a DI singleton and change
    /// process-wide state. The plan accepts <c>void</c> returns, so nothing downstream caught it.
    /// </summary>
    private static bool IsCallbackCandidate(MethodInfo method)
        => !method.IsSpecialName && method.DeclaringType != typeof(object);

    private static AsyncMethodInvoker CreateInvoker(MethodInfo method, ParameterInfo[] parameters)
    {
        var service = Expression.Parameter(typeof(object), "service");
        var args = Expression.Parameter(typeof(object?[]), "args");
        var instance = Expression.Convert(service, method.DeclaringType!);
        var callArgs = new Expression[parameters.Length];

        for (var i = 0; i < parameters.Length; i++)
        {
            var arg = Expression.ArrayIndex(args, Expression.Constant(i));
            callArgs[i] = Expression.Convert(arg, parameters[i].ParameterType);
        }

        var call = Expression.Call(instance, method, callArgs);
        var body = ToValueTaskExpression(call, method.ReturnType);
        return Expression.Lambda<AsyncMethodInvoker>(body, service, args).Compile();
    }

    [UnconditionalSuppressMessage("Trimming", "IL2060",
        Justification = "AwaitGenericValueTask<T> is instantiated over the callback method's ValueTask<T> result type. The " +
                        "callback method itself was rooted at registration, and for reference-type results shared generic code " +
                        "always exists. Value-type results over a method never instantiated statically fail closed at dispatch " +
                        "with a clear exception rather than silently misroute.")]
    [UnconditionalSuppressMessage("AOT", "IL3050",
        Justification = "Same contract: reference-type ValueTask<T> results use shared generic code under Native AOT; the " +
                        "exotic value-type case throws an actionable error at dispatch.")]
    private static Expression ToValueTaskExpression(MethodCallExpression call, Type returnType)
    {
        if (returnType == typeof(void))
            return Expression.Block(call, Expression.Default(typeof(ValueTask)));

        if (typeof(Task).IsAssignableFrom(returnType))
            return Expression.Call(ToValueTaskMethod, Expression.Convert(call, typeof(Task)));

        if (returnType == typeof(ValueTask))
            return call;

        if (returnType.IsGenericType && returnType.GetGenericTypeDefinition() == typeof(ValueTask<>))
            return Expression.Call(AwaitGenericValueTaskMethod.MakeGenericMethod(returnType.GetGenericArguments()[0]), call);

        return Expression.Block(call, Expression.Default(typeof(ValueTask)));
    }

    private static ValueTask ToValueTask(Task? task)
        => task is null ? default : new ValueTask(task);

    private static async ValueTask AwaitGenericValueTask<T>(ValueTask<T> task)
        => await task.ConfigureAwait(false);

    private static ConversionPlan GetConversionPlan(Type targetType)
    {
        // Collectible (plugin) target types are planned per call: a strong Type-keyed cache entry
        // would pin the plugin's AssemblyLoadContext after unload. Cold path — only plugin hosts
        // resolve conversions for collectible types, and only on recovery/callback traffic.
        ArgumentNullException.ThrowIfNull(targetType);

        if (targetType.Assembly.IsCollectible)
            return new ConversionPlan(targetType);

        return ConversionPlans.GetOrAdd(targetType, static type => new ConversionPlan(type));
    }

    /// <summary>
    /// Given a callback template whose <c>Params</c> are <see cref="CallbackParam"/>s, produces a
    /// <see cref="ReflectionInvocationDto"/> whose <c>Params</c> are the real objects
    /// (payload, exception, correlation id, or literal values).
    /// </summary>
    public static ReflectionInvocationDto ResolveCallback(
        ReflectionCallDto template,
        object? payload,
        Exception? exception,
        string? correlationId)
    {
        var args = template.Params
            .Select(p => p.Placeholder switch
            {
                PlaceholderType.Payload => payload,
                PlaceholderType.Exception => exception,
                PlaceholderType.CorrelationId => correlationId,
                _ => p.Value
            })
            .ToArray();

        return new ReflectionInvocationDto
        {
            ServiceInterfaceFullName = template.ServiceInterfaceFullName,
            MethodName = template.MethodName,
            Params = args
        };
    }

    private readonly record struct InvocationPlanKey(Type ServiceType, string MethodName, int ParameterCount);

    private sealed class InvocationPlan(ConversionPlan[] converters, AsyncMethodInvoker invoker)
    {
        /// <summary>Runs the ConvertArguments operation.</summary>
        public object?[] ConvertArguments(object?[] args)
        {
            object?[]? converted = null;

            for (var i = 0; i < converters.Length; i++)
            {
                var raw = args[i];
                var value = converters[i].Convert(raw);
                if (!ReferenceEquals(value, raw))
                {
                    converted ??= CopyPrefix(args, i);
                    converted[i] = value;
                }
                else if (converted is not null)
                {
                    converted[i] = raw;
                }
            }

            return converted ?? args;
        }

        /// <summary>Invokes the reflected operation.</summary>
        public ValueTask Invoke(object service, object?[] args)
            => invoker(service, args);

        private static object?[] CopyPrefix(object?[] args, int length)
        {
            var copy = new object?[args.Length];
            Array.Copy(args, copy, length);
            return copy;
        }
    }

    private sealed class ConversionPlan(Type targetType)
    {
        private readonly Type? _underlyingType = Nullable.GetUnderlyingType(targetType);
        private readonly Type _conversionType = Nullable.GetUnderlyingType(targetType) ?? targetType;
        private readonly bool _isNonNullableValueType = targetType.IsValueType && Nullable.GetUnderlyingType(targetType) is null;
        private readonly bool _isString = targetType == typeof(string);

        /// <summary>Converts the supplied value.</summary>
        public object? Convert(object? value)
        {
            // Handle JSON payloads (contract metadata resolved through the AOT-safe chain; loose
            // case-insensitive matching as before).
            if (value is JsonElement je)
            {
                return JsonSerializer.Deserialize(je, AsyncResponseJson.GetTypeInfo(targetType, AsyncResponseJson.CaseInsensitive));
            }

            // Already the correct CLR type (a boxed value also satisfies its nullable counterpart).
            // This must precede the JSON-in-a-string fallback: values that crossed a serialization
            // boundary arrive as JsonElement (see AsyncResponseJsonContext), so a raw string here
            // is a LIVE in-memory value (a literal captured by the callback expression, or the
            // CorrelationId placeholder) — JSON-parsing it into an object-typed parameter would
            // throw on any plain string like "ORD-42".
            if (targetType.IsInstanceOfType(value) || (_underlyingType?.IsInstanceOfType(value) ?? false))
            {
                return value;
            }

            // JSON in a string (a target the string cannot satisfy directly)
            if (value is string s && !_isString)
            {
                return JsonSerializer.Deserialize(s, AsyncResponseJson.GetTypeInfo(targetType, AsyncResponseJson.CaseInsensitive));
            }

            // Null handling
            if (value is null)
            {
                // The target being a non-nullable value type cannot represent null.
                if (_isNonNullableValueType)
                {
                    throw new InvalidCastException($"Cannot convert null to non-nullable type {targetType}.");
                }

                return null;
            }

            // Fallback for primitives
            return System.Convert.ChangeType(value, _conversionType);
        }
    }
}
