using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Reflection;
using System.Text.Json;

namespace AsyncResponse;

/// <summary>
/// Conversion and invocation helpers for the reflection-based callback machinery:
/// materializing untyped JSON payloads as CLR types, resolving placeholder parameters, and
/// invoking <see cref="ReflectionInvocationDto"/>s against the DI container.
/// </summary>
internal static class ReflectionExtensions
{
    private delegate ValueTask AsyncMethodInvoker(object service, object?[] args);

    // Loose options for any JSON deserialization here.
    private static readonly JsonSerializerOptions _looseJsonOptions = new()
    {
        PropertyNameCaseInsensitive = true
    };

    private static readonly ConcurrentDictionary<string, Type> ServiceTypes = new(StringComparer.Ordinal);
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
            // 1) Load the service type by full name
            var serviceType = ResolveServiceType(dto.ServiceInterfaceFullName);

            if (serviceType == null)
                throw new InvalidOperationException(
                    $"Type '{dto.ServiceInterfaceFullName}' not found in loaded assemblies.");

            // 1b) Opt-in authorization: when an IAsyncResponseCallbackAuthorizer is registered, only
            // allowed (service, method) pairs may be invoked — defense-in-depth even if the recovery
            // store or worker transport is compromised. No authorizer registered ⇒ allow all. The
            // built-in flow executor is implicitly allowed: durable flows register it as their
            // resume/failure target, and its methods accept only a flow id (and an exception).
            if (dto.ServiceInterfaceFullName != typeof(IDurableFlowExecutor).FullName
                && provider.GetService(typeof(IAsyncResponseCallbackAuthorizer)) is IAsyncResponseCallbackAuthorizer authorizer
                && !authorizer.IsAllowed(dto.ServiceInterfaceFullName, dto.MethodName))
            {
                throw new InvalidOperationException(
                    $"Callback target '{dto.ServiceInterfaceFullName}.{dto.MethodName}' is not authorized by the registered " +
                    $"{nameof(IAsyncResponseCallbackAuthorizer)}; add it to the allowlist (AuthorizeCallbacks) to permit it.");
            }

            // 2) Resolve the service instance
            var service = provider.GetService(serviceType)
                       ?? throw new InvalidOperationException(
                            $"Service '{dto.ServiceInterfaceFullName}' is not registered.");

            // 3) Resolve and cache method metadata + compiled invocation delegate.
            var plan = InvocationPlans.GetOrAdd(
                new InvocationPlanKey(serviceType, dto.MethodName, dto.Params.Length),
                static key => CreateInvocationPlan(key));

            // 4) Convert only the arguments that need conversion, keeping already-typed arrays hot.
            var invocationArgs = plan.ConvertArguments(dto.Params);

            // 5) Invoke through the compiled delegate and await Task/ValueTask results.
            var pending = plan.Invoke(service, invocationArgs);
            return pending.IsCompletedSuccessfully ? Task.CompletedTask : AwaitSlow(pending);
        }
        catch (Exception ex)
        {
            // Match async-method exception behavior without paying for a state machine on the hot path.
            return Task.FromException(ex);
        }
    }

    private static async Task AwaitSlow(ValueTask pending)
        => await pending.ConfigureAwait(false);

    // Internal: the durable-flow executor resolves persisted flow/input type names through the
    // same default-ALC scan + custom-resolver chain as persisted callback targets.
    internal static Type? ResolveServiceType(string serviceInterfaceFullName)
    {
        if (ServiceTypes.TryGetValue(serviceInterfaceFullName, out var cached))
        {
            return cached;
        }

        foreach (var assembly in AppDomain.CurrentDomain.GetAssemblies())
        {
            var resolved = assembly.GetType(serviceInterfaceFullName, throwOnError: false);
            if (resolved is not null)
            {
                ServiceTypes.TryAdd(serviceInterfaceFullName, resolved);
                return resolved;
            }
        }

        // Opt-in fallback for callback targets loaded into a non-default AssemblyLoadContext (plugins).
        var custom = AsyncResponseTypeResolution.Resolve(serviceInterfaceFullName);
        if (custom is not null)
        {
            ServiceTypes.TryAdd(serviceInterfaceFullName, custom);
            return custom;
        }

        AsyncResponseDiagnostics.RecordTypeResolutionFailure("service");
        return null;
    }

    private static InvocationPlan CreateInvocationPlan(InvocationPlanKey key)
    {
        // Pick the overload by name + parameter count once, then reuse the compiled plan.
        var candidates = key.ServiceType.GetMethods(BindingFlags.Instance | BindingFlags.Public)
            .Where(m => m.Name == key.MethodName
                     && m.GetParameters().Length == key.ParameterCount)
            .ToArray();

        if (candidates.Length == 0)
            throw new InvalidOperationException(
                $"No method '{key.MethodName}' with {key.ParameterCount} parameter(s) on '{key.ServiceType.Name}'.");

        if (candidates.Length > 1)
            throw new InvalidOperationException(
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
                throw new NotSupportedException(
                    $"Callback method '{method.Name}' on '{key.ServiceType.Name}' uses by-ref parameter '{parameters[i].Name}', which is not supported.");
            }

            converters[i] = GetConversionPlan(parameterType);
        }

        if (method.ContainsGenericParameters)
        {
            throw new NotSupportedException(
                $"Callback method '{method.Name}' on '{key.ServiceType.Name}' has unbound generic parameters, which are not supported.");
        }

        return new InvocationPlan(converters, CreateInvoker(method, parameters));
    }

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
        ArgumentNullException.ThrowIfNull(targetType);
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
            // Handle JSON payloads
            if (value is JsonElement je)
            {
                return JsonSerializer.Deserialize(je, targetType, _looseJsonOptions);
            }

            // JSON in a string
            if (value is string s && !_isString)
            {
                return JsonSerializer.Deserialize(s, targetType, _looseJsonOptions);
            }

            // Already the correct CLR type (a boxed value also satisfies its nullable counterpart)
            if (targetType.IsInstanceOfType(value) || (_underlyingType?.IsInstanceOfType(value) ?? false))
            {
                return value;
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
