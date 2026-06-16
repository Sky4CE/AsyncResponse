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
    // Loose options for any JSON deserialization here.
    private static readonly JsonSerializerOptions _looseJsonOptions = new()
    {
        PropertyNameCaseInsensitive = true
    };

    // Cached MethodInfo for invoking As<T> at runtime.
    private static readonly MethodInfo _asMethod = typeof(ReflectionExtensions)
        .GetMethod(nameof(As), BindingFlags.Public | BindingFlags.Static)!;

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
    {
        // Handle JSON payloads
        if (o is JsonElement je)
        {
            return JsonSerializer.Deserialize(je, targetType, _looseJsonOptions);
        }
        // JSON in a string
        if (o is string s && targetType != typeof(string))
        {
            return JsonSerializer.Deserialize(s, targetType, _looseJsonOptions);
        }
        // Already the correct CLR type (a boxed value also satisfies its nullable counterpart)
        var underlyingType = Nullable.GetUnderlyingType(targetType);
        if (targetType.IsInstanceOfType(o) || (underlyingType?.IsInstanceOfType(o) ?? false))
        {
            return o;
        }
        // Null handling
        if (o is null)
        {
            // The target being a non-nullable value type cannot represent null.
            if (targetType.IsValueType && underlyingType == null)
            {
                throw new InvalidCastException($"Cannot convert null to non-nullable type {targetType}.");
            }
            return null;
        }
        // Fallback for primitives
        return Convert.ChangeType(o, underlyingType ?? targetType);
    }

    /// <summary>
    /// Resolves the requested service from the provider and invokes the described method,
    /// converting each parameter to the method's parameter type via <see cref="As{T}"/>.
    /// </summary>
    public static async Task InvokeAsync(this IServiceProvider provider, ReflectionInvocationDto dto)
    {
        // 1) Load the service type by full name
        var serviceType = AppDomain.CurrentDomain
            .GetAssemblies()
            .Select(a => a.GetType(dto.ServiceInterfaceFullName, throwOnError: false))
            .FirstOrDefault(t => t != null);

        if (serviceType == null)
            throw new InvalidOperationException(
                $"Type '{dto.ServiceInterfaceFullName}' not found in loaded assemblies.");

        // 2) Resolve the service instance
        var service = provider.GetService(serviceType)
                   ?? throw new InvalidOperationException(
                        $"Service '{dto.ServiceInterfaceFullName}' is not registered.");

        // 3) Pick the overload by name + parameter count
        var candidates = serviceType.GetMethods()
            .Where(m => m.Name == dto.MethodName
                     && m.GetParameters().Length == dto.Params.Length)
            .ToList();

        if (candidates.Count == 0)
            throw new InvalidOperationException(
                $"No method '{dto.MethodName}' with {dto.Params.Length} parameter(s) on '{serviceType.Name}'.");

        if (candidates.Count > 1)
            throw new InvalidOperationException(
                $"Method '{dto.MethodName}' on '{serviceType.Name}' has {candidates.Count} overloads with " +
                $"{dto.Params.Length} parameter(s); persisted callbacks cannot disambiguate overloads. " +
                "Give the callback target a unique name/arity.");

        var method = candidates[0];
        var parameters = method.GetParameters();

        // 4) Build invocation args via the As<T> helper
        var invocationArgs = new object?[parameters.Length];
        for (int i = 0; i < parameters.Length; i++)
        {
            var raw = dto.Params[i];
            var targetType = parameters[i].ParameterType;
            // call the generic As<T> at runtime:
            var asMethod = _asMethod.MakeGenericMethod(targetType);
            invocationArgs[i] = asMethod.Invoke(null, [raw]);
        }

        // 5) Invoke
        var result = method.Invoke(service, invocationArgs);

        // 6) If it's Task or Task<T>, await & unwrap
        if (result is Task task)
        {
            await task.ConfigureAwait(false);
        }
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
}
