using System.Linq.Expressions;
using System.Reflection;

namespace AsyncResponse;

/// <summary>
/// Converts strongly-typed lambda expressions into serializable <see cref="ReflectionCallDto"/>s.
/// Literal arguments are evaluated and captured by value; <see cref="Placeholder"/> marker calls
/// become runtime placeholders. This gives compile-time safety (rename-refactoring, type checks)
/// over hand-written reflection descriptors.
/// </summary>
internal static class CallbackExpressionConverter
{
    /// <summary>Converts the callback expression to a reflection call descriptor.</summary>
    public static ReflectionCallDto ToReflectionCall<TService>(Expression<Action<TService>> expression)
        => Build<TService>(expression.Body);

    /// <summary>Converts the callback expression to a reflection call descriptor.</summary>
    public static ReflectionCallDto ToReflectionCall<TService>(Expression<Func<TService, Task>> expression)
        => Build<TService>(expression.Body);

    /// <summary>Converts the callback expression to a reflection call descriptor.</summary>
    public static ReflectionCallDto ToReflectionCall<TService>(Expression<Func<TService, ValueTask>> expression)
        => Build<TService>(expression.Body);

    private static ReflectionCallDto Build<TService>(Expression body)
    {
        if (body is not MethodCallExpression call)
            throw new NotSupportedException($"Only direct method calls are supported. Got: {body.NodeType}");

        // instance call on the lambda parameter (svc => svc.Method(...))
        if (call.Object is not ParameterExpression svcParam || svcParam.Type != typeof(TService))
            throw new NotSupportedException("Lambda must be like: svc => svc.YourMethod(args)");

        var args = call.Arguments.Select(arg => ConvertArgument(arg, svcParam)).ToArray();

        return new ReflectionCallDto
        {
            ServiceInterfaceFullName = typeof(TService).FullName
                ?? throw new InvalidOperationException("Service interface must have FullName."),
            MethodName = call.Method.Name,
            Params = args
        };
    }

    private static CallbackParam ConvertArgument(Expression expression, ParameterExpression svcParam)
    {
        // A generic payload passed to an object parameter is wrapped in an implicit Convert node.
        // Strip only that harmless boxing/reference conversion so the marker remains top-level.
        if (expression is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } conversion
            && conversion.Method is null
            && conversion.Type.IsAssignableFrom(conversion.Operand.Type))
            expression = conversion.Operand;

        // Placeholder.Payload<T>() / Placeholder.Exception() / Placeholder.CorrelationId()
        // used directly as an argument become runtime placeholders.
        if (expression is MethodCallExpression marker && marker.Method.DeclaringType == typeof(Placeholder))
        {
            return marker.Method.Name switch
            {
                nameof(Placeholder.Payload) => CallbackParam.ForPlaceholder(PlaceholderType.Payload),
                nameof(Placeholder.Exception) => CallbackParam.ForPlaceholder(PlaceholderType.Exception),
                nameof(Placeholder.CorrelationId) => CallbackParam.ForPlaceholder(PlaceholderType.CorrelationId),
                _ => throw new NotSupportedException($"Unknown placeholder marker '{marker.Method.Name}'.")
            };
        }

        return CallbackParam.ForValue(EvaluateArgument(expression, svcParam));
    }

    // Evaluate each argument expression as a closed lambda. Disallow:
    //  - method calls inside args (conservative; placeholders are handled above)
    //  - any reference to the svc parameter
    private static object? EvaluateArgument(Expression expression, ParameterExpression svcParam)
    {
        if (ContainsMethodCall(expression))
            throw new NotSupportedException(
                "Arguments must be constants/new/array/member access, or a top-level Placeholder marker; " +
                "other method-call arguments are not allowed.");

        if (ReferencesParameter(expression, svcParam))
            throw new NotSupportedException("Argument must not reference the service parameter (e.g., svc => svc.M(svc.Prop)).");

        if (expression is ConstantExpression constant)
        {
            return constant.Value;
        }

        // The dominant non-constant shape is a C# closure capture — a field read off the
        // compiler's display class (a ConstantExpression), or a static field. Read it directly:
        // building and interpreting a lambda for it cost ~60x in time and allocations, per
        // argument, per enqueue. Fields cannot throw, so behavior is identical to the
        // interpreted read. Anything deeper (nested members, properties, new/array) falls
        // through to the interpreter below.
        if (expression is MemberExpression { Member: FieldInfo field } fieldAccess)
        {
            if (fieldAccess.Expression is ConstantExpression closure)
                return field.GetValue(closure.Value);

            if (fieldAccess.Expression is null)
                return field.GetValue(null);
        }

        // Use the interpreter to avoid JIT'ing lots of tiny dynamic methods, but keep the delegate
        // typed so argument capture does not pay DynamicInvoke's reflection dispatch cost.
        var boxed = expression.Type == typeof(object)
            ? expression
            : Expression.Convert(expression, typeof(object));
        var lambda = Expression.Lambda<Func<object?>>(boxed);
        return lambda.Compile(preferInterpretation: true)();
    }

    private static bool ContainsMethodCall(Expression expression)
    {
        var visitor = new MethodCallGuard();
        visitor.Visit(expression);
        return visitor.Found;
    }

    private static bool ReferencesParameter(Expression expression, ParameterExpression parameter)
    {
        var visitor = new ParameterGuard(parameter);
        visitor.Visit(expression);
        return visitor.Found;
    }

    private sealed class MethodCallGuard : ExpressionVisitor
    {
        public bool Found;

        /// <summary>Runs the Visit operation.</summary>
        public override Expression? Visit(Expression? node)
        {
            if (Found || node is null) return node;
            if (node.NodeType == ExpressionType.Call) { Found = true; return node; }
            return base.Visit(node);
        }
    }

    private sealed class ParameterGuard(ParameterExpression _parameter) : ExpressionVisitor
    {
        public bool Found;

        /// <summary>Runs the VisitParameter operation.</summary>
        protected override Expression VisitParameter(ParameterExpression node)
        {
            if (node == _parameter) Found = true;
            return base.VisitParameter(node);
        }
    }
}
