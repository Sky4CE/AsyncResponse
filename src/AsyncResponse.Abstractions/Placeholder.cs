namespace AsyncResponse;

/// <summary>
/// Expression markers for registering recoverable lost-subscriber callbacks with compile-time safety.
/// Use them inside the lambda passed to <c>OnLostSubscriberResume</c>/<c>OnLostSubscriberFailure</c>
/// to mark which arguments the runtime should substitute when the callback fires:
/// <code>
/// recoverableBuilder.For&lt;OrderResult&gt;(correlationId)
///                   .OnLostSubscriberResume&lt;IOrderFlow&gt;(flow =>
///                       flow.ResumeAsync(orderId, Placeholder.Payload&lt;OrderResult&gt;(), Placeholder.CorrelationId()))
/// </code>
/// The marker methods are never executed; they only exist inside expression trees and throw if
/// invoked directly.
/// </summary>
public static class Placeholder
{
    /// <summary>Marks an argument to be replaced by the response payload at invocation time.</summary>
    public static TPayload Payload<TPayload>() => throw MarkerInvoked();

    /// <summary>Marks an argument to be replaced by the exception at invocation time.</summary>
    public static Exception Exception() => throw MarkerInvoked();

    /// <summary>Marks an argument to be replaced by the correlation id at invocation time.</summary>
    public static string CorrelationId() => throw MarkerInvoked();

    private static InvalidOperationException MarkerInvoked()
        => new("Placeholder methods are expression-tree markers and must not be invoked at runtime. " +
               "Use them only inside OnLostSubscriberResume/OnLostSubscriberFailure lambdas.");
}
