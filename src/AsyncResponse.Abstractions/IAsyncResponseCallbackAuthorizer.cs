namespace AsyncResponse;

/// <summary>
/// Opt-in authorization gate for the reflection-based callback machinery (lost-subscriber recovery
/// callbacks and worker jobs). When an implementation is registered in DI, every persisted callback
/// is checked against it immediately before the target service method is invoked; a target that is
/// not allowed is rejected rather than invoked.
/// <para>
/// This is defense-in-depth on top of "secure your store/transport": even if an attacker can write a
/// recovery entry or worker job into the backing store, only allowed <c>(service, method)</c> pairs
/// can be invoked — narrowing arbitrary-method-invocation on DI-registered services down to an
/// explicit set. Registering no authorizer preserves the default behavior (any DI-registered service
/// method may be a callback target), so there is <b>zero required boilerplate</b>: authorization is
/// purely opt-in, and it is configured once at the type level rather than per callback method.
/// </para>
/// </summary>
public interface IAsyncResponseCallbackAuthorizer
{
    /// <summary>
    /// Returns <c>true</c> if a callback may invoke <paramref name="methodName"/> on the service
    /// identified by <paramref name="serviceInterfaceFullName"/>; <c>false</c> to reject it.
    /// </summary>
    bool IsAllowed(string serviceInterfaceFullName, string methodName);
}
