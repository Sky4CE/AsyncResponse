namespace AsyncResponse;

/// <summary>
/// A flow state store that can tell, at host start, that the backend it was pointed at is one it
/// refuses to run on. The startup validator constructs the store once and, when the store
/// implements this, asks it — so a configuration the store would refuse on its first operation
/// fails the host start instead of accepting flow starts whose jobs then fail in the workers until
/// they dead-letter (the starter publishes first and tolerates store faults after the publish).
/// <para>
/// Only a REFUSAL may throw. Constructors do no I/O by convention and a backend that is briefly
/// unreachable must not keep a host from starting: an implementation bounds its own I/O, swallows
/// every fault that is not the refusal, and leaves those to the store's first operation.
/// </para>
/// </summary>
internal interface IFlowStateStoreStartupProbe
{
    /// <summary>Throws when the store refuses the backend configuration; returns otherwise.</summary>
    Task VerifyConfigurationAsync(CancellationToken cancellationToken);
}
