using Microsoft.Extensions.Hosting;

namespace AsyncResponse.Transports;

// Shared source for every transport's worker subscriber: each csproj that uses it pulls this file in
// via <Compile Include="..\Shared\WorkerIntakeGate.cs" />, so it compiles INTO each provider
// assembly, like SubscriberSupervisor.cs.

/// <summary>
/// The host-stop signal a WORKER subscriber stops taking new deliveries at. Once
/// <see cref="IHostApplicationLifetime.ApplicationStopping"/> fires, the durable-flow engine hands an
/// in-process timer over (checkpoint, immediate wake-up, acknowledge) and hands back — throws
/// <see cref="DurableFlowInterruptedException"/> — any timer or lease wait that STARTS on this host.
/// A delivery this host takes after that point therefore runs at most up to its first such wait and
/// is handed back again: under ack-after-handler a spent delivery attempt and a lock held until it
/// lapses; under early ACK a wake-up already settled at enqueue, turned into a dead-letter copy (or
/// lost, where the transport has no dead-letter destination). The worker subscribers are registered
/// first and so stop last: the gap between ApplicationStopping and their own stop — the web host's
/// drain, the response subscriber's stop — is the window in which this host took, and handed back,
/// the very wake-ups its own hand-overs had just published for a live replica. Taking nothing new
/// from ApplicationStopping on leaves them to that replica.
/// Worker role only: a response subscriber keeps delivering to the waiters host stop deliberately
/// does not interrupt. Without a registered lifetime the gate never closes — and nothing needs it
/// to, since the engine hands back only on ApplicationStopping.
/// </summary>
internal sealed class WorkerIntakeGate
{
    private readonly CancellationToken _hostStopping;

    public WorkerIntakeGate(IHostApplicationLifetime? hostLifetime) =>
        _hostStopping = hostLifetime?.ApplicationStopping ?? CancellationToken.None;

    /// <summary>True once host stop has begun: take no new delivery.</summary>
    public bool IsClosed => _hostStopping.IsCancellationRequested;

    /// <summary>
    /// Fires when host stop begins — for a loop parked in a receive, a poll delay or a backpressure
    /// wait that must stop taking deliveries at once rather than at its own stop.
    /// </summary>
    public CancellationToken HostStopping => _hostStopping;
}
