using AsyncResponse.Conformance;

namespace AsyncResponse.Tests;

/// <summary>
/// The transport contract's in-memory derivation — the spec the container-backed transports are held
/// to, calibrated here first for the same reason
/// <see cref="AsyncResponse.Conformance.ChannelConformanceSuite"/>'s in-memory derivation lives in
/// this project: it needs no Docker, so a contract regression shows up in the fast suite.
/// <para>
/// The in-process queue is deliberately the least capable transport — no redelivery bound, no
/// dead-letter destination, no addressable reply target, and nothing outlives the host — so most of
/// the fault-injection facts skip here with that reason named. See
/// <see cref="TransportCapabilities.For"/>.
/// </para>
/// </summary>
public sealed class InMemoryTransportConformanceTests : TransportConformanceSuite
{
    protected override MatrixTransport Transport => MatrixTransport.InMemory;

    /// <summary>No backend: the in-memory transport reaching for one would be the bug.</summary>
    protected override MatrixBackends Backends => new();

    protected override TimeSpan Generous => TimeSpan.FromSeconds(10);
}
