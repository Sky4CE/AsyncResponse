using System.Text;

namespace AsyncResponse;

/// <summary>
/// Shared startup check that a transport's worst-case graceful-shutdown spend fits inside the
/// mirrored <c>HostShutdownTimeout</c>. Early-ACK subscribers keep working after the broker ACK,
/// so on shutdown the host must wait for the background drain (plus, on some transports, a bounded
/// connection close/join) — if that budget exceeds what the Generic Host allows, work that was
/// already ACKed is silently lost. Each transport passes only the timeouts it actually spends at
/// shutdown; a single shared comparison keeps the ten hand-copied checks from drifting again.
/// </summary>
internal static class ShutdownBudgetValidator
{
    /// <summary>
    /// Validates that the summed <paramref name="components"/> fit within
    /// <paramref name="hostShutdownTimeout"/>. A <c>null</c> budget opts out of the check
    /// (the caller validates it externally); a non-positive budget is a configuration error.
    /// </summary>
    /// <param name="transportName">Human-readable transport name used in the guidance text (e.g. "Kafka").</param>
    /// <param name="hostShutdownTimeoutName">Full option path of the transport's mirror option (e.g. "KafkaAsyncResponseTransportOptions.HostShutdownTimeout").</param>
    /// <param name="hostShutdownTimeout">The configured mirror of <c>HostOptions.ShutdownTimeout</c>, or <c>null</c> to skip the check.</param>
    /// <param name="components">Option path + value of each timeout the transport spends during graceful shutdown.</param>
    public static void Validate(
        string transportName,
        string hostShutdownTimeoutName,
        TimeSpan? hostShutdownTimeout,
        params (string Name, TimeSpan Value)[] components)
    {
        if (hostShutdownTimeout is not { } budget)
            return;

        if (budget <= TimeSpan.Zero)
            throw new InvalidOperationException($"{hostShutdownTimeoutName} must be positive when set.");

        var total = TimeSpan.Zero;
        foreach (var (_, value) in components)
            total += value;

        // Equality is allowed: the host grants exactly ShutdownTimeout, so a budget that matches
        // it can still complete. Only a strictly larger spend guarantees truncation.
        if (total <= budget)
            return;

        var itemized = new StringBuilder();
        for (var i = 0; i < components.Length; i++)
        {
            if (i > 0)
                itemized.Append(" plus ");
            itemized.Append(components[i].Name).Append(" (").Append(components[i].Value).Append(')');
        }

        throw new InvalidOperationException(
            $"{itemized} requires a shutdown budget of {total}, which exceeds " +
            $"{hostShutdownTimeoutName} ({budget}). Increase Microsoft.Extensions.Hosting.HostOptions.ShutdownTimeout " +
            $"and mirror that value in {hostShutdownTimeoutName}, " +
            $"or reduce the {transportName} shutdown/drain timeouts.");
    }
}
