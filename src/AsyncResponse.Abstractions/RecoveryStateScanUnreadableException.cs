namespace AsyncResponse;

/// <summary>
/// Thrown by <see cref="IRecoveryStateScanner.ScanAsync"/> <em>after</em> it has yielded every
/// registration it could read, when the scan also found stored, unexpired registrations this build
/// cannot interpret: malformed JSON, an incomplete identity, or a schema version outside
/// <see cref="RecoveryStateSchema.IsReadable"/>.
/// <para>
/// Unreadable is not absent. The callback of such a registration cannot run — a response whose
/// registrations are all unreadable is refused with <see cref="RecoveryStateUnreadableException"/>,
/// one beside readable siblings is dispatched to those only — so a scan that silently skipped them
/// attested a clean store: the recovery health check read <c>Healthy</c> while callbacks could not
/// run. Thrown at the end rather than at the first unreadable record, so one corrupt record
/// never hides the staleness of every readable one: the watchdog keeps what the scan yielded and
/// reports <see cref="UnreadableCount"/> (the health check degrades on it), and any other caller
/// sees an incomplete scan fail, as the scanner contract requires.
/// </para>
/// </summary>
public sealed class RecoveryStateScanUnreadableException : InvalidOperationException
{
    /// <summary>Creates the exception for a scan that found <paramref name="unreadableCount"/> unreadable registrations.</summary>
    public RecoveryStateScanUnreadableException(int unreadableCount)
        : base(
            $"The recovery-state scan found {unreadableCount} stored registration(s) this build cannot read (malformed, " +
            "incomplete identity, or an unsupported schema version). Their recovery callbacks cannot run until a build " +
            "that can read them, or an operator, resolves them; every readable registration was yielded before this was thrown.")
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(unreadableCount);
        UnreadableCount = unreadableCount;
    }

    /// <summary>
    /// How many stored registrations the scan could not interpret. A stored record the scan cannot
    /// parse at all counts once, however many registrations it held.
    /// </summary>
    public int UnreadableCount { get; }
}
