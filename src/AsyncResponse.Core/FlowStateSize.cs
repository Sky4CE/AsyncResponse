using System.Text;

namespace AsyncResponse;

/// <summary>
/// The size a ledger really has in its store, for the <see cref="DurableFlowOptions.LedgerSizeWarningBytes"/>
/// warning: the UTF-8 length of the JSON a store last read for, or serialized from, one
/// <see cref="FlowState"/> instance.
/// <para>
/// <see cref="FlowStateJson.EstimateLedgerChars"/> only bounds that size from below — it counts
/// the characters of the strings the ledger carries, while the store writes their JSON-escaped
/// UTF-8 form plus every property name — and a step result full of quotes or non-ASCII text is
/// stored several times larger than its estimate. Judged by the estimate, a growing ledger reached
/// the store's <c>MaxStateBytes</c> and was refused while the warning still thought it small. The
/// built-in stores already measure the JSON they serialize against that cap; they record the
/// measurement on the instance itself, and the warning takes it right after the write.
/// </para>
/// </summary>
internal static class FlowStateSize
{
    /// <summary>Records the JSON a store has just read for, or serialized from, <paramref name="state"/>.</summary>
    internal static void Record(FlowState state, string json) => Record(state, Encoding.UTF8.GetByteCount(json));

    /// <inheritdoc cref="Record(FlowState, string)"/>
    internal static void Record(FlowState state, long utf8Bytes) => state.MeasuredUtf8Bytes = utf8Bytes;

    /// <summary>
    /// The ledger's size as the warning judges it: the store's measurement of this instance's last
    /// read or write — consumed, so a later write the store does not measure (a custom store) is
    /// judged by the estimate instead of by an older, smaller measurement — and never less than the
    /// estimate.
    /// </summary>
    internal static long Take(FlowState state)
    {
        var estimate = FlowStateJson.EstimateLedgerChars(state);
        if (state.MeasuredUtf8Bytes is not { } measured)
            return estimate;

        state.MeasuredUtf8Bytes = null;
        return Math.Max(measured, estimate);
    }
}
