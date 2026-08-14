namespace AsyncResponse;

/// <summary>
/// Text rules shared by the two identifier contracts — correlation ids
/// (<see cref="AsyncResponseChannelOptions.CorrelationIdNotPortable"/>) and flow ids
/// (<c>FlowStateConcurrency.FlowIdNotPortable</c>). They are checked in one place because an
/// identifier crosses the same boundaries either way: it is encoded to UTF-8 for a subject, a key,
/// or a column, and compared ordinally by the engine on the way back.
/// </summary>
internal static class PortableText
{
    /// <summary>
    /// Finds the first ill-formed UTF-16 code unit — an unpaired surrogate — or <c>-1</c> when the
    /// string is well-formed.
    /// <para>
    /// A .NET <see cref="string"/> can hold one, and it is not merely exotic: every UTF-8 encoder
    /// in the framework defaults to REPLACING it with U+FFFD rather than failing. That silent
    /// substitution is what makes an unpaired surrogate dangerous here rather than merely invalid.
    /// Two ids the engine considers different — a lone <c>U+D800</c> and a literal <c>U+FFFD</c> —
    /// encode to identical bytes, so they collide on anything derived from those bytes: a NATS
    /// subject, a recovery key, a hash. One conversation's response then reaches the other's
    /// waiter, which is exactly the failure the ordinal-identity contract exists to prevent.
    /// </para>
    /// </summary>
    internal static int IndexOfIllFormedUtf16(string value)
    {
        for (var index = 0; index < value.Length; index++)
        {
            if (!char.IsSurrogate(value[index]))
                continue;

            // A high surrogate followed by a low one is a well-formed pair: skip both.
            if (char.IsHighSurrogate(value[index])
                && index + 1 < value.Length
                && char.IsLowSurrogate(value[index + 1]))
            {
                index++;
                continue;
            }

            // Anything else is unpaired: a high surrogate at the end or before a non-low unit, or
            // a low surrogate with no high unit before it.
            return index;
        }

        return -1;
    }

    /// <summary>The rejection message for an unpaired surrogate, worded for the given kind of id.</summary>
    internal static string IllFormedUtf16Rejection(string kind, string excerpt, char offending, int index)
        => $"{kind} '{excerpt}' is not well-formed UTF-16: the code unit at index {index} (\\u{(int)offending:x4}) is an unpaired " +
            "surrogate. Encoders substitute U+FFFD for it rather than failing, so this id and one containing a literal U+FFFD " +
            "produce identical bytes — and therefore the same NATS subject, recovery key, and stored value — while the engine " +
            "compares them ordinally and treats them as two different conversations. Send a well-formed id.";
}
