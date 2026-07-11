using System.Text;

namespace AsyncResponse.Channels.NATS;

/// <summary>
/// The single source of truth for NATS subject and Key-Value key shapes used by the channel and the
/// recovery store. Correlation ids are arbitrary strings, but NATS subject tokens and KV keys accept
/// only a restricted character set, so ids are encoded with URL-safe Base64 (alphabet
/// <c>A–Z a–z 0–9 - _</c>, no padding) — every character of which is legal in both a subject token
/// and a KV key. Subject/key shapes are a storage contract: changing them orphans in-flight recovery
/// state.
/// </summary>
internal sealed class NatsSubjectSchema(string _subjectPrefix)
{
    /// <summary>The response subject a waiter subscribes to and a publisher requests on.</summary>
    public string ResponseSubject(string correlationId) => $"{_subjectPrefix}.response.{Encode(correlationId)}";

    /// <summary>The Key-Value key under which a correlation id's recovery state is stored.</summary>
    public static string RecoveryKey(string correlationId) => Encode(correlationId);

    /// <summary>
    /// Recovers the original correlation id from a recovery Key-Value key. Returns the key verbatim
    /// when it is not valid encoded content (e.g. a key written by an older/foreign producer).
    /// </summary>
    public static string CorrelationIdFromRecoveryKey(string recoveryKey) => Decode(recoveryKey) ?? recoveryKey;

    /// <summary>
    /// Encodes an arbitrary string to a NATS-safe token using URL-safe Base64 without padding.
    /// Implemented over <see cref="Convert.ToBase64String(byte[])"/> so it works identically on every
    /// target framework.
    /// </summary>
    public static string Encode(string value)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(value);

        var base64 = Convert.ToBase64String(Encoding.UTF8.GetBytes(value));
        // '+' and '/' are illegal in NATS subject tokens / KV keys; '=' padding is dropped.
        return base64.Replace('+', '-').Replace('/', '_').TrimEnd('=');
    }

    /// <summary>Decodes a token produced by <see cref="Encode"/>, or returns <c>null</c> when it is not decodable.</summary>
    public static string? Decode(string token)
    {
        if (string.IsNullOrEmpty(token))
            return null;

        var base64 = token.Replace('-', '+').Replace('_', '/');
        switch (base64.Length % 4)
        {
            case 2: base64 += "=="; break;
            case 3: base64 += "="; break;
            case 1: return null; // never produced by Encode
        }

        try
        {
            return Encoding.UTF8.GetString(Convert.FromBase64String(base64));
        }
        catch (FormatException)
        {
            return null;
        }
    }
}
