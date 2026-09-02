using StackExchange.Redis;

namespace AsyncResponse.Channels.Redis;

/// <summary>
/// The single source of truth for Redis key/channel shapes, shared by the channel and the
/// watchdog. Key shapes are a storage contract: changing them orphans in-flight recovery state.
/// </summary>
internal sealed class RedisKeySchema
{
    private readonly string _keyPrefix;

    /// <summary>Validates the prefix once at the single choke point every consumer constructs through.</summary>
    public RedisKeySchema(string keyPrefix) => _keyPrefix = ValidateKeyPrefix(keyPrefix);

    /// <summary>
    /// The prefix feeds both literal keys and the recovery SCAN <c>MATCH</c> pattern, where
    /// <c>* ? [ ] \</c> are glob metacharacters: a prefix like <c>app[prod]</c> writes keys
    /// literally but scans a character class, so the watchdog silently finds nothing — and a
    /// <c>*</c> would sweep another deployment's keys instead.
    /// </summary>
    internal static string ValidateKeyPrefix(string keyPrefix)
    {
        if (string.IsNullOrWhiteSpace(keyPrefix))
        {
            throw new InvalidOperationException(
                $"{nameof(RedisAsyncResponseOptions)}.{nameof(RedisAsyncResponseOptions.KeyPrefix)} must be a non-empty string.");
        }

        foreach (var ch in keyPrefix)
        {
            if (ch is '*' or '?' or '[' or ']' or '\\' || char.IsWhiteSpace(ch))
            {
                throw new InvalidOperationException(
                    $"{nameof(RedisAsyncResponseOptions)}.{nameof(RedisAsyncResponseOptions.KeyPrefix)} must not contain whitespace or the " +
                    "Redis glob metacharacters '*', '?', '[', ']', '\\': keys are written literally, but the recovery scan uses the " +
                    "prefix inside a SCAN MATCH pattern, so a metacharacter makes the watchdog miss this deployment's keys (or sweep a foreign deployment's).");
            }
        }

        return keyPrefix;
    }

    public string RecoveryKeyPattern => $"{_keyPrefix}:recovery:*";

    /// <summary>
    /// The response pub/sub channel for a correlation id. Key-routed: on Redis Cluster a plain
    /// channel lets each client pick its own node, and PUBLISH's reply counts only the
    /// subscribers on the node that received it — so a waiter subscribed elsewhere received the
    /// message while the publisher read 0, re-published a duplicate into the waiter's predicate
    /// (or fired lost-subscriber recovery for a delivered response). Routing SUBSCRIBE and
    /// PUBLISH to the channel's slot owner makes the count mean what the code reads it as; on a
    /// single node it changes nothing.
    /// </summary>
    public RedisChannel Channel(string correlationId)
        => new RedisChannel($"{_keyPrefix}:response:{correlationId}", RedisChannel.PatternMode.Literal).WithKeyRouting();

    /// <summary>Runs the RecoveryKey operation.</summary>
    public string RecoveryKey(string correlationId) => $"{_keyPrefix}:recovery:{correlationId}";

    /// <summary>Runs the CorrelationIdFromRecoveryKey operation.</summary>
    public string CorrelationIdFromRecoveryKey(string recoveryKey)
        => recoveryKey[$"{_keyPrefix}:recovery:".Length..];
}
