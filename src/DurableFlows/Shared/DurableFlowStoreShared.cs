using System.Data.Common;
using System.Text;

namespace AsyncResponse.DurableFlows.Internal;

internal static class DurableFlowStoreShared
{
    /// <summary>
    /// Upper bound for TTL values handed to server-clock date arithmetic (~68 years). SQL Server's
    /// <c>DATEADD</c> takes <c>int</c> seconds, and MySQL/Oracle datetime types stop at year 9999,
    /// so an absurd <see cref="DurableFlowOptions.StateExpiry"/> (for example
    /// <see cref="TimeSpan.MaxValue"/>) would overflow inside the database. Clamping mirrors
    /// <see cref="AddSaturating(DateTime, TimeSpan)"/> on the client side: huge expiries saturate
    /// to "effectively never" instead of failing every write.
    /// </summary>
    private static readonly TimeSpan MaxServerClockTtl = TimeSpan.FromSeconds(int.MaxValue);

    public static void ValidateCreate(string flowId, FlowState state, TimeSpan ttl)
    {
        ValidateWrite(flowId, state, ttl);
        if (state.Revision != 0)
            throw new ArgumentException("A new flow ledger must start at revision zero.", nameof(state));
    }

    public static void ValidateUpdate(string flowId, FlowState state, long expectedRevision, TimeSpan ttl)
    {
        ValidateWrite(flowId, state, ttl);
        if (expectedRevision < 0)
            throw new ArgumentOutOfRangeException(nameof(expectedRevision), "The expected revision cannot be negative.");
        if (state.Revision != checked(expectedRevision + 1))
            throw new ArgumentException("The new flow-state revision must increment the expected revision by one.", nameof(state));
    }

    /// <summary>
    /// Argument preamble shared by every store's lease acquire/renew path. The stores pass their
    /// own parameters straight through, so the thrown <c>ParamName</c>s ("flowId", "leaseId",
    /// "leaseDuration") match the public <c>IFlowStateStore</c> signatures exactly.
    /// </summary>
    public static void ValidateLeaseArgs(string flowId, string leaseId, TimeSpan leaseDuration)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(flowId);
        ArgumentException.ThrowIfNullOrWhiteSpace(leaseId);
        if (leaseDuration <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(leaseDuration));
    }

    /// <summary>
    /// The ledger has exactly ONE wire format: Core's <c>FlowStateJson</c> (source-generated
    /// metadata, nulls omitted on write, resolved through the <c>AsyncResponseJson</c> chain so
    /// <c>AsyncResponseJsonSerialization.RegisterResolver</c> — the documented trim/AOT seam —
    /// reaches every store). Serialize and Deserialize both delegate there, so a ledger written
    /// through any provider store always loads through any other, byte for byte.
    /// </summary>
    public static string Serialize(FlowState state) => FlowStateJson.Serialize(state);

    /// <summary>
    /// Serializes a ledger for a full-state write and enforces the store's <c>MaxStateBytes</c>
    /// budget. Without the guard an oversized ledger surfaces as the provider's opaque payload
    /// error (DynamoDB 400 KB item cap, Cosmos 2 MB, MongoDB 16 MB) which the executor retries
    /// into the dead-letter queue with no hint at the real cause. Throwing here keeps the same
    /// at-least-once semantics (run fails → retries → DLQ = operator alarm) but names the cause.
    /// </summary>
    /// <exception cref="FlowStateTooLargeException">The serialized state exceeds <paramref name="maxStateBytes"/>.</exception>
    public static string SerializeBounded(string flowId, FlowState state, long? maxStateBytes, string providerName)
    {
        var json = Serialize(state);
        if (maxStateBytes is { } limit)
        {
            long size = Encoding.UTF8.GetByteCount(json);
            if (size > limit)
                throw new FlowStateTooLargeException(flowId, size, limit, providerName);
        }

        return json;
    }

    /// <summary>
    /// Materializes a loaded ledger row. Unreadable JSON, an unknown schema version, a revision
    /// that does not match the stored row, and an identity-mismatched ledger
    /// (<c>state.FlowId != flowId</c>) all load as absent — the read-side mirror of the write-side
    /// key/identity validation in <see cref="ValidateCreate"/>, so a row copied or restored under
    /// the wrong key can never resurrect as that flow.
    /// </summary>
    public static FlowState? ReadState(string flowId, string stateJson, long revision)
    {
        var state = Deserialize(stateJson);
        return state is not null
            && state.Revision == revision
            && string.Equals(state.FlowId, flowId, StringComparison.Ordinal)
                ? state
                : null;
    }

    /// <summary>
    /// <paramref name="instant"/> + <paramref name="ttl"/>, saturating at
    /// <see cref="DateTime.MaxValue"/> instead of throwing: an absurd
    /// <see cref="DurableFlowOptions.StateExpiry"/> then means "effectively never expires" rather
    /// than failing every write with an <see cref="ArgumentOutOfRangeException"/>.
    /// </summary>
    public static DateTime AddSaturating(DateTime instant, TimeSpan ttl)
        => ttl > DateTime.MaxValue - instant ? DateTime.MaxValue : instant + ttl;

    /// <inheritdoc cref="AddSaturating(DateTime, TimeSpan)"/>
    public static DateTimeOffset AddSaturating(DateTimeOffset instant, TimeSpan ttl)
        => ttl > DateTimeOffset.MaxValue - instant ? DateTimeOffset.MaxValue : instant + ttl;

    /// <summary>TTL clamped for server-clock date arithmetic; see <see cref="MaxServerClockTtl"/>.</summary>
    public static TimeSpan ServerClockTtl(TimeSpan ttl)
        => ttl > MaxServerClockTtl ? MaxServerClockTtl : ttl;

    /// <summary>Whole milliseconds of <see cref="ServerClockTtl"/>, for stores that bind the TTL as a number.</summary>
    public static long ServerClockTtlMilliseconds(TimeSpan ttl)
        => (long)ServerClockTtl(ttl).TotalMilliseconds;

    /// <inheritdoc cref="Serialize"/>
    public static FlowState? Deserialize(string json) => FlowStateJson.Deserialize(json);

    /// <summary>
    /// Throttles opportunistic expired-state pruning: returns <c>true</c> at most once per
    /// <paramref name="interval"/> (a non-positive interval prunes on every operation, matching the
    /// channel packages). Loads already filter on expiry, so throttling never affects correctness.
    /// </summary>
    public static bool ShouldPrune(ref long lastTicks, TimeSpan interval)
    {
        if (interval <= TimeSpan.Zero)
            return true;

        var now = DateTime.UtcNow.Ticks;
        var last = Interlocked.Read(ref lastTicks);
        return now - last >= interval.Ticks
            && Interlocked.CompareExchange(ref lastTicks, now, last) == last;
    }

    /// <summary>
    /// Advisory-lock key for schema DDL, derived exactly like the channel/transport packages
    /// (FNV-1a over <c>asyncresponse:ddl:{schemaName}</c>) so flow-store DDL serializes with any
    /// channel/transport DDL running against the same schema.
    /// </summary>
    public static long SchemaLockKey(string schemaName)
    {
        const ulong offset = 14695981039346656037UL;
        const ulong prime = 1099511628211UL;
        var hash = offset;
        foreach (var b in Encoding.UTF8.GetBytes(SchemaLockResource(schemaName)))
        {
            hash ^= b;
            hash *= prime;
        }

        return unchecked((long)hash);
    }

    /// <summary>SQL Server <c>sp_getapplock</c> resource name for schema DDL (shared with the channel/transport packages).</summary>
    public static string SchemaLockResource(string schemaName)
        => $"asyncresponse:ddl:{schemaName}";

    /// <summary>Connection-string guard shared by the stores that own their connections.</summary>
    public static void ValidateConnectionString(string? connectionString, string optionsTypeName)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
            throw new InvalidOperationException($"{optionsTypeName}.ConnectionString must be configured.");
    }

    /// <summary><c>MaxStateBytes</c> guard shared by all nine stores (null disables the budget).</summary>
    public static void ValidateMaxStateBytes(long? maxStateBytes, string optionsTypeName)
    {
        if (maxStateBytes is <= 0)
            throw new InvalidOperationException($"{optionsTypeName}.MaxStateBytes must be positive when configured.");
    }

    /// <summary>
    /// Opens a fresh provider connection, disposing it when open fails — an ADO.NET connection
    /// that failed to open still holds its allocation until disposed, and the caller never
    /// receives it.
    /// </summary>
    public static async Task<TConnection> OpenConnectionAsync<TConnection>(
        string? connectionString,
        CancellationToken cancellationToken)
        where TConnection : DbConnection, new()
    {
        var connection = new TConnection { ConnectionString = connectionString };
        try
        {
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            return connection;
        }
        catch
        {
            await connection.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    public static void ValidateIdentifier(string? value, string optionName, string providerName, int identifierCap = 0)
    {
        if (string.IsNullOrWhiteSpace(value))
            throw new InvalidOperationException($"{optionName} must be configured.");

        if (!(char.IsAsciiLetter(value[0]) || value[0] == '_'))
            throw new InvalidOperationException($"{optionName} '{value}' must be a simple {providerName} identifier (letters, digits, and underscores; not starting with a digit).");

        foreach (var c in value)
        {
            if (!(char.IsAsciiLetterOrDigit(c) || c == '_'))
                throw new InvalidOperationException($"{optionName} '{value}' must be a simple {providerName} identifier (letters, digits, and underscores; not starting with a digit).");
        }

        if (identifierCap > 0 && value.Length > identifierCap)
            throw new InvalidOperationException(
                $"{optionName} '{value}' is {value.Length} characters; {providerName} identifiers are limited to {identifierCap}.");
    }

    /// <summary>
    /// Derived object name with suffix space RESERVED before the provider's identifier cap:
    /// truncating the whole "{table}{suffix}" lets a maximum-length table name derive its own
    /// name — on providers where indexes share the table namespace the DDL is then silently
    /// skipped, on the rest it fails outright.
    /// <para>
    /// Kept in step with <c>AsyncResponse.Internal.RelationalNamePlan.DerivedName</c>, which is
    /// the same rule for the channel and transport packages. The two cannot be one method: this
    /// file is source-linked into all nine flow stores, and RelationalNamePlan is linked only
    /// into the four relational channel/transport packages that need a name plan.
    /// </para>
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="suffix"/> leaves no room for a stem inside <paramref name="identifierCap"/>.
    /// Guarded explicitly: the slice below would otherwise take a negative length and fail schema
    /// creation with a bare index-out-of-range naming neither the suffix nor the cap.
    /// </exception>
    public static string DerivedName(string tableName, string suffix, int identifierCap)
    {
        if (identifierCap <= 0 || tableName.Length + suffix.Length <= identifierCap)
            return tableName + suffix;

        if (suffix.Length >= identifierCap)
        {
            throw new ArgumentOutOfRangeException(
                nameof(suffix),
                $"The derived-name suffix '{suffix}' is {suffix.Length} characters, which leaves no room for a table stem inside " +
                $"the {identifierCap}-character identifier limit. Shorten the suffix.");
        }

        return tableName[..(identifierCap - suffix.Length)] + suffix;
    }

    private static void ValidateWrite(string flowId, FlowState state, TimeSpan ttl)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(flowId);
        ArgumentNullException.ThrowIfNull(state);
        if (!string.Equals(state.FlowId, flowId, StringComparison.Ordinal))
            throw new ArgumentException("The flow state id must match the store key.", nameof(state));
        if (state.SchemaVersion != FlowStateSchema.Current)
            throw new ArgumentException("The flow state must use the current schema version.", nameof(state));
        if (ttl <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(ttl), "TTL must be greater than zero.");
    }
}

/// <summary>
/// Thrown when a serialized durable-flow ledger exceeds the store's configured
/// <c>MaxStateBytes</c> budget. Internal on purpose: this shared source is compiled into every
/// store package, so a public type here would surface as identically-named colliding public types
/// when a host references two store packages. Callers catch it as its
/// <see cref="InvalidOperationException"/> base; the message carries the diagnosis.
/// </summary>
internal sealed class FlowStateTooLargeException(string flowId, long serializedSizeBytes, long maxStateBytes, string providerName)
    : InvalidOperationException(
        $"Flow '{flowId}' state serialized to {serializedSizeBytes} bytes, exceeding the {providerName} MaxStateBytes limit of {maxStateBytes} bytes — " +
        "flow state exceeded the provider's size limit. Keep large payloads in your own storage and pass references in flow state; " +
        "see docs/durable-flows.md (ledger-size note).")
{
    /// <summary>The flow whose ledger write was rejected.</summary>
    public string FlowId { get; } = flowId;

    /// <summary>Serialized ledger size in UTF-8 bytes.</summary>
    public long SerializedSizeBytes { get; } = serializedSizeBytes;

    /// <summary>The configured budget the write exceeded.</summary>
    public long MaxStateBytes { get; } = maxStateBytes;
}
