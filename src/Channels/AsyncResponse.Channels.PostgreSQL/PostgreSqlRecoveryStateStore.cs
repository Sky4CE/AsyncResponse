using Microsoft.Extensions.Logging;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace AsyncResponse.Channels.PostgreSQL;

/// <summary>
/// PostgreSQL implementation of <see cref="IRecoveryStateStore"/> and
/// <see cref="IRecoveryStateScanner"/>.
/// </summary>
internal sealed class PostgreSqlRecoveryStateStore(
    PostgreSqlChannelSql _sql,
    ILogger<PostgreSqlRecoveryStateStore> _logger) : IRecoveryStateStore, IRecoveryStateScanner
{
    /// <inheritdoc />
    public async Task SaveAsync(string correlationId, RecoveryState state, TimeSpan ttl, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(correlationId);
        ArgumentNullException.ThrowIfNull(state);
        if (ttl <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(ttl), "TTL must be greater than zero.");
        if (!string.Equals(state.CorrelationId, correlationId, StringComparison.Ordinal))
            throw new ArgumentException("The recovery-state correlation id must match the store key.", nameof(state));
        if (state.SchemaVersion != RecoveryStateSchema.Current)
            throw new ArgumentException("The recovery state must use the current schema version.", nameof(state));

        cancellationToken.ThrowIfCancellationRequested();
        if (state.RegistrationId == Guid.Empty)
            state.RegistrationId = Guid.NewGuid();

        await _sql.SaveRecoveryStateAsync(correlationId, state, ttl, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<RecoveryState>> GetAllAsync(string correlationId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(correlationId);
        cancellationToken.ThrowIfCancellationRequested();

        var jsonStates = await _sql.LoadRecoveryStatesAsync(correlationId, cancellationToken).ConfigureAwait(false);
        return DeserializeStates(jsonStates, correlationId);
    }

    /// <inheritdoc />
    public Task<bool> TryDeleteAsync(string correlationId, Guid registrationId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(correlationId);
        if (registrationId == Guid.Empty)
            throw new ArgumentException("Registration id cannot be empty.", nameof(registrationId));
        cancellationToken.ThrowIfCancellationRequested();
        return _sql.DeleteRecoveryStateAsync(correlationId, registrationId, cancellationToken);
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<RecoveryState> ScanAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var json in _sql.ScanRecoveryStateJsonAsync(cancellationToken).ConfigureAwait(false))
        {
            var ignored = 0;
            var state = DeserializeState(json, correlationId: null, ref ignored);
            if (state is not null)
                yield return state;
        }
    }

    private IReadOnlyList<RecoveryState> DeserializeStates(IReadOnlyList<string> jsonStates, string correlationId)
    {
        if (jsonStates.Count == 0)
            return [];

        var states = new List<RecoveryState>(jsonStates.Count);
        var unreadable = 0;
        foreach (var json in jsonStates)
        {
            var state = DeserializeState(json, correlationId, ref unreadable);
            if (state is not null)
                states.Add(state);
        }

        // Rows existed and none of them survived materialization. Returning an empty list here told
        // the dispatcher "no recovery callback was ever armed", which it answers by acknowledging
        // the response — so a corrupt or newer-schema registration silently consumed a terminal
        // response its callback never saw. Fail instead, and let redelivery reach a build that can
        // read it. A PARTIALLY readable batch deliberately does not throw: see
        // RecoveryStateUnreadableException.
        // Only rows this build could not INTERPRET count. A row rejected for belonging to another
        // correlation id is perfectly readable — it surfaced because a legacy case-insensitive
        // collation matched the wrong key, and refusing it is the ordinal re-check doing its job.
        // For the id actually asked about, that is absence, not corruption, and absence must stay
        // an empty list.
        if (states.Count == 0 && unreadable > 0)
            throw new RecoveryStateUnreadableException(correlationId, unreadable);

        return states;
    }

    /// <summary>The registration's metadata off the library's resolver — case-sensitive matching, as before.</summary>
    private static readonly JsonTypeInfo<RecoveryState> _stateTypeInfo =
        AsyncResponseJson.GetTypeInfo<RecoveryState>(AsyncResponseJson.Default);

    private RecoveryState? DeserializeState(string json, string? correlationId, ref int unreadable)
    {
        try
        {
            // Through JsonSafety, not the raw reader: the exception logged below is the body-free
            // rebuild (size and position). The reader's own appends `Path: $.Context['<key>']`
            // built from the stored registration's context keys — tenant and auth baggage — which
            // the warning then carried into the application log.
            var state = JsonSafety.SafeDeserialize(json, _stateTypeInfo);
            if (state is null)
            {
                unreadable++;
                return null;
            }

            if (state.RegistrationId == Guid.Empty || string.IsNullOrWhiteSpace(state.CorrelationId))
            {
                _logger.LogWarning(
                    "PostgreSQL recovery state for correlationId {CorrelationId} has an incomplete identity; rejecting it.",
                    correlationId ?? state.CorrelationId);
                unreadable++;
                return null;
            }

            if (!RecoveryStateSchema.IsReadable(state.SchemaVersion))
            {
                _logger.LogWarning(
                    "PostgreSQL recovery state for correlationId {CorrelationId} has unsupported schema version {SchemaVersion} (current: {Current}); rejecting it instead of risking a misinterpreted recovery.",
                    correlationId ?? state.CorrelationId,
                    state.SchemaVersion,
                    RecoveryStateSchema.Current);
                unreadable++;
                return null;
            }

            if (!string.IsNullOrWhiteSpace(correlationId)
                && !string.Equals(state.CorrelationId, correlationId, StringComparison.Ordinal))
            {
                _logger.LogWarning(
                    "PostgreSQL recovery state has correlationId {StoredCorrelationId}, expected {CorrelationId}; rejecting it.",
                    state.CorrelationId, correlationId);
                return null;
            }

            return state;
        }
        catch (Exception ex) when (ex is JsonException or InvalidDataException)
        {
            _logger.LogWarning(ex, "Unreadable PostgreSQL recovery state for correlationId {CorrelationId}; skipping.", correlationId);
            unreadable++;
            return null;
        }
    }
}
