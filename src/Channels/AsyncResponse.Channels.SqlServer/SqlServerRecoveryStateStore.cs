using Microsoft.Extensions.Logging;
using System.Runtime.CompilerServices;
using System.Text.Json;

namespace AsyncResponse.Channels.SqlServer;

/// <summary>
/// Microsoft SQL Server implementation of <see cref="IRecoveryStateStore"/> and
/// <see cref="IRecoveryStateScanner"/>.
/// </summary>
internal sealed class SqlServerRecoveryStateStore(
    SqlServerChannelSql _sql,
    ILogger<SqlServerRecoveryStateStore> _logger) : IRecoveryStateStore, IRecoveryStateScanner
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
            var state = DeserializeState(json, correlationId: null);
            if (state is not null)
                yield return state;
        }
    }

    private IReadOnlyList<RecoveryState> DeserializeStates(IReadOnlyList<string> jsonStates, string correlationId)
    {
        if (jsonStates.Count == 0)
            return [];

        var states = new List<RecoveryState>(jsonStates.Count);
        foreach (var json in jsonStates)
        {
            var state = DeserializeState(json, correlationId);
            if (state is not null)
                states.Add(state);
        }

        return states;
    }

    private RecoveryState? DeserializeState(string json, string? correlationId)
    {
        try
        {
            var state = JsonSerializer.Deserialize<RecoveryState>(json);
            if (state is null)
                return null;

            if (state.RegistrationId == Guid.Empty || string.IsNullOrWhiteSpace(state.CorrelationId))
            {
                _logger.LogWarning(
                    "SQL Server recovery state for correlationId {CorrelationId} has an incomplete identity; rejecting it.",
                    correlationId ?? state.CorrelationId);
                return null;
            }

            if (!RecoveryStateSchema.IsReadable(state.SchemaVersion))
            {
                _logger.LogWarning(
                    "SQL Server recovery state for correlationId {CorrelationId} has unsupported schema version {SchemaVersion} (current: {Current}); rejecting it instead of risking a misinterpreted recovery.",
                    correlationId ?? state.CorrelationId,
                    state.SchemaVersion,
                    RecoveryStateSchema.Current);
                return null;
            }

            if (!string.IsNullOrWhiteSpace(correlationId)
                && !string.Equals(state.CorrelationId, correlationId, StringComparison.Ordinal))
            {
                _logger.LogWarning(
                    "SQL Server recovery state has correlationId {StoredCorrelationId}, expected {CorrelationId}; rejecting it.",
                    state.CorrelationId, correlationId);
                return null;
            }

            return state;
        }
        catch (JsonException ex)
        {
            _logger.LogWarning(ex, "Unreadable SQL Server recovery state for correlationId {CorrelationId}; skipping.", correlationId);
            return null;
        }
    }
}
