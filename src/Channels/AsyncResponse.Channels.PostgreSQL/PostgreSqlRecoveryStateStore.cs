using Microsoft.Extensions.Logging;

namespace AsyncResponse.Channels.PostgreSQL;

/// <summary>
/// PostgreSQL implementation of <see cref="IRecoveryStateStore"/> and
/// <see cref="IRecoveryStateScanner"/>. The behaviour is shared source
/// (src/Channels/Shared/DbRecoveryStateStoreShared.cs).
/// </summary>
internal sealed class PostgreSqlRecoveryStateStore(
    PostgreSqlChannelSql sql,
    ILogger<PostgreSqlRecoveryStateStore> logger) : DbRecoveryStateStoreBase(sql, logger, "PostgreSQL");
