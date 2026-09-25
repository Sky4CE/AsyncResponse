using Microsoft.Extensions.Logging;

namespace AsyncResponse.Channels.SqlServer;

/// <summary>
/// Microsoft SQL Server implementation of <see cref="IRecoveryStateStore"/> and
/// <see cref="IRecoveryStateScanner"/>. The behaviour is shared source
/// (src/Channels/Shared/DbRecoveryStateStoreShared.cs).
/// </summary>
internal sealed class SqlServerRecoveryStateStore(
    SqlServerChannelSql sql,
    ILogger<SqlServerRecoveryStateStore> logger) : DbRecoveryStateStoreBase(sql, logger, "SQL Server");
