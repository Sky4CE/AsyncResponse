using Microsoft.Extensions.Logging;

namespace AsyncResponse.Channels.MongoDB;

/// <summary>
/// MongoDB implementation of <see cref="IRecoveryStateStore"/> and
/// <see cref="IRecoveryStateScanner"/>. Entries live in a TTL-indexed collection, so MongoDB itself
/// reaps expired registrations. The behaviour is shared source
/// (src/Channels/Shared/DbRecoveryStateStoreShared.cs).
/// </summary>
internal sealed class MongoDbRecoveryStateStore(
    MongoDbChannelStore store,
    ILogger<MongoDbRecoveryStateStore> logger) : DbRecoveryStateStoreBase(store, logger, "MongoDB");
