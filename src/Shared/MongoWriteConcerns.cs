using MongoDB.Bson;
using MongoDB.Driver;

namespace AsyncResponse.Internal;

/// <summary>
/// The write concerns the MongoDB stores pin on their collection handles (shared source, compiled
/// into the channel, transport, and durable-flow packages so the three cannot diverge): a bounded
/// majority on every write that must survive a failover — all of the channel's and flow store's,
/// the transport's inserts — and primary acknowledgement on the transport's lease writes and
/// deletes (<see cref="PrimaryAcknowledged(WriteConcern?)"/>); and how a store reads the bound
/// lapsing (<see cref="IsReplicationTimeout"/>).
/// </summary>
internal static class MongoWriteConcerns
{
    /// <summary>
    /// The replication bound used when the host-supplied database states none: well under the
    /// transport's default 30 s <c>LockTimeout</c> and the flow store's 1 min lease.
    /// </summary>
    public static readonly TimeSpan DefaultMajorityTimeout = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Majority acknowledgement, with a bound, keeping what the operator configured otherwise.
    /// <list type="bullet">
    /// <item><c>w: "majority"</c> whatever the host-supplied database carries: under an inherited
    /// <c>w: 1</c> (a connection-string setting, the default on a primary-secondary-arbiter set, any
    /// pre-5.0 server) the primary acknowledges a write before any secondary has it, and a failover
    /// rolls it back — a published job, a stored response, a flow checkpoint the caller already saw
    /// succeed silently disappears.</item>
    /// <item><c>wtimeout</c>: an inherited one survives; an unset one gets
    /// <paramref name="defaultWTimeout"/>. A bare <c>WriteConcern.WMajority</c> carried NO bound and
    /// replaced the inherited concern wholesale (discarding the operator's <c>wtimeoutMS</c> and
    /// <c>journal</c>), so on a primary-secondary-arbiter set with its secondary down — where the
    /// majority of data-bearing nodes can never acknowledge — every write blocked indefinitely.
    /// A lapsed bound surfaces as a write-concern error with the write already applied on the
    /// primary (<see cref="IsReplicationTimeout"/>). It carries no <c>RetryableWriteError</c>
    /// label, and a retry is no way out of it: the retried write — a no-op by then — waits on the
    /// same replication and lapses again for as long as the set stays degraded (a
    /// primary-secondary-arbiter set with its secondary down lapses on every write). So each store
    /// settles it by what the primary now holds: revision- and lease-fenced flow writes fail as a
    /// retriable error their fences make safe to repeat; the channel re-reads the response or claim
    /// it just wrote by id and acts on that; the transport's publish and dead-letter insert count
    /// as applied with a warning — a same-id re-upsert could re-create a job a subscriber had
    /// already claimed, run and deleted in the meantime. What the writer gives up for those writes
    /// is only the majority guarantee: a failover before replication can still roll them back.
    /// It is not absorbed by a write that stamps a time-bounded lease the caller then acts on — an
    /// applied-but-thrown claim burns a delivery attempt for work that never ran, and a claim
    /// acknowledged only after the replication wait can return after its own lease expired — so
    /// the MongoDB transport claims, renews, and NAKs with <see cref="PrimaryAcknowledged(WriteConcern?)"/>,
    /// and deletes (ack, burial, prune) with it too: a rolled-back delete only makes a document
    /// claimable again, while a majority one held the claim loop for the whole bound and then
    /// reported a delete the primary had applied as failed (see <c>MongoDbTransportStore</c>).</item>
    /// <item><c>journal</c>/<c>fsync</c> are inherited untouched.</item>
    /// </list>
    /// </summary>
    /// <param name="inherited">The host-supplied database's write concern (<c>null</c> reads as the server default).</param>
    /// <param name="defaultWTimeout">The bound applied when <paramref name="inherited"/> states none.</param>
    public static WriteConcern BoundedMajority(WriteConcern? inherited, TimeSpan defaultWTimeout)
    {
        var baseline = inherited ?? WriteConcern.Acknowledged;
        return baseline.With(w: WriteConcern.WMajority.W, wTimeout: baseline.WTimeout ?? defaultWTimeout);
    }

    /// <inheritdoc cref="BoundedMajority(WriteConcern?, TimeSpan)"/>
    public static WriteConcern BoundedMajority(IMongoDatabase database)
        => BoundedMajority(database.Settings?.WriteConcern, DefaultMajorityTimeout);

    /// <summary>
    /// Primary acknowledgement (<c>w: 1</c>, no <c>wtimeout</c> — there is no replication wait to
    /// bound), keeping the inherited <c>journal</c>/<c>fsync</c>: the transport's lease writes and
    /// deletes.
    /// Stated explicitly rather than inherited, because the inherited concern is majority on most
    /// modern deployments — an Atlas-style <c>w=majority</c> connection string, or no <c>w</c> at
    /// all against a 5.0+ primary-secondary-secondary set, whose implicit default is majority with
    /// no bound — and a majority lease write has exactly the two hazards
    /// <see cref="BoundedMajority(WriteConcern?, TimeSpan)"/> describes.
    /// </summary>
    /// <param name="inherited">The host-supplied database's write concern (<c>null</c> reads as the server default).</param>
    public static WriteConcern PrimaryAcknowledged(WriteConcern? inherited)
        => (inherited ?? WriteConcern.Acknowledged).With(w: WriteConcern.W1.W, wTimeout: null);

    /// <inheritdoc cref="PrimaryAcknowledged(WriteConcern?)"/>
    public static WriteConcern PrimaryAcknowledged(IMongoDatabase database)
        => PrimaryAcknowledged(database.Settings?.WriteConcern);

    /// <summary>
    /// Whether <paramref name="exception"/> is a lapsed replication bound and nothing else: the
    /// write succeeded on the primary, and only the wait for its majority acknowledgement timed
    /// out (<c>WriteConcernFailed</c>, code 64, <c>errInfo.wtimeout</c>). The driver reports it as
    /// a <see cref="MongoWriteConcernException"/> for command writes (<c>findAndModify</c>, which
    /// throws before the reply's document is read), and as a <see cref="MongoWriteException"/> or
    /// <see cref="MongoBulkWriteException"/> with a write-concern error and no write error for CRUD
    /// writes. A write that itself failed is never a replication timeout.
    /// </summary>
    public static bool IsReplicationTimeout(Exception exception) => exception switch
    {
        MongoWriteConcernException { WriteConcernResult.Response: { } response }
            => response.TryGetValue("writeConcernError", out var error)
               && error is BsonDocument details
               && IsWTimeout(
                   details.TryGetValue("code", out var code) && code.IsNumeric ? code.ToInt32() : 0,
                   details.TryGetValue("errInfo", out var errInfo) ? errInfo as BsonDocument : null),
        MongoWriteException { WriteError: null, WriteConcernError: { } error }
            => IsWTimeout(error.Code, error.Details),
        MongoBulkWriteException { WriteErrors.Count: 0, WriteConcernError: { } error }
            => IsWTimeout(error.Code, error.Details),
        _ => false
    };

    private static bool IsWTimeout(int code, BsonDocument? errInfo)
        => code == WriteConcernFailed
           || (errInfo is not null
               && errInfo.TryGetValue("wtimeout", out var wtimeout)
               && wtimeout is BsonBoolean { Value: true });

    /// <summary>The server's <c>WriteConcernFailed</c> error code, which a lapsed <c>wtimeout</c> reports.</summary>
    private const int WriteConcernFailed = 64;
}
