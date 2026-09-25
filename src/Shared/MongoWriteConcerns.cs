using MongoDB.Driver;

namespace AsyncResponse.Internal;

/// <summary>
/// The one write concern every MongoDB store pins on its collection handles (shared source,
/// compiled into the channel, transport, and durable-flow packages so the three cannot diverge) —
/// all but the transport's lease-write handles (see <see cref="BoundedMajority(WriteConcern?, TimeSpan)"/>).
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
    /// A lapsed bound surfaces as <see cref="MongoWriteConcernException"/> with the write already
    /// applied on the primary: an indeterminate, retriable outcome that idempotent inserts and
    /// deletes and revision-fenced writes absorb. It is NOT absorbed by a write that stamps a
    /// time-bounded lease the caller then acts on — an applied-but-thrown claim burns a delivery
    /// attempt for work that never ran, and a claim acknowledged only after the replication wait
    /// can return after its own lease expired — so the MongoDB transport keeps its claim, renew,
    /// and NAK on the inherited concern (see <c>MongoDbTransportStore</c>).</item>
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
}
