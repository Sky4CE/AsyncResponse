using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Serializers;

namespace AsyncResponse.Internal;

/// <summary>
/// A stored instant as a UTC BSON date, set on the member itself
/// (<c>[BsonSerializer(typeof(UtcBsonDateSerializer))]</c>) so the global serializer registry is
/// never consulted. Shared source, compiled into the channel, transport, and durable-flow
/// packages: every <see cref="DateTime"/> member of their documents carries it.
/// <para>
/// Every instant those stores persist is stamped server-side (<c>$$NOW</c> pipelines, or the
/// server's <c>hello.localTime</c>), so the stored value is always a BSON date — but the class map
/// decides how a filter value and a read are (de)serialized. Under a host-registered
/// <c>DateTimeSerializer(BsonType.String)</c> (or Document/Int64) every date filter rendered a
/// string, which by BSON type bracketing never matches a date: the channel's message sweep found
/// nothing, so no response was ever delivered live. Under a host's own string-only
/// <c>IBsonSerializer&lt;DateTime&gt;</c> every read of a stored date threw
/// <see cref="FormatException"/>: every channel read failed, and the transport's claim buried
/// every job as unreadable without running it.
/// </para>
/// <para>
/// <c>[BsonDateTimeOptions]</c> would not do: it RECONFIGURES whatever serializer the registry
/// returns for <see cref="DateTime"/>, and for a host that registered its own
/// <c>IBsonSerializer&lt;DateTime&gt;</c> (anything but the driver's <see cref="DateTimeSerializer"/>)
/// freezing the class map throws <see cref="NotSupportedException"/>, failing every store
/// operation. The driver's serializers are sealed, so this delegates to a privately held one
/// instead of deriving from it.
/// </para>
/// </summary>
internal sealed class UtcBsonDateSerializer : SerializerBase<DateTime>
{
    internal static readonly DateTimeSerializer Pinned = new(DateTimeKind.Utc, BsonType.DateTime);

    public override DateTime Deserialize(BsonDeserializationContext context, BsonDeserializationArgs args)
        => Pinned.Deserialize(context, args);

    public override void Serialize(BsonSerializationContext context, BsonSerializationArgs args, DateTime value)
        => Pinned.Serialize(context, args, value);
}

/// <summary>The nullable twin of <see cref="UtcBsonDateSerializer"/>, for optional instants (a lease expiry, an ack time).</summary>
internal sealed class NullableUtcBsonDateSerializer : SerializerBase<DateTime?>
{
    private static readonly NullableSerializer<DateTime> Pinned = new(UtcBsonDateSerializer.Pinned);

    public override DateTime? Deserialize(BsonDeserializationContext context, BsonDeserializationArgs args)
        => Pinned.Deserialize(context, args);

    public override void Serialize(BsonSerializationContext context, BsonSerializationArgs args, DateTime? value)
        => Pinned.Serialize(context, args, value);
}
