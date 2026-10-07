using System.Globalization;
using System.Reflection;
using AsyncResponse.Channels.MongoDB;
using AsyncResponse.DurableFlows.MongoDB;
using AsyncResponse.Transports.MongoDB;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Serializers;
using MongoDB.Driver;
using Moq;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Round 65 (mongo): the channel and transport documents pin their instants to BSON dates whatever
/// DateTime serializer the host registered (the flow store's fix, carried to its siblings); index
/// DDL rides a w=1 handle in the channel and flow store (the transport's fix, carried to its
/// siblings); and the transport's wake change stream projects each event down to what it reads.
/// </summary>
public sealed class Round65MongoTests
{
    private static readonly DateTime Now = new(2031, 3, 14, 9, 26, 53, 589, DateTimeKind.Utc);

    /// <summary>
    /// F-04 (a): under a host-wide <c>DateTimeSerializer(BsonType.String)</c> the channel's sweep
    /// filter rendered its instants as strings — which never match the <c>$$NOW</c>-stamped BSON
    /// dates (type bracketing), so no response was ever delivered live — and the documents wrote
    /// string instants. Pre-fix: the rendered <c>created_at</c> comparands are String.
    /// </summary>
    [Fact]
    public void ChannelAndTransportDates_UnderAHostWideStringRepresentation_FilterAndWriteBsonDates()
    {
        var observed = RunIsolated(nameof(HostRegisteredDateProbes.DriverStringRepresentation));

        // The host's registration really is what the (private) registry answers for DateTime…
        Assert.Equal("String", observed[0]);
        // …and the sweep filter's three created_at comparands ($gte since, $gt / $eq cursor) are dates…
        Assert.Equal(["DateTime", "DateTime", "DateTime"], observed[1].Split(','));
        // …and every instant of every channel and transport document is written as a BSON date.
        Assert.Equal(
            ["DateTime", "DateTime", "DateTime", "DateTime", "DateTime", "DateTime", "DateTime", "DateTime", "DateTime"],
            observed[2..]);
    }

    /// <summary>
    /// F-04 (b): under a host's own string-only <c>IBsonSerializer&lt;DateTime&gt;</c> every read of a
    /// <c>$$NOW</c>-stamped document threw <see cref="FormatException"/>: every channel read failed,
    /// and the transport's claim buried every job as unreadable without running it. Pre-fix: the
    /// first deserialization throws ("ReadString can only be called when CurrentBsonType is String,
    /// not when CurrentBsonType is DateTime").
    /// </summary>
    [Fact]
    public void ChannelAndTransportDocuments_UnderAHostOwnStringSerializer_ReadServerStampedDates()
    {
        var observed = RunIsolated(nameof(HostRegisteredDateProbes.CustomStringSerializer));

        Assert.Equal(nameof(HostRegisteredDateProbes.StringDateSerializer), observed[0]);
        // recovery state, channel message, channel subscriber, transport message: each read back
        // its server-stamped instants as UTC.
        Assert.Equal(["True", "True", "True", "True"], observed[1..]);
    }

    /// <summary>
    /// L-10: the channel created its indexes through its bounded-majority handles, so on a set that
    /// cannot acknowledge majority (PSA with its secondary down) even a no-op createIndexes waited the
    /// whole wtimeout and threw (code 64) — a freshly started host could not publish or subscribe
    /// for the whole degradation. Pre-fix: the majority handles' index managers take every call.
    /// </summary>
    [Fact]
    public async Task ChannelStore_IndexDdl_RidesAW1Handle()
    {
        var database = new Mock<IMongoDatabase>(MockBehavior.Loose).WithTestNamespace();
        var (recovery, recoveryMajorityIndexes, recoveryW1Indexes) = Handles<MongoRecoveryStateDocument>();
        var (messages, messageMajorityIndexes, messageW1Indexes) = Handles<MongoChannelMessageDocument>();
        var (subscribers, subscriberMajorityIndexes, subscriberW1Indexes) = Handles<MongoChannelSubscriberDocument>();
        database.Setup(d => d.GetCollection<MongoRecoveryStateDocument>(It.IsAny<string>(), It.IsAny<MongoCollectionSettings>())).Returns(recovery.Object);
        database.Setup(d => d.GetCollection<MongoChannelMessageDocument>(It.IsAny<string>(), It.IsAny<MongoCollectionSettings>())).Returns(messages.Object);
        database.Setup(d => d.GetCollection<MongoChannelSubscriberDocument>(It.IsAny<string>(), It.IsAny<MongoCollectionSettings>())).Returns(subscribers.Object);

        using var store = new MongoDbChannelStore(
            database.Object,
            Options.Create(new MongoDbAsyncResponseChannelOptions { AutoCreateIndexes = true, UseOwnershipLedger = false }));
        await store.EnsureCreatedAsync();

        // TTL + correlation index per collection, all on the w=1 view; none on the majority handle.
        recoveryW1Indexes.Verify(CreateIndexCall<MongoRecoveryStateDocument>(), Times.Exactly(2));
        messageW1Indexes.Verify(CreateIndexCall<MongoChannelMessageDocument>(), Times.Exactly(2));
        subscriberW1Indexes.Verify(CreateIndexCall<MongoChannelSubscriberDocument>(), Times.Exactly(2));
        recoveryMajorityIndexes.Verify(CreateIndexCall<MongoRecoveryStateDocument>(), Times.Never);
        messageMajorityIndexes.Verify(CreateIndexCall<MongoChannelMessageDocument>(), Times.Never);
        subscriberMajorityIndexes.Verify(CreateIndexCall<MongoChannelSubscriberDocument>(), Times.Never);
    }

    /// <summary>
    /// L-10: the flow store created its TTL index through its bounded-majority handle — the same
    /// no-op-createIndexes wait — so a host started during a majority loss could not even read a
    /// ledger. Pre-fix: the majority handle's index manager takes the call.
    /// </summary>
    [Fact]
    public async Task FlowStore_IndexDdl_RidesAW1Handle()
    {
        var database = new Mock<IMongoDatabase>(MockBehavior.Loose).WithTestNamespace();
        var (collection, majorityIndexes, w1Indexes) = Handles<MongoFlowStateDocument>();
        database
            .Setup(d => d.GetCollection<MongoFlowStateDocument>(It.IsAny<string>(), It.IsAny<MongoCollectionSettings>()))
            .Returns(collection.Object);

        using var store = new MongoDbFlowStateStore(
            database.Object,
            Options.Create(new MongoDbDurableFlowOptions { CollectionName = "flows", AutoCreateIndexes = true, UseOwnershipLedger = false }));
        await (Task)typeof(MongoDbFlowStateStore)
            .GetMethod("EnsureCreatedAsync", BindingFlags.Instance | BindingFlags.NonPublic)!
            .Invoke(store, [CancellationToken.None])!;

        w1Indexes.Verify(CreateIndexCall<MongoFlowStateDocument>(), Times.Once);
        majorityIndexes.Verify(CreateIndexCall<MongoFlowStateDocument>(), Times.Never);
    }

    /// <summary>
    /// L-11: the transport's wake stream had a <c>$match</c> only, so every insert event carried the
    /// whole job (payload and headers) to every subscriber process of the queue, although the wake
    /// reads only the event's clusterTime and the document's available_at. Pre-fix: one stage.
    /// </summary>
    [Fact]
    public void TransportWatchPipeline_ProjectsEachEventDownToWhatTheWakeReads()
    {
        var serializer = new ChangeStreamDocumentSerializer<MongoTransportMessageDocument>(BsonSerializer.LookupSerializer<MongoTransportMessageDocument>());
        var rendered = MongoDbTransportStore.BuildQueueWatchPipeline("worker")
            .Render(new RenderArgs<ChangeStreamDocument<MongoTransportMessageDocument>>(serializer, BsonSerializer.SerializerRegistry))
            .Documents;

        Assert.Equal(2, rendered.Count);
        Assert.True(rendered[0].Contains("$match"));
        var projection = rendered[1]["$project"].AsBsonDocument;
        Assert.Equal(1, projection["clusterTime"].ToInt32());
        Assert.Equal(1, projection["fullDocument.available_at"].ToInt32());
        Assert.Equal(1, projection["operationType"].ToInt32());
        Assert.False(projection.Contains("fullDocument"), "the whole document must not be projected");
        Assert.DoesNotContain(projection.Names, name => name.Contains("payload", StringComparison.Ordinal) || name.Contains("headers", StringComparison.Ordinal));
        Assert.False(projection.Contains("_id") && projection["_id"].ToInt32() == 0, "the resume token must survive the projection");

        // What the projection leaves of an event is still enough to tell a delayed insert (no wake)
        // from an immediate one (wake).
        var writtenAt = new BsonTimestamp((int)(Now - DateTime.UnixEpoch).TotalSeconds, 1);
        static BsonDocument Projected(BsonTimestamp clusterTime, DateTime availableAt) => new()
        {
            ["_id"] = new BsonDocument("_data", "resume-token"),
            ["operationType"] = "insert",
            ["clusterTime"] = clusterTime,
            ["fullDocument"] = new BsonDocument("available_at", new BsonDateTime(availableAt))
        };
        Assert.True(MongoDbTransportStore.IsClaimableOnArrival(Projected(writtenAt, Now)));
        Assert.False(MongoDbTransportStore.IsClaimableOnArrival(Projected(writtenAt, Now.AddMinutes(5))));
    }

    private static (Mock<IMongoCollection<T>> Base, Mock<IMongoIndexManager<T>> MajorityIndexes, Mock<IMongoIndexManager<T>> W1Indexes) Handles<T>()
    {
        // A base handle that pins itself for everything but w=1, which answers a separate view: the
        // DDL must land on that view's index manager, not on the handle the stores write through.
        var majorityIndexes = new Mock<IMongoIndexManager<T>>(MockBehavior.Loose);
        majorityIndexes.Setup(m => m.ListAsync(It.IsAny<CancellationToken>())).ReturnsAsync(() => new MongoListCursor<BsonDocument>([]));
        var w1Indexes = new Mock<IMongoIndexManager<T>>(MockBehavior.Loose);
        w1Indexes.Setup(m => m.ListAsync(It.IsAny<CancellationToken>())).ReturnsAsync(() => new MongoListCursor<BsonDocument>([]));
        var w1 = new Mock<IMongoCollection<T>>(MockBehavior.Loose).SelfPinning();
        w1.SetupGet(c => c.Indexes).Returns(w1Indexes.Object);
        var collection = new Mock<IMongoCollection<T>>(MockBehavior.Loose).SelfPinning();
        collection.Setup(c => c.WithWriteConcern(It.Is<WriteConcern>(concern => concern.W == WriteConcern.W1.W))).Returns(w1.Object);
        collection.SetupGet(c => c.Indexes).Returns(majorityIndexes.Object);
        return (collection, majorityIndexes, w1Indexes);
    }

    private static System.Linq.Expressions.Expression<Func<IMongoIndexManager<T>, Task<string>>> CreateIndexCall<T>()
        => indexes => indexes.CreateOneAsync(It.IsAny<CreateIndexModel<T>>(), It.IsAny<CreateOneIndexOptions>(), It.IsAny<CancellationToken>());

    /// <summary>
    /// Runs one <see cref="HostRegisteredDateProbes"/> method against PRIVATE copies of the driver and
    /// the channel/transport packages (their process-wide serializer registry and class maps
    /// included), so the host's registration cannot leak into the rest of the run.
    /// </summary>
    private static string[] RunIsolated(string probeName)
    {
        var context = new Round65IsolatedBsonLoadContext();
        try
        {
            var probe = context.LoadFromAssemblyPath(typeof(Round65MongoTests).Assembly.Location)
                .GetType(typeof(HostRegisteredDateProbes).FullName!, throwOnError: true)!
                .GetMethod(probeName, BindingFlags.Static | BindingFlags.NonPublic | BindingFlags.Public)!;
            return (string[])probe.Invoke(null, BindingFlags.DoNotWrapExceptions, binder: null, parameters: null, culture: null)!;
        }
        finally
        {
            context.Unload();
            // The registration stayed inside the isolated context.
            Assert.IsType<DateTimeSerializer>(BsonSerializer.LookupSerializer<DateTime>());
        }
    }
}

/// <summary>
/// Private copies of the MongoDB driver and the Mongo channel and transport packages; the runtime
/// and everything else is shared with the default context (see <c>IsolatedBsonLoadContext</c>,
/// the flow store's twin).
/// </summary>
internal sealed class Round65IsolatedBsonLoadContext() : System.Runtime.Loader.AssemblyLoadContext($"asyncresponse-r65-bson-{Guid.NewGuid():N}", isCollectible: true)
{
    private static readonly string Directory = Path.GetDirectoryName(typeof(Round65IsolatedBsonLoadContext).Assembly.Location)!;
    private static readonly string ChannelAssembly = typeof(MongoDbChannelStore).Assembly.GetName().Name!;
    private static readonly string TransportAssembly = typeof(MongoDbTransportStore).Assembly.GetName().Name!;

    protected override Assembly? Load(AssemblyName assemblyName)
    {
        var name = assemblyName.Name!;
        if (!name.StartsWith("MongoDB.", StringComparison.Ordinal) && name != ChannelAssembly && name != TransportAssembly)
            return null;

        var path = Path.Combine(Directory, name + ".dll");
        return File.Exists(path) ? LoadFromAssemblyPath(path) : null;
    }
}

/// <summary>
/// Runs inside <see cref="Round65IsolatedBsonLoadContext"/>: registers a host-wide DateTime
/// serializer, then renders the channel's sweep filter and (de)serializes every channel and
/// transport document. Returns plain strings so nothing crosses the context boundary as a driver
/// type.
/// </summary>
internal static class HostRegisteredDateProbes
{
    private static readonly DateTime Now = new(2031, 3, 14, 9, 26, 53, 589, DateTimeKind.Utc);

    public static string[] DriverStringRepresentation()
    {
        BsonSerializer.RegisterSerializer(typeof(DateTime), new DateTimeSerializer(BsonType.String));

        var filter = MongoDbChannelStore.BuildLoadMessagesFilter("c", Now, Now, Guid.NewGuid())
            .Render(new RenderArgs<MongoChannelMessageDocument>(BsonSerializer.LookupSerializer<MongoChannelMessageDocument>(), BsonSerializer.SerializerRegistry));
        var comparands = new List<string>();
        CollectComparands(filter, "created_at", comparands);

        var recovery = new MongoRecoveryStateDocument { Id = "r", ExpiresAtUtc = Now, RegisteredAtUtc = Now }.ToBsonDocument();
        var message = new MongoChannelMessageDocument { Id = Guid.NewGuid(), CreatedAtUtc = Now, ExpiresAtUtc = Now, AckedAtUtc = Now }.ToBsonDocument();
        var subscriber = new MongoChannelSubscriberDocument { Id = "s", ExpiresAtUtc = Now }.ToBsonDocument();
        var job = new MongoTransportMessageDocument { Id = Guid.NewGuid(), CreatedAtUtc = Now, AvailableAtUtc = Now, LockedUntilUtc = Now }.ToBsonDocument();

        return
        [
            ((DateTimeSerializer)BsonSerializer.LookupSerializer<DateTime>()).Representation.ToString(),
            string.Join(",", comparands),
            recovery["expires_at"].BsonType.ToString(),
            recovery["registered_at"].BsonType.ToString(),
            message["created_at"].BsonType.ToString(),
            message["expires_at"].BsonType.ToString(),
            message["acked_at"].BsonType.ToString(),
            subscriber["expires_at"].BsonType.ToString(),
            job["created_at"].BsonType.ToString(),
            job["available_at"].BsonType.ToString(),
            job["locked_until"].BsonType.ToString()
        ];
    }

    public static string[] CustomStringSerializer()
    {
        BsonSerializer.RegisterSerializer(typeof(DateTime), new StringDateSerializer());
        var stamped = new BsonDateTime(Now);
        var later = new BsonDateTime(Now.AddSeconds(30));

        // Each document exactly as the server's $$NOW pipelines leave it.
        var recovery = BsonSerializer.Deserialize<MongoRecoveryStateDocument>(new BsonDocument
        {
            ["_id"] = "r", ["correlation_id"] = "c", ["registration_id"] = new BsonBinaryData(Guid.NewGuid(), GuidRepresentation.Standard),
            ["state_json"] = "{}", ["expires_at"] = later, ["registered_at"] = stamped
        });
        var message = BsonSerializer.Deserialize<MongoChannelMessageDocument>(new BsonDocument
        {
            ["_id"] = new BsonBinaryData(Guid.NewGuid(), GuidRepresentation.Standard), ["correlation_id"] = "c", ["envelope_json"] = "{}",
            ["created_at"] = stamped, ["expires_at"] = later, ["acked_at"] = stamped, ["recovery_claimed"] = false
        });
        var subscriber = BsonSerializer.Deserialize<MongoChannelSubscriberDocument>(new BsonDocument
        {
            ["_id"] = "s", ["correlation_id"] = "c", ["registration_id"] = new BsonBinaryData(Guid.NewGuid(), GuidRepresentation.Standard),
            ["instance_id"] = "i", ["expires_at"] = later
        });
        var job = BsonSerializer.Deserialize<MongoTransportMessageDocument>(new BsonDocument
        {
            ["_id"] = new BsonBinaryData(Guid.NewGuid(), GuidRepresentation.Standard), ["queue"] = "worker", ["payload"] = "{}",
            ["headers"] = new BsonArray(), ["created_at"] = stamped, ["available_at"] = stamped, ["attempts"] = 1,
            ["dead_letter_reason"] = BsonNull.Value, ["locked_until"] = later,
            ["lock_id"] = new BsonBinaryData(Guid.NewGuid(), GuidRepresentation.Standard)
        });

        return
        [
            BsonSerializer.LookupSerializer<DateTime>().GetType().Name,
            (IsUtc(recovery.RegisteredAtUtc, Now) && IsUtc(recovery.ExpiresAtUtc, Now.AddSeconds(30))).ToString(),
            (IsUtc(message.CreatedAtUtc, Now) && IsUtc(message.ExpiresAtUtc, Now.AddSeconds(30)) && message.AckedAtUtc is { } acked && IsUtc(acked, Now)).ToString(),
            IsUtc(subscriber.ExpiresAtUtc, Now.AddSeconds(30)).ToString(),
            (IsUtc(job.CreatedAtUtc, Now) && IsUtc(job.AvailableAtUtc, Now) && job.LockedUntilUtc is { } locked && IsUtc(locked, Now.AddSeconds(30))).ToString()
        ];
    }

    private static bool IsUtc(DateTime value, DateTime expected) => value == expected && value.Kind == DateTimeKind.Utc;

    /// <summary>Every value compared against <paramref name="field"/> anywhere in the filter, by BSON type.</summary>
    private static void CollectComparands(BsonValue node, string field, List<string> comparands)
    {
        if (node is BsonArray array)
        {
            foreach (var item in array)
                CollectComparands(item, field, comparands);
            return;
        }

        if (node is not BsonDocument document)
            return;

        foreach (var element in document)
        {
            if (element.Name == field)
            {
                // { field: value } (an equality) or { field: { $op: value, … } }.
                if (element.Value is BsonDocument operators && operators.ElementCount > 0 && operators.GetElement(0).Name.StartsWith('$'))
                {
                    foreach (var comparison in operators)
                        comparands.Add(comparison.Value.BsonType.ToString());
                }
                else
                {
                    comparands.Add(element.Value.BsonType.ToString());
                }
            }
            else
            {
                CollectComparands(element.Value, field, comparands);
            }
        }
    }

    /// <summary>A host's own DateTime serializer that reads and writes ISO-8601 strings.</summary>
    internal sealed class StringDateSerializer : SerializerBase<DateTime>
    {
        public override DateTime Deserialize(BsonDeserializationContext context, BsonDeserializationArgs args)
            => DateTime.Parse(context.Reader.ReadString(), CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);

        public override void Serialize(BsonSerializationContext context, BsonSerializationArgs args, DateTime value)
            => context.Writer.WriteString(value.ToUniversalTime().ToString("O", CultureInfo.InvariantCulture));
    }
}
