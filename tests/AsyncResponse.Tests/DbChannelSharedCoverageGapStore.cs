using System.Buffers.Binary;
using System.Data.SqlTypes;
using PgServer = AsyncResponse.Tests.DbChannelCoverageGapPostgresServer;
using SqlServer = AsyncResponse.Tests.DbChannelCoverageGapSqlServer;

namespace AsyncResponse.Tests;

public sealed partial class DbChannelSharedCoverageTests
{
    /// <summary>
    /// A relational channel store (PostgreSQL or SQL Server) backed by a scripted wire server, so
    /// the shared base's store calls succeed, return chosen rows, or fail with a chosen fault
    /// without a container. The statements the channel store sends are recognised by their text
    /// and routed to the handlers below; everything else succeeds with one row affected.
    /// </summary>
    private sealed class ScriptedRelationalStore : IAsyncDisposable
    {
        private static readonly DateTime PostgresEpoch = new(2000, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        private readonly PgServer? _postgres;
        private readonly SqlServer? _sqlServer;
        private readonly object _gate = new();
        private readonly List<string> _operations = [];

        public ScriptedRelationalStore(Provider provider)
        {
            Provider = provider;
            if (provider == Provider.PostgreSql)
            {
                _postgres = new PgServer();
                _postgres.Respond = statement => RespondPostgresAsync(statement);
            }
            else if (provider == Provider.SqlServer)
            {
                _sqlServer = new SqlServer();
                _sqlServer.Respond = statement => RespondSqlServerAsync(statement);
            }
            else
            {
                throw new ArgumentOutOfRangeException(nameof(provider), "Relational providers only.");
            }
        }

        public Provider Provider { get; }

        /// <summary>The rows the message table holds (only <see cref="Load"/>'s default reads it).</summary>
        public List<Row> Rows { get; } = [];

        /// <summary>A forward page read (<c>LoadMessagesAsync</c>). Default: <see cref="Rows"/> under the query's filter and order.</summary>
        public Func<LoadRequest, Task<IReadOnlyList<Row>>>? Load { get; set; }

        /// <summary>The hydration read (<c>LoadMessagesByIdAsync</c>). Default: those ids from <see cref="Rows"/>.</summary>
        public Func<string, IReadOnlyList<Guid>, Task<IReadOnlyList<Row>>>? LoadById { get; set; }

        public Func<Guid, Task<bool>> ClaimForDelivery { get; set; } = _ => Task.FromResult(true);

        public Func<Guid, Task<bool>> ClaimForRecovery { get; set; } = _ => Task.FromResult(true);

        /// <summary>The idempotent insert: the stamped <c>created_at</c>, or <c>null</c> for a duplicate.</summary>
        public Func<Guid, string, Task<DateTimeOffset?>> Insert { get; set; } = (_, _) => Task.FromResult<DateTimeOffset?>(Now());

        /// <summary>The duplicate's lookup: the original row's settlement columns, or <c>null</c> when it is gone.</summary>
        public Func<Guid, Task<Row?>> Lookup { get; set; } = _ => Task.FromResult<Row?>(null);

        public Func<Task<(DateTimeOffset At, long Seq)>> SubscriptionStart { get; set; } = () => Task.FromResult((Now(), 1L));

        public Func<string, Task<long>> Count { get; set; } = _ => Task.FromResult(0L);

        public Func<Guid, Task<bool?>> IsAcknowledged { get; set; } = _ => Task.FromResult<bool?>(false);

        /// <summary>The recovery-state scan and per-id loads: the stored JSON documents.</summary>
        public Func<Task<IReadOnlyList<string>>> RecoveryStates { get; set; } = () => Task.FromResult<IReadOnlyList<string>>([]);

        /// <summary>Subscriber writes: the upsert, the heartbeat round and the delete. Default: succeed.</summary>
        public Func<string, Task> SubscriberWrite { get; set; } = _ => Task.CompletedTask;

        /// <summary>Operations run so far, by name (<c>load</c>, <c>claim</c>, <c>insert</c>, <c>heartbeat</c>, <c>delete-subscriber</c>, ...).</summary>
        public IReadOnlyList<string> Operations
        {
            get { lock (_gate) return _operations.ToArray(); }
        }

        public int Ran(string operation) => Operations.Count(recorded => recorded == operation);

        public string ConnectionString => _postgres?.ConnectionString() ?? _sqlServer!.ConnectionString();

        /// <summary>A harness whose channel runs on this store (its schema check skipped).</summary>
        public Harness CreateHarness(TimeSpan pollInterval, TimeSpan? fullSweepInterval = null, int? pendingMessageBatchSize = null, TimeProvider? timeProvider = null)
        {
            var harness = DbChannelSharedCoverageTests.Harness.Create(
                Provider,
                failing: false,
                pollInterval,
                fullSweepInterval,
                pendingMessageBatchSize: pendingMessageBatchSize,
                timeProvider: timeProvider,
                postgreSqlConnectionString: Provider == Provider.PostgreSql ? ConnectionString : null,
                sqlServerConnectionString: Provider == Provider.SqlServer ? ConnectionString : null);
            harness.MarkStoreCreated();
            return harness;
        }

        public static DateTimeOffset Now()
        {
            // Millisecond precision survives both wire formats (microseconds, 100 ns) unchanged.
            var now = DateTimeOffset.UtcNow;
            return new DateTimeOffset(now.Ticks - now.Ticks % TimeSpan.TicksPerMillisecond, TimeSpan.Zero);
        }

        public sealed record Row(Guid Id, string CorrelationId, string? EnvelopeJson, DateTimeOffset CreatedAtUtc, DateTimeOffset? AckedAtUtc = null, long? AckedSeq = null);

        public sealed record LoadRequest(string CorrelationId, DateTimeOffset Since, DateTimeOffset? AfterCreatedAtUtc, Guid? AfterId, int Limit);

        /// <summary>Thrown by a handler to answer with a store fault: transient (deadlock / serialization failure) or not.</summary>
        public sealed class Fault(bool transient, string message = "scripted store fault", int? sqlServerNumber = null) : Exception(message)
        {
            public bool Transient { get; } = transient;

            /// <summary>The SQL Server error number to answer with, instead of the transient/non-transient default.</summary>
            public int? SqlServerNumber { get; } = sqlServerNumber;
        }

        private void Record(string operation)
        {
            lock (_gate)
                _operations.Add(operation);
        }

        private IEnumerable<Row> Ordered(IEnumerable<Row> rows)
            => Provider == Provider.PostgreSql
                ? rows.OrderBy(row => row.CreatedAtUtc).ThenBy(row => row.Id.ToString(), StringComparer.Ordinal)
                : rows.OrderBy(row => row.CreatedAtUtc).ThenBy(row => new SqlGuid(row.Id));

        private int CompareIds(Guid left, Guid right)
            => Provider == Provider.PostgreSql
                ? string.CompareOrdinal(left.ToString(), right.ToString())
                : new SqlGuid(left).CompareTo(new SqlGuid(right));

        private Task<IReadOnlyList<Row>> DefaultLoad(LoadRequest request)
        {
            List<Row> snapshot;
            lock (_gate)
                snapshot = [.. Rows];
            IReadOnlyList<Row> page = Ordered(snapshot.Where(row =>
                    row.CorrelationId == request.CorrelationId
                    && row.CreatedAtUtc >= request.Since
                    && (request.AfterCreatedAtUtc is not { } after
                        || row.CreatedAtUtc > after
                        || (row.CreatedAtUtc == after && CompareIds(row.Id, request.AfterId!.Value) > 0))))
                .Take(request.Limit)
                .Select(row => row.AckedAtUtc is null ? row : row with { EnvelopeJson = null })
                .ToList();
            return Task.FromResult(page);
        }

        private Task<IReadOnlyList<Row>> DefaultLoadById(string correlationId, IReadOnlyList<Guid> ids)
        {
            List<Row> snapshot;
            lock (_gate)
                snapshot = [.. Rows];
            IReadOnlyList<Row> found = Ordered(snapshot.Where(row => row.CorrelationId == correlationId && ids.Contains(row.Id))).ToList();
            return Task.FromResult(found);
        }

        // ---------------------------------------------------------------------------------
        // PostgreSQL
        // ---------------------------------------------------------------------------------

        private static readonly (string, int)[] PgMessageColumns =
        [
            ("id", PgServer.Uuid), ("correlation_id", PgServer.Text), ("envelope_json", PgServer.Text),
            ("created_at", PgServer.TimestampTz), ("acked_at", PgServer.TimestampTz), ("acked_seq", PgServer.Int8)
        ];

        private async Task<PgServer.Reply> RespondPostgresAsync(PgServer.Statement statement)
        {
            try
            {
                return await RespondPostgresCoreAsync(statement).ConfigureAwait(false);
            }
            catch (Fault fault)
            {
                return PgServer.Reply.Error(fault.Transient ? "40001" : "22P02", fault.Message);
            }
        }

        private async Task<PgServer.Reply> RespondPostgresCoreAsync(PgServer.Statement statement)
        {
            if (statement.Contains("SELECT now(), nextval"))
            {
                Record("subscription-start");
                var (at, seq) = await SubscriptionStart().ConfigureAwait(false);
                return PgServer.Reply.Rows([("now", PgServer.TimestampTz), ("nextval", PgServer.Int8)], [at, seq]);
            }

            if (statement.Contains("CASE WHEN acked_at IS NULL THEN envelope_json::text END"))
            {
                Record("load");
                var withCursor = statement.Parameters.Count == 5;
                var request = new LoadRequest(
                    statement.Text(0)!,
                    PgTimestamp(statement.Parameters[1]!),
                    withCursor ? PgTimestamp(statement.Parameters[2]!) : null,
                    withCursor ? statement.Uuid(3) : null,
                    BinaryPrimitives.ReadInt32BigEndian(statement.Parameters[withCursor ? 4 : 2]));
                return PgRows(await (Load ?? DefaultLoad)(request).ConfigureAwait(false));
            }

            if (statement.Contains("id = ANY("))
            {
                Record("load-by-id");
                return PgRows(await (LoadById ?? DefaultLoadById)(statement.Text(0)!, PgUuidArray(statement.Parameters[1]!)).ConfigureAwait(false));
            }

            if (statement.Contains("WITH inserted AS"))
            {
                Record("insert");
                var createdAt = await Insert(statement.Uuid(0), statement.Text(1)!).ConfigureAwait(false);
                return PgServer.Reply.Rows([("created_at", PgServer.TimestampTz), ("pg_notify", PgServer.Text)], [createdAt, null]);
            }

            if (statement.Contains("SELECT created_at, acked_at, acked_seq"))
            {
                Record("lookup");
                var row = await Lookup(statement.Uuid(0)).ConfigureAwait(false);
                return row is null
                    ? PgServer.Reply.Rows([("created_at", PgServer.TimestampTz), ("acked_at", PgServer.TimestampTz), ("acked_seq", PgServer.Int8)])
                    : PgServer.Reply.Rows([("created_at", PgServer.TimestampTz), ("acked_at", PgServer.TimestampTz), ("acked_seq", PgServer.Int8)], [row.CreatedAtUtc, row.AckedAtUtc, row.AckedSeq]);
            }

            if (statement.Contains("SET acked_at = COALESCE"))
            {
                Record("claim");
                return PgIdResult(await ClaimForDelivery(statement.Uuid(0)).ConfigureAwait(false), statement.Uuid(0));
            }

            if (statement.Contains("SET recovery_claimed = true"))
            {
                Record("recovery-claim");
                return PgIdResult(await ClaimForRecovery(statement.Uuid(0)).ConfigureAwait(false), statement.Uuid(0));
            }

            if (statement.Contains("SELECT acked_at IS NOT NULL"))
            {
                Record("is-acknowledged");
                var acknowledged = await IsAcknowledged(statement.Uuid(0)).ConfigureAwait(false);
                return acknowledged is { } value
                    ? PgServer.Reply.Rows([("acked", PgServer.Bool)], [value])
                    : PgServer.Reply.Rows([("acked", PgServer.Bool)]);
            }

            if (statement.Contains("count(*)"))
            {
                Record("count");
                return PgServer.Reply.Rows([("count", PgServer.Int8)], [await Count(statement.Text(0)!).ConfigureAwait(false)]);
            }

            if (statement.Contains("SELECT state_json::text"))
            {
                Record("recovery-read");
                var states = await RecoveryStates().ConfigureAwait(false);
                return PgServer.Reply.Rows([("state_json", PgServer.Text)], [.. states.Select(state => new object?[] { state })]);
            }

            if (statement.Contains("unnest("))
                return await SubscriberAsync("heartbeat", statement.Sql).ConfigureAwait(false);
            if (statement.Contains("DELETE FROM") && statement.Contains("asyncresponse_channel_subscribers") && statement.Contains("registration_id ="))
                return await SubscriberAsync("delete-subscriber", statement.Sql).ConfigureAwait(false);
            if (statement.Contains("INSERT INTO") && statement.Contains("instance_id"))
                return await SubscriberAsync("upsert-subscriber", statement.Sql).ConfigureAwait(false);

            Record("other");
            return PgServer.Reply.Command(statement.Sql);
        }

        private async Task<PgServer.Reply> SubscriberAsync(string operation, string sql)
        {
            Record(operation);
            await SubscriberWrite(operation).ConfigureAwait(false);
            return PgServer.Reply.Command(sql);
        }

        private static PgServer.Reply PgIdResult(bool won, Guid id)
            => won ? PgServer.Reply.Rows([("id", PgServer.Uuid)], [id]) : PgServer.Reply.Rows([("id", PgServer.Uuid)]);

        private static PgServer.Reply PgRows(IReadOnlyList<Row> rows)
            => PgServer.Reply.Rows(PgMessageColumns, [.. rows.Select(row => new object?[] { row.Id, row.CorrelationId, row.EnvelopeJson, row.CreatedAtUtc, row.AckedAtUtc, row.AckedSeq })]);

        private static DateTimeOffset PgTimestamp(byte[] value)
            => new(PostgresEpoch.AddTicks(BinaryPrimitives.ReadInt64BigEndian(value) * 10), TimeSpan.Zero);

        private static List<Guid> PgUuidArray(byte[] value)
        {
            var dimensions = BinaryPrimitives.ReadInt32BigEndian(value);
            var ids = new List<Guid>();
            if (dimensions == 0)
                return ids;
            var count = BinaryPrimitives.ReadInt32BigEndian(value.AsSpan(12));
            var offset = 12 + 8 * dimensions;
            for (var i = 0; i < count; i++)
            {
                var length = BinaryPrimitives.ReadInt32BigEndian(value.AsSpan(offset));
                offset += 4;
                ids.Add(new Guid(value.AsSpan(offset, length), bigEndian: true));
                offset += length;
            }

            return ids;
        }

        // ---------------------------------------------------------------------------------
        // SQL Server
        // ---------------------------------------------------------------------------------

        private static readonly (string, SqlServer.Type)[] SqlMessageColumns =
        [
            ("id", SqlServer.Type.Guid), ("correlation_id", SqlServer.Type.NVarChar), ("envelope_json", SqlServer.Type.NVarCharMax),
            ("created_at", SqlServer.Type.DateTime2), ("acked_at", SqlServer.Type.DateTime2), ("acked_seq", SqlServer.Type.BigInt)
        ];

        private async Task<SqlServer.Reply> RespondSqlServerAsync(SqlServer.Statement statement)
        {
            try
            {
                return await RespondSqlServerCoreAsync(statement).ConfigureAwait(false);
            }
            catch (Fault fault)
            {
                return SqlServer.Reply.Error(fault.SqlServerNumber ?? (fault.Transient ? 1205 : 50000), fault.Message, fault.Transient ? (byte)13 : (byte)16);
            }
        }

        private async Task<SqlServer.Reply> RespondSqlServerCoreAsync(SqlServer.Statement statement)
        {
            if (statement.Contains("SELECT SYSUTCDATETIME(), NEXT VALUE FOR"))
            {
                Record("subscription-start");
                var (at, seq) = await SubscriptionStart().ConfigureAwait(false);
                return SqlServer.Reply.Rows([("now", SqlServer.Type.DateTime2), ("seq", SqlServer.Type.BigInt)], [at, seq]);
            }

            if (statement.Contains("CASE WHEN acked_at IS NULL THEN envelope_json END"))
            {
                Record("load");
                var request = new LoadRequest(
                    (string)statement["correlation_id"]!,
                    new DateTimeOffset((DateTime)statement["since"]!, TimeSpan.Zero),
                    statement["after_created_at"] is DateTime after ? new DateTimeOffset(after, TimeSpan.Zero) : null,
                    statement["after_id"] as Guid?,
                    Convert.ToInt32(statement["limit"], System.Globalization.CultureInfo.InvariantCulture));
                return SqlRows(await (Load ?? DefaultLoad)(request).ConfigureAwait(false));
            }

            if (statement.Contains("AND id IN ("))
            {
                Record("load-by-id");
                var ids = statement.Parameters.Where(parameter => parameter.Key.StartsWith("id", StringComparison.Ordinal)).Select(parameter => (Guid)parameter.Value!).ToList();
                return SqlRows(await (LoadById ?? DefaultLoadById)((string)statement["correlation_id"]!, ids).ConfigureAwait(false));
            }

            if (statement.Contains("OUTPUT inserted.created_at"))
            {
                Record("insert");
                var createdAt = await Insert((Guid)statement["id"]!, (string)statement["correlation_id"]!).ConfigureAwait(false);
                return createdAt is { } stamped
                    ? SqlServer.Reply.Rows([("created_at", SqlServer.Type.DateTime2)], [stamped])
                    : SqlServer.Reply.Rows([("created_at", SqlServer.Type.DateTime2)]);
            }

            if (statement.Contains("SELECT created_at, acked_at, acked_seq"))
            {
                Record("lookup");
                var row = await Lookup((Guid)statement["id"]!).ConfigureAwait(false);
                (string, SqlServer.Type)[] columns = [("created_at", SqlServer.Type.DateTime2), ("acked_at", SqlServer.Type.DateTime2), ("acked_seq", SqlServer.Type.BigInt)];
                return row is null
                    ? SqlServer.Reply.Rows(columns)
                    : SqlServer.Reply.Rows(columns, [row.CreatedAtUtc, row.AckedAtUtc, row.AckedSeq]);
            }

            if (statement.Contains("SET acked_at = COALESCE"))
            {
                Record("claim");
                return SqlIdResult(await ClaimForDelivery((Guid)statement["id"]!).ConfigureAwait(false), (Guid)statement["id"]!);
            }

            if (statement.Contains("SET recovery_claimed = 1"))
            {
                Record("recovery-claim");
                return SqlIdResult(await ClaimForRecovery((Guid)statement["id"]!).ConfigureAwait(false), (Guid)statement["id"]!);
            }

            if (statement.Contains("CASE WHEN acked_at IS NOT NULL THEN 1 ELSE 0 END AS bit"))
            {
                Record("is-acknowledged");
                var acknowledged = await IsAcknowledged((Guid)statement["id"]!).ConfigureAwait(false);
                return acknowledged is { } value
                    ? SqlServer.Reply.Rows([("acked", SqlServer.Type.Bit)], [value])
                    : SqlServer.Reply.Rows([("acked", SqlServer.Type.Bit)]);
            }

            if (statement.Contains("COUNT_BIG(*)"))
            {
                Record("count");
                return SqlServer.Reply.Rows([("count", SqlServer.Type.BigInt)], [await Count((string)statement["correlation_id"]!).ConfigureAwait(false)]);
            }

            if (statement.Contains("SELECT state_json"))
            {
                Record("recovery-read");
                var states = await RecoveryStates().ConfigureAwait(false);
                return SqlServer.Reply.Rows([("state_json", SqlServer.Type.NVarCharMax)], [.. states.Select(state => new object?[] { state })]);
            }

            if (statement.Contains("USING (VALUES"))
                return await SqlSubscriberAsync("heartbeat").ConfigureAwait(false);
            if (statement.Contains("DELETE FROM") && statement.Contains("asyncresponse_channel_subscribers") && statement.Contains("registration_id ="))
                return await SqlSubscriberAsync("delete-subscriber").ConfigureAwait(false);
            if (statement.Contains("MERGE") && statement.Contains("instance_id"))
                return await SqlSubscriberAsync("upsert-subscriber").ConfigureAwait(false);

            Record("other");
            return SqlServer.Reply.Affected(1);
        }

        private async Task<SqlServer.Reply> SqlSubscriberAsync(string operation)
        {
            Record(operation);
            await SubscriberWrite(operation).ConfigureAwait(false);
            return SqlServer.Reply.Affected(1);
        }

        private static SqlServer.Reply SqlIdResult(bool won, Guid id)
            => won ? SqlServer.Reply.Rows([("id", SqlServer.Type.Guid)], [id]) : SqlServer.Reply.Rows([("id", SqlServer.Type.Guid)]);

        private static SqlServer.Reply SqlRows(IReadOnlyList<Row> rows)
            => SqlServer.Reply.Rows(SqlMessageColumns, [.. rows.Select(row => new object?[] { row.Id, row.CorrelationId, row.EnvelopeJson, row.CreatedAtUtc, row.AckedAtUtc, row.AckedSeq })]);

        public async ValueTask DisposeAsync()
        {
            if (_postgres is not null)
                await _postgres.DisposeAsync();
            if (_sqlServer is not null)
                await _sqlServer.DisposeAsync();
        }
    }
}
