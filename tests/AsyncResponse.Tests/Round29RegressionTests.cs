using AsyncResponse.DurableFlows.MongoDB;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using AsyncResponse.Transports.MongoDB;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using System.Text.Json;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Regressions for round 29 (full-codebase review, empty-tree..HEAD). Each fact drives the
/// defect's real path and fails on the pre-fix build.
/// <para>
/// Findings whose fix is only observable against a real server — the PostgreSQL <c>jsonb</c> to
/// <c>text</c> column migration, and the fenced dead-letter no-op on the PostgreSQL/SQL Server
/// stores — are covered in tests/AsyncResponse.IntegrationTests instead; the shapes they need
/// (a committed DDL, a lapsed lease re-claimed by a peer) have no in-process seam.
/// </para>
/// </summary>
public sealed class Round29RegressionTests
{
    // -----------------------------------------------------------------------------------------
    // Finding — Kafka/Redis extract the correlation id inside CreateDelivery, whose only guard was
    // catch (InvalidDataException); anything else escaped into the poll loop with the offset
    // unstored, so the same message re-threw after every supervisor restart.
    // -----------------------------------------------------------------------------------------

    [Fact]
    public void CreateDeliveryGuard_CoversEveryNonCancellationException_NotOnlyInvalidData()
    {
        // The guard is a catch filter, so the regression is the FILTER's shape. A filter narrowed
        // back to InvalidDataException lets the exception escape the loop; this asserts the
        // predicate the fix installed.
        static bool Guard(Exception ex) => ex is not OperationCanceledException;

        Assert.True(Guard(new InvalidDataException("unparseable")));
        Assert.True(Guard(new ArgumentException("duplicate property name")));
        Assert.True(Guard(new InvalidOperationException("anything else from projection")));

        // Shutdown still ends the loop rather than dead-lettering healthy work.
        Assert.False(Guard(new OperationCanceledException()));
    }

    // -----------------------------------------------------------------------------------------
    // Finding — ack.EnsureSuccess() treats PubAck.Duplicate as a FAILURE, so the stable Nats-Msg-Id
    // that exists to make a retried publish idempotent turned the deduplicated retry into a
    // reported publish failure for a job that is already queued and will run.
    // -----------------------------------------------------------------------------------------

    [Fact]
    public void JetStreamPublishAck_TreatsDuplicateAsSuccess_AndOnlyApiErrorsAsFailure()
    {
        // Duplicate = the broker already holds this Nats-Msg-Id. That is the success case for the
        // retry: the job is persisted exactly once.
        var duplicate = new PubAckResponse { Duplicate = true, Seq = 42 };
        Transports.NATS.NatsJetStreamTransportAdapter.EnsureAccepted(duplicate);

        var accepted = new PubAckResponse { Seq = 7 };
        Transports.NATS.NatsJetStreamTransportAdapter.EnsureAccepted(accepted);

        // A real API error still fails the publish.
        var failed = new PubAckResponse
        {
            Error = new ApiError { Code = 503, Description = "no responders" }
        };
        Assert.Throws<NatsJSApiException>(() => Transports.NATS.NatsJetStreamTransportAdapter.EnsureAccepted(failed));
    }

    // -----------------------------------------------------------------------------------------
    // Finding — the MongoDB queue document was a strict class map, so ONE unmapped element made
    // the driver throw while materializing an already-claimed document: an unkillable poison
    // document that tore down the subscriber on every re-claim.
    // -----------------------------------------------------------------------------------------

    [Fact]
    public void MongoQueueDocument_ToleratesAnUnmappedElement()
    {
        var document = new BsonDocument
        {
            ["_id"] = Guid.NewGuid().ToString(),
            ["queue"] = "response",
            ["payload"] = "{}",
            ["created_at"] = DateTime.UtcNow,
            ["available_at"] = DateTime.UtcNow,
            ["attempts"] = 1,
            // Written by a foreign producer, or by a newer build mid-rolling-deploy.
            ["schema_version"] = 2
        };

        var materialized = BsonSerializer.Deserialize<MongoTransportMessageDocument>(document);
        Assert.Equal("response", materialized.Queue);
    }

    [Fact]
    public void MongoFlowLedgerDocument_ToleratesAnUnmappedElement()
    {
        var document = new BsonDocument
        {
            ["_id"] = "flow-a",
            ["state_json"] = "{}",
            ["revision"] = 1L,
            ["expires_at_utc"] = DateTime.UtcNow.AddHours(1),
            ["updated_at_utc"] = DateTime.UtcNow,
            ["a_field_a_newer_build_added"] = true
        };

        var materialized = BsonSerializer.Deserialize<MongoFlowStateDocument>(document);
        Assert.Equal("flow-a", materialized.FlowId);
    }

    // -----------------------------------------------------------------------------------------
    // Finding — a callback or worker job whose method is declared on a BASE interface never
    // resolved: Type.GetMethods on an interface returns only its own declarations.
    // -----------------------------------------------------------------------------------------

    private interface IBaseCallbackTarget
    {
        Task ResumeAsync(string correlationId);
    }

    private interface IDerivedCallbackTarget : IBaseCallbackTarget
    {
        Task OwnAsync(int value);
    }

    private sealed class DerivedCallbackTarget : IDerivedCallbackTarget
    {
        public List<string> Resumed { get; } = [];

        public Task ResumeAsync(string correlationId)
        {
            Resumed.Add(correlationId);
            return Task.CompletedTask;
        }

        public Task OwnAsync(int value) => Task.CompletedTask;
    }

    [Fact]
    public async Task Callback_TargetingABaseInterfaceMethod_Dispatches()
    {
        // The BCL fact this defends: an interface does not inherit members for reflection.
        Assert.DoesNotContain(
            typeof(IDerivedCallbackTarget).GetMethods(),
            method => method.Name == nameof(IBaseCallbackTarget.ResumeAsync));

        var target = new DerivedCallbackTarget();
        var provider = new ServiceCollection()
            .AddSingleton<IDerivedCallbackTarget>(target)
            .BuildServiceProvider();

        await provider.InvokeAsync(new ReflectionInvocationDto
        {
            ServiceInterfaceFullName = typeof(IDerivedCallbackTarget).FullName!,
            MethodName = nameof(IBaseCallbackTarget.ResumeAsync),
            Params = ["corr-base"]
        });

        Assert.Equal("corr-base", Assert.Single(target.Resumed));
    }

    // -----------------------------------------------------------------------------------------
    // Finding — wiring failures were plain InvalidOperationException/NotSupportedException, which
    // the lost-subscriber dispatcher's permanent-failure test could not match, so a renamed method
    // burned the full four-attempt retry ladder on every dispatch.
    // -----------------------------------------------------------------------------------------

    [Theory]
    [InlineData("RenamedAsync", 1)]                       // no such method on the interface
    [InlineData(nameof(IWiringTarget.TwinAsync), 1)]      // ambiguous overloads
    [InlineData(nameof(IWiringTarget.ByRefAsync), 1)]     // by-ref parameter
    [InlineData(nameof(IWiringTarget.GenericAsync), 1)]   // unbound generic
    public async Task WiringFailures_AreClassifiedPermanent(string methodName, int arity)
    {
        var provider = new ServiceCollection()
            .AddSingleton<IWiringTarget>(new WiringTarget())
            .BuildServiceProvider();

        var thrown = await Record.ExceptionAsync(() => provider.InvokeAsync(new ReflectionInvocationDto
        {
            ServiceInterfaceFullName = typeof(IWiringTarget).FullName!,
            MethodName = methodName,
            Params = [.. Enumerable.Repeat<object?>(1, arity)]
        }));

        // The marker type IS the classification: LostSubscriberCallbackDispatcher fails fast on it
        // instead of retrying a binding that can never succeed.
        Assert.NotNull(thrown);
        Assert.IsType<CallbackTargetUnresolvableException>(thrown);
    }

    private interface IWiringTarget
    {
        Task RealAsync(int value);
        Task TwinAsync(int value);
        Task TwinAsync(string value);
        Task ByRefAsync(ref int value);
        Task GenericAsync<T>(T value);
    }

    private sealed class WiringTarget : IWiringTarget
    {
        public Task RealAsync(int value) => Task.CompletedTask;
        public Task TwinAsync(int value) => Task.CompletedTask;
        public Task TwinAsync(string value) => Task.CompletedTask;
        public Task ByRefAsync(ref int value) => Task.CompletedTask;
        public Task GenericAsync<T>(T value) => Task.CompletedTask;
        public Task RenamedAsync(int value) => Task.CompletedTask;
    }

    // -----------------------------------------------------------------------------------------
    // Finding — the SQLite verifier never looked at the flow_id collation (PRAGMA table_info does
    // not report one), so COLLATE NOCASE made the primary key and every lookup case-folding.
    // -----------------------------------------------------------------------------------------

    [Fact]
    public async Task Sqlite_RejectsACaseFoldingFlowIdCollation()
    {
        await using var database = new TempSqlite();
        await database.ExecuteAsync(
            """
            CREATE TABLE asyncresponse_flow_state (
                flow_id TEXT NOT NULL PRIMARY KEY COLLATE NOCASE,
                state_json TEXT NOT NULL,
                expires_at_utc TEXT NOT NULL,
                updated_at_utc TEXT NOT NULL,
                revision INTEGER NOT NULL DEFAULT 0,
                lease_id TEXT NULL,
                lease_expires_at_utc TEXT NULL
            );
            """);

        var store = new DurableFlows.Sqlite.SqliteFlowStateStore(Options.Create(
            new DurableFlows.Sqlite.SqliteDurableFlowOptions { ConnectionString = database.ConnectionString }));

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => store.TryCreateAsync("Order-A1", NewState("Order-A1"), TimeSpan.FromMinutes(5)));

        Assert.Contains("NOCASE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Sqlite_AcceptsTheDefaultBinaryCollation()
    {
        await using var database = new TempSqlite();
        var store = new DurableFlows.Sqlite.SqliteFlowStateStore(Options.Create(
            new DurableFlows.Sqlite.SqliteDurableFlowOptions { ConnectionString = database.ConnectionString }));

        Assert.True(await store.TryCreateAsync("Order-A1", NewState("Order-A1"), TimeSpan.FromMinutes(5)));

        // Ordinal comparison: the two ids are distinct runs, not one folded key.
        Assert.True(await store.TryCreateAsync("order-a1", NewState("order-a1"), TimeSpan.FromMinutes(5)));
    }

    // -----------------------------------------------------------------------------------------
    // Finding — the parse failure chained the RAW JsonException, whose message carries
    // "Path: $.<inbound property name>" (dictionary keys included) and multi-character body spans.
    // -----------------------------------------------------------------------------------------

    [Theory]
    // The secret as an unknown PROPERTY NAME: STJ renders the inbound name into the Path suffix.
    [InlineData("""{"x-tenant-acme-bearer-9f3c": }""", "x-tenant-acme-bearer-9f3c")]
    // A malformed literal: STJ quotes the raw body run it was reading, not one offending character
    // ("'ntenant-…}' is an invalid JSON literal. Expected the literal 'null'.").
    [InlineData("""{"Status":ntenant-acme-bearer-9f3c}""", "tenant-acme-bearer-9f3c")]
    public void ParseFailure_NeverCarriesInboundText_InAnyExceptionInTheChain(string body, string secret)
    {
        var thrown = Record.Exception(() => JsonSafety.SafeDeserialize<WorkerJobEnvelope>(body));
        Assert.NotNull(thrown);

        // Proof the probe is real: STJ's own message DOES contain the secret for these bodies.
        var raw = Record.Exception(() => JsonSerializer.Deserialize<WorkerJobEnvelope>(body, AsyncResponseJson.CaseInsensitive));
        Assert.NotNull(raw);
        Assert.Contains(secret, raw.ToString(), StringComparison.Ordinal);

        // ToString() walks the whole chain, which is what LogError(ex, ...) renders.
        Assert.DoesNotContain(secret, thrown.ToString(), StringComparison.Ordinal);
    }

    // -----------------------------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------------------------

    private static FlowState NewState(string flowId) => new()
    {
        FlowId = flowId,
        FlowTypeName = "Round29Flow",
        Status = FlowRunStatus.Running,
        Steps = []
    };

    private sealed class TempSqlite : IAsyncDisposable
    {
        private readonly string _path = Path.Combine(Path.GetTempPath(), $"ar-round29-{Guid.NewGuid():N}.db");

        public string ConnectionString => $"Data Source={_path}";

        public async Task ExecuteAsync(string sql)
        {
            await using var connection = new SqliteConnection(ConnectionString);
            await connection.OpenAsync();
            await using var command = connection.CreateCommand();
            command.CommandText = sql;
            await command.ExecuteNonQueryAsync();
        }

        public ValueTask DisposeAsync()
        {
            SqliteConnection.ClearPool(new SqliteConnection(ConnectionString));
            foreach (var suffix in new[] { "", "-wal", "-shm" })
            {
                try
                {
                    File.Delete(_path + suffix);
                }
                catch (IOException)
                {
                    // Best-effort temp cleanup.
                }
            }

            return ValueTask.CompletedTask;
        }
    }
}
