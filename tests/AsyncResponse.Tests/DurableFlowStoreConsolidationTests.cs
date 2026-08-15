using System.Data;
using System.Reflection;
using AsyncResponse.DurableFlows.Cosmos;
using AsyncResponse.DurableFlows.DynamoDB;
using AsyncResponse.DurableFlows.EFCore;
using AsyncResponse.DurableFlows.MongoDB;
using AsyncResponse.DurableFlows.MySql;
using AsyncResponse.DurableFlows.Oracle;
using AsyncResponse.DurableFlows.PostgreSQL;
using AsyncResponse.DurableFlows.Sqlite;
using AsyncResponse.DurableFlows.SqlServer;
using Microsoft.Data.Sqlite;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// The nine store packages compile the same shared source (<c>DurableFlowStoreShared</c>); these
/// facts pin the helpers that must behave identically in every assembly. The serialization pins
/// are the load-bearing ones: the ledger has exactly ONE wire format — Core's
/// <c>FlowStateJson</c>, the same writer <c>WireContractSerializationTests</c> and
/// <c>AotSerializationSeamTests</c> pin — so a ledger written through any provider store loads
/// through any other, and <c>AsyncResponseJsonSerialization.RegisterResolver</c> (the documented
/// trim/AOT seam) reaches every store.
/// </summary>
public sealed class DurableFlowStoreConsolidationTests
{
    private const string SharedTypeName = "AsyncResponse.DurableFlows.Internal.DurableFlowStoreShared";

    public static TheoryData<Type> ProviderOptionTypes =>
    [
        typeof(CosmosDurableFlowOptions),
        typeof(DynamoDbDurableFlowOptions),
        typeof(EFCoreDurableFlowOptions),
        typeof(MongoDbDurableFlowOptions),
        typeof(MySqlDurableFlowOptions),
        typeof(OracleDurableFlowOptions),
        typeof(PostgreSqlDurableFlowOptions),
        typeof(SqliteDurableFlowOptions),
        typeof(SqlServerDurableFlowOptions)
    ];

    [Theory]
    [MemberData(nameof(ProviderOptionTypes))]
    public void Serialize_IsByteIdenticalToFlowStateJson_InEveryStoreAssembly(Type providerOptionsType)
    {
        var shared = providerOptionsType.Assembly.GetType(SharedTypeName, throwOnError: true)!;

        // One populated and one null-heavy ledger: the omitted-null policy is where two writers
        // would drift first.
        foreach (var state in new[] { CreatePopulatedState(), new FlowState { FlowId = "nulls" } })
        {
            var json = Assert.IsType<string>(Invoke(shared, "Serialize", state));
            Assert.Equal(FlowStateJson.Serialize(state), json);

            var restored = Assert.IsType<FlowState>(Invoke(shared, "Deserialize", json));
            Assert.Equal(state.FlowId, restored.FlowId);
            Assert.Equal(state.Revision, restored.Revision);
        }
    }

    [Theory]
    [MemberData(nameof(ProviderOptionTypes))]
    public void Deserialize_ReadsLedgersWrittenByEarlierBuilds(Type providerOptionsType)
    {
        // A ledger as the retired per-package source-gen context wrote it (PascalCase member
        // names, nulls omitted, enums as numbers, ISO-8601 timestamps) — plus one explicit null,
        // which that context's read side accepted. Existing rows must keep loading byte-for-byte
        // as-is after the writer consolidation.
        const string ledger =
            """
            {"SchemaVersion":1,"Revision":3,"FlowId":"flow-1","FlowTypeName":"My.Flows.ProvisioningFlow","InputTypeName":"My.Flows.ProvisionRequest","InputJson":"{\"name\":\"x\"}","Status":0,"LastMessage":null,"CreatedAtUtc":"2026-07-16T08:30:00Z","UpdatedAtUtc":"2026-07-16T08:31:00Z","Attempts":2,"Steps":{"prepare":{"Completed":true,"ResultJson":"{\"slug\":\"x-prep\"}","Faulted":false,"CompletedAtUtc":"2026-07-16T08:30:30Z"},"remote-work":{"Completed":false,"PendingCorrelationId":"cid-42","Faulted":false}},"Values":{"greeting":"\"hi\""},"ParentFlowId":"parent-1","ParentStepName":"await-child","Context":{"tenant":"t-1"}}
            """;

        var shared = providerOptionsType.Assembly.GetType(SharedTypeName, throwOnError: true)!;
        var state = Assert.IsType<FlowState>(Invoke(shared, "Deserialize", ledger));

        Assert.Equal(3, state.Revision);
        Assert.Equal("flow-1", state.FlowId);
        Assert.Equal("My.Flows.ProvisioningFlow", state.FlowTypeName);
        Assert.Equal(FlowRunStatus.Running, state.Status);
        Assert.Null(state.LastMessage);
        Assert.Equal(2, state.Attempts);
        Assert.True(state.Steps!["prepare"].Completed);
        Assert.Equal("cid-42", state.Steps["remote-work"].PendingCorrelationId);
        Assert.Equal("parent-1", state.ParentFlowId);
        Assert.Equal("\"hi\"", state.Values!["greeting"]);

        // The read-side contract is unchanged too: unknown schema versions load as absent.
        Assert.Null(Invoke(shared, "Deserialize", """{"SchemaVersion":2,"FlowId":"flow-1"}"""));
    }

    [Theory]
    [MemberData(nameof(ProviderOptionTypes))]
    public void ValidateLeaseArgs_GuardsTheLeasePreamble_InEveryStoreAssembly(Type providerOptionsType)
    {
        var shared = providerOptionsType.Assembly.GetType(SharedTypeName, throwOnError: true)!;

        Invoke(shared, "ValidateLeaseArgs", "flow", "lease", TimeSpan.FromSeconds(30));

        // ParamNames match the public IFlowStateStore signatures the stores forward.
        Assert.Equal("flowId", AssertInner<ArgumentException>(shared, "ValidateLeaseArgs", " ", "lease", TimeSpan.FromSeconds(30)).ParamName);
        Assert.Equal("leaseId", AssertInner<ArgumentException>(shared, "ValidateLeaseArgs", "flow", " ", TimeSpan.FromSeconds(30)).ParamName);
        Assert.Equal("leaseDuration", AssertInner<ArgumentOutOfRangeException>(shared, "ValidateLeaseArgs", "flow", "lease", TimeSpan.Zero).ParamName);
        AssertInner<ArgumentOutOfRangeException>(shared, "ValidateLeaseArgs", "flow", "lease", TimeSpan.FromSeconds(-1));
    }

    [Fact]
    public void OptionGuards_KeepTheHistoricalMessages()
    {
        // The shared guards parameterize only the options type name; the message text is an
        // operator-facing surface and must not drift.
        var missingConnection = Assert.Throws<InvalidOperationException>(
            () => new SqliteDurableFlowOptions { ConnectionString = " " }.Validate());
        Assert.Equal("SqliteDurableFlowOptions.ConnectionString must be configured.", missingConnection.Message);

        var nonPositiveBudget = Assert.Throws<InvalidOperationException>(
            () => new SqliteDurableFlowOptions { MaxStateBytes = 0 }.Validate());
        Assert.Equal("SqliteDurableFlowOptions.MaxStateBytes must be positive when configured.", nonPositiveBudget.Message);

        var sqlServer = Assert.Throws<InvalidOperationException>(
            () => new SqlServerDurableFlowOptions { ConnectionString = "cs", MaxStateBytes = -1 }.Validate());
        Assert.Equal("SqlServerDurableFlowOptions.MaxStateBytes must be positive when configured.", sqlServer.Message);
    }

    [Fact]
    public async Task OpenConnectionAsync_SharedHelper_OpensAndPropagatesFailures()
    {
        var shared = typeof(SqliteDurableFlowOptions).Assembly.GetType(SharedTypeName, throwOnError: true)!;
        var open = shared.GetMethod("OpenConnectionAsync", BindingFlags.Public | BindingFlags.Static)!
            .MakeGenericMethod(typeof(SqliteConnection));

        await using (var connection = await (Task<SqliteConnection>)open.Invoke(null, ["Data Source=:memory:", CancellationToken.None])!)
        {
            Assert.Equal(ConnectionState.Open, connection.State);
        }

        // A connection that fails to open surfaces the provider's own error (the helper disposes
        // the never-opened instance instead of returning it).
        var missing = Path.Combine(Path.GetTempPath(), $"ar-open-{Guid.NewGuid():N}", "absent.db");
        await Assert.ThrowsAsync<SqliteException>(
            async () => await (Task<SqliteConnection>)open.Invoke(null, [$"Data Source={missing};Mode=ReadOnly", CancellationToken.None])!);
    }

    private static FlowState CreatePopulatedState() => new()
    {
        Revision = 3,
        FlowId = "flow-1",
        FlowTypeName = "My.Flows.ProvisioningFlow",
        InputTypeName = "My.Flows.ProvisionRequest",
        InputJson = """{"name":"x"}""",
        Status = FlowRunStatus.Running,
        LastMessage = "Step 'prepare' completed.",
        CreatedAtUtc = new DateTime(2026, 7, 16, 8, 30, 0, DateTimeKind.Utc),
        UpdatedAtUtc = new DateTime(2026, 7, 16, 8, 31, 0, DateTimeKind.Utc),
        Attempts = 2,
        Steps = new Dictionary<string, FlowStepState>(StringComparer.Ordinal)
        {
            ["prepare"] = new()
            {
                Completed = true,
                ResultJson = """{"slug":"x-prep"}""",
                CompletedAtUtc = new DateTime(2026, 7, 16, 8, 30, 30, DateTimeKind.Utc)
            },
            ["remote-work"] = new() { PendingCorrelationId = "cid-42", Message = null },
            ["park"] = new()
            {
                ChildFlowId = "child-1",
                WakeAtUtc = new DateTime(2026, 7, 17, 0, 0, 0, DateTimeKind.Utc),
                PendingPayloadTypeFullName = "My.Flows.ProvisionResult"
            }
        },
        Values = new Dictionary<string, string>(StringComparer.Ordinal) { ["greeting"] = "\"hi\"" },
        ParentFlowId = "parent-1",
        ParentStepName = "await-child",
        Context = new Dictionary<string, string>(StringComparer.Ordinal) { ["tenant"] = "t-1" }
    };

    private static object? Invoke(Type shared, string methodName, params object?[] arguments)
        => shared.GetMethod(methodName, BindingFlags.Public | BindingFlags.Static)!.Invoke(null, arguments);

    private static TException AssertInner<TException>(Type shared, string methodName, params object?[] arguments)
        where TException : Exception
    {
        var exception = Assert.Throws<TargetInvocationException>(() => Invoke(shared, methodName, arguments));
        return Assert.IsType<TException>(exception.InnerException);
    }
}
