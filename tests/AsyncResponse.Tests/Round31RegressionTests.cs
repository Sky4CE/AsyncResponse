using AsyncResponse.Channels.MongoDB;
using AsyncResponse.Channels.PostgreSQL;
using AsyncResponse.Channels.SqlServer;
using AsyncResponse.DurableFlows.SqlServer;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>Regression pins for the round-31 review's core findings.</summary>
public sealed class Round31RegressionTests
{
    [Fact]
    public async Task StartupValidator_ConstructsTheFlowStore_SoMisconfigurationFailsTheDeployment()
    {
        // Regression: durable-flow store options were the only option family with no
        // startup-time validation — every provider store validates in its constructor, but the
        // store was resolved only inside per-execution scopes, so a misconfigured table name
        // passed startup and first threw inside the worker transport's retry loop, burning a real
        // production run to the delivery cap. The startup validator now constructs the store once.
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddAsyncResponse()
            .WithInMemoryChannel()
            .WithInMemoryTransport()
            .WithSqlServerDurableFlows(options =>
            {
                options.ConnectionString = "Server=localhost;Database=unused;User ID=sa;Password=unused;TrustServerCertificate=True";
                options.TableName = "my-flow-state"; // hyphen: rejected by the store's identifier validation
            });
        await using var provider = services.BuildServiceProvider();
        var validator = provider.GetServices<IHostedService>().OfType<AsyncResponseStartupValidator>().Single();

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => validator.StartAsync(CancellationToken.None));
        Assert.Contains(nameof(SqlServerDurableFlowOptions.TableName), ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task StartupValidator_WithAValidFlowStore_StartsCleanly()
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddAsyncResponse()
            .WithInMemoryChannel()
            .WithInMemoryTransport()
            .WithInMemoryDurableFlows();
        await using var provider = services.BuildServiceProvider();
        var validator = provider.GetServices<IHostedService>().OfType<AsyncResponseStartupValidator>().Single();

        await validator.StartAsync(CancellationToken.None);
    }

    [Fact]
    public void DbChannelFullSweepDefaults_MatchEachProvidersDeliveryMechanism()
    {
        // Regression: FullSweepInterval had no default, so the timer sweep — one store query per
        // subscribed correlation id — ran on EVERY 250 ms poll tick: W sequential queries per tick
        // of pure idle load. PostgreSQL and MongoDB have push listeners (NOTIFY / change streams)
        // carrying normal delivery, so their sweep is only the lost-wake safety net and defaults
        // to 5 seconds. SQL Server has no wake listener — the poll sweep IS its delivery
        // mechanism — so a default interval there would add latency to every response: it stays
        // null (sweep every tick) deliberately.
        Assert.Equal(TimeSpan.FromSeconds(5), new PostgreSqlAsyncResponseChannelOptions().FullSweepInterval);
        Assert.Equal(TimeSpan.FromSeconds(5), new MongoDbAsyncResponseChannelOptions().FullSweepInterval);
        Assert.Null(new SqlServerAsyncResponseChannelOptions().FullSweepInterval);
    }

    [Fact]
    public void JsonContext_RegistersEveryClosedBuiltInScalar_AWorkerArgumentCanCarry()
    {
        // Regression: under trimmed/Native AOT the object converter resolves
        // CallbackParam.Value's RUNTIME type through the source-gen chain, and the built-in
        // context registered only a partial scalar list — an ordinary float/short/DateOnly/...
        // literal in a worker enqueue threw from inside the serializer. Enums stay open-ended and
        // unregistrable; they get the actionable guidance instead (the fact below).
        Type[] scalars =
        [
            typeof(float), typeof(short), typeof(ushort), typeof(byte), typeof(sbyte),
            typeof(uint), typeof(ulong), typeof(char), typeof(DateOnly), typeof(TimeOnly),
            typeof(Uri), typeof(byte[])
        ];

        foreach (var scalar in scalars)
            Assert.NotNull(AsyncResponseJsonContext.Default.GetTypeInfo(scalar));
    }

    [Fact]
    public async Task ReflectiveExecution_DoesNotConstructTheLedgersInputType_BeforeTheContractCheck()
    {
        // Regression: the unregistered-flow fallback deserialized InputJson into whatever CLR
        // type the ledger's InputTypeName named BEFORE checking that the DI-resolved flow
        // implements IDurableFlow<inputType> — running property setters, [JsonConstructor]s and
        // converters of any loadable type on flow-store content, the exact surface the callback
        // path gates with its authorizer. The contract check now runs first, bounding the
        // constructible set to input types the application's own flows actually declare.
        R31ConstructionCanaryInput.Constructed = false;
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddScoped<R31MismatchedInputFlow>();
        services.AddAsyncResponse().WithInMemoryChannel().WithInMemoryTransport().WithInMemoryDurableFlows();
        await using var provider = services.BuildServiceProvider();

        var store = provider.GetRequiredService<InMemoryFlowStateStore>();
        var state = new FlowState
        {
            FlowId = "hostile-input",
            FlowTypeName = typeof(R31MismatchedInputFlow).FullName,
            InputTypeName = typeof(R31ConstructionCanaryInput).FullName,
            InputJson = """{"Name":"acme"}""",
            Status = FlowRunStatus.Running
        };
        Assert.True(await store.TryCreateAsync("hostile-input", state, TimeSpan.FromDays(1)));

        var executor = provider.GetRequiredService<IDurableFlowExecutor>();
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => executor.ExecuteAsync("hostile-input"));

        Assert.Contains("does not implement IDurableFlow", ex.Message, StringComparison.Ordinal);
        Assert.False(R31ConstructionCanaryInput.Constructed);
    }

    [Fact]
    public void Serialize_MetadataFailureRaisedMidGraph_CarriesTheRegistrationGuidance()
    {
        // Regression: when the serializer refuses a type while WALKING the graph (an object-typed
        // member such as CallbackParam.Value whose runtime type has no metadata — an enum under
        // Native AOT), the throw never passed through the GetTypeInfo-level catch, so the
        // actionable register-your-type guidance was skipped exactly where it is hardest to
        // diagnose. System.Type instances are unconditionally unsupported by STJ, which
        // reproduces that mid-graph failure shape on JIT.
        var envelope = new WorkerJobEnvelope
        {
            Call = new ReflectionCallDto
            {
                ServiceInterfaceFullName = "X",
                MethodName = "Y",
                Params = [CallbackParam.ForValue(typeof(int))]
            }
        };

        var ex = Assert.Throws<NotSupportedException>(() => AsyncResponseJson.Serialize(envelope));
        Assert.Contains(nameof(AsyncResponseJsonSerialization.RegisterResolver), ex.Message, StringComparison.Ordinal);
    }
}

/// <summary>Flags construction, so a test can prove deserialization never ran.</summary>
public sealed class R31ConstructionCanaryInput
{
    public static volatile bool Constructed;

    public R31ConstructionCanaryInput() => Constructed = true;

    public string? Name { get; set; }
}

/// <summary>A DI-resolvable flow whose input contract does NOT match the hostile ledger's.</summary>
public sealed class R31MismatchedInputFlow : IDurableFlow<string>
{
    public Task ExecuteAsync(IDurableFlowContext flow, string input) => Task.CompletedTask;
}
