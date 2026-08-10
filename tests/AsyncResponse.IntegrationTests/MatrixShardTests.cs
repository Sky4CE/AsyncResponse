using AsyncResponse.Conformance;
using Xunit;

namespace AsyncResponse.IntegrationTests;

// The provider cross product, one class per shard. Every class runs the same three scenarios over its
// own slice of the 660 combinations, so a failure names the exact channel + transport + store triple
// in the test id — which is the whole point of enumerating the product rather than sampling it.
//
// The scenarios themselves live in MatrixScenarios (shared source): what varies here is only which
// cells a class owns and which fixture supplies its backends. Per-axis depth is NOT here — that
// belongs to ChannelConformanceSuite / TransportConformanceSuite / FlowStoreConformanceSuite, which
// run per provider instead of per combination, so adding a scenario there costs N runs rather than
// 660.

/// <summary>Database and in-process transports, ordinary stores. 288 cells — the largest shard.</summary>
[Collection(MatrixDatabaseLightCollection.Name)]
[Trait(Batches.Trait, Batches.MatrixDatabaseLight)]
public sealed class MatrixDatabaseLightTests(MatrixDatabaseLightFixture fixture)
{
    public static TheoryData<MatrixCell> Cells => MatrixCells.For(MatrixShard.DatabaseLight);

    [Theory, MemberData(nameof(Cells))]
    public Task Matrix_DurableFlow_CompletesAcrossAllThreeProviders(MatrixCell cell)
        => MatrixScenarios.DurableFlowCompletesAsync(cell, fixture.Backends);

    [Theory, MemberData(nameof(Cells))]
    public Task Matrix_DomainFailure_MarksRunTerminallyFailed(MatrixCell cell)
        => MatrixScenarios.DomainFailureMarksRunFailedAsync(cell, fixture.Backends);

    [Theory, MemberData(nameof(Cells))]
    public Task Matrix_WorkerJob_RestoresContextAndAnswersWaiter(MatrixCell cell)
        => MatrixScenarios.WorkerJobRestoresContextAndAnswersWaiterAsync(cell, fixture.Backends);
}

/// <summary>Kafka and RabbitMQ transports, ordinary stores. 96 cells.</summary>
[Collection(MatrixBrokerLightCollection.Name)]
[Trait(Batches.Trait, Batches.MatrixBrokerLight)]
public sealed class MatrixBrokerLightTests(MatrixBrokerLightFixture fixture)
{
    public static TheoryData<MatrixCell> Cells => MatrixCells.For(MatrixShard.BrokerLight);

    [Theory, MemberData(nameof(Cells))]
    public Task Matrix_DurableFlow_CompletesAcrossAllThreeProviders(MatrixCell cell)
        => MatrixScenarios.DurableFlowCompletesAsync(cell, fixture.Backends);

    [Theory, MemberData(nameof(Cells))]
    public Task Matrix_DomainFailure_MarksRunTerminallyFailed(MatrixCell cell)
        => MatrixScenarios.DomainFailureMarksRunFailedAsync(cell, fixture.Backends);

    [Theory, MemberData(nameof(Cells))]
    public Task Matrix_WorkerJob_RestoresContextAndAnswersWaiter(MatrixCell cell)
        => MatrixScenarios.WorkerJobRestoresContextAndAnswersWaiterAsync(cell, fixture.Backends);
}

/// <summary>SQS, Service Bus, and Pub/Sub transports, ordinary stores. 144 cells.</summary>
[Collection(MatrixCloudLightCollection.Name)]
[Trait(Batches.Trait, Batches.MatrixCloudLight)]
public sealed class MatrixCloudLightTests(MatrixCloudLightFixture fixture)
{
    public static TheoryData<MatrixCell> Cells => MatrixCells.For(MatrixShard.CloudLight);

    [Theory, MemberData(nameof(Cells))]
    public Task Matrix_DurableFlow_CompletesAcrossAllThreeProviders(MatrixCell cell)
        => MatrixScenarios.DurableFlowCompletesAsync(cell, fixture.Backends);

    [Theory, MemberData(nameof(Cells))]
    public Task Matrix_DomainFailure_MarksRunTerminallyFailed(MatrixCell cell)
        => MatrixScenarios.DomainFailureMarksRunFailedAsync(cell, fixture.Backends);

    [Theory, MemberData(nameof(Cells))]
    public Task Matrix_WorkerJob_RestoresContextAndAnswersWaiter(MatrixCell cell)
        => MatrixScenarios.WorkerJobRestoresContextAndAnswersWaiterAsync(cell, fixture.Backends);
}

/// <summary>Database and in-process transports, Oracle store. 36 cells.</summary>
[Collection(MatrixDatabaseOracleCollection.Name)]
[Trait(Batches.Trait, Batches.MatrixDatabaseOracle)]
public sealed class MatrixDatabaseOracleTests(MatrixDatabaseOracleFixture fixture)
{
    public static TheoryData<MatrixCell> Cells => MatrixCells.For(MatrixShard.DatabaseOracle);

    [Theory, MemberData(nameof(Cells))]
    public Task Matrix_DurableFlow_CompletesAcrossAllThreeProviders(MatrixCell cell)
        => MatrixScenarios.DurableFlowCompletesAsync(cell, fixture.Backends);

    [Theory, MemberData(nameof(Cells))]
    public Task Matrix_DomainFailure_MarksRunTerminallyFailed(MatrixCell cell)
        => MatrixScenarios.DomainFailureMarksRunFailedAsync(cell, fixture.Backends);

    [Theory, MemberData(nameof(Cells))]
    public Task Matrix_WorkerJob_RestoresContextAndAnswersWaiter(MatrixCell cell)
        => MatrixScenarios.WorkerJobRestoresContextAndAnswersWaiterAsync(cell, fixture.Backends);
}

/// <summary>Kafka and RabbitMQ transports, Oracle store. 12 cells.</summary>
[Collection(MatrixBrokerOracleCollection.Name)]
[Trait(Batches.Trait, Batches.MatrixBrokerOracle)]
public sealed class MatrixBrokerOracleTests(MatrixBrokerOracleFixture fixture)
{
    public static TheoryData<MatrixCell> Cells => MatrixCells.For(MatrixShard.BrokerOracle);

    [Theory, MemberData(nameof(Cells))]
    public Task Matrix_DurableFlow_CompletesAcrossAllThreeProviders(MatrixCell cell)
        => MatrixScenarios.DurableFlowCompletesAsync(cell, fixture.Backends);

    [Theory, MemberData(nameof(Cells))]
    public Task Matrix_DomainFailure_MarksRunTerminallyFailed(MatrixCell cell)
        => MatrixScenarios.DomainFailureMarksRunFailedAsync(cell, fixture.Backends);

    [Theory, MemberData(nameof(Cells))]
    public Task Matrix_WorkerJob_RestoresContextAndAnswersWaiter(MatrixCell cell)
        => MatrixScenarios.WorkerJobRestoresContextAndAnswersWaiterAsync(cell, fixture.Backends);
}

/// <summary>SQS, Service Bus, and Pub/Sub transports, Oracle store. 18 cells.</summary>
[Collection(MatrixCloudOracleCollection.Name)]
[Trait(Batches.Trait, Batches.MatrixCloudOracle)]
public sealed class MatrixCloudOracleTests(MatrixCloudOracleFixture fixture)
{
    public static TheoryData<MatrixCell> Cells => MatrixCells.For(MatrixShard.CloudOracle);

    [Theory, MemberData(nameof(Cells))]
    public Task Matrix_DurableFlow_CompletesAcrossAllThreeProviders(MatrixCell cell)
        => MatrixScenarios.DurableFlowCompletesAsync(cell, fixture.Backends);

    [Theory, MemberData(nameof(Cells))]
    public Task Matrix_DomainFailure_MarksRunTerminallyFailed(MatrixCell cell)
        => MatrixScenarios.DomainFailureMarksRunFailedAsync(cell, fixture.Backends);

    [Theory, MemberData(nameof(Cells))]
    public Task Matrix_WorkerJob_RestoresContextAndAnswersWaiter(MatrixCell cell)
        => MatrixScenarios.WorkerJobRestoresContextAndAnswersWaiterAsync(cell, fixture.Backends);
}

/// <summary>Database and in-process transports, Cosmos store. 36 cells.</summary>
[Collection(MatrixDatabaseCosmosCollection.Name)]
[Trait(Batches.Trait, Batches.MatrixDatabaseCosmos)]
public sealed class MatrixDatabaseCosmosTests(MatrixDatabaseCosmosFixture fixture)
{
    public static TheoryData<MatrixCell> Cells => MatrixCells.For(MatrixShard.DatabaseCosmos);

    [Theory, MemberData(nameof(Cells))]
    public Task Matrix_DurableFlow_CompletesAcrossAllThreeProviders(MatrixCell cell)
        => MatrixScenarios.DurableFlowCompletesAsync(cell, fixture.Backends);

    [Theory, MemberData(nameof(Cells))]
    public Task Matrix_DomainFailure_MarksRunTerminallyFailed(MatrixCell cell)
        => MatrixScenarios.DomainFailureMarksRunFailedAsync(cell, fixture.Backends);

    [Theory, MemberData(nameof(Cells))]
    public Task Matrix_WorkerJob_RestoresContextAndAnswersWaiter(MatrixCell cell)
        => MatrixScenarios.WorkerJobRestoresContextAndAnswersWaiterAsync(cell, fixture.Backends);
}

/// <summary>Kafka and RabbitMQ transports, Cosmos store. 12 cells.</summary>
[Collection(MatrixBrokerCosmosCollection.Name)]
[Trait(Batches.Trait, Batches.MatrixBrokerCosmos)]
public sealed class MatrixBrokerCosmosTests(MatrixBrokerCosmosFixture fixture)
{
    public static TheoryData<MatrixCell> Cells => MatrixCells.For(MatrixShard.BrokerCosmos);

    [Theory, MemberData(nameof(Cells))]
    public Task Matrix_DurableFlow_CompletesAcrossAllThreeProviders(MatrixCell cell)
        => MatrixScenarios.DurableFlowCompletesAsync(cell, fixture.Backends);

    [Theory, MemberData(nameof(Cells))]
    public Task Matrix_DomainFailure_MarksRunTerminallyFailed(MatrixCell cell)
        => MatrixScenarios.DomainFailureMarksRunFailedAsync(cell, fixture.Backends);

    [Theory, MemberData(nameof(Cells))]
    public Task Matrix_WorkerJob_RestoresContextAndAnswersWaiter(MatrixCell cell)
        => MatrixScenarios.WorkerJobRestoresContextAndAnswersWaiterAsync(cell, fixture.Backends);
}

/// <summary>SQS, Service Bus, and Pub/Sub transports, Cosmos store. 18 cells.</summary>
[Collection(MatrixCloudCosmosCollection.Name)]
[Trait(Batches.Trait, Batches.MatrixCloudCosmos)]
public sealed class MatrixCloudCosmosTests(MatrixCloudCosmosFixture fixture)
{
    public static TheoryData<MatrixCell> Cells => MatrixCells.For(MatrixShard.CloudCosmos);

    [Theory, MemberData(nameof(Cells))]
    public Task Matrix_DurableFlow_CompletesAcrossAllThreeProviders(MatrixCell cell)
        => MatrixScenarios.DurableFlowCompletesAsync(cell, fixture.Backends);

    [Theory, MemberData(nameof(Cells))]
    public Task Matrix_DomainFailure_MarksRunTerminallyFailed(MatrixCell cell)
        => MatrixScenarios.DomainFailureMarksRunFailedAsync(cell, fixture.Backends);

    [Theory, MemberData(nameof(Cells))]
    public Task Matrix_WorkerJob_RestoresContextAndAnswersWaiter(MatrixCell cell)
        => MatrixScenarios.WorkerJobRestoresContextAndAnswersWaiterAsync(cell, fixture.Backends);
}

/// <summary>Turns a shard's cells into xUnit theory data.</summary>
internal static class MatrixCells
{
    /// <summary>
    /// Optional substring filter over cell names, for running one combination locally. A shard holds
    /// up to 288 cells and the fleet takes minutes to boot, so re-running the single cell that failed
    /// — <c>ASYNCRESPONSE_MATRIX_FILTER=PostgreSql+Kafka</c> — is the difference between a 20-second
    /// loop and a 20-minute one. CI never sets it; the guard below stops it from silently emptying a
    /// leg if it ever leaks into the environment.
    /// </summary>
    private const string FilterVariable = "ASYNCRESPONSE_MATRIX_FILTER";

    internal static TheoryData<MatrixCell> For(MatrixShard shard)
    {
        var filter = Environment.GetEnvironmentVariable(FilterVariable);
        var cells = ProviderMatrix.CellsFor(shard);
        if (!string.IsNullOrWhiteSpace(filter))
        {
            // A filter matching nothing anywhere is a typo, and silently running zero tests is the one
            // outcome that must not look like success. Matching nothing in *this* shard is normal — the
            // named cell simply lives in another one.
            if (!ProviderMatrix.AllCells.Any(cell => Matches(cell, filter)))
            {
                throw new InvalidOperationException(
                    $"{FilterVariable}='{filter}' matches no cell in the whole cross product. " +
                    "Cell names look like 'PostgreSql+Kafka+MongoDb'.");
            }

            cells = [.. cells.Where(cell => Matches(cell, filter))];
        }

        var data = new TheoryData<MatrixCell>();
        foreach (var cell in cells)
            data.Add(cell);
        return data;
    }

    private static bool Matches(MatrixCell cell, string filter)
        => cell.ToString().Contains(filter, StringComparison.OrdinalIgnoreCase);
}
