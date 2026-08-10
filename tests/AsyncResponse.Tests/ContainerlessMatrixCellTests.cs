using AsyncResponse.Conformance;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// The corner of the provider cross product that needs no container: the in-memory channel and
/// transport paired with the two file-or-process-local stores. Every other cell lives in the
/// integration shards, which boot the servers their combinations need.
/// <para>
/// These run in the fast suite on purpose. They are the cells an application gets by default, and
/// they keep the matrix plumbing itself — <see cref="MatrixHarness"/>' wiring, hosted-service
/// lifecycle, and teardown — under test without Docker, so a break in the harness surfaces in
/// seconds rather than after a fleet boots.
/// </para>
/// </summary>
public sealed class ContainerlessMatrixCellTests
{
    /// <summary>
    /// The cells reachable with no server at all. Cross-checked against
    /// <see cref="ProviderMatrix.AllCells"/> by <see cref="MatrixCompletenessTests"/>, so a new
    /// container-free provider cannot quietly skip this suite.
    /// </summary>
    public static TheoryData<MatrixCell> ContainerlessCells =>
    [
        new MatrixCell(MatrixChannel.InMemory, MatrixTransport.InMemory, MatrixStore.InMemory),
        new MatrixCell(MatrixChannel.InMemory, MatrixTransport.InMemory, MatrixStore.Sqlite)
    ];

    /// <summary>No backend is wired: asking for one is the bug these cells exist to rule out.</summary>
    private static MatrixBackends Backends => new();

    [Theory]
    [MemberData(nameof(ContainerlessCells))]
    public async Task Matrix_DurableFlow_CompletesAcrossAllThreeProviders(MatrixCell cell)
        => await MatrixScenarios.DurableFlowCompletesAsync(cell, Backends);

    [Theory]
    [MemberData(nameof(ContainerlessCells))]
    public async Task Matrix_DomainFailure_MarksRunTerminallyFailed(MatrixCell cell)
        => await MatrixScenarios.DomainFailureMarksRunFailedAsync(cell, Backends);

    [Theory]
    [MemberData(nameof(ContainerlessCells))]
    public async Task Matrix_WorkerJob_RestoresContextAndAnswersWaiter(MatrixCell cell)
        => await MatrixScenarios.WorkerJobRestoresContextAndAnswersWaiterAsync(cell, Backends);
}
