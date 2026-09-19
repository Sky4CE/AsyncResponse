using System.Diagnostics;
using System.Globalization;
using System.Text;
using System.Text.Json;
using AsyncResponse.IntegrationTests.CrashWorker;
using Npgsql;
using Xunit;

namespace AsyncResponse.IntegrationTests;

/// <summary>
/// Abrupt-process-crash recovery for durable flows, on a durable broker/store pair (the PostgreSQL
/// worker transport and the PostgreSQL flow-state store — one container).
/// <para>
/// Everything else that calls itself a recovery test is gentler than a crash: the test harness's
/// <c>SimulateRestartAsync</c> stops services gracefully and expires leases by hand, the sample's
/// <c>/crash</c> endpoint only drops subscriptions in a process that keeps living, and the transport
/// outage contract merely starts a consumer late. Here a real worker process
/// (<c>tests/AsyncResponse.IntegrationTests.CrashWorker</c>) is SIGKILLed mid-delivery — no
/// <c>finally</c>, no host stop, no lease release, no queue settlement — and a second process is
/// started against the unchanged database.
/// </para>
/// <para>
/// The suite only ever READS the database between the two processes. It never deletes a lease,
/// touches a ledger, republishes a wake-up, or calls <c>ResumeAsync</c>: a run that needs any of
/// that to finish is exactly the failure these tests exist to catch.
/// </para>
/// </summary>
[Collection(DataCollection.Name)]
[Trait(Batches.Trait, Batches.Data)]
public sealed class DurableFlowAbruptCrashRecoveryTests(DataBatchFixture fixture, ITestOutputHelper output)
{
    // The queue's claim visibility is deliberately much shorter than every owner lease below: the
    // dead owner's job is redelivered ~2 s after the crash, so the successor meets the owner's
    // execution lease while it is still UNEXPIRED. That collision is the scenario; a redelivery
    // arriving after the lease lapsed would pass on any engine and prove nothing, so every test
    // asserts from the lease journal that the collision really happened.
    private static readonly TimeSpan LockTimeout = TimeSpan.FromSeconds(2);

    private static readonly LeaseSettings DefaultLease = new(TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(2));

    // The configuration-change pair: the owner's deployment issued a 20 s lease; the successor's
    // deployment is configured with 1 s / 300 ms. A successor that judges a held lease by its OWN
    // window gives up after 1.3 s — 16+ s before the dead owner's lease can expire.
    private static readonly LeaseSettings LongOwnerLease = new(TimeSpan.FromSeconds(20), TimeSpan.FromSeconds(5));
    private static readonly LeaseSettings ShortSuccessorLease = new(TimeSpan.FromSeconds(1), TimeSpan.FromMilliseconds(300));

    private static readonly TimeSpan SubprocessStartBudget = TimeSpan.FromSeconds(60);
    private static readonly TimeSpan CrashBudget = TimeSpan.FromSeconds(60);

    // On top of the owner's lease: what the successor may spend on startup, the takeover, and the
    // rest of the flow (a publish, a child flow, three checkpoints) on a loaded CI runner.
    private static readonly TimeSpan CompletionSlack = TimeSpan.FromSeconds(60);

    [Fact]
    public async Task KilledAfterLeaseAcquisition_SuccessorCompletesTheRun()
    {
        await using var scenario = CrashScenario.Create(fixture, output, "lease-acquired");
        await scenario.RunAsync(async () =>
        {
            await scenario.StartFlowAsync();
            var crashed = await scenario.RunOwnerUntilItCrashesAsync(CrashWorkerContract.CrashPoints.AfterLeaseAcquired, DefaultLease);

            // Died before the executor's first write: nothing but the lease records the attempt.
            Assert.Equal(0, crashed.State.Attempts);
            Assert.True(crashed.State.Steps is null or { Count: 0 }, "No step may be recorded before the first checkpoint.");

            await scenario.RunSuccessorToCompletionAsync(DefaultLease, crashed);

            var effects = await scenario.ReadEffectsAsync();
            effects.AssertExecutions(CrashWorkerContract.Steps.First, owner: 0, successor: 1);
            effects.AssertExecutions(CrashWorkerContract.Steps.Second, owner: 0, successor: 1);
            effects.AssertExecutions(CrashWorkerContract.Steps.Publish, owner: 0, successor: 1);
            effects.AssertExecutions(CrashWorkerContract.Steps.PublishedJob, owner: 0, successor: 1);
            effects.AssertExecutions(CrashWorkerContract.Steps.ChildWork, owner: 0, successor: 1);
            effects.AssertExecutions(CrashWorkerContract.Steps.Last, owner: 0, successor: 1);
        });
    }

    /// <summary>
    /// The configuration-change takeover. The dead owner's lease was issued under a LONGER
    /// <c>ExecutionLeaseDuration</c> than the successor is configured with, and the queue redelivers
    /// the run's only wake-up while that lease is still unexpired. An engine that waits out its own
    /// lease window and then acknowledges the wake-up "as a duplicate" strands the run as
    /// <c>Running</c> forever: nothing is queued, nothing holds the lease, nothing will ever wake it.
    /// </summary>
    [Fact]
    public async Task KilledAfterLeaseAcquisition_SuccessorWithAMuchShorterLease_StillCompletesTheRun()
    {
        await using var scenario = CrashScenario.Create(fixture, output, "shorter-lease");
        await scenario.RunAsync(async () =>
        {
            await scenario.StartFlowAsync();
            var crashed = await scenario.RunOwnerUntilItCrashesAsync(CrashWorkerContract.CrashPoints.AfterLeaseAcquired, LongOwnerLease);
            Assert.Equal(0, crashed.State.Attempts);

            await scenario.RunSuccessorToCompletionAsync(ShortSuccessorLease, crashed);

            var effects = await scenario.ReadEffectsAsync();
            effects.AssertExecutions(CrashWorkerContract.Steps.First, owner: 0, successor: 1);
            effects.AssertExecutions(CrashWorkerContract.Steps.Last, owner: 0, successor: 1);
        });
    }

    [Fact]
    public async Task KilledAfterAStepCheckpoint_SuccessorResumesFromIt_WithoutReexecutingTheStep()
    {
        await using var scenario = CrashScenario.Create(fixture, output, "step-checkpoint");
        await scenario.RunAsync(async () =>
        {
            await scenario.StartFlowAsync();
            var crashed = await scenario.RunOwnerUntilItCrashesAsync(CrashWorkerContract.CrashPoints.AfterStepCheckpoint, DefaultLease);

            Assert.Equal(1, crashed.State.Attempts);
            Assert.True(crashed.StepCompleted(CrashWorkerContract.Steps.First), "step-1's checkpoint must be persisted at this crash point.");
            Assert.Null(crashed.Step(CrashWorkerContract.Steps.Second));

            await scenario.RunSuccessorToCompletionAsync(DefaultLease, crashed);

            // The checkpoint is the whole point: the step the owner completed is never run again.
            var effects = await scenario.ReadEffectsAsync();
            effects.AssertExecutions(CrashWorkerContract.Steps.First, owner: 1, successor: 0);
            effects.AssertExecutions(CrashWorkerContract.Steps.Second, owner: 0, successor: 1);
            effects.AssertExecutions(CrashWorkerContract.Steps.Publish, owner: 0, successor: 1);
            effects.AssertExecutions(CrashWorkerContract.Steps.PublishedJob, owner: 0, successor: 1);
            effects.AssertExecutions(CrashWorkerContract.Steps.ChildWork, owner: 0, successor: 1);
            effects.AssertExecutions(CrashWorkerContract.Steps.Last, owner: 0, successor: 1);
        });
    }

    [Fact]
    public async Task KilledAfterAPublish_BeforeItsCheckpoint_SuccessorRepublishes_AndCompletesTheRun()
    {
        await using var scenario = CrashScenario.Create(fixture, output, "publish-no-checkpoint");
        await scenario.RunAsync(async () =>
        {
            await scenario.StartFlowAsync();
            var crashed = await scenario.RunOwnerUntilItCrashesAsync(CrashWorkerContract.CrashPoints.AfterPublishBeforeCheckpoint, DefaultLease);

            Assert.True(crashed.StepCompleted(CrashWorkerContract.Steps.Second));
            Assert.False(crashed.StepCompleted(CrashWorkerContract.Steps.Publish), "The publish step's checkpoint must NOT be persisted at this crash point.");
            // The owner's unsettled flow job plus the job its publish step committed before dying.
            Assert.Equal(2, crashed.QueuedJobs);

            await scenario.RunSuccessorToCompletionAsync(DefaultLease, crashed);

            var effects = await scenario.ReadEffectsAsync();
            effects.AssertExecutions(CrashWorkerContract.Steps.First, owner: 1, successor: 0);
            effects.AssertExecutions(CrashWorkerContract.Steps.Second, owner: 1, successor: 0);

            // No checkpoint, so the step runs again — and publishes again. At-least-once made
            // visible: neither publication is lost, and the duplicate is the handler's to absorb.
            effects.AssertExecutions(CrashWorkerContract.Steps.Publish, owner: 1, successor: 1);
            effects.AssertExecutions(CrashWorkerContract.Steps.PublishedJob, owner: 0, successor: 2);
            effects.AssertExecutions(CrashWorkerContract.Steps.ChildWork, owner: 0, successor: 1);
            effects.AssertExecutions(CrashWorkerContract.Steps.Last, owner: 0, successor: 1);
        });
    }

    [Fact]
    public async Task KilledAfterAChildCheckpoint_BeforeTheChildIsPublished_SuccessorPublishesIt_AndCompletesBothRuns()
    {
        await using var scenario = CrashScenario.Create(fixture, output, "child-no-publish");
        await scenario.RunAsync(async () =>
        {
            await scenario.StartFlowAsync();
            var crashed = await scenario.RunOwnerUntilItCrashesAsync(CrashWorkerContract.CrashPoints.AfterChildCheckpointBeforePublish, DefaultLease);

            // The reverse boundary: both writes landed, the publish did not. The child exists, is
            // Running, was never executed, holds no lease — and no queued job will ever run it.
            Assert.Equal(scenario.ChildFlowId, crashed.Step(CrashWorkerContract.Steps.Child)?.ChildFlowId);
            Assert.False(crashed.StepCompleted(CrashWorkerContract.Steps.Child));
            var orphan = await scenario.ReadLedgerAsync(scenario.ChildFlowId);
            Assert.NotNull(orphan);
            Assert.Equal(FlowRunStatus.Running, orphan.State.Status);
            Assert.Equal(0, orphan.State.Attempts);
            Assert.Null(orphan.LeaseId);
            Assert.Equal(0, await scenario.CountQueuedJobsMentioningAsync(scenario.ChildFlowId));

            await scenario.RunSuccessorToCompletionAsync(DefaultLease, crashed);

            var effects = await scenario.ReadEffectsAsync();
            effects.AssertExecutions(CrashWorkerContract.Steps.First, owner: 1, successor: 0);
            effects.AssertExecutions(CrashWorkerContract.Steps.Second, owner: 1, successor: 0);
            effects.AssertExecutions(CrashWorkerContract.Steps.Publish, owner: 1, successor: 0);
            effects.AssertExecutions(CrashWorkerContract.Steps.PublishedJob, owner: 0, successor: 1);
            effects.AssertExecutions(CrashWorkerContract.Steps.ChildWork, owner: 0, successor: 1);
            effects.AssertExecutions(CrashWorkerContract.Steps.Last, owner: 0, successor: 1);
        });
    }

    private sealed record LeaseSettings(TimeSpan Duration, TimeSpan RenewInterval);

    /// <summary>One ledger row and the queue around it, read in a single statement (one snapshot).</summary>
    private sealed record LedgerSnapshot(
        FlowState State,
        string? LeaseId,
        DateTime? LeaseExpiresAtUtc,
        bool LeaseUnexpired,
        long QueuedJobs,
        long DeadLetters)
    {
        public FlowStepState? Step(string name)
            => State.Steps is { } steps && steps.TryGetValue(name, out var step) ? step : null;

        public bool StepCompleted(string name) => Step(name) is { Completed: true };
    }

    private sealed record LeaseAttempt(string Worker, bool Acquired, DateTime AtUtc);

    private sealed class Effects(Dictionary<(string Step, string Worker), int> executions)
    {
        public void AssertExecutions(string step, int owner, int successor)
        {
            Assert.True(
                owner == Count(step, CrashScenario.OwnerLabel) && successor == Count(step, CrashScenario.SuccessorLabel),
                $"Step '{step}': expected {owner} execution(s) by the owner and {successor} by the successor, " +
                $"found {Count(step, CrashScenario.OwnerLabel)} and {Count(step, CrashScenario.SuccessorLabel)}.");
        }

        private int Count(string step, string worker) => executions.GetValueOrDefault((step, worker));
    }

    /// <summary>
    /// One scenario: a private PostgreSQL schema, the subprocesses run against it, and read-only
    /// probes. Disposal kills whatever is still running and drops the schema.
    /// </summary>
    private sealed class CrashScenario : IAsyncDisposable
    {
        public const string OwnerLabel = "owner";
        public const string SuccessorLabel = "successor";
        private const string DeadLetterQueue = "deadletter"; // the transport's default

        private readonly string _connectionString;
        private readonly NpgsqlDataSource _dataSource;
        private readonly ITestOutputHelper _output;
        private readonly string _name;
        private readonly string _schema;
        private readonly List<CrashWorkerProcess> _processes = [];

        private CrashScenario(string connectionString, ITestOutputHelper output, string name)
        {
            _connectionString = connectionString;
            _dataSource = NpgsqlDataSource.Create(connectionString);
            _output = output;
            _name = name;

            var run = Guid.NewGuid().ToString("N")[..12];
            _schema = $"crashsuite_{run}";
            FlowId = $"crash-{name}-{run}";
        }

        public string FlowId { get; }
        public string ChildFlowId => CrashWorkerContract.ChildFlowId(FlowId);

        public static CrashScenario Create(DataBatchFixture fixture, ITestOutputHelper output, string name)
        {
            var scenario = new CrashScenario(fixture.PostgreSqlConnectionString, output, name);

            // Fail here, by name, rather than as an opaque "process exited with 150".
            var assembly = typeof(CrashWorkerContract).Assembly.Location;
            var runtimeConfig = Path.ChangeExtension(assembly, ".runtimeconfig.json");
            Assert.True(
                File.Exists(runtimeConfig),
                $"The crash worker is not runnable from '{Path.GetDirectoryName(assembly)}': '{Path.GetFileName(runtimeConfig)}' is missing. " +
                "It is copied there by the ProjectReference to tests/AsyncResponse.IntegrationTests.CrashWorker; rebuild the integration tests.");

            return scenario;
        }

        /// <summary>
        /// Runs the scenario body and attaches every subprocess's stdout/stderr to the test output,
        /// pass or fail: a failure is unreadable without the workers' own account of it, and a pass
        /// keeps the timeline (when the redelivery arrived, how long the takeover waited) in the TRX.
        /// </summary>
        public async Task RunAsync(Func<Task> body)
        {
            try
            {
                await body();
            }
            finally
            {
                // Kill first so the transcripts are complete and no worker is still writing them.
                foreach (var process in _processes)
                    await process.StopAsync();
                foreach (var process in _processes)
                    _output.WriteLine(process.Transcript());
            }
        }

        public async Task StartFlowAsync()
        {
            var starter = Spawn("starter", CrashWorkerContract.Roles.Start, CrashWorkerContract.CrashPoints.None, DefaultLease);
            var exitCode = await starter.WaitForExitAsync(SubprocessStartBudget);
            Assert.True(exitCode == 0, $"The starter exited with {exitCode}.");
            Assert.Contains($"{CrashWorkerContract.StartedMarker} {FlowId}", starter.Output(), StringComparison.Ordinal);
        }

        /// <summary>
        /// Starts the owner with <paramref name="crashPoint"/> armed, waits for it to kill itself, and
        /// returns the ledger it left behind after asserting what every crash point has in common:
        /// an abnormal death at the armed point, a <c>Running</c> ledger, a persisted lease that the
        /// database still considers unexpired, and an unsettled queue claim.
        /// </summary>
        public async Task<LedgerSnapshot> RunOwnerUntilItCrashesAsync(string crashPoint, LeaseSettings lease)
        {
            var owner = Spawn(OwnerLabel, CrashWorkerContract.Roles.Worker, crashPoint, lease);
            var exitCode = await owner.WaitForExitAsync(CrashBudget);

            Assert.True(exitCode != 0, "The owner exited cleanly; it was supposed to be killed at its crash point.");
            Assert.Contains($"{CrashWorkerContract.CrashMarker} {crashPoint} flow={FlowId}", owner.Output(), StringComparison.Ordinal);

            var crashed = await ReadLedgerAsync(FlowId);
            Assert.NotNull(crashed);
            Assert.Equal(FlowRunStatus.Running, crashed.State.Status);
            Assert.NotNull(crashed.LeaseId);
            Assert.True(
                crashed.LeaseUnexpired,
                $"The dead owner's lease (expires {crashed.LeaseExpiresAtUtc:O}) must still be unexpired right after the crash; " +
                "otherwise the successor takes over an expired lease and the scenario proves nothing.");
            Assert.True(crashed.QueuedJobs >= 1, "The dead owner's job must still be in the queue: it died without settling its claim.");
            Assert.Equal(0, crashed.DeadLetters);
            return crashed;
        }

        /// <summary>
        /// Starts the successor against the unchanged database and waits — reading only — until the
        /// run and its child are <c>Succeeded</c> and the queue has drained.
        /// </summary>
        public async Task RunSuccessorToCompletionAsync(LeaseSettings lease, LedgerSnapshot crashed)
        {
            var successor = Spawn(SuccessorLabel, CrashWorkerContract.Roles.Worker, CrashWorkerContract.CrashPoints.None, lease);

            var ownerLeaseRemaining = crashed.LeaseExpiresAtUtc!.Value - DateTime.UtcNow;
            var budget = (ownerLeaseRemaining > TimeSpan.Zero ? ownerLeaseRemaining : TimeSpan.Zero) + CompletionSlack;
            var deadline = Stopwatch.StartNew();
            LedgerSnapshot? last = null;
            long drained = -1;

            while (deadline.Elapsed < budget)
            {
                Assert.False(successor.HasExited, $"The successor died with exit code {successor.ExitCodeOrNull}.");

                last = await ReadLedgerAsync(FlowId);
                Assert.NotNull(last);
                Assert.True(last.DeadLetters == 0, "The run's wake-up was dead-lettered: only an operator could revive it now.");

                // Every wake-up is published before the job that produced it is acknowledged, so a
                // Running ledger always has a job behind it. Running with an empty queue — read in
                // one snapshot — is a stranded run: no amount of waiting will finish it. Say so now
                // instead of timing out.
                Assert.False(
                    last.State.Status == FlowRunStatus.Running && last.QueuedJobs == 0,
                    $"Flow '{FlowId}' is stranded: its ledger is still Running (attempts {last.State.Attempts}, lease '{last.LeaseId}' " +
                    $"expiring {last.LeaseExpiresAtUtc:O}) but the worker queue is empty — the successor acknowledged the run's only wake-up " +
                    "without executing it.");

                if (last.State.Status == FlowRunStatus.Succeeded)
                {
                    // The terminal checkpoint precedes the acknowledgement of the job that wrote it.
                    drained = last.QueuedJobs;
                    if (drained == 0)
                        break;
                }
                else
                {
                    Assert.Equal(FlowRunStatus.Running, last.State.Status);
                }

                await Task.Delay(250);
            }

            Assert.True(
                last is { State.Status: FlowRunStatus.Succeeded } && drained == 0,
                $"Flow '{FlowId}' did not complete within {budget}: status {last?.State.Status}, attempts {last?.State.Attempts}, " +
                $"queued jobs {last?.QueuedJobs}, lease '{last?.LeaseId}' expiring {last?.LeaseExpiresAtUtc:O}.");

            Assert.NotNull(last);
            Assert.Null(last.LeaseId); // released by the successor, not left to expire

            // Waited out in place, not bounced off the queue: the store reports the dead owner's
            // lease, so the successor never has to give the wake-up back to the transport.
            Assert.DoesNotContain(nameof(DurableFlowLeaseContendedException), successor.Output(), StringComparison.Ordinal);

            var child = await ReadLedgerAsync(ChildFlowId);
            Assert.NotNull(child);
            Assert.Equal(FlowRunStatus.Succeeded, child.State.Status);

            // The premise, proven rather than assumed: the successor met the dead owner's lease
            // while the database still considered it unexpired, was refused, and did not get in
            // until it had expired — nobody broke the lease for it.
            var attempts = await ReadLeaseJournalAsync(FlowId);
            var ownerLeaseExpiry = crashed.LeaseExpiresAtUtc.Value;
            Assert.Single(attempts, attempt => attempt is { Worker: OwnerLabel, Acquired: true });
            Assert.Contains(attempts, attempt => attempt is { Worker: SuccessorLabel, Acquired: false } && attempt.AtUtc < ownerLeaseExpiry);
            var takeover = attempts.First(attempt => attempt is { Worker: SuccessorLabel, Acquired: true });
            Assert.True(
                takeover.AtUtc >= ownerLeaseExpiry,
                $"The successor acquired the lease at {takeover.AtUtc:O}, before the owner's lease expired at {ownerLeaseExpiry:O}.");
        }

        public async Task<LedgerSnapshot?> ReadLedgerAsync(string flowId)
        {
            await using var connection = await _dataSource.OpenConnectionAsync();
            await using var command = connection.CreateCommand();
            command.CommandText =
                $"""
                SELECT flow.state_json,
                       flow.lease_id,
                       flow.lease_expires_at_utc,
                       COALESCE(flow.lease_expires_at_utc > now(), false),
                       (SELECT count(*) FROM {Table(CrashWorkerContract.Tables.TransportMessages)} WHERE queue = @worker_queue),
                       (SELECT count(*) FROM {Table(CrashWorkerContract.Tables.TransportMessages)} WHERE queue = @dead_letter_queue)
                FROM {Table(CrashWorkerContract.Tables.FlowState)} AS flow
                WHERE flow.flow_id = @flow_id;
                """;
            command.Parameters.AddWithValue("flow_id", flowId);
            command.Parameters.AddWithValue("worker_queue", CrashWorkerContract.WorkerQueue);
            command.Parameters.AddWithValue("dead_letter_queue", DeadLetterQueue);

            await using var reader = await command.ExecuteReaderAsync();
            if (!await reader.ReadAsync())
                return null;

            // The ledger's wire format is plain System.Text.Json over the public FlowState shape.
            var state = JsonSerializer.Deserialize<FlowState>(reader.GetString(0))
                ?? throw new InvalidOperationException($"Flow '{flowId}' has a null ledger.");
            return new LedgerSnapshot(
                state,
                reader.IsDBNull(1) ? null : reader.GetString(1),
                reader.IsDBNull(2) ? null : reader.GetDateTime(2).ToUniversalTime(),
                reader.GetBoolean(3),
                reader.GetInt64(4),
                reader.GetInt64(5));
        }

        public async Task<long> CountQueuedJobsMentioningAsync(string text)
        {
            await using var connection = await _dataSource.OpenConnectionAsync();
            await using var command = connection.CreateCommand();
            command.CommandText =
                $"SELECT count(*) FROM {Table(CrashWorkerContract.Tables.TransportMessages)} WHERE position(@text in payload_json::text) > 0;";
            command.Parameters.AddWithValue("text", text);
            return (long)(await command.ExecuteScalarAsync())!;
        }

        public async Task<Effects> ReadEffectsAsync()
        {
            await using var connection = await _dataSource.OpenConnectionAsync();
            await using var command = connection.CreateCommand();
            command.CommandText = $"SELECT step, worker, executions FROM {Table(CrashWorkerContract.Tables.Effects)};";

            var executions = new Dictionary<(string, string), int>();
            await using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
                executions[(reader.GetString(0), reader.GetString(1))] = reader.GetInt32(2);
            return new Effects(executions);
        }

        private async Task<List<LeaseAttempt>> ReadLeaseJournalAsync(string flowId)
        {
            await using var connection = await _dataSource.OpenConnectionAsync();
            await using var command = connection.CreateCommand();
            command.CommandText =
                $"SELECT worker, acquired, at FROM {Table(CrashWorkerContract.Tables.LeaseJournal)} WHERE flow_id = @flow_id ORDER BY id;";
            command.Parameters.AddWithValue("flow_id", flowId);

            var attempts = new List<LeaseAttempt>();
            await using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
                attempts.Add(new LeaseAttempt(reader.GetString(0), reader.GetBoolean(1), reader.GetDateTime(2).ToUniversalTime()));
            return attempts;
        }

        private CrashWorkerProcess Spawn(string label, string role, string crashPoint, LeaseSettings lease)
        {
            var process = CrashWorkerProcess.Start(label, new Dictionary<string, string>(StringComparer.Ordinal)
            {
                [CrashWorkerContract.Env.ConnectionString] = _connectionString,
                [CrashWorkerContract.Env.Schema] = _schema,
                [CrashWorkerContract.Env.Role] = role,
                [CrashWorkerContract.Env.FlowId] = FlowId,
                [CrashWorkerContract.Env.Scenario] = _name,
                [CrashWorkerContract.Env.WorkerLabel] = label,
                [CrashWorkerContract.Env.CrashPoint] = crashPoint,
                [CrashWorkerContract.Env.LeaseDurationMs] = Milliseconds(lease.Duration),
                [CrashWorkerContract.Env.LeaseRenewIntervalMs] = Milliseconds(lease.RenewInterval),
                [CrashWorkerContract.Env.LockTimeoutMs] = Milliseconds(LockTimeout),

                // The PostgreSQL transport's DEFAULT retry budget, on purpose. The built-in stores
                // report their leases, so a contended wake-up is waited out in place on ONE
                // delivery; a budget inflated to survive being handed back to the queue every
                // second would hide a regression to that fallback, which under these defaults
                // dead-letters the wake-up before a 20 s owner lease has expired.
                [CrashWorkerContract.Env.MaxDeliveryAttempts] = "5",
                [CrashWorkerContract.Env.RedeliveryDelayMs] = "5000",
                [CrashWorkerContract.Env.MaxLifetimeSeconds] = "300",
            });
            _processes.Add(process);
            return process;
        }

        private string Table(string name) => $"\"{_schema}\".\"{name}\"";

        private static string Milliseconds(TimeSpan value)
            => ((long)value.TotalMilliseconds).ToString(CultureInfo.InvariantCulture);

        public async ValueTask DisposeAsync()
        {
            foreach (var process in _processes)
                await process.DisposeAsync();

            try
            {
                await using var connection = await _dataSource.OpenConnectionAsync();
                await using var command = connection.CreateCommand();
                command.CommandText = $"DROP SCHEMA IF EXISTS \"{_schema}\" CASCADE;";
                await command.ExecuteNonQueryAsync();
            }
            catch (Exception ex) when (ex is NpgsqlException or InvalidOperationException)
            {
                _output.WriteLine($"Could not drop schema {_schema}: {ex.Message}");
            }

            await _dataSource.DisposeAsync();
        }
    }

    /// <summary>A crash-worker subprocess with its stdout/stderr captured.</summary>
    private sealed class CrashWorkerProcess : IAsyncDisposable
    {
        private readonly Process _process;
        private readonly string _label;
        private readonly StringBuilder _output = new();

        private CrashWorkerProcess(Process process, string label)
        {
            _process = process;
            _label = label;
        }

        public bool HasExited => _process.HasExited;
        public int? ExitCodeOrNull => _process.HasExited ? _process.ExitCode : null;

        public static CrashWorkerProcess Start(string label, IReadOnlyDictionary<string, string> environment)
        {
            var assembly = typeof(CrashWorkerContract).Assembly.Location;
            var startInfo = new ProcessStartInfo
            {
                // The muxer that launched this test run when it says so, PATH otherwise.
                FileName = Environment.GetEnvironmentVariable("DOTNET_HOST_PATH") is { Length: > 0 } host && File.Exists(host) ? host : "dotnet",
                WorkingDirectory = Path.GetDirectoryName(assembly)!,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true,
            };
            startInfo.ArgumentList.Add(assembly);
            foreach (var (name, value) in environment)
                startInfo.Environment[name] = value;

            var process = new Process { StartInfo = startInfo };
            var worker = new CrashWorkerProcess(process, label);
            process.OutputDataReceived += (_, line) => worker.Append(line.Data);
            process.ErrorDataReceived += (_, line) => worker.Append(line.Data);
            process.Start();
            process.BeginOutputReadLine();
            process.BeginErrorReadLine();
            return worker;
        }

        public async Task<int> WaitForExitAsync(TimeSpan timeout)
        {
            using var cancellation = new CancellationTokenSource(timeout);
            try
            {
                // Completes only after both redirected streams reached end-of-file, so the
                // transcript is whole by the time a caller asserts on it.
                await _process.WaitForExitAsync(cancellation.Token);
            }
            catch (OperationCanceledException)
            {
                Kill();
                Assert.Fail($"The {_label} subprocess was still running after {timeout} and was killed.");
            }

            return _process.ExitCode;
        }

        public string Output()
        {
            lock (_output)
                return _output.ToString();
        }

        public string Transcript()
            => $"----- {_label} (pid {SafeId()}, exit {ExitCodeOrNull?.ToString(CultureInfo.InvariantCulture) ?? "running"}) -----{Environment.NewLine}{Output()}";

        public void Kill()
        {
            try
            {
                if (!_process.HasExited)
                    _process.Kill(entireProcessTree: true);
            }
            catch (Exception ex) when (ex is InvalidOperationException or System.ComponentModel.Win32Exception)
            {
                // Already gone.
            }
        }

        /// <summary>Kills the process if it is still running and waits (bounded) for it to be gone.</summary>
        public async Task StopAsync()
        {
            Kill();
            try
            {
                using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(10));
                await _process.WaitForExitAsync(cancellation.Token);
            }
            catch (OperationCanceledException)
            {
                // Nothing more to do for a process the OS will not reap in ten seconds.
            }
        }

        public async ValueTask DisposeAsync()
        {
            await StopAsync();
            _process.Dispose();
        }

        private void Append(string? line)
        {
            if (line is null)
                return;

            lock (_output)
                _output.AppendLine(line);
        }

        private string SafeId()
        {
            try
            {
                return _process.Id.ToString(CultureInfo.InvariantCulture);
            }
            catch (InvalidOperationException)
            {
                return "?";
            }
        }
    }
}
