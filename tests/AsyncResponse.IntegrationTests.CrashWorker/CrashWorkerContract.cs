namespace AsyncResponse.IntegrationTests.CrashWorker;

/// <summary>
/// Everything the crash worker and the suite that launches it have to agree on, in one place: the
/// environment variables that configure a subprocess, the crash points, the step names, the tables
/// the flow journals into, and the stdout markers the suite waits for. The suite references this
/// assembly, so neither side spells any of it twice.
/// </summary>
public static class CrashWorkerContract
{
    /// <summary>Environment variables read by <c>CrashWorkerSettings</c>.</summary>
    public static class Env
    {
        /// <summary>Npgsql connection string of the database every subprocess of a scenario shares.</summary>
        public const string ConnectionString = "CRASHSUITE_CONNECTION_STRING";

        /// <summary>
        /// PostgreSQL schema holding the scenario's queue table, flow-state table, and journals. One
        /// schema per scenario keeps it away from the batch's other PostgreSQL consumers — the sample
        /// apps subscribe to the default queue table and would otherwise claim these jobs.
        /// </summary>
        public const string Schema = "CRASHSUITE_SCHEMA";

        /// <summary><see cref="Roles.Start"/> or <see cref="Roles.Worker"/>.</summary>
        public const string Role = "CRASHSUITE_ROLE";

        /// <summary>The flow to start (<see cref="Roles.Start"/>) or the flow the crash point is armed for.</summary>
        public const string FlowId = "CRASHSUITE_FLOW_ID";

        /// <summary>Scenario name stamped into the started flow's input (<see cref="Roles.Start"/> only).</summary>
        public const string Scenario = "CRASHSUITE_SCENARIO";

        /// <summary>Names this process in the journals, e.g. <c>owner</c> / <c>successor</c>.</summary>
        public const string WorkerLabel = "CRASHSUITE_WORKER_LABEL";

        /// <summary>One of <see cref="CrashPoints"/>. Default: <see cref="CrashPoints.None"/>.</summary>
        public const string CrashPoint = "CRASHSUITE_CRASH_POINT";

        /// <summary><c>DurableFlowOptions.ExecutionLeaseDuration</c>, in milliseconds.</summary>
        public const string LeaseDurationMs = "CRASHSUITE_LEASE_DURATION_MS";

        /// <summary><c>DurableFlowOptions.ExecutionLeaseRenewInterval</c>, in milliseconds.</summary>
        public const string LeaseRenewIntervalMs = "CRASHSUITE_LEASE_RENEW_MS";

        /// <summary>The transport's claim visibility (<c>LockTimeout</c>), in milliseconds.</summary>
        public const string LockTimeoutMs = "CRASHSUITE_LOCK_TIMEOUT_MS";

        /// <summary>Worker <c>MaxDeliveryAttempts</c> (0 = unlimited).</summary>
        public const string MaxDeliveryAttempts = "CRASHSUITE_MAX_DELIVERY_ATTEMPTS";

        /// <summary>Worker <c>RedeliveryDelay</c> after a failed handler, in milliseconds.</summary>
        public const string RedeliveryDelayMs = "CRASHSUITE_REDELIVERY_DELAY_MS";

        /// <summary>
        /// Hard ceiling on a worker's lifetime, in seconds. A safety net only: the suite kills its
        /// subprocesses itself, and this bounds the ones a killed test host would orphan.
        /// </summary>
        public const string MaxLifetimeSeconds = "CRASHSUITE_MAX_LIFETIME_SECONDS";
    }

    /// <summary>What a subprocess does.</summary>
    public static class Roles
    {
        /// <summary>Starts one flow through <c>IDurableFlows.StartAsync</c> and exits 0. Runs no subscriber.</summary>
        public const string Start = "start";

        /// <summary>Hosts the worker subscriber until it is killed — by a crash point or by the suite.</summary>
        public const string Worker = "worker";
    }

    /// <summary>Where a worker terminates itself. Every one leaves the execution lease persisted and unexpired.</summary>
    public static class CrashPoints
    {
        /// <summary>Never crash: the successor.</summary>
        public const string None = "none";

        /// <summary>
        /// Right after the store granted the execution lease, before the executor's first write: the
        /// ledger is still <c>Running</c> with <c>Attempts == 0</c> and no steps.
        /// </summary>
        public const string AfterLeaseAcquired = "after-lease-acquired";

        /// <summary>
        /// Right after <see cref="Steps.First"/>'s completion checkpoint was persisted, before
        /// <see cref="Steps.Second"/> starts.
        /// </summary>
        public const string AfterStepCheckpoint = "after-step-checkpoint";

        /// <summary>
        /// Inside <see cref="Steps.Publish"/>, right after its worker job was published and before
        /// the step's checkpoint: the publication is durable, the ledger does not know it happened.
        /// </summary>
        public const string AfterPublishBeforeCheckpoint = "after-publish-before-checkpoint";

        /// <summary>
        /// The reverse boundary, on <see cref="Steps.Child"/>: the child ledger and the parent's
        /// breadcrumb checkpoint are persisted, the job that would execute the child is not yet
        /// published. Nothing but the parent's redelivery can ever make that child runnable.
        /// </summary>
        public const string AfterChildCheckpointBeforePublish = "after-child-checkpoint-before-publish";
    }

    /// <summary>Step names of <c>CrashSuiteFlow</c>, in execution order, and of its child.</summary>
    public static class Steps
    {
        public const string First = "step-1";
        public const string Second = "step-2";
        public const string Publish = "publish";
        public const string Child = "child";
        public const string Last = "step-3";

        /// <summary>The one step of <c>CrashSuiteChildFlow</c>.</summary>
        public const string ChildWork = "child-work";

        /// <summary>Not a flow step: the handler of the worker job <see cref="Publish"/> publishes.</summary>
        public const string PublishedJob = "published-job";
    }

    /// <summary>Tables inside <see cref="Env.Schema"/>.</summary>
    public static class Tables
    {
        /// <summary>The PostgreSQL transport's queue table (its default name).</summary>
        public const string TransportMessages = "asyncresponse_transport_messages";

        /// <summary>The PostgreSQL flow-state store's ledger table (its default name).</summary>
        public const string FlowState = "asyncresponse_flow_state";

        /// <summary>
        /// One row per (flow_id, step, worker) with an <c>executions</c> counter: the side effect
        /// every step body records, so the suite can tell which process ran a step and how often.
        /// </summary>
        public const string Effects = "crashsuite_effects";

        /// <summary>
        /// One row per <c>TryAcquireLeaseAsync</c> call (flow_id, worker, acquired, at — the database
        /// clock): proves a successor really met the dead owner's lease while it was still unexpired.
        /// </summary>
        public const string LeaseJournal = "crashsuite_lease_journal";
    }

    /// <summary>The transport's worker queue name.</summary>
    public const string WorkerQueue = "crashsuite-worker";

    /// <summary>The child flow's id for a given parent (the engine's default composition).</summary>
    public static string ChildFlowId(string parentFlowId) => $"{parentFlowId}:{Steps.Child}";

    /// <summary>Printed on stdout once a worker's host has started.</summary>
    public const string ReadyMarker = "CRASHSUITE-READY";

    /// <summary>Printed (and flushed) on stdout immediately before a crash point kills the process.</summary>
    public const string CrashMarker = "CRASHSUITE-CRASHING";

    /// <summary>Printed on stdout when the <see cref="Roles.Start"/> role has started its flow.</summary>
    public const string StartedMarker = "CRASHSUITE-STARTED";
}
