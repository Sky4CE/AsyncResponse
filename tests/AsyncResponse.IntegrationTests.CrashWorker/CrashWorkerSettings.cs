using System.Globalization;

namespace AsyncResponse.IntegrationTests.CrashWorker;

/// <summary>One subprocess's configuration, read from <see cref="CrashWorkerContract.Env"/>.</summary>
internal sealed record CrashWorkerSettings(
    string ConnectionString,
    string Schema,
    string Role,
    string? FlowId,
    string WorkerLabel,
    string CrashPoint,
    TimeSpan LeaseDuration,
    TimeSpan LeaseRenewInterval,
    TimeSpan LockTimeout,
    int MaxDeliveryAttempts,
    TimeSpan RedeliveryDelay,
    TimeSpan MaxLifetime)
{
    public static CrashWorkerSettings FromEnvironment()
    {
        var crashPoint = Optional(CrashWorkerContract.Env.CrashPoint) ?? CrashWorkerContract.CrashPoints.None;
        if (crashPoint is not (CrashWorkerContract.CrashPoints.None
            or CrashWorkerContract.CrashPoints.AfterLeaseAcquired
            or CrashWorkerContract.CrashPoints.AfterStepCheckpoint
            or CrashWorkerContract.CrashPoints.AfterPublishBeforeCheckpoint
            or CrashWorkerContract.CrashPoints.AfterChildCheckpointBeforePublish))
        {
            throw new InvalidOperationException($"{CrashWorkerContract.Env.CrashPoint} has unknown value '{crashPoint}'.");
        }

        var settings = new CrashWorkerSettings(
            ConnectionString: Required(CrashWorkerContract.Env.ConnectionString),
            Schema: Required(CrashWorkerContract.Env.Schema),
            Role: Required(CrashWorkerContract.Env.Role),
            FlowId: Optional(CrashWorkerContract.Env.FlowId),
            WorkerLabel: Optional(CrashWorkerContract.Env.WorkerLabel) ?? $"pid-{Environment.ProcessId}",
            CrashPoint: crashPoint,
            LeaseDuration: Milliseconds(CrashWorkerContract.Env.LeaseDurationMs, 10_000),
            LeaseRenewInterval: Milliseconds(CrashWorkerContract.Env.LeaseRenewIntervalMs, 2_000),
            LockTimeout: Milliseconds(CrashWorkerContract.Env.LockTimeoutMs, 2_000),
            MaxDeliveryAttempts: Integer(CrashWorkerContract.Env.MaxDeliveryAttempts, 100),
            RedeliveryDelay: Milliseconds(CrashWorkerContract.Env.RedeliveryDelayMs, 1_000),
            MaxLifetime: TimeSpan.FromSeconds(Integer(CrashWorkerContract.Env.MaxLifetimeSeconds, 300)));

        // An armed crash point without a flow would fire on whichever flow came first — including
        // the child — and the scenario would no longer be the one its name says.
        if (settings.CrashPoint != CrashWorkerContract.CrashPoints.None && settings.FlowId is null)
            throw new InvalidOperationException($"{CrashWorkerContract.Env.FlowId} is required when a crash point is armed.");
        if (settings.Role == CrashWorkerContract.Roles.Start && settings.FlowId is null)
            throw new InvalidOperationException($"{CrashWorkerContract.Env.FlowId} is required for the '{CrashWorkerContract.Roles.Start}' role.");

        return settings;
    }

    public bool IsArmedFor(string crashPoint, string flowId)
        => string.Equals(CrashPoint, crashPoint, StringComparison.Ordinal)
           && string.Equals(FlowId, flowId, StringComparison.Ordinal);

    private static string? Optional(string name)
        => Environment.GetEnvironmentVariable(name) is { Length: > 0 } value ? value : null;

    private static string Required(string name)
        => Optional(name) ?? throw new InvalidOperationException($"Environment variable {name} is required.");

    private static int Integer(string name, int fallback)
        => Optional(name) is { } raw ? int.Parse(raw, NumberStyles.None, CultureInfo.InvariantCulture) : fallback;

    private static TimeSpan Milliseconds(string name, int fallbackMs)
        => TimeSpan.FromMilliseconds(Integer(name, fallbackMs));
}
