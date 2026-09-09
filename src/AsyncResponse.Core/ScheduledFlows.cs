using System.Globalization;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace AsyncResponse;

/// <summary>Per-schedule options for <c>WithScheduledFlow</c>.</summary>
public sealed class ScheduledFlowOptions
{
    /// <summary>The time zone the cron expression is evaluated in. Default: UTC.</summary>
    public TimeZoneInfo TimeZone { get; set; } = TimeZoneInfo.Utc;

    /// <summary>
    /// Whether this schedule runs. Default: <c>true</c>. Set <c>false</c> to keep the registration
    /// (and its flow-type routing) while pausing new occurrences — e.g. per environment.
    /// </summary>
    public bool Enabled { get; set; } = true;

    /// <summary>
    /// How long the scheduler waits between attempts to re-drive an occurrence whose ledger was
    /// committed but whose worker job could not be published (a broker outage outlasting the
    /// start's own in-process retry ladder). A re-drive is the same idempotent start — the run
    /// already exists, so only its wake-up is re-published — and repeats at this interval until
    /// the job is published, the run is seen to have executed (another replica re-drove it), or
    /// its ledger is gone. Default: 30 seconds.
    /// </summary>
    public TimeSpan RedriveInterval { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// How far back the scheduler looks at startup for occurrences of this schedule whose ledger
    /// exists, is still <see cref="FlowRunStatus.Running"/>, and has never been executed
    /// (<see cref="FlowState.Attempts"/> is zero) — the shape a process crash between the ledger
    /// commit and the job publish leaves behind, and the shape an in-process re-drive queue lost
    /// with its process. Each such occurrence is re-driven (at most the 64 most recent in the
    /// window). A run that is merely queued behind a busy worker looks the same and is re-driven
    /// too, harmlessly: the duplicate wake-up is deduplicated by the execution lease. Default:
    /// 1 hour; zero disables the probe.
    /// </summary>
    public TimeSpan StartupRedriveWindow { get; set; } = TimeSpan.FromHours(1);
}

/// <summary>
/// One registered cron schedule: the parsed-on-registration expression, its options, and the
/// statically-typed start route captured at registration (AOT-safe — no type names, no reflection).
/// </summary>
internal sealed class ScheduledFlowRegistration
{
    public required string Name { get; init; }
    public required string CronExpression { get; init; }
    public required ScheduledFlowOptions Options { get; init; }
    public required Func<IDurableFlows, string, DateTimeOffset, CancellationToken, Task> StartOccurrenceAsync { get; init; }
}

/// <summary>
/// Hosted scheduler for <c>WithScheduledFlow</c> registrations. One loop per schedule computes the
/// next occurrence in the schedule's time zone, sleeps on the engine clock, and starts the flow
/// with a <b>deterministic occurrence id</b> (<c>sched:{name}:{occurrenceUtc}</c>).
/// <para>
/// <b>Exactly-once per occurrence across replicas, with no coordinator:</b> every replica runs the
/// same loop and computes the same occurrence id; the flow store's atomic create accepts exactly
/// one, and the losers re-enqueue the same run (a duplicate wake-up the execution lease already
/// dedups). Occurrences missed while every replica was down are <em>skipped</em> — on restart the
/// loop resumes from "now", by design (an at-most-once schedule; the run history shows the gap).
/// A late timer fire (seconds) still starts its own occurrence — only occurrences whose successor
/// is already due are skipped.
/// </para>
/// <para>
/// <b>A due occurrence whose start could not be published is never abandoned.</b> Skipping
/// applies only to occurrences the loop never reached. An occurrence whose start job could not be
/// published (<see cref="DurableFlowNotDispatchedException"/>: the broker outage outlasted the
/// start's retry ladder — nothing was persisted, the publish is the start's commit point) is kept
/// in an in-process re-drive queue and its idempotent start repeated every
/// <see cref="ScheduledFlowOptions.RedriveInterval"/> until the job is published. Because that
/// queue dies with the process, each loop also probes
/// <see cref="ScheduledFlowOptions.StartupRedriveWindow"/> of recent occurrences at startup and
/// re-drives any whose ledger is Running with zero attempts — a run whose wake-up was lost in
/// transit (an early-ACK worker subscriber, a broker that dropped it) and that nothing else will
/// find.
/// </para>
/// </summary>
internal sealed class ScheduledFlowService(
    IDurableFlows _flows,
    IEnumerable<ScheduledFlowRegistration> _registrations,
    ILogger<ScheduledFlowService> _logger,
    TimeProvider? _timeProvider = null) : BackgroundService
{
    /// <summary>
    /// Longest single sleep between checks. Chunking keeps every armed delay far under the BCL
    /// timer ceiling and re-reads the clock hourly, so a suspended laptop's clock jump is honored
    /// within an hour instead of after a season. Time-zone RULES are not re-read while waiting:
    /// occurrences convert through the <see cref="TimeZoneInfo"/> captured at parse, whose
    /// adjustment rules are immutable (and the BCL caches OS zone data for the process lifetime),
    /// so an OS tz-database update only takes effect after a process restart.
    /// </summary>
    private static readonly TimeSpan MaxSleepChunk = TimeSpan.FromHours(1);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var registrations = _registrations.ToArray();
        if (registrations.Length == 0)
            return;

        // WithScheduledFlow rejects duplicate names at registration; this is the defensive backstop
        // for hand-registered ScheduledFlowRegistration instances. A BackgroundService fault would
        // surface only in logs, so log-and-drop the duplicates rather than half-starting.
        var duplicate = registrations.GroupBy(r => r.Name, StringComparer.Ordinal).FirstOrDefault(g => g.Count() > 1);
        if (duplicate is not null)
        {
            _logger.LogError(
                "Two scheduled flows share the name '{Schedule}'. Schedule names key the deterministic occurrence ids; not scheduling ANY occurrences until the duplicate registration is removed.",
                duplicate.Key);
            return;
        }

        var loops = registrations
            .Where(registration =>
            {
                if (registration.Options.Enabled)
                    return true;

                _logger.LogInformation("Scheduled flow '{Schedule}' is disabled; not scheduling occurrences.", registration.Name);
                return false;
            })
            .Select(registration => RunScheduleAsync(registration, stoppingToken))
            .ToArray();

        // Fail fast: Task.WhenAll would sit on one faulted loop until every other loop ended at
        // shutdown, degrading "fail the host" into a silently dead schedule while the app reports
        // healthy. Await completions one at a time so the first fault propagates immediately.
        var pending = new List<Task>(loops);
        while (pending.Count > 0)
        {
            var finished = await Task.WhenAny(pending).ConfigureAwait(false);
            pending.Remove(finished);
            if (finished.IsFaulted)
            {
                // The first fault propagates and fails the host; the sibling loops keep running
                // until shutdown cancels them, no longer awaited by anyone. Observe their outcomes
                // so a second fault in the same outage window is logged with its exception instead
                // of dying as a finalizer-time unobserved-task event with no schedule context.
                foreach (var sibling in pending)
                {
                    _ = sibling.ContinueWith(
                        static (task, state) => ((ILogger)state!).LogError(
                            task.Exception?.GetBaseException(),
                            "A scheduled-flow loop faulted while the scheduler was already failing."),
                        _logger,
                        CancellationToken.None,
                        TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
                        TaskScheduler.Default);
                }
            }

            await finished.ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Upper bound on the in-process re-drive queue per schedule. An outage long enough to queue
    /// this many undispatched occurrences (a per-minute schedule down for four hours) is an
    /// operator incident; beyond it the OLDEST entries are dropped with an error log naming the
    /// flow id, so they stay re-drivable by hand.
    /// </summary>
    internal const int MaxUndispatchedOccurrences = 256;

    /// <summary>Most recent occurrences inside <see cref="ScheduledFlowOptions.StartupRedriveWindow"/> the startup probe loads.</summary>
    internal const int MaxStartupProbes = 64;

    private sealed class UndispatchedOccurrence
    {
        public required string FlowId { get; init; }
        public required DateTimeOffset Occurrence { get; init; }
        public required DateTimeOffset DueUtc { get; set; }
    }

    private enum RedriveOutcome
    {
        /// <summary>The wake-up is published, or the run no longer needs one; drop the entry.</summary>
        Settled,

        /// <summary>Still undispatched; try again after <see cref="ScheduledFlowOptions.RedriveInterval"/>.</summary>
        Retry
    }

    private async Task RunScheduleAsync(ScheduledFlowRegistration registration, CancellationToken stoppingToken)
    {
        var timeProvider = _timeProvider ?? TimeProvider.System;
        CronSchedule schedule;
        try
        {
            schedule = CronSchedule.Parse(registration.CronExpression, registration.Options.TimeZone);
        }
        catch (FormatException ex)
        {
            // Registration already validated the expression; reaching this means the expression
            // text was mutated afterwards. Fail the host rather than silently never firing.
            throw new InvalidOperationException($"Scheduled flow '{registration.Name}' has an invalid cron expression.", ex);
        }

        var next = schedule.GetNextOccurrence(timeProvider.GetUtcNow());
        _logger.LogInformation(
            "Scheduled flow '{Schedule}' ({Cron}, {TimeZone}): first occurrence at {NextOccurrence}.",
            registration.Name, registration.CronExpression, registration.Options.TimeZone.Id, next);

        var undispatched = new List<UndispatchedOccurrence>();
        try
        {
            await ProbeUndispatchedAtStartupAsync(registration, schedule, undispatched, timeProvider, stoppingToken).ConfigureAwait(false);

            while (!stoppingToken.IsCancellationRequested)
            {
                var now = timeProvider.GetUtcNow();
                if (next is { } occurrence && occurrence <= now)
                {
                    if (!await StartOccurrenceAsync(registration, occurrence, stoppingToken).ConfigureAwait(false))
                        Enqueue(undispatched, registration, occurrence, timeProvider.GetUtcNow());

                    // Strictly after the fired occurrence, then skip anything already due (missed
                    // occurrences are dropped by policy, not replayed in a burst).
                    var resumeFrom = timeProvider.GetUtcNow();
                    next = schedule.GetNextOccurrence(occurrence > resumeFrom ? occurrence : resumeFrom);
                    continue;
                }

                await RedriveDueAsync(registration, undispatched, timeProvider, stoppingToken).ConfigureAwait(false);

                if (next is null && undispatched.Count == 0)
                {
                    _logger.LogWarning(
                        "Scheduled flow '{Schedule}' ({Cron}) has no future occurrence (unsatisfiable expression); stopping its loop.",
                        registration.Name, registration.CronExpression);
                    return;
                }

                // Wake for whichever comes first: the next occurrence or the earliest re-drive.
                var wakeAt = next ?? DateTimeOffset.MaxValue;
                foreach (var entry in undispatched)
                {
                    if (entry.DueUtc < wakeAt)
                        wakeAt = entry.DueUtc;
                }

                now = timeProvider.GetUtcNow();
                if (wakeAt > now)
                {
                    var sleep = wakeAt - now;
                    await Task.Delay(sleep <= MaxSleepChunk ? sleep : MaxSleepChunk, timeProvider, stoppingToken).ConfigureAwait(false);
                }
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Host shutdown.
        }
    }

    /// <summary>
    /// Starts one occurrence. Returns <c>false</c> only when the occurrence's ledger is committed
    /// but its worker job was not published — the one outcome the loop must keep re-driving.
    /// </summary>
    private async Task<bool> StartOccurrenceAsync(
        ScheduledFlowRegistration registration,
        DateTimeOffset occurrence,
        CancellationToken stoppingToken)
    {
        var flowId = OccurrenceFlowId(registration.Name, occurrence);
        try
        {
            await registration.StartOccurrenceAsync(_flows, flowId, occurrence, stoppingToken).ConfigureAwait(false);
            _logger.LogInformation("Scheduled flow '{Schedule}' started occurrence {FlowId}.", registration.Name, flowId);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            throw;
        }
        catch (DurableFlowIdConflictException ex)
        {
            // The deterministic id already exists with a DIFFERENT input: another replica won the
            // create with a value this replica's input factory did not reproduce. The occurrence
            // ran (exactly once) — only the factory's determinism is at fault, so say exactly that.
            // ONLY the dedicated conflict type gets this benign reading: the delegate also runs
            // the user's input factory, and a plain InvalidOperationException from it (or from the
            // store) means nothing was started — that is the generic failure logged below.
            _logger.LogWarning(
                ex,
                "Scheduled flow '{Schedule}' occurrence {FlowId} was already started with different input — the input factory is not deterministic across replicas. The occurrence still ran exactly once.",
                registration.Name, flowId);
        }
        catch (DurableFlowNotDispatchedException ex)
        {
            // The start's publish failed after retries, so the occurrence was NOT started (the
            // publish is the start's commit point; nothing was persisted). Unlike a plain failure
            // this one is worth re-driving on its own: the id is deterministic and the start
            // idempotent, so repeating it publishes the job once the broker is back — and if the
            // publish had landed ambiguously, the same id dedupes against the run it created.
            _logger.LogError(
                ex,
                "Scheduled flow '{Schedule}' could not publish the start job for occurrence {FlowId}; the occurrence is not started and will be re-driven every {RedriveInterval} until it is published.",
                registration.Name, flowId, registration.Options.RedriveInterval);
            return false;
        }
        catch (Exception ex)
        {
            // A failed start (store or transport outage) is this occurrence's loss only; the loop
            // lives on for the next one. Another replica may still have started it.
            _logger.LogError(ex, "Scheduled flow '{Schedule}' failed to start occurrence {FlowId}.", registration.Name, flowId);
        }

        return true;
    }

    private void Enqueue(List<UndispatchedOccurrence> undispatched, ScheduledFlowRegistration registration, DateTimeOffset occurrence, DateTimeOffset now)
    {
        var flowId = OccurrenceFlowId(registration.Name, occurrence);
        foreach (var existing in undispatched)
        {
            if (string.Equals(existing.FlowId, flowId, StringComparison.Ordinal))
                return;
        }

        while (undispatched.Count >= MaxUndispatchedOccurrences)
        {
            var dropped = undispatched[0];
            undispatched.RemoveAt(0);
            _logger.LogError(
                "Scheduled flow '{Schedule}' has {Count} undispatched occurrences queued for re-drive; dropping the oldest, {FlowId}. Its ledger is still Running — re-drive it by starting the same occurrence id again once the worker transport is back.",
                registration.Name, MaxUndispatchedOccurrences, dropped.FlowId);
        }

        undispatched.Add(new UndispatchedOccurrence
        {
            FlowId = flowId,
            Occurrence = occurrence,
            DueUtc = now + registration.Options.RedriveInterval
        });
    }

    private async Task RedriveDueAsync(
        ScheduledFlowRegistration registration,
        List<UndispatchedOccurrence> undispatched,
        TimeProvider timeProvider,
        CancellationToken stoppingToken)
    {
        for (var i = 0; i < undispatched.Count;)
        {
            var entry = undispatched[i];
            if (entry.DueUtc > timeProvider.GetUtcNow())
            {
                i++;
                continue;
            }

            if (await RedriveAsync(registration, entry, stoppingToken).ConfigureAwait(false) == RedriveOutcome.Retry)
            {
                entry.DueUtc = timeProvider.GetUtcNow() + registration.Options.RedriveInterval;
                i++;
            }
            else
            {
                undispatched.RemoveAt(i);
            }
        }
    }

    private async Task<RedriveOutcome> RedriveAsync(
        ScheduledFlowRegistration registration,
        UndispatchedOccurrence entry,
        CancellationToken stoppingToken)
    {
        FlowState? state;
        try
        {
            state = await _flows.GetStateAsync(entry.FlowId, stoppingToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Scheduled flow '{Schedule}' could not load occurrence {FlowId} to re-drive it; retrying after {RedriveInterval}.", registration.Name, entry.FlowId, registration.Options.RedriveInterval);
            return RedriveOutcome.Retry;
        }

        if (state is null)
        {
            _logger.LogWarning("Scheduled flow '{Schedule}' occurrence {FlowId} no longer has a ledger (expired or deleted); giving up its re-drive.", registration.Name, entry.FlowId);
            return RedriveOutcome.Settled;
        }

        if (state.Status != FlowRunStatus.Running || state.Attempts > 0)
        {
            // Another replica re-drove it (or its own wake-up arrived after all) and the run
            // executed: nothing left to publish.
            _logger.LogInformation("Scheduled flow '{Schedule}' occurrence {FlowId} has been picked up ({Status}, {Attempts} attempt(s)); no re-drive needed.", registration.Name, entry.FlowId, state.Status, state.Attempts);
            return RedriveOutcome.Settled;
        }

        try
        {
            await registration.StartOccurrenceAsync(_flows, entry.FlowId, entry.Occurrence, stoppingToken).ConfigureAwait(false);
            _logger.LogInformation("Scheduled flow '{Schedule}' re-drove occurrence {FlowId}: its worker job is published.", registration.Name, entry.FlowId);
            return RedriveOutcome.Settled;
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            throw;
        }
        catch (DurableFlowNotDispatchedException ex)
        {
            _logger.LogWarning(ex, "Scheduled flow '{Schedule}' could not publish the worker job for occurrence {FlowId} on re-drive; retrying after {RedriveInterval}.", registration.Name, entry.FlowId, registration.Options.RedriveInterval);
            return RedriveOutcome.Retry;
        }
        catch (DurableFlowIdConflictException ex)
        {
            _logger.LogWarning(ex, "Scheduled flow '{Schedule}' occurrence {FlowId} cannot be re-driven: the input factory produced a different input than the persisted run (it is not deterministic). Giving up its re-drive; the run is still Running and needs a manual re-drive.", registration.Name, entry.FlowId);
            return RedriveOutcome.Settled;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Scheduled flow '{Schedule}' failed to re-drive occurrence {FlowId}; retrying after {RedriveInterval}.", registration.Name, entry.FlowId, registration.Options.RedriveInterval);
            return RedriveOutcome.Retry;
        }
    }

    /// <summary>
    /// Finds recent occurrences whose ledger is committed and Running with zero attempts — never
    /// executed — and queues them for an immediate re-drive. The in-process queue above does not
    /// survive a restart, and a crash between the ledger commit and the publish never even reached
    /// it; this is the only place such a run is ever looked for, because the store has no
    /// enumeration. Best-effort: a failed load ends the probe (the loop starts regardless).
    /// </summary>
    private async Task ProbeUndispatchedAtStartupAsync(
        ScheduledFlowRegistration registration,
        CronSchedule schedule,
        List<UndispatchedOccurrence> undispatched,
        TimeProvider timeProvider,
        CancellationToken stoppingToken)
    {
        var window = registration.Options.StartupRedriveWindow;
        if (window <= TimeSpan.Zero)
            return;

        var now = timeProvider.GetUtcNow();
        var cursor = window >= now - DateTimeOffset.UnixEpoch ? DateTimeOffset.UnixEpoch : now - window;
        var candidates = new List<DateTimeOffset>();
        while (schedule.GetNextOccurrence(cursor) is { } occurrence && occurrence <= now)
        {
            candidates.Add(occurrence);
            if (candidates.Count > MaxStartupProbes)
                candidates.RemoveAt(0);
            cursor = occurrence;
        }

        foreach (var occurrence in candidates)
        {
            var flowId = OccurrenceFlowId(registration.Name, occurrence);
            FlowState? state;
            try
            {
                state = await _flows.GetStateAsync(flowId, stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Scheduled flow '{Schedule}' could not probe occurrence {FlowId} for an undispatched run at startup; skipping the rest of the probe.", registration.Name, flowId);
                return;
            }

            if (state is not { Status: FlowRunStatus.Running, Attempts: 0 })
                continue;

            _logger.LogWarning(
                "Scheduled flow '{Schedule}' found occurrence {FlowId} committed but never executed (Running, 0 attempts) — its worker job was lost before publish (a crash or an outage in a previous process). Re-driving it.",
                registration.Name, flowId);
            undispatched.Add(new UndispatchedOccurrence { FlowId = flowId, Occurrence = occurrence, DueUtc = now });
        }
    }

    internal static string OccurrenceFlowId(string name, DateTimeOffset occurrence)
        // Invariant culture: interpolation formats with CurrentCulture, whose default calendar can
        // rewrite the digits (Buddhist 25730615, UmAlQura 14520214) — replicas with different
        // cultures would then mint different ids for the same occurrence and both would run.
        => string.Create(CultureInfo.InvariantCulture, $"sched:{name}:{occurrence.UtcDateTime:yyyyMMdd'T'HHmmss'Z'}");
}
