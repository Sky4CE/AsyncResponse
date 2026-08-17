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

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                if (next is not { } occurrence)
                {
                    _logger.LogWarning(
                        "Scheduled flow '{Schedule}' ({Cron}) has no future occurrence (unsatisfiable expression); stopping its loop.",
                        registration.Name, registration.CronExpression);
                    return;
                }

                var now = timeProvider.GetUtcNow();
                if (occurrence > now)
                {
                    var sleep = occurrence - now;
                    await Task.Delay(sleep <= MaxSleepChunk ? sleep : MaxSleepChunk, timeProvider, stoppingToken).ConfigureAwait(false);
                    continue;
                }

                await StartOccurrenceAsync(registration, occurrence, stoppingToken).ConfigureAwait(false);

                // Strictly after the fired occurrence, then skip anything already due (missed
                // occurrences are dropped by policy, not replayed in a burst).
                var resumeFrom = timeProvider.GetUtcNow();
                next = schedule.GetNextOccurrence(occurrence > resumeFrom ? occurrence : resumeFrom);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Host shutdown.
        }
    }

    private async Task StartOccurrenceAsync(
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
            // Worse than a failed start: the occurrence's ledger IS committed and Running, and only
            // its wake-up was lost. Nothing will retry it on its own, so this is called out
            // separately from the nothing-happened case below — the id is deterministic, so an
            // operator (or a re-drive job) can start the same occurrence again idempotently.
            _logger.LogError(
                ex,
                "Scheduled flow '{Schedule}' persisted occurrence {FlowId} but could not publish its worker job; the run exists with no wake-up and needs to be re-driven with this id.",
                registration.Name, flowId);
        }
        catch (Exception ex)
        {
            // A failed start (store or transport outage) is this occurrence's loss only; the loop
            // lives on for the next one. Another replica may still have started it.
            _logger.LogError(ex, "Scheduled flow '{Schedule}' failed to start occurrence {FlowId}.", registration.Name, flowId);
        }
    }

    internal static string OccurrenceFlowId(string name, DateTimeOffset occurrence)
        // Invariant culture: interpolation formats with CurrentCulture, whose default calendar can
        // rewrite the digits (Buddhist 25730615, UmAlQura 14520214) — replicas with different
        // cultures would then mint different ids for the same occurrence and both would run.
        => string.Create(CultureInfo.InvariantCulture, $"sched:{name}:{occurrence.UtcDateTime:yyyyMMdd'T'HHmmss'Z'}");
}
