using System.Diagnostics;
using Microsoft.Extensions.Logging;

namespace AsyncResponse.Internal;

/// <summary>
/// The one shape every opportunistic housekeeping prune of the database channels and transports
/// shares (shared source, compiled into the PostgreSQL/SQL Server channel packages and the three
/// database transports): a monotonic throttle deciding WHEN a prune runs, and a budgeted, guarded
/// batch loop deciding HOW MUCH it deletes and that it never fails the operation it rides on.
/// Mirrors <c>DurableFlowStoreShared.PruneQuietlyAsync</c> (which keeps its own flow-state metrics).
/// </summary>
internal static class OpportunisticPrune
{
    /// <summary>
    /// Rows one prune statement deletes: well under SQL Server's ~5,000-lock escalation threshold
    /// (a table lock READPAST cannot skip would stall every claim sharing the table), and small
    /// enough that no single statement nears a command timeout.
    /// </summary>
    public const int BatchSize = 1000;

    /// <summary>
    /// Time one prune may spend draining batches after the first (durable-flow
    /// <c>DefaultPruneBudget</c> parity). One batch per window was a hard throughput ceiling —
    /// 1,000 rows per 30 s channel window, per one-minute dead-letter window — which any
    /// deployment producing more than that outgrew forever; an unbounded statement instead held a
    /// publish for the whole command timeout on a large backlog, rolled back, and never shrank it.
    /// </summary>
    public static readonly TimeSpan DefaultBudget = TimeSpan.FromSeconds(2);

    /// <summary>
    /// Throttles a prune to at most once per <paramref name="interval"/> (a non-positive interval
    /// prunes on every call). <paramref name="lastStamp"/> holds a <see cref="Stopwatch"/> timestamp,
    /// NOT wall-clock ticks: with <c>DateTime.UtcNow</c> a backward system-clock step (VM restore,
    /// NTP step) kept <c>now - last</c> below the interval for the size of the step, suspending the
    /// housekeeping on that process for as long. <c>0</c> means "never pruned", so the first call
    /// always runs — Stopwatch counts from boot, and on a freshly booted machine a real stamp can
    /// be smaller than one interval.
    /// </summary>
    public static bool ShouldRun(ref long lastStamp, TimeSpan interval)
    {
        if (interval <= TimeSpan.Zero)
            return true;

        var now = Stopwatch.GetTimestamp();
        var last = Interlocked.Read(ref lastStamp);
        if (last != 0 && Stopwatch.GetElapsedTime(last, now) < interval)
            return false;

        // Never store the "never pruned" sentinel itself.
        var stamp = now == 0 ? 1 : now;
        return Interlocked.CompareExchange(ref lastStamp, stamp, last) == last;
    }

    /// <summary>
    /// Runs <paramref name="pruneBatch"/> until a batch comes back short of <see cref="BatchSize"/>
    /// (the backlog is gone) or <paramref name="budget"/> (default <see cref="DefaultBudget"/>)
    /// lapses; the first batch always runs. Every failure — cancellation included — is logged and
    /// swallowed: the prune rides on a publish, a registration, or a probe whose own work is (or is
    /// about to be) done, and a prune that threw failed it — on a transport publish, AFTER the row
    /// had committed, so the caller's retry ran the job twice. Reads filter on expiry, so a skipped
    /// prune costs nothing but disk until the next window. A lapsed budget with rows remaining is a
    /// warning: the backlog is outgrowing the prune.
    /// </summary>
    /// <param name="pruneBatch">Deletes one bounded batch and returns the rows it deleted.</param>
    /// <param name="logger">The store's logger, when it has one.</param>
    /// <param name="description">Log subject, e.g. "PostgreSQL transport dead-letter prune".</param>
    /// <param name="cancellationToken">The caller's token, handed to every batch.</param>
    /// <param name="budget">Time for batches after the first; <see cref="DefaultBudget"/> when omitted.</param>
    public static async Task DrainQuietlyAsync(
        Func<CancellationToken, Task<int>> pruneBatch,
        ILogger? logger,
        string description,
        CancellationToken cancellationToken,
        TimeSpan? budget = null)
    {
        var limit = budget ?? DefaultBudget;
        var started = Stopwatch.GetTimestamp();
        var deleted = 0L;
        var batches = 0;
        try
        {
            while (true)
            {
                var batchDeleted = await pruneBatch(cancellationToken).ConfigureAwait(false);
                batches++;
                deleted += Math.Max(batchDeleted, 0);
                if (batchDeleted < BatchSize)
                    return;

                if (Stopwatch.GetElapsedTime(started) >= limit)
                {
                    logger?.LogWarning(
                        "{Prune} deleted {Deleted} expired rows in {Batches} batches and stopped at its {Budget} budget with expired rows remaining; the rest drain in the next windows.",
                        description,
                        deleted,
                        batches,
                        limit);
                    return;
                }
            }
        }
        catch (OperationCanceledException ex) when (cancellationToken.IsCancellationRequested)
        {
            // The caller is going away; its own next statement observes the same token.
            logger?.LogDebug(ex, "{Prune} was cancelled after deleting {Deleted} expired rows in {Batches} batches.", description, deleted, batches);
        }
        catch (Exception ex)
        {
            logger?.LogWarning(
                ex,
                "{Prune} failed after deleting {Deleted} expired rows in {Batches} batches; the operation it rode on is unaffected and the next window retries.",
                description,
                deleted,
                batches);
        }
    }
}
