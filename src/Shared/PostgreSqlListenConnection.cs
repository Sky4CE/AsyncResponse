using System.Data;
using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace AsyncResponse.Internal;

/// <summary>
/// Hands a <c>LISTEN</c> connection back to its pool without its subscription, within a bounded
/// wait. Source-linked into the PostgreSQL channel and transport packages (separate packages
/// cannot share compiled code), whose listen loops each hold one pooled connection per
/// <c>LISTEN</c>.
/// <para>
/// Npgsql keeps a connector usable after a cancelled <c>WaitAsync</c>, and with
/// <c>No Reset On Close=true</c> (which docs/postgresql.md recommends) the pool never runs
/// <c>DISCARD ALL</c> on it — so a torn-down listener's connection went back still listening,
/// and every later NOTIFY queued up on an idle pooled backend (or was drained, unseen, by its next
/// user). Hence the <c>UNLISTEN *</c>. But awaited in full on a half-open socket (still
/// <c>Open</c> as far as the client knows), it held the teardown — a channel's
/// <c>DisposeAsync</c>, a transport subscriber's restart — for the command timeout, then Npgsql's
/// cancel request on a fresh socket, then its cancellation timeout: about 7 s, and up to about
/// 22 s against a host that black-holes the cancel connect.
/// </para>
/// </summary>
internal static class PostgreSqlListenConnection
{
    /// <summary>How long a listener's teardown waits for its <c>UNLISTEN</c> before leaving it to finish in the background.</summary>
    internal static readonly TimeSpan UnlistenBudget = TimeSpan.FromSeconds(1);

    /// <summary>Seconds the <c>UNLISTEN</c> itself may run (in the background past the budget) before Npgsql times it out.</summary>
    private const int UnlistenCommandTimeoutSeconds = 5;

    /// <summary>
    /// Seconds the one retry of a failed <c>UNLISTEN</c> may run, on a connection still open after
    /// an established <c>LISTEN</c>, before the pool is cleared instead.
    /// </summary>
    private const int UnlistenRetryCommandTimeoutSeconds = 30;

    /// <summary>How often a pool clear is logged at Warning; the ones in between log at Debug.</summary>
    internal static readonly TimeSpan PoolClearWarningInterval = TimeSpan.FromMinutes(1);

    // Stopwatch timestamp of the last pool clear logged at Warning; 0 = none yet.
    private static long _lastPoolClearWarningAt;

    /// <summary>
    /// Runs <c>UNLISTEN *</c> and disposes <paramref name="connection"/> (opened from
    /// <paramref name="dataSource"/>), waiting at most <see cref="UnlistenBudget"/>. Takes
    /// ownership of the connection: the caller must not dispose it. <paramref name="listening"/>
    /// says whether the <c>LISTEN</c> was established on it — only then can a failed
    /// <c>UNLISTEN</c> leave it listening. A pool clear is reported to <paramref name="logger"/>.
    /// Never throws, so it cannot replace an exception already unwinding.
    /// </summary>
    public static Task ReleaseAsync(NpgsqlDataSource dataSource, NpgsqlConnection connection, bool listening, ILogger? logger = null)
        => ReleaseAsync(dataSource, connection, listening, logger, UnlistenBudget, UnlistenCommandTimeoutSeconds, UnlistenRetryCommandTimeoutSeconds);

    internal static async Task ReleaseAsync(
        NpgsqlDataSource dataSource,
        NpgsqlConnection connection,
        bool listening,
        ILogger? logger,
        TimeSpan budget,
        int commandTimeoutSeconds,
        int retryCommandTimeoutSeconds)
    {
        // The release owns the connection end to end, so the teardown can stop waiting without
        // the connection being disposed under a command still in flight on it.
        var release = UnlistenThenDisposeAsync(dataSource, connection, listening, logger, commandTimeoutSeconds, retryCommandTimeoutSeconds);
        using var stopWaiting = new CancellationTokenSource();
        await Task.WhenAny(release, Task.Delay(budget, stopWaiting.Token)).ConfigureAwait(false);
        await stopWaiting.CancelAsync().ConfigureAwait(false);
    }

    private static async Task UnlistenThenDisposeAsync(
        NpgsqlDataSource dataSource,
        NpgsqlConnection connection,
        bool listening,
        ILogger? logger,
        int commandTimeoutSeconds,
        int retryCommandTimeoutSeconds)
    {
        var (unlistened, unlistenFailure) = await TryUnlistenAsync(connection, commandTimeoutSeconds).ConfigureAwait(false);

        // One retry, with a longer timeout, before the pool is cleared: an overloaded server that
        // timed out the listener's liveness probe (and honoured its cancel, so the connection is
        // still open) usually timed out the first UNLISTEN too, and clearing the application's
        // whole pool on every listen cycle — about every 15 s while the overload lasted — made
        // every connection of a struggling server reconnect. It already runs in the background
        // past the teardown's budget, so nothing waits for it.
        if (listening && !unlistened && connection.State == ConnectionState.Open)
            (unlistened, unlistenFailure) = await TryUnlistenAsync(connection, retryCommandTimeoutSeconds).ConfigureAwait(false);

        var clear = listening && !unlistened && connection.State == ConnectionState.Open;
        try
        {
            // A connection Npgsql broke (the half-open socket: its timed-out UNLISTEN's cancel got
            // no answer) is closed rather than pooled on disposal. One still open but still
            // listening — the server answered the UNLISTEN with an error, or honoured the cancel
            // of a timed-out one — must not go back either, and clearing its data source's pool
            // is the only public way to keep it out: connections opened before the clear are
            // closed when returned (idle ones at once), so the rest of the pool reconnects too.
            // (NpgsqlConnection.ClearPool would not do: it clears the global connection-string
            // pool, which a data source's connections are not in.) Rare: a server that answers
            // runs UNLISTEN in microseconds, even on a hot standby, and one too overloaded to has
            // had the retry above to do it.
            //
            // But the pool is the application's own (the host registers one shared data source),
            // so the clear drops every idle connection EF Core and everything else holds, and makes
            // them all reconnect. Only a connection whose LISTEN was established can still be
            // listening: when the LISTEN itself failed there is nothing to keep out, and a server
            // that also rejects the UNLISTEN no longer clears the pool on every retry.
            if (clear)
                dataSource.Clear();
            await connection.DisposeAsync().ConfigureAwait(false);
        }
        catch (Exception)
        {
            // Disposal breaks or returns the connection either way.
        }

        // Reported only once the connection is disposed, and never allowed to throw: a logging
        // provider that throws (Microsoft.Extensions.Logging rethrows a provider's failure) must
        // not skip the disposal above, which would leak the still-listening connection's pool
        // slot and keep its backend receiving every NOTIFY.
        if (clear)
            LogPoolClear(logger, unlistenFailure);
    }

    /// <summary>One <c>UNLISTEN *</c> on an open connection: whether it completed, and why not.</summary>
    private static async Task<(bool Unlistened, Exception? Failure)> TryUnlistenAsync(NpgsqlConnection connection, int commandTimeoutSeconds)
    {
        try
        {
            if (connection.State != ConnectionState.Open)
                return (false, null);

            await using var unlisten = connection.CreateCommand();
            unlisten.CommandText = "UNLISTEN *;";
            unlisten.CommandTimeout = commandTimeoutSeconds;
            await unlisten.ExecuteNonQueryAsync(CancellationToken.None).ConfigureAwait(false);
            return (true, null);
        }
        catch (Exception ex)
        {
            // Judged by the caller through the connection's state.
            return (false, ex);
        }
    }

    private static void LogPoolClear(ILogger? logger, Exception? unlistenFailure)
    {
        if (logger is null)
            return;

        var now = Stopwatch.GetTimestamp();
        var last = Interlocked.Read(ref _lastPoolClearWarningAt);
        var warn = last == 0 || Stopwatch.GetElapsedTime(last, now) >= PoolClearWarningInterval;
        if (warn)
            Interlocked.Exchange(ref _lastPoolClearWarningAt, now == 0 ? 1 : now);

        SafeLog.Try((Logger: logger, Error: unlistenFailure, Warn: warn), static state =>
        {
            if (state.Warn)
            {
                state.Logger.LogWarning(
                    state.Error,
                    "PostgreSQL UNLISTEN did not complete, even on its retry, on a LISTEN connection that is still open (the server refused it or was too slow to answer); " +
                    "cleared the NpgsqlDataSource's connection pool so the still-listening connection is not reused, which makes every connection of that data source reconnect. " +
                    "Further clears within {Interval} log at Debug.",
                    PoolClearWarningInterval);
            }
            else
            {
                state.Logger.LogDebug(
                    state.Error,
                    "PostgreSQL UNLISTEN did not complete on a LISTEN connection that is still open; cleared the NpgsqlDataSource's connection pool again.");
            }
        });
    }
}
