using System.Data;
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
    /// Runs <c>UNLISTEN *</c> and disposes <paramref name="connection"/> (opened from
    /// <paramref name="dataSource"/>), waiting at most <see cref="UnlistenBudget"/>. Takes
    /// ownership of the connection: the caller must not dispose it. <paramref name="listening"/>
    /// says whether the <c>LISTEN</c> was established on it — only then can a failed
    /// <c>UNLISTEN</c> leave it listening. Never throws, so it cannot replace an exception already
    /// unwinding.
    /// </summary>
    public static Task ReleaseAsync(NpgsqlDataSource dataSource, NpgsqlConnection connection, bool listening)
        => ReleaseAsync(dataSource, connection, listening, UnlistenBudget, UnlistenCommandTimeoutSeconds);

    internal static async Task ReleaseAsync(
        NpgsqlDataSource dataSource,
        NpgsqlConnection connection,
        bool listening,
        TimeSpan budget,
        int commandTimeoutSeconds)
    {
        // The release owns the connection end to end, so the teardown can stop waiting without
        // the connection being disposed under a command still in flight on it.
        var release = UnlistenThenDisposeAsync(dataSource, connection, listening, commandTimeoutSeconds);
        using var stopWaiting = new CancellationTokenSource();
        await Task.WhenAny(release, Task.Delay(budget, stopWaiting.Token)).ConfigureAwait(false);
        await stopWaiting.CancelAsync().ConfigureAwait(false);
    }

    private static async Task UnlistenThenDisposeAsync(
        NpgsqlDataSource dataSource,
        NpgsqlConnection connection,
        bool listening,
        int commandTimeoutSeconds)
    {
        var unlistened = false;
        try
        {
            if (connection.State == ConnectionState.Open)
            {
                await using var unlisten = connection.CreateCommand();
                unlisten.CommandText = "UNLISTEN *;";
                unlisten.CommandTimeout = commandTimeoutSeconds;
                await unlisten.ExecuteNonQueryAsync(CancellationToken.None).ConfigureAwait(false);
                unlistened = true;
            }
        }
        catch (Exception)
        {
            // Judged below by the connection's state.
        }

        try
        {
            // A connection Npgsql broke (the half-open socket: its timed-out UNLISTEN's cancel got
            // no answer) is closed rather than pooled on disposal. One still open but still
            // listening — the server answered the UNLISTEN with an error, or honoured the cancel
            // of a timed-out one — must not go back either, and clearing its data source's pool
            // is the only public way to keep it out: connections opened before the clear are
            // closed when returned (idle ones at once), so the rest of the pool reconnects too.
            // (NpgsqlConnection.ClearPool would not do: it clears the global connection-string
            // pool, which a data source's connections are not in.) Rare by construction: a
            // server that answers at all runs UNLISTEN in microseconds, even on a hot standby.
            //
            // But the pool is the application's own (the host registers one shared data source),
            // so the clear drops every idle connection EF Core and everything else holds, and makes
            // them all reconnect. Only a connection whose LISTEN was established can still be
            // listening: when the LISTEN itself failed there is nothing to keep out, and a server
            // that also rejects the UNLISTEN no longer clears the pool on every retry.
            if (listening && !unlistened && connection.State == ConnectionState.Open)
                dataSource.Clear();
            await connection.DisposeAsync().ConfigureAwait(false);
        }
        catch (Exception)
        {
            // Disposal breaks or returns the connection either way.
        }
    }
}
