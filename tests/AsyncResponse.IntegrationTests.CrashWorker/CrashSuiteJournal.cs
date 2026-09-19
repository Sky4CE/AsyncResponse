using Npgsql;

namespace AsyncResponse.IntegrationTests.CrashWorker;

/// <summary>
/// The scenario's observable record, written to PostgreSQL so it survives the process that wrote it:
/// which worker executed which step how often, and every execution-lease acquisition attempt with
/// its outcome. Each write is one autocommitted statement — durable the moment it returns, which is
/// what lets a crash point kill the process on the very next line.
/// </summary>
internal sealed class CrashSuiteJournal(NpgsqlDataSource dataSource, CrashWorkerSettings settings)
{
    private string Effects => Qualified(CrashWorkerContract.Tables.Effects);
    private string LeaseJournal => Qualified(CrashWorkerContract.Tables.LeaseJournal);

    public async Task EnsureCreatedAsync(CancellationToken cancellationToken)
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText =
            $"""
            CREATE SCHEMA IF NOT EXISTS {Quote(settings.Schema)};
            CREATE TABLE IF NOT EXISTS {Effects} (
                flow_id text NOT NULL,
                step text NOT NULL,
                worker text NOT NULL,
                executions integer NOT NULL DEFAULT 1,
                first_at timestamptz NOT NULL DEFAULT now(),
                last_at timestamptz NOT NULL DEFAULT now(),
                PRIMARY KEY (flow_id, step, worker));
            CREATE TABLE IF NOT EXISTS {LeaseJournal} (
                id bigserial PRIMARY KEY,
                flow_id text NOT NULL,
                worker text NOT NULL,
                acquired boolean NOT NULL,
                at timestamptz NOT NULL DEFAULT now());
            """;
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// A step body's side effect. Idempotent in shape — one row per (flow, step, worker) — and
    /// counting in value, so a step that ran twice is visible as such instead of being absorbed.
    /// </summary>
    public async Task RecordEffectAsync(string flowId, string step)
    {
        await using var connection = await dataSource.OpenConnectionAsync().ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText =
            $"""
            INSERT INTO {Effects} (flow_id, step, worker) VALUES (@flow_id, @step, @worker)
            ON CONFLICT (flow_id, step, worker) DO UPDATE
            SET executions = {Effects}.executions + 1, last_at = now();
            """;
        command.Parameters.AddWithValue("flow_id", flowId);
        command.Parameters.AddWithValue("step", step);
        command.Parameters.AddWithValue("worker", settings.WorkerLabel);
        await command.ExecuteNonQueryAsync().ConfigureAwait(false);
    }

    public async Task RecordLeaseAttemptAsync(string flowId, bool acquired)
    {
        await using var connection = await dataSource.OpenConnectionAsync().ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = $"INSERT INTO {LeaseJournal} (flow_id, worker, acquired) VALUES (@flow_id, @worker, @acquired);";
        command.Parameters.AddWithValue("flow_id", flowId);
        command.Parameters.AddWithValue("worker", settings.WorkerLabel);
        command.Parameters.AddWithValue("acquired", acquired);
        await command.ExecuteNonQueryAsync().ConfigureAwait(false);
    }

    private string Qualified(string table) => $"{Quote(settings.Schema)}.{Quote(table)}";

    private static string Quote(string identifier) => "\"" + identifier.Replace("\"", "\"\"", StringComparison.Ordinal) + "\"";
}
