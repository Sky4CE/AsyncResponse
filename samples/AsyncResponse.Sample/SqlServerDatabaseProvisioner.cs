using Microsoft.Data.SqlClient;

namespace AsyncResponse.Sample;

/// <summary>
/// Creates the SQL Server database named by the connection string if it does not exist yet
/// (container-and-development convenience; production databases are provisioned out of band).
/// The AsyncResponse SQL Server packages create their own schema and tables, but they cannot
/// connect to a database that is missing entirely. Registered before the AsyncResponse wiring so
/// its <see cref="StartAsync"/> completes before any subscriber touches the database, and retries
/// while a freshly started SQL Server container is still warming up.
/// </summary>
internal sealed class SqlServerDatabaseProvisioner(
    IConfiguration configuration,
    ILogger<SqlServerDatabaseProvisioner> logger) : IHostedService
{
    // 1801 = "Database ... already exists": another app instance won the CREATE DATABASE race.
    private const int DatabaseAlreadyExists = 1801;

    private static readonly TimeSpan StartupBudget = TimeSpan.FromMinutes(2);
    private static readonly TimeSpan RetryDelay = TimeSpan.FromSeconds(2);

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        var connectionString = configuration.GetConnectionString("SqlServer")
            ?? configuration["SqlServer:ConnectionString"];
        if (string.IsNullOrWhiteSpace(connectionString))
            return;

        var builder = new SqlConnectionStringBuilder(connectionString);
        var database = builder.InitialCatalog;
        if (string.IsNullOrWhiteSpace(database) || string.Equals(database, "master", StringComparison.OrdinalIgnoreCase))
            return;

        builder.InitialCatalog = "master";
        var deadline = DateTimeOffset.UtcNow + StartupBudget;
        while (true)
        {
            try
            {
                await using var connection = new SqlConnection(builder.ConnectionString);
                await connection.OpenAsync(cancellationToken);
                await using var command = connection.CreateCommand();
                command.CommandText = $"IF DB_ID(N'{database.Replace("'", "''")}') IS NULL CREATE DATABASE [{database.Replace("]", "]]")}];";
                await command.ExecuteNonQueryAsync(cancellationToken);
                logger.LogInformation("SQL Server database {Database} is ready.", database);
                return;
            }
            catch (SqlException ex) when (ex.Number == DatabaseAlreadyExists)
            {
                return; // another instance created it between the check and the CREATE
            }
            catch (Exception ex) when (ex is not OperationCanceledException && DateTimeOffset.UtcNow < deadline)
            {
                logger.LogInformation(ex, "SQL Server not ready yet; retrying database provisioning in {Delay}.", RetryDelay);
                await Task.Delay(RetryDelay, cancellationToken);
            }
        }
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
