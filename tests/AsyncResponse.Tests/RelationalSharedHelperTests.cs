using System.Reflection;
using AsyncResponse.Channels.PostgreSQL;
using AsyncResponse.Channels.SqlServer;
using AsyncResponse.Transports.PostgreSQL;
using AsyncResponse.Transports.SqlServer;
using Microsoft.Data.SqlClient;
using Npgsql;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Behaviour pins for the relational helpers that used to be copy-pasted per package: the
/// transient-fault classifiers (one curated SQL Server error table, one Npgsql predicate) and the
/// name-plan distinctness loop. Each was duplicated across the channel and transport packages, so
/// these facts assert the two entry points still agree — and that every rendered error message is
/// byte-for-byte what its own options type produced before the extraction.
/// </summary>
public sealed class RelationalSharedHelperTests
{
    /// <summary>Every error number the per-package tables listed, all of which must stay transient.</summary>
    public static TheoryData<int> CuratedTransientErrorNumbers =>
    [
        -2, 20, 64, 121, 233, 997, 1204, 1205, 1222, 4060, 4221, 10053, 10054, 10060,
        10928, 10929, 40143, 40197, 40501, 40540, 40613, 49918, 49919, 49920
    ];

    [Theory]
    [MemberData(nameof(CuratedTransientErrorNumbers))]
    public void SqlServerChannelAndTransport_ClassifyEveryCuratedNumberTheSameWay(int number)
    {
        var failure = SqlExceptionWith(number);

        Assert.True(SqlServerChannelSql.IsTransient(failure));
        Assert.True(SqlServerTransportRetry.IsTransient(failure));
    }

    [Theory]
    [InlineData(2627)] // PRIMARY KEY/UNIQUE violation — the publish path's idempotency signal
    [InlineData(2601)] // unique index violation
    [InlineData(207)]  // invalid column name: a schema fault, retrying it only wastes the budget
    [InlineData(2714)] // name already in use
    [InlineData(8134)] // divide by zero
    public void SqlServerChannelAndTransport_LeavePermanentErrorsPermanent(int number)
    {
        var failure = SqlExceptionWith(number);

        Assert.False(SqlServerChannelSql.IsTransient(failure));
        Assert.False(SqlServerTransportRetry.IsTransient(failure));
    }

    [Fact]
    public void SqlServerClassifier_KeepsSeverityAndNonProviderVerdicts()
    {
        // Severity ≥ 20 is a broken connection whatever the number says.
        var brokenConnection = SqlExceptionWith(207, errorClass: 20);
        Assert.True(SqlServerChannelSql.IsTransient(brokenConnection));
        Assert.True(SqlServerTransportRetry.IsTransient(brokenConnection));

        Assert.True(SqlServerChannelSql.IsTransient(new TimeoutException()));
        Assert.True(SqlServerTransportRetry.IsTransient(new TimeoutException()));

        // Cancellation is the caller's decision, never a fault to retry.
        Assert.False(SqlServerChannelSql.IsTransient(new OperationCanceledException()));
        Assert.False(SqlServerTransportRetry.IsTransient(new OperationCanceledException()));
        Assert.False(SqlServerChannelSql.IsTransient(new InvalidOperationException()));
        Assert.False(SqlServerTransportRetry.IsTransient(new InvalidOperationException()));
    }

    [Fact]
    public void PostgreSqlChannelAndTransport_ClassifyTheSameWay()
    {
        var transientDriverFailure = new NpgsqlException("network", new TimeoutException());
        var permanentDriverFailure = new NpgsqlException("bad request");

        foreach (var (exception, transient) in new (Exception Exception, bool Transient)[]
        {
            (transientDriverFailure, true),
            (new TimeoutException(), true),
            (permanentDriverFailure, false),
            (new OperationCanceledException(), false),
            (new InvalidOperationException(), false),
        })
        {
            Assert.Equal(transient, PostgreSqlChannelSql.IsTransient(exception));
            Assert.Equal(transient, PostgreSqlTransportRetry.IsTransient(exception));
        }
    }

    [Fact]
    public void PostgreSqlChannelNamePlan_RendersItsOwnGuidance()
    {
        var options = new PostgreSqlAsyncResponseChannelOptions
        {
            RecoveryStateTable = "shared_tbl",
            MessageTable = "shared_tbl"
        };

        var ex = Assert.Throws<InvalidOperationException>(() => PostgreSqlChannelSql.ValidateNamePlan(options));
        Assert.Equal(
            "PostgreSqlAsyncResponseChannelOptions: the RecoveryStateTable table and the MessageTable table both resolve to 'shared_tbl'. " +
            "All tables and the index/sequence names derived from them share one namespace and must be distinct " +
            "(long names reserve suffix space by truncating the table stem, which can make distinct tables derive " +
            "the same name). Shorten or de-overlap the configured table names.",
            ex.Message);
    }

    [Fact]
    public void SqlServerChannelNamePlan_RendersItsOwnGuidance()
    {
        var options = new SqlServerAsyncResponseChannelOptions
        {
            RecoveryStateTable = "shared_tbl",
            MessageTable = "shared_tbl"
        };

        var ex = Assert.Throws<InvalidOperationException>(() => SqlServerChannelSql.ValidateNamePlan(options));
        Assert.Equal(
            "SqlServerAsyncResponseChannelOptions: the RecoveryStateTable table and the MessageTable table both resolve to 'shared_tbl'. " +
            "Tables and the sequence derived from MessageTable share one schema-object namespace and must be distinct " +
            "(long names reserve suffix space by truncating the table stem). Shorten or de-overlap the configured table names.",
            ex.Message);
    }

    [Fact]
    public void PostgreSqlTransportNamePlan_RendersItsOwnGuidance()
    {
        // A maximum-length table name whose stem truncates exactly onto its own claim-index name:
        // the only way this package's single configured name can collide with a derived one.
        var messageTable = new string('a', 53) + "_claim_idx";
        var options = new PostgreSqlAsyncResponseTransportOptions { MessageTable = messageTable };

        var ex = Assert.Throws<InvalidOperationException>(() => PostgreSqlTransportOptionsValidator.ValidateCommon(options));
        Assert.Equal(
            "PostgreSqlAsyncResponseTransportOptions: the MessageTable table and the claim index (derived from MessageTable) both resolve to " +
            $"'{messageTable}'; rename MessageTable so the derived index names stay distinct.",
            ex.Message);
    }

    /// <summary>
    /// Builds a <see cref="SqlException"/> carrying one error. SqlClient exposes no public
    /// constructor, and its internal signatures differ across versions, so the arguments are
    /// matched by name and type rather than positionally.
    /// </summary>
    private static SqlException SqlExceptionWith(int number, byte errorClass = 0)
    {
        var errorConstructor = typeof(SqlError).GetConstructors(BindingFlags.NonPublic | BindingFlags.Instance)[0];
        var errorParameters = errorConstructor.GetParameters();
        var errorArgs = new object?[errorParameters.Length];
        for (var i = 0; i < errorParameters.Length; i++)
        {
            var parameter = errorParameters[i];
            if (parameter.Name == "infoNumber")
                errorArgs[i] = number;
            else if (parameter.Name == "errorClass")
                errorArgs[i] = errorClass;
            else if (parameter.ParameterType == typeof(string))
                errorArgs[i] = "";
            else if (parameter.ParameterType == typeof(int))
                errorArgs[i] = 0;
            else if (parameter.ParameterType == typeof(uint))
                errorArgs[i] = 0U;
            else if (parameter.ParameterType == typeof(byte))
                errorArgs[i] = (byte)0;
            else
                errorArgs[i] = null;
        }

        var errors = (SqlErrorCollection)typeof(SqlErrorCollection)
            .GetConstructors(BindingFlags.NonPublic | BindingFlags.Instance)[0]
            .Invoke([]);
        typeof(SqlErrorCollection)
            .GetMethod("Add", BindingFlags.NonPublic | BindingFlags.Instance, null, [typeof(SqlError)], null)!
            .Invoke(errors, [(SqlError)errorConstructor.Invoke(errorArgs)]);

        var exceptionConstructor = typeof(SqlException).GetConstructors(BindingFlags.NonPublic | BindingFlags.Instance)[0];
        var exceptionParameters = exceptionConstructor.GetParameters();
        var exceptionArgs = new object?[exceptionParameters.Length];
        for (var i = 0; i < exceptionParameters.Length; i++)
        {
            var parameter = exceptionParameters[i];
            if (parameter.ParameterType == typeof(string))
                exceptionArgs[i] = "message";
            else if (parameter.ParameterType == typeof(SqlErrorCollection))
                exceptionArgs[i] = errors;
            else if (parameter.ParameterType == typeof(Guid))
                exceptionArgs[i] = Guid.Empty;
            else
                exceptionArgs[i] = null;
        }

        return (SqlException)exceptionConstructor.Invoke(exceptionArgs);
    }
}
