using AsyncResponse.DurableFlows.Oracle;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// The Oracle startup-verification decision table, unit-tested against the store's internal
/// catalog-row model so the accept/reject logic runs on every platform. The integration suite
/// proves the same decisions against a real server's USER_TAB_COLS / USER_CONSTRAINTS /
/// NLS_SESSION_PARAMETERS, but only where the Oracle container runs; these tests pin the logic
/// itself — the sibling MySQL store's r18–r20 lessons (prefix keys, charset vs collation, extra
/// NOT NULL columns) each began as a decision this table would have caught.
/// </summary>
public sealed class OracleFlowTableVerificationTests
{
    [Theory]
    // NLS_COMP=BINARY is ordinal no matter what NLS_SORT says: comparison consults the sort only
    // under LINGUISTIC (or the deprecated ANSI).
    [InlineData("BINARY", "BINARY", false)]
    [InlineData("BINARY", "GERMAN", false)]
    [InlineData("LINGUISTIC", "BINARY", false)]
    [InlineData("binary", "binary", false)]
    // BINARY_CI is the trap this check exists for: it SOUNDS binary, folds case, and leaves the
    // primary-key index binary — both casings insert, and every predicate matches both.
    [InlineData("LINGUISTIC", "BINARY_CI", true)]
    [InlineData("LINGUISTIC", "BINARY_AI", true)]
    [InlineData("LINGUISTIC", "GERMAN", true)]
    [InlineData("ANSI", "FRENCH", true)]
    // A session whose parameters could not be read is not known to be ordinal; silence must not pass.
    [InlineData(null, null, true)]
    public void DiagnoseComparisonSemantics_RequiresBinaryCompOrBinarySort(string? comp, string? sort, bool rejected)
    {
        var diagnosis = OracleFlowStateStore.DiagnoseComparisonSemantics(comp, sort);
        if (!rejected)
        {
            Assert.Null(diagnosis);
            return;
        }

        Assert.NotNull(diagnosis);
        // The message must name the offending values and the remediation, not just say "bad".
        Assert.Contains($"NLS_COMP='{comp ?? "(unknown)"}'", diagnosis, StringComparison.Ordinal);
        Assert.Contains($"NLS_SORT='{sort ?? "(unknown)"}'", diagnosis, StringComparison.Ordinal);
        Assert.Contains("ALTER SESSION SET NLS_COMP = BINARY", diagnosis, StringComparison.Ordinal);
    }

    [Fact]
    public void Mismatch_AcceptsTheShapeTheStoresOwnDdlCreates()
    {
        // USER_TAB_COLS rows for the exact table EnsureCreatedAsync creates; a false alarm here
        // would fail every healthy deployment at startup.
        Assert.Null(Expected("flow_id").Mismatch(Column("NVARCHAR2", charLength: 400)));
        Assert.Null(Expected("state_json").Mismatch(Column("NCLOB", charLength: 0)));
        Assert.Null(Expected("expires_at_utc").Mismatch(Column("TIMESTAMP(6)", scale: 6)));
        Assert.Null(Expected("updated_at_utc").Mismatch(Column("TIMESTAMP(6)", scale: 6)));
        Assert.Null(Expected("revision").Mismatch(Column("NUMBER", precision: 19, scale: 0, hasDefault: true)));
        Assert.Null(Expected("lease_id").Mismatch(Column("NVARCHAR2", nullable: true, charLength: 64)));
        Assert.Null(Expected("lease_expires_at_utc").Mismatch(Column("TIMESTAMP(6)", nullable: true, scale: 6)));
    }

    [Fact]
    public void Mismatch_AcceptsWiderColumnsButNothingNarrower()
    {
        // Widths are minima: a more generous hand-written schema keeps every promise the store
        // makes, and rejecting it would be a false alarm.
        Assert.Null(Expected("flow_id").Mismatch(Column("NVARCHAR2", charLength: 2000)));
        Assert.Null(Expected("expires_at_utc").Mismatch(Column("TIMESTAMP(9)", scale: 9)));
        Assert.Null(Expected("revision").Mismatch(Column("NUMBER", precision: 38)));
        // An unconstrained NUMBER reports null precision and holds 38 digits — wider, not unknown.
        Assert.Null(Expected("revision").Mismatch(Column("NUMBER")));

        // Too narrow truncates or errors on ids and math the public contract permits.
        Assert.Equal(
            "holds 100 where at least 400 is required",
            Expected("flow_id").Mismatch(Column("NVARCHAR2", charLength: 100)));
        Assert.Equal(
            "holds 0 where at least 6 is required",
            Expected("expires_at_utc").Mismatch(Column("TIMESTAMP(0)", scale: 0)));
        Assert.Equal(
            "holds 9 where at least 19 is required",
            Expected("revision").Mismatch(Column("NUMBER", precision: 9)));
    }

    [Fact]
    public void Mismatch_RejectsTypesWhoseAlphabetOrClockSemanticsDiffer()
    {
        // VARCHAR2's alphabet is the database character set, which this store does not control;
        // on a non-AL32UTF8 database a legal flow id mangles or fails on insert (the same reason
        // the MySQL sibling names utf8mb4 instead of "any Unicode set").
        Assert.Equal("is a 'VARCHAR2'", Expected("flow_id").Mismatch(Column("VARCHAR2", charLength: 400)));
        Assert.Equal("is a 'CLOB'", Expected("state_json").Mismatch(Column("CLOB", charLength: 0)));

        // The time-zone variants shift with the session time zone; expiry and lease math must
        // stay on the plain UTC timestamps this store writes. DATE has no fractional seconds.
        Assert.Equal(
            "is a 'TIMESTAMP(6) WITH TIME ZONE'",
            Expected("expires_at_utc").Mismatch(Column("TIMESTAMP(6) WITH TIME ZONE", scale: 6)));
        Assert.Equal(
            "is a 'TIMESTAMP(6) WITH LOCAL TIME ZONE'",
            Expected("lease_expires_at_utc").Mismatch(Column("TIMESTAMP(6) WITH LOCAL TIME ZONE", nullable: true, scale: 6)));
        Assert.Equal("is a 'DATE'", Expected("updated_at_utc").Mismatch(Column("DATE")));
        Assert.Equal("is a 'BINARY_DOUBLE'", Expected("revision").Mismatch(Column("BINARY_DOUBLE")));
    }

    [Fact]
    public void Mismatch_RejectsNullabilityFlipsInBothDirections()
    {
        // A nullable flow_id admits rows the unique key does not police; a NOT NULL lease_id
        // rejects the NULL every release writes.
        Assert.Equal("is nullable", Expected("flow_id").Mismatch(Column("NVARCHAR2", nullable: true, charLength: 400)));
        Assert.Equal(
            "is NOT NULL (this store writes NULL to it)",
            Expected("lease_id").Mismatch(Column("NVARCHAR2", charLength: 64)));
    }

    [Theory]
    // An extra NOT NULL column with no default: the store never names it, so EVERY create fails.
    [InlineData(false, false, false, false, false)]
    // Anything the database can fill in for itself is harmless bookkeeping.
    [InlineData(true, false, false, false, true)]
    [InlineData(false, true, false, false, true)]
    [InlineData(false, false, true, false, true)]
    [InlineData(false, false, false, true, true)]
    public void IsWritableWithoutValue_AcceptsOnlyColumnsInsertsCanOmit(
        bool nullable, bool hasDefault, bool virtualColumn, bool identity, bool writable)
    {
        var column = Column("NUMBER", nullable: nullable, hasDefault: hasDefault, virtualColumn: virtualColumn, identity: identity);
        Assert.Equal(writable, column.IsWritableWithoutValue);
    }

    [Fact]
    public void ExpectedColumns_CoverEveryColumnTheStoreReadsOrWrites()
    {
        // The verifier's shape and the store's SQL must not drift apart: a column the SQL names
        // but the verifier does not check re-opens the silent-mismatch gap this fix closed.
        Assert.Equal(
            ["flow_id", "state_json", "expires_at_utc", "updated_at_utc", "revision", "lease_id", "lease_expires_at_utc"],
            OracleFlowStateStore.ExpectedColumns.Select(column => column.Name));
    }

    private static OracleFlowStateStore.ExpectedColumn Expected(string name)
        => OracleFlowStateStore.ExpectedColumns.Single(column => column.Name == name);

    private static OracleFlowStateStore.ActualColumn Column(
        string dataType,
        bool nullable = false,
        long? charLength = null,
        long? precision = null,
        long? scale = null,
        bool hasDefault = false,
        bool virtualColumn = false,
        bool identity = false)
        => new(dataType, nullable, charLength, precision, scale, hasDefault, virtualColumn, identity);
}
