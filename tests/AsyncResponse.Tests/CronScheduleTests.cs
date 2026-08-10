using Xunit;

namespace AsyncResponse.Tests;

public class CronScheduleTests
{
    private static DateTimeOffset Utc(int year, int month, int day, int hour = 0, int minute = 0)
        => new(year, month, day, hour, minute, 0, TimeSpan.Zero);

    private static DateTimeOffset Next(string cron, DateTimeOffset after, TimeZoneInfo? zone = null)
        => CronSchedule.Parse(cron, zone).GetNextOccurrence(after)!.Value;

    [Fact]
    public void EveryMinute_AdvancesToTheNextWholeMinute()
    {
        Assert.Equal(Utc(2030, 1, 1, 0, 1), Next("* * * * *", Utc(2030, 1, 1, 0, 0)));
        // Mid-minute: seconds are truncated, so the next occurrence is the next minute boundary.
        Assert.Equal(
            Utc(2030, 1, 1, 0, 1),
            Next("* * * * *", new DateTimeOffset(2030, 1, 1, 0, 0, 30, TimeSpan.Zero)));
    }

    [Fact]
    public void FixedTime_DailySchedule()
    {
        Assert.Equal(Utc(2030, 1, 1, 3, 30), Next("30 3 * * *", Utc(2030, 1, 1, 0, 0)));
        Assert.Equal(Utc(2030, 1, 2, 3, 30), Next("30 3 * * *", Utc(2030, 1, 1, 3, 30)));
    }

    [Fact]
    public void Steps_Lists_And_Ranges()
    {
        Assert.Equal(Utc(2030, 1, 1, 0, 15), Next("*/15 * * * *", Utc(2030, 1, 1, 0, 0)));
        Assert.Equal(Utc(2030, 1, 1, 0, 45), Next("*/15 * * * *", Utc(2030, 1, 1, 0, 30)));
        Assert.Equal(Utc(2030, 1, 1, 1, 0), Next("*/15 * * * *", Utc(2030, 1, 1, 0, 45)));

        Assert.Equal(Utc(2030, 1, 1, 0, 5), Next("5,20,50 * * * *", Utc(2030, 1, 1, 0, 0)));
        Assert.Equal(Utc(2030, 1, 1, 0, 50), Next("5,20,50 * * * *", Utc(2030, 1, 1, 0, 20)));

        Assert.Equal(Utc(2030, 1, 1, 10, 16), Next("10-40/6 9-17 * * *", Utc(2030, 1, 1, 10, 11)));
        // 8/2 = "from 8 in steps of 2 to the max" (Vixie extension).
        Assert.Equal(Utc(2030, 1, 1, 0, 8), Next("8/2 * * * *", Utc(2030, 1, 1, 0, 7)));
        Assert.Equal(Utc(2030, 1, 1, 0, 58), Next("8/2 * * * *", Utc(2030, 1, 1, 0, 56)));
    }

    [Fact]
    public void WrapAroundRanges_CoverBothSegments()
    {
        // Hours 22-2: late evening through early morning.
        Assert.Equal(Utc(2030, 1, 1, 22, 0), Next("0 22-2 * * *", Utc(2030, 1, 1, 12, 0)));
        Assert.Equal(Utc(2030, 1, 2, 0, 0), Next("0 22-2 * * *", Utc(2030, 1, 1, 23, 0)));
        Assert.Equal(Utc(2030, 1, 2, 22, 0), Next("0 22-2 * * *", Utc(2030, 1, 2, 2, 0)));
    }

    [Fact]
    public void MonthAndDayNames_AreCaseInsensitive()
    {
        Assert.Equal(Utc(2030, 3, 7, 0, 0), Next("0 0 * MAR *", Utc(2030, 3, 6, 12, 0)));
        // First Monday of 2030 is Jan 7.
        Assert.Equal(Utc(2030, 1, 7, 9, 0), Next("0 9 * * mon", Utc(2030, 1, 5, 0, 0)));
    }

    [Fact]
    public void SundayIsBothZeroAndSeven()
    {
        // Jan 6, 2030 is a Sunday.
        Assert.Equal(Utc(2030, 1, 6, 8, 0), Next("0 8 * * 0", Utc(2030, 1, 1, 0, 0)));
        Assert.Equal(Utc(2030, 1, 6, 8, 0), Next("0 8 * * 7", Utc(2030, 1, 1, 0, 0)));
    }

    [Fact]
    public void DayOfMonthAndDayOfWeek_UseVixieOrSemantics()
    {
        // "0 0 13 * FRI": fires on the 13th OR on Fridays. From Jan 1 2030 (Tuesday):
        // the first Friday is Jan 4 — before the 13th.
        Assert.Equal(Utc(2030, 1, 4, 0, 0), Next("0 0 13 * FRI", Utc(2030, 1, 1, 0, 0)));
        // With day-of-week unrestricted, the 13th decides alone.
        Assert.Equal(Utc(2030, 1, 13, 0, 0), Next("0 0 13 * *", Utc(2030, 1, 1, 0, 0)));
        // With day-of-month unrestricted, Friday decides alone.
        Assert.Equal(Utc(2030, 1, 4, 0, 0), Next("0 0 * * FRI", Utc(2030, 1, 1, 0, 0)));
    }

    [Fact]
    public void QuestionMark_IsWildcardInDayFields()
    {
        Assert.Equal(Utc(2030, 1, 1, 5, 0), Next("0 5 ? * ?", Utc(2030, 1, 1, 0, 0)));
    }

    [Fact]
    public void StepInDayOfMonth_RestrictsTheDays()
    {
        // The regression this pins: "*/N" in a day field used to be classified as unrestricted, so
        // its mask was discarded and "every other day" fired every day (twice the intended runs,
        // each with its own deterministic occurrence id).
        Assert.Equal(Utc(2030, 1, 3, 0, 0), Next("0 0 */2 * *", Utc(2030, 1, 1, 0, 0)));
        Assert.Equal(Utc(2030, 1, 5, 0, 0), Next("0 0 */2 * *", Utc(2030, 1, 3, 0, 0)));
        // Same schedule spelled as an explicit range-step, which always worked.
        Assert.Equal(Utc(2030, 1, 3, 0, 0), Next("0 0 1-31/2 * *", Utc(2030, 1, 1, 0, 0)));
    }

    [Fact]
    public void StepInDayOfWeek_RestrictsTheDays()
    {
        // "*/2" over 0-7: Sun(0), Tue, Thu, Sat(6) — Jan 1 2030 is a Tuesday, so the same day at
        // a later hour qualifies, then Thursday Jan 3.
        Assert.Equal(Utc(2030, 1, 1, 6, 0), Next("0 6 * * */2", Utc(2030, 1, 1, 0, 0)));
        Assert.Equal(Utc(2030, 1, 3, 6, 0), Next("0 6 * * */2", Utc(2030, 1, 1, 6, 0)));
        // Wednesday Jan 2 must NOT match.
        Assert.NotEqual(Utc(2030, 1, 2, 6, 0), Next("0 6 * * */2", Utc(2030, 1, 1, 6, 0)));
    }

    [Fact]
    public void StarStepInDayOfMonth_AndsWithRestrictedDayOfWeek()
    {
        // Vixie's star rule: "*/2" starts with '*', so it stays OUT of the dom/dow either-or and
        // both masks must match (crontab(5): odd days AND Friday). Jan 2030 Fridays fall on the
        // 4th, 11th, 18th and 25th; the odd ones are the 11th and 25th.
        Assert.Equal(Utc(2030, 1, 11, 0, 0), Next("0 0 */2 * FRI", Utc(2030, 1, 1, 0, 0)));
        Assert.Equal(Utc(2030, 1, 25, 0, 0), Next("0 0 */2 * FRI", Utc(2030, 1, 11, 0, 0)));
    }

    [Fact]
    public void ExplicitRangeStepInDayOfMonth_KeepsVixieOrSemantics()
    {
        // "1-31/2" does not start with '*', so both day fields are explicitly restricted and the
        // classic either-or applies: odd days OR Fridays. From Jan 1 (odd, but past 00:00) the
        // next is Jan 3 (odd), and from Jan 3 the next is Jan 4 — a Friday that is an even day.
        Assert.Equal(Utc(2030, 1, 3, 0, 0), Next("0 0 1-31/2 * FRI", Utc(2030, 1, 1, 0, 0)));
        Assert.Equal(Utc(2030, 1, 4, 0, 0), Next("0 0 1-31/2 * FRI", Utc(2030, 1, 3, 0, 0)));
    }

    [Fact]
    public void StepOfOneInDayFields_IsUnrestricted()
    {
        // "*/1" is every value — equivalent to "*", so day-of-week stays out of the Vixie OR and
        // the day-of-month alone decides.
        Assert.Equal(Utc(2030, 1, 13, 0, 0), Next("0 0 13 * */1", Utc(2030, 1, 1, 0, 0)));
    }

    [Fact]
    public void LeapDay_ResolvesAcrossYears()
    {
        // Feb 29 exists in 2032 (2030 and 2031 are not leap years).
        Assert.Equal(Utc(2032, 2, 29, 12, 0), Next("0 12 29 2 *", Utc(2030, 1, 1, 0, 0)));
    }

    [Fact]
    public void UnsatisfiableDate_ReturnsNull()
    {
        Assert.Null(CronSchedule.Parse("0 0 30 2 *").GetNextOccurrence(Utc(2030, 1, 1)));
        Assert.Null(CronSchedule.Parse("0 0 31 4 *").GetNextOccurrence(Utc(2030, 1, 1)));
    }

    [Theory]
    [InlineData("* * * *")]
    [InlineData("* * * * * *")]
    [InlineData("60 * * * *")]
    [InlineData("* 24 * * *")]
    [InlineData("* * 0 * *")]
    [InlineData("* * 32 * *")]
    [InlineData("* * * 13 *")]
    [InlineData("* * * * 8")]
    [InlineData("*/0 * * * *")]
    [InlineData("1--2 * * * *")]
    [InlineData("1/2/3 * * * *")]
    [InlineData("a * * * *")]
    [InlineData("* * * JANUARY *")]
    [InlineData("")]
    public void InvalidExpressions_ThrowFormatOrArgument(string cron)
    {
        if (string.IsNullOrWhiteSpace(cron))
            Assert.ThrowsAny<ArgumentException>(() => CronSchedule.Parse(cron));
        else
            Assert.Throws<FormatException>(() => CronSchedule.Parse(cron));
    }

    [Fact]
    public void TimeZone_EvaluatesWallClockAndReturnsUtc()
    {
        var newYork = TimeZoneInfo.FindSystemTimeZoneById("America/New_York");
        // 09:00 New York in January = 14:00 UTC (EST, UTC-5).
        Assert.Equal(Utc(2030, 1, 15, 14, 0), Next("0 9 * * *", Utc(2030, 1, 15, 0, 0), newYork));
        // In July (EDT, UTC-4) the same wall time is 13:00 UTC.
        Assert.Equal(Utc(2030, 7, 15, 13, 0), Next("0 9 * * *", Utc(2030, 7, 15, 0, 0), newYork));
    }

    [Fact]
    public void SpringForwardGap_FiresAtTheJump()
    {
        var newYork = TimeZoneInfo.FindSystemTimeZoneById("America/New_York");
        // US DST 2030 starts Sunday March 10: 02:30 local does not exist that day.
        // The occurrence fires at the gap's end instant: 02:30 interpreted with the
        // pre-transition offset (UTC-5) = 07:30 UTC = 03:30 EDT, the moment the clock has jumped past it.
        var occurrence = Next("30 2 * * *", Utc(2030, 3, 10, 0, 0), newYork);
        Assert.Equal(Utc(2030, 3, 10, 7, 30), occurrence);
        // The next day is back to normal: 02:30 EDT = 06:30 UTC.
        Assert.Equal(Utc(2030, 3, 11, 6, 30), Next("30 2 * * *", occurrence, newYork));
    }

    [Fact]
    public void FallBackAmbiguity_FiresOnTheFirstPassOnly()
    {
        var newYork = TimeZoneInfo.FindSystemTimeZoneById("America/New_York");
        // US DST 2030 ends Sunday November 3: 01:30 local happens twice. The schedule fires on the
        // FIRST (EDT, UTC-4) pass: 05:30 UTC — and not again on the repeated (EST) hour.
        var occurrence = Next("30 1 * * *", Utc(2030, 11, 3, 0, 0), newYork);
        Assert.Equal(Utc(2030, 11, 3, 5, 30), occurrence);
        // Next fire is the NEXT day's 01:30 EST (06:30 UTC), not the second pass at 06:30 same day…
        // which is exactly the same UTC instant a naive double-fire would produce a day early.
        Assert.Equal(Utc(2030, 11, 4, 6, 30), Next("30 1 * * *", occurrence, newYork));
    }

    [Fact]
    public void OccurrenceSequences_AreStrictlyIncreasing()
    {
        var schedule = CronSchedule.Parse("*/5 8-17 * * MON-FRI");
        var cursor = Utc(2030, 1, 1, 0, 0);
        var previous = default(DateTimeOffset?);
        for (var i = 0; i < 500; i++)
        {
            var next = schedule.GetNextOccurrence(cursor)!.Value;
            if (previous is { } p)
                Assert.True(next > p, $"occurrence {next:O} must be after {p:O}");
            Assert.Equal(0, next.Minute % 5);
            Assert.InRange(next.Hour, 8, 17);
            Assert.NotEqual(DayOfWeek.Saturday, next.DayOfWeek);
            Assert.NotEqual(DayOfWeek.Sunday, next.DayOfWeek);
            previous = next;
            cursor = next;
        }
    }
}
