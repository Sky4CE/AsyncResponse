using System.Globalization;

namespace AsyncResponse;

/// <summary>
/// A parsed five-field cron expression (<c>minute hour day-of-month month day-of-week</c>) with an
/// optional time zone, used by <c>WithScheduledFlow</c> to start flows on a schedule.
/// <para>
/// Supported syntax per field: <c>*</c>, single values, lists (<c>1,15</c>), ranges (<c>1-5</c>,
/// wrap-around <c>22-2</c> included), steps (<c>*/15</c>, <c>10-40/5</c>, <c>8/2</c>), and names
/// (<c>JAN…DEC</c>, <c>SUN…SAT</c>, case-insensitive). <c>?</c> is accepted as <c>*</c> in the two
/// day fields. Day-of-month and day-of-week combine with classic Vixie-cron semantics: when both
/// fields are explicitly restricted (neither starts with <c>*</c>), a date matches if <em>either</em>
/// matches; otherwise both masks must match — a star-step field such as <c>*/2</c> stays out of the
/// either/or rule (exactly as Vixie's <c>DOM_STAR</c>/<c>DOW_STAR</c> flags keep it) while its step
/// mask still applies. Day-of-week accepts <c>0</c> and <c>7</c> as Sunday.
/// </para>
/// <para>
/// Occurrences are computed minute-aligned in the schedule's time zone (default UTC) and returned
/// as UTC instants. Around daylight-saving transitions: a local occurrence that does not exist
/// (spring-forward gap) fires at the moment the clock jumps past it; an occurrence in a repeated
/// hour (fall-back) fires on the first (earlier-offset) pass only.
/// </para>
/// </summary>
public sealed class CronSchedule
{
    private readonly ulong _minutes;      // bits 0..59
    private readonly uint _hours;         // bits 0..23
    private readonly uint _daysOfMonth;   // bits 1..31
    private readonly ushort _months;      // bits 1..12
    private readonly byte _daysOfWeek;    // bits 0..6 (Sunday = 0)
    private readonly bool _dayOfMonthRestricted;
    private readonly bool _dayOfWeekRestricted;

    /// <summary>The original expression text.</summary>
    public string Expression { get; }

    /// <summary>The time zone the schedule is evaluated in (default UTC).</summary>
    public TimeZoneInfo TimeZone { get; }

    private CronSchedule(
        string expression,
        TimeZoneInfo timeZone,
        ulong minutes,
        uint hours,
        uint daysOfMonth,
        ushort months,
        byte daysOfWeek,
        bool dayOfMonthRestricted,
        bool dayOfWeekRestricted)
    {
        Expression = expression;
        TimeZone = timeZone;
        _minutes = minutes;
        _hours = hours;
        _daysOfMonth = daysOfMonth;
        _months = months;
        _daysOfWeek = daysOfWeek;
        _dayOfMonthRestricted = dayOfMonthRestricted;
        _dayOfWeekRestricted = dayOfWeekRestricted;
    }

    /// <summary>Parses a five-field cron expression; throws <see cref="FormatException"/> with the offending field on invalid input.</summary>
    public static CronSchedule Parse(string expression, TimeZoneInfo? timeZone = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(expression);

        var fields = expression.Split(' ', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        if (fields.Length != 5)
            throw new FormatException($"Cron expression '{expression}' must have exactly 5 fields (minute hour day-of-month month day-of-week); found {fields.Length}.");

        var minutes = ParseField(expression, fields[0], 0, 59, names: null, allowQuestionMark: false);
        var hours = ParseField(expression, fields[1], 0, 23, names: null, allowQuestionMark: false);
        var daysOfMonth = ParseField(expression, fields[2], 1, 31, names: null, allowQuestionMark: true);
        var months = ParseField(expression, fields[3], 1, 12, MonthNames, allowQuestionMark: false);
        var daysOfWeek = ParseField(expression, fields[4], 0, 7, DayNames, allowQuestionMark: true);

        // 7 is Sunday too; fold it onto bit 0 so matching only ever looks at bits 0..6.
        if ((daysOfWeek & (1UL << 7)) != 0)
            daysOfWeek = (daysOfWeek & ~(1UL << 7)) | 1UL;

        var dayOfMonthRestricted = !IsStarShaped(fields[2]);
        var dayOfWeekRestricted = !IsStarShaped(fields[4]);

        return new CronSchedule(
            expression,
            timeZone ?? TimeZoneInfo.Utc,
            minutes,
            (uint)hours,
            (uint)daysOfMonth,
            (ushort)months,
            (byte)daysOfWeek,
            dayOfMonthRestricted,
            dayOfWeekRestricted);

        // Vixie sets its DOM_STAR/DOW_STAR flags on any day field whose text starts with '*'
        // ("*", "*/2"): such a field stays OUT of the either/or rule below, but its step mask
        // still applies — DayMatches ANDs the two masks unless BOTH fields are explicitly
        // restricted. That keeps "0 0 */2 * *" at every other day (not every day) without turning
        // "0 0 */2 * FRI" into odd-days-OR-Fridays (crontab(5): odd Fridays only).
        static bool IsStarShaped(string field) => field is "?" || field.StartsWith('*');
    }

    /// <summary>
    /// Returns the first occurrence strictly after <paramref name="afterUtc"/>, as a UTC instant,
    /// or <c>null</c> when no occurrence exists within the search horizon (about eight years —
    /// enough to prove an expression such as "Feb 30" unsatisfiable).
    /// </summary>
    public DateTimeOffset? GetNextOccurrence(DateTimeOffset afterUtc)
    {
        // Work minute-aligned in schedule-local time: advance to the next whole minute after
        // `afterUtc`, then scan forward. The scan is bounded, not clever — correctness and DST
        // honesty beat arithmetic shortcuts at one iteration per minute only in the worst field.
        var local = TimeZoneInfo.ConvertTime(afterUtc, TimeZone);
        var candidate = new DateTime(local.Year, local.Month, local.Day, local.Hour, local.Minute, 0, DateTimeKind.Unspecified)
            .AddMinutes(1);

        // Horizon: 8 years covers every leap-year/day-of-week alignment a satisfiable expression
        // can need (worst real case, Feb 29 on a fixed weekday, recurs within 40 years — but any
        // expression matching a *day* recurs within 8; Feb-29-with-weekday beyond that is treated
        // as unsatisfiable together with the genuinely impossible dates).
        var horizon = candidate.AddYears(8);

        while (candidate < horizon)
        {
            if (!MonthMatches(candidate.Month))
            {
                // Jump to the first minute of the next month; day scanning below stays in-month.
                candidate = new DateTime(candidate.Year, candidate.Month, 1, 0, 0, 0, DateTimeKind.Unspecified).AddMonths(1);
                continue;
            }

            if (!DayMatches(candidate))
            {
                candidate = candidate.Date.AddDays(1);
                continue;
            }

            if (!HourMatches(candidate.Hour))
            {
                candidate = new DateTime(candidate.Year, candidate.Month, candidate.Day, candidate.Hour, 0, 0, DateTimeKind.Unspecified).AddHours(1);
                continue;
            }

            if (!MinuteMatches(candidate.Minute))
            {
                candidate = candidate.AddMinutes(1);
                continue;
            }

            // A schedule-local match. Map it onto the UTC timeline honoring DST:
            if (TimeZone.IsInvalidTime(candidate))
            {
                // Spring-forward gap: the wall-clock time never happens. Interpreting the wall time
                // with the PRE-transition offset yields the exact instant the clock jumps past it —
                // i.e. the job fires at the gap's end, matching what cron daemons do for jobs the
                // jump would otherwise skip.
                var preTransitionOffset = TimeZone.GetUtcOffset(candidate.AddDays(-1));
                var instant = new DateTimeOffset(candidate, preTransitionOffset).ToUniversalTime();
                if (instant > afterUtc)
                    return instant;

                candidate = candidate.AddMinutes(1);
                continue;
            }

            DateTimeOffset occurrence;
            if (TimeZone.IsAmbiguousTime(candidate))
            {
                // Fall-back repeat: fire on the FIRST (earlier-offset, typically DST) pass only.
                var offsets = TimeZone.GetAmbiguousTimeOffsets(candidate);
                var first = offsets[0];
                foreach (var offset in offsets)
                {
                    if (offset > first)
                        first = offset; // larger UTC offset = earlier UTC instant
                }

                occurrence = new DateTimeOffset(candidate, first).ToUniversalTime();
            }
            else
            {
                occurrence = new DateTimeOffset(candidate, TimeZone.GetUtcOffset(candidate)).ToUniversalTime();
            }

            if (occurrence > afterUtc)
                return occurrence;

            candidate = candidate.AddMinutes(1);
        }

        return null;
    }

    private bool MinuteMatches(int minute) => (_minutes & (1UL << minute)) != 0;
    private bool HourMatches(int hour) => (_hours & (1U << hour)) != 0;
    private bool MonthMatches(int month) => (_months & (1 << month)) != 0;

    private bool DayMatches(DateTime date)
    {
        var dayOfMonth = (_daysOfMonth & (1U << date.Day)) != 0;
        var dayOfWeek = (_daysOfWeek & (1 << (int)date.DayOfWeek)) != 0;

        // Vixie-cron dom/dow rule: only when BOTH fields are explicitly restricted does either
        // match suffice; when either field is star-shaped ("*", "*/N", "?") both masks must match
        // (a plain "*" mask is full, so the other field decides alone — the classic behavior).
        return _dayOfMonthRestricted && _dayOfWeekRestricted
            ? dayOfMonth || dayOfWeek
            : dayOfMonth && dayOfWeek;
    }

    private static readonly string[] MonthNames = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"];
    private static readonly string[] DayNames = ["SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"];

    private static ulong ParseField(string expression, string field, int min, int max, string[]? names, bool allowQuestionMark)
    {
        if (field == "*" || (allowQuestionMark && field == "?"))
            return RangeMask(min, max, min, max, 1);

        ulong mask = 0;
        foreach (var part in field.Split(','))
        {
            if (part.Length == 0)
                throw Invalid(expression, field, "empty list entry");

            var stepSplit = part.Split('/');
            if (stepSplit.Length > 2)
                throw Invalid(expression, field, $"'{part}' has more than one '/'");

            var step = 1;
            if (stepSplit.Length == 2)
            {
                if (!int.TryParse(stepSplit[1], NumberStyles.None, CultureInfo.InvariantCulture, out step) || step <= 0)
                    throw Invalid(expression, field, $"step '{stepSplit[1]}' must be a positive integer");
            }

            var rangePart = stepSplit[0];
            int low, high;
            if (rangePart == "*" || (allowQuestionMark && rangePart == "?"))
            {
                low = min;
                high = max;
            }
            else
            {
                var rangeSplit = rangePart.Split('-');
                if (rangeSplit.Length > 2)
                    throw Invalid(expression, field, $"'{rangePart}' has more than one '-'");

                low = ParseValue(expression, field, rangeSplit[0], min, max, names);
                if (rangeSplit.Length == 2)
                {
                    high = ParseValue(expression, field, rangeSplit[1], min, max, names);
                }
                else if (stepSplit.Length == 2)
                {
                    // Vixie extension "N/step": start at N, run to the field maximum.
                    high = max;
                }
                else
                {
                    high = low;
                }
            }

            mask |= RangeMask(min, max, low, high, step);
        }

        return mask;
    }

    private static int ParseValue(string expression, string field, string token, int min, int max, string[]? names)
    {
        if (names is not null)
        {
            for (var index = 0; index < names.Length; index++)
            {
                if (string.Equals(token, names[index], StringComparison.OrdinalIgnoreCase))
                {
                    // Month names are 1-based (JAN=1); day names 0-based (SUN=0).
                    return names == MonthNames ? index + 1 : index;
                }
            }
        }

        if (!int.TryParse(token, NumberStyles.None, CultureInfo.InvariantCulture, out var value))
            throw Invalid(expression, field, $"'{token}' is not a number{(names is null ? "" : " or name")}");
        if (value < min || value > max)
            throw Invalid(expression, field, $"'{token}' is outside {min}-{max}");
        return value;
    }

    private static ulong RangeMask(int min, int max, int low, int high, int step)
    {
        ulong mask = 0;
        if (low <= high)
        {
            for (var value = low; value <= high; value += step)
                mask |= 1UL << value;
            return mask;
        }

        // Wrap-around range (e.g. hours 22-2): low..max then min..high, stepping continuously.
        var position = low;
        while (position <= max)
        {
            mask |= 1UL << position;
            position += step;
        }

        // Continue the stride into the wrapped segment so 50-10/4 keeps its cadence across the wrap.
        position = min + (position - max - 1);
        while (position <= high)
        {
            mask |= 1UL << position;
            position += step;
        }

        return mask;
    }

    private static FormatException Invalid(string expression, string field, string reason)
        => new($"Cron expression '{expression}' has an invalid field '{field}': {reason}.");
}
