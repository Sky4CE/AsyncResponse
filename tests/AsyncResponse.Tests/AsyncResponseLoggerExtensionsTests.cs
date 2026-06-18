using Microsoft.Extensions.Logging;
using Xunit;
using LogExtensions = Microsoft.Extensions.Logging.AsyncResponseLoggerExtensions;

namespace AsyncResponse.Tests;

public class AsyncResponseLoggerExtensionsTests
{
    [Fact]
    public void LoggerExtensions_InvokeAllEnabledOverloads()
    {
        var logger = new RecordingLogger(true);
        var exception = new InvalidOperationException("boom");

        LogExtensions.LogDebug(logger, "debug0");
        LogExtensions.LogDebug(logger, "debug1 {A}", 1);
        LogExtensions.LogDebug(logger, "debug2 {A} {B}", 1, 2);
        LogExtensions.LogDebug(logger, "debug3 {A} {B} {C}", 1, 2, 3);
        LogExtensions.LogDebug(logger, "debug4 {A} {B} {C} {D}", 1, 2, 3, 4);
        LogExtensions.LogDebug(logger, "debug5 {A} {B} {C} {D} {E}", 1, 2, 3, 4, 5);
        LogExtensions.LogDebug(logger, "debug6 {A} {B} {C} {D} {E} {F}", 1, 2, 3, 4, 5, 6);
        LogExtensions.LogDebug(logger, exception, "debugex0");
        LogExtensions.LogDebug(logger, exception, "debugex1 {A}", 1);
        LogExtensions.LogDebug(logger, exception, "debugex2 {A} {B}", 1, 2);
        LogExtensions.LogDebug(logger, exception, "debugex3 {A} {B} {C}", 1, 2, 3);
        LogExtensions.LogDebug(logger, exception, "debugex4 {A} {B} {C} {D}", 1, 2, 3, 4);
        LogExtensions.LogDebug(logger, exception, "debugex5 {A} {B} {C} {D} {E}", 1, 2, 3, 4, 5);
        LogExtensions.LogDebug(logger, exception, "debugex6 {A} {B} {C} {D} {E} {F}", 1, 2, 3, 4, 5, 6);

        LogExtensions.LogInformation(logger, "info0");
        LogExtensions.LogInformation(logger, "info1 {A}", 1);
        LogExtensions.LogInformation(logger, "info2 {A} {B}", 1, 2);
        LogExtensions.LogInformation(logger, "info3 {A} {B} {C}", 1, 2, 3);
        LogExtensions.LogInformation(logger, "info4 {A} {B} {C} {D}", 1, 2, 3, 4);
        LogExtensions.LogInformation(logger, "info5 {A} {B} {C} {D} {E}", 1, 2, 3, 4, 5);
        LogExtensions.LogInformation(logger, "info6 {A} {B} {C} {D} {E} {F}", 1, 2, 3, 4, 5, 6);
        LogExtensions.LogInformation(logger, exception, "infoex0");
        LogExtensions.LogInformation(logger, exception, "infoex1 {A}", 1);
        LogExtensions.LogInformation(logger, exception, "infoex2 {A} {B}", 1, 2);
        LogExtensions.LogInformation(logger, exception, "infoex3 {A} {B} {C}", 1, 2, 3);
        LogExtensions.LogInformation(logger, exception, "infoex4 {A} {B} {C} {D}", 1, 2, 3, 4);
        LogExtensions.LogInformation(logger, exception, "infoex5 {A} {B} {C} {D} {E}", 1, 2, 3, 4, 5);
        LogExtensions.LogInformation(logger, exception, "infoex6 {A} {B} {C} {D} {E} {F}", 1, 2, 3, 4, 5, 6);

        LogExtensions.LogWarning(logger, "warn0");
        LogExtensions.LogWarning(logger, "warn1 {A}", 1);
        LogExtensions.LogWarning(logger, "warn2 {A} {B}", 1, 2);
        LogExtensions.LogWarning(logger, "warn3 {A} {B} {C}", 1, 2, 3);
        LogExtensions.LogWarning(logger, "warn4 {A} {B} {C} {D}", 1, 2, 3, 4);
        LogExtensions.LogWarning(logger, "warn5 {A} {B} {C} {D} {E}", 1, 2, 3, 4, 5);
        LogExtensions.LogWarning(logger, "warn6 {A} {B} {C} {D} {E} {F}", 1, 2, 3, 4, 5, 6);
        LogExtensions.LogWarning(logger, exception, "warnex0");
        LogExtensions.LogWarning(logger, exception, "warnex1 {A}", 1);
        LogExtensions.LogWarning(logger, exception, "warnex2 {A} {B}", 1, 2);
        LogExtensions.LogWarning(logger, exception, "warnex3 {A} {B} {C}", 1, 2, 3);
        LogExtensions.LogWarning(logger, exception, "warnex4 {A} {B} {C} {D}", 1, 2, 3, 4);
        LogExtensions.LogWarning(logger, exception, "warnex5 {A} {B} {C} {D} {E}", 1, 2, 3, 4, 5);
        LogExtensions.LogWarning(logger, exception, "warnex6 {A} {B} {C} {D} {E} {F}", 1, 2, 3, 4, 5, 6);

        LogExtensions.LogError(logger, "error0");
        LogExtensions.LogError(logger, "error1 {A}", 1);
        LogExtensions.LogError(logger, "error2 {A} {B}", 1, 2);
        LogExtensions.LogError(logger, "error3 {A} {B} {C}", 1, 2, 3);
        LogExtensions.LogError(logger, "error4 {A} {B} {C} {D}", 1, 2, 3, 4);
        LogExtensions.LogError(logger, "error5 {A} {B} {C} {D} {E}", 1, 2, 3, 4, 5);
        LogExtensions.LogError(logger, "error6 {A} {B} {C} {D} {E} {F}", 1, 2, 3, 4, 5, 6);
        LogExtensions.LogError(logger, exception, "errorex0");
        LogExtensions.LogError(logger, exception, "errorex1 {A}", 1);
        LogExtensions.LogError(logger, exception, "errorex2 {A} {B}", 1, 2);
        LogExtensions.LogError(logger, exception, "errorex3 {A} {B} {C}", 1, 2, 3);
        LogExtensions.LogError(logger, exception, "errorex4 {A} {B} {C} {D}", 1, 2, 3, 4);
        LogExtensions.LogError(logger, exception, "errorex5 {A} {B} {C} {D} {E}", 1, 2, 3, 4, 5);
        LogExtensions.LogError(logger, exception, "errorex6 {A} {B} {C} {D} {E} {F}", 1, 2, 3, 4, 5, 6);

        Assert.Equal(56, logger.Entries.Count);
        Assert.Contains(logger.Entries, entry => entry.Level == LogLevel.Debug && entry.Message.Contains("debug6", StringComparison.Ordinal));
        Assert.Contains(logger.Entries, entry => entry.Level == LogLevel.Information && entry.Exception == exception);
        Assert.Contains(logger.Entries, entry => entry.Level == LogLevel.Warning && entry.Message.Contains("warnex5", StringComparison.Ordinal));
        Assert.Contains(logger.Entries, entry => entry.Level == LogLevel.Error && entry.Message.Contains("errorex5", StringComparison.Ordinal));
    }

    [Fact]
    public void LoggerExtensions_DoNothingWhenLevelDisabled()
    {
        var logger = new RecordingLogger(false);

        LogExtensions.LogDebug(logger, "debug0");
        LogExtensions.LogDebug(logger, "debug1 {A}", 1);
        LogExtensions.LogDebug(logger, "debug2 {A} {B}", 1, 2);
        LogExtensions.LogDebug(logger, "debug3 {A} {B} {C}", 1, 2, 3);
        LogExtensions.LogDebug(logger, "debug4 {A} {B} {C} {D}", 1, 2, 3, 4);
        LogExtensions.LogDebug(logger, "debug5 {A} {B} {C} {D} {E}", 1, 2, 3, 4, 5);
        LogExtensions.LogDebug(logger, "debug6 {A} {B} {C} {D} {E} {F}", 1, 2, 3, 4, 5, 6);

        Assert.Empty(logger.Entries);
    }

    private sealed class RecordingLogger(bool _enabled) : ILogger
    {
        public List<(LogLevel Level, Exception? Exception, string Message)> Entries { get; } = [];

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => _enabled;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
            => Entries.Add((logLevel, exception, formatter(state, exception)));
    }
}
