using System.Collections.Concurrent;

namespace Microsoft.Extensions.Logging;

internal static class AsyncResponseLoggerExtensions
{
    private static readonly LogDefineOptions Options = new() { SkipEnabledCheck = true };

    /// <summary>Runs the LogDebug operation.</summary>
    public static void LogDebug(this ILogger logger, string message)
        => Log(logger, LogLevel.Debug, exception: null, message);

    /// <summary>Runs the LogDebug operation.</summary>
    public static void LogDebug<T0>(this ILogger logger, string message, T0 arg0)
        => Log(logger, LogLevel.Debug, exception: null, message, arg0);

    /// <summary>Runs the LogDebug operation.</summary>
    public static void LogDebug<T0, T1>(this ILogger logger, string message, T0 arg0, T1 arg1)
        => Log(logger, LogLevel.Debug, exception: null, message, arg0, arg1);

    /// <summary>Runs the LogDebug operation.</summary>
    public static void LogDebug<T0, T1, T2>(this ILogger logger, string message, T0 arg0, T1 arg1, T2 arg2)
        => Log(logger, LogLevel.Debug, exception: null, message, arg0, arg1, arg2);

    /// <summary>Runs the LogDebug operation.</summary>
    public static void LogDebug<T0, T1, T2, T3>(this ILogger logger, string message, T0 arg0, T1 arg1, T2 arg2, T3 arg3)
        => Log(logger, LogLevel.Debug, exception: null, message, arg0, arg1, arg2, arg3);

    /// <summary>Runs the LogDebug operation.</summary>
    public static void LogDebug<T0, T1, T2, T3, T4>(this ILogger logger, string message, T0 arg0, T1 arg1, T2 arg2, T3 arg3, T4 arg4)
        => Log(logger, LogLevel.Debug, exception: null, message, arg0, arg1, arg2, arg3, arg4);

    /// <summary>Runs the LogDebug operation.</summary>
    public static void LogDebug<T0, T1, T2, T3, T4, T5>(this ILogger logger, string message, T0 arg0, T1 arg1, T2 arg2, T3 arg3, T4 arg4, T5 arg5)
        => Log(logger, LogLevel.Debug, exception: null, message, arg0, arg1, arg2, arg3, arg4, arg5);

    /// <summary>Runs the LogDebug operation.</summary>
    public static void LogDebug(this ILogger logger, Exception? exception, string message)
        => Log(logger, LogLevel.Debug, exception, message);

    /// <summary>Runs the LogDebug operation.</summary>
    public static void LogDebug<T0>(this ILogger logger, Exception? exception, string message, T0 arg0)
        => Log(logger, LogLevel.Debug, exception, message, arg0);

    /// <summary>Runs the LogDebug operation.</summary>
    public static void LogDebug<T0, T1>(this ILogger logger, Exception? exception, string message, T0 arg0, T1 arg1)
        => Log(logger, LogLevel.Debug, exception, message, arg0, arg1);

    /// <summary>Runs the LogDebug operation.</summary>
    public static void LogDebug<T0, T1, T2>(this ILogger logger, Exception? exception, string message, T0 arg0, T1 arg1, T2 arg2)
        => Log(logger, LogLevel.Debug, exception, message, arg0, arg1, arg2);

    /// <summary>Runs the LogDebug operation.</summary>
    public static void LogDebug<T0, T1, T2, T3>(this ILogger logger, Exception? exception, string message, T0 arg0, T1 arg1, T2 arg2, T3 arg3)
        => Log(logger, LogLevel.Debug, exception, message, arg0, arg1, arg2, arg3);

    /// <summary>Runs the LogDebug operation.</summary>
    public static void LogDebug<T0, T1, T2, T3, T4>(this ILogger logger, Exception? exception, string message, T0 arg0, T1 arg1, T2 arg2, T3 arg3, T4 arg4)
        => Log(logger, LogLevel.Debug, exception, message, arg0, arg1, arg2, arg3, arg4);

    /// <summary>Runs the LogDebug operation.</summary>
    public static void LogDebug<T0, T1, T2, T3, T4, T5>(this ILogger logger, Exception? exception, string message, T0 arg0, T1 arg1, T2 arg2, T3 arg3, T4 arg4, T5 arg5)
        => Log(logger, LogLevel.Debug, exception, message, arg0, arg1, arg2, arg3, arg4, arg5);

    /// <summary>Runs the LogInformation operation.</summary>
    public static void LogInformation(this ILogger logger, string message)
        => Log(logger, LogLevel.Information, exception: null, message);

    /// <summary>Runs the LogInformation operation.</summary>
    public static void LogInformation<T0>(this ILogger logger, string message, T0 arg0)
        => Log(logger, LogLevel.Information, exception: null, message, arg0);

    /// <summary>Runs the LogInformation operation.</summary>
    public static void LogInformation<T0, T1>(this ILogger logger, string message, T0 arg0, T1 arg1)
        => Log(logger, LogLevel.Information, exception: null, message, arg0, arg1);

    /// <summary>Runs the LogInformation operation.</summary>
    public static void LogInformation<T0, T1, T2>(this ILogger logger, string message, T0 arg0, T1 arg1, T2 arg2)
        => Log(logger, LogLevel.Information, exception: null, message, arg0, arg1, arg2);

    /// <summary>Runs the LogInformation operation.</summary>
    public static void LogInformation<T0, T1, T2, T3>(this ILogger logger, string message, T0 arg0, T1 arg1, T2 arg2, T3 arg3)
        => Log(logger, LogLevel.Information, exception: null, message, arg0, arg1, arg2, arg3);

    /// <summary>Runs the LogInformation operation.</summary>
    public static void LogInformation<T0, T1, T2, T3, T4>(this ILogger logger, string message, T0 arg0, T1 arg1, T2 arg2, T3 arg3, T4 arg4)
        => Log(logger, LogLevel.Information, exception: null, message, arg0, arg1, arg2, arg3, arg4);

    /// <summary>Runs the LogInformation operation.</summary>
    public static void LogInformation<T0, T1, T2, T3, T4, T5>(this ILogger logger, string message, T0 arg0, T1 arg1, T2 arg2, T3 arg3, T4 arg4, T5 arg5)
        => Log(logger, LogLevel.Information, exception: null, message, arg0, arg1, arg2, arg3, arg4, arg5);

    /// <summary>Runs the LogInformation operation.</summary>
    public static void LogInformation(this ILogger logger, Exception? exception, string message)
        => Log(logger, LogLevel.Information, exception, message);

    /// <summary>Runs the LogInformation operation.</summary>
    public static void LogInformation<T0>(this ILogger logger, Exception? exception, string message, T0 arg0)
        => Log(logger, LogLevel.Information, exception, message, arg0);

    /// <summary>Runs the LogInformation operation.</summary>
    public static void LogInformation<T0, T1>(this ILogger logger, Exception? exception, string message, T0 arg0, T1 arg1)
        => Log(logger, LogLevel.Information, exception, message, arg0, arg1);

    /// <summary>Runs the LogInformation operation.</summary>
    public static void LogInformation<T0, T1, T2>(this ILogger logger, Exception? exception, string message, T0 arg0, T1 arg1, T2 arg2)
        => Log(logger, LogLevel.Information, exception, message, arg0, arg1, arg2);

    /// <summary>Runs the LogInformation operation.</summary>
    public static void LogInformation<T0, T1, T2, T3>(this ILogger logger, Exception? exception, string message, T0 arg0, T1 arg1, T2 arg2, T3 arg3)
        => Log(logger, LogLevel.Information, exception, message, arg0, arg1, arg2, arg3);

    /// <summary>Runs the LogInformation operation.</summary>
    public static void LogInformation<T0, T1, T2, T3, T4>(this ILogger logger, Exception? exception, string message, T0 arg0, T1 arg1, T2 arg2, T3 arg3, T4 arg4)
        => Log(logger, LogLevel.Information, exception, message, arg0, arg1, arg2, arg3, arg4);

    /// <summary>Runs the LogInformation operation.</summary>
    public static void LogInformation<T0, T1, T2, T3, T4, T5>(this ILogger logger, Exception? exception, string message, T0 arg0, T1 arg1, T2 arg2, T3 arg3, T4 arg4, T5 arg5)
        => Log(logger, LogLevel.Information, exception, message, arg0, arg1, arg2, arg3, arg4, arg5);

    /// <summary>Runs the LogWarning operation.</summary>
    public static void LogWarning(this ILogger logger, string message)
        => Log(logger, LogLevel.Warning, exception: null, message);

    /// <summary>Runs the LogWarning operation.</summary>
    public static void LogWarning<T0>(this ILogger logger, string message, T0 arg0)
        => Log(logger, LogLevel.Warning, exception: null, message, arg0);

    /// <summary>Runs the LogWarning operation.</summary>
    public static void LogWarning<T0, T1>(this ILogger logger, string message, T0 arg0, T1 arg1)
        => Log(logger, LogLevel.Warning, exception: null, message, arg0, arg1);

    /// <summary>Runs the LogWarning operation.</summary>
    public static void LogWarning<T0, T1, T2>(this ILogger logger, string message, T0 arg0, T1 arg1, T2 arg2)
        => Log(logger, LogLevel.Warning, exception: null, message, arg0, arg1, arg2);

    /// <summary>Runs the LogWarning operation.</summary>
    public static void LogWarning<T0, T1, T2, T3>(this ILogger logger, string message, T0 arg0, T1 arg1, T2 arg2, T3 arg3)
        => Log(logger, LogLevel.Warning, exception: null, message, arg0, arg1, arg2, arg3);

    /// <summary>Runs the LogWarning operation.</summary>
    public static void LogWarning<T0, T1, T2, T3, T4>(this ILogger logger, string message, T0 arg0, T1 arg1, T2 arg2, T3 arg3, T4 arg4)
        => Log(logger, LogLevel.Warning, exception: null, message, arg0, arg1, arg2, arg3, arg4);

    /// <summary>Runs the LogWarning operation.</summary>
    public static void LogWarning<T0, T1, T2, T3, T4, T5>(this ILogger logger, string message, T0 arg0, T1 arg1, T2 arg2, T3 arg3, T4 arg4, T5 arg5)
        => Log(logger, LogLevel.Warning, exception: null, message, arg0, arg1, arg2, arg3, arg4, arg5);

    /// <summary>Runs the LogWarning operation.</summary>
    public static void LogWarning(this ILogger logger, Exception? exception, string message)
        => Log(logger, LogLevel.Warning, exception, message);

    /// <summary>Runs the LogWarning operation.</summary>
    public static void LogWarning<T0>(this ILogger logger, Exception? exception, string message, T0 arg0)
        => Log(logger, LogLevel.Warning, exception, message, arg0);

    /// <summary>Runs the LogWarning operation.</summary>
    public static void LogWarning<T0, T1>(this ILogger logger, Exception? exception, string message, T0 arg0, T1 arg1)
        => Log(logger, LogLevel.Warning, exception, message, arg0, arg1);

    /// <summary>Runs the LogWarning operation.</summary>
    public static void LogWarning<T0, T1, T2>(this ILogger logger, Exception? exception, string message, T0 arg0, T1 arg1, T2 arg2)
        => Log(logger, LogLevel.Warning, exception, message, arg0, arg1, arg2);

    /// <summary>Runs the LogWarning operation.</summary>
    public static void LogWarning<T0, T1, T2, T3>(this ILogger logger, Exception? exception, string message, T0 arg0, T1 arg1, T2 arg2, T3 arg3)
        => Log(logger, LogLevel.Warning, exception, message, arg0, arg1, arg2, arg3);

    /// <summary>Runs the LogWarning operation.</summary>
    public static void LogWarning<T0, T1, T2, T3, T4>(this ILogger logger, Exception? exception, string message, T0 arg0, T1 arg1, T2 arg2, T3 arg3, T4 arg4)
        => Log(logger, LogLevel.Warning, exception, message, arg0, arg1, arg2, arg3, arg4);

    /// <summary>Runs the LogWarning operation.</summary>
    public static void LogWarning<T0, T1, T2, T3, T4, T5>(this ILogger logger, Exception? exception, string message, T0 arg0, T1 arg1, T2 arg2, T3 arg3, T4 arg4, T5 arg5)
        => Log(logger, LogLevel.Warning, exception, message, arg0, arg1, arg2, arg3, arg4, arg5);

    /// <summary>Runs the LogError operation.</summary>
    public static void LogError(this ILogger logger, string message)
        => Log(logger, LogLevel.Error, exception: null, message);

    /// <summary>Runs the LogError operation.</summary>
    public static void LogError<T0>(this ILogger logger, string message, T0 arg0)
        => Log(logger, LogLevel.Error, exception: null, message, arg0);

    /// <summary>Runs the LogError operation.</summary>
    public static void LogError<T0, T1>(this ILogger logger, string message, T0 arg0, T1 arg1)
        => Log(logger, LogLevel.Error, exception: null, message, arg0, arg1);

    /// <summary>Runs the LogError operation.</summary>
    public static void LogError<T0, T1, T2>(this ILogger logger, string message, T0 arg0, T1 arg1, T2 arg2)
        => Log(logger, LogLevel.Error, exception: null, message, arg0, arg1, arg2);

    /// <summary>Runs the LogError operation.</summary>
    public static void LogError<T0, T1, T2, T3>(this ILogger logger, string message, T0 arg0, T1 arg1, T2 arg2, T3 arg3)
        => Log(logger, LogLevel.Error, exception: null, message, arg0, arg1, arg2, arg3);

    /// <summary>Runs the LogError operation.</summary>
    public static void LogError<T0, T1, T2, T3, T4>(this ILogger logger, string message, T0 arg0, T1 arg1, T2 arg2, T3 arg3, T4 arg4)
        => Log(logger, LogLevel.Error, exception: null, message, arg0, arg1, arg2, arg3, arg4);

    /// <summary>Runs the LogError operation.</summary>
    public static void LogError<T0, T1, T2, T3, T4, T5>(this ILogger logger, string message, T0 arg0, T1 arg1, T2 arg2, T3 arg3, T4 arg4, T5 arg5)
        => Log(logger, LogLevel.Error, exception: null, message, arg0, arg1, arg2, arg3, arg4, arg5);

    /// <summary>Runs the LogError operation.</summary>
    public static void LogError(this ILogger logger, Exception? exception, string message)
        => Log(logger, LogLevel.Error, exception, message);

    /// <summary>Runs the LogError operation.</summary>
    public static void LogError<T0>(this ILogger logger, Exception? exception, string message, T0 arg0)
        => Log(logger, LogLevel.Error, exception, message, arg0);

    /// <summary>Runs the LogError operation.</summary>
    public static void LogError<T0, T1>(this ILogger logger, Exception? exception, string message, T0 arg0, T1 arg1)
        => Log(logger, LogLevel.Error, exception, message, arg0, arg1);

    /// <summary>Runs the LogError operation.</summary>
    public static void LogError<T0, T1, T2>(this ILogger logger, Exception? exception, string message, T0 arg0, T1 arg1, T2 arg2)
        => Log(logger, LogLevel.Error, exception, message, arg0, arg1, arg2);

    /// <summary>Runs the LogError operation.</summary>
    public static void LogError<T0, T1, T2, T3>(this ILogger logger, Exception? exception, string message, T0 arg0, T1 arg1, T2 arg2, T3 arg3)
        => Log(logger, LogLevel.Error, exception, message, arg0, arg1, arg2, arg3);

    /// <summary>Runs the LogError operation.</summary>
    public static void LogError<T0, T1, T2, T3, T4>(this ILogger logger, Exception? exception, string message, T0 arg0, T1 arg1, T2 arg2, T3 arg3, T4 arg4)
        => Log(logger, LogLevel.Error, exception, message, arg0, arg1, arg2, arg3, arg4);

    /// <summary>Runs the LogError operation.</summary>
    public static void LogError<T0, T1, T2, T3, T4, T5>(this ILogger logger, Exception? exception, string message, T0 arg0, T1 arg1, T2 arg2, T3 arg3, T4 arg4, T5 arg5)
        => Log(logger, LogLevel.Error, exception, message, arg0, arg1, arg2, arg3, arg4, arg5);

    private static void Log(ILogger logger, LogLevel level, Exception? exception, string message)
    {
        if (!logger.IsEnabled(level))
            return;

        LogCache.Get(level, message)(logger, exception);
    }

    private static void Log<T0>(ILogger logger, LogLevel level, Exception? exception, string message, T0 arg0)
    {
        if (!logger.IsEnabled(level))
            return;

        LogCache<T0>.Get(level, message)(logger, arg0, exception);
    }

    private static void Log<T0, T1>(ILogger logger, LogLevel level, Exception? exception, string message, T0 arg0, T1 arg1)
    {
        if (!logger.IsEnabled(level))
            return;

        LogCache<T0, T1>.Get(level, message)(logger, arg0, arg1, exception);
    }

    private static void Log<T0, T1, T2>(ILogger logger, LogLevel level, Exception? exception, string message, T0 arg0, T1 arg1, T2 arg2)
    {
        if (!logger.IsEnabled(level))
            return;

        LogCache<T0, T1, T2>.Get(level, message)(logger, arg0, arg1, arg2, exception);
    }

    private static void Log<T0, T1, T2, T3>(ILogger logger, LogLevel level, Exception? exception, string message, T0 arg0, T1 arg1, T2 arg2, T3 arg3)
    {
        if (!logger.IsEnabled(level))
            return;

        LogCache<T0, T1, T2, T3>.Get(level, message)(logger, arg0, arg1, arg2, arg3, exception);
    }

    private static void Log<T0, T1, T2, T3, T4>(ILogger logger, LogLevel level, Exception? exception, string message, T0 arg0, T1 arg1, T2 arg2, T3 arg3, T4 arg4)
    {
        if (!logger.IsEnabled(level))
            return;

        LogCache<T0, T1, T2, T3, T4>.Get(level, message)(logger, arg0, arg1, arg2, arg3, arg4, exception);
    }

    private static void Log<T0, T1, T2, T3, T4, T5>(ILogger logger, LogLevel level, Exception? exception, string message, T0 arg0, T1 arg1, T2 arg2, T3 arg3, T4 arg4, T5 arg5)
    {
        if (!logger.IsEnabled(level))
            return;

        LogCache<T0, T1, T2, T3, T4, T5>.Get(level, message)(logger, arg0, arg1, arg2, arg3, arg4, arg5, exception);
    }

    private readonly record struct LogKey(LogLevel Level, string Message);

    private static class LogCache
    {
        private static readonly ConcurrentDictionary<LogKey, Action<ILogger, Exception?>> Delegates = new();

        /// <summary>Runs the Get operation.</summary>
        public static Action<ILogger, Exception?> Get(LogLevel level, string message)
            => Delegates.GetOrAdd(
                new LogKey(level, message),
                static key => LoggerMessage.Define(key.Level, default, key.Message, Options));
    }

    private static class LogCache<T0>
    {
        private static readonly ConcurrentDictionary<LogKey, Action<ILogger, T0, Exception?>> Delegates = new();

        /// <summary>Runs the Get operation.</summary>
        public static Action<ILogger, T0, Exception?> Get(LogLevel level, string message)
            => Delegates.GetOrAdd(
                new LogKey(level, message),
                static key => LoggerMessage.Define<T0>(key.Level, default, key.Message, Options));
    }

    private static class LogCache<T0, T1>
    {
        private static readonly ConcurrentDictionary<LogKey, Action<ILogger, T0, T1, Exception?>> Delegates = new();

        /// <summary>Runs the Get operation.</summary>
        public static Action<ILogger, T0, T1, Exception?> Get(LogLevel level, string message)
            => Delegates.GetOrAdd(
                new LogKey(level, message),
                static key => LoggerMessage.Define<T0, T1>(key.Level, default, key.Message, Options));
    }

    private static class LogCache<T0, T1, T2>
    {
        private static readonly ConcurrentDictionary<LogKey, Action<ILogger, T0, T1, T2, Exception?>> Delegates = new();

        /// <summary>Runs the Get operation.</summary>
        public static Action<ILogger, T0, T1, T2, Exception?> Get(LogLevel level, string message)
            => Delegates.GetOrAdd(
                new LogKey(level, message),
                static key => LoggerMessage.Define<T0, T1, T2>(key.Level, default, key.Message, Options));
    }

    private static class LogCache<T0, T1, T2, T3>
    {
        private static readonly ConcurrentDictionary<LogKey, Action<ILogger, T0, T1, T2, T3, Exception?>> Delegates = new();

        /// <summary>Runs the Get operation.</summary>
        public static Action<ILogger, T0, T1, T2, T3, Exception?> Get(LogLevel level, string message)
            => Delegates.GetOrAdd(
                new LogKey(level, message),
                static key => LoggerMessage.Define<T0, T1, T2, T3>(key.Level, default, key.Message, Options));
    }

    private static class LogCache<T0, T1, T2, T3, T4>
    {
        private static readonly ConcurrentDictionary<LogKey, Action<ILogger, T0, T1, T2, T3, T4, Exception?>> Delegates = new();

        /// <summary>Runs the Get operation.</summary>
        public static Action<ILogger, T0, T1, T2, T3, T4, Exception?> Get(LogLevel level, string message)
            => Delegates.GetOrAdd(
                new LogKey(level, message),
                static key => LoggerMessage.Define<T0, T1, T2, T3, T4>(key.Level, default, key.Message, Options));
    }

    private static class LogCache<T0, T1, T2, T3, T4, T5>
    {
        private static readonly ConcurrentDictionary<LogKey, Action<ILogger, T0, T1, T2, T3, T4, T5, Exception?>> Delegates = new();

        /// <summary>Runs the Get operation.</summary>
        public static Action<ILogger, T0, T1, T2, T3, T4, T5, Exception?> Get(LogLevel level, string message)
            => Delegates.GetOrAdd(
                new LogKey(level, message),
                static key => LoggerMessage.Define<T0, T1, T2, T3, T4, T5>(key.Level, default, key.Message, Options));
    }
}
