using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// The one log-capturing test logger: records every rendered message together with its attached
/// exception, so a test can await a background loop's retry (<see cref="WaitForAsync"/>) instead of
/// sleeping, or pin WHICH failure was logged (<see cref="Entries"/>) instead of only that something
/// was. Usable directly where a plain <see cref="ILogger"/> is wanted, or through
/// <see cref="For{T}"/> where a typed <see cref="ILogger{T}"/> is required. Replaces four
/// near-identical private copies that had drifted apart across the coverage suites.
/// </summary>
internal sealed class CollectingLogger : ILogger
{
    private readonly List<(string Message, Exception? Exception)> _entries = [];
    private readonly object _gate = new();

    public IReadOnlyList<string> Messages
    {
        get { lock (_gate) return _entries.Select(entry => entry.Message).ToArray(); }
    }

    /// <summary>Message + attached exception — lets a test pin WHICH failure was logged.</summary>
    public IReadOnlyList<(string Message, Exception? Exception)> Entries
    {
        get { lock (_gate) return _entries.ToArray(); }
    }

    public ILogger<T> For<T>() => new Typed<T>(this);

    /// <summary>Polls until a loop has logged <paramref name="fragment"/>, or fails the test.</summary>
    public async Task WaitForAsync(string fragment, int occurrences = 1)
    {
        var deadline = DateTime.UtcNow.AddSeconds(30);
        while (DateTime.UtcNow < deadline)
        {
            if (Messages.Count(message => message.Contains(fragment, StringComparison.Ordinal)) >= occurrences)
                return;
            await Task.Delay(15);
        }

        Assert.Fail($"Timed out waiting for a log message containing '{fragment}'. Saw: {string.Join(" | ", Messages)}");
    }

    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => NullLogger.Instance.BeginScope(state);

    // Debug stays enabled so IsEnabled-guarded debug paths in the code under test are taken too.
    public bool IsEnabled(LogLevel logLevel) => true;

    public void Log<TState>(
        LogLevel logLevel,
        EventId eventId,
        TState state,
        Exception? exception,
        Func<TState, Exception?, string> formatter)
        => Add(formatter(state, exception), exception);

    private void Add(string message, Exception? exception)
    {
        lock (_gate) _entries.Add((message, exception));
    }

    private sealed class Typed<T>(CollectingLogger owner) : ILogger<T>
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => NullLogger.Instance.BeginScope(state);

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
            => owner.Add(formatter(state, exception), exception);
    }
}
