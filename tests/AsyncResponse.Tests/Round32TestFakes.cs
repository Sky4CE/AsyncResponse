using Microsoft.Extensions.Logging;
using MongoDB.Bson;
using MongoDB.Driver;

namespace AsyncResponse.Tests;

/// <summary>
/// A log sink that throws from every <see cref="LogLevel.Error"/> entry — the shape of a
/// misbehaving provider (a disposed buffer, an exporter that lost its connection) at the exact
/// point the ACK-after-enqueue worker loops log a failed handler OUTSIDE their own guard.
/// </summary>
internal sealed class ErrorThrowingLogger : ILogger
{
    public TaskCompletionSource ErrorThrown { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public IDisposable? BeginScope<TState>(TState state)
        where TState : notnull
        => null;

    public bool IsEnabled(LogLevel logLevel) => true;

    public void Log<TState>(
        LogLevel logLevel,
        EventId eventId,
        TState state,
        Exception? exception,
        Func<TState, Exception?, string> formatter)
    {
        if (logLevel != LogLevel.Error)
            return;

        ErrorThrown.TrySetResult();
        throw new InvalidOperationException("log sink boom");
    }
}

/// <summary>One-batch <c>listIndexes</c> cursor over canned index documents.</summary>
internal sealed class BsonListCursor(List<BsonDocument> items) : IAsyncCursor<BsonDocument>
{
    private bool _moved;

    public IEnumerable<BsonDocument> Current => items;

    public bool MoveNext(CancellationToken cancellationToken = default)
    {
        if (_moved)
            return false;
        _moved = true;
        return true;
    }

    public Task<bool> MoveNextAsync(CancellationToken cancellationToken = default)
        => Task.FromResult(MoveNext(cancellationToken));

    public void Dispose()
    {
    }
}
