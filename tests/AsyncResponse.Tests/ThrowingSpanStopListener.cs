using System.Diagnostics;

namespace AsyncResponse.Tests;

/// <summary>
/// A telemetry listener whose <c>ActivityStopped</c> throws for one AsyncResponse span name, scoped
/// to THIS test: the constructor starts a private root activity, and only spans in its trace are
/// sampled or failed. Every span the test's flow starts inherits that trace through
/// <see cref="Activity.Current"/>, so parallel tests never see the throw (an exception out of one
/// listener also skips the listeners behind it, which would make unrelated tests flaky).
/// </summary>
internal sealed class ThrowingSpanStopListener : IDisposable
{
    private readonly ActivityListener _listener;
    private readonly Activity? _previous;
    private readonly Activity _scope;
    private int _thrown;

    public ThrowingSpanStopListener(string operationName = "asyncresponse.wait")
    {
        _previous = Activity.Current;
        Activity.Current = null;
        _scope = new Activity(nameof(ThrowingSpanStopListener)).Start();
        var traceId = _scope.TraceId;
        _listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == AsyncResponseDiagnostics.ActivitySourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> options) =>
                options.Parent.TraceId == traceId ? ActivitySamplingResult.AllDataAndRecorded : ActivitySamplingResult.None,
            ActivityStopped = activity =>
            {
                if (activity.TraceId != traceId || activity.OperationName != operationName)
                    return;

                Interlocked.Increment(ref _thrown);
                throw new InvalidOperationException("telemetry listener failed");
            }
        };
        ActivitySource.AddActivityListener(_listener);
    }

    /// <summary>How many times the listener threw — a test that never reached it proved nothing.</summary>
    public int Thrown => Volatile.Read(ref _thrown);

    public void Dispose()
    {
        _listener.Dispose();
        _scope.Stop();
        Activity.Current = _previous;
    }
}
