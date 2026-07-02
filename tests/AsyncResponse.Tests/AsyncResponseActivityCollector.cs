using System.Diagnostics;
using Xunit;

namespace AsyncResponse.Tests;

internal sealed class AsyncResponseActivityCollector : IDisposable
{
    private readonly object _gate = new();
    private readonly List<Activity> _activities = [];
    private readonly ActivityListener _listener;
    private readonly Activity? _previousActivity;
    private readonly Activity _scope;
    private readonly ActivityTraceId _traceId;

    public AsyncResponseActivityCollector()
    {
        _previousActivity = Activity.Current;
        Activity.Current = null;
        _scope = new Activity("AsyncResponseActivityCollector").Start();
        _traceId = _scope.TraceId;
        _listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == AsyncResponseDiagnostics.ActivitySourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            SampleUsingParentId = (ref ActivityCreationOptions<string> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = activity =>
            {
                if (activity.TraceId != _traceId)
                    return;

                lock (_gate)
                    _activities.Add(activity);
            }
        };
        ActivitySource.AddActivityListener(_listener);
    }

    public Activity Single(string name, string tagKey, object? tagValue)
    {
        lock (_gate)
            return Assert.Single(_activities, activity => activity.OperationName == name && Equals(Tag(activity, tagKey), tagValue));
    }

    public static object? Tag(Activity activity, string key)
        => activity.TagObjects.FirstOrDefault(tag => tag.Key == key).Value;

    public void Dispose()
    {
        _listener.Dispose();
        _scope.Stop();
        Activity.Current = _previousActivity;
    }
}
