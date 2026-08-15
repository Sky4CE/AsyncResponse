using MongoDB.Driver;

namespace AsyncResponse.Internal;

/// <summary>
/// Classifies MongoDB driver exceptions as transient (worth an in-process retry) versus
/// permanent. Shared by every MongoDB retry loop so the classification cannot drift between the
/// channel's publish retry and the transport's publish retry.
/// </summary>
internal static class MongoTransientFaults
{
    public static bool IsTransient(Exception exception)
        => exception is not OperationCanceledException
           && (exception is MongoConnectionException
               or MongoNotPrimaryException
               or MongoNodeIsRecoveringException
               or MongoExecutionTimeoutException
               or TimeoutException
               || (exception is MongoException mongoException && mongoException.HasErrorLabel("RetryableWriteError")));
}
