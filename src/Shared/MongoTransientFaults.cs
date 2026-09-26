using MongoDB.Driver;

namespace AsyncResponse.Internal;

/// <summary>
/// Classifies MongoDB driver exceptions as transient (worth an in-process retry) versus
/// permanent. Shared by every MongoDB retry loop so the classification cannot drift between the
/// channel's publish retry and the transport's publish retry.
/// </summary>
/// <remarks>
/// A lapsed majority <c>wtimeout</c> (<see cref="MongoWriteConcerns.IsReplicationTimeout"/>) is
/// deliberately NOT transient: the write already applied on the primary, and a retry re-waits on
/// the same replication — lapsing again for as long as the set stays degraded — or, for a
/// consumable transport document, re-creates a job a subscriber already ran and deleted. The
/// stores settle it where they write instead (see <see cref="MongoWriteConcerns.BoundedMajority(MongoDB.Driver.WriteConcern?, TimeSpan)"/>).
/// </remarks>
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
