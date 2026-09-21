using System.Reflection;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// The contention decision in isolation: which job a lease records, and what a contended wake-up
/// may conclude from two observations of the lease in its way. No store, clock, or transport —
/// the executor-level behaviour is pinned in <see cref="DurableFlowOwnJobRedeliveryTests"/>.
/// </summary>
public sealed class FlowLeaseContentionTests
{
    private static readonly DateTime Expiry = new(2031, 3, 14, 9, 26, 53, DateTimeKind.Utc);

    /// <summary>The same lease, observed with a different persisted expiry.</summary>
    private static FlowLeaseObservation At(FlowLeaseObservation lease, DateTime expiresAtUtc)
        => new(lease.LeaseId, expiresAtUtc);

    // ---- The tag and the lease id that carries it. ----

    [Fact]
    public void JobTag_IsAFixedWidthUrlSafeDigest_StableForTheSameJob_AndDistinctAcrossJobs()
    {
        var tag = FlowLeaseContention.JobTag("job-1");

        Assert.NotNull(tag);
        Assert.Equal(FlowLeaseContention.JobTagLength, tag!.Length);
        Assert.Matches("^[A-Za-z0-9_-]{22}$", tag);
        Assert.Equal(tag, FlowLeaseContention.JobTag("job-1"));
        Assert.NotEqual(tag, FlowLeaseContention.JobTag("job-2"));
        // Ordinal, like the JobId contract: case is part of the identity.
        Assert.NotEqual(tag, FlowLeaseContention.JobTag("JOB-1"));
    }

    [Fact]
    public void JobTag_IsTheFirst22CharactersOfTheBase64UrlSha256OfTheUtf8JobId()
    {
        // The shape is a cross-deployment contract: two builds must derive the same tag for the
        // same job, or a redelivery handled by the newer one stops recognising the older one's lease.
        var digest = System.Security.Cryptography.SHA256.HashData(System.Text.Encoding.UTF8.GetBytes("ce0f6a2b9d7e4c0fa1b2c3d4e5f60718"));
        var expected = Convert.ToBase64String(digest).Replace('+', '-').Replace('/', '_')[..22];

        Assert.Equal(expected, FlowLeaseContention.JobTag("ce0f6a2b9d7e4c0fa1b2c3d4e5f60718"));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    public void AJobWithoutAnIdentity_HasNoTag(string? jobId)
        => Assert.Null(FlowLeaseContention.JobTag(jobId));

    [Fact]
    public void AnAttackerSizedJobId_NeverWidensTheLeaseId()
    {
        // JobId is a wire value a foreign producer controls; the relational stores keep the lease
        // id in a 64-character column (Oracle NVARCHAR2(64), SQL Server nvarchar(64), MySQL varchar(64)).
        var leaseId = FlowLeaseContention.NewLeaseId(FlowLeaseContention.JobTag(new string('x', 1_000_000)));

        Assert.Equal(55, leaseId.Length);
        Assert.True(leaseId.Length <= 64);
        Assert.Matches("^[0-9a-f]{32}\\.[A-Za-z0-9_-]{22}$", leaseId);
    }

    [Fact]
    public void LeaseId_RoundTripsItsTag_AndIsUniquePerAcquire()
    {
        var tag = FlowLeaseContention.JobTag("job-1");
        var first = FlowLeaseContention.NewLeaseId(tag);
        var second = FlowLeaseContention.NewLeaseId(tag);

        Assert.NotEqual(first, second);
        Assert.Equal(tag, FlowLeaseContention.JobTagOf(first));
        Assert.Equal(tag, FlowLeaseContention.JobTagOf(second));
    }

    [Fact]
    public void WithoutAJob_TheLeaseIdKeepsItsPrevious32CharacterForm()
    {
        var leaseId = FlowLeaseContention.NewLeaseId(jobTag: null);

        Assert.Matches("^[0-9a-f]{32}$", leaseId);
        Assert.Null(FlowLeaseContention.JobTagOf(leaseId));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("crashed-owner")]
    [InlineData("0123456789abcdef0123456789abcdef")]                           // a lease from a build before job tags
    [InlineData("0123456789abcdef0123456789abcdef.")]                          // separator, no tag
    [InlineData("0123456789abcdef0123456789abcdef.short")]                     // wrong tag width
    [InlineData("0123456789abcdef0123456789abcdef-AAAAAAAAAAAAAAAAAAAAAA")]    // right width, wrong separator
    [InlineData("0123456789abcdef0123456789abcdef.AAAAAAAAAAAAAAAAAAAAAAA")]   // one character too long
    public void ALeaseIdOfAnyOtherShape_CarriesNoTag(string? leaseId)
        => Assert.Null(FlowLeaseContention.JobTagOf(leaseId));

    // ---- The verdict. ----

    [Fact]
    public void AnUnchangedLease_ProvesNothing_WhicheverJobHoldsIt()
    {
        var own = FlowLeaseContention.JobTag("job-1");
        var sameJob = new FlowLeaseObservation(FlowLeaseContention.NewLeaseId(own), Expiry);
        var otherJob = new FlowLeaseObservation(FlowLeaseContention.NewLeaseId(FlowLeaseContention.JobTag("job-2")), Expiry);

        // A dead holder's lease reads exactly like this: it is waited out, never acknowledged.
        Assert.Equal(FlowLeaseContentionVerdict.KeepWaiting, FlowLeaseContention.Judge(sameJob, sameJob, own));
        Assert.Equal(FlowLeaseContentionVerdict.KeepWaiting, FlowLeaseContention.Judge(otherJob, otherJob, own));
        // An expiry that moved BACKWARDS is not a renewal.
        Assert.Equal(
            FlowLeaseContentionVerdict.KeepWaiting,
            FlowLeaseContention.Judge(otherJob, At(otherJob, Expiry.AddSeconds(-1)), own));
    }

    [Fact]
    public void ALeaseRenewedByTheHolderOfThisVeryJob_IsNeverADuplicate()
    {
        var own = FlowLeaseContention.JobTag("job-1");
        var baseline = new FlowLeaseObservation(FlowLeaseContention.NewLeaseId(own), Expiry);
        var renewed = At(baseline, Expiry.AddSeconds(20));

        Assert.Equal(FlowLeaseContentionVerdict.HolderOwnJobRedelivered, FlowLeaseContention.Judge(baseline, renewed, own));
    }

    [Fact]
    public void ALeaseTakenOverByAnotherDeliveryOfThisVeryJob_IsNeverADuplicateEither()
    {
        // Two redeliveries of one job in flight at once: the second took over a dead holder's
        // lease. The broker still has only ONE job for the run — this one.
        var own = FlowLeaseContention.JobTag("job-1");
        var baseline = new FlowLeaseObservation("crashed-owner", Expiry);
        var takenOver = new FlowLeaseObservation(FlowLeaseContention.NewLeaseId(own), Expiry);

        Assert.Equal(FlowLeaseContentionVerdict.HolderOwnJobRedelivered, FlowLeaseContention.Judge(baseline, takenOver, own));
    }

    [Fact]
    public void ALeaseRenewedOrTakenOverByADifferentJob_MakesThisDeliveryADuplicate()
    {
        var own = FlowLeaseContention.JobTag("job-1");
        var baseline = new FlowLeaseObservation(FlowLeaseContention.NewLeaseId(FlowLeaseContention.JobTag("job-2")), Expiry);

        Assert.Equal(
            FlowLeaseContentionVerdict.AcknowledgeDuplicate,
            FlowLeaseContention.Judge(baseline, At(baseline, Expiry.AddSeconds(20)), own));
        Assert.Equal(
            FlowLeaseContentionVerdict.AcknowledgeDuplicate,
            FlowLeaseContention.Judge(
                baseline,
                new FlowLeaseObservation(FlowLeaseContention.NewLeaseId(FlowLeaseContention.JobTag("job-3")), Expiry),
                own));

        // What the verdict follows is the CURRENT holder: a same-job baseline taken over by a
        // different job leaves that job's own delivery at the broker.
        Assert.Equal(
            FlowLeaseContentionVerdict.AcknowledgeDuplicate,
            FlowLeaseContention.Judge(new FlowLeaseObservation(FlowLeaseContention.NewLeaseId(own), Expiry), baseline, own));
    }

    [Fact]
    public void WhenEitherSideHasNoJobIdentity_TheEvidenceBasedAckStillApplies()
    {
        var tagged = new FlowLeaseObservation(FlowLeaseContention.NewLeaseId(FlowLeaseContention.JobTag("job-1")), Expiry);
        var untagged = new FlowLeaseObservation(FlowLeaseContention.NewLeaseId(jobTag: null), Expiry);

        // A delivery written before JobId existed, contending with a tagged lease.
        Assert.Equal(
            FlowLeaseContentionVerdict.AcknowledgeDuplicate,
            FlowLeaseContention.Judge(tagged, At(tagged, Expiry.AddSeconds(20)), ownJobTag: null));
        // A tagged delivery contending with a lease an older build issued.
        Assert.Equal(
            FlowLeaseContentionVerdict.AcknowledgeDuplicate,
            FlowLeaseContention.Judge(untagged, At(untagged, Expiry.AddSeconds(20)), FlowLeaseContention.JobTag("job-1")));
        // Neither side identified.
        Assert.Equal(
            FlowLeaseContentionVerdict.AcknowledgeDuplicate,
            FlowLeaseContention.Judge(untagged, At(untagged, Expiry.AddSeconds(20)), ownJobTag: null));
    }

    [Fact]
    public void AHolderWithoutAReadableExpiry_IsJudgedOnItsOwnerAlone()
    {
        var own = FlowLeaseContention.JobTag("job-1");
        var baseline = new FlowLeaseObservation(FlowLeaseContention.NewLeaseId(own), null);

        Assert.Equal(FlowLeaseContentionVerdict.KeepWaiting, FlowLeaseContention.Judge(baseline, baseline, own));
        Assert.Equal(
            FlowLeaseContentionVerdict.HolderOwnJobRedelivered,
            FlowLeaseContention.Judge(baseline, new FlowLeaseObservation(FlowLeaseContention.NewLeaseId(own), null), own));
    }

    // ---- The re-published copy. ----

    [Fact]
    public void TheRedelayCopy_CarriesEveryEnvelopeMember_ExceptTheFinishedDueTimeChain()
    {
        var job = new WorkerJobEnvelope
        {
            SchemaVersion = WorkerJobEnvelopeSchema.Current,
            Call = new ReflectionCallDto { ServiceInterfaceFullName = "My.IWorker", MethodName = "Run", Params = [] },
            CorrelationId = "corr-1",
            ReplyTarget = new AsyncResponseReplyTarget { Name = "default", Transport = "test", Address = "test://reply" },
            Context = new Dictionary<string, string> { ["tenant"] = "acme" },
            NotBeforeUtc = Expiry.AddHours(-1),
            LastRedelayRemaining = TimeSpan.FromMinutes(3),
            RedelayStallCount = 2,
            JobId = "job-1"
        };

        var copy = DurableFlowExecutor.CopyForRedelay(job, Expiry);

        Assert.NotSame(job, copy);
        Assert.Equal(Expiry, copy.NotBeforeUtc);
        // A fresh due-time chain: the stall proof belonged to the chain that ended with this delivery.
        Assert.Null(copy.LastRedelayRemaining);
        Assert.Equal(0, copy.RedelayStallCount);
        // The delivered instance is the transport's: it is never re-stamped.
        Assert.Equal(Expiry.AddHours(-1), job.NotBeforeUtc);

        // Every OTHER member rides along — a property added to the envelope later and forgotten
        // in the copy would silently drop it from the run's only remaining wake-up.
        string[] dueTimeChain = [nameof(WorkerJobEnvelope.NotBeforeUtc), nameof(WorkerJobEnvelope.LastRedelayRemaining), nameof(WorkerJobEnvelope.RedelayStallCount)];
        foreach (var property in typeof(WorkerJobEnvelope).GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            if (dueTimeChain.Contains(property.Name))
                continue;

            var original = property.GetValue(job);
            Assert.True(original is not null, $"The fixture must set {property.Name} so the copy can be checked for it.");
            Assert.Equal(original, property.GetValue(copy));
        }
    }
}
