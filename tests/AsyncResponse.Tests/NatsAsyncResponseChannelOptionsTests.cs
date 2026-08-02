using AsyncResponse.Channels.NATS;
using Xunit;

namespace AsyncResponse.Tests;

public class NatsAsyncResponseChannelOptionsTests
{
    [Fact]
    public void RecoveryKey_InvalidBase64Length_FallsBackToVerbatimKey()
        => Assert.Equal("a", NatsSubjectSchema.CorrelationIdFromRecoveryKey("a"));

    [Fact]
    public void Validate_Passes_ForDefaults() => new NatsAsyncResponseChannelOptions().Validate();

    [Fact]
    public void Validate_Throws_ForEmptySubjectPrefix()
        => AssertInvalid(o => o.SubjectPrefix = "  ");

    [Fact]
    public void Validate_Throws_ForSubjectPrefixWithWildcard()
    {
        AssertInvalid(o => o.SubjectPrefix = "ap*p");
        AssertInvalid(o => o.SubjectPrefix = "app>");
        AssertInvalid(o => o.SubjectPrefix = "a b");
    }

    [Fact]
    public void Validate_Throws_ForEmptyRecoveryBucket()
        => AssertInvalid(o => o.RecoveryBucket = " ");

    [Fact]
    public void Validate_Throws_ForInvalidRecoveryBucketCharacters()
    {
        AssertInvalid(o => o.RecoveryBucket = "bad.bucket");
        AssertInvalid(o => o.RecoveryBucket = "bad bucket");
    }

    [Fact]
    public void Validate_Accepts_DashAndUnderscoreBucket()
        => new NatsAsyncResponseChannelOptions { RecoveryBucket = "ar-recovery_1" }.Validate();

    [Fact]
    public void Validate_Throws_ForNonPositiveReplicas()
        => AssertInvalid(o => o.RecoveryBucketReplicas = 0);

    [Fact]
    public void Validate_Throws_ForNonPositiveTimers()
    {
        AssertInvalid(o => o.RecoveryStateExpiry = TimeSpan.Zero);
        // Shared-base knob: enforced via ValidateShared — the bespoke validator accepted
        // TimeSpan.Zero here, defeating the promised disposal bound.
        AssertInvalid(o => o.DisposalDrainTimeout = TimeSpan.Zero);
        // The ~49.7-day BCL timer ceiling: every one of these knobs arms a timer the runtime
        // rejects above uint.MaxValue - 1 ms, and RecoveryStateExpiry doubles as the
        // waiter-timeout fallback — over-ceiling values used to throw only at arming, AFTER the
        // subscription and recovery state existed.
        AssertInvalid(o => o.RecoveryStateExpiry = TimeSpan.FromDays(50));
        AssertInvalid(o => o.DefaultTimeout = TimeSpan.FromDays(50));
        AssertInvalid(o => o.DisposalDrainTimeout = TimeSpan.FromDays(50));
        AssertInvalid(o => o.DeliveryConfirmationTimeout = TimeSpan.Zero);
        AssertInvalid(o => o.PresenceProbeTimeout = TimeSpan.FromSeconds(-1));
        AssertInvalid(o => o.DefaultTimeout = TimeSpan.Zero);
    }

    [Fact]
    public void Validate_Throws_ForNegativeRemoteStackTraceLength()
        => AssertInvalid(o => o.MaxRemoteStackTraceLength = -1);

    [Fact]
    public void Validate_Accepts_NullDefaultTimeout()
        => new NatsAsyncResponseChannelOptions { DefaultTimeout = null }.Validate();

    private static void AssertInvalid(Action<NatsAsyncResponseChannelOptions> mutate)
    {
        var options = new NatsAsyncResponseChannelOptions();
        mutate(options);
        Assert.Throws<InvalidOperationException>(options.Validate);
    }
}
