using AsyncResponse.Channels.NATS;
using Xunit;

namespace AsyncResponse.Tests;

public class NatsAsyncResponseChannelOptionsTests
{
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
