using AsyncResponse.Channels.NATS;
using System.Text;
using Xunit;

namespace AsyncResponse.Tests;

public class NatsAsyncResponseChannelOptionsTests
{
    [Fact]
    public void RecoveryKey_InvalidBase64Length_FallsBackToVerbatimKey()
        => Assert.Equal("a", NatsSubjectSchema.CorrelationIdFromRecoveryKey("a"));

    [Fact]
    public void Encode_RefusesIllFormedUtf16_RatherThanCollidingWithTheReplacementCharacter()
    {
        // The mechanism behind the whole ill-formed-UTF-16 rule, shown at the place it does damage.
        // The default UTF-8 encoder substitutes U+FFFD for an unpaired surrogate instead of
        // failing, so "corr-\ud800" and "corr-�" encode to the SAME bytes — and therefore the
        // same Base64 token, which is the response subject a waiter subscribes to AND the key its
        // recovery state is stored under. Two conversations, one mailbox: whichever waiter is
        // listening gets the other's response. Validation rejects such ids at the public boundary;
        // this makes the collision unreachable from anywhere else, including an id read back from
        // an older store.
        Assert.Throws<EncoderFallbackException>(() => NatsSubjectSchema.Encode("corr-\ud800"));

        // The literal replacement character is a perfectly ordinary id and still encodes.
        var replacement = NatsSubjectSchema.Encode("corr-�");
        Assert.Equal("corr-�", NatsSubjectSchema.Decode(replacement));

        // And a real supplementary character — a well-formed pair — round-trips untouched.
        var emoji = NatsSubjectSchema.Encode("corr-\U0001F600");
        Assert.Equal("corr-\U0001F600", NatsSubjectSchema.Decode(emoji));
        Assert.NotEqual(replacement, emoji);
    }

    [Fact]
    public void Decode_OfBytesThatAreNotUtf8_FallsBackToVerbatimKey()
    {
        // A key some other producer wrote: strict decoding must report "not mine" rather than
        // manufacture a U+FFFD-bearing correlation id that could then collide with a real one.
        var notUtf8 = Convert.ToBase64String([0xC3, 0x28]).TrimEnd('=').Replace('+', '-').Replace('/', '_');

        Assert.Null(NatsSubjectSchema.Decode(notUtf8));
        Assert.Equal(notUtf8, NatsSubjectSchema.CorrelationIdFromRecoveryKey(notUtf8));
    }

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
        // The ~49.7-day BCL timer ceiling: these knobs arm timers the runtime rejects above
        // uint.MaxValue - 1 ms — over-ceiling values used to throw only at arming, AFTER the
        // subscription and recovery state existed. RecoveryStateExpiry is ceiling-bound only in
        // its timer-armed role (the waiter-timeout fallback while DefaultTimeout is null).
        AssertInvalid(o => o.RecoveryStateExpiry = TimeSpan.FromDays(50));
        AssertInvalid(o => o.DefaultTimeout = TimeSpan.FromDays(50));
        AssertInvalid(o => o.DisposalDrainTimeout = TimeSpan.FromDays(50));
        AssertInvalid(o => o.DeliveryConfirmationTimeout = TimeSpan.Zero);
        AssertInvalid(o => o.PresenceProbeTimeout = TimeSpan.FromSeconds(-1));
        AssertInvalid(o => o.DefaultTimeout = TimeSpan.Zero);
    }

    [Fact]
    public void Validate_Accepts_LongRecoveryRetentionWhenDefaultTimeoutIsConfigured()
        // As a pure persistence TTL — with DefaultTimeout supplying every waiter's timer — the
        // expiry legitimately exceeds the timer ceiling; capping it unconditionally blocked
        // 90-day recovery retention for no reason.
        => new NatsAsyncResponseChannelOptions
        {
            RecoveryStateExpiry = TimeSpan.FromDays(90),
            DefaultTimeout = TimeSpan.FromHours(12)
        }.Validate();

    [Fact]
    public void Validate_Throws_ForOverflowingRecoveryRetention()
        // TimeSpan.MaxValue slipped past the conditional timer ceiling (DefaultTimeout set means
        // the expiry is a pure TTL) but overflowed the "now + expiry" stamp at the first
        // recovery-state save — the persistence bound catches it at startup instead.
        => AssertInvalid(o =>
        {
            o.RecoveryStateExpiry = TimeSpan.MaxValue;
            o.DefaultTimeout = TimeSpan.FromHours(12);
        });

    [Fact]
    public void Validate_Throws_ForNegativeRemoteStackTraceLength()
        => AssertInvalid(o => o.MaxRemoteStackTraceLength = -1);

    [Fact]
    public void Validate_Accepts_NullDefaultTimeout()
        => new NatsAsyncResponseChannelOptions { DefaultTimeout = null }.Validate();

    /// <summary>
    /// Regression (round 33): the prefix guard rejected whitespace and the wildcards but not an
    /// EMPTY subject token. A leading, trailing or doubled '.' passed startup; nats-server then
    /// rejected every SUB with a non-fatal -ERR that NATS.Net never surfaces, so every waiter
    /// registered with no server-side interest and every response took the lost-subscriber path
    /// while the live waiter ran to its timeout. Pre-fix: <c>Validate</c> accepted all three.
    /// </summary>
    [Theory]
    [InlineData(".asyncresponse")]
    [InlineData("asyncresponse.")]
    [InlineData("async..response")]
    public void Validate_Throws_ForSubjectPrefixWithAnEmptyToken(string prefix)
        => AssertInvalid(o => o.SubjectPrefix = prefix);

    /// <summary>
    /// A dotted prefix is the intended namespacing ("my.app" → my.app.response.&lt;id&gt;) and must
    /// keep passing: the rule is about EMPTY tokens, not about dots.
    /// </summary>
    [Fact]
    public void Validate_Accepts_DottedSubjectPrefix()
        => new NatsAsyncResponseChannelOptions { SubjectPrefix = "my.app" }.Validate();

    private static void AssertInvalid(Action<NatsAsyncResponseChannelOptions> mutate)
    {
        var options = new NatsAsyncResponseChannelOptions();
        mutate(options);
        Assert.Throws<InvalidOperationException>(options.Validate);
    }
}
