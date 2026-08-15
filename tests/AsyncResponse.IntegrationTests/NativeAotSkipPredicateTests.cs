using Xunit;

namespace AsyncResponse.IntegrationTests;

/// <summary>
/// The AOT gate's skip is legitimate ONLY for a missing platform toolchain: a publish that failed
/// carrying a trim/AOT analysis finding must fail the gate even when toolchain-probe text also
/// appears in its output — otherwise the exact defect class the gate exists for becomes a green
/// skip that nothing ever reports.
/// </summary>
[Trait(Batches.Trait, Batches.None)]
public sealed class NativeAotSkipPredicateTests
{
    [Fact]
    public void MissingIlCompilerAlone_Skips_AndNamesTheProbe()
    {
        var probe = NativeAotPublishGateTests.MissingNativeToolchainProbe(
            exitCode: 1,
            "error : Microsoft.DotNet.ILCompiler is not supported on this platform.");

        Assert.Equal("Microsoft.DotNet.ILCompiler is not supported", probe);
    }

    [Fact]
    public void MissingPlatformLinkerAlone_Skips()
    {
        Assert.NotNull(NativeAotPublishGateTests.MissingNativeToolchainProbe(1, "error : Platform linker not found."));
        Assert.NotNull(NativeAotPublishGateTests.MissingNativeToolchainProbe(1, "Platform linker ('clang') could not be run."));
    }

    [Fact]
    public void ProbeText_WithTrimAnalysisError_DoesNotSkip()
    {
        var output =
            "ILC: Microsoft.DotNet.ILCompiler is not supported in this configuration." + Environment.NewLine +
            "src/Thing.cs(12,3): error IL2026: Using member 'X' which has 'RequiresUnreferencedCodeAttribute'.";

        Assert.Null(NativeAotPublishGateTests.MissingNativeToolchainProbe(1, output));
    }

    [Fact]
    public void ProbeText_WithAotAnalysisWarning_DoesNotSkip()
    {
        // -warnaserror fails the publish on the warning even when a component prints it
        // un-promoted, so a failing publish containing it is a real finding, not a toolchain gap.
        var output =
            "error : Platform linker not found" + Environment.NewLine +
            "src/Thing.cs(1,1): warning IL3050: Using member 'Y' which has 'RequiresDynamicCodeAttribute'.";

        Assert.Null(NativeAotPublishGateTests.MissingNativeToolchainProbe(1, output));
    }

    [Fact]
    public void SuccessfulPublish_NeverSkips()
        => Assert.Null(NativeAotPublishGateTests.MissingNativeToolchainProbe(0, "Microsoft.DotNet.ILCompiler is not supported"));

    [Fact]
    public void FailureWithoutProbeText_DoesNotSkip()
        => Assert.Null(NativeAotPublishGateTests.MissingNativeToolchainProbe(1, "error CS1002: ; expected"));
}
