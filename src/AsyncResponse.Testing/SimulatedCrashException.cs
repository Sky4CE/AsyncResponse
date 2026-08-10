namespace AsyncResponse.Testing;

/// <summary>
/// The exception <see cref="FlowTestHarness"/> throws from an injected crash point
/// (<see cref="FlowTestHarness.CrashBeforeStep"/> / <see cref="FlowTestHarness.CrashAfterStep"/>).
/// It rides the executor's ordinary failure path: the execution attempt fails exactly as if the
/// process had died at that step boundary, and the next delivery resumes from the last checkpoint.
/// Assert on this type to distinguish injected crashes from real test failures.
/// </summary>
public sealed class SimulatedCrashException(string stepName, bool beforeStep)
    : Exception($"Simulated crash {(beforeStep ? "before" : "after")} step '{stepName}'.")
{
    /// <summary>The step the crash was injected at.</summary>
    public string StepName { get; } = stepName;

    /// <summary>Whether the crash fired before the step executed (<c>true</c>) or after its checkpoint persisted (<c>false</c>).</summary>
    public bool BeforeStep { get; } = beforeStep;
}
