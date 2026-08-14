using AsyncResponse.Testing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace AsyncResponse.Tests;

public sealed record RecursiveChildInput(int RemainingChildren, bool FailAtLeaf = false, bool ContinueOnChildFailure = false);

public sealed record NaiveChildInput(string ParentCorrelationId);

public sealed class ChildFlowProbe
{
    private readonly Dictionary<string, int> _runs = new(StringComparer.Ordinal);

    public void Bump(string name)
    {
        lock (_runs)
            _runs[name] = Count(name) + 1;
    }

    public int Count(string name)
    {
        lock (_runs)
            return _runs.TryGetValue(name, out var count) ? count : 0;
    }
}

public sealed class RecursiveChildFlow(ChildFlowProbe _probe) : IDurableFlow<RecursiveChildInput>
{
    public async Task ExecuteAsync(IDurableFlowContext flow, RecursiveChildInput input)
    {
        _probe.Bump($"enter-{input.RemainingChildren}");

        if (input.RemainingChildren == 0)
        {
            if (input.FailAtLeaf)
                throw new DurableFlowFailedException("leaf failed");

            await flow.StepAsync("leaf", () =>
            {
                _probe.Bump("leaf");
                return Task.CompletedTask;
            });
        }
        else
        {
            var child = await flow.AwaitChildFlowAsync<RecursiveChildFlow, RecursiveChildInput>(
                $"child-{input.RemainingChildren}",
                input with { RemainingChildren = input.RemainingChildren - 1 },
                failOnChildFailure: !input.ContinueOnChildFailure);

            await flow.SetValueAsync($"child-{input.RemainingChildren}", child.FlowId);
        }

        await flow.StepAsync($"finish-{input.RemainingChildren}", () =>
        {
            _probe.Bump($"finish-{input.RemainingChildren}");
            return Task.CompletedTask;
        });
    }
}

public sealed class NaiveParentFlow(IDurableFlows _flows) : IDurableFlow<TestFlowInput>
{
    public async Task ExecuteAsync(IDurableFlowContext flow, TestFlowInput input)
    {
        await flow.AwaitStepAsync<OperationResult>(
            "run-child",
            cid => _flows.StartAsync<NaiveChildFlow, NaiveChildInput>(
                new NaiveChildInput(cid),
                flowId: $"{flow.FlowId}:child"),
            timeout: TimeSpan.FromMilliseconds(150));

        await flow.StepAsync("after-child", () => Task.CompletedTask);
    }
}

public sealed class NaiveChildFlow(ChildFlowProbe _probe, IAsyncResponsePublisher _publisher) : IDurableFlow<NaiveChildInput>
{
    public async Task ExecuteAsync(IDurableFlowContext flow, NaiveChildInput input)
    {
        _probe.Bump("naive-child");
        await _publisher.SetResponse(new OperationResult { Status = OperationStatus.Completed }, input.ParentCorrelationId);
    }
}

public class DurableChildFlowTests
{
    [Fact]
    public async Task RootFlowId_BeyondThePortableMaximum_IsRejectedAtStart()
    {
        await using var harness = await FlowTestHarness.StartAsync(options =>
        {
            options.ConfigureServices = services => services.AddSingleton(new ChildFlowProbe());
            options.ConfigureAsyncResponse = builder => builder.WithDurableFlow<RecursiveChildFlow, RecursiveChildInput>();
        });

        // Pre-fix this id was accepted by the unbounded stores (PostgreSQL, Redis, Mongo,
        // in-memory) and rejected by the 400-character-column ones (SQL Server, MySQL, Oracle,
        // EF Core) — the same application worked or failed depending on the provider package.
        var ex = await Assert.ThrowsAsync<ArgumentException>(() => harness.StartFlowAsync<RecursiveChildFlow, RecursiveChildInput>(
            new RecursiveChildInput(0),
            new string('x', DurableFlowOptions.MaxFlowIdLength + 1)));
        Assert.Contains("portable maximum is 400", ex.Message, StringComparison.Ordinal);
    }

    [Theory]
    // 400 characters is inside the character cap, but three-byte characters make it 1200 UTF-8
    // bytes — past the 1023-byte Cosmos id limit, so the id worked on every store except that one.
    [InlineData(400, '世', "UTF-8 bytes")]
    // Cosmos rejects these four characters in an id outright; the other stores accept them, so an
    // id containing one is portable right up until the day someone switches store.
    [InlineData(0, '/', "not portable")]
    [InlineData(0, '\\', "not portable")]
    [InlineData(0, '?', "not portable")]
    [InlineData(0, '#', "not portable")]
    public async Task RootFlowId_ThatIsNotPortable_IsRejectedAtStart(int repeat, char character, string expectedMessage)
    {
        await using var harness = await FlowTestHarness.StartAsync(options =>
        {
            options.ConfigureServices = services => services.AddSingleton(new ChildFlowProbe());
            options.ConfigureAsyncResponse = builder => builder.WithDurableFlow<RecursiveChildFlow, RecursiveChildInput>();
        });

        var flowId = repeat > 0 ? new string(character, repeat) : $"flow{character}a";
        var ex = await Assert.ThrowsAsync<ArgumentException>(() => harness.StartFlowAsync<RecursiveChildFlow, RecursiveChildInput>(
            new RecursiveChildInput(0),
            flowId));
        Assert.Contains(expectedMessage, ex.Message, StringComparison.Ordinal);
    }

    [Theory]
    // Shapes, not literals: xUnit serializes theory arguments and that round trip substitutes
    // U+FFFD for an unpaired surrogate, which would hand the test a well-formed string.
    [InlineData("high")]
    [InlineData("low")]
    [InlineData("high-then-char")]
    public async Task RootFlowId_ThatIsIllFormedUtf16_IsRejectedAtStart(string shape)
    {
        var flowId = "flow-" + shape switch
        {
            "high" => "\ud800",
            "low" => "\udc00",
            _ => "\ud800x"
        };

        // The flow-id twin of the correlation-id rule, and it matters here for a second reason:
        // the UTF-8 BYTE budget this contract enforces is measured with an encoder that substitutes
        // U+FFFD for an unpaired surrogate, so the count would be taken against a different string
        // than the caller passed. Rejected before that measurement, not after it.
        await using var harness = await FlowTestHarness.StartAsync(options =>
        {
            options.ConfigureServices = services => services.AddSingleton(new ChildFlowProbe());
            options.ConfigureAsyncResponse = builder => builder.WithDurableFlow<RecursiveChildFlow, RecursiveChildInput>();
        });

        var ex = await Assert.ThrowsAsync<ArgumentException>(() => harness.StartFlowAsync<RecursiveChildFlow, RecursiveChildInput>(
            new RecursiveChildInput(0),
            flowId));
        Assert.Contains("unpaired surrogate", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task RootFlowId_WithASupplementaryCharacter_IsAcceptedAndRuns()
    {
        // The false-positive guard for the rule above: a real supplementary character IS a surrogate
        // pair, so a check that rejected surrogates rather than ILL-FORMED ones would refuse every
        // emoji and most CJK-extension id. It also has to survive the UTF-8 byte budget, which this
        // contract measures separately — four bytes for this one character, not two.
        await using var harness = await FlowTestHarness.StartAsync(options =>
        {
            options.ConfigureServices = services => services.AddSingleton(new ChildFlowProbe());
            options.ConfigureAsyncResponse = builder => builder.WithDurableFlow<RecursiveChildFlow, RecursiveChildInput>();
        });

        var run = await harness.StartFlowAsync<RecursiveChildFlow, RecursiveChildInput>(
            new RecursiveChildInput(0),
            "flow-\U0001F600-ok");

        Assert.Equal(FlowRunStatus.Succeeded, await run.WaitForFinishedAsync());
    }

    [Theory]
    [InlineData("flow-a ")]
    [InlineData(" flow-a")]
    public async Task RootFlowId_WithSurroundingSpaces_IsRejectedAtStart(string flowId)
    {
        // Probed on SQL Server 2022: equality pads the shorter operand even under
        // Latin1_General_100_BIN2, and MySQL's utf8mb4_bin is PAD SPACE — so 'flow-a ' and
        // 'flow-a' are ONE primary key to those stores, while the engine compares ids ordinally
        // and believes it started two different runs. The second create fails as a duplicate and a
        // load returns the other run's state.
        await using var harness = await FlowTestHarness.StartAsync(options =>
        {
            options.ConfigureServices = services => services.AddSingleton(new ChildFlowProbe());
            options.ConfigureAsyncResponse = builder => builder.WithDurableFlow<RecursiveChildFlow, RecursiveChildInput>();
        });

        var ex = await Assert.ThrowsAsync<ArgumentException>(() => harness.StartFlowAsync<RecursiveChildFlow, RecursiveChildInput>(
            new RecursiveChildInput(0),
            flowId));
        Assert.Contains("begins or ends with a space", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ChildFlowId_ComposedBeyondThePortableMaximum_FailsTheParentTerminally()
    {
        var probe = new ChildFlowProbe();
        await using var harness = await FlowTestHarness.StartAsync(options =>
        {
            options.ConfigureServices = services => services.AddSingleton(probe);
            options.ConfigureAsyncResponse = builder => builder.WithDurableFlow<RecursiveChildFlow, RecursiveChildInput>();
        });

        // The root id fits (395 ≤ 400) but the composed "{parent}:child-1" (403) does not. The
        // composition is deterministic on every replay, so the parent fails terminally with the
        // budget in the message instead of burning its redelivery budget on a store rejection.
        var rootId = new string('p', DurableFlowOptions.MaxFlowIdLength - 5);
        var run = await harness.StartFlowAsync<RecursiveChildFlow, RecursiveChildInput>(new RecursiveChildInput(1), rootId);
        Assert.Equal(FlowRunStatus.Failed, await run.WaitForFinishedAsync());
        var state = await run.GetStateAsync();
        Assert.Contains("non-portable child flow id", state!.LastMessage, StringComparison.Ordinal);
        Assert.Contains("portable maximum is 400", state.LastMessage, StringComparison.Ordinal);
        Assert.Equal(0, probe.Count("leaf"));
    }

    private static ServiceProvider CreateProvider()
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddSingleton<ChildFlowProbe>();
        services.AddScoped<RecursiveChildFlow>();
        services.AddScoped<NaiveParentFlow>();
        services.AddScoped<NaiveChildFlow>();
        services.AddAsyncResponse(options => options.Watchdog.Enabled = false)
            .WithInMemoryChannel()
            .WithInMemoryTransport()
            .WithInMemoryDurableFlows();
        return services.BuildServiceProvider();
    }

    [Fact]
    public async Task AwaitChildFlow_CompletesMultipleNestedFlows_OnSingleInMemoryWorker()
    {
        await using var provider = CreateProvider();
        var hosted = await StartHostedServicesAsync(provider);
        try
        {
            var flows = provider.GetRequiredService<IDurableFlows>();
            var probe = provider.GetRequiredService<ChildFlowProbe>();

            var rootId = await flows.StartAsync<RecursiveChildFlow, RecursiveChildInput>(
                new RecursiveChildInput(RemainingChildren: 2),
                flowId: "root-nested");

            var root = await WaitForStateAsync(flows, rootId, FlowRunStatus.Succeeded);
            var child = await flows.GetStateAsync("root-nested:child-2");
            var grandchild = await flows.GetStateAsync("root-nested:child-2:child-1");

            Assert.Equal(FlowRunStatus.Succeeded, child!.Status);
            Assert.Equal(FlowRunStatus.Succeeded, grandchild!.Status);
            Assert.Equal("root-nested", child.ParentFlowId);
            Assert.Equal("child-2", child.ParentStepName);
            Assert.Equal("root-nested:child-2", grandchild.ParentFlowId);
            Assert.Equal("child-1", grandchild.ParentStepName);
            Assert.True(root.Steps!["child-2"].Completed);
            Assert.Equal("root-nested:child-2", root.Steps["child-2"].ChildFlowId);
            Assert.True(child.Steps!["child-1"].Completed);
            Assert.Equal("root-nested:child-2:child-1", child.Steps["child-1"].ChildFlowId);
            Assert.Equal(1, probe.Count("leaf"));
            Assert.Equal(1, probe.Count("finish-0"));
            Assert.Equal(1, probe.Count("finish-1"));
            Assert.Equal(1, probe.Count("finish-2"));
        }
        finally
        {
            await StopHostedServicesAsync(hosted);
        }
    }

    [Fact]
    public async Task AwaitChildFlow_FailedChild_FailsParentByDefault()
    {
        await using var provider = CreateProvider();
        var hosted = await StartHostedServicesAsync(provider);
        try
        {
            var flows = provider.GetRequiredService<IDurableFlows>();

            var rootId = await flows.StartAsync<RecursiveChildFlow, RecursiveChildInput>(
                new RecursiveChildInput(RemainingChildren: 1, FailAtLeaf: true),
                flowId: "root-failed-child");

            var root = await WaitForStateAsync(flows, rootId, FlowRunStatus.Failed);
            var child = await flows.GetStateAsync("root-failed-child:child-1");

            Assert.Equal(FlowRunStatus.Failed, child!.Status);
            Assert.Contains("Child flow 'root-failed-child:child-1' failed", root.LastMessage);
            Assert.True(root.Steps!["child-1"].Completed);
            // The memoized failed child keeps the step's Faulted marker so operators can spot the
            // failure on the step itself instead of digging through ResultJson.
            Assert.True(root.Steps["child-1"].Faulted);
            Assert.Equal("root-failed-child:child-1", root.Steps["child-1"].ChildFlowId);
        }
        finally
        {
            await StopHostedServicesAsync(hosted);
        }
    }

    [Fact]
    public async Task AwaitChildFlow_FailedChild_CanBeHandledAsData()
    {
        await using var provider = CreateProvider();
        var hosted = await StartHostedServicesAsync(provider);
        try
        {
            var flows = provider.GetRequiredService<IDurableFlows>();

            var rootId = await flows.StartAsync<RecursiveChildFlow, RecursiveChildInput>(
                new RecursiveChildInput(RemainingChildren: 1, FailAtLeaf: true, ContinueOnChildFailure: true),
                flowId: "root-handled-child");

            var root = await WaitForStateAsync(flows, rootId, FlowRunStatus.Succeeded);
            var child = await flows.GetStateAsync("root-handled-child:child-1");

            Assert.Equal(FlowRunStatus.Failed, child!.Status);
            Assert.True(root.Steps!["child-1"].Completed);
            Assert.True(root.Steps["finish-1"].Completed);
            Assert.Contains("root-handled-child:child-1", root.Values!["child-1"]);
        }
        finally
        {
            await StopHostedServicesAsync(hosted);
        }
    }

    [Fact]
    public async Task ManualChildWait_StarvesSingleInMemoryWorker_RegressionProof()
    {
        await using var provider = CreateProvider();
        var hosted = await StartHostedServicesAsync(provider);
        try
        {
            var flows = provider.GetRequiredService<IDurableFlows>();
            var probe = provider.GetRequiredService<ChildFlowProbe>();

            var rootId = await flows.StartAsync<NaiveParentFlow, TestFlowInput>(
                new TestFlowInput(7),
                flowId: "naive-root");

            FlowState? root;
            var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(5);
            do
            {
                root = await flows.GetStateAsync(rootId);
                if (root?.Steps?.TryGetValue("run-child", out var step) == true
                    && step.Faulted
                    && probe.Count("naive-child") == 1)
                {
                    break;
                }

                await Task.Delay(25);
            } while (DateTime.UtcNow < deadline);

            Assert.Equal(FlowRunStatus.Running, root!.Status);
            Assert.True(root.Steps!["run-child"].Faulted);
            Assert.False(root.Steps["run-child"].Completed);
            Assert.Equal(1, probe.Count("naive-child"));
        }
        finally
        {
            await StopHostedServicesAsync(hosted);
        }
    }

    private static async Task<FlowState> WaitForStateAsync(IDurableFlows flows, string flowId, FlowRunStatus status)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
        FlowState? state;
        do
        {
            state = await flows.GetStateAsync(flowId);
            if (state?.Status == status)
                return state;

            await Task.Delay(25);
        } while (DateTime.UtcNow < deadline);

        throw new TimeoutException($"Flow {flowId} did not reach {status}; last status was {state?.Status} ({state?.LastMessage}).");
    }

    private static async Task<IReadOnlyList<IHostedService>> StartHostedServicesAsync(IServiceProvider provider)
    {
        var hosted = provider.GetServices<IHostedService>().ToArray();
        foreach (var service in hosted)
            await service.StartAsync(CancellationToken.None);
        return hosted;
    }

    private static async Task StopHostedServicesAsync(IEnumerable<IHostedService> hosted)
    {
        foreach (var service in hosted)
            await service.StopAsync(CancellationToken.None);
    }
}
