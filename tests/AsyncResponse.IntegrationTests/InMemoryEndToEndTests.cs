using AsyncResponse.Sample;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.DependencyInjection;
using System.Net;
using System.Net.Http.Json;
using Xunit;

namespace AsyncResponse.IntegrationTests;

/// <summary>
/// End-to-end coverage of the fully in-memory stack (in-memory channel + in-memory worker transport),
/// hosted in-process with <see cref="WebApplicationFactory{TEntryPoint}"/> — no Docker, so these run
/// everywhere (including the default CI job). They exercise the same sample app the Aspire/Redis tests
/// drive, just configured for the zero-dependency path.
/// </summary>
public sealed class InMemoryEndToEndTests : IClassFixture<InMemoryEndToEndTests.InMemoryAppFactory>, IAsyncLifetime
{
    private readonly InMemoryAppFactory _factory;
    private readonly HttpClient _client;

    public InMemoryEndToEndTests(InMemoryAppFactory factory)
    {
        _factory = factory;
        _client = factory.CreateClient();
    }

    // Reset the recovery store + flow recorder before each test (the app instance is shared per class).
    public async ValueTask InitializeAsync()
        => (await _client.PostAsync("/test/reset", content: null)).EnsureSuccessStatusCode();

    public ValueTask DisposeAsync()
    {
        _client.Dispose();
        return ValueTask.CompletedTask;
    }

    [Fact]
    public async Task RequestResponse_Succeed_ConsumesProgressAndCompletes()
    {
        var response = await _client.PostAsync("/request-response?behavior=Succeed", content: null);

        response.EnsureSuccessStatusCode();
        var result = await response.Content.ReadFromJsonAsync<Result>();
        Assert.Equal(OperationStatus.Completed, result!.Status);
        Assert.Equal("done", result.Message);
    }

    [Fact]
    public async Task RequestResponse_FailDomain_ReturnsFailedPayload()
    {
        var response = await _client.PostAsync("/request-response?behavior=FailDomain", content: null);

        response.EnsureSuccessStatusCode();
        var result = await response.Content.ReadFromJsonAsync<Result>();
        Assert.Equal(OperationStatus.Failed, result!.Status);
        Assert.Equal("remote failed", result.Message);
    }

    [Fact]
    public async Task RequestResponse_Fail_FaultsWaiterWith500()
    {
        var response = await _client.PostAsync("/request-response?behavior=Fail", content: null);

        Assert.Equal(HttpStatusCode.InternalServerError, response.StatusCode);
        Assert.Contains("remote technical error", await response.Content.ReadAsStringAsync(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task RequestResponse_Timeout_Returns504()
    {
        var response = await _client.PostAsync("/request-response?behavior=Timeout", content: null);

        Assert.Equal(HttpStatusCode.GatewayTimeout, response.StatusCode);
    }

    [Fact]
    public async Task Attach_CompletesByCorrelationId()
    {
        // For<T>(correlationId) is wait-only: the /attach endpoint starts the work then awaits by id.
        var response = await _client.PostAsync("/attach", content: null);

        response.EnsureSuccessStatusCode();
        var result = await response.Content.ReadFromJsonAsync<Result>();
        Assert.Equal(OperationStatus.Completed, result!.Status);
    }

    [Fact]
    public async Task MultiStep_SucceedThenSucceed_CompletesBothStepsInOrder()
    {
        var response = await _client.PostAsync("/multi-step?first=Succeed&second=Succeed", content: null);

        response.EnsureSuccessStatusCode();
        var result = await response.Content.ReadFromJsonAsync<MultiStepFlowResult>();
        Assert.True(result!.Completed);
        Assert.Null(result.FailedAt);
        Assert.Collection(
            result.Steps,
            first =>
            {
                Assert.Equal("first", first.Name);
                Assert.True(first.Succeeded);
                Assert.Equal(OperationStatus.Completed, first.Status);
            },
            second =>
            {
                Assert.Equal("second", second.Name);
                Assert.True(second.Succeeded);
                Assert.Equal(OperationStatus.Completed, second.Status);
            });
        Assert.NotEqual(result.Steps[0].CorrelationId, result.Steps[1].CorrelationId);
    }

    [Fact]
    public async Task MultiStep_FirstTechnicalFailure_StopsBeforeSecondStep()
    {
        var response = await _client.PostAsync("/multi-step?first=Fail&second=Succeed", content: null);

        response.EnsureSuccessStatusCode();
        var result = await response.Content.ReadFromJsonAsync<MultiStepFlowResult>();
        Assert.False(result!.Completed);
        Assert.Equal("first", result.FailedAt);
        var step = Assert.Single(result.Steps);
        Assert.Equal("first", step.Name);
        Assert.False(step.Succeeded);
        Assert.Equal(nameof(InvalidOperationException), step.ExceptionType);
        Assert.Contains("first technical error", step.Detail, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AmbientException_UsesPublisherAmbientCorrelationFallback()
    {
        var response = await _client.PostAsync("/ambient-exception?message=ambient%20boom", content: null);

        response.EnsureSuccessStatusCode();
        var result = await response.Content.ReadFromJsonAsync<AmbientExceptionResult>();
        Assert.True(result!.Faulted);
        Assert.Equal(nameof(InvalidOperationException), result.ExceptionType);
        Assert.Equal("ambient boom", result.Detail);
    }

    [Fact]
    public async Task SharedCorrelationException_FaultsBothAttachedWaiters()
    {
        var response = await _client.PostAsync("/shared-correlation-exception?message=fanout%20boom", content: null);

        response.EnsureSuccessStatusCode();
        var result = await response.Content.ReadFromJsonAsync<SharedExceptionResult>();
        Assert.Equal(2, result!.Failures.Count);
        Assert.All(result.Failures, failure =>
        {
            Assert.Contains(nameof(InvalidOperationException), failure, StringComparison.Ordinal);
            Assert.Contains("fanout boom", failure, StringComparison.Ordinal);
        });
    }

    [Fact]
    public async Task Worker_RunsInProcess_AndFlowsTraceAndTenantViaExecutionContext()
    {
        var token = $"order-{Guid.NewGuid():N}";
        var trace = $"trace-{Guid.NewGuid():N}";

        (await _client.PostAsync($"/worker?token={token}&trace={trace}", content: null)).EnsureSuccessStatusCode();

        var call = await WaitForCallAsync($"worker:{token}");
        Assert.Equal("worker", call.Kind);
        Assert.Equal(token, call.Detail);
        Assert.Equal(trace, call.Trace);             // trace flows in-process via ExecutionContext
        Assert.Equal("tenant-acme", call.Tenant);    // second propagator's value flows too
    }

    [Fact]
    public async Task ConcurrentRequests_AreIsolatedByCorrelationId()
    {
        // Fire many independent request/response flows at once; each must complete with its own result.
        var responses = await Task.WhenAll(Enumerable.Range(0, 12)
            .Select(_ => _client.PostAsync("/request-response?behavior=Succeed", content: null)));

        foreach (var response in responses)
        {
            response.EnsureSuccessStatusCode();
            var result = await response.Content.ReadFromJsonAsync<Result>();
            Assert.Equal(OperationStatus.Completed, result!.Status);
        }
    }

    [Fact]
    public async Task ConcurrentWaiters_WithDistinctCorrelationIds_EachReceivesItsOwnResponse()
    {
        // Drive the builder/publisher directly (in-process) for fine-grained correlation isolation:
        // each waiter triggers a publish to its own generated correlation id.
        var asyncResponse = _factory.Services.GetRequiredService<IAsyncResponseBuilder>();
        var publisher = _factory.Services.GetRequiredService<IAsyncResponsePublisher>();

        var results = await Task.WhenAll(Enumerable.Range(0, 8).Select(i =>
            asyncResponse
                .For<OperationResult>()
                .WithTimeout(TimeSpan.FromSeconds(10))
                .WaitAsync(ctx => publisher.SetResponse(
                    new OperationResult { Status = OperationStatus.Completed, Message = $"r{i}" }, ctx.CorrelationId))));

        // Every waiter completed and got exactly the message its own trigger published — no crosstalk.
        Assert.All(results, r => Assert.Equal(OperationStatus.Completed, r.Status));
        Assert.Equal(
            Enumerable.Range(0, 8).Select(i => $"r{i}").ToHashSet(),
            results.Select(r => r.Message!).ToHashSet());
    }

    [Fact]
    public async Task Config_ReportsTheInMemoryProviders()
    {
        // Proves this tier boots the sample on the in-memory channel + transport (useRedis=false).
        var config = await _client.GetFromJsonAsync<ProviderConfig>("/config");
        Assert.Equal("InMemory", config!.Channel);
        Assert.Equal("InMemory", config.Transport);
    }

    private async Task<FlowCall> WaitForCallAsync(string key)
    {
        var response = await _client.GetAsync($"/calls?key={Uri.EscapeDataString(key)}&timeoutMs=15000");
        response.EnsureSuccessStatusCode();
        return (await response.Content.ReadFromJsonAsync<FlowCall>())!;
    }

    private sealed record Result(OperationStatus Status, string? Message);
    private sealed record ProviderConfig(string Channel, string Transport);
    private sealed record AmbientExceptionResult(bool Faulted, string ExceptionType, string Detail);

    /// <summary>
    /// Boots the sample app in-process with the fully in-memory provider configuration. The type
    /// argument only identifies the sample's assembly — <see cref="SampleFlowService"/> is used
    /// instead of <c>Program</c> because the referenced Aspire AppHost also defines a <c>Program</c>.
    /// </summary>
    public sealed class InMemoryAppFactory : WebApplicationFactory<SampleFlowService>
    {
        protected override void ConfigureWebHost(IWebHostBuilder builder)
        {
            builder.UseSetting("AsyncResponse:Channel", "InMemory");
            builder.UseSetting("AsyncResponse:Transport", "InMemory");
        }
    }
}
