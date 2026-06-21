using AsyncResponse.Sample;
using System.Net.Http.Json;

namespace AsyncResponse.IntegrationTests;

/// <summary>
/// Shared helpers for the integration tests; all derive from this and share one fixture. The SUT runs
/// as a separate Aspire-orchestrated process, so every scenario is driven and observed over HTTP.
/// </summary>
public abstract class IntegrationTestBase(IntegrationFixture fixture)
{
    protected static readonly TimeSpan DefaultTimeout = TimeSpan.FromSeconds(30);

    protected IntegrationFixture Fixture => fixture;
    protected HttpClient Client => fixture.Client;

    protected static string NewId(string prefix) => $"{prefix}-{Guid.NewGuid():N}";

    /// <summary>Arms a recoverable waiter via the SUT endpoint and returns its generated correlation id.</summary>
    protected async Task<string> ArmAsync(string trace)
        => await ArmAsync(Client, trace).ConfigureAwait(false);

    protected static async Task<string> ArmAsync(HttpClient client, string trace)
    {
        var response = await client.PostAsync($"/arm?trace={Uri.EscapeDataString(trace)}", content: null);
        response.EnsureSuccessStatusCode();
        var arm = await response.Content.ReadFromJsonAsync<ArmResponse>();
        return arm!.CorrelationId;
    }

    /// <summary>
    /// Long-polls the SUT for a recorded flow call (e.g. <c>worker:{token}</c>, <c>resume:{cid}</c>,
    /// <c>fail:{cid}</c>, <c>waiter:{cid}</c>). Throws if the call does not arrive within the timeout.
    /// </summary>
    protected async Task<FlowCall> WaitForCallAsync(string key, TimeSpan? timeout = null)
        => await WaitForCallAsync(Client, key, timeout).ConfigureAwait(false);

    protected static async Task<FlowCall> WaitForCallAsync(HttpClient client, string key, TimeSpan? timeout = null)
    {
        var ms = (int)(timeout ?? DefaultTimeout).TotalMilliseconds;
        var response = await client.GetAsync($"/calls?key={Uri.EscapeDataString(key)}&timeoutMs={ms}");
        response.EnsureSuccessStatusCode();
        return (await response.Content.ReadFromJsonAsync<FlowCall>())!;
    }

    /// <summary>Polls until <paramref name="done"/> is satisfied or the timeout elapses, returning the last value.</summary>
    protected static async Task<T> PollAsync<T>(Func<Task<T>> probe, Func<T, bool> done, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (true)
        {
            var value = await probe();
            if (done(value) || DateTime.UtcNow >= deadline)
                return value;
            await Task.Delay(250);
        }
    }

    protected sealed record ArmResponse(string CorrelationId);
    protected sealed record RequestResponseResult(OperationStatus Status, string? Message);
}
