using Aspire.Hosting;
using Aspire.Hosting.Testing;
using Xunit;

namespace AsyncResponse.IntegrationTests;

/// <summary>
/// Boots the Aspire AppHost once for the whole collection — real Redis + a Google Pub/Sub emulator
/// (containers) and the system-under-test app (<c>itest-app</c>), all orchestrated by the dedicated
/// integration AppHost. Tests drive the SUT entirely over HTTP via <see cref="Client"/>.
/// </summary>
public sealed class IntegrationFixture : IAsyncLifetime
{
    /// <summary>Response topic id the AppHost configures — asserted by the reply-target scenario.</summary>
    public const string ResponseTopicId = "response-topic";

    private static readonly TimeSpan StartupTimeout = TimeSpan.FromMinutes(3);

    private DistributedApplication? _app;

    public HttpClient Client { get; private set; } = null!;
    public HttpClient EarlyAckClient { get; private set; } = null!;

    public async ValueTask InitializeAsync()
    {
        var appHost = await DistributedApplicationTestingBuilder.CreateAsync<Projects.AsyncResponse_IntegrationTests_AppHost>();
        _app = await appHost.BuildAsync().WaitAsync(StartupTimeout);
        await _app.StartAsync().WaitAsync(StartupTimeout);

        // Wait until the SUT reports healthy (its /alive probe) — i.e. it has connected to Redis and the
        // emulator and provisioned its topics/subscriptions — before driving any scenario.
        await _app.ResourceNotifications
            .WaitForResourceHealthyAsync("itest-app")
            .WaitAsync(StartupTimeout);

        Client = _app.CreateHttpClient("itest-app");
        EarlyAckClient = _app.CreateHttpClient("itest-app-early-ack");
        await ResetTestStateAsync(Client).WaitAsync(StartupTimeout);
        await ResetTestStateAsync(EarlyAckClient).WaitAsync(StartupTimeout);
    }

    public async ValueTask DisposeAsync()
    {
        Client?.Dispose();
        EarlyAckClient?.Dispose();
        if (_app is not null)
            await _app.DisposeAsync();
    }

    private static async Task ResetTestStateAsync(HttpClient client)
    {
        var response = await client.PostAsync("/test/reset", content: null);
        response.EnsureSuccessStatusCode();
    }
}
