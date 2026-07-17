using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Text;
using Xunit;

namespace AsyncResponse.IntegrationTests;

/// <summary>
/// The Native AOT gate, runnable from any full local test run (no Docker, no fixture): publishes
/// the sample with <c>-warnaserror</c> — the exact bar CI's <c>integration-tests-aot</c> job
/// applies — and then boots the produced native binary and drives its core endpoints.
/// <para>
/// This exists because two whole classes of defect are invisible to a normal build + unit run:
/// ILC-only trim errors (e.g. anonymous-type LINQ projections lower to the
/// RequiresUnreferencedCode <c>Expression.New</c> overload that the Roslyn analyzer never flags),
/// and runtime-only Native AOT breaks (missing JSON metadata for a statically-referenced type,
/// <c>Type.GetInterfaceMap</c> over default-interface-method interfaces, anonymous endpoint
/// returns under the Request Delegate Generator). Every one of those has bitten this repo; this
/// test makes them fail a local "run all tests" instead of the pipeline.
/// </para>
/// <para>Set <c>ASYNCRESPONSE_SKIP_AOT_GATE=1</c> to skip locally when iterating quickly.</para>
/// </summary>
public sealed class NativeAotPublishGateTests
{
    private static readonly TimeSpan PublishTimeout = TimeSpan.FromMinutes(12);

    [Fact]
    public async Task Sample_PublishesNativeAot_AndServesCoreScenarios()
    {
        Assert.SkipWhen(
            Environment.GetEnvironmentVariable("ASYNCRESPONSE_SKIP_AOT_GATE") == "1",
            "ASYNCRESPONSE_SKIP_AOT_GATE=1 — Native AOT gate skipped by request.");

        var repoRoot = FindRepoRoot();
        var output = Path.Combine(Path.GetTempPath(), $"ar-aot-gate-{Guid.NewGuid():N}");
        try
        {
            // 1) The compile-time gate: Roslyn analyzers AND ILC trim analysis, warnings as
            //    errors — identical severity to CI, so ILC-only findings fail here first.
            var publish = await RunAsync(
                "dotnet",
                [
                    "publish",
                    Path.Combine("samples", "AsyncResponse.Sample", "AsyncResponse.Sample.csproj"),
                    "-c", "Release",
                    "-warnaserror",
                    "-o", output,
                ],
                repoRoot,
                PublishTimeout);

            if (publish.ExitCode != 0 && LooksLikeMissingNativeToolchain(publish.Output))
            {
                Assert.Skip(
                    "Native AOT publish needs the platform native toolchain (Windows: 'Desktop development with C++'; " +
                    "Linux: clang + zlib1g-dev). Install it to run the AOT gate locally. Publish output tail: " +
                    Tail(publish.Output, 500));
            }

            Assert.True(
                publish.ExitCode == 0,
                $"Native AOT publish failed (exit {publish.ExitCode}) — an ILC trim error or analyzer regression. " +
                $"Output tail:\n{Tail(publish.Output, 4000)}");

            // 2) The runtime gate: boot the native binary (in-memory channel/transport, no
            //    external dependencies) and drive a request/response round-trip plus a durable
            //    flow to completion.
            var binary = Path.Combine(output, OperatingSystem.IsWindows() ? "AsyncResponse.Sample.exe" : "AsyncResponse.Sample");
            Assert.True(File.Exists(binary), $"Published binary not found at {binary}.");

            var port = GetFreePort();
            using var app = new Process();
            app.StartInfo = new ProcessStartInfo(binary)
            {
                WorkingDirectory = output,
                UseShellExecute = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                Environment =
                {
                    ["ASPNETCORE_HTTP_PORTS"] = port.ToString(),
                    ["AsyncResponse__Channel"] = "InMemory",
                    ["AsyncResponse__Transport"] = "InMemory",
                },
            };
            var appLog = new StringBuilder();
            app.OutputDataReceived += (_, e) => { if (e.Data is not null) lock (appLog) appLog.AppendLine(e.Data); };
            app.ErrorDataReceived += (_, e) => { if (e.Data is not null) lock (appLog) appLog.AppendLine(e.Data); };
            app.Start();
            app.BeginOutputReadLine();
            app.BeginErrorReadLine();

            try
            {
                using var client = new HttpClient { BaseAddress = new Uri($"http://localhost:{port}") };

                await WaitForAliveAsync(client, app, appLog);

                var roundTrip = await client.PostAsync("/request-response?behavior=Succeed", content: null);
                Assert.True(
                    roundTrip.StatusCode == HttpStatusCode.OK,
                    $"/request-response returned {(int)roundTrip.StatusCode} on the native binary. App log tail:\n{Tail(appLog.ToString(), 3000)}");
                Assert.Contains("\"message\":\"done\"", await roundTrip.Content.ReadAsStringAsync(), StringComparison.Ordinal);

                var start = await client.PostAsync("/durable-flow?name=aot-gate", content: null);
                Assert.Equal(HttpStatusCode.OK, start.StatusCode);
                var flowId = System.Text.Json.JsonDocument.Parse(await start.Content.ReadAsStringAsync())
                    .RootElement.GetProperty("flowId").GetString();

                var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(60);
                var status = -1;
                while (DateTime.UtcNow < deadline)
                {
                    var state = await client.GetAsync($"/durable-flow/{flowId}");
                    if (state.StatusCode == HttpStatusCode.OK)
                    {
                        status = System.Text.Json.JsonDocument.Parse(await state.Content.ReadAsStringAsync())
                            .RootElement.GetProperty("status").GetInt32();
                        if (status == 1) // FlowRunStatus.Succeeded
                            break;
                    }

                    await Task.Delay(250);
                }

                Assert.True(status == 1, $"Durable flow did not reach Succeeded on the native binary (last status {status}). App log tail:\n{Tail(appLog.ToString(), 3000)}");
            }
            finally
            {
                try
                {
                    app.Kill(entireProcessTree: true);
                }
                catch (InvalidOperationException)
                {
                    // Already exited.
                }
            }
        }
        finally
        {
            try
            {
                Directory.Delete(output, recursive: true);
            }
            catch (IOException)
            {
                // Best-effort cleanup of the publish output.
            }
        }
    }

    private static async Task WaitForAliveAsync(HttpClient client, Process app, StringBuilder appLog)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(30);
        while (DateTime.UtcNow < deadline)
        {
            if (app.HasExited)
                Assert.Fail($"Native binary exited early (code {app.ExitCode}). App log tail:\n{Tail(appLog.ToString(), 3000)}");

            try
            {
                if ((await client.GetAsync("/alive")).IsSuccessStatusCode)
                    return;
            }
            catch (HttpRequestException)
            {
                // Not listening yet.
            }

            await Task.Delay(200);
        }

        Assert.Fail($"Native binary never answered /alive. App log tail:\n{Tail(appLog.ToString(), 3000)}");
    }

    private static async Task<(int ExitCode, string Output)> RunAsync(string command, string[] args, string workingDirectory, TimeSpan timeout)
    {
        using var process = new Process();
        process.StartInfo = new ProcessStartInfo(command)
        {
            WorkingDirectory = workingDirectory,
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
        };
        foreach (var arg in args)
            process.StartInfo.ArgumentList.Add(arg);

        var output = new StringBuilder();
        process.OutputDataReceived += (_, e) => { if (e.Data is not null) lock (output) output.AppendLine(e.Data); };
        process.ErrorDataReceived += (_, e) => { if (e.Data is not null) lock (output) output.AppendLine(e.Data); };
        process.Start();
        process.BeginOutputReadLine();
        process.BeginErrorReadLine();

        using var cts = new CancellationTokenSource(timeout);
        try
        {
            await process.WaitForExitAsync(cts.Token);
        }
        catch (OperationCanceledException)
        {
            try
            {
                process.Kill(entireProcessTree: true);
            }
            catch (InvalidOperationException)
            {
            }

            Assert.Fail($"'{command} {string.Join(' ', args)}' timed out after {timeout}. Output tail:\n{Tail(output.ToString(), 3000)}");
        }

        return (process.ExitCode, output.ToString());
    }

    private static bool LooksLikeMissingNativeToolchain(string output)
        => output.Contains("Platform linker not found", StringComparison.OrdinalIgnoreCase)
           || output.Contains("Platform linker ('", StringComparison.OrdinalIgnoreCase)
           || output.Contains("Microsoft.DotNet.ILCompiler is not supported", StringComparison.OrdinalIgnoreCase);

    private static string FindRepoRoot()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        while (directory is not null)
        {
            if (File.Exists(Path.Combine(directory.FullName, "AsyncResponse.slnx")))
                return directory.FullName;
            directory = directory.Parent!;
        }

        throw new InvalidOperationException("Repository root (AsyncResponse.slnx) not found above the test base directory.");
    }

    private static int GetFreePort()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        return ((IPEndPoint)listener.LocalEndpoint).Port;
    }

    private static string Tail(string text, int maxChars)
        => text.Length <= maxChars ? text : text[^maxChars..];
}
