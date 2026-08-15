using AsyncResponse.Testing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// The harness's ConfigureServices contract: user registrations may fake anything the flows and
/// triggers inject — but never the engine clock (the harness owns virtual time, and a displaced
/// clock strands every timer as an unexplained real-time-guard timeout), and logging registered
/// there must actually receive the engine's output instead of being pinned to NullLogger.
/// </summary>
public sealed class TestingHarnessConfigurationTests
{
    [Fact]
    public async Task ConfigureServices_RegisteringATimeProvider_FailsConstructionWithGuidance()
    {
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            AsyncResponseTestHarness.StartAsync(options =>
                options.ConfigureServices = services => services.AddSingleton(TimeProvider.System)));

        Assert.Contains("virtual clock", ex.Message, StringComparison.Ordinal);
        Assert.Contains("AdvanceAsync", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ConfigureServices_RegisteringATimeProviderFactory_FailsConstruction()
    {
        // A factory registration cannot be identity-checked against the harness clock, so it is
        // rejected the same way: the engine would resolve it (last-wins) over the virtual clock.
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            AsyncResponseTestHarness.StartAsync(options =>
                options.ConfigureServices = services => services.AddSingleton<TimeProvider>(_ => TimeProvider.System)));
    }

    [Fact]
    public async Task ConfigureServices_AddLogging_ReceivesEngineLoggerOutput()
    {
        var sink = new CollectingLoggerProvider();
        await using var harness = await AsyncResponseTestHarness.StartAsync(options =>
            options.ConfigureServices = services =>
                services.AddLogging(logging => logging.AddProvider(sink).SetMinimumLevel(LogLevel.Debug)));

        // The same open-generic resolution every engine component's constructor uses.
        var logger = harness.Services.GetRequiredService<ILogger<TestingHarnessConfigurationTests>>();
        Assert.IsNotType<NullLogger<TestingHarnessConfigurationTests>>(logger);

        logger.LogInformation("user-configured-logging-visible");
        Assert.Contains(sink.Messages, message => message.Contains("user-configured-logging-visible", StringComparison.Ordinal));
    }

    [Fact]
    public async Task NoUserLogging_StillFallsBackToNullLogger()
    {
        await using var harness = await AsyncResponseTestHarness.StartAsync();

        var logger = harness.Services.GetRequiredService<ILogger<TestingHarnessConfigurationTests>>();
        Assert.IsType<NullLogger<TestingHarnessConfigurationTests>>(logger);
    }

    private sealed class CollectingLoggerProvider : ILoggerProvider
    {
        private readonly List<string> _messages = [];

        public IReadOnlyList<string> Messages
        {
            get { lock (_messages) return [.. _messages]; }
        }

        public ILogger CreateLogger(string categoryName) => new CollectingLogger(this);

        public void Dispose()
        {
        }

        private void Record(string message)
        {
            lock (_messages)
                _messages.Add(message);
        }

        private sealed class CollectingLogger(CollectingLoggerProvider owner) : ILogger
        {
            public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

            public bool IsEnabled(LogLevel logLevel) => true;

            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
                => owner.Record(formatter(state, exception));
        }
    }
}
