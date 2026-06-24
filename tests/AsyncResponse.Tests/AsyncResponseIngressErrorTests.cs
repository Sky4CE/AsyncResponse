using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace AsyncResponse.Tests;

public class AsyncResponseIngressErrorTests
{
    [Fact]
    public async Task HandleResponseMessageAsync_WhenRawPublishFails_PublishesExceptionAndSwallowsFallbackFailure()
    {
        var original = new InvalidDataException("bad payload");
        var rawPublisher = new ThrowingRawPublisher(original);
        var publisher = new RecordingPublisher(new InvalidOperationException("fallback failed"));
        var ingress = CreateIngress(rawPublisher, publisher);

        await ingress.HandleResponseMessageAsync("<html>bad gateway</html>", "corr-a");

        Assert.Same(original, publisher.Exception);
        Assert.Equal("corr-a", publisher.CorrelationId);
    }

    [Fact]
    public async Task HandleWorkerMessageAsync_InvalidPayload_IsContained()
    {
        var ingress = CreateIngress(new ThrowingRawPublisher(), new RecordingPublisher());

        await ingress.HandleWorkerMessageAsync("null");
        await ingress.HandleWorkerMessageAsync("{not-json");
    }

    private static AsyncResponseIngress CreateIngress(
        IRawAsyncResponsePublisher rawPublisher,
        IAsyncResponsePublisher publisher)
    {
        var provider = new ServiceCollection().BuildServiceProvider();
        return new AsyncResponseIngress(
            rawPublisher,
            publisher,
            new WorkerJobExecutor(provider.GetRequiredService<IServiceScopeFactory>(), NullLogger<WorkerJobExecutor>.Instance),
            new AsyncResponseContextPropagation([]),
            NullLogger<AsyncResponseIngress>.Instance);
    }

    private sealed class ThrowingRawPublisher(Exception? _exception = null) : IRawAsyncResponsePublisher
    {
        public Task SetRawResponse(object? response, string? correlationId, CancellationToken cancellationToken = default)
            => _exception is null ? Task.CompletedTask : Task.FromException(_exception);

        public Task SetRawResponseJson(string responseJson, string? correlationId, CancellationToken cancellationToken = default)
            => _exception is null ? Task.CompletedTask : Task.FromException(_exception);
    }

    private sealed class RecordingPublisher(Exception? _throwOnException = null) : IAsyncResponsePublisher
    {
        public Exception? Exception { get; private set; }
        public string? CorrelationId { get; private set; }

        public Task SetResponse<T>(T response, string? correlationId = null, CancellationToken cancellationToken = default) where T : IAsyncResponsePayload
            => Task.CompletedTask;

        public Task SetException(Exception exception, string? correlationId = null, CancellationToken cancellationToken = default)
        {
            Exception = exception;
            CorrelationId = correlationId;
            return _throwOnException is null ? Task.CompletedTask : Task.FromException(_throwOnException);
        }
    }
}
