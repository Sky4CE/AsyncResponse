using AsyncResponse;
using AsyncResponse.IntegrationTests.App;
using AsyncResponse.Transports.GooglePubSub;
using Google.Api.Gax;
using Google.Cloud.PubSub.V1;
using Google.Protobuf;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;
using StackExchange.Redis;
using System.Text.Json;

var builder = WebApplication.CreateBuilder(args);

// OpenTelemetry + health-check defaults so logs/traces (incl. the "AsyncResponse" ActivitySource) flow
// to the Aspire dashboard when this app runs under the AppHost.
builder.AddServiceDefaults();

// Redis + Pub/Sub config are supplied by the Aspire AppHost (connection string + PubSub:* env, and
// PUBSUB_EMULATOR_HOST). The GCP clients honor PUBSUB_EMULATOR_HOST via EmulatorDetection.
var redisConnectionString = builder.Configuration.GetConnectionString("Redis") ?? "localhost:6379";
builder.Services.AddSingleton<IConnectionMultiplexer>(_ => ConnectionMultiplexer.Connect(redisConnectionString));

// Provisions the emulator's topics/subscriptions before the transport's subscribers start
// (emulator-only; no-op against real GCP). Registered first so its StartAsync completes first.
builder.Services.AddHostedService<PubSubEmulatorProvisioner>();

builder.Services.AddAsyncResponse(options =>
{
    // Aggressive watchdog so the stale-flow health scenario resolves within a test.
    options.Watchdog.StartupDelay = TimeSpan.FromSeconds(1);
    options.Watchdog.Interval = TimeSpan.FromSeconds(1);
    options.Watchdog.StaleAfter = TimeSpan.FromSeconds(2);
})
.WithRedisChannel(options =>
{
    options.KeyPrefix = builder.Configuration["AsyncResponse:KeyPrefix"] ?? "itest";
    options.DefaultTimeout = TimeSpan.FromSeconds(30);
})
.WithGooglePubSubTransport(options =>
{
    options.ProjectId = builder.Configuration["PubSub:ProjectId"] ?? "itest-project";
    options.WorkerTopicId = builder.Configuration["PubSub:WorkerTopicId"] ?? "worker-topic";
    options.WorkerSubscriptionId = builder.Configuration["PubSub:WorkerSubscriptionId"] ?? "worker-sub";
    options.ResponseTopicId = builder.Configuration["PubSub:ResponseTopicId"] ?? "response-topic";
    options.ResponseSubscriptionId = builder.Configuration["PubSub:ResponseSubscriptionId"] ?? "response-sub";
})
.WithContextPropagator<ItestTracePropagator>();

builder.Services.AddHealthChecks().AddAsyncResponseRecoveryCheck();
builder.Services.AddSingleton<ItestFlowService>();
builder.Services.AddSingleton<IItestFlowService>(sp => sp.GetRequiredService<ItestFlowService>());

var app = builder.Build();

// Liveness probe used by the AppHost health check — always 200 while the process is up, independent of
// the recovery health check (which goes Degraded in the watchdog scenario).
app.MapGet("/alive", () => Results.Ok("alive"));

// Active-waiter Redis round-trip with a selectable terminal behavior. The trigger publishes a progress
// message then the chosen terminal over the real Redis pub/sub channel; outcomes are mapped to HTTP.
app.MapPost("/request-response", async (IAsyncResponseBuilder asyncResponse, IAsyncResponsePublisher publisher, string? behavior) =>
{
    var kind = (behavior ?? "Succeed").ToLowerInvariant();
    var timeout = kind == "timeout" ? TimeSpan.FromSeconds(2) : TimeSpan.FromSeconds(20);
    try
    {
        var result = await asyncResponse
            .For<ItestPayload>()
            .WithTimeout(timeout)
            .Until(r => r.Status != ItestStatus.Running)
            .WaitAsync(async ctx =>
            {
                await publisher.SetResponse(new ItestPayload { Status = ItestStatus.Running, Message = "progress" }, ctx.CorrelationId);
                switch (kind)
                {
                    case "faildomain":
                        await publisher.SetResponse(new ItestPayload { Status = ItestStatus.Failed, Message = "remote failed" }, ctx.CorrelationId);
                        break;
                    case "fail":
                        await publisher.SetException(new InvalidOperationException("remote technical error"), ctx.CorrelationId);
                        break;
                    case "timeout":
                        break; // send nothing terminal → the waiter times out
                    default:
                        await publisher.SetResponse(new ItestPayload { Status = ItestStatus.Completed, Message = "done" }, ctx.CorrelationId);
                        break;
                }
            });

        return Results.Ok(new { result.Status, result.Message });
    }
    catch (TimeoutException)
    {
        return Results.StatusCode(StatusCodes.Status504GatewayTimeout);
    }
    catch (Exception ex)
    {
        return Results.Problem(detail: ex.Message, statusCode: StatusCodes.Status500InternalServerError);
    }
});

// Fire-and-forget worker job over the real Pub/Sub worker transport. Returns the correlation id it set
// so the round-trip test can assert the id is restored on the consuming side.
app.MapPost("/worker", async (IAsyncResponseBuilder asyncResponse, string token, string? trace) =>
{
    var correlationId = AsyncResponseContext.CreateCorrelationId();
    ItestTraceContext.Set(trace);
    await asyncResponse.EnqueueWorkerAsync<IItestFlowService>(flow => flow.ProcessWorkAsync(token));
    return Results.Ok(new { correlationId });
});

// Arms a real recoverable waiter in the background and returns its generated correlation id once the
// subscription + recovery state exist. The waiter's terminal outcome is recorded for assertions.
app.MapPost("/arm", async (IAsyncResponseBuilder asyncResponse, ItestFlowService flowService, string? trace) =>
{
    ItestTraceContext.Set(trace);
    var armed = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);

    var waitTask = asyncResponse
        .For<ItestPayload>()
        .WithTimeout(TimeSpan.FromMinutes(2))
        .Until(r => r.Status != ItestStatus.Running)
        .OnLostSubscriberResume<IItestFlowService>(flow => flow.ResumeAsync(Placeholder.Payload<ItestPayload>(), Placeholder.CorrelationId()))
        .OnLostSubscriberFailure<IItestFlowService>(flow => flow.FailAsync(Placeholder.Exception(), Placeholder.CorrelationId()))
        .WaitAsync(ctx =>
        {
            armed.SetResult(ctx.CorrelationId);
            return Task.CompletedTask;
        });

    var correlationId = await armed.Task;
    _ = waitTask.ContinueWith(t => flowService.RecordWaiterResult(correlationId, t), TaskScheduler.Default);
    return Results.Ok(new { correlationId });
});

// Delivers a late response/exception for a correlation id through the real Redis channel (used by the
// lost-subscriber recovery scenarios after /crash).
app.MapPost("/publish", async (IAsyncResponsePublisher publisher, string correlationId, string? status, string? message, string? exception) =>
{
    if (exception is not null)
    {
        await publisher.SetException(new InvalidOperationException(exception), correlationId);
    }
    else
    {
        var parsedStatus = Enum.TryParse<ItestStatus>(status, ignoreCase: true, out var s) ? s : ItestStatus.Completed;
        await publisher.SetResponse(new ItestPayload { Status = parsedStatus, Message = message }, correlationId);
    }

    return Results.Accepted();
});

// Publishes a raw response message to the Pub/Sub response topic, acting as the remote system. With
// useAttribute the correlation id rides a message attribute; otherwise it goes in the JSON body so the
// extractor's JSON-path fallback is exercised.
app.MapPost("/emit-response", async (IOptions<GooglePubSubAsyncResponseOptions> options, string correlationId, string? status, bool useAttribute, string? message) =>
{
    var o = options.Value;
    var parsedStatus = Enum.TryParse<ItestStatus>(status, ignoreCase: true, out var s) ? s : ItestStatus.Completed;

    var json = useAttribute
        ? JsonSerializer.Serialize(new ItestPayload { Status = parsedStatus, Message = message })
        : JsonSerializer.Serialize(new { CorrelationId = correlationId, Status = (int)parsedStatus, Message = message });

    var pubsubMessage = new PubsubMessage { Data = ByteString.CopyFromUtf8(json) };
    if (useAttribute)
        pubsubMessage.Attributes[o.CorrelationIdAttribute] = correlationId;

    var publisher = await new PublisherServiceApiClientBuilder
    {
        EmulatorDetection = EmulatorDetection.EmulatorOrProduction
    }.BuildAsync();
    await publisher.PublishAsync(TopicName.FromProjectTopic(o.ProjectId!, o.ResponseTopicId!), [pubsubMessage]);

    return Results.Accepted();
});

// Long-polls the flow recorder for a recorded call (e.g. worker:{token}, resume:{cid}, waiter:{cid}).
app.MapGet("/calls", async (ItestFlowService flow, string key, int? timeoutMs) =>
{
    try
    {
        var call = await flow.WaitForAsync(key).WaitAsync(TimeSpan.FromMilliseconds(timeoutMs ?? 15000));
        return Results.Ok(call);
    }
    catch (TimeoutException)
    {
        return Results.StatusCode(StatusCodes.Status408RequestTimeout);
    }
});

// Seeds a stale recovery entry (no live subscriber) so the watchdog surfaces it as Degraded health.
app.MapPost("/seed-recovery", async (IRecoveryStateStore store, string correlationId, int? ageMinutes) =>
{
    await store.SaveAsync(correlationId, new RecoveryState
    {
        CorrelationId = correlationId,
        PayloadTypeFullName = typeof(ItestPayload).FullName,
        RegisteredAtUtc = DateTime.UtcNow.AddMinutes(-(ageMinutes ?? 5))
    }, TimeSpan.FromMinutes(10));

    return Results.Accepted();
});

// Runs a WithReplyTarget round-trip and returns the reply target observed by the trigger.
app.MapGet("/reply-target", async (IAsyncResponseBuilder asyncResponse, IAsyncResponsePublisher publisher) =>
{
    AsyncResponseReplyTarget? observed = null;

    await asyncResponse
        .For<ItestPayload>()
        .WithReplyTarget()
        .WithTimeout(TimeSpan.FromSeconds(20))
        .Until(r => r.Status != ItestStatus.Running)
        .WaitAsync(async ctx =>
        {
            observed = ctx.ReplyTarget;
            await publisher.SetResponse(new ItestPayload { Status = ItestStatus.Completed }, ctx.CorrelationId);
        });

    return Results.Ok(new { observed?.Transport, observed?.Address });
});

// Simulate a redeploy: drop every Redis subscription, leaving recovery state behind.
app.MapPost("/crash", (IConnectionMultiplexer multiplexer) =>
{
    multiplexer.GetSubscriber().UnsubscribeAll();
    return Results.Ok();
});

app.MapHealthChecks("/healthz", new HealthCheckOptions
{
    ResponseWriter = async (context, report) =>
    {
        context.Response.ContentType = "application/json";
        await context.Response.WriteAsync(JsonSerializer.Serialize(new
        {
            status = report.Status.ToString(),
            checks = report.Entries.Select(e => new { name = e.Key, status = e.Value.Status.ToString(), data = e.Value.Data })
        }));
    }
});

app.Run();
