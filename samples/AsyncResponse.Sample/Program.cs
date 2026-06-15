using AsyncResponse;
using AsyncResponse.Sample;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using StackExchange.Redis;
using System.Text.Json;

var builder = WebApplication.CreateBuilder(args);

// --- AsyncResponse wiring -------------------------------------------------------------------
var redisConnectionString = builder.Configuration.GetConnectionString("Redis") ?? "localhost:6379";
builder.Services.AddSingleton<IConnectionMultiplexer>(_ => ConnectionMultiplexer.Connect(redisConnectionString));

builder.Services.AddAsyncResponse(options =>
{
    // Aggressive watchdog values so the demo shows results quickly; defaults are 6h/24h/5m.
    options.Watchdog.StartupDelay = TimeSpan.FromSeconds(10);
    options.Watchdog.Interval = TimeSpan.FromSeconds(30);
    options.Watchdog.StaleAfter = TimeSpan.FromMinutes(2);
})
.WithRedisChannel(options =>
{
    options.KeyPrefix = "sample";
    options.RecoveryStateExpiry = TimeSpan.FromHours(1);
})
.WithInMemoryTransport()
.WithContextPropagator<SampleTracePropagator>()    // carry the trace id across serialized hops…
.WithContextPropagator<SampleTenantPropagator>();  // …and the tenant — propagators compose
builder.Services.AddHealthChecks().AddAsyncResponseRecoveryCheck();

// --- Sample services ------------------------------------------------------------------------
builder.Services.AddSingleton<ISampleFlowService, SampleFlowService>();
builder.Services.AddSingleton<RemoteWorkSimulator>();

var app = builder.Build();

// Waiters armed for the lost-subscriber demo. Held (not awaited) so they stay alive until
// "crashed"; a real flow would be awaiting them inside a worker.
// Background waits armed by /demo/lost-subscriber/arm — kept only so the tasks stay referenced.
var armedWaits = new List<Task<OperationResult>>();

app.MapGet("/", () => Results.Text(
    """
    AsyncResponse sample. Endpoints:

      POST /demo/request-response?behavior=Succeed|FailDomain   happy path / domain failure with an active waiter
      POST /demo/request-response/reply-target                  same flow with explicit reply-to metadata
      POST /demo/timeout                                         2s timeout against a 15s remote operation
      POST /demo/worker?orderId=42                               fire-and-forget background worker job
      POST /demo/lost-subscriber/arm                             register a waiter with recovery callbacks
      POST /demo/lost-subscriber/crash                           simulate a redeploy (kill all Redis subscriptions)
      POST /demo/lost-subscriber/respond?correlationId=…&status=Completed|Failed
                                                                 deliver the late response → watch recovery in logs
      GET  /healthz                                              health report incl. the recovery watchdog
    """));

// 1) Request/response with an active waiter. For<T>() generates the correlation id and the
//    required WaitAsync trigger guarantees subscribe-before-send.
app.MapPost("/demo/request-response", async (IAsyncResponseBuilder asyncResponse, RemoteWorkSimulator remote, RemoteBehavior? behavior, ILoggerFactory loggerFactory) =>
{
    var logger = loggerFactory.CreateLogger("RequestResponse");

    // Per-request ambient context. It flows into the response handler below — which runs on a Redis
    // subscriber thread — via the captured ExecutionContext, so the HANDLER log lines carry it. This
    // is exactly the OptimaticV2 "restore context inside the redis worker" case, now automatic.
    SampleTraceContext.Set($"trace-{Guid.NewGuid().ToString("N")[..8]}");
    SampleTenantContext.Set("tenant-acme");
    var startedCorrelationId = string.Empty;

    var result = await asyncResponse
        .For<OperationResult>()
        .WithTimeout(TimeSpan.FromSeconds(30))
        .Until(response =>
        {
            logger.LogInformation("HANDLER: progress {Status} (traceId: {TraceId}, tenant: {Tenant})",
                response.Status, SampleTraceContext.Current, SampleTenantContext.Current);
            return response.Status != OperationStatus.Running; // consume progress messages
        })
        .WaitAsync(context =>
        {
            startedCorrelationId = context.CorrelationId;
            remote.Start(context.CorrelationId, behavior ?? RemoteBehavior.Succeed);
            return Task.CompletedTask;
        });

    return result.Status == OperationStatus.Completed
        ? Results.Ok(new { correlationId = startedCorrelationId, result.Status, result.Message })
        : Results.Problem(title: "Remote operation failed", detail: result.Message, statusCode: 502);
});

// 2) Request/response with explicit reply-to metadata. Transport packages usually provide this
//    via WithReplyTarget(); this sample uses an explicit target so it stays infrastructure-free.
app.MapPost("/demo/request-response/reply-target", async (IAsyncResponseBuilder asyncResponse, RemoteWorkSimulator remote, RemoteBehavior? behavior) =>
{
    AsyncResponseRequestContext? startedContext = null;
    var replyTarget = new AsyncResponseReplyTarget
    {
        Name = "sample",
        Transport = "sample",
        Address = "sample://remote-work-simulator"
    };

    var result = await asyncResponse
        .For<OperationResult>()
        .WithReplyTarget(replyTarget)
        .WithTimeout(TimeSpan.FromSeconds(30))
        .Until(response => response.Status != OperationStatus.Running)
        .WaitAsync(context =>
        {
            startedContext = context;
            remote.Start(context, behavior ?? RemoteBehavior.Succeed);
            return Task.CompletedTask;
        });

    return result.Status == OperationStatus.Completed
        ? Results.Ok(new
        {
            correlationId = startedContext?.CorrelationId,
            replyTarget = startedContext?.ReplyTarget,
            result.Status,
            result.Message
        })
        : Results.Problem(title: "Remote operation failed", detail: result.Message, statusCode: 502);
});

// 3) Timeout: the remote takes 15s, the waiter allows 2s. Also shows the no-correlation-id
//    flow: For<T>() generates one and hands it to the trigger.
app.MapPost("/demo/timeout", async (IAsyncResponseBuilder asyncResponse, RemoteWorkSimulator remote) =>
{
    try
    {
        await asyncResponse
            .For<OperationResult>()
            .WithTimeout(TimeSpan.FromSeconds(2))
            .Until(response => response.Status != OperationStatus.Running)
            .WaitAsync(context =>
            {
                remote.Start(context.CorrelationId, RemoteBehavior.Slow);
                return Task.CompletedTask;
            });

        return Results.Ok("Unexpectedly completed in time.");
    }
    catch (TimeoutException timeout)
    {
        return Results.Problem(title: "Timed out as expected", detail: timeout.Message, statusCode: 504);
    }
});

// 4) Fire-and-forget worker job via the in-process worker transport.
app.MapPost("/demo/worker", async (IAsyncResponseBuilder asyncResponse, int orderId) =>
{
    AsyncResponseContext.CreateCorrelationId();
    SampleTraceContext.Set($"trace-{Guid.NewGuid().ToString("N")[..8]}");
    SampleTenantContext.Set("tenant-acme");
    await asyncResponse.EnqueueWorkerAsync<ISampleFlowService>(flow => flow.ProcessOrderAsync(orderId));
    return Results.Accepted(value: new { orderId, traceId = SampleTraceContext.Current, note = "Watch the logs for WORKER output — the traceId flows in-process via ExecutionContext." });
});

// 5a) Lost-subscriber demo — arm: register a waiter with recovery callbacks and keep it
// waiting in the background (like a worker awaiting a slow remote operation). The HTTP request
// returns immediately; the subscription and the persisted recovery state stay alive.
app.MapPost("/demo/lost-subscriber/arm", async (IAsyncResponseBuilder asyncResponse) =>
{
    // The propagators capture this trace id and tenant into the persisted recovery state, so they
    // are restored before the recovery callback runs after the "crash" — even though the waiter is
    // long gone (the values must survive serialization, which is what the propagators are for).
    SampleTraceContext.Set($"trace-{Guid.NewGuid().ToString("N")[..8]}");
    SampleTenantContext.Set("tenant-acme");

    var armed = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);

    armedWaits.Add(asyncResponse
        .For<OperationResult>()
        .Until(response => response.Status != OperationStatus.Running)
        .OnLostSubscriberResume<ISampleFlowService>(flow =>
            flow.ResumeFlowAsync("sample-flow", Placeholder.Payload<OperationResult>(), Placeholder.CorrelationId()))
        .OnLostSubscriberFailure<ISampleFlowService>(flow =>
            flow.FailFlowAsync(Placeholder.Exception(), Placeholder.CorrelationId()))
        .WaitAsync(context =>
        {
            // The trigger runs once the subscription and recovery state exist. The "send" here
            // is handing the id to the operator: the remote work is delivered manually via
            // /demo/lost-subscriber/respond.
            armed.SetResult(context.CorrelationId);
            return Task.CompletedTask;
        }));

    var armedCorrelationId = await armed.Task;

    return Results.Ok(new
    {
        correlationId = armedCorrelationId,
        traceId = SampleTraceContext.Current,   // this should reappear in the RECOVERY log after the crash
        next = "POST /demo/lost-subscriber/crash, then /demo/lost-subscriber/respond?correlationId=…&status=Completed|Failed"
    });
});

// 5b) Lost-subscriber demo — crash: drop every Redis subscription, like a redeploy would.
// The recovery state stays in Redis; only the in-memory waiters die.
app.MapPost("/demo/lost-subscriber/crash", (IConnectionMultiplexer multiplexer) =>
{
    multiplexer.GetSubscriber().UnsubscribeAll();
    return Results.Ok("All subscriptions dropped (simulated redeploy). The armed waiters are now lost.");
});

// 5c) Lost-subscriber demo — respond: deliver the late terminal response. With no subscriber
// alive, the lost-subscriber dispatcher classifies the payload: Completed → ResumeFlowAsync,
// Failed → FailFlowAsync (as AsyncResponseDomainFailureException). Watch the RECOVERY logs.
app.MapPost("/demo/lost-subscriber/respond", async (RemoteWorkSimulator remote, string correlationId, OperationStatus status) =>
{
    await remote.DeliverAsync(correlationId, new OperationResult
    {
        Status = status,
        Message = $"late response delivered after crash ({status})"
    });

    return Results.Ok("Delivered. Watch the application logs for the RECOVERY route taken.");
});

// Health endpoint with full JSON details, including the recovery check's data payload.
app.MapHealthChecks("/healthz", new HealthCheckOptions
{
    ResponseWriter = async (context, report) =>
    {
        context.Response.ContentType = "application/json; charset=utf-8";
        await context.Response.WriteAsync(JsonSerializer.Serialize(new
        {
            status = report.Status.ToString(),
            checks = report.Entries.Select(entry => new
            {
                name = entry.Key,
                status = entry.Value.Status.ToString(),
                description = entry.Value.Description,
                data = entry.Value.Data
            })
        }, new JsonSerializerOptions { WriteIndented = true }));
    }
});

app.Run();
