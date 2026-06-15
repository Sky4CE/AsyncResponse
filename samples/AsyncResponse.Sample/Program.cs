using AsyncResponse;
using AsyncResponse.Sample;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using StackExchange.Redis;
using System.Collections.Concurrent;
using System.Text.Json;

var builder = WebApplication.CreateBuilder(args);

// --- AsyncResponse wiring -------------------------------------------------------------------
var redisConnectionString = builder.Configuration.GetConnectionString("Redis") ?? "localhost:6379";
builder.Services.AddSingleton<IConnectionMultiplexer>(_ => ConnectionMultiplexer.Connect(redisConnectionString));

builder.Services.AddRedisAsyncResponse(options =>
{
    options.KeyPrefix = "sample";
    options.RecoveryStateExpiry = TimeSpan.FromHours(1);
});
builder.Services.AddInProcessWorkerTransport();
builder.Services.AddAsyncResponseWatchdog(options =>
{
    // Aggressive values so the demo shows results quickly; defaults are 6h/24h/5m.
    options.StartupDelay = TimeSpan.FromSeconds(10);
    options.Interval = TimeSpan.FromSeconds(30);
    options.StaleAfter = TimeSpan.FromMinutes(2);
});
builder.Services.AddHealthChecks().AddAsyncResponseRecoveryCheck();

// --- Sample services ------------------------------------------------------------------------
builder.Services.AddSingleton<ISampleFlowService, SampleFlowService>();
builder.Services.AddSingleton<RemoteWorkSimulator>();

var app = builder.Build();

// Waiters armed for the lost-subscriber demo. Held (not awaited) so they stay alive until
// "crashed"; a real flow would be awaiting them inside a worker.
var armedWaiters = new ConcurrentDictionary<string, IAsyncResponseWaiter<OperationResult>>();

app.MapGet("/", () => Results.Text(
    """
    AsyncResponse sample. Endpoints:

      POST /demo/request-response?behavior=Succeed|FailDomain   happy path / domain failure with an active waiter
      POST /demo/timeout                                         2s timeout against a 15s remote operation
      POST /demo/worker?orderId=42                               fire-and-forget background worker job
      POST /demo/lost-subscriber/arm                             register a waiter with recovery callbacks
      POST /demo/lost-subscriber/crash                           simulate a redeploy (kill all Redis subscriptions)
      POST /demo/lost-subscriber/respond?correlationId=…&status=Completed|Failed
                                                                 deliver the late response → watch recovery in logs
      GET  /healthz                                              health report incl. the recovery watchdog
    """));

// 1) Request/response with an active waiter. The WaitAsync trigger guarantees
//    subscribe-before-send.
app.MapPost("/demo/request-response", async (IAsyncResponseBuilder asyncResponse, RemoteWorkSimulator remote, RemoteBehavior? behavior) =>
{
    var correlationId = AsyncResponseContext.CreateCorrelationId();

    var result = await asyncResponse
        .For<OperationResult>(correlationId)
        .WithTimeout(TimeSpan.FromSeconds(30))
        .Until(response => response.Status != OperationStatus.Running) // consume progress messages
        .WaitAsync(() =>
        {
            remote.Start(correlationId, behavior ?? RemoteBehavior.Succeed);
            return Task.CompletedTask;
        });

    return result.Status == OperationStatus.Completed
        ? Results.Ok(new { correlationId, result.Status, result.Message })
        : Results.Problem(title: "Remote operation failed", detail: result.Message, statusCode: 502);
});

// 2) Timeout: the remote takes 15s, the waiter allows 2s. Also shows the no-correlation-id
//    flow: For<T>() generates one and hands it to the trigger.
app.MapPost("/demo/timeout", async (IAsyncResponseBuilder asyncResponse, RemoteWorkSimulator remote) =>
{
    try
    {
        await asyncResponse
            .For<OperationResult>()
            .WithTimeout(TimeSpan.FromSeconds(2))
            .Until(response => response.Status != OperationStatus.Running)
            .WaitAsync(correlationId =>
            {
                remote.Start(correlationId, RemoteBehavior.Slow);
                return Task.CompletedTask;
            });

        return Results.Ok("Unexpectedly completed in time.");
    }
    catch (TimeoutException timeout)
    {
        return Results.Problem(title: "Timed out as expected", detail: timeout.Message, statusCode: 504);
    }
});

// 3) Fire-and-forget worker job via the in-process worker transport.
app.MapPost("/demo/worker", async (IAsyncResponseBuilder asyncResponse, int orderId) =>
{
    AsyncResponseContext.CreateCorrelationId();
    await asyncResponse.EnqueueWorkerAsync<ISampleFlowService>(flow => flow.ProcessOrderAsync(orderId));
    return Results.Accepted(value: new { orderId, note = "Watch the logs for WORKER output." });
});

// 4a) Lost-subscriber demo — arm: register a waiter with recovery callbacks and keep it alive.
app.MapPost("/demo/lost-subscriber/arm", async (IAsyncResponseBuilder asyncResponse) =>
{
    var correlationId = AsyncResponseContext.CreateCorrelationId();

    var waiter = await asyncResponse
        .For<OperationResult>(correlationId)
        .Until(response => response.Status != OperationStatus.Running)
        .OnLostSubscriberResume<ISampleFlowService>(flow =>
            flow.ResumeFlowAsync("sample-flow", Placeholder.Payload<OperationResult>(), Placeholder.CorrelationId()))
        .OnLostSubscriberFailure<ISampleFlowService>(flow =>
            flow.FailFlowAsync(Placeholder.Exception(), Placeholder.CorrelationId()))
        .BuildWaiterAsync();

    armedWaiters[correlationId] = waiter;

    return Results.Ok(new
    {
        correlationId,
        next = "POST /demo/lost-subscriber/crash, then /demo/lost-subscriber/respond?correlationId=…&status=Completed|Failed"
    });
});

// 4b) Lost-subscriber demo — crash: drop every Redis subscription, like a redeploy would.
// The recovery state stays in Redis; only the in-memory waiters die.
app.MapPost("/demo/lost-subscriber/crash", (IConnectionMultiplexer multiplexer) =>
{
    multiplexer.GetSubscriber().UnsubscribeAll();
    return Results.Ok("All subscriptions dropped (simulated redeploy). The armed waiters are now lost.");
});

// 4c) Lost-subscriber demo — respond: deliver the late terminal response. With no subscriber
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
