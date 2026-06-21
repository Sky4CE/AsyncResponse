using AsyncResponse;
using AsyncResponse.Channels.Redis;
using AsyncResponse.Sample;
using AsyncResponse.Transports.GooglePubSub;
using Google.Api.Gax;
using Google.Cloud.PubSub.V1;
using Google.Protobuf;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;
using StackExchange.Redis;
using System.Text.Json;

var builder = WebApplication.CreateBuilder(args);
builder.AddServiceDefaults();
builder.Services.AddOpenApi();

// --- Provider selection (configuration-driven) ----------------------------------------------
// Channel = the response/recovery substrate (exactly one); Transport = worker dispatch (exactly one).
// Defaults are fully in-memory so `dotnet run` works with no external dependencies; the AppHost
// overrides them to Redis + Google Pub/Sub to exercise the durable, broker-backed stack.
var channel = builder.Configuration["AsyncResponse:Channel"] ?? "InMemory";      // InMemory | Redis
var transport = builder.Configuration["AsyncResponse:Transport"] ?? "InMemory";  // InMemory | GooglePubSub
var useInMemoryChannel = string.Equals(channel, "InMemory", StringComparison.OrdinalIgnoreCase);
var useRedis = string.Equals(channel, "Redis", StringComparison.OrdinalIgnoreCase);
var useInMemoryTransport = string.Equals(transport, "InMemory", StringComparison.OrdinalIgnoreCase);
var useGooglePubSub = string.Equals(transport, "GooglePubSub", StringComparison.OrdinalIgnoreCase);

if (useRedis)
{
    var redisConnectionString = builder.Configuration.GetConnectionString("Redis") ?? "localhost:6379";
    builder.Services.AddSingleton<IConnectionMultiplexer>(_ => ConnectionMultiplexer.Connect(redisConnectionString));
}

// Provision the emulator's topics/subscriptions before the transport's subscribers start
// (emulator-only; no-op against real GCP). Registered first so its StartAsync completes first.
if (useGooglePubSub)
{
    ConfigureHostShutdownBudget(builder.Configuration, builder.Services);
    builder.Services.AddHostedService<PubSubEmulatorProvisioner>();
    builder.Services.AddSingleton(_ => new Lazy<Task<PublisherServiceApiClient>>(() => new PublisherServiceApiClientBuilder
    {
        EmulatorDetection = EmulatorDetection.EmulatorOrProduction
    }.BuildAsync()));
}

var asyncResponse = builder.Services.AddAsyncResponse(options =>
{
    // Aggressive watchdog so the stale-recovery demo/health scenario resolves quickly; defaults 6h/24h/5m.
    options.Watchdog.StartupDelay = TimeSpan.FromSeconds(1);
    options.Watchdog.Interval = TimeSpan.FromSeconds(1);
    options.Watchdog.StaleAfter = TimeSpan.FromSeconds(2);
});

// Exactly one channel (enforced at host startup).
if (useRedis)
{
    asyncResponse.WithRedisChannel(options =>
    {
        options.KeyPrefix = builder.Configuration["AsyncResponse:KeyPrefix"] ?? "sample";
        options.DefaultTimeout = TimeSpan.FromSeconds(30);
    });
}
else if (useInMemoryChannel)
{
    asyncResponse.WithInMemoryChannel();
}
else
{
    throw new InvalidOperationException(
        "Unsupported AsyncResponse:Channel value. Use 'InMemory' or 'Redis'.");
}

// Exactly one worker transport (enforced at host startup).
if (useGooglePubSub)
{
    asyncResponse.WithGooglePubSubTransport(options =>
    {
        options.ProjectId = builder.Configuration["PubSub:ProjectId"];
        options.WorkerTopicId = builder.Configuration["PubSub:WorkerTopicId"];
        options.WorkerSubscriptionId = builder.Configuration["PubSub:WorkerSubscriptionId"];
        options.ResponseTopicId = builder.Configuration["PubSub:ResponseTopicId"];
        options.ResponseSubscriptionId = builder.Configuration["PubSub:ResponseSubscriptionId"];
        ConfigurePubSubShutdownBudget(builder.Configuration, options);
        ConfigureSubscriberAckMode(builder.Configuration, "PubSub:Worker", options.WorkerSubscriber);
        ConfigureSubscriberAckMode(builder.Configuration, "PubSub:Response", options.ResponseSubscriber);
    });
}
else if (useInMemoryTransport)
{
    asyncResponse.WithInMemoryTransport();
}
else
{
    throw new InvalidOperationException(
        "Unsupported AsyncResponse:Transport value. Use 'InMemory' or 'GooglePubSub'.");
}

// Ambient-context propagators — trace and tenant compose, each carrying its own key across hops.
asyncResponse
    .WithContextPropagator<SampleTracePropagator>()
    .WithContextPropagator<SampleTenantPropagator>();

builder.Services.AddHealthChecks().AddAsyncResponseRecoveryCheck();

builder.Services.AddSingleton<FlowRecorder>();
builder.Services.AddSingleton<ISampleFlowService, SampleFlowService>();
builder.Services.AddSingleton<RemoteWorkSimulator>();

var app = builder.Build();
app.Logger.LogInformation("AsyncResponse sample started: channel={Channel}, transport={Transport}.", channel, transport);
app.MapOpenApi();
app.UseSwaggerUI(options => options.SwaggerEndpoint("/openapi/v1.json", "AsyncResponse sample v1"));

app.MapGet("/", () => Results.Text(
    $"""
    AsyncResponse sample — channel: {channel}, transport: {transport}.

      GET  /swagger                                              interactive request playground
      POST /request-response?behavior=Succeed|FailDomain|Fail|Timeout   active-waiter round-trip
      POST /attach                                               wait by a known correlation id (no trigger)
      POST /multi-step?first=Succeed&second=Succeed              sequential two-step flow
      POST /ambient-exception                                    SetException via ambient correlation id
      POST /shared-correlation-exception                         one SetException faults multiple waiters
      POST /worker?token=42                                      fire-and-forget background worker job
      POST /arm  + POST /crash + POST /publish                   lost-subscriber recovery flow (Redis)
      POST /lost-subscriber-flow?outcome=Completed|Failed|Exception  composed recovery flow (Redis)
      GET  /reply-target                                         provider-resolved reply target (Pub/Sub)
      GET  /config                                               resolved channel/transport/ACK mode
      GET  /healthz                                              health report incl. the recovery watchdog
    """)).ExcludeFromDescription();

// Liveness probe (used by the AppHost health check) — always 200 while the process is up,
// independent of the recovery health check (which may go Degraded in the watchdog scenario).
app.MapGet("/alive", () => Results.Ok("alive")).ExcludeFromDescription();

// Reports the providers this instance resolved from configuration. The in-process tests assert
// InMemory/InMemory and the Aspire SUT asserts Redis/GooglePubSub, so each variation is provably
// exercised even though only one is ever booted per process.
app.MapGet("/config", (IServiceProvider services) =>
{
    object? pubsub = null;
    if (useGooglePubSub)
    {
        var googleOptions = services.GetRequiredService<IOptions<GooglePubSubAsyncResponseOptions>>().Value;
        pubsub = new
        {
            workerAckMode = googleOptions.WorkerSubscriber.AckMode.ToString(),
            workerBackgroundWorkerCount = googleOptions.WorkerSubscriber.BackgroundWorkerCount,
            workerBackgroundQueueCapacity = googleOptions.WorkerSubscriber.BackgroundQueueCapacity,
            responseAckMode = googleOptions.ResponseSubscriber.AckMode.ToString(),
            responseBackgroundWorkerCount = googleOptions.ResponseSubscriber.BackgroundWorkerCount,
            responseBackgroundQueueCapacity = googleOptions.ResponseSubscriber.BackgroundQueueCapacity
        };
    }

    return Results.Ok(new { channel, transport, pubsub });
}).WithTags("Observability");

static string NormalizeBehavior(string? behavior)
    => (behavior ?? "Succeed").Trim().ToLowerInvariant() switch
    {
        "fail" => "fail",
        "failtechnical" => "fail",
        "technical" => "fail",
        "faildomain" => "faildomain",
        "domain" => "faildomain",
        "timeout" => "timeout",
        _ => "succeed"
    };

static void ConfigurePubSubShutdownBudget(
    IConfiguration configuration,
    GooglePubSubAsyncResponseOptions options)
{
    var timeout = ReadOptionalPositiveTimeout(configuration, "PubSub:HostShutdownTimeoutSeconds");
    if (timeout is not null)
        options.HostShutdownTimeout = timeout.Value;
}

static void ConfigureHostShutdownBudget(
    IConfiguration configuration,
    IServiceCollection services)
{
    var timeout = ReadOptionalPositiveTimeout(configuration, "PubSub:HostShutdownTimeoutSeconds");
    if (timeout is not null)
        services.Configure<HostOptions>(options => options.ShutdownTimeout = timeout.Value);
}

static TimeSpan? ReadOptionalPositiveTimeout(IConfiguration configuration, string key)
{
    var rawValue = configuration[key];
    if (string.IsNullOrWhiteSpace(rawValue))
        return null;

    if (!int.TryParse(rawValue, out var seconds) || seconds <= 0)
        throw new InvalidOperationException($"{key} must be a positive integer when set.");

    return TimeSpan.FromSeconds(seconds);
}

static void ConfigureSubscriberAckMode(
    IConfiguration configuration,
    string prefix,
    GooglePubSubSubscriberOptions subscriberOptions)
{
    var rawMode = configuration[$"{prefix}:AckMode"];
    if (string.IsNullOrWhiteSpace(rawMode))
        return;

    if (!Enum.TryParse<GooglePubSubAckMode>(rawMode, ignoreCase: true, out var mode))
        throw new InvalidOperationException($"{prefix}:AckMode must be one of: {string.Join(", ", Enum.GetNames<GooglePubSubAckMode>())}.");

    if (mode is GooglePubSubAckMode.AckAfterHandlerCompletes)
    {
        subscriberOptions.AckMode = GooglePubSubAckMode.AckAfterHandlerCompletes;
        return;
    }

    if (mode is not GooglePubSubAckMode.AckAfterEnqueue)
        throw new InvalidOperationException($"{prefix}:AckMode has unsupported value '{rawMode}'.");

    var workerCount = ReadRequiredPositiveInt(configuration, $"{prefix}:BackgroundWorkerCount");
    var queueCapacity = ReadRequiredPositiveInt(configuration, $"{prefix}:BackgroundQueueCapacity");
    var drainTimeoutSeconds = configuration[$"{prefix}:BackgroundDrainTimeoutSeconds"];
    TimeSpan? drainTimeout = null;
    if (!string.IsNullOrWhiteSpace(drainTimeoutSeconds))
    {
        if (!int.TryParse(drainTimeoutSeconds, out var seconds) || seconds <= 0)
            throw new InvalidOperationException($"{prefix}:BackgroundDrainTimeoutSeconds must be a positive integer when set.");

        drainTimeout = TimeSpan.FromSeconds(seconds);
    }

    subscriberOptions.UseAckAfterEnqueue(workerCount, queueCapacity, drainTimeout);
}

static int ReadRequiredPositiveInt(IConfiguration configuration, string key)
{
    var rawValue = configuration[key];
    if (!int.TryParse(rawValue, out var value) || value <= 0)
        throw new InvalidOperationException($"{key} must be explicitly set to a positive integer.");

    return value;
}

static async Task<StepOutcome> RunStepAsync(
    IAsyncResponseBuilder asyncResponse,
    IAsyncResponsePublisher publisher,
    RemoteWorkSimulator remote,
    FlowRecorder recorder,
    string stepName,
    string? behavior)
{
    var kind = NormalizeBehavior(behavior);
    string? correlationId = null;

    try
    {
        var result = await asyncResponse
            .For<OperationResult>()
            .WithTimeout(kind == "timeout" ? TimeSpan.FromSeconds(2) : TimeSpan.FromSeconds(30))
            .Until(response => response.Status != OperationStatus.Running)
            .WaitAsync(context =>
            {
                correlationId = context.CorrelationId;
                return kind switch
                {
                    "faildomain" => StartRemoteAsync(remote, context.CorrelationId, RemoteBehavior.FailDomain),
                    "fail" => publisher.SetException(new InvalidOperationException($"{stepName} technical error"), context.CorrelationId),
                    "timeout" => StartRemoteAsync(remote, context.CorrelationId, RemoteBehavior.Slow),
                    _ => StartRemoteAsync(remote, context.CorrelationId, RemoteBehavior.Succeed)
                };
            });

        var succeeded = result.Status == OperationStatus.Completed;
        recorder.Record($"multi:{correlationId}", new FlowCall(
            $"step-{stepName}",
            correlationId,
            SampleTraceContext.Current,
            SampleTenantContext.Current,
            result.Status,
            result.Message));

        return new StepOutcome(
            stepName,
            correlationId!,
            succeeded,
            result.Status,
            result.Message,
            null,
            succeeded ? null : result.Message);
    }
    catch (Exception ex)
    {
        recorder.Record($"multi:{correlationId}", new FlowCall(
            $"step-{stepName}-faulted",
            correlationId,
            SampleTraceContext.Current,
            SampleTenantContext.Current,
            null,
            ex.Message));

        return new StepOutcome(
            stepName,
            correlationId ?? string.Empty,
            false,
            null,
            null,
            ex.GetType().Name,
            ex.Message);
    }
}

static Task StartRemoteAsync(RemoteWorkSimulator remote, string correlationId, RemoteBehavior behavior)
{
    remote.Start(correlationId, behavior);
    return Task.CompletedTask;
}

static async Task<string> CaptureFailureAsync(Task<OperationResult> task)
{
    try
    {
        var result = await task.ConfigureAwait(false);
        return $"completed:{result.Status}";
    }
    catch (Exception ex)
    {
        return $"{ex.GetType().Name}: {ex.Message}";
    }
}

// 1) Request/response with an active waiter and a selectable terminal behavior. For<T>() generates
//    the correlation id; the required WaitAsync trigger guarantees subscribe-before-send.
app.MapPost("/request-response", async (
    IAsyncResponseBuilder asyncResponse, IAsyncResponsePublisher publisher, RemoteWorkSimulator remote,
    string? behavior, string? trace, ILoggerFactory loggerFactory) =>
{
    var logger = loggerFactory.CreateLogger("RequestResponse");
    var kind = (behavior ?? "Succeed").ToLowerInvariant();

    // Per-request ambient context flows into the response handler (a subscriber-thread callback) via
    // the captured ExecutionContext, so the HANDLER log lines carry it.
    SampleTraceContext.Set(trace ?? $"trace-{Guid.NewGuid().ToString("N")[..8]}");
    SampleTenantContext.Set("tenant-acme");

    try
    {
        var result = await asyncResponse
            .For<OperationResult>()
            .WithTimeout(kind == "timeout" ? TimeSpan.FromSeconds(2) : TimeSpan.FromSeconds(30))
            .Until(response =>
            {
                logger.LogInformation("HANDLER: progress {Status} (traceId: {TraceId}, tenant: {Tenant})",
                    response.Status, SampleTraceContext.Current, SampleTenantContext.Current);
                return response.Status != OperationStatus.Running; // consume progress messages
            })
            .WaitAsync(async context =>
            {
                switch (kind)
                {
                    case "faildomain":
                        remote.Start(context.CorrelationId, RemoteBehavior.FailDomain);
                        break;
                    case "fail":
                        await publisher.SetException(new InvalidOperationException("remote technical error"), context.CorrelationId);
                        break;
                    case "timeout":
                        remote.Start(context.CorrelationId, RemoteBehavior.Slow); // never terminal in time
                        break;
                    default:
                        remote.Start(context.CorrelationId, RemoteBehavior.Succeed);
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
})
.WithTags("Request/response");

// 2) Attach to an in-flight operation by a known correlation id. For<T>(correlationId) is wait-only
//    (no trigger): the operation was started elsewhere; here we just await its response.
app.MapPost("/attach", async (IAsyncResponseBuilder asyncResponse, RemoteWorkSimulator remote) =>
{
    var correlationId = AsyncResponseContext.GenerateCorrelationId();
    remote.Start(correlationId, RemoteBehavior.Succeed); // a 400ms head start before the first delivery

    var result = await asyncResponse
        .For<OperationResult>(correlationId)
        .WithTimeout(TimeSpan.FromSeconds(30))
        .Until(response => response.Status != OperationStatus.Running)
        .WaitAsync();

    return Results.Ok(new { correlationId, result.Status, result.Message });
})
.WithTags("Request/response");

// 2b) Sequential multi-step flow. Step 2 is only started if step 1 completes successfully, which
//     makes fail-fast behavior visible over HTTP instead of hidden inside a unit-only helper.
app.MapPost("/multi-step", async (
    IAsyncResponseBuilder asyncResponse,
    IAsyncResponsePublisher publisher,
    RemoteWorkSimulator remote,
    FlowRecorder recorder,
    string? first,
    string? second,
    string? trace) =>
{
    SampleTraceContext.Set(trace ?? $"trace-{Guid.NewGuid().ToString("N")[..8]}");
    SampleTenantContext.Set("tenant-acme");

    var steps = new List<StepOutcome>(capacity: 2);

    var firstStep = await RunStepAsync(asyncResponse, publisher, remote, recorder, "first", first);
    steps.Add(firstStep);
    if (!firstStep.Succeeded)
        return Results.Ok(new MultiStepFlowResult(false, firstStep.Name, steps));

    var secondStep = await RunStepAsync(asyncResponse, publisher, remote, recorder, "second", second);
    steps.Add(secondStep);

    return Results.Ok(new MultiStepFlowResult(secondStep.Succeeded, secondStep.Succeeded ? null : secondStep.Name, steps));
})
.WithTags("Flows");

// 2c) Demonstrates the publisher's ambient correlation fallback: the trigger receives a context,
//     but SetException is intentionally called without passing the id.
app.MapPost("/ambient-exception", async (
    IAsyncResponseBuilder asyncResponse,
    IAsyncResponsePublisher publisher,
    string? message) =>
{
    try
    {
        await asyncResponse
            .For<OperationResult>()
            .WithTimeout(TimeSpan.FromSeconds(10))
            .WaitAsync(_ => publisher.SetException(new InvalidOperationException(message ?? "ambient technical error")));

        return Results.Problem("The waiter completed successfully; it was expected to fault.");
    }
    catch (Exception ex)
    {
        return Results.Ok(new { faulted = true, exceptionType = ex.GetType().Name, detail = ex.Message });
    }
})
.WithTags("Request/response");

// 2d) Multiple waiters attached to the same correlation id should all fault when a technical
//     failure is published for that id.
app.MapPost("/shared-correlation-exception", async (
    IAsyncResponseSubscriber subscriber,
    IAsyncResponsePublisher publisher,
    string? message) =>
{
    var correlationId = AsyncResponseContext.GenerateCorrelationId();
    await using var first = await subscriber.CreateResponseWaiter<OperationResult>(
        correlationId,
        timeout: TimeSpan.FromSeconds(10));
    await using var second = await subscriber.CreateResponseWaiter<OperationResult>(
        correlationId,
        timeout: TimeSpan.FromSeconds(10));

    var firstResponse = first.ResponseTask;
    var secondResponse = second.ResponseTask;

    await publisher.SetException(new InvalidOperationException(message ?? "shared technical error"), correlationId);
    var failures = await Task.WhenAll(CaptureFailureAsync(firstResponse), CaptureFailureAsync(secondResponse));

    return Results.Ok(new SharedExceptionResult(correlationId, failures));
})
.WithTags("Request/response");

// 3) Reply target resolved by the registered transport's provider (Pub/Sub). Returns the target the
//    trigger observed; falls back to a clear 409 when no provider is registered (e.g. in-memory).
app.MapGet("/reply-target", async (IAsyncResponseBuilder asyncResponse, IAsyncResponsePublisher publisher) =>
{
    AsyncResponseReplyTarget? observed = null;
    try
    {
        await asyncResponse
            .For<OperationResult>()
            .WithReplyTarget()
            .WithTimeout(TimeSpan.FromSeconds(20))
            .Until(r => r.Status != OperationStatus.Running)
            .WaitAsync(async ctx =>
            {
                observed = ctx.ReplyTarget;
                await publisher.SetResponse(new OperationResult { Status = OperationStatus.Completed }, ctx.CorrelationId);
            });
    }
    catch (InvalidOperationException ex)
    {
        return Results.Conflict(ex.Message); // no reply-target provider registered for this transport
    }

    return Results.Ok(new { observed?.Transport, observed?.Address });
})
.WithTags("Request/response");

// 4) Fire-and-forget worker job over the configured transport. Returns the correlation id it set so a
//    round-trip test can assert it is restored on the consuming side.
app.MapPost("/worker", async (IAsyncResponseBuilder asyncResponse, string token, string? trace) =>
{
    var correlationId = AsyncResponseContext.CreateCorrelationId();
    SampleTraceContext.Set(trace);
    SampleTenantContext.Set("tenant-acme");
    await asyncResponse.EnqueueWorkerAsync<ISampleFlowService>(flow => flow.ProcessWorkAsync(token));
    return Results.Ok(new { correlationId });
})
.WithTags("Workers");

// 5a) Lost-subscriber recovery — arm: register a waiter with recovery callbacks and keep it waiting
//     in the background. The HTTP request returns immediately; the subscription and persisted
//     recovery state stay alive. The propagators capture the trace/tenant into the recovery state.
app.MapPost("/arm", async (IAsyncResponseBuilder asyncResponse, FlowRecorder recorder, string? trace) =>
{
    SampleTraceContext.Set(trace);
    SampleTenantContext.Set("tenant-acme");
    var armed = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);

    var waitTask = asyncResponse
        .For<OperationResult>()
        .WithTimeout(TimeSpan.FromMinutes(2))
        .Until(r => r.Status != OperationStatus.Running)
        .OnLostSubscriberResume<ISampleFlowService>(flow =>
            flow.ResumeFlowAsync("sample-flow", Placeholder.Payload<OperationResult>(), Placeholder.CorrelationId()))
        .OnLostSubscriberFailure<ISampleFlowService>(flow =>
            flow.FailFlowAsync(Placeholder.Exception(), Placeholder.CorrelationId()))
        .WaitAsync(context =>
        {
            armed.SetResult(context.CorrelationId);
            return Task.CompletedTask;
        });

    var correlationId = await armed.Task;
    _ = waitTask.ContinueWith(t => recorder.RecordWaiterResult(correlationId, t), TaskScheduler.Default);
    return Results.Ok(new { correlationId });
})
.WithTags("Recovery");

// 5b) Lost-subscriber recovery — crash: drop every Redis subscription, like a redeploy would. The
//     recovery state stays in Redis; only the in-memory waiters die. (Redis channel only.)
app.MapPost("/crash", (IServiceProvider services) =>
{
    var multiplexer = services.GetService<IConnectionMultiplexer>();
    if (multiplexer is null)
        return Results.Conflict("Crash simulation requires the Redis channel (the in-memory channel has no durable recovery to survive it).");

    multiplexer.GetSubscriber().UnsubscribeAll();
    return Results.Ok();
})
.WithTags("Recovery");

// 5c) Deliver a late response/exception for a correlation id through the configured channel (used by
//     the lost-subscriber recovery scenarios after /crash, and by the active-waiter scenarios).
app.MapPost("/publish", async (IAsyncResponsePublisher publisher, string correlationId, string? status, string? message, string? exception) =>
{
    if (exception is not null)
    {
        await publisher.SetException(new InvalidOperationException(exception), correlationId);
    }
    else
    {
        var parsedStatus = Enum.TryParse<OperationStatus>(status, ignoreCase: true, out var s) ? s : OperationStatus.Completed;
        await publisher.SetResponse(new OperationResult { Status = parsedStatus, Message = message }, correlationId);
    }

    return Results.Accepted();
})
.WithTags("Recovery");

// 5d) Composed lost-subscriber recovery: arm, simulate the crash, publish the late terminal signal,
//     and wait for the recovery callback in one request. This endpoint complements the lower-level
//     /arm + /crash + /publish endpoints that integration tests can still drive step by step.
app.MapPost("/lost-subscriber-flow", async (
    IAsyncResponseBuilder asyncResponse,
    IAsyncResponsePublisher publisher,
    FlowRecorder recorder,
    IServiceProvider services,
    IOptions<RedisAsyncResponseOptions> redisOptions,
    string? outcome,
    string? trace) =>
{
    var multiplexer = services.GetService<IConnectionMultiplexer>();
    if (multiplexer is null)
        return Results.Conflict("Composed lost-subscriber recovery requires the Redis channel.");

    SampleTraceContext.Set(trace ?? $"trace-{Guid.NewGuid().ToString("N")[..8]}");
    SampleTenantContext.Set("tenant-acme");
    var armed = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);

    var waitTask = asyncResponse
        .For<OperationResult>()
        .WithTimeout(TimeSpan.FromMinutes(2))
        .Until(r => r.Status != OperationStatus.Running)
        .OnLostSubscriberResume<ISampleFlowService>(flow =>
            flow.ResumeFlowAsync("sample-flow", Placeholder.Payload<OperationResult>(), Placeholder.CorrelationId()))
        .OnLostSubscriberFailure<ISampleFlowService>(flow =>
            flow.FailFlowAsync(Placeholder.Exception(), Placeholder.CorrelationId()))
        .WaitAsync(context =>
        {
            armed.SetResult(context.CorrelationId);
            return Task.CompletedTask;
        });

    var correlationId = await armed.Task.WaitAsync(TimeSpan.FromSeconds(10));
    _ = waitTask.ContinueWith(t => recorder.RecordWaiterResult(correlationId, t), TaskScheduler.Default);

    var responseChannel = new RedisChannel(
        $"{redisOptions.Value.KeyPrefix}:response:{correlationId}",
        RedisChannel.PatternMode.Literal);
    await multiplexer.GetSubscriber().UnsubscribeAsync(responseChannel);
    await Task.Delay(100);

    var normalized = (outcome ?? "Completed").Trim().ToLowerInvariant();
    if (normalized is "exception" or "failtechnical" or "technical")
    {
        await publisher.SetException(new InvalidOperationException("lost-subscriber technical error"), correlationId);
        normalized = "exception";
    }
    else
    {
        var status = normalized switch
        {
            "failed" or "faildomain" or "domain" => OperationStatus.Failed,
            "running" => OperationStatus.Running,
            _ => OperationStatus.Completed
        };
        normalized = status.ToString();
        await publisher.SetResponse(new OperationResult { Status = status, Message = "late" }, correlationId);
    }

    var callbackKind = normalized is nameof(OperationStatus.Completed) or nameof(OperationStatus.Running)
        ? "resume"
        : "fail";
    var callback = await recorder.WaitForAsync($"{callbackKind}:{correlationId}").WaitAsync(TimeSpan.FromSeconds(30));

    return Results.Ok(new LostSubscriberFlowResult(correlationId, normalized, callback));
})
.WithTags("Recovery");

// 6) Publish a raw response to the Pub/Sub response topic, acting as the remote system. With
//    useAttribute the correlation id rides a message attribute; otherwise it goes in the JSON body so
//    the extractor's JSON-path fallback is exercised. (Google Pub/Sub transport only.)
app.MapPost("/emit-response", async (
    IServiceProvider services, string correlationId, string? status, bool useAttribute, string? message) =>
{
    var options = services.GetService<IOptions<GooglePubSubAsyncResponseOptions>>();
    if (options is null)
        return Results.Conflict("Pub/Sub response ingress requires the Google Pub/Sub transport.");
    var publisherFactory = services.GetService<Lazy<Task<PublisherServiceApiClient>>>();
    if (publisherFactory is null)
        return Results.Conflict("Pub/Sub publisher client is not registered.");

    var o = options.Value;
    var parsedStatus = Enum.TryParse<OperationStatus>(status, ignoreCase: true, out var s) ? s : OperationStatus.Completed;

    var json = useAttribute
        ? JsonSerializer.Serialize(new OperationResult { Status = parsedStatus, Message = message })
        : JsonSerializer.Serialize(new { CorrelationId = correlationId, Status = (int)parsedStatus, Message = message });

    var pubsubMessage = new PubsubMessage { Data = ByteString.CopyFromUtf8(json) };
    if (useAttribute)
        pubsubMessage.Attributes[o.CorrelationIdAttribute] = correlationId;

    var publisher = await publisherFactory.Value.ConfigureAwait(false);
    await publisher.PublishAsync(TopicName.FromProjectTopic(o.ProjectId!, o.ResponseTopicId!), [pubsubMessage]);

    return Results.Accepted();
})
.WithTags("Workers");

// --- Observability / test affordances --------------------------------------------------------

// Long-poll the flow recorder for a recorded call (e.g. worker:{token}, resume:{cid}, waiter:{cid}).
app.MapGet("/calls", async (FlowRecorder recorder, string key, int? timeoutMs) =>
{
    try
    {
        var call = await recorder.WaitForAsync(key).WaitAsync(TimeSpan.FromMilliseconds(timeoutMs ?? 15000));
        return Results.Ok(call);
    }
    catch (TimeoutException)
    {
        return Results.StatusCode(StatusCodes.Status408RequestTimeout);
    }
})
.WithTags("Observability");

// Seed a stale recovery entry (no live subscriber) so the watchdog surfaces it as Degraded health.
app.MapPost("/seed-recovery", async (IRecoveryStateStore store, string correlationId, int? ageMinutes) =>
{
    await store.SaveAsync(correlationId, new RecoveryState
    {
        CorrelationId = correlationId,
        PayloadTypeFullName = typeof(OperationResult).FullName,
        RegisteredAtUtc = DateTime.UtcNow.AddMinutes(-(ageMinutes ?? 5))
    }, TimeSpan.FromMinutes(10));

    return Results.Accepted();
})
.WithTags("Observability");

app.MapDelete("/test/recovery/{correlationId}", async (IRecoveryStateStore store, string correlationId) =>
{
    var deleted = await store.TryDeleteAsync(correlationId);
    return Results.Ok(new { deleted });
})
.WithTags("Observability");

app.MapPost("/test/reset", async (IRecoveryStateScanner scanner, IRecoveryStateStore store, FlowRecorder recorder, CancellationToken cancellationToken) =>
{
    var deleted = 0;
    await foreach (var state in scanner.ScanAsync(cancellationToken))
    {
        if (!string.IsNullOrWhiteSpace(state.CorrelationId)
            && await store.TryDeleteAsync(state.CorrelationId, cancellationToken))
        {
            deleted++;
        }
    }

    recorder.Clear();
    return Results.Ok(new { deleted });
})
.WithTags("Observability");

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
}).WithTags("Health");

app.MapDefaultEndpoints();
app.Run();

/// <summary>Exposed so in-process integration tests can boot the app with WebApplicationFactory.</summary>
public partial class Program;
