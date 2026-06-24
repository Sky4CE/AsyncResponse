namespace AsyncResponse;

/// <summary>
/// Internal ingress hook for broker/webhook adapters that receive raw JSON before the waiter's
/// payload type is known. Public publishing remains restricted to typed async-response payloads.
/// </summary>
internal interface IRawAsyncResponsePublisher
{
    Task SetRawResponse(object? response, string correlationId, CancellationToken cancellationToken = default);

    Task SetRawResponseJson(string responseJson, string correlationId, CancellationToken cancellationToken = default);
}
