namespace AsyncResponse;

/// <summary>
/// Raw broker/webhook response payload that can be materialized into the active waiter's payload
/// type without first allocating an intermediate JsonElement.
/// </summary>
internal sealed class RawJsonResponse
{
    private readonly string _json;

    // One instance is shared across every subscriber of a correlation id, and fan-out dispatch can
    // materialize payloads from multiple threads. TYPED payloads are deliberately NOT memoized:
    // handing one mutable payload instance to multiple same-type waiters would alias user state
    // across concurrently-running predicates and handlers — every durable channel deserializes a
    // private instance per waiter, and the in-memory raw path must match (wire parity, same rule
    // as the typed path's MaterializeAs). The untyped memo below stays: it materializes an
    // immutable JsonElement, so sharing it is safe; _gate guards its torn-publication hazard.
    private readonly object _gate = new();
    private object? _untypedPayload;
    private bool _hasUntypedPayload;

    /// <summary>Runs the RawJsonResponse operation.</summary>
    public RawJsonResponse(string json)
    {
        JsonSafety.ThrowIfClearlyNotJson(json);
        _json = json;
    }

    public string Json => _json;

    /// <summary>Runs the DeserializeUntyped operation.</summary>
    public object? DeserializeUntyped()
    {
        lock (_gate)
        {
            if (_hasUntypedPayload)
                return _untypedPayload;

            _untypedPayload = JsonSafety.SafeDeserialize<object?>(_json);
            _hasUntypedPayload = true;
            return _untypedPayload;
        }
    }

    /// <summary>Runs the Deserialize operation.</summary>
    public T? Deserialize<T>() => (T?)Deserialize(typeof(T));

    /// <summary>Materializes a private payload instance per call — see the aliasing note above.</summary>
    public object? Deserialize(Type payloadType)
        => JsonSafety.SafeDeserialize(_json, payloadType);
}
