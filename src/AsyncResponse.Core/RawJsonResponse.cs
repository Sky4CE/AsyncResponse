namespace AsyncResponse;

/// <summary>
/// Raw broker/webhook response payload that can be materialized into the active waiter's payload
/// type without first allocating an intermediate JsonElement.
/// </summary>
internal sealed class RawJsonResponse
{
    private readonly string _json;

    // One instance is shared across every subscriber of a correlation id, and fan-out dispatch can
    // materialize payloads from multiple threads. All memoization state below is guarded by _gate:
    // an unsynchronized publication could hand one waiter a torn (type set, payload null) pair, and
    // a plain Dictionary corrupts under concurrent writes. The uncontended lock is cheap next to
    // the deserialization it guards, and allocation behavior is unchanged.
    private readonly object _gate = new();
    private Dictionary<Type, object?>? _typedPayloads;
    private Type? _singlePayloadType;
    private object? _singlePayload;
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

    /// <summary>Runs the Deserialize operation.</summary>
    public object? Deserialize(Type payloadType)
    {
        lock (_gate)
        {
            if (_singlePayloadType == payloadType)
                return _singlePayload;

            if (_typedPayloads?.TryGetValue(payloadType, out var cached) == true)
                return cached;

            var payload = JsonSafety.SafeDeserialize(_json, payloadType);
            if (_singlePayloadType is null)
            {
                _singlePayloadType = payloadType;
                _singlePayload = payload;
                return payload;
            }

            if (_typedPayloads is null)
            {
                _typedPayloads = new Dictionary<Type, object?>
                {
                    [_singlePayloadType] = _singlePayload
                };
            }

            _typedPayloads.Add(payloadType, payload);
            return payload;
        }
    }
}
