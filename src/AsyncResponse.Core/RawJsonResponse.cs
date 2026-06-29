namespace AsyncResponse;

/// <summary>
/// Raw broker/webhook response payload that can be materialized into the active waiter's payload
/// type without first allocating an intermediate JsonElement.
/// </summary>
internal sealed class RawJsonResponse
{
    private readonly string _json;
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
        if (_hasUntypedPayload)
            return _untypedPayload;

        _untypedPayload = JsonSafety.SafeDeserialize<object?>(_json);
        _hasUntypedPayload = true;
        return _untypedPayload;
    }

    /// <summary>Runs the Deserialize operation.</summary>
    public T? Deserialize<T>() => (T?)Deserialize(typeof(T));

    /// <summary>Runs the Deserialize operation.</summary>
    public object? Deserialize(Type payloadType)
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
