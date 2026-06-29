namespace AsyncResponse.Transports.RabbitMQ;

internal static class RabbitMqOptionsValidator
{
    /// <summary>Validates the supplied options.</summary>
    public static string Required(string? value, string name)
        => !string.IsNullOrWhiteSpace(value)
            ? value
            : throw new InvalidOperationException($"{nameof(RabbitMqAsyncResponseOptions)}.{name} must be configured.");

    /// <summary>Validates the supplied options.</summary>
    public static void Positive(TimeSpan value, string name)
    {
        if (value <= TimeSpan.Zero)
            throw new InvalidOperationException($"{nameof(RabbitMqAsyncResponseOptions)}.{name} must be positive.");
    }
}
