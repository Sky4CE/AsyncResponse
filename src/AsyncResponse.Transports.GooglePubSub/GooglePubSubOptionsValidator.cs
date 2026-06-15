namespace AsyncResponse.Transports.GooglePubSub;

internal static class GooglePubSubOptionsValidator
{
    public static string Required(string? value, string name)
        => !string.IsNullOrWhiteSpace(value)
            ? value
            : throw new InvalidOperationException($"{nameof(GooglePubSubAsyncResponseOptions)}.{name} must be configured.");
}
