namespace SourceIngestor.Worker.Messaging.Contracts;

public sealed class PostsBatchEnvelope
{
    // Where the data came from
    public string SourceUrl { get; init; } = string.Empty;

    // When the batch was fetched
    public DateTimeOffset FetchedAtUtc { get; init; }

    // The raw JSON array text: e.g. "[{...},{...}]"
    public string PayloadRawJson { get; init; } = "[]";

    // Simple "fieldName -> jsonType" map, e.g. "userId" -> "number"
    public Dictionary<string, string> TypeMap { get; init; } = new();

    // Optional: strongly-typed payload (keep if you want, not required by MongoWriter)
    public List<PostDto> Payload { get; init; } = new();
}