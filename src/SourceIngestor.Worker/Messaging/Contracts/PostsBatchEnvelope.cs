using System.Text.Json;

namespace SourceIngestor.Worker.Messaging.Contracts;

public sealed class PostsBatchEnvelope
{
    // Where the data came from
    public string SourceUrl { get; init; } = string.Empty;

    // When the batch was fetched
    public DateTimeOffset FetchedAtUtc { get; init; }

    // The raw JSON array text: e.g. "[{...},{...}]"
    // This is the canonical value used by MongoWriter to insert into MongoDB
    // without forcing any schema/type coercion.
    public string PayloadRawJson { get; init; } = "[]";

    // Simple "fieldName -> jsonType" map, e.g. "userId" -> "number"
    public Dictionary<string, string> TypeMap { get; init; } = new();

    // Raw JSON payload as JsonElement (kept for flexibility / future use).
    // MongoWriter currently uses PayloadRawJson for BSON insert.
    public JsonElement payload { get; init; }
}