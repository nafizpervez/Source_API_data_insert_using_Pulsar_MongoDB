using System.Text.Json;

namespace SourceIngestor.Worker.Messaging.Contracts;

public sealed class PostsBatchEnvelope
{
    // Correlation Id across Pulsar + Mongo
    // This is a Mongo ObjectId string and will be used as _id in Source_Data.
    public string JobId { get; init; } = string.Empty;

    public string SourceUrl { get; init; } = string.Empty;

    public DateTimeOffset FetchedAtUtc { get; init; }

    public string PayloadRawJson { get; init; } = "[]";

    public Dictionary<string, string> TypeMap { get; init; } = new();

    public JsonElement payload { get; init; }
}