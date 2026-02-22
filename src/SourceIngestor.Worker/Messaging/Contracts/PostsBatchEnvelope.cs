using System.Text.Json;

namespace SourceIngestor.Worker.Messaging.Contracts;

public sealed class PostsBatchEnvelope
{
    // JobId is the correlation id across retries for the same source check job.
    // This is NOT used as Source_Data _id anymore when chunking into multiple docs.
    public string JobId { get; init; } = string.Empty;

    // BatchId is a Mongo ObjectId string and WILL be used as _id in Source_Data + Source_Success_ID.
    public string BatchId { get; init; } = string.Empty;

    public int PartNo { get; init; }

    public int TotalParts { get; init; }

    public string SourceUrl { get; init; } = string.Empty;

    public DateTimeOffset FetchedAtUtc { get; init; }

    public string PayloadRawJson { get; init; } = "[]";

    public Dictionary<string, string> TypeMap { get; init; } = new();

    public JsonElement payload { get; init; }
}