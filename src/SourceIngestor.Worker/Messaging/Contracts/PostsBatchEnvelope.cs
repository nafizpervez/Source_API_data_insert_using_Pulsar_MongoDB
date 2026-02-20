namespace SourceIngestor.Worker.Messaging.Contracts;

public sealed record PostsBatchEnvelope(
    string SourceUrl,
    DateTimeOffset FetchedAtUtc,
    string PayloadRawJson,
    Dictionary<string, string> TypeMap
);