namespace SourceIngestor.Worker.Messaging.Contracts;

public sealed record SourceCheckJob(string Url, int Attempt, DateTimeOffset CreatedAtUtc);