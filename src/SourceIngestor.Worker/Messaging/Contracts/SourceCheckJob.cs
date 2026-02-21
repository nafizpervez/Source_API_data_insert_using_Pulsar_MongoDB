namespace SourceIngestor.Worker.Messaging.Contracts;

// JobId is a Mongo ObjectId string that stays the same across retries.
// Attempt meanings:
// Attempt 0 => try 1 (immediate)
// Attempt 1 => try 2 (+1m)
// Attempt 2 => try 3 (+3m)
// After Attempt 2 fails => stop + mark Failed (no more retries)
public sealed record SourceCheckJob(string JobId, string Url, int Attempt, DateTimeOffset CreatedAtUtc);