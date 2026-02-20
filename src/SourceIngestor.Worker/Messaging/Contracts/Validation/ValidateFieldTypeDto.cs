namespace SourceIngestor.Worker.Messaging.Contracts.Validation
{
    // Used for validation workflows (typed expectations), not used in ingestion pipeline.
    public sealed class ValidateFieldTypeDto
    {
        public int UserId { get; init; }
        public int Id { get; init; }
        public string Title { get; init; } = "";
        public string? Body { get; init; }
    }
}