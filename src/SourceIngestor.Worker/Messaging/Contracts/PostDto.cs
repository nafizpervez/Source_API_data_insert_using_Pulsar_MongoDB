namespace SourceIngestor.Worker.Messaging.Contracts;

public sealed class PostDto
{
    public int? UserId { get; init; }
    public int? Id { get; init; }
    public string? Title { get; init; }
    public string? Body { get; init; }
}