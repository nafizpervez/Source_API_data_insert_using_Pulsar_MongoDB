namespace SourceIngestor.Worker.Options;

public sealed class SourceApiOptions
{
    // Backward compatible single source.
    public string PostsUrl { get; set; } = "";

    // Future: multiple sources. If any entry is "" or null/whitespace, it will be skipped.
    public string[]? PostsUrls { get; set; }

    // Chunk size for publishing batches to Pulsar (and later Mongo docs).
    // MUST be configured from appsettings.json 
    public int MaxItemsPerBatch { get; set; }
}