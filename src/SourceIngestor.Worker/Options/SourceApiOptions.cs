namespace SourceIngestor.Worker.Options;

public sealed class SourceApiOptions
{
    public string PostsUrl { get; set; } = "";

    // Chunk size for publishing batches to Pulsar (and later Mongo docs).
    // MUST be configured from appsettings.json 
    public int MaxItemsPerBatch { get; set; }
}