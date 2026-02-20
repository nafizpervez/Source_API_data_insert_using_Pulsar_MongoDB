namespace SourceIngestor.Worker.Options;

public sealed class MongoOptions
{
    public string ConnectionString { get; set; } = "";
    public string Database { get; set; } = "";

    // Source collection (raw batches)
    public string Collection { get; set; } = "";

    // Target collection (validated/typed batches)
    public string ValidatedCollection { get; set; } = "ValidatedSourceData";

    public string? TimeZoneId { get; set; }
}