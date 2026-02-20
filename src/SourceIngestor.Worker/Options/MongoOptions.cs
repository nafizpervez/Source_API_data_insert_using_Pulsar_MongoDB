namespace SourceIngestor.Worker.Options;

public sealed class MongoOptions
{
    public string ConnectionString { get; set; } = "";
    public string Database { get; set; } = "";

    // Source collection (raw batches)
    public string Collection { get; set; } = "";

    // Target collection (validated/typed batches)
    public string ValidatedCollection { get; set; } = "ValidatedSourceData";

    // Target collection (invalid items from the batch)
    public string InvalidCollection { get; set; } = "InvalidSourceData";

    public string? TimeZoneId { get; set; }
}