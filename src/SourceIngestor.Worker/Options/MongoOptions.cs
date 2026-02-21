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

    // NEW: duplicate items collection (dedup output)
    public string DuplicatedCollection { get; set; } = "Duplicated_Data";

    // Destination raw snapshot collection (ArcGIS FS read)
    public string DestinationRawCollection { get; set; } = "Destination_Raw_Data";

    // NEW: Source API fetch outcome collections (same DB)
    public string SourceSuccessIdCollection { get; set; } = "Source_Success_ID";
    public string SourceFailedIdCollection { get; set; } = "Source_Failed_ID";

    public string? TimeZoneId { get; set; }
}