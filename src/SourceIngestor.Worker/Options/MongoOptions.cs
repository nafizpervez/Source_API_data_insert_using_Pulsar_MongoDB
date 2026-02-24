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

    // duplicate items collection (dedup output)
    public string DuplicatedCollection { get; set; } = "Duplicated_Data";

    // Destination raw snapshot collection (ArcGIS FS read)
    public string DestinationRawCollection { get; set; } = "Destination_Raw_Data";

    // update items snapshot collection (diff output)
    public string UpdateCollection { get; set; } = "Update_Data";

    // skip items snapshot collection (exact match output)
    public string SkipCollection { get; set; } = "Skip_Data";

    // delete items snapshot collection (exists in destination but NOT in source)
    public string DeleteCollection { get; set; } = "Delete_Data";

    // NEW: insert items snapshot collection (exists in source but NOT in destination)
    public string InsertCollection { get; set; } = "Insert_Data";

    // Source API fetch outcome collections (same DB)
    public string SourceSuccessIdCollection { get; set; } = "Source_Success_ID";
    public string SourceFailedIdCollection { get; set; } = "Source_Failed_ID";

    public string? TimeZoneId { get; set; }
}