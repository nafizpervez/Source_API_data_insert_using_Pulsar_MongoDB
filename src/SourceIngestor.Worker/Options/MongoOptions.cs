namespace SourceIngestor.Worker.Options;

public sealed class MongoOptions
{
    public string ConnectionString { get; set; } = "";
    public string Database { get; set; } = "";

    // Source collection (raw batches)
    public string Collection { get; set; } = "Source_Data";

    // Source validation outputs (single snapshot docs)
    public string ValidatedCollection { get; set; } = "Valid_Source_Data";
    public string InvalidCollection { get; set; } = "Invalid_Data";
    public string DuplicatedCollection { get; set; } = "Duplicate_Data";

    // Destination snapshots (single snapshot docs)
    public string DestinationCollection { get; set; } = "Destination_Data";
    public string ValidDestinationCollection { get; set; } = "Valid_Destination_Data";

    // Diff outputs (single snapshot docs)
    public string UpdateCollection { get; set; } = "Update_Data";
    public string SkipCollection { get; set; } = "Skip_Data";
    public string DeleteCollection { get; set; } = "Delete_Data";
    public string InsertCollection { get; set; } = "Insert_Data";

    // Final destination snapshot after edits
    public string FinalDestinationCollection { get; set; } = "Final_Destination_Data";

    // Count summary
    public string CountSummaryCollection { get; set; } = "Count_Summary";

    // Source API fetch outcome collections
    public string SourceSuccessIdCollection { get; set; } = "Source_Success_ID";
    public string SourceFailedIdCollection { get; set; } = "Source_Failed_ID";

    // Destination apply outcome collections
    public string DestinationSuccessIdCollection { get; set; } = "Destination_Success_ID";
    public string DestinationFailedIdCollection { get; set; } = "Destination_Failed_ID";

    public string? TimeZoneId { get; set; }
}