namespace SourceIngestor.Worker.Options;

public sealed class DestinationApiOptions
{
    public string FeatureLayerUrl { get; set; } = "";

    public string Where { get; set; } = "1=1";
    public string OutFields { get; set; } = "*";
    public bool ReturnGeometry { get; set; } = false;

    // Query in batches of OBJECTIDs to avoid maxRecordCount limits
    public int QueryBatchSize { get; set; } = 500;

    // Run interval for syncing destination raw data
    public int SyncIntervalSeconds { get; set; } = 600;
}