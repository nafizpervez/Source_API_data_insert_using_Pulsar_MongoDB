// src/SourceIngestor.Worker/Options/DestinationApiOptions.cs
namespace SourceIngestor.Worker.Options;

public sealed class DestinationApiOptions
{
    public string FeatureLayerUrl { get; set; } = "";

    public string Where { get; set; } = "1=1";
    public string OutFields { get; set; } = "*";
    public bool ReturnGeometry { get; set; } = false;

    // Query in batches of OBJECTIDs to avoid maxRecordCount limits
    public int QueryBatchSize { get; set; }

    // Run interval for syncing destination raw data / detection loops
    public int SyncIntervalSeconds { get; set; }

    // ------------------------------------------------------------
    // DestinationCheckProcessor gating controls (race-condition fix)
    // ------------------------------------------------------------

    // How often DestinationCheckProcessor polls Mongo snapshots during gating.
    // Keep this small; it’s cheap Mongo reads.
    public int GatePollSeconds { get; set; } = 5;

    // Maximum time DestinationCheckProcessor will wait for diff snapshots
    // (Update/Delete/Insert/Skip) to be status="ok" before treating as failure.
    // This prevents infinite hang if a worker is broken.
    public int GateMaxWaitSeconds { get; set; } = 300; // 5 minutes default
}