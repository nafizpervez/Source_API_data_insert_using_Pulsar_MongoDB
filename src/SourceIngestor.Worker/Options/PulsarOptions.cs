namespace SourceIngestor.Worker.Options;

public sealed class PulsarOptions
{
    public string ServiceUrl { get; set; } = "";

    public string SourceCheckTopic { get; set; } = "";
    public string PostsBatchTopic { get; set; } = "";

    // Reuse this DLQ for destination-check failures too
    public string DlqTopic { get; set; } = "";

    public string Subscription { get; set; } = "";

    // NEW: destination-check topic
    public string DestinationCheckTopic { get; set; } = "";
}