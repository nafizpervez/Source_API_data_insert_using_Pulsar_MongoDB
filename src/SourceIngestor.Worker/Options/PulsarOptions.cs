namespace SourceIngestor.Worker.Options;

public sealed class PulsarOptions
{
    public string ServiceUrl { get; set; } = "";
    public string SourceCheckTopic { get; set; } = "";
    public string PostsBatchTopic { get; set; } = "";
    public string DlqTopic { get; set; } = "";
    public string Subscription { get; set; } = "";
}