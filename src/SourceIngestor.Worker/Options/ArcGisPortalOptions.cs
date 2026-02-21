namespace SourceIngestor.Worker.Options;

public sealed class ArcGisPortalOptions
{
    public string GenerateTokenUrl { get; set; } = "";
    public string Username { get; set; } = "";
    public string Password { get; set; } = "";

    // ArcGIS token params
    // client: "requestip" OR "referer"
    public string Client { get; set; } = "requestip";
    public string? Referer { get; set; } = "";

    // Token lifetime requested from portal (minutes)
    public int ExpirationMinutes { get; set; } = 60;
}