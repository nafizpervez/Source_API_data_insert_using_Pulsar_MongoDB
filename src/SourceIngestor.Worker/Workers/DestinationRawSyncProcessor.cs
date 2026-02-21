using System.Globalization;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;
using SourceIngestor.Worker.Options;

namespace SourceIngestor.Worker.Workers;

public sealed class DestinationRawSyncProcessor : BackgroundService
{
    private const string LatestSnapshotId = "destination_raw_latest";

    private readonly IHttpClientFactory _httpFactory;
    private readonly IMongoClient _mongoClient;

    private readonly MongoOptions _mongo;
    private readonly ArcGisPortalOptions _portal;
    private readonly DestinationApiOptions _dest;

    private readonly ILogger<DestinationRawSyncProcessor> _logger;

    // token cache
    private string? _token;
    private DateTimeOffset _tokenExpiresAtUtc = DateTimeOffset.MinValue;

    public DestinationRawSyncProcessor(
        IHttpClientFactory httpFactory,
        IMongoClient mongoClient,
        IOptions<MongoOptions> mongo,
        IOptions<ArcGisPortalOptions> portal,
        IOptions<DestinationApiOptions> dest,
        ILogger<DestinationRawSyncProcessor> logger)
    {
        _httpFactory = httpFactory;
        _mongoClient = mongoClient;
        _mongo = mongo.Value;
        _portal = portal.Value;
        _dest = dest.Value;
        _logger = logger;
    }

    private void ApplyRefererHeader(HttpRequestMessage req)
    {
        if (!string.Equals(_portal.Client, "referer", StringComparison.OrdinalIgnoreCase))
            return;

        var referer = _portal.Referer;
        if (string.IsNullOrWhiteSpace(referer))
            return;

        if (Uri.TryCreate(referer, UriKind.Absolute, out var uri))
            req.Headers.Referrer = uri;

        req.Headers.TryAddWithoutValidation("Referer", referer);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("DestinationRawSyncProcessor started.");
        _logger.LogInformation("Destination layer: {Url}", _dest.FeatureLayerUrl);
        _logger.LogInformation("Mongo target: {Db}.{Col}", _mongo.Database, _mongo.DestinationRawCollection);

        var interval = TimeSpan.FromSeconds(Math.Max(10, _dest.SyncIntervalSeconds));

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await SyncOnce(stoppingToken);
            }
            catch (TaskCanceledException)
            {
                // shutdown
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "DestinationRawSyncProcessor SyncOnce failed.");
            }

            await Task.Delay(interval, stoppingToken);
        }
    }

    private async Task SyncOnce(CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(_portal.GenerateTokenUrl))
            throw new InvalidOperationException("ArcGisPortal.GenerateTokenUrl is empty.");

        if (string.IsNullOrWhiteSpace(_dest.FeatureLayerUrl))
            throw new InvalidOperationException("DestinationApi.FeatureLayerUrl is empty.");

        var token = await GetPortalToken(ct);

        var layerUrl = _dest.FeatureLayerUrl.TrimEnd('/');
        var db = _mongoClient.GetDatabase(_mongo.Database);
        var col = db.GetCollection<BsonDocument>(_mongo.DestinationRawCollection);

        // NOTE: No more DeleteMany/InsertMany per feature.
        // We keep ONE snapshot document and replace it each run.

        var tz = ResolveTimeZone(_mongo.TimeZoneId);

        var fetchedUtc = DateTime.UtcNow;
        var fetchedLocal = TimeZoneInfo.ConvertTimeFromUtc(fetchedUtc, tz);
        var fetchedAtLocalText = fetchedLocal.ToString("M/d/yyyy, hh:mm tt", CultureInfo.InvariantCulture);

        var offset = tz.GetUtcOffset(fetchedUtc);
        var sign = offset >= TimeSpan.Zero ? "+" : "-";
        var abs = offset.Duration();
        var offsetText = $" {sign}{abs:hh\\:mm}";
        var timeZoneDisplay = $"{tz.Id} UTC{offsetText}";

        var batchSize = Math.Max(1, _dest.QueryBatchSize);

        // Accumulate ALL features into one payload array (single batch document)
        var payload = new BsonArray();
        var resultOffset = 0;

        string oidField = "OBJECTID";
        var oidResolved = false;

        while (!ct.IsCancellationRequested)
        {
            var pageRoot = await QueryDestinationPage(layerUrl, token, resultOffset, batchSize, ct);

            // Learn OID field name once (nice-to-have metadata inside the batch doc)
            if (!oidResolved &&
                pageRoot.TryGetProperty("objectIdFieldName", out var oidEl) &&
                oidEl.ValueKind == JsonValueKind.String)
            {
                oidField = oidEl.GetString() ?? "OBJECTID";
                oidResolved = true;
            }

            if (!pageRoot.TryGetProperty("features", out var featuresEl) || featuresEl.ValueKind != JsonValueKind.Array)
            {
                _logger.LogWarning("Destination query returned no features array. Stopping. payload={Payload}", pageRoot.GetRawText());
                break;
            }

            var features = featuresEl.EnumerateArray().ToList();
            if (features.Count == 0)
                break;

            // Convert each feature into a compact BsonDocument and append to payload
            // IMPORTANT: do NOT include per-feature _id here; this is one batch document.
            foreach (var feat in features)
            {
                // Expect ArcGIS feature JSON shape: { attributes: {...}, geometry: {...}? }
                if (!feat.TryGetProperty("attributes", out var attrsEl) || attrsEl.ValueKind != JsonValueKind.Object)
                    continue;

                var featureDoc = new BsonDocument
                {
                    { "attributes", BsonDocument.Parse(attrsEl.GetRawText()) }
                };

                if (_dest.ReturnGeometry &&
                    feat.TryGetProperty("geometry", out var geomEl) &&
                    geomEl.ValueKind == JsonValueKind.Object)
                {
                    featureDoc["geometry"] = BsonDocument.Parse(geomEl.GetRawText());
                }
                else
                {
                    featureDoc["geometry"] = BsonNull.Value;
                }

                // Keep the objectId value in the feature doc if present (helps later diffing)
                if (attrsEl.TryGetProperty(oidField, out var idEl) && TryGetIntLike(idEl, out var idVal))
                    featureDoc["objectId"] = idVal;

                payload.Add(featureDoc);
            }

            _logger.LogInformation(
                "Destination page fetched. offset={Offset} requested={Requested} received={Received} accumulated={Total}",
                resultOffset, batchSize, features.Count, payload.Count);

            // Pagination
            resultOffset += features.Count;

            // Stop when ArcGIS says no more pages
            var exceeded = pageRoot.TryGetProperty("exceededTransferLimit", out var exEl) &&
                           exEl.ValueKind == JsonValueKind.True;

            if (!exceeded && features.Count < batchSize)
                break;
        }

        // Build one single envelope document (same pattern as Source_Data)
        var envelope = new BsonDocument
        {
            { "_id", LatestSnapshotId }, // fixed id: always 1 doc
            { "layerUrl", layerUrl },
            { "objectIdField", oidField },
            { "fetchedAtLocalText", fetchedAtLocalText },
            { "timeZoneId", timeZoneDisplay },
            // { "where", _dest.Where },
            // { "outFields", _dest.OutFields },
            // { "returnGeometry", _dest.ReturnGeometry },
            { "batchSize", batchSize },
            { "itemCount", payload.Count },
            { "payload", payload }
        };

        // Replace snapshot atomically (upsert)
        await col.ReplaceOneAsync(
            filter: Builders<BsonDocument>.Filter.Eq("_id", LatestSnapshotId),
            replacement: envelope,
            options: new ReplaceOptions { IsUpsert = true },
            cancellationToken: ct);

        _logger.LogInformation(
            "Destination snapshot updated. itemCount={Count} mongo={Db}.{Col} _id={Id}",
            payload.Count, _mongo.Database, _mongo.DestinationRawCollection, LatestSnapshotId);
    }

    private async Task<JsonElement> QueryDestinationPage(
        string layerUrl,
        string token,
        int resultOffset,
        int resultRecordCount,
        CancellationToken ct)
    {
        var http = _httpFactory.CreateClient("arcgis");

        var qs = new Dictionary<string, string>
        {
            ["where"] = _dest.Where,
            ["outFields"] = _dest.OutFields,
            ["returnGeometry"] = _dest.ReturnGeometry ? "true" : "false",
            ["f"] = "json",
            ["token"] = token,
            ["resultOffset"] = resultOffset.ToString(CultureInfo.InvariantCulture),
            ["resultRecordCount"] = resultRecordCount.ToString(CultureInfo.InvariantCulture)
        };

        var url = $"{layerUrl}/query?{ToQueryString(qs)}";

        using var req = new HttpRequestMessage(HttpMethod.Get, url);
        ApplyRefererHeader(req);

        using var res = await http.SendAsync(req, ct);
        var raw = await res.Content.ReadAsStringAsync(ct);

        if (!res.IsSuccessStatusCode)
            throw new InvalidOperationException($"Destination /query failed. status={(int)res.StatusCode} body={raw}");

        using var doc = JsonDocument.Parse(raw);
        var root = doc.RootElement;

        if (root.TryGetProperty("error", out var err))
            throw new InvalidOperationException($"Destination /query error: {err.GetRawText()}");

        return root.Clone();
    }

    private async Task<string> GetPortalToken(CancellationToken ct)
    {
        // refresh token 2 minutes before expiry
        if (!string.IsNullOrWhiteSpace(_token) && _tokenExpiresAtUtc > DateTimeOffset.UtcNow.AddMinutes(2))
            return _token!;

        if (string.IsNullOrWhiteSpace(_portal.Username) || string.IsNullOrWhiteSpace(_portal.Password))
            throw new InvalidOperationException("ArcGisPortal.Username/Password must be set in appsettings.json.");

        var http = _httpFactory.CreateClient("arcgis");

        var form = new Dictionary<string, string>
        {
            ["f"] = "json",
            ["username"] = _portal.Username,
            ["password"] = _portal.Password,
            ["client"] = string.IsNullOrWhiteSpace(_portal.Client) ? "requestip" : _portal.Client,
            ["expiration"] = Math.Max(1, _portal.ExpirationMinutes).ToString(CultureInfo.InvariantCulture)
        };

        if (string.Equals(form["client"], "referer", StringComparison.OrdinalIgnoreCase))
            form["referer"] = _portal.Referer ?? "";

        using var req = new HttpRequestMessage(HttpMethod.Post, _portal.GenerateTokenUrl)
        {
            Content = new FormUrlEncodedContent(form)
        };

        ApplyRefererHeader(req);

        using var res = await http.SendAsync(req, ct);
        var raw = await res.Content.ReadAsStringAsync(ct);

        if (!res.IsSuccessStatusCode)
            throw new InvalidOperationException($"generateToken failed. status={(int)res.StatusCode} body={raw}");

        using var doc = JsonDocument.Parse(raw);
        var root = doc.RootElement;

        if (root.TryGetProperty("error", out var err))
            throw new InvalidOperationException($"generateToken error: {err.GetRawText()}");

        if (!root.TryGetProperty("token", out var tokenEl) || tokenEl.ValueKind != JsonValueKind.String)
            throw new InvalidOperationException($"generateToken missing token. body={raw}");

        var token = tokenEl.GetString()!;

        DateTimeOffset expiresAtUtc = DateTimeOffset.UtcNow.AddMinutes(Math.Max(1, _portal.ExpirationMinutes));
        if (root.TryGetProperty("expires", out var expEl) && expEl.ValueKind == JsonValueKind.Number)
        {
            if (expEl.TryGetInt64(out var ms))
                expiresAtUtc = DateTimeOffset.FromUnixTimeMilliseconds(ms);
        }

        _token = token;
        _tokenExpiresAtUtc = expiresAtUtc;

        _logger.LogInformation("Portal token acquired. expiresAtUtc={Expires}", _tokenExpiresAtUtc);
        return token;
    }

    private static bool TryGetIntLike(JsonElement el, out int value)
    {
        value = default;

        try
        {
            return el.ValueKind switch
            {
                JsonValueKind.Number when el.TryGetInt32(out var i) => (value = i) == i,
                JsonValueKind.Number when el.TryGetInt64(out var l) && l >= int.MinValue && l <= int.MaxValue
                    => (value = (int)l) == (int)l,
                JsonValueKind.String when int.TryParse(el.GetString(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var s)
                    => (value = s) == s,
                _ => false
            };
        }
        catch
        {
            return false;
        }
    }

    private static string ToQueryString(Dictionary<string, string> qs)
    {
        var sb = new StringBuilder();
        foreach (var (k, v) in qs)
        {
            if (sb.Length > 0) sb.Append('&');
            sb.Append(Uri.EscapeDataString(k));
            sb.Append('=');
            sb.Append(Uri.EscapeDataString(v ?? ""));
        }
        return sb.ToString();
    }

    private static TimeZoneInfo ResolveTimeZone(string? timeZoneId)
    {
        if (string.IsNullOrWhiteSpace(timeZoneId))
            return TimeZoneInfo.Local;

        try
        {
            return TimeZoneInfo.FindSystemTimeZoneById(timeZoneId);
        }
        catch
        {
            var map = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["Asia/Dhaka"] = "Bangladesh Standard Time",
                ["Asia/Kuala_Lumpur"] = "Singapore Standard Time",
                ["Asia/Singapore"] = "Singapore Standard Time"
            };

            if (map.TryGetValue(timeZoneId, out var windowsId))
            {
                try { return TimeZoneInfo.FindSystemTimeZoneById(windowsId); }
                catch { }
            }

            return TimeZoneInfo.Local;
        }
    }
}