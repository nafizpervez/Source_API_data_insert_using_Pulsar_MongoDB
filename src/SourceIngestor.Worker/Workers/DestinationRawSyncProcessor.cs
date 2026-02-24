// src/SourceIngestor.Worker/Workers/DestinationRawSyncProcessor.cs
using System.Globalization;
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

        // Run id to correlate batch documents of the same sync cycle
        var runId = ObjectId.GenerateNewId();

        // 1) Get ALL ids first (stable and avoids transfer limits / weird paging)
        var (oidField, objectIds) = await QueryAllObjectIds(layerUrl, token, ct);

        _logger.LogInformation("Destination ids fetched. oidField={OidField} count={Count}", oidField, objectIds.Count);

        // IMPORTANT: We only touch Destination_Raw_Data collection.
        // Cleanup old batch docs for this layerUrl, keep index doc (LatestSnapshotId) intact.
        // This prevents infinite growth and ensures the collection reflects the latest sync.
        var cleanupFilter = Builders<BsonDocument>.Filter.And(
            Builders<BsonDocument>.Filter.Eq("docType", "batch"),
            Builders<BsonDocument>.Filter.Eq("layerUrl", layerUrl)
        );

        var cleanupResult = await col.DeleteManyAsync(cleanupFilter, ct);
        if (cleanupResult.DeletedCount > 0)
            _logger.LogInformation("Destination_Raw_Data cleanup removed {Count} old batch documents for layerUrl={LayerUrl}", cleanupResult.DeletedCount, layerUrl);

        // 2) Pull by objectIds in chunks; write each chunk as its own Mongo document
        var fetchedFeaturesTotal = 0;
        var chunks = 0;

        var batchDocIds = new BsonArray(); // store ObjectIds of batch docs
        var batchCounts = new List<int>();

        foreach (var chunk in Chunk(objectIds, batchSize))
        {
            chunks++;

            var pageRoot = await QueryByObjectIds(layerUrl, token, oidField, chunk, ct);

            if (!pageRoot.TryGetProperty("features", out var featuresEl) || featuresEl.ValueKind != JsonValueKind.Array)
            {
                _logger.LogWarning("Destination query returned no features array. Stopping. payload={Payload}", pageRoot.GetRawText());
                break;
            }

            var features = featuresEl.EnumerateArray().ToList();
            if (features.Count == 0)
                continue;

            var batchPayload = new BsonArray();

            foreach (var feat in features)
            {
                if (!feat.TryGetProperty("attributes", out var attrsEl) || attrsEl.ValueKind != JsonValueKind.Object)
                    continue;

                var attrsDoc = BsonDocument.Parse(attrsEl.GetRawText());

                // Convert epoch ms fields to "M/d/yyyy, hh:mm tt" in LOCAL TZ for Mongo snapshot
                ConvertEpochMsFieldToLocalText(attrsDoc, "created_date", tz);
                ConvertEpochMsFieldToLocalText(attrsDoc, "last_edited_date", tz);

                var featureDoc = new BsonDocument
                {
                    { "attributes", attrsDoc }
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

                if (attrsEl.TryGetProperty(oidField, out var idEl) && TryGetIntLike(idEl, out var idVal))
                    featureDoc["objectId"] = idVal;

                batchPayload.Add(featureDoc);
            }

            fetchedFeaturesTotal += features.Count;

            // write one Mongo doc per chunk
            var batchDocId = ObjectId.GenerateNewId();

            // Determine payload range (based on objectIds positions, for easy debugging)
            var rangeStart = ((chunks - 1) * batchSize);
            var rangeEnd = Math.Min(rangeStart + batchSize - 1, Math.Max(0, objectIds.Count - 1));

            var batchDoc = new BsonDocument
            {
                { "_id", batchDocId },
                { "docType", "batch" },
                { "runId", runId },
                { "layerUrl", layerUrl },
                { "objectIdField", oidField },
                { "fetchedAtLocalText", fetchedAtLocalText },
                { "timeZoneId", timeZoneDisplay },

                { "queryBatchSize", batchSize },
                { "idsCount", objectIds.Count },

                { "batchNo", chunks },
                { "batchRange", new BsonDocument { { "startIndex", rangeStart }, { "endIndex", rangeEnd } } },
                { "requestedObjectIdCount", chunk.Count },
                { "featuresFetched", features.Count },
                { "itemCount", batchPayload.Count },

                { "payload", batchPayload }
            };

            await col.InsertOneAsync(batchDoc, cancellationToken: ct);

            batchDocIds.Add(batchDocId);
            batchCounts.Add(batchPayload.Count);

            _logger.LogInformation(
                "Destination batch stored. batchNo={BatchNo} requestedIds={Requested} received={Received} payloadCount={PayloadCount} mongo={Db}.{Col} _id={Id}",
                chunks, chunk.Count, features.Count, batchPayload.Count, _mongo.Database, _mongo.DestinationRawCollection, batchDocId);
        }

        // 3) Write/replace index doc (destination_raw_latest) WITHOUT payload
        // This lets other components find the latest run and locate all batch docs.
        var indexEnvelope = new BsonDocument
        {
            { "_id", LatestSnapshotId },
            { "docType", "index" },
            { "status", "ok" },

            { "runId", runId },
            { "layerUrl", layerUrl },
            { "objectIdField", oidField },
            { "fetchedAtLocalText", fetchedAtLocalText },
            { "timeZoneId", timeZoneDisplay },

            { "queryBatchSize", batchSize },
            { "idsCount", objectIds.Count },
            { "featuresFetched", fetchedFeaturesTotal },

            { "batchCount", batchDocIds.Count },
            { "batchDocIds", batchDocIds },
            { "batchItemCounts", new BsonArray(batchCounts) },

            // keep the same field name for backwards visibility (but NOT storing everything here)
            { "itemCount", fetchedFeaturesTotal }
        };

        await col.ReplaceOneAsync(
            filter: Builders<BsonDocument>.Filter.Eq("_id", LatestSnapshotId),
            replacement: indexEnvelope,
            options: new ReplaceOptions { IsUpsert = true },
            cancellationToken: ct);

        _logger.LogInformation(
            "Destination index updated. idsCount={Ids} featuresFetched={Fetched} batches={Batches} mongo={Db}.{Col} _id={Id}",
            objectIds.Count, fetchedFeaturesTotal, batchDocIds.Count, _mongo.Database, _mongo.DestinationRawCollection, LatestSnapshotId);
    }

    private async Task<(string oidField, List<int> objectIds)> QueryAllObjectIds(string layerUrl, string token, CancellationToken ct)
    {
        var http = _httpFactory.CreateClient("arcgis");

        var qs = new Dictionary<string, string>
        {
            ["where"] = _dest.Where,
            ["returnIdsOnly"] = "true",
            ["f"] = "json",
            ["token"] = token
        };

        var url = $"{layerUrl}/query?{ToQueryString(qs)}";

        using var req = new HttpRequestMessage(HttpMethod.Get, url);
        ApplyRefererHeader(req);

        using var res = await http.SendAsync(req, ct);
        var raw = await res.Content.ReadAsStringAsync(ct);

        if (!res.IsSuccessStatusCode)
            throw new InvalidOperationException($"Destination returnIdsOnly failed. status={(int)res.StatusCode} body={raw}");

        using var doc = JsonDocument.Parse(raw);
        var root = doc.RootElement;

        if (root.TryGetProperty("error", out var err))
            throw new InvalidOperationException($"Destination returnIdsOnly error: {err.GetRawText()}");

        var oidField = "OBJECTID";
        if (root.TryGetProperty("objectIdFieldName", out var oidEl) && oidEl.ValueKind == JsonValueKind.String)
            oidField = oidEl.GetString() ?? "OBJECTID";

        var ids = new List<int>();

        if (root.TryGetProperty("objectIds", out var idsEl) && idsEl.ValueKind == JsonValueKind.Array)
        {
            foreach (var el in idsEl.EnumerateArray())
            {
                if (TryGetIntLike(el, out var v))
                    ids.Add(v);
            }
        }

        ids.Sort();
        return (oidField, ids);
    }

    private async Task<JsonElement> QueryByObjectIds(
        string layerUrl,
        string token,
        string oidField,
        List<int> objectIds,
        CancellationToken ct)
    {
        var http = _httpFactory.CreateClient("arcgis");

        var qs = new Dictionary<string, string>
        {
            ["where"] = _dest.Where,
            ["objectIds"] = string.Join(",", objectIds),
            ["outFields"] = _dest.OutFields,
            ["returnGeometry"] = _dest.ReturnGeometry ? "true" : "false",
            ["orderByFields"] = oidField,
            ["f"] = "json",
            ["token"] = token
        };

        var url = $"{layerUrl}/query?{ToQueryString(qs)}";

        using var req = new HttpRequestMessage(HttpMethod.Get, url);
        ApplyRefererHeader(req);

        using var res = await http.SendAsync(req, ct);
        var raw = await res.Content.ReadAsStringAsync(ct);

        if (!res.IsSuccessStatusCode)
            throw new InvalidOperationException($"Destination /query by objectIds failed. status={(int)res.StatusCode} body={raw}");

        using var doc = JsonDocument.Parse(raw);
        var root = doc.RootElement;

        if (root.TryGetProperty("error", out var err))
            throw new InvalidOperationException($"Destination /query by objectIds error: {err.GetRawText()}");

        return root.Clone();
    }

    private async Task<string> GetPortalToken(CancellationToken ct)
    {
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

    private static IEnumerable<List<int>> Chunk(List<int> source, int size)
    {
        if (size <= 0) size = 1;

        for (var i = 0; i < source.Count; i += size)
        {
            var take = Math.Min(size, source.Count - i);
            yield return source.GetRange(i, take);
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

    private static void ConvertEpochMsFieldToLocalText(BsonDocument attrs, string fieldName, TimeZoneInfo tz)
    {
        if (attrs is null || string.IsNullOrWhiteSpace(fieldName))
            return;

        // case-insensitive find
        string? actualName = null;
        BsonValue val = BsonNull.Value;

        foreach (var e in attrs.Elements)
        {
            if (string.Equals(e.Name, fieldName, StringComparison.OrdinalIgnoreCase))
            {
                actualName = e.Name;
                val = e.Value;
                break;
            }
        }

        if (actualName is null)
            return;

        // Already a nice formatted string? keep it.
        if (val.BsonType == BsonType.String)
        {
            var s = (val.AsString ?? "").Trim();
            if (s.Length == 0) return;

            // If it's numeric string, treat as epoch-ms and convert
            if (long.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out var msStr))
            {
                attrs[actualName] = FormatEpochMs(msStr, tz);
            }

            return;
        }

        if (!TryReadEpochMs(val, out var ms))
            return;

        attrs[actualName] = FormatEpochMs(ms, tz);
    }

    private static bool TryReadEpochMs(BsonValue v, out long ms)
    {
        ms = 0;

        try
        {
            return v.BsonType switch
            {
                BsonType.Int64 => (ms = v.AsInt64) >= 0,
                BsonType.Int32 => (ms = v.AsInt32) >= 0,
                BsonType.Double => (ms = Convert.ToInt64(v.AsDouble)) >= 0,
                BsonType.Decimal128 => (ms = (long)Decimal128.ToDecimal(v.AsDecimal128)) >= 0,
                BsonType.String when long.TryParse(v.AsString, NumberStyles.Integer, CultureInfo.InvariantCulture, out var s)
                    => (ms = s) >= 0,
                _ => false
            };
        }
        catch
        {
            return false;
        }
    }

    private static string FormatEpochMs(long ms, TimeZoneInfo tz)
    {
        try
        {
            var utc = DateTimeOffset.FromUnixTimeMilliseconds(ms).UtcDateTime;
            var local = TimeZoneInfo.ConvertTimeFromUtc(utc, tz);
            return local.ToString("M/d/yyyy, hh:mm tt", CultureInfo.InvariantCulture);
        }
        catch
        {
            return "";
        }
    }
}