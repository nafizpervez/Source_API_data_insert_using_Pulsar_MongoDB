using System.Globalization;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;
using SourceIngestor.Worker.Options;

namespace SourceIngestor.Worker.Workers;

public sealed class InsertDetectionProcessor : BackgroundService
{
    private const string ValidLatestId = "valid_latest";
    private const string UpdateLatestId = "update_latest";
    private const string SkipLatestId = "skip_latest";
    private const string InsertLatestId = "insert_latest";

    private readonly IHttpClientFactory _httpFactory;
    private readonly IMongoClient _mongoClient;

    private readonly MongoOptions _mongo;
    private readonly ArcGisPortalOptions _portal;
    private readonly DestinationApiOptions _dest;
    private readonly DuplicationOptions _dup;

    private readonly ILogger<InsertDetectionProcessor> _logger;

    // token cache
    private string? _token;
    private DateTimeOffset _tokenExpiresAtUtc = DateTimeOffset.MinValue;

    public InsertDetectionProcessor(
        IHttpClientFactory httpFactory,
        IMongoClient mongoClient,
        IOptions<MongoOptions> mongo,
        IOptions<ArcGisPortalOptions> portal,
        IOptions<DestinationApiOptions> dest,
        IOptions<DuplicationOptions> dup,
        ILogger<InsertDetectionProcessor> logger)
    {
        _httpFactory = httpFactory;
        _mongoClient = mongoClient;
        _mongo = mongo.Value;
        _portal = portal.Value;
        _dest = dest.Value;
        _dup = dup.Value;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("InsertDetectionProcessor started.");
        _logger.LogInformation("Valid snapshot: {Col} _id={Id}", _mongo.ValidatedCollection, ValidLatestId);
        _logger.LogInformation("Insert snapshot: {Col} _id={Id}", _mongo.InsertCollection, InsertLatestId);
        _logger.LogInformation("Destination endpoint (LIVE): {Url}", _dest.FeatureLayerUrl);

        var interval = TimeSpan.FromSeconds(Math.Max(10, _dest.SyncIntervalSeconds));

        while (!stoppingToken.IsCancellationRequested)
        {
            try { await DetectOnce(stoppingToken); }
            catch (TaskCanceledException) { }
            catch (Exception ex) { _logger.LogError(ex, "InsertDetectionProcessor DetectOnce failed."); }

            await Task.Delay(interval, stoppingToken);
        }
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

    private async Task DetectOnce(CancellationToken ct)
    {
        var db = _mongoClient.GetDatabase(_mongo.Database);

        var validCol = db.GetCollection<BsonDocument>(_mongo.ValidatedCollection);
        var updateCol = db.GetCollection<BsonDocument>(_mongo.UpdateCollection);
        var skipCol = db.GetCollection<BsonDocument>(_mongo.SkipCollection);
        var insertCol = db.GetCollection<BsonDocument>(_mongo.InsertCollection);

        var validSnap = await validCol.Find(Builders<BsonDocument>.Filter.Eq("_id", ValidLatestId)).FirstOrDefaultAsync(ct);

        if (validSnap is null)
        {
            await WriteWaiting(insertCol, "waiting_for_valid_latest", ct);
            return;
        }

        if (!validSnap.TryGetValue("payload", out var validPayloadVal) || validPayloadVal.BsonType != BsonType.Array)
        {
            await WriteWaiting(insertCol, "waiting_for_valid_payload", ct);
            return;
        }

        if (string.IsNullOrWhiteSpace(_portal.GenerateTokenUrl))
            throw new InvalidOperationException("ArcGisPortal.GenerateTokenUrl is empty.");

        if (string.IsNullOrWhiteSpace(_dest.FeatureLayerUrl))
            throw new InvalidOperationException("DestinationApi.FeatureLayerUrl is empty.");

        var token = await GetPortalToken(ct);
        var layerUrl = _dest.FeatureLayerUrl.TrimEnd('/');

        var tz = ResolveTimeZone(_mongo.TimeZoneId);

        // ------------------------------------------------------------
        // Build Update + Skip key sets (defensive deduction)
        // ------------------------------------------------------------
        var updateKeySet = new HashSet<string>(StringComparer.Ordinal);
        var updateSnap = await updateCol.Find(Builders<BsonDocument>.Filter.Eq("_id", UpdateLatestId)).FirstOrDefaultAsync(ct);
        if (updateSnap is not null &&
            updateSnap.TryGetValue("payload", out var updatePayloadVal) &&
            updatePayloadVal.BsonType == BsonType.Array)
        {
            foreach (var u in updatePayloadVal.AsBsonArray)
            {
                if (u.BsonType != BsonType.Document) continue;
                var ud = u.AsBsonDocument;

                var k = BuildCandidateKeyFromAnyDoc(ud);
                if (!string.IsNullOrWhiteSpace(k))
                    updateKeySet.Add(k);
            }
        }

        var skipKeySet = new HashSet<string>(StringComparer.Ordinal);
        var skipSnap = await skipCol.Find(Builders<BsonDocument>.Filter.Eq("_id", SkipLatestId)).FirstOrDefaultAsync(ct);
        if (skipSnap is not null &&
            skipSnap.TryGetValue("payload", out var skipPayloadVal) &&
            skipPayloadVal.BsonType == BsonType.Array)
        {
            foreach (var s in skipPayloadVal.AsBsonArray)
            {
                if (s.BsonType != BsonType.Document) continue;
                var sd = s.AsBsonDocument;

                var k = BuildCandidateKeyFromAnyDoc(sd);
                if (!string.IsNullOrWhiteSpace(k))
                    skipKeySet.Add(k);
            }
        }

        // ------------------------------------------------------------
        // Fetch destination keys DIRECTLY from FeatureLayer endpoint
        // (DO NOT rely on CandidateKeyFields here; destination uses user_id)
        // ------------------------------------------------------------
        var (oidField, objectIds) = await QueryAllObjectIds(layerUrl, token, ct);

        var destKeySet = new HashSet<string>(StringComparer.Ordinal);
        var destRecordsSeen = 0;
        var destDistinctKeys = 0;
        var destSkippedNoAttrs = 0;
        var destSkippedNoKey = 0;

        var batchSize = Math.Max(1, _dest.QueryBatchSize);

        foreach (var chunk in Chunk(objectIds, batchSize))
        {
            var pageRoot = await QueryByObjectIds(layerUrl, token, oidField, chunk, ct);

            if (!pageRoot.TryGetProperty("features", out var featuresEl) || featuresEl.ValueKind != JsonValueKind.Array)
                break;

            foreach (var feat in featuresEl.EnumerateArray())
            {
                destRecordsSeen++;

                if (!feat.TryGetProperty("attributes", out var attrsEl) || attrsEl.ValueKind != JsonValueKind.Object)
                {
                    destSkippedNoAttrs++;
                    continue;
                }

                var attrs = BsonDocument.Parse(attrsEl.GetRawText());

                var key = BuildCandidateKeyFromAnyDoc(attrs);
                if (string.IsNullOrWhiteSpace(key))
                {
                    destSkippedNoKey++;
                    continue;
                }

                if (destKeySet.Add(key))
                    destDistinctKeys++;
            }
        }

        // ------------------------------------------------------------
        // Insert = Valid keys NOT in destination keys
        // ------------------------------------------------------------
        var validPayload = validPayloadVal.AsBsonArray;

        var insertPayload = new BsonArray();

        var comparedValidKeys = 0;
        var validDistinctKeys = 0;
        var validSkippedNoKey = 0;
        var insertCount = 0;

        var seenValidKeys = new HashSet<string>(StringComparer.Ordinal);

        foreach (var v in validPayload)
        {
            if (v.BsonType != BsonType.Document)
                continue;

            var validDoc = v.AsBsonDocument;

            var key = BuildCandidateKeyFromAnyDoc(validDoc);
            if (string.IsNullOrWhiteSpace(key))
            {
                validSkippedNoKey++;
                continue;
            }

            if (!seenValidKeys.Add(key))
                continue;

            validDistinctKeys++;
            comparedValidKeys++;

            // Defensive deduction
            if (updateKeySet.Contains(key) || skipKeySet.Contains(key))
                continue;

            // If destination already has it -> not an insert
            if (destKeySet.Contains(key))
                continue;

            // Build insert row FROM VALID schema (userid + "id " supported)
            var vUserId = GetBestField(validDoc, "user_id", "userId", "UserId", "userid");
            var vId = GetBestField(validDoc, "id", "Id", "id ");

            // Destination schema uses user_id + id
            if (vUserId.IsBsonNull || vId.IsBsonNull)
                continue;

            var vTitle = GetBestField(validDoc, "Title", "title");
            var vBody = GetBestField(validDoc, "Body", "body");

            var outDoc = new BsonDocument
            {
                { "objectid", BsonNull.Value },
                { "user_id", vUserId },
                { "id", vId },
                { "title", vTitle.IsBsonNull ? BsonNull.Value : vTitle },
                { "body", vBody.IsBsonNull ? BsonNull.Value : vBody },
                { _dup.ComputedKeyFieldName ?? "Candidate_Key_combination", key }
            };

            insertPayload.Add(outDoc);
            insertCount++;
        }

        // ------------------------------------------------------------
        // Write Insert snapshot
        // ------------------------------------------------------------
        var fetchedUtc = DateTime.UtcNow;
        var fetchedLocal = TimeZoneInfo.ConvertTimeFromUtc(fetchedUtc, tz);
        var fetchedAtLocalText = fetchedLocal.ToString("M/d/yyyy, hh:mm tt", CultureInfo.InvariantCulture);

        var offset = tz.GetUtcOffset(fetchedUtc);
        var sign = offset >= TimeSpan.Zero ? "+" : "-";
        var abs = offset.Duration();
        var offsetText = $" {sign}{abs:hh\\:mm}";
        var timeZoneDisplay = $"{tz.Id} UTC{offsetText}";

        var envelope = new BsonDocument
        {
            { "_id", InsertLatestId },
            { "status", "ok" },
            { "layerUrl", layerUrl },
            { "fetchedAtLocalText", fetchedAtLocalText },
            { "timeZoneId", timeZoneDisplay },

            { "destOidField", oidField },
            { "destObjectIdCount", objectIds.Count },
            { "destRecordsSeen", destRecordsSeen },
            { "destDistinctKeys", destDistinctKeys },
            { "destSkippedNoAttrs", destSkippedNoAttrs },
            { "destSkippedNoKey", destSkippedNoKey },

            { "validDistinctKeys", validDistinctKeys },
            { "validSkippedNoKey", validSkippedNoKey },
            { "comparedValidKeys", comparedValidKeys },

            { "deductedUpdateKeysCount", updateKeySet.Count },
            { "deductedSkipKeysCount", skipKeySet.Count },

            { "insertCount", insertCount },
            { "itemCount", insertPayload.Count },
            { "payload", insertPayload }
        };

        await insertCol.ReplaceOneAsync(
            Builders<BsonDocument>.Filter.Eq("_id", InsertLatestId),
            envelope,
            new ReplaceOptions { IsUpsert = true },
            ct);

        _logger.LogInformation(
            "Insert_Data updated. destDistinctKeys={DestKeys} validDistinctKeys={ValidKeys} comparedValidKeys={Compared} inserts={Inserts}",
            destDistinctKeys, validDistinctKeys, comparedValidKeys, insertCount);
    }

    private static async Task WriteWaiting(IMongoCollection<BsonDocument> col, string status, CancellationToken ct)
    {
        var envelope = new BsonDocument
        {
            { "_id", InsertLatestId },
            { "status", status },
            { "insertCount", 0 },
            { "itemCount", 0 },
            { "payload", new BsonArray() }
        };

        await col.ReplaceOneAsync(
            Builders<BsonDocument>.Filter.Eq("_id", InsertLatestId),
            envelope,
            new ReplaceOptions { IsUpsert = true },
            ct);
    }

    // ----------------------------------------------------------------
    // Destination querying
    // ----------------------------------------------------------------

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
                if (TryGetIntLike(el, out var v))
                    ids.Add(v);
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
            ["returnGeometry"] = "false",
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
            if (expEl.TryGetInt64(out var ms))
                expiresAtUtc = DateTimeOffset.FromUnixTimeMilliseconds(ms);

        _token = token;
        _tokenExpiresAtUtc = expiresAtUtc;

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
        catch { return false; }
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

    // ----------------------------------------------------------------
    // Key + field helpers (this is the core fix)
    // ----------------------------------------------------------------

    private string BuildCandidateKeyFromAnyDoc(BsonDocument doc)
    {
        // prefer stored key if present
        var existing = GetStringCaseInsensitive(doc, _dup.ComputedKeyFieldName ?? "Candidate_Key_combination");
        var existingNorm = NormalizeKey(existing);
        if (!string.IsNullOrWhiteSpace(existingNorm))
            return existingNorm;

        var user = GetBestField(doc, "user_id", "userid", "userId", "UserId");
        var id = GetBestField(doc, "id", "Id", "id ");

        if (user.IsBsonNull || id.IsBsonNull) return "";

        var u = NormalizeForCompare(user);
        var i = NormalizeForCompare(id);
        if (string.IsNullOrWhiteSpace(u) || string.IsNullOrWhiteSpace(i)) return "";

        return $"{u}{(_dup.KeyJoiner ?? "|")}{i}";
    }

    private static string NormalizeKey(string? s)
        => string.IsNullOrWhiteSpace(s) ? "" : s.Trim().TrimEnd(',');

    private static BsonValue GetBestField(BsonDocument doc, params string[] names)
    {
        foreach (var n in names)
        {
            var v = GetFieldWithCasingFallback(doc, n);
            if (!v.IsBsonNull) return v;
        }
        return BsonNull.Value;
    }

    private static string GetStringCaseInsensitive(BsonDocument doc, string fieldName)
    {
        if (string.IsNullOrWhiteSpace(fieldName))
            return "";

        foreach (var e in doc.Elements)
            if (string.Equals(e.Name, fieldName, StringComparison.OrdinalIgnoreCase))
                return e.Value.IsBsonNull ? "" : (e.Value.BsonType == BsonType.String ? (e.Value.AsString ?? "") : (e.Value.ToString() ?? ""));

        var target = NormalizeFieldName(fieldName);
        foreach (var e in doc.Elements)
            if (NormalizeFieldName(e.Name) == target)
                return e.Value.IsBsonNull ? "" : (e.Value.BsonType == BsonType.String ? (e.Value.AsString ?? "") : (e.Value.ToString() ?? ""));

        return "";
    }

    private static BsonValue GetFieldWithCasingFallback(BsonDocument doc, string field)
    {
        if (string.IsNullOrWhiteSpace(field))
            return BsonNull.Value;

        foreach (var e in doc.Elements)
            if (string.Equals(e.Name, field, StringComparison.OrdinalIgnoreCase))
                return e.Value;

        var target = NormalizeFieldName(field);
        foreach (var e in doc.Elements)
            if (NormalizeFieldName(e.Name) == target)
                return e.Value;

        return BsonNull.Value;
    }

    private static string NormalizeFieldName(string s)
    {
        if (string.IsNullOrWhiteSpace(s)) return "";
        var chars = new List<char>(s.Length);
        foreach (var ch in s)
            if (char.IsLetterOrDigit(ch))
                chars.Add(char.ToLowerInvariant(ch));
        return new string(chars.ToArray());
    }

    private static string NormalizeForCompare(BsonValue v)
    {
        if (v is null || v.IsBsonNull) return "";
        var s = v.BsonType == BsonType.String ? (v.AsString ?? "") : (v.ToString() ?? "");
        return (s ?? "").Trim().TrimEnd(',');
    }

    private static TimeZoneInfo ResolveTimeZone(string? timeZoneId)
    {
        if (string.IsNullOrWhiteSpace(timeZoneId))
            return TimeZoneInfo.Local;

        try { return TimeZoneInfo.FindSystemTimeZoneById(timeZoneId); }
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
                try { return TimeZoneInfo.FindSystemTimeZoneById(windowsId); } catch { }
            }

            return TimeZoneInfo.Local;
        }
    }
}