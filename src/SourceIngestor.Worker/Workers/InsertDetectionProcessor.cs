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
        _logger.LogInformation("Destination endpoint (NOT Mongo snapshot): {Url}", _dest.FeatureLayerUrl);

        var interval = TimeSpan.FromSeconds(Math.Max(10, _dest.SyncIntervalSeconds));

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await DetectOnce(stoppingToken);
            }
            catch (TaskCanceledException)
            {
                // shutdown
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "InsertDetectionProcessor DetectOnce failed.");
            }

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
            var envelopeMissing = new BsonDocument
            {
                { "_id", InsertLatestId },
                { "status", "waiting_for_valid_latest" },
                { "insertCount", 0 },
                { "itemCount", 0 },
                { "payload", new BsonArray() }
            };

            await insertCol.ReplaceOneAsync(
                Builders<BsonDocument>.Filter.Eq("_id", InsertLatestId),
                envelopeMissing,
                new ReplaceOptions { IsUpsert = true },
                ct);

            return;
        }

        if (!validSnap.TryGetValue("payload", out var validPayloadVal) || validPayloadVal.BsonType != BsonType.Array)
            return;

        var validPayload = validPayloadVal.AsBsonArray;

        if (string.IsNullOrWhiteSpace(_portal.GenerateTokenUrl))
            throw new InvalidOperationException("ArcGisPortal.GenerateTokenUrl is empty.");

        if (string.IsNullOrWhiteSpace(_dest.FeatureLayerUrl))
            throw new InvalidOperationException("DestinationApi.FeatureLayerUrl is empty.");

        var token = await GetPortalToken(ct);

        var layerUrl = _dest.FeatureLayerUrl.TrimEnd('/');

        // Read TZ for consistency if you later want to format times, but inserts will have null metadata.
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

                var k = GetStringCaseInsensitive(ud, _dup.ComputedKeyFieldName ?? "Candidate_Key_combination");
                k = NormalizeKey(k);
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

                var k = GetStringCaseInsensitive(sd, _dup.ComputedKeyFieldName ?? "Candidate_Key_combination");
                k = NormalizeKey(k);
                if (!string.IsNullOrWhiteSpace(k))
                    skipKeySet.Add(k);
            }
        }

        // ------------------------------------------------------------
        // Fetch destination keys DIRECTLY from FeatureLayer endpoint
        // ------------------------------------------------------------
        var (oidField, objectIds) = await QueryAllObjectIds(layerUrl, token, ct);

        var destKeySet = new HashSet<string>(StringComparer.Ordinal);
        var destKeysBuilt = 0;
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
                if (!feat.TryGetProperty("attributes", out var attrsEl) || attrsEl.ValueKind != JsonValueKind.Object)
                {
                    destSkippedNoAttrs++;
                    continue;
                }

                // Parse attrs to BsonDocument so we can reuse the same casing/key helpers.
                var attrs = BsonDocument.Parse(attrsEl.GetRawText());

                var keyRaw = ComputeKeyFromAnyDoc(attrs);
                var key = NormalizeKey(keyRaw);

                if (string.IsNullOrWhiteSpace(key))
                {
                    destSkippedNoKey++;
                    continue;
                }

                if (destKeySet.Add(key))
                    destKeysBuilt++;
            }
        }

        // ------------------------------------------------------------
        // Insert = Valid keys NOT in destination keys
        // ------------------------------------------------------------
        var insertPayload = new BsonArray();

        var comparedValidKeys = 0;
        var validSkippedNoKey = 0;
        var insertCount = 0;

        var seenValidKeys = new HashSet<string>(StringComparer.Ordinal);

        foreach (var v in validPayload)
        {
            if (v.BsonType != BsonType.Document)
                continue;

            var validDoc = v.AsBsonDocument;

            var keyFromField = GetStringCaseInsensitive(validDoc, _dup.ComputedKeyFieldName ?? "Candidate_Key_combination");
            var key = NormalizeKey(keyFromField);

            if (string.IsNullOrWhiteSpace(key))
                key = NormalizeKey(ComputeKeyFromAnyDoc(validDoc));

            if (string.IsNullOrWhiteSpace(key))
            {
                validSkippedNoKey++;
                continue;
            }

            if (!seenValidKeys.Add(key))
                continue;

            comparedValidKeys++;

            // Deduct if it appears in Update/Skip (defensive)
            if (updateKeySet.Contains(key) || skipKeySet.Contains(key))
                continue;

            // If destination already has it -> not an insert
            if (destKeySet.Contains(key))
                continue;

            // Build insert payload row from Valid_Data fields
            var vUserId = ChooseValidUserId(validDoc);
            var vId = ChooseValidId(validDoc);

            var vTitle = GetFieldWithCasingFallback(validDoc, "Title");
            if (vTitle.IsBsonNull) vTitle = GetFieldWithCasingFallback(validDoc, "title");

            var vBody = GetFieldWithCasingFallback(validDoc, "Body");
            if (vBody.IsBsonNull) vBody = GetFieldWithCasingFallback(validDoc, "body");

            var outDoc = new BsonDocument
            {
                // inserts have no objectid yet
                { "objectid", BsonNull.Value },

                // keep consistent with your destination schema
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
            { "destDistinctKeys", destKeysBuilt },
            { "destSkippedNoAttrs", destSkippedNoAttrs },
            { "destSkippedNoKey", destSkippedNoKey },

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
            "Insert_Data snapshot updated. destDistinctKeys={DestKeys} comparedValidKeys={Compared} insertCount={InsertCount} mongo={Db}.{Col} _id={Id}",
            destKeysBuilt, comparedValidKeys, insertCount, _mongo.Database, _mongo.InsertCollection, InsertLatestId);
    }

    // ----------------------------------------------------------------
    // Destination querying (same pattern as DestinationRawSyncProcessor)
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

        // OutFields: keep user config unless you want to optimize later.
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

    // ------------------------------------------------------------
    // Key + casing helpers (aligned with your other workers)
    // ------------------------------------------------------------

    private BsonValue ChooseValidUserId(BsonDocument validDoc)
    {
        var v = GetFieldWithCasingFallback(validDoc, "UserId");
        if (!v.IsBsonNull) return v;

        v = GetFieldWithCasingFallback(validDoc, "userId");
        if (!v.IsBsonNull) return v;

        v = GetFieldWithCasingFallback(validDoc, "user_id");
        return v;
    }

    private BsonValue ChooseValidId(BsonDocument validDoc)
    {
        var v = GetFieldWithCasingFallback(validDoc, "Id");
        if (!v.IsBsonNull) return v;

        v = GetFieldWithCasingFallback(validDoc, "id");
        return v;
    }

    private string ComputeKeyFromAnyDoc(BsonDocument doc)
    {
        var fields = _dup.CandidateKeyFields ?? Array.Empty<string>();
        if (fields.Length == 0) return "";

        var parts = new List<string>(fields.Length);

        foreach (var f0 in fields)
        {
            var f = (f0 ?? "").Trim();
            if (string.IsNullOrWhiteSpace(f)) return "";

            var v = GetFieldWithCasingFallback(doc, f);
            if (v.IsBsonNull) return "";

            var part = NormalizeKeyPart(v);
            if (string.IsNullOrWhiteSpace(part)) return "";

            parts.Add(part);
        }

        return string.Join(_dup.KeyJoiner ?? "", parts);
    }

    private static string NormalizeKey(string? s)
    {
        if (string.IsNullOrWhiteSpace(s)) return "";
        return s.Trim();
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

    private static string GetStringCaseInsensitive(BsonDocument doc, string fieldName)
    {
        if (string.IsNullOrWhiteSpace(fieldName))
            return "";

        foreach (var e in doc.Elements)
        {
            if (string.Equals(e.Name, fieldName, StringComparison.OrdinalIgnoreCase))
            {
                if (e.Value.IsBsonNull) return "";
                var s = e.Value.BsonType == BsonType.String ? (e.Value.AsString ?? "") : (e.Value.ToString() ?? "");
                return s ?? "";
            }
        }

        var target = NormalizeFieldName(fieldName);
        foreach (var e in doc.Elements)
        {
            if (NormalizeFieldName(e.Name) == target)
            {
                if (e.Value.IsBsonNull) return "";
                var s = e.Value.BsonType == BsonType.String ? (e.Value.AsString ?? "") : (e.Value.ToString() ?? "");
                return s ?? "";
            }
        }

        return "";
    }

    private static BsonValue GetFieldWithCasingFallback(BsonDocument doc, string field)
    {
        if (string.IsNullOrWhiteSpace(field))
            return BsonNull.Value;

        foreach (var e in doc.Elements)
            if (string.Equals(e.Name, field, StringComparison.OrdinalIgnoreCase))
                return e.Value;

        var pas = ToPascalCase(field);
        foreach (var e in doc.Elements)
            if (string.Equals(e.Name, pas, StringComparison.OrdinalIgnoreCase))
                return e.Value;

        var cam = ToCamelCase(field);
        foreach (var e in doc.Elements)
            if (string.Equals(e.Name, cam, StringComparison.OrdinalIgnoreCase))
                return e.Value;

        var target = NormalizeFieldName(field);
        foreach (var e in doc.Elements)
            if (NormalizeFieldName(e.Name) == target)
                return e.Value;

        return BsonNull.Value;
    }

    private static string NormalizeKeyPart(BsonValue v)
    {
        if (v is null || v.IsBsonNull) return "";

        try
        {
            return v.BsonType switch
            {
                BsonType.String => (v.AsString ?? "").Trim(),
                BsonType.Int32 => v.AsInt32.ToString(CultureInfo.InvariantCulture),
                BsonType.Int64 => v.AsInt64.ToString(CultureInfo.InvariantCulture),
                BsonType.Double => v.AsDouble.ToString("G17", CultureInfo.InvariantCulture),
                BsonType.Decimal128 => Decimal128.ToDecimal(v.AsDecimal128).ToString(CultureInfo.InvariantCulture),
                BsonType.Boolean => v.AsBoolean ? "true" : "false",
                _ => (v.ToString() ?? "").Trim()
            };
        }
        catch
        {
            return (v.ToString() ?? "").Trim();
        }
    }

    private static string ToPascalCase(string s)
    {
        if (string.IsNullOrWhiteSpace(s)) return s;
        if (s.Length == 1) return s.ToUpperInvariant();
        return char.ToUpperInvariant(s[0]) + s.Substring(1);
    }

    private static string ToCamelCase(string s)
    {
        if (string.IsNullOrWhiteSpace(s)) return s;
        if (s.Length == 1) return s.ToLowerInvariant();
        return char.ToLowerInvariant(s[0]) + s.Substring(1);
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