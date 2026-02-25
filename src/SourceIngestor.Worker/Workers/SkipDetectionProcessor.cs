// src/SourceIngestor.Worker/Workers/SkipDetectionProcessor.cs
using System.Globalization;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;
using SourceIngestor.Worker.Options;

namespace SourceIngestor.Worker.Workers;

/// <summary>
/// SkipDetectionProcessor builds Skip_Data snapshot:
/// - "Skip" means: the record exists in Destination Feature Service AND matches Valid_Data exactly (user_id, id, title, body)
/// - Also: if a key appears in Update_Data, we DO NOT allow it to be in Skip_Data.
///
/// IMPORTANT:
/// - Destination data is read DIRECTLY from the Destination Feature Service (live).
///
/// CRITICAL FIX:
/// - Destination may contain duplicate candidate keys.
/// - We MUST deduplicate destination per candidate key so skipCount <= validCount by design.
/// - Winner rule (deterministic): smallest OBJECTID wins.
/// </summary>
public sealed class SkipDetectionProcessor : BackgroundService
{
    private const string ValidLatestId = "valid_latest";
    private const string UpdateLatestId = "update_latest";
    private const string SkipLatestId = "skip_latest";

    private readonly IHttpClientFactory _httpFactory;
    private readonly IMongoClient _mongoClient;

    private readonly MongoOptions _mongo;
    private readonly ArcGisPortalOptions _portal;
    private readonly DestinationApiOptions _dest;
    private readonly DuplicationOptions _dup;

    private readonly ILogger<SkipDetectionProcessor> _logger;

    // Token cache (avoid spamming generateToken)
    private string? _token;
    private DateTimeOffset _tokenExpiresAtUtc = DateTimeOffset.MinValue;

    public SkipDetectionProcessor(
        IHttpClientFactory httpFactory,
        IMongoClient mongoClient,
        IOptions<MongoOptions> mongo,
        IOptions<ArcGisPortalOptions> portal,
        IOptions<DestinationApiOptions> dest,
        IOptions<DuplicationOptions> dup,
        ILogger<SkipDetectionProcessor> logger)
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
        _logger.LogInformation("SkipDetectionProcessor started.");
        _logger.LogInformation("Valid snapshot: {Col} _id={Id}", _mongo.ValidatedCollection, ValidLatestId);
        _logger.LogInformation("Update snapshot: {Col} _id={Id}", _mongo.UpdateCollection, UpdateLatestId);
        _logger.LogInformation("Skip snapshot: {Col} _id={Id}", _mongo.SkipCollection, SkipLatestId);
        _logger.LogInformation("Destination Feature Service (LIVE): {Url}", _dest.FeatureLayerUrl);

        var interval = TimeSpan.FromSeconds(Math.Max(10, _dest.SyncIntervalSeconds));

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await DetectOnce(stoppingToken);
            }
            catch (TaskCanceledException)
            {
                // normal shutdown
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "SkipDetectionProcessor DetectOnce failed.");
            }

            await Task.Delay(interval, stoppingToken);
        }
    }

    private async Task DetectOnce(CancellationToken ct)
    {
        var db = _mongoClient.GetDatabase(_mongo.Database);

        var validCol = db.GetCollection<BsonDocument>(_mongo.ValidatedCollection);
        var updateCol = db.GetCollection<BsonDocument>(_mongo.UpdateCollection);
        var skipCol = db.GetCollection<BsonDocument>(_mongo.SkipCollection);

        var validSnap = await validCol.Find(Builders<BsonDocument>.Filter.Eq("_id", ValidLatestId)).FirstOrDefaultAsync(ct);
        var updateSnap = await updateCol.Find(Builders<BsonDocument>.Filter.Eq("_id", UpdateLatestId)).FirstOrDefaultAsync(ct);

        if (validSnap is null)
        {
            await WriteWaiting(skipCol, "waiting_for_valid_latest", "Valid_Data snapshot not found yet.", ct);
            return;
        }

        if (!validSnap.TryGetValue("payload", out var validPayloadVal) || validPayloadVal.BsonType != BsonType.Array)
        {
            await WriteWaiting(skipCol, "waiting_for_valid_payload", "Valid_Data payload missing/not array.", ct);
            return;
        }

        if (string.IsNullOrWhiteSpace(_dest.FeatureLayerUrl))
            throw new InvalidOperationException("DestinationApi.FeatureLayerUrl is empty.");

        // Build Update key set (anything in Update_Data cannot be in Skip_Data)
        var updateKeySet = new HashSet<string>(StringComparer.Ordinal);
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

        // Read LIVE destination data (FeatureService)
        var token = await GetPortalToken(ct);
        var layerUrl = _dest.FeatureLayerUrl.TrimEnd('/');

        var (oidField, objectIds) = await QueryAllObjectIds(layerUrl, token, ct);
        var batchSize = Math.Max(1, _dest.QueryBatchSize);

        // Winner per key: key => minimal destination record (deterministic: smallest OBJECTID).
        var destWinnerByKey = new Dictionary<string, BsonDocument>(StringComparer.Ordinal);

        var destDistinctKeys = 0;        // distinct keys (after dedupe)
        var destRecordsSeen = 0;         // total destination records scanned
        var destDuplicatesCollapsed = 0; // how many duplicates were collapsed by winner selection
        var destSkippedNoAttrs = 0;
        var destSkippedNoKey = 0;
        var destSkippedNoObjectId = 0;

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

                var keyRaw = ComputeKeyFromAnyDoc(attrs);
                var key = NormalizeKey(keyRaw);
                if (string.IsNullOrWhiteSpace(key))
                {
                    destSkippedNoKey++;
                    continue;
                }

                var objectIdVal = GetFieldWithCasingFallback(attrs, oidField);
                if (objectIdVal.IsBsonNull)
                {
                    destSkippedNoObjectId++;
                    continue;
                }

                var oid = GetIntLike(objectIdVal);
                if (oid is null)
                {
                    destSkippedNoObjectId++;
                    continue;
                }

                var minimal = new BsonDocument
                {
                    { "objectid", oid.Value },
                    { "user_id", GetFieldWithCasingFallback(attrs, "user_id").IsBsonNull ? GetFieldWithCasingFallback(attrs, "userId") : GetFieldWithCasingFallback(attrs, "user_id") },
                    { "id", GetFieldWithCasingFallback(attrs, "id") },
                    { "title", GetFieldWithCasingFallback(attrs, "title") },
                    { "body", GetFieldWithCasingFallback(attrs, "body") },
                    { _dup.ComputedKeyFieldName ?? "Candidate_Key_combination", key }
                };

                if (!destWinnerByKey.TryGetValue(key, out var existing))
                {
                    destWinnerByKey[key] = minimal;
                    destDistinctKeys++;
                    continue;
                }

                // Winner selection: smallest OBJECTID wins (stable and deterministic).
                var existingOid = GetIntLike(existing.GetValue("objectid", BsonNull.Value));
                if (existingOid is null || oid.Value < existingOid.Value)
                {
                    destWinnerByKey[key] = minimal;
                }

                destDuplicatesCollapsed++;
            }
        }

        // Compare Valid_Data vs destination winner per key for exact match => Skip_Data
        var validPayload = validPayloadVal.AsBsonArray;

        var skipPayload = new BsonArray();

        var validCount = validPayload.Count;
        var validDistinctKeys = 0;

        var comparedKeys = 0;
        var matchedKeys = 0;

        var skipCount = 0;
        var validSkippedNoKey = 0;

        var seenKeys = new HashSet<string>(StringComparer.Ordinal);

        foreach (var v in validPayload)
        {
            if (v.BsonType != BsonType.Document)
                continue;

            var validDoc = v.AsBsonDocument;

            var keyFromField = GetStringCaseInsensitive(validDoc, _dup.ComputedKeyFieldName ?? "");
            var key = NormalizeKey(keyFromField);

            if (string.IsNullOrWhiteSpace(key))
                key = NormalizeKey(ComputeKeyFromAnyDoc(validDoc));

            if (string.IsNullOrWhiteSpace(key))
            {
                validSkippedNoKey++;
                continue;
            }

            if (!seenKeys.Add(key))
                continue;

            validDistinctKeys++;
            comparedKeys++;

            // Rule: if in Update_Data => do NOT include in Skip_Data
            if (updateKeySet.Contains(key))
                continue;

            if (!destWinnerByKey.TryGetValue(key, out var destMin))
                continue;

            matchedKeys++;

            // Valid fields (source-of-truth)
            var vUserId = ChooseValidUserId(validDoc);
            var vId = ChooseValidId(validDoc);

            var vTitle = GetFieldWithCasingFallback(validDoc, "Title");
            if (vTitle.IsBsonNull) vTitle = GetFieldWithCasingFallback(validDoc, "title");

            var vBody = GetFieldWithCasingFallback(validDoc, "Body");
            if (vBody.IsBsonNull) vBody = GetFieldWithCasingFallback(validDoc, "body");

            var dUserId = destMin.GetValue("user_id", BsonNull.Value);
            var dId = destMin.GetValue("id", BsonNull.Value);
            var dTitle = destMin.GetValue("title", BsonNull.Value);
            var dBody = destMin.GetValue("body", BsonNull.Value);

            var userOk = BsonValuesEqual(vUserId, dUserId);
            var idOk = BsonValuesEqual(vId, dId);
            var titleOk = BsonValuesEqual(vTitle, dTitle);
            var bodyOk = BsonValuesEqual(vBody, dBody);

            if (!userOk || !idOk || !titleOk || !bodyOk)
                continue;

            var objectId = destMin.GetValue("objectid", BsonNull.Value);
            if (objectId.IsBsonNull)
                continue;

            var outDoc = new BsonDocument
            {
                { "objectid", objectId },
                { "user_id", dUserId.IsBsonNull ? vUserId : dUserId },
                { "id", dId.IsBsonNull ? vId : dId },
                { "title", dTitle.IsBsonNull ? vTitle : dTitle },
                { "body", dBody.IsBsonNull ? vBody : dBody },
                { _dup.ComputedKeyFieldName ?? "Candidate_Key_combination", key }
            };

            skipPayload.Add(outDoc);
            skipCount++;
        }

        // Write Skip_Data snapshot
        var tz = ResolveTimeZone(_mongo.TimeZoneId);
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
            { "_id", SkipLatestId },
            { "status", "ok" },
            { "layerUrl", layerUrl },
            { "objectIdField", oidField },
            { "fetchedAtLocalText", fetchedAtLocalText },
            { "timeZoneId", timeZoneDisplay },

            { "validCount", validCount },
            { "validDistinctKeys", validDistinctKeys },
            { "validSkippedNoKey", validSkippedNoKey },

            { "destObjectIdCount", objectIds.Count },
            { "destRecordsSeen", destRecordsSeen },
            { "destDistinctKeys", destDistinctKeys },
            { "destDuplicatesCollapsed", destDuplicatesCollapsed },
            { "destSkippedNoAttrs", destSkippedNoAttrs },
            { "destSkippedNoKey", destSkippedNoKey },
            { "destSkippedNoObjectId", destSkippedNoObjectId },

            { "comparedValidKeys", comparedKeys },
            { "matchedKeys", matchedKeys },

            { "deductedUpdateKeysCount", updateKeySet.Count },

            { "skipCount", skipCount },
            { "itemCount", skipPayload.Count },
            { "payload", skipPayload }
        };

        await skipCol.ReplaceOneAsync(
            Builders<BsonDocument>.Filter.Eq("_id", SkipLatestId),
            envelope,
            new ReplaceOptions { IsUpsert = true },
            ct);

        _logger.LogInformation(
            "Skip_Data snapshot updated (LIVE FS, deduped). validCount={ValidCount} validDistinctKeys={ValidDistinctKeys} comparedKeys={Compared} matchedKeys={MatchedKeys} skipCount={SkipCount} (guaranteed skipCount<=validCount).",
            validCount, validDistinctKeys, comparedKeys, matchedKeys, skipCount);
    }

    private async Task WriteWaiting(IMongoCollection<BsonDocument> skipCol, string status, string reason, CancellationToken ct)
    {
        var envelope = new BsonDocument
        {
            { "_id", SkipLatestId },
            { "status", status },
            { "reason", reason },
            { "skipCount", 0 },
            { "itemCount", 0 },
            { "payload", new BsonArray() }
        };

        await skipCol.ReplaceOneAsync(
            Builders<BsonDocument>.Filter.Eq("_id", SkipLatestId),
            envelope,
            new ReplaceOptions { IsUpsert = true },
            ct);
    }

    // ---------------------------
    // ArcGIS REST helpers
    // ---------------------------

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

    private async Task<string> GetPortalToken(CancellationToken ct)
    {
        if (!string.IsNullOrWhiteSpace(_token) && _tokenExpiresAtUtc > DateTimeOffset.UtcNow.AddMinutes(2))
            return _token!;

        if (string.IsNullOrWhiteSpace(_portal.GenerateTokenUrl))
            throw new InvalidOperationException("ArcGisPortal.GenerateTokenUrl is empty.");

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

    private async Task<(string oidField, List<int> objectIds)> QueryAllObjectIds(string layerUrl, string token, CancellationToken ct)
    {
        var http = _httpFactory.CreateClient("arcgis");

        var qs = new Dictionary<string, string>
        {
            ["where"] = _dest.Where ?? "1=1",
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

    private async Task<JsonElement> QueryByObjectIds(string layerUrl, string token, string oidField, List<int> objectIds, CancellationToken ct)
    {
        var http = _httpFactory.CreateClient("arcgis");

        var qs = new Dictionary<string, string>
        {
            ["where"] = _dest.Where ?? "1=1",
            ["objectIds"] = string.Join(",", objectIds),
            ["outFields"] = _dest.OutFields ?? "*",
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

    private static bool TryGetIntLike(JsonElement el, out int value)
    {
        value = default;
        try
        {
            return el.ValueKind switch
            {
                JsonValueKind.Number when el.TryGetInt32(out var i) => (value = i) == i,
                JsonValueKind.Number when el.TryGetInt64(out var l) && l >= int.MinValue && l <= int.MaxValue => (value = (int)l) == (int)l,
                JsonValueKind.String when int.TryParse(el.GetString(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var s) => (value = s) == s,
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

    // ---------------------------
    // Key + comparison helpers
    // ---------------------------

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

    private static string NormalizeKey(string? s) => string.IsNullOrWhiteSpace(s) ? "" : s.Trim();

    private static string GetStringCaseInsensitive(BsonDocument doc, string fieldName)
    {
        if (string.IsNullOrWhiteSpace(fieldName))
            return "";

        foreach (var e in doc.Elements)
            if (string.Equals(e.Name, fieldName, StringComparison.OrdinalIgnoreCase))
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

        return BsonNull.Value;
    }

    private static int? GetIntLike(BsonValue v)
    {
        try
        {
            return v.BsonType switch
            {
                BsonType.Int32 => v.AsInt32,
                BsonType.Int64 when v.AsInt64 >= int.MinValue && v.AsInt64 <= int.MaxValue => (int)v.AsInt64,
                BsonType.Double => (int)v.AsDouble,
                BsonType.Decimal128 => (int)Decimal128.ToDecimal(v.AsDecimal128),
                BsonType.String when int.TryParse(v.AsString, NumberStyles.Integer, CultureInfo.InvariantCulture, out var i) => i,
                _ => null
            };
        }
        catch
        {
            return null;
        }
    }

    private static bool BsonValuesEqual(BsonValue a, BsonValue b)
    {
        a ??= BsonNull.Value;
        b ??= BsonNull.Value;

        if (a.IsBsonNull && b.IsBsonNull)
            return true;

        return string.Equals(NormalizeForCompare(a), NormalizeForCompare(b), StringComparison.Ordinal);
    }

    private static string NormalizeForCompare(BsonValue v)
    {
        if (v is null || v.IsBsonNull)
            return "";

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

    private static string NormalizeKeyPart(BsonValue v)
    {
        if (v is null || v.IsBsonNull) return "";
        return NormalizeForCompare(v);
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
                try { return TimeZoneInfo.FindSystemTimeZoneById(windowsId); } catch { }
            }

            return TimeZoneInfo.Local;
        }
    }
}