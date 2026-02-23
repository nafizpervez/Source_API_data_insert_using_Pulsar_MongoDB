using System.Globalization;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;
using SourceIngestor.Worker.Options;

namespace SourceIngestor.Worker.Workers;

public sealed class SkipDetectionProcessor : BackgroundService
{
    private const string ValidLatestId = "valid_latest";
    private const string DestinationLatestId = "destination_raw_latest";
    private const string UpdateLatestId = "update_latest";

    private const string SkipLatestId = "skip_latest";

    private readonly IMongoClient _mongoClient;
    private readonly MongoOptions _mongo;
    private readonly DuplicationOptions _dup;
    private readonly DestinationApiOptions _dest;
    private readonly ILogger<SkipDetectionProcessor> _logger;

    public SkipDetectionProcessor(
        IMongoClient mongoClient,
        IOptions<MongoOptions> mongo,
        IOptions<DuplicationOptions> dup,
        IOptions<DestinationApiOptions> dest,
        ILogger<SkipDetectionProcessor> logger)
    {
        _mongoClient = mongoClient;
        _mongo = mongo.Value;
        _dup = dup.Value;
        _dest = dest.Value;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("SkipDetectionProcessor started.");
        _logger.LogInformation("Valid snapshot: {Col} _id={Id}", _mongo.ValidatedCollection, ValidLatestId);
        _logger.LogInformation("Destination snapshot: {Col} _id={Id}", _mongo.DestinationRawCollection, DestinationLatestId);
        _logger.LogInformation("Update snapshot: {Col} _id={Id}", _mongo.UpdateCollection, UpdateLatestId);
        _logger.LogInformation("Skip snapshot: {Col} _id={Id}", _mongo.SkipCollection, SkipLatestId);

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
                _logger.LogError(ex, "SkipDetectionProcessor DetectOnce failed.");
            }

            await Task.Delay(interval, stoppingToken);
        }
    }

    private async Task DetectOnce(CancellationToken ct)
    {
        var db = _mongoClient.GetDatabase(_mongo.Database);

        var validCol = db.GetCollection<BsonDocument>(_mongo.ValidatedCollection);
        var destCol = db.GetCollection<BsonDocument>(_mongo.DestinationRawCollection);
        var updateCol = db.GetCollection<BsonDocument>(_mongo.UpdateCollection);
        var skipCol = db.GetCollection<BsonDocument>(_mongo.SkipCollection);

        var validSnap = await validCol.Find(Builders<BsonDocument>.Filter.Eq("_id", ValidLatestId)).FirstOrDefaultAsync(ct);
        var destSnap = await destCol.Find(Builders<BsonDocument>.Filter.Eq("_id", DestinationLatestId)).FirstOrDefaultAsync(ct);
        var updateSnap = await updateCol.Find(Builders<BsonDocument>.Filter.Eq("_id", UpdateLatestId)).FirstOrDefaultAsync(ct);

        if (validSnap is null || destSnap is null)
        {
            var status = validSnap is null && destSnap is null
                ? "waiting_for_valid_latest_and_destination_latest"
                : validSnap is null
                    ? "waiting_for_valid_latest"
                    : "waiting_for_destination_latest";

            var envelopeMissing = new BsonDocument
            {
                { "_id", SkipLatestId },
                { "status", status },
                { "skipCount", 0 },
                { "itemCount", 0 },
                { "payload", new BsonArray() }
            };

            await skipCol.ReplaceOneAsync(
                Builders<BsonDocument>.Filter.Eq("_id", SkipLatestId),
                envelopeMissing,
                new ReplaceOptions { IsUpsert = true },
                ct);

            return;
        }

        if (!validSnap.TryGetValue("payload", out var validPayloadVal) || validPayloadVal.BsonType != BsonType.Array)
            return;

        if (!destSnap.TryGetValue("payload", out var destPayloadVal) || destPayloadVal.BsonType != BsonType.Array)
            return;

        var validPayload = validPayloadVal.AsBsonArray;
        var destPayload = destPayloadVal.AsBsonArray;

        var tz = ResolveTimeZone(_mongo.TimeZoneId);

        // Build update-key set for Rule #2 (deduct)
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

        // Index destination by CandidateKey -> list of destination minimal docs
        var destByKey = new Dictionary<string, List<BsonDocument>>(StringComparer.Ordinal);

        var destKeysBuilt = 0;
        var destDistinctKeys = 0;
        var destSkippedNoAttrs = 0;
        var destSkippedNoKey = 0;
        var destSkippedNoObjectId = 0;

        var oidFieldFromSnap = destSnap.GetValue("objectIdField", "OBJECTID");
        var oidField = oidFieldFromSnap.IsBsonNull ? "OBJECTID" : (oidFieldFromSnap.ToString() ?? "OBJECTID");

        for (var i = 0; i < destPayload.Count; i++)
        {
            if (destPayload[i].BsonType != BsonType.Document)
                continue;

            var featureDoc = destPayload[i].AsBsonDocument;

            if (!featureDoc.TryGetValue("attributes", out var attrsVal) || attrsVal.BsonType != BsonType.Document)
            {
                destSkippedNoAttrs++;
                continue;
            }

            var attrs = attrsVal.AsBsonDocument;

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

            var minimal = new BsonDocument
            {
                { "objectid", objectIdVal }
            };

            minimal["user_id"] = GetFieldWithCasingFallback(attrs, "user_id");
            if (minimal["user_id"].IsBsonNull)
                minimal["user_id"] = GetFieldWithCasingFallback(attrs, "userId");

            minimal["id"] = GetFieldWithCasingFallback(attrs, "id");
            minimal["title"] = GetFieldWithCasingFallback(attrs, "title");
            minimal["body"] = GetFieldWithCasingFallback(attrs, "body");

            // metadata fields required in Skip_Data payload
            minimal["created_user"] = GetFieldWithCasingFallback(attrs, "created_user");
            minimal["last_edited_user"] = GetFieldWithCasingFallback(attrs, "last_edited_user");
            minimal["created_date"] = FormatDateFieldToLocalText(GetFieldWithCasingFallback(attrs, "created_date"), tz);
            minimal["last_edited_date"] = FormatDateFieldToLocalText(GetFieldWithCasingFallback(attrs, "last_edited_date"), tz);

            minimal[_dup.ComputedKeyFieldName ?? "Candidate_Key_combination"] = key;

            if (!destByKey.TryGetValue(key, out var list))
            {
                list = new List<BsonDocument>(capacity: 1);
                destByKey[key] = list;
                destDistinctKeys++;
            }

            list.Add(minimal);
            destKeysBuilt++;
        }

        var skipPayload = new BsonArray();

        var comparedKeys = 0;
        var matchedKeys = 0;
        var matchedRecords = 0;

        var skipCount = 0;
        var validSkippedNoKey = 0;

        var seenKeys = new HashSet<string>(StringComparer.Ordinal);
        var seenObjectIds = new HashSet<string>(StringComparer.Ordinal);

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

            comparedKeys++;

            if (!destByKey.TryGetValue(key, out var destList) || destList.Count == 0)
                continue;

            matchedKeys++;

            // If key appears in Update_Data, then it must NOT be in Skip_Data (Rule #2)
            if (updateKeySet.Contains(key))
                continue;

            // Valid fields (source)
            var vUserId = ChooseValidUserId(validDoc);
            var vId = ChooseValidId(validDoc);

            var vTitle = GetFieldWithCasingFallback(validDoc, "Title");
            if (vTitle.IsBsonNull) vTitle = GetFieldWithCasingFallback(validDoc, "title");

            var vBody = GetFieldWithCasingFallback(validDoc, "Body");
            if (vBody.IsBsonNull) vBody = GetFieldWithCasingFallback(validDoc, "body");

            foreach (var destMin in destList)
            {
                matchedRecords++;

                // destination fields
                var dUserId = destMin.GetValue("user_id", BsonNull.Value);
                var dId = destMin.GetValue("id", BsonNull.Value);
                var dTitle = destMin.GetValue("title", BsonNull.Value);
                var dBody = destMin.GetValue("body", BsonNull.Value);

                // Rule #1 exact match all 4 fields
                var userOk = BsonValuesEqual(vUserId, dUserId);
                var idOk = BsonValuesEqual(vId, dId);
                var titleOk = BsonValuesEqual(vTitle, dTitle);
                var bodyOk = BsonValuesEqual(vBody, dBody);

                if (!userOk || !idOk || !titleOk || !bodyOk)
                    continue;

                var objectId = destMin.GetValue("objectid", BsonNull.Value);
                if (objectId.IsBsonNull)
                    continue;

                var oidKey = NormalizeForCompare(objectId);
                if (!seenObjectIds.Add(oidKey))
                    continue;

                var outDoc = new BsonDocument
                {
                    { "objectid", objectId },
                    { "user_id", dUserId.IsBsonNull ? vUserId : dUserId },
                    { "id", dId.IsBsonNull ? vId : dId },
                    { "title", dTitle.IsBsonNull ? vTitle : dTitle },
                    { "body", dBody.IsBsonNull ? vBody : dBody },
                    { _dup.ComputedKeyFieldName ?? "Candidate_Key_combination", key },

                    // metadata fields you requested
                    { "created_user", destMin.GetValue("created_user", BsonNull.Value) },
                    { "last_edited_user", destMin.GetValue("last_edited_user", BsonNull.Value) },
                    { "created_date", destMin.GetValue("created_date", BsonNull.Value) },
                    { "last_edited_date", destMin.GetValue("last_edited_date", BsonNull.Value) }
                };

                skipPayload.Add(outDoc);
                skipCount++;
            }
        }

        var fetchedAtLocalText = destSnap.GetValue("fetchedAtLocalText", BsonNull.Value);
        var timeZoneId = destSnap.GetValue("timeZoneId", BsonNull.Value);
        var layerUrl = destSnap.GetValue("layerUrl", BsonNull.Value);

        var envelope = new BsonDocument
        {
            { "_id", SkipLatestId },
            { "status", "ok" },
            { "layerUrl", layerUrl },
            { "fetchedAtLocalText", fetchedAtLocalText },
            { "timeZoneId", timeZoneId },

            { "destKeysBuilt", destKeysBuilt },
            { "destDistinctKeys", destDistinctKeys },
            { "destSkippedNoAttrs", destSkippedNoAttrs },
            { "destSkippedNoKey", destSkippedNoKey },
            { "destSkippedNoObjectId", destSkippedNoObjectId },
            { "validSkippedNoKey", validSkippedNoKey },

            { "comparedValidKeys", comparedKeys },
            { "matchedKeys", matchedKeys },
            { "matchedRecords", matchedRecords },

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
            "Skip_Data snapshot updated. destKeysBuilt={DestKeysBuilt} destDistinctKeys={DestDistinctKeys} comparedKeys={ComparedKeys} matchedKeys={MatchedKeys} matchedRecords={MatchedRecords} skipCount={SkipCount} mongo={Db}.{Col} _id={Id}",
            destKeysBuilt, destDistinctKeys, comparedKeys, matchedKeys, matchedRecords, skipCount, _mongo.Database, _mongo.SkipCollection, SkipLatestId);
    }

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
        {
            if (char.IsLetterOrDigit(ch))
                chars.Add(char.ToLowerInvariant(ch));
        }
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

    private static bool BsonValuesEqual(BsonValue a, BsonValue b)
    {
        if (a is null) a = BsonNull.Value;
        if (b is null) b = BsonNull.Value;

        if (a.IsBsonNull && b.IsBsonNull)
            return true;

        var sa = NormalizeForCompare(a);
        var sb = NormalizeForCompare(b);
        return string.Equals(sa, sb, StringComparison.Ordinal);
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

    private static BsonValue FormatDateFieldToLocalText(BsonValue v, TimeZoneInfo tz)
    {
        if (v is null || v.IsBsonNull)
            return BsonNull.Value;

        if (v.BsonType == BsonType.String)
        {
            var s = (v.AsString ?? "").Trim();
            if (s.Length == 0) return BsonNull.Value;

            if (long.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out var msStr))
                return new BsonString(FormatEpochMs(msStr, tz));

            return v;
        }

        if (!TryReadEpochMs(v, out var ms))
            return v;

        var formatted = FormatEpochMs(ms, tz);
        return string.IsNullOrWhiteSpace(formatted) ? v : new BsonString(formatted);
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