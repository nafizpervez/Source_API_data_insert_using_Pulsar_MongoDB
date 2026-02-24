using System.Globalization;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;
using SourceIngestor.Worker.Options;

namespace SourceIngestor.Worker.Workers;

public sealed class DeleteDetectionProcessor : BackgroundService
{
    private const string ValidLatestId = "valid_latest";
    private const string DestinationLatestId = "destination_raw_latest";
    private const string DeleteLatestId = "delete_latest";

    private readonly IMongoClient _mongoClient;
    private readonly MongoOptions _mongo;
    private readonly DuplicationOptions _dup;
    private readonly DestinationApiOptions _dest;
    private readonly ILogger<DeleteDetectionProcessor> _logger;

    public DeleteDetectionProcessor(
        IMongoClient mongoClient,
        IOptions<MongoOptions> mongo,
        IOptions<DuplicationOptions> dup,
        IOptions<DestinationApiOptions> dest,
        ILogger<DeleteDetectionProcessor> logger)
    {
        _mongoClient = mongoClient;
        _mongo = mongo.Value;
        _dup = dup.Value;
        _dest = dest.Value;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("DeleteDetectionProcessor started.");
        _logger.LogInformation("Valid snapshot: {Col} _id={Id}", _mongo.ValidatedCollection, ValidLatestId);
        _logger.LogInformation("Destination snapshot (index): {Col} _id={Id}", _mongo.DestinationRawCollection, DestinationLatestId);
        _logger.LogInformation("Delete snapshot: {Col} _id={Id}", _mongo.DeleteCollection, DeleteLatestId);

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
                _logger.LogError(ex, "DeleteDetectionProcessor DetectOnce failed.");
            }

            await Task.Delay(interval, stoppingToken);
        }
    }

    private async Task DetectOnce(CancellationToken ct)
    {
        var db = _mongoClient.GetDatabase(_mongo.Database);

        var validCol = db.GetCollection<BsonDocument>(_mongo.ValidatedCollection);
        var destCol = db.GetCollection<BsonDocument>(_mongo.DestinationRawCollection);
        var deleteCol = db.GetCollection<BsonDocument>(_mongo.DeleteCollection);

        var validSnap = await validCol.Find(Builders<BsonDocument>.Filter.Eq("_id", ValidLatestId)).FirstOrDefaultAsync(ct);
        var destIndexOrOld = await destCol.Find(Builders<BsonDocument>.Filter.Eq("_id", DestinationLatestId)).FirstOrDefaultAsync(ct);

        if (validSnap is null || destIndexOrOld is null)
        {
            var status = validSnap is null && destIndexOrOld is null
                ? "waiting_for_valid_latest_and_destination_latest"
                : validSnap is null
                    ? "waiting_for_valid_latest"
                    : "waiting_for_destination_latest";

            var envelopeMissing = new BsonDocument
            {
                { "_id", DeleteLatestId },
                { "status", status },
                { "deleteCount", 0 },
                { "itemCount", 0 },
                { "payload", new BsonArray() }
            };

            await deleteCol.ReplaceOneAsync(
                Builders<BsonDocument>.Filter.Eq("_id", DeleteLatestId),
                envelopeMissing,
                new ReplaceOptions { IsUpsert = true },
                ct);

            return;
        }

        if (!validSnap.TryGetValue("payload", out var validPayloadVal) || validPayloadVal.BsonType != BsonType.Array)
            return;

        // ✅ Load destination payload from index+batch docs (or old single payload doc)
        var destLoad = await LoadDestinationPayloadAsync(destCol, destIndexOrOld, ct);
        if (!destLoad.Success)
        {
            _logger.LogWarning("DeleteDetectionProcessor: destination payload could not be loaded. reason={Reason}", destLoad.Reason);

            var envelopeMissing = new BsonDocument
            {
                { "_id", DeleteLatestId },
                { "status", "waiting_for_destination_payload" },
                { "reason", destLoad.Reason },
                { "deleteCount", 0 },
                { "itemCount", 0 },
                { "payload", new BsonArray() }
            };

            await deleteCol.ReplaceOneAsync(
                Builders<BsonDocument>.Filter.Eq("_id", DeleteLatestId),
                envelopeMissing,
                new ReplaceOptions { IsUpsert = true },
                ct);

            return;
        }

        var validPayload = validPayloadVal.AsBsonArray;
        var destPayload = destLoad.Payload;

        var tz = ResolveTimeZone(_mongo.TimeZoneId);

        // 1) Build SOURCE key set (what should exist)
        var sourceKeySet = new HashSet<string>(StringComparer.Ordinal);
        var sourceKeysBuilt = 0;
        var sourceSkippedNoKey = 0;

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
                sourceSkippedNoKey++;
                continue;
            }

            if (sourceKeySet.Add(key))
                sourceKeysBuilt++;
        }

        // 2) Index DESTINATION by key (what currently exists)
        var destByKey = new Dictionary<string, List<BsonDocument>>(StringComparer.Ordinal);

        var destKeysBuilt = 0;
        var destDistinctKeys = 0;
        var destSkippedNoAttrs = 0;
        var destSkippedNoKey = 0;
        var destSkippedNoObjectId = 0;

        var oidField = string.IsNullOrWhiteSpace(destLoad.ObjectIdField) ? "OBJECTID" : destLoad.ObjectIdField;

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

        // 3) Delete = destination keys that do NOT exist in sourceKeySet
        var deletePayload = new BsonArray();
        var deleteCount = 0;

        var seenObjectIds = new HashSet<string>(StringComparer.Ordinal);

        foreach (var (key, destList) in destByKey)
        {
            if (sourceKeySet.Contains(key))
                continue;

            foreach (var destMin in destList)
            {
                var objectId = destMin.GetValue("objectid", BsonNull.Value);
                if (objectId.IsBsonNull)
                    continue;

                var oidKey = NormalizeForCompare(objectId);
                if (!seenObjectIds.Add(oidKey))
                    continue;

                var outDoc = new BsonDocument
                {
                    { "objectid", objectId },
                    { "user_id", destMin.GetValue("user_id", BsonNull.Value) },
                    { "id", destMin.GetValue("id", BsonNull.Value) },
                    { "title", destMin.GetValue("title", BsonNull.Value) },
                    { "body", destMin.GetValue("body", BsonNull.Value) },
                    { _dup.ComputedKeyFieldName ?? "Candidate_Key_combination", key },

                    { "created_user", destMin.GetValue("created_user", BsonNull.Value) },
                    { "last_edited_user", destMin.GetValue("last_edited_user", BsonNull.Value) },
                    { "created_date", destMin.GetValue("created_date", BsonNull.Value) },
                    { "last_edited_date", destMin.GetValue("last_edited_date", BsonNull.Value) }
                };

                deletePayload.Add(outDoc);
                deleteCount++;
            }
        }

        var envelope = new BsonDocument
        {
            { "_id", DeleteLatestId },
            { "status", "ok" },

            { "layerUrl", destLoad.LayerUrl ?? BsonNull.Value },
            { "fetchedAtLocalText", destLoad.FetchedAtLocalText ?? BsonNull.Value },
            { "timeZoneId", destLoad.TimeZoneId ?? BsonNull.Value },

            { "sourceDistinctKeys", sourceKeysBuilt },
            { "sourceSkippedNoKey", sourceSkippedNoKey },

            { "destKeysBuilt", destKeysBuilt },
            { "destDistinctKeys", destDistinctKeys },
            { "destSkippedNoAttrs", destSkippedNoAttrs },
            { "destSkippedNoKey", destSkippedNoKey },
            { "destSkippedNoObjectId", destSkippedNoObjectId },

            { "deleteCount", deleteCount },
            { "itemCount", deletePayload.Count },
            { "payload", deletePayload }
        };

        await deleteCol.ReplaceOneAsync(
            Builders<BsonDocument>.Filter.Eq("_id", DeleteLatestId),
            envelope,
            new ReplaceOptions { IsUpsert = true },
            ct);

        _logger.LogInformation(
            "Delete_Data snapshot updated. sourceDistinctKeys={SourceDistinctKeys} destKeysBuilt={DestKeysBuilt} destDistinctKeys={DestDistinctKeys} deleteCount={DeleteCount} mongo={Db}.{Col} _id={Id}",
            sourceKeysBuilt, destKeysBuilt, destDistinctKeys, deleteCount, _mongo.Database, _mongo.DeleteCollection, DeleteLatestId);
    }

    // ------------------------------------------------------------
    // Destination payload loader (supports old + new storage)
    // ------------------------------------------------------------

    private sealed record DestinationLoadResult(
        bool Success,
        string Reason,
        BsonArray Payload,
        string ObjectIdField,
        BsonValue? LayerUrl,
        BsonValue? FetchedAtLocalText,
        BsonValue? TimeZoneId);

    private static bool IsIndexDoc(BsonDocument doc)
    {
        if (!doc.TryGetValue("docType", out var v)) return false;
        return v.BsonType == BsonType.String && string.Equals(v.AsString, "index", StringComparison.OrdinalIgnoreCase);
    }

    private async Task<DestinationLoadResult> LoadDestinationPayloadAsync(
        IMongoCollection<BsonDocument> destCol,
        BsonDocument destIndexOrOld,
        CancellationToken ct)
    {
        if (!IsIndexDoc(destIndexOrOld))
        {
            if (destIndexOrOld.TryGetValue("payload", out var p) && p.BsonType == BsonType.Array)
            {
                var oidFieldOld = destIndexOrOld.GetValue("objectIdField", "OBJECTID").ToString() ?? "OBJECTID";
                return new DestinationLoadResult(true, "ok_old_format", p.AsBsonArray, oidFieldOld,
                    destIndexOrOld.GetValue("layerUrl", BsonNull.Value),
                    destIndexOrOld.GetValue("fetchedAtLocalText", BsonNull.Value),
                    destIndexOrOld.GetValue("timeZoneId", BsonNull.Value));
            }

            return new DestinationLoadResult(false, "old_format_missing_payload", new BsonArray(), "OBJECTID", null, null, null);
        }

        var oidField = destIndexOrOld.GetValue("objectIdField", "OBJECTID").ToString() ?? "OBJECTID";

        if (!destIndexOrOld.TryGetValue("batchDocIds", out var idsVal) || idsVal.BsonType != BsonType.Array)
            return new DestinationLoadResult(false, "index_missing_batchDocIds", new BsonArray(), oidField, null, null, null);

        var idsArr = idsVal.AsBsonArray;
        if (idsArr.Count == 0)
            return new DestinationLoadResult(true, "index_has_no_batches", new BsonArray(), oidField,
                destIndexOrOld.GetValue("layerUrl", BsonNull.Value),
                destIndexOrOld.GetValue("fetchedAtLocalText", BsonNull.Value),
                destIndexOrOld.GetValue("timeZoneId", BsonNull.Value));

        var objectIds = new List<ObjectId>(idsArr.Count);
        foreach (var v in idsArr)
            if (v.BsonType == BsonType.ObjectId)
                objectIds.Add(v.AsObjectId);

        if (objectIds.Count == 0)
            return new DestinationLoadResult(false, "index_batchDocIds_not_objectIds", new BsonArray(), oidField, null, null, null);

        var filter = Builders<BsonDocument>.Filter.In("_id", objectIds);
        var batchDocs = await destCol.Find(filter).ToListAsync(ct);

        var merged = new BsonArray();
        foreach (var b in batchDocs)
        {
            if (!b.TryGetValue("payload", out var pv) || pv.BsonType != BsonType.Array)
                continue;

            foreach (var item in pv.AsBsonArray)
                merged.Add(item);
        }

        return new DestinationLoadResult(true, "ok_index_format", merged, oidField,
            destIndexOrOld.GetValue("layerUrl", BsonNull.Value),
            destIndexOrOld.GetValue("fetchedAtLocalText", BsonNull.Value),
            destIndexOrOld.GetValue("timeZoneId", BsonNull.Value));
    }

    // ------------------------------------------------------------
    // Original helper methods (unchanged)
    // ------------------------------------------------------------

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
                BsonType.String when long.TryParse(v.AsString, NumberStyles.Integer, CultureInfo.InvariantCulture, out var s) => (ms = s) >= 0,
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