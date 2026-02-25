using System.Globalization;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;
using SourceIngestor.Worker.Options;

namespace SourceIngestor.Worker.Workers;

public sealed class UpdateDetectionProcessor : BackgroundService
{
    private const string ValidLatestId = "valid_latest";
    private const string DestinationLatestId = "destination_raw_latest";
    private const string UpdateLatestId = "update_latest";

    private readonly IMongoClient _mongoClient;
    private readonly MongoOptions _mongo;
    private readonly DuplicationOptions _dup;
    private readonly DestinationApiOptions _dest;
    private readonly ILogger<UpdateDetectionProcessor> _logger;

    public UpdateDetectionProcessor(
        IMongoClient mongoClient,
        IOptions<MongoOptions> mongo,
        IOptions<DuplicationOptions> dup,
        IOptions<DestinationApiOptions> dest,
        ILogger<UpdateDetectionProcessor> logger)
    {
        _mongoClient = mongoClient;
        _mongo = mongo.Value;
        _dup = dup.Value;
        _dest = dest.Value;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("UpdateDetectionProcessor started.");
        var interval = TimeSpan.FromSeconds(Math.Max(10, _dest.SyncIntervalSeconds));

        while (!stoppingToken.IsCancellationRequested)
        {
            try { await DetectOnce(stoppingToken); }
            catch (TaskCanceledException) { }
            catch (Exception ex) { _logger.LogError(ex, "UpdateDetectionProcessor DetectOnce failed."); }

            await Task.Delay(interval, stoppingToken);
        }
    }

    private async Task DetectOnce(CancellationToken ct)
    {
        var db = _mongoClient.GetDatabase(_mongo.Database);

        var validCol = db.GetCollection<BsonDocument>(_mongo.ValidatedCollection);
        var destCol = db.GetCollection<BsonDocument>(_mongo.DestinationRawCollection);
        var updateCol = db.GetCollection<BsonDocument>(_mongo.UpdateCollection);

        var validSnap = await validCol.Find(Builders<BsonDocument>.Filter.Eq("_id", ValidLatestId)).FirstOrDefaultAsync(ct);
        var destIndexOrOld = await destCol.Find(Builders<BsonDocument>.Filter.Eq("_id", DestinationLatestId)).FirstOrDefaultAsync(ct);

        if (validSnap is null || destIndexOrOld is null)
        {
            await WriteWaiting(updateCol,
                validSnap is null && destIndexOrOld is null ? "waiting_for_valid_latest_and_destination_latest"
                : validSnap is null ? "waiting_for_valid_latest"
                : "waiting_for_destination_latest",
                ct);
            return;
        }

        if (!validSnap.TryGetValue("payload", out var validPayloadVal) || validPayloadVal.BsonType != BsonType.Array)
        {
            await WriteWaiting(updateCol, "waiting_for_valid_payload", ct);
            return;
        }

        var destLoad = await LoadDestinationPayloadAsync(destCol, destIndexOrOld, ct);
        if (!destLoad.Success)
        {
            await updateCol.ReplaceOneAsync(
                Builders<BsonDocument>.Filter.Eq("_id", UpdateLatestId),
                new BsonDocument
                {
                    { "_id", UpdateLatestId },
                    { "status", "waiting_for_destination_payload" },
                    { "reason", destLoad.Reason },
                    { "updateCount", 0 },
                    { "itemCount", 0 },
                    { "payload", new BsonArray() }
                },
                new ReplaceOptions { IsUpsert = true },
                ct);
            return;
        }

        var validPayload = validPayloadVal.AsBsonArray;
        var destPayload = destLoad.Payload;
        var oidField = string.IsNullOrWhiteSpace(destLoad.ObjectIdField) ? "OBJECTID" : destLoad.ObjectIdField;

        // ------------------------------------------------------------
        // DEST winner per key (smallest OBJECTID)
        // ------------------------------------------------------------
        var destWinnerByKey = new Dictionary<string, BsonDocument>(StringComparer.Ordinal);

        var destRecordsSeen = 0;
        var destDistinctKeys = 0;
        var destDuplicatesCollapsed = 0;
        var destSkippedNoAttrs = 0;
        var destSkippedNoKey = 0;
        var destSkippedNoObjectId = 0;

        foreach (var it in destPayload)
        {
            if (it.BsonType != BsonType.Document) continue;
            var featureDoc = it.AsBsonDocument;

            if (!featureDoc.TryGetValue("attributes", out var attrsVal) || attrsVal.BsonType != BsonType.Document)
            {
                destSkippedNoAttrs++;
                continue;
            }

            destRecordsSeen++;
            var attrs = attrsVal.AsBsonDocument;

            var key = BuildCandidateKeyFromAnyDoc(attrs);
            if (string.IsNullOrWhiteSpace(key))
            {
                destSkippedNoKey++;
                continue;
            }

            var oidVal = GetFieldWithCasingFallback(attrs, oidField);
            var oid = GetIntLike(oidVal);
            if (oid is null)
            {
                destSkippedNoObjectId++;
                continue;
            }

            var minimal = new BsonDocument
            {
                { "objectid", oid.Value },
                { "user_id", GetBestField(attrs, "user_id", "userId", "UserId", "userid") },
                { "id", GetBestField(attrs, "id", "Id", "id ") },
                { "title", GetBestField(attrs, "title", "Title") },
                { "body", GetBestField(attrs, "body", "Body") },
                { _dup.ComputedKeyFieldName ?? "Candidate_Key_combination", key }
            };

            if (!destWinnerByKey.TryGetValue(key, out var existing))
            {
                destWinnerByKey[key] = minimal;
                destDistinctKeys++;
                continue;
            }

            var existingOid = GetIntLike(existing.GetValue("objectid", BsonNull.Value));
            if (existingOid is null || oid.Value < existingOid.Value)
                destWinnerByKey[key] = minimal;

            destDuplicatesCollapsed++;
        }

        // ------------------------------------------------------------
        // VALID => compare to DEST winner => updates
        // ------------------------------------------------------------
        var updatePayload = new BsonArray();

        var validDistinctKeys = 0;
        var validSkippedNoKey = 0;
        var comparedKeys = 0;
        var matchedKeys = 0;

        var identityMismatchCount = 0;
        var titleDiffCount = 0;
        var bodyDiffCount = 0;

        var seenValidKeys = new HashSet<string>(StringComparer.Ordinal);

        foreach (var v in validPayload)
        {
            if (v.BsonType != BsonType.Document) continue;
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
            comparedKeys++;

            if (!destWinnerByKey.TryGetValue(key, out var destMin))
                continue;

            matchedKeys++;

            // STRICT identity match
            var vUserId = GetBestField(validDoc, "userid", "user_id", "userId", "UserId");
            var vId = GetBestField(validDoc, "id", "Id", "id ");

            var dUserId = destMin.GetValue("user_id", BsonNull.Value);
            var dId = destMin.GetValue("id", BsonNull.Value);

            if (!BsonValuesEqual(vUserId, dUserId) || !BsonValuesEqual(vId, dId))
            {
                identityMismatchCount++;
                continue;
            }

            var vTitle = GetBestField(validDoc, "Title", "title");
            var vBody = GetBestField(validDoc, "Body", "body");

            var dTitle = destMin.GetValue("title", BsonNull.Value);
            var dBody = destMin.GetValue("body", BsonNull.Value);

            var titleDiff = !BsonValuesEqual(vTitle, dTitle);
            var bodyDiff = !BsonValuesEqual(vBody, dBody);

            if (!titleDiff && !bodyDiff)
                continue;

            if (titleDiff) titleDiffCount++;
            if (bodyDiff) bodyDiffCount++;

            var objectId = destMin.GetValue("objectid", BsonNull.Value);
            if (objectId.IsBsonNull)
                continue;

            updatePayload.Add(new BsonDocument
            {
                { "objectid", objectId },
                { "user_id", vUserId },
                { "id", vId },
                { "final_title", vTitle.IsBsonNull ? BsonNull.Value : vTitle },
                { "final_body", vBody.IsBsonNull ? BsonNull.Value : vBody },
                { _dup.ComputedKeyFieldName ?? "Candidate_Key_combination", key },
                { "old_title", dTitle.IsBsonNull ? BsonNull.Value : dTitle },
                { "old_body", dBody.IsBsonNull ? BsonNull.Value : dBody }
            });
        }

        var envelope = new BsonDocument
        {
            { "_id", UpdateLatestId },
            { "status", "ok" },

            { "layerUrl", destLoad.LayerUrl ?? BsonNull.Value },
            { "fetchedAtLocalText", destLoad.FetchedAtLocalText ?? BsonNull.Value },
            { "timeZoneId", destLoad.TimeZoneId ?? BsonNull.Value },

            { "validDistinctKeys", validDistinctKeys },
            { "validSkippedNoKey", validSkippedNoKey },

            { "destRecordsSeen", destRecordsSeen },
            { "destDistinctKeys", destDistinctKeys },
            { "destDuplicatesCollapsed", destDuplicatesCollapsed },
            { "destSkippedNoAttrs", destSkippedNoAttrs },
            { "destSkippedNoKey", destSkippedNoKey },
            { "destSkippedNoObjectId", destSkippedNoObjectId },

            { "comparedValidKeys", comparedKeys },
            { "matchedKeys", matchedKeys },

            { "identityMismatchCount", identityMismatchCount },
            { "titleDiffCount", titleDiffCount },
            { "bodyDiffCount", bodyDiffCount },

            { "updateCount", updatePayload.Count },
            { "itemCount", updatePayload.Count },
            { "payload", updatePayload }
        };

        await updateCol.ReplaceOneAsync(
            Builders<BsonDocument>.Filter.Eq("_id", UpdateLatestId),
            envelope,
            new ReplaceOptions { IsUpsert = true },
            ct);

        _logger.LogInformation(
            "Update_Data updated. validDistinctKeys={ValidDistinctKeys} matchedKeys={MatchedKeys} updates={Updates} identityMismatch={IdentityMismatch} titleDiff={TitleDiff} bodyDiff={BodyDiff}",
            validDistinctKeys, matchedKeys, updatePayload.Count, identityMismatchCount, titleDiffCount, bodyDiffCount);
    }

    private static async Task WriteWaiting(IMongoCollection<BsonDocument> col, string status, CancellationToken ct)
    {
        var envelope = new BsonDocument
        {
            { "_id", UpdateLatestId },
            { "status", status },
            { "updateCount", 0 },
            { "itemCount", 0 },
            { "payload", new BsonArray() }
        };

        await col.ReplaceOneAsync(
            Builders<BsonDocument>.Filter.Eq("_id", UpdateLatestId),
            envelope,
            new ReplaceOptions { IsUpsert = true },
            ct);
    }

    // ---------------- Destination loader ----------------

    private sealed record DestinationLoadResult(
        bool Success,
        string Reason,
        BsonArray Payload,
        string ObjectIdField,
        BsonValue? LayerUrl,
        BsonValue? FetchedAtLocalText,
        BsonValue? TimeZoneId);

    private static bool IsIndexDoc(BsonDocument doc)
        => doc.TryGetValue("docType", out var v) && v.BsonType == BsonType.String &&
           string.Equals(v.AsString, "index", StringComparison.OrdinalIgnoreCase);

    private async Task<DestinationLoadResult> LoadDestinationPayloadAsync(
        IMongoCollection<BsonDocument> destCol,
        BsonDocument destIndexOrOld,
        CancellationToken ct)
    {
        // Old format
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

        // Index+batch format
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
            if (v.BsonType == BsonType.ObjectId) objectIds.Add(v.AsObjectId);

        if (objectIds.Count == 0)
            return new DestinationLoadResult(false, "index_batchDocIds_not_objectIds", new BsonArray(), oidField, null, null, null);

        var filter = Builders<BsonDocument>.Filter.In("_id", objectIds);
        var batchDocs = await destCol.Find(filter).ToListAsync(ct);

        var merged = new BsonArray();
        foreach (var b in batchDocs)
        {
            if (!b.TryGetValue("payload", out var pv) || pv.BsonType != BsonType.Array) continue;
            foreach (var item in pv.AsBsonArray) merged.Add(item);
        }

        return new DestinationLoadResult(true, "ok_index_format", merged, oidField,
            destIndexOrOld.GetValue("layerUrl", BsonNull.Value),
            destIndexOrOld.GetValue("fetchedAtLocalText", BsonNull.Value),
            destIndexOrOld.GetValue("timeZoneId", BsonNull.Value));
    }

    // ---------------- Key + field helpers ----------------

    private string BuildCandidateKeyFromAnyDoc(BsonDocument doc)
    {
        // prefer stored key if present
        var existing = GetStringCaseInsensitive(doc, _dup.ComputedKeyFieldName ?? "Candidate_Key_combination");
        var norm = NormalizeKey(existing);
        if (!string.IsNullOrWhiteSpace(norm)) return norm;

        var user = GetBestField(doc, "user_id", "userid", "userId", "UserId");
        var id = GetBestField(doc, "id", "Id", "id ");

        if (user.IsBsonNull || id.IsBsonNull) return "";

        var u = NormalizeForCompare(user);
        var i = NormalizeForCompare(id);
        if (string.IsNullOrWhiteSpace(u) || string.IsNullOrWhiteSpace(i)) return "";

        return $"{u}{(_dup.KeyJoiner ?? "|")}{i}";
    }

    private static string NormalizeKey(string? s) => string.IsNullOrWhiteSpace(s) ? "" : s.Trim().TrimEnd(',');

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
        if (string.IsNullOrWhiteSpace(fieldName)) return "";

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
        if (string.IsNullOrWhiteSpace(field)) return BsonNull.Value;

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
            if (char.IsLetterOrDigit(ch)) chars.Add(char.ToLowerInvariant(ch));
        return new string(chars.ToArray());
    }

    private static bool BsonValuesEqual(BsonValue a, BsonValue b)
        => string.Equals(NormalizeForCompare(a), NormalizeForCompare(b), StringComparison.Ordinal);

    private static string NormalizeForCompare(BsonValue v)
    {
        if (v is null || v.IsBsonNull) return "";
        var s = v.BsonType == BsonType.String ? (v.AsString ?? "") : (v.ToString() ?? "");
        return (s ?? "").Trim().TrimEnd(',');
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
                BsonType.String when int.TryParse(v.AsString?.Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var i) => i,
                _ => null
            };
        }
        catch { return null; }
    }
}