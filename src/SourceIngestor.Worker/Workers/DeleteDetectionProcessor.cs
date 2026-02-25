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
        var interval = TimeSpan.FromSeconds(Math.Max(10, _dest.SyncIntervalSeconds));

        while (!stoppingToken.IsCancellationRequested)
        {
            try { await DetectOnce(stoppingToken); }
            catch (TaskCanceledException) { }
            catch (Exception ex) { _logger.LogError(ex, "DeleteDetectionProcessor DetectOnce failed."); }

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
                : validSnap is null ? "waiting_for_valid_latest" : "waiting_for_destination_latest";

            await deleteCol.ReplaceOneAsync(
                Builders<BsonDocument>.Filter.Eq("_id", DeleteLatestId),
                new BsonDocument { { "_id", DeleteLatestId }, { "status", status }, { "deleteCount", 0 }, { "itemCount", 0 }, { "payload", new BsonArray() } },
                new ReplaceOptions { IsUpsert = true },
                ct);
            return;
        }

        if (!validSnap.TryGetValue("payload", out var validPayloadVal) || validPayloadVal.BsonType != BsonType.Array)
        {
            await deleteCol.ReplaceOneAsync(
                Builders<BsonDocument>.Filter.Eq("_id", DeleteLatestId),
                new BsonDocument { { "_id", DeleteLatestId }, { "status", "waiting_for_valid_payload" }, { "deleteCount", 0 }, { "itemCount", 0 }, { "payload", new BsonArray() } },
                new ReplaceOptions { IsUpsert = true },
                ct);
            return;
        }

        var destLoad = await LoadDestinationPayloadAsync(destCol, destIndexOrOld, ct);
        if (!destLoad.Success)
        {
            await deleteCol.ReplaceOneAsync(
                Builders<BsonDocument>.Filter.Eq("_id", DeleteLatestId),
                new BsonDocument
                {
                    { "_id", DeleteLatestId },
                    { "status", "waiting_for_destination_payload" },
                    { "reason", destLoad.Reason },
                    { "deleteCount", 0 },
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

        // VALID key set
        var validKeySet = new HashSet<string>(StringComparer.Ordinal);
        var validDistinctKeys = 0;
        var validSkippedNoKey = 0;

        foreach (var v in validPayload)
        {
            if (v.BsonType != BsonType.Document) continue;
            var d = v.AsBsonDocument;

            var key = BuildCandidateKeyFromAnyDoc(d);
            if (string.IsNullOrWhiteSpace(key))
            {
                validSkippedNoKey++;
                continue;
            }

            if (validKeySet.Add(key))
                validDistinctKeys++;
        }

        // DELETE = destination records whose key NOT in validKeySet (delete ALL duplicates)
        var deletePayload = new BsonArray();
        var seenObjectIds = new HashSet<string>(StringComparer.Ordinal);

        var destRecordsSeen = 0;
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

            if (validKeySet.Contains(key))
                continue;

            var objectIdVal = GetFieldWithCasingFallback(attrs, oidField);
            if (objectIdVal.IsBsonNull)
            {
                destSkippedNoObjectId++;
                continue;
            }

            var oidKey = NormalizeForCompare(objectIdVal);
            if (!seenObjectIds.Add(oidKey))
                continue;

            deletePayload.Add(new BsonDocument
            {
                { "objectid", objectIdVal },
                { "user_id", GetBestField(attrs, "user_id", "userId", "UserId", "userid") },
                { "id", GetBestField(attrs, "id", "Id", "id ") },
                { "title", GetBestField(attrs, "title", "Title") },
                { "body", GetBestField(attrs, "body", "Body") },
                { _dup.ComputedKeyFieldName ?? "Candidate_Key_combination", key }
            });
        }

        var envelope = new BsonDocument
        {
            { "_id", DeleteLatestId },
            { "status", "ok" },

            { "layerUrl", destLoad.LayerUrl ?? BsonNull.Value },
            { "fetchedAtLocalText", destLoad.FetchedAtLocalText ?? BsonNull.Value },
            { "timeZoneId", destLoad.TimeZoneId ?? BsonNull.Value },

            { "validDistinctKeys", validDistinctKeys },
            { "validSkippedNoKey", validSkippedNoKey },

            { "destRecordsSeen", destRecordsSeen },
            { "destSkippedNoAttrs", destSkippedNoAttrs },
            { "destSkippedNoKey", destSkippedNoKey },
            { "destSkippedNoObjectId", destSkippedNoObjectId },

            { "deleteCount", deletePayload.Count },
            { "itemCount", deletePayload.Count },
            { "payload", deletePayload }
        };

        await deleteCol.ReplaceOneAsync(
            Builders<BsonDocument>.Filter.Eq("_id", DeleteLatestId),
            envelope,
            new ReplaceOptions { IsUpsert = true },
            ct);

        _logger.LogInformation("Delete_Data updated. validDistinctKeys={ValidDistinctKeys} deletes={Deletes}",
            validDistinctKeys, deletePayload.Count);
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

    private static string NormalizeForCompare(BsonValue v)
    {
        if (v is null || v.IsBsonNull) return "";
        var s = v.BsonType == BsonType.String ? (v.AsString ?? "") : (v.ToString() ?? "");
        return (s ?? "").Trim().TrimEnd(',');
    }
}