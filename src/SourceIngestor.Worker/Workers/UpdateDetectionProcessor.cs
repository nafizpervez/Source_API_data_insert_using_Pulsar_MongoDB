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
        _logger.LogInformation("Valid snapshot: {Col} _id={Id}", _mongo.ValidatedCollection, ValidLatestId);
        _logger.LogInformation("Destination snapshot: {Col} _id={Id}", _mongo.DestinationRawCollection, DestinationLatestId);
        _logger.LogInformation("Update snapshot: {Col} _id={Id}", _mongo.UpdateCollection, UpdateLatestId);

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
                _logger.LogError(ex, "UpdateDetectionProcessor DetectOnce failed.");
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

        var validSnap = await validCol.Find(Builders<BsonDocument>.Filter.Eq("_id", ValidLatestId)).FirstOrDefaultAsync(ct);
        var destSnap = await destCol.Find(Builders<BsonDocument>.Filter.Eq("_id", DestinationLatestId)).FirstOrDefaultAsync(ct);

        // Always upsert Update_Data so the collection exists
        if (validSnap is null || destSnap is null)
        {
            var status = validSnap is null && destSnap is null
                ? "waiting_for_valid_latest_and_destination_latest"
                : validSnap is null
                    ? "waiting_for_valid_latest"
                    : "waiting_for_destination_latest";

            var envelopeMissing = new BsonDocument
            {
                { "_id", UpdateLatestId },
                { "status", status },
                { "updateCount", 0 },
                { "itemCount", 0 },
                { "payload", new BsonArray() }
            };

            await updateCol.ReplaceOneAsync(
                Builders<BsonDocument>.Filter.Eq("_id", UpdateLatestId),
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

        // Build destination index: key => attributes doc
        var destByKey = new Dictionary<string, BsonDocument>(StringComparer.Ordinal);

        for (var i = 0; i < destPayload.Count; i++)
        {
            if (destPayload[i].BsonType != BsonType.Document)
                continue;

            var featureDoc = destPayload[i].AsBsonDocument;

            if (!featureDoc.TryGetValue("attributes", out var attrsVal) || attrsVal.BsonType != BsonType.Document)
                continue;

            var attrs = attrsVal.AsBsonDocument;

            var key = ComputeKeyFromAnyDoc(attrs);
            if (string.IsNullOrWhiteSpace(key))
                continue;

            destByKey[key] = attrs;
        }

        // Update payload: FULL destination rows patched with Valid_Data title/body
        var updatePayload = new BsonArray();

        var compared = 0;
        var matched = 0;
        var updateCount = 0;

        foreach (var v in validPayload)
        {
            if (v.BsonType != BsonType.Document)
                continue;

            var validDoc = v.AsBsonDocument;

            var key = GetStringCaseInsensitive(validDoc, _dup.ComputedKeyFieldName ?? "");
            if (string.IsNullOrWhiteSpace(key))
                key = ComputeKeyFromAnyDoc(validDoc);

            if (string.IsNullOrWhiteSpace(key))
                continue;

            compared++;

            if (!destByKey.TryGetValue(key, out var destAttrs))
                continue;

            matched++;

            // Compare ONLY title + body
            var (titleDifferent, bodyDifferent) = TitleBodyDifferent(validDoc, destAttrs);

            if (!titleDifferent && !bodyDifferent)
                continue;

            // Patch: start from FULL destination attributes
            var patched = destAttrs.DeepClone().AsBsonDocument;

            if (titleDifferent)
            {
                var newTitle = GetFieldWithCasingFallback(validDoc, "title"); // will find Title or title
                SetDestinationFieldPreservingCasing(patched, "title", newTitle);
            }

            if (bodyDifferent)
            {
                var newBody = GetFieldWithCasingFallback(validDoc, "body"); // will find Body or body
                SetDestinationFieldPreservingCasing(patched, "body", newBody);
            }

            // Ensure Candidate_Key_combination exists
            var computed = _dup.ComputedKeyFieldName ?? "Candidate_Key_combination";
            patched[computed] = key;

            // Ensure candidate key fields exist in the patched output (use destination if present else valid)
            EnsureCandidateKeys(patched, validDoc);

            updatePayload.Add(patched);
            updateCount++;
        }

        var fetchedAtLocalText = destSnap.GetValue("fetchedAtLocalText", BsonNull.Value);
        var timeZoneId = destSnap.GetValue("timeZoneId", BsonNull.Value);
        var layerUrl = destSnap.GetValue("layerUrl", BsonNull.Value);

        var envelope = new BsonDocument
        {
            { "_id", UpdateLatestId },
            { "status", "ok" },
            { "layerUrl", layerUrl },
            { "fetchedAtLocalText", fetchedAtLocalText },
            { "timeZoneId", timeZoneId },
            { "comparedValidKeys", compared },
            { "matchedKeys", matched },
            { "updateCount", updateCount },
            { "itemCount", updatePayload.Count },
            { "payload", updatePayload }
        };

        await updateCol.ReplaceOneAsync(
            Builders<BsonDocument>.Filter.Eq("_id", UpdateLatestId),
            envelope,
            new ReplaceOptions { IsUpsert = true },
            ct);

        _logger.LogInformation(
            "Update_Data snapshot updated. compared={Compared} matched={Matched} updates={Updates} mongo={Db}.{Col} _id={Id}",
            compared, matched, updateCount, _mongo.Database, _mongo.UpdateCollection, UpdateLatestId);
    }

    private (bool titleDiff, bool bodyDiff) TitleBodyDifferent(BsonDocument validDoc, BsonDocument destAttrs)
    {
        var vTitle = GetFieldWithCasingFallback(validDoc, "title");
        var dTitle = GetFieldWithCasingFallback(destAttrs, "title");

        var vBody = GetFieldWithCasingFallback(validDoc, "body");
        var dBody = GetFieldWithCasingFallback(destAttrs, "body");

        var titleDiff = !BsonValuesEqual(vTitle, dTitle);
        var bodyDiff = !BsonValuesEqual(vBody, dBody);

        return (titleDiff, bodyDiff);
    }

    // Compute candidate key using Duplication config from ANY doc (Valid or Destination)
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

    // Overwrite destination field name with same casing if exists (title vs Title)
    private static void SetDestinationFieldPreservingCasing(BsonDocument dest, string logicalField, BsonValue newValue)
    {
        // If destination has exact/case-insensitive match, overwrite that actual field name
        foreach (var e in dest.Elements)
        {
            if (string.Equals(e.Name, logicalField, StringComparison.OrdinalIgnoreCase))
            {
                dest[e.Name] = newValue;
                return;
            }
        }

        // If not present, default to logicalField (camel)
        dest[logicalField] = newValue;
    }

    private void EnsureCandidateKeys(BsonDocument dest, BsonDocument validDoc)
    {
        foreach (var f0 in _dup.CandidateKeyFields ?? Array.Empty<string>())
        {
            var f = (f0 ?? "").Trim();
            if (string.IsNullOrWhiteSpace(f))
                continue;

            // If destination has it, keep it
            foreach (var e in dest.Elements)
            {
                if (string.Equals(e.Name, f, StringComparison.OrdinalIgnoreCase) && !e.Value.IsBsonNull)
                    goto next;
            }

            // Else copy from valid (UserId/Id vs userId/id)
            var vv = GetFieldWithCasingFallback(validDoc, f);
            if (!vv.IsBsonNull)
                dest[f] = vv;

        next:
            continue;
        }
    }

    // -------- helpers --------

    private static string GetStringCaseInsensitive(BsonDocument doc, string fieldName)
    {
        if (string.IsNullOrWhiteSpace(fieldName))
            return "";

        foreach (var e in doc.Elements)
        {
            if (string.Equals(e.Name, fieldName, StringComparison.OrdinalIgnoreCase))
            {
                if (e.Value.IsBsonNull) return "";
                return e.Value.BsonType == BsonType.String ? (e.Value.AsString ?? "") : (e.Value.ToString() ?? "");
            }
        }

        return "";
    }

    private static BsonValue GetFieldWithCasingFallback(BsonDocument doc, string field)
    {
        // direct case-insensitive match
        foreach (var e in doc.Elements)
            if (string.Equals(e.Name, field, StringComparison.OrdinalIgnoreCase))
                return e.Value;

        // try Pascal/Camel
        var pas = ToPascalCase(field);
        foreach (var e in doc.Elements)
            if (string.Equals(e.Name, pas, StringComparison.OrdinalIgnoreCase))
                return e.Value;

        var cam = ToCamelCase(field);
        foreach (var e in doc.Elements)
            if (string.Equals(e.Name, cam, StringComparison.OrdinalIgnoreCase))
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
}