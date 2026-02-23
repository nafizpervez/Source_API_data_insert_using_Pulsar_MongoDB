using System.Globalization;
using MongoDB.Bson;
using MongoDB.Driver;
using Microsoft.Extensions.Options;
using SourceIngestor.Worker.Options;

namespace SourceIngestor.Worker.Workers;

public sealed class ValidationProcessor : BackgroundService
{
    // One snapshot document per collection (like DestinationRawSyncProcessor)
    private const string ValidLatestId = "valid_latest";
    private const string InvalidLatestId = "invalid_latest";
    private const string DuplicatedLatestId = "duplicated_latest";

    private readonly IMongoClient _mongoClient;
    private readonly MongoOptions _mongo;
    private readonly DuplicationOptions _dup;
    private readonly ILogger<ValidationProcessor> _logger;

    public ValidationProcessor(
        IMongoClient mongoClient,
        IOptions<MongoOptions> mongo,
        IOptions<DuplicationOptions> dup,
        ILogger<ValidationProcessor> logger)
    {
        _mongoClient = mongoClient;
        _mongo = mongo.Value;
        _dup = dup.Value;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var db = _mongoClient.GetDatabase(_mongo.Database);

        var sourceCol = db.GetCollection<BsonDocument>(_mongo.Collection);
        var validatedCol = db.GetCollection<BsonDocument>(_mongo.ValidatedCollection);
        var invalidCol = db.GetCollection<BsonDocument>(_mongo.InvalidCollection);
        var duplicatedCol = db.GetCollection<BsonDocument>(_mongo.DuplicatedCollection);

        _logger.LogInformation("ValidationProcessor started.");
        _logger.LogInformation("Mongo source: {Db}.{Col}", _mongo.Database, _mongo.Collection);
        _logger.LogInformation("Mongo target(valid): {Db}.{Col}", _mongo.Database, _mongo.ValidatedCollection);
        _logger.LogInformation("Mongo target(invalid): {Db}.{Col}", _mongo.Database, _mongo.InvalidCollection);
        _logger.LogInformation("Mongo target(duplicated): {Db}.{Col}", _mongo.Database, _mongo.DuplicatedCollection);

        _logger.LogInformation("Duplication mode: {Mode}", _dup.Mode);
        _logger.LogInformation("Duplication primaryKeyField: {Pk}", _dup.PrimaryKeyField);
        _logger.LogInformation("Duplication candidateKeyFields: {Ck}", string.Join(",", _dup.CandidateKeyFields ?? Array.Empty<string>()));
        _logger.LogInformation("Duplication computedKeyFieldName: {Field}", _dup.ComputedKeyFieldName);
        _logger.LogInformation("Duplication keyJoiner: '{Joiner}'", _dup.KeyJoiner ?? "");

        // ✅ NEW: Ensure snapshot docs exist immediately (prevents "valid_latest not found yet")
        await EnsureSnapshotsExist(validatedCol, invalidCol, duplicatedCol, stoppingToken);

        // Watermark for this process lifetime
        ObjectId? lastSeenId = null;

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var filter = lastSeenId is null
                    ? Builders<BsonDocument>.Filter.Empty
                    : Builders<BsonDocument>.Filter.Gt("_id", lastSeenId.Value);

                var docs = await sourceCol
                    .Find(filter)
                    .Sort(Builders<BsonDocument>.Sort.Ascending("_id"))
                    .Limit(25)
                    .ToListAsync(stoppingToken);

                if (docs.Count == 0)
                {
                    await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken);
                    continue;
                }

                foreach (var src in docs)
                {
                    stoppingToken.ThrowIfCancellationRequested();

                    var srcId = src["_id"].AsObjectId;
                    lastSeenId = srcId;

                    var sourceUrl = src.GetValue("sourceUrl", BsonNull.Value);
                    var fetchedAtLocalText = src.GetValue("fetchedAtLocalText", BsonNull.Value);
                    var timeZoneId = src.GetValue("timeZoneId", BsonNull.Value);
                    var typeMap = src.GetValue("typeMap", new BsonDocument());

                    if (!src.TryGetValue("payload", out var payloadVal) || payloadVal.BsonType != BsonType.Array)
                    {
                        var errArr = new BsonArray
                        {
                            new BsonDocument
                            {
                                { "sourceDocId", srcId },
                                { "index", -1 },
                                { "reason", "missing_or_non_array_payload" }
                            }
                        };

                        // Append empty results but log the error into invalid + validated + duplicated snapshots
                        await AppendValidatedSnapshot(
                            validatedCol,
                            srcId,
                            sourceUrl,
                            fetchedAtLocalText,
                            timeZoneId,
                            typeMap,
                            validatedItems: new BsonArray(),
                            validCount: 0,
                            invalidCount: 0,
                            duplicatedCount: 0,
                            errors: errArr,
                            stoppingToken);

                        await AppendInvalidSnapshot(
                            invalidCol,
                            srcId,
                            sourceUrl,
                            fetchedAtLocalText,
                            timeZoneId,
                            typeMap,
                            invalidItems: new BsonArray(),
                            invalidCount: 0,
                            errors: errArr,
                            stoppingToken);

                        await AppendDuplicatedSnapshot(
                            duplicatedCol,
                            srcId,
                            sourceUrl,
                            fetchedAtLocalText,
                            timeZoneId,
                            typeMap,
                            duplicatedItems: new BsonArray(),
                            duplicatedCount: 0,
                            errors: errArr,
                            stoppingToken);

                        continue;
                    }

                    var payload = payloadVal.AsBsonArray;

                    var validatedItemsArr = new BsonArray();
                    var invalidItemsArr = new BsonArray();
                    var duplicatedItemsArr = new BsonArray();
                    var errorsArr = new BsonArray();

                    var validCount = 0;
                    var invalidCount = 0;
                    var duplicatedCount = 0;

                    // Track seen keys in this batch (batch-level dedup as you already designed)
                    var seen = new HashSet<string>(StringComparer.Ordinal);

                    for (var i = 0; i < payload.Count; i++)
                    {
                        var item = payload[i];

                        if (item.BsonType != BsonType.Document)
                        {
                            invalidCount++;
                            errorsArr.Add(new BsonDocument
                            {
                                { "sourceDocId", srcId },
                                { "index", i },
                                { "reason", "payload_item_not_document" },
                                { "bsonType", item.BsonType.ToString() }
                            });
                            continue;
                        }

                        var doc = item.AsBsonDocument;

                        var okUserId = TryGetInt(doc, "userId", out var userId, out var userIdErr);
                        var okId = TryGetInt(doc, "id", out var id, out var idErr);

                        var okTitle = TryGetRequiredNonNullField(doc, "title", out var titleErr);
                        var title = GetStringCoerce(doc, "title");

                        var okBody = TryGetRequiredNonNullField(doc, "body", out var bodyErr);
                        var body = GetStringOrNull(doc, "body");

                        if (!okUserId || !okId || !okTitle || !okBody)
                        {
                            invalidCount++;

                            var err = new BsonDocument { { "sourceDocId", srcId }, { "index", i } };
                            if (!okUserId) err.Add("userId", userIdErr);
                            if (!okId) err.Add("id", idErr);
                            if (!okTitle) err.Add("title", titleErr);
                            if (!okBody) err.Add("body", bodyErr);

                            err.Add("raw", ShrinkRaw(doc, maxChars: 500));
                            errorsArr.Add(err);

                            invalidItemsArr.Add(new BsonDocument
                            {
                                { "sourceDocId", srcId },
                                { "userId", doc.GetValue("userId", BsonNull.Value) },
                                { "id", doc.GetValue("id", BsonNull.Value) },
                                { "title", doc.GetValue("title", BsonNull.Value) },
                                { "body", doc.GetValue("body", BsonNull.Value) }
                            });

                            continue;
                        }

                        // --- de-dup key computation (configurable) ---
                        var (keyOk, computedKey, keyErr) = TryComputeDedupKey(doc, _dup);

                        if (!keyOk)
                        {
                            invalidCount++;

                            var err = new BsonDocument
                            {
                                { "sourceDocId", srcId },
                                { "index", i },
                                { "reason", "dedup_key_error" },
                                { "detail", keyErr },
                                { "raw", ShrinkRaw(doc, maxChars: 500) }
                            };
                            errorsArr.Add(err);

                            invalidItemsArr.Add(new BsonDocument
                            {
                                { "sourceDocId", srcId },
                                { "userId", doc.GetValue("userId", BsonNull.Value) },
                                { "id", doc.GetValue("id", BsonNull.Value) },
                                { "title", doc.GetValue("title", BsonNull.Value) },
                                { "body", doc.GetValue("body", BsonNull.Value) }
                            });

                            continue;
                        }

                        // If key already seen -> duplicate
                        if (!seen.Add(computedKey))
                        {
                            duplicatedCount++;

                            var dupDoc = new BsonDocument
                            {
                                { "sourceDocId", srcId },
                                { _dup.ComputedKeyFieldName, computedKey },
                                { "userId", userId },
                                { "id", id },
                                { "title", title ?? "" },
                                { "body", body.AsString ?? "" }
                            };

                            duplicatedItemsArr.Add(dupDoc);
                            continue;
                        }

                        var validated = new BsonDocument
                        {
                            { "sourceDocId", srcId },
                            { _dup.ComputedKeyFieldName, computedKey },
                            { "UserId", userId },
                            { "Id", id },
                            { "Title", title ?? "" },
                            { "Body", body.AsString ?? "" }
                        };

                        validatedItemsArr.Add(validated);
                        validCount++;
                    }

                    // Append into single snapshot docs (one doc per collection)
                    await AppendValidatedSnapshot(
                        validatedCol,
                        srcId,
                        sourceUrl,
                        fetchedAtLocalText,
                        timeZoneId,
                        typeMap,
                        validatedItemsArr,
                        validCount,
                        invalidCount,
                        duplicatedCount,
                        errorsArr,
                        stoppingToken);

                    await AppendInvalidSnapshot(
                        invalidCol,
                        srcId,
                        sourceUrl,
                        fetchedAtLocalText,
                        timeZoneId,
                        typeMap,
                        invalidItemsArr,
                        invalidCount,
                        errorsArr,
                        stoppingToken);

                    await AppendDuplicatedSnapshot(
                        duplicatedCol,
                        srcId,
                        sourceUrl,
                        fetchedAtLocalText,
                        timeZoneId,
                        typeMap,
                        duplicatedItemsArr,
                        duplicatedCount,
                        errorsArr,
                        stoppingToken);

                    _logger.LogInformation(
                        "Validated source batch sourceDocId={SourceDocId} valid={Valid} invalid={Invalid} duplicated={Duplicated}",
                        srcId, validCount, invalidCount, duplicatedCount);
                }
            }
            catch (TaskCanceledException)
            {
                // shutdown
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "ValidationProcessor loop error.");
                await Task.Delay(TimeSpan.FromSeconds(3), stoppingToken);
            }
        }
    }

    // ✅ NEW: create empty snapshot docs so other workers can read immediately
    private static async Task EnsureSnapshotsExist(
        IMongoCollection<BsonDocument> validatedCol,
        IMongoCollection<BsonDocument> invalidCol,
        IMongoCollection<BsonDocument> duplicatedCol,
        CancellationToken ct)
    {
        await EnsureOneSnapshotExists(validatedCol, ValidLatestId, ct);
        await EnsureOneSnapshotExists(invalidCol, InvalidLatestId, ct);
        await EnsureOneSnapshotExists(duplicatedCol, DuplicatedLatestId, ct);
    }

    private static async Task EnsureOneSnapshotExists(IMongoCollection<BsonDocument> col, string id, CancellationToken ct)
    {
        var filter = Builders<BsonDocument>.Filter.Eq("_id", id);

        var update = Builders<BsonDocument>.Update
            .SetOnInsert("_id", id)
            .SetOnInsert("itemCount", 0)
            .SetOnInsert("payload", new BsonArray())
            .SetOnInsert("errors", new BsonArray());

        await col.UpdateOneAsync(filter, update, new UpdateOptions { IsUpsert = true }, ct);
    }

    // ---------------------------
    // SNAPSHOT APPEND HELPERS
    // ---------------------------

    private static async Task AppendValidatedSnapshot(
        IMongoCollection<BsonDocument> validatedCol,
        ObjectId sourceDocId,
        BsonValue sourceUrl,
        BsonValue fetchedAtLocalText,
        BsonValue timeZoneId,
        BsonValue typeMap,
        BsonArray validatedItems,
        int validCount,
        int invalidCount,
        int duplicatedCount,
        BsonArray errors,
        CancellationToken ct)
    {
        var filter = Builders<BsonDocument>.Filter.Eq("_id", ValidLatestId);

        var update = Builders<BsonDocument>.Update
            .SetOnInsert("_id", ValidLatestId)
            .Set("lastSourceDocId", sourceDocId)
            .Set("sourceUrl", sourceUrl)
            .Set("fetchedAtLocalText", fetchedAtLocalText)
            .Set("timeZoneId", timeZoneId)
            .Set("typeMapSource", typeMap)
            .Set("typeMapConverted", new BsonDocument
            {
                { "userId", "int" },
                { "id", "int" },
                { "title", "string" },
                { "body", "string" }
            })
            .Inc("validCount", validCount)
            .Inc("invalidCount", invalidCount)
            .Inc("duplicatedCount", duplicatedCount)
            .Inc("itemCount", validatedItems.Count);

        if (validatedItems.Count > 0)
            update = update.PushEach("payload", validatedItems);

        if (errors.Count > 0)
            update = update.PushEach("errors", errors);

        await validatedCol.UpdateOneAsync(filter, update, new UpdateOptions { IsUpsert = true }, ct);
    }

    private static async Task AppendInvalidSnapshot(
        IMongoCollection<BsonDocument> invalidCol,
        ObjectId sourceDocId,
        BsonValue sourceUrl,
        BsonValue fetchedAtLocalText,
        BsonValue timeZoneId,
        BsonValue typeMap,
        BsonArray invalidItems,
        int invalidCount,
        BsonArray errors,
        CancellationToken ct)
    {
        var filter = Builders<BsonDocument>.Filter.Eq("_id", InvalidLatestId);

        var update = Builders<BsonDocument>.Update
            .SetOnInsert("_id", InvalidLatestId)
            .Set("lastSourceDocId", sourceDocId)
            .Set("sourceUrl", sourceUrl)
            .Set("fetchedAtLocalText", fetchedAtLocalText)
            .Set("timeZoneId", timeZoneId)
            .Set("typeMapSource", typeMap)
            .Set("typeMapConverted", new BsonDocument
            {
                { "userId", "int" },
                { "id", "int" },
                { "title", "string" },
                { "body", "string" }
            })
            .Inc("invalidCount", invalidCount)
            .Inc("itemCount", invalidItems.Count);

        if (invalidItems.Count > 0)
            update = update.PushEach("payload", invalidItems);

        if (errors.Count > 0)
            update = update.PushEach("errors", errors);

        await invalidCol.UpdateOneAsync(filter, update, new UpdateOptions { IsUpsert = true }, ct);
    }

    private static async Task AppendDuplicatedSnapshot(
        IMongoCollection<BsonDocument> duplicatedCol,
        ObjectId sourceDocId,
        BsonValue sourceUrl,
        BsonValue fetchedAtLocalText,
        BsonValue timeZoneId,
        BsonValue typeMap,
        BsonArray duplicatedItems,
        int duplicatedCount,
        BsonArray errors,
        CancellationToken ct)
    {
        var filter = Builders<BsonDocument>.Filter.Eq("_id", DuplicatedLatestId);

        var update = Builders<BsonDocument>.Update
            .SetOnInsert("_id", DuplicatedLatestId)
            .Set("lastSourceDocId", sourceDocId)
            .Set("sourceUrl", sourceUrl)
            .Set("fetchedAtLocalText", fetchedAtLocalText)
            .Set("timeZoneId", timeZoneId)
            .Set("typeMapSource", typeMap)
            .Inc("duplicatedCount", duplicatedCount)
            .Inc("itemCount", duplicatedItems.Count);

        if (duplicatedItems.Count > 0)
            update = update.PushEach("payload", duplicatedItems);

        if (errors.Count > 0)
            update = update.PushEach("errors", errors);

        await duplicatedCol.UpdateOneAsync(filter, update, new UpdateOptions { IsUpsert = true }, ct);
    }

    // ---------------------------
    // KEY + FIELD HELPERS (unchanged)
    // ---------------------------

    private static (bool ok, string key, string error) TryComputeDedupKey(BsonDocument doc, DuplicationOptions opts)
    {
        if (opts is null)
            return (false, "", "duplication_options_missing");

        if (string.IsNullOrWhiteSpace(opts.ComputedKeyFieldName))
            return (false, "", "computed_key_field_name_missing");

        if (opts.Mode == DuplicationMode.PrimaryKey)
        {
            var pk = opts.PrimaryKeyField?.Trim();
            if (string.IsNullOrWhiteSpace(pk))
                return (false, "", "primary_key_field_not_configured");

            if (!doc.TryGetValue(pk, out var v) || v.IsBsonNull)
                return (false, "", $"primary_key_field_missing_or_null:{pk}");

            var primaryKeyValue = NormalizeKeyPart(v);
            if (string.IsNullOrWhiteSpace(primaryKeyValue))
                return (false, "", $"primary_key_field_empty_after_normalize:{pk}");

            return (true, primaryKeyValue, "");
        }

        // CandidateKey mode
        var fields = opts.CandidateKeyFields ?? Array.Empty<string>();
        if (fields.Length == 0)
            return (false, "", "candidate_key_fields_not_configured");

        var joiner = opts.KeyJoiner ?? "";

        var parts = new List<string>(fields.Length);
        foreach (var f0 in fields)
        {
            var f = (f0 ?? "").Trim();
            if (string.IsNullOrWhiteSpace(f))
                return (false, "", "candidate_key_fields_contains_empty_name");

            if (!doc.TryGetValue(f, out var v) || v.IsBsonNull)
                return (false, "", $"candidate_key_field_missing_or_null:{f}");

            var part = NormalizeKeyPart(v);
            if (string.IsNullOrWhiteSpace(part))
                return (false, "", $"candidate_key_field_empty_after_normalize:{f}");

            parts.Add(part);
        }

        var candidateKeyValue = string.Join(joiner, parts);
        if (string.IsNullOrWhiteSpace(candidateKeyValue))
            return (false, "", "candidate_key_join_result_empty");

        return (true, candidateKeyValue, "");
    }

    private static string NormalizeKeyPart(BsonValue v)
    {
        if (v is null || v.IsBsonNull) return string.Empty;

        try
        {
            switch (v.BsonType)
            {
                case BsonType.String:
                    return (v.AsString ?? string.Empty).Trim();

                case BsonType.Int32:
                    return v.AsInt32.ToString(CultureInfo.InvariantCulture);

                case BsonType.Int64:
                    return v.AsInt64.ToString(CultureInfo.InvariantCulture);

                case BsonType.Double:
                    return v.AsDouble.ToString("G17", CultureInfo.InvariantCulture);

                case BsonType.Decimal128:
                    return Decimal128.ToDecimal(v.AsDecimal128).ToString(CultureInfo.InvariantCulture);

                case BsonType.Boolean:
                    return v.AsBoolean ? "true" : "false";

                default:
                    return v.ToString() ?? string.Empty;
            }
        }
        catch
        {
            return string.Empty;
        }
    }

    private static bool TryGetRequiredNonNullField(BsonDocument doc, string key, out string error)
    {
        error = "";

        if (!doc.Contains(key))
        {
            error = "missing";
            return false;
        }

        var v = doc[key];
        if (v.IsBsonNull)
        {
            error = "null_not_allowed";
            return false;
        }

        return true;
    }

    private static BsonValue GetStringOrNull(BsonDocument doc, string key)
    {
        if (!doc.TryGetValue(key, out var v) || v.IsBsonNull)
            return BsonNull.Value;

        return v.BsonType == BsonType.String ? v.AsString : v.ToString();
    }

    private static bool TryGetInt(BsonDocument doc, string key, out int value, out string error)
    {
        value = default;
        error = "";

        if (!doc.TryGetValue(key, out var v) || v.IsBsonNull)
        {
            error = "missing_or_null";
            return false;
        }

        try
        {
            switch (v.BsonType)
            {
                case BsonType.Int32:
                    value = v.AsInt32;
                    return true;

                case BsonType.Int64:
                    {
                        var l = v.AsInt64;
                        if (l < int.MinValue || l > int.MaxValue)
                        {
                            error = "int64_out_of_int32_range";
                            return false;
                        }
                        value = (int)l;
                        return true;
                    }

                case BsonType.Double:
                    {
                        var d = v.AsDouble;
                        if (double.IsNaN(d) || double.IsInfinity(d))
                        {
                            error = "double_nan_or_infinity";
                            return false;
                        }

                        var floored = Math.Floor(d);

                        if (floored < int.MinValue || floored > int.MaxValue)
                        {
                            error = "double_floor_out_of_int32_range";
                            return false;
                        }

                        value = (int)floored;
                        return true;
                    }

                case BsonType.Decimal128:
                    {
                        var dec = Decimal128.ToDecimal(v.AsDecimal128);
                        var floored = decimal.Floor(dec);

                        if (floored < int.MinValue || floored > int.MaxValue)
                        {
                            error = "decimal_floor_out_of_int32_range";
                            return false;
                        }

                        value = (int)floored;
                        return true;
                    }

                case BsonType.String:
                    {
                        var s = v.AsString?.Trim();

                        if (string.IsNullOrWhiteSpace(s))
                        {
                            error = "string_empty";
                            return false;
                        }

                        if (long.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out var l))
                        {
                            if (l < int.MinValue || l > int.MaxValue)
                            {
                                error = "string_int_out_of_int32_range";
                                return false;
                            }
                            value = (int)l;
                            return true;
                        }

                        if (double.TryParse(s, NumberStyles.Float, CultureInfo.InvariantCulture, out var d))
                        {
                            if (double.IsNaN(d) || double.IsInfinity(d))
                            {
                                error = "string_double_nan_or_infinity";
                                return false;
                            }

                            var floored = Math.Floor(d);

                            if (floored < int.MinValue || floored > int.MaxValue)
                            {
                                error = "string_double_floor_out_of_int32_range";
                                return false;
                            }

                            value = (int)floored;
                            return true;
                        }

                        error = "string_not_numeric";
                        return false;
                    }

                default:
                    error = "unsupported_bson_type:" + v.BsonType;
                    return false;
            }
        }
        catch (Exception ex)
        {
            error = "exception:" + ex.GetType().Name;
            return false;
        }
    }

    private static string? GetStringCoerce(BsonDocument doc, string key)
    {
        if (!doc.TryGetValue(key, out var v) || v.IsBsonNull)
            return "";

        return v.BsonType switch
        {
            BsonType.String => v.AsString,
            _ => v.ToString()
        };
    }

    private static string ShrinkRaw(BsonDocument doc, int maxChars)
    {
        var s = doc.ToJson();
        return s.Length <= maxChars ? s : s.Substring(0, maxChars) + "...";
    }
}