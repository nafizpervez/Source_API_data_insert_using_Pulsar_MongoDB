using System.Globalization;
using MongoDB.Bson;
using MongoDB.Driver;
using Microsoft.Extensions.Options;
using SourceIngestor.Worker.Options;

namespace SourceIngestor.Worker.Workers;

public sealed class ValidationProcessor : BackgroundService
{
    private readonly IMongoClient _mongoClient;
    private readonly MongoOptions _mongo;
    private readonly ILogger<ValidationProcessor> _logger;

    public ValidationProcessor(
        IMongoClient mongoClient,
        IOptions<MongoOptions> mongo,
        ILogger<ValidationProcessor> logger)
    {
        _mongoClient = mongoClient;
        _mongo = mongo.Value;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var db = _mongoClient.GetDatabase(_mongo.Database);

        var sourceCol = db.GetCollection<BsonDocument>(_mongo.Collection);
        var validatedCol = db.GetCollection<BsonDocument>(_mongo.ValidatedCollection);
        var invalidCol = db.GetCollection<BsonDocument>(_mongo.InvalidCollection);

        _logger.LogInformation("ValidationProcessor started.");
        _logger.LogInformation("Mongo source: {Db}.{Col}", _mongo.Database, _mongo.Collection);
        _logger.LogInformation("Mongo target(valid): {Db}.{Col}", _mongo.Database, _mongo.ValidatedCollection);
        _logger.LogInformation("Mongo target(invalid): {Db}.{Col}", _mongo.Database, _mongo.InvalidCollection);


        // Watermark for this process lifetime
        ObjectId? lastSeenId = null;

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                // Pull a small batch each loop to avoid heavy loads
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

                    // Expect your MongoWriter schema
                    var sourceUrl = src.GetValue("sourceUrl", BsonNull.Value);
                    var fetchedAtLocalText = src.GetValue("fetchedAtLocalText", BsonNull.Value);
                    var timeZoneId = src.GetValue("timeZoneId", BsonNull.Value);
                    var typeMap = src.GetValue("typeMap", new BsonDocument());

                    if (!src.TryGetValue("payload", out var payloadVal) || payloadVal.BsonType != BsonType.Array)
                    {
                        // Keep only the latest output docs
                        await validatedCol.DeleteManyAsync(FilterDefinition<BsonDocument>.Empty, stoppingToken);
                        await invalidCol.DeleteManyAsync(FilterDefinition<BsonDocument>.Empty, stoppingToken);

                        var errArr = new BsonArray
                        {
                            new BsonDocument
                            {
                                { "index", -1 },
                                { "reason", "missing_or_non_array_payload" }
                            }
                        };

                        await UpsertValidatedBatch(
                            validatedCol,
                            srcId,
                            sourceUrl,
                            fetchedAtLocalText,
                            timeZoneId,
                            typeMap,
                            validatedItems: new BsonArray(),
                            validCount: 0,
                            invalidCount: 0,
                            errors: errArr,
                            stoppingToken);

                        await UpsertInvalidBatch(
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

                        continue;
                    }

                    var payload = payloadVal.AsBsonArray;

                    var validatedItemsArr = new BsonArray();
                    var invalidItemsArr = new BsonArray();
                    var errorsArr = new BsonArray();

                    var validCount = 0;
                    var invalidCount = 0;

                    for (var i = 0; i < payload.Count; i++)
                    {
                        var item = payload[i];

                        if (item.BsonType != BsonType.Document)
                        {
                            invalidCount++;
                            errorsArr.Add(new BsonDocument
                            {
                                { "index", i },
                                { "reason", "payload_item_not_document" },
                                { "bsonType", item.BsonType.ToString() }
                            });
                            continue;
                        }

                        var doc = item.AsBsonDocument;

                        var okUserId = TryGetInt(doc, "userId", out var userId, out var userIdErr);
                        var okId = TryGetInt(doc, "id", out var id, out var idErr);

                        // NEW RULES:
                        // title must exist AND not be null (empty string is allowed)
                        var okTitle = TryGetRequiredNonNullField(doc, "title", out var titleErr);
                        var title = GetStringCoerce(doc, "title"); // we still coerce non-string -> string

                        // body must exist AND not be null (empty string is allowed)
                        var okBody = TryGetRequiredNonNullField(doc, "body", out var bodyErr);
                        var body = GetStringOrNull(doc, "body"); // keep as string value, null if missing/null (we'll mark invalid)

                        if (!okUserId || !okId || !okTitle || !okBody)
                        {
                            invalidCount++;

                            var err = new BsonDocument { { "index", i } };
                            if (!okUserId) err.Add("userId", userIdErr);
                            if (!okId) err.Add("id", idErr);
                            if (!okTitle) err.Add("title", titleErr);
                            if (!okBody) err.Add("body", bodyErr);

                            err.Add("raw", ShrinkRaw(doc, maxChars: 500));
                            errorsArr.Add(err);

                            // Store invalid item snapshot in InvalidSourceData payload
                            invalidItemsArr.Add(new BsonDocument
                            {
                                { "userId", doc.GetValue("userId", BsonNull.Value) },
                                { "id", doc.GetValue("id", BsonNull.Value) },
                                { "title", doc.GetValue("title", BsonNull.Value) },
                                { "body", doc.GetValue("body", BsonNull.Value) }
                            });

                            continue;
                        }

                        // We build a validated document that matches the DTO intent
                        // Using PascalCase keys is fine, but you can keep lower-case if you prefer.
                        var validated = new BsonDocument
                        {
                            { "UserId", userId },
                            { "Id", id },
                            { "Title", title ?? "" },
                            { "Body", body.AsString ?? "" } // body is guaranteed non-null here
                        };
    
                        validatedItemsArr.Add(validated);
                        validCount++;
                    }

                    await validatedCol.DeleteManyAsync(FilterDefinition<BsonDocument>.Empty, stoppingToken);
                    await invalidCol.DeleteManyAsync(FilterDefinition<BsonDocument>.Empty, stoppingToken);

                    await UpsertValidatedBatch(
                        validatedCol,
                        srcId,
                        sourceUrl,
                        fetchedAtLocalText,
                        timeZoneId,
                        typeMap,
                        validatedItemsArr,
                        validCount,
                        invalidCount,
                        errorsArr,
                        stoppingToken);

                    await UpsertInvalidBatch(
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


                    _logger.LogInformation(
                        "Validated batch sourceDocId={SourceDocId} valid={Valid} invalid={Invalid} target={Db}.{Col}",
                        srcId, validCount, invalidCount, _mongo.Database, _mongo.ValidatedCollection);
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

    private static async Task UpsertValidatedBatch(
        IMongoCollection<BsonDocument> validatedCol,
        ObjectId sourceDocId,
        BsonValue sourceUrl,
        BsonValue fetchedAtLocalText,
        BsonValue timeZoneId,
        BsonValue typeMap,
        BsonArray validatedItems,
        int validCount,
        int invalidCount,
        BsonArray errors,
        CancellationToken ct)
    {

        var validatedDoc = new BsonDocument
        {
            { "sourceDocId", sourceDocId },
            { "sourceUrl", sourceUrl },
            { "fetchedAtLocalText", fetchedAtLocalText },
            { "timeZoneId", timeZoneId },
            { "itemCount", validatedItems.Count },
            { "validCount", validCount },
            { "invalidCount", invalidCount },
            { "typeMapSource", typeMap }, // original map from source batch doc

            // add fixed converted type map for the validated payload 
            { "typeMapConverted", new BsonDocument
                {
                    { "userId", "int" },
                    { "id", "int" },
                    { "title", "string" },
                    { "body", "string" }
                }
            },
            { "payload", validatedItems },
            // { "errors", errors }
        };

        var filter = Builders<BsonDocument>.Filter.Eq("sourceDocId", sourceDocId);

        await validatedCol.ReplaceOneAsync(
            filter,
            validatedDoc,
            new ReplaceOptions { IsUpsert = true },
            ct);
    }

    private static async Task UpsertInvalidBatch(
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
    var invalidDoc = new BsonDocument
    {
        { "sourceDocId", sourceDocId },
        { "sourceUrl", sourceUrl },
        { "fetchedAtLocalText", fetchedAtLocalText },
        { "timeZoneId", timeZoneId },

        { "itemCount", invalidItems.Count },
        { "invalidCount", invalidCount },

        { "typeMapSource", typeMap },

        // Optional: document expected/validated types (same as your converted schema)
        { "typeMapConverted", new BsonDocument
            {
                { "userId", "int" },
                { "id", "int" },
                { "title", "string" },
                { "body", "string" }
            }
        },

        { "payload", invalidItems },
        // { "errors", errors }
    };

    var filter = Builders<BsonDocument>.Filter.Eq("sourceDocId", sourceDocId);

    await invalidCol.ReplaceOneAsync(
        filter,
        invalidDoc,
        new ReplaceOptions { IsUpsert = true },
        ct);
}

// title/body must exist and must not be null (empty string is acceptable)
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
                        var rounded = Math.Truncate(d);
                        if (Math.Abs(d - rounded) > 0.0000001)
                        {
                            error = "double_not_integer";
                            return false;
                        }
                        if (rounded < int.MinValue || rounded > int.MaxValue)
                        {
                            error = "double_out_of_int32_range";
                            return false;
                        }
                        value = (int)rounded;
                        return true;
                    }

                case BsonType.Decimal128:
                    {
                        var dec = Decimal128.ToDecimal(v.AsDecimal128);
                        if (dec != decimal.Truncate(dec))
                        {
                            error = "decimal_not_integer";
                            return false;
                        }
                        if (dec < int.MinValue || dec > int.MaxValue)
                        {
                            error = "decimal_out_of_int32_range";
                            return false;
                        }
                        value = (int)dec;
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

                        // Allow numeric strings: "12"
                        if (int.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out var i))
                        {
                            value = i;
                            return true;
                        }

                        // Also allow "12.0" but reject "12.5"
                        if (double.TryParse(s, NumberStyles.Float, CultureInfo.InvariantCulture, out var d))
                        {
                            var rounded = Math.Truncate(d);
                            if (Math.Abs(d - rounded) > 0.0000001)
                            {
                                error = "string_number_not_integer";
                                return false;
                            }
                            if (rounded < int.MinValue || rounded > int.MaxValue)
                            {
                                error = "string_number_out_of_int32_range";
                                return false;
                            }
                            value = (int)rounded;
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
            _ => v.ToString() // coercion to keep pipeline moving
        };
    }

    private static BsonValue GetNullableStringCoerce(BsonDocument doc, string key)
    {
        if (!doc.TryGetValue(key, out var v) || v.IsBsonNull)
            return BsonNull.Value;

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