// src/SourceIngestor.Worker/Workers/DestinationCheckProcessor.cs
using System.Globalization;
using System.Net;
using System.Text;
using System.Text.Json;
using DotPulsar;
using DotPulsar.Abstractions;
using DotPulsar.Extensions;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;
using SourceIngestor.Worker.Messaging.Contracts;
using SourceIngestor.Worker.Options;

namespace SourceIngestor.Worker.Workers;

public sealed class DestinationCheckProcessor : BackgroundService
{
    // Hard-failure retry plan (total 4 tries): immediate, +1m, +3m, +9m then DLQ
    private static readonly TimeSpan[] RetryDelays =
    [
        TimeSpan.FromMinutes(1),
        TimeSpan.FromMinutes(3),
        TimeSpan.FromMinutes(9)
    ];

    // Soft-gate retry delay (DO NOT consume retry budget)
    private static readonly TimeSpan GateDelay = TimeSpan.FromSeconds(30);

    // Snapshot IDs (single-doc tables)
    private const string SourceLatestId = "source_latest";
    private const string ValidLatestId = "valid_latest";
    private const string InvalidLatestId = "invalid_latest";
    private const string DuplicateLatestId = "duplicate_latest";

    private const string DestinationLatestId = "destination_latest";
    private const string ValidDestinationLatestId = "valid_destination_latest";

    private const string SkipLatestId = "skip_latest";
    private const string UpdateLatestId = "update_latest";
    private const string DeleteLatestId = "delete_latest";
    private const string InsertLatestId = "insert_latest";

    private const string FinalLatestId = "final_destination_latest";
    private const string CountLatestId = "count_latest";

    private readonly IPulsarClient _pulsarClient;
    private readonly IHttpClientFactory _httpFactory;
    private readonly IMongoClient _mongoClient;

    private readonly PulsarOptions _pulsar;
    private readonly MongoOptions _mongo;
    private readonly ArcGisPortalOptions _portal;
    private readonly DestinationApiOptions _dest;
    private readonly DuplicationOptions _dup;

    private readonly ILogger<DestinationCheckProcessor> _logger;

    private string? _token;
    private DateTimeOffset _tokenExpiresAtUtc = DateTimeOffset.MinValue;

    public DestinationCheckProcessor(
        IPulsarClient pulsarClient,
        IHttpClientFactory httpFactory,
        IMongoClient mongoClient,
        IOptions<PulsarOptions> pulsar,
        IOptions<MongoOptions> mongo,
        IOptions<ArcGisPortalOptions> portal,
        IOptions<DestinationApiOptions> dest,
        IOptions<DuplicationOptions> dup,
        ILogger<DestinationCheckProcessor> logger)
    {
        _pulsarClient = pulsarClient;
        _httpFactory = httpFactory;
        _mongoClient = mongoClient;

        _pulsar = pulsar.Value;
        _mongo = mongo.Value;
        _portal = portal.Value;
        _dest = dest.Value;
        _dup = dup.Value;

        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (string.IsNullOrWhiteSpace(_pulsar.DestinationCheckTopic))
            throw new InvalidOperationException("Pulsar.DestinationCheckTopic is empty.");

        if (string.IsNullOrWhiteSpace(_dest.FeatureLayerUrl))
            throw new InvalidOperationException("DestinationApi.FeatureLayerUrl is empty.");

        // ✅ Pre-create all snapshot docs so Compass shows all collections immediately
        await EnsureAllSnapshotDocsExist(stoppingToken);

        await using var consumer = _pulsarClient.NewConsumer(Schema.String)
            .Topic(_pulsar.DestinationCheckTopic)
            .SubscriptionName(_pulsar.Subscription + "-destination-orchestrator")
            .InitialPosition(SubscriptionInitialPosition.Earliest)
            .Create();

        await using var producer = _pulsarClient.NewProducer(Schema.String)
            .Topic(_pulsar.DestinationCheckTopic)
            .Create();

        await using var dlqProducer = _pulsarClient.NewProducer(Schema.String)
            .Topic(_pulsar.DlqTopic)
            .Create();

        // Seed exactly one orchestration job on startup
        var seedJobId = ObjectId.GenerateNewId().ToString();
        var seed = new DestinationCheckJob(seedJobId, _dest.FeatureLayerUrl.TrimEnd('/'), 0, DateTimeOffset.UtcNow);
        await producer.Send(JsonSerializer.Serialize(seed), stoppingToken);

        _logger.LogInformation("Seeded DestinationCheckJob jobId={JobId} try=1 url={Url}", seedJobId, seed.Url);

        await foreach (var msg in consumer.Messages(stoppingToken))
        {
            try
            {
                var payload = msg.Value();
                var job = JsonSerializer.Deserialize<DestinationCheckJob>(payload);
                if (job is null || string.IsNullOrWhiteSpace(job.JobId) || string.IsNullOrWhiteSpace(job.Url))
                {
                    await consumer.Acknowledge(msg, stoppingToken);
                    continue;
                }

                if (!ObjectId.TryParse(job.JobId, out var jobObjectId))
                {
                    await consumer.Acknowledge(msg, stoppingToken);
                    continue;
                }

                var tryNo = job.Attempt + 1;

                // Gate 1: Source_Data must exist
                var sourceGate = await TryLoadLatestSourceMergedAsync(stoppingToken);
                if (!sourceGate.Success)
                {
                    // ✅ FIX: waiting for source is NOT a hard failure, do NOT DLQ, do NOT consume retry budget.
                    _logger.LogWarning(
                        "PIPE_GATE_NOT_READY jobId={JobId} try={TryNo} reason={Reason} => will recheck in {Delay}",
                        job.JobId, tryNo, sourceGate.Reason, GateDelay);

                    await RequeueSameAttempt(job, producer, GateDelay, stoppingToken);

                    await consumer.Acknowledge(msg, stoppingToken);
                    continue;
                }

                // Gate 2: Destination must be alive (this IS a hard failure)
                var alive = await IsDestinationAlive(job.Url, stoppingToken);
                if (!alive.Success)
                {
                    _logger.LogWarning("DEST_DOWN jobId={JobId} try={TryNo} reason={Reason}", job.JobId, tryNo, alive.Reason);
                    await ScheduleRetryOrStop(job, jobObjectId, producer, dlqProducer, $"dest_down:{alive.Reason}", stoppingToken);
                    await consumer.Acknowledge(msg, stoppingToken);
                    continue;
                }

                // Run pipeline deterministically
                var run = await RunOnce(job.Url.TrimEnd('/'), sourceGate, stoppingToken);

                if (!run.Success)
                {
                    _logger.LogError("PIPE_FAILED jobId={JobId} try={TryNo} reason={Reason}", job.JobId, tryNo, run.Reason);
                    await ScheduleRetryOrStop(job, jobObjectId, producer, dlqProducer, $"pipe_failed:{run.Reason}", stoppingToken);
                    await consumer.Acknowledge(msg, stoppingToken);
                    continue;
                }

                // Success audit
                await WriteOutcomeAsync(
                    _mongo.DestinationSuccessIdCollection,
                    jobObjectId,
                    job.Url,
                    BuildFetchedAtLocalText(_mongo.TimeZoneId),
                    BuildTimeZoneDisplay(_mongo.TimeZoneId),
                    $"Success try={tryNo} | valid={run.ValidCount} dupGroups={run.DuplicateGroups} invalid={run.InvalidCount} | skip={run.SkipCount} update={run.UpdateCount} delete={run.DeleteCount} insert={run.InsertCount} | final={run.FinalCount}",
                    stoppingToken);

                await consumer.Acknowledge(msg, stoppingToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Destination orchestrator failed; leaving unacked for redelivery.");
            }
        }
    }

    // ------------------------------------------------------------
    // ✅ Pre-create snapshot docs
    // ------------------------------------------------------------
    private async Task EnsureAllSnapshotDocsExist(CancellationToken ct)
    {
        var db = _mongoClient.GetDatabase(_mongo.Database);

        // Each of these will create the collection if it doesn't exist
        await EnsureSnapshotDoc(db.GetCollection<BsonDocument>(_mongo.Collection), SourceLatestId, ct);

        await EnsureSnapshotDoc(db.GetCollection<BsonDocument>(_mongo.ValidatedCollection), ValidLatestId, ct);
        await EnsureSnapshotDoc(db.GetCollection<BsonDocument>(_mongo.InvalidCollection), InvalidLatestId, ct);
        await EnsureSnapshotDoc(db.GetCollection<BsonDocument>(_mongo.DuplicatedCollection), DuplicateLatestId, ct);

        await EnsureSnapshotDoc(db.GetCollection<BsonDocument>(_mongo.DestinationCollection), DestinationLatestId, ct);
        await EnsureSnapshotDoc(db.GetCollection<BsonDocument>(_mongo.ValidDestinationCollection), ValidDestinationLatestId, ct);

        await EnsureSnapshotDoc(db.GetCollection<BsonDocument>(_mongo.SkipCollection), SkipLatestId, ct);
        await EnsureSnapshotDoc(db.GetCollection<BsonDocument>(_mongo.UpdateCollection), UpdateLatestId, ct);
        await EnsureSnapshotDoc(db.GetCollection<BsonDocument>(_mongo.DeleteCollection), DeleteLatestId, ct);
        await EnsureSnapshotDoc(db.GetCollection<BsonDocument>(_mongo.InsertCollection), InsertLatestId, ct);

        await EnsureSnapshotDoc(db.GetCollection<BsonDocument>(_mongo.FinalDestinationCollection), FinalLatestId, ct);
        await EnsureSnapshotDoc(db.GetCollection<BsonDocument>(_mongo.CountSummaryCollection), CountLatestId, ct);
    }

    private static async Task EnsureSnapshotDoc(IMongoCollection<BsonDocument> col, string id, CancellationToken ct)
    {
        var filter = Builders<BsonDocument>.Filter.Eq("_id", id);

        var update = Builders<BsonDocument>.Update
            .SetOnInsert("_id", id)
            .SetOnInsert("status", "init")
            .SetOnInsert("itemCount", 0)
            .SetOnInsert("payload", new BsonArray());

        await col.UpdateOneAsync(filter, update, new UpdateOptions { IsUpsert = true }, ct);
    }

    // ------------------------------------------------------------
    // Requeue same attempt (soft gate)
    // ------------------------------------------------------------
    private async Task RequeueSameAttempt(DestinationCheckJob job, IProducer<string> producer, TimeSpan delay, CancellationToken ct)
    {
        var deliverAtUtc = DateTimeOffset.UtcNow.Add(delay);

        await producer.NewMessage()
            .DeliverAt(deliverAtUtc.UtcDateTime)
            .Send(JsonSerializer.Serialize(job), ct);
    }

    private sealed record SourceGate(bool Success, string Reason, string JobId, BsonValue SourceUrl, BsonValue FetchedAtLocalText, BsonValue TimeZoneId, BsonArray Payload);
    private sealed record RunResult(bool Success, string Reason, int ValidCount, int InvalidCount, int DuplicateGroups, int SkipCount, int UpdateCount, int DeleteCount, int InsertCount, int FinalCount);

    // -----------------------------
    // PIPELINE RUN (same logic as your current file)
    // -----------------------------
    private async Task<RunResult> RunOnce(string layerUrl, SourceGate source, CancellationToken ct)
    {
        var nowLocalText = BuildFetchedAtLocalText(_mongo.TimeZoneId);
        var tzDisplay = BuildTimeZoneDisplay(_mongo.TimeZoneId);

        var db = _mongoClient.GetDatabase(_mongo.Database);

        var sourceSnapCol = db.GetCollection<BsonDocument>(_mongo.Collection);
        var validCol = db.GetCollection<BsonDocument>(_mongo.ValidatedCollection);
        var invalidCol = db.GetCollection<BsonDocument>(_mongo.InvalidCollection);
        var dupCol = db.GetCollection<BsonDocument>(_mongo.DuplicatedCollection);

        var destCol = db.GetCollection<BsonDocument>(_mongo.DestinationCollection);
        var validDestCol = db.GetCollection<BsonDocument>(_mongo.ValidDestinationCollection);

        var skipCol = db.GetCollection<BsonDocument>(_mongo.SkipCollection);
        var updateCol = db.GetCollection<BsonDocument>(_mongo.UpdateCollection);
        var deleteCol = db.GetCollection<BsonDocument>(_mongo.DeleteCollection);
        var insertCol = db.GetCollection<BsonDocument>(_mongo.InsertCollection);

        var finalCol = db.GetCollection<BsonDocument>(_mongo.FinalDestinationCollection);
        var countCol = db.GetCollection<BsonDocument>(_mongo.CountSummaryCollection);

        // 1) Source snapshot
        await UpsertSnapshot(sourceSnapCol, SourceLatestId, new BsonDocument
        {
            { "status", "ok" },
            { "jobId", source.JobId },
            { "sourceUrl", source.SourceUrl },
            { "fetchedAtLocalText", source.FetchedAtLocalText },
            { "timeZoneId", source.TimeZoneId },
            { "itemCount", source.Payload.Count },
            { "payload", source.Payload }
        }, ct);

        // 2) Validate + normalize + full-row duplicates
        var (validArr, invalidArr, dupGroupsArr) = ValidateAndSplit(source.Payload);

        await UpsertSnapshot(validCol, ValidLatestId, new BsonDocument
        {
            { "status", "ok" },
            { "sourceJobId", source.JobId },
            { "sourceUrl", source.SourceUrl },
            { "fetchedAtLocalText", source.FetchedAtLocalText },
            { "timeZoneId", source.TimeZoneId },
            { "validCount", validArr.Count },
            { "itemCount", validArr.Count },
            { "payload", validArr }
        }, ct);

        await UpsertSnapshot(invalidCol, InvalidLatestId, new BsonDocument
        {
            { "status", "ok" },
            { "sourceJobId", source.JobId },
            { "sourceUrl", source.SourceUrl },
            { "fetchedAtLocalText", source.FetchedAtLocalText },
            { "timeZoneId", source.TimeZoneId },
            { "invalidCount", invalidArr.Count },
            { "itemCount", invalidArr.Count },
            { "payload", invalidArr }
        }, ct);

        await UpsertSnapshot(dupCol, DuplicateLatestId, new BsonDocument
        {
            { "status", "ok" },
            { "sourceJobId", source.JobId },
            { "sourceUrl", source.SourceUrl },
            { "fetchedAtLocalText", source.FetchedAtLocalText },
            { "timeZoneId", source.TimeZoneId },
            { "duplicateGroupCount", dupGroupsArr.Count },
            { "itemCount", dupGroupsArr.Count },
            { "payload", dupGroupsArr }
        }, ct);

        // 3) Destination snapshot ONCE
        var token = await GetPortalToken(ct);
        var (oidField, objectIds) = await QueryAllObjectIds(layerUrl, token, ct);
        var destPayload = await QueryDestinationAll(layerUrl, token, oidField, objectIds, ct);

        await UpsertSnapshot(destCol, DestinationLatestId, new BsonDocument
        {
            { "status", "ok" },
            { "layerUrl", layerUrl },
            { "objectIdField", oidField },
            { "fetchedAtLocalText", nowLocalText },
            { "timeZoneId", tzDisplay },
            { "itemCount", destPayload.Count },
            { "payload", destPayload }
        }, ct);

        // 4) Valid_Destination_Data projection
        var validDestPayload = BuildValidDestinationProjection(destPayload, oidField);

        await UpsertSnapshot(validDestCol, ValidDestinationLatestId, new BsonDocument
        {
            { "status", "ok" },
            { "layerUrl", layerUrl },
            { "objectIdField", oidField },
            { "fetchedAtLocalText", nowLocalText },
            { "timeZoneId", tzDisplay },
            { "itemCount", validDestPayload.Count },
            { "payload", validDestPayload }
        }, ct);

        // 5) Diffs
        var diffs = ComputeDiffs(validArr, validDestPayload);

        await UpsertSnapshot(skipCol, SkipLatestId, diffs.SkipDoc(nowLocalText, tzDisplay, layerUrl, oidField), ct);
        await UpsertSnapshot(updateCol, UpdateLatestId, diffs.UpdateDoc(nowLocalText, tzDisplay, layerUrl, oidField), ct);
        await UpsertSnapshot(deleteCol, DeleteLatestId, diffs.DeleteDoc(nowLocalText, tzDisplay, layerUrl, oidField), ct);
        await UpsertSnapshot(insertCol, InsertLatestId, diffs.InsertDoc(nowLocalText, tzDisplay, layerUrl, oidField), ct);

        // 6) applyEdits
        var apply = await ApplyEdits(layerUrl, token, oidField, diffs, ct);
        if (!apply.Success)
            return new RunResult(false, apply.Reason, validArr.Count, invalidArr.Count, dupGroupsArr.Count, diffs.SkipCount, diffs.UpdateCount, diffs.DeleteCount, diffs.InsertCount, 0);

        // 7) Final snapshot after edits
        var finalPayload = await QueryDestinationAll(layerUrl, token, oidField, objectIds, ct, outFieldsOverride: "*");

        await UpsertSnapshot(finalCol, FinalLatestId, new BsonDocument
        {
            { "status", "ok" },
            { "layerUrl", layerUrl },
            { "objectIdField", oidField },
            { "fetchedAtLocalText", nowLocalText },
            { "timeZoneId", tzDisplay },
            { "itemCount", finalPayload.Count },
            { "payload", finalPayload }
        }, ct);

        // 8) Count summary
        await UpsertSnapshot(countCol, CountLatestId, new BsonDocument
        {
            { "status", "ok" },
            { "runAtLocalText", nowLocalText },
            { "timeZoneId", tzDisplay },
            { "sourceJobId", source.JobId },
            { "sourceCount", source.Payload.Count },
            { "validCount", validArr.Count },
            { "invalidCount", invalidArr.Count },
            { "duplicateGroupCount", dupGroupsArr.Count },
            { "destinationCount", validDestPayload.Count },
            { "skipCount", diffs.SkipCount },
            { "updateCount", diffs.UpdateCount },
            { "deleteCount", diffs.DeleteCount },
            { "insertCount", diffs.InsertCount },
            { "finalCount", finalPayload.Count }
        }, ct);

        return new RunResult(true, "ok", validArr.Count, invalidArr.Count, dupGroupsArr.Count, diffs.SkipCount, diffs.UpdateCount, diffs.DeleteCount, diffs.InsertCount, finalPayload.Count);
    }

    // -----------------------------
    // SOURCE LOAD (merge latest job)
    // -----------------------------
    private async Task<SourceGate> TryLoadLatestSourceMergedAsync(CancellationToken ct)
    {
        var db = _mongoClient.GetDatabase(_mongo.Database);
        var col = db.GetCollection<BsonDocument>(_mongo.Collection);

        var newest = await col.Find(Builders<BsonDocument>.Filter.Exists("jobId"))
            .Sort(Builders<BsonDocument>.Sort.Descending("_id"))
            .Limit(1)
            .FirstOrDefaultAsync(ct);

        if (newest is null)
            return new SourceGate(false, "no_source_batches_found", "", BsonNull.Value, BsonNull.Value, BsonNull.Value, new BsonArray());

        var jobId = newest.GetValue("jobId", "").ToString() ?? "";
        if (string.IsNullOrWhiteSpace(jobId))
            return new SourceGate(false, "latest_source_missing_jobId", "", BsonNull.Value, BsonNull.Value, BsonNull.Value, new BsonArray());

        var parts = await col.Find(Builders<BsonDocument>.Filter.Eq("jobId", jobId))
            .Sort(Builders<BsonDocument>.Sort.Ascending("partNo"))
            .ToListAsync(ct);

        var merged = new BsonArray();
        BsonValue sourceUrl = BsonNull.Value;
        BsonValue fetchedAtLocalText = BsonNull.Value;
        BsonValue timeZoneId = BsonNull.Value;

        foreach (var p in parts)
        {
            sourceUrl = p.GetValue("sourceUrl", sourceUrl);
            fetchedAtLocalText = p.GetValue("fetchedAtLocalText", fetchedAtLocalText);
            timeZoneId = p.GetValue("timeZoneId", timeZoneId);

            if (p.TryGetValue("payload", out var pv) && pv.BsonType == BsonType.Array)
                foreach (var item in pv.AsBsonArray)
                    merged.Add(item);
        }

        if (merged.Count == 0)
            return new SourceGate(false, "latest_job_has_empty_payload", jobId, sourceUrl, fetchedAtLocalText, timeZoneId, merged);

        return new SourceGate(true, "ok", jobId, sourceUrl, fetchedAtLocalText, timeZoneId, merged);
    }

    // -----------------------------
    // VALIDATION (fixes definite assignment issue)
    // -----------------------------
    private (BsonArray valid, BsonArray invalid, BsonArray duplicateGroups) ValidateAndSplit(BsonArray payload)
    {
        var normalized = new List<(int userId, int id, string? title, string? body, BsonDocument outDoc, string rowKey)>();
        var invalid = new BsonArray();

        for (int i = 0; i < payload.Count; i++)
        {
            if (payload[i].BsonType != BsonType.Document)
            {
                invalid.Add(new BsonDocument { { "index", i }, { "reason", "payload_item_not_document" } });
                continue;
            }

            var doc = payload[i].AsBsonDocument;

            // ✅ Avoid short-circuit so errors are always definitely assigned
            var okUser = TryFloorInt(doc.GetValue("userId", BsonNull.Value), out var userId, out var uErr);
            var okId = TryFloorInt(doc.GetValue("id", BsonNull.Value), out var id, out var idErr);

            if (!okUser || !okId)
            {
                invalid.Add(new BsonDocument
                {
                    { "index", i },
                    { "reason", "userId_or_id_invalid" },
                    { "userIdErr", okUser ? "" : uErr },
                    { "idErr", okId ? "" : idErr },
                    { "raw", Shrink(doc, 400) }
                });
                continue;
            }

            var titleVal = doc.GetValue("title", BsonNull.Value);
            var bodyVal = doc.GetValue("body", BsonNull.Value);

            if (!IsStringOrNull(titleVal) || !IsStringOrNull(bodyVal))
            {
                invalid.Add(new BsonDocument
                {
                    { "index", i },
                    { "reason", "title_or_body_not_string_or_null" },
                    { "raw", Shrink(doc, 400) }
                });
                continue;
            }

            var title = titleVal.IsBsonNull ? null : (titleVal.AsString ?? "");
            var body = bodyVal.IsBsonNull ? null : (bodyVal.AsString ?? "");

            var rowKey = $"{userId}|{id}|{(title is null ? "<null>" : title)}|{(body is null ? "<null>" : body)}";

            var outDoc = new BsonDocument
            {
                { "userId", userId },
                { "id", id },
                { "title", title is null ? BsonNull.Value : new BsonString(title) },
                { "body", body is null ? BsonNull.Value : new BsonString(body) },
                { _dup.ComputedKeyFieldName ?? "Candidate_Key_combination", $"{userId}|{id}" }
            };

            normalized.Add((userId, id, title, body, outDoc, rowKey));
        }

        var groups = normalized.GroupBy(x => x.rowKey).ToList();
        var duplicateGroups = new BsonArray();

        var dupRowKeys = new HashSet<string>(groups.Where(g => g.Count() > 1).Select(g => g.Key), StringComparer.Ordinal);

        foreach (var g in groups.Where(g => g.Count() > 1))
        {
            var first = g.First();
            duplicateGroups.Add(new BsonDocument
            {
                { "userId", first.userId },
                { "id", first.id },
                { "title", first.title is null ? BsonNull.Value : new BsonString(first.title) },
                { "body", first.body is null ? BsonNull.Value : new BsonString(first.body) },
                { _dup.ComputedKeyFieldName ?? "Candidate_Key_combination", $"{first.userId}|{first.id}" },
                { "duplicateCount", g.Count() }
            });
        }

        var valid = new BsonArray();
        foreach (var item in normalized)
        {
            if (dupRowKeys.Contains(item.rowKey))
                continue;

            valid.Add(item.outDoc);
        }

        return (valid, invalid, duplicateGroups);
    }

    private static bool IsStringOrNull(BsonValue v) => v.IsBsonNull || v.BsonType == BsonType.String;

    private static bool TryFloorInt(BsonValue v, out int value, out string error)
    {
        value = default;
        error = "";

        if (v is null || v.IsBsonNull)
        {
            error = "null";
            return false;
        }

        if (v.BsonType == BsonType.String)
        {
            var s = (v.AsString ?? "").Trim();
            if (s.Length == 0) { error = "empty"; return false; }
            if (string.Equals(s, "null", StringComparison.OrdinalIgnoreCase)) { error = "string_null"; return false; }

            if (double.TryParse(s, NumberStyles.Float, CultureInfo.InvariantCulture, out var d))
            {
                var f = Math.Floor(d);
                if (f < int.MinValue || f > int.MaxValue) { error = "range"; return false; }
                value = (int)f;
                return true;
            }

            error = "not_numeric";
            return false;
        }

        try
        {
            switch (v.BsonType)
            {
                case BsonType.Int32: value = v.AsInt32; return true;
                case BsonType.Int64:
                    if (v.AsInt64 < int.MinValue || v.AsInt64 > int.MaxValue) { error = "range"; return false; }
                    value = (int)v.AsInt64; return true;
                case BsonType.Double:
                    if (double.IsNaN(v.AsDouble) || double.IsInfinity(v.AsDouble)) { error = "nan_inf"; return false; }
                    var fd = Math.Floor(v.AsDouble);
                    if (fd < int.MinValue || fd > int.MaxValue) { error = "range"; return false; }
                    value = (int)fd; return true;
                case BsonType.Decimal128:
                    var dec = Decimal128.ToDecimal(v.AsDecimal128);
                    var fdec = decimal.Floor(dec);
                    if (fdec < int.MinValue || fdec > int.MaxValue) { error = "range"; return false; }
                    value = (int)fdec; return true;
                default:
                    error = "unsupported:" + v.BsonType;
                    return false;
            }
        }
        catch (Exception ex)
        {
            error = "exception:" + ex.GetType().Name;
            return false;
        }
    }

    private static string Shrink(BsonDocument doc, int max)
    {
        var s = doc.ToJson();
        return s.Length <= max ? s : s.Substring(0, max) + "...";
    }

    // -----------------------------
    // DESTINATION PROJECTION + DIFFS
    // -----------------------------
    private static BsonArray BuildValidDestinationProjection(BsonArray destinationRaw, string oidField)
    {
        var outArr = new BsonArray();

        foreach (var item in destinationRaw)
        {
            if (item.BsonType != BsonType.Document) continue;
            var feat = item.AsBsonDocument;

            if (!feat.TryGetValue("attributes", out var attrsVal) || attrsVal.BsonType != BsonType.Document)
                continue;

            var attrs = attrsVal.AsBsonDocument;

            outArr.Add(new BsonDocument
            {
                { "objectid", GetFieldCI(attrs, oidField) }, // OBJECTID
                { "userid", GetFieldCI(attrs, "userid") },   // userid (your schema)
                { "id", GetFieldCI(attrs, "id") },
                { "title", GetFieldCI(attrs, "title") },
                { "body", GetFieldCI(attrs, "body") }
            });
        }

        return outArr;
    }

    private sealed record DiffSet(
        BsonArray SkipPayload,
        BsonArray UpdatePayload,
        BsonArray DeletePayload,
        BsonArray InsertPayload,
        int SkipCount,
        int UpdateCount,
        int DeleteCount,
        int InsertCount)
    {
        public BsonDocument SkipDoc(string at, string tz, string layerUrl, string oidField) => new()
        {
            { "_id", SkipLatestId }, { "status", "ok" }, { "layerUrl", layerUrl }, { "objectIdField", oidField },
            { "fetchedAtLocalText", at }, { "timeZoneId", tz }, { "skipCount", SkipCount }, { "itemCount", SkipPayload.Count }, { "payload", SkipPayload }
        };

        public BsonDocument UpdateDoc(string at, string tz, string layerUrl, string oidField) => new()
        {
            { "_id", UpdateLatestId }, { "status", "ok" }, { "layerUrl", layerUrl }, { "objectIdField", oidField },
            { "fetchedAtLocalText", at }, { "timeZoneId", tz }, { "updateCount", UpdateCount }, { "itemCount", UpdatePayload.Count }, { "payload", UpdatePayload }
        };

        public BsonDocument DeleteDoc(string at, string tz, string layerUrl, string oidField) => new()
        {
            { "_id", DeleteLatestId }, { "status", "ok" }, { "layerUrl", layerUrl }, { "objectIdField", oidField },
            { "fetchedAtLocalText", at }, { "timeZoneId", tz }, { "deleteCount", DeleteCount }, { "itemCount", DeletePayload.Count }, { "payload", DeletePayload }
        };

        public BsonDocument InsertDoc(string at, string tz, string layerUrl, string oidField) => new()
        {
            { "_id", InsertLatestId }, { "status", "ok" }, { "layerUrl", layerUrl }, { "objectIdField", oidField },
            { "fetchedAtLocalText", at }, { "timeZoneId", tz }, { "insertCount", InsertCount }, { "itemCount", InsertPayload.Count }, { "payload", InsertPayload }
        };
    }

    private DiffSet ComputeDiffs(BsonArray validSource, BsonArray validDest)
    {
        var sourceByKey = new Dictionary<string, BsonDocument>(StringComparer.Ordinal);
        foreach (var v in validSource)
        {
            if (v.BsonType != BsonType.Document) continue;
            var d = v.AsBsonDocument;

            if (!TryFloorInt(d.GetValue("userId", BsonNull.Value), out var su, out _) ||
                !TryFloorInt(d.GetValue("id", BsonNull.Value), out var si, out _))
                continue;

            sourceByKey[$"{su}|{si}"] = d;
        }

        var destByKey = new Dictionary<string, List<BsonDocument>>(StringComparer.Ordinal);
        foreach (var v in validDest)
        {
            if (v.BsonType != BsonType.Document) continue;
            var d = v.AsBsonDocument;

            if (d.GetValue("objectid", BsonNull.Value).IsBsonNull)
                continue;

            if (!TryFloorInt(d.GetValue("userid", BsonNull.Value), out var du, out _) ||
                !TryFloorInt(d.GetValue("id", BsonNull.Value), out var di, out _))
                continue;

            d["userid"] = du;
            d["id"] = di;

            var key = $"{du}|{di}";
            if (!destByKey.TryGetValue(key, out var list))
            {
                list = new List<BsonDocument>(1);
                destByKey[key] = list;
            }
            list.Add(d);
        }

        var skip = new BsonArray();
        var update = new BsonArray();
        var del = new BsonArray();
        var ins = new BsonArray();

        foreach (var (key, sdoc) in sourceByKey)
        {
            if (!destByKey.TryGetValue(key, out var dlist) || dlist.Count == 0)
            {
                ins.Add(new BsonDocument
                {
                    { "userid", sdoc["userId"] },
                    { "id", sdoc["id"] },
                    { "title", sdoc.GetValue("title", BsonNull.Value) },
                    { "body", sdoc.GetValue("body", BsonNull.Value) },
                    { _dup.ComputedKeyFieldName ?? "Candidate_Key_combination", key }
                });
                continue;
            }

            foreach (var dd in dlist)
            {
                var sameTitle = BsonEqual(sdoc.GetValue("title", BsonNull.Value), dd.GetValue("title", BsonNull.Value));
                var sameBody = BsonEqual(sdoc.GetValue("body", BsonNull.Value), dd.GetValue("body", BsonNull.Value));

                if (sameTitle && sameBody)
                {
                    skip.Add(new BsonDocument
                    {
                        { "objectid", dd["objectid"] },
                        { "userid", dd["userid"] },
                        { "id", dd["id"] },
                        { "title", dd.GetValue("title", BsonNull.Value) },
                        { "body", dd.GetValue("body", BsonNull.Value) },
                        { _dup.ComputedKeyFieldName ?? "Candidate_Key_combination", key }
                    });
                }
                else
                {
                    update.Add(new BsonDocument
                    {
                        { "objectid", dd["objectid"] },
                        { "userid", dd["userid"] },
                        { "id", dd["id"] },
                        { "final_title", sdoc.GetValue("title", BsonNull.Value) },
                        { "final_body", sdoc.GetValue("body", BsonNull.Value) },
                        { "old_title", dd.GetValue("title", BsonNull.Value) },
                        { "old_body", dd.GetValue("body", BsonNull.Value) },
                        { _dup.ComputedKeyFieldName ?? "Candidate_Key_combination", key }
                    });
                }
            }
        }

        foreach (var (key, dlist) in destByKey)
        {
            if (sourceByKey.ContainsKey(key))
                continue;

            foreach (var dd in dlist)
            {
                del.Add(new BsonDocument
                {
                    { "objectid", dd["objectid"] },
                    { "userid", dd["userid"] },
                    { "id", dd["id"] },
                    { "title", dd.GetValue("title", BsonNull.Value) },
                    { "body", dd.GetValue("body", BsonNull.Value) },
                    { _dup.ComputedKeyFieldName ?? "Candidate_Key_combination", key }
                });
            }
        }

        return new DiffSet(skip, update, del, ins, skip.Count, update.Count, del.Count, ins.Count);
    }

    private static bool BsonEqual(BsonValue a, BsonValue b)
    {
        a ??= BsonNull.Value;
        b ??= BsonNull.Value;
        if (a.IsBsonNull && b.IsBsonNull) return true;
        return string.Equals(Norm(a), Norm(b), StringComparison.Ordinal);
    }

    private static string Norm(BsonValue v)
    {
        if (v is null || v.IsBsonNull) return "";
        return v.BsonType == BsonType.String ? (v.AsString ?? "") : (v.ToString() ?? "");
    }

    private static BsonValue GetFieldCI(BsonDocument doc, string field)
    {
        foreach (var e in doc.Elements)
            if (string.Equals(e.Name, field, StringComparison.OrdinalIgnoreCase))
                return e.Value;
        return BsonNull.Value;
    }

    // -----------------------------
    // APPLY EDITS (OBJECTID + userid)
    // -----------------------------
    private sealed record ApplyResult(bool Success, string Reason);

    private async Task<ApplyResult> ApplyEdits(string layerUrl, string token, string oidField, DiffSet diffs, CancellationToken ct)
    {
        var updates = new List<Dictionary<string, object>>();
        foreach (var u in diffs.UpdatePayload)
        {
            if (u.BsonType != BsonType.Document) continue;
            var d = u.AsBsonDocument;

            var oid = GetIntLike(d.GetValue("objectid", BsonNull.Value));
            if (oid is null) continue;

            var attrs = new Dictionary<string, object>
            {
                [oidField] = oid.Value
            };

            if (d.TryGetValue("final_title", out var ft)) attrs["title"] = ft.IsBsonNull ? null! : ft.ToString()!;
            if (d.TryGetValue("final_body", out var fb)) attrs["body"] = fb.IsBsonNull ? null! : fb.ToString()!;

            updates.Add(new Dictionary<string, object> { ["attributes"] = attrs });
        }

        var deletes = new HashSet<int>();
        foreach (var x in diffs.DeletePayload)
        {
            if (x.BsonType != BsonType.Document) continue;
            var d = x.AsBsonDocument;

            var oid = GetIntLike(d.GetValue("objectid", BsonNull.Value));
            if (oid is not null) deletes.Add(oid.Value);
        }

        var inserts = new List<Dictionary<string, object>>();
        foreach (var x in diffs.InsertPayload)
        {
            if (x.BsonType != BsonType.Document) continue;
            var d = x.AsBsonDocument;

            if (!TryFloorInt(d.GetValue("userid", BsonNull.Value), out var du, out _) ||
                !TryFloorInt(d.GetValue("id", BsonNull.Value), out var di, out _))
                continue;

            var attrs = new Dictionary<string, object>
            {
                ["userid"] = du,
                ["id"] = di
            };

            if (d.TryGetValue("title", out var t)) attrs["title"] = t.IsBsonNull ? null! : t.ToString()!;
            if (d.TryGetValue("body", out var b)) attrs["body"] = b.IsBsonNull ? null! : b.ToString()!;

            inserts.Add(new Dictionary<string, object> { ["attributes"] = attrs });
        }

        if (updates.Count == 0 && deletes.Count == 0 && inserts.Count == 0)
            return new ApplyResult(true, "no_edits");

        var form = new Dictionary<string, string>
        {
            ["f"] = "json",
            ["token"] = token
        };

        if (updates.Count > 0) form["updates"] = JsonSerializer.Serialize(updates);
        if (inserts.Count > 0) form["adds"] = JsonSerializer.Serialize(inserts);
        if (deletes.Count > 0) form["deletes"] = string.Join(",", deletes);

        var http = _httpFactory.CreateClient("arcgis");
        var url = $"{layerUrl}/applyEdits";

        using var req = new HttpRequestMessage(HttpMethod.Post, url) { Content = new FormUrlEncodedContent(form) };
        ApplyRefererHeader(req);

        using var res = await http.SendAsync(req, ct);
        var raw = await res.Content.ReadAsStringAsync(ct);

        _logger.LogInformation("applyEdits response (first 2000 chars): {Raw}", raw.Length <= 2000 ? raw : raw.Substring(0, 2000) + "...");

        if (!res.IsSuccessStatusCode)
            return new ApplyResult(false, $"http_{(int)res.StatusCode}");

        using var doc = JsonDocument.Parse(raw);
        if (doc.RootElement.TryGetProperty("error", out var err))
            return new ApplyResult(false, "arcgis_error:" + err.GetRawText());

        return new ApplyResult(true, "ok");
    }

    private static int? GetIntLike(BsonValue v)
    {
        try
        {
            return v.BsonType switch
            {
                BsonType.Int32 => v.AsInt32,
                BsonType.Int64 when v.AsInt64 >= int.MinValue && v.AsInt64 <= int.MaxValue => (int)v.AsInt64,
                BsonType.Double => (int)Math.Floor(v.AsDouble),
                BsonType.Decimal128 => (int)decimal.Floor(Decimal128.ToDecimal(v.AsDecimal128)),
                BsonType.String when int.TryParse(v.AsString, NumberStyles.Integer, CultureInfo.InvariantCulture, out var i) => i,
                _ => null
            };
        }
        catch { return null; }
    }

    // -----------------------------
    // DESTINATION QUERIES
    // -----------------------------
    private async Task<BsonArray> QueryDestinationAll(string layerUrl, string token, string oidField, List<int> objectIds, CancellationToken ct, string? outFieldsOverride = null)
    {
        var batchSize = Math.Max(1, _dest.QueryBatchSize);
        var outFields = outFieldsOverride ?? _dest.OutFields;

        var payload = new BsonArray();
        foreach (var chunk in Chunk(objectIds, batchSize))
        {
            var root = await QueryByObjectIds(layerUrl, token, oidField, chunk, outFields, ct);
            if (!root.TryGetProperty("features", out var featuresEl) || featuresEl.ValueKind != JsonValueKind.Array)
                break;

            foreach (var feat in featuresEl.EnumerateArray())
            {
                if (!feat.TryGetProperty("attributes", out var attrsEl) || attrsEl.ValueKind != JsonValueKind.Object)
                    continue;

                payload.Add(new BsonDocument { { "attributes", BsonDocument.Parse(attrsEl.GetRawText()) } });
            }
        }
        return payload;
    }

    // -----------------------------
    // Retry + audit + utilities
    // -----------------------------
    private async Task ScheduleRetryOrStop(
        DestinationCheckJob job,
        ObjectId jobObjectId,
        IProducer<string> producer,
        IProducer<string> dlqProducer,
        string reason,
        CancellationToken ct)
    {
        var nextAttempt = job.Attempt + 1;

        if (nextAttempt > RetryDelays.Length)
        {
            var lastTry = job.Attempt + 1;

            var dlqPayload = JsonSerializer.Serialize(new
            {
                job.JobId,
                job.Url,
                attempt = job.Attempt,
                tryNo = lastTry,
                failedAtUtc = DateTimeOffset.UtcNow,
                reason
            });

            await dlqProducer.Send(dlqPayload, ct);

            await WriteOutcomeAsync(
                _mongo.DestinationFailedIdCollection,
                jobObjectId,
                job.Url,
                BuildFetchedAtLocalText(_mongo.TimeZoneId),
                BuildTimeZoneDisplay(_mongo.TimeZoneId),
                $"DLQ | stoppedAfterTry={lastTry} | reason={reason}",
                ct);

            return;
        }

        var delay = RetryDelays[nextAttempt - 1];
        var deliverAtUtc = DateTimeOffset.UtcNow.Add(delay);

        var nextJob = job with { Attempt = nextAttempt };

        await producer.NewMessage()
            .DeliverAt(deliverAtUtc.UtcDateTime)
            .Send(JsonSerializer.Serialize(nextJob), ct);
    }

    private async Task WriteOutcomeAsync(
        string collectionName,
        ObjectId id,
        string destinationUrl,
        string fetchedAtLocalText,
        string timeZoneDisplay,
        string pulsarText,
        CancellationToken ct)
    {
        var db = _mongoClient.GetDatabase(_mongo.Database);
        var col = db.GetCollection<BsonDocument>(collectionName);

        var doc = new BsonDocument
        {
            { "_id", id },
            { "destinationUrl", destinationUrl },
            { "fetchedAtLocalText", fetchedAtLocalText },
            { "timeZoneId", timeZoneDisplay },
            { "pulsar", pulsarText }
        };

        await col.ReplaceOneAsync(Builders<BsonDocument>.Filter.Eq("_id", id), doc, new ReplaceOptions { IsUpsert = true }, ct);
    }

    private async Task UpsertSnapshot(IMongoCollection<BsonDocument> col, string id, BsonDocument body, CancellationToken ct)
    {
        body["_id"] = id;
        await col.ReplaceOneAsync(Builders<BsonDocument>.Filter.Eq("_id", id), body, new ReplaceOptions { IsUpsert = true }, ct);
    }

    private sealed record AliveResult(bool Success, HttpStatusCode? StatusCode, string Reason);

    private async Task<AliveResult> IsDestinationAlive(string layerUrl, CancellationToken ct)
    {
        try
        {
            var token = await GetPortalToken(ct);

            var http = _httpFactory.CreateClient("arcgis");
            var qs = new Dictionary<string, string>
            {
                ["where"] = _dest.Where,
                ["returnIdsOnly"] = "true",
                ["f"] = "json",
                ["token"] = token
            };

            using var req = new HttpRequestMessage(HttpMethod.Get, $"{layerUrl.TrimEnd('/')}/query?{ToQueryString(qs)}");
            ApplyRefererHeader(req);

            using var res = await http.SendAsync(req, ct);
            var raw = await res.Content.ReadAsStringAsync(ct);

            if (!res.IsSuccessStatusCode) return new AliveResult(false, res.StatusCode, "non_success_status");

            using var doc = JsonDocument.Parse(raw);
            if (doc.RootElement.TryGetProperty("error", out _)) return new AliveResult(false, res.StatusCode, "arcgis_error_payload");

            return new AliveResult(true, res.StatusCode, "ok");
        }
        catch (Exception ex)
        {
            return new AliveResult(false, null, ex.GetType().Name);
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

    private async Task<string> GetPortalToken(CancellationToken ct)
    {
        if (!string.IsNullOrWhiteSpace(_token) && _tokenExpiresAtUtc > DateTimeOffset.UtcNow.AddMinutes(2))
            return _token!;

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

        using var req = new HttpRequestMessage(HttpMethod.Post, _portal.GenerateTokenUrl) { Content = new FormUrlEncodedContent(form) };
        ApplyRefererHeader(req);

        using var res = await http.SendAsync(req, ct);
        var raw = await res.Content.ReadAsStringAsync(ct);

        if (!res.IsSuccessStatusCode)
            throw new InvalidOperationException($"generateToken failed. status={(int)res.StatusCode} body={raw}");

        using var doc = JsonDocument.Parse(raw);
        var root = doc.RootElement;

        if (root.TryGetProperty("error", out var err))
            throw new InvalidOperationException($"generateToken error: {err.GetRawText()}");

        var token = root.GetProperty("token").GetString() ?? throw new InvalidOperationException("generateToken missing token");

        DateTimeOffset expiresAtUtc = DateTimeOffset.UtcNow.AddMinutes(Math.Max(1, _portal.ExpirationMinutes));
        if (root.TryGetProperty("expires", out var expEl) && expEl.ValueKind == JsonValueKind.Number && expEl.TryGetInt64(out var ms))
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
            ["where"] = _dest.Where,
            ["returnIdsOnly"] = "true",
            ["f"] = "json",
            ["token"] = token
        };

        using var req = new HttpRequestMessage(HttpMethod.Get, $"{layerUrl}/query?{ToQueryString(qs)}");
        ApplyRefererHeader(req);

        using var res = await http.SendAsync(req, ct);
        var raw = await res.Content.ReadAsStringAsync(ct);

        if (!res.IsSuccessStatusCode)
            throw new InvalidOperationException($"returnIdsOnly failed status={(int)res.StatusCode} body={raw}");

        using var doc = JsonDocument.Parse(raw);
        var root = doc.RootElement;

        if (root.TryGetProperty("error", out var err))
            throw new InvalidOperationException($"returnIdsOnly error: {err.GetRawText()}");

        var oidField = root.TryGetProperty("objectIdFieldName", out var oidEl) && oidEl.ValueKind == JsonValueKind.String
            ? (oidEl.GetString() ?? "OBJECTID")
            : "OBJECTID";

        var ids = new List<int>();
        if (root.TryGetProperty("objectIds", out var idsEl) && idsEl.ValueKind == JsonValueKind.Array)
            foreach (var el in idsEl.EnumerateArray())
                if (el.ValueKind == JsonValueKind.Number && el.TryGetInt32(out var i)) ids.Add(i);

        ids.Sort();
        return (oidField, ids);
    }

    private async Task<JsonElement> QueryByObjectIds(string layerUrl, string token, string oidField, List<int> objectIds, string outFields, CancellationToken ct)
    {
        var http = _httpFactory.CreateClient("arcgis");

        var qs = new Dictionary<string, string>
        {
            ["where"] = _dest.Where,
            ["objectIds"] = string.Join(",", objectIds),
            ["outFields"] = outFields,
            ["returnGeometry"] = "false",
            ["orderByFields"] = oidField,
            ["f"] = "json",
            ["token"] = token
        };

        using var req = new HttpRequestMessage(HttpMethod.Get, $"{layerUrl}/query?{ToQueryString(qs)}");
        ApplyRefererHeader(req);

        using var res = await http.SendAsync(req, ct);
        var raw = await res.Content.ReadAsStringAsync(ct);

        if (!res.IsSuccessStatusCode)
            throw new InvalidOperationException($"query failed status={(int)res.StatusCode} body={raw}");

        using var doc = JsonDocument.Parse(raw);
        var root = doc.RootElement;

        if (root.TryGetProperty("error", out var err))
            throw new InvalidOperationException($"query error: {err.GetRawText()}");

        return root.Clone();
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

    private static string BuildFetchedAtLocalText(string? timeZoneId)
    {
        var tz = ResolveTimeZone(timeZoneId);
        var local = TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, tz);
        return local.ToString("M/d/yyyy, hh:mm tt", CultureInfo.InvariantCulture);
    }

    private static string BuildTimeZoneDisplay(string? timeZoneId)
    {
        var tz = ResolveTimeZone(timeZoneId);
        var offset = tz.GetUtcOffset(DateTime.UtcNow);
        var sign = offset >= TimeSpan.Zero ? "+" : "-";
        var abs = offset.Duration();
        return $"{tz.Id} UTC {sign}{abs:hh\\:mm}";
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