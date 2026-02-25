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

/// <summary>
/// DestinationCheckProcessor (NON-CONTINUOUS):
/// - Seeds exactly 1 job on container start.
/// - Consumes that job, checks Destination FS is alive.
/// - WAITS (gates) until diff snapshots are ready (Update/Delete/Insert/Skip status="ok").
/// - Applies applyEdits using Update_Data + Delete_Data + Insert_Data.
/// - Fetches final destination snapshot into Final_Destination_Data.
/// - Writes Destination_Success_ID or Destination_Failed_ID.
/// - On any failure, schedules retry:
///     try1 immediate -> +1m -> +3m -> +9m -> then DLQ + Failed.
/// - On SUCCESS or TERMINAL FAIL -> stops (non-continuous).
/// </summary>
public sealed class DestinationCheckProcessor : BackgroundService
{
    // Retry plan (total 4 tries):
    private static readonly TimeSpan[] RetryDelays =
    [
        TimeSpan.FromMinutes(1), // Attempt 1 => Try 2
        TimeSpan.FromMinutes(3), // Attempt 2 => Try 3
        TimeSpan.FromMinutes(9)  // Attempt 3 => Try 4
    ];

    // Snapshot ids
    private const string UpdateLatestId = "update_latest";
    private const string SkipLatestId = "skip_latest";
    private const string DeleteLatestId = "delete_latest";
    private const string InsertLatestId = "insert_latest";

    private const string FinalLatestId = "final_destination_latest";

    private readonly IPulsarClient _pulsarClient;
    private readonly IHttpClientFactory _httpFactory;

    private readonly IMongoClient _mongoClient;
    private readonly MongoOptions _mongo;

    private readonly PulsarOptions _pulsar;
    private readonly ArcGisPortalOptions _portal;
    private readonly DestinationApiOptions _dest;

    private readonly ILogger<DestinationCheckProcessor> _logger;

    // Token cache (avoid spamming generateToken)
    private string? _token;
    private DateTimeOffset _tokenExpiresAtUtc = DateTimeOffset.MinValue;

    public DestinationCheckProcessor(
        IPulsarClient pulsarClient,
        IHttpClientFactory httpFactory,
        IMongoClient mongoClient,
        IOptions<MongoOptions> mongo,
        IOptions<PulsarOptions> pulsar,
        IOptions<ArcGisPortalOptions> portal,
        IOptions<DestinationApiOptions> dest,
        ILogger<DestinationCheckProcessor> logger)
    {
        _pulsarClient = pulsarClient;
        _httpFactory = httpFactory;

        _mongoClient = mongoClient;
        _mongo = mongo.Value;

        _pulsar = pulsar.Value;
        _portal = portal.Value;
        _dest = dest.Value;

        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("DestinationCheckProcessor starting...");

        if (string.IsNullOrWhiteSpace(_pulsar.DestinationCheckTopic))
            throw new InvalidOperationException("Pulsar.DestinationCheckTopic is empty. Set it in appsettings.json.");

        if (string.IsNullOrWhiteSpace(_dest.FeatureLayerUrl))
            throw new InvalidOperationException("DestinationApi.FeatureLayerUrl is empty.");

        _logger.LogInformation("Pulsar destination-check topic: {Topic}", _pulsar.DestinationCheckTopic);
        _logger.LogInformation("Pulsar DLQ topic: {Topic}", _pulsar.DlqTopic);

        _logger.LogInformation("Mongo Update snapshot: {Db}.{Col} _id={Id}", _mongo.Database, _mongo.UpdateCollection, UpdateLatestId);
        _logger.LogInformation("Mongo Skip snapshot: {Db}.{Col} _id={Id}", _mongo.Database, _mongo.SkipCollection, SkipLatestId);
        _logger.LogInformation("Mongo Delete snapshot: {Db}.{Col} _id={Id}", _mongo.Database, _mongo.DeleteCollection, DeleteLatestId);
        _logger.LogInformation("Mongo Insert snapshot: {Db}.{Col} _id={Id}", _mongo.Database, _mongo.InsertCollection, InsertLatestId);

        _logger.LogInformation("Mongo destination success collection: {Db}.{Col}", _mongo.Database, _mongo.DestinationSuccessIdCollection);
        _logger.LogInformation("Mongo destination failed collection: {Db}.{Col}", _mongo.Database, _mongo.DestinationFailedIdCollection);
        _logger.LogInformation("Mongo final destination collection: {Db}.{Col}", _mongo.Database, _mongo.FinalDestinationCollection);

        var pollSeconds = Math.Max(1, _dest.GatePollSeconds);
        var maxWaitSeconds = Math.Max(5, _dest.GateMaxWaitSeconds);

        _logger.LogInformation("Gating config: pollSeconds={PollSeconds} maxWaitSeconds={MaxWaitSeconds}", pollSeconds, maxWaitSeconds);

        await using var consumer = _pulsarClient.NewConsumer(Schema.String)
            .Topic(_pulsar.DestinationCheckTopic)
            .SubscriptionName(_pulsar.Subscription + "-destination-check")
            .InitialPosition(SubscriptionInitialPosition.Earliest)
            .Create();

        await using var checkProducer = _pulsarClient.NewProducer(Schema.String)
            .Topic(_pulsar.DestinationCheckTopic)
            .Create();

        await using var dlqProducer = _pulsarClient.NewProducer(Schema.String)
            .Topic(_pulsar.DlqTopic)
            .Create();

        // Seed exactly one job on startup (NON-CONTINUOUS).
        var seedJobId = ObjectId.GenerateNewId().ToString();
        var seed = new DestinationCheckJob(
            JobId: seedJobId,
            Url: _dest.FeatureLayerUrl.TrimEnd('/'),
            Attempt: 0,
            CreatedAtUtc: DateTimeOffset.UtcNow);

        await checkProducer.Send(JsonSerializer.Serialize(seed), stoppingToken);

        _logger.LogInformation(
            "Seeded DestinationCheckJob jobId={JobId} (try=1) url={Url} into {Topic}",
            seedJobId, seed.Url, _pulsar.DestinationCheckTopic);

        // Non-continuous: after SUCCESS or TERMINAL FAIL, we stop consuming.
        await foreach (var msg in consumer.Messages(stoppingToken))
        {
            try
            {
                var payload = msg.Value();
                var job = JsonSerializer.Deserialize<DestinationCheckJob>(payload);

                if (job is null)
                {
                    _logger.LogWarning("Invalid DestinationCheckJob payload. ack+skip. payload={Payload}", payload);
                    await consumer.Acknowledge(msg, stoppingToken);
                    continue;
                }

                if (string.IsNullOrWhiteSpace(job.Url))
                {
                    _logger.LogWarning("DestinationCheckJob has empty url. ack+skip. jobId={JobId}", job.JobId);
                    await consumer.Acknowledge(msg, stoppingToken);
                    continue;
                }

                if (string.IsNullOrWhiteSpace(job.JobId) || !ObjectId.TryParse(job.JobId, out var jobObjectId))
                {
                    _logger.LogWarning("Invalid JobId in DestinationCheckJob. ack+skip. jobId={JobId}", job.JobId);
                    await consumer.Acknowledge(msg, stoppingToken);
                    continue;
                }

                var tryNo = job.Attempt + 1;

                _logger.LogInformation(
                    "DEST_CHECK jobId={JobId} try={TryNo} url={Url} createdAtUtc={CreatedAtUtc}",
                    job.JobId, tryNo, job.Url, job.CreatedAtUtc);

                // 1) Check destination service alive
                var alive = await IsDestinationAlive(job.Url, stoppingToken);

                if (!alive.Success)
                {
                    _logger.LogWarning(
                        "DEST_RESULT jobId={JobId} try={TryNo} => DOWN status={Status} reason={Reason}",
                        job.JobId, tryNo, alive.StatusCode, alive.Reason);

                    var terminal = await ScheduleRetryOrStop(job, jobObjectId, checkProducer, dlqProducer, alive, stoppingToken);

                    await consumer.Acknowledge(msg, stoppingToken);

                    if (terminal)
                    {
                        _logger.LogInformation("DEST_TERMINAL_FAIL_STOP jobId={JobId} try={TryNo}", job.JobId, tryNo);
                        return;
                    }

                    continue;
                }

                _logger.LogInformation(
                    "DEST_RESULT jobId={JobId} try={TryNo} => ALIVE status={Status}. Waiting for diff snapshots...",
                    job.JobId, tryNo, alive.StatusCode);

                // 2) GATING (CRITICAL FIX):
                // Wait/poll until Update/Delete/Insert/Skip exist and status="ok" within a max wait window.
                var gateWait = await WaitForDiffSnapshotsAsync(
                    pollInterval: TimeSpan.FromSeconds(pollSeconds),
                    maxWait: TimeSpan.FromSeconds(maxWaitSeconds),
                    ct: stoppingToken);

                if (!gateWait.Ready)
                {
                    _logger.LogWarning(
                        "DEST_GATE_TIMEOUT jobId={JobId} try={TryNo} => {Reason}. Will retry if attempts remain.",
                        job.JobId, tryNo, gateWait.Reason);

                    var terminal = await ScheduleRetryOrStop(
                        job,
                        jobObjectId,
                        checkProducer,
                        dlqProducer,
                        new AliveResult(false, HttpStatusCode.OK, "waiting_for_diff_snapshots:" + gateWait.Reason),
                        stoppingToken);

                    await consumer.Acknowledge(msg, stoppingToken);

                    if (terminal)
                    {
                        _logger.LogInformation("DEST_TERMINAL_FAIL_STOP jobId={JobId} try={TryNo}", job.JobId, tryNo);
                        return;
                    }

                    continue;
                }

                _logger.LogInformation(
                    "DEST_GATE_READY jobId={JobId} try={TryNo} => Diff snapshots ready. Applying edits...",
                    job.JobId, tryNo);

                // 3) Apply edits (Update/Delete/Insert)
                var apply = await ApplyAllEdits(job.Url.TrimEnd('/'), stoppingToken);

                if (!apply.Success)
                {
                    _logger.LogError(
                        "DEST_APPLY_FAILED jobId={JobId} try={TryNo} reason={Reason}",
                        job.JobId, tryNo, apply.Reason);

                    var terminal = await ScheduleRetryOrStop(
                        job,
                        jobObjectId,
                        checkProducer,
                        dlqProducer,
                        new AliveResult(false, apply.StatusCode, "apply_failed:" + apply.Reason),
                        stoppingToken);

                    await consumer.Acknowledge(msg, stoppingToken);

                    if (terminal)
                    {
                        _logger.LogInformation("DEST_TERMINAL_FAIL_STOP jobId={JobId} try={TryNo}", job.JobId, tryNo);
                        return;
                    }

                    continue;
                }

                // 4) Read Skip_Data count (report-only)
                var skipCount = await ReadSkipCount(stoppingToken);

                // 5) Fetch final destination snapshot (after applyEdits)
                var finalCount = await FetchAndStoreFinalDestination(job.Url.TrimEnd('/'), stoppingToken);

                // 6) Write success audit
                await WriteDestinationOutcomeAsync(
                    collectionName: _mongo.DestinationSuccessIdCollection,
                    id: jobObjectId,
                    destinationUrl: job.Url,
                    fetchedAtLocalText: BuildFetchedAtLocalText(_mongo.TimeZoneId),
                    timeZoneDisplay: BuildTimeZoneDisplay(_mongo.TimeZoneId),
                    pulsarText: $"Success With try {tryNo} | updates={apply.UpdateCount} deletes={apply.DeleteCount} inserts={apply.InsertCount} skip={skipCount} | finalCount={finalCount}",
                    stoppingToken: stoppingToken);

                _logger.LogInformation(
                    "DEST_SUCCESS_STOP jobId={JobId} try={TryNo} => Success audit written. Non-continuous mode: stopping.",
                    job.JobId, tryNo);

                await consumer.Acknowledge(msg, stoppingToken);

                // Non-continuous: stop the worker after a successful run.
                return;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "DestinationCheckProcessor failed. Leaving unacked for redelivery.");
            }
        }
    }

    // ------------------------------------------------------------
    // GATING LOGIC (poll until diff snapshots exist and ready)
    // ------------------------------------------------------------

    private sealed record GateResult(bool Ready, string Reason);

    private async Task<GateResult> WaitForDiffSnapshotsAsync(TimeSpan pollInterval, TimeSpan maxWait, CancellationToken ct)
    {
        var start = DateTimeOffset.UtcNow;
        var tries = 0;

        while (!ct.IsCancellationRequested)
        {
            tries++;

            var gate = await CheckDiffSnapshotsReady(ct);
            if (gate.Ready)
            {
                _logger.LogInformation("DEST_GATE_OK after {Tries} checks. waitedSeconds={WaitedSeconds}",
                    tries, (DateTimeOffset.UtcNow - start).TotalSeconds.ToString("F0", CultureInfo.InvariantCulture));

                return gate;
            }

            var elapsed = DateTimeOffset.UtcNow - start;
            if (elapsed >= maxWait)
            {
                _logger.LogWarning("DEST_GATE_TIMEOUT after {Tries} checks. waitedSeconds={WaitedSeconds}. lastReason={Reason}",
                    tries, elapsed.TotalSeconds.ToString("F0", CultureInfo.InvariantCulture), gate.Reason);

                return new GateResult(false, $"timeout_after_{elapsed.TotalSeconds:F0}s: {gate.Reason}");
            }

            if (tries == 1 || tries % 6 == 0)
            {
                _logger.LogInformation("DEST_GATE_WAITING check={Check} waitedSeconds={WaitedSeconds} reason={Reason}",
                    tries, elapsed.TotalSeconds.ToString("F0", CultureInfo.InvariantCulture), gate.Reason);
            }

            await Task.Delay(pollInterval, ct);
        }

        return new GateResult(false, "canceled");
    }

    private async Task<GateResult> CheckDiffSnapshotsReady(CancellationToken ct)
    {
        var db = _mongoClient.GetDatabase(_mongo.Database);

        var updateCol = db.GetCollection<BsonDocument>(_mongo.UpdateCollection);
        var deleteCol = db.GetCollection<BsonDocument>(_mongo.DeleteCollection);
        var insertCol = db.GetCollection<BsonDocument>(_mongo.InsertCollection);
        var skipCol = db.GetCollection<BsonDocument>(_mongo.SkipCollection);

        var updateSnap = await updateCol.Find(Builders<BsonDocument>.Filter.Eq("_id", UpdateLatestId)).FirstOrDefaultAsync(ct);
        var deleteSnap = await deleteCol.Find(Builders<BsonDocument>.Filter.Eq("_id", DeleteLatestId)).FirstOrDefaultAsync(ct);
        var insertSnap = await insertCol.Find(Builders<BsonDocument>.Filter.Eq("_id", InsertLatestId)).FirstOrDefaultAsync(ct);
        var skipSnap = await skipCol.Find(Builders<BsonDocument>.Filter.Eq("_id", SkipLatestId)).FirstOrDefaultAsync(ct);

        if (updateSnap is null) return new GateResult(false, "Update_Data snapshot not found");
        if (deleteSnap is null) return new GateResult(false, "Delete_Data snapshot not found");
        if (insertSnap is null) return new GateResult(false, "Insert_Data snapshot not found");
        if (skipSnap is null) return new GateResult(false, "Skip_Data snapshot not found");

        var uStatus = GetStringSafe(updateSnap, "status");
        var dStatus = GetStringSafe(deleteSnap, "status");
        var iStatus = GetStringSafe(insertSnap, "status");
        var sStatus = GetStringSafe(skipSnap, "status");

        if (!string.Equals(uStatus, "ok", StringComparison.OrdinalIgnoreCase))
            return new GateResult(false, $"Update_Data status='{uStatus}'");

        if (!string.Equals(dStatus, "ok", StringComparison.OrdinalIgnoreCase))
            return new GateResult(false, $"Delete_Data status='{dStatus}'");

        if (!string.Equals(iStatus, "ok", StringComparison.OrdinalIgnoreCase))
            return new GateResult(false, $"Insert_Data status='{iStatus}'");

        if (!string.Equals(sStatus, "ok", StringComparison.OrdinalIgnoreCase))
            return new GateResult(false, $"Skip_Data status='{sStatus}'");

        return new GateResult(true, "ok");
    }

    private static string GetStringSafe(BsonDocument doc, string field)
    {
        if (doc is null) return "";
        if (!doc.TryGetValue(field, out var v) || v.IsBsonNull) return "";
        return v.BsonType == BsonType.String ? (v.AsString ?? "") : (v.ToString() ?? "");
    }

    // Reads Skip_Data.skip_latest.skipCount (report-only)
    private async Task<int> ReadSkipCount(CancellationToken ct)
    {
        try
        {
            var db = _mongoClient.GetDatabase(_mongo.Database);
            var skipCol = db.GetCollection<BsonDocument>(_mongo.SkipCollection);

            var snap = await skipCol.Find(Builders<BsonDocument>.Filter.Eq("_id", SkipLatestId)).FirstOrDefaultAsync(ct);
            if (snap is null) return 0;

            if (snap.TryGetValue("skipCount", out var v))
            {
                var i = GetIntLike(v);
                if (i is not null) return i.Value;
            }

            if (snap.TryGetValue("itemCount", out var itemCount))
            {
                var i = GetIntLike(itemCount);
                if (i is not null) return i.Value;
            }

            return 0;
        }
        catch
        {
            return 0;
        }
    }

    // ------------------------------------------------------------
    // Alive check
    // ------------------------------------------------------------

    private sealed record AliveResult(bool Success, HttpStatusCode? StatusCode, string Reason);

    private async Task<AliveResult> IsDestinationAlive(string layerUrl, CancellationToken ct)
    {
        try
        {
            var token = await GetPortalToken(ct);

            var http = _httpFactory.CreateClient("arcgis");

            var qs = new Dictionary<string, string>
            {
                ["where"] = _dest.Where ?? "1=1",
                ["returnIdsOnly"] = "true",
                ["f"] = "json",
                ["token"] = token
            };

            var url = $"{layerUrl.TrimEnd('/')}/query?{ToQueryString(qs)}";

            using var req = new HttpRequestMessage(HttpMethod.Get, url);
            ApplyRefererHeader(req);

            using var res = await http.SendAsync(req, ct);
            var raw = await res.Content.ReadAsStringAsync(ct);

            if (!res.IsSuccessStatusCode)
                return new AliveResult(false, res.StatusCode, "non_success_status");

            using var doc = JsonDocument.Parse(raw);
            var root = doc.RootElement;

            if (root.TryGetProperty("error", out _))
                return new AliveResult(false, res.StatusCode, "arcgis_error_payload");

            return new AliveResult(true, res.StatusCode, "ok");
        }
        catch (TaskCanceledException)
        {
            return new AliveResult(false, null, "timeout_or_canceled");
        }
        catch (HttpRequestException)
        {
            return new AliveResult(false, null, "http_request_exception");
        }
        catch (Exception ex)
        {
            return new AliveResult(false, null, $"unexpected_exception:{ex.GetType().Name}");
        }
    }

    // ------------------------------------------------------------
    // Apply edits (Update/Delete/Insert)
    // ------------------------------------------------------------

    private sealed record ApplyResult(
        bool Success,
        HttpStatusCode? StatusCode,
        string Reason,
        int UpdateCount,
        int DeleteCount,
        int InsertCount);

    private async Task<ApplyResult> ApplyAllEdits(string layerUrl, CancellationToken ct)
    {
        var token = await GetPortalToken(ct);

        // Determine the real OBJECTID field name from the layer (do not assume casing).
        var (oidField, _) = await QueryAllObjectIds(layerUrl, token, ct);

        var db = _mongoClient.GetDatabase(_mongo.Database);

        var updateCol = db.GetCollection<BsonDocument>(_mongo.UpdateCollection);
        var deleteCol = db.GetCollection<BsonDocument>(_mongo.DeleteCollection);
        var insertCol = db.GetCollection<BsonDocument>(_mongo.InsertCollection);

        var updateSnap = await updateCol.Find(Builders<BsonDocument>.Filter.Eq("_id", UpdateLatestId)).FirstOrDefaultAsync(ct);
        var deleteSnap = await deleteCol.Find(Builders<BsonDocument>.Filter.Eq("_id", DeleteLatestId)).FirstOrDefaultAsync(ct);
        var insertSnap = await insertCol.Find(Builders<BsonDocument>.Filter.Eq("_id", InsertLatestId)).FirstOrDefaultAsync(ct);

        var updates = ExtractUpdateFeatures(updateSnap, oidField);
        var deletes = ExtractDeleteObjectIds(deleteSnap);
        var inserts = ExtractInsertFeatures(insertSnap);

        if (updates.Count == 0 && deletes.Count == 0 && inserts.Count == 0)
        {
            _logger.LogInformation("No edits to apply (Update/Delete/Insert are all empty).");
            return new ApplyResult(true, HttpStatusCode.OK, "no_edits", 0, 0, 0);
        }

        var http = _httpFactory.CreateClient("arcgis");

        var updateJson = updates.Count == 0 ? "" : JsonSerializer.Serialize(updates);
        var insertJson = inserts.Count == 0 ? "" : JsonSerializer.Serialize(inserts);
        var deleteCsv = deletes.Count == 0 ? "" : string.Join(",", deletes);

        var form = new Dictionary<string, string>
        {
            ["f"] = "json",
            ["token"] = token
        };

        // ArcGIS REST applyEdits expects: adds, updates, deletes
        if (updates.Count > 0) form["updates"] = updateJson;
        if (inserts.Count > 0) form["adds"] = insertJson;
        if (deletes.Count > 0) form["deletes"] = deleteCsv;

        var url = $"{layerUrl.TrimEnd('/')}/applyEdits";

        using var req = new HttpRequestMessage(HttpMethod.Post, url)
        {
            Content = new FormUrlEncodedContent(form)
        };
        ApplyRefererHeader(req);

        using var res = await http.SendAsync(req, ct);
        var raw = await res.Content.ReadAsStringAsync(ct);

        _logger.LogInformation("applyEdits raw response (first 2000 chars): {Raw}",
            raw.Length <= 2000 ? raw : raw.Substring(0, 2000) + "...");

        if (!res.IsSuccessStatusCode)
            return new ApplyResult(false, res.StatusCode, $"applyEdits_http_{(int)res.StatusCode}", updates.Count, deletes.Count, inserts.Count);

        using var doc = JsonDocument.Parse(raw);
        var root = doc.RootElement;

        if (root.TryGetProperty("error", out var err))
            return new ApplyResult(false, res.StatusCode, $"applyEdits_error:{err.GetRawText()}", updates.Count, deletes.Count, inserts.Count);

        var ok = true;
        ok = ok && CheckEditsArray(root, "updateResults", "update");
        ok = ok && CheckEditsArray(root, "addResults", "add");
        ok = ok && CheckEditsArray(root, "deleteResults", "delete");

        if (!ok)
            return new ApplyResult(false, res.StatusCode, "applyEdits_partial_or_failed_results", updates.Count, deletes.Count, inserts.Count);

        _logger.LogInformation("applyEdits success. updates={Updates} deletes={Deletes} inserts={Inserts}",
            updates.Count, deletes.Count, inserts.Count);

        return new ApplyResult(true, res.StatusCode, "ok", updates.Count, deletes.Count, inserts.Count);
    }

    private bool CheckEditsArray(JsonElement root, string propName, string tag)
    {
        if (!root.TryGetProperty(propName, out var arr) || arr.ValueKind != JsonValueKind.Array)
            return true;

        var i = 0;
        foreach (var el in arr.EnumerateArray())
        {
            i++;
            if (el.ValueKind != JsonValueKind.Object)
                continue;

            if (el.TryGetProperty("success", out var s) && s.ValueKind == JsonValueKind.True)
                continue;

            _logger.LogError("applyEdits {Tag}Results item {Index} indicates failure. item={Item}", tag, i, el.GetRawText());
            return false;
        }

        return true;
    }

    // Update_Data row: { objectid, final_title, final_body, ... }
    // ArcGIS requires the correct OBJECTID field name; we use oidField from the service.
    private static List<Dictionary<string, object>> ExtractUpdateFeatures(BsonDocument? snap, string oidField)
    {
        var list = new List<Dictionary<string, object>>();
        if (snap is null) return list;
        if (!snap.TryGetValue("payload", out var payloadVal) || payloadVal.BsonType != BsonType.Array) return list;

        foreach (var v in payloadVal.AsBsonArray)
        {
            if (v.BsonType != BsonType.Document) continue;
            var d = v.AsBsonDocument;

            var oid = GetIntLike(d.GetValue("objectid", BsonNull.Value));
            if (oid is null) continue;

            // Prefer final_* but fall back to title/body if your UpdateDetectionProcessor writes those instead.
            var finalTitle = GetStringLike(d.GetValue("final_title", BsonNull.Value)) ?? GetStringLike(d.GetValue("title", BsonNull.Value));
            var finalBody = GetStringLike(d.GetValue("final_body", BsonNull.Value)) ?? GetStringLike(d.GetValue("body", BsonNull.Value));

            // If nothing to update, don't send this record (some services reject "update with only OBJECTID")
            if (finalTitle is null && finalBody is null)
                continue;

            var attrs = new Dictionary<string, object>
            {
                [oidField] = oid.Value
            };

            if (finalTitle is not null) attrs["title"] = finalTitle;
            if (finalBody is not null) attrs["body"] = finalBody;

            list.Add(new Dictionary<string, object> { ["attributes"] = attrs });
        }

        return list;
    }

    // Delete_Data row: { objectid, ... }
    private static List<int> ExtractDeleteObjectIds(BsonDocument? snap)
    {
        var ids = new List<int>();

        if (snap is null) return ids;
        if (!snap.TryGetValue("payload", out var payloadVal) || payloadVal.BsonType != BsonType.Array) return ids;

        var seen = new HashSet<int>();

        foreach (var v in payloadVal.AsBsonArray)
        {
            if (v.BsonType != BsonType.Document) continue;
            var d = v.AsBsonDocument;

            var oid = GetIntLike(d.GetValue("objectid", BsonNull.Value));
            if (oid is null) continue;

            if (seen.Add(oid.Value))
                ids.Add(oid.Value);
        }

        return ids;
    }

    // Insert_Data row: { user_id, id, title, body, objectid:null, ... }
    private static List<Dictionary<string, object>> ExtractInsertFeatures(BsonDocument? snap)
    {
        var list = new List<Dictionary<string, object>>();

        if (snap is null) return list;
        if (!snap.TryGetValue("payload", out var payloadVal) || payloadVal.BsonType != BsonType.Array) return list;

        foreach (var v in payloadVal.AsBsonArray)
        {
            if (v.BsonType != BsonType.Document) continue;
            var d = v.AsBsonDocument;

            var userId = GetIntLike(d.GetValue("user_id", BsonNull.Value)) ?? GetIntLike(d.GetValue("userId", BsonNull.Value));
            var id = GetIntLike(d.GetValue("id", BsonNull.Value));
            var title = GetStringLike(d.GetValue("title", BsonNull.Value));
            var body = GetStringLike(d.GetValue("body", BsonNull.Value));

            if (userId is null || id is null) continue;

            var attrs = new Dictionary<string, object>
            {
                ["user_id"] = userId.Value,
                ["id"] = id.Value
            };

            if (title is not null) attrs["title"] = title;
            if (body is not null) attrs["body"] = body;

            list.Add(new Dictionary<string, object> { ["attributes"] = attrs });
        }

        return list;
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

    private static string? GetStringLike(BsonValue v)
    {
        if (v is null || v.IsBsonNull) return null;

        try
        {
            if (v.BsonType == BsonType.String)
            {
                var s = (v.AsString ?? "").Trim();
                return s.Length == 0 ? null : s;
            }

            var x = (v.ToString() ?? "").Trim();
            return x.Length == 0 ? null : x;
        }
        catch
        {
            return null;
        }
    }

    // ------------------------------------------------------------
    // Fetch final dataset and store it in Final_Destination_Data
    // ------------------------------------------------------------

    private async Task<int> FetchAndStoreFinalDestination(string layerUrl, CancellationToken ct)
    {
        var token = await GetPortalToken(ct);

        var tz = ResolveTimeZone(_mongo.TimeZoneId);

        var fetchedUtc = DateTime.UtcNow;
        var fetchedLocal = TimeZoneInfo.ConvertTimeFromUtc(fetchedUtc, tz);
        var fetchedAtLocalText = fetchedLocal.ToString("M/d/yyyy, hh:mm tt", CultureInfo.InvariantCulture);

        var offset = tz.GetUtcOffset(fetchedUtc);
        var sign = offset >= TimeSpan.Zero ? "+" : "-";
        var abs = offset.Duration();
        var offsetText = $" {sign}{abs:hh\\:mm}";
        var timeZoneDisplay = $"{tz.Id} UTC{offsetText}";

        var (oidField, objectIds) = await QueryAllObjectIds(layerUrl, token, ct);

        var batchSize = Math.Max(1, _dest.QueryBatchSize);
        var payload = new BsonArray();

        foreach (var chunk in Chunk(objectIds, batchSize))
        {
            var root = await QueryByObjectIds(layerUrl, token, oidField, chunk, ct);

            if (!root.TryGetProperty("features", out var featuresEl) || featuresEl.ValueKind != JsonValueKind.Array)
                break;

            foreach (var feat in featuresEl.EnumerateArray())
            {
                if (!feat.TryGetProperty("attributes", out var attrsEl) || attrsEl.ValueKind != JsonValueKind.Object)
                    continue;

                var attrsDoc = BsonDocument.Parse(attrsEl.GetRawText());

                ConvertEpochMsFieldToLocalText(attrsDoc, "created_date", tz);
                ConvertEpochMsFieldToLocalText(attrsDoc, "last_edited_date", tz);

                var featureDoc = new BsonDocument
                {
                    { "attributes", attrsDoc }
                };

                if (_dest.ReturnGeometry &&
                    feat.TryGetProperty("geometry", out var geomEl) &&
                    geomEl.ValueKind == JsonValueKind.Object)
                {
                    featureDoc["geometry"] = BsonDocument.Parse(geomEl.GetRawText());
                }
                else
                {
                    featureDoc["geometry"] = BsonNull.Value;
                }

                payload.Add(featureDoc);
            }
        }

        var db = _mongoClient.GetDatabase(_mongo.Database);
        var col = db.GetCollection<BsonDocument>(_mongo.FinalDestinationCollection);

        var envelope = new BsonDocument
        {
            { "_id", FinalLatestId },
            { "status", "ok" },
            { "layerUrl", layerUrl },
            { "objectIdField", oidField },
            { "fetchedAtLocalText", fetchedAtLocalText },
            { "timeZoneId", timeZoneDisplay },
            { "batchSize", batchSize },
            { "itemCount", payload.Count },
            { "payload", payload }
        };

        await col.ReplaceOneAsync(
            Builders<BsonDocument>.Filter.Eq("_id", FinalLatestId),
            envelope,
            new ReplaceOptions { IsUpsert = true },
            ct);

        _logger.LogInformation(
            "Final_Destination_Data updated. itemCount={Count} mongo={Db}.{Col} _id={Id}",
            payload.Count, _mongo.Database, _mongo.FinalDestinationCollection, FinalLatestId);

        return payload.Count;
    }

    // ------------------------------------------------------------
    // Retry scheduler + audit writers
    // ------------------------------------------------------------

    // Returns true if terminal failure happened (DLQ + Failed written).
    private async Task<bool> ScheduleRetryOrStop(
        DestinationCheckJob job,
        ObjectId jobObjectId,
        IProducer<string> checkProducer,
        IProducer<string> dlqProducer,
        AliveResult result,
        CancellationToken ct)
    {
        var nextAttempt = job.Attempt + 1;

        // RetryDelays has 3 entries => attempts allowed: 0..3 => tries allowed: 1..4
        if (nextAttempt > RetryDelays.Length)
        {
            var lastTryNo = job.Attempt + 1;

            _logger.LogError(
                "DEST_FAILED_STOP jobId={JobId} lastTry={TryNo} reason={Reason} lastStatus={Status}",
                job.JobId, lastTryNo, result.Reason, result.StatusCode);

            var retryScheduleMinutes = RetryDelays.Select(d => (int)Math.Round(d.TotalMinutes)).ToArray();
            var lastDelayMinutes = retryScheduleMinutes.Length > 0 ? retryScheduleMinutes[^1] : 0;

            var dlqPayload = JsonSerializer.Serialize(new
            {
                job.JobId,
                job.Url,
                attempt = job.Attempt,
                tryNo = lastTryNo,
                retryScheduleMinutes,
                lastDelayMinutes,
                createdAtUtc = job.CreatedAtUtc,
                failedAtUtc = DateTimeOffset.UtcNow,
                lastStatus = result.StatusCode?.ToString(),
                lastReason = result.Reason,
                reason = "DTQ Raised - Service Unavailable (stopped after try 4)"
            });

            await dlqProducer.Send(dlqPayload, ct);

            await WriteDestinationOutcomeAsync(
                collectionName: _mongo.DestinationFailedIdCollection,
                id: jobObjectId,
                destinationUrl: job.Url,
                fetchedAtLocalText: BuildFetchedAtLocalText(_mongo.TimeZoneId),
                timeZoneDisplay: BuildTimeZoneDisplay(_mongo.TimeZoneId),
                pulsarText: $"DTQ Raised - Service Unavailable | stoppedAfterTry={lastTryNo} | schedule={string.Join(",", retryScheduleMinutes)}m | lastStatus={result.StatusCode} | lastReason={result.Reason}",
                stoppingToken: ct);

            return true; // terminal
        }

        var delay = RetryDelays[nextAttempt - 1];
        var deliverAtUtc = DateTimeOffset.UtcNow.Add(delay);

        _logger.LogWarning(
            "DEST_RETRY_SCHEDULED jobId={JobId} prevTry={PrevTry} nextTry={NextTry} delay={Delay} deliverAtUtc={DeliverAtUtc} reason={Reason}",
            job.JobId, job.Attempt + 1, nextAttempt + 1, delay, deliverAtUtc, result.Reason);

        var nextJob = job with { Attempt = nextAttempt };
        var payload = JsonSerializer.Serialize(nextJob);

        await checkProducer.NewMessage()
            .DeliverAt(deliverAtUtc.UtcDateTime)
            .Send(payload, ct);

        return false;
    }

    private async Task WriteDestinationOutcomeAsync(
        string collectionName,
        ObjectId id,
        string destinationUrl,
        string fetchedAtLocalText,
        string timeZoneDisplay,
        string pulsarText,
        CancellationToken stoppingToken)
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

        await col.ReplaceOneAsync(
            Builders<BsonDocument>.Filter.Eq("_id", id),
            doc,
            new ReplaceOptions { IsUpsert = true },
            stoppingToken);
    }

    // ------------------------------------------------------------
    // ArcGIS token + query helpers
    // ------------------------------------------------------------

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

        _logger.LogInformation("Portal token acquired. expiresAtUtc={Expires}", _tokenExpiresAtUtc);
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
            ["returnGeometry"] = _dest.ReturnGeometry ? "true" : "false",
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

    private static string BuildFetchedAtLocalText(string? timeZoneId)
    {
        var tz = ResolveTimeZone(timeZoneId);
        var utc = DateTime.UtcNow;
        var local = TimeZoneInfo.ConvertTimeFromUtc(utc, tz);
        return local.ToString("M/d/yyyy, hh:mm tt", CultureInfo.InvariantCulture);
    }

    private static string BuildTimeZoneDisplay(string? timeZoneId)
    {
        var tz = ResolveTimeZone(timeZoneId);
        var utc = DateTime.UtcNow;
        var offset = tz.GetUtcOffset(utc);
        var sign = offset >= TimeSpan.Zero ? "+" : "-";
        var abs = offset.Duration();
        var offsetText = $" {sign}{abs:hh\\:mm}";
        return $"{tz.Id} UTC{offsetText}";
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

    private static void ConvertEpochMsFieldToLocalText(BsonDocument attrs, string fieldName, TimeZoneInfo tz)
    {
        if (attrs is null || string.IsNullOrWhiteSpace(fieldName))
            return;

        string? actualName = null;
        BsonValue val = BsonNull.Value;

        foreach (var e in attrs.Elements)
        {
            if (string.Equals(e.Name, fieldName, StringComparison.OrdinalIgnoreCase))
            {
                actualName = e.Name;
                val = e.Value;
                break;
            }
        }

        if (actualName is null)
            return;

        if (val.BsonType == BsonType.String)
        {
            var s = (val.AsString ?? "").Trim();
            if (s.Length == 0) return;

            if (long.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out var msStr))
                attrs[actualName] = FormatEpochMs(msStr, tz);

            return;
        }

        if (!TryReadEpochMs(val, out var ms))
            return;

        attrs[actualName] = FormatEpochMs(ms, tz);
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