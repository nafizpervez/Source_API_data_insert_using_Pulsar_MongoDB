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
using SourceIngestor.Worker.Utils;

namespace SourceIngestor.Worker.Workers;

public sealed class SourceCheckProcessor : BackgroundService
{
    // Retry plan (total 4 tries):
    // - Try 1: immediately (Attempt = 0)
    // - Try 2: after 1 minute (Attempt = 1)
    // - Try 3: after 3 minutes (Attempt = 2)
    // - Try 4: after 9 minutes (Attempt = 3)
    // If try 4 also fails -> push to DLQ + write to Source_Failed_ID and STOP.
    private static readonly TimeSpan[] RetryDelays =
    [
        TimeSpan.FromMinutes(1), // Attempt 1 => Try 2
        TimeSpan.FromMinutes(3), // Attempt 2 => Try 3
        TimeSpan.FromMinutes(9)  // Attempt 3 => Try 4
    ];

    private readonly IPulsarClient _pulsarClient;
    private readonly IHttpClientFactory _httpFactory;

    private readonly IMongoClient _mongoClient;
    private readonly MongoOptions _mongo;

    private readonly PulsarOptions _pulsar;
    private readonly SourceApiOptions _api;
    private readonly ILogger<SourceCheckProcessor> _logger;

    public SourceCheckProcessor(
        IPulsarClient pulsarClient,
        IHttpClientFactory httpFactory,
        IMongoClient mongoClient,
        IOptions<MongoOptions> mongo,
        IOptions<PulsarOptions> pulsar,
        IOptions<SourceApiOptions> api,
        ILogger<SourceCheckProcessor> logger)
    {
        _pulsarClient = pulsarClient;
        _httpFactory = httpFactory;

        _mongoClient = mongoClient;
        _mongo = mongo.Value;

        _pulsar = pulsar.Value;
        _api = api.Value;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("SourceCheckProcessor starting...");

        var sources = ResolveSourceUrls(_api);
        _logger.LogInformation("Configured source count (non-empty): {Count}", sources.Count);
        foreach (var s in sources)
            _logger.LogInformation("Source URL: {Url}", s);

        _logger.LogInformation("Pulsar source-check topic: {Topic}", _pulsar.SourceCheckTopic);
        _logger.LogInformation("Pulsar posts-batch topic: {Topic}", _pulsar.PostsBatchTopic);
        _logger.LogInformation("Pulsar DLQ topic: {Topic}", _pulsar.DlqTopic);

        _logger.LogInformation("Mongo audit success collection: {Db}.{Col}", _mongo.Database, _mongo.SourceSuccessIdCollection);
        _logger.LogInformation("Mongo audit failed collection: {Db}.{Col}", _mongo.Database, _mongo.SourceFailedIdCollection);

        await using var consumer = _pulsarClient.NewConsumer(Schema.String)
            .Topic(_pulsar.SourceCheckTopic)
            .SubscriptionName(_pulsar.Subscription)
            .InitialPosition(SubscriptionInitialPosition.Earliest)
            .Create();

        await using var checkProducer = _pulsarClient.NewProducer(Schema.String)
            .Topic(_pulsar.SourceCheckTopic)
            .Create();

        await using var batchProducer = _pulsarClient.NewProducer(Schema.String)
            .Topic(_pulsar.PostsBatchTopic)
            .Create();

        // DLQ producer is used only when we finally give up after try 4.
        await using var dlqProducer = _pulsarClient.NewProducer(Schema.String)
            .Topic(_pulsar.DlqTopic)
            .Create();

        // Seed one job per valid source URL so it starts immediately (Try 1)
        if (sources.Count == 0)
        {
            _logger.LogWarning("No valid SourceApi URLs configured. All entries are empty/null. Processor will idle.");
        }
        else
        {
            foreach (var url in sources)
            {
                var jobId = ObjectId.GenerateNewId().ToString();
                var seed = new SourceCheckJob(JobId: jobId, Url: url, Attempt: 0, CreatedAtUtc: DateTimeOffset.UtcNow);
                await checkProducer.Send(JsonSerializer.Serialize(seed), stoppingToken);

                _logger.LogInformation("Seeded SourceCheckJob jobId={JobId} (try=1) url={Url} into {Topic}", jobId, url, _pulsar.SourceCheckTopic);
            }
        }

        await foreach (var msg in consumer.Messages(stoppingToken))
        {
            try
            {
                var payload = msg.Value();

                var job = JsonSerializer.Deserialize<SourceCheckJob>(payload);
                if (job is null)
                {
                    _logger.LogWarning("Invalid SourceCheckJob payload. ack+skip. payload={Payload}", payload);
                    await consumer.Acknowledge(msg, stoppingToken);
                    continue;
                }

                if (string.IsNullOrWhiteSpace(job.Url))
                {
                    _logger.LogWarning("SourceCheckJob has empty url. ack+skip. jobId={JobId}", job.JobId);
                    await consumer.Acknowledge(msg, stoppingToken);
                    continue;
                }

                if (string.IsNullOrWhiteSpace(job.JobId) || !ObjectId.TryParse(job.JobId, out var jobObjectId))
                {
                    _logger.LogWarning("Invalid JobId in SourceCheckJob. ack+skip. jobId={JobId}", job.JobId);
                    await consumer.Acknowledge(msg, stoppingToken);
                    continue;
                }

                var tryNo = job.Attempt + 1;

                _logger.LogInformation(
                    "CHECK jobId={JobId} try={TryNo} url={Url} createdAtUtc={CreatedAtUtc}",
                    job.JobId, tryNo, job.Url, job.CreatedAtUtc);

                var fetch = await TryFetchPostsJson(job.Url, stoppingToken);

                if (!fetch.Success)
                {
                    _logger.LogWarning(
                        "RESULT jobId={JobId} try={TryNo} url={Url} => DOWN status={Status} reason={Reason}",
                        job.JobId, tryNo, job.Url, fetch.StatusCode, fetch.Reason);

                    await ScheduleRetryOrStop(job, jobObjectId, checkProducer, dlqProducer, fetch, stoppingToken);
                    await consumer.Acknowledge(msg, stoppingToken);
                    continue;
                }

                // SUCCESS => publish batch(es) + write audit success doc PER BATCH + NO more retries for this job
                _logger.LogInformation(
                    "RESULT jobId={JobId} try={TryNo} url={Url} => VALUE_FOUND status={Status} jsonLen={Len}",
                    job.JobId, tryNo, job.Url, fetch.StatusCode, fetch.RawJson?.Length ?? 0);

                var rawJson = fetch.RawJson!;

                var maxPerBatch = _api.MaxItemsPerBatch;
                if (maxPerBatch <= 0) maxPerBatch = 1000;

                using var doc = JsonDocument.Parse(rawJson);
                if (doc.RootElement.ValueKind != JsonValueKind.Array)
                {
                    _logger.LogWarning(
                        "Expected JSON array but got {Kind}. Treating as failure. jobId={JobId}",
                        doc.RootElement.ValueKind, job.JobId);

                    await ScheduleRetryOrStop(
                        job,
                        jobObjectId,
                        checkProducer,
                        dlqProducer,
                        new FetchResult(false, fetch.StatusCode, null, "unexpected_json_shape_non_array"),
                        stoppingToken);

                    await consumer.Acknowledge(msg, stoppingToken);
                    continue;
                }

                var arr = doc.RootElement;
                var total = arr.GetArrayLength();

                _logger.LogInformation("Chunking source array. totalItems={Total} maxPerBatch={Max}", total, maxPerBatch);

                var totalParts = (int)Math.Ceiling(total / (double)maxPerBatch);
                var published = 0;

                for (var start = 0; start < total; start += maxPerBatch)
                {
                    var take = Math.Min(maxPerBatch, total - start);

                    using var ms = new MemoryStream();
                    using (var writer = new Utf8JsonWriter(ms))
                    {
                        writer.WriteStartArray();
                        for (var i = start; i < start + take; i++)
                            arr[i].WriteTo(writer);
                        writer.WriteEndArray();
                    }

                    var chunkJson = Encoding.UTF8.GetString(ms.ToArray());

                    var typeMap = JsonTypeMap.BuildTypeMapFromFirstArrayObject(chunkJson);

                    using var chunkDoc = JsonDocument.Parse(chunkJson);
                    var chunkPayload = chunkDoc.RootElement.Clone();

                    // IMPORTANT: unique BatchId per chunk => unique Mongo _id per batch doc.
                    var batchId = ObjectId.GenerateNewId().ToString();
                    var batchObjectId = ObjectId.Parse(batchId);

                    published++;

                    var envelope = new PostsBatchEnvelope
                    {
                        JobId = job.JobId,
                        BatchId = batchId,
                        PartNo = published,
                        TotalParts = totalParts,
                        SourceUrl = job.Url,
                        FetchedAtUtc = DateTimeOffset.UtcNow,
                        PayloadRawJson = chunkJson,
                        TypeMap = typeMap,
                        payload = chunkPayload
                    };

                    await batchProducer.Send(JsonSerializer.Serialize(envelope), stoppingToken);

                    _logger.LogInformation(
                        "PUBLISHED_BATCH_PART jobId={JobId} batchId={BatchId} url={Url} topic={Topic} try={TryNo} part={Part}/{TotalParts} start={Start} take={Take}",
                        job.JobId, batchId, job.Url, _pulsar.PostsBatchTopic, tryNo, published, totalParts, start, take);

                    // Write audit success per batch (same _id as Source_Data batch doc)
                    await WriteSourceOutcomeAsync(
                        collectionName: _mongo.SourceSuccessIdCollection,
                        id: batchObjectId,
                        sourceUrl: job.Url,
                        fetchedAtLocalText: BuildFetchedAtLocalText(_mongo.TimeZoneId),
                        timeZoneDisplay: BuildTimeZoneDisplay(_mongo.TimeZoneId),
                        pulsarText: $"Found With try {tryNo} (jobId={job.JobId}) batch {published}/{totalParts}",
                        stoppingToken);
                }

                _logger.LogInformation(
                    "PUBLISHED_BATCH_DONE jobId={JobId} url={Url} totalItems={Total} parts={Parts} maxPerBatch={Max}",
                    job.JobId, job.Url, total, published, maxPerBatch);

                _logger.LogInformation(
                    "SOURCE_SUCCESS_STOP jobId={JobId} try={TryNo} => wrote {Db}.{Col} per batch and stopping retries.",
                    job.JobId, tryNo, _mongo.Database, _mongo.SourceSuccessIdCollection);

                await consumer.Acknowledge(msg, stoppingToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "SourceCheckProcessor failed. Leaving unacked for redelivery.");
            }
        }
    }

    private static List<string> ResolveSourceUrls(SourceApiOptions api)
    {
        var urls = new List<string>();

        if (api.PostsUrls is { Length: > 0 })
            urls.AddRange(api.PostsUrls);

        // Backward compatibility: if PostsUrls not set, or set but empty, fall back to PostsUrl
        if (urls.Count == 0)
            urls.Add(api.PostsUrl);

        return urls
            .Where(u => !string.IsNullOrWhiteSpace(u))
            .Select(u => u.Trim())
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    private sealed record FetchResult(
        bool Success,
        HttpStatusCode? StatusCode,
        string? RawJson,
        string Reason
    );

    private async Task<FetchResult> TryFetchPostsJson(string url, CancellationToken ct)
    {
        try
        {
            var http = _httpFactory.CreateClient("source");

            using var req = new HttpRequestMessage(HttpMethod.Get, url);
            using var res = await http.SendAsync(req, ct);

            if (!res.IsSuccessStatusCode)
                return new FetchResult(false, res.StatusCode, null, "non_success_status");

            var raw = await res.Content.ReadAsStringAsync(ct);

            if (string.IsNullOrWhiteSpace(raw))
                return new FetchResult(false, res.StatusCode, null, "empty_body");

            var trimmed = raw.Trim();
            if (trimmed == "[]")
                return new FetchResult(false, res.StatusCode, null, "empty_array");

            if (!trimmed.StartsWith("[", StringComparison.Ordinal))
                return new FetchResult(false, res.StatusCode, null, "unexpected_json_shape");

            return new FetchResult(true, res.StatusCode, raw, "ok");
        }
        catch (TaskCanceledException)
        {
            return new FetchResult(false, null, null, "timeout_or_canceled");
        }
        catch (HttpRequestException)
        {
            return new FetchResult(false, null, null, "http_request_exception");
        }
        catch (Exception ex)
        {
            return new FetchResult(false, null, null, $"unexpected_exception:{ex.GetType().Name}");
        }
    }

    private async Task ScheduleRetryOrStop(
        SourceCheckJob job,
        ObjectId jobObjectId,
        IProducer<string> checkProducer,
        IProducer<string> dlqProducer,
        FetchResult fetch,
        CancellationToken ct)
    {
        var nextAttempt = job.Attempt + 1;

        // RetryDelays has 3 entries => Attempt allowed: 0..3 => Try allowed: 1..4
        // If we go beyond that => STOP + DLQ + Failed audit.
        if (nextAttempt > RetryDelays.Length)
        {
            var lastTryNo = job.Attempt + 1; // this is the try that just failed (1..4)

            _logger.LogError(
                "SOURCE_FAILED_STOP jobId={JobId} lastTry={TryNo} reason={FetchReason} lastStatus={Status}",
                job.JobId, lastTryNo, fetch.Reason, fetch.StatusCode);

            // For debugging: record the exact retry schedule + where we stopped.
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
                lastStatus = fetch.StatusCode?.ToString(),
                lastReason = fetch.Reason,
                reason = "DTQ Raised - Service Unavailable (stopped after try 4)"
            });

            await dlqProducer.Send(dlqPayload, ct);

            await WriteSourceOutcomeAsync(
                collectionName: _mongo.SourceFailedIdCollection,
                id: jobObjectId,
                sourceUrl: job.Url,
                fetchedAtLocalText: BuildFetchedAtLocalText(_mongo.TimeZoneId),
                timeZoneDisplay: BuildTimeZoneDisplay(_mongo.TimeZoneId),
                pulsarText: $"DTQ Raised - Service Unavailable | stoppedAfterTry={lastTryNo} | schedule={string.Join(",", retryScheduleMinutes)}m | lastStatus={fetch.StatusCode} | lastReason={fetch.Reason}",
                ct);

            return;
        }

        // Otherwise schedule the next retry based on the retry array.
        var delay = RetryDelays[nextAttempt - 1];
        var deliverAtUtc = DateTimeOffset.UtcNow.Add(delay);

        _logger.LogWarning(
            "RETRY_SCHEDULED jobId={JobId} previousTry={PrevTry} nextTry={NextTry} delay={Delay} deliverAtUtc={DeliverAtUtc} url={Url}",
            job.JobId, job.Attempt + 1, nextAttempt + 1, delay, deliverAtUtc, job.Url);

        var nextJob = job with { Attempt = nextAttempt };
        var payload = JsonSerializer.Serialize(nextJob);

        await checkProducer.NewMessage()
            .DeliverAt(deliverAtUtc.UtcDateTime)
            .Send(payload, ct);
    }

    private async Task WriteSourceOutcomeAsync(
        string collectionName,
        ObjectId id,
        string sourceUrl,
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
            { "sourceUrl", sourceUrl },
            { "fetchedAtLocalText", fetchedAtLocalText },
            { "timeZoneId", timeZoneDisplay },
            { "pulsar", pulsarText }
        };

        await col.ReplaceOneAsync(
            filter: Builders<BsonDocument>.Filter.Eq("_id", id),
            replacement: doc,
            options: new ReplaceOptions { IsUpsert = true },
            cancellationToken: ct);
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
                try { return TimeZoneInfo.FindSystemTimeZoneById(windowsId); }
                catch { }
            }

            return TimeZoneInfo.Local;
        }
    }
}