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
    // Max 3 tries total:
    // Attempt 0 => try 1 (immediate)
    // Attempt 1 => try 2 (+1m)
    // Attempt 2 => try 3 (+3m)
    // After that => STOP + mark Failed
    private static readonly TimeSpan[] RetryDelays =
    [
        TimeSpan.FromMinutes(1), // attempt 1
        TimeSpan.FromMinutes(3)  // attempt 2
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
        _logger.LogInformation("Source URL: {Url}", _api.PostsUrl);
        _logger.LogInformation("Pulsar source-check topic: {Topic}", _pulsar.SourceCheckTopic);
        _logger.LogInformation("Pulsar posts-batch topic: {Topic}", _pulsar.PostsBatchTopic);
        _logger.LogInformation("Pulsar DLQ topic (unused for stop-on-fail): {Topic}", _pulsar.DlqTopic);

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

        // We keep DLQ producer for compatibility/logging, but per your new requirement
        // we will STOP after try 3 and write Source_Failed_ID instead of continuing retries.
        await using var dlqProducer = _pulsarClient.NewProducer(Schema.String)
            .Topic(_pulsar.DlqTopic)
            .Create();

        // Seed 1 job so it runs immediately (Attempt 0 => try 1)
        var jobId = ObjectId.GenerateNewId().ToString();
        var seed = new SourceCheckJob(JobId: jobId, Url: _api.PostsUrl, Attempt: 0, CreatedAtUtc: DateTimeOffset.UtcNow);
        var seedJson = JsonSerializer.Serialize(seed);
        await checkProducer.Send(seedJson, stoppingToken);

        _logger.LogInformation("Seeded SourceCheckJob jobId={JobId} (try=1) into {Topic}", jobId, _pulsar.SourceCheckTopic);

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
                        "RESULT jobId={JobId} try={TryNo} url={Url} => NOT_FOUND/DOWN status={Status} reason={Reason}",
                        job.JobId, tryNo, job.Url, fetch.StatusCode, fetch.Reason);

                    await ScheduleRetryOrStop(job, jobObjectId, checkProducer, dlqProducer, fetch, stoppingToken);

                    await consumer.Acknowledge(msg, stoppingToken);
                    continue;
                }

                // SUCCESS => publish batch(es) + write audit success doc + NO more retries
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

                    var envelope = new PostsBatchEnvelope
                    {
                        JobId = job.JobId, // critical: same _id in Source_Data
                        SourceUrl = job.Url,
                        FetchedAtUtc = DateTimeOffset.UtcNow,
                        PayloadRawJson = chunkJson,
                        TypeMap = typeMap,
                        payload = chunkPayload
                    };

                    var envJson = JsonSerializer.Serialize(envelope);
                    await batchProducer.Send(envJson, stoppingToken);

                    published++;

                    _logger.LogInformation(
                        "PUBLISHED_BATCH_PART jobId={JobId} url={Url} topic={Topic} try={TryNo} part={Part} start={Start} take={Take}",
                        job.JobId, job.Url, _pulsar.PostsBatchTopic, tryNo, published, start, take);
                }

                _logger.LogInformation(
                    "PUBLISHED_BATCH_DONE jobId={JobId} url={Url} totalItems={Total} parts={Parts} maxPerBatch={Max}",
                    job.JobId, job.Url, total, published, maxPerBatch);

                // Write audit success with same ObjectId
                await WriteSourceOutcomeAsync(
                    collectionName: _mongo.SourceSuccessIdCollection,
                    id: jobObjectId,
                    sourceUrl: job.Url,
                    fetchedAtLocalText: BuildFetchedAtLocalText(_mongo.TimeZoneId),
                    timeZoneDisplay: BuildTimeZoneDisplay(_mongo.TimeZoneId),
                    pulsarText: $"Found With try {tryNo}",
                    stoppingToken);

                _logger.LogInformation(
                    "SOURCE_SUCCESS_STOP jobId={JobId} try={TryNo} => wrote {Db}.{Col} and stopping retries.",
                    job.JobId, tryNo, _mongo.Database, _mongo.SourceSuccessIdCollection);

                await consumer.Acknowledge(msg, stoppingToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "SourceCheckProcessor failed. Leaving unacked for redelivery.");
            }
        }
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
            {
                return new FetchResult(false, res.StatusCode, null, "non_success_status");
            }

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

        // With RetryDelays length=2:
        // attempts allowed: 0,1,2 (tries 1,2,3)
        // if nextAttempt > 2 => this would become attempt 3 (try 4) => STOP + FAILED
        if (nextAttempt > RetryDelays.Length)
        {
            var tryNo = job.Attempt + 1;

            _logger.LogError(
                "SOURCE_FAILED_STOP jobId={JobId} lastTry={TryNo} reason={FetchReason} lastStatus={Status}",
                job.JobId, tryNo, fetch.Reason, fetch.StatusCode);

            // Optional: also publish to DLQ topic for visibility (but STOP retries anyway)
            var dlqPayload = JsonSerializer.Serialize(new
            {
                job.JobId,
                job.Url,
                attempt = job.Attempt,
                createdAtUtc = job.CreatedAtUtc,
                failedAtUtc = DateTimeOffset.UtcNow,
                lastStatus = fetch.StatusCode?.ToString(),
                lastReason = fetch.Reason,
                reason = "DTQ Raised - Service Unavailable (stopped after try 3)"
            });

            await dlqProducer.Send(dlqPayload, ct);

            // Write audit failed with same ObjectId
            await WriteSourceOutcomeAsync(
                collectionName: _mongo.SourceFailedIdCollection,
                id: jobObjectId,
                sourceUrl: job.Url,
                fetchedAtLocalText: BuildFetchedAtLocalText(_mongo.TimeZoneId),
                timeZoneDisplay: BuildTimeZoneDisplay(_mongo.TimeZoneId),
                pulsarText: "DTQ Raised - Service Unavailable",
                ct);

            return;
        }

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