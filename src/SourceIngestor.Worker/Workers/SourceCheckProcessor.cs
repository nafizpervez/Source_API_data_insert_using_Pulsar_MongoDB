// D:\Source_API_data_insert_using_Pulsar_MongoDB\src\SourceIngestor.Worker\Workers\SourceCheckProcessor.cs
using System.Net;
using System.Text.Json;
using DotPulsar;
using DotPulsar.Abstractions;
using DotPulsar.Extensions;
using Microsoft.Extensions.Options;
using SourceIngestor.Worker.Messaging.Contracts;
using SourceIngestor.Worker.Options;
using SourceIngestor.Worker.Utils;

namespace SourceIngestor.Worker.Workers;

public sealed class SourceCheckProcessor : BackgroundService
{
    // Your required schedule:
    // Attempt 0: immediate (no delay)
    // Attempt 1: +1 minute
    // Attempt 2: +3 minutes
    // Attempt 3: +9 minutes
    // After that: DLQ
    private static readonly TimeSpan[] RetryDelays =
    [
        TimeSpan.FromMinutes(1),
        TimeSpan.FromMinutes(3),
        TimeSpan.FromMinutes(9)
    ];

    private readonly IPulsarClient _pulsarClient;
    private readonly IHttpClientFactory _httpFactory;
    private readonly PulsarOptions _pulsar;
    private readonly SourceApiOptions _api;
    private readonly JsonSerializerOptions _json;
    private readonly ILogger<SourceCheckProcessor> _logger;

    public SourceCheckProcessor(
        IPulsarClient pulsarClient,
        IHttpClientFactory httpFactory,
        IOptions<PulsarOptions> pulsar,
        IOptions<SourceApiOptions> api,
        JsonSerializerOptions json,
        ILogger<SourceCheckProcessor> logger)
    {
        _pulsarClient = pulsarClient;
        _httpFactory = httpFactory;
        _pulsar = pulsar.Value;
        _api = api.Value;
        _json = json;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("SourceCheckProcessor starting...");
        _logger.LogInformation("Source URL: {Url}", _api.PostsUrl);
        _logger.LogInformation("Pulsar source-check topic: {Topic}", _pulsar.SourceCheckTopic);
        _logger.LogInformation("Pulsar posts-batch topic: {Topic}", _pulsar.PostsBatchTopic);
        _logger.LogInformation("Pulsar DLQ topic: {Topic}", _pulsar.DlqTopic);

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

        await using var dlqProducer = _pulsarClient.NewProducer(Schema.String)
            .Topic(_pulsar.DlqTopic)
            .Create();

        // Seed 1 job so it runs immediately (Attempt 0)
        var seed = new SourceCheckJob(_api.PostsUrl, Attempt: 0, CreatedAtUtc: DateTimeOffset.UtcNow);
        var seedJson = JsonSerializer.Serialize(seed, _json);
        await checkProducer.Send(seedJson, stoppingToken);

        _logger.LogInformation("Seeded SourceCheckJob (attempt=0) into {Topic}", _pulsar.SourceCheckTopic);

        await foreach (var msg in consumer.Messages(stoppingToken))
        {
            try
            {
                var payload = msg.Value();

                var job = JsonSerializer.Deserialize<SourceCheckJob>(payload, _json);
                if (job is null)
                {
                    _logger.LogWarning("Invalid SourceCheckJob payload. ack+skip. payload={Payload}", payload);
                    await consumer.Acknowledge(msg, stoppingToken);
                    continue;
                }

                _logger.LogInformation(
                    "CHECK attempt={Attempt} url={Url} createdAtUtc={CreatedAtUtc}",
                    job.Attempt, job.Url, job.CreatedAtUtc);

                // SINGLE HTTP call per attempt:
                // - If not success or invalid payload -> retry/DLQ
                // - If success and non-empty -> publish batch
                var fetch = await TryFetchPostsJson(job.Url, stoppingToken);

                if (!fetch.Success)
                {
                    _logger.LogWarning(
                        "RESULT attempt={Attempt} url={Url} => NOT_FOUND/DOWN status={Status} reason={Reason}",
                        job.Attempt, job.Url, fetch.StatusCode, fetch.Reason);

                    await ScheduleRetryOrDlq(job, checkProducer, dlqProducer, fetch, stoppingToken);
                    await consumer.Acknowledge(msg, stoppingToken);
                    continue;
                }

                // success
                _logger.LogInformation(
                    "RESULT attempt={Attempt} url={Url} => VALUE_FOUND. retriesNotNeeded=true status={Status} jsonLen={Len}",
                    job.Attempt, job.Url, fetch.StatusCode, fetch.RawJson?.Length ?? 0);

                var rawJson = fetch.RawJson!;

                // Build a simple runtime type map (for logging/debugging in Mongo)
                var typeMap = JsonTypeMap.BuildTypeMapFromFirstArrayObject(rawJson);

                // Strongly-typed payload (optional but useful)
                var posts = JsonSerializer.Deserialize<List<PostDto>>(rawJson, _json) ?? new List<PostDto>();

                // Publish the batch envelope to Pulsar
                // NOTE: this assumes PostsBatchEnvelope has these settable properties:
                // SourceUrl, FetchedAtUtc, PayloadRawJson, TypeMap, Payload
                var envelope = new PostsBatchEnvelope
                {
                    SourceUrl = job.Url,
                    FetchedAtUtc = DateTimeOffset.UtcNow,
                    PayloadRawJson = rawJson,
                    TypeMap = typeMap,
                    Payload = posts
                };

                var envJson = JsonSerializer.Serialize(envelope, _json);
                await batchProducer.Send(envJson, stoppingToken);

                _logger.LogInformation(
                    "PUBLISHED_BATCH url={Url} topic={Topic} attempt={Attempt}",
                    job.Url, _pulsar.PostsBatchTopic, job.Attempt);

                await consumer.Acknowledge(msg, stoppingToken);
            }
            catch (Exception ex)
            {
                // Leave unacked => redelivery by Pulsar subscription policy
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
                return new FetchResult(
                    Success: false,
                    StatusCode: res.StatusCode,
                    RawJson: null,
                    Reason: "non_success_status"
                );
            }

            var raw = await res.Content.ReadAsStringAsync(ct);

            // Treat "cannot find data" as:
            // - empty/whitespace
            // - or looks like an empty JSON array "[]"
            if (string.IsNullOrWhiteSpace(raw))
            {
                return new FetchResult(
                    Success: false,
                    StatusCode: res.StatusCode,
                    RawJson: null,
                    Reason: "empty_body"
                );
            }

            var trimmed = raw.Trim();
            if (trimmed == "[]")
            {
                return new FetchResult(
                    Success: false,
                    StatusCode: res.StatusCode,
                    RawJson: null,
                    Reason: "empty_array"
                );
            }

            // Ensure it's a JSON array (expected for /posts)
            if (!trimmed.StartsWith("[", StringComparison.Ordinal))
            {
                return new FetchResult(
                    Success: false,
                    StatusCode: res.StatusCode,
                    RawJson: null,
                    Reason: "unexpected_json_shape"
                );
            }

            return new FetchResult(
                Success: true,
                StatusCode: res.StatusCode,
                RawJson: raw,
                Reason: "ok"
            );
        }
        catch (TaskCanceledException)
        {
            return new FetchResult(
                Success: false,
                StatusCode: null,
                RawJson: null,
                Reason: "timeout_or_canceled"
            );
        }
        catch (HttpRequestException)
        {
            return new FetchResult(
                Success: false,
                StatusCode: null,
                RawJson: null,
                Reason: "http_request_exception"
            );
        }
        catch (Exception ex)
        {
            return new FetchResult(
                Success: false,
                StatusCode: null,
                RawJson: null,
                Reason: $"unexpected_exception:{ex.GetType().Name}"
            );
        }
    }

    private async Task ScheduleRetryOrDlq(
        SourceCheckJob job,
        IProducer<string> checkProducer,
        IProducer<string> dlqProducer,
        FetchResult fetch,
        CancellationToken ct)
    {
        // Attempt meanings:
        // job.Attempt == 0 => immediate attempt
        // nextAttempt == 1 => retry after 1m
        // nextAttempt == 2 => retry after 3m
        // nextAttempt == 3 => retry after 9m
        // nextAttempt >= 4 => DLQ
        var nextAttempt = job.Attempt + 1;

        // We only have 3 retry delays (for attempts 1..3). Anything beyond => DLQ.
        if (nextAttempt > RetryDelays.Length)
        {
            _logger.LogError(
                "DLQ_SENT url={Url} finalAttempt={Attempt} reason=unavailable_after_retries lastStatus={Status} lastFetchReason={FetchReason}",
                job.Url, nextAttempt, fetch.StatusCode, fetch.Reason);

            var dlqPayload = JsonSerializer.Serialize(new
            {
                job.Url,
                attempt = nextAttempt,
                createdAtUtc = job.CreatedAtUtc,
                failedAtUtc = DateTimeOffset.UtcNow,
                lastStatus = fetch.StatusCode?.ToString(),
                lastReason = fetch.Reason,
                reason = "Source API unavailable after attempts: 0 (immediate), 1(+1m), 2(+3m), 3(+9m)"
            }, _json);

            await dlqProducer.Send(dlqPayload, ct);
            return;
        }

        var delay = RetryDelays[nextAttempt - 1];
        var deliverAtUtc = DateTimeOffset.UtcNow.Add(delay);

        _logger.LogWarning(
            "RETRY_SCHEDULED previousAttempt={PrevAttempt} nextAttempt={NextAttempt} delay={Delay} deliverAtUtc={DeliverAtUtc} url={Url}",
            job.Attempt, nextAttempt, delay, deliverAtUtc, job.Url);

        var nextJob = job with { Attempt = nextAttempt };
        var nextPayload = JsonSerializer.Serialize(nextJob, _json);

        // Pulsar delayed delivery
        await checkProducer.NewMessage()
            .DeliverAt(deliverAtUtc.UtcDateTime)
            .Send(nextPayload, ct);
    }
}