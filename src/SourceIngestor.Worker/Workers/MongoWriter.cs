// D:\Source_API_data_insert_using_Pulsar_MongoDB\src\SourceIngestor.Worker\Workers\MongoWriter.cs

using System.Globalization;
using System.Text.Json;
using DotPulsar;
using DotPulsar.Abstractions;
using DotPulsar.Extensions;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;
using SourceIngestor.Worker.Messaging.Contracts;
using SourceIngestor.Worker.Options;

namespace SourceIngestor.Worker.Workers;

public sealed class MongoWriter : BackgroundService
{
    private readonly IPulsarClient _pulsarClient;
    private readonly IMongoClient _mongoClient;
    private readonly PulsarOptions _pulsar;
    private readonly MongoOptions _mongo;
    private readonly ILogger<MongoWriter> _logger;

    public MongoWriter(
        IPulsarClient pulsarClient,
        IMongoClient mongoClient,
        IOptions<PulsarOptions> pulsar,
        IOptions<MongoOptions> mongo,
        ILogger<MongoWriter> logger)
    {
        _pulsarClient = pulsarClient;
        _mongoClient = mongoClient;
        _pulsar = pulsar.Value;
        _mongo = mongo.Value;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var db = _mongoClient.GetDatabase(_mongo.Database);
        var col = db.GetCollection<BsonDocument>(_mongo.Collection);

        await using var consumer = _pulsarClient.NewConsumer(Schema.String)
            .Topic(_pulsar.PostsBatchTopic)
            .SubscriptionName(_pulsar.Subscription + "-mongo")
            .InitialPosition(SubscriptionInitialPosition.Earliest)
            .Create();

        _logger.LogInformation("MongoWriter started. Listening on {Topic}", _pulsar.PostsBatchTopic);
        _logger.LogInformation("Mongo target: {Db}.{Col}", _mongo.Database, _mongo.Collection);
        _logger.LogInformation("Configured Mongo TimeZoneId: {TimeZoneId}", _mongo.TimeZoneId ?? "(null)");

        await foreach (var msg in consumer.Messages(stoppingToken))
        {
            try
            {
                var payload = msg.Value();
                _logger.LogInformation("MongoWriter received batch message. size={Size}", payload.Length);

                var env = JsonSerializer.Deserialize<PostsBatchEnvelope>(payload);
                if (env is null)
                {
                    _logger.LogWarning("Invalid PostsBatchEnvelope, ack + skip.");
                    await consumer.Acknowledge(msg, stoppingToken);
                    continue;
                }

                // Resolve timezone PER MESSAGE (safe if you later change config dynamically)
                var tz = ResolveTimeZone(_mongo.TimeZoneId);

                // Parse payload JSON into BSON (keeps types like numbers as numbers)
                var wrapper = BsonDocument.Parse("{\"items\":" + env.PayloadRawJson + "}");
                var items = wrapper["items"].AsBsonArray;

                // RULE: Convert any ISO8601 datetime string found anywhere inside payload
                // into "M/d/yyyy, hh:mm tt" in the configured deployment timezone.
                ConvertIsoDateStringsInPlace(items, tz);

                // Type map stored for debugging
                var typeMapDoc = new BsonDocument();
                foreach (var kv in env.TypeMap)
                    typeMapDoc[kv.Key] = kv.Value;

                // Convert fetched time to local and format
                var fetchedUtc = env.FetchedAtUtc.UtcDateTime;
                var fetchedLocal = TimeZoneInfo.ConvertTimeFromUtc(fetchedUtc, tz);
                var fetchedAtLocalText = fetchedLocal.ToString("M/d/yyyy, hh:mm tt", CultureInfo.InvariantCulture);

                // Build "Asia/Dhaka UTC+06:00"
                var offset = tz.GetUtcOffset(fetchedUtc);
                var sign = offset >= TimeSpan.Zero ? "+" : "-";
                var abs = offset.Duration();
                var offsetText = $" {sign}{abs:hh\\:mm}";
                var timeZoneDisplay = $"{tz.Id} UTC{offsetText}";

                // IMPORTANT: You requested fetchedAtUtc should NOT be stored.
                var doc = new BsonDocument
                {
                    { "sourceUrl", env.SourceUrl },
                    { "fetchedAtLocalText", fetchedAtLocalText },
                    { "timeZoneId", timeZoneDisplay },
                    { "itemCount", items.Count },
                    { "typeMap", typeMapDoc },
                    { "payload", items }
                };
                
                // Keep only the latest batch: clear existing docs before inserting the new one.
                await col.DeleteManyAsync(FilterDefinition<BsonDocument>.Empty, cancellationToken: stoppingToken);

                await col.InsertOneAsync(doc, cancellationToken: stoppingToken);

                _logger.LogInformation(
                    "Inserted batch into MongoDB. itemCount={Count} tz={Tz} fetchedAtLocal={FetchedLocal}",
                    items.Count, timeZoneDisplay, fetchedAtLocalText);

                await consumer.Acknowledge(msg, stoppingToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "MongoWriter failed. Leaving unacked for redelivery.");
            }
        }
    }

    private static void ConvertIsoDateStringsInPlace(BsonValue value, TimeZoneInfo tz)
    {
        switch (value.BsonType)
        {
            case BsonType.Array:
            {
                var arr = value.AsBsonArray;
                for (var i = 0; i < arr.Count; i++)
                {
                    var v = arr[i];

                    if (v.BsonType == BsonType.String)
                    {
                        var s = v.AsString;
                        if (TryParseIso8601(s, out var dto))
                        {
                            // Convert to target timezone and format
                            var local = TimeZoneInfo.ConvertTime(dto, tz);
                            var text = local.ToString("M/d/yyyy, hh:mm tt", CultureInfo.InvariantCulture);
                            arr[i] = text;
                        }
                    }
                    else
                    {
                        ConvertIsoDateStringsInPlace(v, tz);
                    }
                }
                break;
            }

            case BsonType.Document:
            {
                var doc = value.AsBsonDocument;
                var keys = doc.Names.ToList(); // snapshot keys
                foreach (var key in keys)
                {
                    var v = doc[key];

                    if (v.BsonType == BsonType.String)
                    {
                        var s = v.AsString;
                        if (TryParseIso8601(s, out var dto))
                        {
                            var local = TimeZoneInfo.ConvertTime(dto, tz);
                            var text = local.ToString("M/d/yyyy, hh:mm tt", CultureInfo.InvariantCulture);
                            doc[key] = text;
                        }
                    }
                    else
                    {
                        ConvertIsoDateStringsInPlace(v, tz);
                    }
                }
                break;
            }
        }
    }

    private static bool TryParseIso8601(string s, out DateTimeOffset dto)
    {
        dto = default;

        if (string.IsNullOrWhiteSpace(s))
            return false;

        // quick filter to avoid parsing normal strings like titles/bodies
        if (!s.Contains('T') || !s.Contains(':'))
            return false;

        return DateTimeOffset.TryParse(
            s,
            CultureInfo.InvariantCulture,
            DateTimeStyles.RoundtripKind,
            out dto
        );
    }

    private static TimeZoneInfo ResolveTimeZone(string? timeZoneId)
    {
        // If env not set, fallback to container local (often UTC)
        if (string.IsNullOrWhiteSpace(timeZoneId))
            return TimeZoneInfo.Local;

        // Primary: try as-is (works for IANA IDs on Linux if tzdata installed)
        try
        {
            return TimeZoneInfo.FindSystemTimeZoneById(timeZoneId);
        }
        catch
        {
            // If running on Windows, TimeZoneInfo expects Windows IDs.
            // Minimal IANA -> Windows mapping fallback.
            var map = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["Asia/Dhaka"] = "Bangladesh Standard Time",
                ["Asia/Kuala_Lumpur"] = "Singapore Standard Time",
                ["Asia/Singapore"] = "Singapore Standard Time"
            };

            if (map.TryGetValue(timeZoneId, out var windowsId))
            {
                try { return TimeZoneInfo.FindSystemTimeZoneById(windowsId); }
                catch { /* ignore */ }
            }

            // final fallback
            return TimeZoneInfo.Local;
        }
    }
}