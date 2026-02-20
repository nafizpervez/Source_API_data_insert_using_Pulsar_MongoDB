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

                // Preserve JSON types by parsing into BSON
                var wrapper = BsonDocument.Parse("{\"items\":" + env.PayloadRawJson + "}");
                var items = wrapper["items"].AsBsonArray;

                var typeMapDoc = new BsonDocument();
                foreach (var kv in env.TypeMap)
                    typeMapDoc[kv.Key] = kv.Value;

                var doc = new BsonDocument
                {
                    { "sourceUrl", env.SourceUrl },
                    { "fetchedAtUtc", env.FetchedAtUtc.UtcDateTime },
                    { "itemCount", items.Count },
                    { "typeMap", typeMapDoc },
                    { "payload", items }
                };

                await col.InsertOneAsync(doc, cancellationToken: stoppingToken);

                _logger.LogInformation("Inserted batch into MongoDB. itemCount={Count}", items.Count);

                await consumer.Acknowledge(msg, stoppingToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "MongoWriter failed. Leaving unacked for redelivery.");
            }
        }
    }
}