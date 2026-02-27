using DotPulsar;
using DotPulsar.Abstractions;
using MongoDB.Driver;
using SourceIngestor.Worker.Options;
using SourceIngestor.Worker.Workers;

var builder = Host.CreateApplicationBuilder(args);

// Options
builder.Services.Configure<PulsarOptions>(builder.Configuration.GetSection("Pulsar"));
builder.Services.Configure<MongoOptions>(builder.Configuration.GetSection("Mongo"));
builder.Services.Configure<SourceApiOptions>(builder.Configuration.GetSection("SourceApi"));
builder.Services.Configure<ArcGisPortalOptions>(builder.Configuration.GetSection("ArcGisPortal"));
builder.Services.Configure<DestinationApiOptions>(builder.Configuration.GetSection("DestinationApi"));
builder.Services.Configure<DuplicationOptions>(builder.Configuration.GetSection("Duplication"));

// Http clients
builder.Services.AddHttpClient("source", http =>
{
    http.Timeout = TimeSpan.FromSeconds(15);
});

builder.Services.AddHttpClient("arcgis", http =>
{
    http.Timeout = TimeSpan.FromSeconds(30);
});

// Pulsar client
builder.Services.AddSingleton<IPulsarClient>(_ =>
{
    var opts = builder.Configuration.GetSection("Pulsar").Get<PulsarOptions>()!;
    return PulsarClient.Builder()
        .ServiceUrl(new Uri(opts.ServiceUrl))
        .Build();
});

// Mongo client
builder.Services.AddSingleton<IMongoClient>(_ =>
{
    var opts = builder.Configuration.GetSection("Mongo").Get<MongoOptions>()!;
    return new MongoClient(opts.ConnectionString);
});

// Hosted services
builder.Services.AddHostedService<SourceCheckProcessor>();
builder.Services.AddHostedService<MongoWriter>();

// SINGLE orchestrator (does: valid/dup/skip/update/delete/insert/applyEdits/final/counts)
builder.Services.AddHostedService<DestinationCheckProcessor>();

var host = builder.Build();
host.Run();