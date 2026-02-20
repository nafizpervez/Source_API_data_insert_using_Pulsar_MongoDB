using DotPulsar;
using DotPulsar.Abstractions;
using Microsoft.Extensions.Options;
using MongoDB.Driver;
using SourceIngestor.Worker.Options;
using SourceIngestor.Worker.Workers;

var builder = Host.CreateApplicationBuilder(args);

// Options
builder.Services.Configure<PulsarOptions>(builder.Configuration.GetSection("Pulsar"));
builder.Services.Configure<MongoOptions>(builder.Configuration.GetSection("Mongo"));
builder.Services.Configure<SourceApiOptions>(builder.Configuration.GetSection("SourceApi"));

// Http client
builder.Services.AddHttpClient("source", http =>
{
    http.Timeout = TimeSpan.FromSeconds(15);
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
builder.Services.AddHostedService<ValidationProcessor>();

var host = builder.Build();
host.Run();