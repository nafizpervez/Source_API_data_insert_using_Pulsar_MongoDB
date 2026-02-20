using System.Text.Json;
using System.Text.Json.Nodes;

namespace SourceIngestor.Worker.Utils;

public static class JsonTypeMap
{
    public static Dictionary<string, string> BuildTypeMapFromFirstArrayObject(string rawJson)
    {
        var map = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        JsonNode? node;
        try { node = JsonNode.Parse(rawJson); }
        catch { return map; }

        var first = node?.AsArray().FirstOrDefault()?.AsObject();
        if (first is null) return map;

        foreach (var kv in first)
        {
            map[kv.Key] = kv.Value switch
            {
                null => "null",
                JsonValue v when v.TryGetValue<int>(out _) => "int",
                JsonValue v when v.TryGetValue<long>(out _) => "long",
                JsonValue v when v.TryGetValue<double>(out _) => "double",
                JsonValue v when v.TryGetValue<bool>(out _) => "bool",
                JsonValue v when v.TryGetValue<string>(out _) => "string",
                JsonArray => "array",
                JsonObject => "object",
                _ => "unknown"
            };
        }

        return map;
    }
}