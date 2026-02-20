using System.Text.Json;

namespace SourceIngestor.Worker.Utils;

public static class JsonTypeMap
{
    // Builds a "fieldName -> jsonType" map from the first object in a JSON array payload.
    // jsonType values are JSON-native: number|string|boolean|null|object|array
    public static Dictionary<string, string> BuildTypeMapFromFirstArrayObject(string rawJson)
    {
        var map = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        if (string.IsNullOrWhiteSpace(rawJson))
            return map;

        try
        {
            using var doc = JsonDocument.Parse(rawJson);
            var root = doc.RootElement;

            if (root.ValueKind != JsonValueKind.Array || root.GetArrayLength() == 0)
                return map;

            var first = root[0];
            if (first.ValueKind != JsonValueKind.Object)
                return map;

            foreach (var prop in first.EnumerateObject())
            {
                map[prop.Name] = prop.Value.ValueKind switch
                {
                    JsonValueKind.Number => "number",
                    JsonValueKind.String => "string",
                    JsonValueKind.True or JsonValueKind.False => "boolean",
                    JsonValueKind.Null => "null",
                    JsonValueKind.Object => "object",
                    JsonValueKind.Array => "array",
                    JsonValueKind.Undefined => "undefined",
                    _ => "unknown"
                };
            }

            return map;
        }
        catch
        {
            return map;
        }
    }
}