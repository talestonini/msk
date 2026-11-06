namespace Com.Example;

/// <summary>
/// Loads a Java-style properties file (key=value, # comments) holding both Kafka
/// (librdkafka) and Glue Schema Registry settings, and separates the two: librdkafka
/// rejects unknown properties, and the Glue serializers read their settings from the
/// file themselves.
/// </summary>
public static class ConfigFile
{
    // Glue Schema Registry settings. Add any other Glue setting you use here,
    // otherwise librdkafka fails with "No such configuration property".
    private static readonly HashSet<string> GlueKeys = new(StringComparer.OrdinalIgnoreCase)
    {
        "region", "endpoint", "proxyUrl", "registry.name", "description", "dataFormat",
        "compatibility", "schemaAutoRegistrationEnabled", "cacheSize", "timeToLiveMillis",
    };

    /// <summary>Returns the file's Kafka settings, i.e. everything that isn't a Glue setting.</summary>
    public static Dictionary<string, string> LoadKafkaConfig(string path)
    {
        var config = new Dictionary<string, string>();
        foreach (var line in File.ReadAllLines(path))
        {
            var trimmed = line.Trim();
            if (trimmed.Length == 0 || trimmed.StartsWith('#'))
            {
                continue;
            }
            var equals = trimmed.IndexOf('=');
            if (equals <= 0)
            {
                continue;
            }
            var key = trimmed[..equals].Trim();
            if (!GlueKeys.Contains(key))
            {
                config[key] = trimmed[(equals + 1)..].Trim();
            }
        }
        return config;
    }
}
