using AWSGsrSerDe.serializer;
using com.example.events;
using Confluent.Kafka;

namespace Com.Example;

/// <summary>
/// Sends a number of random TemperatureChanged events to Kafka in a single transaction,
/// so either all of them are delivered exactly once or none are (exactly-once semantics).
///
/// <para>Usage: <c>dotnet run -- produce [config-file] [event-count]</c>
/// (defaults: <c>producer.properties</c>, 10 events).</para>
///
/// <para>All Kafka, SASL/SCRAM and Glue Schema Registry settings come from the config file,
/// which must set <c>transactional.id</c>.</para>
/// </summary>
public static class TemperatureProducer
{
    private const string Topic = "com.example.events.TemperatureChanged";
    private static readonly string[] ThermometerIds = ["thermometer-1", "thermometer-2", "thermometer-3"];
    private static readonly TimeSpan TransactionTimeout = TimeSpan.FromSeconds(30);

    public static int Run(string[] args)
    {
        var configFile = args.Length > 0 ? args[0] : "producer.properties";
        var eventCount = args.Length > 1 ? int.Parse(args[1]) : 10;

        var config = new ProducerConfig(ConfigFile.LoadKafkaConfig(configFile));
        var random = new Random();

        using var producer = new ProducerBuilder<string, TemperatureChanged>(config)
            .SetValueSerializer(new GlueAvroSerializer(configFile))
            .Build();

        // Registers the transactional.id with the cluster and fences off any older
        // producer instance using the same id.
        try
        {
            producer.InitTransactions(TransactionTimeout);
        }
        catch (KafkaException e)
        {
            Console.Error.WriteLine($"Could not initialise transactions: {e.Message}");
            return 1;
        }

        try
        {
            producer.BeginTransaction();
            for (var i = 0; i < eventCount; i++)
            {
                var thermometerId = ThermometerIds[random.Next(ThermometerIds.Length)];
                var now = DateTime.UtcNow;
                var evt = new TemperatureChanged
                {
                    thermometerId = thermometerId,
                    temperature = 15.0f + random.NextSingle() * 20.0f,
                    timestamp = now.AddTicks(-(now.Ticks % TimeSpan.TicksPerMillisecond)),
                };

                // Keyed by thermometer, so each thermometer's events stay in order on one partition.
                producer.Produce(Topic, new Message<string, TemperatureChanged> { Key = thermometerId, Value = evt },
                    report =>
                    {
                        if (report.Error.Code == ErrorCode.NoError)
                        {
                            Console.WriteLine($"Sent {Describe(evt)} -> partition {report.Partition.Value}, offset {report.Offset.Value}");
                        }
                    });
            }
            // Flushes all sends; throws if any of them failed.
            producer.CommitTransaction(TransactionTimeout);
        }
        catch (KafkaException e) when (e.Error.IsFatal)
        {
            // Fatal (for example fenced by a newer producer with the same transactional.id):
            // this producer can't continue, or even abort.
            Console.Error.WriteLine($"Transaction failed, producer must be closed: {e.Message}");
            return 1;
        }
        catch (Exception e) when (e is KafkaException or ProduceException<string, TemperatureChanged>)
        {
            // Anything else: abort, so consumers using read_committed never see these events.
            producer.AbortTransaction(TransactionTimeout);
            Console.Error.WriteLine($"Transaction aborted: {e.Message}");
            return 1;
        }

        Console.WriteLine($"Committed {eventCount} events to {Topic}");
        return 0;
    }

    private static string Describe(TemperatureChanged e) =>
        $"{{thermometerId={e.thermometerId}, temperature={e.temperature:F2}, timestamp={e.timestamp:O}}}";

    /// <summary>Adapts the Glue Schema Registry serializer to Confluent.Kafka's ISerializer.</summary>
    private sealed class GlueAvroSerializer(string configFile) : ISerializer<TemperatureChanged>
    {
        private readonly GlueSchemaRegistryKafkaSerializer _serializer = new(configFile);

        // The schema is looked up in the registry by the topic name.
        public byte[] Serialize(TemperatureChanged data, SerializationContext context) =>
            _serializer.Serialize(data, context.Topic);
    }
}
