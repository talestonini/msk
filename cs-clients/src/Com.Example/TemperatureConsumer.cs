using AWSGsrSerDe.common;
using AWSGsrSerDe.deserializer;
using com.example.events;
using Confluent.Kafka;

namespace Com.Example;

/// <summary>
/// Reads TemperatureChanged events and prints them, until stopped with Ctrl+C.
///
/// <para>Usage: <c>dotnet run -- consume [config-file]</c> (default: <c>consumer.properties</c>).</para>
///
/// <para>With <c>isolation.level=read_committed</c> it only sees events from committed
/// transactions. Offsets are committed after each event is processed, so a crash
/// between processing and committing re-delivers that event: processing must be
/// idempotent for the end result to be exactly-once.</para>
/// </summary>
public static class TemperatureConsumer
{
    private const string Topic = "com.example.events.TemperatureChanged";

    public static int Run(string[] args)
    {
        var configFile = args.Length > 0 ? args[0] : "consumer.properties";
        var config = new ConsumerConfig(ConfigFile.LoadKafkaConfig(configFile));

        using var consumer = new ConsumerBuilder<string, TemperatureChanged>(config)
            .SetValueDeserializer(new GlueAvroDeserializer(configFile))
            .Build();

        // On Ctrl+C, stop the consume loop and close cleanly.
        using var cancellation = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;
            cancellation.Cancel();
        };

        consumer.Subscribe(Topic);
        Console.WriteLine($"Consuming from {Topic} (Ctrl+C to stop)");
        try
        {
            while (true)
            {
                var result = consumer.Consume(cancellation.Token);
                Process(result);
                consumer.Commit(result);
            }
        }
        catch (OperationCanceledException)
        {
            // Expected on shutdown.
        }
        finally
        {
            // Leaves the group right away, so its partitions are reassigned without waiting for a timeout.
            consumer.Close();
        }
        Console.WriteLine("Consumer closed");
        return 0;
    }

    private static void Process(ConsumeResult<string, TemperatureChanged> result)
    {
        var e = result.Message.Value;
        Console.WriteLine($"Received {e.thermometerId}={e.temperature:F2}°C at {e.timestamp:O} " +
                          $"(partition {result.Partition.Value}, offset {result.Offset.Value})");
    }

    /// <summary>Adapts the Glue Schema Registry deserializer to Confluent.Kafka's IDeserializer.</summary>
    private sealed class GlueAvroDeserializer(string configFile) : IDeserializer<TemperatureChanged>
    {
        // Deserialize into the generated com.example.events.TemperatureChanged class. This must be set
        // in code: the library can't read it from the properties file.
        private readonly GlueSchemaRegistryKafkaDeserializer _deserializer = new(
            configFile,
            new GlueSchemaRegistryDataFormatConfiguration(new Dictionary<string, dynamic>
            {
                [GlueSchemaRegistryConstants.AvroRecordType] = AvroRecordType.SpecificRecord,
            }));

        // The writer's schema version ID is embedded in each message.
        public TemperatureChanged Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context) =>
            isNull ? null! : (TemperatureChanged)_deserializer.Deserialize(context.Topic, data.ToArray());
    }
}
