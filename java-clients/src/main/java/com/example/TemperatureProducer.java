package com.example;

import com.example.events.TemperatureChanged;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;

/**
 * Sends a number of random TemperatureChanged events to Kafka in a single transaction,
 * so either all of them are delivered exactly once or none are (exactly-once semantics).
 *
 * <p>Usage: {@code java -cp temperature-clients.jar com.example.TemperatureProducer [config-file] [event-count]}
 * (defaults: {@code producer.properties}, 10 events).
 *
 * <p>All Kafka, SASL/SCRAM and Glue Schema Registry settings come from the config file,
 * which must set {@code transactional.id}.
 */
public final class TemperatureProducer {

    private static final String TOPIC = "com.example.events.TemperatureChanged";
    private static final String[] THERMOMETER_IDS = {"thermometer-1", "thermometer-2", "thermometer-3"};

    public static void main(String[] args) throws IOException {
        Path configFile = Path.of(args.length > 0 ? args[0] : "producer.properties");
        int eventCount = args.length > 1 ? Integer.parseInt(args[1]) : 10;

        Properties config = loadConfig(configFile);
        Random random = new Random();

        try (KafkaProducer<String, TemperatureChanged> producer = new KafkaProducer<>(config)) {
            // Registers the transactional.id with the cluster and fences off any older
            // producer instance using the same id.
            producer.initTransactions();
            try {
                producer.beginTransaction();
                for (int i = 0; i < eventCount; i++) {
                    String thermometerId = THERMOMETER_IDS[random.nextInt(THERMOMETER_IDS.length)];
                    TemperatureChanged event = TemperatureChanged.newBuilder()
                            .setThermometerId(thermometerId)
                            .setTemperature(15.0f + random.nextFloat() * 20.0f)
                            .setTimestamp(Instant.now().truncatedTo(ChronoUnit.MILLIS))
                            .build();

                    // Keyed by thermometer, so each thermometer's events stay in order on one partition.
                    producer.send(new ProducerRecord<>(TOPIC, thermometerId, event), (metadata, error) -> {
                        if (error == null) {
                            System.out.printf("Sent %s -> partition %d, offset %d%n",
                                    event, metadata.partition(), metadata.offset());
                        }
                    });
                }
                // Flushes all sends; throws if any of them failed.
                producer.commitTransaction();
            } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
                // Fatal: this producer can't continue (or even abort); closing it is the only option.
                System.err.println("Transaction failed, producer must be closed: " + e);
                System.exit(1);
            } catch (KafkaException e) {
                // Anything else: abort, so consumers using read_committed never see these events.
                producer.abortTransaction();
                System.err.println("Transaction aborted: " + e);
                System.exit(1);
            }
        }

        System.out.printf("Committed %d events to %s%n", eventCount, TOPIC);
    }

    private static Properties loadConfig(Path configFile) throws IOException {
        Properties config = new Properties();
        try (InputStream in = Files.newInputStream(configFile)) {
            config.load(in);
        }
        return config;
    }
}
