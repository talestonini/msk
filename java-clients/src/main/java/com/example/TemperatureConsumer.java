package com.example;

import com.example.events.TemperatureChanged;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

/**
 * Reads TemperatureChanged events and prints them, until stopped with Ctrl+C.
 *
 * <p>Usage: {@code java -cp temperature-clients.jar com.example.TemperatureConsumer [config-file]}
 * (default: {@code consumer.properties}).
 *
 * <p>With {@code isolation.level=read_committed} it only sees events from committed
 * transactions. Offsets are committed after each batch is processed, so a crash
 * between processing and committing re-delivers that batch: processing must be
 * idempotent for the end result to be exactly-once.
 */
public final class TemperatureConsumer {

    private static final String TOPIC = "com.example.events.TemperatureChanged";

    public static void main(String[] args) throws IOException {
        Path configFile = Path.of(args.length > 0 ? args[0] : "consumer.properties");
        Properties config = loadConfig(configFile);

        KafkaConsumer<String, TemperatureChanged> consumer = new KafkaConsumer<>(config);

        // On Ctrl+C, interrupt poll() and wait for the loop to finish cleanly.
        Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            consumer.wakeup();
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }));

        try (consumer) {
            consumer.subscribe(List.of(TOPIC));
            System.out.printf("Consuming from %s (Ctrl+C to stop)%n", TOPIC);
            while (true) {
                ConsumerRecords<String, TemperatureChanged> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, TemperatureChanged> record : records) {
                    process(record);
                }
                if (!records.isEmpty()) {
                    consumer.commitSync();
                }
            }
        } catch (WakeupException e) {
            // Expected on shutdown.
        }
        System.out.println("Consumer closed");
    }

    private static void process(ConsumerRecord<String, TemperatureChanged> record) {
        TemperatureChanged event = record.value();
        System.out.printf("Received %s=%.2f°C at %s (partition %d, offset %d)%n",
                event.getThermometerId(), event.getTemperature(), event.getTimestamp(),
                record.partition(), record.offset());
    }

    private static Properties loadConfig(Path configFile) throws IOException {
        Properties config = new Properties();
        try (InputStream in = Files.newInputStream(configFile)) {
            config.load(in);
        }
        return config;
    }
}
