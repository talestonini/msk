# temperature-clients

A Java producer and consumer for `TemperatureChanged` Avro events on the
`com.example.events.TemperatureChanged` topic in MSK. Events are serialized with the
AWS Glue Schema Registry, and both clients are set up for exactly-once semantics (EOS).

- `com.example.TemperatureProducer`: sends N random events in a single Kafka transaction.
- `com.example.TemperatureConsumer`: prints events from committed transactions until Ctrl+C.
- `com.example.events.TemperatureChanged`: generated at build time from
  [`../avro/temperature-changed.avsc`](../avro/temperature-changed.avsc) by `avro-maven-plugin`.

All settings come from `producer.properties` / `consumer.properties`.

## Delivery guarantees

| Stage | Guarantee | How |
| --- | --- | --- |
| Producer → topic | Exactly once, all-or-nothing per run | `enable.idempotence=true`, `transactional.id`, `commitTransaction()` (or `abortTransaction()` on failure) |
| Topic → consumer | Only committed events, in order per partition | `isolation.level=read_committed` |
| Consumer processing | At least once | Offsets are committed after each batch, so a crash before the commit re-delivers that batch |

The consumer only prints, so a duplicate after a crash is harmless. If you replace `process()`
with something that has side effects, make it idempotent, or store the offsets atomically with
its results, to keep the end result exactly-once. If the consumer writes back to Kafka instead,
use a transactional consume-transform-produce loop (`sendOffsetsToTransaction`).

## 1. Create the schema and topic (once, from the bastion, at the repo root)

The clients don't create either: the brokers have `auto.create.topics.enable=false`, and the
producer has `schemaAutoRegistrationEnabled=false`.

```bash
msk-setup   # admin credentials -> ~/msk/

# Schema: named after the topic, which is how the serializer finds it.
aws glue create-schema --registry-id RegistryName=basic-msk-cluster-registry \
  --schema-name com.example.events.TemperatureChanged --data-format AVRO --compatibility BACKWARD \
  --schema-definition file://avro/temperature-changed.avsc

# Topic
kafka-topics.sh --bootstrap-server $(cat ~/msk/bootstrap.txt) --command-config ~/msk/admin.properties \
  --create --topic com.example.events.TemperatureChanged --partitions 3 --replication-factor 2
```

If you change the `.avsc`, register it as a new schema version **before** running the
updated producer. Otherwise the producer fails because it can't find a matching version:

```bash
aws glue register-schema-version --schema-id RegistryName=basic-msk-cluster-registry,SchemaName=com.example.events.TemperatureChanged \
  --schema-definition file://avro/temperature-changed.avsc
```

## 2. Configure

Edit `producer.properties` and `consumer.properties`:

- `bootstrap.servers`: the contents of `~/msk/bootstrap.txt`.
- `sasl.jaas.config`: copy the line from `~/msk/<username>.properties`, after running `msk-setup <username>`.

The files then contain a password, so don't commit them.

`transactional.id` (producer) must be stable across restarts and unique per running instance.
If you run several producers at once, give each its own id. `group.id` (consumer) is shared by
all instances that split the topic's partitions between them.

If you run as a non-admin user, it needs Kafka ACLs once ACLs are enforced:

```bash
B="--bootstrap-server $(cat ~/msk/bootstrap.txt) --command-config ~/msk/admin.properties"
# Producer: write to the topic using its transactional id
kafka-acls.sh $B --add --allow-principal User:<username> --producer \
  --topic com.example.events.TemperatureChanged --transactional-id temperature-producer-1
# Consumer: read the topic as part of its group
kafka-acls.sh $B --add --allow-principal User:<username> --consumer \
  --topic com.example.events.TemperatureChanged --group temperature-consumers
```

## 3. Build and run (Java 21, Maven)

Run from this directory. The build reads `../avro`, so copy the `avro` directory to the bastion too.

```bash
mvn package

# Terminal 1: consumer
java -cp target/temperature-clients.jar com.example.TemperatureConsumer consumer.properties

# Terminal 2: producer (config file, number of events)
java -cp target/temperature-clients.jar com.example.TemperatureProducer producer.properties 10
```
