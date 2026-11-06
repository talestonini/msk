# cs-clients

A C# (.NET 10) producer and consumer for `TemperatureChanged` Avro events on the
`com.example.events.TemperatureChanged` topic in MSK. They're equivalent to the Java apps in
[`../java-clients`](../java-clients): Glue Schema Registry serialization and the same
exactly-once setup.

```text
cs-clients/
├── Com.Example.slnx                # solution
├── .config/dotnet-tools.json       # avrogen (Apache.Avro.Tools) version
└── src/Com.Example/                # the project; namespace Com.Example
    ├── Com.Example.csproj
    ├── Program.cs                  # entry point: picks the producer or consumer
    ├── ConfigFile.cs               # loads the .properties files
    ├── TemperatureProducer.cs      # sends N random events in a single Kafka transaction
    ├── TemperatureConsumer.cs      # prints committed events until Ctrl+C
    ├── producer.properties
    └── consumer.properties
```

`com.example.events.TemperatureChanged` is generated at build time from
[`../avro/temperature-changed.avsc`](../avro/temperature-changed.avsc) by `avrogen`, into
`src/Com.Example/obj/generated-avro/`. Unlike the hand-written code, it keeps the Avro
namespace: the Avro library finds the class by the schema's full name when deserializing.

| Purpose | Java | C# |
| --- | --- | --- |
| Kafka client | `kafka-clients` | `Confluent.Kafka` (librdkafka) |
| Glue Schema Registry serde | `software.amazon.glue:schema-registry-serde` | `AWS.Glue.SchemaRegistry` (`1.0.1-linux-x64`) |
| Avro code generation | `avro-maven-plugin` | `avrogen`, run by an MSBuild target |

`AWS.Glue.SchemaRegistry` ships native libraries for one platform per package version. This
project uses `linux-x64` (glibc Linux on x86_64, which is what the bastion is), so it builds and
runs on Linux only.

## Delivery guarantees

These are the same as the Java apps (see [its README](../java-clients/README.md#delivery-guarantees)):
the producer writes exactly once, all-or-nothing per run, and the consumer reads only committed
events and processes them at least once, committing each offset after processing.

The C# apps use their own `transactional.id` (`temperature-producer-cs-1`) and consumer group
(`temperature-consumers-cs`). That way they don't fence or share partitions with the Java apps,
and both consumers receive every event.

## 1. Prerequisites

The schema and the topic must already exist; see
[java-clients/README.md, section 1](../java-clients/README.md#1-create-the-schema-and-topic-once-from-the-bastion-at-the-repo-root).

If you run as a non-admin user, it needs ACLs for the C# transactional id and group:

```bash
B="--bootstrap-server $(cat ~/msk/bootstrap.txt) --command-config ~/msk/admin.properties"
kafka-acls.sh $B --add --allow-principal User:<username> --producer \
  --topic com.example.events.TemperatureChanged --transactional-id temperature-producer-cs-1
kafka-acls.sh $B --add --allow-principal User:<username> --consumer \
  --topic com.example.events.TemperatureChanged --group temperature-consumers-cs
```

## 2. Configure

Edit `producer.properties` and `consumer.properties` in `src/Com.Example/`. Kafka settings use
[librdkafka names](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md),
which differ from Java's. In particular, SCRAM credentials go in `sasl.username` and
`sasl.password` instead of a JAAS line:

- `bootstrap.servers`: the contents of `~/msk/bootstrap.txt`.
- `sasl.username` / `sasl.password`: `MSK_SCRAM_USERNAME` / `MSK_SCRAM_PASSWORD` from
  `~/msk/<username>.env`, after running `msk-setup <username>`.

The files then contain a password, so don't commit them.

Each file also holds Glue Schema Registry settings (`region`, `registry.name`, ...). The Glue
library reads those from the file itself. `ConfigFile.cs` leaves them out of the settings passed
to librdkafka, which rejects properties it doesn't know. If you add another Glue setting, add its
name to `ConfigFile.GlueKeys`.

## 3. Build and run (.NET 10 SDK)

The build reads the repo's `avro` directory, so copy it to the bastion too.

```bash
# From cs-clients/: build the solution
dotnet build

# Run from the project directory, where the .properties files are
cd src/Com.Example

# Terminal 1: consumer
dotnet run --no-build -- consume consumer.properties

# Terminal 2: producer (config file, number of events)
dotnet run --no-build -- produce producer.properties 10
```

The Glue library logs a few `INFO ... GlueSchemaRegistryConfiguration` lines at startup about
cache settings it didn't find. They're harmless.
