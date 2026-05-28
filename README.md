# kafka-data-reader

A CLI tool to extract and relay messages from Apache Kafka topics.

## Features

- Read messages by partition and offset
- Search messages by key (from earliest, latest, or a specific timestamp)
- Decode protobuf messages to JSON using file descriptors
- Copy messages to another Kafka topic

## Installation

```bash
go install github.com/aifaniyi/kafka-data-reader@latest
```

## Usage

### `fromPartitionOffset`

Extract a message from a specific partition and offset.

```bash
kafka-data-reader fromPartitionOffset [flags]
```

| Flag | Short | Required | Default | Description |
|------|-------|----------|---------|-------------|
| `--broker` | `-b` | Yes | | Kafka broker(s), e.g. `host1:9092,host2:9092` |
| `--topic` | `-t` | Yes | | Topic to read from |
| `--partition` | `-p` | Yes | | Partition number |
| `--offset` | `-o` | Yes | | Offset number |
| `--output` | `-x` | No | `file` | Output destination: `file` or `kafka` |
| `--format` | `-f` | No | `binary` | Output format: `binary` or `json` |
| `--outbroker` | | No | same as `--broker` | Broker for kafka output |
| `--outtopic` | | No | | Topic for kafka output |
| `--filedescriptor` | | No | | Path to compiled protobuf `.desc` file |
| `--filedescriptorfullname` | | No | | Protobuf message full name, e.g. `package.Message` |

#### Examples

```bash
# Read a single message
kafka-data-reader fromPartitionOffset \
    --broker kafka-host-1:9092,kafka-host-2:9092 \
    --topic topic-name \
    --partition 0 \
    --offset 3

# Decode protobuf message as JSON
kafka-data-reader fromPartitionOffset \
    --broker kafka-a-01:9092 \
    --topic sample-topic-v1 \
    --partition 0 \
    --offset 11 \
    --output file \
    --format json \
    --filedescriptor /path/to/user.desc \
    --filedescriptorfullname protomodel.User

# Copy message to another topic
kafka-data-reader fromPartitionOffset \
    --broker kafka-a-01:9092 \
    --topic sample-topic-v1 \
    --partition 3 \
    --offset 11 \
    --output kafka \
    --outtopic sample-topic-v2
```

### `byMessageKey`

Find message(s) by key, scanning from a configurable starting point.

```bash
kafka-data-reader byMessageKey [flags]
```

| Flag | Short | Required | Default | Description |
|------|-------|----------|---------|-------------|
| `--broker` | `-b` | Yes | | Kafka broker(s), e.g. `host1:9092,host2:9092` |
| `--topic` | `-t` | Yes | | Topic to read from |
| `--key` | `-k` | Yes | | Message key to search for (string keys) |
| `--offset` | `-o` | No | `earliest` | Start position: `earliest`, `latest`, or `timestamp` |
| `--timestamp` | `-m` | No | | Local timestamp to start from, e.g. `2022-01-13T15:07:52.000` |
| `--output` | `-x` | No | `file` | Output destination: `file` or `kafka` |
| `--format` | `-f` | No | `binary` | Output format: `binary` or `json` |
| `--outbroker` | | No | same as `--broker` | Broker for kafka output |
| `--outtopic` | | No | | Topic for kafka output |
| `--filedescriptor` | | No | | Path to compiled protobuf `.desc` file |
| `--filedescriptorfullname` | | No | | Protobuf message full name, e.g. `package.Message` |

#### Examples

```bash
# Search from earliest
kafka-data-reader byMessageKey \
    --broker kafka-a-01:9092 \
    --topic sample-topic-v1 \
    --key key2 \
    --offset earliest

# Search from latest
kafka-data-reader byMessageKey \
    --broker kafka-a-01:9092 \
    --topic sample-topic-v1 \
    --key key2 \
    --offset latest

# Search from a specific timestamp
kafka-data-reader byMessageKey \
    --broker kafka-a-01:9092 \
    --topic sample-topic-v1 \
    --key key2 \
    --offset timestamp \
    --timestamp 2022-11-26T12:57:30.860
```

## Dependencies

- [confluent-kafka-go](https://github.com/confluentinc/confluent-kafka-go) – Kafka client
- [cobra](https://github.com/spf13/cobra) – CLI framework
- [protobuf](https://pkg.go.dev/google.golang.org/protobuf) – Protobuf decoding

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.
