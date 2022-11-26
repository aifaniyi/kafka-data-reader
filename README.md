# kafka-data-reader
Extract message from kafka

## installation
```bash
go install github.com/aifaniyi/kafka-data-reader
```

## usage

Read message(s) from topic
```bash
# by partition and offset
kafka-data-reader fromPartitionOffset \
    --broker kafka-host-1:9092,kafka-host-2:9092 \
    --topic topic-name \
    --partition 0 \
    --offset 3

# by key (from earliest)
kafka-data-reader byMessageKey \
    --broker kafka-a-01:9092 \
     --topic sample-topic-v1 \
     --key key2 \
     --offset earliest

# by key (from latest)
kafka-data-reader byMessageKey \
    --broker kafka-a-01:9092 \
     --topic sample-topic-v1 \
     --key key2 \
     --offset latest

# by key (from timestamp)
kafka-data-reader byMessageKey \
    --broker kafka-a-01:9092 \
     --topic sample-topic-v1 \
     --key key2 \
     --offset timestamp \
     --timestamp 2022-11-26T12:57:30.860
```