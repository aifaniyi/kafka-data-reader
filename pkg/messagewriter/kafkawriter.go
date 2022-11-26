package messagewriter

import (
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type KafkaWriter struct {
	producer    *kafka.Producer
	outputTopic string
}

func NewKafkaWriter(brokers, outputTopic string) (*KafkaWriter, error) {

	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":  brokers,
		"enable.idempotence": true,
		"batch.size":         16384,
		"linger.ms":          5,
	})
	if err != nil {
		return nil, err
	}

	go func(c *kafka.Producer) {
		for ev := range producer.Events() {
			switch msg := ev.(type) {
			case *kafka.Message:
				if err = msg.TopicPartition.Error; err != nil {
					log.Printf("error publishing message %v to kafka: %v\n", msg.TopicPartition, err)
				}
			}
		}
	}(producer)

	return &KafkaWriter{
		producer:    producer,
		outputTopic: outputTopic,
	}, nil
}

func (k *KafkaWriter) Write(message *kafka.Message) error {
	if err := k.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &k.outputTopic, Partition: kafka.PartitionAny},
		Key:            message.Key,
		Value:          message.Value,
		Timestamp:      time.Now(),
		Headers:        message.Headers,
	}, nil); err != nil {
		return err
	}

	return nil
}

func (k *KafkaWriter) Close() {
	k.producer.Close()
}
