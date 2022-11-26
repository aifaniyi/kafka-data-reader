package kafkadatareader

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func (r *ReaderImpl) ReadMessageByKey(topic, key, offset string, timestamp *time.Time) error {
	defer r.consumer.Close()
	defer r.writer.Close()
	defer close(r.sigchan)

	var rebalanceCb = func(consumer *kafka.Consumer, ev kafka.Event) error {
		switch e := ev.(type) {
		case kafka.AssignedPartitions:
			switch offset {
			case "latest":
				topicPartitions := make([]kafka.TopicPartition, len(e.Partitions))

				for i, tp := range e.Partitions {
					topicPartitions[i] = kafka.TopicPartition{
						Topic:     &topic,
						Partition: tp.Partition,
						Offset:    kafka.Offset(-1),
					}
				}

				log.Printf("latest: setting starting partition-offsets %v\n", topicPartitions)
				consumer.Assign(topicPartitions)

			case "timestamp":
				topicPartitions := make([]kafka.TopicPartition, len(e.Partitions))

				timeMs := timestamp.UnixMilli()

				for i, tp := range e.Partitions {
					topicPartitions[i] = kafka.TopicPartition{
						Topic:     &topic,
						Partition: tp.Partition,
						Offset:    kafka.Offset(timeMs),
					}
				}

				// get offsets for timestamp
				resultTopicPartitions, err := consumer.OffsetsForTimes(topicPartitions, 1000)
				if err != nil {
					return err
				}

				log.Printf("timestamp: setting starting partition-offsets %v\n", resultTopicPartitions)
				consumer.Assign(resultTopicPartitions)

			default: // defaults to earliest offset
				log.Printf("earliest: setting starting partition-offsets %v\n", e.Partitions)
				consumer.Assign(e.Partitions)
			}

		case kafka.RevokedPartitions:
			fmt.Fprintf(os.Stderr,
				"%s rebalance: %d partition(s) revoked: %v\n",
				consumer.GetRebalanceProtocol(), len(e.Partitions),
				e.Partitions)
			if consumer.AssignmentLost() {
				// Our consumer has been kicked out of the group and the
				// entire assignment is thus lost.
				fmt.Fprintf(os.Stderr, "%% Current assignment lost!\n")
			}

			// The client automatically calls Unassign() unless
			// the callback has already called that method.
		}

		return nil
	}

	err := r.consumer.SubscribeTopics([]string{topic}, rebalanceCb)
	if err != nil {
		return err
	}

	run := true

	for run {
		select {
		case sig := <-r.sigchan:
			log.Printf("received shutdown signal %v. stopping ...\n", sig)
			run = false

		default:
			ev := r.consumer.Poll(100)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				topicPartition := e.TopicPartition
				if topicPartition.Topic != nil &&
					topic == *topicPartition.Topic &&
					key == string(e.Key) {

					log.Printf("--- found message --- partition %d, offset %d, timestamp %v",
						topicPartition.Partition, topicPartition.Offset, e.Timestamp)
					r.writer.Write(e)
				}

				r.consumer.CommitMessage(e)

			case kafka.PartitionEOF:
				log.Printf("%% reached %v\n", e)
				run = false

			case kafka.Error:
				fmt.Fprintf(os.Stderr, "%% error: %v\n", e)
				run = false

			default:
				log.Printf("Ignored %v\n", e)
			}
		}
	}

	return nil
}
