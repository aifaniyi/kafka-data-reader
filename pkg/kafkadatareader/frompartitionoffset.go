package kafkadatareader

import (
	"fmt"
	"log"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func (r *ReaderImpl) ReadMessageAtPartitionOffset(topic string, partition int32, offset int64) error {
	defer r.consumer.Close()
	defer r.writer.Close()
	defer close(r.sigchan)

	err := r.consumer.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		return err
	}

	run := true

	for run {
		select {
		case sig := <-r.sigchan:
			log.Printf("Caught signal %v: terminating\n", sig)
			run = false

		default:
			ev := r.consumer.Poll(100)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case kafka.AssignedPartitions:
				topicPartitions := make([]kafka.TopicPartition, len(e.Partitions))
				for i, tp := range e.Partitions {
					if partition == tp.Partition {
						tp.Offset = kafka.Offset(offset)
						topicPartitions[i] = tp
					} else {
						tp.Offset = kafka.OffsetTail(0) // Set start offset to 0 messages from end of partition
						topicPartitions[i] = tp
					}
				}

				log.Printf("setting starting partition-offsets %v\n", topicPartitions)
				r.consumer.Assign(topicPartitions)

			case *kafka.Message:
				topicPartition := e.TopicPartition
				if topicPartition.Topic != nil &&
					topic == *topicPartition.Topic &&
					topicPartition.Partition == partition {

					if topicPartition.Offset == kafka.Offset(offset) {
						log.Printf("--- found message --- partition %d, offset %d, timestamp %v", partition, offset, e.Timestamp)

						r.writer.Write(e)
						run = false
					}

					if topicPartition.Offset > kafka.Offset(offset) {
						run = false
					}

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
