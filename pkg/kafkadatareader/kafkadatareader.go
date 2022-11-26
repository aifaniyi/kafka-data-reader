package kafkadatareader

import "time"

type Reader interface {
	ReadMessageAtPartitionOffset(topic string, partition int, offset int) error
	ReadMessageByKey(topic string, key, offset string, timestamp *time.Time) error
}
