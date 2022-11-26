package messagewriter

import "github.com/confluentinc/confluent-kafka-go/kafka"

type OutputFormat int

const (
	Binary OutputFormat = iota
	Json
)

type OutputLocation int

const (
	File OutputLocation = iota
	KafkaTopic
)

type OutputConfig struct {
	OutputLocation OutputLocation
	KafkaConfig    *struct {
		Brokers string
		Topic   string
	}
	OutputFormat OutputFormat
}

type Writer interface {
	Write(event *kafka.Message) error
	Close()
}
