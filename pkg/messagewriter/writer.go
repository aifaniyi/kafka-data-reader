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

type ProtoJsonOutputFormat struct {
	DescriptorFile     string
	DescriptorFullname string
}

type OutputConfig struct {
	OutputLocation OutputLocation
	KafkaConfig    *struct {
		Brokers string
		Topic   string
	}
	OutputFormat          OutputFormat
	ProtoJsonOutputFormat *ProtoJsonOutputFormat
}

type Writer interface {
	Write(event *kafka.Message) error
	Flush()
	Close()
}
