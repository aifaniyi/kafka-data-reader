package seed

import (
	"testing"

	"github.com/aifaniyi/kafka-data-reader/pkg/messagewriter"
)

func TestGenerateSampleData(t *testing.T) {

	writer, err := messagewriter.NewKafkaWriter("kafka-a-01:9092", "sample-topic-v1")
	if err != nil {
		t.Fatalf("unable to obtain kafka writer")
	}
	defer writer.Close()

	if err = GenerateSampleData(writer); err != nil {
		t.Fatalf("error writing sample data to kafka")
	}
}
