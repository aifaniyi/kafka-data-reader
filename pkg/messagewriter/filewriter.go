package messagewriter

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type FileWriter struct {
	baseDir string
}

// TODO: add json file generation for filewriter
func NewFileWriter(topic string) (*FileWriter, error) {

	var baseDir string
	timestamp := time.Now().Format("2006-01-02/15:04")

	if topic == "" {
		return nil, fmt.Errorf("invalid topic")
	}

	baseDir = filepath.Join(topic, timestamp)
	err := os.MkdirAll(baseDir, 0755)
	if err != nil {
		return nil, err
	}

	return &FileWriter{
		baseDir: baseDir,
	}, nil
}

func (f *FileWriter) Write(message *kafka.Message) error {
	topicPartition := message.TopicPartition

	filename := filepath.Join(f.baseDir, fmt.Sprintf("%d-%d.bin", topicPartition.Partition, topicPartition.Offset))

	err := os.WriteFile(filename, message.Value, 0644)
	if err != nil {
		return err
	}

	return nil
}

func (f *FileWriter) Close() {}
