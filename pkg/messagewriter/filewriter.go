package messagewriter

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type FileWriter struct {
	baseDir            string
	outputFormat       OutputFormat
	descriptorFile     string
	descriptorFullname string
}

func NewFileWriter(topic string, outputFormat OutputFormat,
	descriptorFile string, descriptorFullname string) (*FileWriter, error) {

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
		baseDir:            baseDir,
		outputFormat:       outputFormat,
		descriptorFile:     descriptorFile,
		descriptorFullname: descriptorFullname,
	}, nil
}

func (f *FileWriter) Write(message *kafka.Message) error {
	topicPartition := message.TopicPartition

	extension := "bin"
	if f.outputFormat == Json {
		extension = "json"
	}

	filename := filepath.Join(f.baseDir, fmt.Sprintf("%d-%d.%s", topicPartition.Partition, topicPartition.Offset, extension))

	err := os.WriteFile(filename, f.getValue(message.Value), 0644)
	if err != nil {
		return err
	}

	return nil
}

func (f *FileWriter) Close() {}
func (f *FileWriter) Flush() {}

func (f *FileWriter) getValue(data []byte) []byte {
	if f.outputFormat == Binary {
		return data
	}

	value, err := protoBin2Json(data, f.descriptorFile, f.descriptorFullname)
	if err != nil {
		log.Printf("unable to convert protobuf binary to json: %v; writing binary format instead", err)
		return data
	}

	return value
}
