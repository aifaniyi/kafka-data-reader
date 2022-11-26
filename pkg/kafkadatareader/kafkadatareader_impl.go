package kafkadatareader

import (
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/aifaniyi/kafka-data-reader/pkg/messagewriter"
	uuid "github.com/satori/go.uuid"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type ReaderImpl struct {
	consumer *kafka.Consumer
	sigchan  chan os.Signal
	writer   messagewriter.Writer
}

const (
	timeout = 1000 * time.Millisecond
)

func NewReaderImpl(writer messagewriter.Writer, broker string) (*ReaderImpl, error) {
	// check host(s) is listening
	if err := verifyHostIsValid(broker); err != nil {
		return nil, err
	}

	// create kafka client
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":               broker,
		"group.id":                        fmt.Sprintf("tmp_%s", uuid.NewV4().String()),
		"go.application.rebalance.enable": true,
		"session.timeout.ms":              6000,
		"default.topic.config":            kafka.ConfigMap{"auto.offset.reset": "earliest"}})
	if err != nil {
		return nil, err
	}

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	return &ReaderImpl{
		consumer: consumer,
		sigchan:  sigchan,
		writer:   writer,
	}, nil
}

// verifies at least one of the supplied hosts is alive
// hosts: comma separated list of host:port values example
// host1:9092,host2:9092
func verifyHostIsValid(hosts string) error {
	d := net.Dialer{Timeout: timeout}

	hostAddresses := strings.Split(hosts, ",")
	errors := []error{}

	for _, addr := range hostAddresses {
		conn, err := d.Dial("tcp", addr)
		if err != nil {
			errors = append(errors, err)
		} else {
			conn.Close()
		}
	}

	if len(hostAddresses) == len(errors) {
		return fmt.Errorf("all provided hosts %v are not reacheable", hostAddresses)
	}

	return nil
}
