package seed

import (
	"fmt"
	"time"

	"github.com/aifaniyi/kafka-data-reader/data/protomodel"
	"github.com/aifaniyi/kafka-data-reader/pkg/messagewriter"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"google.golang.org/protobuf/proto"
)

func GenerateSampleData(protobufMessageWriter messagewriter.Writer) error {

	for i := 0; i < 100; i++ {
		user := &protomodel.User{
			Id:         fmt.Sprintf("identifier-%d", i),
			Firstname:  fmt.Sprintf("firstname-%d", i),
			Middlename: []string{fmt.Sprintf("middlename-%d", i)},
			Lastname:   fmt.Sprintf("lastname-%d", i),
			CurrentAddress: &protomodel.Address{
				StreetNumber: fmt.Sprintf("current-streetnumber-%d", i),
				StreetName:   fmt.Sprintf("current-streetname-%d", i),
				City:         fmt.Sprintf("current-city-%d", i),
				Country:      fmt.Sprintf("current-country-%d", i),
			},
			Address: []*protomodel.Address{
				{
					StreetNumber: fmt.Sprintf("streetnumber-%d", i),
					StreetName:   fmt.Sprintf("streetname-%d", i),
					City:         fmt.Sprintf("city-%d", i),
					Country:      fmt.Sprintf("country-%d", i),
				},
			},
		}

		data, err := proto.Marshal(user)
		if err != nil {
			return err
		}

		err = protobufMessageWriter.Write(&kafka.Message{
			Value:     data,
			Key:       []byte(fmt.Sprintf("key-%d-", i)),
			Timestamp: time.Now(),
		})
		if err != nil {
			return err
		}
	}

	return nil
}
