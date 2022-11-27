/*
Copyright © 2022 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"fmt"
	"time"

	"github.com/aifaniyi/kafka-data-reader/pkg/kafkadatareader"
	"github.com/aifaniyi/kafka-data-reader/pkg/messagewriter"
	"github.com/spf13/cobra"
)

const (
	timestampFormat = "2006-01-02T15:04:05.000"
)

// byMessageKeyCmd represents the byMessageKey command
var byMessageKeyCmd = &cobra.Command{
	Use:   "byMessageKey",
	Short: "Find message(s) by message key",
	Long:  `Find message(s) by message key.`,
	Run: func(cmd *cobra.Command, args []string) {

		broker, err := cmd.Flags().GetString("broker")
		if err != nil || broker == "" {
			fmt.Printf("\nbroker must be provided\n\n")
			cmd.Help()
			return
		}

		topic, err := cmd.Flags().GetString("topic")
		if err != nil || topic == "" {
			fmt.Printf("\ntopic must be provided\n\n")
			cmd.Help()
			return
		}

		key, err := cmd.Flags().GetString("key")
		if err != nil || key == "" {
			fmt.Printf("\nkey must be provided\n\n")
			cmd.Help()
			return
		}

		offset, err := cmd.Flags().GetString("offset")
		if err != nil {
			fmt.Printf("\noffset must be provided\n\n")
			cmd.Help()
			return
		}

		var timestamp *time.Time
		timestampString, err := cmd.Flags().GetString("timestamp")
		if err != nil {
			fmt.Printf("\ntimestamp must be provided\n\n")
			cmd.Help()
			return
		}
		if t, err := validateTimestamp(offset, timestampString); err != nil {
			fmt.Printf("\nprovided timestamp %s does not match required format %s\n\n",
				timestampString, timestampFormat)
			cmd.Help()
			return
		} else {
			timestamp = t
		}

		outputConfig, err := getOutputConfig(cmd)
		if err != nil {
			fmt.Printf("\n%v\n\n", err)
			cmd.Help()
			return
		}

		err = runByMessageKey(broker, topic, key, offset, timestamp, outputConfig)
		if err != nil {
			fmt.Printf("\nerror: %v\n\n", err)
		}
	},
}

func init() {
	rootCmd.AddCommand(byMessageKeyCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// byMessageKeyCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// byMessageKeyCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
	byMessageKeyCmd.Flags().StringP("broker", "b", "", "kafka broker e.g host1:9092,host2:9092")
	byMessageKeyCmd.Flags().StringP("topic", "t", "", "topic name to fetch data from")
	byMessageKeyCmd.Flags().StringP("key", "k", "", "message key (supports string keys)")
	byMessageKeyCmd.Flags().StringP("offset", "o", "earliest", `offset to start consuming from. 
Possible values include 
- earliest (default)
- latest
- timestamp (requires --timestamp option)`)
	byMessageKeyCmd.Flags().StringP("timestamp", "m", "", "local timestamp to start consuming from. e.g --timestamp 2022-01-13T15:07:52.000")
	byMessageKeyCmd.Flags().StringP("output", "x", "file", "output to file or kafka. kafka only supports binary output")
	byMessageKeyCmd.Flags().StringP("format", "f", "binary", "use binary or json output format")
	byMessageKeyCmd.Flags().StringP("outbroker", "", "", "output broker to use if kafka output is selected, defaults to broker if none is provided")
	byMessageKeyCmd.Flags().StringP("outtopic", "", "", "output topic to use if kafka output is selected")
	byMessageKeyCmd.Flags().StringP("filedescriptor", "", "", "protobuf file descriptor to be used i.e a compiled protobuf .desc file")
	byMessageKeyCmd.Flags().StringP("filedescriptorfullname", "", "", "protobuf message fullname e.g package.Message")

	// non optional
	// byMessageKeyCmd.MarkFlagRequired("broker")
	// byMessageKeyCmd.MarkFlagRequired("topic")
	// byMessageKeyCmd.MarkFlagRequired("key")
}

func runByMessageKey(broker, topic string,
	key, offset string, timestamp *time.Time,
	outputConfig *messagewriter.OutputConfig) error {

	var writer messagewriter.Writer
	writer, err := messagewriter.NewWriter(topic, outputConfig)
	if err != nil {
		return err
	}

	reader, err := kafkadatareader.NewReaderImpl(writer, broker)
	if err != nil {
		return err
	}

	err = reader.ReadMessageByKey(topic, key, offset, timestamp)
	if err != nil {
		return err
	}

	return nil
}

func validateTimestamp(offset, timestampString string) (*time.Time, error) {
	if offset == "timestamp" {
		location, err := time.LoadLocation("Local")
		if err != nil {
			return nil, err
		}
		t, err := time.ParseInLocation(timestampFormat, timestampString, location)
		if err != nil {
			return nil, fmt.Errorf("provided timestamp %s does not match required format %s", timestampString, timestampFormat)
		}
		return &t, nil
	}
	return nil, nil
}

func getOutputConfig(cmd *cobra.Command) (*messagewriter.OutputConfig, error) {
	broker, err := cmd.Flags().GetString("broker")
	if err != nil || broker == "" {
		return nil, fmt.Errorf("broker must be provided: %v", err)
	}

	outbroker, err := cmd.Flags().GetString("outbroker")
	if err != nil {
		return nil, fmt.Errorf("outbroker must be provided: %v", err)
	}

	output, err := cmd.Flags().GetString("output")
	if err != nil {
		return nil, fmt.Errorf("output must be provided: %v", err)
	}

	format, err := cmd.Flags().GetString("format")
	if err != nil {
		return nil, fmt.Errorf("format must be provided: %v", err)
	}

	fileDescriptor, err := cmd.Flags().GetString("filedescriptor")
	if err != nil {
		return nil, fmt.Errorf("filedescriptor must be provided: %v", err)
	}

	fileDescriptorFullname, err := cmd.Flags().GetString("filedescriptorfullname")
	if err != nil {
		return nil, fmt.Errorf("filedescriptorfullname must be provided: %v", err)
	}

	var protoJsonOutputFormat *messagewriter.ProtoJsonOutputFormat
	outputFormat := messagewriter.Binary // binary output by default
	if format == "json" && output != "kafka" {
		outputFormat = messagewriter.Json

		// ensure filedescriptor and descriptor name params are provided
		if fileDescriptor == "" || fileDescriptorFullname == "" {
			return nil, fmt.Errorf("filedescriptor and filedescriptorfullname must be provided: %v", err)
		}

		protoJsonOutputFormat = &messagewriter.ProtoJsonOutputFormat{
			DescriptorFile:     fileDescriptor,
			DescriptorFullname: fileDescriptorFullname,
		}
	}

	switch output {
	case "kafka":
		outtopic, err := cmd.Flags().GetString("outtopic")
		if err != nil || outtopic == "" {
			return nil, fmt.Errorf("outtopic must be provided: %v", err)
		}

		if outbroker == "" {
			outbroker = broker
		}

		return &messagewriter.OutputConfig{
			OutputLocation: messagewriter.KafkaTopic,
			KafkaConfig: &struct {
				Brokers string
				Topic   string
			}{
				Brokers: outbroker,
				Topic:   outtopic,
			},
			OutputFormat: outputFormat,
		}, nil

	default: // write to file
		return &messagewriter.OutputConfig{
			OutputLocation:        messagewriter.File,
			KafkaConfig:           nil,
			OutputFormat:          outputFormat,
			ProtoJsonOutputFormat: protoJsonOutputFormat,
		}, nil
	}
}
