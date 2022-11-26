/*
Copyright © 2022 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"fmt"

	"github.com/aifaniyi/kafka-data-reader/pkg/kafkadatareader"
	"github.com/aifaniyi/kafka-data-reader/pkg/messagewriter"
	"github.com/spf13/cobra"
)

// fromPartitionOffsetCmd represents the fromPartitionOffset command
var fromPartitionOffsetCmd = &cobra.Command{
	Use:   "fromPartitionOffset",
	Short: "Extract message from kafka using a partition and offset",
	Long: `Extract message from kafka using a partition and offset. 
For example:
	kafka-data-reader fromPartitionOffset --broker kafka-a-01 --topic sample-topic-v1 --partition 0 --offset`,
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

		partition, err := cmd.Flags().GetInt32("partition")
		if err != nil || partition < 0 {
			fmt.Printf("\npartition must be provided\n\n")
			cmd.Help()
			return
		}

		offset, err := cmd.Flags().GetInt64("offset")
		if err != nil || offset < 0 {
			fmt.Printf("\noffset must be provided\n\n")
			cmd.Help()
			return
		}

		outputConfig, err := getOutputConfig(cmd)
		if err != nil {
			fmt.Printf("\n%v\n\n", err)
			cmd.Help()
			return
		}

		err = run(broker, topic, partition, offset, outputConfig)
		if err != nil {
			fmt.Printf("\nerror: %v\n\n", err)
		}
	},
}

func init() {
	rootCmd.AddCommand(fromPartitionOffsetCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// fromPartitionOffsetCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// fromPartitionOffsetCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
	fromPartitionOffsetCmd.Flags().StringP("broker", "b", "", "kafka broker e.g host1:9092,host2:9092")
	fromPartitionOffsetCmd.Flags().StringP("topic", "t", "", "topic name to fetch data from")
	fromPartitionOffsetCmd.Flags().Int32P("partition", "p", -1, "partition to fetch data from")
	fromPartitionOffsetCmd.Flags().Int64P("offset", "o", -1, "offset to fetch data from")
	fromPartitionOffsetCmd.Flags().StringP("output", "x", "file", "output to file or kafka. kafka only supports binary output")
	fromPartitionOffsetCmd.Flags().StringP("format", "f", "binary", "use binary or json output format")
	fromPartitionOffsetCmd.Flags().StringP("outbroker", "", "", "output broker to use if kafka output is selected, defaults to broker if none is provided")
	fromPartitionOffsetCmd.Flags().StringP("outtopic", "", "", "output topic to use if kafka output is selected")
}

func run(broker, topic string, partition int32, offset int64,
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

	err = reader.ReadMessageAtPartitionOffset(topic, partition, offset)
	if err != nil {
		return err
	}

	return nil
}
