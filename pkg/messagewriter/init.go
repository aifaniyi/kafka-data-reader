package messagewriter

func NewWriter(inTopic string, outputConfig *OutputConfig) (Writer, error) {

	switch outputConfig.OutputLocation {
	case KafkaTopic:
		return NewKafkaWriter(outputConfig.KafkaConfig.Brokers, outputConfig.KafkaConfig.Topic)

	default:
		return NewFileWriter(inTopic)
	}
}
