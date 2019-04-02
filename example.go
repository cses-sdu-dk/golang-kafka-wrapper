package main

func main() {
	brokers := []string{"put your server address here"}

	producer := MakeSyncProducer(brokers)
	consumer := MakeConsumer(brokers)

	producer.SendMessage("test.topic", "Something important!")

	consumer.Consume("test.topic")
}
