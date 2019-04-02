package main

import (
	"fmt"
	"os"
	"os/signal"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
)

// Consumer is the struct
type Consumer struct {
	Parent sarama.Consumer
}

// GroupConsumer is a consumer that belongs to a group
type GroupConsumer struct {
	Parent cluster.Consumer
}

// MakeGroupConsumer subscribes to a topic but belongs to a certain group
func MakeGroupConsumer(brokers []string, topics []string, group string) (cgstruct *GroupConsumer) {
	configuration := cluster.NewConfig()
	configuration.Consumer.Return.Errors = true
	configuration.Consumer.Offsets.Initial = sarama.OffsetNewest

	gConsumer, err := cluster.NewConsumer(brokers, group, topics, configuration)

	cgstruct = &GroupConsumer{
		Parent: *gConsumer,
	}

	if err != nil {
		fmt.Println("ERROR: Failed to start group-consumer.")
	}

	return cgstruct
}

func (consumer *GroupConsumer) GroupConsume() {
	// trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// consume partitions
	for {
		select {
		case part, ok := <-consumer.Parent.Partitions():
			if !ok {
				return
			}

			go func(pc cluster.PartitionConsumer) {
				for msg := range pc.Messages() {
					fmt.Printf("Received messages from topic(%s), key(%s), message(%s), time(%s).\n", string(msg.Topic), string(msg.Key), string(msg.Value), msg.Timestamp.String())
					consumer.Parent.MarkOffset(msg, "") // mark message as processed
				}
			}(part)
		case <-signals:
			return
		}
	}
}

// MakeConsumer creates a consumer
func MakeConsumer(brokers []string) (cstruct *Consumer) {
	configuration := sarama.NewConfig() // Create a config file to determine client attributes such as SASL
	configuration.Consumer.Return.Errors = true

	consumer, err := sarama.NewConsumer(brokers, configuration)

	cstruct = &Consumer{
		Parent: consumer,
	}

	if err != nil {
		fmt.Println("ERROR: Failed to start consumer.")
	}

	return cstruct
}

// ListenTo starts listening to the topic via the declared brokers
// The idea is to consume everything that was ever send to the topic (this last retention policy clean)
func (consumer *Consumer) Consume(topic string) {
	comsumerPartition, err := consumer.Parent.ConsumePartition(topic, 0, sarama.OffsetOldest) // This is the equivalent of "earliest" and "latest", nevertheless, we choose "earliest"

	if err != nil {
		panic(err)
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt) // Causes package signal to relay incoming signals to signals

	finish := make(chan struct{})
	go func() { // Starting goroutine
		for {
			select { // blocks until one of the cases can run, then executes that case.
			case err := <-comsumerPartition.Errors():
				fmt.Println(err)
			case msg := <-comsumerPartition.Messages():
				fmt.Printf("Received messages from topic(%s), key(%s), message(%s), time(%s).\n", string(msg.Topic), string(msg.Key), string(msg.Value), msg.Timestamp.String())
			case <-signals:
				fmt.Println("Process interrupted.")
				finish <- struct{}{}
			}
		}
	}()

	<-finish // Adding constraint <- READ ONLY

	defer func() {
		if err := consumer.Parent.Close(); err != nil {
			panic(err)
		}
	}()
}
