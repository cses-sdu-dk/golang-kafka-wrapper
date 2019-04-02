package main

// ./kafka/kafka_2.12-2.1.1/

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/Shopify/sarama"
)

const charset = "abcdefghijklmnopqrstuvwxyz" + "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

var seededRand *rand.Rand = rand.New(rand.NewSource(time.Now().UnixNano()))

// SyncProducer is the struct that represents a synchronious producer
type SyncProducer struct {
	Parent sarama.SyncProducer
}

// MakeProducer publishes a message to a topic via a set amount of brokers
func MakeSyncProducer(brokers []string) (pstruct *SyncProducer) {
	configuration := sarama.NewConfig()
	configuration.Producer.Return.Errors = true
	configuration.Producer.Return.Successes = true
	configuration.Producer.RequiredAcks = sarama.WaitForAll
	configuration.Producer.Retry.Max = 5

	producer, err := sarama.NewSyncProducer(brokers, configuration)

	pstruct = &SyncProducer{
		Parent: producer,
	}

	if err != nil {
		panic(err)
	}

	return pstruct
}

// SendMessage sends a message from a designated producer
func (producer *SyncProducer) SendMessage(topic string, content string) {
	message := &sarama.ProducerMessage{
		Topic:     topic,
		Key:       sarama.StringEncoder(MakeKey()),
		Value:     sarama.StringEncoder(content),
		Timestamp: time.Now().UTC(),
	}

	partition, offset, err := producer.Parent.SendMessage(message)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Message is stored in topic(%s), partition(%d) and offset(%d).\n", topic, partition, offset)
}

// MakeKey makes a charset key for a message to be recognized
func MakeKey() string {
	b := make([]byte, 10)
	for i := range b {
		b[i] = charset[seededRand.Intn(len(charset))]
	}
	return string(b)
}
