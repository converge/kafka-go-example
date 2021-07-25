package main

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	// instantiate NewConsumer object
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost",
		"group.id":          "myGroup",
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		panic(err)
	}

	// subscribe to topic
	c.SubscribeTopics([]string{"comments"}, nil)

	//
	for {
		msg, err := c.ReadMessage(-1)
		if err != nil {
			// The client will automatically try to recover from all errors.
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
			continue
		}
		fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
	}

	c.Close()
}
