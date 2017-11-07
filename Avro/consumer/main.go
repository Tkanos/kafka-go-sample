package main

import (
	"bytes"
	"fmt"
	"log"
	"os/signal"

	"os"

	"github.com/Shopify/sarama"
	"github.com/linkedin/goavro"
)

func main() {

	//addresses of available kafka brokers
	brokers := []string{"broker1:9092", "broker2:9092"}

	consumer, err := sarama.NewConsumer(brokers, nil)

	if err != nil {
		fmt.Println(err.Error())
		os.Exit(-1)
	}

	subscribe(consumer, "users-domain-api")

	exit := make(chan os.Signal, 1)
	signal.Notify(exit, os.Interrupt)

	<-exit
	consumer.Close()
	fmt.Println("Bye")

}

func subscribe(consumer sarama.Consumer, topic string) {
	partitionList, err := consumer.Partitions(topic) //get all partitions on the given topic
	if err != nil {
		fmt.Println("Error retrieving partitionList ", err)
	}
	//initialOffset := sarama.OffsetOldest //get all the offsets
	initialOffset := sarama.OffsetNewest // get all new initialOffset

	for _, partition := range partitionList {
		pc, _ := consumer.ConsumePartition(topic, partition, initialOffset)

		go func(pc sarama.PartitionConsumer) {
			for {
				select {
				case err := <-pc.Errors():
					fmt.Println(err)
				case message := <-pc.Messages():
					messageReceived(message)
				}
			}
		}(pc)
	}
}

func messageReceived(message *sarama.ConsumerMessage) {
	fmt.Printf("%v : %s : %s : %s\n", message.Offset, message.Topic, string(message.Key), string(message.Value))
	DecodeMessage(message.Value)
}

func DecodeMessage(msg []byte) {
	recordSchemaJSON := `
{
  "type": "record",
  "name": "comments",
  "doc:": "A basic schema for storing blog comments",
  "namespace": "com.example",
  "fields": [
    {
      "doc": "Id of user",
      "type": "long",
      "name": "userId"
    },
    {
      "doc": "Name of user",
      "type": "string",
      "name": "username"
    }
  ]
}
`
	codec, err := goavro.NewCodec(recordSchemaJSON)
	if err != nil {
		log.Fatal(err)
	}

	//encoded := []byte(msg)
	bb := bytes.NewBuffer(msg)
	decoded, err := codec.Decode(bb)

	fmt.Println(decoded) // default String() representation is JSON

	// but direct access to data is provided
	record := decoded.(*goavro.Record)
	fmt.Println("Record Name:", record.Name)
	fmt.Println("Record Fields:")
	for i, field := range record.Fields {
		fmt.Println(" field", i, field.Name, ":", field.Datum)
	}
}
