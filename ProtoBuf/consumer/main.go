package main

import (
	"fmt"
	"os/signal"

	"os"

	"github.com/Shopify/sarama"
	"github.com/golang/protobuf/proto"
	p "github.com/tkanos/kafka-go-sample/ProtoBuf/proto"
)

func main() {

	//addresses of available kafka brokers
	brokers := []string{"broker1:9092", "broker2:9092"}

	consumer, err := sarama.NewConsumer(brokers, nil)

	if err != nil {
		fmt.Println(err.Error())
		os.Exit(-1)
	}

	subscribe(consumer, "ping-pong-new-proto")

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
	user := p.Person{}

	if err := proto.Unmarshal(msg, &user); err != nil {
		fmt.Println(err)
	}

	fmt.Println(user)
}
