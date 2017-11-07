package main

import (
	"fmt"
	"os/signal"
	"strings"

	"os"

	"time"

	"github.com/Shopify/sarama"
	"github.com/wvanbergen/kafka/consumergroup"
	"github.com/wvanbergen/kazoo-go"
)

func main() {
	zookeper := "zook1:2181,rzook2:2181"
	topics := "users-domain-api"
	groupsName := "users-group"
	consumer, err := NewConsumer(zookeper, groupsName, topics)

	if err != nil {
		fmt.Println(err.Error())
		os.Exit(-1)
	}

	subscribe(consumer)
	//subscribe(consumer, "users-domain-api")

	exit := make(chan os.Signal, 1)
	signal.Notify(exit, os.Interrupt)

	<-exit
	consumer.Close()
	fmt.Println("Bye")

}

func NewConsumer(zookeeper, consumerGroupName, topics string) (*consumergroup.ConsumerGroup, error) {
	config := consumergroup.NewConfig()
	config.Offsets.Initial = sarama.OffsetNewest
	config.Offsets.ProcessingTimeout = 10 * time.Second

	var zookeeperNodes []string
	zookeeperNodes, config.Zookeeper.Chroot = kazoo.ParseConnectionString(zookeeper)

	kafkaTopics := strings.Split(topics, ",")

	consumer, err := consumergroup.JoinConsumerGroup(consumerGroupName, kafkaTopics, zookeeperNodes, config)
	if err != nil {
		return nil, err
	}

	return consumer, nil
}

func subscribe(consumer *consumergroup.ConsumerGroup) {
	go func(consumer *consumergroup.ConsumerGroup) {
		for {
			select {
			case err := <-consumer.Errors():
				fmt.Println(err)
			case message := <-consumer.Messages():
				//time.Sleep(5 * time.Minute)
				messageReceived(message)
				consumer.CommitUpto(message)
			}
		}
	}(consumer)
}

func messageReceived(message *sarama.ConsumerMessage) {
	fmt.Printf("%v : %s : %s : %s\n", message.Offset, message.Topic, string(message.Key), string(message.Value))
}
