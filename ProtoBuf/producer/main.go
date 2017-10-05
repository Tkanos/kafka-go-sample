package main

import (
	"fmt"
	"os"

	"github.com/Shopify/sarama"
	"github.com/golang/protobuf/proto"
	p "github.com/tkanos/kafka-go-sample/ProtoBuf/proto"
)

func main() {

	//addresses of available kafka brokers
	brokers := []string{"rm-be-k8k73.beta.local:9092", "rm-be-k8k74.beta.local:9092", "rm-be-k8k75.beta.local:9092"}

	producer, err := NewProducer(brokers)

	if err != nil {
		fmt.Println(err.Error())
		os.Exit(-1)
	}

	defer producer.Close()

	partition, offset, err := SendMessage(producer, EncodeMessage(50, "Hello Ligia-Maria"), "ping-pong-proto")
	fmt.Printf("%v %v %v", partition, offset, err)

}

func NewProducer(brokers []string) (sarama.SyncProducer, error) {
	//setup relevant config info
	//config := sarama.NewConfig()
	//config.Producer...... to have more information about configuration se https://godoc.org/github.com/Shopify/sarama#Config
	producer, err := sarama.NewSyncProducer(brokers, nil) //NewAsyncProducer(brokers, config)

	return producer, err
}

func SendMessage(producer sarama.SyncProducer, msg []byte /*interface{}*/, topic string) (int32, int64, error) {

	/*m, err := json.Marshal(msg)
	if err != nil {
		return nil, nil, err
	}*/

	message := &sarama.ProducerMessage{
		Topic:     topic,
		Partition: -1,
		Value:     sarama.ByteEncoder(msg),
	}
	partition, offset, err := producer.SendMessage(message)

	return partition, offset, err
}

func EncodeMessage(id int64, username string) []byte {
	user := &p.Person{
		UserName:        username,
		FavouriteNumber: id,
		Company:         "Ricardo",
	}

	data, _ := proto.Marshal(user)

	return data

}
