package main

import (
	"fmt"
	"os"

	"github.com/Shopify/sarama"
	"github.com/golang/protobuf/proto"
	p "github.com/tkanos/kafka-go-sample/ProtoBuf.test/proto"
)

func main() {

	//addresses of available kafka brokers
	brokers := []string{"broker1:9092", "broker2:9092"}

	producer, err := NewProducer(brokers)

	if err != nil {
		fmt.Println(err.Error())
		os.Exit(-1)
	}

	defer producer.Close()

	partition, offset, err := SendMessage(producer, EncodeMessage("406669554", "Robin", "email_not_confirmed"), "user-login-failed")
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

func EncodeMessage(id string, username, reason string) []byte {
	user := &p.LoginFailed{
		UserId:   string(id),
		UserName: username,
		Reason:   reason,
	}

	data, _ := proto.Marshal(user)

	return data

}
