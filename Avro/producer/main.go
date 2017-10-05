package main

import (
	"bytes"
	"fmt"
	"log"
	"os"

	"github.com/Shopify/sarama"
	"github.com/linkedin/goavro"
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

	partition, offset, err := SendMessage(producer, EncodeMessage(2, "Felipe"), "users-domain-api")
	fmt.Printf("%v %v %v", partition, offset, err)

}

func NewProducer(brokers []string) (sarama.SyncProducer, error) {
	//setup relevant config info
	//config := sarama.NewConfig()
	//config.Producer...... to have more information about configuration se https://godoc.org/github.com/Shopify/sarama#Config
	producer, err := sarama.NewSyncProducer(brokers, nil) //NewAsyncProducer(brokers, config)

	return producer, err
}

func SendMessage(producer sarama.SyncProducer, msg string /*interface{}*/, topic string) (int32, int64, error) {

	/*m, err := json.Marshal(msg)
	if err != nil {
		return nil, nil, err
	}*/

	message := &sarama.ProducerMessage{
		Topic:     topic,
		Partition: -1,
		Value:     sarama.StringEncoder(msg),
	}
	partition, offset, err := producer.SendMessage(message)

	return partition, offset, err
}

func EncodeMessage(id int64, username string) string {
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
	someRecord, err := goavro.NewRecord(goavro.RecordSchema(recordSchemaJSON))
	if err != nil {
		log.Fatal(err)
	}
	// identify field name to set datum for
	someRecord.Set("userId", id)
	someRecord.Set("username", username)

	codec, err := goavro.NewCodec(recordSchemaJSON)
	if err != nil {
		log.Fatal(err)
	}

	bb := new(bytes.Buffer)
	if err = codec.Encode(bb, someRecord); err != nil {
		log.Fatal(err)
	}

	return string(bb.Bytes())
}
