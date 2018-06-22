package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/Shopify/sarama"
)

func main() {

	//addresses of available kafka brokers
	brokers := []string{"broker1:9092"}

	start := time.Now()
	nbW := 400

	var wg sync.WaitGroup

	for i := 0; i < nbW; i++ {
		producer, _ := NewProducer(brokers)
		/*if err != nil {
			fmt.Println(err.Error())
			os.Exit(-1)
		}*/
		defer producer.Close()
		//producers = append(producers, producer)
		wg.Add(1000)
		go func(p sarama.SyncProducer, worker int) {
			for i := 0; i < 1000; i++ {

				_, _, err := SendMessage(p, fmt.Sprintf(`{ "id": %d }`, i), "b4")
				if err != nil {
					fmt.Println(err)
				}

				wg.Done()
			}
			fmt.Println("worker ", worker)
		}(producer, i+1)
	}

	wg.Wait()

	elapsed := time.Since(start)

	fmt.Printf("\n%d messages in %s", (nbW * 1000), elapsed)

	time.Sleep(20 * time.Second)

}

func NewProducer(brokers []string) (sarama.SyncProducer, error) {
	//setup relevant config info
	//config := sarama.NewConfig()
	//config.Producer...... to have more information about configuration se https://godoc.org/github.com/Shopify/sarama#Config
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Errors = true
	config.Producer.Return.Successes = true
	config.Producer.Retry.Max = 5
	config.Producer.Timeout = time.Millisecond * 10

	//producer, err := sarama.NewSyncProducer(brokers, config) //NewAsyncProducer(brokers, config)
	producer, err := sarama.NewSyncProducer(brokers, config) //NewAsyncProducer(brokers, config)

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
