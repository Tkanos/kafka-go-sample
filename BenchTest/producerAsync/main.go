package main

import (
	"fmt"
	"log"
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
		producer, _ := NewAsyncProducer(brokers)

		defer producer.Close()
		wg.Add(1000)
		go func(p sarama.AsyncProducer, worker int) {
			for i := 0; i < 1000; i++ {

				SendMessageAsync(p, fmt.Sprintf(`{ "id": %d }`, i), "b5")

				wg.Done()
			}
			fmt.Println("worker ", worker)
		}(producer, i+1)
	}

	wg.Wait()

	elapsed := time.Since(start)

	fmt.Printf("\n%d messages in %s", (nbW * 1000), elapsed)

	time.Sleep(elapsed * time.Second)

}

func NewAsyncProducer(brokers []string) (sarama.AsyncProducer, error) {
	//setup relevant config info
	//config := sarama.NewConfig()
	//config.Producer...... to have more information about configuration se https://godoc.org/github.com/Shopify/sarama#Config
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Errors = true
	config.Producer.Return.Successes = false
	config.Producer.Retry.Max = 5
	config.Producer.Timeout = time.Millisecond * 10

	//producer, err := sarama.NewSyncProducer(brokers, config) //NewAsyncProducer(brokers, config)
	producer, err := sarama.NewAsyncProducer(brokers, config) //NewAsyncProducer(brokers, config)

	go func() {
		for err := range producer.Errors() {
			log.Println("Failed to write access log entry:", err)
		}
	}()

	return producer, err
}

func SendMessageAsync(producer sarama.AsyncProducer, msg string, topic string) {
	producer.Input() <- &sarama.ProducerMessage{
		Topic:     topic,
		Partition: -1,
		Value:     sarama.StringEncoder(msg),
	}
}
