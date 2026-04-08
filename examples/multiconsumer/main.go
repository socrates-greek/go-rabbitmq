package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	rabbitmq "github.com/Bifang-Bird/go-rabbitmq"
)

//"amqp://Simba_admin:Simba_123@amqp-5kggpo9g-nj-public-d.amqp.tencenttdmq.com:5672"
//"amqp://Simba_admin:Simba_123@192.168.20.201:30672",

func main() {
	conn, err := rabbitmq.NewConn(
		//"amqp://guest:guest@localhost:5672",
		"amqp://Simba_admin:Simba_123@amqp-5kggpo9g-nj-public-d.amqp.tencenttdmq.com:5672",
		rabbitmq.WithConnectionOptionsLogging,
	)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	consumer, err := rabbitmq.NewConsumer(
		conn,
		func(d rabbitmq.Delivery) rabbitmq.Action {
			//log.Printf("consumed: %v", string(d.Body))
			// rabbitmq.Ack, rabbitmq.NackDiscard, rabbitmq.NackRequeue
			return rabbitmq.Ack
		},
		"my_queue",
		rabbitmq.WithConsumerOptionsConsumerName("consumer_1"),
		rabbitmq.WithConsumerOptionsRoutingKey("test_key"),
		rabbitmq.WithConsumerOptionsRoutingKey("my_routing_key_2"),
		rabbitmq.WithConsumerOptionsExchangeName("test-exchange"),
		rabbitmq.WithConsumerOptionsConcurrency(10),
		rabbitmq.WithConsumerOptionsQOSPrefetch(100),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	consumer2, err := rabbitmq.NewConsumer(
		conn,
		func(d rabbitmq.Delivery) rabbitmq.Action {
			//log.Printf("consumed 2: %v", string(d.Body))
			// rabbitmq.Ack, rabbitmq.NackDiscard, rabbitmq.NackRequeue
			return rabbitmq.Ack
		},
		"my_queue",
		rabbitmq.WithConsumerOptionsQOSPrefetch(100),
		rabbitmq.WithConsumerOptionsConsumerName("consumer_2"),
		rabbitmq.WithConsumerOptionsRoutingKey("test_key"),
		rabbitmq.WithConsumerOptionsExchangeName("test-exchange"),
		rabbitmq.WithConsumerOptionsConcurrency(10),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer consumer2.Close()

	//consumer3, err := rabbitmq.NewConsumer(
	//	conn,
	//	func(d rabbitmq.Delivery) rabbitmq.Action {
	//		log.Printf("consumed 3: %v", string(d.Body))
	//		// rabbitmq.Ack, rabbitmq.NackDiscard, rabbitmq.NackRequeue
	//		return rabbitmq.Ack
	//	},
	//	"my_queue",
	//	rabbitmq.WithConsumerOptionsQOSPrefetch(100),
	//	rabbitmq.WithConsumerOptionsConsumerName("consumer_3"),
	//	rabbitmq.WithConsumerOptionsRoutingKey("my_routing_key"),
	//	rabbitmq.WithConsumerOptionsExchangeName("events"),
	//	rabbitmq.WithConsumerOptionsConcurrency(10),
	//)
	//if err != nil {
	//	log.Fatal(err)
	//}
	//defer consumer3.Close()

	// block main thread - wait for shutdown signal
	sigs := make(chan os.Signal, 1)
	done := make(chan bool, 1)

	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigs
		fmt.Println()
		fmt.Println(sig)
		done <- true
	}()

	fmt.Println("awaiting signal")
	<-done
	fmt.Println("stopping consumer")
}
