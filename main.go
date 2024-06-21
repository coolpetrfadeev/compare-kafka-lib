package main

import (
	"context"
	"fmt"
	"log"
	"time"

	confluent "github.com/confluentinc/confluent-kafka-go/kafka"
	segmentio "github.com/segmentio/kafka-go"
)

type Result struct {
	Name     string
	Duration time.Duration
}

func createTopicConfluent(topic string, broker string) {
	fmt.Printf("Creating topic %s on broker %s\n", topic, broker)
	adminClient, err := confluent.NewAdminClient(&confluent.ConfigMap{"bootstrap.servers": broker})
	if err != nil {
		panic(err)
	}
	defer adminClient.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	results, err := adminClient.CreateTopics(
		ctx,
		[]confluent.TopicSpecification{{
			Topic:             topic,
			NumPartitions:     1,
			ReplicationFactor: 1,
		}},
	)
	if err != nil {
		fmt.Printf("Failed to create topic: %v\n", err)
		return
	}

	for _, result := range results {
		fmt.Printf("Topic %s creation result: %v\n", result.Topic, result.Error)
	}
}

func createTopicSegmentio(topic string, brokers string) {
	conn, err := segmentio.Dial("tcp", brokers)
	if err != nil {
		log.Fatalf("failed to dial leader: %s", err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		log.Fatalf("failed to get controller: %s", err)
	}

	var controllerConn *segmentio.Conn
	controllerConn, err = segmentio.Dial("tcp", controller.Host)
	if err != nil {
		log.Fatalf("failed to dial controller: %s", err)
	}
	defer controllerConn.Close()

	err = controllerConn.CreateTopics(segmentio.TopicConfig{
		Topic:             topic,
		NumPartitions:     1,
		ReplicationFactor: 1,
	})

	if err != nil {
		log.Fatalf("failed to create topic: %s", err)
	}

	log.Println("Topic created successfully")
}

func produceMessagesConfluent(topic string, broker string, results chan<- Result) {
	start := time.Now()
	fmt.Printf("Producing messages to topic %s on broker %s\n", topic, broker)
	producer, err := confluent.NewProducer(&confluent.ConfigMap{"bootstrap.servers": broker})
	if err != nil {
		panic(err)
	}
	defer producer.Close()

	for i := 0; i < 100; i++ {
		message := fmt.Sprintf("Message %d from Confluent", i)
		producer.Produce(&confluent.Message{
			TopicPartition: confluent.TopicPartition{Topic: &topic, Partition: confluent.PartitionAny},
			Value:          []byte(message),
		}, nil)
		time.Sleep(1 * time.Millisecond)
	}

	producer.Flush(15 * 1000)

	duration := time.Since(start)
	results <- Result{"Confluent Producer", duration}
}

func produceMessagesSegmentio(topic string, broker string, results chan<- Result) {
	start := time.Now()
	fmt.Printf("Producing messages to topic %s on broker %s\n", topic, broker)
	writer := segmentio.NewWriter(segmentio.WriterConfig{
		Brokers: []string{broker},
		Topic:   topic,
	})
	defer writer.Close()

	for i := 0; i < 100; i++ {
		message := fmt.Sprintf("Message %d from Segmentio", i)
		err := writer.WriteMessages(context.Background(),
			segmentio.Message{
				Value: []byte(message),
			},
		)
		if err != nil {
			fmt.Println("could not write message " + err.Error())
		}
		time.Sleep(1 * time.Millisecond)
	}

	duration := time.Since(start)
	results <- Result{"Segmentio Producer", duration}
}

func consumeMessagesConfluent(topic string, broker string, results chan<- Result) {
	start := time.Now()
	fmt.Printf("Consuming messages from topic %s on broker %s\n", topic, broker)
	consumer, err := confluent.NewConsumer(&confluent.ConfigMap{
		"bootstrap.servers": broker,
		"group.id":          "test-group-confluent",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		panic(err)
	}
	defer consumer.Close()

	consumer.SubscribeTopics([]string{topic, "^aRegex.*[Tt]opic"}, nil)

	msgCount := 0
	for msgCount < 100 {
		msg, err := consumer.ReadMessage(-1)
		if err == nil {
			fmt.Printf("[Confluent] Received message: %s\n", string(msg.Value))
			msgCount++
		} else {
			fmt.Printf("[Confluent] Consumer error: %v (%v)\n", err, msg)
		}
	}

	duration := time.Since(start)
	results <- Result{"Confluent Consumer", duration}
}

func consumeMessagesSegmentio(topic string, broker string, results chan<- Result) {
	start := time.Now()
	fmt.Printf("Consuming messages from topic %s on broker %s\n", topic, broker)
	reader := segmentio.NewReader(segmentio.ReaderConfig{
		Brokers:   []string{broker},
		Topic:     topic,
		Partition: 0,
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
	})
	defer reader.Close()

	msgCount := 0
	for msgCount < 100 {
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			fmt.Printf("[Segmentio] could not read message: %v\n", err)
			continue
		}
		fmt.Printf("[Segmentio] Received message: %s\n", string(msg.Value))
		msgCount++
	}

	duration := time.Since(start)
	results <- Result{"Segmentio Consumer", duration}
}

func main() {
	topic1 := "topic1"
	topic2 := "topic2"
	results := make(chan Result, 4)

	createTopicConfluent(topic1, "kafka-1:9092")
	createTopicSegmentio(topic2, "kafka-2:9093")

	time.Sleep(3 * time.Second) // ждем пока создадутся топики

	go produceMessagesConfluent(topic1, "kafka-1:9092", results)
	go produceMessagesSegmentio(topic2, "kafka-2:9093", results)
	time.Sleep(3 * time.Second)
	go consumeMessagesConfluent(topic1, "kafka-1:9092", results)
	go consumeMessagesSegmentio(topic2, "kafka-2:9093", results)

	for i := 0; i < 4; i++ {
		result := <-results
		fmt.Printf("%s took %v\n", result.Name, result.Duration)
	}
}
