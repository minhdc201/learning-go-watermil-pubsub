package main

import (
	"context"
	"log"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-redisstream/pkg/redisstream"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/redis/go-redis/v9"
)

func process(messages <-chan *message.Message) {
	for msg := range messages {
		log.Printf("receive message: %s, payload: %s", msg.UUID, string(msg.Payload))
		msg.Ack()
	}
}

func publishMessages(publisher message.Publisher) {
	for {
		msg := message.NewMessage(watermill.NewUUID(), []byte("Hello, world!"))
		if err := publisher.Publish("example.topic", msg); err != nil {
			panic(err)
		}
		time.Sleep(time.Second)
	}
}

func main() {
	logger := watermill.NewStdLogger(true, false)

	// create new redis subscribing Client
	subClient := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   0,
	})

	// create new subscriber that subscribing the "test_consumer_group"
	subscriber, err := redisstream.NewSubscriber(
		redisstream.SubscriberConfig{
			Client:        subClient,
			Unmarshaller:  redisstream.DefaultMarshallerUnmarshaller{},
			ConsumerGroup: "test_consumer_group",
		},
		logger,
	)
	if err != nil {
		panic(err)
	}
	messages, err := subscriber.Subscribe(context.Background(), "example.topic")
	if err != nil {
		panic(err)
	}
	go process(messages)

	pubClient := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   0,
	})
	defer pubClient.Close()

	publisher, err := redisstream.NewPublisher(
		redisstream.PublisherConfig{
			Marshaller: redisstream.DefaultMarshallerUnmarshaller{},
			Client:     pubClient,
		},
		logger,
	)
	if err != nil {
		log.Fatalf("Couldn't create publisher: %v", err)
	}

	publishMessages(publisher)

}
