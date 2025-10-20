package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync/atomic"
	"time"

	"pack.ag/amqp"
)

func main() {
	serverAddr := flag.String("server", "", "The host and port of the ActiveMQ Artemis AMQP acceptor.")
	msgSize := flag.Int("size", 1024, "The size of the payload for each message sent.")
	username := flag.String("username", "admin", "Username for authentication.")
	password := flag.String("password", "admin", "Password for authentication.")
	queueName := flag.String("queue", "DLQ", "The target queue where messages will be sent.")
	flag.Parse()

	if *serverAddr == "" {
		flag.Usage()
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle graceful shutdown
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	go func() {
		<-interrupt
		cancel()
	}()

	client, err := amqp.Dial("amqp://"+*serverAddr, amqp.ConnSASLPlain(*username, *password))
	if err != nil {
		log.Fatalf("Failed to connect to AMQP server: %v", err)
	}
	defer client.Close()

	session, err := client.NewSession()
	if err != nil {
		log.Fatalf("Failed to create AMQP session: %v", err)
	}

	sender, err := session.NewSender(amqp.LinkTargetAddress(*queueName))
	if err != nil {
		log.Fatalf("Failed to create AMQP sender: %v", err)
	}
	defer sender.Close(context.Background())

	var messagesSent atomic.Uint64
	startTime := time.Now()

	// Start the producer goroutine
	go producer(ctx, sender, *msgSize, &messagesSent)

	// Start the throughput reporter goroutine
	go throughputReporter(ctx, &messagesSent)

	<-ctx.Done()

	// Calculate and print final summary
	totalTime := time.Since(startTime).Seconds()
	totalMessages := messagesSent.Load()
	avgThroughput := float64(totalMessages) / totalTime

	fmt.Println("\nShutdown complete.")
	fmt.Printf("Total messages sent: %d\n", totalMessages)
	fmt.Printf("Overall average throughput: %.2f msg/s\n", avgThroughput)
}

func producer(ctx context.Context, sender *amqp.Sender, msgSize int, messagesSent *atomic.Uint64) {
	payload := make([]byte, msgSize)
	for i := range payload {
		payload[i] = 'X'
	}
	message := amqp.NewMessage(payload)
	message.Annotations = map[interface{}]interface{}{
		"x-opt-delivery-mode": uint8(2),
	}
	for {
		select {
		case <-ctx.Done():
			return
		default:
			err := sender.Send(ctx, message)
			if err != nil {
				log.Printf("Failed to send message: %v", err)
				time.Sleep(100 * time.Millisecond)
			} else {
				messagesSent.Add(1)
			}
		}
	}
}

func throughputReporter(ctx context.Context, messagesSent *atomic.Uint64) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	var lastCount uint64
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			currentCount := messagesSent.Load()
			throughput := currentCount - lastCount
			lastCount = currentCount
			fmt.Printf("Throughput: %d msg/s\n", throughput)
		}
	}
}
