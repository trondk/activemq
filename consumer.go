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
	username := flag.String("username", "admin", "Username for authentication.")
	password := flag.String("password", "admin", "Password for authentication.")
	queueName := flag.String("queue", "DLQ", "The target queue where messages will be received from.")
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

	receiver, err := session.NewReceiver(amqp.LinkSourceAddress(*queueName))
	if err != nil {
		log.Fatalf("Failed to create AMQP receiver: %v", err)
	}
	defer receiver.Close(context.Background())

	var messagesReceived atomic.Uint64
	startTime := time.Now()

	// Start the consumer goroutine
	go consumer(ctx, receiver, &messagesReceived)

	// Start the throughput reporter goroutine
	go throughputReporter(ctx, &messagesReceived)

	<-ctx.Done()

	// Calculate and print final summary
	totalTime := time.Since(startTime).Seconds()
	totalMessages := messagesReceived.Load()
	avgThroughput := float64(totalMessages) / totalTime

	fmt.Println("\nShutdown complete.")
	fmt.Printf("Total messages received: %d\n", totalMessages)
	fmt.Printf("Overall average throughput: %.2f msg/s\n", avgThroughput)
}

func consumer(ctx context.Context, receiver *amqp.Receiver, messagesReceived *atomic.Uint64) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			msg, err := receiver.Receive(ctx)
			if err != nil {
				log.Printf("Failed to receive message: %v", err)
				continue
			}
			msg.Accept()
			messagesReceived.Add(1)
		}
	}
}

func throughputReporter(ctx context.Context, messagesReceived *atomic.Uint64) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	var lastCount uint64
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			currentCount := messagesReceived.Load()
			throughput := currentCount - lastCount
			lastCount = currentCount
			fmt.Printf("Throughput: %d msg/s\n", throughput)
		}
	}
}
