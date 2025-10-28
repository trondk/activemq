package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"pack.ag/amqp"
)

type ConnectionManager struct {
	servers     []string
	username    string
	password    string
	queueName   string
	currentIdx  int
	mu          sync.Mutex
	client      *amqp.Client
	session     *amqp.Session
	receiver    *amqp.Receiver
}

func main() {
	serverAddr := flag.String("server", "", "Comma-separated list of host:port of the ActiveMQ Artemis AMQP acceptors (e.g., 'localhost:5672,localhost:5673').")
	username := flag.String("username", "admin", "Username for authentication.")
	password := flag.String("password", "admin", "Password for authentication.")
	queueName := flag.String("queue", "DLQ", "The target queue where messages will be received from.")
	flag.Parse()

	if *serverAddr == "" {
		flag.Usage()
		os.Exit(1)
	}

	servers := strings.Split(*serverAddr, ",")

	// Trim spaces from server addresses
	for i := range servers {
		servers[i] = strings.TrimSpace(servers[i])
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

	connMgr := &ConnectionManager{
		servers:   servers,
		username:  *username,
		password:  *password,
		queueName: *queueName,
	}

	// Initial connection
	if err := connMgr.Connect(ctx); err != nil {
		log.Fatalf("Failed to establish initial connection: %v", err)
	}
	defer connMgr.Close()

	var messagesReceived atomic.Uint64
	startTime := time.Now()

	// Start the consumer goroutine
	go consumer(ctx, connMgr, &messagesReceived)

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

func (cm *ConnectionManager) Connect(ctx context.Context) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	// Try all servers in the cluster
	for i := 0; i < len(cm.servers); i++ {
		serverIdx := (cm.currentIdx + i) % len(cm.servers)
		server := cm.servers[serverIdx]

		log.Printf("Attempting to connect to %s...", server)
		client, err := amqp.Dial("amqp://"+server, amqp.ConnSASLPlain(cm.username, cm.password))
		if err != nil {
			log.Printf("Failed to connect to %s: %v", server, err)
			continue
		}

		session, err := client.NewSession()
		if err != nil {
			log.Printf("Failed to create session on %s: %v", server, err)
			client.Close()
			continue
		}

		receiver, err := session.NewReceiver(amqp.LinkSourceAddress(cm.queueName))
		if err != nil {
			log.Printf("Failed to create receiver on %s: %v", server, err)
			client.Close()
			continue
		}

		cm.client = client
		cm.session = session
		cm.receiver = receiver
		cm.currentIdx = serverIdx
		log.Printf("Successfully connected to %s", server)
		return nil
	}

	return fmt.Errorf("failed to connect to any server in the cluster")
}

func (cm *ConnectionManager) Reconnect(ctx context.Context) error {
	cm.mu.Lock()
	// Close existing connections
	if cm.receiver != nil {
		cm.receiver.Close(context.Background())
	}
	if cm.client != nil {
		cm.client.Close()
	}
	cm.receiver = nil
	cm.session = nil
	cm.client = nil
	// Move to next server
	cm.currentIdx = (cm.currentIdx + 1) % len(cm.servers)
	cm.mu.Unlock()

	// Wait a bit before reconnecting
	time.Sleep(1 * time.Second)

	return cm.Connect(ctx)
}

func (cm *ConnectionManager) GetReceiver() *amqp.Receiver {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.receiver
}

func (cm *ConnectionManager) Close() {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.receiver != nil {
		cm.receiver.Close(context.Background())
	}
	if cm.client != nil {
		cm.client.Close()
	}
}

func consumer(ctx context.Context, connMgr *ConnectionManager, messagesReceived *atomic.Uint64) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			receiver := connMgr.GetReceiver()
			if receiver == nil {
				log.Printf("No active receiver, attempting to reconnect...")
				if err := connMgr.Reconnect(ctx); err != nil {
					log.Printf("Reconnection failed: %v", err)
					time.Sleep(1 * time.Second)
					continue
				}
				receiver = connMgr.GetReceiver()
			}

			msg, err := receiver.Receive(ctx)
			if err != nil {
				log.Printf("Failed to receive message: %v, attempting reconnection...", err)
				if err := connMgr.Reconnect(ctx); err != nil {
					log.Printf("Reconnection failed: %v", err)
				}
				time.Sleep(100 * time.Millisecond)
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
