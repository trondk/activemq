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

type MessageIDExporter struct {
	file   *os.File
	mu     sync.Mutex
	closed bool
}

func NewMessageIDExporter(filePath string) (*MessageIDExporter, error) {
	file, err := os.Create(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create export file: %w", err)
	}
	return &MessageIDExporter{
		file:   file,
		closed: false,
	}, nil
}

func (e *MessageIDExporter) Write(messageID string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return fmt.Errorf("exporter is closed")
	}

	_, err := fmt.Fprintln(e.file, messageID)
	return err
}

func (e *MessageIDExporter) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return nil
	}

	e.closed = true
	return e.file.Close()
}

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
	username := flag.String("username", "", "Username for authentication (optional).")
	password := flag.String("password", "", "Password for authentication (optional).")
	queueName := flag.String("queue", "DLQ", "The target queue where messages will be received from.")
	exportMessageIDs := flag.String("export-message-ids", "", "Optional file path to export message IDs to check order (e.g., 'message_ids.txt').")
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

	// Setup message ID exporter if requested
	var exporter *MessageIDExporter
	if *exportMessageIDs != "" {
		var err error
		exporter, err = NewMessageIDExporter(*exportMessageIDs)
		if err != nil {
			log.Fatalf("Failed to create message ID exporter: %v", err)
		}
		defer exporter.Close()
	}

	// Start the consumer goroutine
	go consumer(ctx, connMgr, &messagesReceived, exporter)

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

		var client *amqp.Client
		var err error

		// Use authentication if username is provided, otherwise connect anonymously
		if cm.username != "" {
			client, err = amqp.Dial("amqp://"+server, amqp.ConnSASLPlain(cm.username, cm.password))
		} else {
			client, err = amqp.Dial("amqp://"+server, amqp.ConnSASLAnonymous())
		}

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

func consumer(ctx context.Context, connMgr *ConnectionManager, messagesReceived *atomic.Uint64, exporter *MessageIDExporter) {
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

			// Export message number if exporter is configured
			if exporter != nil {
				if msg.ApplicationProperties != nil {
					if messageNum, ok := msg.ApplicationProperties["messageNumber"]; ok {
						msgNumStr := fmt.Sprintf("%v", messageNum)
						if err := exporter.Write(msgNumStr); err != nil {
							log.Printf("Failed to write message number to file: %v", err)
						} else {
							log.Printf("Exported message number: %s", msgNumStr)
						}
					} else {
						log.Printf("Warning: Message has no messageNumber in ApplicationProperties")
					}
				} else {
					log.Printf("Warning: Message has no ApplicationProperties")
				}
			}
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
