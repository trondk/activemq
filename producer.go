package main

import (
	"context"
	"flag"
	"fmt"
	"io"
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
	servers    []string
	username   string
	password   string
	queueName  string
	currentIdx int
	mu         sync.RWMutex
	client     *amqp.Client
	session    *amqp.Session
	sender     *amqp.Sender
}

type MessageWithHeaders struct {
	Content []byte
	Headers map[string]interface{}
}

// parseSingleMessage parses a single message with headers.
func parseSingleMessage(messageData string) MessageWithHeaders {
	headers := make(map[string]interface{})
	content := messageData

	// Check if file has header section (separated by ---)
	parts := strings.Split(content, "---")
	if len(parts) >= 2 {
		// Parse header section
		headerSection := strings.TrimSpace(parts[0])
		lines := strings.Split(headerSection, "\n")

		for _, line := range lines {
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}

			// Parse key: value
			colonIdx := strings.Index(line, ":")
			if colonIdx > 0 {
				key := strings.TrimSpace(line[:colonIdx])
				value := strings.TrimSpace(line[colonIdx+1:])

				// Convert "true"/"false" to boolean
				if value == "true" {
					headers[key] = true
				} else if value == "false" {
					headers[key] = false
				} else {
					headers[key] = value
				}
			}
		}

		// Get message content (everything after ---)
		messageContent := strings.Join(parts[1:], "---")
		return MessageWithHeaders{
			Content: []byte(strings.TrimSpace(messageContent)),
			Headers: headers,
		}
	}

	// No header section, return content as-is
	return MessageWithHeaders{
		Content: []byte(content),
		Headers: headers,
	}
}

// parseMessagesFromFile parses a file that contains one or more messages
// separated by a delimiter.
func parseMessagesFromFile(data []byte, delimiter string) []MessageWithHeaders {
	messagesStr := string(data)
	messageParts := strings.Split(messagesStr, delimiter)
	var messages []MessageWithHeaders

	for _, part := range messageParts {
		if strings.TrimSpace(part) == "" {
			continue
		}
		messages = append(messages, parseSingleMessage(part))
	}

	return messages
}

func main() {
	serverAddr := flag.String("server", "", "Comma-separated list of host:port of the ActiveMQ Artemis AMQP acceptors (e.g., 'localhost:5672,localhost:5673').")
	msgSize := flag.Int("size", 1024, "The size of the payload for each message sent.")
	username := flag.String("username", "admin", "Username for authentication.")
	password := flag.String("password", "admin", "Password for authentication.")
	queueName := flag.String("queue", "DLQ", "The target queue where messages will be sent.")
	durable := flag.Bool("durable", false, "Set message delivery mode to Persistent (Durable).")
	inputFile := flag.String("file", "", "Optional: Load message content from a text file instead of generating dummy payload.")
	batchSize := flag.Int("batch", 100, "Number of messages to send before waiting for confirmation (async batch size).")
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

	// Load message content from file if specified
	var messages []MessageWithHeaders
	if *inputFile != "" {
		file, err := os.Open(*inputFile)
		if err != nil {
			log.Fatalf("Failed to open input file %s: %v", *inputFile, err)
		}
		defer file.Close()

		fileData, err := io.ReadAll(file)
		if err != nil {
			log.Fatalf("Failed to read input file %s: %v", *inputFile, err)
		}

		messages = parseMessagesFromFile(fileData, "%%%")
		log.Printf("Loaded %d messages from file: %s", len(messages), *inputFile)
	}

	var messagesSent atomic.Uint64
	startTime := time.Now()

	// Start the appropriate producer goroutine
	if len(messages) > 0 {
		go producer_file(ctx, connMgr, *durable, *batchSize, &messagesSent, messages)
	} else {
		go producer(ctx, connMgr, *msgSize, *durable, *batchSize, &messagesSent)
	}

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

		sender, err := session.NewSender(amqp.LinkTargetAddress(cm.queueName))
		if err != nil {
			log.Printf("Failed to create sender on %s: %v", server, err)
			client.Close()
			continue
		}

		cm.client = client
		cm.session = session
		cm.sender = sender
		cm.currentIdx = serverIdx
		log.Printf("Successfully connected to %s", server)
		return nil
	}

	return fmt.Errorf("failed to connect to any server in the cluster")
}

func (cm *ConnectionManager) Reconnect(ctx context.Context) error {
	cm.mu.Lock()
	// Close existing connections
	if cm.sender != nil {
		cm.sender.Close(context.Background())
	}
	if cm.client != nil {
		cm.client.Close()
	}
	cm.sender = nil
	cm.session = nil
	cm.client = nil
	// Move to next server
	cm.currentIdx = (cm.currentIdx + 1) % len(cm.servers)
	cm.mu.Unlock()

	// Wait a bit before reconnecting
	time.Sleep(1 * time.Second)

	return cm.Connect(ctx)
}

func (cm *ConnectionManager) GetSender() *amqp.Sender {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.sender
}

func (cm *ConnectionManager) Close() {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.sender != nil {
		cm.sender.Close(context.Background())
	}
	if cm.client != nil {
		cm.client.Close()
	}
}

func producer_file(ctx context.Context, connMgr *ConnectionManager, durable bool, batchSize int, messagesSent *atomic.Uint64, messages []MessageWithHeaders) {
	messageNum := uint64(0)
	for {
		for _, msgWithHeaders := range messages {
			select {
			case <-ctx.Done():
				return
			default:
				payload := msgWithHeaders.Content
				message := amqp.NewMessage(payload)

				// Set ApplicationProperties from parsed headers
				if len(msgWithHeaders.Headers) > 0 {
					message.ApplicationProperties = msgWithHeaders.Headers
					log.Printf("Set ApplicationProperties: %v", msgWithHeaders.Headers)
				} else {
					message.ApplicationProperties = make(map[string]interface{})
				}

				// Add increasing message number
				message.ApplicationProperties["messageNumber"] = messageNum
				messageNum++

				if durable {
					message.Header = &amqp.MessageHeader{
						Durable: true,
					}
				}

			retrySend:
				for {
					select {
					case <-ctx.Done():
						return
					default:
						sender := connMgr.GetSender()
						if sender == nil {
							log.Printf("No active sender, attempting to reconnect...")
							if err := connMgr.Reconnect(ctx); err != nil {
								log.Printf("Reconnection failed: %v", err)
								time.Sleep(1 * time.Second)
								continue
							}
							continue
						}

						err := sender.Send(ctx, message)
						if err != nil {
							log.Printf("Failed to send message: %v, attempting reconnection...", err)
							if err := connMgr.Reconnect(ctx); err != nil {
								log.Printf("Reconnection failed: %v", err)
							}
							time.Sleep(100 * time.Millisecond)
							continue
						}

						messagesSent.Add(1)
						break retrySend
					}
				}
				// Removed sleep - let batching control the flow
			}
		}
		log.Printf("All %d messages from file have been sent. Restarting...", len(messages))
		time.Sleep(1 * time.Second) // Wait before re-sending the batch of messages
	}
}

func producer(ctx context.Context, connMgr *ConnectionManager, msgSize int, durable bool, batchSize int, messagesSent *atomic.Uint64) {
	// Generate and send dummy payload continuously
	payload := make([]byte, msgSize)
	for i := range payload {
		payload[i] = 'X'
	}

	var messageNum uint64
	batchCount := 0

	for {
		select {
		case <-ctx.Done():
			return
		default:
			message := amqp.NewMessage(payload)

			// Set increasing message number in ApplicationProperties
			message.ApplicationProperties = map[string]interface{}{
				"messageNumber": messageNum,
			}
			messageNum++

			if durable {
				message.Header = &amqp.MessageHeader{
					Durable: true,
				}
			}

			sender := connMgr.GetSender()
			if sender == nil {
				log.Printf("No active sender, attempting to reconnect...")
				if err := connMgr.Reconnect(ctx); err != nil {
					log.Printf("Reconnection failed: %v", err)
					time.Sleep(1 * time.Second)
				}
				continue
			}

			// Send without waiting for each message (async)
			// The AMQP library handles flow control internally
			err := sender.Send(ctx, message)
			if err != nil {
				log.Printf("Failed to send message: %v, attempting reconnection...", err)
				if err := connMgr.Reconnect(ctx); err != nil {
					log.Printf("Reconnection failed: %v", err)
				}
				time.Sleep(100 * time.Millisecond)
				batchCount = 0
				continue
			}

			messagesSent.Add(1)
			batchCount++

			// Every batchSize messages, add a small yield to prevent CPU spin
			if batchCount >= batchSize {
				batchCount = 0
				// Small yield to allow other goroutines to run
				time.Sleep(1 * time.Microsecond)
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
