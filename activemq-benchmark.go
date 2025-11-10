package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
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
	mu         sync.Mutex
	client     *amqp.Client
	session    *amqp.Session
	sender     *amqp.Sender
	receiver   *amqp.Receiver
}

func main() {
	senderAddr := flag.String("sender", "", "Comma-separated list of sender broker addresses (e.g., 'localhost:5672,localhost:5673').")
	receiverAddr := flag.String("receiver", "", "Comma-separated list of receiver broker addresses (e.g., 'localhost:5672,localhost:5673').")
	msgSize := flag.Int("size", 1024, "The size of the payload for each message sent.")
	duration := flag.Duration("duration", 1*time.Second, "Duration to send messages (e.g., '1s', '5s', '1m').")
	username := flag.String("username", "admin", "Username for authentication.")
	password := flag.String("password", "admin", "Password for authentication.")
	queueName := flag.String("queue", "DLQ", "The target queue for sending and receiving messages.")
	prometheusOut := flag.String("prometheus-out", "", "Optional: Path to write Prometheus .prom file for node exporter.")
	flag.Parse()

	if *senderAddr == "" {
		fmt.Println("Error: -sender parameter is required")
		flag.Usage()
		os.Exit(1)
	}

	// If receiver not specified, use same as sender
	if *receiverAddr == "" {
		*receiverAddr = *senderAddr
	}

	senderServers := strings.Split(*senderAddr, ",")
	receiverServers := strings.Split(*receiverAddr, ",")

	// Trim spaces from server addresses
	for i := range senderServers {
		senderServers[i] = strings.TrimSpace(senderServers[i])
	}
	for i := range receiverServers {
		receiverServers[i] = strings.TrimSpace(receiverServers[i])
	}

	ctx := context.Background()

	// Create connection manager for sending
	senderConnMgr := &ConnectionManager{
		servers:   senderServers,
		username:  *username,
		password:  *password,
		queueName: *queueName,
	}

	// Connect sender
	log.Println("Connecting to sender broker(s)...")
	if err := senderConnMgr.ConnectSender(ctx); err != nil {
		log.Fatalf("Failed to establish sender connection: %v", err)
	}
	defer senderConnMgr.Close()

	// Phase 1: Send messages for specified duration
	log.Printf("Starting send phase for %v...\n", *duration)
	messagesSent, sendTime := sendPhase(ctx, senderConnMgr, *msgSize, *duration)
	log.Printf("Send phase complete. Sent %d messages in %.2f seconds\n", messagesSent, sendTime)

	// Create connection manager for receiving
	receiverConnMgr := &ConnectionManager{
		servers:   receiverServers,
		username:  *username,
		password:  *password,
		queueName: *queueName,
	}

	// Connect receiver
	log.Println("Connecting to receiver broker(s)...")
	if err := receiverConnMgr.ConnectReceiver(ctx); err != nil {
		log.Fatalf("Failed to establish receiver connection: %v", err)
	}
	defer receiverConnMgr.Close()

	// Phase 2: Consume all messages
	log.Println("Starting consume phase...")
	messagesReceived, consumeTime := consumePhase(ctx, receiverConnMgr, messagesSent)
	log.Printf("Consume phase complete. Received %d messages in %.2f seconds\n", messagesReceived, consumeTime)

	// Calculate and print results
	sendSpeed := float64(messagesSent) / sendTime
	consumeSpeed := float64(messagesReceived) / consumeTime
	totalTime := sendTime + consumeTime

	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Println("BENCHMARK RESULTS")
	fmt.Println(strings.Repeat("=", 60))
	fmt.Printf("Queue: %s\n", *queueName)
	fmt.Printf("Message size: %d bytes\n", *msgSize)
	fmt.Printf("Send duration: %v\n", *duration)
	fmt.Println(strings.Repeat("-", 60))
	fmt.Printf("Messages sent:     %d\n", messagesSent)
	fmt.Printf("Send time:         %.2f seconds\n", sendTime)
	fmt.Printf("Send speed:        %.2f msg/s\n", sendSpeed)
	fmt.Println(strings.Repeat("-", 60))
	fmt.Printf("Messages received: %d\n", messagesReceived)
	fmt.Printf("Consume time:      %.2f seconds\n", consumeTime)
	fmt.Printf("Consume speed:     %.2f msg/s\n", consumeSpeed)
	fmt.Println(strings.Repeat("-", 60))
	fmt.Printf("Total test time:   %.2f seconds\n", totalTime)
	fmt.Println(strings.Repeat("=", 60))

	// Write Prometheus output if requested
	if *prometheusOut != "" {
		err := writePrometheusMetrics(*prometheusOut, messagesSent, messagesReceived, sendSpeed, consumeSpeed, sendTime, consumeTime, totalTime, *senderAddr, *receiverAddr)
		if err != nil {
			log.Printf("Failed to write Prometheus metrics: %v", err)
		} else {
			log.Printf("Prometheus metrics written to %s", *prometheusOut)
		}
	}
}

func (cm *ConnectionManager) ConnectSender(ctx context.Context) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	// Try all servers in the cluster
	for i := 0; i < len(cm.servers); i++ {
		serverIdx := (cm.currentIdx + i) % len(cm.servers)
		server := cm.servers[serverIdx]

		log.Printf("Attempting to connect sender to %s...", server)
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
		log.Printf("Successfully connected sender to %s", server)
		return nil
	}

	return fmt.Errorf("failed to connect sender to any server in the cluster")
}

func (cm *ConnectionManager) ConnectReceiver(ctx context.Context) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	// Try all servers in the cluster
	for i := 0; i < len(cm.servers); i++ {
		serverIdx := (cm.currentIdx + i) % len(cm.servers)
		server := cm.servers[serverIdx]

		log.Printf("Attempting to connect receiver to %s...", server)
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
		log.Printf("Successfully connected receiver to %s", server)
		return nil
	}

	return fmt.Errorf("failed to connect receiver to any server in the cluster")
}

func (cm *ConnectionManager) Close() {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.sender != nil {
		cm.sender.Close(context.Background())
	}
	if cm.receiver != nil {
		cm.receiver.Close(context.Background())
	}
	if cm.client != nil {
		cm.client.Close()
	}
}

func sendPhase(ctx context.Context, connMgr *ConnectionManager, msgSize int, duration time.Duration) (uint64, float64) {
	// Create payload
	payload := make([]byte, msgSize)
	for i := range payload {
		payload[i] = 'X'
	}

	var messagesSent atomic.Uint64
	startTime := time.Now()
	deadline := startTime.Add(duration)

	// Send messages until deadline
	for time.Now().Before(deadline) {
		message := amqp.NewMessage(payload)

		sender := connMgr.sender
		if sender == nil {
			log.Printf("No active sender available")
			break
		}

		err := sender.Send(ctx, message)
		if err != nil {
			log.Printf("Failed to send message: %v", err)
			break
		}

		messagesSent.Add(1)
	}

	elapsed := time.Since(startTime).Seconds()
	return messagesSent.Load(), elapsed
}

func consumePhase(ctx context.Context, connMgr *ConnectionManager, expectedMessages uint64) (uint64, float64) {
	var messagesReceived atomic.Uint64
	startTime := time.Now()

	// Set a timeout to avoid waiting forever if messages are lost
	timeout := time.After(30 * time.Second)

	for messagesReceived.Load() < expectedMessages {
		select {
		case <-timeout:
			log.Printf("Consume phase timeout after 30 seconds")
			elapsed := time.Since(startTime).Seconds()
			return messagesReceived.Load(), elapsed

		default:
			receiver := connMgr.receiver
			if receiver == nil {
				log.Printf("No active receiver available")
				break
			}

			// Use a context with timeout for each receive
			receiveCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			msg, err := receiver.Receive(receiveCtx)
			cancel()

			if err != nil {
				// Check if we've received all expected messages
				if messagesReceived.Load() >= expectedMessages {
					break
				}
				// Otherwise, log the error and continue
				log.Printf("Failed to receive message: %v", err)
				continue
			}

			msg.Accept()
			messagesReceived.Add(1)
		}
	}

	elapsed := time.Since(startTime).Seconds()
	return messagesReceived.Load(), elapsed
}

func writePrometheusMetrics(filename string, messagesSent, messagesReceived uint64, sendSpeed, consumeSpeed, sendTime, consumeTime, totalTime float64, sender, receiver string) error {
	f, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create file: %v", err)
	}
	defer f.Close()

	_, err = fmt.Fprintf(f, `# HELP activemq_benchmark_messages_sent_total Total messages sent during benchmark
# TYPE activemq_benchmark_messages_sent_total counter
activemq_benchmark_messages_sent_total{sender="%s",receiver="%s"} %d

# HELP activemq_benchmark_messages_received_total Total messages received during benchmark
# TYPE activemq_benchmark_messages_received_total counter
activemq_benchmark_messages_received_total{sender="%s",receiver="%s"} %d

# HELP activemq_benchmark_send_throughput_msg_per_sec Send throughput in messages per second
# TYPE activemq_benchmark_send_throughput_msg_per_sec gauge
activemq_benchmark_send_throughput_msg_per_sec{sender="%s",receiver="%s"} %.2f

# HELP activemq_benchmark_receive_throughput_msg_per_sec Receive throughput in messages per second
# TYPE activemq_benchmark_receive_throughput_msg_per_sec gauge
activemq_benchmark_receive_throughput_msg_per_sec{sender="%s",receiver="%s"} %.2f

# HELP activemq_benchmark_send_duration_seconds Send phase duration in seconds
# TYPE activemq_benchmark_send_duration_seconds gauge
activemq_benchmark_send_duration_seconds{sender="%s",receiver="%s"} %.3f

# HELP activemq_benchmark_receive_duration_seconds Receive phase duration in seconds
# TYPE activemq_benchmark_receive_duration_seconds gauge
activemq_benchmark_receive_duration_seconds{sender="%s",receiver="%s"} %.3f

# HELP activemq_benchmark_total_duration_seconds Total benchmark duration in seconds
# TYPE activemq_benchmark_total_duration_seconds gauge
activemq_benchmark_total_duration_seconds{sender="%s",receiver="%s"} %.3f
`,
		sender, receiver, messagesSent,
		sender, receiver, messagesReceived,
		sender, receiver, sendSpeed,
		sender, receiver, consumeSpeed,
		sender, receiver, sendTime,
		sender, receiver, consumeTime,
		sender, receiver, totalTime,
	)

	return err
}
