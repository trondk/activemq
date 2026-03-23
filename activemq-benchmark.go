package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Azure/go-amqp"
)

type ConnectionManager struct {
	servers    []string
	username   string
	password   string
	queueName  string
	currentIdx int
	mu         sync.RWMutex
	conn       *amqp.Conn
	session    *amqp.Session
	sender     *amqp.Sender
	receiver   *amqp.Receiver
	tlsConfig  *tls.Config
	ssl        bool
	durable    bool
	debug      bool
}

func main() {
	senderAddr := flag.String("sender", "", "Comma-separated list of sender broker addresses (e.g., 'localhost:5672,localhost:5673').")
	receiverAddr := flag.String("receiver", "", "Comma-separated list of receiver broker addresses (e.g., 'localhost:5672,localhost:5673').")
	msgSize := flag.Int("size", 1024, "The size of the payload for each message sent.")
	duration := flag.Duration("duration", 1*time.Second, "Duration to send messages (e.g., '1s', '5s', '1m').")
	username := flag.String("username", "admin", "Username for authentication.")
	password := flag.String("password", "admin", "Password for authentication.")
	queueName := flag.String("queue", "DLQ", "The target queue for sending and receiving messages.")
	durable := flag.Bool("durable", false, "Set message delivery mode to Persistent (Durable).")
	prometheusOut := flag.String("prometheus-out", "", "Optional: Path to write Prometheus .prom file for node exporter.")
	tlsEnabled := flag.Bool("tls", false, "Enable TLS/SSL encryption.")
	tlsCert := flag.String("tls-cert", "", "Path to client certificate file (optional).")
	tlsKey := flag.String("tls-key", "", "Path to client key file (optional).")
	tlsCA := flag.String("tls-ca", "", "Path to CA certificate file (optional).")
	insecure := flag.Bool("insecure", false, "Skip TLS certificate verification (insecure, use for testing only).")
	debug := flag.Bool("debug", false, "Enable debug logging for detailed connection and message information.")
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

	// Build TLS config if enabled
	var tlsConfig *tls.Config
	if *tlsEnabled {
		var err error
		tlsConfig, err = buildTLSConfig(*tlsCert, *tlsKey, *tlsCA, *insecure)
		if err != nil {
			log.Fatalf("Failed to build TLS config: %v", err)
		}
		log.Println("TLS/SSL enabled")
	}

	ctx := context.Background()

	// Create connection manager for sending
	senderConnMgr := &ConnectionManager{
		servers:   senderServers,
		username:  *username,
		password:  *password,
		queueName: *queueName,
		tlsConfig: tlsConfig,
		ssl:       *tlsEnabled,
		durable:   *durable,
		debug:     *debug,
	}

	// Connect sender
	log.Println("Connecting to sender broker(s)...")
	if err := senderConnMgr.ConnectSender(ctx); err != nil {
		log.Fatalf("Failed to establish sender connection: %v", err)
	}
	defer senderConnMgr.Close()

	// Phase 1: Send messages for specified duration
	log.Printf("Starting send phase for %v...\n", *duration)
	messagesSent, sendTime := sendPhase(ctx, senderConnMgr, *msgSize, *duration, *durable)
	log.Printf("Send phase complete. Sent %d messages in %.2f seconds\n", messagesSent, sendTime)

	// Create connection manager for receiving
	receiverConnMgr := &ConnectionManager{
		servers:   receiverServers,
		username:  *username,
		password:  *password,
		queueName: *queueName,
		tlsConfig: tlsConfig,
		ssl:       *tlsEnabled,
		durable:   *durable,
		debug:     *debug,
	}

	// Connect receiver
	log.Println("Connecting to receiver broker(s)...")
	if err := receiverConnMgr.ConnectReceiver(ctx); err != nil {
		log.Fatalf("Failed to establish receiver connection: %v", err)
	}
	defer receiverConnMgr.Close()

	// Phase 2: Consume all messages
	log.Println("Starting consume phase...")
	// Give broker a moment to settle messages
	time.Sleep(1 * time.Second)
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
		if cm.debug {
			log.Printf("DEBUG: Auth: %v, SSL: %v, Queue: %s", cm.username != "", cm.ssl, cm.queueName)
		}

		// Determine scheme based on SSL flag
		scheme := "amqp://"
		if cm.ssl {
			scheme = "amqps://"
		}

		// Create connection options with SASL PLAIN authentication
		connOpts := &amqp.ConnOptions{
			SASLType: amqp.SASLTypePlain(cm.username, cm.password),
		}
		if cm.debug {
			log.Printf("DEBUG: Using scheme %s", scheme)
		}

		// Add TLS configuration if SSL is enabled
		if cm.ssl {
			connOpts.TLSConfig = cm.tlsConfig
		}

		// Create AMQP connection
		conn, err := amqp.Dial(ctx, scheme+server, connOpts)
		if err != nil {
			log.Printf("Failed to connect to %s: %v", server, err)
			continue
		}

		// Create session
		session, err := conn.NewSession(ctx, nil)
		if err != nil {
			log.Printf("Failed to create session on %s: %v", server, err)
			conn.Close()
			continue
		}

		// Create sender with no options - let defaults handle it
		sender, err := session.NewSender(ctx, cm.queueName, nil)
		if err != nil {
			log.Printf("Failed to create sender on %s: %v", server, err)
			conn.Close()
			continue
		}

		cm.conn = conn
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
		if cm.debug {
			log.Printf("DEBUG: Auth: %v, SSL: %v, Queue: %s", cm.username != "", cm.ssl, cm.queueName)
		}

		// Determine scheme based on SSL flag
		scheme := "amqp://"
		if cm.ssl {
			scheme = "amqps://"
		}

		// Create connection options with SASL PLAIN authentication
		connOpts := &amqp.ConnOptions{
			SASLType: amqp.SASLTypePlain(cm.username, cm.password),
		}

		// Add TLS configuration if SSL is enabled
		if cm.ssl {
			connOpts.TLSConfig = cm.tlsConfig
		}

		// Create AMQP connection
		conn, err := amqp.Dial(ctx, scheme+server, connOpts)
		if err != nil {
			log.Printf("Failed to connect to %s: %v", server, err)
			continue
		}

		// Create session
		session, err := conn.NewSession(ctx, nil)
		if err != nil {
			log.Printf("Failed to create session on %s: %v", server, err)
			conn.Close()
			continue
		}

		// Create receiver with credit for flow control
		receiverOpts := &amqp.ReceiverOptions{
			Credit: 300, // Buffer 300 messages, will replenish as we accept them
		}
		receiver, err := session.NewReceiver(ctx, cm.queueName, receiverOpts)
		if cm.debug {
			log.Printf("DEBUG: Created receiver for queue %s with Credit: 300", cm.queueName)
		}
		if err != nil {
			log.Printf("Failed to create receiver on %s: %v", server, err)
			conn.Close()
			continue
		}

		cm.conn = conn
		cm.session = session
		cm.receiver = receiver
		cm.currentIdx = serverIdx
		log.Printf("Successfully connected receiver to %s", server)
		return nil
	}

	return fmt.Errorf("failed to connect receiver to any server in the cluster")
}

func (cm *ConnectionManager) GetSender() *amqp.Sender {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.sender
}

func (cm *ConnectionManager) GetReceiver() *amqp.Receiver {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.receiver
}

func (cm *ConnectionManager) Close() {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	ctx := context.Background()
	if cm.sender != nil {
		cm.sender.Close(ctx)
	}
	if cm.receiver != nil {
		cm.receiver.Close(ctx)
	}
	if cm.session != nil {
		cm.session.Close(ctx)
	}
	if cm.conn != nil {
		cm.conn.Close()
	}
}

func sendPhase(ctx context.Context, connMgr *ConnectionManager, msgSize int, duration time.Duration, durable bool) (uint64, float64) {
	// Create payload
	payload := make([]byte, msgSize)
	for i := range payload {
		payload[i] = 'X'
	}

	var messagesSent atomic.Uint64
	var messageNum uint64
	startTime := time.Now()
	deadline := startTime.Add(duration)

	// Send messages until deadline
	for time.Now().Before(deadline) {
		message := amqp.NewMessage(payload)

		// Add message number for tracking
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
			log.Printf("No active sender available")
			time.Sleep(100 * time.Millisecond)
			continue
		}

		err := sender.Send(ctx, message, nil)
		if err != nil {
			log.Printf("Failed to send message: %v", err)
			time.Sleep(100 * time.Millisecond)
			continue
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
	timeout := time.After(60 * time.Second)

	for messagesReceived.Load() < expectedMessages {
		select {
		case <-timeout:
			log.Printf("Consume phase timeout after 60 seconds. Received %d of %d expected messages", messagesReceived.Load(), expectedMessages)
			elapsed := time.Since(startTime).Seconds()
			return messagesReceived.Load(), elapsed

		default:
			receiver := connMgr.GetReceiver()
			if receiver == nil {
				log.Printf("No active receiver available")
				time.Sleep(100 * time.Millisecond)
				continue
			}

			// Receive with overall context timeout
			msg, err := receiver.Receive(ctx, nil)

			if err != nil {
				// Check if we've received all expected messages
				if messagesReceived.Load() >= expectedMessages {
					log.Printf("All expected messages received: %d", messagesReceived.Load())
					break
				}
				// Otherwise, log the error and continue
				log.Printf("Failed to receive message: %v (received %d/%d)", err, messagesReceived.Load(), expectedMessages)
				time.Sleep(100 * time.Millisecond)
				continue
			}

			// Accept the message (releases credit for next message)
			err = receiver.AcceptMessage(ctx, msg)
			if err != nil {
				log.Printf("Failed to accept message: %v", err)
				continue
			}

			// Log received message number if debug enabled
			if connMgr.debug && msg.ApplicationProperties != nil {
				if msgNum, ok := msg.ApplicationProperties["messageNumber"]; ok {
					if messagesReceived.Load()%100 == 0 {
						log.Printf("DEBUG: Received message #%v (%d/%d)", msgNum, messagesReceived.Load()+1, expectedMessages)
					}
				}
			}

			// Message received successfully
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

func buildTLSConfig(certFile, keyFile, caFile string, insecure bool) (*tls.Config, error) {
	tlsConfig := &tls.Config{
		InsecureSkipVerify: insecure,
	}

	// Load CA certificate if provided
	if caFile != "" {
		caCert, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA certificate: %v", err)
		}
		caCertPool, err := getCertPool(caCert)
		if err != nil {
			return nil, fmt.Errorf("failed to parse CA certificate: %v", err)
		}
		tlsConfig.RootCAs = caCertPool
	}

	// Load client certificate if provided
	if certFile != "" && keyFile != "" {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load client certificate: %v", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	return tlsConfig, nil
}

func getCertPool(caCert []byte) (*x509.CertPool, error) {
	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM(caCert) {
		return nil, fmt.Errorf("failed to append CA certificate to pool")
	}
	return caCertPool, nil
}
